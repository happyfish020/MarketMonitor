# core/factors/cn/crowding_concentration_factor.py
# UnifiedRisk V12 FULL
#
# ETF × Index Proxy × Spot Synchronization Factor
#
# Phase-2 结构因子（不参与预测）
# ------------------------------------------------------------
# 输入依赖（只读）：
# - input_block["core_theme_raw"]
# - input_block["etf_spot_sync"]
#
# 输出：
# - 单一结构性评分（越高 = 承接/一致性越好；越低 = 执行摩擦/不同步越高）
# - 不引入新枚举（仅使用 build_result 的 level_from_score）
# - data_status 仅写入 details
# ------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.CrowdingConcentrationFactor")


class CrowdingConcentrationFactor(FactorBase):
    """
    ETF × Index × Spot 同步性结构因子（Phase-2）

    结构含义（只读解释）：
    - ETF 与主线指数方向是否一致（同向/背离）
    - ETF 的表现是否得到全市场横截面的“广泛承接”（adv_ratio）
    - 市场资金是否过度集中（top20_amount_ratio）
    - 市场涨跌分化是否过高（dispersion）

    注意：
    - 不做趋势、不做预测、不做 Gate 决策
    - 仅输出结构性评分 + 审计细节
    """

    def __init__(self) -> None:
        super().__init__(name="crowding_concentration")

    # ---------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        # 冻结铁律：不抛异常，不依赖 assert
        # 只做尽力解析；缺失 → score=50, level=NEUTRAL + data_status
        self._require_dict(input_block, factor_name=self.name)

        # ===============================
        # 1) core_theme_raw（ETF / Index proxy）
        # ===============================
        core_theme = self.pick(input_block, "core_theme_raw", None)
        if not isinstance(core_theme, dict) or not core_theme:
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "core_theme_raw missing or invalid",
                },
            )

        index_blk, etf_blk, pick_reason = self._pick_index_and_etf(core_theme)
        if index_blk is None or etf_blk is None:
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "INVALID_CORE_THEME_STRUCTURE",
                    "reason": pick_reason,
                    "core_theme_keys": list(core_theme.keys())[:20],
                },
            )

        idx_pct = self._to_float(index_blk.get("pct"))
        etf_pct = self._to_float(etf_blk.get("pct"))
        if idx_pct is None or etf_pct is None:
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "PCT_MISSING_OR_INVALID",
                    "index_symbol": index_blk.get("symbol"),
                    "etf_symbol": etf_blk.get("symbol"),
                    "index_pct_raw": index_blk.get("pct"),
                    "etf_pct_raw": etf_blk.get("pct"),
                },
            )

        # ===============================
        # 2) etf_spot_sync（横截面事实）
        # ===============================
        spot_blk = self.pick(input_block, "etf_spot_sync_daily", None)
        if not isinstance(spot_blk, dict) or not spot_blk:
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "etf_spot_sync missing or invalid",
                },
            )

        adv_ratio = self._to_float(spot_blk.get("adv_ratio"))
        top20_ratio = self._to_float(spot_blk.get("top20_amount_ratio"))
        dispersion = self._to_float(spot_blk.get("dispersion"))

        if adv_ratio is None or top20_ratio is None or dispersion is None:
            LOG.warning(
                "[CrowdingConcentrationFactor] SPOT_MISSING_FIELDS | adv_ratio=%r | top20_ratio=%r | dispersion=%r",
                spot_blk.get("adv_ratio"),
                spot_blk.get("top20_amount_ratio"),
                spot_blk.get("dispersion"),
            )
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "SPOT_MISSING_FIELDS",
                    "spot_keys": list(spot_blk.keys())[:40],
                },
            )

        # 可选：盘中/盘后标注（只读，不影响 score 的“预测性”）
        snapshot_type = spot_blk.get("snapshot_type") if isinstance(spot_blk.get("snapshot_type"), str) else None
        amount_stage = spot_blk.get("amount_stage") if isinstance(spot_blk.get("amount_stage"), str) else None

        # ===============================
        # 3) 结构度量
        # ===============================
        same_direction = (idx_pct >= 0 and etf_pct >= 0) or (idx_pct <= 0 and etf_pct <= 0)
        divergence_idx = abs(etf_pct - idx_pct)  # 注意：pct 为小数（如 -0.0054 = -0.54%）

        # ===============================
        # 4) 结构评分（冻结：对“跨市场/主题联动”更宽容）
        # ===============================
        # 设计原则：
        # - score 表示“结构一致性/承接质量”（越高越好）
        # - 先算 friction_score（越高 = 摩擦越大），再转 score = 100 - friction
        # - 不允许因为 same_direction=False 就直接打到 0 分（避免与你的字段语义冲突）
        friction_score, friction_reasons = self._calc_friction(
            same_direction=same_direction,
            divergence_idx=float(divergence_idx),
            adv_ratio=float(adv_ratio),
            top20_ratio=float(top20_ratio),
            dispersion=float(dispersion),
        )
        score = 100.0 - float(friction_score)
        score = self.clamp_score(score)

        # ===============================
        # 5) 细节输出（审计友好，不塞巨量 raw）
        # ===============================
        details: Dict[str, Any] = {
            "data_status": "OK",
            "index_symbol": index_blk.get("symbol"),
            "etf_symbol": etf_blk.get("symbol"),
            "index_pct": idx_pct,
            "etf_pct": etf_pct,
            "same_direction": same_direction,
            "divergence_index": round(float(divergence_idx), 4),

            "adv_ratio": round(float(adv_ratio), 4),
            "top20_amount_ratio": round(float(top20_ratio), 4),
            "dispersion": round(float(dispersion), 4),

            # 盘中/盘后标注：只读，不参与枚举体系
            "snapshot_type": snapshot_type,
            "amount_stage": amount_stage,

            # 结构解释（不引入枚举，仅文本标注）
            "interpretation": self._interpret(
                same_direction,
                float(divergence_idx),
                float(adv_ratio),
                float(top20_ratio),
                float(dispersion),
            ),

            # 评分审计（append-only）
            "score_semantics": "QUALITY_100_MINUS_FRICTION",
            "friction_score": round(float(friction_score), 2),
            "friction_reasons": friction_reasons,

            # 保留最小 raw（不把整个 core_theme 塞进去）
            "_raw_ref": {
                "core_theme_keys": list(core_theme.keys())[:20],
                "spot_keys": list(spot_blk.keys())[:40],
            },
        }

        LOG.info(
            "[CrowdingConcentrationFactor] result | score=%.2f | level=%s | same_direction=%s | div=%.4f | friction=%.2f",
            score,
            self.level_from_score(score),
            same_direction,
            float(divergence_idx),
            float(friction_score),
        )
        return self.build_result(score=score, details=details)

    # ---------------------------------------------------------
    def _calc_friction(
        self,
        same_direction: bool,
        divergence_idx: float,
        adv_ratio: float,
        top20_ratio: float,
        dispersion: float,
    ) -> Tuple[float, list]:
        """
        Frozen minimal scoring (explainable):

        friction_score 越高 ⇒ 执行摩擦/胜率下降越明显（拥挤/参与弱/集中/不同步）
        quality score = 100 - friction_score

        权重目标：让你贴的典型读数（crowding=high, participation=weak, top20≈0.77, divergence low）
        不再落到 score=0，而是落到“中等偏差”（例如 score≈55, level≈MED）。
        """
        f = 0.0
        reasons: list = []

        # crowding: high (>=0.60) → 摩擦上升
        if top20_ratio >= 0.60:
            f += 15.0
            reasons.append(f"crowding_high(top20={top20_ratio:.3f})")

        # extreme concentration (>0.70) → 进一步惩罚
        if top20_ratio > 0.70:
            f += 15.0
            reasons.append(f"top20_extreme(top20={top20_ratio:.3f})")

        # participation weak (adv_ratio < 0.45)
        if adv_ratio < 0.45:
            f += 20.0
            reasons.append(f"participation_weak(adv={adv_ratio:.3f})")

        # direction diverged (same_direction False) → 小幅摩擦
        if not same_direction:
            f += 5.0
            reasons.append("direction_diverged")

        # dispersion only penalize when truly high (>=2.5)
        if dispersion >= 2.50:
            f += 10.0
            reasons.append(f"dispersion_high(disp={dispersion:.3f})")

        # divergence magnitude (pct is fraction)
        if divergence_idx >= 0.015:  # 1.5%+
            f += 15.0
            reasons.append(f"divergence_high(div={divergence_idx:.4f})")
        elif divergence_idx >= 0.008:  # 0.8%+
            f += 8.0
            reasons.append(f"divergence_moderate(div={divergence_idx:.4f})")
        elif divergence_idx < 0.010:  # <1%: 缓和项（不是撕裂）
            f -= 10.0
            reasons.append(f"divergence_low_relief(div={divergence_idx:.4f})")

        # clamp
        if f < 0.0:
            f = 0.0
        if f > 100.0:
            f = 100.0

        return f, reasons

    # ---------------------------------------------------------
    def _pick_index_and_etf(self, core_theme: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
        """
        从 core_theme_raw 中挑出 index proxy 和 etf proxy。
        冻结原则：只做尽力解析，不假设结构一定正确。
        """
        index_blk: Optional[Dict[str, Any]] = None
        etf_blk: Optional[Dict[str, Any]] = None

        for _, blk in core_theme.items():
            if not isinstance(blk, dict):
                continue
            symbol = blk.get("symbol")
            if not isinstance(symbol, str) or not symbol:
                continue

            # 粗粒度规则：^ 开头（美股指数），或 .HK 结尾（港股指数proxy）
            if symbol.startswith("^") or symbol.endswith(".HK"):
                index_blk = blk
            else:
                # 其余默认当 ETF proxy
                etf_blk = blk

        if index_blk is None and etf_blk is None:
            return None, None, "no_valid_blocks"

        if index_blk is None:
            return None, etf_blk, "index_proxy_not_found"

        if etf_blk is None:
            return index_blk, None, "etf_proxy_not_found"

        return index_blk, etf_blk, "ok"

    def _to_float(self, v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    def _interpret(
        self,
        same_direction: bool,
        divergence_idx: float,
        adv_ratio: float,
        top20_ratio: float,
        dispersion: float,
    ) -> Dict[str, str]:
        """
        只读解释：不做预测，不引入枚举。
        注意：pct 为小数（如 0.0078 = 0.78%），因此阈值应按“小数”标度设置。
        """
        parts: Dict[str, str] = {}

        parts["direction"] = "same" if same_direction else "diverged"

        # divergence magnitude (fraction)
        if divergence_idx >= 0.015:
            parts["divergence"] = "high"
        elif divergence_idx >= 0.008:
            parts["divergence"] = "moderate"
        else:
            parts["divergence"] = "low"

        if adv_ratio < 0.45:
            parts["participation"] = "weak"
        elif adv_ratio >= 0.60:
            parts["participation"] = "strong"
        else:
            parts["participation"] = "neutral"

        if top20_ratio >= 0.60:
            parts["crowding"] = "high"
        elif top20_ratio >= 0.52:
            parts["crowding"] = "moderate"
        else:
            parts["crowding"] = "low"

        if dispersion >= 2.50:
            parts["dispersion"] = "high"
        elif dispersion >= 2.00:
            parts["dispersion"] = "moderate"
        else:
            parts["dispersion"] = "low"

        return parts
