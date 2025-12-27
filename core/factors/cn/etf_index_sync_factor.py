# core/factors/cn/etf_index_sync_factor.py
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
# - 单一结构性评分（越低 = 承接/一致性越差）
# - 不引入新枚举（仅使用 build_result 的 level_from_score）
# - data_status 仅写入 details
# ------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.ETFIndexSyncFactor")


class ETFIndexSyncFactor(FactorBase):
    """
    ETF × Index × Spot 同步性结构因子（Phase-2）

    结构含义（只读解释）：
    - ETF 与主线指数方向是否一致（同向/背离）
    - ETF 的表现是否得到全市场横截面的“广泛承接”（adv_ratio）
    - 市场资金是否过度集中（top20_turnover_ratio）
    - 市场涨跌分化是否过高（dispersion）

    注意：
    - 不做趋势、不做预测、不做 Gate 决策
    - 仅输出结构性评分 + 审计细节
    """

    def __init__(self) -> None:
        super().__init__(name="etf_index_sync")

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
        spot_blk = self.pick(input_block, "etf_spot_sync", None)
        if not isinstance(spot_blk, dict) or not spot_blk:
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "etf_spot_sync missing or invalid",
                },
            )

        adv_ratio = self._to_float(spot_blk.get("adv_ratio"))
        top20_ratio = self._to_float(spot_blk.get("top20_turnover_ratio"))
        dispersion = self._to_float(spot_blk.get("dispersion"))

        if adv_ratio is None or top20_ratio is None or dispersion is None:
            LOG.warning(
                "[ETFIndexSyncFactor] SPOT_MISSING_FIELDS | adv_ratio=%r | top20_ratio=%r | dispersion=%r",
                spot_blk.get("adv_ratio"),
                spot_blk.get("top20_turnover_ratio"),
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
        turnover_stage = spot_blk.get("turnover_stage") if isinstance(spot_blk.get("turnover_stage"), str) else None

        # ===============================
        # 3) 结构度量
        # ===============================
        same_direction = (idx_pct >= 0 and etf_pct >= 0) or (idx_pct <= 0 and etf_pct <= 0)
        divergence_idx = abs(etf_pct - idx_pct)

        # ===============================
        # 4) 结构评分（保守、非对称；仅规则映射，不做预测）
        # ===============================
        score = 50.0

        # ---- ETF × Index 方向一致性 ----
        if same_direction:
            score += 15.0
        else:
            score -= 25.0

        # ETF 明显强，但指数不确认（结构性风险：单边拉ETF/主题，指数未跟随）
        if etf_pct > 0 and idx_pct <= 0:
            score -= 25.0

        # 背离过大（幅度差异太大）
        if divergence_idx >= 1.5:
            score -= 10.0

        # ---- ETF × Spot 横截面承接质量 ----
        # 上涨承接不足（参与面偏弱）
        if adv_ratio < 0.45:
            score -= 15.0

        # 成交额过度集中（资金未扩散 → 结构韧性下降）
        if top20_ratio >= 0.60:
            score -= 10.0

        # 分化过高（结构不稳）
        if dispersion >= 2.50:
            score -= 10.0

        # clamp
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
            "top20_turnover_ratio": round(float(top20_ratio), 4),
            "dispersion": round(float(dispersion), 4),

            # 盘中/盘后标注：只读，不参与枚举体系
            "snapshot_type": snapshot_type,
            "turnover_stage": turnover_stage,

            # 结构解释（不引入枚举，仅文本标注）
            "interpretation": self._interpret(same_direction, divergence_idx, adv_ratio, top20_ratio, dispersion),
            # 保留最小 raw（不把整个 core_theme 塞进去）
            "_raw_ref": {
                "core_theme_keys": list(core_theme.keys())[:20],
                "spot_keys": list(spot_blk.keys())[:40],
            },
        }

        LOG.info(
            "[ETFIndexSyncFactor] result | score=%.2f | level=%s | same_direction=%s | div=%.3f",
            score,
            self.level_from_score(score),
            same_direction,
            float(divergence_idx),
        )
        return self.build_result(score=score, details=details)

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
        """
        parts: Dict[str, str] = {}

        parts["direction"] = "same" if same_direction else "diverged"

        if divergence_idx >= 1.5:
            parts["divergence"] = "high"
        elif divergence_idx >= 0.8:
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
