# core/factors/cn/etf_index_sync_factor.py
# UnifiedRisk V12 FULL
#
# ETF × Index Proxy × Spot Synchronization Factor
#
# Phase-2 结构因子（不参与预测）
# ------------------------------------------------------------
# 输入依赖（只读）：
# - input_block["core_theme_raw"]
# - input_block["etf_spot_sync_raw"]
#
# 输出：
# - 单一结构性风险评分（越低 = 承接越差）
# - 不引入新枚举
# - data_status 仅写入 details
# ------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict


from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.ETFIndexSyncFactor")

class ETFIndexSyncFactor(FactorBase):
    """
    ETF × Index × Spot 同步性结构因子（Phase-2）

    结构含义：
    - ETF 是否与主线指数方向一致
    - ETF 上涨是否得到市场横截面的“广泛承接”
    """

    def __init__(self) -> None:
        super().__init__(name="etf_index_sync_raw")

    # ---------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        self._require_dict(input_block, factor_name=self.name)

        # ===============================
        # 1️⃣ 读取 core_theme（ETF / Index）
        # ===============================
        LOG.info( "[ETFIndexSyncFactor] compute invoked | input_keys=%s",
                   list(input_block.keys()),)
        core_theme = self.pick(input_block, "core_theme_raw", {})
        assert core_theme, "core_theme_raw is missing"  
        # ① 数据完全缺失 → DATA_NOT_CONNECTED
        if not core_theme:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "core_theme data missing",
                },
            )

        if not isinstance(core_theme, dict) or not core_theme:
            return self.build_result(
                score=50.0,
                details={"data_status": "MISSING_CORE_THEME"},
            )

        index_blk = None
        etf_blk = None
        
        for blk in core_theme.values():
            if not isinstance(blk, dict):
                continue
            symbol = blk.get("symbol", "")
            if symbol.startswith("^") or symbol.endswith(".HK"):
                index_blk = blk
            else:
                etf_blk = blk
        
        if not index_blk or not etf_blk:
            return self.build_result(
                score=50.0,
                details={"data_status": "INVALID_CORE_THEME_STRUCTURE"},
            )
        
        
        if not isinstance(index_blk, dict) or not isinstance(etf_blk, dict):
            return self.build_result(
                score=50.0,
                details={"data_status": "INCOMPLETE_CORE_THEME"},
            )

        idx_pct = index_blk.get("pct")
        etf_pct = etf_blk.get("pct")

        if idx_pct is None or etf_pct is None:
            return self.build_result(
                score=50.0,
                details={"data_status": "PCT_MISSING"},
            )

        try:
            idx_pct = float(idx_pct)
            etf_pct = float(etf_pct)
        except Exception:
            return self.build_result(
                score=50.0,
                details={"data_status": "PCT_INVALID"},
            )

        # ===============================
        # 2️⃣ ETF × Index 同步性
        # ===============================
        same_direction = (idx_pct >= 0 and etf_pct >= 0) or (idx_pct <= 0 and etf_pct <= 0)
        divergence_idx = abs(etf_pct - idx_pct)

        # ===============================
        # 3️⃣ 读取 ETF × Spot 事实
        # ===============================
        spot_blk = self.pick(input_block, "etf_spot_sync_raw", {})
        assert spot_blk, "etf_spot_sync_raw is missing"  
        # ① 数据完全缺失 → DATA_NOT_CONNECTED
        if not spot_blk:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "etf_spot_sync_raw data missing",
                },
            )


        if not isinstance(spot_blk, dict) or not spot_blk:
            return self.build_result(
                score=50.0,
                details={"data_status": "MISSING_SPOT_SYNC"},
            )

        adv_ratio = spot_blk.get("adv_ratio")
        top20_ratio = spot_blk.get("top20_turnover_ratio")
        dispersion = spot_blk.get("dispersion")

        if adv_ratio is None or top20_ratio is None or dispersion is None:
            LOG.error(
                "[ETFIndexSyncFactor] SPOT_MISSING_FIELDS | adv_ratio=%r | top20_ratio=%r | dispersion=%r",
                adv_ratio, top20_ratio, dispersion,
            )
            return self.build_result(
                score=50.0,
                details={"data_status": "SPOT_MISSING_FIELDS"},
            )
        

        adv_ratio = spot_blk.get("adv_ratio")
        top20_ratio = spot_blk.get("top20_turnover_ratio")
        dispersion = spot_blk.get("dispersion")

        try:
            adv_ratio = float(adv_ratio)
            top20_ratio = float(top20_ratio)
            dispersion = float(dispersion)
        except Exception as e :
            LOG.exception(
                "[ETFIndexSyncFactor] SPOT_INVALID | spot_blk=%s",
                spot_blk,
            )
            return self.build_result(
                score=50.0,
                details={
                    "data_status": "SPOT_INVALID",
                    "error": repr(e),
                    "spot_keys": list(spot_blk.keys()) if isinstance(spot_blk, dict) else None,
                },
            )

        # ===============================
        # 4️⃣ 结构评分（保守、非对称）
        # ===============================
        score = 50.0

        # ---- ETF × Index ----
        if same_direction:
            score += 15.0
        else:
            score -= 25.0

        # ETF 明显强，但指数不确认 → 明确风险
        if etf_pct > 0 and idx_pct <= 0:
            score -= 25.0

        # 背离过大
        if divergence_idx >= 1.5:
            score -= 10.0

        # ---- ETF × Spot ----
        # 上涨承接不足
        if adv_ratio < 0.45:
            score -= 15.0

        # 成交额过度集中（资金没扩散）
        if top20_ratio >= 0.6:
            score -= 10.0

        # 分化过高（结构不稳）
        if dispersion >= 2.5:
            score -= 10.0

        score = self.clamp_score(score)

        # ===============================
        # 5️⃣ 细节输出（审计友好）
        # ===============================
        details = {
            "index_symbol": index_blk.get("symbol"),
            "etf_symbol": etf_blk.get("symbol"),
            "index_pct": idx_pct,
            "etf_pct": etf_pct,
            "same_direction": same_direction,
            "divergence_index": divergence_idx,
            "adv_ratio": adv_ratio,
            "top20_turnover_ratio": top20_ratio,
            "dispersion": dispersion,
            "data_status": "OK",
        }

        LOG.info(
            "[ETFIndexSyncFactor] result | score=%.2f | level=%s",
            score,
            self.level_from_score(score),
        )
        return self.build_result(score=score, details=details)
