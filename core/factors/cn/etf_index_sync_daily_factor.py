# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
ETF × Index Synchronization Factor (Daily / T-1 Confirmed)

职责：
- 读取 etf_spot_sync_daily（T-1 确认态）
- 判断 ETF 与市场结构是否同步
- 仅输出“结构质量”，不预测方向
- 可安全进入 GateDecision
"""

from typing import Dict, Any

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class ETFIndexSyncDailyFactor(FactorBase):
    """
    ETF × 市场结构同步性（Daily / Confirmed）

    Level 语义：
    - LOW     : 广泛参与，同步健康
    - NEUTRAL  : 轻度集中 / 分化
    - HIGH    : 明显集中 / 分化（结构不同步）
    """

    def __init__(self):
        super().__init__(name="etf_index_sync_daily")

    # ---------------------------------------------------------
    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        """
        snapshot: Report snapshot (slots)
        """
        data = snapshot.get("etf_spot_sync_daily")
        if not isinstance(data, dict):
            return self._neutral("missing etf_spot_sync_daily")

        # 明确只接受确认态
        if data.get("snapshot_type") != "EOD":
            return self._neutral("non-eod snapshot")

        adv_ratio = data.get("adv_ratio")
        dispersion = data.get("dispersion")
        top20_amount_ratio = data.get("top20_amount_ratio")

        # 基础字段校验
        if not self._is_valid_number(adv_ratio, dispersion, top20_amount_ratio):
            return self._neutral("invalid numeric fields")

        # -----------------------------------------------------
        # 结构判定逻辑（与你现有 ETFIndexSyncFactor 保持一致）
        # -----------------------------------------------------
        score = 50.0
        reasons = []

        # 广度不足
        if adv_ratio < 0.45:
            score -= 20
            reasons.append(f"low_adv_ratio={adv_ratio:.2f}")

        # 分化加剧
        if dispersion > 2.5:
            score -= 15
            reasons.append(f"high_dispersion={dispersion:.2f}")

        # 成交额集中
        if top20_amount_ratio > 0.60:
            score -= 15
            reasons.append(f"high_top20_amount={top20_amount_ratio:.2f}")

        # -----------------------------------------------------
        # Level 映射（冻结）
        # -----------------------------------------------------
        if score >= 60:
            level = "LOW"
        elif score >= 45:
            level = "NEUTRAL"
        else:
            level = "HIGH"

        return FactorResult(
            name=self.name,
            score=round(score, 2),
            level=level,
            details={
                "adv_ratio": round(adv_ratio, 4),
                "dispersion": round(dispersion, 4),
                "top20_amount_ratio": round(top20_amount_ratio, 4),
                "reasons": reasons,
                "snapshot_type": "EOD",
                "trade_date": data.get("trade_date"),
                "as_of_date": data.get("as_of_date"),
                "_raw_data": data
            },
        )

    # ---------------------------------------------------------
    def _neutral(self, reason: str) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=50.0,
            level="NEUTRAL",
            details={"note": reason},
        )

    @staticmethod
    def _is_valid_number(*vals) -> bool:
        for v in vals:
            if not isinstance(v, (int, float)):
                return False
        return True
