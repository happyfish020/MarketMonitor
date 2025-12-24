# core/factors/cn/trend_in_force_factor.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict

from core.factors.factor_base import FactorBase, RiskLevel
from core.factors.factor_result import FactorResult

#import FactorBase, FactorResult, RiskLevel
from core.utils.logger import get_logger

LOG = get_logger("Factor.TrendInForce")


class TrendInForceFactor(FactorBase):
    """
    Trend-in-Force 因子（冻结）

    职责：
    - 消费 snapshot['trend_in_force_raw']
    - 判定趋势结构是否仍然成立
    - 输出结构有效性等级（HIGH / NEUTRAL / LOW）
    - 不读取 history、不影响 Gate、不做预测
    """

    def __init__(self) -> None:
        #super().__init__("trend_in_force_raw")
        super().__init__("trend_in_force_raw")

    # ------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        trend_facts = input_block.get("trend_in_force_raw")

        if not isinstance(trend_facts, dict):
            LOG.warning("[TrendInForce] missing or invalid trend_in_force_raw")
            return self._neutral_result(
                reason="missing trend_in_force_raw",
                raw_data=trend_facts,
            )

        # P0：仅使用 turnover 趋势事实
        turnover = trend_facts.get("turnover")
        if not isinstance(turnover, dict):
            return self._neutral_result(
                reason="missing turnover trend facts",
                raw_data=trend_facts,
            )

        slope_10d = turnover.get("slope_10d")
        slope_5d = turnover.get("slope_5d")
        ratio_vs_10d = turnover.get("ratio_vs_10d")

        # -------------------------------
        # 判定逻辑（冻结最小集）
        # -------------------------------
        level: RiskLevel
        score: int
        reason: str

        try:
            if slope_10d is None or ratio_vs_10d is None:
                raise ValueError("incomplete turnover trend facts")

            # 趋势失效
            if slope_10d < 0:
                level = "LOW"
                score = 35
                reason = "中期趋势斜率为负，趋势结构已失效。"

            # 趋势成立
            elif slope_10d > 0 and ratio_vs_10d >= 1.0:
                level = "HIGH"
                score = 65
                reason = "中期趋势斜率为正，参与度维持在中期水平之上。"

            # 趋势减弱
            else:
                level = "NEUTRAL"
                score = 50
                reason = "趋势方向尚未反转，但参与度或短期斜率已走弱。"

        except Exception as e:
            LOG.warning("[TrendInForce] evaluation error: %s", e)
            return self._neutral_result(
                reason="trend facts evaluation error",
                raw_data=trend_facts,
            )

        details = {
            "reason": reason,
            "_raw_data": trend_facts,
            "turnover": {
                "slope_5d": slope_5d,
                "slope_10d": slope_10d,
                "ratio_vs_10d": ratio_vs_10d,
            },
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details=details,
        )

    # ------------------------------------------------------------
    # Helpers（与 UnifiedEmotionFactor 风格一致）
    # ------------------------------------------------------------
    def _neutral_result(self, *, reason: str, raw_data: Any) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=50,
            level="NEUTRAL",
            details={
                "reason": reason,
                "_raw_data": raw_data,
            },
        )
