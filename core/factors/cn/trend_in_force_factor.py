# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict

from core.factors.factor_base import FactorBase, RiskLevel
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.TrendInForce")


class TrendInForceFactor(FactorBase):
    """Trend-in-Force factor.

    Uses trend facts built by TrendFactsBlockBuilder and prefers
    price+amount joint judgment to avoid volume-only false negatives.
    """

    def __init__(self) -> None:
        super().__init__("trend_in_force")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        trend_facts = input_block.get("trend_in_force")

        if not isinstance(trend_facts, dict):
            LOG.warning("[TrendInForce] missing or invalid trend_in_force")
            return self._neutral_result(
                state="DATA_MISSING",
                reason="missing trend_in_force",
                raw_data=trend_facts,
            )

        amount = trend_facts.get("amount")
        price = trend_facts.get("price")
        if not isinstance(amount, dict):
            return self._neutral_result(
                state="DATA_MISSING",
                reason="missing amount trend facts",
                raw_data=trend_facts,
            )

        slope_10d = amount.get("slope_10d")
        slope_5d = amount.get("slope_5d")
        ratio_vs_10d = amount.get("ratio_vs_10d")

        p_slope_10d = price.get("slope_10d") if isinstance(price, dict) else None
        p_ratio_vs_10d = price.get("ratio_vs_10d") if isinstance(price, dict) else None

        level: RiskLevel
        score: int
        reason: str
        state: str

        try:
            if slope_10d is None or ratio_vs_10d is None:
                raise ValueError("incomplete amount trend facts")

            # Preferred branch: joint price + amount trend.
            if isinstance(p_slope_10d, (int, float)) and isinstance(p_ratio_vs_10d, (int, float)):
                if float(p_slope_10d) < 0 and float(slope_10d) < 0:
                    level = "LOW"
                    score = 35
                    state = "BROKEN"
                    reason = "价量同步走弱，趋势结构失效。"
                elif float(p_slope_10d) > 0 and float(p_ratio_vs_10d) >= 1.0 and float(ratio_vs_10d) >= 0.90:
                    level = "HIGH"
                    score = 65
                    state = "IN_FORCE"
                    reason = "价格中期趋势向上，量能未明显失速。"
                else:
                    level = "NEUTRAL"
                    score = 50
                    state = "WEAKENING"
                    reason = "价格与量能出现背离，趋势延续性待确认。"

            # Legacy fallback: amount only.
            elif float(slope_10d) < 0:
                level = "LOW"
                score = 35
                state = "BROKEN"
                reason = "中期量能斜率为负，趋势结构失效。"
            elif float(slope_10d) > 0 and float(ratio_vs_10d) >= 1.0:
                level = "HIGH"
                score = 65
                state = "IN_FORCE"
                reason = "中期量能斜率为正，参与度维持在中期水平之上。"
            else:
                level = "NEUTRAL"
                score = 50
                state = "WEAKENING"
                reason = "趋势方向未反转，但参与度或短期斜率转弱。"

        except Exception as e:
            LOG.warning("[TrendInForce] evaluation error: %s", e)
            return self._neutral_result(
                state="DATA_MISSING",
                reason="trend facts evaluation error",
                raw_data=trend_facts,
            )

        details = {
            "state": state,
            "reason": reason,
            "_raw_data": trend_facts,
            "amount": {
                "slope_5d": slope_5d,
                "slope_10d": slope_10d,
                "ratio_vs_10d": ratio_vs_10d,
            },
            "price": {
                "slope_10d": p_slope_10d,
                "ratio_vs_10d": p_ratio_vs_10d,
            },
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details=details,
        )

    def _neutral_result(self, *, state: str, reason: str, raw_data: Any) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=50,
            level="NEUTRAL",
            details={
                "state": state,
                "reason": reason,
                "_raw_data": raw_data,
            },
        )
