# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict

from core.factors.factor_base import FactorBase, RiskLevel
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.TrendInForce")


class TrendInForceFactor(FactorBase):
    """
    Trend-in-Force 因子（冻结）

    职责：
    - 消费 snapshot['trend_in_force']
    - 判定趋势结构是否仍然成立
    - 输出结构有效性等级（HIGH / NEUTRAL / LOW）
    - 不读取 history、不影响 Gate、不做预测
     state - IN_FORCE     # 趋势结构成立
WEAKENING    # 趋势仍在，但结构走弱
BROKEN       # 趋势结构失效
DATA_MISSING # 数据不足，无法判断
        """

    def __init__(self) -> None:
        super().__init__("trend_in_force")

    # ------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        trend_facts = input_block.get("trend_in_force")

        # ==========================================================
        # 数据缺失 / 异常回退
        # ==========================================================
        if not isinstance(trend_facts, dict):
            LOG.warning("[TrendInForce] missing or invalid trend_in_force")
            return self._neutral_result(
                state="DATA_MISSING",
                reason="missing trend_in_force",
                raw_data=trend_facts,
            )

        amount = trend_facts.get("amount")
        if not isinstance(amount, dict):
            return self._neutral_result(
                state="DATA_MISSING",
                reason="missing amount trend facts",
                raw_data=trend_facts,
            )

        slope_10d = amount.get("slope_10d")
        slope_5d = amount.get("slope_5d")
        ratio_vs_10d = amount.get("ratio_vs_10d")

        # -------------------------------
        # 判定逻辑（冻结最小集）
        # -------------------------------
        level: RiskLevel
        score: int
        reason: str
        state: str

        try:
            if slope_10d is None or ratio_vs_10d is None:
                raise ValueError("incomplete amount trend facts")

            # 趋势失效
            if slope_10d < 0:
                level = "LOW"
                score = 35
                state = "BROKEN"
                reason = "中期趋势斜率为负，趋势结构已失效。"

            # 趋势成立
            elif slope_10d > 0 and ratio_vs_10d >= 1.0:
                level = "HIGH"
                score = 65
                state = "IN_FORCE"
                reason = "中期趋势斜率为正，参与度维持在中期水平之上。"

            # 趋势减弱
            else:
                level = "NEUTRAL"
                score = 50
                state = "WEAKENING"
                reason = "趋势方向尚未反转，但参与度或短期斜率已走弱。"

        except Exception as e:
            LOG.warning("[TrendInForce] evaluation error: %s", e)
            return self._neutral_result(
                state="DATA_MISSING",
                reason="trend facts evaluation error",
                raw_data=trend_facts,
            )

        details = {
            "state": state,              # ✅ 新增：制度语义
            "reason": reason,            # 保留：原有说明
            "_raw_data": trend_facts,    # 保留：原始事实
            "amount": {
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
    # Helpers（冻结）
    # ------------------------------------------------------------
    def _neutral_result(self, *, state: str, reason: str, raw_data: Any) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=50,
            level="NEUTRAL",
            details={
                "state": state,       # ✅ 新增
                "reason": reason,
                "_raw_data": raw_data,
            },
        )
