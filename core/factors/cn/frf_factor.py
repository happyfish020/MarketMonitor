# -*- coding: utf-8 -*-
"""
Failure-Rate Factor (FRF) - V12 冻结版

职责（P0）：
- 消费 snapshot['trend_in_force_raw']
- 在不引入新 DS / 不读 history 的前提下，
  对“趋势结构失效迹象”做窗口化失败率评估
- 输出 FactorResult(name="failure_rate", score/level/details)
- 不影响 Gate、不做预测、不产生交易含义
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.factors.factor_base import FactorBase, RiskLevel
from core.factors.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.FRF")


class FRFFactor(FactorBase):
    """
    FRF（Failure-Rate Factor）

    设计冻结要点：
    - P0 仅基于 trend_in_force_raw 做等价映射
    - 不引入新的“失败定义”，只做结构退化统计
    - 失败率用于风险环境解释，而非 Gate 判决
    """

    def __init__(self) -> None:
        # 因子名固定为 "failure_rate"
        super().__init__("failure_rate")

    # ------------------------------------------------------------
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        trend_facts = input_block.get("trend_in_force_raw")

        if not isinstance(trend_facts, dict):
            LOG.warning("[FRF] missing or invalid trend_in_force_raw")
            return self._neutral_result(
                reason="missing trend_in_force_raw",
                raw_data=trend_facts,
            )

        # P0：与 TrendInForceFactor 对齐，仅使用 turnover 子结构
        turnover = trend_facts.get("turnover")
        if not isinstance(turnover, dict):
            return self._neutral_result(
                reason="missing turnover trend facts",
                raw_data=trend_facts,
            )

        slope_10d = turnover.get("slope_10d")
        ratio_vs_10d = turnover.get("ratio_vs_10d")

        # --------------------------------------------------------
        # P0 失败定义（等价映射，不引入新制度）
        #
        # 失败事件 = 中期趋势斜率为负
        # 该条件与 TrendInForceFactor 中
        #   slope_10d < 0 → level=LOW
        # 保持语义一致
        # --------------------------------------------------------
        fail_flags: List[bool] = []

        try:
            if slope_10d is None:
                raise ValueError("incomplete turnover trend facts")

            fail_flags.append(bool(slope_10d < 0))

        except Exception as e:
            LOG.warning("[FRF] evaluation error: %s", e)
            return self._neutral_result(
                reason="trend facts evaluation error",
                raw_data=trend_facts,
            )

        # P0 窗口 = 当前快照（1 个观测）
        total_count = len(fail_flags)
        fail_count = sum(1 for x in fail_flags if x)
        fail_rate = fail_count / total_count if total_count > 0 else 0.0

        # --------------------------------------------------------
        # 分数 / 等级映射（冻结最小集）
        #
        # - fail_rate = 0   → LOW 风险压力
        # - fail_rate = 1   → HIGH 风险压力
        #
        # 注意：这里的 HIGH/LOW 是“失败率风险”
        #       与 TrendInForce 的 HIGH/LOW 语义不同，
        #       但仅用于解释层（DRS / 报告）
        # --------------------------------------------------------
        score = 100.0 * fail_rate

        if fail_rate >= 1.0:
            level: RiskLevel = "HIGH"
            meaning = "趋势结构出现明确失效信号。"
        elif fail_rate > 0.0:
            level = "NEUTRAL"
            meaning = "趋势结构存在失效迹象，但尚不连续。"
        else:
            level = "LOW"
            meaning = "趋势结构未出现失效迹象。"

        details = {
            "meaning": meaning,
            "window": {
                "type": "snapshot_only",
                "count": total_count,
            },
            "fail_count": fail_count,
            "total_count": total_count,
            "fail_rate": fail_rate,
            "definition": "fail_if_slope_10d_lt_0 (aligned_with_trend_in_force)",
            "_raw_data": {
                "turnover": {
                    "slope_10d": slope_10d,
                    "ratio_vs_10d": ratio_vs_10d,
                }
            },
        }

        return FactorResult(
            name=self.name,
            score=score,
            level=level,
            details=details,
        )

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
