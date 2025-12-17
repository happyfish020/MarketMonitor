# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - BreadthFactor (Phase-2 Veto)

职责：
- 基于 new_low_ratio + persistence（连续性）映射结构损伤状态
- 仅输出状态，不做交易建议
"""

from __future__ import annotations
from typing import Dict, Any, List

from core.utils.logger import get_logger
from core.factors.factor_base import FactorBase, FactorResult

LOG = get_logger("Factor.Breadth")


class BreadthFactor(FactorBase):
    def __init__(self):
        super().__init__(name="breadth_raw")

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        block = snapshot.get("breadth_raw") or {}
        ratio = float(block.get("new_low_ratio", 0.0))

        state, score, level, reason = self._map_state(ratio)

        LOG.info(
            "[BreadthFactor] state=%s ratio=%.4f",
            state, ratio
        )

        return self.build_result(
            score=score,
            level=level,
            
            details={
                "state": state,
                "new_low_ratio": ratio,
                "reason": reason,
                "data_status": "OK",
            },
        )

    @staticmethod
    def _map_state(r: float):
        """
        冻结阈值（保守、稳定）：
        - Healthy:   r < 0.05
        - Early:     0.05 <= r < 0.10
        - Confirmed: 0.10 <= r < 0.20
        - Breakdown: r >= 0.20
        """
        if r >= 0.20:
            return "Breakdown", 10.0, "HIGH", "大量个股创 50 日新低，结构性破坏"
        if r >= 0.10:
            return "Confirmed", 25.0, "HIGH", "新低比例持续偏高，结构损伤确认"
        if r >= 0.05:
            return "Early", 40.0, "NEUTRAL", "新低比例上升，结构开始磨损"
        return "Healthy", 60.0, "LOW", "新低比例低，结构健康"
