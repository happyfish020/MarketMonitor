# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - BreadthFactor (Phase-2 Veto)

职责：
- 基于 new_low_ratio + persistence（连续性）映射结构损伤状态
- 仅输出状态，不做交易建议
- "data_status" -  "OK | DATA_NOT_CONNECTED | PARTIAL | STALE",
- state  - HEALTHY
WEAK
BREAKDOWN
DATA_MISSING

"""

from __future__ import annotations
from typing import Dict, Any, List

from core.utils.logger import get_logger
from core.factors.factor_base import FactorBase, FactorResult

LOG = get_logger("Factor.Breadth")


class BreadthFactor(FactorBase):
    def __init__(self):
        super().__init__(name="breadth")

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        data = snapshot.get("breadth_raw") or {}
        assert data, "breadth_raw is missing"

        if not data:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "state": "DATA_MISSING",
                    "reason": "breadth_raw data missing",
                },
            )

        ratio = self._normalize_new_low_ratio(data)

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
                "_raw_data": data
            },
        )

    @staticmethod
    def _normalize_new_low_ratio(data: Dict[str, Any]) -> float:
        """
        Normalize new_low_ratio to [0,1].
        Upstream may provide:
        - ratio in fraction form: 0.0077
        - ratio in percent points: 0.77 (means 0.77%)
        - ratio in percent number: 77 (means 77%)
        Prefer count-based recompute when available.
        """
        cnt = data.get("count_new_low")
        total = data.get("count_total")
        if isinstance(cnt, (int, float)) and isinstance(total, (int, float)) and float(total) > 0:
            return max(0.0, min(1.0, float(cnt) / float(total)))

        raw = float(data.get("new_low_ratio", 0.0))
        if raw > 1.0:
            return max(0.0, min(1.0, raw / 100.0))
        # 0<x<1 could be either fraction(0.0077) or percent-point(0.77).
        # Heuristic: values above 0.30 are almost surely percent-point for breadth.
        if raw > 0.30:
            return max(0.0, min(1.0, raw / 100.0))
        return max(0.0, min(1.0, raw))

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

