# core/actions/summary_mapper.py
from __future__ import annotations

from typing import Any, Literal

Summary = Literal["A", "N", "D"]

class SummaryMapper:
    """
    冻结：Summary = Gate 语义映射
    NORMAL -> A
    CAUTION -> N
    FREEZE -> D
    """

    def map_gate_to_summary(self, gate: Any) -> Summary:
        g = str(gate).upper()
        if g == "NORMAL":
            return "A"
        if g == "CAUTION":
            return "N"
        if g == "FREEZE":
            return "D"
        # 冻结规则：不允许新增 UNKNOWN 枚举
        raise ValueError(f"Invalid gate for summary mapping: {gate}")
