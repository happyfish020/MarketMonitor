# core/factors/base_factor.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Literal


RiskLevel = Literal["LOW", "NEUTRAL", "HIGH"]


@dataclass(frozen=True, slots=True)
class FactorResult:
    """
    V12 Factor 固定输出结构（铁律 C）：

    - name: 因子唯一名（用于 report / weights / predictor）
    - score: 0~100
    - level: LOW / NEUTRAL / HIGH
    - details: 解释用结构化信息（给 Reporter 使用，Factor 内不得拼报告文本）
    """
    name: str
    score: float
    level: RiskLevel
    details: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # dataclass(frozen=True) 下需要 object.__setattr__ 做纠正或报错
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("FactorResult.name must be a non-empty str")

        if not isinstance(self.score, (int, float)):
            raise TypeError("FactorResult.score must be a number")
        if self.score < 0 or self.score > 100:
            raise ValueError("FactorResult.score must be in [0, 100]")

        if self.level not in ("LOW", "NEUTRAL", "HIGH"):
            raise ValueError("FactorResult.level must be one of: LOW / NEUTRAL / HIGH")

        if not isinstance(self.details, dict):
            raise TypeError("FactorResult.details must be a dict")

 