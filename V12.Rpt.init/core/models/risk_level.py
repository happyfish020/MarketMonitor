# core/models/risk_level.py
from enum import Enum


class RiskLevel(str, Enum):
    LOW = "LOW"
    NEUTRAL = "NEUTRAL"
    HIGH = "HIGH"

    @classmethod
    def from_score(
        cls,
        score: float,
        *,
        low: float = 30.0,
        high: float = 70.0,
    ) -> "RiskLevel":
        if score <= low:
            return cls.LOW
        if score >= high:
            return cls.HIGH
        return cls.NEUTRAL
