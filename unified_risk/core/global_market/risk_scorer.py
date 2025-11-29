from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

from ...common.scoring import aggregate_factors, classify_level, build_description
from ...common.logger import get_logger

LOG = get_logger("UnifiedRisk.GlobalScore")


@dataclass
class GlobalRiskResult:
    date: str
    total_score: float
    level: str
    description: str
    factor_scores: Dict[str, float] = field(default_factory=dict)


class GlobalRiskScorer:
    def __init__(self, factor_weights: Optional[Dict[str, float]] = None) -> None:
        self.factor_weights = factor_weights or {
            "us_equity_risk": 1.0,
            "volatility_risk": 1.0,
            "cny_fx_risk": 0.8,
            "jpy_risk": 0.8,
            "gold_risk": 0.8,
            "cibr_risk": 0.8,
        }

    def score(
        self,
        date_str: str,
        factors,
        yesterday_score: Optional[float] = None,
    ) -> GlobalRiskResult:
        vals = factors.values
        total = aggregate_factors(vals, self.factor_weights)
        level = classify_level(total)
        desc = build_description(total, yesterday_score)

        LOG.info(f"Global 风险总分={total:.2f}, 等级={level}")
        return GlobalRiskResult(
            date=date_str,
            total_score=total,
            level=level,
            description=desc,
            factor_scores=vals,
        )
