from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

from .factor_loader import AShareFactors
from ...common.scoring import aggregate_factors, classify_level, build_description
from ...common.logger import get_logger

LOG = get_logger("UnifiedRisk.AShareScore")


@dataclass
class AShareRiskResult:
    date: str
    total_score: float
    level: str
    description: str
    factor_scores: Dict[str, float] = field(default_factory=dict)


class AShareRiskScorer:
    def __init__(self, factor_weights: Optional[Dict[str, float]] = None) -> None:
        self.factor_weights = factor_weights or {
            "turnover_risk": 1.0,
            "northbound_risk": 1.2,
            "margin_risk": 0.8,
            "main_fund_risk": 1.0,
            "etf_flow_risk": 0.8,
        }

    def score(
        self,
        date_str: str,
        factors: AShareFactors,
        yesterday_score: Optional[float] = None,
    ) -> AShareRiskResult:
        vals = factors.values
        total = aggregate_factors(vals, self.factor_weights)
        level = classify_level(total)
        desc = build_description(total, yesterday_score)

        LOG.info(f"A 股总分={total:.2f}, 等级={level}")
        return AShareRiskResult(
            date=date_str,
            total_score=total,
            level=level,
            description=desc,
            factor_scores=vals,
        )
