from datetime import date
from typing import Dict, Any

from unified_risk.scorer.scoring_rules import classify_risk_level, score_to_advise
from unified_risk.common.config_manager import CONFIG


class DailyRiskScorer:
    """日级风险打分器：只对 *_score 因子求和，其余视为原始指标。"""

    def __init__(self) -> None:
        self.weights = CONFIG.get("weights", default={})

    def score(self, d: date, factors: Dict[str, Any]) -> Dict[str, Any]:
        total = 0.0
        factor_scores = {}

        for key, value in factors.items():
            if not key.endswith("_score"):
                # 非打分字段，例如 turnover_ratio 等，直接跳过
                continue
            base = key[:-6]  # 去掉 "_score"
            w = float(self.weights.get(base, 1.0))
            v = float(value)
            factor_scores[key] = v
            total += v * w

        level = classify_risk_level(total)
        advise = score_to_advise(total)

        return {
            "date": d.strftime("%Y-%m-%d"),
            "total_risk_score": total,
            "risk_level": level,
            "advise": advise,
            "factor_scores": factor_scores,
            "raw_factors": factors,
        }
