from typing import Dict, Any

from core.common.logger import get_logger
from core.common.scoring import map_score_to_level
from core.factors.base import FactorResult

LOG = get_logger("CN.Score.ShortTerm")

# 先用现有 4 因子占位
T1_WEIGHTS: Dict[str, float] = {
    "north_nps": 0.35,
    "turnover": 0.20,
    "market_sentiment": 0.25,
    "margin": 0.20,
}

T5_WEIGHTS: Dict[str, float] = {
    "north_nps": 0.30,
    "turnover": 0.20,
    "market_sentiment": 0.20,
    "margin": 0.30,
}


def _compute_weighted_score(
    factor_results: Dict[str, FactorResult],
    weights: Dict[str, float],
) -> float:
    score = 0.0
    for key, w in weights.items():
        fr = factor_results.get(key)
        if fr is None:
            LOG.warning(f"[ShortTerm] missing factor result: {key}")
            continue
        score += fr.score * w
    return score


def compute_short_term_scores(
    factor_results: Dict[str, FactorResult],
) -> Dict[str, Any]:
    """基于因子结果计算 T+1 / T+3~5 统一评分"""
    score_t1 = _compute_weighted_score(factor_results, T1_WEIGHTS)
    score_t5 = _compute_weighted_score(factor_results, T5_WEIGHTS)

    result: Dict[str, Any] = {
        "T1": {
            "score": score_t1,
            "level": map_score_to_level(score_t1),
            "weights": T1_WEIGHTS,
        },
        "T5": {
            "score": score_t5,
            "level": map_score_to_level(score_t5),
            "weights": T5_WEIGHTS,
        },
    }

    LOG.info(
        "[ShortTerm] T1=%.2f(%s), T5=%.2f(%s)",
        score_t1,
        result["T1"]["level"],
        score_t5,
        result["T5"]["level"],
    )
    return result
