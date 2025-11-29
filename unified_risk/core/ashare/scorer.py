from __future__ import annotations

from typing import Dict, Any

from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Scorer.AShare")


class AShareRiskScorer:
    def score_daily(self, snapshot: Dict[str, Any], nb_snap: Any) -> Dict[str, float]:
        scores: Dict[str, float] = {}

        turnover_score = self._score_turnover(snapshot)
        margin_score = self._score_margin(snapshot)

        scores["turnover_score"] = turnover_score
        scores["margin_score"] = margin_score
        scores["northbound_score"] = float(nb_snap.northbound_score)
        scores["nb_nps_score"] = float(nb_snap.nb_nps_score)
        scores["global_score"] = 0.0

        total = (
            turnover_score * 0.4
            + margin_score * 0.3
            + nb_snap.northbound_score * 0.3
        )
        scores["total_risk_score"] = float(total)

        LOG.info(
            "[Scorer] total=%.3f turnover=%.1f margin=%.1f nb=%.1f global=%.1f",
            total,
            turnover_score,
            margin_score,
            nb_snap.northbound_score,
            scores["global_score"],
        )
        return scores

    def _score_turnover(self, snapshot: Dict[str, Any]) -> float:
        try:
            sh = snapshot["turnover"]["sh"].get("turnover", 0.0)
            sz = snapshot["turnover"]["sz"].get("turnover", 0.0)
            total = sh + sz
            if total <= 0:
                return 0.0

            if total >= 8e10:
                return 2.0
            if total >= 6e10:
                return 1.0
            if total <= 3e10:
                return -2.0
            if total <= 4.5e10:
                return -1.0
            return 0.0
        except Exception:
            return 0.0

    def _score_margin(self, snapshot: Dict[str, Any]) -> float:
        try:
            rzrq = snapshot["margin"].get("rzrqye", 0.0)
            if rzrq <= 0:
                return 0.0

            if rzrq >= 1.9e12:
                return -2.0
            if rzrq >= 1.7e12:
                return -1.0
            if rzrq <= 1.1e12:
                return 1.0
            return 0.0
        except Exception:
            return 0.0
