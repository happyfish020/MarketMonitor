from __future__ import annotations

from typing import Dict, Any

from unified_risk.common.logger import get_logger
from unified_risk.common.config_manager import CONFIG

LOG = get_logger("UnifiedRisk.Scorer.AShare")


class AShareRiskScorer:
    """A股日级别风险打分：聚合成交、两融、北向等因子。"""

    def __init__(self) -> None:
        # 统一使用 config/weights 中的权重配置：
        # weights:
        #   turnover: 1.0
        #   margin: 1.0
        #   northbound: 1.0
        #   global: 1.0
        self.weights = CONFIG.get("weights", default={}) or {}

    def score_daily(self, snapshot: Dict[str, Any], nb_snap: Any) -> Dict[str, float]:
        scores: Dict[str, float] = {}

        turnover_score = self._score_turnover(snapshot)
        margin_score = self._score_margin(snapshot)

        # 北向相关打分
        nb_score = float(getattr(nb_snap, "northbound_score", 0.0))
        nb_strength = float(getattr(nb_snap, "nb_nps_score", 0.0))

        scores["turnover_score"] = turnover_score
        scores["margin_score"] = margin_score
        scores["northbound_score"] = nb_score
        scores["nb_nps_score"] = nb_strength

        # 预留 global 因子，在后续版本接入
        global_score = 0.0
        scores["global_score"] = global_score

        def _w(name: str, default: float = 1.0) -> float:
            try:
                return float(self.weights.get(name, default))
            except Exception:
                return default

        total = (
            turnover_score * _w("turnover", 1.0)
            + margin_score * _w("margin", 1.0)
            + nb_score * _w("northbound", 1.0)
            + global_score * _w("global", 1.0)
        )
        scores["total_risk_score"] = float(total)

        LOG.info(
            "[Scorer] total=%.3f turnover=%.1f margin=%.1f nb=%.1f global=%.1f",
            total,
            turnover_score,
            margin_score,
            nb_score,
            global_score,
        )
        return scores

    def _score_turnover(self, snapshot: Dict[str, Any]) -> float:
        """基于沪深两市总成交额的简化估算。"""
        try:
            sh = snapshot["turnover"]["sh"].get("turnover", 0.0)
            sz = snapshot["turnover"]["sz"].get("turnover", 0.0)
            total = sh + sz
            if total <= 0:
                return 0.0

            # 粗略分档：>8e10 偏强，3e10 以下偏弱
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
        """基于两融余额的风险评估。"""
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
