"""Aggregate multi-horizon risk into a unified view."""
from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import yaml

from unifiedrisk.common.scoring import classify_level
from unifiedrisk.common.utils import project_root
from unifiedrisk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Aggregation")


class UnifiedAggregator:
    def __init__(self) -> None:
        root = project_root()
        weights_path = root / "config" / "weights.yaml"
        thresholds_path = root / "config" / "thresholds.yaml"
        self.weights = yaml.safe_load(weights_path.read_text(encoding="utf-8"))
        self.thresholds = yaml.safe_load(thresholds_path.read_text(encoding="utf-8"))

    def aggregate(self, horizon_results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate per-horizon results using weights.yaml."""
        if not horizon_results:
            return {
                "total_score": 0.0,
                "risk_level": "unknown",
                "risk_label": "未知 / Unknown",
                "details": {},
            }

        horizon_weights = self.weights.get("horizons", {})
        total_score = 0.0
        weight_sum = 0.0
        details = {}

        for horizon, res in horizon_results.items():
            score = float(res.get("total_score", 0.0))
            w = float(horizon_weights.get(horizon, 0.0))
            LOG.info("Horizon %-12s score=%6.2f weight=%.2f", horizon, score, w)
            total_score += score * w
            weight_sum += w
            details[horizon] = res

        if weight_sum > 0:
            total_score /= weight_sum

        level_key, level_label = classify_level(total_score, self.thresholds)

        return {
            "total_score": total_score,
            "risk_level": level_key,
            "risk_label": level_label,
            "details": details,
        }
