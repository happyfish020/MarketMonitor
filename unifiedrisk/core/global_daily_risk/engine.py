from typing import Dict, Any
from pathlib import Path
import yaml

from unifiedrisk.common.logger import get_logger
from unifiedrisk.common.scoring import classify_level
from unifiedrisk.common.utils import project_root
from .factors.aggregator import aggregate_global_daily_factors

LOG = get_logger("UnifiedRisk.GlobalDaily")


class GlobalDailyRiskEngine:
    """Global daily risk engine (v1.2 with live factor scoring)."""

    def __init__(self) -> None:
        root = project_root()
        thresholds_path = root / "config" / "thresholds.yaml"
        self.thresholds = yaml.safe_load(thresholds_path.read_text(encoding="utf-8"))

    def run(self) -> Dict[str, Any]:
        LOG.info("Running GlobalDailyRiskEngine (v1.2 live).")

        raw_data: Dict[str, Any] = {}
        total_score, factors, comments = aggregate_global_daily_factors(raw_data)

        level_key, level_label = classify_level(total_score, self.thresholds)

        LOG.info("GlobalDaily total_score=%.2f → level=%s", total_score, level_key)

        return {
            "horizon": "global_daily",
            "total_score": total_score,
            "risk_level": level_key,
            "risk_label": level_label,
            "factors": factors,
            "comments": comments,
        }
