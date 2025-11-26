from typing import Dict, Any
from unifiedrisk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Shortterm")


class ShorttermEngine:
    """1-2 week / intraday-focused short-term engine (MorningView will live here)."""

    def run(self) -> Dict[str, Any]:
        LOG.info("Running ShorttermEngine (stub).")
        return {
            "horizon": "shortterm",
            "total_score": 0.0,
            "risk_level": "neutral",
            "factors": {},
            "comments": {},
        }
