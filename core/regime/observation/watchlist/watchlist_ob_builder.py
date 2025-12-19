from __future__ import annotations

import logging
from typing import Any, Dict

from core.regime.observation.watchlist.sector_observation import WatchlistSectorsObservation

LOG = logging.getLogger(__name__)


class WatchlistObservationBuilder:
    """
    Facade：生成 factors_bound["watchlist"]
    """
    def __init__(self):
        self._obs = WatchlistSectorsObservation()

    def build(self, *, slots: Dict[str, Any], asof: str) -> Dict[str, Any]:
        out = self._obs.build(inputs=slots, asof=asof)
        LOG.info("[WatchlistObservationBuilder] built watchlist observation asof=%s", asof)
        return out
