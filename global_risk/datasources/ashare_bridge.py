
from __future__ import annotations

from typing import Dict, Any
from datetime import datetime

from ..utils.logging_utils import setup_logger

logger = setup_logger(__name__)


def get_daily_snapshot(bj_time: datetime) -> Dict[str, Any]:
    try:
        from core.fetchers.ashare_fetcher import AshareDataFetcher  # type: ignore
    except Exception as e:
        logger.warning("AshareDataFetcher import failed: %s", e)
        return {}

    try:
        fetcher = AshareDataFetcher()
        snap = fetcher.prepare_daily_market_snapshot(bj_time)
        if not isinstance(snap, dict):
            logger.warning("AshareDataFetcher.prepare_daily_market_snapshot returned non-dict")
            return {}
        return snap
    except Exception as e:
        logger.warning("AshareDataFetcher.prepare_daily_market_snapshot failed: %s", e)
        return {}
