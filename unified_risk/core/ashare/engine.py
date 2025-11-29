from __future__ import annotations

from dataclasses import asdict
from typing import Dict, Any, Optional

from .data_fetcher import AShareDataFetcher
from .factor_loader import AShareFactorLoader
from .risk_scorer import AShareRiskScorer
from ...common.cache_manager import CacheManager
from ...common.logger import get_logger
from ...common.time_utils import now_bj, fmt_date, fmt_date_compact

LOG = get_logger("UnifiedRisk.AShareEngine")


class AShareDailyEngine:
    """A 股日级风险引擎（v6.2d）。""" 

    def __init__(self, cache: Optional[CacheManager] = None) -> None:
        self.cache = cache or CacheManager()
        self.fetcher = AShareDataFetcher(self.cache)
        self.factor_loader = AShareFactorLoader()
        self.scorer = AShareRiskScorer()

    def run(
        self,
        date_str: Optional[str] = None,
        yesterday_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        if date_str is None:
            date_str = fmt_date_compact(now_bj())

        LOG.info(f"Running AShareDailyEngine for {date_str}")

        raw = self.fetcher.get_raw_data(date_str)
        factors = self.factor_loader.build_factors(raw)
        result = self.scorer.score(date_str, factors, yesterday_score=yesterday_score)

        payload = {
            "meta": {
                "bj_time": fmt_date(now_bj()),
                "version": "UnifiedRisk_v6.2d",
                "market": "ASHARE",
            },
            "result": asdict(result),
        }

        self.cache.write_key(date_str, "ashare", "risk_result", payload)
        return payload
