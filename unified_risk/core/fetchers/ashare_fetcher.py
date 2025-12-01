# unified_risk/core/fetchers/ashare_fetcher.py
# Version: v10.0-alpha compatible

from __future__ import annotations

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from unified_risk.common.logging_utils import log_info, log_warning
from unified_risk.common.cache_manager import DayCacheManager, AshareDailyDB

BJ_TZ = timezone(timedelta(hours=8))


class AshareDataFetcher:
    """
    A-share daily data fetcher (v10-alpha compatible).

    Responsibilities:
    - Provide daily market snapshot (index, adv/dec, turnover, A50, northbound proxy)
    - Provide full-market dataframe via ak.stock_zh_a_spot()
    - Save full dataframe into parquet via AshareDailyDB
    - Cooperate with v10 engine for market breadth and sector rotation
    """

    # ----------------------------- #
    # Static, simple placeholder APIs
    # ----------------------------- #
    def get_china_index_snapshot(self, bj_time):
        # Placeholder. Replace with real China index API later.
        return {
            "sh": {"close": 3000, "pct": 0.5},
            "sz": {"close": 9500, "pct": 0.8},
        }

    def get_advdec(self):
        return {"adv": 1200, "dec": 800}

    def get_turnover(self):
        return {"turnover_yi": 9500.0}

    def get_a50(self):
        return {"a50_change": 0.3}

    def get_northbound_proxy(self):
        return {"northbound_proxy": 1.2}

    # ----------------------------- #
    # Daily cache wrappers
    # ----------------------------- #
    def _get_index_daily(self, bj_now, cache, force):
        return cache.get_or_fetch(
            "index.json",
            lambda: self.get_china_index_snapshot(bj_now),
            force_refresh=force
        )

    def _get_advdec_daily(self, cache, force):
        return cache.get_or_fetch(
            "advdec.json",
            self.get_advdec,
            force_refresh=force
        )

    def _get_turnover_daily(self, cache, force):
        return cache.get_or_fetch(
            "turnover.json",
            self.get_turnover,
            force_refresh=force
        )

    def _get_a50_daily(self, cache, force):
        return cache.get_or_fetch(
            "a50.json",
            self.get_a50,
            force_refresh=force
        )

    def _get_northbound_daily(self, cache, force):
        return cache.get_or_fetch(
            "northbound_proxy.json",
            self.get_northbound_proxy,
            force_refresh=force
        )

    # ----------------------------- #
    # Full market universe (parquet DB)
    # ----------------------------- #
    def _get_all_stocks_db(self, bj_now, overwrite_db_today=False):
        date_str = bj_now.strftime("%Y%m%d")
        db = AshareDailyDB(date_str)

        if db.exists() and not overwrite_db_today:
            df = db.load()
            return df, db.file

        # fresh fetch
        try:
            df = ak.stock_zh_a_spot()
            if df is not None:
                db.save(df, overwrite=True)
        except Exception as e:
            log_warning(f"ak.stock_zh_a_spot() failed: {e}")
            df = db.load()

        return df, db.file

    # ----------------------------- #
    # v10 snapshot assembly
    # ----------------------------- #
    def prepare_daily_market_snapshot(self, bj_now: datetime, force=False, overwrite_db_today=False):
        cache = DayCacheManager(bj_now)

        snap = {}
        snap["index"] = self._get_index_daily(bj_now, cache, force)
        snap["advdec"] = self._get_advdec_daily(cache, force)
        snap["turnover"] = self._get_turnover_daily(cache, force)
        snap["a50"] = self._get_a50_daily(cache, force)
        snap["northbound_proxy"] = self._get_northbound_daily(cache, force)

        df, path = self._get_all_stocks_db(bj_now, overwrite_db_today)
        snap["all_stocks_file"] = str(path)

        cache.save("snapshot.json", snap)
        log_info("[Snapshot] Daily snapshot completed (v10-alpha)")

        return snap

    # ----------------------------- #
    # Additional method used by v10 engine
    # ----------------------------- #
    def get_today_stock_spot_df(self):
        """Return today's full A-share universe (ak), without DB logic."""
        try:
            df = ak.stock_zh_a_spot()
            return df
        except Exception as e:
            log_warning(f"[Fetcher] get_today_stock_spot_df failed: {e}")
            return None
