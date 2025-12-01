from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Union

import requests
import yfinance as yf
import pandas as pd

from unified_risk.core.datasources.commodity_fetcher import get_commodity_snapshot
from unified_risk.core.datasources.index_fetcher import fetch_index_snapshot
from unified_risk.common.logging_utils import log_info, log_warning, log_error
from unified_risk.core.datasources.sgx_a50_fetcher import fetch_sgx_a50_change_pct

import akshare as ak
from datetime import datetime
from unified_risk.common.cache_manager import DayCacheManager, AshareDailyDB
from unified_risk.common.logging_utils import log_info, log_warning




BJ_TZ = timezone(timedelta(hours=8))


def safe_int(value) -> int:
    """
    robust int converter for f49/f50:
    '-', '--', None, ''  → 0
    '123'                → 123
    123.0                → 123
    """
    try:
        if value in [None, "", "-", "--"]:
            return 0
        # float-like?
        if isinstance(value, float):
            return int(value)
        # string number?
        s = str(value).strip()
        if s in ["", "-", "--"]:
            return 0
        return int(float(s))     # handles "12.0"
    except Exception:
        return 0


 

class AshareDataFetcher:

    # ====== 原有接口（可与实际接口替换） ======
    def get_china_index_snapshot(self, bj_time):
        return {
            "sh": {"close": 3000, "pct": 0.5},
            "sz": {"close": 9500, "pct": 0.8},
        }

    def get_advance_decline(self):
        return {"adv": 1200, "dec": 800}

    def get_turnover(self):
        return 9500.0

    def get_a50_night_session(self):
        return {"a50_change": 0.3}

    def get_northbound_etf_proxy(self):
        return {"northbound_proxy": 1.2}

    # ====== 日级缓存封装 ======
    def _get_index_daily(self, bj_now, cache, force):
        return cache.get_or_fetch(
            "index.json",
            lambda: self.get_china_index_snapshot(bj_now),
            force_refresh=force
        )

    def _get_advdec_daily(self, cache, force):
        return cache.get_or_fetch(
            "advdec.json",
            self.get_advance_decline,
            force_refresh=force
        )

    def _get_turnover_daily(self, cache, force):
        return cache.get_or_fetch(
            "turnover.json",
            lambda: {"turnover_yi": float(self.get_turnover())},
            force_refresh=force
        )

    def _get_a50_daily(self, cache, force):
        return cache.get_or_fetch(
            "a50.json",
            self.get_a50_night_session,
            force_refresh=force
        )

    def _get_northbound_daily(self, cache, force):
        return cache.get_or_fetch(
            "northbound_proxy.json",
            self.get_northbound_etf_proxy,
            force_refresh=force
        )

    # ====== 全市场行情（当日专用 DB） ======
    def _get_all_stocks_db(self, bj_now, overwrite_db_today):
        date_str = bj_now.strftime("%Y%m%d")
        db = AshareDailyDB(date_str)

        if db.exists() and not overwrite_db_today:
            df = db.load()
            return df, db.file

        try:
            df = ak.stock_zh_a_spot()
        except Exception as e:
            log_warning(f"ak.stock_zh_a_spot() 抓取失败: {e}")
            df = db.load()
            return df, db.file

        if df is not None:
            db.save(df, overwrite=True)

        return df, db.file

    # ====== v9.5.1 最终签名（与 engine 匹配） ======
    def prepare_daily_market_snapshot(self, bj_now: datetime, force=False, overwrite_db_today=False):
        cache = DayCacheManager(bj_now)

        snap = {}
        snap["index"] = self._get_index_daily(bj_now, cache, force)
        snap["advdec"] = self._get_advdec_daily(cache, force)
        snap["northbound_proxy"] = self._get_northbound_daily(cache, force)
        snap["a50"] = self._get_a50_daily(cache, force)
        snap["turnover"] = self._get_turnover_daily(cache, force)

        df, path = self._get_all_stocks_db(bj_now, overwrite_db_today)
        snap["all_stocks_file"] = str(path)

        # 写 snapshot.json
        cache.save("snapshot.json", snap)
        log_info("[Snapshot] 日级 snapshot 完成")

        return snap
