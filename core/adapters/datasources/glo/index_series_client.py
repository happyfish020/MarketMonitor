# core/adapters/datasources/global/index_series_client.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from datetime import date as Date
from typing import Dict, Any

from core.utils.logger import log
from core.adapters.cache.symbol_cache import get_symbol_daily_path
from core.adapters.datasources.cn.yf_client_cn import get_index_daily

_INDEX_REFRESHED: Dict[str, bool] = {}


def _force_delete_index_cache(market: str, trade_date: Date, symbol: str) -> None:
    cache_path = get_symbol_daily_path(market, trade_date, symbol, kind="index")
    if os.path.exists(cache_path):
        os.remove(cache_path)


def get_index_series(
    market: str,
    symbol: str,
    trade_date: Date,
    force_refresh: bool = False,
) -> Dict[str, Any]:

    if force_refresh and not _INDEX_REFRESHED.get(symbol):
        _force_delete_index_cache(market, trade_date, symbol)
        _INDEX_REFRESHED[symbol] = True

    snap = get_index_daily(symbol, trade_date)
    return snap or {}


# ==============================================================
# ⭐ 修复：提供类包装 + fetch() 方法
# ==============================================================

class IndexSeriesClient:

    SYMBOLS = {
        "sh": "000001.SS",
        "sz": "399001.SZ",
        "cyb": "399006.SZ",
        "hs300": "000300.SS",
    }

    def fetch(self, trade_date: Date, force_refresh: bool = False) -> Dict[str, Any]:
        block = {}
        for key, sym in self.SYMBOLS.items():
            block[key] = get_index_series("global", sym, trade_date, force_refresh)
        return block
