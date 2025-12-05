# core/adapters/datasources/global/global_lead_client.py
# -*- coding: utf-8 -*-

from __future__ import annotations
import os
from datetime import date
from typing import Dict, Any, Optional

from core.utils.logger import log
from core.adapters.cache.symbol_cache import (
    load_symbol_daily,
    save_symbol_daily,
    get_symbol_daily_path,
)

from core.adapters.datasources.cn.yf_client_cn import get_macro_daily


_GLOBAL_LEAD_REFRESHED: Dict[str, bool] = {}


def _force_delete_lead_cache(trade_date: date, symbol: str) -> None:
    cache_path = get_symbol_daily_path("global", trade_date, symbol, kind="lead")
    if os.path.exists(cache_path):
        os.remove(cache_path)


def get_global_lead(symbol: str, trade_date: date, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
    global _GLOBAL_LEAD_REFRESHED

    if force_refresh and not _GLOBAL_LEAD_REFRESHED.get(symbol):
        _force_delete_lead_cache(trade_date, symbol)
        _GLOBAL_LEAD_REFRESHED[symbol] = True

    cached = load_symbol_daily("global", trade_date, symbol, kind="lead")
    if cached is not None:
        return cached

    snap = get_macro_daily(symbol, trade_date)
    if snap is None:
        return None

    save_symbol_daily("global", trade_date, symbol, kind="lead", data=snap)
    return snap


# ==============================================================
# ⭐ 修复：提供类包装 + fetch() 方法  (ashare_fetcher 需要这一层)
# ==============================================================

class GlobalLeadClient:
    """
    Wrapper：统一对外接口，使 ashare_fetcher 能使用 client.fetch(trade_date)
    """

    SYMBOLS = ["^TNX", "^FVX", "DX-Y.NYB", "^IXIC"]

    def fetch(self, trade_date: date, force_refresh: bool = False) -> Dict[str, Any]:
        result = {}
        for sym in self.SYMBOLS:
            snap = get_global_lead(sym, trade_date, force_refresh)
            result[sym] = snap
        return result
