# core/adapters/datasources/global/index_series_client.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from datetime import date as Date
from typing import Dict, Any

from core.utils.logger import log
from core.adapters.cache.symbol_cache import get_symbol_daily_path
#from core.adapters.datasources.global.yf_client_global import get_index_daily  # ⚠️ 如名字不同，改这里
from core.adapters.datasources.cn.yf_client_cn import get_index_daily 
# —— 本进程级别的「仅刷新一次」标记 —— 
_INDEX_REFRESHED: Dict[str, bool] = {}
# 也可以只用一个全局 bool，这里用 per-symbol 更稳妥一点


def _force_delete_index_cache(market: str, trade_date: Date, symbol: str) -> None:
    """
    FORCE 模式下：删除当日某个指数的缓存 JSON。

    统一使用 symbol_cache.get_symbol_daily_path 形成规范文件名，例如：
        data/cache/day_global/20251205/index_SPX.json
    """
    if not symbol:
        return

    cache_path = get_symbol_daily_path(market, trade_date, symbol, kind="index")
    abs_path = os.path.abspath(cache_path)

    if os.path.exists(cache_path):
        try:
            os.remove(cache_path)
            log(f"[IO] Remove JSON (force) → {abs_path}")
        except Exception as e:
            log(f"[IO] Remove JSON FAIL → {abs_path}: {e}")


def get_index_series(
    market: str,
    symbol: str,
    trade_date: Date,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """
    获取某个指数在 trade_date 当日的「时间序列快照」或日级数据。

    规范：
    - 使用 symbol_cache 路径
    - 支持 FORCE 刷新（本进程内每个 symbol 只真正删一次缓存）
    - 实际的数据抓取由 get_index_daily() 完成（内部也会做缓存）
    """
    global _INDEX_REFRESHED

    # === FORCE 模式：仅首次调用时，删除当日 指数 缓存 JSON ===
    if force_refresh and not _INDEX_REFRESHED.get(symbol) and symbol:
        log(f"[IndexSeries] FORCE → refresh index cache for {trade_date} / {symbol}")
        _force_delete_index_cache(market, trade_date, symbol)
        _INDEX_REFRESHED[symbol] = True

    # === 拉取数据（内部已通过 yf_client_global 做了缓存读写）===
    snap = get_index_daily(market, symbol, trade_date)

    if not snap:
        log(f"[IndexSeries] {trade_date} {symbol} 无指数数据，返回空。")
        return {}

    return snap
