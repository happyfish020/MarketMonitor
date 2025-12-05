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

# get_macro_daily = 你上传文件中的统一 global-leading fetch 函数
from core.adapters.datasources.cn.yf_client_cn import get_macro_daily


# =============================================================
# 本进程级别 “仅一次 FORCE 刷新” 控制
# =============================================================
_GLOBAL_LEAD_REFRESHED: Dict[str, bool] = {}


# =============================================================
# FORCE 删除缓存
# =============================================================
def _force_delete_lead_cache(trade_date: date, symbol: str) -> None:
    """
    删除 global lead 指标的缓存 JSON。
    结构例如：
        data/cache/day_global/20251205/lead_^TNX.json
    """
    cache_path = get_symbol_daily_path("global", trade_date, symbol, kind="lead")
    abs_path = os.path.abspath(cache_path)

    if os.path.exists(cache_path):
        try:
            os.remove(cache_path)
            log(f"[IO] Remove JSON (force) → {abs_path}")
        except Exception as e:
            log(f"[IO] Remove JSON FAIL → {abs_path}: {e}")


# =============================================================
# 主入口：获取 global lead 数据（带缓存 + FORCE）
# =============================================================
def get_global_lead(
    symbol: str,
    trade_date: date,
    force_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Global Leading 指标（美债、美元、纳指、VIX 等）
    与 index_series_client 完全一致的缓存规范：
    1) symbol_cache 路径
    2) FORCE：仅首次真正删除缓存
    3) fetch 逻辑统一使用 get_macro_daily()
    4) 返回结构：
        {
            symbol, date, close, prev_close, pct_change, volume
        }
    """
    global _GLOBAL_LEAD_REFRESHED

    # ========= FORCE 刷新：仅一次 ==========
    if force_refresh and not _GLOBAL_LEAD_REFRESHED.get(symbol):
        log(f"[GlobalLead] FORCE → refresh lead cache for {trade_date} / {symbol}")
        _force_delete_lead_cache(trade_date, symbol)
        _GLOBAL_LEAD_REFRESHED[symbol] = True

    # ========= Step 1: 尝试读取缓存 ==========
    cached = load_symbol_daily("global", trade_date, symbol, kind="lead")
    if cached is not None:
        return cached

    # ========= Step 2: 调用统一 fetch 函数（含 retry）==========
    snap = get_macro_daily(symbol, trade_date)
    if snap is None:
        log(f"[GlobalLead] {trade_date} {symbol} 无数据，返回 None")
        return None

    # ========= Step 3: 写缓存 ==========
    try:
        save_symbol_daily("global", trade_date, symbol, kind="lead", data=snap)
    except Exception as e:
        log(f"[GlobalLead] 写入缓存失败 {symbol} {trade_date}: {e}")

    return snap
