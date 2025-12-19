# core/adapters/cache/symbol_cache.py
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Optional

from core.utils.config_loader import load_paths
from core.adapters.cache.file_cache import load_json, save_json

_paths = load_paths()
_BASE_CACHE_DIR = _paths.get("cache_dir", "data/cache/")


def normalize_ashare_symbol(symbol: str) -> str:
    """
    将 spot parquet 的 A 股 symbol (sh600000, sz000001, bj830799)
    转换为标准化 symbol (600000.SH, 000001.SZ, 830799.BJ)。

    不会替代 _normalize_symbol，仅用于 A 股数据源。
    """

    if not symbol:
        return ""

    s = symbol.strip().lower()

    # 深交所
    if s.startswith("sz") and len(s) >= 8:
        return s[2:8].upper() + ".SZ"

    # 上交所
    if s.startswith("sh") and len(s) >= 8:
        return s[2:8].upper() + ".SH"

    # 北交所
    if s.startswith("bj") and len(s) >= 8:
        return s[2:8].upper() + ".BJ"

    # 如果已经是标准格式 000001.SZ
    if "." in s and len(s) >= 9:
        core, exch = s.split(".", 1)
        return core.upper() + "." + exch.upper()

    # fallback（异常符号）
    return s.upper()


def normalize_symbol(symbol: str) -> str:
    """
    统一把 symbol 变成文件名安全的形式：
    510300.SS   -> 510300_SS
    ^VIX        -> VIX
    GC=F       -> GC_F
    """
    s = symbol.strip()
    for ch in ["^", ".", "=", "/", "\\", " "]:
        s = s.replace(ch, "_")
    return s


def _day_root(market: str, trade_date: date) -> str:
    """
    日级缓存根目录：
    data/cache/day_cn/YYYYMMDD/
    data/cache/day_us/YYYYMMDD/
    data/cache/day_global/YYYYMMDD/
    """
    day_str = trade_date.strftime("%Y%m%d")
    market = market.lower()
    return os.path.join(_BASE_CACHE_DIR, f"day_{market}", day_str)


def get_symbol_daily_path(market: str, trade_date: date, symbol: str, kind: str = "generic") -> str:
    """
    获取某个 symbol 的日级缓存文件路径。

    示例：
    - market = "cn",  symbol="510300.SS", kind="etf"
      => data/cache/day_cn/20251202/etf_510300_SS.json

    - market = "global", symbol="^VIX", kind="macro"
      => data/cache/day_global/20251202/macro_VIX.json
    """
    root = _day_root(market, trade_date)
    sym_norm = normalize_symbol(symbol)
    fname = f"{kind}_{sym_norm}.json"
    return os.path.join(root, fname)


def load_symbol_daily(market: str, trade_date: date, symbol: str, kind: str = "generic") -> Optional[Any]:
    path = get_symbol_daily_path(market, trade_date, symbol, kind)
    return load_json(path)


def save_symbol_daily(market: str, trade_date: date, symbol: str, kind: str, data: Any) -> str:
    path = get_symbol_daily_path(market, trade_date, symbol, kind)
    save_json(path, data)
    return path


def get_symbol_intraday_path(market: str, symbol: str, kind: str = "generic") -> str:
    """
    盘中缓存路径：
    data/cache/intraday_cn/etf_510300_SS.json
    data/cache/intraday_global/macro_VIX.json
    """
    market = market.lower()
    root = os.path.join(_BASE_CACHE_DIR, f"intraday_{market}")
    sym_norm = normalize_symbol(symbol)
    fname = f"{kind}_{sym_norm}.json"
    return os.path.join(root, fname)


def load_symbol_intraday(market: str, symbol: str, kind: str = "generic") -> Optional[Any]:
    path = get_symbol_intraday_path(market, symbol, kind)
    return load_json(path)


def save_symbol_intraday(market: str, symbol: str, kind: str, data: Any) -> str:
    path = get_symbol_intraday_path(market, symbol, kind)
    save_json(path, data)
    return path
