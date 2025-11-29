from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ...common import CacheManager, get_logger
from ...common.time_utils import fmt_date_compact, now_bj
from ...common.yf_fetcher import fetch_last_close

LOG = get_logger("UnifiedRisk.GlobalFetcher")


@dataclass
class GlobalRawData:
    indices: Dict[str, Any]
    fx: Dict[str, Any]
    gold: Dict[str, Any]
    crypto: Dict[str, Any]


class GlobalDataFetcher:
    """全球市场数据抓取 + day_cache 管理。""" 

    def __init__(self, cache: Optional[CacheManager] = None) -> None:
        self.cache = cache or CacheManager()

    def get_raw_data(self, date_str: Optional[str] = None) -> GlobalRawData:
        if date_str is None:
            date_str = fmt_date_compact(now_bj())

        indices = self._get_or_fetch_indices(date_str)
        fx = self._get_or_fetch_fx(date_str)
        gold = self._get_or_fetch_gold(date_str)
        crypto = self._get_or_fetch_crypto(date_str)

        return GlobalRawData(indices=indices, fx=fx, gold=gold, crypto=crypto)

    def _get_or_fetch_indices(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "global", "indices")
        if cached is not None:
            return cached
        data = {
            "nasdaq": fetch_last_close("^IXIC"),
            "spy": fetch_last_close("SPY"),
            "vix": fetch_last_close("^VIX"),
            "a50": fetch_last_close("510300.SS"),
        }
        self.cache.write_key(date_str, "global", "indices", data)
        return data

    def _get_or_fetch_fx(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "global", "fx")
        if cached is not None:
            return cached
        data = {
            "usdcnh": fetch_last_close("USDCNH=X"),
            "usdjpy": fetch_last_close("JPY=X"),
        }
        self.cache.write_key(date_str, "global", "fx", data)
        return data

    def _get_or_fetch_gold(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "global", "gold")
        if cached is not None:
            return cached
        data = {
            "gold_usd": fetch_last_close("GC=F"),
        }
        self.cache.write_key(date_str, "global", "gold", data)
        return data

    def _get_or_fetch_crypto(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "global", "crypto")
        if cached is not None:
            return cached
        data = {
            "btc_usd": fetch_last_close("BTC-USD"),
            "mstr": fetch_last_close("MSTR"),
        }
        self.cache.write_key(date_str, "global", "crypto", data)
        return data
