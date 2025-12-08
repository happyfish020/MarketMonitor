# core/adapters/datasources/cn/futures_source.py

"""
UnifiedRisk V12 - FuturesSource
股指期货（IF/IC/IM 等）数据源（合并式）
"""

import os
from typing import Optional

import pandas as pd

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.adapters.cache.symbol_cache import _normalize_symbol
from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger
from core.utils.yf_utils import fetch_yf_history

LOG = get_logger("DS.Futures")


class FuturesSource(BaseDataSource):
    """
    get_series(symbol, refresh=False) -> DataFrame(date, close, pct)
    get_last_quote(symbol, refresh=False) -> {"close": float, "pct": float}
    """

    def __init__(self, market: str = "cn"):
        super().__init__("FuturesSource")
        self.config = DataSourceConfig(market=market, ds_name="futures")
        self.config.ensure_dirs()

        self.history_root = os.path.join(self.config.history_root, "futures")
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info("FuturesSource 初始化: history_root=%s", self.history_root)

    def _history_path(self, symbol: str) -> str:
        safe = _normalize_symbol(symbol)
        return os.path.join(self.history_root, f"{safe}.json")

    def _load_local(self, symbol: str) -> pd.DataFrame:
        path = self._history_path(symbol)
        data = load_json(path)
        if not data:
            LOG.warning("Futures Cache empty: symbol=%s path=%s", symbol, path)
            return pd.DataFrame(columns=["date", "close", "pct"])
        df = pd.DataFrame(data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
        LOG.info("Futures CacheRead: symbol=%s rows=%s path=%s",
                 symbol, len(df), path)
        return df

    def _save_local(self, symbol: str, df: pd.DataFrame):
        if df is None or df.empty:
            LOG.warning("Futures SaveLocal: empty df, symbol=%s", symbol)
            return
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        records = df[["date", "close", "pct"]].to_dict(orient="records")
        path = self._history_path(symbol)
        LOG.info("Futures CacheWrite: symbol=%s rows=%s path=%s",
                 symbol, len(records), path)
        save_json(path, records)

    def _fetch_remote(self, symbol: str) -> pd.DataFrame:
        """
        默认使用 YF 获取期指代理（如 IF=^IF 等，你可在 symbols.yaml 映射）。
        """
        LOG.info("Futures RemoteFetch: symbol=%s (via YF)", symbol)
        df = fetch_yf_history(symbol, period="3mo", interval="1d")
        return df

    def get_series(self, symbol: str, refresh: bool = False) -> pd.DataFrame:
        LOG.info("Futures GetSeries: symbol=%s refresh=%s", symbol, refresh)
        local_df = self._load_local(symbol)

        if not refresh and not local_df.empty:
            LOG.info("Futures UseCacheOnly: symbol=%s rows=%s",
                     symbol, len(local_df))
            return local_df

        remote_df = self._fetch_remote(symbol)
        if remote_df is None or remote_df.empty:
            LOG.warning("Futures Remote empty, fallback cache: symbol=%s", symbol)
            return local_df

        if local_df.empty:
            merged = remote_df
        else:
            merged = pd.concat([local_df, remote_df], ignore_index=True)
            merged = merged.sort_values("date").drop_duplicates("date").reset_index(drop=True)

        self._save_local(symbol, merged)
        return merged

    def get_last_quote(self, symbol: str, refresh: bool = False) -> dict:
        df = self.get_series(symbol, refresh=refresh)
        if df is None or df.empty:
            LOG.warning("Futures NoData: symbol=%s", symbol)
            return {"close": None, "pct": None}

        last = df.iloc[-1]
        close = float(last["close"])
        pct = float(last.get("pct", 0.0))
        LOG.info("Futures LastQuote: symbol=%s close=%.3f pct=%.2f",
                 symbol, close, pct)
        return {"close": close, "pct": pct}
