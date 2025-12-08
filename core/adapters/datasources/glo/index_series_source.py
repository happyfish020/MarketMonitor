# core/adapters/datasources/glo/index_series_source.py

"""
UnifiedRisk V12 - IndexSeriesSource
指数历史数据源（合并版，默认使用 YF）
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

LOG = get_logger("DS.IndexSeries")


class IndexSeriesSource(BaseDataSource):
    """
    通用指数数据源：
      - get_series(symbol, refresh=False) -> DataFrame(date, close, pct)
    """

    def __init__(self, market: str = "cn"):
        super().__init__("IndexSeriesSource")
        self.config = DataSourceConfig(market=market, ds_name="index_series")
        self.config.ensure_dirs()

        # 使用 history_root 存放时间序列
        self.history_root = os.path.join(self.config.history_root, "index_series")
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info("IndexSeriesSource 初始化: history_root=%s", self.history_root)

    def _history_path(self, symbol: str) -> str:
        safe = _normalize_symbol(symbol)
        return os.path.join(self.history_root, f"{safe}.json")

    def _load_local(self, symbol: str) -> pd.DataFrame:
        path = self._history_path(symbol)
        data = load_json(path)
        if not data:
            LOG.warning("IndexSeries Cache empty: symbol=%s path=%s", symbol, path)
            return pd.DataFrame(columns=["date", "close", "pct"])
    
        df = pd.DataFrame(data)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            df = df.sort_values("date").reset_index(drop=True)
    
        LOG.info("IndexSeries CacheRead: symbol=%s rows=%s path=%s",
                 symbol, len(df), path)
        return df
    
    def _save_local(self, symbol: str, df: pd.DataFrame):
        if df is None or df.empty:
            LOG.warning("IndexSeries SaveLocal: empty df, symbol=%s", symbol)
            return

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        records = df[["date", "close", "pct"]].to_dict(orient="records")

        path = self._history_path(symbol)
        LOG.info("IndexSeries CacheWrite: symbol=%s rows=%s path=%s",
                 symbol, len(records), path)
        save_json(path, records)

    def _fetch_remote(self, symbol: str) -> pd.DataFrame:
        LOG.info("IndexSeries RemoteFetch: symbol=%s (via YF)", symbol)
        df = fetch_yf_history(symbol, period="6mo", interval="1d")
    
        if df is None or df.empty:
            return df
    
        # 强制转 tz-naive
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        return df
    
    def get_series(self, symbol: str, refresh: bool = False) -> pd.DataFrame:
        LOG.info("IndexSeries GetSeries: symbol=%s refresh=%s", symbol, refresh)

        local_df = self._load_local(symbol)
        if not refresh and not local_df.empty:
            LOG.info("IndexSeries UseCacheOnly: symbol=%s rows=%s",
                     symbol, len(local_df))
            return local_df

        # 需要刷新（或无缓存）
        remote_df = self._fetch_remote(symbol)
        if remote_df is None or remote_df.empty:
            LOG.warning("IndexSeries Remote empty, fallback cache: symbol=%s", symbol)
            return local_df

        if local_df.empty:
            merged = remote_df
        else:
            merged = pd.concat([local_df, remote_df], ignore_index=True)
            merged["date"] = pd.to_datetime(merged["date"]).dt.tz_localize(None)
            merged = merged.sort_values("date").drop_duplicates("date").reset_index(drop=True)

        self._save_local(symbol, merged)
        return merged
