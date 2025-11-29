# unified_risk/common/yf_fetcher.py
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

import pandas as pd
import yfinance as yf

from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.YF.ETF")

BJ_TZ = timezone(timedelta(hours=8))


class YFETFClient:
    """
    轻量级 yfinance ETF 客户端，带内存缓存。
    - get_etf_daily(symbol): 返回最近 N 日日线 DataFrame（date, close, volume）
    - get_latest_change_pct(symbol): 返回最近一个交易日涨跌幅（%）
    """

    def __init__(self, cache_ttl: int = 600):
        self._yf_cache: Dict[str, pd.DataFrame] = {}
        self._yf_cache_expire: Dict[str, float] = {}
        self.cache_ttl = cache_ttl

    def get_etf_daily(self, symbol: str, days: int = 20) -> pd.DataFrame:
        """
        优先使用缓存；如无缓存或缓存过期，则调用 yfinance。
        返回字段：date, close, volume（按日期升序）
        """
        now = time.time()
        if symbol in self._yf_cache and now < self._yf_cache_expire.get(symbol, 0):
            return self._yf_cache[symbol].copy()

        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="60d", interval="1d")
            if hist is None or hist.empty:
                LOG.warning(f"[YF] history empty for {symbol}")
                df = pd.DataFrame(columns=["date", "close", "volume"])
            else:
                df = (
                    hist.reset_index()
                    .rename(columns={"Date": "date", "Close": "close", "Volume": "volume"})
                    [["date", "close", "volume"]]
                )
                if len(df) > days:
                    df = df.iloc[-days:].reset_index(drop=True)

            self._yf_cache[symbol] = df
            self._yf_cache_expire[symbol] = now + self.cache_ttl
            return df.copy()
        except Exception as e:
            LOG.error(f"[YF] fetch failed for {symbol}: {e}", exc_info=True)
            return pd.DataFrame(columns=["date", "close", "volume"])

    def get_latest_change_pct(self, symbol: str) -> Optional[float]:
        """返回最近一个交易日的涨跌幅（%）。"""
        df = self.get_etf_daily(symbol, days=5)
        if df.empty or len(df) < 2:
            return None
        last = df.iloc[-1]
        prev = df.iloc[-2]
        if prev["close"] == 0 or pd.isna(prev["close"]) or pd.isna(last["close"]):
            return None
        return float((last["close"] / prev["close"] - 1.0) * 100.0)

    def get_multi_latest_change_pct(self, symbols: List[str]) -> Dict[str, Optional[float]]:
        """批量获取多个 ETF 最近一个交易日涨跌幅。"""
        return {s: self.get_latest_change_pct(s) for s in symbols}
