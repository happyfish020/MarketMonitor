from __future__ import annotations

import time
from typing import Dict, Optional

import pandas as pd
import yfinance as yf

try:
    import akshare as ak
except Exception:
    ak = None

from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.YF.ETF")


class YFETFClient:
    def __init__(self, ttl_seconds: int = 600) -> None:
        self._yf_cache: Dict[str, pd.DataFrame] = {}
        self._yf_cache_expire: Dict[str, float] = {}
        self._ttl = ttl_seconds

        self._yf_map: Dict[str, str] = {
            "510300": "510300.SS",
            "510050": "510050.SS",
            "159902": "159902.SZ",
            "512880": "512880.SS",
            "159915": "159915.SZ",
            "159922": "159922.SZ",
            "159619": "159619.SZ",
            "512000": "512000.SS",
            "159901": "159901.SZ",
            "159919": "159919.SZ",
        }

    def get_etf_daily(self, symbol: str) -> Optional[pd.DataFrame]:
        now = time.time()
        if symbol in self._yf_cache and now < self._yf_cache_expire.get(symbol, 0):
            return self._yf_cache[symbol].copy()

        try:
            yf_symbol = self._yf_map.get(symbol, symbol)
            if yf_symbol == symbol and symbol.isdigit():
                yf_symbol = symbol + ".SS"

            tk = yf.Ticker(yf_symbol)
            hist = tk.history(period="90d")

            if hist is not None and not hist.empty:
                tmp = hist[["Close", "Volume"]].copy()
                tmp.reset_index(inplace=True)
                tmp.rename(
                    columns={
                        "Date": "date",
                        "Close": "close",
                        "Volume": "volume",
                    },
                    inplace=True,
                )
                df = tmp.sort_values("date").reset_index(drop=True)

                self._yf_cache[symbol] = df
                self._yf_cache_expire[symbol] = now + self._ttl

                LOG.info(
                    "[YF] ETF %s(%s) rows=%d", symbol, yf_symbol, len(df)
                )
                return df.copy()
            else:
                LOG.warning("[YF] ETF %s(%s) 返回空数据", symbol, yf_symbol)
        except Exception as e:
            LOG.warning("[YF] ETF 获取失败 %s: %s", symbol, e)

        if ak is not None and symbol.isdigit():
            try:
                df = ak.fund_etf_hist_sina(symbol=symbol)
                if df is not None and not df.empty:
                    df = df.rename(
                        columns={
                            "日期": "date",
                            "收盘": "close",
                            "成交量": "volume",
                        }
                    )
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date").reset_index(drop=True)[
                        ["date", "close", "volume"]
                    ]
                    self._yf_cache[symbol] = df
                    self._yf_cache_expire[symbol] = now + self._ttl
                    LOG.info("[AK] ETF %s 使用 akshare 备份成功", symbol)
                    return df.copy()
            except Exception as e:
                LOG.warning("[AK] ETF 备份失败 %s: %s", symbol, e)

        LOG.error("[ETF] 无法获取 ETF 日线数据: %s", symbol)
        return None

    def get_latest_change_pct(self, symbol: str) -> Optional[float]:
        if symbol.isdigit():
            df = self.get_etf_daily(symbol)
            if df is None or df.empty or len(df) < 2:
                return None
            last = df.iloc[-1]
            prev = df.iloc[-2]
            if prev["close"] == 0:
                return None
            return float((last["close"] / prev["close"] - 1.0) * 100.0)

        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="5d")
            if hist is None or hist.empty or len(hist) < 2:
                return None
            last = hist["Close"].iloc[-1]
            prev = hist["Close"].iloc[-2]
            if prev == 0:
                return None
            return float((last / prev - 1.0) * 100.0)
        except Exception as e:
            LOG.error("[YF] get_latest_change_pct 失败 %s: %s", symbol, e)
            return None
