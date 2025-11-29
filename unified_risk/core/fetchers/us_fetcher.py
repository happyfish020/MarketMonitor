"""US market data fetcher for v5.2."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

import yfinance as yf

from unified_risk.common.logging_utils import log_info, log_warning


@dataclass
class USDailySnapshot:
    nasdaq_change: float = 0.0
    spy_change: float = 0.0
    vix: float = 20.0


class USDataFetcher:
    def _fetch_yahoo(self, symbol: str) -> Dict[str, float]:
        """使用 yfinance 获取最近两天数据，计算当日涨跌幅。"""
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="2d")
            if hist is None or hist.empty:
                log_warning(f"Yahoo history empty for {symbol}")
                return {"price": 0.0, "changePct": 0.0}
            hist = hist.tail(2)
            last = float(hist["Close"].iloc[-1])
            prev = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else last
            change = (last - prev) / prev * 100.0 if prev else 0.0
            return {"price": last, "changePct": change}
        except Exception as e:
            log_warning(f"Yahoo fetch failed for {symbol}: {e}")
            return {"price": 0.0, "changePct": 0.0}

    def _fetch_series(self, symbol: str, period: str = "10d") -> list[float]:
        """获取某指数最近 N 日涨跌幅序列。"""
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period=period)
            if hist is None or hist.empty or len(hist) < 2:
                return []
            closes = hist["Close"]
            rets = closes.pct_change().dropna() * 100.0
            return [float(x) for x in rets.tolist()]
        except Exception as e:
            log_warning(f"Yahoo series fetch failed for {symbol}: {e}")
            return []

    def _fetch_weekly_series(self, symbol: str, period: str = "3mo") -> list[float]:
        """获取某指数周线涨跌幅序列。"""
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period=period)
            if hist is None or hist.empty or len(hist) < 5:
                return []
            # 周线收盘（按周五）
            weekly = hist["Close"].resample("W-FRI").last().dropna()
            rets = weekly.pct_change().dropna() * 100.0
            return [float(x) for x in rets.tolist()]
        except Exception as e:
            log_warning(f"Yahoo weekly series failed for {symbol}: {e}")
            return []

    def get_daily_snapshot(self) -> Dict[str, Any]:
        nas = self._fetch_yahoo("^IXIC")
        spy = self._fetch_yahoo("SPY")
        vix = self._fetch_yahoo("^VIX")
        log_info(f"[RAW] ^IXIC | Change%: {nas['changePct']:.3f}")
        log_info(f"[RAW] SPY   | Change%: {spy['changePct']:.3f}")
        log_info(f"[RAW] ^VIX  | Price  : {vix['price']:.3f}")
        return {"nasdaq": nas, "spy": spy, "vix": vix}

    def get_short_term_series(self) -> Dict[str, list[float]]:
        """美股短期日线序列（纳指）。"""
        return {"nasdaq": self._fetch_series("^IXIC", period="10d")}

    def get_weekly_series(self) -> Dict[str, list[float]]:
        """美股周线序列（SP500）。"""
        return {"sp500": self._fetch_weekly_series("^GSPC", period="6mo")}
