from __future__ import annotations

import logging
from typing import Dict, Any, Optional

import yfinance as yf

logger = logging.getLogger(__name__)


def _fetch_yahoo_change_pct(symbol: str) -> Optional[float]:
    try:
        tk = yf.Ticker(symbol.upper())
        hist = tk.history(period="5d", interval="1d")
        closes = hist["Close"].dropna().tail(2)
        if len(closes) < 2:
            return None
        prev, last = closes.iloc[-2], closes.iloc[-1]
        if prev == 0:
            return None
        return round((last - prev) / prev * 100.0, 4)
    except Exception as e:
        logger.warning("yfinance pct fail %s: %s", symbol, e)
        return None


def _fetch_yahoo_last_price(symbol: str) -> Optional[float]:
    try:
        tk = yf.Ticker(symbol.upper())
        hist = tk.history(period="1d", interval="1d")
        if hist.empty:
            return None
        return float(hist["Close"].dropna().iloc[-1])
    except Exception as e:
        logger.warning("yfinance last fail %s: %s", symbol, e)
        return None


def get_commodity_snapshot() -> Dict[str, Any]:
    """大宗商品快照：黄金/原油/铜/美元指数，全部基于 yfinance。"""
    gold_pct = _fetch_yahoo_change_pct("GC=F")
    gold_usd = _fetch_yahoo_last_price("GC=F")
    oil_pct = _fetch_yahoo_change_pct("CL=F")
    copper_pct = _fetch_yahoo_change_pct("HG=F")
    dxy_pct = _fetch_yahoo_change_pct("DX-Y.NYB")

    return {
        "gold": {"pct": gold_pct, "usd": gold_usd},
        "oil": {"pct": oil_pct},
        "copper": {"pct": copper_pct},
        "dxy": {"pct": dxy_pct},
    }
