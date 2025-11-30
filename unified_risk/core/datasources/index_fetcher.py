
from __future__ import annotations
import yfinance as yf
from typing import Optional, Dict, Any
import logging
from unified_risk.common.symbol_mapper import map_symbol

logger = logging.getLogger(__name__)

def fetch_index_change_pct(symbol: str) -> Optional[float]:
    mapped = map_symbol(symbol)
    try:
        tk = yf.Ticker(mapped)
        hist = tk.history(period="5d", interval="1d")
        closes = hist["Close"].dropna().tail(2)
        if len(closes) < 2:
            return None
        prev, last = closes.iloc[-2], closes.iloc[-1]
        if prev == 0:
            return None
        pct = (last - prev) / prev * 100
        return round(pct, 4)
    except Exception as e:
        logger.warning(f"[Index] pct failed {symbol}->{mapped}: {e}")
        return None

def fetch_index_last_price(symbol: str) -> Optional[float]:
    mapped = map_symbol(symbol)
    try:
        tk = yf.Ticker(mapped)
        hist = tk.history(period="1d", interval="1d")
        if hist.empty:
            return None
        return float(hist["Close"].dropna().iloc[-1])
    except Exception as e:
        logger.warning(f"[Index] last failed {symbol}->{mapped}: {e}")
        return None

def fetch_index_snapshot(symbol: str) -> Dict[str, Any]:
    mapped = map_symbol(symbol)
    try:
        tk = yf.Ticker(mapped)
        hist = tk.history(period="5d", interval="1d")
        closes = hist["Close"].dropna().tail(2)
        if len(closes) < 2:
            return {
                "symbol": symbol,
                "mapped": mapped,
                "last": None,
                "prev": None,
                "ret": None,
                "pct": None,
            }
        prev, last = float(closes.iloc[-2]), float(closes.iloc[-1])
        ret = (last - prev) / prev if prev != 0 else None
        pct = ret * 100 if ret is not None else None
        return {
            "symbol": symbol,
            "mapped": mapped,
            "last": last,
            "prev": prev,
            "ret": round(ret, 6) if ret is not None else None,
            "pct": round(pct, 4) if pct is not None else None,
        }
    except Exception as e:
        logger.warning(f"[Index] snapshot failed {symbol}->{mapped}: {e}")
        return {
            "symbol": symbol,
            "mapped": mapped,
            "last": None,
            "prev": None,
            "ret": None,
            "pct": None,
        }

def get_a50_night_session() -> Dict[str, Any]:
    return fetch_index_snapshot("^FTXIN9")
