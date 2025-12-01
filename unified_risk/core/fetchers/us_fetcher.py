
"""US market data fetcher (安全版，支持周末/假日)。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional

from unified_risk.common.logging_utils import log_info, log_warning
from unified_risk.common.yf_fetcher import get_last_valid_bar, safe_yf_last_bars


def map_us_symbol(symbol: str) -> str:
    s = symbol.upper()
    mapping = {
        "SPY": "SPY",
        "SP500": "^GSPC",
        "SPX": "^GSPC",
        "NASDAQ": "^IXIC",
        "NDX": "^NDX",
        "DOW": "^DJI",
        "DJI": "^DJI",
        "R3000": "^RUA",
        "R2000": "^RUT",
        "VIX": "^VIX",
        "DXY": "DX-Y.NYB",
    }
    return mapping.get(s, symbol)


def fetch_us_last_price(symbol: str) -> Optional[float]:
    yf_symbol = map_us_symbol(symbol)
    bar = get_last_valid_bar(yf_symbol, lookback_days=15, interval="1d")
    if bar is None:
        log_warning(f"[US] {symbol}({yf_symbol}): no valid bar")
        return None
    _, close, _ = bar
    return float(close)


def fetch_us_change_pct(symbol: str) -> Optional[float]:
    yf_symbol = map_us_symbol(symbol)
    rets = safe_yf_last_bars(yf_symbol, lookback_days=20, interval="1d", min_points=2)
    if not rets:
        log_warning(f"[US] {symbol}: no change pct")
        return None
    return rets[-1]


@dataclass
class USDailySnapshot:
    nasdaq_change: float = 0.0
    spy_change: float = 0.0
    vix_level: float = 0.0


class USFetcher:
    def get_us_daily_snapshot(self) -> USDailySnapshot:
        nas = fetch_us_change_pct("NASDAQ") or 0.0
        spy = fetch_us_change_pct("SPY") or 0.0
        vix = fetch_us_last_price("VIX") or 0.0

        log_info(f"[RAW] ^IXIC | Change%: {nas:.3f}")
        log_info(f"[RAW] SPY   | Change%: {spy:.3f}")
        log_info(f"[RAW] ^VIX  | Price  : {vix:.3f}")

        return USDailySnapshot(
            nasdaq_change=nas,
            spy_change=spy,
            vix_level=vix,
        )

    def get_short_term_series(self) -> Dict[str, list[float]]:
        return {"nasdaq": safe_yf_last_bars("^IXIC", lookback_days=10, interval="1d")}

    def get_weekly_series(self) -> Dict[str, list[float]]:
        return {"sp500": safe_yf_last_bars("^GSPC", lookback_days=180, interval="1wk")}
