
"""指数行情抓取模块（安全版，支持周末/假日自动回退）。"""

from __future__ import annotations

from typing import Optional, Dict

from unified_risk.common.logging_utils import log_warning
from unified_risk.common.yf_fetcher import get_last_valid_bar, safe_yf_last_bars
from unified_risk.common.yf_safe import safe_yf_last_bars
from unified_risk.common.logging_utils import log_info, log_warning


def map_symbol(symbol: str) -> str:
    s = symbol.upper()
    mapping = {
        "SH": "000001.SS",
        "SZ": "399001.SZ",
        "HS300": "000300.SS",
        "CSI300": "000300.SS",
        "A50": "XIN9.DE",
        "SPX": "^GSPC",
        "SP500": "^GSPC",
        "SPY": "SPY",
        "NASDAQ": "^IXIC",
        "NDX": "^NDX",
        "DOW": "^DJI",
        "DJI": "^DJI",
        "VIX": "^VIX",
        "DXY": "DX-Y.NYB",
        "HSI": "^HSI",
        "^FTXIN9": "XIN9.FGI",
        "FTXIN9": "XIN9.FGI",
         "A50": "XIN9.FGI",
    }
    return mapping.get(s, symbol)


def fetch_index_last_price(symbol: str) -> Optional[float]:
    yf_symbol = map_symbol(symbol)
    bar = get_last_valid_bar(yf_symbol, lookback_days=15, interval="1d")
    if bar is None:
        log_warning(f"[INDEX] {symbol}({yf_symbol}): no valid bar")
        return None
    _, close, _ = bar
    return float(close)

def fetch_index_change_pct(symbol: str) -> Optional[float]:
    yf_symbol = map_symbol(symbol)
    rets = safe_yf_last_bars(
        yf_symbol,
        lookback_days=10,
        interval="1d",
        min_points=1,
    )

    # 周末 / 长假：返回 0，不视为异常
    if not rets:
        return 0.0

    return rets[-1]


def fetch_index_snapshot(symbol: str):
    """
    统一指数行情接口
    - 自动 fallback 最近有效交易日
    """
    yf_symbol = SYMBOL_MAP.get(symbol, symbol)

    snap = safe_yf_last_bars(yf_symbol, lookback_days=10, interval="1d", min_points=2)
    if not snap:
        log_warning(f"[INDEX] {symbol}({yf_symbol}): empty snapshot")
        return {"price": 0.0, "pct": 0.0, "last": 0.0}

    price = snap["last"]
    pct = snap["changePct"]

    log_info(f"[INDEX] {symbol} last={price:.3f}, pct={pct:.3f}")
    return {"price": price, "pct": pct, "last": price}
 