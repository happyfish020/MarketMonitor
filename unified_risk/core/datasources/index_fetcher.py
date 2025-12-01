
from typing import Dict, Any

from unified_risk.common.yf_safe import safe_yf_last_bars
from unified_risk.common.logging_utils import log_info, log_warning
from unified_risk.common.symbol_mapper import map_symbol
 
def _map_symbol(symbol: str) -> str:
    return  map_symbol(symbol)


def fetch_index_last_price(symbol: str) -> float:
    yf_symbol = map_symbol(symbol)
    snap = safe_yf_last_bars(yf_symbol, lookback_days=10, interval="1d", min_points=1)
    if not snap:
        log_warning(f"[INDEX] {symbol}({yf_symbol}): no last price, use 0.0")
        return 0.0
    price = float(snap["last"])
    log_info(f"[INDEX] {symbol}: last={price:.4f}")
    return price


def fetch_index_change_pct(symbol: str) -> float:
    yf_symbol = _map_symbol(symbol)
    snap = safe_yf_last_bars(yf_symbol, lookback_days=10, interval="1d", min_points=2)
    if not snap:
        log_warning(f"[INDEX] {symbol}({yf_symbol}): no change pct, use 0.0")
        return 0.0
    pct = float(snap["changePct"])
    log_info(f"[INDEX] {symbol}: pct={pct:.3f}")
    return pct


def fetch_index_snapshot(symbol: str) -> Dict[str, Any]:
    yf_symbol = _map_symbol(symbol)
    snap = safe_yf_last_bars(yf_symbol, lookback_days=10, interval="1d", min_points=2)
    if not snap:
        log_warning(f"[INDEX] {symbol}({yf_symbol}): empty snapshot")
        return {"price": 0.0, "pct": 0.0, "last": 0.0}

    price = float(snap["last"])
    pct = float(snap["changePct"])
    log_info(f"[INDEX] {symbol}: last={price:.4f}, pct={pct:.3f}")
    return {"price": price, "pct": pct, "last": price}
