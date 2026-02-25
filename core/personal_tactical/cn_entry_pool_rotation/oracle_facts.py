#-*- coding: utf-8 -*-
"""
oracle_facts.py

Only responsible for reading price facts from Oracle.

Hard constraints (frozen):
- Use Oracle table: SECOPR.CN_STOCK_DAILY_PRICE
- Fields: SYMBOL, TRADE_DATE, CLOSE, VOLUME
- Must use DATE binds (no string implicit comparisons)
- No silent fail: missing facts must raise
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Sequence, List, Tuple

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class SymbolFacts:
    symbol: str               # internal symbol (e.g., "002463")
    oracle_symbol: str        # oracle symbol used in DB (e.g., "002463" or "002463.SZ")
    trade_date: date

    close: float
    volume: float

    # computed with lookback windows excluding trade_date
    high_60d: float
    vol_ma20: float

    # for diagnostics
    start_date: date
    end_date: date
    rows_loaded: int


def make_oracle_engine(oracle_dsn: str) -> Engine:
    # frozen DSN style: oracle+oracledb://...
    return create_engine(oracle_dsn, pool_pre_ping=True, future=True)


def get_price_series(
    engine: Engine,
    trade_date: date,
    oracle_symbols: Sequence[str],
    lookback_days: int,
) -> Dict[str, List[Tuple[date, float, float]]]:
    """
    Returns per-oracle_symbol series of (trade_date, close, volume) for a range.

    Implementation note:
    - oracledb driver doesn't support binding a Python tuple directly to "IN :symbols"
    - Use SQLAlchemy expanding bindparam: SYMBOL IN (:symbols_1, :symbols_2, ...)
    """
    if not oracle_symbols:
        return {}

    end_date = trade_date
    start_date = trade_date - timedelta(days=int(lookback_days) + 120)  # buffer for non-trading days

    stmt = (
        text(
            """
            SELECT SYMBOL, TRADE_DATE, CLOSE, VOLUME
            FROM SECOPR.CN_STOCK_DAILY_PRICE
            WHERE TRADE_DATE BETWEEN :start_date AND :end_date
              AND SYMBOL IN :symbols
            ORDER BY SYMBOL, TRADE_DATE
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )

    params = {
        "start_date": start_date,  # DATE bind
        "end_date": end_date,      # DATE bind
        "symbols": list(oracle_symbols),
    }

    series: Dict[str, List[Tuple[date, float, float]]] = {s: [] for s in oracle_symbols}

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).fetchall()

    for r in rows:
        sym = r[0]
        td = r[1]
        close = float(r[2]) if r[2] is not None else None
        vol = float(r[3]) if r[3] is not None else None
        if close is None or vol is None:
            # We keep rows but downstream will fail if required day missing.
            continue
        # td from Oracle is datetime.datetime; convert to date
        td_date = td.date() if hasattr(td, "date") else td
        if sym in series:
            series[sym].append((td_date, close, vol))

    return series


def _compute_high(series: List[Tuple[date, float, float]], end_exclusive: date, window: int) -> float | None:
    vals = [c for (d, c, v) in series if d < end_exclusive]
    if len(vals) < window:
        return None
    return max(vals[-window:])


def _compute_ma(series: List[Tuple[date, float, float]], end_exclusive: date, window: int) -> float | None:
    vals = [v for (d, c, v) in series if d < end_exclusive]
    if len(vals) < window:
        return None
    w = vals[-window:]
    return sum(w) / float(window)


def get_facts_for_symbols(
    engine: Engine,
    trade_date: date,
    symbol_map: Dict[str, str],  # internal_symbol -> oracle_symbol
    lookback_high: int = 60,
    lookback_vol_ma: int = 20,
) -> Dict[str, SymbolFacts]:
    """
    Builds SymbolFacts for each internal symbol for the given trade_date.
    Raises if any symbol is missing required facts on trade_date.
    """
    internal_symbols = list(symbol_map.keys())
    oracle_symbols = [symbol_map[s] for s in internal_symbols]

    # Load enough history to compute indicators; range buffer handled in get_price_series()
    series_map = get_price_series(engine, trade_date, oracle_symbols, lookback_days=max(lookback_high, lookback_vol_ma))

    facts: Dict[str, SymbolFacts] = {}
    missing: List[str] = []

    for internal_sym, oracle_sym in symbol_map.items():
        s = series_map.get(oracle_sym) or []
        # locate today's row
        today_rows = [x for x in s if x[0] == trade_date]
        if not today_rows:
            missing.append(oracle_sym)
            continue
        td, close, vol = today_rows[-1]

        high_60d = _compute_high(s, trade_date, lookback_high)
        vol_ma20 = _compute_ma(s, trade_date, lookback_vol_ma)
        if high_60d is None or vol_ma20 is None:
            # insufficient history is also a hard failure (frozen: no silent)
            missing.append(oracle_sym)
            continue

        facts[internal_sym] = SymbolFacts(
            symbol=internal_sym,
            oracle_symbol=oracle_sym,
            trade_date=trade_date,
            close=float(close),
            volume=float(vol),
            high_60d=float(high_60d),
            vol_ma20=float(vol_ma20),
            start_date=trade_date - timedelta(days=max(lookback_high, lookback_vol_ma) + 120),
            end_date=trade_date,
            rows_loaded=len(s),
        )

    if missing:
        raise RuntimeError(f"Missing facts for symbols on {trade_date}: {missing}")

    return facts
