from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.types import Date

from .config import ORACLE_DSN, ORACLE_PRICE_TABLE


@dataclass(frozen=True)
class PriceBar:
    trade_date: _dt.date
    close: float
    volume: float


@dataclass(frozen=True)
class OracleFactsResult:
    symbol: str
    bars: List[PriceBar]  # sorted ascending


class OracleFacts:
    """Oracle facts loader (DATE bind only; no string comparisons)."""

    def __init__(self) -> None:
        self._engine = create_engine(ORACLE_DSN, pool_pre_ping=True, future=True)

    @staticmethod
    def _to_date(s: str) -> _dt.date:
        return _dt.date.fromisoformat(s)

    def load_bars(self, symbols: Sequence[str], end_trade_date: str, lookback_days: int = 35) -> Dict[str, OracleFactsResult]:
        """Load up to lookback_days of daily bars ending at end_trade_date (inclusive)."""
        end_d = self._to_date(end_trade_date)
        start_d = end_d - _dt.timedelta(days=lookback_days * 2)  # calendar buffer for non-trading days

        sql = text(
            f"""
            SELECT SYMBOL, TRADE_DATE, CLOSE, VOLUME
            FROM {ORACLE_PRICE_TABLE}
            WHERE SYMBOL IN :symbols
              AND TRADE_DATE >= :start_date
              AND TRADE_DATE <= :end_date
            ORDER BY SYMBOL, TRADE_DATE
            """
        ).bindparams(
            bindparam("start_date", type_=Date()),
            bindparam("end_date", type_=Date()),
        )

        # For Oracle IN :symbols, SQLAlchemy requires expanding bindparam
        sql = sql.bindparams(bindparam("symbols", expanding=True))

        out: Dict[str, List[PriceBar]] = {s: [] for s in symbols}
        with self._engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "symbols": list(symbols),
                    "start_date": start_d,
                    "end_date": end_d,
                },
            ).fetchall()

        for r in rows:
            out[str(r[0])].append(
                PriceBar(
                    trade_date=r[1],
                    close=float(r[2]),
                    volume=float(r[3]),
                )
            )

        # Keep only the most recent N trading bars (by rows) per symbol
        res: Dict[str, OracleFactsResult] = {}
        for sym, bars in out.items():
            if not bars:
                res[sym] = OracleFactsResult(symbol=sym, bars=[])
                continue
            # already ordered
            if len(bars) > lookback_days:
                bars = bars[-lookback_days:]
            res[sym] = OracleFactsResult(symbol=sym, bars=bars)
        return res
