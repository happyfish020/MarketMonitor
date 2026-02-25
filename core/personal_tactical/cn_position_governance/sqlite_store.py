from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple, Dict, Any, List


@dataclass(frozen=True)
class PositionConfig:
    symbol: str
    theme: str
    max_lots: int
    theme_cap_pct: float
    enable_add: int
    enable_trim: int


@dataclass(frozen=True)
class PositionStateRow:
    trade_date: str
    symbol: str
    lots_held: int
    avg_cost: float
    exposure_pct: float
    risk_level: str
    add_permission: int
    trim_required: int
    run_id: str
    created_at: str


@dataclass(frozen=True)
class EventRow:
    trade_date: str
    symbol: str
    event_type: str
    reason: str
    run_id: str
    created_at: str


class SqliteStore:
    """SQLite store for CN_POSITION_GOVERNANCE_V1 (idempotent upserts)."""

    def __init__(self, sqlite_path: Path, schema_sql_path: Path) -> None:
        self.sqlite_path = sqlite_path
        self.schema_sql_path = schema_sql_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.sqlite_path))
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        sql = self.schema_sql_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(sql)

    def list_position_configs(self) -> List[PositionConfig]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol, theme, max_lots, theme_cap_pct, enable_add, enable_trim "
                "FROM cn_pg_position_config ORDER BY symbol"
            ).fetchall()
        return [
            PositionConfig(
                symbol=r["symbol"],
                theme=r["theme"],
                max_lots=int(r["max_lots"]),
                theme_cap_pct=float(r["theme_cap_pct"]),
                enable_add=int(r["enable_add"]),
                enable_trim=int(r["enable_trim"]),
            )
            for r in rows
        ]

    def upsert_position_state(self, row: PositionStateRow) -> None:
        """Upsert by PK(trade_date, symbol) to support reruns safely."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cn_pg_position_state(
                    trade_date, symbol, lots_held, avg_cost, exposure_pct,
                    risk_level, add_permission, trim_required, run_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, symbol) DO UPDATE SET
                    lots_held=excluded.lots_held,
                    avg_cost=excluded.avg_cost,
                    exposure_pct=excluded.exposure_pct,
                    risk_level=excluded.risk_level,
                    add_permission=excluded.add_permission,
                    trim_required=excluded.trim_required,
                    run_id=excluded.run_id,
                    created_at=excluded.created_at
                """,
                (
                    row.trade_date, row.symbol, row.lots_held, row.avg_cost, row.exposure_pct,
                    row.risk_level, row.add_permission, row.trim_required, row.run_id, row.created_at
                ),
            )

    def insert_event_dedup(self, event: EventRow) -> None:
        """Insert event with run_id dedup (unique index)."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO cn_pg_event_log(
                    trade_date, symbol, event_type, reason, run_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event.trade_date, event.symbol, event.event_type, event.reason, event.run_id, event.created_at),
            )

    def get_state_rows(self, trade_date: str) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM cn_pg_position_state WHERE trade_date=? ORDER BY symbol",
                (trade_date,),
            ).fetchall()

    def get_theme_exposure_pct(self, trade_date: str) -> Dict[str, float]:
        """Sum exposure_pct by theme for a given trade_date using current configs."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT c.theme AS theme, COALESCE(SUM(s.exposure_pct), 0.0) AS exp
                FROM cn_pg_position_config c
                LEFT JOIN cn_pg_position_state s
                  ON s.trade_date = ? AND s.symbol = c.symbol
                GROUP BY c.theme
                """,
                (trade_date,),
            ).fetchall()
        return {r["theme"]: float(r["exp"]) for r in rows}
