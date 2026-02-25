#-*- coding: utf-8 -*-
"""SQLite schema for ENTRY_POOL_ROTATION_REPORT_V1 (CN).

Frozen constraints:
- All objects use `cn_` prefix.
- Event sourcing + daily snapshot.
- No silent fail: schema init errors should raise.
"""

from __future__ import annotations

import sqlite3
from typing import Iterable


DDL: tuple[str, ...] = (
    # Entry pool registry
    """
    CREATE TABLE IF NOT EXISTS cn_epr_entry_pool (
      symbol TEXT PRIMARY KEY,
      name TEXT,
      group_code TEXT NOT NULL,
      entry_mode TEXT NOT NULL,
      max_lots_2026 INTEGER NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT,
      updated_at TEXT
    );
    """,
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_entry_pool_group_active
         ON cn_epr_entry_pool(group_code, is_active);""",

    # Daily state snapshot
    """
    CREATE TABLE IF NOT EXISTS cn_epr_state_snap (
      trade_date TEXT NOT NULL,
      symbol TEXT NOT NULL,
      state TEXT NOT NULL,
      breakout_level REAL,
      trigger_close REAL,
      trigger_volume_ratio REAL,
      cooldown_days_left INTEGER,
      holding_lots INTEGER,
      entry_allowed INTEGER,
      add_allowed INTEGER,
      reduce_required INTEGER,
      suggested_action TEXT,
      reason_codes TEXT,
      evidence_json TEXT,
      run_id TEXT,
      created_at TEXT,
      PRIMARY KEY (trade_date, symbol)
    );
    """,
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_state_snap_symbol_date
         ON cn_epr_state_snap(symbol, trade_date);""",
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_state_snap_date_state
         ON cn_epr_state_snap(trade_date, state);""",

    # Event log (source of truth)
    """
    CREATE TABLE IF NOT EXISTS cn_epr_state_event (
      event_id TEXT PRIMARY KEY,
      trade_date TEXT NOT NULL,
      symbol TEXT NOT NULL,
      prev_state TEXT,
      new_state TEXT NOT NULL,
      event_type TEXT NOT NULL,
      transition_rule TEXT,
      reason_codes TEXT,
      payload_json TEXT,
      run_id TEXT,
      created_at TEXT
    );
    """,
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_state_event_symbol_date
         ON cn_epr_state_event(symbol, trade_date);""",
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_state_event_run
         ON cn_epr_state_event(run_id);""",
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_state_event_date_newstate
         ON cn_epr_state_event(trade_date, new_state);""",

    # User execution records
    """
    CREATE TABLE IF NOT EXISTS cn_epr_execution (
      exec_id TEXT PRIMARY KEY,
      trade_date TEXT NOT NULL,
      symbol TEXT NOT NULL,
      action TEXT NOT NULL,
      lots INTEGER NOT NULL,
      price_ref REAL,
      source TEXT,
      note TEXT,
      created_at TEXT
    );
    """,
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_execution_date_symbol
         ON cn_epr_execution(trade_date, symbol);""",

    # Position snapshot
    """
    CREATE TABLE IF NOT EXISTS cn_epr_position_snap (
      trade_date TEXT NOT NULL,
      symbol TEXT NOT NULL,
      holding_lots INTEGER NOT NULL,
      cost_basis REAL,
      market_value REAL,
      source TEXT,
      evidence_json TEXT,
      run_id TEXT,
      created_at TEXT,
      PRIMARY KEY (trade_date, symbol)
    );
    """,
    """CREATE INDEX IF NOT EXISTS idx_cn_epr_position_symbol_date
         ON cn_epr_position_snap(symbol, trade_date);""",
)


def ensure_cn_epr_schema(conn: sqlite3.Connection, ddl: Iterable[str] = DDL) -> None:
    """Create tables/indexes if absent. Raises on any error."""
    cur = conn.cursor()
    try:
        for stmt in ddl:
            cur.execute(stmt)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
