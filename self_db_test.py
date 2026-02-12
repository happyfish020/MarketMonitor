
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - External Validation Self DB Test (Merged & Fixed)

NOTE:
- This file is primarily for SELF-TEST / DRY-RUN.
- trade_date is dynamically set to today's date to mimic PRD behavior.
- verdict is TEMPORARY hard-coded and MUST be replaced by L2-based parsing in PRD.
"""

import sqlite3
from datetime import date
from typing import List, Dict

from core.persistence.sqlite.sqlite_schema import ensure_schema_l2
from core.persistence.sqlite.sqlite_schema_l1 import ensure_schema_l1

DB_PATH = r"data\persistent\unifiedrisk.db"

def get_trade_date() -> str:
    """Return today's trade date (ISO format)."""
    return date.today().isoformat()

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def ensure_ext_schema(conn: sqlite3.Connection):
    ddl = """
    CREATE TABLE IF NOT EXISTS ext_market_snapshot (
        trade_date      TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        close           REAL,
        pct_change      REAL,
        amount          REAL,
        amount_ma20     REAL,
        up_ratio        REAL,
        north_net       REAL,
        created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
        PRIMARY KEY (trade_date, symbol)
    );

    CREATE TABLE IF NOT EXISTS ext_report_claim (
        claim_id    TEXT PRIMARY KEY,
        claim_type  TEXT NOT NULL,
        description TEXT,
        active      INTEGER NOT NULL DEFAULT 1,
        created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
    );

    CREATE TABLE IF NOT EXISTS ext_claim_verdict (
        trade_date TEXT NOT NULL,
        claim_id   TEXT NOT NULL,
        verdict    TEXT NOT NULL,
        created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
        PRIMARY KEY (trade_date, claim_id)
    );

    CREATE TABLE IF NOT EXISTS ext_claim_followup (
        trade_date TEXT NOT NULL,
        claim_id   TEXT NOT NULL,
        horizon    INTEGER NOT NULL,
        ret_cum    REAL,
        created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
        PRIMARY KEY (trade_date, claim_id, horizon)
    );

    CREATE TABLE IF NOT EXISTS ext_validation_summary (
        asof_date        TEXT NOT NULL,
        lookback_days    INTEGER NOT NULL,
        horizon          INTEGER NOT NULL,
        total_cnt        INTEGER NOT NULL,
        supported_cnt    INTEGER NOT NULL,
        weakened_cnt     INTEGER NOT NULL,
        unverifiable_cnt INTEGER NOT NULL,
        supported_rate   REAL,
        avg_ret_all      REAL,
        avg_ret_supported REAL,
        created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
        PRIMARY KEY (asof_date, lookback_days, horizon)
    );
    """
    conn.executescript(ddl)

DEFAULT_CLAIMS = [
    ("IDX_TREND_STABLE", "INDEX", "Index trend stable"),
    ("BREADTH_HEALTHY", "BREADTH", "Market breadth healthy"),
    ("LIQUIDITY_OK", "LIQUIDITY", "Liquidity not drying"),
]

def seed_claims(conn: sqlite3.Connection):
    conn.executemany(
        "INSERT OR IGNORE INTO ext_report_claim (claim_id, claim_type, description) VALUES (?, ?, ?)",
        DEFAULT_CLAIMS,
    )

def seed_market_prices(conn: sqlite3.Connection, symbol: str, prices: Dict[str, float]):
    for d, close in prices.items():
        conn.execute(
            "INSERT OR REPLACE INTO ext_market_snapshot (trade_date, symbol, close) VALUES (?, ?, ?)",
            (d, symbol, close),
        )

def seed_verdicts(conn: sqlite3.Connection, trade_date: str, verdict: str):
    rows = conn.execute("SELECT claim_id FROM ext_report_claim WHERE active=1").fetchall()
    conn.executemany(
        "INSERT OR REPLACE INTO ext_claim_verdict (trade_date, claim_id, verdict) VALUES (?, ?, ?)",
        [(trade_date, r[0], verdict) for r in rows],
    )

def compute_followup(conn: sqlite3.Connection, trade_date: str, symbol: str, horizons: List[int]):
    dates = [r[0] for r in conn.execute(
        "SELECT trade_date FROM ext_market_snapshot WHERE symbol=? ORDER BY trade_date",
        (symbol,)
    ).fetchall()]

    if trade_date not in dates:
        return

    idx = dates.index(trade_date)
    base_close = conn.execute(
        "SELECT close FROM ext_market_snapshot WHERE trade_date=? AND symbol=?",
        (trade_date, symbol),
    ).fetchone()[0]

    for h in horizons:
        if idx + h >= len(dates):
            continue
        d2 = dates[idx + h]
        close2 = conn.execute(
            "SELECT close FROM ext_market_snapshot WHERE trade_date=? AND symbol=?",
            (d2, symbol),
        ).fetchone()[0]
        ret = (close2 / base_close) - 1.0

        for (claim_id,) in conn.execute("SELECT claim_id FROM ext_report_claim WHERE active=1").fetchall():
            conn.execute(
                "INSERT OR REPLACE INTO ext_claim_followup (trade_date, claim_id, horizon, ret_cum) VALUES (?, ?, ?, ?)",
                (trade_date, claim_id, h, ret),
            )

def aggregate_summary(conn: sqlite3.Connection, asof_date: str, lookback_days: int, horizons: List[int]):
    trade_dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT trade_date FROM ext_claim_verdict ORDER BY trade_date DESC LIMIT ?",
        (lookback_days,)
    ).fetchall()]

    for h in horizons:
        rows = conn.execute(
            f"""
            SELECT v.verdict, COUNT(*), AVG(f.ret_cum)
            FROM ext_claim_verdict v
            LEFT JOIN ext_claim_followup f
              ON v.trade_date=f.trade_date AND v.claim_id=f.claim_id AND f.horizon=?
            WHERE v.trade_date IN ({",".join("?"*len(trade_dates))})
            GROUP BY v.verdict
            """,
            (h, *trade_dates),
        ).fetchall()

        total = sum(r[1] for r in rows)
        sup = sum(r[1] for r in rows if r[0] == "SUPPORTED")
        weak = sum(r[1] for r in rows if r[0] == "WEAKENED")
        unv = sum(r[1] for r in rows if r[0] == "UNVERIFIABLE")

        avg_all = sum((r[1] * (r[2] or 0.0)) for r in rows) / total if total else None
        avg_sup = next((r[2] for r in rows if r[0] == "SUPPORTED"), None)
        rate = sup / total if total else None

        conn.execute(
            "INSERT OR REPLACE INTO ext_validation_summary (asof_date,lookback_days,horizon,total_cnt,supported_cnt,weakened_cnt,unverifiable_cnt,avg_ret_supported,supported_rate,avg_ret_all) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (asof_date, lookback_days, h, total, sup, weak, unv, rate, avg_all, avg_sup),
        )

def main():
    conn = connect(DB_PATH)
    ensure_ext_schema(conn)
    ensure_schema_l1(conn)
    ensure_schema_l2(conn)
    
    trade_date = get_trade_date()
    verdict = "SUPPORTED"  # TEMP: replace with L2-based verdict in PRD

    with conn:
        seed_claims(conn)

        # Minimal rolling price path including today for T+N demo
        prices = {
            trade_date: 4000,
        }
        seed_market_prices(conn, "HS300", prices)
        seed_verdicts(conn, trade_date, verdict)
        compute_followup(conn, trade_date, "HS300", [1, 3, 5])
        aggregate_summary(conn, trade_date, 10, [1, 3, 5])

    conn.close()
    print(f"External Validation self-test OK for trade_date={trade_date}")

if __name__ == "__main__":
    main()