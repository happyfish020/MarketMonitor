# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import sqlite3
from pathlib import Path

from core.persistence.sqlite.sqlite_connection import connect_sqlite
from core.persistence.sqlite.sqlite_schema import ensure_schema_l2
from core.persistence.sqlite.sqlite_schema_l1 import ensure_schema_l1
from core.persistence.sqlite.sqlite_l2_publisher import SqliteL2Publisher
from core.persistence.sqlite.sqlite_report_store import SqliteReportStore
from core.persistence.sqlite.sqlite_des_store import SqliteDecisionEvidenceStore
from core.persistence.sqlite.sqlite_run_persistence import SqliteRunPersistence
from core.persistence.contracts.errors import AlreadyPublishedError
from core.validation.external_validation_aggregator import ExternalValidationAggregator, AggregationConfig


# ------------------------------------------------------------
# H2: Default Claims (v1, frozen for selftest)
# ------------------------------------------------------------

DEFAULT_CLAIMS = [
    {
        "claim_id": "IDX_TREND_STABLE",
        "claim_type": "INDEX_TREND",
        "description": "指数趋势结构仍然成立",
        "target_symbol": "HS300",
        "expected_regime": "UP",
    },
    {
        "claim_id": "BREADTH_HEALTHY",
        "claim_type": "BREADTH",
        "description": "市场广度处于健康区间",
        "target_symbol": None,
        "expected_regime": "UP",
    },
    {
        "claim_id": "LIQUIDITY_OK",
        "claim_type": "LIQUIDITY",
        "description": "市场流动性未显著收缩",
        "target_symbol": None,
        "expected_regime": "RANGE",
    },
]


def main():
    db_path = Path("./data/persistent/unifiedrisk.db")
    conn = connect_sqlite(str(db_path))
    
    ensure_ext_schema(conn)
    seed_claims(conn)

    seed_minimal_external_prices(conn)
    seed_minimal_verdicts(conn, "2025-12-30")

    run_validation_aggregation(conn)

    print("H3-B aggregation OK")

    #trade_date = '20251229'
    #validate_claims(conn, trade_date)


def main_p1():
    db_path = Path("./data/persistent/unifiedrisk.db")
    if db_path.exists():
        db_path.unlink()

    conn = connect_sqlite(str(db_path))
    ensure_schema_l2(conn)
    ensure_schema_l1(conn)

    # --- L2 publish happy path ---
    publisher = SqliteL2Publisher(conn)
    trade_date = "2025-12-27"
    kind = "EOD"
    report_text = "# Demo Report\n\nHello."
    des_payload = {
        "context": {"trade_date": trade_date, "report_kind": kind, "engine_version": "v12.demo"},
        "factors": {"breadth": {"score": 50, "state": "NEUTRAL", "data_status": "OK"}},
        "structure": {"trend_in_force": "IN_FORCE"},
        "governance": {"DRS": "N", "FRF": "NORMAL", "GATE": "NORMAL"},
        "rule_trace": [{"rule_id": "GX-DEMO", "role": "note", "result": True}],
    }
    rh, dh = publisher.publish(
        trade_date=trade_date,
        report_kind=kind,
        report_text=report_text,
        des_payload=des_payload,
        engine_version="v12.demo",
        meta={"engine_version": "v12.demo", "run_id": "demo-run"},
    )
    print("L2 publish OK", rh[:8], dh[:8])

    # verify
    rs = SqliteReportStore(conn)
    ds = SqliteDecisionEvidenceStore(conn)
    assert rs.verify_report(trade_date, kind) is True
    assert ds.verify_des(trade_date, kind) is True
    print("L2 verify OK")

    # duplicate publish should fail
    try:
        publisher.publish(
            trade_date=trade_date,
            report_kind=kind,
            report_text=report_text,
            des_payload=des_payload,
            engine_version="v12.demo",
            meta={"engine_version": "v12.demo"},
        )
        raise AssertionError("Expected AlreadyPublishedError")
    except AlreadyPublishedError:
        print("L2 duplicate publish rejected OK")

    # --- L1 run persistence ---
    rp = SqliteRunPersistence(conn)
    run_id = rp.start_run(trade_date=trade_date, report_kind=kind, engine_version="v12.demo")
    rp.record_snapshot(run_id, "internal_snapshot", {"x": 1})
    rp.record_factor(run_id, "breadth", {"score": 50, "details": {"a": 1}})
    rp.record_gate_init(run_id, gate="NORMAL", drs="N", frf="NORMAL", action_hint=None, rule_hits={"GX-DEMO": True})
    rp.finish_run(run_id, status="SUCCESS")
    print("L1 run persist OK", run_id)

    conn.close()
    print("SELFTEST DONE")


def run_validation_aggregation(conn):
    agg = ExternalValidationAggregator(conn, AggregationConfig(lookback_days=20, horizons=(1, 3, 5)))
    agg.run(asof_date="2026-01-04")




def seed_minimal_external_prices(conn):
    # 6 days so T+5 exists
    days = [
        ("2025-12-29", 100.0),
        ("2025-12-30", 101.0),
        ("2025-12-31", 102.0),
        ("2026-01-02", 101.5),
        ("2026-01-03", 103.0),
        ("2026-01-04", 104.0),
    ]
    for d, close in days:
        conn.execute(
            """
            INSERT OR REPLACE INTO ext_market_snapshot
            (trade_date, symbol, close, pct_change, amount, amount_ma20, up_ratio, north_net)
            VALUES (?, 'HS300', ?, NULL, NULL, NULL, NULL, NULL)
            """,
            (d, float(close)),
        )


def seed_minimal_verdicts(conn, trade_date: str):
    # 硬塞 3 条 verdict，测试用
    rows = conn.execute(
        "SELECT claim_id FROM ext_report_claim WHERE active=1"
    ).fetchall()
    for (claim_id,) in rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO ext_claim_verdict
            (trade_date, claim_id, verdict, evidence)
            VALUES (?, ?, 'SUPPORTED', '{}')
            """,
            (trade_date, claim_id),
        )


def ensure_ext_schema(conn):
    conn.executescript("""
    
    CREATE TABLE IF NOT EXISTS ext_validation_by_claim_type (
        asof_date         TEXT NOT NULL,
        lookback_days     INTEGER NOT NULL,
        horizon           INTEGER NOT NULL,
        claim_type        TEXT NOT NULL,
        total_cnt         INTEGER NOT NULL,
        supported_cnt     INTEGER NOT NULL,
        weakened_cnt      INTEGER NOT NULL,
        unverifiable_cnt  INTEGER NOT NULL,
        supported_rate    REAL,
        avg_ret_all       REAL,
        avg_ret_supported REAL,
        created_at        TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (asof_date, lookback_days, horizon, claim_type)
      );
      
    
    CREATE TABLE IF NOT EXISTS ext_claim_followup (
      trade_date    TEXT NOT NULL,
      claim_id      TEXT NOT NULL,
      horizon       INTEGER NOT NULL,
      target_symbol TEXT NOT NULL,
      ret_cum       REAL,
      created_at    TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (trade_date, claim_id, horizon)
    );

    CREATE TABLE IF NOT EXISTS ext_validation_summary (
      asof_date         TEXT NOT NULL,
      lookback_days     INTEGER NOT NULL,
      horizon           INTEGER NOT NULL,
      total_cnt         INTEGER NOT NULL,
      supported_cnt     INTEGER NOT NULL,
      weakened_cnt      INTEGER NOT NULL,
      unverifiable_cnt  INTEGER NOT NULL,
      supported_rate    REAL,
      avg_ret_all       REAL,
      avg_ret_supported REAL,
      created_at        TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (asof_date, lookback_days, horizon)
    );                       

    CREATE TABLE IF NOT EXISTS ext_report_claim (
      claim_id        TEXT PRIMARY KEY,
      claim_type      TEXT NOT NULL,
      description     TEXT NOT NULL,
      target_symbol   TEXT,
      expected_regime TEXT NOT NULL,
      active          INTEGER NOT NULL DEFAULT 1,
      created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS ext_claim_verdict (
      trade_date    TEXT NOT NULL,
      claim_id      TEXT NOT NULL,
      verdict       TEXT NOT NULL,
      evidence      TEXT,
      created_at    TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (trade_date, claim_id)
    );
    """)

def seed_claims(conn):
    for c in DEFAULT_CLAIMS:
        conn.execute(
            """
            INSERT OR IGNORE INTO ext_report_claim
            (claim_id, claim_type, description, target_symbol, expected_regime)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                c["claim_id"],
                c["claim_type"],
                c["description"],
                c["target_symbol"],
                c["expected_regime"],
            ),
        )

import json

def validate_claims(conn, trade_date: str):
    claims = conn.execute(
        "SELECT claim_id, claim_type, target_symbol, expected_regime "
        "FROM ext_report_claim WHERE active=1"
    ).fetchall()

    for claim_id, ctype, symbol, expected in claims:
        row = None
        if symbol:
            row = conn.execute(
                "SELECT pct_change, amount, amount_ma20, up_ratio "
                "FROM ext_market_snapshot WHERE trade_date=? AND symbol=?",
                (trade_date, symbol),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT AVG(up_ratio), SUM(amount), SUM(amount_ma20) "
                "FROM ext_market_snapshot WHERE trade_date=?",
                (trade_date,),
            ).fetchone()

        if row is None or all(v is None for v in row):
            verdict = "UNVERIFIABLE"
            evidence = {}
        else:
            # 极简规则
            actual = "RANGE"
            pct, amount, ma20, up_ratio = (row + (None, None, None, None))[:4]
            if ctype == "INDEX_TREND" and pct is not None:
                actual = "UP" if pct > 0 else "DOWN"
            elif ctype == "BREADTH" and up_ratio is not None:
                actual = "UP" if up_ratio >= 0.5 else "DOWN"
            elif ctype == "LIQUIDITY" and amount is not None and ma20 is not None:
                actual = "RANGE" if amount >= ma20 else "DOWN"

            verdict = "SUPPORTED" if actual == expected else "WEAKENED"
            evidence = {"actual": actual}

        conn.execute(
            """
            INSERT OR REPLACE INTO ext_claim_verdict
            (trade_date, claim_id, verdict, evidence)
            VALUES (?, ?, ?, ?)
            """,
            (trade_date, claim_id, verdict, json.dumps(evidence)),
        )


from core.validation.external_validation_aggregator import AggregationConfig
from core.validation.external_validation_by_claim_type import ExternalValidationByClaimType

def run_b1_claim_type_aggregation(conn):
    agg = ExternalValidationByClaimType(conn)
    out = agg.run(
        asof_date="2026-01-04",
        lookback_days=20,
        horizons=[1, 3, 5],
        persist=True,
    )
    run_b1_claim_type_aggregation(conn)

    print("B1 aggregation OK:", out)

if __name__ == "__main__":
    #main_p1()
    main()