# -*- coding: utf-8 -*-
"""
tools/dump_cn_epr_db.py

Purpose:
- Audit / verify writes for a given trade_date in cn_entry_pool_rotation.db
- Read-only utility
Constraints:
- Only accepts --trade-date YYYY-MM-DD (same UX as run scripts)
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _print_rows(title: str, rows):
    print("")
    print(title)
    if not rows:
        print("- (none)")
        return
    for r in rows:
        d = dict(r)
        # compact rendering
        items = []
        for k in d.keys():
            items.append(f"{k}={d[k]}")
        print("- " + ", ".join(items))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    db_path = repo_root / "data" / "cn_entry_pool_rotation.db"
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    td = args.trade_date

    with _connect(db_path) as conn:
        # entry pool
        rows = conn.execute("SELECT * FROM cn_epr_entry_pool ORDER BY symbol;").fetchall()
        _print_rows("[cn_epr_entry_pool]", rows)

        # state snap
        rows = conn.execute(
            "SELECT * FROM cn_epr_state_snap WHERE trade_date=:d ORDER BY symbol;",
            {"d": td},
        ).fetchall()
        _print_rows(f"[cn_epr_state_snap] trade_date={td}", rows)

        # state event
        rows = conn.execute(
            "SELECT * FROM cn_epr_state_event WHERE trade_date=:d ORDER BY symbol, rowid;",
            {"d": td},
        ).fetchall()
        _print_rows(f"[cn_epr_state_event] trade_date={td}", rows)

        # position snap
        rows = conn.execute(
            "SELECT * FROM cn_epr_position_snap WHERE trade_date=:d ORDER BY symbol;",
            {"d": td},
        ).fetchall()
        _print_rows(f"[cn_epr_position_snap] trade_date={td}", rows)

        # execution
        rows = conn.execute(
            "SELECT * FROM cn_epr_execution WHERE trade_date=:d ORDER BY symbol, action;",
            {"d": td},
        ).fetchall()
        _print_rows(f"[cn_epr_execution] trade_date={td}", rows)

    print("\nOK\n")


if __name__ == "__main__":
    main()
