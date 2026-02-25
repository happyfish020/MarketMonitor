# -*- coding: utf-8 -*-
"""
tools/scan_cn_epr_eod_breakouts.py

Batch run EOD engine over fixed date range (2025-01-01 .. 2026-02-13),
detect READY -> TRIGGERED transitions, and append findings to a log file.

Why results differed from previous version:
- Previous scanner matched substring in CLI output.
- Newer version tried to read cn_epr_state_event(from_state/to_state), but legacy DB may contain
  old rows with NULLs (due to older schema), so query returned no hits.

This version is consistent and schema-agnostic:
- Detect "newly TRIGGERED" by comparing today's cn_epr_state_snap.state == 'TRIGGERED'
  vs the latest prior state before today for the same symbol.
- This does NOT depend on state_event completeness, and stays stable across CLI formatting.
- HIT line includes symbol codes.

Output:
- Writes to data/cn_epr_breakout_scan.log
"""

from __future__ import annotations

import argparse
import sys
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path


# Ensure repo root on sys.path
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.personal_tactical.cn_entry_pool_rotation.engine import build_engine  # noqa: E402


START_DATE = date(2025, 1, 1)
END_DATE = date(2026, 2, 13)

DB_REL_PATH = Path("data") / "cn_entry_pool_rotation.db"
LOG_REL_PATH = Path("data") / "cn_epr_breakout_scan.log"


def daterange(d0: date, d1: date):
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def append_log(line: str) -> None:
    p = REPO_ROOT / LOG_REL_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def _connect_db() -> sqlite3.Connection:
    db_path = REPO_ROOT / DB_REL_PATH
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _get_newly_triggered_symbols(trade_date: str) -> list[str]:
    """
    Schema-agnostic hit detection:
    - today snap: state='TRIGGERED'
    - prior snap (latest before today): state != 'TRIGGERED' or missing
    """
    with _connect_db() as conn:
        today = conn.execute(
            """
            SELECT symbol
            FROM cn_epr_state_snap
            WHERE trade_date = :d
              AND state = 'TRIGGERED'
            """,
            {"d": trade_date},
        ).fetchall()
        today_syms = [r["symbol"] for r in today if r["symbol"]]

        newly = []
        for sym in today_syms:
            prior = conn.execute(
                """
                SELECT state
                FROM cn_epr_state_snap
                WHERE symbol = :s AND trade_date < :d
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                {"s": sym, "d": trade_date},
            ).fetchone()
            prior_state = prior["state"] if prior and "state" in prior.keys() else None
            if prior_state != "TRIGGERED":
                newly.append(sym)

        return sorted(set(newly))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write log file; only print findings to stdout.",
    )
    args = ap.parse_args()

    engine = build_engine()

    scan_started = datetime.now().isoformat(timespec="seconds")
    header = f"[SCAN_START] {scan_started} range={START_DATE.isoformat()}..{END_DATE.isoformat()}"
    if args.dry_run:
        print(header)
    else:
        append_log(header)

    hit_count = 0
    skip_count = 0

    for d in daterange(START_DATE, END_DATE):
        td = d.isoformat()
        try:
            engine.run_eod(td)
        except Exception as e:
            skip_count += 1
            msg = f"[SKIP] {td} {type(e).__name__}: {e}"
            if args.dry_run:
                print(msg)
            else:
                append_log(msg)
            continue

        syms = _get_newly_triggered_symbols(td)
        if syms:
            hit_count += 1
            ts = datetime.now().isoformat(timespec="seconds")
            msg = f"[HIT] {ts} trade_date={td} symbols={','.join(syms)}"
            if args.dry_run:
                print(msg)
            else:
                append_log(msg)

    tail = f"[SCAN_END] {datetime.now().isoformat(timespec='seconds')} hits={hit_count} skips={skip_count}"
    if args.dry_run:
        print(tail)
    else:
        append_log(tail)


if __name__ == "__main__":
    main()
