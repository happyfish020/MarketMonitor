# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Regime Shift Auditor · v1.2

Writes REGIME_SHIFT events into SQLite L2 table ur_persistence_audit.

Fix:
- Your ur_persistence_audit schema uses column 'created_at' (as seen from query output),
  not 'created_at_utc'. Previous versions attempted to insert into created_at_utc and failed silently.
- This version auto-detects which created_at column exists and inserts accordingly.

Frozen rules:
- Audit is read-only; does not affect Gate/DRS/Execution.
- Best-effort; never raises to break engine.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
import time
import sqlite3


def _stage_key(stage_text: str) -> str:
    if not isinstance(stage_text, str):
        return "NA"
    s = stage_text.strip()
    if "（S" in s and "）" in s:
        try:
            inner = s.split("（", 1)[1].split("）", 1)[0]
            if inner.startswith("S"):
                return inner
        except Exception:
            pass
    return s[:32] if s else "NA"


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        for r in rows or []:
            # r: (cid, name, type, notnull, dflt_value, pk)
            if len(r) >= 2 and str(r[1]).lower() == col.lower():
                return True
    except Exception:
        return False
    return False


@dataclass
class RegimeShiftAuditor:
    conn: sqlite3.Connection

    def log_shift(self, trade_date: str, report_kind: str, regime_history: Optional[List[Dict[str, Any]]]) -> None:
        try:
            if not trade_date or not report_kind:
                return
            if not isinstance(regime_history, list) or len(regime_history) < 2:
                return

            last = regime_history[-1]
            prev = regime_history[-2]
            last_stage = _stage_key(str(last.get("stage", "")))
            prev_stage = _stage_key(str(prev.get("stage", "")))

            if last_stage == "NA" or prev_stage == "NA":
                return
            if last_stage == prev_stage:
                return

            # Dedupe per (trade_date, report_kind, event)
            cur = self.conn.execute(
                """SELECT 1 FROM ur_persistence_audit
                   WHERE trade_date=? AND report_kind=? AND event=?
                   ORDER BY id DESC LIMIT 1""",
                (trade_date, report_kind, "REGIME_SHIFT"),
            ).fetchone()
            if cur:
                return

            note_obj = {
                "from": prev_stage,
                "to": last_stage,
                "prev": {"trade_date": prev.get("trade_date"), "drs": prev.get("drs"), "trend": prev.get("trend")},
                "curr": {"trade_date": last.get("trade_date"), "drs": last.get("drs"), "trend": last.get("trend")},
            }
            note = json.dumps(note_obj, ensure_ascii=False)

            # Schema-aware insert
            has_created_at_utc = _has_column(self.conn, "ur_persistence_audit", "created_at_utc")
            has_created_at = _has_column(self.conn, "ur_persistence_audit", "created_at")

            if has_created_at_utc:
                self.conn.execute(
                    """INSERT INTO ur_persistence_audit(trade_date, report_kind, event, note, created_at_utc)
                       VALUES(?,?,?,?,?)""",
                    (trade_date, report_kind, "REGIME_SHIFT", note, int(time.time())),
                )
            elif has_created_at:
                # created_at is typically TEXT/TS; use sqlite CURRENT_TIMESTAMP
                self.conn.execute(
                    """INSERT INTO ur_persistence_audit(trade_date, report_kind, event, note, created_at)
                       VALUES(?,?,?,?,CURRENT_TIMESTAMP)""",
                    (trade_date, report_kind, "REGIME_SHIFT", note),
                )
            else:
                # minimal columns
                self.conn.execute(
                    """INSERT INTO ur_persistence_audit(trade_date, report_kind, event, note)
                       VALUES(?,?,?,?)""",
                    (trade_date, report_kind, "REGIME_SHIFT", note),
                )

            self.conn.commit()
        except Exception:
            try:
                self.conn.rollback()
            except Exception:
                pass
            return
