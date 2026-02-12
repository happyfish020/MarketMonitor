# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
import time
from typing import Optional

from core.persistence.contracts.errors import PersistenceError


class SqliteUnitOfWork:
    """L2 Unit-of-Work for atomic publish.

    Owns a single connection and manages a BEGIN IMMEDIATE transaction.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._active = False

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def begin_immediate(self) -> None:
        if self._active:
            raise PersistenceError("Transaction already active")
        try:
            self._conn.execute("BEGIN IMMEDIATE;")
            self._active = True
        except Exception as e:
            raise PersistenceError("Failed to begin transaction", e)

    def commit(self) -> None:
        if not self._active:
            return
        try:
            self._conn.commit()
        except Exception as e:
            raise PersistenceError("Failed to commit transaction", e)
        finally:
            self._active = False

    def rollback(self) -> None:
        if not self._active:
            return
        try:
            self._conn.rollback()
        except Exception:
            # best effort rollback
            pass
        finally:
            self._active = False

    # ----------------------------
    # Audit utilities (schema-aware)
    # ----------------------------

    def _has_column(self, table: str, col: str) -> bool:
        try:
            rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
            for r in rows or []:
                # r: (cid, name, type, notnull, dflt_value, pk)
                if len(r) >= 2 and str(r[1]).lower() == col.lower():
                    return True
        except Exception:
            return False
        return False

    def record_audit(self, trade_date: str, report_kind: str, event: str, note: Optional[str] = None) -> None:
        """Record an audit event inside the current transaction.

        Contract:
        - Never commits (caller controls commit).
        - Schema-aware for created_at_utc vs created_at.
        - For single-version-per-day events, replace old row (Scheme-1 in code).
        """
        if not trade_date or not report_kind or not event:
            return

        # Scheme-1: keep only one version per day for these events
        if event in ("REGIME_SHIFT", "REGIME_STATS"):
            self._conn.execute(
                """DELETE FROM ur_persistence_audit
                     WHERE trade_date=? AND report_kind=? AND event=?;""",
                (trade_date, report_kind, event),
            )

        has_created_at_utc = self._has_column("ur_persistence_audit", "created_at_utc")
        has_created_at = self._has_column("ur_persistence_audit", "created_at")

        if has_created_at_utc:
            self._conn.execute(
                """INSERT INTO ur_persistence_audit
                     (trade_date, report_kind, event, note, created_at_utc)
                     VALUES (?, ?, ?, ?, ?);""",
                (trade_date, report_kind, event, note, int(time.time())),
            )
        elif has_created_at:
            self._conn.execute(
                """INSERT INTO ur_persistence_audit
                     (trade_date, report_kind, event, note, created_at)
                     VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP);""",
                (trade_date, report_kind, event, note),
            )
        else:
            self._conn.execute(
                """INSERT INTO ur_persistence_audit
                     (trade_date, report_kind, event, note)
                     VALUES (?, ?, ?, ?);""",
                (trade_date, report_kind, event, note),
            )
