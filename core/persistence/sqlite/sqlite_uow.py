# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
import time
from typing import Optional

from core.persistence.contracts.errors import PersistenceError


class SqliteUnitOfWork:
    """L2 Unit-of-Work for S1 atomic publish.

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

    def record_audit(self, trade_date: str, report_kind: str, event: str, note: Optional[str] = None) -> None:
        created_at_utc = int(time.time())
        self._conn.execute(
            """INSERT INTO ur_persistence_audit
               (trade_date, report_kind, event, note, created_at_utc)
               VALUES (?, ?, ?, ?, ?);""",
            (trade_date, report_kind, event, note, created_at_utc),
        )
