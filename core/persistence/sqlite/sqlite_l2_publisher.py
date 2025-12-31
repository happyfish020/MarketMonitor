# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Optional, Tuple

from core.persistence.contracts.errors import AlreadyPublishedError, PersistenceError
from core.persistence.sqlite.sqlite_report_store import SqliteReportStore
from core.persistence.sqlite.sqlite_des_store import SqliteDecisionEvidenceStore
from core.persistence.sqlite.sqlite_uow import SqliteUnitOfWork


class SqliteL2Publisher:
    """Atomic L2 publisher (S1): report + DES + link + audit in one transaction."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._report_store = SqliteReportStore(conn)
        self._des_store = SqliteDecisionEvidenceStore(conn)

    def publish(
        self,
        trade_date: str,
        report_kind: str,
        report_text: str,
        des_payload: Dict[str, Any],
        engine_version: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, str]:
        uow = SqliteUnitOfWork(self._conn)
        try:
            uow.begin_immediate()

            report_hash = self._report_store.save_report(
                trade_date=trade_date,
                report_kind=report_kind,
                content_text=report_text,
                meta=meta,
            )
            des_hash = self._des_store.save_des(
                trade_date=trade_date,
                report_kind=report_kind,
                engine_version=engine_version,
                des_payload=des_payload,
            )

            created_at_utc = int(time.time())
            self._conn.execute(
                """INSERT INTO ur_report_des_link
                   (trade_date, report_kind, report_hash, des_hash, created_at_utc)
                   VALUES (?, ?, ?, ?, ?);""",
                (trade_date, report_kind, report_hash, des_hash, created_at_utc),
            )

            uow.record_audit(trade_date, report_kind, "CREATED", None)
            uow.commit()
            return report_hash, des_hash

        except AlreadyPublishedError as e:
            try:
                uow.rollback()
                # best-effort audit outside the transaction
                self._conn.execute(
                    """INSERT INTO ur_persistence_audit
                       (trade_date, report_kind, event, note, created_at_utc)
                       VALUES (?, ?, 'FAILED', ?, ?);""",
                    (trade_date, report_kind, str(e), int(time.time())),
                )
                self._conn.commit()
            except Exception:
                pass
            raise

        except Exception as e:
            try:
                uow.rollback()
                self._conn.execute(
                    """INSERT INTO ur_persistence_audit
                       (trade_date, report_kind, event, note, created_at_utc)
                       VALUES (?, ?, 'FAILED', ?, ?);""",
                    (trade_date, report_kind, repr(e), int(time.time())),
                )
                self._conn.commit()
            except Exception:
                pass
            if isinstance(e, PersistenceError):
                raise
            raise PersistenceError("L2 publish failed", e)
