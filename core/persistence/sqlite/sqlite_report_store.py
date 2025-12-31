# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Optional

from core.persistence.contracts.errors import AlreadyPublishedError, InvalidPayloadError, PersistenceError, TamperedError
from core.persistence.contracts.report_store_contract import ReportStoreContract
from core.persistence.models.report_artifact import ReportArtifact
from core.persistence.sqlite.sqlite_hashing import report_hash


class SqliteReportStore(ReportStoreContract):
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def save_report(
        self,
        trade_date: str,
        report_kind: str,
        content_text: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> str:
        if content_text is None or not str(content_text).strip():
            raise InvalidPayloadError("report", "empty content")

        meta_json = None
        if meta is not None:
            try:
                meta_json = json.dumps(meta, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            except Exception as e:
                raise InvalidPayloadError("report", f"meta_json_encode_failed: {e}")

        h = report_hash(trade_date, report_kind, content_text, meta_json)
        created_at_utc = int(time.time())

        try:
            self._conn.execute(
                """INSERT INTO ur_report_artifact
                   (trade_date, report_kind, content_text, content_hash, meta_json, created_at_utc)
                   VALUES (?, ?, ?, ?, ?, ?);""",
                (trade_date, report_kind, content_text, h, meta_json, created_at_utc),
            )
        except sqlite3.IntegrityError as e:
            raise AlreadyPublishedError(trade_date, report_kind)
        except Exception as e:
            raise PersistenceError("Failed to save report", e)

        return h

    def get_report(self, trade_date: str, report_kind: str) -> Optional[ReportArtifact]:
        row = self._conn.execute(
            """SELECT trade_date, report_kind, content_text, content_hash, meta_json, created_at_utc
               FROM ur_report_artifact WHERE trade_date=? AND report_kind=?;""",
            (trade_date, report_kind),
        ).fetchone()
        if row is None:
            return None
        meta = None
        if row["meta_json"]:
            try:
                meta = json.loads(row["meta_json"])
            except Exception:
                # keep raw invalid json as None; do not crash reads
                meta = None
        return ReportArtifact(
            trade_date=row["trade_date"],
            report_kind=row["report_kind"],
            content_text=row["content_text"],
            content_hash=row["content_hash"],
            meta=meta,
            created_at_utc=int(row["created_at_utc"]),
        )

    def verify_report(self, trade_date: str, report_kind: str) -> bool:
        row = self._conn.execute(
            """SELECT trade_date, report_kind, content_text, content_hash, meta_json
               FROM ur_report_artifact WHERE trade_date=? AND report_kind=?;""",
            (trade_date, report_kind),
        ).fetchone()
        if row is None:
            return False
        expected = report_hash(
            row["trade_date"],
            row["report_kind"],
            row["content_text"],
            row["meta_json"],
        )
        if expected != row["content_hash"]:
            raise TamperedError("report", trade_date, report_kind)
        return True
