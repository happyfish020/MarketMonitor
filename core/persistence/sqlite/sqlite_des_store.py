# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Optional

from core.persistence.contracts.des_store_contract import DecisionEvidenceStoreContract
from core.persistence.contracts.errors import AlreadyPublishedError, InvalidPayloadError, PersistenceError, TamperedError
from core.persistence.models.decision_evidence_snapshot import DecisionEvidenceSnapshot
from core.persistence.sqlite.sqlite_hashing import des_hash


_TOP_KEYS = ("context", "factors", "structure", "governance", "rule_trace")


class SqliteDecisionEvidenceStore(DecisionEvidenceStoreContract):
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def save_des(
        self,
        trade_date: str,
        report_kind: str,
        engine_version: str,
        des_payload: Dict[str, Any],
    ) -> str:
        if not isinstance(des_payload, dict):
            raise InvalidPayloadError("des", "payload_not_dict")

        missing = [k for k in _TOP_KEYS if k not in des_payload]
        if missing:
            raise InvalidPayloadError("des", f"missing_top_keys:{','.join(missing)}")

        try:
            payload_json = json.dumps(des_payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        #except Exception as e:
        #    raise InvalidPayloadError("des", f"payload_json_encode_failed:{e}")
        except sqlite3.IntegrityError as e:
             raise AlreadyPublishedError(trade_date, report_kind) from e

        h = des_hash(trade_date, report_kind, engine_version, payload_json)
        created_at_utc = int(time.time())

        try:
            self._conn.execute(
                """INSERT INTO ur_decision_evidence_snapshot
                   (trade_date, report_kind, engine_version, des_payload_json, des_hash, created_at_utc)
                   VALUES (?, ?, ?, ?, ?, ?);""",
                (trade_date, report_kind, engine_version, payload_json, h, created_at_utc),
            )
        except sqlite3.IntegrityError as e:
            raise AlreadyPublishedError(trade_date, report_kind)
        except Exception as e:
            raise PersistenceError("Failed to save DES", e)

        return h

    def get_des(self, trade_date: str, report_kind: str) -> Optional[DecisionEvidenceSnapshot]:
        row = self._conn.execute(
            """SELECT trade_date, report_kind, engine_version, des_payload_json, des_hash, created_at_utc
               FROM ur_decision_evidence_snapshot WHERE trade_date=? AND report_kind=?;""",
            (trade_date, report_kind),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["des_payload_json"])
        except Exception as e:
            raise PersistenceError("DES payload json decode failed", e)

        return DecisionEvidenceSnapshot(
            trade_date=row["trade_date"],
            report_kind=row["report_kind"],
            engine_version=row["engine_version"],
            des_payload=payload,
            des_hash=row["des_hash"],
            created_at_utc=int(row["created_at_utc"]),
        )

    def verify_des(self, trade_date: str, report_kind: str) -> bool:
        row = self._conn.execute(
            """SELECT trade_date, report_kind, engine_version, des_payload_json, des_hash
               FROM ur_decision_evidence_snapshot WHERE trade_date=? AND report_kind=?;""",
            (trade_date, report_kind),
        ).fetchone()
        if row is None:
            return False
        expected = des_hash(
            row["trade_date"],
            row["report_kind"],
            row["engine_version"],
            row["des_payload_json"],
        )
        if expected != row["des_hash"]:
            raise TamperedError("des", trade_date, report_kind)
        return True
