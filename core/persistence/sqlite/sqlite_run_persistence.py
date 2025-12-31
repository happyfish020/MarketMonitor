# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Sqlite Run Persistence (L1)

Contract (Frozen):
- L1 allows overwrite-on-rerun for the same (trade_date, report_kind).
- On start_run(), existing runs with the same (trade_date, report_kind)
  MUST be purged atomically from all L1 tables before inserting a new run.
- This applies ONLY to L1 engineering traces.
- L2 institutional artifacts MUST NOT be overwritten.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from core.persistence.contracts.errors import PersistenceError


class SqliteRunPersistence:
    def __init__(self, conn: sqlite3.Connection):
        if conn is None:
            raise PersistenceError("SqliteRunPersistence requires a valid sqlite3.Connection")
        self._conn = conn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_run(
        self,
        trade_date: str,
        report_kind: str,
        engine_version: str,
    ) -> str:
        """
        Start a new run.

        Behavior (Frozen):
        - If an existing run with the same (trade_date, report_kind) exists,
          purge ALL its L1 records first (overwrite-on-rerun).
        - Then insert a new run_meta row and return new run_id.
        """
        try:
            with self._conn:
                # 🔥 핵심逻辑：同日同 kind 覆盖式重跑
                self._purge_runs_by_date_kind(trade_date, report_kind)

                run_id = str(uuid.uuid4())
                self._conn.execute(
                    """
                    INSERT INTO ur_run_meta
                    (run_id, trade_date, report_kind, engine_version, status, created_at)
                    VALUES (?, ?, ?, ?, 'STARTED', ?)
                    """,
                    (
                        run_id,
                        trade_date,
                        report_kind,
                        engine_version,
                        datetime.utcnow().isoformat(timespec="seconds"),
                    ),
                )
                return run_id

        except Exception as e:
            raise PersistenceError("Failed to start_run") from e

    def finish_run(
        self,
        run_id: str,
        status: str,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark a run as finished (SUCCESS / FAILED).
        """
        try:
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE ur_run_meta
                    SET status = ?, error_type = ?, error_message = ?, finished_at = ?
                    WHERE run_id = ?
                    """,
                    (
                        status,
                        error_type,
                        error_message,
                        datetime.utcnow().isoformat(timespec="seconds"),
                        run_id,
                    ),
                )
        except Exception as e:
            raise PersistenceError("Failed to finish_run") from e

    def record_snapshot(
        self,
        run_id: str,
        snapshot_name: str,
        payload: Dict[str, Any],
    ) -> None:
        """
        Record raw snapshot payload (JSON-safe dict).
        """
        try:
            data = json.dumps(payload, ensure_ascii=False)
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO ur_snapshot_raw
                    (run_id, snapshot_name, payload_json, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        snapshot_name,
                        data,
                        datetime.utcnow().isoformat(timespec="seconds"),
                    ),
                )
        except Exception as e:
            raise PersistenceError("Failed to record_snapshot") from e

    def record_factor(
        self,
        run_id: str,
        factor_name: str,
        payload: Dict[str, Any],
        factor_version: Optional[str] = None,
        seq: int = 0,
    ) -> None:
        """
        Record one factor result (JSON-safe dict).
        Multiple factors per run are allowed.
        """
        try:
            data = json.dumps(payload, ensure_ascii=False)
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO ur_factor_result
                    (run_id, factor_name, seq, factor_version, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        factor_name,
                        seq,
                        factor_version,
                        data,
                        datetime.utcnow().isoformat(timespec="seconds"),
                    ),
                )
        except Exception as e:
            raise PersistenceError("Failed to record_factor") from e

    def record_gate(
        self,
        run_id: str,
        gate: str,
        drs: str,
        frf: str,
        action_hint: Optional[str],
        rule_hits: Optional[Dict[str, Any]],
    ) -> None:
        """
        Record gate / governance result for this run.
        """
        try:
            rule_hits_json = (
                json.dumps(rule_hits, ensure_ascii=False)
                if rule_hits is not None
                else None
            )
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO ur_gate_decision
                    (run_id, gate, drs, frf, action_hint, rule_hits_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        gate,
                        drs,
                        frf,
                        action_hint,
                        rule_hits_json,
                        datetime.utcnow().isoformat(timespec="seconds"),
                    ),
                )
        except Exception as e:
            raise PersistenceError("Failed to record_gate") from e

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _purge_runs_by_date_kind(
        self,
        trade_date: str,
        report_kind: str,
    ) -> None:
        """
        Purge ALL L1 records for existing runs with the same
        (trade_date, report_kind).

        Order is important: delete children first, then run_meta.
        """
        rows = self._conn.execute(
            """
            SELECT run_id
            FROM ur_run_meta
            WHERE trade_date = ? AND report_kind = ?
            """,
            (trade_date, report_kind),
        ).fetchall()

        for (run_id,) in rows:
            self._conn.execute(
                "DELETE FROM ur_snapshot_raw WHERE run_id = ?",
                (run_id,),
            )
            self._conn.execute(
                "DELETE FROM ur_factor_result WHERE run_id = ?",
                (run_id,),
            )
            self._conn.execute(
                "DELETE FROM ur_gate_decision WHERE run_id = ?",
                (run_id,),
            )
            self._conn.execute(
                "DELETE FROM ur_run_meta WHERE run_id = ?",
                (run_id,),
            )
