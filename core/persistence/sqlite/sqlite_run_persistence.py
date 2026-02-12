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
from core.persistence.sqlite.sqlite_hashing import sha256_hex


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
                    (run_id, trade_date, report_kind, engine_version, status, started_at_utc)
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
            raise e
            #raise PersistenceError("Failed to start_run") from e


    def finish_run(
        self,
        run_id: str,
        status: str,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Mark a run as finished.
    
        Frozen P0-2 contract:
        - status=COMPLETED is only allowed if L2 artifacts exist for the same (trade_date, report_kind):
          ur_report_artifact, ur_decision_evidence_snapshot, ur_report_des_link.
        - If L2 is missing, the run MUST be downgraded to FAILED with a clear error message.
        """
        now = datetime.utcnow().isoformat(timespec="seconds")
        want = str(status or "").upper()

        def _rows_to_list(rows):
            return [dict(r) for r in rows]

        def _compute_l1_rollup(run_id_: str) -> Dict[str, Any]:
            """Deterministic rollup for L1 tables (snapshot/factor/gate) for audit hashing."""
            # NOTE: use stored JSON strings to avoid float formatting drift.
            snap_rows = _rows_to_list(
                self._conn.execute(
                    """SELECT snapshot_name, COALESCE(seq, 0) AS seq, payload_json
                       FROM ur_snapshot_raw
                       WHERE run_id = ?
                       ORDER BY snapshot_name, COALESCE(seq, 0)""",
                    (run_id_,),
                ).fetchall()
            )
            fac_rows = _rows_to_list(
                self._conn.execute(
                    """SELECT factor_name, COALESCE(seq, 0) AS seq, COALESCE(factor_version, '') AS factor_version, payload_json
                       FROM ur_factor_result
                       WHERE run_id = ?
                       ORDER BY factor_name, COALESCE(seq, 0)""",
                    (run_id_,),
                ).fetchall()
            )
            gate_row = self._conn.execute(
                """SELECT gate, drs, frf, COALESCE(action_hint, '') AS action_hint, COALESCE(rule_hits_json, '') AS rule_hits_json
                   FROM ur_gate_decision
                   WHERE run_id = ?
                   ORDER BY created_at_utc DESC
                   LIMIT 1""",
                (run_id_,),
            ).fetchone()
            gate_d = dict(gate_row) if gate_row else {}

            rollup = {
                "run_id": run_id_,
                "l1": {
                    "snapshot": snap_rows,
                    "factor": fac_rows,
                    "gate": gate_d,
                },
                "counts": {
                    "snapshot": len(snap_rows),
                    "factor": len(fac_rows),
                    "gate": 1 if gate_row else 0,
                },
            }
            rollup["l1_hash"] = sha256_hex(rollup)
            return rollup

        def _record_audit_hash(
            trade_date_: str,
            report_kind_: str,
            run_id_: str,
            engine_version_: str,
        ) -> None:
            """Write an AUDIT_HASH record linking L1+L2 hashes (tamper-evident)."""
            # L2 hashes (must exist for COMPLETED)
            link = self._conn.execute(
                """SELECT report_hash, des_hash
                   FROM ur_report_des_link
                   WHERE trade_date = ? AND report_kind = ?""",
                (trade_date_, report_kind_),
            ).fetchone()
            if not link:
                raise PersistenceError(f"L2 link missing for ({trade_date_}, {report_kind_})")

            l1_rollup = _compute_l1_rollup(run_id_)
            report_hash = str(link[0])
            des_hash = str(link[1])

            # Optional chaining: previous audit hash for same (date,kind)
            prev = self._conn.execute(
                """SELECT note
                   FROM ur_persistence_audit
                   WHERE trade_date = ? AND report_kind = ? AND event = 'AUDIT_HASH'
                   ORDER BY created_at_utc DESC, id DESC
                   LIMIT 1""",
                (trade_date_, report_kind_),
            ).fetchone()
            prev_hash = None
            if prev and prev[0]:
                try:
                    prev_note = json.loads(prev[0]) if isinstance(prev[0], str) else None
                    if isinstance(prev_note, dict):
                        prev_hash = prev_note.get("audit_hash")
                except Exception:
                    prev_hash = None

            audit_payload = {
                "trade_date": trade_date_,
                "report_kind": report_kind_,
                "engine_version": engine_version_,
                "run_id": run_id_,
                "hashes": {
                    "l1_hash": l1_rollup.get("l1_hash"),
                    "report_hash": report_hash,
                    "des_hash": des_hash,
                    "prev_audit_hash": prev_hash,
                },
                "counts": l1_rollup.get("counts"),
            }
            audit_hash = sha256_hex(audit_payload)
            audit_payload["audit_hash"] = audit_hash

            with self._conn:
                self._conn.execute(
                    """INSERT INTO ur_persistence_audit
                       (trade_date, report_kind, event, note, created_at_utc)
                       VALUES (?, ?, 'AUDIT_HASH', ?, ?)""",
                    (
                        trade_date_,
                        report_kind_,
                        json.dumps(audit_payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
                        int(datetime.utcnow().timestamp()),
                    ),
                )
    
        def _cols() -> set:
            return {row[1] for row in self._conn.execute("PRAGMA table_info(ur_run_meta)").fetchall()}
    
        def _update(status2: str, et: Optional[str], em: Optional[str]) -> None:
            cols = _cols()
            sets = ["status = ?", "error_type = ?", "error_message = ?"]
            vals = [status2, et, em]
            if "finished_at_utc" in cols:
                sets.append("finished_at_utc = ?")
                vals.append(now)
            elif "finished_at" in cols:
                sets.append("finished_at = ?")
                vals.append(now)
            sql = f"UPDATE ur_run_meta SET {', '.join(sets)} WHERE run_id = ?"
            vals.append(run_id)
            with self._conn:
                self._conn.execute(sql, tuple(vals))
    
        def _assert_l2_complete(trade_date: str, report_kind: str) -> None:
            # Ensure required tables exist
            tables = {r[0] for r in self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            required = ["ur_report_artifact", "ur_decision_evidence_snapshot", "ur_report_des_link"]
            missing_tables = [t for t in required if t not in tables]
            if missing_tables:
                raise PersistenceError(f"L2 tables missing: {missing_tables}")
    
            def _cnt(table: str) -> int:
                row = self._conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE trade_date = ? AND report_kind = ?",
                    (trade_date, report_kind),
                ).fetchone()
                return int(row[0]) if row and row[0] is not None else 0
    
            c_report = _cnt("ur_report_artifact")
            c_des = _cnt("ur_decision_evidence_snapshot")
            c_link = _cnt("ur_report_des_link")
    
            if c_report < 1 or c_des < 1 or c_link < 1:
                raise PersistenceError(
                    f"L2 incomplete: report={c_report} des={c_des} link={c_link} "
                    f"for ({trade_date}, {report_kind})"
                )
    
        try:
            # P0-2: forbid COMPLETED without L2
            if want == "COMPLETED":
                row = self._conn.execute(
                    "SELECT trade_date, report_kind, engine_version FROM ur_run_meta WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if not row:
                    raise PersistenceError(f"run_id not found: {run_id}")
                trade_date, report_kind, engine_version = str(row[0]), str(row[1]), str(row[2] or "")
                _assert_l2_complete(trade_date, report_kind)

                # P0-3 (Frozen add-on): persist an auditable hash that ties L1 rollup to L2 artifacts.
                # This does NOT affect governance; it only improves replay/audit integrity.
                _record_audit_hash(trade_date, report_kind, run_id, engine_version)
    
            _update(status, error_type, error_message)
    
        except Exception as e:
            # Best-effort downgrade to FAILED (never silent)
            et = type(e).__name__
            em = f"P0-2:L2_REQUIRED :: {e}"
            try:
                _update("FAILED", et, em)
            except Exception:
                pass
            # Raise for caller visibility
            if isinstance(e, PersistenceError):
                raise
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
                    (run_id, snapshot_name, payload_json, created_at_utc)
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
            raise e
            #raise PersistenceError("Failed to record_snapshot") from e

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
                    (run_id, factor_name, seq, factor_version, payload_json, created_at_utc)
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
            raise e
            #raise PersistenceError("Failed to record_factor") from e

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
                    (run_id, gate, drs, frf, action_hint, rule_hits_json, created_at_utc)
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
            raise e 
            #raise PersistenceError("Failed to record_gate") from e

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
