# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.persistence.run_store import RunPayload, RunStore

_DEFAULT_SCHEMA_VERSION = "URV12-SQLITE-L1L2"


class SqliteRunStore(RunStore):
    """SQLite adapter for UnifiedRisk V12 persistence (L1 + L2).

    Reads:
    - L1 tables (run_meta, snapshot_raw, factor_result, gate_decision)
    - L2 tables (report_artifact, decision_evidence_snapshot) when available

    Notes:
    - SQLite is typeless; we tolerate TEXT/INTEGER drift in *_utc columns.
    - Table/column drift across iterations is handled via schema introspection.
    """

    def __init__(self, conn: sqlite3.Connection, *, schema_version: str = _DEFAULT_SCHEMA_VERSION):
        self._conn = conn
        self._schema_version = schema_version
        self._cols_cache: Dict[str, Tuple[str, ...]] = {}
        self._require_tables(
            required=("ur_run_meta", "ur_snapshot_raw", "ur_factor_result", "ur_gate_decision"),
            optional=("ur_report_artifact", "ur_decision_evidence_snapshot"),
        )

    # ---------- schema helpers ----------

    def _require_tables(self, *, required: Sequence[str], optional: Sequence[str]) -> None:
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        have = {r[0] if not isinstance(r, sqlite3.Row) else r["name"] for r in rows}

        missing = [t for t in required if t not in have]
        if missing:
            raise RuntimeError(f"missing required tables in db: {missing}")

        # optional are allowed to be missing

    def _cols(self, table: str) -> Tuple[str, ...]:
        cached = self._cols_cache.get(table)
        if cached is not None:
            return cached
        rows = self._conn.execute(f"PRAGMA table_info({table});").fetchall()
        cols = tuple((r[1] if not isinstance(r, sqlite3.Row) else r["name"]) for r in rows)
        self._cols_cache[table] = cols
        return cols

    def _has_col(self, table: str, col: str) -> bool:
        return col in self._cols(table)

    def _safe_json(self, s: Any, *, where: str) -> Any:
        if s is None:
            return None
        if isinstance(s, (dict, list)):
            return s
        if not isinstance(s, str):
            raise TypeError(f"{where}: expected json str, got {type(s)}")
        try:
            return json.loads(s)
        except Exception as e:
            raise ValueError(f"{where}: invalid json ({e})") from e

    # ---------- public API ----------

    def find_runs(self, trade_date=None, kind=None, limit: int = 50):
        cols = {r[1] for r in self._conn.execute("PRAGMA table_info(ur_run_meta)").fetchall()}
    
        select_cols = [
            "run_id", "trade_date", "report_kind",
            "engine_version",
            "status", "started_at_utc", 
        ]
        # optional columns (existence-checked)
        if "finished_at_utc" in cols:
            select_cols.append("finished_at_utc")
        elif "finished_at" in cols:
            select_cols.append("finished_at")
    
        if "error_type" in cols:
            select_cols.append("error_type")
        if "error_message" in cols:
            select_cols.append("error_message")
    
        where = []
        args = []
        if trade_date:
            where.append("trade_date = ?")
            args.append(trade_date)
        if kind:
            where.append("report_kind = ?")
            args.append(kind)
    
        sql = f"SELECT {', '.join(select_cols)} FROM ur_run_meta"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY started_at_utc DESC LIMIT ?"
        args.append(int(limit))
    
        rows = self._conn.execute(sql, tuple(args)).fetchall()
        return [dict(zip(select_cols, row)) for row in rows]
    

    def load_run(self, run_id: str) -> RunPayload:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be non-empty str")
        run_id = run_id.strip()

        rm = self._conn.execute(
            "SELECT * FROM ur_run_meta WHERE run_id=?;",
            (run_id,),
        ).fetchone()
        if rm is None:
            raise KeyError(f"run_id not found: {run_id}")

        rm_d = dict(rm)
        trade_date = str(rm_d.get("trade_date") or "")
        kind = str(rm_d.get("report_kind") or "")
        engine_version = rm_d.get("engine_version")

        snapshot_raw = self._load_snapshots(run_id)
        factor_result = self._load_factors(run_id)
        gate_decision = self._load_gate(run_id)

        # L2 artifacts (optional)
        report_text, report_meta = self._load_report_artifact(trade_date, kind)
        des_payload, des_engine_version = self._load_des_snapshot(trade_date, kind)

        if not engine_version and des_engine_version:
            engine_version = des_engine_version

        slots_final: Optional[Dict[str, Any]] = des_payload if isinstance(des_payload, dict) else None

        report_dump: Optional[Dict[str, Any]] = None
        if isinstance(des_payload, dict) or isinstance(report_text, str):
            report_dump = {}
            if isinstance(des_payload, dict):
                report_dump["des_payload"] = des_payload
            if isinstance(report_text, str) and report_text:
                report_dump["rendered"] = report_text
            if isinstance(report_meta, dict) and report_meta:
                report_dump["report_meta"] = report_meta

        return RunPayload(
            run_id=run_id,
            trade_date=trade_date,
            kind=kind,
            schema_version=self._schema_version,
            engine_version=str(engine_version) if engine_version is not None else None,
            snapshot_raw=snapshot_raw,
            factor_result=factor_result,
            gate_decision=gate_decision,
            slots_final=slots_final,
            report_dump=report_dump,
        )

    # ---------- loaders ----------

    def _load_snapshots(self, run_id: str) -> Dict[str, Any]:
        cols = self._cols("ur_snapshot_raw")
        has_seq = "seq" in cols
        sql = "SELECT snapshot_name, payload_json" + (", seq" if has_seq else "") + " FROM ur_snapshot_raw WHERE run_id=?"
        if has_seq:
            sql += " ORDER BY snapshot_name, seq"
        else:
            sql += " ORDER BY snapshot_name"
        rows = self._conn.execute(sql + ";", (run_id,)).fetchall()

        grouped: Dict[str, List[Tuple[int, Any]]] = {}
        for r in rows:
            d = dict(r)
            name = str(d.get("snapshot_name") or "")
            if not name:
                continue
            seq = int(d.get("seq") or 0) if has_seq else 0
            payload = self._safe_json(d.get("payload_json"), where=f"snapshot:{name}")
            grouped.setdefault(name, []).append((seq, payload))

        out: Dict[str, Any] = {}
        for name, items in grouped.items():
            items_sorted = [p for _, p in sorted(items, key=lambda x: x[0])]
            out[name] = items_sorted[0] if len(items_sorted) == 1 else items_sorted

        return out

    def _load_factors(self, run_id: str) -> Dict[str, Any]:
        cols = self._cols("ur_factor_result")
        has_seq = "seq" in cols
        has_factor_version = "factor_version" in cols

        sel = ["factor_name", "payload_json"]
        if has_seq:
            sel.append("seq")
        if has_factor_version:
            sel.append("factor_version")

        sql = f"SELECT {', '.join(sel)} FROM ur_factor_result WHERE run_id=?"
        if has_seq:
            sql += " ORDER BY factor_name, seq"
        else:
            sql += " ORDER BY factor_name"
        rows = self._conn.execute(sql + ";", (run_id,)).fetchall()

        grouped: Dict[str, List[Tuple[int, Any]]] = {}
        for r in rows:
            d = dict(r)
            name = str(d.get("factor_name") or "")
            if not name:
                continue
            seq = int(d.get("seq") or 0) if has_seq else 0
            payload = self._safe_json(d.get("payload_json"), where=f"factor:{name}")
            grouped.setdefault(name, []).append((seq, payload))

        out: Dict[str, Any] = {}
        for name, items in grouped.items():
            items_sorted = [p for _, p in sorted(items, key=lambda x: x[0])]
            out[name] = items_sorted[0] if len(items_sorted) == 1 else items_sorted
        return out

    def _load_gate(self, run_id: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT * FROM ur_gate_decision WHERE run_id=?;",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        # parse rule_hits_json if present
        if "rule_hits_json" in d:
            d["rule_hits"] = self._safe_json(d.get("rule_hits_json"), where="gate:rule_hits_json")
        return d

    def _load_report_artifact(self, trade_date: str, kind: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        # optional table
        rows = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        have = {r[0] if not isinstance(r, sqlite3.Row) else r["name"] for r in rows}
        if "ur_report_artifact" not in have:
            return None, None

        cols = self._cols("ur_report_artifact")
        if "content_text" not in cols:
            return None, None

        row = self._conn.execute(
            "SELECT * FROM ur_report_artifact WHERE trade_date=? AND report_kind=?;",
            (trade_date, kind),
        ).fetchone()
        if row is None:
            return None, None
        d = dict(row)
        meta = None
        if "meta_json" in d and d.get("meta_json"):
            meta = self._safe_json(d.get("meta_json"), where="report:meta_json")
            if not isinstance(meta, dict):
                meta = None
        return str(d.get("content_text") or ""), meta

    def _load_des_snapshot(self, trade_date: str, kind: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        # optional table
        cols_master = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        have = {r[0] if not isinstance(r, sqlite3.Row) else r["name"] for r in cols_master}
        if "ur_decision_evidence_snapshot" not in have:
            return None, None

        row = self._conn.execute(
            "SELECT * FROM ur_decision_evidence_snapshot WHERE trade_date=? AND report_kind=?;",
            (trade_date, kind),
        ).fetchone()
        if row is None:
            return None, None
        d = dict(row)
        payload = self._safe_json(d.get("des_payload_json"), where="des:des_payload_json")
        if not isinstance(payload, dict):
            raise TypeError("des_payload_json must decode to dict")
        engine_version = d.get("engine_version")
        return payload, (str(engine_version) if engine_version is not None else None)
