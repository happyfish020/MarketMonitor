#-*- coding: utf-8 -*-
"""
sqlite_store.py (CN_ENTRY_POOL_ROTATION_V1)

This store owns ALL SQLite I/O.

Legacy compatibility:
- Existing local DB may have older schemas with missing columns or extra NOT NULL columns.
- We apply additive migrations (ALTER TABLE ADD COLUMN) and populate deterministic defaults
  for legacy NOT NULL columns during upserts.

This delivery fixes:
- Legacy cn_epr_state_event missing 'event_kind' (and other core columns) causing
  OperationalError during scan replay.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Dict, Any, Optional

import sqlite3


class SQLiteStore:
    def __init__(self, sqlite_path: Path, schema_sql_path: Path):
        self.sqlite_path = Path(sqlite_path)
        self.schema_sql_path = Path(schema_sql_path)

    def _connect(self) -> sqlite3.Connection:
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.sqlite_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    # --------- schema / migrations ----------
    def ensure_schema(self) -> None:
        if not self.schema_sql_path.exists():
            raise RuntimeError(f"Missing schema file: {self.schema_sql_path}")

        schema_sql = self.schema_sql_path.read_text(encoding="utf-8")
        with self._connect() as conn:
            conn.executescript(schema_sql)

            # Additive migrations for existing DBs
            self._ensure_columns_entry_pool(conn)
            self._ensure_columns_state_snap(conn)
            self._ensure_columns_state_event(conn)
            self._ensure_columns_position_snap(conn)
            self._ensure_columns_execution(conn)

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
        if not rows:
            raise RuntimeError(f"Missing required table: {table}")
        return {r["name"] for r in rows}

    @staticmethod
    def _add_missing_columns(conn: sqlite3.Connection, table: str, required: dict[str, str]) -> None:
        cols = SQLiteStore._table_columns(conn, table)
        for col, typ in required.items():
            if col not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ};")

    # ----- migrations per table -----
    def _ensure_columns_entry_pool(self, conn: sqlite3.Connection) -> None:
        self._add_missing_columns(conn, "cn_epr_entry_pool", {
            "theme": "TEXT",
            "name": "TEXT",
            "oracle_symbol": "TEXT",
            "entry_mode": "TEXT",
            "max_lots_2026": "INTEGER",
            "is_active": "INTEGER",
            "updated_at": "TEXT",
        })

    def _ensure_columns_state_snap(self, conn: sqlite3.Connection) -> None:
        self._add_missing_columns(conn, "cn_epr_state_snap", {
            "breakout_level": "REAL",
            "confirm_ok_streak": "INTEGER DEFAULT 0",
            "fail_streak": "INTEGER DEFAULT 0",
            "cooldown_days_left": "INTEGER DEFAULT 0",
            "asof": "TEXT",
            "updated_at": "TEXT",
        })

    def _ensure_columns_state_event(self, conn: sqlite3.Connection) -> None:
        # FIX: some legacy DBs had a minimal event table without these columns.
        self._add_missing_columns(conn, "cn_epr_state_event", {
            "event_kind": "TEXT",
            "from_state": "TEXT",
            "to_state": "TEXT",
            "reason_code": "TEXT",
            "reason_text": "TEXT",
            "payload_json": "TEXT",
            "created_at": "TEXT",
        })

    def _ensure_columns_position_snap(self, conn: sqlite3.Connection) -> None:
        self._add_missing_columns(conn, "cn_epr_position_snap", {
            "avg_cost": "REAL",
            "asof": "TEXT",
            "updated_at": "TEXT",
        })

    def _ensure_columns_execution(self, conn: sqlite3.Connection) -> None:
        self._add_missing_columns(conn, "cn_epr_execution", {
            "limit_price": "REAL",
            "note": "TEXT",
            "payload_json": "TEXT",
            "created_at": "TEXT",
            "updated_at": "TEXT",
        })

    # ---------- Entry Pool ----------
    @staticmethod
    def _legacy_entry_pool_defaults(d: dict) -> dict:
        theme = (d.get("theme") or "").strip()
        group_code = theme or "UNKNOWN"
        if theme in ("AI_HARDWARE", "AI"):
            group_code = "AI_HARDWARE"
        elif theme in ("SEMI_SUBSTITUTION", "SEMI"):
            group_code = "SEMI_SUBSTITUTION"
        return {"group_code": group_code}

    def upsert_entry_pool(self, items: Dict[str, Any]) -> None:
        with self._connect() as conn:
            self._ensure_columns_entry_pool(conn)
            cols = self._table_columns(conn, "cn_epr_entry_pool")

            for symbol, item in items.items():
                if hasattr(item, "__dataclass_fields__"):
                    d = asdict(item)
                elif isinstance(item, dict):
                    d = dict(item)
                else:
                    raise TypeError(f"Unsupported entry pool item type for {symbol}: {type(item)}")

                d["symbol"] = d.get("symbol") or symbol
                for k in ("symbol", "oracle_symbol", "entry_mode"):
                    if not d.get(k):
                        raise RuntimeError(f"Entry pool item missing required field '{k}': {d}")

                d.setdefault("theme", None)
                d.setdefault("name", None)
                d.setdefault("max_lots_2026", 0)
                d.setdefault("is_active", 1)

                legacy = self._legacy_entry_pool_defaults(d)

                insert_fields = []
                insert_values = []
                params = {}

                def add_param(f: str, v):
                    insert_fields.append(f); insert_values.append(f":{f}"); params[f] = v

                def add_sql(f: str, expr: str):
                    insert_fields.append(f); insert_values.append(expr)

                for f in ("symbol","theme","name","oracle_symbol","entry_mode","max_lots_2026","is_active"):
                    if f in cols:
                        if f in ("max_lots_2026","is_active"):
                            add_param(f, int(d.get(f) or 0))
                        else:
                            add_param(f, d.get(f))

                for f, v in legacy.items():
                    if f in cols:
                        add_param(f, v)

                if "created_at" in cols:
                    add_sql("created_at", "datetime('now')")
                if "updated_at" in cols:
                    add_sql("updated_at", "datetime('now')")

                update_assignments = []
                for f in insert_fields:
                    if f in ("symbol", "created_at"):
                        continue
                    update_assignments.append(f"{f}=excluded.{f}")
                if "updated_at" in cols:
                    update_assignments.append("updated_at=datetime('now')")

                sql = f"""
                    INSERT INTO cn_epr_entry_pool ({", ".join(insert_fields)})
                    VALUES ({", ".join(insert_values)})
                    ON CONFLICT(symbol) DO UPDATE SET {", ".join(update_assignments)}
                """
                conn.execute(sql, params)

    # ---------- State / Event / Position / Execution ----------
    def get_latest_state_before(self, trade_date: str) -> Dict[str, dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT s.*
                FROM cn_epr_state_snap s
                JOIN (
                  SELECT symbol, MAX(trade_date) AS max_date
                  FROM cn_epr_state_snap
                  WHERE trade_date < :trade_date
                  GROUP BY symbol
                ) m
                ON s.symbol = m.symbol AND s.trade_date = m.max_date
                """,
                {"trade_date": trade_date},
            ).fetchall()
            return {r["symbol"]: dict(r) for r in rows}

    def upsert_state_snap(self, trade_date: str, symbol: str, state: str,
                         breakout_level: Optional[float],
                         confirm_ok_streak: int,
                         fail_streak: int,
                         cooldown_days_left: int,
                         asof: str) -> None:
        with self._connect() as conn:
            self._ensure_columns_state_snap(conn)
            cols = self._table_columns(conn, "cn_epr_state_snap")

            legacy_params = {}
            legacy_sql_fields = {}

            if "run_id" in cols:
                legacy_params["run_id"] = "CN_ENTRY_POOL_ROTATION_V1"
            if "suggested_action" in cols:
                legacy_params["suggested_action"] = "NONE"
            if "suggested_lots" in cols:
                legacy_params["suggested_lots"] = 0
            if "suggested_note" in cols:
                legacy_params["suggested_note"] = ""
            if "note" in cols and "suggested_note" not in legacy_params:
                legacy_params["note"] = ""
            if "created_at" in cols:
                legacy_sql_fields["created_at"] = "datetime('now')"

            insert_fields = ["trade_date", "symbol", "state", "breakout_level",
                             "confirm_ok_streak", "fail_streak", "cooldown_days_left", "asof"]
            insert_values = [":trade_date", ":symbol", ":state", ":breakout_level",
                             ":confirm_ok_streak", ":fail_streak", ":cooldown_days_left", ":asof"]

            params = {
                "trade_date": trade_date,
                "symbol": symbol,
                "state": state,
                "breakout_level": breakout_level,
                "confirm_ok_streak": int(confirm_ok_streak),
                "fail_streak": int(fail_streak),
                "cooldown_days_left": int(cooldown_days_left),
                "asof": asof,
            }

            for k, v in legacy_params.items():
                insert_fields.append(k)
                insert_values.append(f":{k}")
                params[k] = v
            for k, expr in legacy_sql_fields.items():
                insert_fields.append(k)
                insert_values.append(expr)

            if "updated_at" in cols:
                insert_fields.append("updated_at")
                insert_values.append("datetime('now')")

            update_assignments = [
                "state=excluded.state",
                "breakout_level=excluded.breakout_level",
                "confirm_ok_streak=excluded.confirm_ok_streak",
                "fail_streak=excluded.fail_streak",
                "cooldown_days_left=excluded.cooldown_days_left",
                "asof=excluded.asof",
            ]
            for k in legacy_params.keys():
                update_assignments.append(f"{k}=excluded.{k}")
            if "updated_at" in cols:
                update_assignments.append("updated_at=datetime('now')")

            sql = f"""
                INSERT INTO cn_epr_state_snap ({", ".join(insert_fields)})
                VALUES ({", ".join(insert_values)})
                ON CONFLICT(trade_date, symbol) DO UPDATE SET {", ".join(update_assignments)}
            """
            conn.execute(sql, params)

    def insert_state_event(self, trade_date: str, symbol: str, event_kind: str,
                           from_state: str, to_state: str, reason_code: str,
                           reason_text: str, payload_json: str) -> None:
        with self._connect() as conn:
            self._ensure_columns_state_event(conn)
            conn.execute(
                """
                INSERT OR IGNORE INTO cn_epr_state_event
                  (trade_date, symbol, event_kind, from_state, to_state, reason_code, reason_text,
                   payload_json, created_at)
                VALUES
                  (:trade_date, :symbol, :event_kind, :from_state, :to_state, :reason_code, :reason_text,
                   :payload_json, datetime('now'))
                """,
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "event_kind": event_kind,
                    "from_state": from_state,
                    "to_state": to_state,
                    "reason_code": reason_code,
                    "reason_text": reason_text,
                    "payload_json": payload_json,
                },
            )

    def get_latest_position_before(self, trade_date: str) -> Dict[str, dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT p.*
                FROM cn_epr_position_snap p
                JOIN (
                  SELECT symbol, MAX(trade_date) AS max_date
                  FROM cn_epr_position_snap
                  WHERE trade_date < :trade_date
                  GROUP BY symbol
                ) m
                ON p.symbol = m.symbol AND p.trade_date = m.max_date
                """,
                {"trade_date": trade_date},
            ).fetchall()
            return {r["symbol"]: dict(r) for r in rows}

    def upsert_position_snap(self, trade_date: str, symbol: str, position_lots: int,
                             avg_cost: Optional[float], asof: str) -> None:
        with self._connect() as conn:
            self._ensure_columns_position_snap(conn)
            conn.execute(
                """
                INSERT INTO cn_epr_position_snap
                  (trade_date, symbol, position_lots, avg_cost, asof, updated_at)
                VALUES
                  (:trade_date, :symbol, :position_lots, :avg_cost, :asof, datetime('now'))
                ON CONFLICT(trade_date, symbol) DO UPDATE SET
                  position_lots=excluded.position_lots,
                  avg_cost=excluded.avg_cost,
                  asof=excluded.asof,
                  updated_at=datetime('now')
                """,
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "position_lots": int(position_lots),
                    "avg_cost": avg_cost,
                    "asof": asof,
                },
            )

    def clear_execution_on(self, trade_date: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM cn_epr_execution WHERE trade_date = :d;", {"d": trade_date})

    def upsert_execution(self, trade_date: str, symbol: str, action: str, lots: int,
                         limit_price: Optional[float], note: str, payload_json: str) -> None:
        with self._connect() as conn:
            self._ensure_columns_execution(conn)
            conn.execute(
                """
                INSERT INTO cn_epr_execution
                  (trade_date, symbol, action, lots, limit_price, note, payload_json, created_at, updated_at)
                VALUES
                  (:trade_date, :symbol, :action, :lots, :limit_price, :note, :payload_json, datetime('now'), datetime('now'))
                ON CONFLICT(trade_date, symbol, action) DO UPDATE SET
                  lots=excluded.lots,
                  limit_price=excluded.limit_price,
                  note=excluded.note,
                  payload_json=excluded.payload_json,
                  updated_at=datetime('now')
                """,
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "action": action,
                    "lots": int(lots),
                    "limit_price": limit_price,
                    "note": note,
                    "payload_json": payload_json,
                },
            )
