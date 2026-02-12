# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3


def ensure_schema_l1(conn: sqlite3.Connection) -> None:
    """Ensure L1 (run persistence) tables exist. Idempotent."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ur_run_meta (
          run_id          TEXT    NOT NULL PRIMARY KEY,
          trade_date      TEXT    NOT NULL,
          report_kind     TEXT    NOT NULL,
          engine_version  TEXT    NOT NULL,
          started_at_utc  INTEGER NOT NULL,
          finished_at_utc INTEGER,
          status          TEXT    NOT NULL,
          error_type      TEXT,
          error_message   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_ur_run_by_date_kind
          ON ur_run_meta (trade_date, report_kind);

        CREATE INDEX IF NOT EXISTS idx_ur_run_engine
          ON ur_run_meta (engine_version);

        CREATE INDEX IF NOT EXISTS idx_ur_run_status
          ON ur_run_meta (status);

        CREATE INDEX IF NOT EXISTS idx_ur_run_started
          ON ur_run_meta (started_at_utc);

        CREATE TABLE IF NOT EXISTS ur_snapshot_raw (
          run_id         TEXT    NOT NULL,
          snapshot_name    TEXT    NOT NULL,
          seq            INTEGER  ,
          payload_json   TEXT    NOT NULL,
          created_at_utc INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
          PRIMARY KEY (run_id, snapshot_name, seq)
        );

        CREATE INDEX IF NOT EXISTS idx_ur_snap_run
          ON ur_snapshot_raw (run_id);

        CREATE INDEX IF NOT EXISTS idx_ur_snap_source
          ON ur_snapshot_raw (snapshot_name);

        CREATE TABLE IF NOT EXISTS ur_factor_result (
          run_id         TEXT    NOT NULL,
          factor_name    TEXT    NOT NULL,
          factor_version TEXT,
          seq            INTEGER ,
          payload_json   TEXT    NOT NULL,
          created_at_utc INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
          PRIMARY KEY (run_id, factor_name, seq)
        );

        CREATE INDEX IF NOT EXISTS idx_ur_factor_run
          ON ur_factor_result (run_id);

        CREATE INDEX IF NOT EXISTS idx_ur_factor_name
          ON ur_factor_result (factor_name);

        CREATE INDEX IF NOT EXISTS idx_ur_factor_created
          ON ur_factor_result (created_at_utc);

        CREATE TABLE IF NOT EXISTS ur_gate_decision (
          run_id         TEXT    NOT NULL PRIMARY KEY,
          gate           TEXT    NOT NULL,
          drs            TEXT    NOT NULL,
          frf            TEXT    NOT NULL,
          action_hint    TEXT,
          rule_hits_json TEXT,
          created_at_utc INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE INDEX IF NOT EXISTS idx_ur_gate_gate
          ON ur_gate_decision (gate);

        CREATE INDEX IF NOT EXISTS idx_ur_gate_created
          ON ur_gate_decision (created_at_utc);
        """
    )
    conn.commit()
