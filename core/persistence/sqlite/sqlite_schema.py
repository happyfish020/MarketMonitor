# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3


def ensure_schema_l2(conn: sqlite3.Connection) -> None:
    """Ensure L2 (institutional) tables exist. Idempotent."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ur_report_artifact (
          trade_date       TEXT    NOT NULL,
          report_kind      TEXT    NOT NULL,
          content_text     TEXT    NOT NULL,
          content_hash     TEXT    NOT NULL,
          meta_json        TEXT,
          created_at_utc   INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
          PRIMARY KEY (trade_date, report_kind)
        );

        CREATE INDEX IF NOT EXISTS idx_ur_report_created_at
          ON ur_report_artifact (created_at_utc);

        CREATE INDEX IF NOT EXISTS idx_ur_report_kind_date
          ON ur_report_artifact (report_kind, trade_date);

        CREATE TABLE IF NOT EXISTS ur_decision_evidence_snapshot (
          trade_date        TEXT    NOT NULL,
          report_kind       TEXT    NOT NULL,
          engine_version    TEXT    NOT NULL,
          des_payload_json  TEXT    NOT NULL,
          des_hash          TEXT    NOT NULL,
          created_at_utc    INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
          PRIMARY KEY (trade_date, report_kind)
        );

        CREATE INDEX IF NOT EXISTS idx_ur_des_created_at
          ON ur_decision_evidence_snapshot (created_at_utc);

        CREATE INDEX IF NOT EXISTS idx_ur_des_engine
          ON ur_decision_evidence_snapshot (engine_version);

        CREATE TABLE IF NOT EXISTS ur_report_des_link (
          trade_date       TEXT    NOT NULL,
          report_kind      TEXT    NOT NULL,
          report_hash      TEXT    NOT NULL,
          des_hash         TEXT    NOT NULL,
          created_at_utc   INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
          PRIMARY KEY (trade_date, report_kind),
          FOREIGN KEY (trade_date, report_kind)
            REFERENCES ur_report_artifact (trade_date, report_kind)
        );

        CREATE TABLE IF NOT EXISTS ur_persistence_audit (
          id               INTEGER PRIMARY KEY AUTOINCREMENT,
          trade_date       TEXT    NOT NULL,
          report_kind      TEXT    NOT NULL,
          event            TEXT    NOT NULL,
          note             TEXT,
          created_at_utc   INTEGER NOT NULL,
          created_at      TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE INDEX IF NOT EXISTS idx_ur_audit_created_at
          ON ur_persistence_audit (created_at_utc);
        """
    )
    conn.commit()
