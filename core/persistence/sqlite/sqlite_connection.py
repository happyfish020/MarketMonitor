# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from typing import Optional


def connect_sqlite(db_path: str, timeout: float = 30.0) -> sqlite3.Connection:
    """Create a configured SQLite connection for UnifiedRisk persistence."""
    conn = sqlite3.connect(db_path, timeout=timeout)
    conn.row_factory = sqlite3.Row

    # Pragmas
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn
