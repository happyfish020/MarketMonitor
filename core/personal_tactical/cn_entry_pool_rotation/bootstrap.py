from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config_loader import load_config
from .db import connect, init_schema, upsert_many, fetch_all


def bootstrap(db_path: Path, config_path: Path) -> None:
    """Initialize schema and upsert entry pool from YAML.

    This function is idempotent: running it multiple times will keep schema and pool in sync.
    """
    conn = connect(db_path)
    schema_sql = Path(__file__).parent / "resources" / "sqlite_schema.sql"
    init_schema(conn, schema_sql)

    cfg = load_config(config_path)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    rows = []
    for it in cfg.pool:
        rows.append(
            (
                it.symbol,
                it.name,
                it.group_code,
                cfg.entry_mode,
                int(it.max_lots_2026),
                1 if it.is_active else 0,
                now,
                now,
            )
        )

    upsert_many(
        conn,
        table="cn_epr_entry_pool",
        columns=[
            "symbol",
            "name",
            "group_code",
            "entry_mode",
            "max_lots_2026",
            "is_active",
            "created_at",
            "updated_at",
        ],
        rows=rows,
        conflict_cols=["symbol"],
    )

    conn.close()


def ensure_sqlite_initialized(db_path: Path, config_path: Path) -> None:
    """Compatibility wrapper expected by engine.py.

    Ensures schema exists and the entry pool is present. Safe to call every run.
    """
    # We rely on bootstrap's idempotency.
    bootstrap(db_path=db_path, config_path=config_path)


def load_entry_pool_from_db(db_path: Path, only_active: bool = True) -> List[Dict[str, Any]]:
    """Load entry pool from SQLite.

    Returns list of dicts with at least: symbol, name, group_code, entry_mode, max_lots_2026, is_active.
    """
    conn = connect(db_path)
    if only_active:
        rows = fetch_all(
            conn,
            "SELECT symbol,name,group_code,entry_mode,max_lots_2026,is_active FROM cn_epr_entry_pool WHERE is_active=1 ORDER BY group_code, symbol",
            (),
        )
    else:
        rows = fetch_all(
            conn,
            "SELECT symbol,name,group_code,entry_mode,max_lots_2026,is_active FROM cn_epr_entry_pool ORDER BY group_code, symbol",
            (),
        )
    conn.close()
    return rows
