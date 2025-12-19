"""core/utils/db_config.py

UnifiedRisk V12 FULL - DB Config Loader
--------------------------------------
Purpose:
    Provide a single, flexible configuration source for DB-backed DataSources.

Design (frozen):
    - UnifiedRisk does NOT do ETL here.
    - DataSource only connects and queries.
    - Provider implementations (Oracle/MySQL) must be swappable.

Config priority:
    1) Environment variables (highest)
    2) config/db.yaml (if present)

Environment variables:
    UR_DB_TYPE        : "oracle" | "mysql"
    UR_DB_USER        : username
    UR_DB_PASSWORD    : password
    UR_DB_DSN         : oracle DSN or mysql "host:port/db"
    UR_DB_SCHEMA      : schema name (default: SECOPR)
    UR_DB_TABLE       : table name (default: CN_STOCK_DAILY_PRICE)

YAML example (config/db.yaml):
    type: oracle
    user: scott
    password: tiger
    dsn: host:port/service
    schema: SECOPR
    table: CN_STOCK_DAILY_PRICE
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml

from core.utils.logger import get_logger
from core.utils.config_loader import ROOT_DIR

LOG = get_logger("DB.Config")


@dataclass(frozen=True, slots=True)
class DBConfig:
    db_type: str
    user: str
    password: str
    dsn: str
    schema: str = "SECOPR"
    table: str = "CN_STOCK_DAILY_PRICE"

    @property
    def full_table(self) -> str:
        # Oracle may use SCHEMA.TABLE, MySQL typically uses TABLE (schema is DB)
        if self.schema:
            return f"{self.schema}.{self.table}"
        return self.table


def _load_yaml_db() -> Dict[str, Any]:
    path = os.path.join(ROOT_DIR, "config", "db.yaml")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        LOG.error("[DB.Config] failed to load config/db.yaml: %s", e)
        return {}


def load_db_config() -> Optional[DBConfig]:
    """Load DB config.

    Returns None if not configured (DataSource should decide whether to hard-fail).
    """

    y = _load_yaml_db()

    db_type = (os.getenv("UR_DB_TYPE") or y.get("type") or "").strip().lower()
    user = (os.getenv("UR_DB_USER") or y.get("user") or "").strip()
    password = (os.getenv("UR_DB_PASSWORD") or y.get("password") or "").strip()
    dsn = (os.getenv("UR_DB_DSN") or y.get("dsn") or "").strip()
    schema = (os.getenv("UR_DB_SCHEMA") or y.get("schema") or "SECOPR").strip()
    table = (os.getenv("UR_DB_TABLE") or y.get("table") or "CN_STOCK_DAILY_PRICE").strip()

    if not (db_type and user and password and dsn):
        LOG.warning(
            "[DB.Config] DB not configured (need UR_DB_TYPE/USER/PASSWORD/DSN or config/db.yaml)"
        )
        return None

    if db_type not in {"oracle", "mysql"}:
        LOG.error("[DB.Config] unsupported db_type=%s", db_type)
        raise ValueError(f"Unsupported DB type: {db_type}")

    return DBConfig(
        db_type=db_type,
        user=user,
        password=password,
        dsn=dsn,
        schema=schema,
        table=table,
    )
