from __future__ import annotations

from typing import Any

from core.utils.config_loader import load_config
from core.utils.logger import get_logger


LOG = get_logger("Provider.DBFactory")


def get_db_provider() -> Any:
    """Return a DB provider instance based on config/config.yaml.

    Only Oracle is implemented in Phase-2; MySQL can be added with the same
    interface without changing DataSources.
    """

    cfg = load_config().get("db", {}) or {}
    db_type = str(cfg.get("type", "mysql")).lower().strip()

    if db_type in ("mysql", "oracle"):
        from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

        return DBMySQLMarketProvider()

    raise ValueError(f"Unsupported db.type={db_type}")

