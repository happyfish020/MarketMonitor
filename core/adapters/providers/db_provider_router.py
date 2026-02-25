from __future__ import annotations

"""DB Provider Router (UnifiedRisk V12)

Resolve a DB provider implementation based on root/config/config.yaml.

This is intentionally separate from ProviderRouter (market data providers like
yf/em/bs) to avoid layer confusion.
"""

from functools import lru_cache
from core.utils.config_loader import load_config
from core.utils.logger import get_logger

from core.adapters.providers.db_provider_base import DBProviderBase
from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider


LOG = get_logger("Provider.DB.Router")


@lru_cache()
def get_db_provider() -> DBProviderBase:
    cfg = load_config() or {}
    db_cfg = cfg.get("db", {}) or {}
    db_type = str(db_cfg.get("type", "mysql")).strip().lower()

    if db_type in ("mysql", "oracle"):
        LOG.info("[DBProviderRouter] using mysql market provider")
        return DBMySQLMarketProvider()

    LOG.error(f"[DBProviderRouter] unsupported db.type={db_type}")
    raise RuntimeError(f"unsupported db.type: {db_type}")

