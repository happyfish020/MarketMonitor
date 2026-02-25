from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


# Primary DSN (MySQL)
ORACLE_DSN = os.getenv(
    "CN_PG_DB_DSN",
    "mysql+pymysql://cn_opr:sec%40Bobo123@localhost:3306/cn_market?charset=utf8mb4",
)

# Source table
ORACLE_PRICE_TABLE = os.getenv("CN_PG_PRICE_TABLE", "CN_STOCK_DAILY_PRICE")

# Frozen field names
F_SYMBOL = "SYMBOL"
F_TRADE_DATE = "TRADE_DATE"
F_CLOSE = "CLOSE"
F_VOLUME = "VOLUME"

# Frozen SQLite path (independent DB file)
SQLITE_PATH = Path("data") / "cn_position_governance.db"

# Frozen risk levels
RISK_LOW = "LOW"
RISK_NORMAL = "NORMAL"
RISK_HIGH = "HIGH"


@dataclass(frozen=True)
class PgRunContext:
    trade_date: str  # YYYY-MM-DD (stored in SQLite as TEXT)
    run_id: str
