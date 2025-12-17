from typing import Any, Dict, List, Tuple
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text

from core.utils.config_loader import load_config
from core.utils.logger import get_logger


logger = get_logger(__name__)


def _to_date(x) -> date:
    """
    Normalize any date-like input to python datetime.date
    Accepts: str ('YYYYMMDD' or 'YYYY-MM-DD'), datetime, date
    """
    if x is None:
        return None
    if isinstance(x, date):
        return x
    return pd.to_datetime(x).date()


class DBOracleProvider:
    """
    Oracle DB provider (thin mode, TCP only)

    Rules (FROZEN):
    - NEVER use TO_DATE in SQL
    - ALWAYS bind python date/datetime to Oracle DATE
    """

    def __init__(self):
        cfg = load_config().get("db", {}).get("oracle", {})
        if not cfg:
            raise RuntimeError("missing db.oracle config in config.yaml")

        self.user = cfg["user"]
        self.password = cfg["password"]
        self.host = cfg["host"]
        self.port = cfg.get("port", 1521)
        self.service = cfg["service"]
        self.schema = cfg.get("schema")
        self.tables = cfg.get("tables", {})

        dsn = f"{self.host}:{self.port}/{self.service}"
        conn_str = f"oracle+oracledb://{self.user}:{self.password}@{dsn}"

        logger.info(f"[DBOracleProvider] connecting to oracle tcp dsn={dsn}")

        self.engine = create_engine(
            conn_str,
            pool_pre_ping=True,
            future=True,
        )

    # ==================================================
    # low-level executor
    # ==================================================
    def execute(self, sql: str, params: Dict[str, Any] | None = None):
        logger.debug(f"[DBOracleProvider] execute sql={sql} params={params}")
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return result.fetchall()

    # ==================================================
    # stock daily prices (CLOSE -> close_price)
    # ==================================================
    def query_stock_closes(
        self,
        window_start,
        trade_date,
    ) -> List[Tuple[str, str, Any, float]]:
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")

        sql = f"""
        SELECT
            SYMBOL        AS symbol,
            EXCHANGE      AS exchange,
            TRADE_DATE    AS trade_date,
            CLOSE         AS close_price
        FROM {self.schema}.{table}
        WHERE TRADE_DATE >= :window_start
          AND TRADE_DATE <= :trade_date
        """

        params = {
            "window_start": _to_date(window_start),
            "trade_date": _to_date(trade_date),
        }

        return self.execute(sql, params)

    # ==================================================
    # index daily prices (CLOSE -> close_price)
    # ==================================================
    def query_index_closes(
        self,
        index_code: str,
        window_start,
        trade_date,
    ) -> List[Tuple[str, Any, float]]:
        table = self.tables.get("index_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.index_daily not configured")

        sql = f"""
        SELECT
            INDEX_CODE    AS index_code,
            TRADE_DATE    AS trade_date,
            CLOSE         AS close_price
        FROM {self.schema}.{table}
        WHERE INDEX_CODE = :index_code
          AND TRADE_DATE >= :window_start
          AND TRADE_DATE <= :trade_date
        """

        params = {
            "index_code": index_code,
            "window_start": _to_date(window_start),
            "trade_date": _to_date(trade_date),
        }

        return self.execute(sql, params)

    # ==================================================
    # universe symbols (industry mapping)
    # ==================================================
    def query_universe_symbols(self):
        table = self.tables.get("universe")
        if not table:
            raise RuntimeError("db.oracle.tables.universe not configured")

        sql = f"""
        SELECT
            SYMBOL   AS symbol,
            EXCHANGE AS exchange,
            SW_L1    AS sw_l1
        FROM {self.schema}.{table}
        """

        return self.execute(sql)
