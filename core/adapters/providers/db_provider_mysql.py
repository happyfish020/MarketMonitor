"""core/adapters/providers/db_provider_mysql.py

UnifiedRisk V12 FULL - MySQL DB Provider
---------------------------------------
Implements DBProviderBase for MySQL 8+.

Notes:
    - Requires `pymysql` package.
    - DSN format for mysql in UR_DB_DSN or config/db.yaml:
          host:port/db
    - Table schema expected:
          SYMBOL (varchar), EXCHANGE (varchar), TRADE_DATE (date), CLOSE (numeric)
"""

from __future__ import annotations

from typing import List, Tuple

from core.adapters.providers.db_provider_base import BreadthNewLowsResult, DBProviderBase
from core.utils.db_config import DBConfig
from core.utils.logger import get_logger

LOG = get_logger("Provider.DB.MySQL")


def _parse_mysql_dsn(dsn: str) -> Tuple[str, int, str]:
    """Parse 'host:port/db' into components."""
    host_port, db = dsn.split("/", 1)
    if ":" in host_port:
        host, port_s = host_port.split(":", 1)
        port = int(port_s)
    else:
        host = host_port
        port = 3306
    return host, port, db


class MySQLDBProvider(DBProviderBase):
    def __init__(self, cfg: DBConfig) -> None:
        # In MySQL, `schema` is the database name; cfg.schema may be used as alias.
        super().__init__(schema=cfg.schema, table=cfg.table)
        self.cfg = cfg
        self._conn = None

    def _connect(self):
        if self._conn is not None:
            return self._conn
        try:
            import pymysql  # type: ignore
        except Exception as e:
            LOG.error("[MySQLDBProvider] missing pymysql dependency: %s", e)
            raise

        host, port, db = _parse_mysql_dsn(self.cfg.dsn)
        try:
            self._conn = pymysql.connect(
                host=host,
                port=port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=db,
                autocommit=True,
            )
            return self._conn
        except Exception as e:
            LOG.error("[MySQLDBProvider] connect failed: %s", e)
            raise

    def ping(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()

    def list_trade_dates(self, start: str, end: str) -> List[str]:
        conn = self._connect()
        full_table = f"{self.table}"  # database is selected on connect
        sql = (
            f"SELECT DATE_FORMAT(trade_date, '%Y-%m-%d') AS d "
            f"FROM {full_table} "
            "WHERE trade_date BETWEEN %s AND %s "
            "GROUP BY trade_date ORDER BY trade_date"
        )
        with conn.cursor() as cur:
            cur.execute(sql, (start, end))
            return [r[0] for r in cur.fetchall()]

    def calc_new_lows(self, trade_date: str, window: int) -> BreadthNewLowsResult:
        conn = self._connect()
        full_table = f"{self.table}"

        # MySQL 8 window functions supported.
        sql = f"""
WITH base AS (
    SELECT
        symbol,
        exchange,
        trade_date,
        close,
        MIN(close) OVER (
            PARTITION BY symbol, exchange
            ORDER BY trade_date
            ROWS BETWEEN {window} PRECEDING AND 1 PRECEDING
        ) AS min_prev
    FROM {full_table}
    WHERE trade_date <= %s
),
today AS (
    SELECT * FROM base WHERE trade_date = %s
)
SELECT
    SUM(CASE WHEN min_prev IS NOT NULL AND close <= min_prev THEN 1 ELSE 0 END) AS new_lows,
    COUNT(*) AS universe
FROM today
"""

        with conn.cursor() as cur:
            cur.execute(sql, (trade_date, trade_date))
            row = cur.fetchone()
            new_lows = int(row[0] or 0)
            universe = int(row[1] or 0)
            return BreadthNewLowsResult(
                trade_date=trade_date,
                window=int(window),
                new_lows=new_lows,
                universe=universe,
            )
