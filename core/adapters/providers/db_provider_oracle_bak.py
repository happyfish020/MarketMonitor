from typing import Any, Dict, List, Tuple
from datetime import date
import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from core.utils.config_loader import load_config
from core.utils.logger import get_logger


logger = get_logger(__name__)
from sqlalchemy.dialects import oracle

LOG = get_logger("DS.provider.oracle")


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
        db_cfg = load_config().get("db", {}) or {}
        cfg = db_cfg.get("oracle", {}) or {}

        self.user = cfg.get("user", "")
        self.password = cfg.get("password", "")
        self.host = cfg.get("host", "localhost")
        self.port = cfg.get("port", 1521)
        self.service = cfg.get("service", "xe")
        self.schema = cfg.get("schema")
        self.tables = cfg.get("tables", {})

        dsn = f"{self.host}:{self.port}/{self.service}"
        self._oracle_dsn = dsn
        self._oracle_conn_str = f"oracle+oracledb://{self.user}:{self.password}@{dsn}"
        self.engine = None
        self.mysql_stock_oracle_fallback = str(
            os.getenv("MYSQL_STOCK_ORACLE_FALLBACK", "0")
        ).strip().lower() in ("1", "true", "yes", "on")

        # Local MySQL is the preferred path for all non-EPR modules.
        mysql_cfg = db_cfg.get("mysql", {}) or {}
        self.mysql_cfg = {
            "host": os.getenv("MYSQL_HOST", str(mysql_cfg.get("host", "localhost"))),
            "port": int(os.getenv("MYSQL_PORT", str(mysql_cfg.get("port", 3306)))),
            "user": os.getenv("MYSQL_USER", str(mysql_cfg.get("user", "cn_opr"))),
            "password": os.getenv("MYSQL_PASSWORD", str(mysql_cfg.get("password", "sec@Bobo123"))),
            "database": os.getenv("MYSQL_DATABASE", str(mysql_cfg.get("database", "cn_market"))),
            "charset": str(mysql_cfg.get("charset", "utf8mb4")),
        }
        mysql_tables = mysql_cfg.get("tables", {}) or {}
        self.mysql_stock_table = os.getenv("MYSQL_STOCK_TABLE", str(mysql_tables.get("stock_daily", "CN_STOCK_DAILY_PRICE")))
        self.mysql_etf_table = os.getenv("MYSQL_ETF_TABLE", str(mysql_tables.get("fund_etf_hist", "CN_FUND_ETF_HIST_EM")))
        self.mysql_index_table = os.getenv("MYSQL_INDEX_TABLE", str(mysql_tables.get("index_daily", "CN_INDEX_DAILY_PRICE")))
        self.mysql_fut_table = os.getenv("MYSQL_FUT_TABLE", str(mysql_tables.get("fut_index_hist", "CN_FUT_INDEX_HIS")))
        self.mysql_option_table = os.getenv("MYSQL_OPTION_TABLE", str(mysql_tables.get("option_daily", "CN_OPTION_SSE_DAILY")))
        self.mysql_universe_table = os.getenv("MYSQL_UNIVERSE_TABLE", str(mysql_tables.get("universe", "CN_UNIVERSE_SYMBOLS")))
        self.mysql_engine = None
        try:
            _pwd = quote_plus(str(self.mysql_cfg["password"]))
            mysql_conn_str = (
                f"mysql+pymysql://{self.mysql_cfg['user']}:{_pwd}"
                f"@{self.mysql_cfg['host']}:{self.mysql_cfg['port']}/{self.mysql_cfg['database']}"
                f"?charset={self.mysql_cfg['charset']}"
            )
            self.mysql_engine = create_engine(
                mysql_conn_str,
                pool_pre_ping=True,
                future=True,
            )
            LOG.info(
                "[DBOracleProvider] mysql source enabled: %s:%s/%s stock=%s etf=%s index=%s fut=%s option=%s universe=%s",
                self.mysql_cfg["host"],
                self.mysql_cfg["port"],
                self.mysql_cfg["database"],
                self.mysql_stock_table,
                self.mysql_etf_table,
                self.mysql_index_table,
                self.mysql_fut_table,
                self.mysql_option_table,
                self.mysql_universe_table,
            )
        except Exception as e:
            LOG.warning("[DBOracleProvider] mysql source init failed: %s", e)
        
         
    def _ensure_oracle_engine(self):
        if not (self.user and self.password and self.host and self.service):
            raise RuntimeError("Oracle config is incomplete (db.oracle), cannot create oracle engine")
        if self.engine is None:
            logger.info(
                "[DBOracleProvider] initializing oracle engine lazily dsn=%s",
                self._oracle_dsn,
            )
            self.engine = create_engine(
                self._oracle_conn_str,
                pool_pre_ping=True,
                future=True,
            )
        return self.engine

    # ==================================================
    # low-level executor
    # ==================================================
    def execute(self, sql: str, params: Dict[str, Any] | None = None):
        logger.debug(f"[DBOracleProvider] execute sql={sql} params={params}")
        with self._ensure_oracle_engine().connect() as conn:
            result = conn.execute(text(sql), params or {})
            return result.fetchall()

    def execute_mysql(self, sql: str, params: Dict[str, Any] | None = None):
        if self.mysql_engine is None:
            raise RuntimeError("mysql engine not available")
        logger.debug(f"[DBOracleProvider] execute_mysql sql={sql} params={params}")
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return result.fetchall()

    def _stock_table_ref(self, use_mysql: bool) -> str:
        if use_mysql:
            return self.mysql_stock_table
        return f"{self.schema}.{self.tables.get('stock_daily')}"

    def _etf_table_ref(self, use_mysql: bool) -> str:
        if use_mysql:
            return self.mysql_etf_table
        return f"{self.schema}.{self.tables.get('fund_etf_hist') or 'CN_FUND_ETF_HIST_EM'}"

    def _index_table_ref(self, use_mysql: bool) -> str:
        if use_mysql:
            return self.mysql_index_table
        return f"{self.schema}.{self.tables.get('index_daily') or 'CN_INDEX_DAILY_PRICE'}"

    def _fut_table_ref(self, use_mysql: bool) -> str:
        if use_mysql:
            return self.mysql_fut_table
        return f"{self.schema}.{self.tables.get('fut_index_hist') or 'CN_FUT_INDEX_HIS'}"

    def _option_table_ref(self, use_mysql: bool) -> str:
        if use_mysql:
            return self.mysql_option_table
        return f"{self.schema}.{self.tables.get('option_daily') or 'CN_OPTION_SSE_DAILY'}"

    def _universe_table_ref(self, use_mysql: bool) -> str:
        if use_mysql:
            return self.mysql_universe_table
        return f"{self.schema}.{self.tables.get('universe') or 'CN_UNIVERSE_SYMBOLS'}"

    def _use_mysql_stock(self) -> bool:
        return self.mysql_engine is not None

    def _require_mysql(self, table_name: str):
        if self.mysql_engine is None:
            raise RuntimeError(
                f"MySQL is required for table {table_name}, but mysql engine is not available"
            )

    def _can_fallback_oracle_stock(self) -> bool:
        return self.mysql_stock_oracle_fallback

    # ==================================================
    # stock daily prices (CLOSE -> CLOSE)
    # ==================================================
    def query_stock_closes(
        self,
        window_start,
        trade_date,
    ) -> List[Tuple[str, str, Any, float]]:
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")

        params = {
            "window_start": _to_date(window_start),
            "trade_date": _to_date(trade_date),
        }
        if self._use_mysql_stock():
            sql = f"""
            SELECT
                SYMBOL      AS symbol,
                EXCHANGE    AS exchange,
                TRADE_DATE  AS trade_date,
                PRE_CLOSE   AS pre_close,
                CHG_PCT     AS chg_pct,
                CLOSE       AS close,
                AMOUNT      AS amount
            FROM {self._stock_table_ref(use_mysql=True)}
            WHERE TRADE_DATE >= :window_start
              AND TRADE_DATE <= :trade_date
            """
            return self.execute_mysql(sql, params)
        if not self._can_fallback_oracle_stock():
            raise RuntimeError(
                "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
            )

        sql = f"""
        SELECT
            SYMBOL        AS symbol,
            EXCHANGE      AS exchange,
            TRADE_DATE    AS trade_date,
            PRE_CLOSE     AS pre_close,
            CHG_PCT       AS chg_pct,
            CLOSE         AS close,
            AMOUNT        AS amount
        FROM {self.schema}.{table}
        WHERE TRADE_DATE >= :window_start
          AND TRADE_DATE <= :trade_date
        """
        return self.execute(sql, params)

    # ==================================================
    # index daily prices (CLOSE -> CLOSE)
    # ==================================================
    def query_index_closes(
        self,
        index_code: str,
        window_start,
        trade_date,
    ) -> List[Tuple[str, Any, float]]:
        params = {
            "index_code": index_code,
            "window_start": _to_date(window_start),
            "trade_date": _to_date(trade_date),
        }
        mysql_sql = f"""
        SELECT
            INDEX_CODE    AS index_code,
            TRADE_DATE    AS trade_date,
            CLOSE         AS CLOSE
        FROM {self._index_table_ref(use_mysql=True)}
        WHERE INDEX_CODE = :index_code
          AND TRADE_DATE >= :window_start
          AND TRADE_DATE <= :trade_date
        """
        self._require_mysql("CN_INDEX_DAILY_PRICE")
        return self.execute_mysql(mysql_sql, params)

    def query_index_close_with_prev(
        self,
        index_code: str,
        trade_date,
    ) -> List[Tuple[str, Any, float, float]]:
        """Query single-day index close and pre_close.

        Used as a fallback when the index window has fewer than 2 rows.
        """
        params = {
            "index_code": index_code,
            "trade_date": _to_date(trade_date),
        }
        mysql_sql = f"""
        SELECT
            INDEX_CODE    AS index_code,
            TRADE_DATE    AS trade_date,
            PRE_CLOSE     AS pre_close,
            CLOSE         AS close
        FROM {self._index_table_ref(use_mysql=True)}
        WHERE INDEX_CODE = :index_code
          AND TRADE_DATE = :trade_date
        """
        self._require_mysql("CN_INDEX_DAILY_PRICE")
        return self.execute_mysql(mysql_sql, params)

    # ==================================================
    # ETF daily prices (CN_FUND_ETF_HIST_EM)
    # ==================================================
    def query_etf_prices(
        self,
        code: str,
        window_start,
        trade_date,
    ) -> List[Tuple[str, Any, Any, Any, Any, Any]]:
        """Query ETF daily OHLCV from CN_FUND_ETF_HIST_EM (or configured table).

        Notes:
        - code in DB is typically like 'sh.510300' / 'sz.159915'
        - table may contain multiple ADJUST_TYPE rows per day; we pick one per day
          by priority: POST > qfq > others.
        """
        params = {
            "code": code,
            "window_start": _to_date(window_start),
            "trade_date": _to_date(trade_date),
        }
        mysql_sql = f"""
        SELECT
            CODE        AS code,
            DATA_DATE   AS trade_date,
            OPEN_PRICE  AS open,
            HIGH_PRICE  AS high,
            LOW_PRICE   AS low,
            CLOSE_PRICE AS close,
            VOLUME      AS volume
        FROM (
            SELECT
                t.*,
                ROW_NUMBER() OVER (
                    PARTITION BY t.CODE, t.DATA_DATE
                    ORDER BY CASE
                        WHEN LOWER(t.ADJUST_TYPE) = 'post' THEN 1
                        WHEN LOWER(t.ADJUST_TYPE) = 'qfq'  THEN 2
                        ELSE 9
                    END
                ) AS rn
            FROM {self._etf_table_ref(use_mysql=True)} t
            WHERE t.CODE = :code
              AND t.DATA_DATE >= :window_start
              AND t.DATA_DATE <= :trade_date
        ) x
        WHERE x.rn = 1
        ORDER BY x.trade_date
        """
        self._require_mysql("CN_FUND_ETF_HIST_EM")
        return self.execute_mysql(mysql_sql, params)

    # ==================================================
    # universe symbols (industry mapping)
    # ==================================================
    def query_universe_symbols(self):
        sql = f"""
        SELECT
            SYMBOL   AS symbol,
            EXCHANGE AS exchange,
            SW_L1    AS sw_l1
        FROM {self._universe_table_ref(use_mysql=True)}
        """
        self._require_mysql("CN_UNIVERSE_SYMBOLS")
        return self.execute_mysql(sql)


    def fetch_daily_amount_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        鑾峰彇鎸囧畾鏃ユ湡鍖洪棿鍐呭叏甯傚満姣忔棩鎴愪氦棰濓紙浜垮厓锛夋椂闂村簭鍒?        杩斿洖 columns:
            trade_date (datetime)
            total_amount (float)  # 鍗曚綅锛氫嚎鍏冿紝宸查櫎 1e8
        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
        }

        if self._use_mysql_stock():
            sql = f"""
            SELECT
                TRADE_DATE,
                SUM(AMOUNT) AS total_amount
            FROM {self._stock_table_ref(use_mysql=True)}
            WHERE TRADE_DATE >= DATE_SUB(:start_date, INTERVAL :look_back_days DAY)
              AND TRADE_DATE <= :start_date
            GROUP BY TRADE_DATE
            ORDER BY TRADE_DATE
            """
            raw = self.execute_mysql(sql, params)
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""
            SELECT
                TRADE_DATE,
                SUM(AMOUNT) AS total_amount
            FROM {self.schema}.{table}
            WHERE TRADE_DATE >= :start_date - :look_back_days
                  AND TRADE_DATE <= :start_date
            GROUP BY TRADE_DATE
            ORDER BY TRADE_DATE
            """
            raw = self.execute(sql, params)

        if not raw:
            return pd.DataFrame(columns=["trade_date", "total_amount"])

        df = pd.DataFrame(raw, columns=["trade_date", "total_amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["total_amount"] = (df["total_amount"].astype(float) / 1e8).round(2)
        df = df[["trade_date", "total_amount"]].set_index("trade_date")

        return df    
# #core/adapters/providers/db_provider_oracle.py
# 閺傛澘顤冩禒銉ょ瑓閺傝纭堕敍鍫熸杹閸︺劎琚崘鍛村劥閸氬牓鈧倷缍呯純顕嗙礆

    def fetch_stock_daily_chg_pct_raw(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        鑾峰彇鎸囧畾浜ゆ槗鏃ュ線鍓?look_back_days 澶╁唴鐨勬瘡鏃ュ競鍦烘儏缁仛鍚堟暟鎹€?        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
            "eps": 0.0001,
        }

        if self._use_mysql_stock():
            sql = f"""
            WITH base AS (
                SELECT
                    TRADE_DATE,
                    SYMBOL,
                    PRE_CLOSE,
                    CLOSE,
                    NAME
                FROM {self._stock_table_ref(use_mysql=True)}
                WHERE TRADE_DATE >= DATE_SUB(:start_date, INTERVAL :look_back_days DAY)
                  AND TRADE_DATE <= :start_date
                  AND CLOSE IS NOT NULL
                  AND PRE_CLOSE IS NOT NULL
                  AND PRE_CLOSE > 0
            ),
            calc AS (
                SELECT
                    TRADE_DATE,
                    PRE_CLOSE,
                    CLOSE,
                    CASE
                        WHEN SUBSTR(SYMBOL, 1, 3) IN ('300','301','688','689') THEN 0.20
                        WHEN SUBSTR(SYMBOL, 1, 1) = '8'
                          OR SUBSTR(SYMBOL, 1, 2) IN ('43','83','87') THEN 0.30
                        WHEN NAME IS NOT NULL AND (
                            UPPER(TRIM(NAME)) LIKE '*ST%' OR UPPER(TRIM(NAME)) LIKE 'ST%'
                        ) THEN 0.05
                        ELSE 0.10
                    END AS limit_frac
                FROM base
            )
            SELECT
                TRADE_DATE,
                COUNT(*) AS total_stocks,
                SUM(CASE WHEN CLOSE > PRE_CLOSE THEN 1 ELSE 0 END) AS adv,
                SUM(CASE WHEN CLOSE < PRE_CLOSE THEN 1 ELSE 0 END) AS dec_cnt,
                SUM(CASE WHEN CLOSE = PRE_CLOSE THEN 1 ELSE 0 END) AS flat,
                SUM(CASE WHEN CLOSE >= ROUND(PRE_CLOSE * (1 + limit_frac), 2) - :eps THEN 1 ELSE 0 END) AS limit_up,
                SUM(CASE WHEN CLOSE <= ROUND(PRE_CLOSE * (1 - limit_frac), 2) + :eps THEN 1 ELSE 0 END) AS limit_down,
                ROUND(SUM(CASE WHEN CLOSE > PRE_CLOSE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS adv_ratio
            FROM calc
            GROUP BY TRADE_DATE
            ORDER BY TRADE_DATE DESC
            LIMIT 30
            """
            raw = self.execute_mysql(sql, params)
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""
            WITH base AS (
                SELECT
                    TRADE_DATE,
                    SYMBOL,
                    PRE_CLOSE,
                    CLOSE,
                    NAME
                FROM {self.schema}.{table}
                WHERE TRADE_DATE >= TRUNC(:start_date) - :look_back_days
                  AND TRADE_DATE <= TRUNC(:start_date)
                  AND CLOSE IS NOT NULL
                  AND PRE_CLOSE IS NOT NULL
                  AND PRE_CLOSE > 0
            ),
            calc AS (
                SELECT
                    TRADE_DATE,
                    PRE_CLOSE,
                    CLOSE,
                    CASE
                        WHEN SUBSTR(SYMBOL, 1, 3) IN ('300','301','688','689') THEN 0.20
                        WHEN SUBSTR(SYMBOL, 1, 1) = '8'
                          OR SUBSTR(SYMBOL, 1, 2) IN ('43','83','87') THEN 0.30
                        WHEN NAME IS NOT NULL AND (
                            UPPER(TRIM(NAME)) LIKE '*ST%' OR UPPER(TRIM(NAME)) LIKE 'ST%'
                        ) THEN 0.05
                        ELSE 0.10
                    END AS limit_frac
                FROM base
            )
            SELECT
                TRADE_DATE,
                COUNT(*) AS total_stocks,
                SUM(CASE WHEN CLOSE > PRE_CLOSE THEN 1 ELSE 0 END) AS adv,
                SUM(CASE WHEN CLOSE < PRE_CLOSE THEN 1 ELSE 0 END) AS dec,
                SUM(CASE WHEN CLOSE = PRE_CLOSE THEN 1 ELSE 0 END) AS flat,
                SUM(CASE WHEN CLOSE >= ROUND(PRE_CLOSE * (1 + limit_frac), 2) - :eps THEN 1 ELSE 0 END) AS limit_up,
                SUM(CASE WHEN CLOSE <= ROUND(PRE_CLOSE * (1 - limit_frac), 2) + :eps THEN 1 ELSE 0 END) AS limit_down,
                ROUND(SUM(CASE WHEN CLOSE > PRE_CLOSE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS adv_ratio
            FROM calc
            GROUP BY TRADE_DATE
            ORDER BY TRADE_DATE DESC
            FETCH FIRST 30 ROWS ONLY
            """
            raw = self.execute(sql, params)

        if not raw:
            LOG.warning("[DBProvider] fetch_stock_daily_sentiment_stats returned no data for %s", start_date)
            return pd.DataFrame(columns=[
                "trade_date", "total_stocks", "adv", "dec", "flat",
                "limit_up", "limit_down", "adv_ratio"
            ])

        df = pd.DataFrame(
            raw,
            columns=[
                "trade_date", "total_stocks", "adv", "dec", "flat",
                "limit_up", "limit_down", "adv_ratio"
            ]
        )
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        LOG.info(
            "[DBProvider] fetch_stock_daily_sentiment_stats success: %d trading days for %s",
            len(df),
            start_date,
        )

        return df


    def fetch_daily_new_low_stats(
        self,
        trade_date: str,
        look_back_days: int = 150,
    ) -> pd.DataFrame:
        """
        鑾峰彇鎸囧畾浜ゆ槗鏃ュ墠 look_back_days 鍐呯殑鈥?0鏃ユ柊浣庘€濆箍搴︾粺璁°€?        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

        params = {
            "trade_date": _to_date(trade_date),
            "look_back_days": look_back_days,
        }

        if self._use_mysql_stock():
            sql = f"""
            WITH daily_data AS (
                SELECT
                    TRADE_DATE,
                    SYMBOL,
                    CLOSE
                FROM {self._stock_table_ref(use_mysql=True)}
                WHERE TRADE_DATE >= DATE_SUB(:trade_date, INTERVAL :look_back_days DAY)
                  AND TRADE_DATE <= :trade_date
                  AND CLOSE IS NOT NULL
            ),
            with_50d_low AS (
                SELECT
                    TRADE_DATE,
                    SYMBOL,
                    CLOSE,
                    MIN(CLOSE) OVER (
                        PARTITION BY SYMBOL
                        ORDER BY TRADE_DATE
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS low_50d
                FROM daily_data
            )
            SELECT
                TRADE_DATE,
                COUNT(*) AS count_total,
                COUNT(CASE WHEN CLOSE = low_50d THEN 1 END) AS count_new_low_50d,
                ROUND(
                    COUNT(CASE WHEN CLOSE = low_50d THEN 1 END) * 100.0 / COUNT(*),
                    2
                ) AS new_low_50d_ratio
            FROM with_50d_low
            GROUP BY TRADE_DATE
            ORDER BY TRADE_DATE DESC
            LIMIT 30
            """
            raw = self.execute_mysql(sql, params)
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""
            WITH daily_data AS (
                SELECT
                    TRADE_DATE,
                    SYMBOL,
                    CLOSE
                FROM {self.schema}.{table}
                WHERE TRADE_DATE >= TRUNC(:trade_date) - :look_back_days
                  AND TRADE_DATE <= TRUNC(:trade_date)
                  AND CLOSE IS NOT NULL
            ),
            with_50d_low AS (
                SELECT
                    TRADE_DATE,
                    SYMBOL,
                    CLOSE,
                    MIN(CLOSE) OVER (
                        PARTITION BY SYMBOL
                        ORDER BY TRADE_DATE
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS low_50d
                FROM daily_data
            )
            SELECT
                TRADE_DATE,
                COUNT(*) AS count_total,
                COUNT(CASE WHEN CLOSE = low_50d THEN 1 END) AS count_new_low_50d,
                ROUND(
                    COUNT(CASE WHEN CLOSE = low_50d THEN 1 END) * 100.0 / COUNT(*),
                    2
                ) AS new_low_50d_ratio
            FROM with_50d_low
            GROUP BY TRADE_DATE
            ORDER BY TRADE_DATE DESC
            FETCH FIRST 30 ROWS ONLY
            """
            raw = self.execute(sql, params)

        if not raw:
            LOG.warning("[DBProvider] fetch_daily_new_low_stats returned no data for %s", trade_date)
            return pd.DataFrame(columns=["trade_date", "count_total", "count_new_low_50d", "new_low_50d_ratio"])

        df = pd.DataFrame(
            raw,
            columns=["trade_date", "count_total", "count_new_low_50d", "new_low_50d_ratio"]
        )
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        LOG.info(
            "[DBProvider] fetch_daily_new_low_stats success: %d trading days for %s (50-day new low)",
            len(df),
            trade_date,
        )

        return df

    def load_full_market_eod_snapshot(
        self,
        trade_date,
    ) -> Dict[str, Any]:
        """
        Load confirmed full-market EOD snapshot (T-1) from oracle DB.
    
        Contract (frozen):
        - trade_date: confirmed trading day (T-1)
        - source: oracle DB only (no network)
        - snapshot_type: EOD
        - full-market coverage
        - replayable / deterministic
        """
        # 1) confirmed single-day snapshot (T-1)
        rows = self.query_stock_closes(
            window_start=trade_date,
            trade_date=trade_date,
        )
        if not rows:
            raise RuntimeError(f"no EOD stock closes found for {trade_date}")

        # 2) assemble full-market snapshot (no inference)
        market: Dict[str, Dict[str, Any]] = {}
        for symbol, exchange, td, pre_close, chg_pct, close, amount in rows:
            market[symbol] = {
                "symbol": symbol,
                "exchange": exchange,
                "trade_date": td,
                "close": close,
                "pre_close": pre_close,
                "chg_pct": chg_pct,
                "amount": amount,
            }

        # 3) return confirmed snapshot
        return {
            "trade_date": trade_date,
            "snapshot_type": "EOD",
            "market": market,
            "_meta": {
                "source": "mysql" if self._use_mysql_stock() else "oracle",
                "confirmed": True,
                "record_count": len(market),
            },
        }
    
    def query_last_trade_date(self, as_of_date) -> str:
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")
        params = {"as_of_date": _to_date(as_of_date)}
        if self._use_mysql_stock():
            sql = f"""
            SELECT MAX(TRADE_DATE) AS last_trade_date
            FROM {self._stock_table_ref(use_mysql=True)}
            WHERE TRADE_DATE <= :as_of_date
            """
            rows = self.execute_mysql(sql, params)
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""
            SELECT MAX(TRADE_DATE) AS last_trade_date
            FROM {self.schema}.{table}
            WHERE TRADE_DATE <= :as_of_date
            """
            rows = self.execute(sql, params)
        if not rows or rows[0][0] is None:
            raise RuntimeError(f"no trade_date found <= {as_of_date}")
        return rows[0][0]    
        
    def load_latest_full_market_eod_snapshot(
        self,
        as_of_date,
    ) -> Dict[str, Any]:
        """
        Load latest confirmed full-market EOD snapshot
        as of a given date (usually PRE_OPEN uses T).
    
        Contract (FROZEN):
        - as_of_date: evaluation date (e.g. today T)
        - internally resolves last_trade_date <= as_of_date
        - returns confirmed EOD snapshot for last_trade_date
        """
    
        last_trade_date = self.query_last_trade_date(as_of_date)
    
        snapshot = self.load_full_market_eod_snapshot(last_trade_date)
    
        # 閺勫海鈥橀弽鍥ㄦ暈 as_of 鐠囶厺绠熼敍鍫ユ姜鐢悂鍣哥憰渚婄礉娓氬じ绨€孤ゎ吀 / 閸ョ偞鏂侀敍?        snapshot["_meta"] = snapshot.get("_meta", {})
        snapshot["_meta"].update(
            {
                "as_of_date": _to_date(as_of_date),
                "resolved_trade_date": last_trade_date,
            }
        )
    
        return snapshot

    # ==================================================
    # ETF 閺冦儴顢戦幆鍛颁粵閸氬牊妞傞梻鏉戠碍閸掓绱機 Block閿?    # ==================================================
    def fetch_etf_hist_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        娴犲骸鐔€闁?ETF 閺冦儴顢戦幆鍛般€冮幓鎰絿閹稿洤鐣剧粣妤€褰涢崘鍛畱閼辨艾鎮庢惔蹇撳灙閵?
        鏉堟挸鍤?DataFrame index=trade_date閿涘潐atetime閿涘绱濈€涙顔岄敍?            total_change_amount: 瑜版挻妫╂禒閿嬬壐濞戙劏绌兼０婵呯閸?            total_volume: 瑜版挻妫╅幋鎰唉闁插繋绠ｉ崪?            total_amount: 瑜版挻妫╅幋鎰唉妫版繀绠ｉ崪?
        閸欏倹鏆熺拠瀛樻閿?            start_date: 缂佹挻娼弮銉︽埂閿涘牆瀵橀幏顒冾嚉閺冦儻绱?            look_back_days: 閸ョ偞鍑芥径鈺傛殶閿涘牆鎯?start_date 閸︺劌鍞撮敍?
        濞夈劍鍓伴敍?        - 閺傝纭堕柆闈涙儕 DBOracleProvider 缁撅箑鐣鹃敍灞肩瑝娴ｈ法鏁?TO_DATE
        - 鐞涖劌鎮曢悽?config.yaml 娑?db.oracle.tables.fund_etf_hist 闁板秶鐤?          閼汇儲婀柊宥囩枂閿涘苯鍨妯款吇娑?"CN_FUND_ETF_HIST_EM"
        """
        # Build per-day aggregated price-change/volume/amount series.
        sql = f"""
        SELECT
            DATA_DATE   AS trade_date,
            SUM(COALESCE(CHANGE_AMOUNT, 0)) AS total_change_amount,
            SUM(COALESCE(VOLUME, 0))        AS total_volume,
            SUM(COALESCE(AMOUNT, 0))        AS total_amount
        FROM {self._etf_table_ref(use_mysql=True)}
        WHERE DATA_DATE >= DATE_SUB(:start_date, INTERVAL :look_back_days DAY)
          AND DATA_DATE <= :start_date
        GROUP BY DATA_DATE
        ORDER BY DATA_DATE
        """
        params = {
            "start_date": _to_date(start_date),
            "look_back_days": int(look_back_days),
        }
        self._require_mysql("CN_FUND_ETF_HIST_EM")
        raw = self.execute_mysql(sql, params)
        if not raw:
            return pd.DataFrame(columns=["trade_date", "total_change_amount", "total_volume", "total_amount"])
        df = pd.DataFrame(raw, columns=["trade_date", "total_change_amount", "total_volume", "total_amount"])
        # 鏉烆剚宕查弮銉︽埂缁鐎?        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # Type conversion
        df["total_change_amount"] = df["total_change_amount"].astype(float)
        df["total_volume"] = df["total_volume"].astype(float)
        df["total_amount"] = df["total_amount"].astype(float)
        df = df.set_index("trade_date")
        return df

    # ==================================================
    # Futures basis series (D Block)
    # ==================================================
    def fetch_futures_basis_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        娴犲氦鍋傞幐鍥ㄦ埂鐠愌勬）鐞涘本鍎忕悰銊ユ嫲閹稿洦鏆熼弮銉攽閹懓銆冮幓鎰絿閸╁搫妯婇弮鍫曟？鎼村繐鍨妴?
        鏉堟挸鍤?DataFrame index=trade_date閿涘潐atetime閿涘绱濈€涙顔岄敍?            avg_basis:   閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕唨瀹割喖娼庨崐纭风礄閺堢喕鎻ｆ禒閿嬬壐 - 閹稿洦鏆熼弨鍓佹磸娴犲嚖绱?            total_basis: 閹鐔€瀹割噯绱欐稉宥呭閺夊喛绱?            basis_ratio: 閸╁搫妯婃稉搴㈠瘹閺佹澘濮為弶鍐╂暪閻╂ü鐜稊瀣槷
            total_volume: 閹粯鍨氭禍銈夊櫤閿涘牏鏁ゆ禍搴″閺夊喛绱?            weighted_future_price: 閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕埂鐠愌傜幆閺?            weighted_index_price:  閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕箛鐠愌勫瘹閺侀鐜弽?
        閸欏倹鏆熺拠瀛樻閿?            start_date: 缂佹挻娼弮銉︽埂閿涘牆瀵橀幏顒冾嚉閺冦儻绱?            look_back_days: 閸ョ偞鍑芥径鈺傛殶閿涘牆鎯?start_date 閸︺劌鍞撮敍?
        濞夈劍鍓伴敍?        - 娴犲懓浠涢崥鍫濇惂缁?IF/IH/IC/IM閿涘苯顕惔鏃€鍕冨ǎ?00閵嗕椒绗傜拠?0閵嗕椒鑵戠拠?00閵嗕椒鑵戠拠?000閹稿洦鏆熼妴?        - 閸╁搫妯婂锝呪偓鑹般€冪粈鐑樻埂鐠愌冨磳濮樿揪绱濈拹鐔封偓鑹般€冪粈鐑樻埂鐠愌嗗垱濮樻番鈧?        """
        # 閺嬪嫰鈧姵鐓＄拠顫窗鐠恒劏銆?join 閹稿鍨氭禍銈夊櫤閸旂姵娼堢拋锛勭暬閸╁搫妯婇崪灞肩幆閺?        # 娴ｈ法鏁ょ紒鎴濈暰闁灝鍘?TO_DATE
        mysql_sql = f"""
        SELECT
            t.TRADE_DATE                AS trade_date,
            SUM((COALESCE(t.SETTLE_PRICE, t.CLOSE_PRICE) - idx.CLOSE) * t.VOLUME) / NULLIF(SUM(t.VOLUME), 0) AS avg_basis,
            SUM((COALESCE(t.SETTLE_PRICE, t.CLOSE_PRICE) - idx.CLOSE))                                        AS total_basis,
            SUM(t.VOLUME)                                                                                      AS total_volume,
            SUM(COALESCE(t.SETTLE_PRICE, t.CLOSE_PRICE) * t.VOLUME) / NULLIF(SUM(t.VOLUME),0)                AS weighted_future_price,
            SUM(idx.CLOSE * t.VOLUME) / NULLIF(SUM(t.VOLUME),0)                                               AS weighted_index_price
        FROM {self._fut_table_ref(use_mysql=True)} t
        JOIN {self._index_table_ref(use_mysql=True)} idx
          ON idx.TRADE_DATE = t.TRADE_DATE
         AND idx.INDEX_CODE = (
              CASE
                  WHEN t.VARIETY = 'IF' THEN 'sh000300'
                  WHEN t.VARIETY = 'IH' THEN 'sh000016'
                  WHEN t.VARIETY = 'IC' THEN 'sh000905'
                  WHEN t.VARIETY = 'IM' THEN 'sh000852'
              END
          )
        WHERE t.VARIETY IN ('IF','IH','IC','IM')
          AND t.TRADE_DATE >= DATE_SUB(:start_date, INTERVAL :look_back_days DAY)
          AND t.TRADE_DATE <= :start_date
        GROUP BY t.TRADE_DATE
        ORDER BY t.TRADE_DATE
        """
        params = {
            "start_date": _to_date(start_date),
            "look_back_days": int(look_back_days),
        }
        self._require_mysql("CN_FUT_INDEX_HIS/CN_INDEX_DAILY_PRICE")
        raw = self.execute_mysql(mysql_sql, params)
        if not raw:
            return pd.DataFrame(columns=[
                "trade_date",
                "avg_basis",
                "total_basis",
                "basis_ratio",
                "total_volume",
                "weighted_future_price",
                "weighted_index_price",
            ])

        df = pd.DataFrame(raw, columns=[
            "trade_date",
            "avg_basis",
            "total_basis",
            "total_volume",
            "weighted_future_price",
            "weighted_index_price",
        ])
        # 鐠侊紕鐣?ratio = avg_basis / weighted_index_price
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["avg_basis"] = df["avg_basis"].astype(float)
        df["total_basis"] = df["total_basis"].astype(float)
        df["total_volume"] = df["total_volume"].astype(float)
        df["weighted_future_price"] = df["weighted_future_price"].astype(float)
        df["weighted_index_price"] = df["weighted_index_price"].astype(float)
        # Avoid divide-by-zero; ratio None if weighted_index_price is zero or null
        df["basis_ratio"] = df.apply(
            lambda row: (row["avg_basis"] / row["weighted_index_price"]) if row["weighted_index_price"] not in (None, 0) else None,
            axis=1,
        )
        df = df.set_index("trade_date")
        return df

    # ==================================================
    # Options risk series (E Block)
    # ==================================================
    def fetch_options_risk_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        娴?ETF 閺堢喐娼堥弮銉攽閹懓銆冮懕姘値鐠侊紕鐣婚張鐔告綀妞嬪酣娅撻惄绋垮彠閹稿洦鐖ｉ妴?
        鏉堟挸鍤?DataFrame index=trade_date閿涘潐atetime閿涘绱濈€涙顔岄敍?            weighted_change: 閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕畾鐠哄矂顤傞崸鍥р偓?                           閿涘澃um((close - prev_close) * volume) / sum(volume)閿?            total_change:    閸氬牏瀹冲☉銊ㄧ┘妫版繃鐪伴崪?            total_volume:    閹存劒姘﹂柌蹇旂湴閸?            weighted_close:  閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕暪閻╂ü鐜?            change_ratio:    weighted_change / weighted_close

        閸欏倹鏆熺拠瀛樻閿?            start_date: 缂佹挻娼弮銉︽埂閿涘牆瀵橀幏顒冾嚉閺冦儻绱?            look_back_days: 閸ョ偞鍑芥径鈺傛殶閿涘牆鎯?start_date 閸︺劌鍞撮敍?
        濞夈劍鍓伴敍?        - 娴犲懓浠涢崥鍫ュ帳缂冾喕鑵戦惃?ETF 閺堢喐娼堥弽鍥╂畱閿涘牐顫?etf_codes 閸掓銆冮敍澶堚偓?        - 閻㈠彉绨柈銊ュ瀻閸忓秷鍨傚┃?閸忋儱绨辩悰銊ュ讲閼宠姤鐥呴張?CHANGE_AMOUNT/CHANGE_PCT 鐎涙顔岄敍?          閺堫剚鏌熷▔鏇氬▏閻?Oracle 缁愭褰涢崙鑺ユ殶 LAG(CLOSE_PRICE) 閸?SQL 閸愬懎宓嗛弮鎯邦吀缁犳瀹氱捄宀勵杺閵?        - 瑜版挻鈧粯鍨氭禍銈夊櫤娑?0 閺冭绱漺eighted_change閵嗕簚eighted_close 閸?ratio 缂佹挻鐏夋稉?None閵?        """
        # ETF 閺堢喐娼堥弽鍥╂畱閸掓銆冮敍鍫濇祼鐎规熬绱?        etf_codes = [
            '510050',  # 閸楀骸顦存稉濠呯槈50ETF
            '510300',  # 閸楀孩鍢查弻蹇曟喓濞岊亝绻?00ETF
            '510500',  # 閸楁鏌熸稉顓＄槈500ETF
            '588000',  # 閸楀骸顦寸粔鎴濆灡50ETF
            '588080',  # 閺勬挻鏌熸潏鍓ь潠閸?0ETF
            '159919',  # 閸㈠鐤勫▽顏呯箒300ETF (濞ｅ崬绔?
            '159922',  # 閸㈠鐤勬稉顓＄槈500ETF (濞ｅ崬绔?
            '159915',  # 閺勬挻鏌熸潏鎯у灡娑撴碍婢楨TF
            '159901',  # 閺勬挻鏌熸潏鐐箒鐠?00ETF
        ]
        # 閺嬪嫰鈧?IN 閸掓銆?        in_list = ",".join([f"'{code}'" for code in etf_codes])
        # IMPORTANT (FROZEN):
        # - No TO_DATE in SQL; bind python date to Oracle DATE
        # - Compute change_amount from CLOSE_PRICE using LAG to avoid requiring CHANGE_AMOUNT column
        # - Pull a few extra days to reduce boundary effects when computing LAG
        mysql_sql = f"""
        WITH base AS (
            SELECT
                t.CONTRACT_CODE AS contract_code,
                t.DATA_DATE     AS trade_date,
                t.CLOSE_PRICE   AS close_price,
                COALESCE(t.VOLUME, 0) AS volume,
                LAG(t.CLOSE_PRICE) OVER (
                    PARTITION BY t.CONTRACT_CODE
                    ORDER BY t.DATA_DATE
                ) AS prev_close
            FROM {self._option_table_ref(use_mysql=True)} t
            WHERE t.UNDERLYING_CODE IN ({in_list})
              AND t.CLOSE_PRICE IS NOT NULL
              AND t.DATA_DATE >= DATE_SUB(:start_date, INTERVAL :look_back_days_plus DAY)
              AND t.DATA_DATE <= :start_date
        ),
        calc AS (
            SELECT
                trade_date,
                (close_price - prev_close) AS change_amount,
                close_price,
                volume
            FROM base
        )
        SELECT
            trade_date,
            SUM(COALESCE(change_amount, 0) * volume) / NULLIF(SUM(volume), 0) AS weighted_change,
            SUM(COALESCE(change_amount, 0)) AS total_change,
            SUM(volume) AS total_volume,
            SUM(close_price * volume) / NULLIF(SUM(volume), 0) AS weighted_close
        FROM calc
        WHERE trade_date >= DATE_SUB(:start_date, INTERVAL :look_back_days DAY)
        GROUP BY trade_date
        ORDER BY trade_date
        """
        params = {
            "start_date": _to_date(start_date),
            "look_back_days": int(look_back_days),
            "look_back_days_plus": int(look_back_days) + 10,
        }
        self._require_mysql("CN_OPTION_SSE_DAILY")
        raw = self.execute_mysql(mysql_sql, params)
        if not raw:
            return pd.DataFrame(columns=[
                "trade_date",
                "weighted_change",
                "total_change",
                "total_volume",
                "weighted_close",
                "change_ratio",
            ])
        df = pd.DataFrame(raw, columns=[
            "trade_date",
            "weighted_change",
            "total_change",
            "total_volume",
            "weighted_close",
        ])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # Convert to float (NULL -> NaN)
        df["weighted_change"] = pd.to_numeric(df["weighted_change"], errors="coerce")
        df["total_change"] = pd.to_numeric(df["total_change"], errors="coerce")
        df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")
        df["weighted_close"] = pd.to_numeric(df["weighted_close"], errors="coerce")
        # 鐠侊紕鐣?ratio閿涘矂浼╅崗宥夋珟 0
        def _calc_ratio(row):
            wc = row["weighted_change"]
            wclose = row["weighted_close"]
            if wclose is None or wclose == 0:
                return None
            try:
                return wc / wclose
            except Exception:
                return None
        df["change_ratio"] = df.apply(_calc_ratio, axis=1)
        df = df.set_index("trade_date")
        return df

    # ------------------------------------------------------------------
    # Leading-Structure (WatchlistLead) - BreadthPlus helpers
    # ------------------------------------------------------------------
    def fetch_advdec_series(self, asof_date, look_back_days: int = 30) -> Dict[str, Any]:
        """Fetch market adv/dec series from local daily price table.

        Returns a dict with a stable shape for BreadthPlusDataSource.
        """
        table = self.tables.get("stock_daily") or "CN_STOCK_DAILY_PRICE"

        if self._use_mysql_stock():
            sql = f"""
                SELECT
                    TRADE_DATE AS trade_date,
                    COUNT(*) AS total,
                    SUM(CASE WHEN CLOSE > PRE_CLOSE THEN 1 ELSE 0 END) AS adv,
                    SUM(CASE WHEN CLOSE < PRE_CLOSE THEN 1 ELSE 0 END) AS dec_cnt,
                    SUM(CASE WHEN CLOSE = PRE_CLOSE THEN 1 ELSE 0 END) AS flat
                FROM {self._stock_table_ref(use_mysql=True)}
                WHERE TRADE_DATE >= DATE_SUB(:asof_date, INTERVAL :look_back_days DAY)
                  AND TRADE_DATE <= :asof_date
                  AND CLOSE IS NOT NULL
                  AND PRE_CLOSE IS NOT NULL
                GROUP BY TRADE_DATE
                ORDER BY TRADE_DATE ASC
            """
            rows = self.execute_mysql(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""
                SELECT
                    TRADE_DATE AS trade_date,
                    COUNT(*) AS total,
                    SUM(CASE WHEN CLOSE > PRE_CLOSE THEN 1 ELSE 0 END) AS adv,
                    SUM(CASE WHEN CLOSE < PRE_CLOSE THEN 1 ELSE 0 END) AS dec,
                    SUM(CASE WHEN CLOSE = PRE_CLOSE THEN 1 ELSE 0 END) AS flat
                FROM {self.schema}.{table}
                WHERE TRADE_DATE >= TRUNC(:asof_date) - :look_back_days
                  AND TRADE_DATE <= TRUNC(:asof_date)
                  AND CLOSE IS NOT NULL
                  AND PRE_CLOSE IS NOT NULL
                GROUP BY TRADE_DATE
                ORDER BY TRADE_DATE ASC
            """
            rows = self.execute(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
        df = pd.DataFrame(rows)
        if df is None or df.empty:
            return {
                "asof_date": str(asof_date),
                "look_back_days": int(look_back_days),
                "series": [],
                "data_status": "MISSING",
            }

        # normalize types
        try:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        except Exception:
            pass

        if "dec" not in df.columns and "dec_cnt" in df.columns:
            df["dec"] = df["dec_cnt"]

        df["total"] = df["total"].astype(float)
        df["adv"] = df["adv"].astype(float)
        df["dec"] = df["dec"].astype(float)
        df["flat"] = df["flat"].astype(float)

        df["adv_ratio"] = df.apply(lambda r: (r["adv"] / r["total"]) if r["total"] else 0.0, axis=1)
        df["ad"] = df["adv"] - df["dec"]
        df["ad_line"] = df["ad"].cumsum()

        series = []
        for _, r in df.iterrows():
            series.append(
                {
                    "trade_date": str(r.get("trade_date")),
                    "total": float(r.get("total") or 0.0),
                    "adv": float(r.get("adv") or 0.0),
                    "dec": float(r.get("dec") or 0.0),
                    "flat": float(r.get("flat") or 0.0),
                    "adv_ratio_pct": round(float(r.get("adv_ratio") or 0.0) * 100.0, 2),
                    "ad": float(r.get("ad") or 0.0),
                    "ad_line": float(r.get("ad_line") or 0.0),
                }
            )

        ad_last = float(df["ad_line"].iloc[-1]) if len(df) else 0.0
        ad_5d = None
        ad_20d = None
        if len(df) >= 6:
            ad_5d = float(df["ad_line"].iloc[-1] - df["ad_line"].iloc[-6])
        if len(df) >= 21:
            ad_20d = float(df["ad_line"].iloc[-1] - df["ad_line"].iloc[-21])

        return {
            "asof_date": str(asof_date),
            "look_back_days": int(look_back_days),
            "series": series,
            "ad_line_last": ad_last,
            "ad_line_chg_5d": ad_5d,
            "ad_line_chg_20d": ad_20d,
            "data_status": "OK",
        }

    def fetch_breadth_plus_metrics(
        self,
        asof_date,
        look_back_days: int = 120,
        ma20_window: int = 20,
        ma50_window: int = 50,
        nhnl_window: int = 20,
    ) -> Dict[str, Any]:
        """Compute breadth+ metrics from local daily price table.

        Metrics (asof_date cross-section):
        - % above MA20 / MA50
        - 20D new highs / new lows (exclude current day when building window)
        - new highs vs new lows ratio

        Notes:
        - This method is designed to be robust and fast enough for daily runs:
          it pulls the last N calendar days for all symbols (roughly 300k~600k rows).
        """
        table = self.tables.get("stock_daily") or "CN_STOCK_DAILY_PRICE"

        if self._use_mysql_stock():
            sql = f"""
                SELECT
                    SYMBOL AS symbol,
                    TRADE_DATE AS trade_date,
                    CLOSE AS close
                FROM {self._stock_table_ref(use_mysql=True)}
                WHERE TRADE_DATE >= DATE_SUB(:asof_date, INTERVAL :look_back_days DAY)
                  AND TRADE_DATE <= :asof_date
                  AND CLOSE IS NOT NULL
                ORDER BY SYMBOL ASC, TRADE_DATE ASC
            """
            rows = self.execute_mysql(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""
                SELECT
                    SYMBOL AS symbol,
                    TRADE_DATE AS trade_date,
                    CLOSE AS close
                FROM {self.schema}.{table}
                WHERE TRADE_DATE >= TRUNC(:asof_date) - :look_back_days
                  AND TRADE_DATE <= TRUNC(:asof_date)
                  AND CLOSE IS NOT NULL
                ORDER BY SYMBOL ASC, TRADE_DATE ASC
            """
            rows = self.execute(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
        df = pd.DataFrame(rows)
        if df is None or df.empty:
            return {
                "asof_date": str(asof_date),
                "data_status": "MISSING",
                "reason": "empty_price_window",
            }

        # normalize types
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["symbol", "trade_date", "close"]).copy()
        if df.empty:
            return {
                "asof_date": str(asof_date),
                "data_status": "MISSING",
                "reason": "no_valid_rows",
            }

        # rolling computations per symbol
        g = df.groupby("symbol", sort=False)["close"]
        df["ma20"] = g.transform(lambda s: s.rolling(int(ma20_window), min_periods=int(ma20_window)).mean())
        df["ma50"] = g.transform(lambda s: s.rolling(int(ma50_window), min_periods=int(ma50_window)).mean())

        # new high/low window based on prior N days (exclude current day)
        df["max_prev"] = g.apply(
            lambda s: s.shift(1).rolling(int(nhnl_window), min_periods=int(nhnl_window)).max()
        ).reset_index(level=0, drop=True)
        df["min_prev"] = g.apply(
            lambda s: s.shift(1).rolling(int(nhnl_window), min_periods=int(nhnl_window)).min()
        ).reset_index(level=0, drop=True)

        # asof cross-section
        asof_str = str(asof_date)
        d0 = df[df["trade_date"] == pd.to_datetime(asof_str).date()].copy()
        if d0.empty:
            return {
                "asof_date": asof_str,
                "data_status": "MISSING",
                "reason": "asof_not_in_window",
            }

        total = int(len(d0))
        valid_ma20 = int(d0["ma20"].notna().sum())
        valid_ma50 = int(d0["ma50"].notna().sum())
        valid_nhnl = int(d0["max_prev"].notna().sum())

        above_ma20 = int(((d0["close"] > d0["ma20"]) & d0["ma20"].notna()).sum())
        above_ma50 = int(((d0["close"] > d0["ma50"]) & d0["ma50"].notna()).sum())

        new_high = int(((d0["close"] >= d0["max_prev"]) & d0["max_prev"].notna()).sum())
        new_low = int(((d0["close"] <= d0["min_prev"]) & d0["min_prev"].notna()).sum())

        pct_above_ma20 = round((above_ma20 / valid_ma20 * 100.0) if valid_ma20 else 0.0, 2)
        pct_above_ma50 = round((above_ma50 / valid_ma50 * 100.0) if valid_ma50 else 0.0, 2)

        nhnl_ratio = None
        if new_low > 0:
            nhnl_ratio = round(float(new_high) / float(new_low), 4)

        return {
            "asof_date": asof_str,
            "data_status": "OK",
            "window": {
                "look_back_days": int(look_back_days),
                "ma20_window": int(ma20_window),
                "ma50_window": int(ma50_window),
                "nhnl_window": int(nhnl_window),
            },
            "coverage": {
                "total": total,
                "valid_ma20": valid_ma20,
                "valid_ma50": valid_ma50,
                "valid_nhnl": valid_nhnl,
            },
            "pct_above_ma20_pct": pct_above_ma20,
            "pct_above_ma50_pct": pct_above_ma50,
            "new_high": new_high,
            "new_low": new_low,
            "new_high_to_low_ratio": nhnl_ratio,
        }


# ==================================================
# Market-data Provider adapter: DB-first with YF fallback
# ==================================================

def _normalize_etf_db_code(symbol: str) -> str:
    """Normalize ETF symbol to DB CODE format.

    Accepts:
      - "510300.SS" -> "sh.510300"
      - "159915.SZ" -> "sz.159915"
      - "sh.510300" / "sz.159915" -> unchanged
      - "510300" -> infer by prefix (5 => sh., 1 => sz.)
    """
    s = (symbol or "").strip()
    if not s:
        return s

    sl = s.lower()
    if sl.startswith("sh.") or sl.startswith("sz."):
        return sl
    if sl.startswith("sh") and len(sl) >= 8 and sl[2:8].isdigit() and not sl.startswith("sh."):
        return f"sh.{sl[2:8]}"
    if sl.startswith("sz") and len(sl) >= 8 and sl[2:8].isdigit() and not sl.startswith("sz."):
        return f"sz.{sl[2:8]}"


    # handle db style without dot: sh510300 / sz159915
    if (sl.startswith('sh') or sl.startswith('sz')) and len(''.join([c for c in sl if c.isdigit()])) == 6:
        prefix = sl[:2]
        digits = ''.join([c for c in sl if c.isdigit()])
        return f"{prefix}.{digits}"

    # handle yfinance-style suffix
    if sl.endswith(".ss"):
        core = sl[:-3]
        core = "".join([c for c in core if c.isdigit()])
        return f"sh.{core}" if core else sl
    if sl.endswith(".sz"):
        core = sl[:-3]
        core = "".join([c for c in core if c.isdigit()])
        return f"sz.{core}" if core else sl

    digits = "".join([c for c in sl if c.isdigit()])
    if len(digits) == 6:
        if digits.startswith("5"):
            return f"sh.{digits}"
        if digits.startswith("1"):
            return f"sz.{digits}"
    return sl


class DBMarketProvider:
    """ProviderRouter 'db' provider.

    DB-first: query local Oracle tables.
    Fallback: yfinance (YFProvider).

    Contract: returns a DataFrame with columns: date/open/high/low/close/volume
    (pct computed by ProviderBase.normalize_df).
    """

    def __init__(self):
        from core.adapters.providers.provider_base import ProviderBase  # local import

        class _Impl(ProviderBase):
            def __init__(self):
                super().__init__(name="db")
                self._db = DBOracleProvider()
                self._yf = None

            def _ensure_yf(self):
                if self._yf is None:
                    from core.adapters.providers.provider_yf import YFProvider

                    self._yf = YFProvider()

            def fetch_series_raw(self, symbol: str, window: int, method: str = "default"):
                m = (method or "default").strip().lower()
                window = int(window) if window and int(window) > 0 else 60

                # Calendar backoff to cover window without needing a calendar table
                trade_date = pd.Timestamp("today").date()
                # If caller passed a synthetic symbol like '..._YYYY-MM-DD', ignore here.
                start_dt = (pd.Timestamp(trade_date) - pd.Timedelta(days=window * 3)).date()

                try:
                    if m in ("index", "idx"):
                        # Normalize yfinance-style symbols like '000300.SS' / '399001.SZ' -> '000300' / '399001'
                        idx_code = symbol
                        #if isinstance(idx_code, str) and idx_code.endswith(('.SS', '.SZ')) and '.' in idx_code:
                        #    idx_code = idx_code.split('.', 1)[0]
                        if isinstance(idx_code, str) and idx_code.endswith('.SS') and '.' in idx_code:
                           
                            idx_code = 'sh' + idx_code.split('.', 1)[0]
                        elif isinstance(idx_code, str) and idx_code.endswith('.SZ') and '.' in idx_code:
                           
                            idx_code = 'sz' + idx_code.split('.', 1)[0]
                        rows = self._db.query_index_closes(index_code=idx_code, window_start=start_dt, trade_date=trade_date)
                        df = pd.DataFrame(rows, columns=["index_code", "trade_date", "close"])
                        if df.empty:
                            raise RuntimeError("empty")
                        df["date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
                        df["close"] = pd.to_numeric(df["close"], errors="coerce")
                        df = df.sort_values("date").reset_index(drop=True)
                        return df[["date", "close"]]
                    
                    if m in ("etf", "etf_hist"):
                        base_code = _normalize_etf_db_code(symbol)
                        # Support both CODE formats: 'sh.510300' and 'sh510300' (same for sz)
                        candidates = []
                        if base_code:
                            candidates.append(base_code)
                            digits = ''.join([c for c in base_code if c.isdigit()])
                            if digits:
                                if base_code.startswith('sh.'):
                                    candidates.append(f'sh{digits}')
                                elif base_code.startswith('sz.'):
                                    candidates.append(f'sz{digits}')
                                elif base_code.startswith('sh') and not base_code.startswith('sh.'):
                                    candidates.append(f'sh.{digits}')
                                elif base_code.startswith('sz') and not base_code.startswith('sz.'):
                                    candidates.append(f'sz.{digits}')
                        # de-dup while preserving order
                        seen=set(); candidates=[c for c in candidates if not (c in seen or seen.add(c))]
                        rows = []
                        for c in candidates:
                            rows = self._db.query_etf_prices(code=c, window_start=start_dt, trade_date=trade_date)
                            if rows:
                                break

                        df = pd.DataFrame(rows, columns=["code", "trade_date", "open", "high", "low", "close", "volume"])
                        if df.empty:
                            raise RuntimeError("empty")
                        df["date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
                        for c in ("open", "high", "low", "close", "volume"):
                            df[c] = pd.to_numeric(df[c], errors="coerce")
                        df = df.sort_values("date").reset_index(drop=True)
                        return df[["date", "open", "high", "low", "close", "volume"]]

                    # default: treat as stock
                    rows = self._db.query_stock_closes(window_start=start_dt, trade_date=trade_date)
                    df = pd.DataFrame(
                        rows,
                        columns=["symbol", "exchange", "trade_date", "pre_close", "chg_pct", "close", "amount"],
                    )
                    df = df[df["symbol"] == symbol]
                    if df.empty:
                        raise RuntimeError("empty")
                    df["date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
                    df["close"] = pd.to_numeric(df["close"], errors="coerce")
                    df = df.sort_values("date").reset_index(drop=True)
                    return df[["date", "close"]]

                except Exception as e:
                    # Fallback to yfinance
                    self._ensure_yf()
                    LOG.warning(f"[DBMarketProvider] DB fetch failed for {symbol} method={method}: {e}; fallback=yf")
                    return self._yf.fetch_series_raw(symbol, window=window, method=method)

        self._impl = _Impl()

    def fetch(self, symbol: str, window: int = 60, method: str = "default"):
        return self._impl.fetch(symbol=symbol, window=window, method=method)



