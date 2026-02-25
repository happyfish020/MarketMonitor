from typing import Any, Dict, List, Tuple
from datetime import date
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from core.utils.config_loader import load_config
from core.utils.logger import get_logger


logger = get_logger(__name__)

LOG = get_logger("DS.provider.mysql.market")
_SQL_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _safe_ident(name: str, field: str) -> str:
    value = str(name or "").strip()
    if not value or not _SQL_IDENT_RE.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier in {field}: {name!r}")
    return value


class DBMySQLMarketProvider:
    """
    MySQL-only market DB provider.
    """

    def __init__(self):
        db_cfg = load_config().get("db", {}) or {}

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
        self.mysql_stock_table = _safe_ident(
            os.getenv("MYSQL_STOCK_TABLE", str(mysql_tables.get("stock_daily", "CN_STOCK_DAILY_PRICE"))),
            "MYSQL_STOCK_TABLE",
        )
        self.mysql_etf_table = _safe_ident(
            os.getenv("MYSQL_ETF_TABLE", str(mysql_tables.get("fund_etf_hist", "CN_FUND_ETF_HIST_EM"))),
            "MYSQL_ETF_TABLE",
        )
        self.mysql_index_table = _safe_ident(
            os.getenv("MYSQL_INDEX_TABLE", str(mysql_tables.get("index_daily", "CN_INDEX_DAILY_PRICE"))),
            "MYSQL_INDEX_TABLE",
        )
        self.mysql_fut_table = _safe_ident(
            os.getenv("MYSQL_FUT_TABLE", str(mysql_tables.get("fut_index_hist", "CN_FUT_INDEX_HIS"))),
            "MYSQL_FUT_TABLE",
        )
        self.mysql_option_table = _safe_ident(
            os.getenv("MYSQL_OPTION_TABLE", str(mysql_tables.get("option_daily", "CN_OPTION_SSE_DAILY"))),
            "MYSQL_OPTION_TABLE",
        )
        self.mysql_universe_table = _safe_ident(
            os.getenv("MYSQL_UNIVERSE_TABLE", str(mysql_tables.get("universe", "CN_UNIVERSE_SYMBOLS"))),
            "MYSQL_UNIVERSE_TABLE",
        )
        self.schema = self.mysql_cfg["database"]
        self.tables = {
            "stock_daily": self.mysql_stock_table,
            "fund_etf_hist": self.mysql_etf_table,
            "index_daily": self.mysql_index_table,
            "fut_index_hist": self.mysql_fut_table,
            "option_daily": self.mysql_option_table,
            "universe": self.mysql_universe_table,
        }
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
                "[DBMySQLMarketProvider] mysql source enabled: %s:%s/%s stock=%s etf=%s index=%s fut=%s option=%s universe=%s",
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
            LOG.warning("[DBMySQLMarketProvider] mysql source init failed: %s", e)
        
         
    def _ensure_oracle_engine(self):
        raise RuntimeError("Oracle engine is not supported in DBMySQLMarketProvider")

    # ==================================================
    # low-level executor
    # ==================================================
    def execute(self, sql: str, params: Dict[str, Any] | None = None):
        return self.execute_mysql(sql, params)

    def execute_mysql(self, sql: str, params: Dict[str, Any] | None = None):
        if self.mysql_engine is None:
            raise RuntimeError("mysql engine not available")
        logger.debug(f"[DBMySQLMarketProvider] execute_mysql sql={sql} params={params}")
        with self.mysql_engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return result.fetchall()

    def _stock_table_ref(self, use_mysql: bool) -> str:
        return self.mysql_stock_table

    def _etf_table_ref(self, use_mysql: bool) -> str:
        return self.mysql_etf_table

    def _index_table_ref(self, use_mysql: bool) -> str:
        return self.mysql_index_table

    def _fut_table_ref(self, use_mysql: bool) -> str:
        return self.mysql_fut_table

    def _option_table_ref(self, use_mysql: bool) -> str:
        return self.mysql_option_table

    def _universe_table_ref(self, use_mysql: bool) -> str:
        return self.mysql_universe_table

    def _use_mysql_stock(self) -> bool:
        return self.mysql_engine is not None

    def _require_mysql(self, table_name: str):
        if self.mysql_engine is None:
            raise RuntimeError(
                f"MySQL is required for table {table_name}, but mysql engine is not available"
            )

    def _can_fallback_oracle_stock(self) -> bool:
        return False

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
            sql = f"""  # nosec B608
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

        sql = f"""  # nosec B608
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
        mysql_sql = f"""  # nosec B608
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
        rows = []
        try:
            rows = self.execute_mysql(mysql_sql, params)
        except Exception as e:
            LOG.error(f"query_index_closes({index_code}, {window_start}, {trade_date}) failed: {e}")
        
        
        return rows

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
        mysql_sql = f"""  # nosec B608
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
        mysql_sql = f"""  # nosec B608
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
        ORDER BY x.DATA_DATE
        """
        rows = []
        try:        
           self._require_mysql("CN_FUND_ETF_HIST_EM")
           rows = self.execute_mysql(mysql_sql, params)
        except Exception as e:
            LOG.error(f"query_etf_prices({code}, {window_start}, {trade_date}) failed: {e}")
        return rows

    # ==================================================
    # universe symbols (industry mapping)
    # ==================================================
    def query_universe_symbols(self):
        sql = f"""  # nosec B608
        SELECT
            SYMBOL   AS symbol,
            EXCHANGE AS exchange,
            SW_L1    AS sw_l1
        FROM {self._universe_table_ref(use_mysql=True)}
        """
        self._require_mysql("CN_UNIVERSE_SYMBOLS")
        rows = []
        try:
            rows = self.execute_mysql(sql)
        except Exception as e:
            LOG.error(f"query_universe_symbols() failed: {e}")
        return rows 


    def fetch_daily_amount_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        闁兼儳鍢茶ぐ鍥箰閸パ呮毎闁哄啨鍎插﹢锟犲礌濞差亝锛熼柛鎰噹閸欏繒鏁崒姘皻婵絽绻戝Λ鈺呭箣閹邦亝鍞夊Λ鐗堢箰缁辨瑦绂嶉崹顔煎笚闁挎稑顦板鍌炴⒒閺夋垹纰嶉柛?        閺夆晜鏌ㄥú?columns:
            trade_date (datetime)
            total_amount (float)  # 闁告娲戠紞鍛存晬濮橆偄娈犻柛蹇撳枦缁辨繂顔忛弻銉︾彑 1e8
        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
        }

        if self._use_mysql_stock():
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
# 闂佸搫鍊瑰姗€路閸愨晝顩烽柕澶堝€楅悷鎾绘煛閸屾繍娼愮痪顓炵埣閺佸秹宕奸悢鍛婃緬闂侀潻璐熼崝搴ｆ偖椤愶箑绀冮柛娑欐綑閸斻儵鏌涘顒傚ⅵ闁逞屽墮閸婇绱為崨顖滅＞妞ゆ洖妫涚粈?

    def fetch_stock_daily_chg_pct_raw(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        闁兼儳鍢茶ぐ鍥箰閸パ呮毎濞存嚎鍊栧Σ妤呭籍閵夈儳绐旈柛?look_back_days 濠㈠灈鏅涢崬鎾儍閸曨剛妲ㄩ柡鍐﹀劚缁斿爼宕烽悜妯哄壈缂備緡浜ｆ禒娑㈠触閸喐娈堕柟璇″枔閳?        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
            "eps": 0.0001,
        }

        if self._use_mysql_stock():
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
        闁兼儳鍢茶ぐ鍥箰閸パ呮毎濞存嚎鍊栧Σ妤呭籍閵夈儱顤?look_back_days 闁告劕鎳愬▓鎴﹀灳?0闁哄啨鍎查弻濠冩媴鎼存ǚ鍋撳┑鍡欑暛閹艰揪濡囩划铏规媼鎺抽埀?        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

        params = {
            "trade_date": _to_date(trade_date),
            "look_back_days": look_back_days,
        }

        if self._use_mysql_stock():
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
    
        # 闂佸搫瀚ù鐑藉灳濮椻偓瀵粙宕堕妸锔芥畼 as_of 闁荤姴娴傞崢铏圭不閻斿吋鏅柛顐犲劜婵粓鎮介娑欏€愰柛锝呮憸閹茬増绗熸繝鍕槷婵炴挻鑹鹃妵妯艰姳椤掑倵鍋撶€涖們鍊ら崥鈧?/ 闂佹悶鍎抽崑鐐哄棘娓氣偓閺?        snapshot["_meta"] = snapshot.get("_meta", {})
        snapshot["_meta"].update(
            {
                "as_of_date": _to_date(as_of_date),
                "resolved_trade_date": last_trade_date,
            }
        )
    
        return snapshot

    # ==================================================
    # ETF 闂佸搫鍟ㄩ崕鎾€侀幋锕€绠氶柛娑㈩暒缁敻鏌涘顒傚婵＄偛鍊垮濠氬级閹寸姷顣查梺鍛婂笚椤ㄦ劗妲愬?Block闂?    # ==================================================
    def fetch_etf_hist_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        婵炲濮存鎼佹偄閳ь剟姊?ETF 闂佸搫鍟ㄩ崕鎾€侀幋锕€绠氶柛娑滃焽閳ь剙鍟撮獮鎾诲箛椤掆偓缁插潡鏌熺粙鎸庢悙闁伙絽澧界划锝呂旈埀顒冦亹濞戙垹绀冮柛娑卞灡閻ｉ亶鏌ゆ潏銊ㄥ闁诡喖瀛╅幆鏃囩疀閹惧磭浠氶梺?
        闁哄鐗婇幐鎼佸吹?DataFrame index=trade_date闂佹寧绋戝鎭唗etime闂佹寧绋戦¨鈧紒杈ㄧ箘閳ь剚绋掗〃鍡涱敊瀹€鍕櫖?            total_change_amount: 閻熸粎澧楅幐璇参涢埡鍌滎浄闂佸灝顑囨竟鎰箾閹存瑥濮€缂佸苯鍚嬮敍鎰攽閸涱垼鍚呴梺?            total_volume: 閻熸粎澧楅幐璇参涢埡鍛闁归偊浜濋崬澶愭⒑閹绘帞绠ｇ紒鐙呯秮瀹?            total_amount: 閻熸粎澧楅幐璇参涢埡鍛闁归偊浜濋崬澶娢涢悧鍫㈢畱缂佺媴缍佸畷?
        闂佸憡鐟ラ崐褰掑汲閻旂儤瀚氶悗娑櫳戦～鏍煥?            start_date: 缂傚倷鐒﹂幐璇差焽椤愶箑绫嶉柕澶涢檮閸╁倿鏌ㄥ☉妯煎閻庡灚锕㈤獮蹇涱敆閸愭儳娈欓梺鍝勫暔閸庤崵妲?            look_back_days: 闂佹悶鍎抽崑鐐哄礄閼恒儱绶為柍鍝勫€瑰▓鍫曟煥濞戞澧曢柟?start_date 闂侀潻璐熼崝宀勫船閹绢喗鏅?
        濠电偛顦崝宥夊礈娴煎瓨鏅?        - 闂佸搫鍊介～澶屾兜閸洘鐒奸梻鍫熺⊕閸?DBMySQLMarketProvider 缂備焦鎷濈粻鎴︽偩妤ｅ啯鏅悘鐐跺亹閻熸繂霉閿濆牊纭堕柡?TO_DATE
        - 闁荤偞绋忛崝宀勫箖閺囥垺鍋?config.yaml 婵?db.oracle.tables.fund_etf_hist 闂備焦婢樼粔鍫曟偪?          闂佸吋鐪归崕鎻掞耿椤撱垺鐓€鐎广儱娲ㄩ弸鍌炴煥濞戞瀚伴柛顭戝灡椤偓婵☆垱顑欓崥鍥р槈?"CN_FUND_ETF_HIST_EM"
        """
        # Build per-day aggregated price-change/volume/amount series.
        sql = f"""  # nosec B608
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
        # 闁哄鍎愰崜姘暦閺屻儱绫嶉柕澶涢檮閸╁倻绱掗銉殭闁?        df["trade_date"] = pd.to_datetime(df["trade_date"])
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
        婵炲濮村锕傚磻閸岀偛绠伴柛銉ｅ妽閸╁倿鎮归幇灞藉閿涘鎮跺☉妯绘拱闁稿骸绻掗幃浼村Ω閵夛箑顏梺鍦焾濞诧箓寮抽悢鐓庣睄闁靛鐓堥弨浠嬫煙椤栨碍鍤€闁靛棗鍟撮獮鎾诲箛椤掆偓缁插潡鏌涢埡浣规儓婵☆垰锕顕€宕奸弴鐕傜吹闁瑰吋娼欑换鎰板垂椤忓牆违?
        闁哄鐗婇幐鎼佸吹?DataFrame index=trade_date闂佹寧绋戝鎭唗etime闂佹寧绋戦¨鈧紒杈ㄧ箘閳ь剚绋掗〃鍡涱敊瀹€鍕櫖?            avg_basis:   闂佸湱顭堥ˇ浼村垂濮橆厾顩查柕鍫濐槸濞呫倝鏌涢弮鍌毿ｆ繛鐓庣墦閹啴宕熼銈呮暏閻庣懓澹婇崰鏍ь焽鎼淬劌纾圭痪顓㈩棑缁€鍕煛閸垹鏋欓柟浼欑稻缁傛帡鏌ㄧ€ｎ剙顥?- 闂佸湱顭堝ú锕傚汲閻旂厧缁╅柛鎾茬劍绾剧霉閻樻彃娈╃紒?            total_basis: 闂佽鍓濋褔鎮㈤埀顒傗偓鐟板閸ｎ垳妲愬▎鎰枖鐎广儱鎳庨～锝夋煛婢跺﹤鏋︾紒?            basis_ratio: 闂佺硶鏅涢幖顐⑽熸繝鍐枖閹兼番鍨归惁褰掓煛娴ｈ绶插┑顔惧仱瀵爼宕橀埡鍌涙瘑闂佺儵鏅滅湁闁绘粠鍨辩粙濠勨偓锝庡亞濡?
            total_volume: 闂佽鍓涚划顖炲垂濮橆厾顩查柕鍫濐槸濞呫倝鏌ㄥ☉妯煎ⅱ闁轰降鍊栫粋宥嗘償閳ユ剚娼遍梺鍝勵槸閸犳稓妲?            weighted_future_price: 闂佸湱顭堥ˇ浼村垂濮橆厾顩查柕鍫濐槸濞呫倝鏌涢弮鍌毿ｆ繛鐓庣墦閹啴宕熼浣哥劯闁荤姵鍓崒婊冪畾闂?            weighted_index_price:  闂佸湱顭堥ˇ浼村垂濮橆厾顩查柕鍫濐槸濞呫倝鏌涢弮鍌毿ｆ繛鐓庣墦閹啴宕熼鐐电杸闁荤姵鍓崟顐ゆ▌闂佽桨绶氶。锕傛偝椤栫偛鍐€?
        闂佸憡鐟ラ崐褰掑汲閻旂儤瀚氶悗娑櫳戦～鏍煥?            start_date: 缂傚倷鐒﹂幐璇差焽椤愶箑绫嶉柕澶涢檮閸╁倿鏌ㄥ☉妯煎閻庡灚锕㈤獮蹇涱敆閸愭儳娈欓梺鍝勫暔閸庤崵妲?            look_back_days: 闂佹悶鍎抽崑鐐哄礄閼恒儱绶為柍鍝勫€瑰▓鍫曟煥濞戞澧曢柟?start_date 闂侀潻璐熼崝宀勫船閹绢喗鏅?
        濠电偛顦崝宥夊礈娴煎瓨鏅?        - 婵炲濮撮幊鎾寸濞戙垹瑙﹂柛顐ｇ箖閹倻绱?IF/IH/IC/IM闂佹寧绋戦懟顖烆敋椤旇姤鍎熼柡鍐ｅ亾闁告洖鍟彁?00闂侀潧妫斿鎺旂箔閸屾粍瀚?0闂侀潧妫斿鎺楁嚈閹寸姵瀚?00闂侀潧妫斿鎺楁嚈閹寸姵瀚?000闂佸湱顭堝ú锕傚汲閻旂厧违?        - 闂佺硶鏅涢幖顐⑽熸繝鍌ゆ桨闁挎繂鎳夐崑鎾绘嚌閼割兘鍋撻崘顏嗙焼闁绘垶蓱閸╁倿鎮归幇灞藉暙绾捐櫕鎱ㄥΟ鎸庡涧缂佽鲸绻勯幏褰掓偄鐏忎礁浜鹃柤纭呭焽閳ь剙鍟扮划鍫ユ倻濡鐒搁柣鐘冲壃閸℃鐏″┑顔缴戦悾顏堝焵?        """
        # 闂佸搫顑呯€氫即鍩€椤掑倸校闁绘搫绱曢幏鐘伙綖椤斿墽鐛ラ柣鐘充航閸斿繘濡?join 闂佸湱顭堥ˇ浼村垂濮橆厾顩查柕鍫濐槸濞呫倝鏌涢弮鍌毿ｆ繛鐓庣墢閹峰鏁嶉崟顓熸瘓闂佺硶鏅涢幖顐⑽熸繝鍥фそ閻忕偠鍋愰獮鍡涙煛?        # 婵炶揪缍€濞夋洟寮妶鍥╃＜闁瑰瓨绻勯弳浼存⒑椤掆偓閻忔繈宕?TO_DATE
        mysql_sql = f"""  # nosec B608
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
        # 闁荤姳绶ょ槐鏇㈡偩?ratio = avg_basis / weighted_index_price
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
        """Aggregate ETF options risk metrics from MySQL option daily table."""
        etf_codes = [
            "510050",
            "510300",
            "510500",
            "588000",
            "588080",
            "159919",
            "159922",
            "159915",
            "159901",
        ]
        in_list = ",".join([f"'{code}'" for code in etf_codes])
        mysql_sql = f"""  # nosec B608
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
            WHERE prev_close IS NOT NULL
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
            return pd.DataFrame(
                columns=[
                    "trade_date",
                    "weighted_change",
                    "total_change",
                    "total_volume",
                    "weighted_close",
                    "change_ratio",
                ]
            )
        df = pd.DataFrame(
            raw,
            columns=[
                "trade_date",
                "weighted_change",
                "total_change",
                "total_volume",
                "weighted_close",
            ],
        )
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["weighted_change"] = pd.to_numeric(df["weighted_change"], errors="coerce")
        df["total_change"] = pd.to_numeric(df["total_change"], errors="coerce")
        df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")
        df["weighted_close"] = pd.to_numeric(df["weighted_close"], errors="coerce")

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

    def fetch_advdec_series(self, asof_date, look_back_days: int = 30) -> Dict[str, Any]:
        """Fetch market adv/dec series from local daily price table.

        Returns a dict with a stable shape for BreadthPlusDataSource.
        """
        table = self.tables.get("stock_daily") or "CN_STOCK_DAILY_PRICE"

        if self._use_mysql_stock():
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
            sql = f"""  # nosec B608
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
            try:
                rows = self.execute_mysql(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
            except Exception as e:
                
                LOG.error(f"fetch_breadth_plus_metrics failed: {e}")
            
        else:
            if not self._can_fallback_oracle_stock():
                raise RuntimeError(
                    "MySQL stock source is unavailable and Oracle stock fallback is disabled "
                    "(set MYSQL_STOCK_ORACLE_FALLBACK=1 to re-enable)"
                )
            sql = f"""  # nosec B608
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
            

            try:
                rows = self.execute(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
            except Exception as e:
                LOG.error(f"fetch_breadth_plus_metrics failed: {e}")
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
                self._db = DBMySQLMarketProvider()
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


# Backward-compatible alias so existing callsites can migrate module path first.
DBOracleProvider = DBMySQLMarketProvider




