from typing import Any, Dict, List, Tuple
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text

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

        sql = f"""
        SELECT
            SYMBOL        AS symbol,
            EXCHANGE      AS exchange,
            TRADE_DATE    AS trade_date,
            CLOSE         AS CLOSE
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
    # index daily prices (CLOSE -> CLOSE)
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
            CLOSE         AS CLOSE
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


    def fetch_daily_turnover_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        获取指定日期区间内全市场每日成交额（亿元）时间序列

        返回 columns:
            trade_date (datetime)
            total_turnover (float)  # 单位：亿元，已除 1e8
        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")

        sql = f"""
        SELECT
            TRADE_DATE,
            SUM(TURNOVER) AS total_turnover
        FROM {self.schema}.{table}
        WHERE TRADE_DATE >= :start_date - :look_back_days
              AND TRADE_DATE <= :start_date
        GROUP BY TRADE_DATE
        ORDER BY TRADE_DATE        
        """

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
        }

        #compiled = text(sql).compile(dialect=oracle.dialect(), compile_kwargs={"literal_binds": True})
        #logger.info(f"[DEBUG SQL] 完整SQL:\n{compiled.string}")
        raw = self.execute(sql, params)

        if not raw:
            return pd.DataFrame(columns=["trade_date", "total_turnover"])

        df = pd.DataFrame(raw, columns=["trade_date", "total_turnover"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        #df["total_turnover"] = (df["total_turnover"] / 1e8).round(2)  # 转为亿元，保留2位小数
        df["total_turnover"] = (df["total_turnover"].astype(float) / 1e8).round(2)
        df = df[["trade_date", "total_turnover"]].set_index("trade_date")

        return df
    
# core/adapters/providers/db_provider_oracle.py
# 新增以下方法（放在类内部合适位置）

    def fetch_stock_daily_chg_pct_raw(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        获取指定交易日往前 look_back_days 天内的每日市场情绪统计汇总数据
        直接在数据库层完成聚合，性能更优，避免传输大量个股明细

        返回 DataFrame columns:
            trade_date (datetime)
            total_stocks (int)          -- 参与交易股票总数
            adv (int)                   -- 上涨家数
            dec (int)                   -- 下跌家数
            flat (int)                  -- 平盘家数
            limit_up (int)              -- 涨停家数 (≥9.9%)
            limit_down (int)            -- 跌停家数 (≤-9.9%)
            adv_ratio (float)      -- 正涨幅占比%（即情绪分数，保留2位小数）
        """
        table = self.tables.get("stock_daily") # 请根据你的实际配置调整表名
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

        sql = f"""
        SELECT
            TRADE_DATE,
            COUNT(*) AS total_stocks,
            SUM(CASE WHEN CHG_PCT > 0  THEN 1 ELSE 0 END) AS adv,
            SUM(CASE WHEN CHG_PCT < 0  THEN 1 ELSE 0 END) AS dec,
            SUM(CASE WHEN CHG_PCT = 0  THEN 1 ELSE 0 END) AS flat,
            SUM(CASE WHEN CHG_PCT >= 9.9 THEN 1 ELSE 0 END) AS limit_up,
            SUM(CASE WHEN CHG_PCT <= -9.9 THEN 1 ELSE 0 END) AS limit_down,
            ROUND(SUM(CASE WHEN CHG_PCT > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS adv_ratio
        FROM {self.schema}.{table}
        WHERE TRADE_DATE >= TRUNC(:start_date) - :look_back_days
          AND TRADE_DATE <= TRUNC(:start_date)
          AND CHG_PCT IS NOT NULL
        GROUP BY TRADE_DATE
        ORDER BY TRADE_DATE DESC
        FETCH FIRST 30 ROWS ONLY  -- 多取几行，确保有足够20个交易日
        """

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
        }

        raw = self.execute(sql, params)

        if not raw:
            LOG.warning("[DBProvider] fetch_stock_daily_sentiment_stats returned no data for %s", start_date)
            return pd.DataFrame(columns=[
                "trade_date", "total_stocks", "adv", "dec", "flat",
                "limit_up", "limit_down", "adv_ratio"
            ])

        # 正确映射所有返回列
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
        look_back_days: int = 150,  # 建议150天，确保覆盖50个交易日 + 节假日
    ) -> pd.DataFrame:
        """
        获取指定交易日往前 look_back_days 天内，每日创“50日新低”的市场广度统计

        返回 DataFrame columns:
            trade_date (datetime)
            count_total (int)           -- 当日参与股票总数
            count_new_low_50d (int)     -- 当日创50日新低股票家数
            new_low_50d_ratio (float)   -- 新低比例%（保留2位小数）
        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

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

        params = {
            "trade_date": _to_date(trade_date),
            "look_back_days": look_back_days,
        }

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