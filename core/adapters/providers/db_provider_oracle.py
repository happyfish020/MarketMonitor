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
            PRE_CLOSE     AS pre_close,
            CHG_PCT       AS chg_pct,
            CLOSE         AS close,
            AMOUNT      AS amount
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
        table = self.tables.get("fund_etf_hist") or "CN_FUND_ETF_HIST_EM"

        sql = f"""
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
            FROM {self.schema}.{table} t
            WHERE t.CODE = :code
              AND t.DATA_DATE >= :window_start
              AND t.DATA_DATE <= :trade_date
        )
        WHERE rn = 1
        ORDER BY trade_date
        """

        params = {
            "code": code,
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


    def fetch_daily_amount_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        获取指定日期区间内全市场每日成交额（亿元）时间序列

        返回 columns:
            trade_date (datetime)
            total_amount (float)  # 单位：亿元，已除 1e8
        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")

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

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
        }

        #compiled = text(sql).compile(dialect=oracle.dialect(), compile_kwargs={"literal_binds": True})
        #logger.info(f"[DEBUG SQL] 完整SQL:\n{compiled.string}")
        raw = self.execute(sql, params)

        if not raw:
            return pd.DataFrame(columns=["trade_date", "total_amount"])

        df = pd.DataFrame(raw, columns=["trade_date", "total_amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        #df["total_amount"] = (df["total_amount"] / 1e8).round(2)  # 转为亿元，保留2位小数
        df["total_amount"] = (df["total_amount"].astype(float) / 1e8).round(2)
        df = df[["trade_date", "total_amount"]].set_index("trade_date")

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
            limit_up (int)              -- 涨停家数（按限价命中；主板ST=5%，板块=20/30/10%）
            limit_down (int)            -- 跌停家数（按限价命中；主板ST=5%，板块=20/30/10%）
            adv_ratio (float)      -- 正涨幅占比%（即情绪分数，保留2位小数）
        """
        table = self.tables.get("stock_daily") # 请根据你的实际配置调整表名
        if not table:
            raise RuntimeError("db.oracle.tables.CN_STOCK_DAILY_PRICE not configured")

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
        FETCH FIRST 30 ROWS ONLY  -- 多取几行，确保有足够20个交易日
        """


        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
            "eps": 0.0001,
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
     

        # 1. 明确只取单日（T-1）
        rows = self.query_stock_closes(
            window_start=trade_date,
            trade_date=trade_date,
        )
    
        if not rows:
            raise RuntimeError(f"no EOD stock closes found for {trade_date}")
    
        # 2. 组装 full-market snapshot（不做任何推断）
        market: Dict[str, Dict[str, Any]] = {}
    
        for symbol, exchange, td, pre_close, chg_pct, close,amount in rows:
            market[symbol] = {
                "symbol": symbol,
                "exchange": exchange,
                "trade_date": td,
                "close": close,
                "pre_close": pre_close,
                "chg_pct": chg_pct,
                "amount": amount,
            }
    
        # 3. 返回“确认态快照”
        return {
            "trade_date": trade_date,
            "snapshot_type": "EOD",
            "market": market,
            "_meta": {
                "source": "oracle",
                "confirmed": True,
                "record_count": len(market),
            },
        }
    
    def query_last_trade_date(self, as_of_date) -> str:
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.oracle.tables.stock_daily not configured")
    
        sql = f"""
        SELECT MAX(TRADE_DATE) AS last_trade_date
        FROM {self.schema}.{table}
        WHERE TRADE_DATE <= :as_of_date
        """
    
        params = {"as_of_date": _to_date(as_of_date)}
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
    
        # 明确标注 as_of 语义（非常重要，便于审计 / 回放）
        snapshot["_meta"] = snapshot.get("_meta", {})
        snapshot["_meta"].update(
            {
                "as_of_date": _to_date(as_of_date),
                "resolved_trade_date": last_trade_date,
            }
        )
    
        return snapshot

    # ==================================================
    # ETF 日行情聚合时间序列（C Block）
    # ==================================================
    def fetch_etf_hist_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        从基金 ETF 日行情表提取指定窗口内的聚合序列。

        输出 DataFrame index=trade_date（datetime），字段：
            total_change_amount: 当日价格涨跌额之和
            total_volume: 当日成交量之和
            total_amount: 当日成交额之和

        参数说明：
            start_date: 结束日期（包括该日）
            look_back_days: 回溯天数（含 start_date 在内）

        注意：
        - 方法遵循 DBOracleProvider 约定，不使用 TO_DATE
        - 表名由 config.yaml 中 db.oracle.tables.fund_etf_hist 配置
          若未配置，则默认为 "CN_FUND_ETF_HIST_EM"
        """
        table = self.tables.get("fund_etf_hist") or "CN_FUND_ETF_HIST_EM"
        # 构造查询：聚合每个交易日的 price change、volume 与金额
        sql = f"""
        SELECT
            DATA_DATE   AS trade_date,
            SUM(NVL(CHANGE_AMOUNT, 0)) AS total_change_amount,
            SUM(NVL(VOLUME, 0))        AS total_volume,
            SUM(NVL(AMOUNT, 0))        AS total_amount
        FROM {self.schema}.{table}
        WHERE DATA_DATE >= :start_date - :look_back_days
          AND DATA_DATE <= :start_date
        GROUP BY DATA_DATE
        ORDER BY DATA_DATE
        """
        params = {
            "start_date": _to_date(start_date),
            "look_back_days": int(look_back_days),
        }
        raw = self.execute(sql, params)
        if not raw:
            return pd.DataFrame(columns=["trade_date", "total_change_amount", "total_volume", "total_amount"])
        df = pd.DataFrame(raw, columns=["trade_date", "total_change_amount", "total_volume", "total_amount"])
        # 转换日期类型
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        # 类型转换，保留原始精度
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
        从股指期货日行情表和指数日行情表提取基差时间序列。

        输出 DataFrame index=trade_date（datetime），字段：
            avg_basis:   按成交量加权的基差均值（期货价格 - 指数收盘价）
            total_basis: 总基差（不加权）
            basis_ratio: 基差与指数加权收盘价之比
            total_volume: 总成交量（用于加权）
            weighted_future_price: 按成交量加权的期货价格
            weighted_index_price:  按成交量加权的现货指数价格

        参数说明：
            start_date: 结束日期（包括该日）
            look_back_days: 回溯天数（含 start_date 在内）

        注意：
        - 仅聚合品种 IF/IH/IC/IM，对应沪深300、上证50、中证500、中证1000指数。
        - 基差正值表示期货升水，负值表示期货贴水。
        """
        fut_table = self.tables.get("fut_index_hist") or "CN_FUT_INDEX_HIS"
        idx_table = self.tables.get("index_daily") or "CN_INDEX_DAILY_PRICE"
        # 构造查询：跨表 join 按成交量加权计算基差和价格
        # 使用绑定避免 TO_DATE
        sql = f"""
        SELECT
            t.TRADE_DATE                AS trade_date,
            /* 加权基差： (期货结算价 - 指数收盘价) * 成交量  / 总成交量 */
            SUM((NVL(t.SETTLE_PRICE, t.CLOSE_PRICE) - idx.CLOSE) * t.VOLUME) / NULLIF(SUM(t.VOLUME), 0) AS avg_basis,
            SUM((NVL(t.SETTLE_PRICE, t.CLOSE_PRICE) - idx.CLOSE))                           AS total_basis,
            SUM(t.VOLUME)                           AS total_volume,
            SUM(NVL(t.SETTLE_PRICE, t.CLOSE_PRICE) * t.VOLUME) / NULLIF(SUM(t.VOLUME),0)    AS weighted_future_price,
            SUM(idx.CLOSE * t.VOLUME) / NULLIF(SUM(t.VOLUME),0)                              AS weighted_index_price
        FROM {self.schema}.{fut_table} t
        JOIN {self.schema}.{idx_table} idx
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
          AND t.TRADE_DATE >= :start_date - :look_back_days
          AND t.TRADE_DATE <= :start_date
        GROUP BY t.TRADE_DATE
        ORDER BY t.TRADE_DATE
        """
        params = {
            "start_date": _to_date(start_date),
            "look_back_days": int(look_back_days),
        }
        raw = self.execute(sql, params)
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
        # 计算 ratio = avg_basis / weighted_index_price
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
        从 ETF 期权日行情表聚合计算期权风险相关指标。

        输出 DataFrame index=trade_date（datetime），字段：
            weighted_change: 按成交量加权的涨跌额均值
                           （sum((close - prev_close) * volume) / sum(volume)）
            total_change:    合约涨跌额求和
            total_volume:    成交量求和
            weighted_close:  按成交量加权的收盘价
            change_ratio:    weighted_change / weighted_close

        参数说明：
            start_date: 结束日期（包括该日）
            look_back_days: 回溯天数（含 start_date 在内）

        注意：
        - 仅聚合配置中的 ETF 期权标的（见 etf_codes 列表）。
        - 由于部分免费源/入库表可能没有 CHANGE_AMOUNT/CHANGE_PCT 字段，
          本方法使用 Oracle 窗口函数 LAG(CLOSE_PRICE) 在 SQL 内即时计算涨跌额。
        - 当总成交量为 0 时，weighted_change、weighted_close 和 ratio 结果为 None。
        """
        option_table = self.tables.get("option_daily") or "CN_OPTION_SSE_DAILY"
        # ETF 期权标的列表（固定）
        etf_codes = [
            '510050',  # 华夏上证50ETF
            '510300',  # 华泰柏瑞沪深300ETF
            '510500',  # 南方中证500ETF
            '588000',  # 华夏科创50ETF
            '588080',  # 易方达科创50ETF
            '159919',  # 嘉实沪深300ETF (深市)
            '159922',  # 嘉实中证500ETF (深市)
            '159915',  # 易方达创业板ETF
            '159901',  # 易方达深证100ETF
        ]
        # 构造 IN 列表
        in_list = ",".join([f"'{code}'" for code in etf_codes])
        # IMPORTANT (FROZEN):
        # - No TO_DATE in SQL; bind python date to Oracle DATE
        # - Compute change_amount from CLOSE_PRICE using LAG to avoid requiring CHANGE_AMOUNT column
        # - Pull a few extra days to reduce boundary effects when computing LAG
        sql = f"""
        WITH base AS (
            SELECT
                t.CONTRACT_CODE AS contract_code,
                t.DATA_DATE     AS trade_date,
                t.CLOSE_PRICE   AS close_price,
                NVL(t.VOLUME, 0) AS volume,
                LAG(t.CLOSE_PRICE) OVER (
                    PARTITION BY t.CONTRACT_CODE
                    ORDER BY t.DATA_DATE
                ) AS prev_close
            FROM {self.schema}.{option_table} t
            WHERE t.UNDERLYING_CODE IN ({in_list})
              AND t.CLOSE_PRICE IS NOT NULL
              AND t.DATA_DATE >= :start_date - :look_back_days - 10
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
            SUM(NVL(change_amount, 0) * volume) / NULLIF(SUM(volume), 0) AS weighted_change,
            SUM(NVL(change_amount, 0)) AS total_change,
            SUM(volume) AS total_volume,
            SUM(close_price * volume) / NULLIF(SUM(volume), 0) AS weighted_close
        FROM calc
        WHERE trade_date >= :start_date - :look_back_days
        GROUP BY trade_date
        ORDER BY trade_date
        """
        params = {
            "start_date": _to_date(start_date),
            "look_back_days": int(look_back_days),
        }
        raw = self.execute(sql, params)
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
        # 转换为 float 类型（允许 NULL -> NaN）
        df["weighted_change"] = pd.to_numeric(df["weighted_change"], errors="coerce")
        df["total_change"] = pd.to_numeric(df["total_change"], errors="coerce")
        df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")
        df["weighted_close"] = pd.to_numeric(df["weighted_close"], errors="coerce")
        # 计算 ratio，避免除 0
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


