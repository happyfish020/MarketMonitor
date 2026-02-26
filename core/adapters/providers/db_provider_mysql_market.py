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
            raise RuntimeError("db.mysql.tables.stock_daily not configured")

        params = {
            "window_start": _to_date(window_start),
            "trade_date": _to_date(trade_date),
        }
        self._require_mysql(table)
        sql = f"""  # nosec B608
        SELECT
            SYMBOL        AS symbol,
            EXCHANGE      AS exchange,
            TRADE_DATE    AS trade_date,
            PRE_CLOSE     AS pre_close,
            CHG_PCT       AS chg_pct,
            CLOSE         AS close,
            AMOUNT        AS amount
        FROM {self._stock_table_ref(use_mysql=True)}
        WHERE TRADE_DATE >= :window_start
          AND TRADE_DATE <= :trade_date
        """
        return self.execute_mysql(sql, params)

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
        self._require_mysql("CN_FUND_ETF_HIST_EM")
        return self.execute_mysql(mysql_sql, params)

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
        return self.execute_mysql(sql)


    def fetch_daily_amount_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        闂佸吋鍎抽崲鑼躲亹閸ヮ剙绠伴柛銉戝懏姣庨梺鍝勫暔閸庢彃锕㈤敓鐘茬婵炲樊浜濋敍鐔兼煕閹邦剚鍣归柛娆忕箳閺侇噣宕掑顓犵毣濠殿噯绲界换鎴澪涢埡鍛闁归偊浜濋崬澶娢涢悧鍫㈢缂佽鲸鐟︾粋宥夊垂椤旂厧绗氶梺鎸庣☉椤︽澘顪冮崒鐐粹拻闁哄鍨圭喊宥夋煕?        闁哄鏅滈弻銊ッ?columns:
            trade_date (datetime)
            total_amount (float)  # 闂佸憡顨嗗ú鎴犵礊閸涘瓨鏅慨姗嗗亜濞堢娀鏌涜箛鎾虫灕缂佽鲸绻傞蹇涘蓟閵夛妇褰?1e8
        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.mysql.tables.stock_daily not configured")

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
        }

        self._require_mysql(table)
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

        if not raw:
            return pd.DataFrame(columns=["trade_date", "total_amount"])

        df = pd.DataFrame(raw, columns=["trade_date", "total_amount"])
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["total_amount"] = (df["total_amount"].astype(float) / 1e8).round(2)
        df = df[["trade_date", "total_amount"]].set_index("trade_date")

        return df    
# 闂備礁鎼崐鐟邦熆濮椻偓璺柛鎰ㄦ櫇椤╃兘鏌曟径鍫濃偓妤呮偡閹剧粯鐓涢柛灞剧箥濞兼劗鐥鐐靛煟闁轰礁绉瑰畷濂告偄閸涘﹥绶梻渚€娼荤拹鐔煎礉鎼达絾鍋栨い鎰剁畱缁€鍐煕濞戞瑦缍戦柛鏂诲劦閺屾稑顫濋鍌氣叺闂侀€炲苯澧柛濠囶棑缁辩偤宕ㄩ婊咃紴濡炪倖娲栧Λ娑氱矆?

    def fetch_stock_daily_chg_pct_raw(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        闂佸吋鍎抽崲鑼躲亹閸ヮ剙绠伴柛銉戝懏姣庢繛瀛樺殠閸婃牕危濡ゅ懎绫嶉柕澶堝劤缁愭棃鏌?look_back_days 婵犮垹鐏堥弲娑㈠船閹绢喗鍎嶉柛鏇ㄥ墰濡层劑鏌￠崘锕€鍔氱紒鏂跨埣瀹曠兘鎮滃Ο鍝勫缂傚倷绶℃禍锝嗙濞戙垹瑙﹂柛顐ゅ枑濞堝爼鏌熺拠鈥虫灁闁?        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.mysql.tables.CN_STOCK_DAILY_PRICE not configured")

        params = {
            "start_date": _to_date(start_date),
            "look_back_days": look_back_days,
            "eps": 0.0001,
        }

        self._require_mysql(table)
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
        闂佸吋鍎抽崲鑼躲亹閸ヮ剙绠伴柛銉戝懏姣庢繛瀛樺殠閸婃牕危濡ゅ懎绫嶉柕澶堝劚椤?look_back_days 闂佸憡鍔曢幊鎰枔閹达箑鐏?0闂佸搫鍟ㄩ崕鏌ュ蓟婵犲啯濯撮幖瀛樓氶崑鎾斥攽閸℃瑧鏆涢柟鑹版彧婵″洨鍒掗搹瑙勫閹烘娊鍩€?        """
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.mysql.tables.CN_STOCK_DAILY_PRICE not configured")

        params = {
            "trade_date": _to_date(trade_date),
            "look_back_days": look_back_days,
        }

        self._require_mysql(table)
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
        Load confirmed full-market EOD snapshot (T-1) from MySQL.
    
        Contract (frozen):
        - trade_date: confirmed trading day (T-1)
        - source: MySQL only (no network)
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
                "source": "mysql",
                "confirmed": True,
                "record_count": len(market),
            },
        }
    
    def query_last_trade_date(self, as_of_date) -> str:
        table = self.tables.get("stock_daily")
        if not table:
            raise RuntimeError("db.mysql.tables.stock_daily not configured")
        params = {"as_of_date": _to_date(as_of_date)}
        self._require_mysql(table)
        sql = f"""  # nosec B608
        SELECT MAX(TRADE_DATE) AS last_trade_date
        FROM {self._stock_table_ref(use_mysql=True)}
        WHERE TRADE_DATE <= :as_of_date
        """
        rows = self.execute_mysql(sql, params)
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
    
        # 闂備礁鎼€氼剙霉閻戣棄鐏虫慨妞诲亾鐎殿喕绮欏畷鍫曞Ω閿旇姤鐣?as_of 闂佽崵濮村ù鍌炲储閾忓湱涓嶉柣鏂垮悑閺咁剟鏌涢鐘插姕濠殿喗绮撻幃浠嬵敍濞戞瑥鈧劙鏌涢敐鍛喐闁硅尙澧楃粭鐔哥節閸曨収妲峰┑鐐存尰閼归箖濡靛Ο鑹板С妞ゆ帒鍊甸崑鎾垛偓娑栧€戦崐銈夊触閳?/ 闂備焦鎮堕崕鎶藉磻閻愬搫妫樺〒姘ｅ亾闁?        snapshot["_meta"] = snapshot.get("_meta", {})
        snapshot["_meta"].update(
            {
                "as_of_date": _to_date(as_of_date),
                "resolved_trade_date": last_trade_date,
            }
        )
    
        return snapshot

    # ==================================================
    # ETF 闂備礁鎼崯銊╁磿閹绢喓鈧線骞嬮敃鈧粻姘舵煕濞戙埄鏆掔紒顔炬暬閺屾稑顫濋鍌氼杸濠碉紕鍋涢崐鍨潖婵犳艾绾ч柟瀵稿Х椤ｆ煡姊洪崨濠傜瑲妞ゃ劍鍔楀Σ鎰潨?Block闂?    # ==================================================
    def fetch_etf_hist_series(
        self,
        start_date: str,
        look_back_days: int = 60,
    ) -> pd.DataFrame:
        """
        濠电偛顕慨瀛橆殽閹间焦鍋勯柍褜鍓熷?ETF 闂備礁鎼崯銊╁磿閹绢喓鈧線骞嬮敃鈧粻姘舵煕濞戞粌鐒介柍褜鍓欓崯鎾嵁閹捐绠涙い鎺嗗亾缂佹彃娼￠弻鐔虹矙閹稿孩鎮欓梺浼欑到婢х晫鍒掗敐鍛傛棃鍩€椤掑啨浜规繛鎴欏灩缁€鍐煕濞戝崬鐏￠柣锝変憾閺屻倖娼忛妸銊ヮ棟闂佽鍠栫€涒晠骞嗛弮鍥╃杸闁规儳纾禒姘舵⒑?
        闂佸搫顦悧濠囧箰閹间礁鍚?DataFrame index=trade_date闂備焦瀵х粙鎴濐焽閹敆etime闂備焦瀵х粙鎴βㄩ埀顒傜磼鏉堛劎绠橀柍褜鍓氱粙鎺椼€冮崱娑辨晩鐎光偓閸曨剚娅?            total_change_amount: 闁荤喐绮庢晶妤呭箰鐠囧弬娑㈠煛閸屾粠娴勯梻浣哥仢椤戝洦绔熼幇顓犵闁瑰瓨鐟ユ慨鈧紓浣歌嫰閸氬鏁嶉幇顑芥斀闁告侗鍨奸崥鍛存⒑?            total_volume: 闁荤喐绮庢晶妤呭箰鐠囧弬娑㈠煛閸涱厾顓洪梺褰掑亰娴滄繈宕径鎰拺闁圭粯甯炵粻锝囩磼閻欏懐绉€?            total_amount: 闁荤喐绮庢晶妤呭箰鐠囧弬娑㈠煛閸涱厾顓洪梺褰掑亰娴滄繈宕径濞㈡盯鎮ч崼銏㈢暠缂備胶濯寸紞浣哥暦?
        闂備礁鎲￠悷銉╁磹瑜版帒姹查柣鏃傚劋鐎氭岸鎮楀☉娅虫垿锝為弽顓熺叆?            start_date: 缂傚倸鍊烽悞锕傚箰鐠囧樊鐒芥い鎰剁畱缁秹鏌曟径娑㈡闁糕晛鍊块弻銊モ槈濡厧顣洪柣搴＄仛閿曘垽鐛箛娑辨晢闁告劖鍎冲▓娆撴⒑閸濆嫬鏆旈柛搴ゅ吹濡?            look_back_days: 闂備焦鎮堕崕鎶藉磻閻愬搫绀勯柤鎭掑劚缁剁偤鏌嶉崫鍕偓鐟扳枔閸洘鐓ユ繛鎴烆焽婢ф洟鏌?start_date 闂備線娼荤拹鐔煎礉瀹€鍕埞闁圭虎鍠楅弲?
        婵犵數鍋涢ˇ顓㈠礉瀹ュ绀堝ù鐓庣摠閺?        - 闂備礁鎼崐浠嬶綖婢跺本鍏滈柛顐ｆ礃閻掑ジ姊婚崼鐔衡姇闁?DBMySQLMarketProvider 缂傚倷鐒﹂幏婵堢不閹达附鍋╁Δ锝呭暞閺咁剟鎮橀悙璺轰汗闁荤喐绻傞湁闁挎繂鐗婄涵鍫曟煛?TO_DATE
        - 闂佽崵鍋炵粙蹇涘礉瀹€鍕畺闁哄洢鍨洪崑?config.yaml 濠?db.mysql.tables.fund_etf_hist 闂傚倷鐒﹀妯肩矓閸洘鍋?          闂備礁鍚嬮惇褰掑磿閹绘帪鑰挎い鎾卞灪閻撯偓閻庡箍鍎卞ú銊╁几閸岀偞鐓ユ繛鎴烆焾鐎氫即鏌涢…鎴濈仭妞ゎ亖鍋撳┑鈽嗗灡椤戞瑩宕ラ崶褉妲?"CN_FUND_ETF_HIST_EM"
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
        # 闂佸搫顦遍崕鎰板礈濮橆剛鏆﹂柡灞诲劚缁秹鏌曟径娑㈡闁糕晛鍊荤槐鎺楊敃閵夘喖娈梺?        df["trade_date"] = pd.to_datetime(df["trade_date"])
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
        濠电偛顕慨鏉戭潩閿曞倸纾婚柛宀€鍋涚粻浼存煕閵夛絽濡介柛鈺佸€块幃褰掑箛鐏炶棄顏柨娑橆樀閹泛鈽夊Ο缁樻嫳闂佺楠哥换鎺楀箖娴兼潙惟闁靛绠戦顖炴⒑閸︻収鐒炬繛璇х畵瀵娊鎮㈤悡搴ｇ潉闂侀潧顦介悡鍫ュ绩娴犲鐓欐い鏍ㄧ閸も偓闂侀潧妫楅崯鎾嵁閹捐绠涙い鎺嗗亾缂佹彃娼￠弻娑㈠煛娴ｈ鍎撳┑鈽嗗灠閿曨亜顕ｉ鈧畷濂稿即閻曞倻鍚归梺鐟板悑濞兼瑧鎹㈤幇鏉垮瀭妞ゅ繐鐗嗚繚?
        闂佸搫顦悧濠囧箰閹间礁鍚?DataFrame index=trade_date闂備焦瀵х粙鎴濐焽閹敆etime闂備焦瀵х粙鎴βㄩ埀顒傜磼鏉堛劎绠橀柍褜鍓氱粙鎺椼€冮崱娑辨晩鐎光偓閸曨剚娅?            avg_basis:   闂備礁婀遍…鍫ニ囨导鏉戝瀭婵﹩鍘鹃々鏌ユ煏閸繍妲告繛鍛€濋弻娑㈠籍閸屾锝嗙箾閻撳海澧﹂柟顖氬暣瀹曠喖顢曢妶鍛殢闁诲海鎳撴竟濠囧窗閺嵮岀劷閹兼番鍔岀壕鍦棯椤撱埄妫戠紒鈧崟顖涚厸闁割偁鍨归弸娆撴煙娴兼瑧绋荤紒鍌涘浮閺屻劎鈧綆鍓欓ˉ?- 闂備礁婀遍…鍫澝洪敃鍌氭辈闁绘梻鍘х紒鈺呮煕閹捐尙鍔嶇痪鍓ь焾闇夐柣妯诲絻濞堚晝绱?            total_basis: 闂備浇顕栭崜婵嬵敋瑜旈幃銏ゅ焵椤掑倵鍋撻悷鏉款棌闁革綆鍨冲Σ鎰枎閹邦喒鏋栭悗骞垮劚閹冲酣锝為敐澶嬬厸濠㈣泛锕ら弸锔剧磼?            basis_ratio: 闂備胶纭堕弲娑㈠箹椤愨懡鐔哥節閸愵亖鏋栭柟鍏肩暘閸ㄥ綊鎯佽ぐ鎺撶厸濞达綀顫夌欢鎻掆攽椤旀儳浠辩€殿喖鐖煎畷姗€鍩￠崒娑欑槕闂備胶鍎甸弲婊呮箒闂佺粯绮犻崹杈╃矙婵犲嫧鍋撻敐搴′簽婵?
            total_volume: 闂備浇顕栭崜娑氬垝椤栫偛鍨傛慨姗嗗幘椤╂煡鏌曢崼婵愭Ц婵炲懌鍊濋弻銊モ槈濡厧鈪遍梺杞伴檷閸婃牜绮嬪鍡樺劅闁炽儲鍓氬閬嶆⒑閸濆嫷妲搁柛鐘崇〒濡?            weighted_future_price: 闂備礁婀遍…鍫ニ囨导鏉戝瀭婵﹩鍘鹃々鏌ユ煏閸繍妲告繛鍛€濋弻娑㈠籍閸屾锝嗙箾閻撳海澧﹂柟顖氬暣瀹曠喖顢楁担鍝ュ姱闂佽崵濮甸崜顒勫磼濠婂啰鐣鹃梻?            weighted_index_price:  闂備礁婀遍…鍫ニ囨导鏉戝瀭婵﹩鍘鹃々鏌ユ煏閸繍妲告繛鍛€濋弻娑㈠籍閸屾锝嗙箾閻撳海澧﹂柟顖氬暣瀹曠喖顢橀悙鐢垫澑闂佽崵濮甸崜顒勫礋椤愩倖鈻岄梻浣芥〃缁舵岸銆傞敃鍌涘仢妞ゆ牜鍋涢崘鈧?
        闂備礁鎲￠悷銉╁磹瑜版帒姹查柣鏃傚劋鐎氭岸鎮楀☉娅虫垿锝為弽顓熺叆?            start_date: 缂傚倸鍊烽悞锕傚箰鐠囧樊鐒芥い鎰剁畱缁秹鏌曟径娑㈡闁糕晛鍊块弻銊モ槈濡厧顣洪柣搴＄仛閿曘垽鐛箛娑辨晢闁告劖鍎冲▓娆撴⒑閸濆嫬鏆旈柛搴ゅ吹濡?            look_back_days: 闂備焦鎮堕崕鎶藉磻閻愬搫绀勯柤鎭掑劚缁剁偤鏌嶉崫鍕偓鐟扳枔閸洘鐓ユ繛鎴烆焽婢ф洟鏌?start_date 闂備線娼荤拹鐔煎礉瀹€鍕埞闁圭虎鍠楅弲?
        婵犵數鍋涢ˇ顓㈠礉瀹ュ绀堝ù鐓庣摠閺?        - 濠电偛顕慨鎾箠閹惧顩锋繛鎴欏灩鐟欙箓鏌涢锝囩畺闁诡垰鍊荤槐?IF/IH/IC/IM闂備焦瀵х粙鎴︽嚐椤栫儐鏁嬫い鏃囧Г閸庣喖鏌￠崘锝呬壕闂佸憡娲栭崯顐ュ絹?00闂備線娼уΛ鏂款渻閹烘梻绠旈柛灞剧矋鐎?0闂備線娼уΛ鏂款渻閹烘鍤堥柟瀵稿У鐎?00闂備線娼уΛ鏂款渻閹烘鍤堥柟瀵稿У鐎?000闂備礁婀遍…鍫澝洪敃鍌氭辈闁绘梻鍘ц繚?        - 闂備胶纭堕弲娑㈠箹椤愨懡鐔哥節閸屻倖妗ㄩ梺鎸庣箓閹冲宕戦幘缁樺殞闁煎壊鍏橀崑鎾诲礃椤忓棛鐒奸梺缁樺灦钃遍柛鈺佸€块幃褰掑箛鐏炶棄鏆欑痪鎹愭珪閹便劌螣閹稿骸娑х紓浣介哺缁诲嫰骞忚ぐ鎺撳亜閻忓繋绀佹禍楣冩煠绾懎鐒介柍褜鍓欓崯鎵垝閸儲鍊绘俊顖濐嚙閻掓悂鏌ｉ悩鍐插闁糕剝顨呴悘鈥斥攽椤旂即鎴︽偩椤忓牆鐒?        """
        # 闂備礁鎼鍛偓姘嵆閸┾偓妞ゆ帒鍊告牎闂佺粯鎼槐鏇㈠箯閻樹紮缍栨い鏂垮⒔閻涖儵鏌ｉ悩鍏呰埅闁告柨绻樻俊?join 闂備礁婀遍…鍫ニ囨导鏉戝瀭婵﹩鍘鹃々鏌ユ煏閸繍妲告繛鍛€濋弻娑㈠籍閸屾锝嗙箾閻撳海澧㈤柟宄邦儔閺佸秹宕熼鐔哥槗闂備胶纭堕弲娑㈠箹椤愨懡鐔哥節閸パ勩仢闁诲繒鍋犻崑鎰扮嵁閸℃稒鐓?        # 濠电偠鎻紞鈧繛澶嬫礋瀵偊濡堕崶鈺冿紲闂佺懓鐡ㄧ换鍕汲娴煎瓨鈷戞い鎺嗗亾闁诲繑绻堝畷?TO_DATE
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
        # 闂佽崵濮崇欢銈囨閺囥垺鍋?ratio = avg_basis / weighted_index_price
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
                COALESCE(close_price - prev_close, 0) AS change_amount,
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
        self._require_mysql(table)
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
        self._require_mysql(table)
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
        rows = self.execute_mysql(sql, {"asof_date": asof_date, "look_back_days": int(look_back_days)})
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
# Market-data Provider adapter: DB-only
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


def _normalize_stock_db_symbol(symbol: str) -> str:
    """Normalize stock symbol to local DB SYMBOL style (prefer 6-digit code)."""
    s = (symbol or "").strip()
    if not s:
        return s
    sl = s.lower()

    if sl.endswith(".ss") or sl.endswith(".sz"):
        core = "".join([c for c in sl[:-3] if c.isdigit()])
        return core if len(core) == 6 else sl

    if sl.startswith("sh.") or sl.startswith("sz."):
        core = "".join([c for c in sl[3:] if c.isdigit()])
        return core if len(core) == 6 else sl

    if sl.startswith("sh") or sl.startswith("sz"):
        core = "".join([c for c in sl[2:] if c.isdigit()])
        return core if len(core) == 6 else sl

    digits = "".join([c for c in sl if c.isdigit()])
    if len(digits) == 6:
        return digits
    return sl


class DBMarketProvider:
    """ProviderRouter 'db' provider.

    DB-only: query local MySQL tables.

    Contract: returns a DataFrame with columns: date/open/high/low/close/volume
    (pct computed by ProviderBase.normalize_df).
    """

    def __init__(self):
        from core.adapters.providers.provider_base import ProviderBase  # local import

        class _Impl(ProviderBase):
            def __init__(self):
                super().__init__(name="db")
                self._db = DBMySQLMarketProvider()

            @staticmethod
            def _empty_frame_for_method(method: str) -> pd.DataFrame:
                m = (method or "default").strip().lower()
                if m in ("etf", "etf_hist"):
                    return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
                return pd.DataFrame(columns=["date", "close"])

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
                    symbol_norm = _normalize_stock_db_symbol(symbol)
                    candidates = [str(symbol), str(symbol_norm)]
                    if isinstance(symbol_norm, str) and len(symbol_norm) == 6 and symbol_norm.isdigit():
                        candidates.extend(
                            [
                                f"sh{symbol_norm}",
                                f"sz{symbol_norm}",
                                f"sh.{symbol_norm}",
                                f"sz.{symbol_norm}",
                            ]
                        )
                    candidates = list(dict.fromkeys([c for c in candidates if c]))
                    df = df[df["symbol"].astype(str).isin(candidates)]
                    if df.empty:
                        raise RuntimeError("empty")
                    df["date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
                    df["close"] = pd.to_numeric(df["close"], errors="coerce")
                    df = df.sort_values("date").reset_index(drop=True)
                    return df[["date", "close"]]

                except Exception as e:
                    raise RuntimeError(
                        f"[DBMarketProvider] DB fetch failed for {symbol} method={method}: {e}"
                    ) from e

        self._impl = _Impl()

    def fetch(self, symbol: str, window: int = 60, method: str = "default"):
        return self._impl.fetch(symbol=symbol, window=window, method=method)



