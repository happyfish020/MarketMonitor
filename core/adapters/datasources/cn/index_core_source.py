# core/adapters/datasources/cn/index_core_source.py
# UnifiedRisk V12.1 - A-share Core Index DataSource (IndexCoreSource)
# 统一使用 SymbolSeriesStore，DS 本身只写每日 cache，不写 history。

from __future__ import annotations

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig,DataSourceBase
from core.adapters.cache.symbol_cache import normalize_symbol
from core.utils.config_loader import load_symbols
from core.utils.ds_refresh import apply_refresh_cleanup
from core.adapters.providers.symbol_series_store import SymbolSeriesStore
from core.adapters.providers.db_provider_router import get_db_provider

LOG = get_logger("DS.IndexCore")


class IndexCoreDateSource(DataSourceBase):
    """
    V12.1 A 股核心指数数据源（上证 / 深证 / 创业板 / 科创50 / HS300 ...）

    设计要点：
    - 指数配置来自 symbols.yaml.index_core
    - 指数日线序列统一由 SymbolSeriesStore 管理
      data/cn/history/symbol_series/*.json（实际路径由 DataSourceConfig 决定 market="glo"/"cn"）
    - 本 DataSource：
        · 只负责从 Store 拿 df
        · 按统一格式构造 block
        · 把 block 写入 cache: data/cn/cache/index_core/{symbol}_{trade_date}.json
    - 不写 history/index_core/*.json
    """

    def __init__(self, config: DataSourceConfig, window: int = 120):
        super().__init__(config)
        self.config = config
        self.market = config.market
        self.ds_name = config.ds_name

        self.cache_root = config.cache_root
        config.ensure_dirs()

        # 统一序列中心
        self.store = SymbolSeriesStore.get_instance()
        self.window = int(window) if window and window > 0 else 120

        # 读取 symbols.yaml.index_core
        symbols_cfg = load_symbols()
        try:
            self.index_cfg: Dict[str, Dict[str, Any]] = symbols_cfg["index_core"]
        except Exception as e:
            LOG.error("[DS.IndexCore] symbols.yaml 缺失 index_core 段: %s", e)
            raise

        LOG.info(
            "[DS.IndexCore] Init ok. market=%s cache_root=%s window=%s",
            self.market,
            self.cache_root,
            self.window,
        )

    # ---------------------------------------------------------
    # cache 文件路径
    # ---------------------------------------------------------
    def _cache_file(self, symbol: str, trade_date: str) -> str:
        safe = normalize_symbol(symbol)
        return os.path.join(self.cache_root, f"{safe}_{trade_date}.json")


    # ---------------------------------------------------------
    # DB 取数（A 股指数：从 local Oracle 取日线，避免 yf 缺当天收盘）
    # ---------------------------------------------------------
    def _get_series_from_db(self, symbol: str, trade_date: str, window: int) -> pd.DataFrame:
        """Fetch index daily series from local DB (Oracle).

        - Uses db.oracle.tables.index_daily (configured in config/config.yaml) which should point to
          SECOPR.CN_INDEX_DAILY_PRICE (or equivalent).
        - Does NOT rely on PRE_CLOSE / CHG_PCT columns (may be empty / unreliable); pct is computed from CLOSE.
        - Enforces that the returned series includes the requested trade_date (EOD completeness).
        """
        db = get_db_provider()

        index_code = symbol  # symbols.yaml.index_core.symbol should match INDEX_CODE (e.g., sh000300)

        # Use a calendar-day backoff to cover the window without requiring a trading calendar table.
        start_dt = (pd.to_datetime(trade_date) - pd.Timedelta(days=window * 3)).strftime("%Y-%m-%d")

        rows = db.query_index_closes(
            index_code=index_code,
            window_start=start_dt,
            trade_date=trade_date,
        )

        if not rows:
            return pd.DataFrame(columns=["date", "close", "pct"])

        df = pd.DataFrame(rows, columns=["index_code", "trade_date", "close"])
        df["date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
        df = df.sort_values("date").reset_index(drop=True)

        # Oracle driver may return Decimal; normalize to float for pct computations
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        # Compute pct from CLOSE; prev_close will be inferred in _df_to_block from prior row's close.
        df["pct"] = df["close"].pct_change() * 100.0

        # Freshness check: allow a small lag to avoid hard-failing the full block
        # when one index table updates slightly later than the market close batch.
        max_dt = df["date"].iloc[-1] if len(df) else None
        if max_dt != str(trade_date):
            try:
                lag_days = (pd.to_datetime(str(trade_date)) - pd.to_datetime(str(max_dt))).days
            except Exception:
                lag_days = None
            if lag_days is None or lag_days > 3:
                raise RuntimeError(
                    f"[DS.IndexCore][DB] DATA_STALE symbol={symbol} max_dt={max_dt} expect={trade_date}"
                )
            LOG.warning(
                "[DS.IndexCore][DB] soft stale accepted symbol=%s max_dt=%s expect=%s lag_days=%s",
                symbol,
                max_dt,
                trade_date,
                lag_days,
            )

        return df[["date", "close", "pct"]].tail(int(window)).reset_index(drop=True)

    # ---------------------------------------------------------
    # 对外主接口：构建 snapshot["index_core"]
    # ---------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        LOG.info("[DS.IndexCore] build_block trade_date=%s mode=%s", trade_date, refresh_mode)
        return self.get_core_block(trade_date, refresh_mode)

    # ---------------------------------------------------------
    def get_core_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        """
        返回 snapshot["index_core"] 结构：

        {
            "sh": {
                "symbol": "000001.SS",
                "close": 3050.12,
                "prev_close": 3042.10,
                "pct": 0.26,
                "window": [...]
            },
            "sz": { ... },
            ...
        }
        """
        result: Dict[str, Any] = {}

        for name, entry in self.index_cfg.items():
            symbol = entry.get("symbol")
            method = entry.get("method", "index")

            if not symbol:
                LOG.error("[DS.IndexCore] Invalid index_core entry: name=%s entry=%s", name, entry)
                result[name] = self._neutral_block(name)
                continue

            cache_path = self._cache_file(symbol, trade_date)

            # 只对 cache 做刷新；history 由 Store 管理
            mode = apply_refresh_cleanup(
                refresh_mode=refresh_mode,
                cache_path=cache_path,
                history_path=None,
                spot_path=None,
            )
            
            # Step1: 命中 cache
            if mode == "none" and os.path.exists(cache_path):
                try:
                    LOG.info("[DS.IndexCore] HitCache %s (%s)", name, cache_path)
                    result[name] = self._load_json(cache_path)
                    continue
                except Exception as exc:
                    LOG.error("[DS.IndexCore] CacheReadError %s: %s", symbol, exc)

            # Step2: 从 SymbolSeriesStore 获取序列
            try:
                provider_label = entry.get("provider","yf")

                if provider_label == "db":
                    df = self._get_series_from_db(symbol, trade_date, self.window)
                else:
                    df = self.store.get_series(
                        symbol=symbol,
                        window=self.window,
                        refresh_mode=mode,
                        method=method,
                        provider=provider_label,
                    )
            except SystemExit:
                raise
            except Exception as exc:
                LOG.error("[DS.IndexCore] StoreFetchError %s: %s", symbol, exc)
                result[name] = self._neutral_block(symbol)
                continue

            block = self._df_to_block(symbol, df)

            # Step3: 写入 cache block
            try:
                self._save_json(cache_path, block)
            except Exception as exc:
                LOG.error("[DS.IndexCore] SaveCacheError %s: %s", symbol, exc)

            result[name] = block

        return result

    # ---------------------------------------------------------
    @staticmethod
    def _df_to_block(symbol: str, df: pd.DataFrame) -> Dict[str, Any]:
        if df is None or df.empty:
            return {
                "symbol": symbol,
                "close": None,
                "prev_close": None,
                "pct": None,
                "window": [],
            }

        if "date" in df.columns:
            df_sorted = df.sort_values("date").reset_index(drop=True)
        else:
            df_sorted = df.reset_index(drop=True)

        # Normalize numeric types and (re)compute pct from close when upstream provider does not supply it.
        # This fixes cases like KC50 where CLOSE updates but pct stays 0 due to provider/field mismatch.
        if "close" in df_sorted.columns:
            df_sorted["close"] = pd.to_numeric(df_sorted["close"], errors="coerce")

        need_recalc = False
        if "pct" not in df_sorted.columns:
            need_recalc = True
        else:
            df_sorted["pct"] = pd.to_numeric(df_sorted["pct"], errors="coerce")
            pct_non_na = df_sorted["pct"].dropna()
            # If all pct are 0/NaN while close has non-trivial moves, treat as broken pct column.
            if len(pct_non_na) == 0:
                need_recalc = True
            else:
                try:
                    close_moves = df_sorted["close"].pct_change().abs().dropna()
                    pct_sum_abs = float(pct_non_na.abs().sum())
                    if pct_sum_abs == 0.0 and float(close_moves.sum()) > 0.0:
                        need_recalc = True
                except Exception:
                    pass

        if need_recalc and "close" in df_sorted.columns:
            df_sorted["pct"] = df_sorted["close"].pct_change() * 100.0

        last = df_sorted.iloc[-1]
        prev = df_sorted.iloc[-2] if len(df_sorted) >= 2 else None

        close = last.get("close")
        pct = last.get("pct")
        prev_close = prev.get("close") if prev is not None else None

        # Final safeguard: if last pct is missing/zero but close & prev_close imply a move, compute it.
        try:
            if (
                isinstance(close, (int, float))
                and isinstance(prev_close, (int, float))
                and prev_close not in (0, 0.0)
            ):
                implied = (float(close) / float(prev_close) - 1.0) * 100.0
                if pct is None or (isinstance(pct, (int, float)) and float(pct) == 0.0 and abs(implied) > 1e-6):
                    pct = implied
        except Exception:
            pass

        return {
            "symbol": symbol,
            "close": None if pd.isna(close) else float(close),
            "prev_close": None if (prev_close is None or pd.isna(prev_close)) else float(prev_close),
            "pct": None if (pct is None or pd.isna(pct)) else float(pct),
            "window": df_sorted.to_dict("records"),
        }

    # ---------------------------------------------------------
    @staticmethod
    def _neutral_block(symbol: str) -> Dict[str, Any]:
        LOG.warning("[DS.IndexCore] Neutral block for %s", symbol)
        return {
            "symbol": symbol,
            "close": None,
            "prev_close": None,
            "pct": None,
            "window": [],
        }

    # ---------------------------------------------------------
    @staticmethod
    def _load_json(path: str) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, obj: Any) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
