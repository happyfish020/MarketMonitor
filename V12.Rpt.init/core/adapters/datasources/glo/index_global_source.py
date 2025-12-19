# core/adapters/datasources/glo/index_global_source.py
# UnifiedRisk V12.1 - Global Daily Index Source (最终版：不写 DS history，只用 SymbolSeriesStore)

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

LOG = get_logger("DS.IndexGlobal")


class IndexGlobalDataSource(DataSourceBase):
    """
    全球指数日线数据源（基于 SymbolSeriesStore）。

    设计要点：
    - 所有“日线历史序列”由 SymbolSeriesStore 统一管理：
        data/glo/history/symbol_series/*.json
    - 本 DS 只负责：
        · 调用 store.get_series() 拿 df
        · 构建当日 block
        · 把 block 写入 cache：data/glo/cache/index_global/{symbol}_{trade_date}.json
    - 不再维护 data/glo/history/index_global/*.json
    """

    def __init__(self, config: DataSourceConfig, window: int = 120):
        super().__init__(config)
        self.config = config
        self.market = config.market
        self.ds_name = config.ds_name

        # 只需要 cache 目录，history_root 虽然会被 DataSourceConfig 建出来，但我们不使用
        self.cache_root = config.cache_root
        config.ensure_dirs()

        self.store = SymbolSeriesStore.get_instance()
        self.window = int(window) if window and window > 0 else 120

        symbols_cfg = load_symbols()
        try:
            self.index_cfg: Dict[str, Dict[str, Any]] = symbols_cfg["index_global"]
        except Exception as e:
            LOG.error("[DS.IndexGlobal] symbols.yaml 缺失 index_global: %s", e)
            raise

        LOG.info(
            "[DS.IndexGlobal] Init ok. cache_root=%s window=%s",
            self.cache_root,
            self.window,
        )
        print(">>> USING NEW INDEX_GLOBAL_SOURCE <<<")

    # ------------------------------------------------------------------
    # cache 文件路径
    # ------------------------------------------------------------------
    def _cache_file(self, symbol: str, trade_date: str) -> str:
        safe = normalize_symbol(symbol)
        return os.path.join(self.cache_root, f"{safe}_{trade_date}.json")

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        LOG.info("[DS.IndexGlobal] build_block trade_date=%s mode=%s", trade_date, refresh_mode)
        return self.get_global_block(trade_date, refresh_mode)

    # ------------------------------------------------------------------
    def get_global_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        """
        返回 snapshot["index_global"] 结构：
        {
            "spx": {
                "symbol": "^GSPC",
                "close": ...,
                "prev_close": ...,
                "pct": ...,
                "window": [...]
            },
            "ndx": {...},
            ...
        }
        """
        result: Dict[str, Any] = {}

        for name, entry in self.index_cfg.items():
            symbol = entry.get("symbol")
            method = entry.get("method", "equity")
            if not symbol:
                LOG.error("[DS.IndexGlobal] invalid entry: name=%s, entry=%s", name, entry)
                result[name] = self._neutral_block(symbol or name)
                continue

            cache_path = self._cache_file(symbol, trade_date)

            # 这里只对 cache 做刷新控制，history 由 SymbolSeriesStore 内部管理
            mode = apply_refresh_cleanup(
                refresh_mode=refresh_mode,
                cache_path=cache_path,
                history_path=None,
                spot_path=None,
            )

            # Step1: mode=none 且 cache 命中 → 直接读
            if mode == "none" and os.path.exists(cache_path):
                try:
                    LOG.info("[DS.IndexGlobal] HitCache %s (%s)", name, cache_path)
                    result[name] = self._load_json(cache_path)
                    continue
                except Exception as exc:
                    LOG.error("[DS.IndexGlobal] CacheReadError %s: %s", symbol, exc)

            # Step2: 通过 SymbolSeriesStore 拿日线序列
            try:
                df = self.store.get_series(
                    symbol=symbol,
                    window=self.window,
                    refresh_mode=mode,
                    method=method,
                )
            except SystemExit:
                # dev_mode 下允许直接退出
                raise
            except Exception as exc:
                LOG.error("[DS.IndexGlobal] StoreFetchError %s: %s", symbol, exc)
                result[name] = self._neutral_block(symbol)
                continue

            block = self._df_to_block(symbol, df)

            # 写入当日 cache（block）
            try:
                self._save_json(cache_path, block)
            except Exception as exc:
                LOG.error("[DS.IndexGlobal] SaveCacheError %s: %s", symbol, exc)

            result[name] = block

        return result

    # ------------------------------------------------------------------
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

        last = df_sorted.iloc[-1]
        prev = df_sorted.iloc[-2] if len(df_sorted) >= 2 else None

        close = last.get("close")
        pct = last.get("pct")
        prev_close = prev.get("close") if prev is not None else None

        return {
            "symbol": symbol,
            "close": None if pd.isna(close) else float(close),
            "prev_close": None if (prev_close is None or pd.isna(prev_close)) else float(prev_close),
            "pct": None if (pct is None or pd.isna(pct)) else float(pct),
            "window": df_sorted.to_dict("records"),
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _neutral_block(symbol: str) -> Dict[str, Any]:
        LOG.warning("[DS.IndexGlobal] Neutral block for %s", symbol)
        return {
            "symbol": symbol,
            "close": None,
            "prev_close": None,
            "pct": None,
            "window": [],
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _load_json(path: str) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, obj: Any) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)