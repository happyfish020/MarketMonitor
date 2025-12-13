# core/adapters/datasources/glo/global_macro_source.py
# UnifiedRisk V12.1 - Global Macro Daily DataSource
# 特点：不写历史序列（由 SymbolSeriesStore 存），只缓存每日 block

from __future__ import annotations

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig,BaseDataSource
from core.adapters.cache.symbol_cache import normalize_symbol
from core.utils.config_loader import load_symbols
from core.utils.ds_refresh import apply_refresh_cleanup
from core.adapters.providers.symbol_series_store import SymbolSeriesStore

LOG = get_logger("DS.GlobalMacro")


class GlobalMacroSource(BaseDataSource):
    """
    V12.1 全球宏观（Macro）数据源：
    - 全部 YF 数据（TNX / FVX / DXY / IXIC ...）通过 SymbolSeriesStore 拿序列
    - 不保存 data/glo/history/global_macro/*
    - 只负责当日 block 缓存
    """

    def __init__(self, config: DataSourceConfig, window: int = 120):
        super().__init__(config)
        self.config = config
        self.market = config.market
        self.ds_name = config.ds_name

        self.cache_root = config.cache_root
        config.ensure_dirs()

        self.store = SymbolSeriesStore.get_instance()
        self.window = int(window) if window and window > 0 else 120

        # 从 symbols.yaml 加载 global_macro 段
        symbols_cfg = load_symbols()
        try:
            self.macro_cfg: Dict[str, Dict[str, Any]] = symbols_cfg["global_macro"]
        except Exception as e:
            LOG.error("[DS.GlobalMacro] symbols.yaml 缺失 global_macro: %s", e)
            raise

        LOG.info(
            "[DS.GlobalMacro] Init ok. cache_root=%s window=%s",
            self.cache_root,
            self.window,
        )

    # ---------------------------------------------------------
    # cache path
    # ---------------------------------------------------------
    def _cache_file(self, symbol: str, trade_date: str) -> str:
        safe = normalize_symbol(symbol)
        return os.path.join(self.cache_root, f"{safe}_{trade_date}.json")

    # ---------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        LOG.info("[DS.GlobalMacro] build_block trade_date=%s mode=%s", trade_date, refresh_mode)
        return self.get_macro_block(trade_date, refresh_mode)

    # ---------------------------------------------------------
    def get_macro_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for name, entry in self.macro_cfg.items():
            symbol = entry.get("symbol")
            method = entry.get("method", "index")

            if not symbol:
                LOG.error("[DS.GlobalMacro] Invalid entry: %s", entry)
                result[name] = self._neutral_block(name)
                continue

            cache_path = self._cache_file(symbol, trade_date)

            # 刷新 cache（不刷新 history，因为 history 只由 Store 管理）
            mode = apply_refresh_cleanup(
                refresh_mode=refresh_mode,
                cache_path=cache_path,
                history_path=None,
                spot_path=None,
            )

            # Step1: 直接命中 cache
            if mode == "none" and os.path.exists(cache_path):
                try:
                    LOG.info("[DS.GlobalMacro] HitCache %s (%s)", name, cache_path)
                    result[name] = self._load_json(cache_path)
                    continue
                except Exception as exc:
                    LOG.error("[DS.GlobalMacro] CacheReadError %s: %s", symbol, exc)

            # Step2: 获取序列
            try:
                df = self.store.get_series(
                    symbol=symbol,
                    window=self.window,
                    refresh_mode=mode,
                    method=method,
                )
            except SystemExit:
                raise
            except Exception as exc:
                LOG.error("[DS.GlobalMacro] StoreFetchError %s: %s", symbol, exc)
                result[name] = self._neutral_block(symbol)
                continue

            block = self._df_to_block(symbol, df)

            # 写入每日 cache block
            try:
                self._save_json(cache_path, block)
            except Exception as exc:
                LOG.error("[DS.GlobalMacro] SaveCacheError %s: %s", symbol, exc)

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

        df_sorted = df.sort_values("date").reset_index(drop=True)
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

    # ---------------------------------------------------------
    @staticmethod
    def _neutral_block(symbol: str) -> Dict[str, Any]:
        LOG.warning("[DS.GlobalMacro] Neutral block for %s", symbol)
        return {
            "symbol": symbol,
            "close": None,
            "prev_close": None,
            "pct": None,
            "window": [],
        }

    @staticmethod
    def _load_json(path: str) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, obj: Any) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
