# core/adapters/datasources/cn/north_nps_source.py
# UnifiedRisk V12.1 - Northbound Proxy DataSource (NPS)
# 说明：
# - 使用 ETF 作为北向资金代理
# - 所有序列统一由 SymbolSeriesStore 管理
# - 本 DS 只构建 raw block，不做评分/趋势判断

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

LOG = get_logger("DS.NorthNPS")


class NorthNPSDataSource(DataSourceBase):
    """
    V12.1 北向资金代理数据源（ETF-based）

    数据来源：
    - ETF 日线（如 510300 / 159915）
    - 统一通过 SymbolSeriesStore 拉取

    本 DS 职责：
    - 构建 snapshot["north_nps_raw"] 的 raw 数据块
    - 不进行任何评分、趋势、强弱判断
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

        # 读取 symbols.yaml.north_nps
        symbols_cfg = load_symbols()
        try:
            self.nps_cfg: Dict[str, Dict[str, Any]] = symbols_cfg["north_nps"]
        except Exception as e:
            LOG.error("[DS.NorthNPS] symbols.yaml 缺失 north_nps 段: %s", e)
            raise

        LOG.info(
            "[DS.NorthNPS] Init ok. market=%s cache_root=%s window=%s",
            self.market,
            self.cache_root,
            self.window,
        )

    # ---------------------------------------------------------
    def _cache_file(self, symbol: str, trade_date: str) -> str:
        safe = normalize_symbol(symbol)
        return os.path.join(self.cache_root, f"{safe}_{trade_date}.json")

    # ---------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        LOG.info("[DS.NorthNPS] build_block trade_date=%s mode=%s", trade_date, refresh_mode)
        return self.get_nps_block(trade_date, refresh_mode)

    # ---------------------------------------------------------
    def get_nps_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        """
        返回 snapshot["north_nps_raw"]：

        {
            "hs300": {
                "symbol": "510300.SS",
                "close": 4.61,
                "prev_close": 4.58,
                "pct": 0.65,
                "window": [...]
            },
            ...
        }
        """
        result: Dict[str, Any] = {}

        for name, entry in self.nps_cfg.items():
            symbol = entry.get("symbol")
            method = entry.get("method", "etf")
            provider = entry.get("provider", "yf")

            if not symbol:
                LOG.error("[DS.NorthNPS] Invalid entry: %s", entry)
                result[name] = self._neutral_block(name)
                continue

            cache_path = self._cache_file(symbol, trade_date)

            mode = apply_refresh_cleanup(
                refresh_mode=refresh_mode,
                cache_path=cache_path,
                history_path=None,
                spot_path=None,
            )

            # Step1: cache 命中
            if mode == "none" and os.path.exists(cache_path):
                try:
                    LOG.info("[DS.NorthNPS] HitCache %s (%s)", name, cache_path)
                    result[name] = self._load_json(cache_path)
                    continue
                except Exception as exc:
                    LOG.error("[DS.NorthNPS] CacheReadError %s: %s", symbol, exc)

            # Step2: 通过 SymbolSeriesStore 拉 ETF 序列
            try:
                df = self.store.get_series(
                    symbol=symbol,
                    window=self.window,
                    refresh_mode=mode,
                    method=method,
                    provider=provider,
                )
            except SystemExit:
                raise
            except Exception as exc:
                LOG.error("[DS.NorthNPS] StoreFetchError %s: %s", symbol, exc)
                result[name] = self._neutral_block(symbol)
                continue

            block = self._df_to_block(symbol, df)

            # Step3: 写入 daily cache
            try:
                self._save_json(cache_path, block)
            except Exception as exc:
                LOG.error("[DS.NorthNPS] SaveCacheError %s: %s", symbol, exc)

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
        LOG.warning("[DS.NorthNPS] Neutral block for %s", symbol)
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
