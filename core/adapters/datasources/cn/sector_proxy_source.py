# core/adapters/datasources/cn/sector_proxy_source.py
# UnifiedRisk V12 - Sector Proxy DataSource (MVP)
#
# 冻结原则：
# - 只构建 snapshot["sector_proxy_raw"] 原始数据块
# - 不做评分/趋势判断
# - 所有序列统一由 SymbolSeriesStore 管理
# - daily cache 落地，便于回放/复核

from __future__ import annotations

import json
import os
from typing import Any, Dict

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.config_loader import load_symbols
from core.utils.ds_refresh import apply_refresh_cleanup
from core.adapters.providers.symbol_series_store import SymbolSeriesStore


LOG = get_logger("DS.SectorProxy")


class SectorProxyDataSource(DataSourceBase):
    """Sector Proxy 数据源（MVP）

    本 DS 职责：
    - 构建 snapshot["sector_proxy_raw"] 的 raw 数据块
    - 不进行任何评分、趋势、强弱判断

    输出结构（冻结版）：

    {
      "benchmark": {"symbol": "...", "close": .., "prev_close": .., "pct": .., "window": [...]},
      "sectors": {
         "<sector_key>": {"symbol": "...", "close": .., "prev_close": .., "pct": .., "window": [...]},
         ...
      },
      "meta": {"trade_date": "YYYY-MM-DD", "window": 120, "data_status": "OK|..."}
    }
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

        symbols_cfg = load_symbols()
        try:
            self.cfg: Dict[str, Any] = symbols_cfg["sector_proxy"]
        except Exception as e:
            LOG.error("[DS.SectorProxy] symbols.yaml 缺失 sector_proxy 段: %s", e)
            raise

        LOG.info(
            "[DS.SectorProxy] Init ok. market=%s cache_root=%s window=%s",
            self.market,
            self.cache_root,
            self.window,
        )

    # ---------------------------------------------------------
    def _cache_file(self, trade_date: str) -> str:
        return os.path.join(self.cache_root, f"sector_proxy_raw_{trade_date}.json")

    # ---------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        LOG.info("[DS.SectorProxy] build_block trade_date=%s mode=%s", trade_date, refresh_mode)
        return self.get_sector_proxy_block(trade_date, refresh_mode)

    # ---------------------------------------------------------
    def get_sector_proxy_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        cache_path = self._cache_file(trade_date)

        mode = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_path,
            history_path=None,
            spot_path=None,
        )

        # Step1: cache 命中
        if mode == "none" and os.path.exists(cache_path):
            try:
                LOG.info("[DS.SectorProxy] HitCache (%s)", cache_path)
                return self._load_json(cache_path)
            except Exception as exc:
                LOG.error("[DS.SectorProxy] CacheReadError: %s", exc)

        # Step2: build raw block
        result: Dict[str, Any] = {
            "benchmark": self._neutral_block("benchmark"),
            "sectors": {},
            "meta": {"trade_date": trade_date, "window": self.window, "data_status": "OK"},
        }

        bench_entry = self.cfg.get("benchmark")
        if isinstance(bench_entry, dict):
            result["benchmark"] = self._fetch_entry(bench_entry, mode)
        else:
            result["meta"]["data_status"] = "CONFIG_MISSING:benchmark"

        sectors_entry = self.cfg.get("sectors", {})
        if isinstance(sectors_entry, dict):
            out_sectors: Dict[str, Any] = {}
            for k, entry in sectors_entry.items():
                if not isinstance(k, str):
                    continue
                if not isinstance(entry, dict):
                    out_sectors[k] = self._neutral_block(str(k))
                    continue
                out_sectors[k] = self._fetch_entry(entry, mode)
            result["sectors"] = out_sectors
        else:
            result["meta"]["data_status"] = "CONFIG_MISSING:sectors"

        # Step3: 写入 daily cache
        try:
            self._save_json(cache_path, result)
        except Exception as exc:
            LOG.error("[DS.SectorProxy] SaveCacheError: %s", exc)

        return result

    # ---------------------------------------------------------
    def _fetch_entry(self, entry: Dict[str, Any], refresh_mode: str) -> Dict[str, Any]:
        symbol = entry.get("symbol")
        method = entry.get("method", "etf")
        provider = entry.get("provider", "yf")

        if not isinstance(symbol, str) or not symbol.strip():
            LOG.error("[DS.SectorProxy] Invalid entry (missing symbol): %s", entry)
            return self._neutral_block("invalid_symbol")

        try:
            df = self.store.get_series(
                symbol=symbol,
                window=self.window,
                refresh_mode=refresh_mode,
                method=method,
                provider=provider,
            )
        except SystemExit:
            raise
        except Exception as exc:
            LOG.error("[DS.SectorProxy] StoreFetchError %s: %s", symbol, exc)
            return self._neutral_block(symbol)

        block = self._df_to_block(symbol, df)
        # carry meta (optional) for report/debug
        if isinstance(entry.get("alias"), str):
            block["alias"] = entry.get("alias")
        if isinstance(entry.get("group"), str):
            block["group"] = entry.get("group")
        return block

    # ---------------------------------------------------------
    @staticmethod
    def _df_to_block(symbol: str, df: pd.DataFrame) -> Dict[str, Any]:
        if df is None or df.empty:
            return {"symbol": symbol, "close": None, "prev_close": None, "pct": None, "window": []}

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

    @staticmethod
    def _neutral_block(symbol: str) -> Dict[str, Any]:
        LOG.warning("[DS.SectorProxy] Neutral block for %s", symbol)
        return {"symbol": symbol, "close": None, "prev_close": None, "pct": None, "window": []}

    @staticmethod
    def _load_json(path: str) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, obj: Any) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
