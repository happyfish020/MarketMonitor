# core/adapters/datasources/providers/symbol_series_store.py
"""
UnifiedRisk V12.1
SymbolSeriesStore - 统一序列历史中心（支持多 Provider：yf / bs）
"""

from __future__ import annotations

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
import yaml

from core.utils.logger import get_logger
from core.utils.config_loader import ROOT_DIR
 
from core.datasources.datasource_base import DataSourceConfig,DataSourceBase
from core.adapters.providers.provider_router import ProviderRouter
from core.adapters.cache.symbol_cache import normalize_symbol

LOG = get_logger("SymbolSeriesStore")


def _load_dev_mode() -> bool:
    cfg = os.path.join(ROOT_DIR, "config", "config.yaml")
    if not os.path.exists(cfg):
        return False
    try:
        with open(cfg, "r", encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}
        return bool(y.get("dev_mode", False))
    except Exception:
        return False


DEV_MODE = _load_dev_mode()


class SymbolSeriesStore:
    _instance = None

    @classmethod
    def get_instance(cls) -> "SymbolSeriesStore":
        if cls._instance is None:
            cls()
        return cls._instance

    def __init__(self, default_window: int = 120):
        if SymbolSeriesStore._instance is not None:
            return
        SymbolSeriesStore._instance = self

        cfg = DataSourceConfig(market="glo", ds_name="symbol_series")
        cfg.ensure_dirs()
        self.history_root = cfg.history_root

        self.router = ProviderRouter()
        self.memory_cache: Dict[str, pd.DataFrame] = {}
        self.default_window = default_window

        LOG.info(
            "[SymbolStore] Initialized history_root=%s providers=%s",
            self.history_root,
            list(self.router.registry.keys()),
        )

    # -------------------------------------------------
    def _history_file(self, symbol: str) -> str:
        return os.path.join(self.history_root, f"{normalize_symbol(symbol)}.json")

    def _load_history(self, symbol: str) -> Optional[Dict[str, Any]]:
        p = self._history_file(symbol)
        if not os.path.exists(p):
            return None
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            LOG.error("[SymbolStore] History read failed %s: %s", symbol, e)
            if DEV_MODE:
                sys.exit(1)
            return None

    def _save_history(self, symbol: str, obj: Dict[str, Any]):
        p = self._history_file(symbol)
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[SymbolStore] History write failed %s: %s", symbol, e)
            if DEV_MODE:
                sys.exit(1)

    # -------------------------------------------------
    @staticmethod
    def _series_to_df(series: list) -> pd.DataFrame:
        if not series:
            return pd.DataFrame(columns=["date", "close", "pct"])
        return pd.DataFrame(series)

    @staticmethod
    def _df_to_series(df: pd.DataFrame) -> list:
        return [
            {
                "date": str(r["date"]),
                "close": None if pd.isna(r["close"]) else float(r["close"]),
                "pct": None if pd.isna(r["pct"]) else float(r["pct"]),
            }
            for _, r in df.iterrows()
        ]

    # -------------------------------------------------
    def _fetch_from_provider(
        self,
        symbol: str,
        provider_label: str,
        window: int,
        method: str,
    ) -> Optional[pd.DataFrame]:
        try:
            provider = self.router.get_provider(provider_label)
            df = provider.fetch(symbol=symbol, window=window, method=method)

            if df is None or df.empty:
                raise ValueError("empty df")

            cols = [c for c in ["date", "close", "pct"] if c in df.columns]
            if len(cols) < 2:
                raise ValueError("missing required columns")

            df = df[cols]
            df["date"] = df["date"].astype(str)
            return df

        except SystemExit:
            raise
        except Exception as e:
            LOG.error(
                "[SymbolStore] Provider fetch failed symbol=%s provider=%s err=%s",
                symbol,
                provider_label,
                e,
            )
            if DEV_MODE:
                sys.exit(1)
            return None

    # -------------------------------------------------
    def get_series(
        self,
        symbol: str,
        window: int = None,
        refresh_mode: str = "none",
        method: str = "index",
        provider: str = "yf",
    ) -> pd.DataFrame:
        window = window or self.default_window
        key = normalize_symbol(symbol)

        # 1. memory
        if key in self.memory_cache and refresh_mode == "none":
            df = self.memory_cache[key]
            if len(df) >= window:
                return df.tail(window).reset_index(drop=True).copy()

        # 2. history
        hist = self._load_history(symbol)
        df_hist = (
            self._series_to_df(hist.get("series", []))
            if hist else pd.DataFrame(columns=["date", "close", "pct"])
        )

        if refresh_mode == "none" and len(df_hist) >= window:
            df_hist = df_hist.sort_values("date")
            self.memory_cache[key] = df_hist.copy()
            return df_hist.tail(window).reset_index(drop=True).copy()

        # 3. provider
        df_new = self._fetch_from_provider(symbol, provider, window, method)

        if df_new is None:
            if not df_hist.empty:
                df_all = df_hist
            else:
                df_all = pd.DataFrame([{
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "close": None,
                    "pct": None,
                }])
        else:
            if df_hist.empty or refresh_mode == "full":
                df_all = df_new
            else:
                df_all = (
                    pd.concat([df_hist, df_new], ignore_index=True)
                    .drop_duplicates(subset=["date"], keep="last")
                )

        df_all = df_all.sort_values("date").reset_index(drop=True)
        self.memory_cache[key] = df_all.copy()

        self._save_history(symbol, {
            "symbol": symbol,
            "provider": provider,
            "series": self._df_to_series(df_all),
        })

        return df_all.tail(window).reset_index(drop=True).copy()
