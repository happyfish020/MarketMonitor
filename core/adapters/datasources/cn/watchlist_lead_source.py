# core/adapters/datasources/cn/watchlist_lead_source.py
# UnifiedRisk V12 · WatchlistLead DataSource (Observation Raw)
#
# Contract:
# - Build snapshot["watchlist_lead_raw"] from config/watchlist_lead.yaml
# - MUST read/write cache explicitly (align with core_theme_source.py)
# - MUST NOT throw (unless SystemExit); fallback to neutral blocks + warnings
# - No silent exception: warnings in payload + LOG.error
#
# Note:
# - This DS is observation-only; it must not affect Gate/DRS/MarketScore.

from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Set

import pandas as pd

from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.symbol_series_store import SymbolSeriesStore
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger
from core.adapters.cache.symbol_cache import normalize_symbol
from core.utils.config_loader import CONFIG_DIR

LOG = get_logger("DS.WatchlistLead")


class WatchlistLeadDataSource(DataSourceBase):
    """WatchlistLead DataSource (raw).

    - Reads config/watchlist_lead.yaml
    - For each symbol key: load from cache if allowed; else fetch via SymbolSeriesStore.get_series(...)
    - Writes per-symbol cache file:
        data/{market}/cache/watchlist_lead/{symbol}_{trade_date}.json

    Output shape (snapshot["watchlist_lead_raw"]):
    {
      "meta": {"schema_version": "WL1", "trade_date": "...", "warnings": [...], "cache_hit_cnt": 0},
      "items": {
        "<key>": {"symbol": "...", "close":..., "prev_close":..., "pct":..., "window":[...], "alias": "..."}
      }
    }
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        super().__init__(config)
        self.config = config
        self.market = config.market
        self.ds_name = config.ds_name

        self.cache_root = config.cache_root
        config.ensure_dirs()

        self.store = SymbolSeriesStore.get_instance()
        self.window = int(window) if window and window > 0 else 60

        self.cfg_path = os.path.join(CONFIG_DIR, "watchlist_lead.yaml")
        self.cfg: Dict[str, Any] = self._load_cfg(self.cfg_path)
        self.schema_version = str(self.cfg.get("schema_version") or "WL1")

        LOG.info(
            "[DS.WatchlistLead] Init ok. market=%s cache_root=%s window=%s cfg=%s",
            self.market,
            self.cache_root,
            self.window,
            self.cfg_path,
        )

    # ---------------------------------------------------------
    def _cache_file(self, symbol: str, trade_date: str) -> str:
        safe = normalize_symbol(symbol)
        return os.path.join(self.cache_root, f"{safe}_{trade_date}.json")

    # ---------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        LOG.info("[DS.WatchlistLead] build_block trade_date=%s mode=%s", trade_date, refresh_mode)
        return self.get_watchlist_block(trade_date=trade_date, refresh_mode=refresh_mode)

    # ---------------------------------------------------------
    def get_watchlist_block(self, trade_date: str, refresh_mode: str) -> Dict[str, Any]:
        warnings: List[str] = []
        items: Dict[str, Any] = {}
        cache_hit_cnt = 0

        symbols_cfg = self.cfg.get("symbols") if isinstance(self.cfg, dict) else {}
        groups_cfg = self.cfg.get("groups") if isinstance(self.cfg, dict) else {}

        wanted_keys = self._collect_symbol_keys(groups_cfg, symbols_cfg)
        if not wanted_keys:
            warnings.append("empty:watchlist_symbols")
            LOG.error("[DS.WatchlistLead] No symbols found in config: %s", self.cfg_path)

        for key in sorted(wanted_keys):
            entry = symbols_cfg.get(key) if isinstance(symbols_cfg, dict) else None
            if not isinstance(entry, dict):
                warnings.append(f"invalid:symbol_entry:{key}")
                items[key] = self._neutral_block(symbol="", alias=str(key))
                continue

            symbol = str(entry.get("symbol") or "").strip()
            method = str(entry.get("method") or "equity").strip()
            provider = str(entry.get("provider") or "yf").strip()
            alias = str(entry.get("alias") or "").strip()

            if not symbol:
                warnings.append(f"missing:symbol:{key}")
                items[key] = self._neutral_block(symbol="", alias=alias)
                continue

            cache_path = self._cache_file(symbol, trade_date)
            mode = apply_refresh_cleanup(refresh_mode=refresh_mode, cache_path=cache_path)

            # Step1: cache read (unless snapshot/full)
            if mode not in ("snapshot", "full") and os.path.exists(cache_path):
                try:
                    LOG.info("[DS.WatchlistLead] HitCache %s (%s)", key, cache_path)
                    block = self._load_json(cache_path)
                    if isinstance(block, dict):
                        block.setdefault("alias", alias)
                    items[key] = block
                    cache_hit_cnt += 1
                    continue
                except Exception as exc:
                    warnings.append(f"cache_read_error:{key}")
                    LOG.error("[DS.WatchlistLead] CacheReadError %s: %s", symbol, exc)

            # Step2: fetch series from store (respect configured provider)
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
                warnings.append(f"store_fetch_error:{key}")
                LOG.error("[DS.WatchlistLead] StoreFetchError %s: %s", symbol, exc)
                items[key] = self._neutral_block(symbol=symbol, alias=alias)
                continue

            block = self._df_to_block(symbol=symbol, df=df, alias=alias)

            # Step3: cache write
            try:
                self._save_json(cache_path, block)
            except Exception as exc:
                warnings.append(f"cache_write_error:{key}")
                LOG.error("[DS.WatchlistLead] SaveCacheError %s: %s", symbol, exc)

            items[key] = block

        return {
            "meta": {
                "schema_version": self.schema_version,
                "trade_date": trade_date,
                "warnings": sorted(set(warnings)),
                "cache_hit_cnt": cache_hit_cnt,
            },
            "items": items,
        }

    # ---------------------------------------------------------
    def _collect_symbol_keys(self, groups_cfg: Any, symbols_cfg: Any) -> Set[str]:
        keys: Set[str] = set()
        if isinstance(groups_cfg, dict):
            for _, g in groups_cfg.items():
                if not isinstance(g, dict):
                    continue
                for k in (g.get("symbols") or []):
                    if isinstance(k, str) and k.strip():
                        keys.add(k.strip())

        # Fallback: if groups empty, take all symbols
        if not keys and isinstance(symbols_cfg, dict):
            for k in symbols_cfg.keys():
                if isinstance(k, str) and k.strip():
                    keys.add(k.strip())
        return keys

    # ---------------------------------------------------------
    def _df_to_block(self, symbol: str, df: Any, alias: str = "") -> Dict[str, Any]:
        if df is None or (hasattr(df, "empty") and df.empty):
            return self._neutral_block(symbol=symbol, alias=alias)

        # SymbolSeriesStore usually provides a 'date' column; keep compatibility with 'trade_date'
        if hasattr(df, "columns"):
            if "date" in df.columns:
                df_sorted = df.sort_values("date").reset_index(drop=True)
            elif "trade_date" in df.columns:
                df_sorted = df.sort_values("trade_date").reset_index(drop=True)
            else:
                df_sorted = df.reset_index(drop=True)
        else:
            return self._neutral_block(symbol=symbol, alias=alias)

        last = df_sorted.iloc[-1]
        prev = df_sorted.iloc[-2] if len(df_sorted) >= 2 else None

        close = last.get("close")
        pct = last.get("pct")
        prev_close = prev.get("close") if prev is not None else None

        def _f(x):
            if x is None:
                return None
            try:
                if pd.isna(x):
                    return None
            except Exception:
                pass
            try:
                return float(x)
            except Exception:
                return None

        return {
            "symbol": symbol,
            "alias": alias,
            "close": _f(close),
            "prev_close": _f(prev_close),
            "pct": _f(pct),
            "window": df_sorted.to_dict("records"),
        }

    # ---------------------------------------------------------
    def _neutral_block(self, symbol: str, alias: str = "") -> Dict[str, Any]:
        return {"symbol": symbol, "alias": alias, "close": None, "prev_close": None, "pct": None, "window": []}

    # ---------------------------------------------------------
    @staticmethod
    def _load_json(path: str) -> Any:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _save_json(path: str, obj: Any) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    # ---------------------------------------------------------
    def _load_cfg(self, path: str) -> Dict[str, Any]:
        try:
            if not os.path.exists(path):
                LOG.error("[DS.WatchlistLead] config missing: %s", path)
                return {}
            import yaml
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception as exc:
            LOG.error("[DS.WatchlistLead] config load error: %s", exc)
            return {}
