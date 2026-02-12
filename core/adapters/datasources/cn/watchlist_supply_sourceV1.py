# -*- coding: utf-8 -*-
"""
core/adapters/datasources/cn/watchlist_supply_source.py

WatchlistSupply DataSource (RAW MVP / UAT)

【定位】
- 只做“cache 编排 + 配置装配 + 调用 fetcher”
- 不做解释/聚合/打分（这些必须在更上层：Factor / WatchlistLeadBuilder）
- 永不抛异常：任何失败都返回结构化 default block + warnings
- refresh_mode 对齐 core/utils/ds_refresh.py（none / refresh / force）

【产出（RAW）】
build_block(trade_date, refresh_mode='none', cfg_path='config/watchlist_supply.yaml') -> dict:
{
  "schema": "...",
  "asof": {"trade_date": "...", "kind": "EOD"},
  "data_status": "OK|PARTIAL|MISSING|ERROR",
  "warnings": [...],
  "meta": {...},
  "items": { "300394.SZ": { "insider": {...}, "block_trade": {...}, ... } }
}

注意：该 DS 仅用于底层可行性验证，不接入 Gate/DRS，也不改引擎 pipeline。
"""
from __future__ import annotations

import json,os
from typing import Any, Dict, List, Optional
from core.utils.ds_refresh import apply_refresh_cleanup

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # type: ignore


from core.adapters.fetchers.cn.watchlist_supply_fetcher import WatchlistSupplyFetcher
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)
#sfrom core.utils.ds_refresh import should_refresh


try:
    from core.utils.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging
    def get_logger(name: str):
        return logging.getLogger(name)


LOG = get_logger("DS.WatchlistSupply")


class WatchlistSupplyDataSource(DataSourceBase):
    """
    RAW 供给压力数据源（董监高 + 大宗）。
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(config)
        self._fetcher = WatchlistSupplyFetcher()
       
        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root

    def _load_cfg(self, cfg_path: str) -> Dict[str, Any]:
        if yaml is None:
            raise RuntimeError("pyyaml_not_installed")
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
   
    def _cache_path(self, trade_date: str) -> str:
        # 使用 DataSourceConfig 的标准路径：data/cn/cache/watchlist_supply/watchlist_supply_YYYY-MM-DD.json
        self.cache_root = self._ds_cfg.cache_root
        cache_file = os.path.join(self.cache_root, f"watchlist_supply_{trade_date}.json")
        return cache_file 
        #return self._ds_cfg.cache_path(trade_date=trade_date)  

    def _read_cache(self, trade_date: str) -> Optional[Dict[str, Any]]:
        p = self._cache_path(trade_date)
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except Exception as e:
            LOG.warning("read_cache failed: %s", e)
            return None

    def _write_cache(self, trade_date: str, payload: Dict[str, Any]) -> None:
        p = self._cache_path(trade_date)
        try:
            self._ds_cfg.ensure_cache_dir()
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            # cache 写入失败不应导致系统崩溃，但要可定位
            LOG.warning("write_cache failed: %s", e)

    def _default_block(self, trade_date: str, kind: str, schema: str, warnings: List[str], err: Optional[str] = None) -> Dict[str, Any]:
        blk = {
            "schema": schema,
            "asof": {"trade_date": trade_date, "kind": kind},
            "data_status": "MISSING" if not err else "ERROR",
            "warnings": warnings,
            "error": err,
            "meta": {
                "contribute_to_market_score": False,
                "source": "watchlist_supply_ds_raw",
                "config_ref": None,
            },
            "items": {},
        }
        return blk

    def build_block(self, trade_date: str, refresh_mode: str = "none", cfg_path: str = "config/watchlist_supply.yaml", kind: str = "EOD") -> Dict[str, Any]:
        warnings: List[str] = []
        schema = "WL_SUPPLY_RAW_MVP_2026Q1"

        # 1) 读配置
        try:
            cfg = self._load_cfg(cfg_path)
        except Exception as e:
            return self._default_block(trade_date, kind, schema, warnings + [f"cfg_load_failed:{type(e).__name__}"], err=str(e))

        schema = str(cfg.get("schema") or schema)

        # 2) refresh 控制：命中 cache 且无需刷新 -> 直接返回
        
        cache_file = os.path.join(self.cache_root, f"watchlist_supply_{trade_date}.json")
        # 命中 cache
        if refresh_mode in ("none", "readonly")  and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.Watchlist_supply] load cache error: %s", e)


        

        # 3) fetch RAW
        symbols = []
        try:
            for it in (cfg.get("symbols") or []):
                sym = (it or {}).get("symbol")
                if sym:
                    symbols.append(str(sym))
        except Exception:
            warnings.append("cfg_symbols_parse_failed")

        if not symbols:
            return self._default_block(trade_date, kind, schema, warnings + ["empty:symbols"])

        fetch_cfg = {
            "lookback_days": int(cfg.get("lookback_days", 60) or 60),
            "max_rows": int((cfg.get("fetch", {}) or {}).get("top_n_records", 60) or 60),
        }

        raw = self._fetcher.fetch(trade_date=trade_date, symbols=symbols, cfg=fetch_cfg)

        # 4) 组装 block（不聚合、不解释）
        items = raw.get("symbols", {}) if isinstance(raw, dict) else {}
        # overall status
        st = "OK"
        ok_cnt = 0
        miss_cnt = 0
        err_cnt = 0
        total = 0
        for sym, v in (items or {}).items():
            total += 1
            ins = (v or {}).get("insider", {}) or {}
            dz = (v or {}).get("block_trade", {}) or {}
            if ins.get("data_status") == "ERROR" or dz.get("data_status") == "ERROR":
                err_cnt += 1
            if ins.get("data_status") in ("OK",) or dz.get("data_status") in ("OK",):
                ok_cnt += 1
            if ins.get("data_status") in ("MISSING",) and dz.get("data_status") in ("MISSING",):
                miss_cnt += 1

        if err_cnt > 0:
            st = "ERROR"
        elif miss_cnt == total:
            st = "MISSING"
        elif miss_cnt > 0:
            st = "PARTIAL"
        else:
            st = "OK"

        block = {
            "schema": schema,
            "asof": {"trade_date": trade_date, "kind": kind},
            "data_status": st,
            "warnings": warnings + (raw.get("warnings", []) if isinstance(raw, dict) else []),
            "error": None,
            "meta": {
                "contribute_to_market_score": False,
                "source": "akshare",
                "raw_meta": raw.get("meta", {}) if isinstance(raw, dict) else {},
                "config_ref": {"path": cfg_path, "version": cfg.get("version")},
            },
            "items": items,
        }
        
         # 写 cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Watchlist_supply] save cache error: %s", e)
            raise e

        # 5) 写 cache（RAW block）
        #self._write_cache(trade_date, block)
        
        
        return block
