# core/adapters/datasources/cn/margin_source.py
# UnifiedRisk V12 - Margin Intensity DataSource (两融强度)

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)
from core.adapters.providers.provider_router import ProviderRouter
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger

LOG = get_logger("DS.Margin")


class MarginDataSource(DataSourceBase):
    """
    两融 DataSource
    - provider: em
    - 负责 cache / history / trend
    """

    def __init__(self, config: DataSourceConfig, window: int = 40):
        super().__init__(config)

        self.window = window
        self.router = ProviderRouter()
        self.provider = self.router.get_provider("em")

        self.cache_file = os.path.join(config.cache_root, "margin_today.json")
        self.history_file = os.path.join(config.history_root, "margin_series.json")

    # --------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        requested_trade_date = trade_date
        apply_refresh_cleanup(
            refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            cached = self._load(self.cache_file)
            if isinstance(cached, dict) and cached:
                return cached

        # 1) provider 拉数据
        try:
            rows = self.provider.fetch_margin_series(days=self.window)
            if not rows:
                LOG.error("[DS.Margin] empty provider data")
                return self._neutral(trade_date=requested_trade_date, data_status="MISSING", warnings=["missing:margin_series"])
    
            # 2) merge history
            series = self._merge_history(rows)
    
            # 3) 计算趋势
            trend_10d, acc_3d = self._calc_trend(series)
            today = series[-1]
    
            # EM/交易所公布的两融数据通常是 T-1；这里允许 OK，但给出 warning 作为证据链。
            warnings: List[str] = []
            asof_date = str(today.get("date") or "")
            if requested_trade_date and asof_date and requested_trade_date != asof_date:
                warnings.append(f"stale:margin_asof={asof_date} requested={requested_trade_date}")
    
            # lag days
            lag_days: Optional[int] = None
            try:
                d_req = datetime.strptime(str(requested_trade_date), "%Y-%m-%d").date()
                d_asof = datetime.strptime(str(asof_date), "%Y-%m-%d").date()
                lag_days = (d_req - d_asof).days
                if lag_days > 0:
                    warnings.append(f"lag_days:{lag_days}")
            except Exception:
                pass
    
            change_ratio = 0.0
            try:
                total = float(today.get("total") or 0.0)
                total_chg = float(today.get("total_chg") or 0.0)
                if total:
                    change_ratio = total_chg / total
            except Exception:
                change_ratio = 0.0
    
            block = {
                "schema_version": "MARGIN_RAW_V1",
                "data_status": "OK",
                "warnings": warnings,
    
                # keep legacy flat fields for backward compatibility
                "trade_date": asof_date,
                "requested_trade_date": requested_trade_date,
                "asof_lag_days": lag_days,
                "rz_balance": float(today.get("rz_balance") or 0.0),
                "rq_balance": float(today.get("rq_balance") or 0.0),
                "total": float(today.get("total") or 0.0),
                "rz_buy": float(today.get("rz_buy") or 0.0),
                "total_chg": float(today.get("total_chg") or 0.0),
                "rz_ratio": float(today.get("rz_ratio") or 0.0),
                "trend_10d": float(trend_10d or 0.0),
                "acc_3d": float(acc_3d or 0.0),
                "change_ratio": float(change_ratio),
                "series_length": int(len(series)),
                "series": series,
    
                # evidence wrapper (preferred by lead panels)
                "evidence": {
                    "trade_date": asof_date,
                    "requested_trade_date": requested_trade_date,
                    "asof_lag_days": lag_days,
                    "rz_balance": float(today.get("rz_balance") or 0.0),
                    "rq_balance": float(today.get("rq_balance") or 0.0),
                    "total": float(today.get("total") or 0.0),
                    "rz_buy": float(today.get("rz_buy") or 0.0),
                    "total_chg": float(today.get("total_chg") or 0.0),
                    "rz_ratio": float(today.get("rz_ratio") or 0.0),
                    "trend_10d": float(trend_10d or 0.0),
                    "acc_3d": float(acc_3d or 0.0),
                    "change_ratio": float(change_ratio),
                    "series_length": int(len(series)),
                    "series": series,
                },
            }
    
            self._save(self.history_file, series)
            self._save(self.cache_file, block)
            return block
    
        # --------------------------------------------------
        except Exception as exc:
            LOG.exception('[DS.Margin] build_block exception: %s', exc)
            return self._neutral(trade_date=requested_trade_date, data_status='MISSING', warnings=[f'exception:margin_ds:{type(exc).__name__}'])
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        old = self._load(self.history_file) if os.path.exists(self.history_file) else []
        if not isinstance(old, list):
            old = []
        buf = {r["date"]: r for r in old if isinstance(r, dict) and r.get("date")}
        for r in recent:
            buf[r["date"]] = r
        out = sorted(buf.values(), key=lambda x: x["date"])
        return out[-self.window :]

    @staticmethod
    def _calc_trend(series: List[Dict[str, Any]]) -> tuple[float, float]:
        if len(series) < 2:
            return 0.0, 0.0
        totals = [s["total"] for s in series]
        t10 = totals[-1] - totals[-11] if len(totals) >= 11 else 0.0
        a3 = totals[-1] - totals[-4] if len(totals) >= 4 else 0.0
        return round(t10, 2), round(a3, 2)

    @staticmethod
    def _load(path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @staticmethod
    def _save(path: str, obj: Any):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _neutral(trade_date: str, data_status: str = "OK", warnings: Optional[List[str]] = None) -> Dict[str, Any]:
        warnings = list(warnings or [])
        return {
            "schema_version": "MARGIN_RAW_V1",
            "data_status": data_status,
            "warnings": warnings,
            "trade_date": trade_date,
            "requested_trade_date": trade_date,
            "asof_lag_days": None,
            "rz_balance": 0.0,
            "rq_balance": 0.0,
            "total": 0.0,
            "rz_buy": 0.0,
            "total_chg": 0.0,
            "rz_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "change_ratio": 0.0,
            "series_length": 0,
            "series": [],
            "evidence": {
                "trade_date": trade_date,
                "requested_trade_date": trade_date,
                "asof_lag_days": None,
                "rz_balance": 0.0,
                "rq_balance": 0.0,
                "total": 0.0,
                "rz_buy": 0.0,
                "total_chg": 0.0,
                "rz_ratio": 0.0,
                "trend_10d": 0.0,
                "acc_3d": 0.0,
                "change_ratio": 0.0,
                "series_length": 0,
                "series": [],
            },
        }