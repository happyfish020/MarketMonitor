# core/adapters/datasources/cn/margin_source.py
# UnifiedRisk V12 - Margin DataSource

from __future__ import annotations

import os
import json
from typing import Dict, Any, List

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
        apply_refresh_cleanup(
            refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        if refresh_mode == "none" and os.path.exists(self.cache_file):
            return self._load(self.cache_file)

        # 1) provider 拉数据
        rows = self.provider.fetch_margin_series(days=self.window)
        if not rows:
            LOG.error("[DS.Margin] empty provider data")
            return self._neutral(trade_date)

        # 2) merge history
        series = self._merge_history(rows)

        # 3) 计算趋势
        trend_10d, acc_3d = self._calc_trend(series)
        today = series[-1]

        block = {
            "trade_date": today["date"],
            "rz_balance": today["rz_balance"],
            "rq_balance": today["rq_balance"],
            "total": today["total"],
            "rz_buy": today["rz_buy"],
            "total_chg": today["total_chg"],
            "rz_ratio": today["rz_ratio"],
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": series,
        }

        self._save(self.history_file, series)
        self._save(self.cache_file, block)
        return block

    # --------------------------------------------------
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        old = self._load(self.history_file) if os.path.exists(self.history_file) else []
        buf = {r["date"]: r for r in old}
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
            return []

    @staticmethod
    def _save(path: str, obj: Any):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _neutral(trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "rz_balance": 0.0,
            "rq_balance": 0.0,
            "total": 0.0,
            "rz_buy": 0.0,
            "total_chg": 0.0,
            "rz_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
        }
