# core/adapters/datasources/cn/zh_spot_source.py

"""
UnifiedRisk V12 - ZhSpotSource
A 股当日情绪结构（涨跌、涨停、跌停、HS300 涨跌）数据源
"""

import os
from typing import Dict, Any

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger

LOG = get_logger("DS.Spot")


class ZhSpotSource(BaseDataSource):
    """
    返回结构：
    {
        "adv": float | None,         # 上涨家数
        "dec": float | None,         # 下跌家数
        "limit_up": float | None,    # 涨停家数
        "limit_down": float | None,  # 跌停家数
        "hs300_pct": float | None,   # 沪深300 日涨跌幅（%）
    }
    """

    def __init__(self):
        super().__init__("ZhSpotSource")
        self.config = DataSourceConfig(market="cn", ds_name="spot")
        self.config.ensure_dirs()

        self.cache_file = os.path.join(self.config.cache_root, "spot.json")

    def _load_cache(self) -> Dict[str, Any]:
        data = load_json(self.cache_file) or {}
        LOG.info("Spot CacheRead: path=%s data=%s", self.cache_file, data)
        return data

    def _save_cache(self, data: Dict[str, Any]):
        LOG.info("Spot CacheWrite: path=%s data=%s", self.cache_file, data)
        save_json(self.cache_file, data)

    def _fetch_remote(self) -> Dict[str, Any]:
        """
        TODO: 在这里整合 zh_spot_utils 的逻辑，或 AkShare 自统计。
        """
        LOG.warning("Spot RemoteFetch 未实现，当前仅使用缓存数据")
        return {}

    def get_spot_snapshot(self, refresh: bool = False) -> Dict[str, Any]:
        LOG.info("Spot FetchStart: refresh=%s", refresh)

        cache = self._load_cache()

        if not refresh:
            LOG.info("Spot FetchEnd(ReadOnly): %s", cache)
            return cache

        remote = self._fetch_remote()
        if not remote:
            LOG.warning("Spot RemoteFetch 返回空，fallback 到缓存")
            return cache

        data = {
            "adv": remote.get("adv"),
            "dec": remote.get("dec"),
            "limit_up": remote.get("limit_up"),
            "limit_down": remote.get("limit_down"),
            "hs300_pct": remote.get("hs300_pct"),
        }
        self._save_cache(data)

        LOG.info("Spot FetchEnd(Refreshed): %s", data)
        return data
