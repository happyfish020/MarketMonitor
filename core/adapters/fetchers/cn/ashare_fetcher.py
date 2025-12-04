"""A 股（CN）数据抓取统一入口（日级 + 盘中）。"""
from __future__ import annotations

from datetime import datetime, date as Date
import os
from typing import Dict, Any

from core.adapters.cache.file_cache import load_json, save_json
from core.adapters.datasources.cn.etf_north_proxy import get_etf_north_proxy
from core.adapters.datasources.cn.market_db_client import MarketDataReaderCN
from core.adapters.datasources.cn.zh_spot_utils import normalize_zh_spot_columns

from core.utils.config_loader import load_paths
from core.utils.time_utils import now_bj
from core.utils.logger import log

_paths = load_paths()

DAY_CACHE_ROOT = os.path.join(_paths.get("cache_dir", "data/cache/"), "day_cn")
INTRADAY_CACHE_ROOT = os.path.join(_paths.get("cache_dir", "data/cache/"), "intraday_cn")


# =====================================================================
# 缓存路径工具函数（engine 依赖）
# =====================================================================

def get_daily_cache_path(trade_date: Date) -> str:
    day_str = trade_date.strftime("%Y%m%d")
    cache_dir = os.path.join(DAY_CACHE_ROOT, day_str)
    return os.path.join(cache_dir, "ashare_daily_snapshot.json")


def get_intraday_cache_path() -> str:
    return os.path.join(INTRADAY_CACHE_ROOT, "ashare_intraday_snapshot.json")


# =====================================================================
# 主 Fetcher
# =====================================================================

class AshareFetcher:
    """CN Fetcher 层：只做缓存 + 调用 datasource + 组装 snapshot。"""

    def __init__(self) -> None:
        pass

    # ================================
    # 日级 snapshot 入口
    # ================================

    def get_daily_snapshot(self, trade_date: Date, force_refresh: bool) -> Dict[str, Any]:
        cache_path = get_daily_cache_path(trade_date)

        # ------------------------
        # 缓存优先
        # ------------------------
        if not force_refresh:
            cached = load_json(cache_path)
            if cached is not None:
                log(f"[CN Fetcher] 使用日级缓存: {cache_path}")
                return cached

        log(f"[CN Fetcher] 刷新日级数据 trade_date={trade_date}, force_refresh={force_refresh}")

        # ------------------------
        # 北向 ETF 代理
        # ------------------------
        etf_proxy = get_etf_north_proxy(trade_date)

        # ------------------------
        # 成交额 + 市场情绪（zh_spot）
        # ------------------------
        reader = MarketDataReaderCN(
            trade_date,
            root="data/ashare",
            spot_mode="fallback_once"
        )

        turnover = reader.get_turnover_summary()
        breadth = reader.get_breadth_summary()

        # ------------------------
        # snapshot 组装
        # ------------------------
        bj_now = now_bj()

        data: Dict[str, Any] = {
            "trade_date": trade_date.isoformat(),
            "debug_flag": f"daily_generated_at_{bj_now.isoformat()}",

            "meta": {"source": "UnifiedRisk_V11.6_cn"},

            # 因子需要的内容
            "etf_proxy": etf_proxy,
            "turnover": turnover,
            "breadth": breadth,
        }

        # ------------------------
        # 写入缓存
        # ------------------------
        save_json(cache_path, data)
        return data

    # ================================
    # 盘中 snapshot 入口
    # ================================

    def get_intraday_snapshot(self, bj_now: datetime, force_refresh: bool) -> Dict[str, Any]:
        cache_path = get_intraday_cache_path()

        if not force_refresh:
            cached = load_json(cache_path)
            if cached is not None:
                log(f"[CN Fetcher] 使用盘中缓存: {cache_path}")
                return cached

        log(f"[CN Fetcher] 刷新盘中数据 time={bj_now}, force_refresh={force_refresh}")

        data: Dict[str, Any] = {
            "timestamp": bj_now.isoformat(),
            "debug_flag": f"intraday_generated_at_{bj_now.isoformat()}",
            "index": {"sh_change": 0.0, "cyb_change": 0.0},
            "meta": {"source": "UnifiedRisk_V11.6_cn_intraday"},
        }

        save_json(cache_path, data)
        return data
