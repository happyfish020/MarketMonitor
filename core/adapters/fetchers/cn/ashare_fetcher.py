# -*- coding: utf-8 -*-
"""
A 股（CN）数据抓取统一入口（日级 + 盘中） — V11.7 FINAL
严格遵守：
- snapshot：统一汇总缓存（唯一文件）
- symbolcache：datasource 单标缓存（内部处理）
- datasource：不写 snapshot，不写 datasource 级 JSON
- breadth：来自 zh_spot，不需要 breadth_series_client
"""

from __future__ import annotations

from datetime import datetime, date as Date
import os
from typing import Dict, Any

from core.adapters.cache.file_cache import load_json, save_json
from core.adapters.datasources.cn.etf_north_proxy import get_etf_north_proxy
from core.adapters.datasources.cn.market_db_client import MarketDataReaderCN
from core.adapters.datasources.cn.zh_spot_utils import normalize_zh_spot_columns

from core.adapters.datasources.cn.em_margin_client import EastmoneyMarginClientCN
from core.adapters.datasources.glo.index_series_client import IndexSeriesClient
from core.adapters.datasources.glo.global_lead_client  import GlobalLeadClient   # ★ breadth_series 已删除

from core.utils.config_loader import load_paths
from core.utils.time_utils import now_bj
from core.utils.logger import log

# ---------------------------------------------------------------------
_paths = load_paths()

DAY_CACHE_ROOT = os.path.join(_paths.get("cache_dir", "data/cache/"), "day_cn")
INTRADAY_CACHE_ROOT = os.path.join(_paths.get("cache_dir", "data/cache/"), "intraday_cn")
ASHARE_ROOT = _paths.get("ashare_root", "data/ashare")


def get_daily_cache_path(trade_date: Date) -> str:
    """日级 snapshot 缓存路径"""
    day_str = trade_date.strftime("%Y%m%d")
    return os.path.join(DAY_CACHE_ROOT, day_str, "ashare_daily_snapshot.json")


def get_intraday_cache_path() -> str:
    """盘中 snapshot 缓存路径"""
    return os.path.join(INTRADAY_CACHE_ROOT, "ashare_intraday_snapshot.json")


# =====================================================================
# 主 Fetcher
# =====================================================================

class AshareFetcher:
    """CN Fetcher 层：只做缓存 + 调用 datasource + 组装 snapshot。"""

    # ================================
    # 日级 snapshot 入口
    # ================================
    def get_daily_snapshot(self, trade_date: Date, force_refresh: bool = False) -> Dict[str, Any]:
        """
        UnifiedRisk V11.7.1（正式）
        A 股日级 snapshot 构建流程：
            1. 缓存命中 → 直接返回
            2. 刷新数据源
               - ETF Proxy（北向代理）
               - zh_spot（成交额 + 宽度）
               - Margin（两融）
               - IndexSeries（大盘指数序列）
               - GlobalLead（海外引导因子）
            3. 写入 snapshot JSON
        """
    
        snapshot_path = get_daily_cache_path(trade_date)
    
        # =====================================================
        # 1）缓存读取
        # =====================================================
        if not force_refresh:
            cached = load_json(snapshot_path)
            if cached:
                log(f"[CN Fetcher] 使用缓存 snapshot: {snapshot_path}")
                return cached
    
        log(f"[CN Fetcher] 刷新 snapshot trade_date={trade_date}, force={force_refresh}")
    
        # =====================================================
        # 2）ETF Proxy（北向代理因子）
        # =====================================================
        etf_proxy = get_etf_north_proxy(trade_date, force_refresh=force_refresh)
    
        # =====================================================
        # 3）zh_spot: 成交额 & 宽度
        # =====================================================
        reader = MarketDataReaderCN(
            trade_date,
            root=ASHARE_ROOT,
            spot_mode="dev_debug_once",
        )
        turnover = reader.get_turnover_summary()
        breadth = reader.get_breadth_summary()
    
        # =====================================================
        # 4）两融（margin）
        # =====================================================
        margin_series = EastmoneyMarginClientCN().get_recent_series(max_days=60)
        margin_block = {"series": margin_series}
    
        # =====================================================
        # 5）指数序列（index_series）
        # =====================================================
        try:
            index_series = IndexSeriesClient().fetch(trade_date)
        except Exception as e:
            log(f"[CN Fetcher] index_series 失败: {e}")
            index_series = {
                "error": str(e)
            }
    
        # =====================================================
        # 6）全球引导（global_lead）
        # =====================================================
        try:
            gl_client = GlobalLeadClient()
            global_lead = gl_client.fetch(trade_date)
    
            # ★ 必须确保 global_lead 是结构化结果
            if not global_lead or isinstance(global_lead, dict) and len(global_lead) == 0:
                log("[CN Fetcher] global_lead 返回空 → 使用兜底结构")
                global_lead = {
                    "score": 50,
                    "level": "中性",
                    "details": {"msg": "global_lead 数据缺失"},
                }
    
        except Exception as e:
            log(f"[CN Fetcher] global_lead 失败: {e}")
            global_lead = {
                "score": 50,
                "level": "中性",
                "details": {"msg": f"fetch 失败: {e}"},
            }
    
        # =====================================================
        # 7）最终 snapshot 数据结构
        # =====================================================
        snapshot: Dict[str, Any] = {
            "meta": {
                "trade_date": trade_date.isoformat(),
                "version": "UnifiedRisk_V11.7.1",
                "source": "cn_ashare_daily",
            },
    
            "etf_proxy": etf_proxy,
            "turnover": turnover,
            "breadth": breadth,
            "margin": margin_block,
    
            # 新增/修复
            "index_series": index_series,
            "global_lead": global_lead,
        }
    
        # =====================================================
        # 8）写入缓存
        # =====================================================
        os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
        save_json(snapshot_path, snapshot)
        log(f"[CN Fetcher] snapshot 写入完成: {snapshot_path}")
    
        return snapshot
    
    # ================================
    # 盘中 snapshot（保持原样）
    # ================================
    def get_intraday_snapshot(self, bj_now: datetime, force_refresh: bool) -> Dict[str, Any]:
        cache_path = get_intraday_cache_path()

        if not force_refresh:
            cached = load_json(cache_path)
            if cached:
                log(f"[CN Fetcher] 使用盘中缓存: {cache_path}")
                return cached

        log(f"[CN Fetcher] 刷新盘中 snapshot → {bj_now}")

        data: Dict[str, Any] = {
            "timestamp": bj_now.isoformat(),
            "debug_flag": f"intraday_generated_at_{bj_now.isoformat()}",
            "index": {"sh_change": 0.0, "cyb_change": 0.0},
            "meta": {"source": "UnifiedRisk_V11.7_cn_intraday"},
        }

        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        save_json(cache_path, data)
        return data
