# core/adapters/datasources/cn/market_sentiment_source.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig,DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.spot_store import get_spot_daily

LOG = get_logger("DS.Sentiment")


class MarketSentimentDataSource:
    """
    V12 市场情绪 (内部宽度) 数据源
    - 来自全行情 spot DataFrame（adv/dec/limit_up/limit_down）
    - 使用 DataSourceConfig 管理 cache/history
    - trade_date 作为参数传入
    """

    def __init__(self, config: DataSourceConfig):
        self.market = config.market
        self.ds_name = config.ds_name
        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            f"[DS.Sentiment] Init: market={self.market}, cache={self.cache_root}, history={self.history_root}"
        )

    # ------------------------------------------------------------
    #   主接口：从 spot DataFrame 统计宽度
    # ------------------------------------------------------------
    def get_sentiment_block(self, spot_df: pd.DataFrame, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        spot_df: 从 SpotStore 读出的全行情行情
        trade_date: YYYY-MM-DD
        """
        cache_today = os.path.join(self.cache_root, f"sentiment_{trade_date}.json")
        hist_path = os.path.join(self.history_root, "sentiment_series.json")
         

 
        # ============= Step 1: 按 protocol 清理文件 ============
        mode = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_today,
            history_path=hist_path,
            spot_path=None,
        )



        LOG.info(
            f"[DS.Sentiment] Request sentiment trade_date={trade_date} refresh_mode={refresh_mode}"
        )

        

        # ---- 1) 若不要求刷新 → 读今日缓存 ----
        if mode!="full"   and os.path.exists(cache_today):
            try:
                with open(cache_today, "r", encoding="utf-8") as f:
                    data = json.load(f)
                LOG.info(f"[DS.Sentiment] HitCache: {cache_today}")
                return data
            except Exception as e:
                LOG.error(f"[DS.Sentiment] CacheReadFailed: {e}")

        # ---- 获取全行情 spot ----
        df: pd.DataFrame = get_spot_daily(
            trade_date=trade_date,
            mode=mode,
        )
        
        # ⭐ V12 修复：
        # 使用 df（spot_store 的结果）作为统计源，不依赖外部的 spot_df
        if df is None or df.empty:
            LOG.warning("[DS.Sentiment] spot_store returned empty df → neutral")
            return self._neutral_block(trade_date)
        
        spot_df = df
        
        # ---- 2) 统计宽度 ----
        adv = int((spot_df["涨跌幅"] > 0).sum())
        dec = int((spot_df["涨跌幅"] < 0).sum())
        flat = int((spot_df["涨跌幅"] == 0).sum())

        limit_up = int((spot_df["涨跌幅"] >= 9.9).sum())
        limit_down = int((spot_df["涨跌幅"] <= -9.9).sum())

        total = adv + dec + flat
        adv_ratio = round(adv / total, 4) if total > 0 else 0.0

        block: Dict[str, Any] = {
            "date": trade_date,
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "adv_ratio": adv_ratio,
        }

        # ---- 3) 追加进 history ----
        hist = []
        if os.path.exists(hist_path):
            try:
                with open(hist_path, "r", encoding="utf-8") as f:
                    hist = json.load(f)
            except Exception:
                hist = []

        hist.append(block)
        hist = hist[-60:]  # 只保留 60 日

        try:
            with open(hist_path, "w", encoding="utf-8") as f:
                json.dump(hist, f, ensure_ascii=False, indent=2)
            LOG.info(f"[DS.Sentiment] SaveHistory rows={len(hist)}")
        except Exception as e:
            LOG.error(f"[DS.Sentiment] SaveHistoryError: {e}")

        # ---- 4) 写今日缓存 ----
        try:
            with open(cache_today, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
            LOG.info(f"[DS.Sentiment] SaveCache {cache_today}")
        except Exception as e:
            LOG.error(f"[DS.Sentiment] CacheSaveError: {e}")

        return block

    # ------------------------------------------------------------
    # 中性返回（当失败或无数据）
    # ------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "date": trade_date,
            "adv": 0,
            "dec": 0,
            "flat": 0,
            "limit_up": 0,
            "limit_down": 0,
            "adv_ratio": 0.0,
        }
