# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - BreadthDataSource (CN A-Share)

职责（事实层）：
- 从本地 DB 读取全市场股票 close
- 计算：50 日新低比例（new_low_ratio）
- 不做状态判断，不做预测

输出（当日）：
{
  "trade_date": "YYYY-MM-DD",
  "window": 50,
  "count_new_low": int,
  "count_total": int,
  "new_low_ratio": float,
}
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any
import os
import json
import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.db_provider_oracle import DBOracleProvider

LOG = get_logger("DS.Breadth")


class BreadthDataSource(DataSourceBase):
    def __init__(self, cfg: DataSourceConfig, window: int = 50):
        super().__init__(cfg)
        self.config = cfg

        self.cache_root = self.config.cache_root        
        self.window = int(window)   # ← ★ 必须有这一行
        
        self.db = DBOracleProvider()

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:

        cache_file = os.path.join(self.cache_root, f"breadth_{trade_date}.json")

        td = pd.to_datetime(trade_date)
        start = (td - timedelta(days=self.window * 2)).strftime("%Y-%m-%d")
        end = td.strftime("%Y-%m-%d")
         
        # 读取窗口内 close（允许停牌缺失）
        #df = self.db.query_stock_closes(window_start=start, trade_date=end)
        df: pd.DataFrame = self.db.fetch_daily_new_low_stats(
            trade_date=trade_date,
            look_back_days=90,  # 回溯足够天数保证有20个交易日
            #window_days=20,     # 窗口固定20日
        )
 


        # === 🔴 关键修复：DB 返回的是 list[tuple] ===
        if df is None or len(df) == 0:
            LOG.warning("[DS.Breadth] empty data (raw)")
            raise Exception("[DS.Breadth] empty data (raw)")
            return {}
        
         
        if df.empty:
            LOG.warning("[DS.Breadth] empty dataframe after conversion")
            return {}
    
###
# SQL 已按日期降序返回，最新在前
        recent_df = df.head(20)  # 取最近20个交易日

        if recent_df.empty:
            LOG.error("[DS.Breadth] no recent data after head(20) for %s", trade_date)
            return self._empty_block()

        # 当前值：最新一天
        latest_row = recent_df.iloc[0]
        current_ratio = float(latest_row["new_low_50d_ratio"])
        current_total = int(latest_row["count_total"])          # ← 原代码 count_total 的替代
        current_new_low = int(latest_row["count_new_low_50d"]) # 可选使用
        latest_trade_date = latest_row["trade_date"].strftime("%Y-%m-%d")

        # window 列表（降序：最新在前）
        window = [
            {
                "trade_date": row["trade_date"].strftime("%Y-%m-%d"),
                "count_new_low": int(row["count_new_low_50d"]),
                "count_total": int(row["count_total"]),            # ← 替代原 merged.shape[0]
                "new_low_ratio": float(row["new_low_50d_ratio"]),
            }
            for _, row in recent_df.iterrows()
        ]

        block = {
            "trade_date": latest_trade_date,
            "count_new_low": current_new_low,           
            "count_total": current_total,          # 如果你原来在 block 中用了 count_total，可加
            "new_low_ratio": current_ratio, 
            "window": window,
        }

        LOG.info(
            "[DS.Breadth] built: trade_date=%s new_low_ratio=%.2f count_total=%d window_len=%d",
            latest_trade_date,
            current_ratio,
            current_total,
            len(window),
        )

        # 缓存 + history（保持不变）
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Breadth] cache save failed: %s", e) 

        return block
