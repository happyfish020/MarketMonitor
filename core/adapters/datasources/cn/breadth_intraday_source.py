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

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.db_provider_oracle import DBOracleProvider

LOG = get_logger("DS.Breadth")


class BreadthDataSource(DataSourceBase):
    def __init__(self, cfg: DataSourceConfig, window: int = 50):
        super().__init__(cfg)
        self.config = cfg
        
        self.window = int(window)   # ← ★ 必须有这一行
        
        self.db = DBOracleProvider()

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:
        td = pd.to_datetime(trade_date)
        start = (td - timedelta(days=self.window * 2)).strftime("%Y-%m-%d")
        end = td.strftime("%Y-%m-%d")

        # 读取窗口内 close（允许停牌缺失）
        df = self.db.query_stock_closes(window_start=start, trade_date=end)
 
 


        # === 🔴 关键修复：DB 返回的是 list[tuple] ===
        if df is None or len(df) == 0:
            LOG.warning("[DS.Breadth] empty data (raw)")
            raise Exception("[DS.Breadth] empty data (raw)")
            return {}
        
        # tuple 结构必须与你 SQL 一致
        # 你截图里是：(symbol, ?, trade_date, close)
        df = pd.DataFrame(
            df,
            columns=["symbol", "_unused", "trade_date", "close_price"],
        )
        
        if df.empty:
            LOG.warning("[DS.Breadth] empty dataframe after conversion")
            return {}
    
###



        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values(["symbol", "trade_date"])

        # 取当日价格
        today = df[df["trade_date"] == td][["symbol", "close_price"]]
        if today.empty:
            LOG.warning("[DS.Breadth] no today prices")
            return {}

        # 计算 50 日最低
        lows = (
            df.groupby("symbol", sort=False)
              .tail(self.window)
              .groupby("symbol", sort=False)["close_price"]
              .min()
              .reset_index(name="low_window")
        )

        merged = today.merge(lows, on="symbol", how="inner")
        merged["is_new_low"] = merged["close_price"] <= merged["low_window"]

        count_total = int(merged.shape[0])
        count_new_low = int(merged["is_new_low"].sum())
        ratio = float(count_new_low / count_total) if count_total > 0 else 0.0

        block = {
            "trade_date": end,
            "window": self.window,
            "count_new_low": count_new_low,
            "count_total": count_total,
            "new_low_ratio": ratio,
        }

        return block
