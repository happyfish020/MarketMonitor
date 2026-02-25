# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - BreadthDataSource (CN A-Share)

鑱岃矗锛堜簨瀹炲眰锛夛細
- 浠庢湰鍦?DB 璇诲彇鍏ㄥ競鍦鸿偂绁?close
- 璁＄畻锛?0 鏃ユ柊浣庢瘮渚嬶紙new_low_ratio锛?
- 涓嶅仛鐘舵€佸垽鏂紝涓嶅仛棰勬祴

杈撳嚭锛堝綋鏃ワ級锛?
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
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.Breadth")


class BreadthDataSource(DataSourceBase):
    def __init__(self, cfg: DataSourceConfig, window: int = 50):
        super().__init__(cfg)
        self.config = cfg
        
        self.window = int(window)   # 鈫?鈽?蹇呴』鏈夎繖涓€琛?
        
        self.db = DBOracleProvider()

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:
        td = pd.to_datetime(trade_date)
        start = (td - timedelta(days=self.window * 2)).strftime("%Y-%m-%d")
        end = td.strftime("%Y-%m-%d")

        # 璇诲彇绐楀彛鍐?close锛堝厑璁稿仠鐗岀己澶憋級
        df = self.db.query_stock_closes(window_start=start, trade_date=end)
 
 


        # === 馃敶 鍏抽敭淇锛欴B 杩斿洖鐨勬槸 list[tuple] ===
        if df is None or len(df) == 0:
            LOG.warning("[DS.Breadth] empty data (raw)")
            raise Exception("[DS.Breadth] empty data (raw)")
            return {}
        
        # tuple 缁撴瀯蹇呴』涓庝綘 SQL 涓€鑷?
        # 浣犳埅鍥鹃噷鏄細(symbol, ?, trade_date, close)
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

        # 鍙栧綋鏃ヤ环鏍?
        today = df[df["trade_date"] == td][["symbol", "close_price"]]
        if today.empty:
            LOG.warning("[DS.Breadth] no today prices")
            return {}

        # 璁＄畻 50 鏃ユ渶浣?
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

