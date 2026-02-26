# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - BreadthDataSource (CN A-Share)

閼卞矁鐭楅敍鍫滅皑鐎圭偛鐪伴敍澶涚窗
- 娴犲孩婀伴崷?DB 鐠囪褰囬崗銊ョ閸﹂缚鍋傜粊?close
- 鐠侊紕鐣婚敍?0 閺冦儲鏌婃担搴㈢槷娓氬绱檔ew_low_ratio閿?
- 娑撳秴浠涢悩鑸碘偓浣稿灲閺傤叏绱濇稉宥呬粵妫板嫭绁?

鏉堟挸鍤敍鍫濈秼閺冦儻绱氶敍?
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
from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

LOG = get_logger("DS.Breadth")


class BreadthDataSource(DataSourceBase):
    def __init__(self, cfg: DataSourceConfig, window: int = 50):
        super().__init__(cfg)
        self.config = cfg
        
        self.window = int(window)   # 閳?閳?韫囧懘銆忛張澶庣箹娑撯偓鐞?
        
        self.db = DBMySQLMarketProvider()

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:
        td = pd.to_datetime(trade_date)
        start = (td - timedelta(days=self.window * 2)).strftime("%Y-%m-%d")
        end = td.strftime("%Y-%m-%d")

        # 鐠囪褰囩粣妤€褰涢崘?close閿涘牆鍘戠拋绋夸粻閻楀瞼宸辨径鎲嬬礆
        df = self.db.query_stock_closes(window_start=start, trade_date=end)
 
 


        # === 棣冩暥 閸忔娊鏁穱顔碱槻閿涙B 鏉╂柨娲栭惃鍕Ц list[tuple] ===
        if df is None or len(df) == 0:
            LOG.warning("[DS.Breadth] empty data (raw)")
            raise Exception("[DS.Breadth] empty data (raw)")
            return {}
        
        # tuple 缂佹挻鐎箛鍛淬€忔稉搴濈稑 SQL 娑撯偓閼?
        # 娴ｇ姵鍩呴崶楣冨櫡閺勵垽绱?symbol, ?, trade_date, close)
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

        # 閸欐牕缍嬮弮銉ょ幆閺?
        today = df[df["trade_date"] == td][["symbol", "close_price"]]
        if today.empty:
            LOG.warning("[DS.Breadth] no today prices")
            return {}

        # 鐠侊紕鐣?50 閺冦儲娓舵担?
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


