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
import os
import json
import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

LOG = get_logger("DS.Breadth")


class BreadthDataSource(DataSourceBase):
    def __init__(self, cfg: DataSourceConfig, window: int = 50):
        super().__init__(cfg)
        self.config = cfg

        self.cache_root = self.config.cache_root        
        self.window = int(window)   # 閳?閳?韫囧懘銆忛張澶庣箹娑撯偓鐞?
        
        self.db = DBMySQLMarketProvider()

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:

        cache_file = os.path.join(self.cache_root, f"breadth_{trade_date}.json")

        td = pd.to_datetime(trade_date)
        start = (td - timedelta(days=self.window * 2)).strftime("%Y-%m-%d")
        end = td.strftime("%Y-%m-%d")
         
        # 鐠囪褰囩粣妤€褰涢崘?close閿涘牆鍘戠拋绋夸粻閻楀瞼宸辨径鎲嬬礆
        #df = self.db.query_stock_closes(window_start=start, trade_date=end)
        df: pd.DataFrame = self.db.fetch_daily_new_low_stats(
            trade_date=trade_date,
            look_back_days=90,  # 閸ョ偞鍑界搾鍐差檮婢垛晜鏆熸穱婵婄槈閺?0娑擃亙姘﹂弰鎾存）
            #window_days=20,     # 缁愭褰涢崶鍝勭暰20閺?
        )
 


        # === 棣冩暥 閸忔娊鏁穱顔碱槻閿涙B 鏉╂柨娲栭惃鍕Ц list[tuple] ===
        if df is None or len(df) == 0:
            LOG.warning("[DS.Breadth] empty data (raw)")
            raise Exception("[DS.Breadth] empty data (raw)")
            return {}
        
         
        if df.empty:
            LOG.warning("[DS.Breadth] empty dataframe after conversion")
            return {}
    
###
# SQL 瀹稿弶瀵滈弮銉︽埂闂勫秴绨潻鏂挎礀閿涘本娓堕弬鏉挎躬閸?
        recent_df = df.head(20)  # 閸欐牗娓舵潻?0娑擃亙姘﹂弰鎾存）

        if recent_df.empty:
            LOG.error("[DS.Breadth] no recent data after head(20) for %s", trade_date)
            return self._empty_block()

        # 瑜版挸澧犻崐纭风窗閺堚偓閺傞绔存径?
        latest_row = recent_df.iloc[0]
        current_ratio = float(latest_row["new_low_50d_ratio"])
        current_total = int(latest_row["count_total"])          # 閳?閸樼喍鍞惍?count_total 閻ㄥ嫭娴涙禒?
        current_new_low = int(latest_row["count_new_low_50d"]) # 閸欘垶鈧濞囬悽?
        latest_trade_date = latest_row["trade_date"].strftime("%Y-%m-%d")

        # window 閸掓銆冮敍鍫ユ鎼村骏绱伴張鈧弬鏉挎躬閸撳稄绱?
        window = [
            {
                "trade_date": row["trade_date"].strftime("%Y-%m-%d"),
                "count_new_low": int(row["count_new_low_50d"]),
                "count_total": int(row["count_total"]),            # 閳?閺囧じ鍞崢?merged.shape[0]
                "new_low_ratio": float(row["new_low_50d_ratio"]),
            }
            for _, row in recent_df.iterrows()
        ]

        block = {
            "trade_date": latest_trade_date,
            "count_new_low": current_new_low,           
            "count_total": current_total,          # 婵″倹鐏夋担鐘插斧閺夈儱婀?block 娑擃厾鏁ゆ禍?count_total閿涘苯褰查崝?
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

        # 缂傛挸鐡?+ history閿涘牅绻氶幐浣风瑝閸欐﹫绱?
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Breadth] cache save failed: %s", e) 

        return block


