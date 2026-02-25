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
import os
import json
import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.Breadth")


class BreadthDataSource(DataSourceBase):
    def __init__(self, cfg: DataSourceConfig, window: int = 50):
        super().__init__(cfg)
        self.config = cfg

        self.cache_root = self.config.cache_root        
        self.window = int(window)   # 鈫?鈽?蹇呴』鏈夎繖涓€琛?
        
        self.db = DBOracleProvider()

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:

        cache_file = os.path.join(self.cache_root, f"breadth_{trade_date}.json")

        td = pd.to_datetime(trade_date)
        start = (td - timedelta(days=self.window * 2)).strftime("%Y-%m-%d")
        end = td.strftime("%Y-%m-%d")
         
        # 璇诲彇绐楀彛鍐?close锛堝厑璁稿仠鐗岀己澶憋級
        #df = self.db.query_stock_closes(window_start=start, trade_date=end)
        df: pd.DataFrame = self.db.fetch_daily_new_low_stats(
            trade_date=trade_date,
            look_back_days=90,  # 鍥炴函瓒冲澶╂暟淇濊瘉鏈?0涓氦鏄撴棩
            #window_days=20,     # 绐楀彛鍥哄畾20鏃?
        )
 


        # === 馃敶 鍏抽敭淇锛欴B 杩斿洖鐨勬槸 list[tuple] ===
        if df is None or len(df) == 0:
            LOG.warning("[DS.Breadth] empty data (raw)")
            raise Exception("[DS.Breadth] empty data (raw)")
            return {}
        
         
        if df.empty:
            LOG.warning("[DS.Breadth] empty dataframe after conversion")
            return {}
    
###
# SQL 宸叉寜鏃ユ湡闄嶅簭杩斿洖锛屾渶鏂板湪鍓?
        recent_df = df.head(20)  # 鍙栨渶杩?0涓氦鏄撴棩

        if recent_df.empty:
            LOG.error("[DS.Breadth] no recent data after head(20) for %s", trade_date)
            return self._empty_block()

        # 褰撳墠鍊硷細鏈€鏂颁竴澶?
        latest_row = recent_df.iloc[0]
        current_ratio = float(latest_row["new_low_50d_ratio"])
        current_total = int(latest_row["count_total"])          # 鈫?鍘熶唬鐮?count_total 鐨勬浛浠?
        current_new_low = int(latest_row["count_new_low_50d"]) # 鍙€変娇鐢?
        latest_trade_date = latest_row["trade_date"].strftime("%Y-%m-%d")

        # window 鍒楄〃锛堥檷搴忥細鏈€鏂板湪鍓嶏級
        window = [
            {
                "trade_date": row["trade_date"].strftime("%Y-%m-%d"),
                "count_new_low": int(row["count_new_low_50d"]),
                "count_total": int(row["count_total"]),            # 鈫?鏇夸唬鍘?merged.shape[0]
                "new_low_ratio": float(row["new_low_50d_ratio"]),
            }
            for _, row in recent_df.iterrows()
        ]

        block = {
            "trade_date": latest_trade_date,
            "count_new_low": current_new_low,           
            "count_total": current_total,          # 濡傛灉浣犲師鏉ュ湪 block 涓敤浜?count_total锛屽彲鍔?
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

        # 缂撳瓨 + history锛堜繚鎸佷笉鍙橈級
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Breadth] cache save failed: %s", e) 

        return block

