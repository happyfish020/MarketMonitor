# core/adapters/datasources/cn/amount_source.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)
#from core.utils.spot_store import get_spot_daily
from core.utils.ds_refresh import apply_refresh_cleanup

from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.Amount")


class AmountDataSource(DataSourceBase):
    """
    V12 鎴愪氦棰濇暟鎹簮锛?
    - 浣跨敤 SpotStore 鎻愪緵鐨勫叏琛屾儏锛坺h_spot锛?
    - 鎸?symbol 鍚庣紑 .SH / .SZ / .BJ 缁熻鎴愪氦棰濓紙浜垮厓锛?
    - 鍙敓鎴愬綋鏃?snapshot锛屼笉鍋氬鏃?history 璁＄畻锛坔istory 浠呯敤浜庢寔涔呭寲锛?
      杈撳嚭锛?
        {
          "trade_date": str,
          "amount": float,
        }
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="DS.Amount")

        self.config = config
        self.db = DBOracleProvider()

        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            "[DS.Amount] Init: market=%s ds=%s cache_root=%s history_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
        )

    # ------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        

        #test 蹇呴』鐨?
        
        cache_file = os.path.join(self.cache_root, f"amount_{trade_date}.json")
        look_back_days = 30

        # 鎸?refresh_mode 娓呯悊 cache锛堝鏋滈渶瑕侊級
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # 鍛戒腑 cache
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.Amount] load cache error: %s", e)

        # 1. 璇诲彇鍏ㄨ鎯咃紙SpotStore 鍐呴儴璐熻矗缂撳瓨锛?
        try:
            #df: pd.DataFrame = get_spot_daily(trade_date, refresh_mode=refresh_mode)
            df: pd.DataFrame = self.db.fetch_daily_amount_series ( start_date=trade_date, look_back_days = look_back_days)
        except Exception as e:
            LOG.error("[DS.Amount] get_market_daily error: %s", e)
            raise Exception(e)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.error("[DS.Amount] Spot DF is empty - return neutral block")
            return self._neutral_block(trade_date)

        # 纭繚 trade_date 涓?datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date")

        # 闄嶅簭鎺掑簭锛堟渶鏂版棩鏈熷湪鍓嶉潰锛?
        df = df.sort_index(ascending=False)

        # 鍙栨渶鏂扮殑 20 look_back_days 涓氦鏄撴棩
        recent_df = df.head(look_back_days)

        if recent_df.empty:
            LOG.error(f"[DS.Amount] no recent data after head({look_back_days}) for %s", trade_date)
            return self._empty_block()

        # 褰撳墠鍊硷細绗竴琛岋紙鍗虫渶鏂颁竴澶╋級
        latest_row = recent_df.iloc[0]
        current_total = float(latest_row["total_amount"])
        latest_trade_date = latest_row.name.strftime("%Y-%m-%d")

        # window 鍒楄〃锛氫粠鏂板埌鏃?
        window = [
            {
                "trade_date": idx.strftime("%Y-%m-%d"),
                "total_amount": float(total_amount),
            }
            for idx, total_amount in recent_df["total_amount"].items()
        ]

        block = {
            "trade_date": latest_trade_date,
            "total_amount": current_total,
            "window": window,  # 鏈€鏂版棩鏈熷湪鍒楄〃鏈€鍓嶉潰
        }

        LOG.info(
            "[DS.Amount] built: trade_date=%s total_val=%.2f window_len=%d (desc order)",
            latest_trade_date,
            current_total,
            len(window),
        )
        LOG.info(
            "[DS.Amount] Trade_date=%s TOTAL=%s",
            trade_date,
            current_total,
        )

        # 鍐?cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Amount] save cache error: %s", e)

 
        return block

    # ------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "total_amount": 0.0,
            "window": {}
        }
 
