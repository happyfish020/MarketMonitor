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

from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

LOG = get_logger("DS.Amount")


class AmountDataSource(DataSourceBase):
    """
    V12 閹存劒姘︽０婵囨殶閹诡喗绨敍?
    - 娴ｈ法鏁?SpotStore 閹绘劒绶甸惃鍕弿鐞涘本鍎忛敍鍧篽_spot閿?
    - 閹?symbol 閸氬海绱?.SH / .SZ / .BJ 缂佺喕顓搁幋鎰唉妫版繐绱欐禍鍨帗閿?
    - 閸欘亞鏁撻幋鎰秼閺?snapshot閿涘奔绗夐崑姘樋閺?history 鐠侊紕鐣婚敍鍧攊story 娴犲懐鏁ゆ禍搴㈠瘮娑斿懎瀵查敍?
      鏉堟挸鍤敍?
        {
          "trade_date": str,
          "amount": float,
        }
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="DS.Amount")

        self.config = config
        self.db = DBMySQLMarketProvider()

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
        

        #test 韫囧懘銆忛惃?
        
        cache_file = os.path.join(self.cache_root, f"amount_{trade_date}.json")
        look_back_days = 30

        # 閹?refresh_mode 濞撳懐鎮?cache閿涘牆顩ч弸婊堟付鐟曚緤绱?
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # 閸涙垝鑵?cache
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.Amount] load cache error: %s", e)

        # 1. 鐠囪褰囬崗銊攽閹拑绱橲potStore 閸愬懘鍎寸拹鐔荤煑缂傛挸鐡ㄩ敍?
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

        # 绾喕绻?trade_date 娑?datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date")

        # 闂勫秴绨幒鎺戠碍閿涘牊娓堕弬鐗堟）閺堢喎婀崜宥夋桨閿?
        df = df.sort_index(ascending=False)

        # 閸欐牗娓堕弬鎵畱 20 look_back_days 娑擃亙姘﹂弰鎾存）
        recent_df = df.head(look_back_days)

        if recent_df.empty:
            LOG.error(f"[DS.Amount] no recent data after head({look_back_days}) for %s", trade_date)
            return self._empty_block()

        # 瑜版挸澧犻崐纭风窗缁楊兛绔寸悰宀嬬礄閸楄櫕娓堕弬棰佺婢垛晪绱?
        latest_row = recent_df.iloc[0]
        current_total = float(latest_row["total_amount"])
        latest_trade_date = latest_row.name.strftime("%Y-%m-%d")

        # window 閸掓銆冮敍姘矤閺傛澘鍩岄弮?
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
            "window": window,  # 閺堚偓閺傜増妫╅張鐔锋躬閸掓銆冮張鈧崜宥夋桨
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

        # 閸?cache
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
 

