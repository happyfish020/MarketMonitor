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

from core.adapters.providers.db_provider_oracle import DBOracleProvider

LOG = get_logger("DS.Amount")


class AmountDataSource(DataSourceBase):
    """
    V12 成交额数据源：
    - 使用 SpotStore 提供的全行情（zh_spot）
    - 按 symbol 后缀 .SH / .SZ / .BJ 统计成交额（亿元）
    - 只生成当日 snapshot，不做多日 history 计算（history 仅用于持久化）
      输出：
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
        

        #test 必须的
        
        cache_file = os.path.join(self.cache_root, f"amount_{trade_date}.json")
        look_back_days = 30

        # 按 refresh_mode 清理 cache（如果需要）
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # 命中 cache
        if refresh_mode in ("none", "readonly") and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.Amount] load cache error: %s", e)

        # 1. 读取全行情（SpotStore 内部负责缓存）
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

        # 确保 trade_date 为 datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date")

        # 降序排序（最新日期在前面）
        df = df.sort_index(ascending=False)

        # 取最新的 20 look_back_days 个交易日
        recent_df = df.head(look_back_days)

        if recent_df.empty:
            LOG.error(f"[DS.Amount] no recent data after head({look_back_days}) for %s", trade_date)
            return self._empty_block()

        # 当前值：第一行（即最新一天）
        latest_row = recent_df.iloc[0]
        current_total = float(latest_row["total_amount"])
        latest_trade_date = latest_row.name.strftime("%Y-%m-%d")

        # window 列表：从新到旧
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
            "window": window,  # 最新日期在列表最前面
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

        # 写 cache
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
 