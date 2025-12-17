# core/adapters/datasources/cn/etf_spot_sync_source.py
# -*- coding: utf-8 -*-

"""
UnifiedRisk V12 FULL
ETF × Spot Synchronization DataSource

设计铁律：
- Phase-1 DataSource（事实层）
- 读取全行情 spot（zh_spot）
- 只做“横截面统计”，不做任何判断
- 不依赖其它 DS 的输出
- 只输出当日 block（cache），不做 history
"""

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    DataSourceBase,
)
from core.utils.spot_store import get_spot_daily
from core.utils.ds_refresh import apply_refresh_cleanup

LOG = get_logger("DS.ETFSpotSync")


class ETFSpotSyncDataSource(DataSourceBase):
    """
    ETF × 市场横截面同步性专用 DS

    输出的是“事实统计”，供 Phase-2 因子使用：
    - 市场上涨/下跌比例
    - 成交额集中度
    - 涨跌幅分化程度
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="DS.ETFSpotSync")

        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            "[DS.ETFSpotSync] Init: market=%s ds=%s cache_root=%s history_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
        )

    # ------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        cache_file = os.path.join(self.cache_root, f"etf_spot_sync_{trade_date}.json")

        # refresh 控制（仅 cache）
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # 命中 cache
        if refresh_mode == "none" and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.error("[DS.ETFSpotSync] load cache error: %s", e)

        # 1️⃣ 获取全行情 spot
        try:
            df: pd.DataFrame = get_spot_daily(trade_date, refresh_mode=refresh_mode)
        except Exception as e:
            LOG.error("[DS.ETFSpotSync] get_spot_daily error: %s", e)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.error("[DS.ETFSpotSync] Spot DF empty")
            return self._neutral_block(trade_date)

        # 必要字段检查
        if "涨跌幅" not in df.columns or "成交额" not in df.columns:
            LOG.error("[DS.ETFSpotSync] required columns missing")
            return self._neutral_block(trade_date)

        total = len(df)
        if total == 0:
            return self._neutral_block(trade_date)

        # --------------------------------------------------------
        # 2️⃣ 横截面统计（纯事实）
        # --------------------------------------------------------

        # 上涨 / 下跌比例
        adv = (df["涨跌幅"] > 0).sum()
        dec = (df["涨跌幅"] < 0).sum()

        adv_ratio = round(adv / total, 4)
        dec_ratio = round(dec / total, 4)

        # 成交额集中度（Top 20%）
        df_turn = df[["成交额"]].copy()
        df_turn = df_turn[df_turn["成交额"] > 0]

        if not df_turn.empty:
            df_turn_sorted = df_turn.sort_values("成交额", ascending=False)
            top_n = max(int(len(df_turn_sorted) * 0.2), 1)
            top_turnover = df_turn_sorted.head(top_n)["成交额"].sum()
            total_turnover = df_turn_sorted["成交额"].sum()
            top20_turnover_ratio = round(top_turnover / total_turnover, 4) if total_turnover > 0 else 0.0
        else:
            top20_turnover_ratio = 0.0

        # 涨跌幅分化（标准差）
        dispersion = round(float(df["涨跌幅"].std(ddof=0)), 4)

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "total_stocks": total,
            "adv_ratio": adv_ratio,
            "dec_ratio": dec_ratio,
            "top20_turnover_ratio": top20_turnover_ratio,
            "dispersion": dispersion,
        }

        # 写 cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.ETFSpotSync] save cache error: %s", e)

        return block

    # ------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "total_stocks": 0,
            "adv_ratio": 0.0,
            "dec_ratio": 0.0,
            "top20_turnover_ratio": 0.0,
            "dispersion": 0.0,
        }
