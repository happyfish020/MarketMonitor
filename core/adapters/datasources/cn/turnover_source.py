# core/adapters/datasources/cn/turnover_source.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import (
    DataSourceConfig,
    BaseDataSource,
)
from core.utils.spot_store import get_spot_daily
from core.utils.ds_refresh import apply_refresh_cleanup

LOG = get_logger("DS.Turnover")


class TurnoverDataSource(BaseDataSource):
    """
    V12 成交额数据源：
    - 使用 SpotStore 提供的全行情（zh_spot）
    - 按 symbol 后缀 .SH / .SZ / .BJ 统计成交额（亿元）
    - 只生成当日 snapshot，不做多日 history 计算（history 仅用于持久化）
      输出：
        {
          "trade_date": str,
          "sh": float,
          "sz": float,
          "bj": float,
          "total": float,
        }
    """

    def __init__(self, config: DataSourceConfig):
        super().__init__(name="DS.Turnover")

        self.config = config
        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            "[DS.Turnover] Init: market=%s ds=%s cache_root=%s history_root=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
        )

    # ------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        cache_file = os.path.join(self.cache_root, f"turnover_{trade_date}.json")

        # 按 refresh_mode 清理 cache（如果需要）
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
                LOG.error("[DS.Turnover] load cache error: %s", e)

        # 1. 读取全行情（SpotStore 内部负责缓存）
        try:
            df: pd.DataFrame = get_spot_daily(trade_date, refresh_mode=refresh_mode)
        except Exception as e:
            LOG.error("[DS.Turnover] get_spot_daily error: %s", e)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.error("[DS.Turnover] Spot DF is empty - return neutral block")
            return self._neutral_block(trade_date)

        # SpotStore 约定：
        # - symbol 列：000001.SZ / 600000.SH / 830799.BJ
        # - 成交额 列：单位为 元
        def sum_market(suffix: str) -> float:
            mask = df["symbol"].astype(str).str.endswith(f".{suffix}")
            sub = df[mask]
            if sub.empty:
                return 0.0
            # 元 -> 亿
            return round(sub["成交额"].sum() / 1e8, 2)

        sh_val = sum_market("SH")
        sz_val = sum_market("SZ")
        bj_val = sum_market("BJ")
        total_val = round(sh_val + sz_val + bj_val, 2)

        block: Dict[str, Any] = {
            "trade_date": trade_date,
            "sh": sh_val,
            "sz": sz_val,
            "bj": bj_val,
            "total": total_val,
        }

        LOG.info(
            "[DS.Turnover] SH=%s SZ=%s BJ=%s TOTAL=%s",
            sh_val,
            sz_val,
            bj_val,
            total_val,
        )

        # 写 cache
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as e:
            LOG.error("[DS.Turnover] save cache error: %s", e)

        return block

    # ------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "sh": 0.0,
            "sz": 0.0,
            "bj": 0.0,
            "total": 0.0,
        }
