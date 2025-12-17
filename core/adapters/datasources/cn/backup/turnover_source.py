# core/adapters/datasources/cn/turnover_source.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Dict, Any

import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig,DataSourceBase
from core.utils.spot_store import get_spot_daily
from core.utils.ds_refresh import apply_refresh_cleanup

LOG = get_logger("DS.Turnover")


class TurnoverDataSource:
    """
    V12 成交额数据源（来自全行情 zh_spot）
    - 统计沪深北成交额
    - 保存 cache/history
    """

    def __init__(self, config: DataSourceConfig):
        self.market = config.market
        self.ds_name = config.ds_name

        self.cache_root = config.cache_root
        self.history_root = config.history_root

        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        LOG.info(
            f"[DS.Turnover] init OK: cache={self.cache_root}, history={self.history_root}"
        )

    # ------------------------------------------------------------
    # 主接口
    # ------------------------------------------------------------
    def get_turnover_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        TurnoverDataSource V12
        使用 SpotStore.get_spot_daily() 获取当日全行情

        refresh_mode : "none" | "daily" | "snapshot"
            - "snapshot": 当日 snapshot 场景，倾向于刷新
            - 其它      : 优先用 cache / 本地 parquet
        """
        LOG.info(f"[DS.Turnover] trade_date={trade_date}, refresh_mode={refresh_mode}")

        snapshot_refresh = (refresh_mode  == "snapshot")

        cache_today = os.path.join(self.cache_root, f"turnover_{trade_date}.json")
        history_path = os.path.join(self.history_root, "turnover_series.json")

        # ============= Step 1: 按 protocol 清理文件 ============
        mode = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_today,
            history_path=history_path,
            spot_path=None,
        )

        


        # ==== 1. 今日 cache 若存在、且非 snapshot 刷新 → 返回 ====
        if mode!="full"   and os.path.exists(cache_today):
            try:
                with open(cache_today, "r", encoding="utf-8") as f:
                    block = json.load(f)
                LOG.info("[DS.Turnover] Hit cache")
                return block
            except Exception as e:
                LOG.error(f"[DS.Turnover] CacheReadFailed: {e}")

        # ==== 2. SpotStore 获取全行情 ====
        df: pd.DataFrame = get_spot_daily(
            trade_date = trade_date,
            mode=mode,
        )

        if df is None or df.empty:
            LOG.error("[DS.Turnover] Spot DF is empty - return neutral block")
            return self._neutral_block(trade_date)

        # ==== 3. 统计成交额 ====
        # SpotStore 已经提供标准化的 symbol 列：000001.SZ / 600000.SH / 830799.BJ
        def sum_market(suffix: str) -> float:
            """按交易所后缀统计成交额（亿元）"""
            mask = df["symbol"].astype(str).str.endswith(f".{suffix}")
            sub = df[mask]
            if sub.empty:
                return 0.0
            return round(sub["成交额"].sum() / 1e8, 2)

        sh_val = sum_market("SH")
        sz_val = sum_market("SZ")
        bj_val = sum_market("BJ")
        total_val = round(sh_val + sz_val + bj_val, 2)

        block = {
            "trade_date": trade_date,
            "sh": sh_val,
            "sz": sz_val,
            "bj": bj_val,
            "total": total_val,
        }

        LOG.info(
            f"[DS.Turnover] SH={sh_val}  SZ={sz_val}  BJ={bj_val}  Total={total_val}"
        )

        # ==== 4. 写入今日 cache ====
        try:
            with open(cache_today, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
            LOG.info(f"[DS.Turnover] Cache saved: {cache_today}")
        except Exception as e:
            LOG.error(f"[DS.Turnover] CacheWriteFailed: {e}")

        # ==== 5. 更新 history ====
        history = {"series": []}
        if os.path.exists(history_path):
            try:
                with open(history_path, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                LOG.warning("[DS.Turnover] History read failed, rebuild new file")

        series = history.get("series", [])
        series.append(block)

        try:
            with open(history_path, "w", encoding="utf-8") as f:
                json.dump({"series": series}, f, ensure_ascii=False, indent=2)
            LOG.info(f"[DS.Turnover] History saved rows={len(series)}")
        except Exception as e:
            LOG.error(f"[DS.Turnover] HistoryWriteFailed: {e}")

        return block

    # ------------------------------------------------------------
    # 中性
    # ------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "sh": 0.0,
            "sz": 0.0,
            "bj": 0.0,
            "total": 0.0,
        }
