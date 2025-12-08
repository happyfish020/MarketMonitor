# core/adapters/datasources/cn/turnover_source.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - TurnoverDataSource
接口必须与 Margin / NPS 完全一致：
    get_turnover_block(refresh=False)

内部逻辑：
    - 自动获取 trade_date（使用 trade_calendar 工具）
    - 自动从 SpotStore 获取当天全行情
    - 自动统计 SH / SZ / total （亿元）
    - 自动维护历史 series
    - 不做趋势/评分（交给 TurnoverFactor）
"""

from __future__ import annotations
import os
import time
from typing import Dict, Any, List

import pandas as pd

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger
from core.utils.spot_store import get_spot_daily

from datetime import datetime

LOG = get_logger("DS.Turnover")


class TurnoverDataSource(BaseDataSource):

    def __init__(self, trade_date: str):
        super().__init__("TurnoverDataSource")

        self.config = DataSourceConfig(market="cn", ds_name="turnover")
        self.config.ensure_dirs()

        self.cache_path = os.path.join(self.config.cache_root, "turnover_today.json")
        self.hist_path = os.path.join(self.config.history_root, "turnover_series.json")

        LOG.info(f"[DS.Turnover] init OK: cache={self.cache_path}, history={self.hist_path}")
        self.trade_date = trade_date
        LOG.info("Init: Trade_date%s", self.trade_date)

    # ----------------------------------------------------------------------
    # Cache
    # ----------------------------------------------------------------------
    def _load_cache(self):
        data = load_json(self.cache_path)
        if not data:
            return None
        if time.time() - data.get("ts", 0) > 600:   # 10 分钟过期
            return None
        LOG.info("[DS.Turnover] Hit cache")
        return data.get("data")

    def _save_cache(self, block):
        save_json(self.cache_path, {"ts": time.time(), "data": block})
        LOG.info("[DS.Turnover] Save cache")

    # ----------------------------------------------------------------------
    # History
    # ----------------------------------------------------------------------
    def _load_history(self):
        hist = load_json(self.hist_path)
        return hist if isinstance(hist, list) else []

    def _save_history(self, hist):
        hist = sorted(hist, key=lambda x: x["date"])[-400:]
        save_json(self.hist_path, hist)
        LOG.info(f"[DS.Turnover] Save history rows={len(hist)}")

    # ----------------------------------------------------------------------
    # 主函数：必须不需要 trade_date 参数！
    # ----------------------------------------------------------------------
    def get_turnover_block(self, refresh_mode: str) -> Dict[str, Any]:
        """
        #完全对齐 margin/nps 的接口：
            block = self.turnover_source.get_turnover_block(refresh_mode)
        """

        # Step 1 — 取 cache
        if refresh_mode !="snatpshot" and refresh_mode !="full":
            cached = self._load_cache()
            if isinstance(cached, dict):
                return cached

        # Step 2 — 自动获取今天交易日
         
        
        LOG.info(f"[DS.Turnover] trade_date = {self.trade_date}")

        # Step 3 — 获取全行情数据
        df: pd.DataFrame = get_spot_daily(self.trade_date, refresh_mode)

        # 兼容字段
        if "成交额" in df.columns:
            amt_col = "成交额"
        elif "amount" in df.columns:
            amt_col = "amount"
        else:
            LOG.error("[DS.Turnover] 没有 成交额/amount 字段")
            return {"sh": 0, "sz": 0, "bj":0 ,"total": 0, "series": []}

        code_col = "代码" if "代码" in df.columns else "symbol"

        # Step 4 — 统计 SH / SZ
        sh_mask = df[code_col].astype(str).str.startswith("sh")
        sz_mask = df[code_col].astype(str).str.startswith("sz")
        bj_mask = df[code_col].astype(str).str.startswith("bj")


        sh = df.loc[sh_mask, amt_col].sum() / 1e8
        sz = df.loc[sz_mask, amt_col].sum() / 1e8
        bj = df.loc[bj_mask, amt_col].sum() / 1e8
        total = sh + sz + bj

        LOG.info(f"[DS.Turnover] SH={sh:.2f}  SZ={sz:.2f} BJ={bj:.2f} Total={total:.2f} 亿")

        # Step 5 — 写历史 series
        hist = self._load_history()
        existed = {x["date"] for x in hist}

        today_row = {"date": self.trade_date, "total": total}
        if self.trade_date not in existed:
            hist.append(today_row)

        self._save_history(hist)

        # Step 6 — 构建 block
        block = {
            "sh": round(sh, 2),
            "sz": round(sz, 2),
            "bj": round(bj, 2),
            "total": round(total, 2),
            "series": hist,
        }

        # Step 7 — 写入 cache
        self._save_cache(block)

        return block
