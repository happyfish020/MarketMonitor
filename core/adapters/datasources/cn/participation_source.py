# core/adapters/datasources/cn/participation_source.py
# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ParticipationDataSource (CN A-Share)

职责（生产向，极简）：
- 从本地 Oracle DB 读取全市场股票日线 close（事实层）
- 计算当日横截面参与质量指标：
    - adv_ratio（上涨家数占比）
    - median_return（个股收益中位数）
    - index_return（指数收益，用于对照：默认 HS300=sh000300）
- 输出为 snapshot["participation"] 的标准输入块（不做状态判定）

注意：
- 停牌/缺失数据合法：单只股票缺 prev_close 则跳过
- 不做收益回测，不做交易信号
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from core.utils.logger import get_logger
from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.adapters.providers.db_provider_oracle import DBOracleProvider


LOG = get_logger("DS.Participation")


def _parse_date(s: str) -> datetime:
    # trade_date 允许 'YYYY-MM-DD' / 'YYYYMMDD'
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return datetime.strptime(s, "%Y%m%d")
    return datetime.strptime(s, "%Y-%m-%d")


class ParticipationDataSource(DataSourceBase):
    """
    输出块结构（snapshot["participation"]）：

    {
      "trade_date": "YYYY-MM-DD",
      "index_code": "sh000300",
      "adv": int,
      "dec": int,
      "flat": int,
      "adv_ratio": float,
      "median_return": float,
      "index_return": float,
      "n_effective": int,
      "_raw_data": {...可选调试...}
    }
    """

    def __init__(self, cfg: DataSourceConfig, index_code: str = "sh000300", lookback_days: int = 10):
        super().__init__(name="DS.Participation")
        self.cfg = cfg
        self.cfg.ensure_dirs()

        self.index_code = index_code
        self.lookback_days = int(lookback_days)

        self.db = DBOracleProvider()

        LOG.info(
            "[DS.Participation] init ok. market=%s ds=%s cache=%s index=%s lookback_days=%s",
            cfg.market,
            cfg.ds_name,
            cfg.cache_root,
            self.index_code,
            self.lookback_days,
        )

    def _cache_path(self, trade_date: str) -> str:
        td = _parse_date(trade_date).strftime("%Y-%m-%d")
        return os.path.join(self.cfg.cache_root, f"participation_{td}.json")

    @staticmethod
    def _calc_last2_returns(df: pd.DataFrame) -> pd.Series:
        """
        df columns: symbol, trade_date, close_price
        return: Series(index=symbol, value=ret)
        """
        if df is None or df.empty:
            return pd.Series(dtype="float64")

        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values(["symbol", "trade_date"])

        # 取每个 symbol 最后两行（不足两行的会在 ret_one 里被过滤）
        last2 = df.groupby("symbol", sort=False).tail(2)

        def _ret_one(x: pd.DataFrame) -> Optional[float]:
            if x.shape[0] < 2:
                return None
            c1 = float(x.iloc[-1]["close_price"])
            c0 = float(x.iloc[-2]["close_price"])
            if c0 == 0:
                return None
            return c1 / c0 - 1.0

        rets = last2.groupby("symbol", sort=False).apply(_ret_one)
        rets = rets.dropna().astype(float)
        return rets

    def build_block(self, trade_date: str, refresh_mode: str = "auto") -> Dict[str, Any]:
        cache_file = self._cache_path(trade_date)

        # refresh 控制（仅 cache）
        _ = apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=cache_file,
            history_path=None,
            spot_path=None,
        )

        # cache 命中
        if refresh_mode == "none" and os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                LOG.warning("[DS.Participation] cache load failed, fallback to db. err=%s", e)

        td_dt = _parse_date(trade_date)
        window_start = (td_dt - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        td_str = td_dt.strftime("%Y-%m-%d")

        # ---- 1) stocks close window ----
        try:
            stock_df = self.db.query_stock_closes(window_start=window_start, trade_date=td_str)
        except Exception as e:
            LOG.error("[DS.Participation] db query_stock_closes failed: %s", e, exc_info=True)
            raise

        if not isinstance(stock_df, pd.DataFrame):
            stock_df = pd.DataFrame(stock_df)

        # 防御：仅保留 <= trade_date
        if "trade_date" in stock_df.columns:
            stock_df["trade_date"] = pd.to_datetime(stock_df["trade_date"])
            stock_df = stock_df[stock_df["trade_date"] <= pd.to_datetime(td_str)]

        rets = self._calc_last2_returns(stock_df)

        # ---- 2) index return (HS300 default) ----
        try:
            idx_df = self.db.query_index_closes(index_code=self.index_code, window_start=window_start, trade_date=td_str)
        except Exception as e:
            LOG.error("[DS.Participation] db query_index_closes failed: %s", e, exc_info=True)
            raise

        if not isinstance(idx_df, pd.DataFrame):
            idx_df = pd.DataFrame(idx_df)

        idx_ret = 0.0
        if not idx_df.empty and "trade_date" in idx_df.columns:
            idx_df["trade_date"] = pd.to_datetime(idx_df["trade_date"])
            idx_df = idx_df.sort_values("trade_date")
            if idx_df.shape[0] >= 2:
                c1 = float(idx_df.iloc[-1]["close_price"])
                c0 = float(idx_df.iloc[-2]["close_price"])
                idx_ret = (c1 / c0 - 1.0) if c0 else 0.0
            else:
                LOG.warning("[DS.Participation] index has <2 points. index_code=%s trade_date=%s", self.index_code, td_str)

        # ---- 3) cross-sectional stats ----
        adv = int((rets > 0).sum())
        dec = int((rets < 0).sum())
        flat = int((rets == 0).sum())
        denom = adv + dec
        adv_ratio = float(adv / denom) if denom > 0 else 0.0
        median_ret = float(np.median(rets.values)) if len(rets) else 0.0

        block: Dict[str, Any] = {
            "trade_date": td_str,
            "index_code": self.index_code,
            "adv": adv,
            "dec": dec,
            "flat": flat,
            "adv_ratio": adv_ratio,
            "median_return": median_ret,
            "index_return": float(idx_ret),
            "n_effective": int(len(rets)),
            "_raw_data": {
                "window_start": window_start,
                "universe_effective": int(len(rets)),
                "ret_sample": rets.head(5).to_dict(),
            },
        }

        # cache 写入
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False)
        except Exception as e:
            LOG.warning("[DS.Participation] cache write failed: %s", e)

        return block
