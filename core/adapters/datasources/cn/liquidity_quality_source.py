# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Liquidity Quality DataSource (F Block)

功能：
    计算全市场流动性质量相关指标，包括：
      - Top20 成交集中度（前 20 名成交额占比）
      - 大/小盘成交占比（按股票代码前缀划分大盘与小盘）
      - 缩量下跌比（下跌股票中成交额低于自身近 20 日均额的比例）

实现要点：
    1. 从 Oracle 中的 CN_STOCK_DAILY_PRICE 表读取指定回溯窗口内的股票日线数据：
         symbol, exchange, trade_date, chg_pct, amount
       通过 DBOracleProvider.query_stock_closes 接口获取。
    2. 按股票逐一计算 20 日滚动平均成交额（含当前日）。
    3. 在所选窗口内（默认 60 日），逐日统计上述指标：
         * Top20 成交集中度 = sum(top 20 amount) / sum(total amount)
         * 大/小盘成交占比 = sum(amount of big-cap) / sum(amount of small-cap)
           大盘股票的判断基于股票代码前缀（600/601/603）：
               若 symbol 以 "60", "601", "603" 开头则视为大盘；否则视为小盘。
         * 缩量下跌比 = count(chg_pct < 0 & amount < ma20_amount) / count(chg_pct < 0)
    4. 计算 10 日趋势和 3 日加速度（取各指标近 10 日/3 日差值）。
    5. 输出最新日期的指标，以及历史序列作为 evidence。

    注意：
      - 本数据源不访问外部 API，仅依赖本地 Oracle 数据。
      - 若返回数据为空或异常，则输出中性块并标记 data_status。
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from core.datasources.datasource_base import DataSourceBase, DataSourceConfig
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger
from core.adapters.providers.db_provider_oracle import DBOracleProvider

LOG = get_logger("DS.LiquidityQuality")


class LiquidityQualityDataSource(DataSourceBase):
    """Liquidity quality DataSource for F block."""

    def __init__(self, config: DataSourceConfig, window: int = 60):
        super().__init__(name="DS.LiquidityQuality")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # Prepare cache and history directories
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        self.cache_file = os.path.join(self.cache_root, "liquidity_quality_today.json")
        self.history_file = os.path.join(self.history_root, "liquidity_quality_series.json")

        LOG.info(
            "[DS.LiquidityQuality] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # --------------------------------------------------------------
    @staticmethod
    def _save(path: str, obj: Any) -> None:
        """Persist json to disk (history/cache helpers).

        NOTE: DataSourceBase in this repo is intentionally minimal and does not
        provide a shared _save(). Other DS modules implement their own helper.
        This DS follows the same convention to avoid runtime AttributeError.
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] _save failed: path=%s err=%s", path, exc)

    # --------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        构建流动性质量原始数据块。

        参数：
            trade_date: 评估日期（字符串格式 'YYYY-MM-DD' 或 'YYYYMMDD'）
            refresh_mode: 刷新策略，支持 'none'|'readonly'|'full'

        返回：包含最新日期指标及历史序列的字典。
        """
        # 清理缓存
        apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        # 尝试读取缓存
        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.LiquidityQuality] load cache error: %s", exc)

        # 计算回溯区间：额外取 20 天用于计算滚动均值
        try:
            as_date = pd.to_datetime(trade_date).date()
        except Exception:
            # 若 trade_date 解析失败，则返回中性
            return self._neutral_block(trade_date)

        look_back_days = self.window + 20
        # 起始日期 = trade_date - look_back_days 天
        start_dt = as_date - timedelta(days=look_back_days)

        try:
            # 调用 DBProvider 获取数据（包括预收盘/涨跌幅/成交额）
            rows = self.db.query_stock_closes(start_dt, as_date)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] oracle fetch error: %s", exc)
            return self._neutral_block(trade_date)

        if not rows:
            LOG.warning("[DS.LiquidityQuality] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        # 转为 DataFrame
        df = pd.DataFrame(rows, columns=["symbol", "exchange", "trade_date", "pre_close", "chg_pct", "close", "amount"])
        # 类型转换
        try:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        except Exception:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["chg_pct"] = pd.to_numeric(df["chg_pct"], errors="coerce").fillna(0.0)

        # 按 symbol + trade_date 排序，以便计算滚动均值
        df_sorted = df.sort_values(["symbol", "trade_date"]).copy()
        # 计算每个 symbol 的近 20 日平均 amount（包含当前日）
        df_sorted["ma20_amount"] = (
            df_sorted.groupby("symbol")["amount"].transform(lambda x: x.rolling(window=20, min_periods=1).mean())
        )

        # 选择最后 window 日的数据
        # 首先找出所有日期，升序排列
        unique_dates = sorted(df_sorted["trade_date"].unique())
        if not unique_dates:
            return self._neutral_block(trade_date)
        # 取最后 window 天
        selected_dates = unique_dates[-self.window:]

        series: List[Dict[str, Any]] = []
        for dt in selected_dates:
            df_day = df_sorted[df_sorted["trade_date"] == dt]
            total_amount = float(df_day["amount"].sum())
            # Top20 成交额占比
            top20_ratio = 0.0
            if total_amount > 0:
                df_sorted_day = df_day.sort_values("amount", ascending=False)
                topn = min(len(df_sorted_day), 20)
                top_amount = float(df_sorted_day.head(topn)["amount"].sum())
                if total_amount > 0:
                    top20_ratio = top_amount / total_amount
            # 大/小盘成交占比
            big_prefixes = ("60", "601", "603")
            # 判断 symbol 首字段，注意 symbol 可能为字符串或数字
            symbols = df_day["symbol"].astype(str).fillna("")
            big_mask = symbols.str.startswith(big_prefixes)
            big_amount = float(df_day.loc[big_mask, "amount"].sum())
            small_amount = float(df_day.loc[~big_mask, "amount"].sum())
            if small_amount > 0:
                big_small_ratio = big_amount / small_amount
            else:
                # 如果小盘成交额为 0，则无法计算比例，设为 None
                big_small_ratio = None
            # 缩量下跌比：下跌且成交额低于 ma20_amount
            df_neg = df_day[df_day["chg_pct"] < 0]
            neg_cnt = len(df_neg)
            if neg_cnt > 0:
                down_low_cnt = (df_neg["amount"] < df_neg["ma20_amount"]).sum()
                down_low_ratio = down_low_cnt / neg_cnt
            else:
                down_low_ratio = None

            series.append(
                {
                    "trade_date": dt.strftime("%Y-%m-%d"),
                    "top20_ratio": round(top20_ratio, 4) if top20_ratio is not None else None,
                    "big_small_ratio": round(big_small_ratio, 4) if big_small_ratio is not None else None,
                    "down_low_ratio": round(down_low_ratio, 4) if down_low_ratio is not None else None,
                }
            )

        # 如未生成 series 或最新日期不匹配，则返回中性
        if not series:
            return self._neutral_block(trade_date)

        # 计算趋势和加速度（近 10 日和近 3 日差值）
        def _calc_delta(vals: List[Optional[float]], days: int) -> Optional[float]:
            try:
                if len(vals) > days and vals[-1] is not None and vals[-days - 1] is not None:
                    return round(float(vals[-1]) - float(vals[-days - 1]), 4)
            except Exception:
                pass
            return None

        # 提取每一列的值
        top20_vals = [s.get("top20_ratio") for s in series]
        big_small_vals = [s.get("big_small_ratio") for s in series]
        down_low_vals = [s.get("down_low_ratio") for s in series]

        top20_trend_10d = _calc_delta(top20_vals, 10)
        top20_acc_3d = _calc_delta(top20_vals, 3)
        big_small_trend_10d = _calc_delta(big_small_vals, 10)
        big_small_acc_3d = _calc_delta(big_small_vals, 3)
        down_low_trend_10d = _calc_delta(down_low_vals, 10)
        down_low_acc_3d = _calc_delta(down_low_vals, 3)

        latest = series[-1]
        latest_date = latest.get("trade_date")
        top20_ratio_last = latest.get("top20_ratio") if latest else None
        big_small_ratio_last = latest.get("big_small_ratio") if latest else None
        down_low_ratio_last = latest.get("down_low_ratio") if latest else None

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "top20_ratio": top20_ratio_last,
            "big_small_ratio": big_small_ratio_last,
            "down_low_ratio": down_low_ratio_last,
            "top20_trend_10d": top20_trend_10d,
            "top20_acc_3d": top20_acc_3d,
            "big_small_trend_10d": big_small_trend_10d,
            "big_small_acc_3d": big_small_acc_3d,
            "down_low_trend_10d": down_low_trend_10d,
            "down_low_acc_3d": down_low_acc_3d,
            "series": series,
        }

        # 保存历史和缓存
        try:
            self._save(self.history_file, series)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.LiquidityQuality] save error: %s", exc)

        return block

    # --------------------------------------------------------------
    def _neutral_block(self, trade_date: str) -> Dict[str, Any]:
        return {
            "trade_date": trade_date,
            "top20_ratio": None,
            "big_small_ratio": None,
            "down_low_ratio": None,
            "top20_trend_10d": None,
            "top20_acc_3d": None,
            "big_small_trend_10d": None,
            "big_small_acc_3d": None,
            "down_low_trend_10d": None,
            "down_low_acc_3d": None,
            "series": [],
        }
