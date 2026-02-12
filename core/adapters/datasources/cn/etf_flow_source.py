# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ETF Flow DataSource (C Block)

设计目的：
    从本地 Oracle 数据库的基金 ETF 日行情表（CN_FUND_ETF_HIST_EM）
    聚合计算 ETF 份额变化代理指标，提供原始窗口序列和趋势指标。

约束：
    - 仅依赖 DBOracleProvider，不访问外部 API
    - 不定义新的 provider 接口，直接调用 provider 层提供的聚合方法
    - 按日构建时间序列，window 默认 60 天

输出字段：
    trade_date: 交易日期（最新一个交易日字符串）
    total_change_amount: 当日所有 ETF price change 之和
    total_volume: 当日 ETF 成交量之和
    total_amount: 当日 ETF 成交额之和
    flow_ratio: 当日价格涨跌额与成交量的比值（proxy）
    trend_10d: 10 日累计变化（总 price change）
    acc_3d: 3 日累计变化（总 price change）
    series: 从旧到新的历史序列列表，每项包含 trade_date、total_change_amount、total_volume、total_amount

当数据缺失或异常时，返回 neutral_block
"""

from __future__ import annotations

import os
import json
from typing import Dict, Any, List

import pandas as pd

from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger
from core.adapters.providers.db_provider_oracle import DBOracleProvider

LOG = get_logger("DS.ETFFlow")


class ETFFlowDataSource(DataSourceBase):
    """
    ETF Flow DataSource

    聚合 ETF 日行情表的 price change / volume / amount 数据，
    通过 10 天和 3 天累积值提供趋势和加速度信息。
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        # 使用固定名称，便于日志识别
        super().__init__(name="DS.ETFFlow")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # cache 和 history 路径
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 单日 cache 统一命名，避免使用 trade_date 作为文件名
        self.cache_file = os.path.join(self.cache_root, "etf_flow_today.json")
        # 持久化历史序列
        self.history_file = os.path.join(self.history_root, "etf_flow_series.json")

        LOG.info(
            "[DS.ETFFlow] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        主入口：构建 ETF flow 原始数据块。

        参数：
            trade_date: 字符串，评估日期（通常为 T 或 T-1）
            refresh_mode: 刷新策略，支持 none/readonly/full
        """
        # 按 refresh_mode 清理缓存文件
        apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        # 命中缓存直接返回
        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.ETFFlow] load cache error: %s", exc)

        # 读取聚合数据
        try:
            df: pd.DataFrame = self.db.fetch_etf_hist_series(
                start_date=trade_date,
                look_back_days=self.window,
            )
        except Exception as exc:
            LOG.error("[DS.ETFFlow] fetch_etf_hist_series error: %s", exc)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.warning("[DS.ETFFlow] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        # 确保有序：按日期升序（旧→新）
        df_sorted = df.sort_index(ascending=True)

        # 将 DataFrame 转为列表 [{trade_date, total_change_amount, ...}]
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            series.append({
                "trade_date": idx.strftime("%Y-%m-%d"),
                "total_change_amount": float(row["total_change_amount"]) if pd.notna(row["total_change_amount"]) else 0.0,
                "total_volume": float(row["total_volume"]) if pd.notna(row["total_volume"]) else 0.0,
                "total_amount": float(row["total_amount"]) if pd.notna(row["total_amount"]) else 0.0,
            })

        # 合并历史（保证滑窗长度固定，向后补齐）
        merged_series = self._merge_history(series)

        # 计算趋势/加速度
        trend_10d, acc_3d = self._calc_trend(merged_series)

        # 最新记录
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.ETFFlow] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        total_change_amount = latest.get("total_change_amount")
        total_volume = latest.get("total_volume")
        total_amount = latest.get("total_amount")
        # 比值：避免除零
        flow_ratio = 0.0
        try:
            flow_ratio = round(total_change_amount / total_volume, 4) if total_volume else 0.0
        except Exception:
            flow_ratio = 0.0

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "total_change_amount": total_change_amount,
            "total_volume": total_volume,
            "total_amount": total_amount,
            "flow_ratio": flow_ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": merged_series,
        }

        # 保存到历史和缓存
        try:
            # 持久化历史
            self._save(self.history_file, merged_series)
            # 缓存当天块
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.ETFFlow] save error: %s", exc)

        return block

    # ------------------------------------------------------------------
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        合并历史序列。

        recent: 当前查询窗口内的列表（升序）。
        history_file 中保留更久远的历史记录，与 recent 合并后截取 window 长度。
        """
        old = []
        if os.path.exists(self.history_file):
            try:
                old = self._load(self.history_file)
            except Exception:
                old = []
        # 构建字典以日期去重
        buf: Dict[str, Dict[str, Any]] = {r["trade_date"]: r for r in old}
        for r in recent:
            buf[r["trade_date"]] = r
        out = sorted(buf.values(), key=lambda x: x["trade_date"])
        return out[-self.window:]

    # ------------------------------------------------------------------
    def _calc_trend(self, series: List[Dict[str, Any]]) -> tuple[float, float]:
        """
        计算 10 天趋势和 3 天加速度。
        trend_10d = last.total_change_amount - total_change_amount[-11]
        acc_3d   = last.total_change_amount - total_change_amount[-4]
        若长度不够，则返回 0.0
        """
        if len(series) < 2:
            return 0.0, 0.0
        values = [s.get("total_change_amount", 0.0) or 0.0 for s in series]
        try:
            t10 = values[-1] - values[-11] if len(values) >= 11 else 0.0
            a3 = values[-1] - values[-4] if len(values) >= 4 else 0.0
            return round(t10, 2), round(a3, 2)
        except Exception:
            return 0.0, 0.0

    # ------------------------------------------------------------------
    @staticmethod
    def _load(path: str) -> Any:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    @staticmethod
    def _save(path: str, obj: Any) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _neutral_block(trade_date: str) -> Dict[str, Any]:
        """
        返回空/中性块。
        """
        return {
            "trade_date": trade_date,
            "total_change_amount": 0.0,
            "total_volume": 0.0,
            "total_amount": 0.0,
            "flow_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
        }