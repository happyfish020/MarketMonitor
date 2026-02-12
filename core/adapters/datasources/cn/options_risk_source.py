# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Options Risk DataSource (E Block)

设计目的：
    聚合 ETF 期权日行情数据，计算加权涨跌额、总涨跌额、加权收盘价以及其变化趋势。
    此数据源为期权风险分析提供基础原始数据，用于后续因子打分和报告展示。

约束：
    - 仅依赖 DBOracleProvider，不访问外部 API。
    - 仅聚合一组固定的 ETF 期权标的（九只ETF），根据配置可调整。
    - 按日构建时间序列，默认回溯 60 日。

输出字段：
    trade_date: 最新交易日期（字符串）
    weighted_change: 按成交量加权的涨跌额均值
    total_change:    所有合约涨跌额求和
    total_volume:    总成交量
    weighted_close:  按成交量加权的收盘价
    change_ratio:    weighted_change / weighted_close（若收盘价为 0，则为 0 或 None）
    trend_10d:       近 10 日 weighted_change 变化
    acc_3d:         近 3 日 weighted_change 变化
    series: 历史序列列表，每项包含 trade_date, weighted_change, total_change, total_volume,
            weighted_close, change_ratio

当数据缺失或异常时，返回 neutral_block。
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

LOG = get_logger("DS.OptionsRisk")


class OptionsRiskDataSource(DataSourceBase):
    """
    Options Risk DataSource

    聚合 ETF 期权日行情数据，计算加权涨跌额及其趋势/加速度。
    """

    def __init__(self, config: DataSourceConfig, window: int = 60) -> None:
        # 固定名称，便于日志识别
        super().__init__(name="DS.OptionsRisk")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # 缓存和历史路径
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 单日 cache 文件名
        self.cache_file = os.path.join(self.cache_root, "options_risk_today.json")
        self.history_file = os.path.join(self.history_root, "options_risk_series.json")

        LOG.info(
            "[DS.OptionsRisk] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        构建期权风险原始数据块。

        参数：
            trade_date: 字符串，评估日期（通常为 T 或 T-1）
            refresh_mode: 刷新策略，支持 none/readonly/full
        """
        # 清理缓存依据 refresh_mode
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
                LOG.error("[DS.OptionsRisk] load cache error: %s", exc)

        # 调用 DB provider 聚合数据
        try:
            df: pd.DataFrame = self.db.fetch_options_risk_series(
                start_date=trade_date,
                look_back_days=self.window,
            )
        except Exception as exc:
            LOG.error("[DS.OptionsRisk] fetch_options_risk_series error: %s", exc)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.warning("[DS.OptionsRisk] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        df_sorted = df.sort_index(ascending=True)
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            dt_str = idx.strftime("%Y-%m-%d")
            try:
                wchg = float(row.get("weighted_change", 0.0)) if pd.notna(row.get("weighted_change")) else 0.0
            except Exception:
                wchg = 0.0
            try:
                tchg = float(row.get("total_change", 0.0)) if pd.notna(row.get("total_change")) else 0.0
            except Exception:
                tchg = 0.0
            try:
                tv = float(row.get("total_volume", 0.0)) if pd.notna(row.get("total_volume")) else 0.0
            except Exception:
                tv = 0.0
            try:
                wclose = float(row.get("weighted_close", 0.0)) if pd.notna(row.get("weighted_close")) else 0.0
            except Exception:
                wclose = 0.0
            try:
                ratio = row.get("change_ratio")
                ratio = float(ratio) if ratio is not None and pd.notna(ratio) else 0.0
            except Exception:
                ratio = 0.0
            series.append({
                "trade_date": dt_str,
                "weighted_change": wchg,
                "total_change": tchg,
                "total_volume": tv,
                "weighted_close": wclose,
                "change_ratio": ratio,
            })

        merged_series = self._merge_history(series)
        trend_10d, acc_3d = self._calc_trend(merged_series)
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.OptionsRisk] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        weighted_change = latest.get("weighted_change", 0.0)
        total_change = latest.get("total_change", 0.0)
        total_volume = latest.get("total_volume", 0.0)
        weighted_close = latest.get("weighted_close", 0.0)
        change_ratio = latest.get("change_ratio", 0.0)

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "weighted_change": weighted_change,
            "total_change": total_change,
            "total_volume": total_volume,
            "weighted_close": weighted_close,
            "change_ratio": change_ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": merged_series,
            # 标记数据状态为 OK，表明数据来源正常
            "data_status": "OK",
            # 默认无 warnings；若上层需要可覆盖
            "warnings": [],
        }

        # 保存历史和缓存
        try:
            self._save(self.history_file, merged_series)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.OptionsRisk] save error: %s", exc)

        return block

    # ------------------------------------------------------------------
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        合并历史与当前窗口，保证长度固定为 window。
        """
        old: List[Dict[str, Any]] = []
        if os.path.exists(self.history_file):
            try:
                old = self._load(self.history_file)
            except Exception:
                old = []
        buf: Dict[str, Dict[str, Any]] = {r["trade_date"]: r for r in old}
        for r in recent:
            buf[r["trade_date"]] = r
        out = sorted(buf.values(), key=lambda x: x["trade_date"])
        return out[-self.window:]

    # ------------------------------------------------------------------
    def _calc_trend(self, series: List[Dict[str, Any]]) -> tuple[float, float]:
        """
        计算 10 日趋势和 3 日加速度（基于 weighted_change）。
        """
        if len(series) < 2:
            return 0.0, 0.0
        values = [s.get("weighted_change", 0.0) or 0.0 for s in series]
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
        返回空/中性块，所有指标为 0.0，series 为空。

        注意：当数据缺失或无法加载时，需明确标注 data_status 为 "MISSING"。如果省略此字段，
        上层因子和报告会默认认为数据正常（"OK"），从而给出不准确的提示。此处我们明确
        设置 data_status 为 "MISSING" 以便 WatchlistLeadFactor 能正确识别数据缺失情况。
        """
        return {
            "trade_date": trade_date,
            "weighted_change": 0.0,
            "total_change": 0.0,
            "total_volume": 0.0,
            "weighted_close": 0.0,
            "change_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
            "data_status": "MISSING",
            # 提供一个 warnings 字段以便上层面板记录缺失原因
            "warnings": ["missing:options_risk_series"],
        }