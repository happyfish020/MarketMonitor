# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Futures Basis DataSource (D Block)

设计目的：
    从本地 Oracle 数据库的股指期货日行情表（CN_FUT_INDEX_HIS）和指数日行情表（CN_INDEX_DAILY_PRICE）
    聚合计算股指期货基差（期货结算价 - 指数收盘价）及其走势，用于风险监测。返回原始时间序列、
    基差均值、趋势和加速度。

约束：
    - 仅依赖 DBOracleProvider，不访问外部 API。
    - 不定义新的 provider 接口，直接调用 provider 层提供的聚合方法 fetch_futures_basis_series。
    - 按日构建时间序列，window 默认 60 天。

输出字段：
    trade_date: 交易日期（最新一个交易日字符串）
    avg_basis:  按成交量加权的基差均值（期货 - 指数），正值为升水，负值为贴水
    total_basis: 按合约简单求和的基差（辅助）
    basis_ratio: 基差相对于加权指数收盘价的比值
    trend_10d:   近 10 日基差变化（基差均值差）
    acc_3d:     近 3 日基差变化（基差均值差）
    series: 历史序列列表，每项包含 trade_date、avg_basis、total_basis、basis_ratio、weighted_future_price、weighted_index_price、total_volume

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

LOG = get_logger("DS.FuturesBasis")


class FuturesBasisDataSource(DataSourceBase):
    """
    Futures Basis DataSource

    聚合股指期货和指数日行情表的数据，计算加权基差序列及其趋势/加速度。
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        # 固定名称，便于日志识别
        super().__init__(name="DS.FuturesBasis")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # 缓存和历史路径
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 单日 cache 文件名
        self.cache_file = os.path.join(self.cache_root, "futures_basis_today.json")
        self.history_file = os.path.join(self.history_root, "futures_basis_series.json")

        LOG.info(
            "[DS.FuturesBasis] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        构建期指基差原始数据块。

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
                LOG.error("[DS.FuturesBasis] load cache error: %s", exc)

        # 读取聚合数据
        try:
            df: pd.DataFrame = self.db.fetch_futures_basis_series(
                start_date=trade_date,
                look_back_days=self.window,
            )
        except Exception as exc:
            LOG.error("[DS.FuturesBasis] fetch_futures_basis_series error: %s", exc)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.warning("[DS.FuturesBasis] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        df_sorted = df.sort_index(ascending=True)
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            dt_str = idx.strftime("%Y-%m-%d")
            try:
                avg_basis = float(row.get("avg_basis", 0.0)) if pd.notna(row.get("avg_basis")) else 0.0
            except Exception:
                avg_basis = 0.0
            try:
                total_basis = float(row.get("total_basis", 0.0)) if pd.notna(row.get("total_basis")) else 0.0
            except Exception:
                total_basis = 0.0
            try:
                basis_ratio = row.get("basis_ratio")
                basis_ratio = float(basis_ratio) if basis_ratio is not None and pd.notna(basis_ratio) else 0.0
            except Exception:
                basis_ratio = 0.0
            try:
                w_fut = float(row.get("weighted_future_price", 0.0)) if pd.notna(row.get("weighted_future_price")) else 0.0
            except Exception:
                w_fut = 0.0
            try:
                w_idx = float(row.get("weighted_index_price", 0.0)) if pd.notna(row.get("weighted_index_price")) else 0.0
            except Exception:
                w_idx = 0.0
            try:
                total_volume = float(row.get("total_volume", 0.0)) if pd.notna(row.get("total_volume")) else 0.0
            except Exception:
                total_volume = 0.0
            series.append({
                "trade_date": dt_str,
                "avg_basis": avg_basis,
                "total_basis": total_basis,
                "basis_ratio": basis_ratio,
                "weighted_future_price": w_fut,
                "weighted_index_price": w_idx,
                "total_volume": total_volume,
            })

        merged_series = self._merge_history(series)
        trend_10d, acc_3d = self._calc_trend(merged_series)
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.FuturesBasis] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        avg_basis = latest.get("avg_basis", 0.0)
        total_basis = latest.get("total_basis", 0.0)
        ratio = latest.get("basis_ratio", 0.0)

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "avg_basis": avg_basis,
            "total_basis": total_basis,
            "basis_ratio": ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": merged_series,
        }

        # 保存历史和缓存
        try:
            self._save(self.history_file, merged_series)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.FuturesBasis] save error: %s", exc)

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
        计算 10 日趋势和 3 日加速度（基差均值差）。
        """
        if len(series) < 2:
            return 0.0, 0.0
        values = [s.get("avg_basis", 0.0) or 0.0 for s in series]
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
        """
        return {
            "trade_date": trade_date,
            "avg_basis": 0.0,
            "total_basis": 0.0,
            "basis_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
        }