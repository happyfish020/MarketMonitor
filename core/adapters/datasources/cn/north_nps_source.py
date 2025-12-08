# core/adapters/datasources/cn/north_nps_source.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Any, List

from core.adapters.datasources.base import BaseDataSource
from core.adapters.datasources.cn.etf_series_source import ETFSeriesSource
from core.utils.config_loader import load_symbols
from core.utils.logger import get_logger

LOG = get_logger("DS.NorthNPS")


class NorthNpsDataSource(BaseDataSource):
    """
    V12 北向代理数据源（松耦合版）

    - 使用 ETFSeriesSource 提供的 core ETF 序列
    - 计算北向代理强度 + 3/5 日趋势 + 强弱区
    - 输出给 snapshot["north_nps"] 使用
    """

    def __init__(self, trade_date:str ):
        super().__init__("NorthNpsDataSource")

        symbols_cfg = load_symbols()
        # 优先使用 cn_north_etf_proxy，找不到再退回 cn_etf.core
        proxy_cfg = symbols_cfg.get("cn_north_etf_proxy") or {}
        core_cfg = symbols_cfg.get("cn_etf", {}).get("core", [])

        self.etf_sh = proxy_cfg.get("north_sh") or (core_cfg[0] if core_cfg else None)
        self.etf_sz = proxy_cfg.get("north_sz") or (core_cfg[1] if len(core_cfg) > 1 else None)

        self.etf_source = ETFSeriesSource()

        LOG.info(
            "[NorthNPS] 初始化: etf_sh=%s etf_sz=%s",
            self.etf_sh,
            self.etf_sz,
        )

    # -------------------------------------------------------
    # 工具：从 ETF 序列构造“成交额 + 涨跌”时间序列
    # -------------------------------------------------------
    def _build_etf_flow_series(self, series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        输入: [{date, close, volume}, ...] (按日期升序)
        输出: [{date, turnover_e9, pct_change}, ...]
        """
        if not series or len(series) < 2:
            return []

        flows: List[Dict[str, Any]] = []
        prev_close = series[0]["close"]

        for row in series[1:]:
            close = float(row["close"])
            vol = float(row["volume"])
            date = row["date"]

            # 成交额估算：close * volume（元）→ 亿
            # 这里 volume 默认为股数（YF 行为），简化为: turnover_e9 = close * volume / 1e8
            turnover_e9 = close * vol / 1e8 if vol > 0 else 0.0

            pct = 0.0
            if prev_close > 0:
                pct = (close - prev_close) / prev_close * 100.0

            flows.append(
                {
                    "date": date,
                    "turnover_e9": turnover_e9,
                    "pct_change": pct,
                }
            )

            prev_close = close

        return flows

    # -------------------------------------------------------
    # 工具：计算 north_proxy 强度序列
    # -------------------------------------------------------
    def _combine_strength(self, flows_sh: List[Dict[str, Any]], flows_sz: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        把沪/深两个 ETF 的“成交额 × 涨跌”加权合成一条 north_proxy 强度序列
        """
        if not flows_sh and not flows_sz:
            return []

        # 用 date 对齐
        map_sh = {r["date"]: r for r in flows_sh}
        map_sz = {r["date"]: r for r in flows_sz}

        all_dates = sorted(set(map_sh.keys()) | set(map_sz.keys()))
        result: List[Dict[str, Any]] = []

        for d in all_dates:
            sh = map_sh.get(d)
            sz = map_sz.get(d)

            strength = 0.0
            turnover_e9 = 0.0

            if sh:
                strength += sh["turnover_e9"] * sh["pct_change"]
                turnover_e9 += sh["turnover_e9"]
            if sz:
                strength += sz["turnover_e9"] * sz["pct_change"]
                turnover_e9 += sz["turnover_e9"]

            result.append(
                {
                    "date": d,
                    "strength": strength,      # 成交额 × 涨跌
                    "turnover_e9": turnover_e9 # 当日总成交额（估算）
                }
            )

        return result

    # -------------------------------------------------------
    # 工具：计算 3 日 / 5 日趋势
    # -------------------------------------------------------
    @staticmethod
    def _calc_trend(series: List[Dict[str, Any]], key: str, window: int) -> float:
        if not series:
            return 0.0
        n = len(series)
        last = series[-1][key]
        idx_prev = max(0, n - 1 - window)
        prev = series[idx_prev][key]
        return last - prev

    # -------------------------------------------------------
    # 对外主入口：构建 snapshot["north_nps"]
    # -------------------------------------------------------
    def build_block(self, refresh: bool = False) -> Dict[str, Any]:
        LOG.info(
            "[NorthNPS] 构建北向代理数据块 refresh=%s sh=%s sz=%s",
            refresh,
            self.etf_sh,
            self.etf_sz,
        )

        if not (self.etf_sh and self.etf_sz):
            LOG.error("[NorthNPS] 没有配置 north ETF 代理符号")
            return {}

        # 1) 取 ETF 日线序列
        series_sh = self.etf_source.get_series(self.etf_sh, refresh)
        series_sz = self.etf_source.get_series(self.etf_sz, refresh)

        # 2) 转为“成交额 + 涨跌”序列
        flows_sh = self._build_etf_flow_series(series_sh)
        flows_sz = self._build_etf_flow_series(series_sz)

        # 3) 合成 north_proxy 强度序列
        strength_series = self._combine_strength(flows_sh, flows_sz)
        if not strength_series:
            LOG.error("[NorthNPS] 无法构建强度序列（可能 ETF 数据为空）")
            return {}

        # 最新
        latest = strength_series[-1]
        strength_today = latest["strength"]
        turnover_today = latest["turnover_e9"]

        trend_3d = self._calc_trend(strength_series, "strength", 3)
        trend_5d = self._calc_trend(strength_series, "strength", 5)

        # 简单分区：强 / 中 / 弱（可以后替换为分位数）
        if strength_today >= 0:
            zone = "强势区" if abs(strength_today) >= 50 else "偏多"
        else:
            zone = "弱势区" if abs(strength_today) >= 50 else "偏空"

        LOG.info(
            "[NorthNPS] strength=%.2f turnover=%.2f trend3=%.2f trend5=%.2f zone=%s",
            strength_today,
            turnover_today,
            trend_3d,
            trend_5d,
            zone,
        )

        return {
            "etf_symbols": {
                "north_sh": self.etf_sh,
                "north_sz": self.etf_sz,
            },
            "etf_series": {
                self.etf_sh: series_sh,
                self.etf_sz: series_sz,
            },
            "strength_series": strength_series,
            "strength_today": strength_today,
            "turnover_today_e9": turnover_today,
            "trend_3d": trend_3d,
            "trend_5d": trend_5d,
            "zone": zone,
        }
