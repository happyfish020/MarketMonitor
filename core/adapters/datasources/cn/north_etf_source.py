# core/adapters/datasources/cn/north_etf_source.py
"""
UnifiedRisk V12+ - NorthETFSource (智能 NorthNPS 代理)

完全基于宽基 ETF 代理北向资金强度，不使用官方北向接口。

功能：
1. 从 symbols.yaml 读取北向代理 ETF 列表（兼容你的现有结构）
2. YF 拉取最近行情，自动校正（缺数据 / 成交量为 0 / 极端涨跌）
3. 多 ETF 加权，计算：
   - total_turnover_e9  （总成交额，估算，单位：亿）
   - total_flow_e9      （方向性流入 proxy）
   - hs300_proxy_pct    （加权涨跌，作为 HS300 代理）
   - today_inflow       （NorthNPS 强度，动态权重）
4. 维护 60 日强度序列，计算：
   - trend_3d / trend_5d
   - range_state （强 / 中性 / 弱，基于分位数）
5. 输出 detail + anomalies 方便 debug
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.utils.datasource_config import DataSourceConfig
from core.utils.logger import get_logger
from core.utils.config_loader import load_symbols
from core.utils.yf_utils import fetch_yf_history

LOG = get_logger("DS.NorthNPS")


class NorthETFSource(BaseDataSource):
    """
    北向 NPS 代理数据源（智能版）
    """

    def __init__(self,trade_date: str):
        super().__init__("NorthETFSource")

       
        self.trade_date = trade_date
        LOG.info("Init: Trade_date%s", self.trade_date)


        # 放在 cn / north_nps 目录下
        self.config = DataSourceConfig(market="cn", ds_name="north_nps")
        self.config.ensure_dirs()

        self.cache_file = os.path.join(self.config.cache_root, "north_nps.json")
        self.series_file = os.path.join(self.config.history_root, "north_nps_series.json")

    # ------------------------------------------------------------------
    # 基本 IO
    # ------------------------------------------------------------------
    def _load_cache(self) -> Dict[str, Any]:
        data = load_json(self.cache_file) or {}
        LOG.info("NorthNPS CacheRead: path=%s data_keys=%s",
                 self.cache_file, list(data.keys()) if isinstance(data, dict) else type(data))
        return data if isinstance(data, dict) else {}

    def _save_cache(self, data: Dict[str, Any]):
        LOG.info("NorthNPS CacheWrite: path=%s keys=%s",
                 self.cache_file, list(data.keys()))
        save_json(self.cache_file, data)

    def _load_series(self) -> List[Dict[str, Any]]:
        data = load_json(self.series_file)
        if not isinstance(data, list):
            data = []
        return data

    def _save_series(self, series: List[Dict[str, Any]]):
        # 只保留最近 60 个
        series = sorted(series, key=lambda x: x.get("date", ""))[-60:]
        LOG.info("NorthNPS SeriesWrite: path=%s size=%s",
                 self.series_file, len(series))
        save_json(self.series_file, series)

    # ------------------------------------------------------------------
    # 从 symbols.yaml 读取 ETF 列表（兼容你当前结构）
    # ------------------------------------------------------------------
    def _get_etf_symbols(self) -> List[str]:
        """
        兼容三种配置来源：

        1) 你的当前格式（优先）：
            cn_north_etf_proxy:
              north_sh: "510300.SS"
              north_sz: "159901.SZ"

        2) V12 官方格式：
            cn:
              etf_proxy:
                north_sh: "510300.SS"
                north_sz: "159901.SZ"

        3) 拓展 ETF 列表：
            cn_etf.core:
              - "510300.SS"
              - "159901.SZ"
              - ...

        返回去重后的 symbol 列表。
        """
        cfg = load_symbols() or {}
        symbols: List[str] = []

        # ① 你的当前专用格式
        proxy_a = cfg.get("cn_north_etf_proxy", {})
        if isinstance(proxy_a, dict) and proxy_a:
            LOG.info("NorthNPS Config: Using cn_north_etf_proxy")
            for key in ("north_sh", "north_sz"):
                sym = proxy_a.get(key)
                if sym:
                    symbols.append(sym)

        # ② V12 官方格式（fallback）
        cn_cfg = cfg.get("cn", {})
        if isinstance(cn_cfg, dict):
            proxy_b = cn_cfg.get("etf_proxy", {})
            if isinstance(proxy_b, dict) and proxy_b:
                LOG.info("NorthNPS Config: Using cn.etf_proxy (fallback)")
                for key in ("north_sh", "north_sz"):
                    sym = proxy_b.get(key)
                    if sym:
                        symbols.append(sym)

        # ③ 拓展 ETF 列表（额外参与加权）
        cn_etf_cfg = cfg.get("cn_etf", {})
        if isinstance(cn_etf_cfg, dict):
            core_list = cn_etf_cfg.get("core", []) or []
            if core_list:
                LOG.info("NorthNPS Config: Using cn_etf.core as extra")
                for sym in core_list:
                    if sym:
                        symbols.append(sym)

        # 去重
        symbols = list(dict.fromkeys(symbols))

        if not symbols:
            LOG.error("NorthNPS ERROR: 未找到任何北向代理 ETF 配置（检查 symbols.yaml）")
        else:
            LOG.info("NorthNPS ETF symbols=%s", symbols)

        return symbols

    # ------------------------------------------------------------------
    # 单只 ETF 数据构建 + 异常检测
    # ------------------------------------------------------------------
    def _build_etf_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        对单只 ETF 计算：
          - pct_change
          - turnover_e9
          - flow_e9
          - flags: [E01...E09]
          - valid: bool
        """
        flags: List[str] = []
        df = fetch_yf_history(symbol, period="10d", interval="1d")

        if df is None or df.empty:
            flags.append("E01:no_data")
            LOG.warning("NorthNPS %s: 无 YF 数据", symbol)
            return {
                "symbol": symbol,
                "pct_change": 0.0,
                "turnover_e9": 0.0,
                "flow_e9": 0.0,
                "valid": False,
                "flags": flags,
            }

        df = df.sort_values("date").reset_index(drop=True)
        if len(df) < 2:
            flags.append("E09:len<2")
            LOG.warning("NorthNPS %s: YF 数据不足 len=%s", symbol, len(df))
            return {
                "symbol": symbol,
                "pct_change": 0.0,
                "turnover_e9": 0.0,
                "flow_e9": 0.0,
                "valid": False,
                "flags": flags,
            }

        last = df.iloc[-1]
        prev = df.iloc[-2]

        close = float(last["close"])
        prev_close = float(prev["close"])

        # 成交量处理
        volume = float(last.get("volume", 0.0))
        if volume <= 0:
            # 用过去 5 日均量估算
            if len(df) > 2:
                hist_vol = df.iloc[:-1]["volume"]
                mean_vol = float(hist_vol.mean()) if not hist_vol.empty else 0.0
            else:
                mean_vol = 0.0

            if mean_vol > 0:
                volume = mean_vol
                flags.append("E02:volume_zero_replaced")
            else:
                flags.append("E02:volume_zero")
                LOG.warning("NorthNPS %s: 成交量为 0 且无法估算", symbol)

        # 涨跌幅
        if prev_close > 0:
            pct = (close - prev_close) / prev_close * 100.0
        else:
            pct = 0.0
            flags.append("E05:prev_close<=0")

        # 成交额（亿）
        turnover_e9 = close * volume / 1e8

        # 方向性流入 proxy
        flow_e9 = turnover_e9 * pct / 100.0

        # 极端涨跌检测
        if abs(pct) > 12.0:
            flags.append("E03:extreme_pct")
            LOG.warning("NorthNPS %s: 极端涨跌 pct=%.2f", symbol, pct)

        # 成交额过低
        if turnover_e9 <= 0:
            flags.append("E04:turnover_zero")

        valid = True
        # 简单定义：极端涨跌但成交额极小 → 不可信
        if "E03:extreme_pct" in flags and turnover_e9 < 5.0:
            valid = False

        LOG.info(
            "NorthNPS ETF %s: pct=%.2f turnover_e9=%.2f flow_e9=%.2f flags=%s valid=%s",
            symbol, pct, turnover_e9, flow_e9, flags, valid,
        )

        return {
            "symbol": symbol,
            "pct_change": pct,
            "turnover_e9": turnover_e9,
            "flow_e9": flow_e9,
            "valid": valid,
            "flags": flags,
        }

    # ------------------------------------------------------------------
    # 智能权重计算
    # ------------------------------------------------------------------
    def _compute_dynamic_weights(
        self,
        total_turnover_e9: float,
        total_flow_e9: float,
        etf_items: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        """
        根据 ETF 异常情况动态调整权重：
            w1: ETF_flow  权重
            w2: turnover  权重
            w3: hs300_pct × turnover  权重
        """
        # 默认权重
        w1, w2, w3 = 0.50, 0.30, 0.20

        flags_all = sum((item.get("flags", []) for item in etf_items), [])
        bad_flow = any("E03:extreme_pct" in f for f in flags_all)
        bad_turnover = any("E02:volume_zero" in f or "E04:turnover_zero" in f for f in flags_all)

        # 成交额整体太低，也视为 turnover 略弱
        if total_turnover_e9 < 10.0:
            bad_turnover = True

        if bad_flow and not bad_turnover:
            # ETF 涨跌不太可信 → 降低 flow 权重，提高成交额权重
            w1, w2, w3 = 0.30, 0.50, 0.20
        elif bad_turnover and not bad_flow:
            # 成交额有问题，涨跌还可以 → 提高 flow 权重
            w1, w2, w3 = 0.60, 0.20, 0.20
        elif bad_flow and bad_turnover:
            # 两边都问题 → 回到均衡
            w1, w2, w3 = 0.40, 0.40, 0.20

        # 归一化避免浮点误差
        s = w1 + w2 + w3
        w1, w2, w3 = w1 / s, w2 / s, w3 / s

        LOG.info(
            "NorthNPS DynamicWeights: w1=%.2f w2=%.2f w3=%.2f bad_flow=%s bad_turnover=%s total_turnover=%.2f",
            w1, w2, w3, bad_flow, bad_turnover, total_turnover_e9,
        )

        return {"w1": w1, "w2": w2, "w3": w3}

    # ------------------------------------------------------------------
    # 构建当日 NorthNPS
    # ------------------------------------------------------------------
    def _build_today_from_etf(self) -> Dict[str, Any]:
        #trade_date = datetime.now().strftime("%Y-%m-%d")
        symbols = self._get_etf_symbols()

        if not symbols:
            LOG.warning("NorthNPS: 无有效 ETF 代理符号，返回中性结构")
            return {
                "trade_date": self.trade_date,
                "today_inflow": 0.0,
                "trend_3d": 0.0,
                "trend_5d": 0.0,
                "range_state": "中性",
                "etf_flow_e9": 0.0,
                "total_turnover_e9": 0.0,
                "hs300_proxy_pct": 0.0,
                "weights": {"w1": 0.5, "w2": 0.3, "w3": 0.2},
                "anomalies": ["NO_SYMBOL"],
                "details": [],
            }

        details: List[Dict[str, Any]] = []
        total_turnover_e9 = 0.0
        total_flow_e9 = 0.0

        for sym in symbols:
            item = self._build_etf_snapshot(sym)
            details.append(item)
            total_turnover_e9 += float(item["turnover_e9"])
            total_flow_e9 += float(item["flow_e9"])

        # 有效 ETF（valid=True）
        valid_items = [d for d in details if d.get("valid")]
        if not valid_items:
            LOG.warning("NorthNPS: 所有 ETF 均被标记为 invalid，使用全部 ETF 参与计算")
            valid_items = details

        # 计算 hs300_proxy_pct（流动性加权涨跌）
        hs300_proxy_pct = 0.0
        denom = sum(d["turnover_e9"] for d in valid_items)
        if denom > 0:
            hs300_proxy_pct = sum(
                d["pct_change"] * d["turnover_e9"] for d in valid_items
            ) / denom

        # 动态权重
        weights = self._compute_dynamic_weights(total_turnover_e9, total_flow_e9, details)
        w1, w2, w3 = weights["w1"], weights["w2"], weights["w3"]

        strength = (
            w1 * total_flow_e9
            + w2 * total_turnover_e9
            + w3 * hs300_proxy_pct * total_turnover_e9 / 100.0
        )

        LOG.info(
            "NorthNPS TodayCalc: date=%s strength=%.2f flow=%.2f turnover=%.2f hs300_pct=%.2f",
            self.trade_date,
            strength,
            total_flow_e9,
            total_turnover_e9,
            hs300_proxy_pct,
        )

        # === 更新历史序列 ===
        series = self._load_series()
        series = [s for s in series if s.get("date") != self.trade_date]
        series.append({"date": self.trade_date, "strength": strength})
        self._save_series(series)

        # === 计算趋势 & 区间 ===
        trend_3d, trend_5d, range_state = self._compute_trend_and_range(series, strength)

        anomalies = sorted(
            list(set(flag for d in details for flag in d.get("flags", [])))
        )

        LOG.info(
            "NorthNPS Derived: trend_3d=%.2f trend_5d=%.2f range=%s anomalies=%s",
            trend_3d, trend_5d, range_state, anomalies,
        )

        return {
            "trade_date": self.cache_filetrade_date,
            "today_inflow": strength,
            "trend_3d": trend_3d,
            "trend_5d": trend_5d,
            "range_state": range_state,
            "etf_flow_e9": total_flow_e9,
            "total_turnover_e9": total_turnover_e9,
            "hs300_proxy_pct": hs300_proxy_pct,
            "weights": weights,
            "anomalies": anomalies,
            "details": details,
        }

    # ------------------------------------------------------------------
    # 趋势 & 区间评分
    # ------------------------------------------------------------------
    def _compute_trend_and_range(
        self,
        series: List[Dict[str, Any]],
        today_strength: float,
    ) -> Tuple[float, float, str]:
        if not series:
            return 0.0, 0.0, "中性"

        df = pd.DataFrame(series)
        df = df.sort_values("date").reset_index(drop=True)
        strengths = df["strength"].astype(float).tolist()

        # 3 日 / 5 日趋势 -> 今日 vs 均线
        last3 = strengths[-3:]
        last5 = strengths[-5:]

        avg3 = sum(last3) / len(last3)
        avg5 = sum(last5) / len(last5)

        # 趋势定义：今日 - 均线（>0 说明强于近几日）
        trend_3d = today_strength - avg3
        trend_5d = today_strength - avg5

        # 区间：用最近 40 日分位数
        if len(strengths) >= 10:
            hist = strengths[-40:] if len(strengths) > 40 else strengths
            s = pd.Series(hist)
            q30 = s.quantile(0.3)
            q70 = s.quantile(0.7)

            if today_strength >= q70:
                range_state = "强"
            elif today_strength <= q30:
                range_state = "弱"
            else:
                range_state = "中性"
        else:
            # 历史太短，用 max_abs 粗分
            max_abs = max(abs(v) for v in strengths) or 1.0
            ratio = today_strength / max_abs
            if ratio >= 0.6:
                range_state = "强"
            elif ratio <= -0.6:
                range_state = "弱"
            else:
                range_state = "中性"

        return trend_3d, trend_5d, range_state

    # ------------------------------------------------------------------
    # 外部主入口
    # ------------------------------------------------------------------
    def get_northbound_snapshot(self, refresh: bool = False) -> Dict[str, Any]:
        LOG.info("NorthNPS FetchStart: refresh=%s", refresh)

        try:
            if not refresh:
                cache = self._load_cache()
                if cache:
                    LOG.info(
                        "NorthNPS FetchEnd(ReadOnly): inflow=%.2f trend3=%.2f range=%s",
                        float(cache.get("today_inflow", 0.0)),
                        float(cache.get("trend_3d", 0.0)),
                        cache.get("range_state"),
                    )
                    return cache
                LOG.warning("NorthNPS Cache empty in readonly mode, fallback to rebuild")

            data = self._build_today_from_etf()
            self._save_cache(data)
            LOG.info(
                "NorthNPS FetchEnd(Refreshed): inflow=%.2f trend3=%.2f range=%s",
                float(data.get("today_inflow", 0.0)),
                float(data.get("trend_3d", 0.0)),
                data.get("range_state"),
            )
            return data

        except Exception as e:
            LOG.error("NorthNPS ERROR: %s", e, exc_info=True)
            return {
                "trade_date": self.trade_date,
                "today_inflow": 0.0,
                "trend_3d": 0.0,
                "trend_5d": 0.0,
                "range_state": "中性",
                "etf_flow_e9": 0.0,
                "total_turnover_e9": 0.0,
                "hs300_proxy_pct": 0.0,
                "weights": {"w1": 0.5, "w2": 0.3, "w3": 0.2},
                "anomalies": ["EXCEPTION"],
                "details": [],
            }
