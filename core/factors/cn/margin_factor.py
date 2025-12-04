from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List

import numpy as np

from core.adapters.datasources.cn.em_margin_client import EastmoneyMarginClientCN
from core.models.factor_result import FactorResult


@dataclass
class MarginFactorConfig:
    max_days: int = 20
    lookback_days: int = 10


class MarginFactor:
    """
    两融因子（V11.4.2）
    - 结构完全兼容你的 FactorResult 定义（name, score, signal, raw）
    """

    name = "margin"

    def __init__(self, config: MarginFactorConfig | None = None):
        self.config = config or MarginFactorConfig()
        self.client = EastmoneyMarginClientCN()

    # ---- 主入口（Engine 调用） ----

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        series = self.client.get_recent_series(max_days=self.config.max_days)

        # 数据不足 → 中性
        if len(series) < self.config.lookback_days + 1:
            return FactorResult(
                name=self.name,
                score=50.0,
                signal="中性",
                raw={"reason": "margin series too short"},
            )

        # 转 numpy
        rz = np.array([float(x["rz"]) for x in series], dtype=float)
        rq = np.array([float(x["rq"]) for x in series], dtype=float)

        # 最近 lookback_days+1 天
        rz = rz[-(self.config.lookback_days + 1):]
        rq = rq[-(self.config.lookback_days + 1):]

        rz_chg = self._pct_change(rz[-2], rz[-1])
        rq_chg = self._pct_change(rq[-2], rq[-1])

        score_rz = self._score_change_pos(rz_chg)
        score_rq = self._score_change_neg(rq_chg)
        score_trend = self._score_trend(rz[-self.config.lookback_days:])

        # raw 分数：[-1, 1]
        raw_score = 0.4 * score_rz + 0.3 * score_rq + 0.3 * score_trend

        # 映射到 0~100
        score_0_100 = self._map_to_0_100(raw_score)

        # signal：统一逻辑
        if score_0_100 >= 55:
            signal = "偏多"
        elif score_0_100 <= 45:
            signal = "偏空"
        else:
            signal = "中性"

        # 回传结构完全匹配 FactorResult
        return FactorResult(
            name=self.name,
            score=score_0_100,
            signal=signal,
            raw={
                "rz_change_pct": rz_chg,
                "rq_change_pct": rq_chg,
                "score_rz": score_rz,
                "score_rq": score_rq,
                "score_trend": score_trend,
                "raw_score_internal": raw_score,
                "recent_rz": rz.tolist(),
                "recent_rq": rq.tolist(),
            },
        )

    # ---- 工具函数 ----

    @staticmethod
    def _pct_change(prev: float, curr: float) -> float:
        if prev == 0:
            return 0.0
        return (curr - prev) / prev

    @staticmethod
    def _score_change_pos(chg: float) -> float:
        if chg >= 0.05:
            return 1.0
        if chg >= 0.02:
            return 0.5
        if chg <= -0.05:
            return -1.0
        if chg <= -0.02:
            return -0.5
        return 0.0

    @staticmethod
    def _score_change_neg(chg: float) -> float:
        if chg <= -0.05:
            return 1.0
        if chg <= -0.02:
            return 0.5
        if chg >= 0.05:
            return -1.0
        if chg >= 0.02:
            return -0.5
        return 0.0

    def _score_trend(self, rz_tail: List[float]) -> float:
        y = np.array(rz_tail, dtype=float)
        n = len(y)
        if n < 3:
            return 0.0

        x = np.arange(n, dtype=float)
        try:
            k, b = np.polyfit(x, y, 1)
        except Exception:
            return 0.0

        mean_val = float(y.mean()) or 1.0
        norm_slope = k / mean_val

        if norm_slope >= 0.01:
            return 1.0
        if norm_slope >= 0.004:
            return 0.5
        if norm_slope <= -0.01:
            return -1.0
        if norm_slope <= -0.004:
            return -0.5
        return 0.0

    @staticmethod
    def _map_to_0_100(raw: float) -> float:
        raw_clamped = max(-1.0, min(1.0, raw))
        return round(50.0 + raw_clamped * 50.0, 2)
