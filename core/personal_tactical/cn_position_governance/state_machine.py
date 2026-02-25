from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from .config import RISK_LOW, RISK_NORMAL, RISK_HIGH
from .oracle_facts import PriceBar


@dataclass(frozen=True)
class RiskMetrics:
    ret_10d: float
    dd_10d: float
    vol_shrink_ratio: float
    ma20: float
    close: float
    below_ma20_3d: bool


class RiskStateMachine:
    """Risk level classification based on 10d return/drawdown and volume shrink."""

    @staticmethod
    def _pct_change(a: float, b: float) -> float:
        # a / b - 1
        if b == 0:
            return 0.0
        return a / b - 1.0

    @staticmethod
    def compute_metrics(bars: List[PriceBar]) -> Optional[RiskMetrics]:
        if not bars or len(bars) < 21:
            return None

        closes = [b.close for b in bars]
        vols = [b.volume for b in bars]

        close = closes[-1]

        # MA20: last 20 closes (including latest)
        ma20 = sum(closes[-20:]) / 20.0

        # 10d return: close_t / close_t-10 - 1
        ret_10d = RiskStateMachine._pct_change(closes[-1], closes[-11])

        # 10d drawdown: min(close/running_max -1) within last 10 bars
        last10 = closes[-10:]
        running_max = []
        m = -10**18
        for c in last10:
            m = max(m, c)
            running_max.append(m)
        dd_10d = min([(c / rm - 1.0) if rm != 0 else 0.0 for c, rm in zip(last10, running_max)])

        # volume shrink ratio: avg(last 5) / avg(prev 5) within last 10 bars
        v_recent = sum(vols[-5:]) / 5.0
        v_prev = sum(vols[-10:-5]) / 5.0 if sum(vols[-10:-5]) != 0 else 0.0
        vol_shrink_ratio = (v_recent / v_prev) if v_prev != 0 else 1.0

        below_ma20_3d = all([c < ma20 for c in closes[-3:]])

        return RiskMetrics(
            ret_10d=ret_10d,
            dd_10d=dd_10d,
            vol_shrink_ratio=vol_shrink_ratio,
            ma20=ma20,
            close=close,
            below_ma20_3d=below_ma20_3d,
        )

    @staticmethod
    def classify(metrics: RiskMetrics) -> str:
        # HIGH triggers
        if metrics.dd_10d <= -0.15:
            return RISK_HIGH
        if metrics.ret_10d <= -0.08:
            return RISK_HIGH
        if metrics.vol_shrink_ratio <= 0.60 and metrics.ret_10d < 0:
            return RISK_HIGH

        # LOW triggers
        if metrics.ret_10d >= 0.03 and metrics.dd_10d >= -0.05 and metrics.vol_shrink_ratio >= 0.80:
            return RISK_LOW

        return RISK_NORMAL
