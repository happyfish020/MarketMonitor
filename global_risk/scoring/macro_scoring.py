
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..utils.logging_utils import setup_logger

logger = setup_logger(__name__)


@dataclass
class GlobalMacroSnapshot:
    treasury_5y: Optional[float] = None
    treasury_10y: Optional[float] = None
    ycurve_bps: Optional[float] = None
    nasdaq_pct: Optional[float] = None
    spy_pct: Optional[float] = None
    vix_last: Optional[float] = None
    dax_pct: Optional[float] = None
    ftse_pct: Optional[float] = None
    a50_night_pct: Optional[float] = None
    a50_night_proxy: Optional[str] = None


@dataclass
class GlobalMacroScore:
    total_score: float
    risk_level: str
    description: str


def score_global_macro(snapshot: GlobalMacroSnapshot) -> GlobalMacroScore:
    score = 50.0
    parts = []

    if snapshot.ycurve_bps is not None:
        if snapshot.ycurve_bps < 0:
            score -= 10
            parts.append("收益率曲线倒挂")
        elif snapshot.ycurve_bps < 50:
            score -= 5
            parts.append("收益率曲线偏平")

    if snapshot.vix_last is not None:
        if snapshot.vix_last > 25:
            score -= 15
            parts.append("VIX 显著偏高")
        elif snapshot.vix_last > 20:
            score -= 8
            parts.append("VIX 略偏高")
        elif snapshot.vix_last < 15:
            score += 5
            parts.append("波动率友好")

    for name, pct in [
        ("纳指", snapshot.nasdaq_pct),
        ("SPY", snapshot.spy_pct),
        ("DAX", snapshot.dax_pct),
        ("FTSE", snapshot.ftse_pct),
    ]:
        if pct is None:
            continue
        if pct < -2.0:
            score -= 6
            parts.append(f"{name} 大幅下跌 {pct:.2f}%")
        elif pct < 0.0:
            score -= 2
        elif pct > 2.0:
            score += 3
            parts.append(f"{name} 大幅上涨 {pct:.2f}%")

    if snapshot.a50_night_pct is not None:
        if snapshot.a50_night_pct < -1.5:
            score -= 6
            parts.append(f"A50 夜盘大跌 {snapshot.a50_night_pct:.2f}%")
        elif snapshot.a50_night_pct > 1.5:
            score += 3
            parts.append(f"A50 夜盘大涨 {snapshot.a50_night_pct:.2f}%")

    score = max(0.0, min(100.0, score))

    if score >= 70:
        level = "外围环境偏友好"
    elif score >= 50:
        level = "外围环境中性"
    elif score >= 30:
        level = "外围环境偏紧"
    else:
        level = "外围环境高风险"

    desc = "；".join(parts) if parts else "指标中性，无显著极端信号"
    return GlobalMacroScore(total_score=score, risk_level=level, description=desc)
