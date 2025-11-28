from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class USMidTermResult:
    score: float
    trend: str
    description: str
    raw: Dict[str, Any]

def compute_us_mid_term(weekly: Dict[str, List[float]]) -> USMidTermResult:
    sp = weekly.get("sp500") or []
    if len(sp) < 4:
        return USMidTermResult(
            score=10.0,
            trend="中性",
            description="美股周线数据不足，中性处理。",
            raw={"len": len(sp)},
        )
    last4 = sp[-4:]
    avg = sum(last4) / len(last4)
    score = max(min(avg * 5 + 10, 20), 0)
    if avg > 0.8:
        trend = "中期多头"
    elif avg < -0.8:
        trend = "中期空头"
    else:
        trend = "区间震荡"
    desc = f"SP500 近4周平均涨跌 {avg:.2f}%，趋势：{trend}（{score:.1f}/20）。"
    return USMidTermResult(score=score, trend=trend, description=desc, raw={"avg4": avg, "last4": last4})
