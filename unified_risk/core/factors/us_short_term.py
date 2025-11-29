from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class USShortTermResult:
    score: float
    direction: str
    description: str
    raw: Dict[str, Any]

def compute_us_short_term(series: Dict[str, List[float]]) -> USShortTermResult:
    ndx = series.get("nasdaq") or []
    if len(ndx) < 5:
        return USShortTermResult(
            score=10.0,
            direction="中性",
            description="美股短期数据不足，维持中性。",
            raw={"len": len(ndx)},
        )
    last5 = ndx[-5:]
    avg = sum(last5) / len(last5)
    score = max(min(avg * 5 + 10, 20), 0)

    if avg > 0.6:
        direction = "短期偏多"
    elif avg < -0.6:
        direction = "短期偏空"
    else:
        direction = "短期震荡"

    desc = f"纳指近5日均涨跌 {avg:.2f}%，短期趋势：{direction}（{score:.1f}/20）。"
    return USShortTermResult(score=score, direction=direction, description=desc, raw={"avg5": avg, "last5": last5})
