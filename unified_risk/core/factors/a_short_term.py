from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class ShortTermResult:
    score: float
    direction: str
    description: str
    raw: Dict[str, Any]

def compute_a_short_term(series: Dict[str, List[float]]) -> ShortTermResult:
    """A股短期（T-5）趋势因子。

    目前使用核心宽基 ETF（如 510300）的近 5 日涨跌幅。
    """
    etf = series.get("etf_core") or []
    if len(etf) < 5:
        return ShortTermResult(
            score=10.0,
            direction="中性",
            description="短期数据不足，维持中性判断。",
            raw={"series_len": len(etf)},
        )

    last5 = etf[-5:]
    avg = sum(last5) / len(last5)
    score = max(min(avg * 5 + 10, 20), 0)

    if avg > 0.6:
        direction = "短期偏多"
    elif avg < -0.6:
        direction = "短期偏空"
    else:
        direction = "短期震荡"

    desc = f"核心ETF近5日平均涨跌 {avg:.2f}%，短期趋势：{direction}（得分 {score:.1f}/20）。"
    return ShortTermResult(score=score, direction=direction, description=desc, raw={"avg5": avg, "last5": last5})
