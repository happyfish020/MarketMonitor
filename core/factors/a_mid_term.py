from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class MidTermResult:
    score: float
    trend: str
    description: str
    raw: Dict[str, Any]

def compute_a_mid_term(weekly: Dict[str, List[float]]) -> MidTermResult:
    """A股中期（周线）趋势因子。"""
    sh = weekly.get("index_sh") or []
    if len(sh) < 4:
        return MidTermResult(
            score=10.0,
            trend="中性",
            description="周线数据不足，暂按中性处理。",
            raw={"len": len(sh)},
        )

    last4 = sh[-4:]
    avg = sum(last4) / len(last4)
    score = max(min(avg * 5 + 10, 20), 0)

    if avg > 0.8:
        trend = "中期多头"
    elif avg < -0.8:
        trend = "中期空头"
    else:
        trend = "区间震荡"

    desc = f"上证近4周平均涨跌 {avg:.2f}%，判定为「{trend}」，中期得分 {score:.1f}/20。"
    return MidTermResult(score=score, trend=trend, description=desc, raw={"avg4w": avg, "last4": last4})
