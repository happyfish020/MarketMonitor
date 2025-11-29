from __future__ import annotations
from typing import Dict, Any

def run_global(a_share: Dict[str, Any], us: Dict[str, Any]) -> Dict[str, Any]:
    a_u = a_share.get("unified")
    us_d = us.get("daily")

    if not a_u or not us_d:
        summary = "Global view: 数据不足。"
        return {"summary": summary}

    total = a_u.total
    us_bias = us_d.score - 10.0

    if us_bias > 3:
        tilt = "外盘偏多，对A股有正向支撑。"
    elif us_bias < -3:
        tilt = "外盘偏空，需警惕情绪传导。"
    else:
        tilt = "外盘中性影响。"

    summary = (
        f"[全球视角] 当前 A股 综合风险 {total:.1f}/100（{a_u.level}）。"
        f" 美股当日得分 {us_d.score:.1f}/20，{tilt}"
    )

    return {
        "summary": summary,
        "a_score": total,
        "us_score": us_d.score,
        "tilt": tilt,
    }
