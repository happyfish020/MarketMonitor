from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class USDailyResult:
    score: float
    level: str
    description: str
    raw: Dict[str, Any]

def compute_us_daily(snapshot: Dict[str, Any]) -> USDailyResult:
    nas = snapshot.get("nasdaq", {}).get("changePct", 0.0)
    spy = snapshot.get("spy", {}).get("changePct", 0.0)
    vix = snapshot.get("vix", {}).get("price", 20.0)

    risk = 0.0
    if nas < 0:
        risk += 3
    if spy < 0:
        risk += 3
    if vix > 20:
        risk += (vix - 20) * 0.5

    score = max(min(20 - risk, 20), 0)

    if score >= 15:
        level = "美股偏强"
    elif score >= 8:
        level = "美股中性"
    else:
        level = "美股偏弱 / 风险偏高"

    desc = f"纳指 {nas:.2f}%，SPY {spy:.2f}%，VIX {vix:.1f}，美股日级得分 {score:.1f}/20（{level}）。"
    return USDailyResult(score=score, level=level, description=desc, raw={"nas": nas, "spy": spy, "vix": vix})
