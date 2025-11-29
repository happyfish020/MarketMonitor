from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class UnifiedScore:
    total: float
    level: str
    components: Dict[str, float]
    explanation: str

def _risk_level(score: float) -> str:
    if score >= 80:
        return "极低风险 / 偏多机会"
    if score >= 60:
        return "低风险 / 温和偏多"
    if score >= 40:
        return "中性 / 平衡"
    if score >= 20:
        return "偏高风险 / 谨慎"
    return "高风险 / 防守优先"

def unify_scores(
    a_emotion: float,
    a_short: float,
    a_mid: float,
    us_daily: float,
    us_short: float,
    us_mid: float,
    a_north: float | None = None,
) -> UnifiedScore:
    comp: Dict[str, float] = {
        "A_Emotion": a_emotion,
        "A_Short": a_short,
        "A_Mid": a_mid,
    }
    if a_north is not None:
        comp["A_North"] = a_north
    comp.update({
        "US_Daily": us_daily,
        "US_Short": us_short,
        "US_Mid": us_mid,
    })

    def _block(scores, max_block: float) -> float:
        vals = [s for s in scores if s is not None]
        if not vals:
            return 0.0
        avg = sum(vals) / len(vals)
        blk = avg / 20.0 * max_block
        return max(0.0, min(max_block, blk))

    a_scores = [a_emotion, a_short, a_mid]
    if a_north is not None:
        a_scores.append(a_north)
    a_block = _block(a_scores, 60.0)
    us_block = _block([us_daily, us_short, us_mid], 40.0)

    total = max(min(a_block * 0.6 + us_block * 0.4, 100), 0)
    level = _risk_level(total)
    expl = f"综合风险得分 {total:.1f} / 100，{level}。A股分块 {a_block:.1f}，美股分块 {us_block:.1f}。"
    return UnifiedScore(total=total, level=level, components=comp, explanation=expl)

