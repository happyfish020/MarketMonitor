from dataclasses import dataclass
from typing import Dict

@dataclass
class RiskScoreResult:
    total_score: float
    level: str
    description: str

def classify_level(score: float) -> str:
    if score <= -4:
        return "极低风险 / 建议积极加仓"
    if score <= -1:
        return "偏低风险 / 可以适度加仓"
    if score <= 1:
        return "中性 / 观望为主"
    if score <= 4:
        return "偏高风险 / 建议适度减仓"
    return "极高风险 / 建议大幅减仓观望"

def build_description(score: float, yesterday: float | None = None) -> str:
    level = classify_level(score)
    if yesterday is None:
        return f"当前风险等级：{level}（无昨日对比）。"
    diff = score - yesterday
    if abs(diff) < 0.5:
        trend = "与昨日变化不大。"
    elif diff > 0:
        trend = f"比昨日风险有所上升（+{diff:.2f} 分）。"
    else:
        trend = f"比昨日风险有所下降（{diff:.2f} 分）。"
    return f"当前风险等级：{level}；{trend}"

def aggregate_factors(factors: Dict[str, float], weights: Dict[str, float]) -> float:
    total = 0.0
    for name, w in weights.items():
        v = factors.get(name, 0.0)
        total += v * w
    return total
