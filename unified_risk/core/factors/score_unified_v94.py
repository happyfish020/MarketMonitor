
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class BlockScore:
    name: str
    score: float
    weight: float
    level: str
    description: str
    raw: Dict[str, Any]

@dataclass
class UnifiedScore:
    total: float
    level: str
    blocks: Dict[str, BlockScore]

def unify_scores(blocks: Dict[str, BlockScore]) -> UnifiedScore:
    """统一合成五大 Block 得分。

    注意：
    - total 为 0-100 区间（假定各 Block 的 score 已经是 0-100 制）
    - level 文案统一为：低风险 / 中性 / 偏高风险
    """
    total = 0.0
    wsum = 0.0
    for b in blocks.values():
        w = float(getattr(b, "weight", 0.0) or 0.0)
        s = float(getattr(b, "score", 0.0) or 0.0)
        if w <= 0:
            continue
        total += s * w
        wsum += w
    total = total / wsum if wsum else 0.0

    if total >= 70:
        level = "低风险 / 偏多"
    elif total >= 40:
        level = "中性 / 观望"
    else:
        level = "偏高风险 / 谨慎"

    return UnifiedScore(total=total, level=level, blocks=blocks)
