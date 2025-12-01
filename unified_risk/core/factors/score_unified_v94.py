
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
    total=0.0
    wsum=0.0
    for b in blocks.values():
        w=b.weight
        total+=b.score*w
        wsum+=w
    total = total/wsum if wsum else 0.0
    if total>=70: lvl="低风险"
    elif total>=40: lvl="中性"
    else: lvl="偏高风险"
    return UnifiedScore(total=total, level=lvl, blocks=blocks)
