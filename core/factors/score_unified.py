# core/factors/score_unified.py

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class UnifiedScore:
    """
    汇总后的统一得分：
        - total: 0~100 综合评分
        - components: 各因子得分明细
    """

    total: float
    components: Dict[str, float] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "total": float(self.total),
            "components": {k: float(v) for k, v in self.components.items()},
        }


def unify_scores(**kwargs: float) -> UnifiedScore:
    """
    将若干因子得分（0~100）汇总成一个 UnifiedScore。

    用法：
        unified = unify_scores(
            emotion=60,
            turnover=55,
            ...
        )
    """
    components = {k: float(v) for k, v in kwargs.items()}

    if not components:
        return UnifiedScore(total=50.0, components={})

    total = sum(components.values()) / len(components)

    return UnifiedScore(total=total, components=components)
