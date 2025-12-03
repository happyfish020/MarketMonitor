from __future__ import annotations

from typing import Dict, Any
from core.models.factor_result import FactorResult


class TurnoverFactor:
    """A股市场成交额因子（流动性 / 动能）。"""

    name = "turnover"

    def compute_from_daily(self, processed: Dict[str, Any]) -> FactorResult:
        f = processed.get("features", {})

        sh = float(f.get("sh_turnover_e9", 0.0) or 0.0)
        sz = float(f.get("sz_turnover_e9", 0.0) or 0.0)
        total = float(f.get("total_turnover_e9", 0.0) or 0.0)

        normal_base = 1000.0
        ratio = total / normal_base if normal_base > 0 else 1.0

        if ratio >= 1.5:
            score = 90.0
            desc = "成交额极度放大，动能强烈"
        elif ratio >= 1.3:
            score = 80.0
            desc = "成交额显著放大，情绪偏强"
        elif ratio >= 1.1:
            score = 70.0
            desc = "成交额放大，情绪偏暖"
        elif ratio >= 0.9:
            score = 50.0
            desc = "成交额正常，情绪中性"
        elif ratio >= 0.8:
            score = 35.0
            desc = "成交额偏低，情绪走弱"
        else:
            score = 20.0
            desc = "成交额极低，风险偏回避"

        signal = f"{desc}（total={total:.1f} 亿, ratio={ratio:.2f}）"

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "sh_turnover_e9": sh,
                "sz_turnover_e9": sz,
                "total_turnover_e9": total,
                "ratio": ratio,
            },
        )