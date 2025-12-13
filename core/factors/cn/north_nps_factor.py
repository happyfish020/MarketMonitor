from typing import Dict, Any
import json
from core.factors.factor_base import BaseFactor, FactorResult


class NorthNPSFactor(BaseFactor):
    def __init__(self):
        super().__init__("north_nps")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "north_nps", {})

        strength = float(data.get("strength_today", 0.0))
        trend = float(data.get("trend_5d", 0.0))

        score = 50.0 + strength * 5 + trend * 2

        return self.build_result(
            score=score,
            details={
                "strength_today": strength,
                "trend_5d": trend,
                "_raw_data": json.dumps(data)[:160] + "..."
            },
        )
