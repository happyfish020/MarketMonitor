from typing import Dict, Any
import json
from core.factors.factor_base import BaseFactor, FactorResult


class IndexTechFactor(BaseFactor):
    def __init__(self):
        super().__init__("index_tech")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "index_tech", {})

        hs300 = float(data.get("hs300_pct", 0.0))
        zz500 = float(data.get("zz500_pct", 0.0))
        kc50  = float(data.get("kc50_pct", 0.0))

        avg = (hs300 + zz500 + kc50) / 3.0
        score = 50.0 + avg * 10.0

        return self.build_result(
            score=score,
            details={
                "hs300_pct": hs300,
                "zz500_pct": zz500,
                "kc50_pct": kc50,
                "avg_pct": avg,
                "_raw_data": json.dumps(data)[:160] + "...",
            },
        )
