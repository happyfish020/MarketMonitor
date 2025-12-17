from typing import Dict, Any
import json
from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult

class IndexTechFactor(FactorBase):
    def __init__(self):
        super().__init__("index_tech_raw")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "index_tech_raw", {})
        if not data:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",   # 风险语义保持中性
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "index_tech_raw data missing",
                    
                },
            )
            

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
                "data_status": "OK",
                "_raw_data": json.dumps(data),
            },
        )
