from typing import Dict, Any
import json
from core.factors.factor_base import BaseFactor, FactorResult


class SectorRotationFactor(BaseFactor):
    def __init__(self):
        super().__init__("sector_rotation")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "sector_rotation", {})

        diff = float(data.get("rotation_diff", 0.0))
        score = 50.0 + diff * 15.0

        return self.build_result(
            score=score,
            details={"rotation_diff": diff,
                    "_raw_data": json.dumps(data)[:160] + "...",
                    },
        )
