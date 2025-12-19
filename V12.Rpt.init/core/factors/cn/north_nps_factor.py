from typing import Dict, Any
import json
from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class NorthNPSFactor(FactorBase):
    def __init__(self):
        super().__init__("north_nps_raw")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "north_nps_raw", {})
        assert data, "north_nps_raw is missing"
        # ① 数据完全缺失 → DATA_NOT_CONNECTED
        if not data:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "north_nps data missing",
                },
            )

        # ② 数据存在，但字段异常 / 不完整（可选示例）
        #    如果你以后要加 STALE / PARTIAL，就在这里判断
        try:
            strength = float(data.get("strength_today", 0.0))
            trend = float(data.get("trend_5d", 0.0))
        except Exception:
            return FactorResult(
                name=self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "STALE",
                    "reason": "north_nps data parse failed",
                },
            )

        # ③ 正常可用数据 → OK
        score = 50.0 + strength * 5 + trend * 2

        return self.build_result(
            score=score,
            details={
                # 🔒 Step-3 核心：显式标 OK
                "data_status": "OK",

                # 原有业务字段（保持不删）
                "strength_today": strength,
                "trend_5d": trend,

                # 调试证据（可审计）
                "_raw_data": json.dumps(data)[:160] + "...",
            },
        )
