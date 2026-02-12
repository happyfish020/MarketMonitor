# -*- coding: utf-8 -*-
"""
Daily Risk Signal (DRS) Observation
UnifiedRisk V12 · Phase-2 Observation（冻结）

性质：
- 解释性 Observation
- 不构成预测 / 推荐 / 行动指令
"""

from __future__ import annotations

from typing import Any, Dict, List
from dataclasses import asdict

from core.regime.observation.observation_base import ObservationBase, ObservationMeta


class DRSObservation(ObservationBase):
    """
    DRS（日度风险信号）

    输入（冻结最小集）：
    - structure.trend_in_force
    - structure.frf（legacy）/ structure.failure_rate（canonical）
    """

    # -----------------------------
    # Meta（冻结字段）
    # -----------------------------
    @property
    def meta(self) -> ObservationMeta:
        return ObservationMeta(
            kind="daily_risk_signal",
            profile="index",
            phase="P2",
            asof="EOD",
            inputs=[
                "structure.trend_in_force",
                "structure.frf",
                "structure.failure_rate",
            ],
            note="日度制度风险信号，仅用于风险环境提示，不构成交易或仓位指令。",
        )

    # -----------------------------
    # Build Observation
    # -----------------------------
    def build(self, *, inputs: Dict[str, Any], asof: str) -> Dict[str, Any]:
        """
        inputs: 来自 Phase-2 的只读结构输出（structure）
        """

        structure = inputs if isinstance(inputs, dict) else {}

        trend_state = structure.get("trend_in_force", {}).get("state")
        # Backward/forward compatible: some builds output `failure_rate`, others use legacy key `frf`.
        frf_state = (structure.get("frf", {}) or {}).get("state")
        if frf_state is None:
            frf_state = (structure.get("failure_rate", {}) or {}).get("state")

        signal = "GREEN"
        meaning = "趋势结构稳定，制度风险环境相对可控。"
        drivers: List[str] = []

        # -----------------------------
        # 冻结判定顺序（不引入新制度）
        # -----------------------------
        if trend_state == "broken":
            signal = "RED"
            meaning = "趋势结构已被破坏，制度风险处于高位。"
            drivers.append("trend_in_force:broken")

        elif frf_state == "elevated_risk":
            signal = "YELLOW"
            meaning = "趋势结构失效迹象增多，需提高风险警惕。"
            drivers.append("frf:elevated_risk")

        elif frf_state == "watch":
            signal = "YELLOW"
            meaning = "趋势结构进入观察阶段，风险环境偏中性。"
            drivers.append("frf:watch")

        observation = {
            "signal": signal,
            "meaning": meaning,
            "drivers": drivers,
        }

        result = {
            "meta": asdict(self.meta),
            "evidence": {
                "trend_in_force_state": trend_state,
                "frf_state": frf_state,
            },
            "observation": observation,
        }

        # 冻结：只做轻校验，不 raise
        self.validate(observation=result)
        return result
