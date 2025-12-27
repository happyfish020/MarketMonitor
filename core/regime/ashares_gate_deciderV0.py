# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 FULL
AShares Gate Decider (Phase-2)

职责（冻结）：
- Phase-2 制度裁决：生成 Gate（NORMAL / CAUTION / PLANB / FREEZE）
- 只做“更保守”的单向裁决（降级快、恢复慢）
- 不依赖 PredictionEngine
- 不生成 Action
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from core.factors.factor_result import FactorResult

_ALLOWED = ("NORMAL", "CAUTION", "PLANB", "FREEZE")
_ORDER = {"NORMAL": 0, "CAUTION": 1, "PLANB": 2, "FREEZE": 3}


def _max(a: str, b: str) -> str:
    return a if _ORDER[a] >= _ORDER[b] else b


@dataclass(frozen=True, slots=True)
class GateDecision:
    level: str
    reasons: List[str]
    evidence: Dict[str, Any]


class ASharesGateDecider:
    """
    A 股 Gate 制度裁决器（Phase-2）
    """
 
    ###
    
    _ORDER = ["ALLOW", "NORMAL", "CAUTION", "FREEZE"]

    def _max_gate(self, cur: str, target: str) -> str:
        return self._ORDER[
            max(self._ORDER.index(cur), self._ORDER.index(target))
        ]
    
    def decide(
        self,
        snapshot: Dict[str, Any],
        slots: Dict[str, Any],
        factors: Dict[str, FactorResult],
    ) -> GateDecision:
        """
        UnifiedRisk V12 - Gate Decision (Frozen)
    
        输入说明：
        - snapshot : 原始快照（只读，不直接使用）
        - slots    : 结构槽位（trend_in_force 等）
        - factors  : Phase-2 / Daily Factor 结果（confirmed only）
    
        设计铁律：
        - Gate 只使用 confirmed daily 结构因子
        - Gate 只允许降级（NORMAL → CAUTION → FREEZE）
        - 不读取 Observation（如 DRS）
        - 不使用 intraday 数据
        """
    
        # --------------------------------------------------
        # 初始 Gate
        # --------------------------------------------------
        gate = "NORMAL"
        reasons: list[str] = []
    
        structure = slots.get("structure", {})
        regime = structure.get("regime", {}) if isinstance(structure, dict) else {}
        dist = regime.get("structure_distribution")

        if dist and dist.get("state") == "DISTRIBUTION_RISK":
            gate = self._max_gate(gate, "CAUTION")
            reasons.append("phase3_structure_distribution")
            evidence= "regime - DISTRIBUTION_RISK"



     
        # ==================================================
        # 1️⃣ Trend-in-Force（最高优先级 · 结构硬破坏）
        # ==================================================
        trend = slots.get("trend_in_force")
        if isinstance(trend, dict):
            state = trend.get("state")
            if state == "broken":
                gate = self._max_gate(gate, "CAUTION")
                reasons.append("trend_in_force=broken")
                evidence= "state - broken"
    
        # ==================================================
        # 2️⃣ Breadth（市场广度 · Phase-2 核心）
        # ==================================================
        breadth = factors.get("breadth")
        if breadth is not None:
            lv = getattr(breadth, "level", None)
            if lv == "HIGH":
                gate = self._max_gate(gate, "CAUTION")
                reasons.append("breadth=HIGH")
                evidence= f"breadth - {lv}"
    
        # ==================================================
        # 3️⃣ Participation（参与度 · 必须项）
        # ==================================================
        participation = factors.get("participation")
        if participation is not None:
            lv = getattr(participation, "level", None)
            if lv == "LOW":
                gate = self._max_gate(gate, "CAUTION")
                reasons.append("participation=LOW")
                evidence= f"participation - {lv}"    
        # ==================================================
        # 4️⃣ ETF × Index Structure Sync（Daily / T-1）
        # ==================================================
        sync = factors.get("etf_index_sync_daily")
        if sync is not None:
            lv = getattr(sync, "level", None)
            if lv == "HIGH":
                gate = self._max_gate(gate, "CAUTION")
                reasons.append("etf_index_sync_daily=HIGH")
                evidence= f"etf_index_sync_daily - {lv}"    
    
        # ==================================================
        # 5️⃣ Failure Rate / Structure Failure（如你已有）
        # ==================================================
        frf = factors.get("failure_rate")
        if frf is not None:
            lv = getattr(frf, "level", None)
            if lv == "HIGH":
                gate = self._max_gate(gate, "CAUTION")
                reasons.append("failure_rate=HIGH")
                evidence= f"failure_rate - {lv}"   
    
        # --------------------------------------------------
        # Finalize
        # --------------------------------------------------
        return GateDecision(
            level = gate,
            reasons=reasons,
            evidence=evidence
        )
    
###
  