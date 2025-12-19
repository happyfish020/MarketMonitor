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

    def decide(self, snapshot: Dict[str, Any], slots: Dict[str, Any]) -> GateDecision:
        """
        输入：
        - snapshot：当日快照（用于取 trade_date 等元信息）
        - slots：PolicySlotBinder 输出的 FactorResult 映射（slot -> FactorResult）

        输出：
        - GateDecision（只读）
        """
        level = "NORMAL"
        reasons: List[str] = []
        evidence: Dict[str, Any] = {}

        # ---- 结构性反证：ETF × Index × Spot 同步 ----
        etf_sync = slots.get("etf_index_sync")
        if etf_sync is not None:
            evidence["etf_index_sync"] = {
                "level": etf_sync.level,
                "score": etf_sync.score,
            }
            # 冻结规则：LOW 只能降级到 CAUTION
            if etf_sync.level == "LOW":
                level = _max(level, "CAUTION")
                reasons.append("etf_index_sync_disconfirm")

        # 你已有的其他结构证据（示例：breadth / participation）
        breadth = slots.get("breadth")
        if breadth is not None:
            evidence["breadth"] = {"level": breadth.level}
            if breadth.level == "LOW":
                level = _max(level, "CAUTION")
                reasons.append("breadth_weak")

        participation = slots.get("participation")
        if participation is not None:
            evidence["participation"] = {"level": participation.level}
            if participation.level == "LOW":
                level = _max(level, "CAUTION")
                reasons.append("participation_weak")

        # 兜底合法性
        if level not in _ALLOWED:
            level = "CAUTION"
            reasons.append("invalid_level_fallback")

        return GateDecision(level=level, reasons=reasons, evidence=evidence)
