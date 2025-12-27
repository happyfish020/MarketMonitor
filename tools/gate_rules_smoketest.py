# -*- coding: utf-8 -*-
"""UnifiedRisk V12
Gate Rule Smoke Tests (GX-STRUCTURE-SYNC-V1)

运行：
  python tools/gate_rules_smoketest.py

说明：
- 该脚本不依赖 pytest
- 仅用于验证冻结规则 GX-STRUCTURE-SYNC-V1 的核心断言
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

from core.regime.ashares_gate_decider import ASharesGateDecider


@dataclass
class DummySlot:
    level: str


@dataclass
class DummyFactor:
    level: str
    score: float = 50.0
    details: Dict[str, Any] = field(default_factory=dict)


def _run_case(case_id: str, phase: str, breadth: str | None, etf_sync: str | None) -> None:
    decider = ASharesGateDecider()

    snapshot = {"_meta": {"phase": phase}}
    slots: Dict[str, Any] = {}
    factors: Dict[str, Any] = {}

    if breadth is not None:
        slots["breadth"] = DummySlot(level=breadth)

    if etf_sync is not None:
        factors["etf_index_sync"] = DummyFactor(level=etf_sync)

    # participation 不参与本规则的断言，为避免其它分支影响，强制 HIGH
    slots["participation"] = DummySlot(level="HIGH")

    decision = decider.decide(snapshot=snapshot, slots=slots, factors=factors)

    print(f"{case_id}: phase={phase} breadth={breadth} etf={etf_sync} -> gate={decision.level} reasons={decision.reasons}")

    # 核心冻结断言：Breadth=LOW 时，不应出现 ETF crowding reason
    if breadth == "LOW":
        assert decision.level in ("CAUTION", "PLANB", "FREEZE")
        assert "gx_structure_sync_breadth_low" in decision.reasons
        assert "gx_structure_sync_etf_crowding" not in decision.reasons


def main() -> None:
    _run_case("GX01", "PHASE_2", "LOW", "HIGH")
    _run_case("GX02", "PHASE_2", "LOW", "LOW")
    _run_case("GX03", "PHASE_2", "NEUTRAL", "HIGH")
    _run_case("GX04", "PHASE_2", "HIGH", "HIGH")
    _run_case("GX05", "PHASE_2", "NEUTRAL", "NEUTRAL")
    _run_case("GX06", "PHASE_2", "NEUTRAL", None)
    _run_case("GX07", "PHASE_2", None, "HIGH")

    print("\n✅ GX-STRUCTURE-SYNC-V1 smoke tests passed")


if __name__ == "__main__":
    main()
