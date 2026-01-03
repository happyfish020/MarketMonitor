# -*- coding: utf-8 -*-
"""UnifiedRisk V12.1 UAT - StructureFactsBuilder state mapping tests (Frozen)

Run:
  python core/uat/uat_structure_state_tests.py

This is a minimal, dependency-light self-test without pytest.
It validates the *priority & behavior* of:
  details_keys -> level_map -> default
  then normalize -> alias_map

Required: project import path available (run from project root).
"""

from __future__ import annotations

import sys
from typing import Any, Dict

from core.factors.factor_result import FactorResult
from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder
from core.utils.logger import get_logger

LOG = get_logger(__name__)


def _build_min_cfg() -> Dict[str, Any]:
    # Minimal structure_facts.yaml-like config (only fields used by builder).
    return {
        "factors": {
            "north_proxy_pressure": {
                "sources": ["north_proxy_pressure"],
                "state": {
                    "details_keys": ["state", "pressure_level"],
                    "normalize": True,
                    "alias_map": {
                        "low": "pressure_low",
                        "neutral": "pressure_medium",
                        "high": "pressure_high",
                    },
                    "level_map": {
                        "LOW": "pressure_low",
                        "NEUTRAL": "pressure_medium",
                        "HIGH": "pressure_high",
                    },
                    "default": "missing",
                },
                "meaning": {
                    "by_state": {
                        "pressure_low": "ok",
                        "pressure_medium": "mid",
                        "pressure_high": "high",
                        "missing": "missing",
                    }
                },
            },
            "trend_in_force": {
                "sources": ["trend_in_force"],
                "state": {
                    "details_keys": ["state"],
                    "normalize": True,
                    "alias_map": {"data_missing": "missing"},
                    "default": "missing",
                },
                "meaning": {
                    "by_state": {
                        "in_force": "ok",
                        "weakening": "weak",
                        "broken": "broken",
                        "missing": "missing",
                    }
                },
            },
        }
    }


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def main() -> int:
    cfg = _build_min_cfg()
    builder = StructureFactsBuilder(structure_cfg=cfg)

    # Case 1: details.state present (LOW) -> normalize -> alias_map -> pressure_low
    fr1 = FactorResult(
        name="north_proxy_pressure",
        score=10.0,
        level="HIGH",
        details={"state": "LOW"},
    )
    out1 = builder.build({"north_proxy_pressure": fr1}, structure_keys=["north_proxy_pressure"])
    _assert(out1["north_proxy_pressure"]["state"] == "pressure_low", "case1 failed")

    # Case 2: details missing, level_map should apply
    fr2 = FactorResult(
        name="north_proxy_pressure",
        score=10.0,
        level="HIGH",
        details={},
    )
    out2 = builder.build({"north_proxy_pressure": fr2}, structure_keys=["north_proxy_pressure"])
    _assert(out2["north_proxy_pressure"]["state"] == "pressure_high", "case2 failed")

    # Case 3: trend_in_force details.state IN_FORCE -> normalize -> in_force
    fr3 = FactorResult(
        name="trend_in_force",
        score=80.0,
        level="LOW",
        details={"state": "IN_FORCE"},
    )
    out3 = builder.build({"trend_in_force": fr3}, structure_keys=["trend_in_force"])
    _assert(out3["trend_in_force"]["state"] == "in_force", "case3 failed")

    print("[ OK ] all 3 StructureFactsBuilder state-mapping cases passed")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        LOG.exception("uat_structure_state_tests failed: %s", e)
        print(f"[FAIL] {e}")
        sys.exit(1)
