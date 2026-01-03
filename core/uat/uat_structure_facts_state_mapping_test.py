# -*- coding: utf-8 -*-
"""
UAT-P0: StructureFactsBuilder state normalization + alias_map mapping

Goal:
- Ensure StructureFactsBuilder._derive_state accepts `details` (dict) and produces normalized state
- Ensure meaning is resolved from config/structure_facts.yaml (no "missing_semantics" for covered states)

Run:
    python -m core.uat.uat_structure_facts_state_mapping_test
"""

from __future__ import annotations

import os
import sys
import yaml

from pathlib import Path

# Allow running this file directly (not only as a module) by ensuring repo root is on sys.path.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.factors.factor_result import FactorResult
from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder


def _load_yaml(rel_path: str):
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    p = os.path.join(root, rel_path)
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f), p


def _assert(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def main():
    cfg, cfg_path = _load_yaml("config/structure_facts.yaml")
    builder = StructureFactsBuilder(spec=cfg)

    # Case 1: trend_in_force (details.state="IN_FORCE") -> "in_force" and meaning exists
    fr_trend = FactorResult(
        name="trend_in_force",
        score=50.0,
        level="NEUTRAL",
        details={"state": "IN_FORCE"},
    )

    # Case 2: failure_rate (details.state="STABLE") -> "stable" and meaning exists
    fr_fr = FactorResult(
        name="failure_rate",
        score=50.0,
        level="NEUTRAL",
        details={"state": "STABLE"},
    )

    # Case 3: north_proxy_pressure (details.state="low") -> alias_map -> "pressure_low"
    fr_north = FactorResult(
        name="north_proxy_pressure",
        score=27.9,
        level="NEUTRAL",
        details={"state": "low", "pressure_level": "NEUTRAL", "pressure_score": 27.9},
    )

    # Case 4: index_tech (no details.state, rely on level_map) HIGH -> "strong"
    fr_idx = FactorResult(
        name="index_tech",
        score=80.0,
        level="HIGH",
        details={},
    )

    structure = builder.build(
        factors={
            "trend_in_force": fr_trend,
            "failure_rate": fr_fr,
            "north_proxy_pressure": fr_north,
            "index_tech": fr_idx,
        },
        structure_keys=["trend_in_force", "failure_rate", "north_proxy_pressure", "index_tech"],
    )

    # Assertions
    _assert(structure["trend_in_force"]["state"] == "in_force", "trend_in_force state normalize failed")
    _assert(isinstance(structure["trend_in_force"].get("meaning"), str) and structure["trend_in_force"]["meaning"], "trend_in_force meaning missing")

    _assert(structure["failure_rate"]["state"] == "stable", "failure_rate state normalize failed")
    _assert(isinstance(structure["failure_rate"].get("meaning"), str) and structure["failure_rate"]["meaning"], "failure_rate meaning missing")

    _assert(structure["north_proxy_pressure"]["state"] == "pressure_low", "north_proxy_pressure alias_map failed")
    _assert(isinstance(structure["north_proxy_pressure"].get("meaning"), str) and structure["north_proxy_pressure"]["meaning"], "north_proxy_pressure meaning missing")

    _assert(structure["index_tech"]["state"] == "strong", "index_tech level_map failed")
    _assert(isinstance(structure["index_tech"].get("meaning"), str) and structure["index_tech"]["meaning"], "index_tech meaning missing")

    print("[PASS] StructureFactsBuilder state normalization + meaning mapping OK")
    print(f"        config loaded from: {cfg_path}")


if __name__ == "__main__":
    main()
