# -*- coding: utf-8 -*-
"""UAT: StructureFacts state normalize + alias_map semantics (3 cases).

Run:
    python core/uat/uat_structure_state_semantics_tests.py
"""
from __future__ import annotations

import yaml

from core.regime.observation.structure.structure_facts_builder import StructureFactsBuilder

def _load_cfg() -> dict:
    with open("config/structure_facts.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main() -> None:
    cfg = _load_cfg()
    spec = cfg.get("factors") or {}
    b = StructureFactsBuilder(spec=cfg)

    # CASE 1: failure_rate details.state = "STABLE" -> normalize -> "stable" -> meaning should exist
    factors = {
        "failure_rate": {"level": "LOW", "score": 10.0, "details": {"state": "STABLE", "fail_rate": 0.02}}
    }
    out = b.build(factors=factors, structure_keys=["failure_rate"])
    assert out["failure_rate"]["state"] == "stable", out["failure_rate"]
    assert "meaning" in out["failure_rate"], out["failure_rate"]

    # CASE 2: trend_in_force details.state = "IN_FORCE" -> normalize -> "in_force" -> meaning should exist
    factors = {
        "trend_in_force": {"level": "HIGH", "score": 90.0, "details": {"state": "IN_FORCE"}}
    }
    out = b.build(factors=factors, structure_keys=["trend_in_force"])
    assert out["trend_in_force"]["state"] == "in_force", out["trend_in_force"]
    assert "meaning" in out["trend_in_force"], out["trend_in_force"]

    # CASE 3: north_proxy_pressure details.state = "low" -> alias_map -> "pressure_low" -> meaning should exist
    factors = {
        "north_proxy_pressure": {"level": "LOW", "score": 20.0, "details": {"state": "low", "pressure_score": 10.0}}
    }
    out = b.build(factors=factors, structure_keys=["north_proxy_pressure"])
    assert out["north_proxy_pressure"]["state"] == "pressure_low", out["north_proxy_pressure"]
    assert "meaning" in out["north_proxy_pressure"], out["north_proxy_pressure"]

    print("ALL TESTS PASSED")

if __name__ == "__main__":
    main()
