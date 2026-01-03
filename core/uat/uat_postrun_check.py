# -*- coding: utf-8 -*-
"""UnifiedRisk V12.1 UAT - Post-run validation checks (Frozen)

Usage (from project root):
  python core/uat/uat_postrun_check.py --trade-date 2025-12-30

This script is intentionally read-only:
- It does NOT modify任何配置/代码/缓存
- 只读取 config/*.yaml 与 run/reports/doc_{trade_date}.json

Checks:
1) factor pipeline is YAML-driven:
   - factors keys == weights.yaml factor_pipeline.enabled
2) structure facts are YAML-driven:
   - structure keys ⊆ weights.yaml factor_pipeline.structure_factors
3) meaning mapping sanity:
   - for selected structure items, if state != missing, meaning must exist
4) report blocks YAML sanity:
   - each builder import string in report_blocks.yaml is importable

Exit code:
- 0: pass
- 1: fail
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from typing import Any, Dict, List

try:
    import yaml  # type: ignore
except Exception as e:
    print(f"[FAIL] missing dependency: pyyaml ({e})")
    sys.exit(1)


def _load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"yaml root must be dict: {path}")
    return data


def _load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"json root must be dict: {path}")
    return data


def _import_by_spec(spec: str) -> None:
    """Spec format: module:attr"""
    if ":" not in spec:
        raise ValueError(f"invalid builder spec (missing ':'): {spec}")
    mod, attr = spec.split(":", 1)
    m = importlib.import_module(mod)
    getattr(m, attr)


def _fail(msg: str) -> None:
    print(f"[FAIL] {msg}")


def _ok(msg: str) -> None:
    print(f"[ OK ] {msg}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--weights",
        default=os.path.join("config", "weights.yaml"),
        help="path to weights.yaml",
    )
    ap.add_argument(
        "--structure-facts",
        default=os.path.join("config", "structure_facts.yaml"),
        help="path to structure_facts.yaml",
    )
    ap.add_argument(
        "--report-blocks",
        default=os.path.join("config", "report_blocks.yaml"),
        help="path to report_blocks.yaml",
    )
    args = ap.parse_args()

    # --- load configs ---
    try:
        weights = _load_yaml(args.weights)
    except Exception as e:
        _fail(f"cannot load weights.yaml: {e}")
        return 1

    fp = weights.get("factor_pipeline") or {}
    enabled: List[str] = fp.get("enabled") or []
    registry: Dict[str, str] = fp.get("registry") or {}
    structure_keys: List[str] = fp.get("structure_factors") or []

    if not isinstance(enabled, list) or not all(isinstance(x, str) for x in enabled):
        _fail("weights.factor_pipeline.enabled must be a list[str]")
        return 1
    if not isinstance(structure_keys, list) or not all(isinstance(x, str) for x in structure_keys):
        _fail("weights.factor_pipeline.structure_factors must be a list[str]")
        return 1

    # registry coverage
    missing_reg = [k for k in enabled if not isinstance(registry.get(k), str) or not registry.get(k).strip()]
    if missing_reg:
        _fail(f"registry missing for enabled factors: {missing_reg}")
        return 1
    _ok(f"registry covers enabled factors: {len(enabled)}")

    # --- post-run payload ---
    doc_path = os.path.join("run", "reports", f"doc_{args.trade_date}.json")
    try:
        payload = _load_json(doc_path)
    except Exception as e:
        _fail(f"cannot load post-run payload: {doc_path} ({e})")
        return 1

    factors = payload.get("factors") or {}
    if not isinstance(factors, dict):
        _fail("payload.factors must be a dict")
        return 1

    factor_keys = list(factors.keys())
    extra = [k for k in factor_keys if k not in enabled]
    missing = [k for k in enabled if k not in factor_keys]

    if extra:
        _fail(f"payload contains factors not in enabled list: {extra}")
        return 1
    if missing:
        _fail(f"payload missing enabled factors: {missing}")
        return 1
    _ok("payload.factors matches weights.factor_pipeline.enabled")

    # structure
    structure = payload.get("structure") or {}
    if not isinstance(structure, dict):
        _fail("payload.structure must be dict")
        return 1

    struct_extra = [k for k in structure.keys() if k not in structure_keys]
    if struct_extra:
        _fail(f"payload.structure contains keys not in structure_factors: {struct_extra}")
        return 1
    _ok("payload.structure ⊆ weights.factor_pipeline.structure_factors")

    # meaning sanity for a few important items
    def _check_meaning(k: str) -> bool:
        obj = structure.get(k)
        if not isinstance(obj, dict):
            _fail(f"structure.{k} missing or not dict")
            return False
        state = obj.get("state")
        meaning = obj.get("meaning")
        # if truly missing state, allow meaning missing
        if state in (None, "missing", "data_missing"):
            return True
        if not isinstance(meaning, str) or not meaning.strip():
            _fail(f"structure.{k}.meaning missing while state={state}")
            return False
        if "语义不可用" in meaning:
            _fail(f"structure.{k} meaning unresolved: {meaning}")
            return False
        return True

    for k in ("north_proxy_pressure", "trend_in_force", "amount"):
        if k in structure_keys:
            if _check_meaning(k):
                _ok(f"meaning mapped: {k}")
            else:
                return 1

    # --- report blocks builder importability ---
    try:
        rb = _load_yaml(args.report_blocks)
    except Exception as e:
        _fail(f"cannot load report_blocks.yaml: {e}")
        return 1

    builders = rb.get("builders") or []
    if not isinstance(builders, list):
        _fail("report_blocks.builders must be list")
        return 1

    bad: List[str] = []
    for b in builders:
        if not isinstance(b, dict):
            continue
        spec = b.get("builder")
        alias = b.get("alias")
        if not isinstance(spec, str) or not spec.strip():
            continue
        try:
            _import_by_spec(spec.strip())
        except Exception as e:
            bad.append(f"{alias}:{spec} ({type(e).__name__}: {e})")

    if bad:
        _fail("some block builders are not importable:\n  - " + "\n  - ".join(bad))
        return 1
    _ok("all report block builders importable")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
