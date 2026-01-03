# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · UAT-P0

结构语义覆盖检查：
- 对照 config/weights.yaml 中 factor_pipeline.structure_factors
- 检查 config/structure_facts.yaml 是否存在相应条目
- 检查 meaning.by_state 是否缺失（至少要覆盖 alias 后的 state 集合）

运行：
- python core/uat/uat_structure_facts_coverage_check.py

输出：
- JSON：missing_facts / missing_meaning_states / warnings

说明：这是一个“配置审计脚本”，用于冻结收口，不会改动任何文件。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


def _repo_root() -> Path:
    # core/uat -> core -> repo
    return Path(__file__).resolve().parents[2]


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML not installed. Please 'pip install pyyaml'.") from e

    if not path.exists():
        raise FileNotFoundError(str(path))

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise TypeError(f"YAML root must be a dict: {path}")
    return data


def _dig(obj: Any, path: List[str]) -> Any:
    cur = obj
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def _extract_structure_factors(weights: Dict[str, Any]) -> List[str]:
    v = _dig(weights, ["factor_pipeline", "structure_factors"])
    if isinstance(v, list):
        return [str(x) for x in v]

    # 兼容另一种写法：factor_pipeline: {structure_factors: {k: ...}}
    if isinstance(v, dict):
        return [str(k) for k in v.keys()]

    return []


def _extract_facts_map(structure_facts_yaml: Dict[str, Any]) -> Dict[str, Any]:
    # 兼容几种常见布局：
    # 1) {facts: {...}}
    # 2) {structure_facts: {facts: {...}}}
    # 3) 直接就是 {...}
    sf = structure_facts_yaml.get("structure_facts")
    if isinstance(sf, dict):
        structure_facts_yaml = sf

    facts = structure_facts_yaml.get("facts")
    if isinstance(facts, dict):
        return facts

    # fallback：把除 meta 字段以外都当 facts
    if isinstance(structure_facts_yaml, dict):
        meta_keys = {"normalize", "version", "meta", "defaults"}
        return {k: v for k, v in structure_facts_yaml.items() if k not in meta_keys}

    return {}


def _meaning_states(fspec: Dict[str, Any]) -> Set[str]:
    meaning = fspec.get("meaning") if isinstance(fspec, dict) else None
    if isinstance(meaning, dict):
        by_state = meaning.get("by_state")
        if isinstance(by_state, dict):
            return {str(k) for k in by_state.keys()}
    return set()


def _alias_values(fspec: Dict[str, Any]) -> Set[str]:
    alias = fspec.get("alias_map") if isinstance(fspec, dict) else None
    if isinstance(alias, dict):
        return {str(v) for v in alias.values()}
    return set()


def main() -> int:
    root = _repo_root()
    w_path = root / "config" / "weights.yaml"
    sf_path = root / "config" / "structure_facts.yaml"

    weights = _load_yaml(w_path)
    sf = _load_yaml(sf_path)

    structure_keys = _extract_structure_factors(weights)
    facts_map = _extract_facts_map(sf)

    missing_facts = [k for k in structure_keys if k not in facts_map]

    missing_meaning_states: Dict[str, List[str]] = {}
    warnings: List[str] = []

    for k in structure_keys:
        fspec = facts_map.get(k)
        if not isinstance(fspec, dict):
            continue

        meaning_states = _meaning_states(fspec)
        alias_values = _alias_values(fspec)

        # 最小要求：alias_map 的 value 必须能在 meaning.by_state 命中
        if alias_values:
            miss = sorted([s for s in alias_values if s not in meaning_states])
            if miss:
                missing_meaning_states[k] = miss

        if not meaning_states:
            warnings.append(f"empty:meaning.by_state:{k}")

    out = {
        "weights_structure_factors": structure_keys,
        "missing_facts": missing_facts,
        "missing_meaning_states": missing_meaning_states,
        "warnings": warnings,
        "paths": {"weights": str(w_path), "structure_facts": str(sf_path)},
    }

    print(json.dumps(out, ensure_ascii=False, indent=2))

    # 非 0 退出码用于 CI/UAT
    if missing_facts or missing_meaning_states:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
