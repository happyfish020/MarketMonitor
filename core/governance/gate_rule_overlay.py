# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · Gate Rule Overlay (UAT / Frozen Engineering)

Purpose
- Load Gate overlay rules from YAML (config/gate_rules.yaml) and apply on top of base_gate.
- Default policy: downgrade_only (only become more cautious).
- Produce audit-friendly hits (rule_id/reason/matched_paths).

Design principles (frozen):
- best-effort evaluation: missing path/op/type mismatch => warning, not crash
- no inference: only evaluate explicit conditions
- deterministic: priority desc, id asc
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import os

try:
    import yaml
except Exception as e:  # pragma: no cover
    yaml = None


@dataclass(frozen=True, slots=True)
class GateRuleHit:
    rule_id: str
    reason: str
    title: Optional[str]
    set_gate: str
    matched_paths: List[str]


@dataclass(frozen=True, slots=True)
class GateOverlayResult:
    mode: str
    raw_gate: str
    final_gate: str
    hits: List[Dict[str, Any]]
    warnings: List[str]
    spec_ref: Dict[str, Any]


def load_yaml_file(path: str, warnings: List[str]) -> Optional[Dict[str, Any]]:
    if yaml is None:
        warnings.append("yaml_not_available")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            warnings.append("invalid_yaml_root_type")
            return None
        return data
    except FileNotFoundError:
        warnings.append(f"missing_file:{path}")
        return None
    except Exception as e:
        warnings.append(f"yaml_load_error:{type(e).__name__}")
        return None


def _resolve_path(root: Dict[str, Any], path: str) -> Tuple[bool, Any]:
    """
    Resolve dotted path against root dict.
    Supports:
      - "structure.xxx.yyy"
      - "governance.gate.final_gate"
      - "trend_in_force.state" (top-level)
    """
    if not path or not isinstance(path, str):
        return False, None
    cur: Any = root
    for token in path.split("."):
        if not token:
            return False, None
        if isinstance(cur, dict) and token in cur:
            cur = cur[token]
        else:
            return False, None
    return True, cur


def _coerce_number(v: Any) -> Optional[float]:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip())
        except Exception:
            return None
    return None


def _eval_atom(root: Dict[str, Any], atom: Dict[str, Any], warnings: List[str]) -> Tuple[bool, List[str]]:
    """
    atom schema:
      path: "structure.xxx.state"
      op: exists|not_exists|==|!=|in|>|>=|<|<=
      value: ...
    """
    if not isinstance(atom, dict):
        warnings.append("invalid_atom_type")
        return False, []
    path = atom.get("path")
    op = atom.get("op")
    if not isinstance(path, str) or not isinstance(op, str):
        warnings.append("invalid_atom_fields")
        return False, []
    ok, v = _resolve_path(root, path)
    matched_paths: List[str] = []

    op = op.strip()
    if op == "exists":
        return (ok and v is not None), ([path] if ok and v is not None else [])
    if op == "not_exists":
        return (not ok) or (v is None), ([] if ok and v is not None else [path])

    if not ok:
        return False, []

    # comparisons
    if op in ("==", "!="):
        target = atom.get("value")
        res = (v == target)
        if op == "!=":
            res = not res
        return res, [path] if res else []

    if op == "in":
        target = atom.get("value")
        if not isinstance(target, list):
            warnings.append("invalid_in_value_type")
            return False, []
        res = v in target
        return res, [path] if res else []

    if op in (">", ">=", "<", "<="):
        left = _coerce_number(v)
        right = _coerce_number(atom.get("value"))
        if left is None or right is None:
            warnings.append("invalid_numeric_compare")
            return False, []
        if op == ">":
            res = left > right
        elif op == ">=":
            res = left >= right
        elif op == "<":
            res = left < right
        else:
            res = left <= right
        return res, [path] if res else []

    warnings.append(f"unsupported_op:{op}")
    return False, []


def _eval_expr(root: Dict[str, Any], expr: Dict[str, Any], warnings: List[str]) -> Tuple[bool, List[str]]:
    """
    expr schema:
      {"all": [atom|expr, ...]} or {"any": [atom|expr, ...]} or atom
    """
    if not isinstance(expr, dict):
        warnings.append("invalid_expr_type")
        return False, []

    if "all" in expr:
        items = expr.get("all")
        if not isinstance(items, list):
            warnings.append("invalid_all_type")
            return False, []
        matched: List[str] = []
        for it in items:
            ok, mp = _eval_expr(root, it, warnings) if isinstance(it, dict) and ("all" in it or "any" in it) else _eval_atom(root, it if isinstance(it, dict) else {}, warnings)
            if not ok:
                return False, []
            matched.extend(mp)
        return True, matched

    if "any" in expr:
        items = expr.get("any")
        if not isinstance(items, list):
            warnings.append("invalid_any_type")
            return False, []
        best_paths: List[str] = []
        for it in items:
            ok, mp = _eval_expr(root, it, warnings) if isinstance(it, dict) and ("all" in it or "any" in it) else _eval_atom(root, it if isinstance(it, dict) else {}, warnings)
            if ok:
                best_paths = mp
                return True, best_paths
        return False, []

    # treat as atom
    return _eval_atom(root, expr, warnings)


def _rank_map(order: List[str]) -> Dict[str, int]:
    return {g: i for i, g in enumerate(order)}


def apply_gate_overlay(
    *,
    raw_gate: str,
    slots: Dict[str, Any],
    spec: Dict[str, Any],
    default_mode: str = "downgrade_only",
) -> GateOverlayResult:
    warnings: List[str] = []

    gate_spec = spec.get("gate") if isinstance(spec, dict) else None
    if not isinstance(gate_spec, dict):
        return GateOverlayResult(
            mode="base_only",
            raw_gate=raw_gate,
            final_gate=raw_gate,
            hits=[],
            warnings=["missing:gate_spec"],
            spec_ref={},
        )

    mode = gate_spec.get("mode") if isinstance(gate_spec.get("mode"), str) else default_mode
    order = gate_spec.get("order")
    if not isinstance(order, list) or not all(isinstance(x, str) for x in order):
        # fallback: keep raw only
        warnings.append("invalid_gate_order")
        order = [raw_gate]

    rank = _rank_map(order)
    if raw_gate not in rank:
        warnings.append("raw_gate_not_in_order")
        # allow raw_gate rank as itself
        rank[raw_gate] = len(rank)

    rules = gate_spec.get("rules")
    if not isinstance(rules, list):
        warnings.append("invalid_rules_type")
        rules = []

    # deterministic order: priority desc, id asc
    def _prio(r: Any) -> Tuple[int, str]:
        if isinstance(r, dict):
            p = r.get("priority")
            pid = r.get("id")
            return (int(p) if isinstance(p, int) else 0, str(pid) if pid is not None else "")
        return (0, "")

    rules_sorted = sorted(rules, key=lambda r: (-_prio(r)[0], _prio(r)[1]))

    final_gate = raw_gate
    hits: List[Dict[str, Any]] = []

    for r in rules_sorted:
        if not isinstance(r, dict):
            warnings.append("invalid_rule_type")
            continue
        rid = r.get("id")
        if not isinstance(rid, str) or not rid.strip():
            warnings.append("invalid_rule_id")
            continue

        when = r.get("when")
        then = r.get("then")

        if not isinstance(when, dict) or not isinstance(then, dict):
            warnings.append(f"invalid_rule_schema:{rid}")
            continue

        ok, matched_paths = _eval_expr(slots, when, warnings)
        if not ok:
            continue

        set_gate = then.get("set_gate")
        reason = then.get("reason") or ""
        title = r.get("title")
        if not isinstance(set_gate, str) or not set_gate.strip():
            warnings.append(f"invalid_set_gate:{rid}")
            continue
        set_gate = set_gate.strip().upper()

        # decide apply
        applied = False
        if mode == "downgrade_only":
            if set_gate in rank and final_gate in rank and rank[set_gate] >= rank[final_gate]:
                final_gate = set_gate
                applied = True
        else:
            # allow any override if in order
            if set_gate in rank:
                final_gate = set_gate
                applied = True

        hits.append(
            {
                "rule_id": rid,
                "title": title if isinstance(title, str) else None,
                "reason": str(reason),
                "set_gate": set_gate,
                "matched_paths": matched_paths,
                "applied": applied,
            }
        )

    # spec ref
    meta = spec.get("meta") if isinstance(spec.get("meta"), dict) else {}
    spec_ref = {
        "spec": meta.get("spec"),
        "version": meta.get("version"),
        "updated_at": meta.get("updated_at"),
    }

    return GateOverlayResult(
        mode=mode,
        raw_gate=raw_gate,
        final_gate=final_gate,
        hits=hits,
        warnings=warnings,
        spec_ref=spec_ref,
    )
