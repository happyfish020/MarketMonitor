# -*- coding: utf-8 -*-
"""UnifiedRisk V12 FULL
Gate Rule Engine (MVP, frozen)

目标：
- 用 RuleSpec (YAML/JSON) 驱动 GateDecider 的“降级治理”
- MVP 支持：eq / in / exists / not / all / any
- 永不抛异常：规则执行失败仅记录 warning，并继续

注意：
- 本引擎只负责 Gate level/reasons/evidence 的生成
- forbid_actions 仅作为 evidence 输出（ActionHint 层是否使用由上层决定）
"""

from __future__ import annotations

import glob
import os
from typing import Any, Dict, List, Optional, Tuple

from core.utils.logger import get_logger
from core.regime.rule_engine.rule_loader import load_rule_file

LOG = get_logger("GateRuleEngine")

_ALLOWED = ("NORMAL", "CAUTION", "PLANB", "FREEZE")
_ORDER = {"NORMAL": 0, "CAUTION": 1, "PLANB": 2, "FREEZE": 3}
_PRIORITY_ORDER = {"L3": 0, "L2": 1, "L1": 2}


def _max_gate(a: str, b: str) -> str:
    if a not in _ORDER or b not in _ORDER:
        return a
    return a if _ORDER[a] >= _ORDER[b] else b


class GateRuleEngine:
    """Load and apply Gate governance rules."""

    def __init__(self, rule_globs: Optional[List[str]] = None) -> None:
        self.rule_globs = rule_globs or []
        self.rules: List[Dict[str, Any]] = []
        self.load_errors: List[str] = []

    def load(self) -> None:
        """Load all rules from globs."""
        self.rules = []
        self.load_errors = []

        paths: List[str] = []
        for g in self.rule_globs:
            paths.extend(glob.glob(g))
        paths = sorted(set(paths))

        for p in paths:
            spec, err = load_rule_file(p)
            if err or spec is None:
                self.load_errors.append(f"{p}::{err}")
                continue
            spec["_path"] = p
            self.rules.append(spec)

        # sort by priority first, then by filename
        def _key(s: Dict[str, Any]) -> Tuple[int, str]:
            pr = str(s.get("priority", "L2")).upper()
            return (_PRIORITY_ORDER.get(pr, 9), str(s.get("_path", "")))

        self.rules.sort(key=_key)

    def apply(self, ctx: Dict[str, Any], initial_gate: str = "NORMAL") -> Tuple[str, List[str], Dict[str, Any]]:
        """Apply loaded rules to context. Returns (gate, reasons, evidence)."""
        gate = initial_gate if initial_gate in _ALLOWED else "NORMAL"
        reasons: List[str] = []
        evidence: Dict[str, Any] = {"rules": []}

        for spec in self.rules:
            try:
                fired, gate, reasons, evidence, stop = self._apply_one(spec, ctx, gate, reasons, evidence)
                if fired and stop:
                    break
            except Exception as e:
                LOG.warning("[GateRuleEngine] rule exec error: %s | rule=%s", e, spec.get("rule_id"))
                continue

        return gate, reasons, evidence

    # ---------------- internal ----------------

    def _apply_one(
        self,
        spec: Dict[str, Any],
        ctx: Dict[str, Any],
        gate: str,
        reasons: List[str],
        evidence: Dict[str, Any],
    ) -> Tuple[bool, str, List[str], Dict[str, Any], bool]:
        rule_id = str(spec.get("rule_id", "UNKNOWN"))
        stop = False
        fired_any = False

        # Support two schemas:
        # (A) {preconditions:[{if,then,stop}], rules:[{if,then}]}  (doc spec)
        # (B) {when, then}                                        (MVP)
        blocks: List[Dict[str, Any]] = []

        pre = spec.get("preconditions")
        if isinstance(pre, list):
            blocks.extend(pre)
        rs = spec.get("rules")
        if isinstance(rs, list):
            blocks.extend(rs)
        if "when" in spec and "then" in spec:
            blocks.append({"name": rule_id, "if": spec.get("when"), "then": spec.get("then")})

        for blk in blocks:
            cond = blk.get("if")
            then = blk.get("then")
            if not isinstance(then, dict):
                continue

            if cond is None or self._eval(cond, ctx):
                fired_any = True

                # actions
                if "set_gate" in then:
                    g = str(then.get("set_gate"))
                    if g in _ALLOWED:
                        gate = _max_gate(gate, g)

                if "downgrade_gate_by" in then:
                    try:
                        step = int(then.get("downgrade_gate_by"))
                    except Exception:
                        step = 0
                    gate = self._downgrade(gate, step)

                # reasons
                add_reasons = then.get("add_reasons") or then.get("reasons")
                if isinstance(add_reasons, list):
                    for r in add_reasons:
                        if isinstance(r, str) and r:
                            reasons.append(r)
                elif isinstance(then.get("reason"), str):
                    reasons.append(str(then.get("reason")))

                # forbid actions (audit only)
                forbid = then.get("forbid_actions")
                if isinstance(forbid, list) and forbid:
                    evidence.setdefault("forbid_actions", [])
                    if isinstance(evidence["forbid_actions"], list):
                        evidence["forbid_actions"].extend([x for x in forbid if isinstance(x, str)])

                # evidence tag
                ev = then.get("add_evidence")
                if isinstance(ev, dict):
                    evidence.setdefault("rules", [])
                    evidence["rules"].append({"rule_id": rule_id, **ev})
                else:
                    evidence.setdefault("rules", [])
                    evidence["rules"].append({"rule_id": rule_id})

                if blk.get("stop") is True or then.get("stop") is True:
                    stop = True
                    break

        return fired_any, gate, reasons, evidence, stop

    def _downgrade(self, gate: str, step: int) -> str:
        if gate not in _ORDER or step <= 0:
            return gate
        idx = _ORDER[gate] + step
        idx = min(idx, _ORDER["FREEZE"])
        for k, v in _ORDER.items():
            if v == idx:
                return k
        return gate

    def _eval(self, expr: Any, ctx: Dict[str, Any]) -> bool:
        """Evaluate a condition expression."""
        if expr is None:
            return True

        if isinstance(expr, dict):
            if "all" in expr and isinstance(expr["all"], list):
                return all(self._eval(x, ctx) for x in expr["all"])
            if "any" in expr and isinstance(expr["any"], list):
                return any(self._eval(x, ctx) for x in expr["any"])
            if "not" in expr:
                return not self._eval(expr["not"], ctx)

            if "eq" in expr and isinstance(expr["eq"], list) and len(expr["eq"]) == 2:
                a, b = expr["eq"]
                return self._get(ctx, a) == b

            if "in" in expr and isinstance(expr["in"], list) and len(expr["in"]) == 2:
                a, arr = expr["in"]
                arr = arr or []
                return self._get(ctx, a) in arr

            if "exists" in expr:
                k = expr["exists"]
                return self._get(ctx, k) is not None

            # shorthand: {"breadth.level": "LOW"}
            if len(expr) == 1:
                k, v = next(iter(expr.items()))
                return self._get(ctx, k) == v

        return False

    def _get(self, ctx: Dict[str, Any], key: Any) -> Any:
        if not isinstance(key, str) or not key:
            return None
        return ctx.get(key)
