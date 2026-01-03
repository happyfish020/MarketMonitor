from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from core.factors.factor_result import FactorResult

# ============================================================
# UnifiedRisk V12 · StructureFactsBuilder (YAML-driven)
#
# 目标：
# - 删除硬编码“因子白名单/映射逻辑”
# - 由 weights.yaml 的 factor_pipeline.structure_factors 决定“哪些因子进入 structure”
# - 由 config/structure_facts.yaml 决定 state/meaning/evidence/summary 的生成规则
#
# 设计原则（Frozen Engineering）：
# - append-only：不破坏既有 Report/Rules 的读取路径
# - fail-safe：缺 spec / 缺字段时，结构仍可生成（占位，不抛异常）
# - 语义集中：meaning/state/evidence 的解释口径统一收敛到 YAML
# ============================================================

# Modifier constants (report/gate may reference)
MOD_NONE = "none"
MOD_DISTRIBUTION_RISK = "distribution_risk"
MOD_SUCCESS_RATE_DECLINING = "success_rate_declining"
MOD_HIGH_EXECUTION_RISK = "high_execution_risk"


class StructureFactsBuilder:
    """Build structure facts from FactorResult dict using YAML spec.

    Output schema (per factor):
    {
      "<factor_key>": {
         "state": "<state>",
         "meaning": "<optional>",
         "evidence": { ... , "modifier": "<modifier>" }
      },
      "_summary": {"tags":[...]}
    }

    Notes:
    - Engine should pass `structure_keys` (from weights.yaml factor_pipeline.structure_factors).
    - Spec is a dict loaded from `config/structure_facts.yaml`.
    """

    def __init__(self, *, spec: Optional[Dict[str, Any]] = None) -> None:
        self.spec: Dict[str, Any] = spec if isinstance(spec, dict) else {}

    # ---------------------------
    # Public API
    # ---------------------------
    def build(
        self,
        *,
        factors: Dict[str, FactorResult],
        structure_keys: Optional[List[str]] = None,
        distribution_risk_active: bool = False,
        drs_signal: Optional[str] = None,  # GREEN / YELLOW / RED
    ) -> Dict[str, Dict[str, Any]]:
        modifier = self._resolve_modifier(
            distribution_risk_active=distribution_risk_active,
            drs_signal=drs_signal,
        )

        keys = [k for k in (structure_keys or []) if isinstance(k, str) and k.strip()]
        structure: Dict[str, Dict[str, Any]] = {}

        # factor specs
        factor_specs: Dict[str, Any] = {}
        if isinstance(self.spec.get("factors"), dict):
            factor_specs = self.spec["factors"]

        for key in keys:
            fspec = factor_specs.get(key, {}) if isinstance(factor_specs.get(key), dict) else {}
            fr = self._resolve_factor(factors=factors, key=key, fspec=fspec)
            if fr is None:
                # 缺失 ≠ 错误：以占位输出，方便 rule/report 发现缺口
                structure[key] = self._placeholder_fact(key=key, modifier=modifier)
                continue
            structure[key] = self._map_factor(key=key, fr=fr, fspec=fspec, modifier=modifier)

        structure["_summary"] = self._build_summary(structure=structure, modifier=modifier)
        return structure

    # ---------------------------
    # Modifier
    # ---------------------------
    def _resolve_modifier(
        self,
        *,
        distribution_risk_active: bool,
        drs_signal: Optional[str],
    ) -> str:
        """modifier 优先级（高 → 低）：
        1) distribution_risk
        2) drs_signal = RED
        3) drs_signal = YELLOW
        4) none
        """
        if distribution_risk_active:
            return MOD_DISTRIBUTION_RISK
        if drs_signal == "RED":
            return MOD_HIGH_EXECUTION_RISK
        if drs_signal == "YELLOW":
            return MOD_SUCCESS_RATE_DECLINING
        return MOD_NONE

    # ---------------------------
    # Factor resolution & mapping
    # ---------------------------
    def _resolve_factor(self, *, factors: Dict[str, FactorResult], key: str, fspec: Dict[str, Any]) -> Optional[FactorResult]:
        """Resolve FactorResult by key and optional aliases (spec.sources)."""
        if not isinstance(factors, dict):
            return None

        # primary
        fr = factors.get(key)
        if fr is not None:
            return fr

        # aliases
        sources = fspec.get("sources")
        if isinstance(sources, list):
            for alt in sources:
                if isinstance(alt, str) and alt in factors:
                    return factors.get(alt)
        return None

    def _map_factor(self, *, key: str, fr: FactorResult, fspec: Dict[str, Any], modifier: str) -> Dict[str, Any]:
        level, score, details = self._unpack_factor(fr)

        state = self._derive_state(key=key, level=level, details=details, fspec=fspec)
        meaning = self._derive_meaning(state=state, details=details, fspec=fspec)
        evidence = self._derive_evidence(details=details, fspec=fspec, modifier=modifier)

        out: Dict[str, Any] = {"state": state, "evidence": evidence}
        if isinstance(meaning, str) and meaning.strip():
            out["meaning"] = meaning
        # optional: expose normalized score/level if user wants
        expose = fspec.get("expose")
        if isinstance(expose, dict):
            if expose.get("level") is True and isinstance(level, str):
                out["level"] = level
            if expose.get("score") is True and isinstance(score, (int, float)):
                out["score"] = float(score)
        return out

    def _unpack_factor(self, fr: FactorResult) -> Tuple[Optional[str], Optional[float], Dict[str, Any]]:
        if isinstance(fr, dict):
            level = fr.get("level")
            score = fr.get("score")
            details = fr.get("details")
        else:
            level = getattr(fr, "level", None)
            score = getattr(fr, "score", None)
            details = getattr(fr, "details", None)

        lvl = str(level) if isinstance(level, str) else None
        try:
            sc = float(score) if isinstance(score, (int, float)) else None
        except Exception:
            sc = None
        det = details if isinstance(details, dict) else {}
        return lvl, sc, det

    def _derive_state(self, *, key: str, level: Optional[str], details: Dict[str, Any], fspec: Dict[str, Any]) -> Optional[str]:
        """Derive normalized state string for a structure fact.

        Priority:
        1) FactorResult.details.state (if present)
        2) state.level_map[level] (if configured)
        3) level.lower() as fallback
        4) state.default (if configured)
        Then: normalize + alias_map (if configured).
        """

        def _norm(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip()
            if not s:
                return None
            # common separators → underscore
            s = s.replace("/", "_")
            s = re.sub(r"[\s\-]+", "_", s)
            s = re.sub(r"_+", "_", s)
            return s.lower()

        state_spec = fspec.get("state") if isinstance(fspec.get("state"), dict) else {}
        normalize_flag = bool(state_spec.get("normalize", True))

        # 1) explicit state from details
        # (details is produced by _unpack_factor(fr) in _map_factor)
        details = details if isinstance(details, dict) else {}
        raw_state: Optional[str] = None
        if isinstance(details, dict):
            v = details.get("state")
            if isinstance(v, str) and v.strip():
                raw_state = v.strip()

        # 2) level_map
        if raw_state is None and isinstance(level, str) and level:
            lvl_map = state_spec.get("level_map") if isinstance(state_spec.get("level_map"), dict) else {}
            # allow various casing in keys
            raw_state = (
                lvl_map.get(level)
                or lvl_map.get(level.upper())
                or lvl_map.get(level.lower())
            )
            if raw_state is None:
                raw_state = level.lower()

        # 3) default
        if raw_state is None:
            v = state_spec.get("default")
            raw_state = v if isinstance(v, str) and v.strip() else None

        if raw_state is None:
            return None

        state = _norm(raw_state) if normalize_flag else str(raw_state)

        # alias_map (after normalization)
        alias_map = state_spec.get("alias_map") if isinstance(state_spec.get("alias_map"), dict) else {}
        if alias_map:
            if normalize_flag:
                amap: Dict[str, str] = {}
                for k, v in alias_map.items():
                    kn = _norm(k) or str(k).strip()
                    vn = _norm(v) if isinstance(v, str) else v
                    if isinstance(vn, str):
                        amap[kn] = vn
                state = amap.get(state, state)
                state = _norm(state) or state
            else:
                if state in alias_map and isinstance(alias_map[state], str):
                    state = alias_map[state]

        return state


    def _derive_meaning(self, *, state: str, details: Dict[str, Any], fspec: Dict[str, Any]) -> Optional[str]:
        meaning_spec = fspec.get("meaning") if isinstance(fspec.get("meaning"), dict) else {}

        # 1) from details key
        dkey = meaning_spec.get("details_key")
        if isinstance(dkey, str):
            v = details.get(dkey)
            if isinstance(v, str) and v.strip():
                return v.strip()

        # 2) by_state
        by_state = meaning_spec.get("by_state")
        if isinstance(by_state, dict):
            v = by_state.get(state)
            if isinstance(v, str) and v.strip():
                return v.strip()

        return None

    def _derive_evidence(self, *, details: Dict[str, Any], fspec: Dict[str, Any], modifier: str) -> Dict[str, Any]:
        ev_spec = fspec.get("evidence") if isinstance(fspec.get("evidence"), dict) else {}
        include = ev_spec.get("include_details_keys")
        rename = ev_spec.get("rename") if isinstance(ev_spec.get("rename"), dict) else {}

        evidence: Dict[str, Any] = {}

        if isinstance(include, list) and include:
            for k in include:
                if not isinstance(k, str):
                    continue
                if k in details:
                    outk = rename.get(k, k) if isinstance(rename.get(k), str) else k
                    evidence[outk] = self._safe_value(details.get(k))
        else:
            # default: include non-private keys (avoid huge blobs)
            for k, v in details.items():
                if not isinstance(k, str):
                    continue
                if k.startswith("_"):
                    continue
                outk = rename.get(k, k) if isinstance(rename.get(k), str) else k
                evidence[outk] = self._safe_value(v)

        extra = ev_spec.get("extra")
        if isinstance(extra, dict):
            for k, v in extra.items():
                if isinstance(k, str):
                    evidence[k] = self._safe_value(v)

        # always include modifier
        evidence["modifier"] = modifier
        return evidence

    def _safe_value(self, v: Any) -> Any:
        # keep numbers/bools as-is
        if isinstance(v, (int, float, bool)) or v is None:
            return v
        # short strings ok, long strings truncated
        if isinstance(v, str):
            s = v.strip()
            return s if len(s) <= 600 else s[:600] + "…"
        # lists/dicts: avoid deep huge nesting by simple truncation
        if isinstance(v, list):
            return v[:50]
        if isinstance(v, dict):
            # shallow copy first 50 keys
            out = {}
            for i, (k, vv) in enumerate(v.items()):
                if i >= 50:
                    break
                out[str(k)] = self._safe_value(vv)
            return out
        return str(v)

    def _placeholder_fact(self, *, key: str, modifier: str) -> Dict[str, Any]:
        return {
            "state": "missing",
            "meaning": f"结构项缺失：{key}（未计算或未注入 structure）。",
            "evidence": {"modifier": modifier},
        }

    # ---------------------------
    # Summary (YAML rules)
    # ---------------------------
    def _build_summary(self, *, structure: Dict[str, Dict[str, Any]], modifier: str) -> Dict[str, Any]:
        rules = []
        summary_spec = self.spec.get("summary") if isinstance(self.spec.get("summary"), dict) else {}
        if isinstance(summary_spec.get("rules"), list):
            rules = summary_spec["rules"]

        tags: List[str] = []

        # apply rules
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            cond = rule.get("when")
            add = rule.get("add")
            if self._match_when(cond=cond, structure=structure, modifier=modifier):
                if isinstance(add, str) and add:
                    tags.append(add)
                elif isinstance(add, list):
                    for t in add:
                        if isinstance(t, str) and t:
                            tags.append(t)

        # default modifier tag (if no summary rules configured)
        if not rules:
            if modifier == MOD_DISTRIBUTION_RISK:
                tags.append("modifier_distribution_risk")
            elif modifier == MOD_SUCCESS_RATE_DECLINING:
                tags.append("modifier_success_rate_declining")
            elif modifier == MOD_HIGH_EXECUTION_RISK:
                tags.append("modifier_high_execution_risk")

        # dedupe while preserving order
        seen = set()
        out_tags: List[str] = []
        for t in tags:
            if t in seen:
                continue
            seen.add(t)
            out_tags.append(t)

        return {"tags": out_tags}

    def _match_when(self, *, cond: Any, structure: Dict[str, Dict[str, Any]], modifier: str) -> bool:
        # allow empty cond => false
        if cond is None:
            return False

        # support {all:[...]} or {any:[...]} or single predicate
        if isinstance(cond, dict) and ("all" in cond or "any" in cond):
            if "all" in cond and isinstance(cond["all"], list):
                return all(self._match_when(cond=c, structure=structure, modifier=modifier) for c in cond["all"])
            if "any" in cond and isinstance(cond["any"], list):
                return any(self._match_when(cond=c, structure=structure, modifier=modifier) for c in cond["any"])
            return False

        if isinstance(cond, dict) and "path" in cond and "op" in cond:
            path = cond.get("path")
            op = cond.get("op")
            value = cond.get("value")
            if not isinstance(path, str) or not isinstance(op, str):
                return False
            cur = self._get_path(structure, path, modifier=modifier)
            return self._eval_op(cur, op, value)

        return False

    def _get_path(self, structure: Dict[str, Any], path: str, *, modifier: str) -> Any:
        # special pseudo path
        if path == "_meta.modifier":
            return modifier

        cur: Any = structure
        for part in path.split("."):
            if not part:
                continue
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        return cur

    def _eval_op(self, cur: Any, op: str, value: Any) -> bool:
        if op == "exists":
            return cur is not None
        if op == "not_exists":
            return cur is None
        if op == "==":
            return cur == value
        if op == "!=":
            return cur != value
        if op == "in":
            if isinstance(value, list):
                return cur in value
            return False
        # numeric compares
        try:
            if op in (">", ">=", "<", "<="):
                c = float(cur)
                v = float(value)
                if op == ">":
                    return c > v
                if op == ">=":
                    return c >= v
                if op == "<":
                    return c < v
                if op == "<=":
                    return c <= v
        except Exception:
            return False
        return False