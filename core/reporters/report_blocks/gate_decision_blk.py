# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

import os

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

try:
    import yaml
except Exception:
    yaml = None


class GateDecisionBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Gate Decision Block（冻结版 · governance.gate 单点显示）

    Changes (v2):
    - If gate_rules_path is available, load semantics/title/description to render human-readable explanations.
    - Still best-effort; never crash report generation.
    """

    block_alias = "governance.gate"
    title = "制度门禁（Gate · Decision）"

    @staticmethod
    def _normalize_mode(mode: Optional[str]) -> str:
        """Normalize gate decision mode for reporting.

        Engine / configs may emit variants like 'BASE', 'BASE_ONLY', 'downgrade', etc.
        Reporting must be tolerant and should avoid emitting noisy 'unknown:gate_mode' warnings
        for benign aliases.
        """
        m = (str(mode).strip().lower() if mode is not None else "")
        if not m:
            return "base_only"
        alias = {
            "base": "base_only",
            "baseonly": "base_only",
            "base_only": "base_only",
            "base-only": "base_only",
            "only_base": "base_only",
            "downgrade": "downgrade_only",
            "downgradeonly": "downgrade_only",
            "downgrade_only": "downgrade_only",
            "downgrade-only": "downgrade_only",
            "override": "override",
            "override_mode": "override",
            "full_override": "override",
        }
        return alias.get(m, m)

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        gate = self._extract_gate(context, warnings)
        if not gate:
            warnings.append("missing:governance_gate")
            payload = "未生成 Gate 决策数据（请检查 GateDecision 写入 slots 的位置）。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        raw_gate = self._as_str(gate.get("raw_gate") or gate.get("raw") or gate.get("base_gate"))
        final_gate = self._as_str(gate.get("final_gate") or gate.get("final") or gate.get("gate"))
        mode_raw = self._as_str(gate.get("mode") or gate.get("gate_mode") or gate.get("policy_mode"))
        mode = self._normalize_mode(mode_raw) or "base_only"

        hits = gate.get("hits")
        hits = hits if isinstance(hits, list) else []

        # Optional: reasons/causes (engine may not provide rule hits during UAT)
        reasons = gate.get("reasons")
        if reasons is None:
            reasons = gate.get("causes")
        if reasons is None:
            reasons = gate.get("reason")
        reasons = reasons if isinstance(reasons, list) else []

        gw = gate.get("warnings")
        gw = gw if isinstance(gw, list) else []

        if not final_gate:
            warnings.append("missing:final_gate")
            payload = "Gate 决策字段缺失（final_gate 为空）。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        # Load semantics from gate rules YAML (optional)
        spec = self._load_gate_rules_spec(context, gate, warnings)
        meaning = None
        rule_lookup: Dict[str, Dict[str, Any]] = {}
        overlay_note = None

        if isinstance(spec, dict):
            meaning = self._get_in(spec, ["semantics", "gate_meaning", final_gate])
            overlay_note = self._get_in(spec, ["semantics", "overlay_policy", "explanation"])
            rules = self._get_in(spec, ["gate", "rules"])
            if isinstance(rules, list):
                for r in rules:
                    if isinstance(r, dict) and isinstance(r.get("id"), str):
                        rule_lookup[r["id"]] = r

        lines: List[str] = []
        lines.append(f"Gate（最终）：{final_gate}")
        if raw_gate:
            lines.append(f"Gate（基础）：{raw_gate}")

        # If AttackPermit=YES, avoid blanket meaning that contradicts overlay permission.
        ap = None
        gov = context.slots.get("governance")
        if isinstance(gov, dict):
            ap = gov.get("attack_permit")
        ap_yes = isinstance(ap, dict) and str(ap.get("permit") or "").upper() == "YES"
        if ap_yes and isinstance(meaning, str):
            if "禁止加仓" in meaning or "只能防守" in meaning:
                meaning = "默认禁止追涨式扩大风险敞口；但在 AttackPermit 覆盖下允许 BASE_ETF_ADD / PULLBACK_ADD(回撤确认)（小步/分批/不追价）。"

        if meaning:
            lines.append(f"含义：{meaning}")

        mode_explain = self._explain_mode(mode)
        if mode_explain:
            lines.append(f"规则模式：{mode}（{mode_explain}）")
        else:
            lines.append(f"规则模式：{mode}")

        # 人话速记：只在已知模式下显示（避免噪音）
        if isinstance(mode, str) and mode.strip() in {"base_only", "downgrade_only", "override"}:
            lines.append("模式速记：base_only=仅按基础门禁；downgrade_only=只会更谨慎（只踩刹车）；override=可放松也可收紧（可踩油门，需成熟后启用）")

        if overlay_note:
            lines.append("")
            lines.append("Overlay 说明：")
            lines.append(str(overlay_note).strip())

        # AttackPermit overlay hint (read-only, does NOT relax Gate)
        ap = None
        gov = context.slots.get("governance")
        if isinstance(gov, dict):
            ap = gov.get("attack_permit")
        if isinstance(ap, dict) and str(ap.get("permit") or "").upper() == "YES":
            ap_label = self._as_str(ap.get("label")) or "YES"
            aa = ap.get("allowed")
            aa = aa if isinstance(aa, list) else []
            aa_s = [str(x) for x in aa if str(x) in {"BASE_ETF_ADD", "PULLBACK_ADD", "SATELLITE_ADD"}]
            cons = ap.get("constraints")
            cons = cons if isinstance(cons, list) else []
            cons_txt = self._as_str(cons[0]) if cons else None
            lines.append(
                f"覆盖提示：AttackPermit={ap_label}；允许 {(' / '.join(aa_s) if aa_s else 'BASE_ETF_ADD / PULLBACK_ADD')}（不追涨，小步分批）。"
                + (f" 约束：{cons_txt}" if cons_txt else "")
            )

        # hits / reasons
        lines.append("")
        if hits:
            lines.append("命中规则（hits）：")
            for h in hits[:12]:
                if not isinstance(h, dict):
                    lines.append("- (invalid hit format)")
                    continue
                rid = self._as_str(h.get("rule_id") or h.get("id")) or "unknown_rule"
                reason = self._as_str(h.get("reason")) or ""
                applied = h.get("applied")
                applied_txt = "✅" if applied is True else ("⛔" if applied is False else "")
                matched_paths = h.get("matched_paths")
                matched_paths = matched_paths if isinstance(matched_paths, list) else []
                # lookup title/desc
                title = None
                desc = None
                rr = rule_lookup.get(rid)
                if rr:
                    title = self._as_str(rr.get("title"))
                    desc = self._as_str(rr.get("description"))
                head = f"- {applied_txt} {rid}"
                if title:
                    head += f" · {title}"
                if reason:
                    head += f" — {reason}"
                lines.append(head)
                if matched_paths:
                    lines.append(f"  - matched: {', '.join(matched_paths[:4])}")
                if desc:
                    lines.append(f"  - note: {desc}")
            if len(hits) > 12:
                lines.append(f"- ...（共 {len(hits)} 条，报告仅展示前 12 条）")
        elif reasons:
            warnings.append("missing:gate_hits")
            lines.append("命中规则（hits）：缺失（使用 reasons/causes 作为解释）")
            lines.append("原因（reasons/causes）：")
            for r in reasons[:12]:
                try:
                    lines.append(f"- {str(r)}")
                except Exception:
                    lines.append("- (invalid reason)")
            if len(reasons) > 12:
                lines.append(f"- ...（共 {len(reasons)} 条，报告仅展示前 12 条）")
        else:
            warnings.append("missing:gate_hits")
            inferred = self._infer_gate_causes(context=context, final_gate=final_gate)
            if inferred:
                warnings.append("inferred:gate_causes")
                lines.append("命中规则（hits）：缺失（未提供 rule hits；以下为推断线索，仅用于解释/审计）")
                lines.append("推断线索（inferred causes）：")
                for it in inferred[:12]:
                    lines.append(f"- {it}")
                if len(inferred) > 12:
                    lines.append(f"- ...（共 {len(inferred)} 条，报告仅展示前 12 条）")
            else:
                lines.append("命中规则（hits）：无（且未提供 reasons/causes）")

        if gw:
            lines.append("")
            lines.append("Gate 警告（warnings）：")
            for w in gw[:8]:
                lines.append(f"- {str(w)}")

        payload = "\n".join(lines).strip()
        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

    # ---------------- helpers (unchanged) ----------------

    def _extract_gate(self, context: ReportContext, warnings: List[str]) -> Optional[Dict[str, Any]]:
        gov = context.slots.get("governance")
        if not isinstance(gov, dict):
            return None
        gate = gov.get("gate")
        return gate if isinstance(gate, dict) else None

    @staticmethod
    def _as_str(x: Any) -> str:
        try:
            return "" if x is None else str(x)
        except Exception:
            return ""

    @staticmethod
    def _get_in(d: Dict[str, Any], path: List[str]) -> Any:
        cur: Any = d
        for p in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
        return cur

    def _load_gate_rules_spec(self, context: ReportContext, gate: Dict[str, Any], warnings: List[str]) -> Any:
        # gate_rules_path priority: gate payload -> context.config -> env
        path = gate.get("gate_rules_path")
        if not path:
            try:
                path = context.config.get("gate_rules_path")  # type: ignore[attr-defined]
            except Exception:
                path = None
        if not path:
            path = os.environ.get("UR_GATE_RULES_PATH")

        if not path:
            warnings.append("missing:gate_rules_path")
            return None

        if not yaml:
            warnings.append("missing:pyyaml")
            return None

        if not os.path.exists(path):
            warnings.append("missing:gate_rules_file")
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception:
            warnings.append("error:gate_rules_load_failed")
            return None

    @staticmethod
    def _explain_mode(mode: str) -> Optional[str]:
        if not mode:
            return None
        m = str(mode).strip().lower()
        if m == "base_only":
            return "只按基础门禁输出（不做覆盖/放松）"
        if m == "downgrade_only":
            return "只允许更谨慎（只踩刹车，不踩油门）"
        if m == "override":
            return "允许覆盖（可放松也可收紧；需成熟后启用）"
        return None

    @staticmethod
    def _infer_gate_causes(context: ReportContext, final_gate: str) -> List[str]:
        inferred: List[str] = []
        snap = context.slots.get("snapshot")
        if isinstance(snap, dict):
            mo = snap.get("market_overview")
            if isinstance(mo, dict):
                risk = mo.get("risk")
                if risk:
                    inferred.append(f"market_overview.risk={risk}")
        return inferred
