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
        mode = self._as_str(gate.get("mode")) or "base_only"

        hits = gate.get("hits")
        hits = hits if isinstance(hits, list) else []
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
    
            # hits
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
            else:
                lines.append("命中规则（hits）：无")
    
            if gw:
                lines.append("")
                lines.append("Gate warnings：")
                for w in gw[:10]:
                    try:
                        lines.append(f"- {str(w)}")
                    except Exception:
                        lines.append("- (invalid warning)")
    
            lines.append("")
            lines.append("制度说明：Gate 表示制度允许的行动边界；是否执行仍需服从 Execution/DRS 的治理层约束。")
    
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload="\n".join(lines),
                warnings=warnings,
            )
    
    
    @staticmethod
    def _explain_mode(mode: Any) -> Optional[str]:
        """
        人话解释（冻结版）：
        - base_only：没有启用覆写层，只按“基础 Gate”走
        - downgrade_only：覆写层只能让你更保守（只踩刹车），不会把门禁放松
        - override：覆写层既能更保守也能放松（可踩油门），仅建议成熟后启用
        """
        if not isinstance(mode, str) or not mode.strip():
            return None
        m = mode.strip()
        if m == "base_only":
            return "只用基础门禁（覆写层未启用/未生效）"
        if m == "downgrade_only":
            return "覆写层只会更谨慎（只踩刹车，不会放行）"
        if m == "override":
            return "覆写层可放松也可收紧（可踩油门，需成熟后启用）"
        return None
    @staticmethod
    def _extract_gate(context: ReportContext, warnings: List[str]) -> Dict[str, Any]:
        gov = context.slots.get("governance")
        if not isinstance(gov, dict):
            return {}
        gate = gov.get("gate")
        if gate is None:
            return {}
        if isinstance(gate, dict):
            return gate
        warnings.append("invalid:governance_gate_type")
        return {}

    @staticmethod
    def _as_str(v: Any) -> Optional[str]:
        if v is None:
            return None
        try:
            s = str(v).strip()
        except Exception:
            return None
        return s or None

    @staticmethod
    def _get_in(d: Dict[str, Any], path: List[str]) -> Any:
        cur: Any = d
        for p in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(p)
        return cur

    def _load_gate_rules_spec(self, context: ReportContext, gate: Dict[str, Any], warnings: List[str]) -> Optional[Dict[str, Any]]:
        # Determine path priority:
        # 1) gate["rule_spec_path"] / gate["spec_path"]
        # 2) gate["spec_ref"]["path"]
        # 3) context.slots["config"]["gate_rules_path"]
        # 4) default "config/gate_rules.yaml"
        path = None
        for k in ("rule_spec_path", "spec_path"):
            if isinstance(gate.get(k), str):
                path = gate.get(k)
                break
        if path is None:
            spec_ref = gate.get("spec_ref")
            if isinstance(spec_ref, dict) and isinstance(spec_ref.get("path"), str):
                path = spec_ref.get("path")
        if path is None:
            cfg = context.slots.get("config")
            if isinstance(cfg, dict) and isinstance(cfg.get("gate_rules_path"), str):
                path = cfg.get("gate_rules_path")
        if path is None:
            path = "config/gate_rules.yaml"

        path = str(path)
        if yaml is None:
            return None

        resolved = path
        if not os.path.isabs(path) and not os.path.exists(path):
            alt = os.path.join(os.getcwd(), path)
            if os.path.exists(alt):
                resolved = alt

        try:
            with open(resolved, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                warnings.append("invalid:gate_rules_yaml_root")
                return None
            return data
        except FileNotFoundError:
            warnings.append(f"missing:gate_rules_yaml:{path}")
            return None
        except Exception:
            warnings.append("error:gate_rules_yaml_load")
            return None
