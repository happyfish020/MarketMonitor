# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Structure Facts block (Frozen)

职责：
- 只读 context.slots["structure"]（由 StructureFactsBuilder 统一构建）
- 不做二次语义映射/硬编码（语义与证据筛选由 config/structure_facts.yaml 决定）
- slot 缺失 ≠ 错误：返回 warning + 占位 payload
- 永不抛异常、不返回 None
"""

from __future__ import annotations

from typing import Dict, Any, List, Optional

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger


log = get_logger(__name__)


class StructureFactsBlock(ReportBlockRendererBase):
    block_alias = "structure.facts"
    title = "结构事实（Structure Facts · 技术轨）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        try:
            structure = context.slots.get("structure")
            if not isinstance(structure, dict) or not structure:
                warnings.append("empty:structure")
                return ReportBlock(
                    block_alias=self.block_alias,
                    title=self.title,
                    warnings=warnings,
                    payload={
                        "content": [
                            "（结构事实为空或未接入）",
                        ],
                        #"note": "注：结构事实为只读解释层，不构成预测或操作建议。",
                    },
                )

            lines: List[str] = []
            lines.append("- 结构事实：")

            for key, item in structure.items():
                # Reserve meta keys (e.g. _summary) for internal tagging; do not render as a factor.
                if isinstance(key, str) and key.startswith("_"):
                    continue
                if not isinstance(item, dict):
                    warnings.append(f"invalid:structure_item:{key}")
                    continue

                state = item.get("state")
                state_str = str(state) if state is not None else "missing"

                meaning = item.get("meaning")
                if not isinstance(meaning, str) or not meaning.strip():
                    warnings.append(f"missing_semantics:{key}")
                    meaning_str = "（语义缺失：请在 config/structure_facts.yaml 补齐 factors.%s.meaning.by_state）" % key
                else:
                    meaning_str = meaning.strip()

                lines.append(f"  - {key}:")
                lines.append(f"      状态：{state_str}")
                lines.append(f"      含义：{meaning_str}")

                evidence = item.get("evidence")
                if isinstance(evidence, dict) and evidence:
                    lines.append("      关键证据：")
                    for ek, ev in evidence.items():
                        # 证据字段应已在 Builder 层完成筛选；此处只做轻量防御
                        if ek is None:
                            continue
                        if isinstance(ek, str) and ek.startswith("_"):
                            continue
                        # Unit fix: if adv_ratio is a ratio (0~1), show both ratio and percent
                        if ek == "adv_ratio" and isinstance(ev, (int, float)) and 0 <= float(ev) <= 1.0:
                            pct = round(float(ev) * 100.0, 2)
                            lines.append(f"        - {ek}: {float(ev):.4f} ({pct:.2f}%)")
                        else:
                            lines.append(f"        - {ek}: {ev}")
                else:
                    lines.append("      关键证据：")
                    lines.append("        - (none)")

            # optional: add compact summary lines for readability (no new judgment)
            lines.extend(self._render_readonly_summary(structure))

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": lines,
                    #"note": "注：以上为 Phase-2 冻结后的结构性事实，仅用于解释 Gate / ActionHint 背景，不构成新的判断、预测或操作建议。",
                },
            )

        except Exception as e:
            log.exception("StructureFactsBlock.render failed: %s", e)
            warnings.append("exception:structure_facts_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": [
                        "结构事实渲染异常（已捕获）。",
                    ],
                    "note": "注：异常已记录日志；本 block 不影响其它 block 生成。",
                },
            )

    def _render_readonly_summary(self, structure: Dict[str, Any]) -> List[str]:
        """Generate a short, read-only readability aid.

        仅基于已存在的结构事实字段拼接，不引入新指标/新判断。
        """
        try:
            # Prefer existing 'summary' / 'feeling' if builder ever provides them in the future.
            if isinstance(structure.get("summary"), str):
                return ["", f"- 总述：{structure.get('summary')}"]
            if isinstance(structure.get("feeling"), str):
                return ["", f"- 体感：{structure.get('feeling')}"]
        except Exception:
            return []

        # Heuristic summary based on a few common keys (still read-only)
        idx = structure.get("index_tech", {}) if isinstance(structure.get("index_tech"), dict) else {}
        trend = structure.get("trend_in_force", {}) if isinstance(structure.get("trend_in_force"), dict) else {}
        fr = structure.get("failure_rate", {}) if isinstance(structure.get("failure_rate"), dict) else {}
        npp = structure.get("north_proxy_pressure", {}) if isinstance(structure.get("north_proxy_pressure"), dict) else {}

        parts: List[str] = []
        if idx.get("state") in ("strong", "neutral") and trend.get("state") in ("in_force", "neutral") and fr.get("state") in ("stable", "neutral"):
            parts.append("结构未见系统性破坏")
        if npp.get("state") in ("pressure_high", "pressure_mid"):
            parts.append("北向代理压力偏高")
        elif npp.get("state") in ("pressure_low",):
            parts.append("北向代理压力不高")

        if not parts:
            return []

        return [
            "",
            "- 总述：" + "，".join(parts) + "。",
        ]
