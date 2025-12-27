# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.StructureFacts")


class StructureFactsBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Structure Facts Block（语义一致性冻结版）
    """

    block_alias = "structure.facts"
    title = "结构事实（技术轨）"

    _EVIDENCE_KEYS = (
        "trend",
        "signal",
        "score",
        "adv_ratio",
        "new_low_ratio",
        "count_new_low",
        "turnover_total",
        "turnover_chg",
        "north_net",
        "north_trend_5d",
    )

    # 明确禁止的进攻性词汇
    _FORBIDDEN_PHRASES = (
        "动能改善",
        "结构偏强",
        "成交活跃",
        "资金参与度较高",
        "趋势向上",
    )

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
        structure = context.slots.get("structure")

        if not isinstance(structure, dict) or not structure:
            warnings.append("structure_missing_or_invalid")
            payload = (
                "- 结构事实：未提供或格式非法\n"
                "  含义：该区块仅用于占位，不影响 Gate / ActionHint\n"
            )
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        lines: List[str] = []
        lines.append("- 结构事实：")

        keys = list(structure.keys())
        keys_sorted = sorted([k for k in keys if k != "_summary"]) + (
            ["_summary"] if "_summary" in structure else []
        )

        for key in keys_sorted:
            item = structure.get(key)
            if not isinstance(item, dict):
                continue

            state = item.get("state") or item.get("status") or "unknown"
            meaning = item.get("meaning") or item.get("reason") or ""

            # 语义去进攻
            for p in self._FORBIDDEN_PHRASES:
                if p in meaning:
                    warnings.append(f"semantic_sanitized:{p}")
                    meaning = meaning.replace(p, "")

            # evidence 白名单
            ev: Dict[str, Any] = {
                k: item[k] for k in self._EVIDENCE_KEYS
                if k in item and item.get(k) is not None
            }

            if key == "_summary":
                lines.append(
                    "  - 总述：结构未坏，但扩散不足，结构同步性与成功率下降。"
                )
                continue

            lines.append(f"  - {key}:")
            lines.append(f"      状态：{state}")

            if meaning:
                lines.append(f"      含义：{meaning}")

            # 关键证据（只展示事实）
            if ev:
                lines.append("      关键证据：")
                for ek, evv in ev.items():
                    lines.append(f"        - {ek}: {self._fmt_value(evv)}")

        lines.append("")
        lines.append(
            "说明：以上为已冻结的结构事实，仅用于解释当前制度背景，"
            "不构成预测、进攻信号或任何形式的操作建议。"
        )

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines).strip(),
            warnings=warnings,
        )

    @staticmethod
    def _fmt_value(v: Any) -> str:
        if isinstance(v, float):
            return f"{v:.4f}"
        if isinstance(v, (list, tuple)):
            return f"[{', '.join(map(str, v[:6]))}{'...' if len(v) > 6 else ''}]"
        if isinstance(v, dict):
            keys = list(v.keys())
            short = {k: v[k] for k in keys[:6]}
            return f"{short}{'...' if len(keys) > 6 else ''}"
        return str(v)
