from __future__ import annotations

import logging
from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.StructureFacts")


class StructureFactsBlock(ReportBlockRendererBase):
    """
    结构事实（技术轨）

    冻结职责：
    - 展示 Phase-2 已冻结的结构性事实（只读）
    - 仅用于解释 Gate / ActionHint（不裁决、不预测）
    - 输出必须“可读”（中文 + 类 YAML 文本），不得强制 JSON
    """

    block_alias = "structure.facts"
    title = "结构事实（技术轨）"

    # 证据白名单：只展示对人类有解释价值的“关键变量”
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

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
        structure = context.slots.get("structure")

        if not isinstance(structure, dict) or not structure:
            warnings.append("structure_missing_or_invalid")
            text = (
                "- 结构事实：未提供或格式非法\n"
                "  含义：该区块仅占位，不影响 Gate / ActionHint 的有效性\n"
                "  检查：请确认 Phase-2 已写入 context.slots['structure']\n"
            )
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=text,
                warnings=warnings,
            )

        # 将 structure dict 渲染成“类 YAML”文本（纯中文可读）
        lines: List[str] = []
        lines.append("- 结构事实：")
        # 约定：跳过内部说明字段（如 _summary）
        keys = [k for k in structure.keys()]
        # 让输出稳定：_summary 放最后
        keys_sorted = sorted([k for k in keys if k != "_summary"]) + (
            ["_summary"] if "_summary" in structure else []
        )

        parsed_any = False

        for key in keys_sorted:
            item = structure.get(key)
            if not isinstance(item, dict):
                continue

            state = item.get("state") or item.get("status") or "unknown"
            meaning = item.get("meaning") or item.get("reason") or ""

            # evidence：只提取白名单字段，避免 raw 灾难
            ev: Dict[str, Any] = {}
            for k in self._EVIDENCE_KEYS:
                if k in item and item.get(k) is not None:
                    ev[k] = item.get(k)

            # 输出
            parsed_any = True
            if key == "_summary":
                # summary 行单独处理：更像“结构总述”
                lines.append(f"  - 总述：{meaning or '（无）'}")
                continue

            lines.append(f"  - {key}:")
            lines.append(f"      状态：{state}")
            if meaning:
                lines.append(f"      含义：{meaning}")

            if ev:
                lines.append("      关键证据：")
                for ek, evv in ev.items():
                    lines.append(f"        - {ek}: {self._fmt_value(evv)}")

        if not parsed_any:
            warnings.append("structure_present_but_unparsed")
            text = (
                "- 结构事实：结构槽位存在，但未解析出可读条目\n"
                "  检查：StructureFactsBuilder/Mapper 输出格式可能不符合约定\n"
            )
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=text,
                warnings=warnings,
            )

        # 冻结声明（技术轨必须保留，但要短、中文、可读）
        lines.append("")
        lines.append("说明：以上为已冻结的结构事实，仅用于解释当前制度背景，不构成预测或操作建议。")

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines).strip(),
            warnings=warnings,
        )

    @staticmethod
    def _fmt_value(v: Any) -> str:
        # 让输出“更像报告”，避免一大坨 dict
        if isinstance(v, float):
            # 保守格式：不强制百分号/单位
            return f"{v:.4f}"
        if isinstance(v, (list, tuple)):
            if len(v) > 6:
                return f"[{', '.join(map(str, v[:6]))}, ...]"
            return f"[{', '.join(map(str, v))}]"
        if isinstance(v, dict):
            # dict 仍可能很大：做短化
            keys = list(v.keys())
            if len(keys) > 6:
                short = {k: v[k] for k in keys[:6]}
                return f"{short}..."
            return str(v)
        return str(v)
