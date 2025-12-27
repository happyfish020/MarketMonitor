# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class SummaryANDBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Summary (A / N / D) Block（语义一致性冻结版）
    """

    block_alias = "summary"
    title = "简要总结（Summary · A / N / D）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        summary_code = doc_partial.get("summary")
        actionhint = doc_partial.get("actionhint")

        if summary_code not in ("A", "N", "D"):
            raise ValueError("invalid summary code")

        reason = (
            actionhint.get("reason")
            if isinstance(actionhint, dict)
            else "制度进入谨慎区间。"
        )

        meaning = (
            f"{reason}\n"
            "趋势结构仍在，但成功率下降，"
            "制度不支持主动扩大风险敞口。"
        )

        # DRS（只读）
        drs = context.slots.get("drs")
        if isinstance(drs, dict):
            meaning += (
                f"\n【DRS · 日度风险信号】：{drs.get('signal')} —— "
                f"{drs.get('meaning')}"
            )

        # Execution（否定性限定）
        execu = context.slots.get("execution_summary")
        if isinstance(execu, dict):
            meaning += (
                "\n【Execution · 2–5D】"
                f"{execu.get('code')}/{execu.get('band')} —— "
                "执行环境未恶化，但在当前 Gate 下不支持基于执行条件"
                "进行任何主动调仓或进攻行为。"
            )

        gate_pre = context.slots.get("gate_pre")
        gate_final = context.slots.get("gate_final")
        if gate_pre and gate_final:
            meaning += (
                f"\n【制度权限（Gate）】\n"
                f"- 原始 Gate：{gate_pre}\n"
                f"- 执行后 Gate：{gate_final}"
            )

        payload = f"Code:{summary_code}\n{meaning}"

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
