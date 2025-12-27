# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class ExecutionSummaryBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Execution Summary Block（语义一致性冻结版）
    """

    block_alias = "execution.summary"
    title = "执行层评估（Execution · 2–5D）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
        execu = context.slots.get("execution_summary")

        if not isinstance(execu, dict):
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload="未生成 Execution Summary（不影响制度裁决）。",
                warnings=["missing:execution_summary"],
            )

        code = execu.get("code")
        band = execu.get("band")

        payload = (
            f"执行评级：{code} / {band}\n"
            "制度说明：Execution 仅评估在 Gate 允许前提下的执行摩擦，"
            "不构成任何新增、调整或进攻行为的依据。\n"
            "当前未观察到显著执行摩擦，但在成功率下降阶段，"
            "不支持基于执行顺畅度采取进攻性操作。"
        )

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
