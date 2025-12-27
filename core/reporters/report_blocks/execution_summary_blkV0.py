# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.utils.logger import get_logger
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = get_logger("Report.ExecutionSummary")


class ExecutionSummaryBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Execution Summary Block（冻结版）

    职责：
    - 展示 Governance.ExecutionSummaryBuilder 的输出结果
    - 用于解释 2–5D 执行层面的摩擦 / 风险环境
    - 只读，不参与任何裁决或降级

    设计铁律：
    - 不修改 Gate / Summary
    - 不读取 factors
    - 不读取 observations
    - 只读取 slots["execution_summary"]
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

        # --------------------------------------------------
        # 缺失处理（允许，不抛异常）
        # --------------------------------------------------
        if execu is None:
            warnings.append("missing:execution_summary")
            payload = (
                "未生成 ExecutionSummary。\n"
                "可能原因：\n"
                "- 当日未触发执行层评估逻辑\n"
                "- Governance 层未接入 ExecutionSummaryBuilder"
            )
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        if not isinstance(execu, dict):
            warnings.append("invalid:execution_summary_format")
            payload = "ExecutionSummary 数据格式异常，无法解析。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        # --------------------------------------------------
        # 正常解析
        # --------------------------------------------------
        code = execu.get("code")
        band = execu.get("band")
        meaning = execu.get("meaning")

        if not code:
            warnings.append("missing:execution_summary.code")

        # --------------------------------------------------
        # 人话输出（冻结格式）
        # --------------------------------------------------
        lines: List[str] = []

        if code:
            header = f"执行评级：{code}"
            if band:
                header = f"{header} / {band}"
            lines.append(header)

        if isinstance(meaning, str) and meaning.strip():
            lines.append(meaning)
        else:
            warnings.append("missing:execution_summary.meaning")
            lines.append("未提供执行层风险的文字说明。")

        payload = "\n".join(lines)

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
