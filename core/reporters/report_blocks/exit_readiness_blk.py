# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.utils.logger import get_logger
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = get_logger("Report.ExitReadiness")


class ExitReadinessBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Exit Readiness Block（冻结版）

    职责：
    - 展示 Governance.ExitReadinessValidator 的评估结果
    - 用于解释“是否需要提前为减仓/退出做准备”
    - 仅用于解释，不参与任何裁决

    设计铁律：
    - 不修改 Gate / Summary
    - 不读取 factors
    - 不读取 observations
    - 只读取 slots["exit_readiness"]
    """

    block_alias = "exit.readiness"
    title = "出场准备度（Exit Readiness · Governance）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        er = context.slots.get("exit_readiness")

        # --------------------------------------------------
        # 缺失处理（允许，不抛异常）
        # --------------------------------------------------
        if er is None:
            warnings.append("missing:exit_readiness")
            payload = (
                "未生成 ExitReadiness 评估结果。\n"
                "可能原因：\n"
                "- Governance 层未执行 ExitReadinessValidator\n"
                "- slots 未接入 exit_readiness"
            )
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        if not isinstance(er, dict):
            warnings.append("invalid:exit_readiness_format")
            payload = "ExitReadiness 数据格式异常，无法解析。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        # --------------------------------------------------
        # 正常解析（冻结字段）
        # --------------------------------------------------
        level = er.get("level")
        action = er.get("action")
        meaning = er.get("meaning")
        reasons = er.get("reasons")

        if not level:
            warnings.append("missing:exit_readiness.level")
        if not action:
            warnings.append("missing:exit_readiness.action")

        # --------------------------------------------------
        # 人话输出（冻结格式，对齐 ExecutionSummary）
        # --------------------------------------------------
        lines: List[str] = []

        # Header
        if level:
            header = f"准备度等级：{level}"
            if action:
                header = f"{header} / 建议动作：{action}"
            lines.append(header)

        # Meaning
        if isinstance(meaning, str) and meaning.strip():
            lines.append(meaning)
        else:
            warnings.append("missing:exit_readiness.meaning")
            lines.append("未提供出场准备度的文字说明。")

        # Reasons（可选，但推荐）
        if isinstance(reasons, list) and reasons:
            lines.append("")
            lines.append("触发依据：")
            for r in reasons:
                lines.append(f"- {r}")

        payload = "\n".join(lines)

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
