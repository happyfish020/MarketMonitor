from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from .report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.ConditionsRuntime")


class ConditionsRuntimeBlock(ReportBlockRendererBase):
    """
    conditions.runtime · 即时验证条件

    职责：
    - 展示“执行前/当下”的制度级校验条件（是否满足执行前置条件）
    - 仅用于解释 ActionHint 的可执行边界
    - 不参与任何预测或裁决
    """

    #block_id = "5"
    block_alias = "conditions.runtime"
    title = "即时验证条件（Runtime Conditions）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render runtime conditions block.

        设计原则：
        - 只读 context.slots["conditions_runtime"]
        - slot 缺失 ≠ 错误 → warning + 占位 payload
        - 永不抛异常、不返回 None
        """

        warnings: List[str] = []

        conditions = context.slots.get("conditions_runtime")

        if conditions is None:
            warnings.append("empty:conditions_runtime")
            payload = {
                "note": (
                    "未提供即时验证条件（conditions_runtime slot 为空）。"
                    "该区块用于说明执行 ActionHint 前的制度校验状态，"
                    "不影响 Gate / ActionHint 的有效性。"
                )
            }
        else:
            payload = {
                "conditions_runtime": conditions,
                "note": (
                    "即时验证条件用于确认 ActionHint 在当前时点是否满足执行前置条件，"
                    "仅用于执行边界说明，不构成新的判断或预测。"
                ),
            }

        block = ReportBlock(
            #block_id=self.block_id,
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

        return block
