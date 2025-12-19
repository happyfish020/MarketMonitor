from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from .report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.ScenariosForward")


class ScenariosForwardBlock(ReportBlockRendererBase):
    """
    scenarios.forward · T+N 情景说明

    职责：
    - 基于当前 Gate / Summary，给出制度层面的情景描述
    - 不进行任何数值预测
    - 不构成交易建议
    """

    #block_id = "6"
    block_alias = "scenarios.forward"
    title = "T+N 情景说明（Forward Scenarios）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render forward scenarios block.

        设计原则：
        - 仅基于 Gate / Summary 进行制度性描述
        - 不访问 DS、不读因子
        - 永不抛异常
        """

        warnings: List[str] = []

        gate = context.slots.get("gate")
        summary = doc_partial.get("summary")

        if gate is None:
            warnings.append("missing:gate")
            payload = {
                "note": (
                    "缺少 Gate 信息，无法生成制度情景说明。"
                    "该区块仅作为占位。"
                )
            }
        else:
            payload = {
                "gate": gate,
                "summary": summary,
                "scenario_note": (
                    "T+N 情景说明基于当前 Gate 与 Summary，"
                    "用于描述可能的制度演化路径。"
                    "该说明不构成预测，也不构成操作建议。"
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
