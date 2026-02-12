from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from .report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.DevEvidence")


class DevEvidenceBlock(ReportBlockRendererBase):
    """
    dev.evidence · 审计证据链

    职责：
    - 用于展示 Phase-3 Report 所使用的输入证据摘要
    - 仅面向审计 / 调试 / 回溯
    - 不参与任何判断、预测或裁决
    """

    #block_id = "7"
    block_alias = "dev.evidence"
    title = "审计证据链（Dev / Evidence）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render dev evidence block.

        设计原则：
        - 不访问 raw / DS
        - 只展示 slots 的“结构性轮廓”
        - 永不抛异常
        """

        warnings: List[str] = []

        slots = context.slots or {}

        # 仅列出 slot 名称与简要状态，不 dump 原始数据
        slot_overview = {}
        for k, v in slots.items():
            slot_overview[k] = {
                "present": v is not None,
                "type": type(v).__name__,
            }

        payload = {
            "slot_overview": slot_overview,
            #"note": (
            #    "该区块用于审计与回溯 Phase-3 Report 的输入证据轮廓，"
            #    "仅展示 slots 的存在性与类型信息，不包含原始数据内容。"
            #),
        }

        block = ReportBlock(
            #block_id=self.block_id,
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

        return block
