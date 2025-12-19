from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


LOG = logging.getLogger("ReportBlock.StructureFacts")


class StructureFactsBlock(ReportBlockRendererBase):
    """
    Block 2 · 结构事实（Fact → 含义）

    职责：
    - 展示 Phase-2 已冻结的结构性事实
    - 仅用于解释 Gate / ActionHint 的制度背景
    - 不参与任何预测、裁决或评分
    """

    #block_id = "2"
    block_alias = "structure.facts"
    title = "结构事实（Fact → 含义）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render structure facts block.

        设计要点：
        - 只读 context.slots["structure"]
        - slot 缺失 → warning + 占位 payload
        - 永不返回 None
        """

        warnings: List[str] = []
        assert "structure" in context.slots, \
                    "structure.facts missing: Phase-2 wiring error" 
        
        structure = context.slots.get("structure")
          
        if structure is None:
            warnings.append("empty:structure")
            self.logger.error(
                    "[DEPRECATED][StructureFactsBlock] "
                    "context.slots['structure'] is missing. "
                    "This indicates Phase-2 → Phase-3 wiring failure. "
                    "Current behavior falls back to placeholder payload. "
                    "This fallback will be removed in a future version."
                )
            payload = {
                "note": (
                    "⚠️ 结构性事实缺失（structure slot 未生成）。\n"
                    "该情况不应在正常流程中出现，"
                    "请检查 Phase-2 结构聚合与接线是否正确。"
                )
            }
        else:
            payload = {
                "structure": structure,
                "note": (
                    "Phase-2 冻结后的结构事实，仅用于解释当前 Gate / ActionHint，"
                    "不构成新的判断、预测或操作建议。"
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
