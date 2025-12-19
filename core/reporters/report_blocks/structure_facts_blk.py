from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.StructureFacts")


class StructureFactsBlock(ReportBlockRendererBase):
    """
    Block · 结构事实（技术轨）

    职责（冻结）：
    - 展示 Phase-2 已冻结的结构性事实
    - 以「状态 + 含义 + 证据」形式输出
    - 仅用于解释 Gate / ActionHint
    - ❌ 不参与裁决
    - ❌ 不做预测
    """

    block_alias = "structure.facts"
    title = "结构事实（Structure Facts · 技术轨）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        structure = context.slots.get("structure")
        if not isinstance(structure, dict):
            warnings.append("structure_missing_or_invalid")
            payload = {
                "note": (
                    "⚠️ structure slot 缺失或非法。\n"
                    "该区块应由 Phase-2 结构观测层生成，"
                    "请检查 Phase-2 → Phase-3 接线。"
                )
            }
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        rows: List[Dict[str, Any]] = []

        for key, item in structure.items():
            if not isinstance(item, dict):
                continue

            state = item.get("state") or item.get("status")
            reason = item.get("reason") or item.get("meaning")
            evidence = {}

            # 只挑“关键证据”，避免 raw data 灾难
            for k in (
                "adv_ratio",
                "new_low_ratio",
                "count_new_low",
                "trend",
                "signal",
                "score",
            ):
                if k in item:
                    evidence[k] = item.get(k)

            rows.append(
                {
                    "structure": key,
                    "state": state,
                    "meaning": reason,
                    "evidence": evidence if evidence else None,
                }
            )

        if not rows:
            warnings.append("structure_empty")
            payload = {
                "note": (
                    "structure slot 存在，但未解析出可用结构项。\n"
                    "请检查 StructureFactsBuilder 的输出格式。"
                )
            }
        else:
            payload = {
                "rows": rows,
                "note": (
                    "以上为 Phase-2 冻结后的结构性事实，"
                    "仅用于解释当前 Gate / ActionHint 的制度背景，"
                    "不构成新的判断、预测或操作建议。"
                ),
            }

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )
