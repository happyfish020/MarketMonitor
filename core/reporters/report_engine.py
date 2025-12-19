from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock, ReportDocument

LOG = logging.getLogger("ReportEngine")

BlockBuilder = Callable[[ReportContext, Dict[str, Any]], ReportBlock]


@dataclass(frozen=True)
class BlockSpec:
    block_alias: str
    title: str


# 🔒 冻结：顺序即制度顺序（不再使用 block_id）
BLOCK_SPECS: List[BlockSpec] = [
    BlockSpec("structure.facts", "结构事实（Fact → 含义）"),
    BlockSpec("summary", "简要总结"),
    BlockSpec("context.overnight", "隔夜维度"),
    BlockSpec("watchlist.sectors", "观察板块对象"),
    BlockSpec("conditions.runtime", "即时验证条件"),
    BlockSpec("scenarios.forward", "T+N 情景说明"),
    BlockSpec("dev.evidence", "审计证据链"),
]
#### new block  to be added 

class ReportEngine:
    """
    UnifiedRisk V12 · Phase-3 ReportEngine（冻结修正版）

    铁律：
    - Summary 必须存在（不可为 None）
    - Block 顺序只由 block_alias 决定
    - block_id 不参与排序与制度语义
    """

    def __init__(
        self,
        *,
        market: str,
        actionhint_service: Any,
        #summary_mapper: Any,
        block_builders: Dict[str, BlockBuilder],  # key = block_alias
    ) -> None:
        self.market = market
        self.actionhint_service = actionhint_service
        #self.summary_mapper = summary_mapper
        self._builders_by_alias = dict(block_builders)

    def build_report(self, *, context: ReportContext) -> ReportDocument:
        meta = {
            "market": self.market,
            "trade_date": context.trade_date,
            "kind": context.kind,
            # "mode": context.mode,  # "DEV" or "PROD"
        }

        # -------- ActionHint --------
        slots = context.slots
        if "gate" not in slots:
            raise ValueError("missing required slot: gate")

        actionhint = self.actionhint_service.build_actionhint(
            gate=slots["gate"],
            structure=slots.get("structure"),
            watchlist=slots.get("watchlist"),
            conditions_runtime=slots.get("conditions_runtime"),
        )

        gate = actionhint.get("gate")
        if gate is None:
            raise ValueError("ActionHint missing gate")

        # -------- Summary（强制不为 None）--------
        #summary = self.summary_mapper.map_gate_to_summary(gate=gate)
        #if summary is None:
        #    raise ValueError("Summary mapping returned None (forbidden by V12)")

        summary = actionhint.get("summary")
        if summary is None:
            raise ValueError("ActionHint missing summary (forbidden by V12)")

        # -------- Blocks（按 alias 顺序）--------
        doc_partial = {"actionhint": actionhint, "summary": summary}
        blocks: List[ReportBlock] = []

        for spec in BLOCK_SPECS:
            builder = self._builders_by_alias.get(spec.block_alias)
            if builder is None:
                LOG.warning("missing block builder: %s", spec.block_alias)
                blocks.append(
                    ReportBlock(
                        block_alias=spec.block_alias,
                        title=spec.title,
                        payload={"note": "BLOCK_NOT_IMPLEMENTED"},
                        warnings=[f"missing_builder:{spec.block_alias}"],
                    )
                )
            else:
                blk = builder(context, doc_partial)
                if blk.block_alias != spec.block_alias:
                    raise ValueError(
                        f"block_alias mismatch: {blk.block_alias} != {spec.block_alias}"
                    )
                blocks.append(blk)

        return ReportDocument(meta, actionhint, summary, blocks)
