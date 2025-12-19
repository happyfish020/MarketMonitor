# core/reporters/report_blocks/watchlist_sectors_blk.py
from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.WatchlistSectors")


class WatchlistSectorsBlock(ReportBlockRendererBase):
    """
    Block · 观察对象（Watchlist）

    职责（冻结）：
    - 用于说明哪些主线 / 风格正在被系统观察
    - 给出结构验证状态、风险提示、同步性参考
    - 明确：不是推荐、不是交易指令
    """

    block_alias = "watchlist.sectors"
    title = "观察对象（Watchlist · 结构验证）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:

        watchlist = context.slots.get("watchlist")
        if not isinstance(watchlist, dict):
            payload = {
                "note": "⚠️ Watchlist 信息缺失，无法进行结构验证说明。"
            }
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
            )

        rendered: List[Dict[str, Any]] = []

        for name, info in watchlist.items():
            if not isinstance(info, dict):
                continue

            state = info.get("state", "UNVERIFIED")
            validation = info.get(
                "validation",
                "当前缺少完整结构输入，未形成参与放行条件。",
            )
            risk = info.get(
                "risk",
                "该观察对象存在波动或失真风险，需结合成交与指数同步性判断。",
            )
            sync = info.get(
                "sync_ref",
                "尚未确认与指数 / 板块的同步关系。",
            )

            rendered.append({
                "name": name,
                "state": state,
                "structure_validation": validation,
                "risk_note": risk,
                "sync_reference": sync,
            })

        payload = {
            "items": rendered,
            "note": (
                "观察对象仅用于结构跟踪与覆盖说明，"
                "不等同于推荐、不构成主线判断、也不直接对应可交易标的。"
            ),
        }

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
        )
