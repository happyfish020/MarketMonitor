# core/reporters/report_blocks/watchlist_sectors_blk.py
from __future__ import annotations

import logging
from typing import Dict, Any

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from .report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.WatchlistSectors")


class WatchlistSectorsBlock(ReportBlockRendererBase):
    #block_id = "4"
    block_alias = "watchlist.sectors"
    title = "观察板块对象（Watchlist）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        try:
            watchlist = context.slots.get("watchlist")
            if watchlist is None:
                raise ValueError("watchlist slot missing")

            payload = {
                "watchlist": watchlist,
                "note": (
                    "观察板块对象用于说明结构覆盖与来源，"
                    "不是推荐、不是主线、不等同于可交易标的。"
                ),
            }

            return ReportBlock(
                #block_id=self.block_id,
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
            )

        except Exception:
            LOG.error("WatchlistSectorsBlock render failed", exc_info=True)
            raise
