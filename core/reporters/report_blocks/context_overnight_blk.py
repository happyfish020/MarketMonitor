from __future__ import annotations

import logging
from typing import Dict, Any, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from .report_block_base import ReportBlockRendererBase

LOG = logging.getLogger("ReportBlock.ContextOvernight")


class ContextOvernightBlock(ReportBlockRendererBase):
    """
    context.overnight · 隔夜维度说明

    职责：
    - 展示与 A 股当日开盘相关的隔夜背景信息
    - 仅用于“环境说明”，不参与任何判断或裁决
    - 数据来源应为 Phase-2 / 预处理结果（通过 slots 注入）
    """

    #block_id = "3"
    block_alias = "context.overnight"
    title = "隔夜维度（Overnight Context）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        """
        Render overnight context block.

        设计原则：
        - 只读 context.slots["overnight"]
        - slot 缺失 ≠ 错误 → warning + 占位 payload
        - 永不抛异常、不返回 None
        """

        warnings: List[str] = []

        trend_notice = None
        trend = context.slots.get("structure", {}).get("trend_in_force")
        if isinstance(trend, dict) and trend.get("state") == "broken":
            trend_notice = self._render_trend_broken_notice()        


        

        overnight = context.slots.get("overnight")


        ###
        if overnight is None:
            warnings.append("empty:overnight")
            payload = {
                "note": (
                    "未提供隔夜维度数据（overnight slot 为空）。"
                    "该区块仅用于补充说明海外市场、宏观或情绪背景，"
                    "不影响 Gate / ActionHint 的有效性。"
                )
            }
            if trend_notice:
                payload["trend_notice"] = trend_notice
        else:
            payload = {
                "overnight": overnight,
                "note": (
                    "隔夜维度用于说明 A 股开盘前的外部环境背景，"
                    "仅作为情境补充，不构成交易信号或操作建议。"
                ),
            }
            if trend_notice:
                payload["trend_notice"] = trend_notice
        

        ###
        block = ReportBlock(
            #block_id=self.block_id,
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

        return block


    def _render_trend_broken_notice(self) -> str:
        return (
            "趋势失效提示：当前趋势结构已被破坏，"
            "原有趋势假设不再成立。"
            "在此结构环境下，任何基于趋势延续的解读均不具备制度可信度。"
        )
    