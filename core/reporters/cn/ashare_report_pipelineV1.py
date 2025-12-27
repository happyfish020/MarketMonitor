# core/reporters/cn/ashare_report_pipeline.py
# -*- coding: utf-8 -*-

from typing import Dict, Any

from core.reporters.cn.ashare_daily_reporter import (
    build_daily_report_text,
    save_daily_report,
)


class AshareReportPipeline:
    """
    A股报告生成管道（冻结）

    职责：
    - 仅封装“生成 + 保存报告”
    - 不改写原有报告语义
    - 不做制度判断
    """

    def run(
        self,
        *,
        trade_date: str,
        snapshot: Dict[str, Any],
        factors: Dict[str, Any],
        prediction: Dict[str, Any],
        meta: Dict[str, Any],
    ) -> None:
        report_text = build_daily_report_text(
            meta=meta,
            factors=factors,
            prediction=prediction,
            snapshot=snapshot,
        )

        if not report_text:
            return

        save_daily_report(
            trade_date=trade_date,
            text=report_text,
        )
