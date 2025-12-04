from __future__ import annotations

from datetime import datetime
from typing import Dict, Any

# 数据抓取
from core.adapters.fetchers.cn.ashare_fetcher import AshareFetcher

# 日级处理器（必须）
from core.processing.cn.ashare_processor import AshareProcessor

# 因子
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.market_sentiment_factor import MarketSentimentFactor
from core.factors.cn.margin_factor import MarginFactor

# 统一评分
from core.factors.score_unified import unify_scores

# 报告
from core.report.cn.ashare_report_cn import (
    build_daily_report_text,
    save_daily_report,
)


def run_cn_ashare_daily(force_daily_refresh: bool = False) -> Dict[str, Any]:
    """
    A股日级风险引擎（V11 FULL）
    """

    bj_now = datetime.now().astimezone()
    trade_date = bj_now.date()
    trade_date_str = str(trade_date)

    print(f"[CN Engine] A股日级开始: trade_date={trade_date_str}")

    # --------------------------------------------------------
    # 1. 获取原始 snapshot（未处理）
    # --------------------------------------------------------
    fetcher = AshareFetcher()
    snapshot = fetcher.get_daily_snapshot(
        trade_date=trade_date,
        force_refresh=force_daily_refresh,
    )

    # --------------------------------------------------------
    # 2. 使用 Processor 构建 processed（因子统一输入格式）
    # --------------------------------------------------------
    processor = AshareProcessor()
    processed = processor.build_from_daily(snapshot)

    # --------------------------------------------------------
    # 3. 计算因子
    # --------------------------------------------------------
    nps = NorthNPSFactor().compute_from_daily(processed)
    turnover = TurnoverFactor().compute_from_daily(processed, snapshot=snapshot)
    emo = MarketSentimentFactor().compute_from_daily(processed)
    margin = MarginFactor().compute_from_daily(processed)
    
    factor_results = {
        "north_nps": nps,
        "turnover": turnover,
        "market_sentiment": emo,
        "margin": margin,
    }

    # --------------------------------------------------------
    # 4. 统一评分
    # --------------------------------------------------------
    summary = unify_scores(
        north_nps=nps.score,
        turnover=turnover.score,
        market_sentiment=emo.score,
        margin=margin.score,
    )

    summary["trade_date"] = trade_date_str

    # --------------------------------------------------------
    # 5. 生成报告
    # --------------------------------------------------------
    report_text = build_daily_report_text(
        trade_date=trade_date_str,
        summary=summary,
        factors=factor_results,
    )

    report_path = save_daily_report(
        market="cn",
        trade_date=trade_date_str,
        text=report_text,
    )

    print(
        f"[CN Engine] A股日级完成: trade_date={trade_date_str}, "
        f"total_score={summary['total_score']:.2f}, level={summary['risk_level']}"
    )

    return {
        "unified": summary,
        "factors": factor_results,
        "processed": processed,
        "snapshot": snapshot,
        "report_path": report_path,
    }
