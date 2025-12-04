from typing import Dict, Any, List
import os

from core.utils.time_utils import now_bj
from core.utils.config_loader import load_paths
from core.adapters.fetchers.cn.ashare_fetcher import (
    AshareFetcher,
    get_daily_cache_path,
    get_intraday_cache_path,
)
from core.processing.cn.ashare_processor import AshareProcessor

from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.market_sentiment_factor import MarketSentimentFactor
from core.factors.cn.margin_factor import MarginFactor

from core.factors.score_unified import UnifiedScoreBuilder
from core.engines.cn.refresh_controller_cn import RefreshControllerCN, RefreshPlanCN
from core.models.factor_result import FactorResult
from core.utils.logger import log
from core.report.cn.ashare_report_cn import build_daily_report_text, save_daily_report

_paths = load_paths()
DATA_CACHE_DIR = _paths.get("cache_dir", "data/cache/")


def _daily_cache_exists(trade_date) -> bool:
    path = get_daily_cache_path(trade_date)
    return os.path.exists(path)


def _intraday_cache_exists() -> bool:
    path = get_intraday_cache_path()
    return os.path.exists(path)


# ======================================================
#  A股日级引擎 V11.4.1（4 因子 + 报告输出）
# ======================================================
def run_cn_ashare_daily(force_daily_refresh: bool = False) -> Dict[str, Any]:
    bj_now = now_bj()
    controller = RefreshControllerCN(bj_now)
    trade_date = controller.trade_date

    has_daily_cache = _daily_cache_exists(trade_date)
    plan: RefreshPlanCN = controller.build_refresh_plan(
        force_daily=force_daily_refresh,
        has_daily_cache=has_daily_cache,
    )

    # === 1) 取原始 snapshot ===
    fetcher = AshareFetcher()
    daily_snapshot = fetcher.get_daily_snapshot(
        trade_date=trade_date,
        force_refresh=plan.should_refresh_daily,
    )

    # === 2) 处理 snapshot => features ===
    processor = AshareProcessor()
    processed = processor.build_from_daily(daily_snapshot)

    # === 3) 运行四个因子 ===
    factors: List[FactorResult] = []

    f_north = NorthNPSFactor()
    f_turnover = TurnoverFactor()
    f_emotion = MarketSentimentFactor()
    f_margin = MarginFactor()

    factors.append(f_north.compute_from_daily(processed))
    factors.append(f_turnover.compute_from_daily(processed))
    factors.append(f_emotion.compute_from_daily(processed))
    factors.append(f_margin.compute_from_daily(processed))

    # === 4) 统一打分 ===
    usb = UnifiedScoreBuilder()
    summary = usb.unify({f.name: f for f in factors})

    log(
        "[CN Engine] A股日级完成: trade_date={}, total_score={:.2f}, level={}".format(
            trade_date, summary["total_score"], summary["risk_level"]
        )
    )

    # === 5) 输出报告到 root/reports ===
    trade_date_str = trade_date.isoformat()
    report_text = build_daily_report_text(trade_date_str, summary)
    report_path = save_daily_report("cn", trade_date_str, report_text)
    log(f"[CN Report] 报告已保存: {report_path}")

    return {
        "meta": {
            "market": "cn",
            "engine": "ashare_daily",
            "time_bj": bj_now.isoformat(),
            "trade_date": trade_date.isoformat(),
        },
        "snapshot": daily_snapshot,
        "processed": processed,
        "factors": {f.name: f for f in factors},
        "summary": summary,
        "report_path": report_path,
    }


# ======================================================
#  A股盘中引擎（保持原有逻辑，仅示例保留 north 因子）
# ======================================================
def run_cn_ashare_intraday(force_intraday_refresh: bool = False) -> Dict[str, Any]:
    bj_now = now_bj()
    controller = RefreshControllerCN(bj_now)

    has_intraday_cache = _intraday_cache_exists()
    plan: RefreshPlanCN = controller.build_refresh_plan(
        force_intraday=force_intraday_refresh,
        has_intraday_cache=has_intraday_cache,
    )

    fetcher = AshareFetcher()
    intraday_snapshot = fetcher.get_intraday_snapshot(
        bj_now=bj_now,
        force_refresh=plan.should_refresh_intraday,
    )

    processor = AshareProcessor()
    processed = processor.build_from_intraday(intraday_snapshot)

    factors: List[FactorResult] = []
    f_north = NorthNPSFactor()
    factors.append(f_north.compute_from_intraday(processed))

    total_score = sum(f.score for f in factors) / len(factors) if factors else 50.0
    if total_score >= 60:
        level = "偏多"
    elif total_score <= 40:
        level = "偏空"
    else:
        level = "中性"

    unified = {
        "total_score": round(total_score, 2),
        "risk_level": level,
        "factor_scores": {f.name: f.score for f in factors},
    }

    log(
        "[CN Engine] A股盘中完成: total_score={:.2f}, level={}".format(
            total_score, level
        )
    )

    return {
        "meta": {
            "market": "cn",
            "engine": "ashare_intraday",
            "time_bj": bj_now.isoformat(),
            "trade_date": controller.trade_date.isoformat(),
        },
        "snapshot": intraday_snapshot,
        "processed": processed,
        "factors": {f.name: f for f in factors},
        "summary": unified,
    }