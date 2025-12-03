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

# === 三大因子 ===
from core.factors.cn.north_nps_factor import NorthNPSFactor
from core.factors.cn.turnover_factor import TurnoverFactor
from core.factors.cn.market_sentiment_factor import MarketSentimentFactor

# === 统一得分 ===
from core.factors.score_unified import UnifiedScoreBuilder

# === 报告输出 ===
from core.report.cn.ashare_report_cn import (
    build_daily_report_text,
    save_daily_report,
)

from core.engines.cn.refresh_controller_cn import RefreshControllerCN, RefreshPlanCN
from core.models.factor_result import FactorResult
from core.utils.logger import log

_paths = load_paths()
DATA_CACHE_DIR = _paths.get("cache_dir", "data/cache/")


def _daily_cache_exists(trade_date) -> bool:
    path = get_daily_cache_path(trade_date)
    return os.path.exists(path)


def _intraday_cache_exists() -> bool:
    path = get_intraday_cache_path()
    return os.path.exists(path)


# ======================================================
#  A股日级引擎 V11 FULL（3 因子 + 报告输出）
# ======================================================
def run_cn_ashare_daily(force_daily_refresh: bool = False) -> Dict[str, Any]:
    bj_now = now_bj()
    controller = RefreshControllerCN(bj_now)
    trade_date = controller.trade_date

    # === 刷新计划 ===
    has_daily_cache = _daily_cache_exists(trade_date)
    plan: RefreshPlanCN = controller.build_refresh_plan(
        force_daily=force_daily_refresh,
        has_daily_cache=has_daily_cache,
    )

    # === 1) 取原始 snapshot（含北代 + 成交额 + 市场情绪） ===
    fetcher = AshareFetcher()
    daily_snapshot = fetcher.get_daily_snapshot(
        trade_date=trade_date,
        force_refresh=plan.should_refresh_daily,
    )

    # === 2) 处理 snapshot => features ===
    processor = AshareProcessor()
    processed = processor.build_from_daily(daily_snapshot)

    # === 3) 运行三个因子 ===
    factors: List[FactorResult] = []

    north = NorthNPSFactor()
    turnover = TurnoverFactor()
    emotion = MarketSentimentFactor()

    factors.append(north.compute_from_daily(processed))
    factors.append(turnover.compute_from_daily(processed))
    factors.append(emotion.compute_from_daily(processed))

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

    # === 返回整体结构（给 main.py 使用） ===
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
#  原盘中引擎保持不动（你没有要求）
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
    north = NorthNPSFactor()
    factors.append(north.compute_from_intraday(processed))

    total_score = sum(f.score for f in factors)
    if total_score >= 60:
        level = "偏多"
    elif total_score <= 40:
        level = "偏空"
        level = "偏空"
    else:
        level = "中性"

    unified = {
        "total": total_score,
        "level": level,
        "components": {f.name: f.score for f in factors},
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
        "factors": factors,
        "unified": unified,
    }
