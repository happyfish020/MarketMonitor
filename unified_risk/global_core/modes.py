from __future__ import annotations
from typing import Dict, Any
from .utils.logging_utils import setup_logger
from .utils.time_utils import now_bj
from .datasources.ashare_bridge import get_daily_snapshot
from .scoring.us_daily_scoring import score_us_daily
from .scoring.tomorrow_scoring import compute_tomorrow_risk
from .scoring.macro_scoring import GlobalMacroSnapshot, score_global_macro
from .scoring.auction_scorer import score_auction_sentiment  # 这个 scorer 我可以下一步帮你写
from .cache.auction_cache import read_auction_cache

def _build_macro_snapshot_from_daily(daily: Dict[str, Any]) -> GlobalMacroSnapshot:
    treasury = (daily.get("treasury") or {}) if daily else {}
    us_eq = (daily.get("us_equity") or {}) if daily else {}
    a50 = (daily.get("a50_night") or {}) if daily else {}
    eu = daily.get("eu_futures") if daily else None

    ycurve_bps = treasury.get("yield_curve_diff")

    nas = (us_eq.get("nasdaq") or {}).get("changePct")
    spy = (us_eq.get("spy") or {}).get("changePct")
    vix_price = (us_eq.get("vix") or {}).get("price")

    dax_pct = eu
    ftse_pct = None

    a50_ret = a50.get("ret") if isinstance(a50, dict) else None
    a50_pct = a50_ret * 100.0 if a50_ret is not None else None
    a50_src = a50.get("source") if isinstance(a50, dict) else None

    return GlobalMacroSnapshot(
        treasury_5y=None,
        treasury_10y=None,
        ycurve_bps=ycurve_bps,
        nasdaq_pct=nas,
        spy_pct=spy,
        vix_last=vix_price,
        dax_pct=dax_pct,
        ftse_pct=ftse_pct,
        a50_night_pct=a50_pct,
        a50_night_proxy=a50_src,
    )


def run_us_daily_mode(logger=None) -> Dict[str, Any]:
    if logger is None:
        logger = setup_logger(__name__)
    bj_time = now_bj()
    logger.info("[US Daily] Beijing time: %s", bj_time.strftime("%Y-%m-%d %H:%M:%S"))

    daily = get_daily_snapshot(bj_time)
    us_eq = (daily.get("us_equity") or {}) if daily else {}
    treasury = (daily.get("treasury") or {}) if daily else {}

    result = score_us_daily(us_eq, treasury)
    logger.info(
        "[US Daily] score=%.1f, level=%s, desc=%s",
        result["score"],
        result["level"],
        result["desc"],
    )
    return result


def run_preopen_mode(logger=None) -> Dict[str, Any]:
    """
    盘前评分：主要看
    - 昨夜美股涨跌
    - VIX
    - A50 夜盘
    - 欧股期货（如果有）
    """
    if logger is None:
        logger = setup_logger(__name__)
    bj_time = now_bj()
    logger.info("[Preopen] Beijing time: %s", bj_time.strftime("%Y-%m-%d %H:%M:%S"))

    daily = get_daily_snapshot(bj_time)
    macro_snap = _build_macro_snapshot_from_daily(daily)
    macro_score = score_global_macro(macro_snap)

    # 盘前给一句话结论
    if macro_score.total_score >= 65:
        view = "外围整体偏多，对 A 股开盘偏正面"
    elif macro_score.total_score >= 50:
        view = "外围中性略偏多，A 股开盘以震荡偏强为主"
    elif macro_score.total_score >= 35:
        view = "外围偏紧，对 A 股开盘有一定压力"
    else:
        view = "外围高风险，对 A 股开盘构成较大压力，防守为主"

    result = {
        "score": macro_score.total_score,
        "level": macro_score.risk_level,
        "desc": macro_score.description,
        "view": view,
    }
    logger.info(
        "[Preopen] score=%.1f, level=%s, view=%s",
        result["score"],
        result["level"],
        result["view"],
    )
    return result

def run_tomorrow_mode(logger=None) -> Dict[str, Any]:
    """
    明日风险偏好：先用 compute_tomorrow_risk 做基础判断，
    再用当日竞价情绪做修正。
    """
    if logger is None:
        logger = setup_logger(__name__)
    bj_time = now_bj()
    logger.info("[Tomorrow Risk] Beijing time: %s", bj_time.strftime("%Y-%m-%d %H:%M:%S"))

    daily = get_daily_snapshot(bj_time)
    macro_snap = _build_macro_snapshot_from_daily(daily)
    macro_score = score_global_macro(macro_snap)

    index = (daily.get("index") or {}) if daily else {}
    sh_change = index.get("sh_change")
    cyb_change = index.get("cyb_change")

    result = compute_tomorrow_risk(macro_snap, macro_score, sh_change, cyb_change)

    # === 竞价修正部分 ===
    if has_auction_cache(bj_time):
        a_raw = read_auction_cache(bj_time)
        a_view = score_auction_sentiment(a_raw)

        base_prob = result.get("probability", 0.5)
        prob = base_prob

        if a_view.score >= 3:
            prob = min(base_prob + 0.1, 0.9)
            extra = "（竞价偏强，对明日走势形成正向修正）"
        elif a_view.score <= -3:
            prob = max(base_prob - 0.1, 0.1)
            extra = "（竞价偏弱，对明日走势形成负向修正）"
        else:
            extra = "（竞价中性，对明日仅有边际影响）"

        result["probability"] = prob
        view_txt = result.get("view", "")
        result["view"] = (view_txt + "；" + extra) if view_txt else extra

        # reason 字段附加说明
        reason = result.get("reason") or ""
        add_reason = f"竞价情绪：{a_view.level}；{a_view.desc}"
        result["reason"] = (reason + "；" + add_reason) if reason else add_reason
    else:
        # 没有竞价缓存，只在 reason 中注明
        reason = result.get("reason") or ""
        extra_reason = "未获取当日竞价缓存，明日风险未应用竞价修正。"
        result["reason"] = (reason + "；" + extra_reason) if reason else extra_reason

    logger.info(
        "[Tomorrow Risk] level=%s, prob=%.0f%%, view=%s",
        result.get("level"),
        result.get("probability", 0.5) * 100,
        result.get("view"),
    )
    return result
