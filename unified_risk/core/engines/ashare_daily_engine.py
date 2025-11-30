from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

import yfinance as yf

from unified_risk.core.fetchers.ashare_fetcher import AshareDataFetcher
from unified_risk.core.factors.a_emotion import compute_a_emotion
from unified_risk.core.factors.a_short_term import compute_a_short_term
from unified_risk.core.factors.a_mid_term import compute_a_mid_term
from unified_risk.core.factors.a_northbound import compute_a_northbound
from unified_risk.core.factors.score_unified import unify_scores
from unified_risk.common.logging_utils import log_warning
from unified_risk.core.factors.a_sector_rotation import compute_sector_rotation


BJ_TZ = timezone(timedelta(hours=8))

def _get_etf_core_series() -> List[float]:
    """从 Yahoo 获取 510300 最近 10 日涨跌幅。"""
    try:
        tk = yf.Ticker("510300.SS")
        hist = tk.history(period="10d")
        if hist is None or hist.empty or len(hist) < 2:
            return []
        rets = hist["Close"].pct_change().dropna() * 100.0
        return [float(x) for x in rets.tolist()]
    except Exception as e:
        log_warning(f"ETF 510300 series fetch failed: {e}")
        return []

def _get_index_sh_weekly() -> List[float]:
    """从 Yahoo 获取上证指数周线涨跌幅序列。"""
    try:
        tk = yf.Ticker("000001.SS")
        hist = tk.history(period="6mo")
        if hist is None or hist.empty or len(hist) < 5:
            return []
        weekly = hist["Close"].resample("W-FRI").last().dropna()
        rets = weekly.pct_change().dropna() * 100.0
        return [float(x) for x in rets.tolist()]
    except Exception as e:
        log_warning(f"SH weekly series fetch failed: {e}")
        return []

def run_ashare_daily() -> Dict[str, Any]:
    bj_now = datetime.now(BJ_TZ)
    fetcher = AshareDataFetcher()
    snap = fetcher.prepare_daily_market_snapshot(bj_now)

    emo = compute_a_emotion(snap)
    etf_series = _get_etf_core_series()
    short = compute_a_short_term({"etf_core": etf_series})

    weekly_sh = _get_index_sh_weekly()
    mid = compute_a_mid_term({"index_sh": weekly_sh})

    north = compute_a_northbound(snap)

    # === 新增：板块轮动因子 ===
    sector_score, sector_desc = compute_sector_rotation(snap)

    unified = unify_scores(
        a_emotion=emo.score,
        a_short=short.score,
        a_mid=mid.score,
        a_north=north.score,
        a_sector=sector_score,  # ⭐（可选）加入权重体系
        us_daily=10.0,
        us_short=10.0,
        us_mid=10.0,
    )

    summary = (
        f"[A股日级] 综合得分 {unified.total:.1f}/100（{unified.level}）\n"
        f"- 情绪：{emo.description}\n"
        f"- 短期：{short.description}\n"
        f"- 中期：{mid.description}\n"
        f"- 北向：{north.description}\n"
        f"- 板块轮动：{sector_desc}"
    )

    return {
        "time": bj_now.isoformat(),
        "snapshot": snap,
        "emotion": emo,
        "short": short,
        "mid": mid,
        "north": north,

        # ⭐ 加入 sector 因子返回
        "sector": {
            "score": sector_score,
            "desc": sector_desc,
        },

        "unified": unified,
        "summary": summary,
    }
