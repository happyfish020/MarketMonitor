from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

import yfinance as yf  # 目前仅用于周线 SH 指数

from unified_risk.core.fetchers.ashare_fetcher import AshareDataFetcher
from unified_risk.core.factors.a_emotion import compute_a_emotion
from unified_risk.core.factors.a_short_term import compute_a_short_term
from unified_risk.core.factors.a_mid_term import compute_a_mid_term
from unified_risk.core.factors.a_northbound import compute_a_northbound
from unified_risk.core.factors.a_sector_rotation import compute_sector_rotation
from unified_risk.core.factors.margin_factor import MarginFactor
from unified_risk.core.factors.score_unified import unify_scores
from unified_risk.common.logging_utils import log_warning
from unified_risk.core.datasources.yf_etf_fetcher import safe_fetch_etf  # ⭐ 新增导入

BJ_TZ = timezone(timedelta(hours=8))


def _fetch_etf_series(symbol: str, days: int = 60) -> List[float]:
    """
    使用统一的 safe_fetch_etf 获取 ETF 近 N 日涨跌幅（百分比）。
    - 自动处理周末 / 假期：safe_fetch_etf 内部已经通过 safe_yf_last_bars 回退到最近交易日
    - 这里仅负责把日度收盘价转换为日收益率序列
    """
    try:
        df = safe_fetch_etf(symbol)
        if df is None or df.empty:
            log_warning(f"[ETF] {symbol} empty data")
            return []

        # 按日期排序，截取最近 N 天
        df = df.sort_values("date")
        if len(df) > days:
            df = df.iloc[-days:]

        closes = df["close"].astype(float).tolist()
        if len(closes) < 2:
            log_warning(f"[ETF] {symbol} not enough closes for return series")
            return []

        rets: List[float] = []
        for i in range(1, len(closes)):
            if closes[i - 1] == 0:
                continue
            rets.append((closes[i] / closes[i - 1] - 1.0) * 100.0)
        return rets
    except Exception as e:
        log_warning(f"[ETF] fetch failed for {symbol}: {e}")
        return []


def _get_etf_core_series() -> Dict[str, List[float]]:
    """核心宽基 ETF 组合，用于短期趋势因子。"""
    # 510300.SS：沪深300ETF，159901.SZ：深成指ETF
    hs300 = _fetch_etf_series("510300.SS")
    sz50 = _fetch_etf_series("159901.SZ")
    # 简单合并为一个核心序列（取有效部分平均）
    if hs300 and sz50 and len(hs300) == len(sz50):
        merged = [(a + b) / 2.0 for a, b in zip(hs300, sz50)]
    else:
        merged = hs300 or sz50 or []
    return {"etf_core": merged}


import pandas as pd

def _get_index_sh_weekly() -> List[float]:
    """
    上证指数（000001.SS）周级别涨跌序列（稳健版）
    修复:
    - MultiIndex 列
    - Close 为 DataFrame 多列的情况
    - 周末/假日只返回一条数据的情况
    """
    try:
        df = yf.download(
            "000001.SS",
            period="12mo",
            interval="1wk",
            auto_adjust=False,
            progress=False
        )

        if df.empty:
            log_warning("[INDEX] 000001.SS weekly empty")
            return []

        # 处理 MultiIndex / DataFrame 的 Close 列
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]

        closes = close.astype(float).tolist()

        # 如果只有 0 或 1 条，周末会出现，只能返回空
        if len(closes) < 2:
            log_warning("[INDEX] 000001.SS weekly: insufficient bars")
            return []

        # 计算涨跌
        rets = []
        for i in range(1, len(closes)):
            if closes[i - 1] == 0:
                continue
            rets.append((closes[i] / closes[i - 1] - 1) * 100.0)

        return rets

    except Exception as e:
        log_warning(f"[INDEX] SH weekly fetch failed: {e}")
        return []

def run_ashare_daily() -> Dict[str, Any]:
    """计算 A 股日级综合评分（含：情绪/短期/中期/北向/板块轮动）。"""
    bj_now = datetime.now(BJ_TZ)
    fetcher = AshareDataFetcher()

    # 1) 构建当日快照
    snap = fetcher.prepare_daily_market_snapshot(bj_now)

    # 2) 各类 A 股因子
    emo = compute_a_emotion(snap)

    etf_series = _get_etf_core_series()
    short = compute_a_short_term(etf_series)

    weekly_sh = _get_index_sh_weekly()
    mid = compute_a_mid_term({"index_sh": weekly_sh})

    north = compute_a_northbound(snap)

    # 板块轮动
    sector_score, sector_desc = compute_sector_rotation(snap)

    # 两融因子
    margin = MarginFactor(cache_manager=None).compute()

    # 3) 统一打分（A 股 + 全球占位）
    unified = unify_scores(
        a_emotion=emo.score,
        a_short=short.score,
        a_mid=mid.score,
        a_north=getattr(north, "score", None),
        a_sector=sector_score,
        # 预留：若接入 margin_score 可传入 a_margin=margin["margin_score"]
        us_daily=10.0,
        us_short=10.0,
        us_mid=10.0,
    )

    summary = (
        f"[A股日级] 综合得分 {unified.total:.1f}/100（{unified.level}）\n"
        f"- 情绪：{emo.description}\n"
        f"- 短期：{short.description}\n"
        f"- 中期：{mid.description}\n"
        f"- 北向：{getattr(north, 'description', '')}\n"
        f"- 板块轮动：{sector_desc}"
    )

    return {
        "time": bj_now.isoformat(),
        "snapshot": snap,
        "emotion": emo,
        "short": short,
        "mid": mid,
        "north": north,
        "sector": {
            "score": sector_score,
            "desc": sector_desc,
        },
        "margin": margin,
        "unified": unified,
        "summary": summary,
    }
