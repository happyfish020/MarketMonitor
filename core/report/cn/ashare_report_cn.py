import os
from datetime import datetime
from typing import Dict, Any

from core.models.factor_result import FactorResult


REPORT_ROOT = "reports"
os.makedirs(REPORT_ROOT, exist_ok=True)


def _fmt_score(score: float) -> str:
    return f"{score:.2f}"


def build_daily_report_text(
    trade_date: str,
    summary: Dict[str, Any],
    factors: Dict[str, FactorResult],
) -> str:
    """
    A 股日级风险报告模板（带明细版）。

    summary:
        {
            "total_score": float,
            "risk_level": str,
            "factor_scores": {name: float, ...}
        }

    factors:
        {name: FactorResult}
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_score = float(summary.get("total_score", 0.0) or 0.0)
    level = summary.get("risk_level", "未知")
    factor_scores: Dict[str, float] = summary.get("factor_scores", {}) or {}

    # ---------- 标题 & 总览 ----------
    title = "=== A股日级风险报告（V11 FULL） ===\n"
    header_lines = [
        f"生成时间：{now_str}",
        f"交易日：{trade_date}",
        f"综合得分：{_fmt_score(total_score)}",
        f"风险等级：{level}",
        "",
    ]
    header = "\n".join(header_lines) + "\n"

    # ---------- 一、因子得分概览 ----------
    factor_overview_lines = ["因子得分："]
    # 固定顺序优先展示
    preferred_order = ["north_nps", "turnover", "market_sentiment", "margin"]
    other_names = [k for k in factor_scores.keys() if k not in preferred_order]
    ordered_names = [n for n in preferred_order if n in factor_scores] + sorted(
        other_names
    )

    for name in ordered_names:
        score = factor_scores.get(name, 0.0)
        factor_overview_lines.append(f"  - {name}: {_fmt_score(score)}")

    factor_overview = "\n".join(factor_overview_lines) + "\n\n"

    # ---------- 二、关键因子明细 ----------
    detail_lines = ["关键因子明细："]

    # 2.1 北向代理 NPS
    nps = factors.get("north_nps")
    if nps is not None:
        raw = nps.raw or {}
        strength_today = raw.get("strength_today")
        trend = raw.get("trend")
        band = raw.get("band")

        desc_parts = []
        if strength_today is not None:
            desc_parts.append(f"当日北向代理强度={strength_today:.2f}")
        if trend is not None:
            # 趋势正负表述为“增/减/均势”
            if trend > 0:
                trend_txt = "近期呈增加趋势"
            elif trend < 0:
                trend_txt = "近期呈减少趋势"
            else:
                trend_txt = "近期基本均势"
            desc_parts.append(f"3日趋势={trend:.2f}（{trend_txt}）")
        if band:
            desc_parts.append(f"当前区间：{band}")

        detail_lines.append(
            f"- north_nps：得分 {_fmt_score(nps.score)}，{nps.signal}"
        )
        if desc_parts:
            detail_lines.append("  · " + "；".join(desc_parts))

    # 2.2 成交额 / 流动性
    to = factors.get("turnover")
    if to is not None:
        raw = to.raw or {}
        total = raw.get("turnover_total")
        etf_turnover = raw.get("turnover_etf")
        etf_ratio = raw.get("etf_ratio")

        desc_parts = []
        if total is not None:
            desc_parts.append(f"全市场成交额≈{total:.1f} 亿")
        if etf_turnover is not None:
            desc_parts.append(f"宽基ETF成交≈{etf_turnover:.1f} 亿")
        if etf_ratio is not None:
            if etf_ratio > 0.2:
                r_txt = "宽基资金主导，偏多"
            elif etf_ratio < 0.05:
                r_txt = "宽基参与较少，结构偏散"
            else:
                r_txt = "宽基与个股参与度均衡"
            desc_parts.append(f"宽基占比={etf_ratio:.2f}（{r_txt}）")

        detail_lines.append(
            f"- turnover：得分 {_fmt_score(to.score)}，{to.signal}"
        )
        if desc_parts:
            detail_lines.append("  · " + "；".join(desc_parts))

    # 2.3 情绪因子
    emo = factors.get("market_sentiment")
    if emo is not None:
        raw = emo.raw or {}
        adv = raw.get("adv")
        dec = raw.get("dec")
        total = raw.get("total")
        lup = raw.get("limit_up")
        ldn = raw.get("limit_down")
        adv_ratio = raw.get("adv_ratio")
        hs300_pct = raw.get("hs300_pct")

        desc_parts = []
        if adv is not None and dec is not None and total:
            desc_parts.append(f"涨跌家数：{adv} / {dec}（总数≈{total}）")
        if lup is not None and ldn is not None:
            desc_parts.append(f"涨停={lup}，跌停={ldn}")
        if adv_ratio is not None:
            if adv_ratio >= 0.65:
                a_txt = "普涨结构，情绪偏热"
            elif adv_ratio >= 0.55:
                a_txt = "多数个股上涨，偏暖"
            elif adv_ratio <= 0.35:
                a_txt = "多数个股下跌，偏冷"
            else:
                a_txt = "涨跌较为均衡"
            desc_parts.append(f"涨家占比={adv_ratio:.2f}（{a_txt}）")
        if hs300_pct is not None:
            desc_parts.append(f"HS300代理涨跌={hs300_pct:.2f}%")

        detail_lines.append(
            f"- market_sentiment：得分 {_fmt_score(emo.score)}，{emo.signal}"
        )
        if desc_parts:
            detail_lines.append("  · " + "；".join(desc_parts))

    # 2.4 两融因子（多空杠杆）
    margin = factors.get("margin")
    if margin is not None:
        raw = margin.raw or {}
        rz_total = raw.get("rz_total")
        rq_total = raw.get("rq_total")
        rz_trend = raw.get("rz_trend")
        rq_trend = raw.get("rq_trend")

        desc_parts = []
        if rz_total is not None:
            desc_parts.append(f"融资余额≈{rz_total:.1f} 亿")
        if rq_total is not None:
            desc_parts.append(f"融券余额≈{rq_total:.1f} 亿")
        if rz_trend is not None:
            if rz_trend > 0:
                desc_parts.append(f"融资趋势向上（多头杠杆增加）")
            elif rz_trend < 0:
                desc_parts.append(f"融资趋势走弱（多头杠杆回落）")
            else:
                desc_parts.append(f"融资趋势基本持平")
        if rq_trend is not None:
            if rq_trend > 0:
                desc_parts.append(f"融券趋势向上（空头仓位增加）")
            elif rq_trend < 0:
                desc_parts.append(f"融券趋势走弱（空头回补）")
            else:
                desc_parts.append(f"融券趋势基本持平")

        detail_lines.append(
            f"- margin：得分 {_fmt_score(margin.score)}，{margin.signal}"
        )
        if desc_parts:
            detail_lines.append("  · " + "；".join(desc_parts))

    detail = "\n".join(detail_lines) + "\n\n"

    # ---------- 三、T+1 ~ T+3 预警（占位，未来可接因子） ----------
    alert_lines = [
        "T+1 ~ T+3 预警（预留接口）：",
        "  - 当前版本尚未接入具体 T+1~T+3 预警模型；",
        "  - 后续可基于 north_nps 趋势、两融加速度、情绪反转信号进行扩展。",
        "",
    ]
    alerts = "\n".join(alert_lines)

    return title + header + factor_overview + detail + alerts + "\n"


def save_daily_report(market: str, trade_date: str, text: str) -> str:
    """
    将报告写入 root/reports 目录。
    """
    filename = f"{market}_ashare_daily_{trade_date}.txt"
    path = os.path.join(REPORT_ROOT, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    return path
