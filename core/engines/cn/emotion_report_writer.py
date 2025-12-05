# -*- coding: utf-8 -*-
"""
CN Emotion Report Writer (UnifiedRisk V11, A-Share)

根据 EmotionEngine 的结果 + 原始指标，
生成详细的《A股情绪监控报告（V11 FULL）》文本。
"""

from __future__ import annotations
from datetime import datetime, date
from typing import Any, Mapping


def _fmt_datetime(dt: Any) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(dt, date):
        return dt.strftime("%Y-%m-%d 00:00:00")
    if isinstance(dt, str) and dt.strip():
        return dt
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _fmt_date(d: Any) -> str:
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    if isinstance(d, datetime):
        return d.date().strftime("%Y-%m-%d")
    if isinstance(d, str) and d.strip():
        return d
    return datetime.now().strftime("%Y-%m-%d")


def _fmt_pct(x: Any, digits: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return "0.00"
    fmt = f"%.{digits}f"
    return fmt % v


def _fmt_score(x: Any) -> str:
    try:
        v = float(x)
    except Exception:
        v = 0.0
    return f"{v:.2f}"


def _render_level_emoji(level: str) -> str:
    level = (level or "").lower()
    if level == "panic":
        return "🟥 Panic"
    if level == "risk-off":
        return "🟧 Risk-Off"
    if level == "neutral":
        return "🟨 Neutral"
    if level == "risk-on":
        return "🟩 Risk-On"
    if level == "euphoria":
        return "🟦 Euphoria"
    return level or "Unknown"


def format_cn_ashare_emotion_report(data: Mapping[str, Any]) -> str:
    """
    data 结构约定：
        {
            "generated_at": datetime/str,
            "trade_date": date/str,
            "emotion": {  # EmotionEngine 输出
                "EmotionScore": float,
                "EmotionLevel": str,
                "IndexScore": ...,
                "VolumeScore": ...,
                ...
                "IndexLabel": "...",
                "VolumeLabel": "...,
                ...
                "raw": {  # EmotionEngine raw 字段
                    "index_pct": ...,
                    "volume_change_pct": ...,
                    "breadth_adv": ...,
                    "breadth_total": ...,
                    "breadth_adv_ratio": ...（可选）,
                    "north_net_flow": ...,
                    "main_force_net_flow": ...,
                    "futures_basis_pct": ...,
                    "ivx_change_pct": ...,
                    "limit_up_count": ...,
                    "limit_down_count": ...,
                }
            }
        }
    """
    generated_at = _fmt_datetime(data.get("generated_at"))
    trade_date = _fmt_date(data.get("trade_date"))

    emo = data.get("emotion") or {}
    raw = emo.get("raw") or {}

    es = emo.get("EmotionScore", 50.0)
    level = emo.get("EmotionLevel", "Neutral")

    idx_s = emo.get("IndexScore", 50.0)
    vol_s = emo.get("VolumeScore", 50.0)
    brd_s = emo.get("BreadthScore", 50.0)
    nf_s = emo.get("NorthboundScore", 50.0)
    mf_s = emo.get("MainForceScore", 50.0)
    der_s = emo.get("DerivativeScore", 50.0)
    lim_s = emo.get("LimitScore", 50.0)

    idx_lbl = emo.get("IndexLabel", "")
    vol_lbl = emo.get("VolumeLabel", "")
    brd_lbl = emo.get("BreadthLabel", "")
    nf_lbl = emo.get("NorthLabel", "")
    mf_lbl = emo.get("MainForceLabel", "")
    der_lbl = emo.get("DerivativeLabel", "")
    lim_lbl = emo.get("LimitLabel", "")

    # 原始指标
    index_pct = raw.get("index_pct", 0.0)
    volume_chg = raw.get("volume_change_pct", 0.0)
    adv = int(raw.get("breadth_adv", 0) or 0)
    total = int(raw.get("breadth_total", 0) or 0)
    adv_ratio = raw.get("breadth_adv_ratio", None)
    north = raw.get("north_net_flow", 0.0)
    main_f = raw.get("main_force_net_flow", 0.0)
    basis = raw.get("futures_basis_pct", 0.0)
    ivx_chg = raw.get("ivx_change_pct", 0.0)
    up_lim = int(raw.get("limit_up_count", 0) or 0)
    down_lim = int(raw.get("limit_down_count", 0) or 0)

    # ============= 报告正文 =============
    lines: list[str] = []

    lines.append("=== A股情绪监控报告（V11 FULL） ===")
    lines.append(f"生成时间：{generated_at}")
    lines.append(f"交易日：{trade_date}")
    lines.append("")
    lines.append(f"情绪总分：{_fmt_score(es)} / 100")
    lines.append(f"情绪等级：{_render_level_emoji(level)}")
    lines.append("")

    # 一、简要总览
    lines.append("一、今日情绪总览")
    lines.append("----------------")
    lv = (level or "").lower()
    if lv == "panic":
        lines.append("今天属于：极端恐慌（Panic）阶段，存在无差别抛售风险。")
    elif lv == "risk-off":
        lines.append("今天属于：Risk-Off（资金撤退/情绪低迷）阶段，资金整体偏防守。")
    elif lv == "neutral":
        lines.append("今天属于：Neutral（冷淡/观望）阶段，更接近“中度悲观、弱反弹修复期”，并非恐慌盘。")
    elif lv == "risk-on":
        lines.append("今天属于：Risk-On（风险偏好回暖）阶段，趋势交易环境较友好。")
    else:
        lines.append("今天属于：Euphoria（情绪亢奋）阶段，需警惕顶部风险。")
    lines.append("")

    # 二、七大情绪因子
    lines.append("二、七大情绪因子明细")
    lines.append("----------------------")

    lines.append(f"1）指数波动（IndexScore = {idx_s:.0f}）")
    lines.append(f"   · 上证/核心指数涨跌幅：{_fmt_pct(index_pct)}% —— {idx_lbl}")
    lines.append("")

    lines.append(f"2）成交量动能（VolumeScore = {vol_s:.0f}）")
    lines.append(f"   · 两市总成交量较昨日变化：{_fmt_pct(volume_chg)}% —— {vol_lbl}")
    lines.append("")

    lines.append(f"3）市场宽度 Breadth（BreadthScore = {brd_s:.0f}）")
    if adv_ratio is not None:
        lines.append(
            f"   · 上涨家数：{adv} / 总数≈{total}，上涨占比≈{_fmt_pct(adv_ratio * 100)}% —— {brd_lbl}"
        )
    else:
        lines.append(f"   · 上涨家数：{adv} —— {brd_lbl}")
    lines.append("")

    lines.append(f"4）北向资金（NorthboundScore = {nf_s:.0f}）")
    lines.append(f"   · 北向净流入：{_fmt_pct(north)} 亿元 —— {nf_lbl}")
    lines.append("")

    lines.append(f"5）主力资金（MainForceScore = {mf_s:.0f}）")
    lines.append(f"   · 主力资金净流入：{_fmt_pct(main_f)} 亿元 —— {mf_lbl}")
    lines.append("")

    lines.append(f"6）衍生品情绪（DerivativeScore = {der_s:.0f}）")
    lines.append(
        f"   · 股指期货升贴水：{_fmt_pct(basis)}%；iVX 单日变化：{_fmt_pct(ivx_chg)}% —— {der_lbl}"
    )
    lines.append("")

    lines.append(f"7）涨跌停结构（LimitScore = {lim_s:.0f}）")
    lines.append(f"   · 涨停家数：{up_lim}；跌停家数：{down_lim} —— {lim_lbl}")
    lines.append("")

    # 三、是否恐慌日？按照你定义的 5 条标准
    lines.append("三、是否属于“极度低迷 / 恐慌日”？")
    lines.append("------------------------------")
    lines.append("你的标准：满足以下 5 条中的 3 条以上 → 才算真正恐慌日：")
    lines.append("  1）指数放量急跌（>1.5% 且巨量）；")
    lines.append("  2）上涨家数 < 800（全面杀跌）；")
    lines.append("  3）北向流出 > 80–120 亿；")
    lines.append("  4）行业板块 90% 以上翻绿，无明显避险方向；")
    lines.append("  5）iVX 波动率大幅上升（资金明显对冲）。")
    lines.append("")
    lines.append("根据以上数据，今天：")
    lines.append(f"  · 指数：{_fmt_pct(index_pct)}%，是否急跌：{'是' if index_pct <= -1.5 else '否'}；")
    lines.append(f"  · 上涨家数：{adv}（是否 < 800：{'是' if adv < 800 else '否'}）；")
    lines.append(f"  · 北向：{_fmt_pct(north)} 亿（是否 > 80 亿流出：{'是' if north <= -80 else '否'}）；")
    lines.append(f"  · iVX 变化：{_fmt_pct(ivx_chg)}%（是否大幅上升：{'是' if ivx_chg >= 10 else '否'}）；")
    lines.append("")
    lines.append("👉 结论：除非以上指标有至少 3 项同时满足，否则按你的定义不属于“恐慌日”。")
    lines.append("")

    # 四、交易环境总结
    lines.append("四、交易环境总结")
    lines.append("----------------")
    if lv in ("panic", "risk-off"):
        lines.append("整体偏 Risk-Off / 冷淡：")
        lines.append("  · 更适合减仓高贝塔、保留核心资产；")
        lines.append("  · 短线逆势抄底胜率不高，仓位宜保守；")
    elif lv == "neutral":
        lines.append("整体偏 Neutral / 冷淡：")
        lines.append("  · 更像“没什么人参与的弱修复”，不是恐慌盘；")
        lines.append("  · 主线仍在局部（如 AI / 科技），适合精选标的而非全市场博反弹；")
    elif lv == "risk-on":
        lines.append("整体偏 Risk-On：")
        lines.append("  · 趋势交易环境友好，可以适度提高进攻仓位，但注意个股分化；")
    else:
        lines.append("整体偏 Euphoria：")
        lines.append("  · 情绪亢奋，需警惕阶段性顶部风险，不宜盲目追高。")

    return "\n".join(lines)


def write_cn_ashare_emotion_report(
    output_path: str,
    data: Mapping[str, Any],
    encoding: str = "utf-8",
) -> None:
    """
    如果你未来想单独输出情绪报告，可以调用本函数。
    在当前 V11.6.2 中，我们是在 ashare_daily_engine 里把情绪报告 append 进同一个文本文件。
    """
    content = format_cn_ashare_emotion_report(data)
    with open(output_path, "w", encoding=encoding) as f:
        f.write(content)
