# core/reporters/cn/ashare_daily_reporter.py
"""
UnifiedRisk V12 - AShare Daily Reporter (统一情绪结构版)
"""

from __future__ import annotations

import os
from typing import Dict, Any

from core.models.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Reporter.AshareDaily")


# ----------------------------------------------------------------------
# 因子格式化
# ----------------------------------------------------------------------
def _format_factor_block(label: str, fr) -> str:
    """
    V12 因子输出格式化：
    - fr.desc: 简短描述（字符串）
    - fr.detail: dict 或 字符串
    """

    desc = fr.desc if isinstance(fr.desc, str) else ""

    # ---- detail 处理 ----
    if isinstance(fr.detail, dict):
        detail_lines = []
        for k, v in fr.detail.items():
            detail_lines.append(f"    · {k}: {v}")
        detail = "\n".join(detail_lines)

    elif isinstance(fr.detail, str):
        detail = fr.detail.rstrip()

    else:
        detail = ""

    return (
        f"{label}: {fr.score:.2f}（{desc}）\n"
        f"{detail}\n"
    )


# ----------------------------------------------------------------------
# 日报文本构建
# ----------------------------------------------------------------------
def build_daily_report_text(
    meta: Dict[str, Any],
    factors: Dict[str, FactorResult],
    prediction_block: Dict[str, Any] | None = None,
) -> str:

    trade_date = meta.get("trade_date", "未知日期")

    header_lines = [
        f"【A股日度风险综述】{trade_date}",
        "=" * 40,
        "",
    ]

    body_parts: list[str] = []

    # --------------------------------------------------------
    # ⭐ V12 因子输出顺序（情绪合并版）
    # --------------------------------------------------------
    order = [
        ("unified_emotion", "综合情绪因子"),
        ("north_nps", "北向代理资金因子"),
        ("turnover", "成交额流动性因子"),
        ("margin", "两融杠杆因子"),
        ("index_tech", "指数技术面因子"),
        ("global_lead", "全球领衔风险因子"),
    ]

    for key, label in order:
        fr = factors.get(key)
        LOG.info(f"Report - label: {label}, found={isinstance(fr, FactorResult)}")

        if isinstance(fr, FactorResult):
            block = _format_factor_block(label, fr)
            if block:
                body_parts.append(block)

    # --------------------------------------------------------
    # T+1~T+5 预测块
    # --------------------------------------------------------
    if prediction_block:
        body_parts.append("【T+1~T+5 趋势预测】")
        desc = prediction_block.get("summary", "").strip()
        if desc:
            body_parts.append(desc + "\n")

    text = "\n".join(header_lines + body_parts).rstrip() + "\n"

    LOG.info("AshareDailyReporter: 报告文本构建完成，长度=%s 字符", len(text))
    return text


# ----------------------------------------------------------------------
# 保存报告
# ----------------------------------------------------------------------
def save_daily_report(market: str, trade_date: str, text: str) -> str:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    reports_root = os.path.join(root, "reports", market, "daily")
    os.makedirs(reports_root, exist_ok=True)

    if market.lower() == "cn":
        filename = f"AShares-{trade_date}.txt"
    else:
        filename = f"{market.upper()}-{trade_date}.txt"

    path = os.path.join(reports_root, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    LOG.info("AshareDailyReporter: 日报保存成功: %s", path)
    return path
