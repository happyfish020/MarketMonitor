# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - AShare Daily Reporter
职责：
- 仅负责“结构化结果 → 文本”
- 不做任何计算、不猜测任何规则
"""

from __future__ import annotations

import os
from typing import Dict, Any, Mapping

from core.factors.factor_base import FactorResult
from core.models.risk_level import RiskLevel
from core.predictors.prediction_block import PredictionBlock
from core.utils.logger import get_logger

LOG = get_logger("Reporter.AshareDaily")


# ----------------------------------------------------------------------
# 基础格式化工具
# ----------------------------------------------------------------------
def _format_factor_block(label: str, fr: FactorResult) -> str:
    """
    V12 因子输出格式化：
    - score + level
    - details 仅作为“事实列表”展示
    """
    lines: list[str] = []
    level = fr.level.value if hasattr(fr.level, "value") else str(fr.level)
    lines.append(f"{label}: {fr.score:.2f}（{level}）")

    if isinstance(fr.details, dict) and fr.details:
        for k, v in fr.details.items():
            lines.append(f"    · {k}: {v}")

    return "\n".join(lines)


def _build_section(
    title: str,
    items: list[tuple[str, str]],
    factors: Mapping[str, FactorResult],
) -> str:
    """
    构建一个分区块：
    - title: 分区标题
    - items: [(factor_key, label), ...]
    """
    blocks: list[str] = []

    for key, label in items:
        fr = factors.get(key)
        if isinstance(fr, FactorResult):
            blocks.append(_format_factor_block(label, fr))

    if not blocks:
        return ""

    lines: list[str] = []
    lines.append(title)
    lines.append("-" * len(title))
    lines.extend(blocks)

    return "\n".join(lines)


# ----------------------------------------------------------------------
# 日报文本构建
# ----------------------------------------------------------------------
def build_daily_report_text(
    meta: Dict[str, Any],
    factors: Dict[str, FactorResult],
    prediction: PredictionBlock | None = None,
) -> str:
    trade_date = meta.get("trade_date", "未知日期")

    header = [
        f"A股日度风险综述 {trade_date}",
        "=" * 40,
        "",
    ]

    body: list[str] = []

    # 1️⃣ 情绪结构
    section = _build_section(
        "情绪结构",
        [("unified_emotion", "综合情绪因子")],
        factors,
    )
    if section:
        body.append(section)
        body.append("")

    # 2️⃣ 宏观结构
    section = _build_section(
        "宏观结构",
        [
            ("global_macro", "全球宏观金融条件"),
            ("index_global", "海外指数强弱"),
        ],
        factors,
    )
    if section:
        body.append(section)
        body.append("")

    # 3️⃣ 日内引导
    section = _build_section(
        "日内引导结构",
        [("global_lead", "全球日内引导")],
        factors,
    )
    if section:
        body.append(section)
        body.append("")

    # 4️⃣ A股核心因子
    section = _build_section(
        "A股核心因子结构",
        [
            ("north_nps", "北向资金代理"),
            ("turnover", "成交额流动性"),
            ("margin", "两融杠杆"),
            ("sector_rotation", "板块轮动"),
        ],
        factors,
    )
    if section:
        body.append(section)
        body.append("")

    # 5️⃣ 技术结构
    section = _build_section(
        "技术结构",
        [("index_tech", "指数技术面")],
        factors,
    )
    if section:
        body.append(section)
        body.append("")

    # 6️⃣ 预测总结
    if isinstance(prediction, PredictionBlock):
        lines: list[str] = []
        lines.append("综合风险判断")
        lines.append("-" * 14)

        lines.append(
            f"综合风险得分：{prediction.overall_score:.2f}（{prediction.overall_level.value}）"
        )

        # diagnostics 只做轻量展示
        if prediction.diagnostics.get("degraded"):
            lines.append("⚠️ 注意：因子不足，预测结果已降级")

        body.append("\n".join(lines))
        body.append("")

    # 清理多余空行
    while body and not body[-1].strip():
        body.pop()

    text = "\n".join(header + body).rstrip() + "\n"
    LOG.info("AshareDailyReporter: 报告文本构建完成，长度=%s", len(text))
    return text


# ----------------------------------------------------------------------
# 保存报告
# ----------------------------------------------------------------------
from datetime import datetime, timedelta, timezone


def save_daily_report_v0(market: str, trade_date, text: str) -> str:
    """
    保存日报文本到 reports/{market}/daily/
    """
    if not trade_date or str(trade_date).lower() == "none":
        bj_tz = timezone(timedelta(hours=8))
        trade_date_str = datetime.now(bj_tz).strftime("%Y-%m-%d")
    else:
        trade_date_str = str(trade_date)

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    reports_root = os.path.join(root, "reports", market, "daily")
    os.makedirs(reports_root, exist_ok=True)

    filename = f"AShares-{trade_date_str}.txt" if market.lower() == "cn" else f"{market.upper()}-{trade_date_str}.txt"
    path = os.path.join(reports_root, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
        

    LOG.info("AshareDailyReporter: 报告已保存到文件: %s", path)
    
    print("############################")
    print(text)
    print("############################")
    return path

def save_daily_report(market: str, trade_date, text: str) -> str:
    """
    保存日报文本（V12 正式版）
    - 文件名仅包含日期
    - 同日覆盖，符合“日报”语义
    """
    if trade_date and str(trade_date).lower() != "none":
        trade_date_str = str(trade_date)
    else:
        bj_tz = timezone(timedelta(hours=8))
        trade_date_str = datetime.now(bj_tz).strftime("%Y-%m-%d")

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    reports_root = os.path.join(root, "reports", market, "daily")
    os.makedirs(reports_root, exist_ok=True)

    if market.lower() == "cn":
        filename = f"AShares-{trade_date_str}.txt"
    else:
        filename = f"{market.upper()}-{trade_date_str}.txt"

    path = os.path.join(reports_root, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    LOG.info("AshareDailyReporter: 报告已保存到文件: %s", path)

    print("############################")
    print(text)
    print("############################")
 
    return path
