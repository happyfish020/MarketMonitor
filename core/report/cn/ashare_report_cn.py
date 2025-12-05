# -*- coding: utf-8 -*-
"""A股日级风险报告生成器（V11.6.6，因子松耦合版）。

核心设计思想：
- 报告模块 **不再关心各因子的内部字段结构**
- 只依赖 FactorResult.ensure_report_block() 得到每个因子的文本块
- 因子内部负责封装自己的展示逻辑（report_block）
"""

import os
from datetime import datetime
from typing import Dict, Any
from core.models.factor_result import FactorResult
from core.utils.config_loader import reports_path

# 报告输出根目录统一使用 config_loader.reports_path
REPORT_ROOT = reports_path()


def build_daily_report_text(meta: Dict[str, Any], factors: Dict[str, FactorResult]) -> str:
    """构建 A 股日级风险报告全文。

    参数：
    - meta:  包含交易日期、版本号、时间戳等信息，如:
             { "market": "CN", "trade_date": "2025-12-04", "version": "UnifiedRisk_v11.6.6" }
    - factors:  各因子计算后的 FactorResult 映射，如:
             { "north_nps": FactorResult(...), "turnover": FactorResult(...), ... }
    """
    market = meta.get("market", "CN")
    trade_date = meta.get("trade_date", "")
    version = meta.get("version", "UnifiedRisk_v11.6.6")
    ts = meta.get("generated_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    title = f"UnifiedRisk A股日级风险报告 ({market})\n"
    header = (
        f"交易日：{trade_date}    生成时间：{ts}    版本：{version}\n"
        f"{'-'*72}\n"
        f"因子得分：\n"
    )

    # 由各因子自行提供 report_block，避免报告模块依赖字段
    # 为了可读性，按固定顺序输出已知核心因子，其它因子按名称排序附在后面
    preferred_order = ["north_nps", "turnover", "market_sentiment", "margin"]
    lines = []

    used_keys = set()
    for key in preferred_order:
        res = factors.get(key)
        if not res:
            continue
        lines.append(res.ensure_report_block().rstrip() + "\n")
        used_keys.add(key)

    # 其它非核心因子
    for name in sorted(k for k in factors.keys() if k not in used_keys):
        res = factors[name]
        lines.append(res.ensure_report_block().rstrip() + "\n")

    return title + header + "\n".join(lines) + "\n"


def save_daily_report(market: str, trade_date: str, text: str) -> str:
    """将报告写入 root/reports 目录。"""
    os.makedirs(REPORT_ROOT, exist_ok=True)
    filename = f"{market}_ashare_daily_{trade_date}.txt"
    path = os.path.join(REPORT_ROOT, filename)

    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    return path
