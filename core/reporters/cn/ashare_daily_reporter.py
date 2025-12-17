# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - AShare Daily Reporter (Dev / Audit Mode)

职责：
- build_daily_report_text : 生成“给人看的”日报文本
- save_daily_report       : 将日报保存到统一 output 目录

⚠️ Dev 铁律：
- Reporter 必须显式展示 DS 级事实数据，用于链路审计
- 不参与计算、不修改因子、不补数据
"""

from __future__ import annotations

import os
from typing import Dict, Any, List

from core.utils.logger import get_logger
from core.utils.path_utils import ensure_dir
from core.utils.config_loader import load_paths

LOG = get_logger("Reporter.AShareDaily")

# ----------------------------------------------------------------------
# 路径规范（V12 冻结）
# ----------------------------------------------------------------------

_paths = load_paths()
REPORT_ROOT = _paths.get("cn_report_dir", "data/reports/cn/daily/")


# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------
def _fmt_pct(x: float, nd: int = 2) -> str:
    try:
        return f"{x * 100:.{nd}f}%"
    except Exception:
        return "N/A"


def _fmt_num(x: float, nd: int = 2) -> str:
    try:
        return f"{x:.{nd}f}"
    except Exception:
        return "N/A"


def _factor_exists(factors: Dict[str, Any], name: str) -> bool:
    return name in factors and factors[name] is not None


# ----------------------------------------------------------------------
# 1️⃣ 构建日报文本
# ----------------------------------------------------------------------
def build_daily_report_text(
    meta: Dict[str, Any],
    factors: Dict[str, Any],
    prediction: Any = None,
    snapshot: Dict[str, Any] | None = None,
) -> str:
    trade_date = meta.get("trade_date", "UNKNOWN")
    lines: List[str] = []

    snapshot = snapshot or {}

    # ==============================================================
    # Header
    # ==============================================================
    lines.append(f"📊 A股每日结构风险报告  {trade_date}")
    lines.append("")

    # ==============================================================
    # 一、结构裁决层
    # ==============================================================
    lines.append("【结构裁决】")

    if _factor_exists(factors, "participation"):
        fr = factors["participation"]
        state = fr.details.get("state", fr.level)
        lines.append(f"- Participation：{state}")
    else:
        lines.append("- Participation：DATA_NOT_CONNECTED")

    if _factor_exists(factors, "breadth"):
        fr = factors["breadth"]
        state = fr.details.get("state", fr.level)
        lines.append(f"- Breadth：{state}")
    else:
        lines.append("- Breadth：DATA_NOT_CONNECTED")

    lines.append("")

    # ==============================================================
    # 二、结构证据层（因子）
    # ==============================================================
    lines.append("【结构证据（Factor）】")

    if _factor_exists(factors, "north_nps_raw"):
        m = factors["north_nps_raw"].details or {}
        lines.append("北向代理：")
        lines.append(f"- strength_today：{_fmt_num(m.get('strength_today'))}")
        lines.append(f"- trend_5d：{_fmt_num(m.get('trend_5d'))}")
        lines.append(f"- _raw_data: {m.get('_raw_data')}")
    else:
        lines.append("北向代理：DATA_NOT_CONNECTED")
    lines.append("")

    if _factor_exists(factors, "margin"):
        m = factors["margin"].details or {}
        lines.append("两融：")
        lines.append(f"- trend_10d：{_fmt_num(m.get('trend_10d'))}")
        lines.append(f"- acc_3d：{_fmt_num(m.get('acc_3d'))}")
        lines.append(f"- _raw_data: {m.get('_raw_data')}")
    else:
        lines.append("两融：DATA_NOT_CONNECTED")
    lines.append("")

    # ==============================================================
    # 三、数据源链路审计（DS 事实层）
    # ==============================================================
    lines.append("【📌 数据源链路检查（DS Raw）】")

    # --- Breadth DS ---
    bd = snapshot.get("breadth")
    if isinstance(bd, dict):
        lines.append("Breadth DS：")
        lines.append(f"- new_low_ratio：{_fmt_pct(bd.get('new_low_ratio'))}")
        lines.append(f"- count_new_low：{bd.get('count_new_low')}")
        lines.append(f"- count_total：{bd.get('count_total')}")
    else:
        lines.append("Breadth DS：MISSING")
    lines.append("")

    # --- North Proxy DS ---
    if _factor_exists(factors, "north_nps_raw"):
        m = factors["north_nps_raw"].details or {}
        lines.append("北向代理：")
        lines.append(f"- _raw_data: {m.get('_raw_data')}")
    else:
        lines.append("北向代理：DATA_NOT_CONNECTED")
    lines.append("")

    # --- Turnover DS ---
    if _factor_exists(factors, "turnover_raw"):
        m = factors["turnover_raw"].details or {}
        lines.append("TurnOver：")
        lines.append(f"- _raw_data: {m.get('_raw_data')}")
    else:
        lines.append("TurnOver： DS：MISSING")
    lines.append("")

    # ==============================================================
    # Step-3 Evidence（新增展示：只展示，不计算、不修正）
    # ==============================================================
    lines.append("【Step-3 Evidence】")

    pred_dict: Dict[str, Any] = {}
    try:
        if prediction is None:
            pred_dict = {}
        elif hasattr(prediction, "to_dict"):
            pred_dict = prediction.to_dict()  # type: ignore[attr-defined]
        elif isinstance(prediction, dict):
            pred_dict = prediction
    except Exception:
        pred_dict = {}

    diag = {}
    try:
        diag = pred_dict.get("diagnostics") or {}
    except Exception:
        diag = {}

    if not isinstance(diag, dict) or not diag:
        lines.append("- diagnostics: N/A")
        lines.append("")
    else:
        pol = diag.get("policy", {})
        if not isinstance(pol, dict):
            pol = {}

        lines.append(f"- policy.result: {pol.get('result', 'N/A')}")
        lines.append(f"- used: {diag.get('used', [])}")
        lines.append(f"- used_in_aggregation: {diag.get('used_in_aggregation', [])}")
        lines.append(f"- missing_factors: {list((diag.get('missing_factors') or {}).keys())}")
        lines.append(f"- degraded_factors: {list((diag.get('degraded_factors') or {}).keys())}")
        lines.append(f"- raw_weight_total: {diag.get('raw_weight_total', 'N/A')}")
        lines.append(f"- normalized_weight_total: {diag.get('normalized_weight_total', 'N/A')}")
        lines.append(f"- zero_weight_used: {diag.get('zero_weight_used', [])}")

        raw_w = diag.get("raw_weights", {})
        norm_w = diag.get("normalized_weights", {})

        if isinstance(raw_w, dict) and raw_w:
            lines.append("- raw_weights:")
            for k in sorted(raw_w.keys()):
                lines.append(f"    · {k}: {raw_w.get(k)}")

        if isinstance(norm_w, dict) and norm_w:
            lines.append("- normalized_weights:")
            for k in sorted(norm_w.keys()):
                lines.append(f"    · {k}: {norm_w.get(k)}")

        lines.append("")

    # ==============================================================
    # 四、风险提示
    # ==============================================================
    lines.append("【风险与前瞻提示】")
    lines.append("- 当前为开发调试报告，DS 数据已显式展示")
    lines.append("- 结构裁决仅依赖 Breadth + Participation")
    lines.append("")

    lines.append("（本报告为结构风险监测，不构成交易建议）")

    text = "\n".join(lines)
    if not text.strip():
        LOG.warning("build_daily_report_text generated EMPTY text")

    return text


# ----------------------------------------------------------------------
# 2️⃣ 保存日报
# ----------------------------------------------------------------------
def save_daily_report(trade_date: str, text: str) -> None:
    if not text or not text.strip():
        LOG.warning("save_daily_report called with empty text")
        return

    ensure_dir(REPORT_ROOT)

    fname = f"ashare_daily_{trade_date}.txt"
    path = os.path.join(REPORT_ROOT, fname)

    try:
        print("##################################")
        print(text)
        print("##################################")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        LOG.info("Daily report saved: %s", path)
    except Exception as e:
        LOG.error("Failed to save daily report: %s", e, exc_info=True)
        raise
