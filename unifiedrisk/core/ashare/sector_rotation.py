# -*- coding: utf-8 -*-
"""
UnifiedRisk v5.0.2 - Sector Rotation View
-----------------------------------------
基于行业主力资金 + 涨跌幅，计算行业强弱 & 轮动视图。
"""

from typing import List, Dict, Any
import math

import pandas as pd

from unifiedrisk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.AShare.SectorRotation")


def _safe_z(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    std = s.std()
    if std == 0 or math.isclose(std, 0.0):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / std


def build_sector_rotation_view(
    raw_sectors: List[Dict[str, Any]],
    top_n: int = 5,
) -> Dict[str, Any]:
    """
    行业轮动视图（当前基于“当天 snapshot”，未来可扩展 1/3/5 日趋势）.

    参数:
        raw_sectors: 行业原始列表，如:
            [
              {"f14": "电力设备", "f62": 123.4, "f3": 2.31, ...},
              ...
            ]
    返回:
        {
          "table": List[Dict],
          "top_strong": [...],
          "top_weak":   [...],
          "summary_lines": [...],
        }
    """
    if not raw_sectors:
        LOG.warning("[SectorRotation] raw_sectors 为空")
        return {
            "table": [],
            "top_strong": [],
            "top_weak": [],
            "summary_lines": ["行业轮动：今日无有效行业资金数据。"],
        }

    df = pd.DataFrame(raw_sectors)

    # 兼容不同字段名：行业名称 & 主力净流入 & 涨跌幅
    name_col = next((c for c in ["f14", "name", "行业名称"] if c in df.columns), None)
    main_col = next((c for c in ["f62", "main_net", "主力净流入"] if c in df.columns), None)
    chg_col = next((c for c in ["f3", "pct_chg", "changepercent", "涨跌幅"] if c in df.columns), None)

    if name_col is None or main_col is None:
        LOG.warning("[SectorRotation] 缺少 name/main 列, columns=%s", list(df.columns))
        return {
            "table": [],
            "top_strong": [],
            "top_weak": [],
            "summary_lines": ["行业轮动：数据格式异常，无法计算行业强度。"],
        }

    table = pd.DataFrame(
        {
            "name": df[name_col],
            "main_net": pd.to_numeric(df[main_col], errors="coerce").fillna(0.0),
        }
    )

    if chg_col is not None:
        table["change_pct"] = pd.to_numeric(df[chg_col], errors="coerce").fillna(0.0)
    else:
        table["change_pct"] = 0.0

    # 强度 = 主力净流入 z-score + 行业涨跌幅 z-score
    main_z = _safe_z(table["main_net"])
    chg_z = _safe_z(table["change_pct"])
    table["strength"] = main_z + chg_z

    table = table.sort_values("strength", ascending=False).reset_index(drop=True)
    table["rank"] = table.index + 1

    top_strong = table.head(top_n).to_dict(orient="records")
    top_weak = table.tail(top_n).sort_values("strength").to_dict(orient="records")

    summary_lines = []

    if top_strong:
        parts = [
            f"{row['rank']}. {row['name']} (主力≈{row['main_net']:.1f}, 涨跌≈{row['change_pct']:.2f}%)"
            for row in top_strong
        ]
        summary_lines.append("📈 行业强势榜：" + "；".join(parts))

    if top_weak:
        parts = [
            f"{row['name']} (主力≈{row['main_net']:.1f}, 涨跌≈{row['change_pct']:.2f}%)"
            for row in top_weak
        ]
        summary_lines.append("📉 行业弱势榜：" + "；".join(parts))

    if not summary_lines:
        summary_lines.append("行业轮动：今日行业强弱分布不明显。")

    return {
        "table": table.to_dict(orient="records"),
        "top_strong": top_strong,
        "top_weak": top_weak,
        "summary_lines": summary_lines,
    }
