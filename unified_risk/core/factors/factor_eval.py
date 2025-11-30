# unified_risk/core/ashare/factors/factor_eval.py

from __future__ import annotations
from pathlib import Path
from typing import List, Dict

import pandas as pd
import numpy as np


HISTORY_FILE = Path("data/ashare/factor_history.csv")


def _load_history() -> pd.DataFrame:
    """读取历史因子 CSV，并构造 T+1 收益标签。"""
    if not HISTORY_FILE.exists():
        raise FileNotFoundError(f"history file not found: {HISTORY_FILE}")

    df = pd.read_csv(HISTORY_FILE)

    # 确保按日期排序
    df = df.sort_values("date").reset_index(drop=True)

    # 构造 label: sh_pct_next = 下一交易日上证涨跌
    df["sh_pct_next"] = df["sh_pct"].shift(-1)
    # 也可以用二分类标签（涨/跌）
    df["label_up"] = (df["sh_pct_next"] > 0).astype(int)

    # 去掉最后一天（因为没有 next）
    df = df.iloc[:-1].copy()
    return df


def _calc_ic(df: pd.DataFrame, factor_col: str, ret_col: str) -> float:
    """简单 Pearson IC."""
    return df[[factor_col, ret_col]].corr().iloc[0, 1]


def _calc_rank_ic(df: pd.DataFrame, factor_col: str, ret_col: str) -> float:
    """Rank IC."""
    return df[[factor_col, ret_col]].rank().corr().iloc[0, 1]


def _calc_hit_rate(df: pd.DataFrame, factor_col: str, label_col: str, top_q: float = 0.3, bottom_q: float = 0.3) -> Dict[str, float]:
    """
    计算：
      - 因子最高分组（top_q）中，第二天上涨的比例
      - 因子最低分组（bottom_q）中，第二天下跌的比例
    """
    n = len(df)
    k_top = int(n * top_q)
    k_bottom = int(n * bottom_q)

    df_sorted = df.sort_values(factor_col)

    bottom = df_sorted.head(k_bottom)
    top = df_sorted.tail(k_top)

    # label_up = 1 表示次日上涨
    top_hit = top["label_up"].mean()       # 高分 → 次日上涨的概率
    bottom_hit = 1.0 - bottom["label_up"].mean()  # 低分 → 次日下跌的概率 = 1 - 上涨概率

    return {
        "top_group_up_rate": float(top_hit),
        "bottom_group_down_rate": float(bottom_hit),
    }


def evaluate_factors() -> pd.DataFrame:
    """
    对 CSV 中的所有因子做一个汇总评估：
      - Pearson IC
      - Rank IC
      - Top30% / Bottom30% 的胜率
    """
    df = _load_history()

    factor_cols: List[str] = [
        "total_score",
        "a_emotion",
        "a_short",
        "a_mid",
        "a_north",
        "us_daily",
        "us_short",
        "us_mid",
    ]

    rows = []
    for col in factor_cols:
        ic = _calc_ic(df, col, "sh_pct_next")
        ric = _calc_rank_ic(df, col, "sh_pct_next")
        hr = _calc_hit_rate(df, col, "label_up", top_q=0.3, bottom_q=0.3)

        rows.append({
            "factor": col,
            "IC": ic,
            "RankIC": ric,
            "Top30%_UpRate": hr["top_group_up_rate"],
            "Bottom30%_DownRate": hr["bottom_group_down_rate"],
        })

    result = pd.DataFrame(rows)
    return result
