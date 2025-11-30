# unified_risk/core/ashare/factors/factor_history.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import csv
import os


HISTORY_FILE = Path("data/ashare/factor_history.csv")


@dataclass
class FactorRow:
    date: str
    sh_pct: float
    cyb_pct: float
    total_score: float
    a_emotion: float
    a_short: float
    a_mid: float
    a_north: float
    us_daily: float
    us_short: float
    us_mid: float


def _ensure_header(path: Path, fieldnames: List[str]) -> None:
    """如果文件不存在，则写入表头。"""
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()


def append_factor_history(result: Dict[str, Any], bj_time: datetime) -> None:
    """
    将当天因子得分 + 市场真实涨跌，写入 factor_history.csv
    - 日期：用 A 股交易日（bj_time.date()）
    - label：使用当日上证涨跌 sh_pct，将来可用来预测 T+1
    """
    raw = result.get("raw", {})
    scores = result.get("scores", {})
    unified = result.get("unified", {})

    index = raw.get("index", {})
    sh_pct = float(index.get("sh_pct", 0.0) or 0.0)
    cyb_pct = float(index.get("cyb_pct", 0.0) or 0.0)

    row = FactorRow(
        date=bj_time.date().isoformat(),
        sh_pct=sh_pct,
        cyb_pct=cyb_pct,
        total_score=float(unified.get("total", 0.0) or 0.0),
        a_emotion=float(scores.get("A_Emotion", 0.0) or 0.0),
        a_short=float(scores.get("A_Short", 0.0) or 0.0),
        a_mid=float(scores.get("A_Mid", 0.0) or 0.0),
        a_north=float(scores.get("A_North", 0.0) or 0.0),
        us_daily=float(scores.get("US_Daily", 0.0) or 0.0),
        us_short=float(scores.get("US_Short", 0.0) or 0.0),
        us_mid=float(scores.get("US_Mid", 0.0) or 0.0),
    )

    fieldnames = [
        "date",
        "sh_pct",
        "cyb_pct",
        "total_score",
        "a_emotion",
        "a_short",
        "a_mid",
        "a_north",
        "us_daily",
        "us_short",
        "us_mid",
    ]

    _ensure_header(HISTORY_FILE, fieldnames)

    with HISTORY_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerow(row.__dict__)
