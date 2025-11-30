from __future__ import annotations
from datetime import datetime
from pathlib import Path
import csv


HISTORY_FILE = Path("data/ashare/factor_history.csv")


def append_factor_history(result: dict, bj_time: datetime):
    """
    将本次 run_ashare_daily 的因子分数映射成一行 CSV
    """
    raw = result.get("snapshot", {})
    emo = result.get("emotion")
    short = result.get("short")
    mid = result.get("mid")
    north = result.get("north")
    unified = result.get("unified")

    # 交易日期
    date_str = bj_time.date().isoformat()

    # 当天上证涨跌幅作为 Label(T0)，第二天预测训练集使用 T+1
    sh_pct = raw.get("sh_pct", 0.0)

    row = {
        "date": date_str,
        "sh_pct": sh_pct,
        "total_score": unified.score if unified else 0,
        "a_emotion": emo.score if emo else 0,
        "a_short": short.score if short else 0,
        "a_mid": mid.score if mid else 0,
        "a_north": north.score if north else 0,
        "us_daily": unified.us_daily if unified else 0,
        "us_short": unified.us_short if unified else 0,
        "us_mid": unified.us_mid if unified else 0,
    }

    # 写表头
    write_header = not HISTORY_FILE.exists()
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)

    with HISTORY_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)
