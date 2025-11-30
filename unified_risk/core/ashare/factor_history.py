from __future__ import annotations
from datetime import datetime
from pathlib import Path
import csv


HISTORY_FILE = Path("data/ashare/factor_history.csv")


def append_factor_history(result: dict, bj_time: datetime):
    """
    将 run_ashare_daily() 的结果写入日级因子历史 CSV。
    自动兼容：
    - unified 为 UnifiedScore 对象
    - scores 为 dict
    """

    raw = result.get("raw", {})
    scores = result.get("scores", {})
    unified = result.get("unified", None)

    # --- 上证涨跌 T0 ---
    index = raw.get("index", {})
    sh_pct = float(index.get("sh_pct", 0.0) or 0.0)

    # --- 统一得分（UnifiedScore 对象） ---
    if unified is not None:
        total_score = getattr(unified, "total", 0.0)
        us_daily = getattr(unified, "us_daily", 0.0)
        us_short = getattr(unified, "us_short", 0.0)
        us_mid = getattr(unified, "us_mid", 0.0)
    else:
        total_score = 0.0
        us_daily = us_short = us_mid = 0.0

    # --- A 股三因子 + 北向 ---
    a_emotion = float(scores.get("A_Emotion", 0.0) or 0.0)
    a_short = float(scores.get("A_Short", 0.0) or 0.0)
    a_mid = float(scores.get("A_Mid", 0.0) or 0.0)
    a_north = float(scores.get("A_North", 0.0) or 0.0)

    row = {
        "date": bj_time.date().isoformat(),
        "sh_pct": sh_pct,

        "total_score": total_score,

        "a_emotion": a_emotion,
        "a_short": a_short,
        "a_mid": a_mid,
        "a_north": a_north,

        "us_daily": us_daily,
        "us_short": us_short,
        "us_mid": us_mid,
    }

    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_header = not HISTORY_FILE.exists()

    with HISTORY_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if write_header:
            writer.writeheader()
        writer.writerow(row)
