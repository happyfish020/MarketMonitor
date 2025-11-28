
from __future__ import annotations

from typing import Any, Dict, List
from pathlib import Path
from datetime import datetime
import json

from .logging_utils import setup_logger
from .json_utils import to_json
from ..config import HISTORY_DIR

logger = setup_logger(__name__)

DAILY_DIR = HISTORY_DIR / "ashare_daily"
DAILY_DIR.mkdir(parents=True, exist_ok=True)


def save_daily_snapshot(bj_time: datetime, snapshot_raw: Dict[str, Any], scores: Dict[str, Any]) -> Path:
    """保存当日快照（仅按日期一份，覆盖写入）。"""
    date_str = bj_time.strftime("%Y-%m-%d")
    path = DAILY_DIR / f"{date_str}.json"
    payload = {
        "date": date_str,
        "snapshot": snapshot_raw,
        "scores": scores,
    }
    text = to_json(payload, ensure_ascii=False)
    path.write_text(text, encoding="utf-8")
    logger.debug("Saved daily snapshot to %s", path)
    return path


def load_recent_snapshots(max_days: int = 5) -> List[Dict[str, Any]]:
    """按日期倒序加载最近 max_days 天的 ashare_daily 快照。"""
    if max_days <= 0:
        return []
    if not DAILY_DIR.exists():
        return []
    items = []
    for p in DAILY_DIR.glob("*.json"):
        try:
            date_str = p.stem  # YYYY-MM-DD
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            continue
        items.append((dt, p))
    items.sort(key=lambda x: x[0], reverse=True)
    result: List[Dict[str, Any]] = []
    for dt, p in items[:max_days]:
        try:
            text = p.read_text(encoding="utf-8")
            obj = json.loads(text)
            result.append(obj)
        except Exception as e:
            logger.warning("Failed to load history file %s: %s", p, e)
    return result
