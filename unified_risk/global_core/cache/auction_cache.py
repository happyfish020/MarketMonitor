# global_risk/cache/auction_cache.py
from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, Optional

from ..config import REPORT_DIR
from ..utils.time_utils import now_bj
from ..utils.logging_utils import setup_logger

logger = setup_logger("auction_cache")

# 缓存目录，例如 MarketMonitor/reports/auction_cache
CACHE_DIR: Path = REPORT_DIR / "auction_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ------------------------------------------------------------
# 工具函数：统一将 datetime / date / None → date
# ------------------------------------------------------------
def _normalize_date(t: Optional[datetime | date]) -> date:
    """
    将输入统一规范化为 date 类型：
      - None → 今天
      - datetime → date
      - date → 原样返回
    """
    if t is None:
        return now_bj().date()
    if isinstance(t, datetime):
        return t.date()
    return t   # 已经是 date


# ------------------------------------------------------------
# 判断是否有竞价缓存
# ------------------------------------------------------------
def has_auction_cache(target_time: Optional[datetime | date] = None) -> bool:
    """
    判断指定日期（或今天）是否存在竞价缓存。
    文件名格式：auction_YYYYMMDD.json
    """
    d = _normalize_date(target_time)
    date_str = d.strftime("%Y%m%d")
    path = CACHE_DIR / f"auction_{date_str}.json"
    return path.exists()


# ------------------------------------------------------------
# 写入竞价缓存（仅 09:15–09:25 有效）
# ------------------------------------------------------------
def write_auction_cache(raw_snapshot: Dict[str, Any],
                        bj_time: Optional[datetime] = None) -> Optional[Path]:
    """
    仅在 09:15–09:25（北京时间）写入竞价缓存。
    写入文件：auction_YYYYMMDD.json
    """
    if bj_time is None:
        bj_time = now_bj()

    h, m = bj_time.hour, bj_time.minute
    in_window = (h == 9 and 15 <= m <= 25)

    if not in_window:
        logger.info(
            "Skip auction cache: %s not in window(09:15–09:25).",
            bj_time.strftime("%Y-%m-%d %H:%M:%S"),
        )
        return None

    date_str = bj_time.strftime("%Y%m%d")
    path = CACHE_DIR / f"auction_{date_str}.json"

    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(
                raw_snapshot or {},
                f,
                ensure_ascii=False,
                indent=2,
                default=str  # 避免 datetime/np 类型出错
            )
        logger.info("Auction cache written to %s", path)
        return path
    except Exception as e:
        logger.warning("Failed to write auction cache: %s", e)
        return None


# ------------------------------------------------------------
# 读取竞价缓存
# ------------------------------------------------------------
def read_auction_cache(target_time: Optional[datetime | date] = None) -> Dict[str, Any]:
    """
    读取竞价缓存。
      - None → 默认读取今天
      - datetime/date → 读取对应日期的缓存
    """
    d = _normalize_date(target_time)
    date_str = d.strftime("%Y%m%d")
    path = CACHE_DIR / f"auction_{date_str}.json"

    if not path.exists():
        logger.info("Auction cache not found: %s", path)
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to read auction cache: %s", e)
        return {}
