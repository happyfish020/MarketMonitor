from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    BJ_TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # pragma: no cover
    BJ_TZ = None

BJ_CLOSE = time(15, 0, 0)

def to_bj(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        if BJ_TZ is not None:
            return dt.replace(tzinfo=BJ_TZ)
        return dt
    if BJ_TZ is None:
        return dt
    return dt.astimezone(BJ_TZ)

def is_after_close_bj(dt: datetime) -> bool:
    bj = to_bj(dt)
    return bj.time() >= BJ_CLOSE

@dataclass
class OverwriteDecision:
    should_overwrite: bool
    reason: str

def decide_overwrite(now_bj: datetime, cached_at_str: Optional[str]) -> OverwriteDecision:
    now_bj = to_bj(now_bj)
    if not cached_at_str:
        return OverwriteDecision(True, "no_existing_cache")
    try:
        cached_at = datetime.fromisoformat(cached_at_str)
    except Exception:
        return OverwriteDecision(True, "cached_at_parse_failed")
    cached_at_bj = to_bj(cached_at)
    now_after_close = is_after_close_bj(now_bj)
    cached_after_close = is_after_close_bj(cached_at_bj)
    if not now_after_close:
        return OverwriteDecision(True, "intraday_always_overwrite")
    if not cached_after_close:
        return OverwriteDecision(True, "upgrade_intraday_to_afterclose")
    return OverwriteDecision(False, "afterclose_keep_first_final")
