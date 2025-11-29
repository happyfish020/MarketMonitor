from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, time
from typing import Any, Callable

from unified_risk.common.config_loader import get_path, CACHE_CFG, SETTINGS
from unified_risk.common.logging_utils import log_info, log_warning


# ------------------------------
# Cache root
# ------------------------------
CACHE_ROOT: Path = get_path("cache_dir", "data/cache")

INTRADAY_SUB = CACHE_CFG.get("intraday_subdir", "intraday")
DAY_SUB = CACHE_CFG.get("day_subdir", "day_cache")
AK_SUB = CACHE_CFG.get("ak_subdir", "ak_cache")


# ------------------------------
# Read market hours from settings.yaml
# ------------------------------
def _get_market_hours(market: str) -> tuple[time, time]:
    mh = SETTINGS.get("market_hours", {})
    cfg = mh.get(market.lower())

    # 默认 A 股
    if not cfg:
        return time(9, 0), time(15, 0)

    open_h, open_m = map(int, cfg["open"].split(":"))
    close_h, close_m = map(int, cfg["close"].split(":"))
    return time(open_h, open_m), time(close_h, close_m)


# ------------------------------
# Intraday 判定：通用化
# ------------------------------
def _is_intraday(bj_time: datetime, market: str) -> bool:
    open_t, close_t = _get_market_hours(market)
    now_t = bj_time.time()
    return open_t <= now_t < close_t


# ------------------------------
# Cache dirs
# ------------------------------
def _trade_date_str(bj_time: datetime) -> str:
    return bj_time.strftime("%Y%m%d")


def get_day_cache_dir(bj_time: datetime) -> Path:
    d = CACHE_ROOT / DAY_SUB / _trade_date_str(bj_time)
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_intraday_cache_dir(bj_time: datetime) -> Path:
    d = CACHE_ROOT / INTRADAY_SUB / _trade_date_str(bj_time)
    d.mkdir(parents=True, exist_ok=True)
    return d


# ------------------------------
# Load / save JSON
# ------------------------------
def _load_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_warning(f"Failed to load cache {path}: {e}")
        return None


def _save_json(path: Path, data: Any):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_warning(f"Failed to save cache {path}: {e}")


# ------------------------------
# Unified C-mode cache
# ------------------------------
def get_or_fetch_json(
    name: str,
    fetch_func: Callable[[], Any],
    *,
    bj_time: datetime,
    market: str = "ashare",     # <<< 新增的市场参数
    scope: str = "auto",        # intraday / day / auto
    force_refresh: bool = False,
) -> Any:

    # 1. Decide intraday vs day_cache
    if scope == "auto":
        if _is_intraday(bj_time, market):
            scope_resolved = "intraday"
        else:
            scope_resolved = "day"
    else:
        scope_resolved = scope

    # 2. Directory
    if scope_resolved == "intraday":
        cache_dir = get_intraday_cache_dir(bj_time)
    else:
        cache_dir = get_day_cache_dir(bj_time)

    cache_path = cache_dir / name

    # 3. If exist -> hit
    if not force_refresh:
        cached = _load_json(cache_path)
        if cached is not None:
            log_info(f"[CACHE] hit {scope_resolved} {cache_path}")
            return cached

    # 4. Miss -> fetch + write
    log_info(f"[CACHE] miss {scope_resolved} {cache_path}, fetching...")
    data = fetch_func()
    _save_json(cache_path, data)
    return data
