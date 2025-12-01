from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unified_risk.common.config_loader import get_path
from unified_risk.core.fetchers.ashare_fetcher import AshareDataFetcher
from unified_risk.common.logging_utils import log_info

BJ_TZ = timezone(timedelta(hours=8))

def is_trading_day(dt: datetime) -> bool:
    return dt.weekday() < 5

def auto_flag_path(date_str: str) -> Path:
    return get_path("day_cache_dir")/date_str/"auto_refresh.done"

def has_auto_flag(date_str:str)->bool:
    return auto_flag_path(date_str).exists()

def write_auto_flag(date_str:str):
    auto_flag_path(date_str).touch()

def run_ashare_daily(force_refresh=False):
    bj_now=datetime.now(BJ_TZ)
    date_str=bj_now.strftime("%Y%m%d")
    hour=bj_now.hour

    log_info(f"[AShareDaily] Run at {bj_now}")

    trading=is_trading_day(bj_now)

    # -------- 自动刷新（19:00后执行一次） --------
    auto_force=False
    if trading and hour>=19 and not has_auto_flag(date_str):
        auto_force=True
        write_auto_flag(date_str)
        log_info("[AUTO] 19:00 auto refresh")

    final_force = bool(force_refresh or auto_force)

    # -------- 修正后的 DB 覆盖逻辑（v9.5.1） --------
    if not trading:
        overwrite_db=False
    else:
        if 9 <= hour < 19:
            overwrite_db=True
        elif hour >= 19:
            overwrite_db = bool(force_refresh or auto_force)
        else:
            overwrite_db=False

    fetcher=AshareDataFetcher()
    snapshot=fetcher.prepare_daily_market_snapshot(
        bj_now=bj_now,
        force=final_force,
        overwrite_db_today=overwrite_db
    )

    return {
        "meta":{
            "bj_time":bj_now.isoformat(),
            "force":final_force,
            "overwrite_db":overwrite_db,
            "trading":trading
        },
        "snapshot":snapshot
    }
