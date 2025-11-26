
import json
from datetime import datetime, time
from typing import Any, Dict, Optional
from pytz import timezone

from unifiedrisk.utils.paths import get_data_dir, ensure_dir

BJ_TZ = timezone("Asia/Shanghai")

DATA_DIR = get_data_dir()
CACHE_DIR = DATA_DIR / "cache" / "index_turnover"
DEFAULT_DIR = DATA_DIR / "default"
ensure_dir(CACHE_DIR)
ensure_dir(DEFAULT_DIR)

DEFAULT_FILE = DEFAULT_DIR / "index_turnover_default.json"

def is_trading_time(now_bj: datetime) -> bool:
    t = now_bj.time()
    return ((time(9, 30) <= t <= time(11, 30)) or (time(13, 0) <= t <= time(15, 0)))

def cache_path(date_str: str):
    return CACHE_DIR / f"{date_str}.json"

def write_turnover_cache(date_str: str, data: Dict[str, Any]) -> None:
    with open(cache_path(date_str), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_turnover_cache(date_str: str) -> Optional[Dict[str, Any]]:
    p = cache_path(date_str)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def load_latest_cache() -> Optional[Dict[str, Any]]:
    if not CACHE_DIR.exists():
        return None
    try:
        files = sorted(
            [f for f in CACHE_DIR.iterdir() if f.suffix == ".json"],
            key=lambda x: x.name,
            reverse=True,
        )
        for fp in files:
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None
    return None

def load_default_cache() -> Optional[Dict[str, Any]]:
    if DEFAULT_FILE.exists():
        try:
            with open(DEFAULT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None
