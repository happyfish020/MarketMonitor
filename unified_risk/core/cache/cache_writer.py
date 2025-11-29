from __future__ import annotations
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from unified_risk.core.cache.write_policy import decide_overwrite

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ASHARE_DATA_ROOT = PROJECT_ROOT / "data" / "ashare"
GLOBAL_DATA_ROOT = PROJECT_ROOT / "data" / "global"

def _ensure_date(d: date | datetime) -> date:
    if isinstance(d, datetime):
        return d.date()
    return d

def _ensure_dir(root: Path, d: date) -> Path:
    dstr = d.strftime("%Y%m%d")
    day_dir = root / dstr
    day_dir.mkdir(parents=True, exist_ok=True)
    return day_dir

def _normalize_date_str_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    v = payload.get("date")
    if isinstance(v, str) and len(v) >= 8:
        return v[:10]
    return None

def _inject_or_validate_date(payload: Dict[str, Any], d: date) -> Dict[str, Any]:
    target_str = d.strftime("%Y-%m-%d")
    inner = _normalize_date_str_from_payload(payload)
    if inner is None:
        payload = dict(payload)
        payload["date"] = target_str
        return payload
    inner_norm = inner[:10]
    if inner_norm != target_str:
        raise ValueError(
            f"[CacheWriter] payload.date({inner_norm}) != target date({target_str})"
        )
    return payload

def _write_json(
    root: Path,
    d: date | datetime,
    name: str,
    payload: Dict[str, Any],
    force_overwrite: bool = False,
) -> Tuple[Path, Dict[str, Any]]:
    d = _ensure_date(d)
    day_dir = _ensure_dir(root, d)
    path = day_dir / f"{name}.json"
    if path.exists() and not force_overwrite:
        with open(path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        print(f"[CacheWriter] Use cached file (no overwrite): {path}")
        return path, existing
    payload = _inject_or_validate_date(payload, d)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[CacheWriter] Write JSON{' (force overwrite)' if force_overwrite else ''}: {path}")
    return path, payload

def _smart_write_json(
    root: Path,
    d: date | datetime,
    name: str,
    payload: Dict[str, Any],
    now_bj: datetime,
    cached_at_key: str = "cached_at",
) -> Tuple[Path, Dict[str, Any]]:
    d = _ensure_date(d)
    day_dir = _ensure_dir(root, d)
    path = day_dir / f"{name}.json"
    existing = None
    existing_cached_at: Optional[str] = None
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        existing_cached_at = existing.get(cached_at_key)
    decision = decide_overwrite(now_bj, existing_cached_at)
    if existing is not None and not decision.should_overwrite:
        print(
            f"[CacheWriter] Keep cached file ({decision.reason}): {path}, "
            f"cached_at={existing_cached_at}"
        )
        return path, existing
    payload2 = dict(payload)
    payload2[cached_at_key] = now_bj.strftime("%Y-%m-%d %H:%M:%S")
    return _write_json(root, d, name, payload2, force_overwrite=True)

def write_ashare_turnover(d: date | datetime, payload: Dict[str, Any], force_overwrite: bool = False):
    return _write_json(ASHARE_DATA_ROOT, d, "turnover", payload, force_overwrite)

def write_ashare_margin(d: date | datetime, payload: Dict[str, Any], force_overwrite: bool = False):
    return _write_json(ASHARE_DATA_ROOT, d, "lsdb", payload, force_overwrite)

def write_ashare_northbound(d: date | datetime, payload: Dict[str, Any], force_overwrite: bool = False):
    return _write_json(ASHARE_DATA_ROOT, d, "northbound", payload, force_overwrite)

def smart_write_ashare_turnover(d: date | datetime, payload: Dict[str, Any], now_bj: datetime):
    return _smart_write_json(ASHARE_DATA_ROOT, d, "turnover", payload, now_bj)

def smart_write_ashare_margin(d: date | datetime, payload: Dict[str, Any], now_bj: datetime):
    return _smart_write_json(ASHARE_DATA_ROOT, d, "lsdb", payload, now_bj)

def smart_write_ashare_northbound(d: date | datetime, payload: Dict[str, Any], now_bj: datetime):
    return _smart_write_json(ASHARE_DATA_ROOT, d, "northbound", payload, now_bj)
