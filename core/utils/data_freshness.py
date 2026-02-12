# core/utils/data_freshness.py
# UnifiedRisk V12 - Data Freshness governance helpers (UNIFIED / COMPAT)
#
# This file intentionally provides BOTH public APIs expected by different engine versions:
#   - compute_data_freshness(trade_date, slots) -> dict
#   - inject_asof_fields(trade_date, slots) -> dict
#
# Behavior:
# - Collect asof dates from:
#   (1) block['meta']['asof'|'as_of'|'max_date'|'trade_date'|'data_date']
#   (2) any '*_asof' keys found in nested dicts (e.g. margin_asof)
#   (3) as a last resort, infer max date from common row arrays (window/rows/series/items/details/data)
# - Classify into L1/L2/L3 via heuristic path matching (configurable later).
# - Reporting is conservative:
#     if missing_asof_paths > 0 => freshness_incomplete (never prints ✅ all fresh)
#
from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import re


def _parse_date(x: Any) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _guess_level(path: str) -> int:
    p = (path or "").lower()
    if any(k in p for k in ("market_overview", "close_facts", "market_close_facts")):
        return 3
    if any(k in p for k in ("breadth", "pct_above", "new_low", "new_high", "adv_dec", "limit_up", "limit_down")):
        return 3
    if any(k in p for k in ("index", "idx", "hs300", "zz500", "sh000", "sz399")):
        return 3

    if any(k in p for k in ("margin", "rz_", "rq_", "financing", "two_financing", "two_finance",
                            "options", "iv", "skew", "basis", "futures_basis")):
        return 2

    if any(k in p for k in ("watchlist", "sector_proxy", "rew", "etf", "intraday", "spot_sync", "crowding")):
        return 1

    return 1


def _walk(obj: Any, path: str, nodes: List[Tuple[str, Any]]) -> None:
    if isinstance(obj, dict):
        nodes.append((path, obj))
        for k, v in obj.items():
            _walk(v, f"{path}.{k}" if path else str(k), nodes)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:50]):
            _walk(v, f"{path}[{i}]", nodes)


def _collect_asof_from_dict(path: str, d: Dict[str, Any], out: Dict[str, date]) -> None:
    meta = d.get("meta")
    if isinstance(meta, dict):
        for k in ("asof", "as_of", "max_date", "trade_date", "data_date"):
            v = meta.get(k)
            if v:
                dd = _parse_date(v)
                if dd:
                    out[path] = max(out.get(path, dd), dd) if path in out else dd
                    return

    for k in ("asof", "as_of", "max_date"):
        v = d.get(k)
        if v:
            dd = _parse_date(v)
            if dd:
                out[path] = max(out.get(path, dd), dd) if path in out else dd
                return

    for k, v in d.items():
        if isinstance(k, str) and k.lower().endswith("_asof") and v:
            dd = _parse_date(v)
            if dd:
                out[path] = max(out.get(path, dd), dd) if path in out else dd


def inject_asof_fields(trade_date: str, slots: Dict[str, Any]) -> Dict[str, Any]:
    injected: List[Dict[str, Any]] = []
    missing: List[str] = []
    nodes: List[Tuple[str, Any]] = []
    _walk(slots, "", nodes)

    for p, obj in nodes:
        if not isinstance(obj, dict):
            continue
        meta = obj.get("meta")
        if not isinstance(meta, dict):
            continue
        if meta.get("asof") or meta.get("as_of"):
            continue

        candidates = []
        for k in ("window", "rows", "series", "items", "details", "data"):
            v = obj.get(k)
            if isinstance(v, list) and v:
                candidates.append(v)

        max_d: Optional[date] = None
        for lst in candidates:
            for row in lst[:500]:
                if not isinstance(row, dict):
                    continue
                for dk in ("date", "trade_date", "DATA_DATE", "data_date"):
                    if dk in row and row.get(dk) is not None:
                        dd = _parse_date(row.get(dk))
                        if dd and (max_d is None or dd > max_d):
                            max_d = dd

        if max_d:
            meta["asof"] = max_d.isoformat()
            injected.append({"path": p, "asof": meta["asof"]})
        else:
            missing.append(p)

    return {
        "asof": trade_date,
        "meta": {"trade_date": trade_date, "injected_count": len(injected), "missing_count": len(missing)},
        "injected": injected[:80],
        "missing": missing[:80],
    }


def compute_data_freshness(trade_date: str, slots: Dict[str, Any]) -> Dict[str, Any]:
    td = _parse_date(trade_date)

    nodes: List[Tuple[str, Any]] = []
    _walk(slots, "", nodes)

    asof_map: Dict[str, date] = {}
    for p, obj in nodes:
        if isinstance(obj, dict):
            _collect_asof_from_dict(p, obj, asof_map)

    stale_items: List[Dict[str, Any]] = []
    missing_asof_paths: List[str] = []

    # P0-FIX-C: Treat explicit error/warning markers as freshness-incomplete.
    error_paths: List[str] = []

    for p, obj in nodes:
        if not isinstance(obj, dict):
            continue
        meta = obj.get("meta")
        if isinstance(meta, dict):
            if not any(meta.get(k) for k in ("asof", "as_of", "max_date", "trade_date", "data_date")):
                if p.count(".") <= 3:
                    missing_asof_paths.append(p)


    # Scan for embedded errors/warnings/data_status flags
    for p, obj in nodes:
        if isinstance(obj, dict):
            # 1) warnings list contains error:*
            w = obj.get("warnings")
            if isinstance(w, list):
                for it in w:
                    if isinstance(it, str) and it.lower().startswith("error:"):
                        error_paths.append(p)
                        break
            # 2) explicit error string fields
            for k in ("error", "errors", "exception"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    error_paths.append(p)
                    break
            # 3) data_status indicates partial/error
            ds = obj.get("data_status")
            if isinstance(ds, str) and ds.upper() in ("ERROR", "PARTIAL", "MISSING", "NA"):
                error_paths.append(p)
    if td is not None:
        for p, d in sorted(asof_map.items(), key=lambda x: x[0]):
            if d < td:
                lag = (td - d).days
                lvl = _guess_level(p)
                action = "READONLY" if lvl == 1 else ("CONFIRM_ONLY" if lvl == 2 else "HARD_REQUIRED(MISSING)")
                stale_items.append({
                    "path": p,
                    "asof": d.isoformat(),
                    "lag_days": lag,
                    "level": lvl,
                    "action": action,
                })

    lines: List[str] = []
    if td is not None:
        lines.append(f"- trade_date={td.isoformat()}")

    if error_paths:
        lines.append(f"- ⚠️ error_sources={len(set(error_paths))} (treated_as=STALE)")
        for p in sorted(set(error_paths))[:20]:
            lvl = _guess_level(p)
            action = "READONLY" if lvl == 1 else ("CONFIRM_ONLY" if lvl == 2 else "HARD_REQUIRED(MISSING)")
            lines.append(f"  - {p}: error/warnings/data_status flagged · L{lvl} · {action}")
        if len(set(error_paths)) > 20:
            lines.append(f"  - ... and {len(set(error_paths))-20} more")

    if missing_asof_paths:
        lines.append(f"- ⚠️ freshness_incomplete: missing_asof_paths={len(set(missing_asof_paths))} (treated_as=STALE)")
        for p in sorted(set(missing_asof_paths))[:20]:
            lvl = _guess_level(p)
            action = "READONLY" if lvl == 1 else ("CONFIRM_ONLY" if lvl == 2 else "HARD_REQUIRED(MISSING)")
            lines.append(f"  - {p}: asof=? · L{lvl} · {action}")
        if len(set(missing_asof_paths)) > 20:
            lines.append(f"  - ... and {len(set(missing_asof_paths))-20} more")

    if stale_items:
        lines.append(f"- ⚠️ stale_sources={len(stale_items)} (trade_date > source_asof)")
        for it in stale_items[:20]:
            lines.append(f"  - {it['path']}: asof={it['asof']} (lag {it['lag_days']}d) · L{it['level']} · {it['action']}")
        if len(stale_items) > 20:
            lines.append(f"  - ... and {len(stale_items)-20} more")
    elif not error_paths and not missing_asof_paths:
        lines.append("- ✅ all sources asof == trade_date")

    return {
        "asof": trade_date,
        "meta": {
            "trade_date": trade_date,
            "stale_count": len(stale_items),
            "missing_asof_paths_count": len(set(missing_asof_paths)),
        },
        "stale": stale_items,
        "missing_asof_paths": sorted(set(missing_asof_paths)),
        "render_lines": lines,
    }