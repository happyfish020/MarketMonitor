
# v5.0.2 DataFetcher Output Normalization Layer (non-destructive)
# This module provides helper functions for Step 2 formatting.
from typing import Dict, Any

def norm_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0

def normalize_mainflow(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        d = {}
    return {
        "date": d.get("date", ""),
        "main_net": norm_float(d.get("main_net")),
        "super_net": norm_float(d.get("super_net")),
        "big_net": norm_float(d.get("big_net")),
        "medium_net": norm_float(d.get("medium_net")),
        "small_net": norm_float(d.get("small_net")),
    }

def normalize_margin(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        d = {}
    return {
        "last_date": d.get("last_date", ""),
        "rzrqye_100m": norm_float(d.get("rzrqye_100m")),
    }

def normalize_north(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        d = {}
    return {
        "net": norm_float(d.get("net")),
        "buy": norm_float(d.get("buy")),
        "sell": norm_float(d.get("sell")),
        "trend": d.get("trend", {"n1": None, "n2": None, "n3": None})
    }

def normalize_sector(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        d = {}
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = {
                "name": v.get("name", ""),
                "main_net": norm_float(v.get("main_net")),
                "super_net": norm_float(v.get("super_net")),
                "big_net": norm_float(v.get("big_net")),
                "medium_net": norm_float(v.get("medium_net")),
            }
    return out
