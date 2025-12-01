
from __future__ import annotations

import json
import re
import requests
from typing import List, Dict, Any

from unified_risk.common.logging_utils import log_info, log_warning

EM_SECTOR_URL = "https://push2.eastmoney.com/api/qt/clist/get"


def fetch_sector_flow_topN(n: int = 80, timeout: float = 5.0) -> List[Dict[str, Any]]:
    """东财行业板块（m:90+t:2）主力流入 f62"""
    params = {
        "po": "1",
        "pz": str(n),
        "pn": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fs": "m:90+t:2",
        "fid": "f62",
        "fields": "f12,f14,f2,f3,f62",
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(EM_SECTOR_URL, params=params, headers=headers, timeout=timeout)
        text = resp.text
        m = re.search(r"{.*}", text)
        data = json.loads(m.group(0)) if m else resp.json()
    except Exception as e:
        log_warning(f"[SECTOR] request failed: {e}")
        return []

    diff = (data.get("data") or {}).get("diff") or []
    out: List[Dict[str, Any]] = []
    for row in diff:
        try:
            out.append(
                {
                    "code": str(row.get("f12")),
                    "name": str(row.get("f14")),
                    "main_flow": float(row.get("f62") or 0.0),
                    "change_pct": float(row.get("f3") or 0.0),
                    "last": float(row.get("f2") or 0.0),
                }
            )
        except Exception:
            pass

    log_info(f"[SECTOR] fetched {len(out)} boards")
    return out
