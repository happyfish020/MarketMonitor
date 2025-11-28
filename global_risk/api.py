
from __future__ import annotations

from typing import Dict, Any

from .engine import GlobalRiskEngine
from .utils.json_utils import to_json


def get_daily_global_risk(as_dict: bool = True) -> Dict[str, Any] | str:
    eng = GlobalRiskEngine()
    result = eng.run_daily()
    payload: Dict[str, Any] = {
        "version": result.version,
        "macro": {
            "score": result.macro_score,
            "level": result.macro_level,
            "desc": result.macro_desc,
        },
        "ashare_daily": {
            "total_score": result.ashare_daily_total,
            "level": result.ashare_daily_level,
            "detail": result.ashare_daily_detail,
        },
        "raw": result.snapshot_raw,
    }
    if as_dict:
        return payload
    return to_json(payload)
