# -*- coding: utf-8 -*-
"""Unified state rendering helpers (ZH-first)."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

_STAGE_ZH = {"S1":"进攻期","S2":"顺风期","S3":"震荡期","S4":"防守期","S5":"去风险期","UNKNOWN":"结构不明期"}
_DRS_ZH = {"GREEN":"绿灯","YELLOW":"黄灯","ORANGE":"橙灯","RED":"红灯"}
_GATE_ZH = {"NORMAL":"正常","CAUTION":"谨慎（禁加仓）","FREEZE":"冻结","D":"冻结"}
_EXEC_ZH = {"D1":"低摩擦","D2":"中摩擦","D3":"高摩擦"}
_AH_ZH = {"A":"可进攻","N":"观望","D":"防守"}
_SHIFT_ZH = {"RISK_ESCALATION":"风险升级","RISK_EASING":"风险缓和"}
_SEVERITY_ZH = {"LOW":"低","MED":"中","HIGH":"高","VERY_HIGH":"很高","EXTREME":"极高"}

def _upper(x: Any) -> str:
    return str(x).strip().upper() if x is not None else ""

def stage_key(x: Any) -> str:
    if x is None:
        return "UNKNOWN"
    s = x.strip() if isinstance(x, str) else str(x).strip()
    m = re.search(r"[（(]([A-Z0-9]+)[）)]", s)
    if m:
        return m.group(1).upper()
    su = s.upper()
    if su.startswith("S") and len(su) <= 4:
        return su
    if su == "UNKNOWN":
        return "UNKNOWN"
    return "UNKNOWN"

def stage_zh(x: Any) -> str:
    k = stage_key(x)
    return f"{_STAGE_ZH.get(k,_STAGE_ZH['UNKNOWN'])}（{k}）"

def drs_zh(x: Any) -> str:
    k = _upper(x)
    return f"{_DRS_ZH.get(k,k)}（{k}）" if k else "未知"

def gate_zh(x: Any) -> str:
    k = _upper(x)
    return f"{_GATE_ZH.get(k,k)}（{k}）" if k else "未知"

def execution_zh(x: Any) -> str:
    k = _upper(x)
    if not k:
        return "未知"
    return f"{_EXEC_ZH[k]}（{k}）" if k in _EXEC_ZH else k

def actionhint_zh(x: Any) -> str:
    k = _upper(x)
    if not k:
        return "未知"
    return f"{_AH_ZH[k]}（{k}）" if k in _AH_ZH else k

def shift_zh(shift_type: Any, severity: Any) -> str:
    st = _upper(shift_type) or "UNKNOWN"
    sev = _upper(severity) or "UNKNOWN"
    return f"{_SHIFT_ZH.get(st,st)}（{st}） | 严重度={_SEVERITY_ZH.get(sev,sev)}（{sev}）"

def safe_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}
