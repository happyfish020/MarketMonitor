# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import Optional

STAGE_ZH = {
    "S1": "进攻期",
    "S2": "顺风期",
    "S3": "震荡期",
    "S4": "防守期",
    "S5": "去风险期",
    "UNKNOWN": "结构不明期",
}

DRS_ZH = {"GREEN": "绿灯", "YELLOW": "黄灯", "ORANGE": "橙灯", "RED": "红灯"}

GATE_ZH = {
    "NORMAL": "正常",
    "CAUTION": "谨慎（禁加仓）",
    "FREEZE": "冻结",
    "D": "冻结",
}

ACTION_ZH = {"A": "可进攻", "N": "观望", "D": "防守", "D1": "防守", "D2": "防守", "D3": "防守"}

def stage_key_from_any(x: Optional[str]) -> str:
    if not x:
        return "UNKNOWN"
    if isinstance(x, str):
        s = x.strip()
    else:
        return "UNKNOWN"
    m = re.search(r"[（(]([A-Z0-9]+)[）)]", s)
    if m:
        return m.group(1).upper()
    su = s.upper()
    if su.startswith("S") and len(su) <= 4:
        return su
    if su == "UNKNOWN":
        return "UNKNOWN"
    return "UNKNOWN"

def stage_zh(x: Optional[str]) -> str:
    k = stage_key_from_any(x)
    return f"{STAGE_ZH.get(k, STAGE_ZH['UNKNOWN'])}（{k}）"

def drs_zh(x: Optional[str]) -> str:
    if not x:
        return "未知"
    k = str(x).strip().upper()
    return f"{DRS_ZH.get(k, k)}（{k}）"

def gate_zh(x: Optional[str]) -> str:
    if not x:
        return "未知"
    k = str(x).strip().upper()
    return f"{GATE_ZH.get(k, k)}（{k}）"

def execution_zh(x: Optional[str]) -> str:
    if not x:
        return "未知"
    k = str(x).strip().upper()
    if k in ("D1","D2","D3"):
        level = {"D1":"低摩擦","D2":"中摩擦","D3":"高摩擦"}[k]
        return f"{level}（{k}）"
    return k

def actionhint_zh(x: Optional[str]) -> str:
    if not x:
        return "未知"
    k = str(x).strip().upper()
    if k in ("A","N","D"):
        return f"{ACTION_ZH[k]}（{k}）"
    return k
