
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any

import requests

from unified_risk.common.logging_utils import log_info

BJ_TZ = timezone(timedelta(hours=8))

SGX_A50_URL = "https://api.sgx.com/derivatives/v1.0/contract-code/CN"


def _pick_nearby_contract(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    items = None
    if isinstance(data.get("data"), dict):
        items = data["data"].get("results") or data["data"].get("contracts")
    elif isinstance(data.get("data"), list):
        items = data["data"]

    if not items:
        # 休市 / 假期：SGX 不返回合约，这是正常情况
        return None

    now = datetime.now(BJ_TZ)
    ym_today = now.year * 100 + now.month

    candidates = []
    for row in items:
        dm = row.get("delivery-month") or row.get("deliveryMonth")
        if dm is None:
            continue
        try:
            dm_int = int(str(dm))
        except Exception:
            continue
        candidates.append((dm_int, row))

    if not candidates:
        return None

    future = [c for c in candidates if c[0] >= ym_today]
    if future:
        return min(future, key=lambda x: x[0])[1]
    else:
        return min(candidates, key=lambda x: x[0])[1]


def fetch_sgx_a50_change_pct(timeout: float = 5.0) -> Optional[float]:
    """获取 SGX FTSE China A50 主力合约涨跌幅（%）。

    - 交易日：返回最新变动百分比
    - 休市日（周末/假期）：返回 None，不记为 Warning，由上层 fallback 到 HSI/ETF
    """
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    params = {
        "order": "asc",
        "orderby": "delivery-month",
        "category": "futures",
    }

    try:
        resp = requests.get(SGX_A50_URL, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        # 网络错误 / API 异常：返回 None 由上层决定如何回退
        return None

    row = _pick_nearby_contract(data)
    if not row:
        # 正常休市：返回 None 让上层回退
        return None

    raw = row.get("change-percentage") or row.get("changePercentage")
    try:
        pct = float(raw)
    except Exception:
        return None

    log_info(f"[A50Night] SGX CN change% = {pct:.3f}")
    return pct
