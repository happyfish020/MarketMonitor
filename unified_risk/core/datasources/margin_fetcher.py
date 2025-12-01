
from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd

from unified_risk.common.logging_utils import log_info, log_warning


def _safe_import_ak():
    try:
        import akshare as ak
        return ak
    except Exception as e:
        log_warning(f"[MARGIN] akshare not installed or failed to import: {e}")
        return None


def _extract(df: pd.DataFrame, cols):
    for c in cols:
        if c in df.columns:
            return df[c]
    return None


def fetch_margin_agg(trade_date: datetime) -> Optional[Dict[str, float]]:
    ak = _safe_import_ak()
    if ak is None:
        return None

    ds = trade_date.strftime("%Y%m%d")

    try:
        sh = ak.stock_margin_sse(start_date=ds, end_date=ds)
    except Exception:
        sh = None
    try:
        sz = ak.stock_margin_szse(date=ds)
    except Exception:
        sz = None

    frames = [x for x in (sh, sz) if x is not None and not x.empty]
    if not frames:
        return None

    df = pd.concat(frames)

    rz = _extract(df, ["融资余额", "rzye"])
    rq = _extract(df, ["融券余额", "rqye"])
    buy = _extract(df, ["融资买入额", "rzmre"])

    total = 0.0
    if rz is not None:
        total += rz.fillna(0).sum()
    if rq is not None:
        total += rq.fillna(0).sum()

    net_buy = buy.fillna(0).sum() if buy is not None else 0.0

    return {"total_balance": float(total), "net_buy": float(net_buy)}
