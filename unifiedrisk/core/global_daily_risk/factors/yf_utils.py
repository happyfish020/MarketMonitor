"""Lightweight Yahoo Finance helper using public chart API.

为避免额外依赖，仅用标准库 urllib + json.
"""
from __future__ import annotations

from typing import Tuple
import json
from urllib import request, error


YF_CHART_URL = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    "?interval=1d&range=2d"
)


def _safe_get(url: str) -> dict | None:
    try:
        with request.urlopen(url, timeout=8) as resp:
            if resp.status != 200:
                return None
            data = resp.read().decode("utf-8")
            return json.loads(data)
    except Exception:
        return None


def get_latest_change(symbol: str) -> Tuple[float, float]:
    """返回 (pct_change, last_close).

    pct_change 单位：%
    - 若无法获取数据，则返回 (0.0, 0.0)
    """
    url = YF_CHART_URL.format(symbol=symbol)
    data = _safe_get(url)
    if not data:
        return 0.0, 0.0

    try:
        result = data["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        # 取最近两个有效收盘价
        prices = [c for c in closes if c is not None]
        if len(prices) < 2:
            return 0.0, prices[-1] if prices else 0.0
        prev, last = float(prices[-2]), float(prices[-1])
        if prev == 0:
            return 0.0, last
        pct = (last - prev) / prev * 100.0
        return pct, last
    except Exception:
        return 0.0, 0.0
