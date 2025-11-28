# global_risk/datasources/commodity_fetcher.py
from __future__ import annotations

import math
import logging
from typing import Dict, Any, Optional

import requests
from datetime import datetime, timezone

logger = logging.getLogger("GlobalMultiRisk.commodity")


YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _fetch_yahoo_change_pct(symbol: str) -> Optional[float]:
    """
    使用 Yahoo chart 接口，拿最近两个收盘价，计算日涨跌幅（%）。
    """
    params = {
        "interval": "1d",
        "range": "5d",
    }
    try:
        resp = requests.get(YF_CHART_URL.format(symbol=symbol), params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            logger.warning("Yahoo chart empty for %s", symbol)
            return None

        result = result[0]
        close_list = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        if not close_list or len(close_list) < 2:
            return None

        # 找最后两个非 None 的收盘价
        valid = [c for c in close_list if c is not None]
        if len(valid) < 2:
            return None

        prev, last = valid[-2], valid[-1]
        if prev == 0 or prev is None or last is None:
            return None

        pct = (last - prev) / prev * 100.0
        return pct
    except Exception as e:
        logger.warning("fetch yahoo change failed for %s: %s", symbol, e)
        return None


def _fetch_yahoo_last_price(symbol: str) -> Optional[float]:
    params = {
        "interval": "1d",
        "range": "1d",
    }
    try:
        resp = requests.get(YF_CHART_URL.format(symbol=symbol), params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return None
        result = result[0]
        close_list = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        valid = [c for c in close_list if c is not None]
        if not valid:
            return None
        return float(valid[-1])
    except Exception as e:
        logger.warning("fetch yahoo last price failed for %s: %s", symbol, e)
        return None


def get_commodity_snapshot() -> Dict[str, Any]:
    """
    大宗商品快照：
      - 黄金期货 GC=F
      - 原油期货 CL=F
      - 期铜 HG=F
      - 美元指数 DX-Y.NYB
    将来可以扩展：
      - 白银 SI=F
      - 锂、铁矿石等
    """
    gold_pct = _fetch_yahoo_change_pct("GC=F")
    gold_usd = _fetch_yahoo_last_price("GC=F")

    oil_pct = _fetch_yahoo_change_pct("CL=F")
    copper_pct = _fetch_yahoo_change_pct("HG=F")

    dxy_pct = _fetch_yahoo_change_pct("DX-Y.NYB")

    snap = {
        "gold_pct": gold_pct,
        "gold_usd": gold_usd,
        "oil_pct": oil_pct,
        "copper_pct": copper_pct,
        "dxy_pct": dxy_pct,
        # 真实利率 real_yield_10y 暂留接口，后续可接入 Macrotrends / FRED：
        "real_yield_10y": None,
    }
    logger.info(
        "[COMMODITY] GC=F %+0.2f%% @ %s, CL=F %+0.2f%%, HG=F %+0.2f%%, DXY %+0.2f%%",
        gold_pct or 0.0,
        f"{gold_usd:.1f}" if gold_usd is not None else "N/A",
        oil_pct or 0.0,
        copper_pct or 0.0,
        dxy_pct or 0.0,
    )
    return snap
