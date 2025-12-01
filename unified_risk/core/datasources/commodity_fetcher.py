
from __future__ import annotations

from typing import Optional
from datetime import datetime

from unified_risk.common.logging_utils import log_warning
from unified_risk.common.yf_fetcher import get_last_valid_bar


def _fetch_yahoo_last_price(symbol: str, lookback_days: int = 15) -> Optional[float]:
    """
    从 yfinance 抓取最近一个有数据的交易日收盘价。
    自动跳过周末/假日。
    """
    bar = get_last_valid_bar(symbol, lookback_days=lookback_days, interval="1d")
    if bar is None:
        log_warning(f"[COMMODITY] {symbol}: no valid last bar")
        return None

    _, close, _ = bar
    return float(close)


class CommodityFetcher:
    """统一商品行情获取：黄金/白银/原油/铜/美元指数/比特币/VIX/美债收益率。"""

    def get_gold(self) -> Optional[float]:
        return _fetch_yahoo_last_price("GC=F")

    def get_silver(self) -> Optional[float]:
        return _fetch_yahoo_last_price("SI=F")

    def get_crude(self) -> Optional[float]:
        return _fetch_yahoo_last_price("CL=F")

    def get_copper(self) -> Optional[float]:
        return _fetch_yahoo_last_price("HG=F")

    def get_dxy(self) -> Optional[float]:
        return _fetch_yahoo_last_price("DX-Y.NYB")

    def get_bitcoin(self) -> Optional[float]:
        return _fetch_yahoo_last_price("BTC-USD")

    def get_vix(self) -> Optional[float]:
        return _fetch_yahoo_last_price("^VIX")

    def get_yield_10y(self) -> Optional[float]:
        val = _fetch_yahoo_last_price("^TNX")
        return val / 10.0 if val else None

    def get_yield_5y(self) -> Optional[float]:
        val = _fetch_yahoo_last_price("^FVX")
        return val / 10.0 if val else None


def get_commodity_snapshot() -> dict:
    """方便 ashare_fetcher 直接获取一份 snapshot 字典。"""
    f = CommodityFetcher()
    return {
        "gold": f.get_gold(),
        "silver": f.get_silver(),
        "crude": f.get_crude(),
        "copper": f.get_copper(),
        "dxy": f.get_dxy(),
        "bitcoin": f.get_bitcoin(),
        "vix": f.get_vix(),
        "ust10y": f.get_yield_10y(),
        "ust5y": f.get_yield_5y(),
    }
