from typing import Any, Dict, Optional

from .http_utils import get_json
from .logger import get_logger

LOG = get_logger("UnifiedRisk.YF")

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

def fetch_daily_bar(
    symbol: str,
    range_: str = "5d",
    interval: str = "1d",
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """从 Yahoo Finance 获取简单的日线数据。""" 
    params = {"range": range_, "interval": interval}
    if extra_params:
        params.update(extra_params)

    url = BASE_URL.format(symbol=symbol)
    data = get_json(url, params=params)
    LOG.debug(f"YF: fetched {symbol} range={range_} interval={interval}")
    return data

def fetch_last_close(symbol: str) -> Optional[float]:
    """返回最近一个交易日的收盘价（简化版）。""" 
    data = fetch_daily_bar(symbol, range_="5d", interval="1d")
    try:
        res = data["chart"]["result"][0]
        close = res["indicators"]["quote"][0]["close"]
        if not close:
            return None
        for c in reversed(close):
            if c is not None:
                return float(c)
    except Exception as e:
        LOG.warning(f"fetch_last_close({symbol}) failed: {e}")
    return None
