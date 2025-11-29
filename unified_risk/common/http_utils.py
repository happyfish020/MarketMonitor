from typing import Any, Dict, Optional
import requests

from .logger import get_logger

LOG = get_logger("UnifiedRisk.HTTP")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Connection": "keep-alive",
}

def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """简单封装 GET JSON 请求。""" 
    hdrs = dict(DEFAULT_HEADERS)
    if headers:
        hdrs.update(headers)

    LOG.debug(f"HTTP GET {url} params={params}")
    resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}
