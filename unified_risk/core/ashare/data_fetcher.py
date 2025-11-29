from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

import requests

from unified_risk.common.logger import get_logger
from unified_risk.common.yf_fetcher import YFETFClient

LOG = get_logger("UnifiedRisk.Fetcher.AShare")

BJ_TZ = timezone(timedelta(hours=8))
CACHE_DIR = Path("data") / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

EM_COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Connection": "keep-alive",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

EM_DATACENTER_HEADERS = {
    **EM_COMMON_HEADERS,
    "Referer": "https://data.eastmoney.com/rzrq/",
}


def _get_with_retry(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 6,
    tries: int = 3,
    sleep_sec: float = 1.0,
) -> Optional[requests.Response]:
    hdrs = headers or EM_COMMON_HEADERS
    for i in range(tries):
        try:
            r = requests.get(url, headers=hdrs, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            LOG.warning("[HTTP] GET failed (%s), retry %d/%d: %s", url, i + 1, tries, e)
            time.sleep(sleep_sec)
    return None


class AShareDataFetcher:
    def __init__(self, yf_client: Optional[YFETFClient] = None) -> None:
        self.yf = yf_client or YFETFClient()

    def build_daily_snapshot(self, bj_time: Optional[datetime] = None) -> Dict[str, Any]:
        bj_now = bj_time or datetime.now(BJ_TZ)
        date_str = bj_now.strftime("%Y-%m-%d")
        LOG.info("[Snapshot] Build for %s", date_str)

        turnover = self.get_turnover()
        margin = self.get_margin()

        return {
            "date": date_str,
            "turnover": turnover,
            "margin": margin,
        }

    def get_turnover(self) -> Dict[str, Any]:
        df_sh = self.yf.get_etf_daily("510300")
        df_sz = self.yf.get_etf_daily("159901")

        if df_sh is not None and not df_sh.empty and df_sz is not None and not df_sz.empty:
            sh_last = df_sh.iloc[-1]
            sz_last = df_sz.iloc[-1]

            sh_prev = df_sh.iloc[-2] if len(df_sh) >= 2 else sh_last
            sz_prev = df_sz.iloc[-2] if len(df_sz) >= 2 else sz_last

            sh_turnover = float(sh_last["close"] * sh_last["volume"])
            sz_turnover = float(sz_last["close"] * sz_last["volume"])

            sh_change = 0.0
            if sh_prev["close"] != 0:
                sh_change = float((sh_last["close"] / sh_prev["close"] - 1.0) * 100.0)

            sz_change = 0.0
            if sz_prev["close"] != 0:
                sz_change = float((sz_last["close"] / sz_prev["close"] - 1.0) * 100.0)

            data = {
                "sh": {
                    "date": str(sh_last["date"]),
                    "close": float(sh_last["close"]),
                    "volume": float(sh_last["volume"]),
                    "turnover": sh_turnover,
                    "change_pct": sh_change,
                },
                "sz": {
                    "date": str(sz_last["date"]),
                    "close": float(sz_last["close"]),
                    "volume": float(sz_last["volume"]),
                    "turnover": sz_turnover,
                    "change_pct": sz_change,
                },
            }
            self._cache_json("turnover.json", data)
            LOG.info(
                "[Turnover] SH=%.2e SZ=%.2e (from YF ETF)",
                sh_turnover,
                sz_turnover,
            )
            return data

        cached = self._load_json("turnover.json")
        if cached:
            LOG.warning("[Turnover] use cached data")
            return cached

        LOG.error("[Turnover] no data (yf+cache both failed)")
        return {"sh": {}, "sz": {}}

    def get_margin(self) -> Dict[str, Any]:
        for i in range(3):
            d = self._fetch_lsdb_primary()
            if d:
                self._cache_json("lsdb.json", d)
                return d
            LOG.warning("[LSDB] primary retry %d failed", i + 1)
            time.sleep(1.0)

        d2 = self._fetch_lsdb_secondary()
        if d2:
            self._cache_json("lsdb.json", d2)
            return d2

        cached = self._load_json("lsdb.json")
        if cached:
            LOG.warning("[LSDB] use cached data")
            return cached

        LOG.error("[LSDB] no data (primary+secondary+cache all failed)")
        return {}

    def _fetch_lsdb_primary(self) -> Optional[Dict[str, Any]]:
        url = (
            "https://datacenter-web.eastmoney.com/api/data/v1/get?"
            "reportName=RPTA_RZRQ_LSDB&columns=ALL&source=WEB"
            "&sortColumns=DIM_DATE&sortTypes=-1&pageNumber=1&pageSize=1"
        )
        r = _get_with_retry(url, headers=EM_DATACENTER_HEADERS, timeout=4, tries=2, sleep_sec=1.0)
        if r is None:
            return None

        try:
            js = r.json()
            data = js.get("result", {}).get("data", [])
            if not data:
                return None
            row = data[0]
            return {
                "date": row.get("DIM_DATE"),
                "rzrqye": row.get("RZRQYE"),
                "rzye": row.get("RZYE"),
                "rqye": row.get("RQYE"),
            }
        except Exception as e:
            LOG.error("[LSDB] primary parse error: %s", e)
            return None

    def _fetch_lsdb_secondary(self) -> Optional[Dict[str, Any]]:
        url = (
            "https://datacenter-web.eastmoney.com/api/data/v1/get?"
            "reportName=RPTA_RZRQ_GGZB&columns=DIM_DATE,RZRQYE,RZYE,RQYE"
            "&source=WEB&sortColumns=DIM_DATE&sortTypes=-1&pageNumber=1&pageSize=1"
        )
        r = _get_with_retry(url, headers=EM_DATACENTER_HEADERS, timeout=4, tries=2, sleep_sec=1.0)
        if r is None:
            return None

        try:
            js = r.json()
            data = js.get("result", {}).get("data", [])
            if not data:
                return None
            row = data[0]
            return {
                "date": row.get("DIM_DATE"),
                "rzrqye": row.get("RZRQYE"),
                "rzye": row.get("RZYE"),
                "rqye": row.get("RQYE"),
            }
        except Exception as e:
            LOG.error("[LSDB] secondary parse error: %s", e)
            return None

    def _cache_json(self, name: str, data: Dict[str, Any]) -> None:
        path = CACHE_DIR / name
        try:
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            LOG.error("[Cache] write %s error: %s", name, e)

    def _load_json(self, name: str) -> Optional[Dict[str, Any]]:
        path = CACHE_DIR / name
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            LOG.error("[Cache] load %s error: %s", name, e)
            return None
