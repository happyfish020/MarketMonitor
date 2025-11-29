# unified_risk/core/ashare/data_fetcher.py
from __future__ import annotations

import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Fetcher.AShare")

BJ_TZ = timezone(timedelta(hours=8))


class AShareDataFetcher:
    """
    v7.5.3 简化版 DataFetcher：
      - get_index_turnover(): ETF proxy 成交额
      - get_margin_lsdb(): 两融余额
      - build_daily_snapshot(): 汇总为 snapshot dict
    """

    def build_daily_snapshot(self, bj_time: Optional[datetime] = None) -> Dict[str, Any]:
        bj_now = bj_time or datetime.now(BJ_TZ)
        date_str = bj_now.strftime("%Y-%m-%d")

        LOG.info("[AShareFetcher] Build snapshot for %s", date_str)

        turnover = self.get_index_turnover()
        margin = self.get_margin_lsdb()

        snapshot = {
            "date": date_str,
            "turnover": turnover,
            "margin": margin,
        }
        return snapshot

    # ---------- 成交额 ----------
    def get_index_turnover(self) -> Dict[str, Any]:
        try:
            sh = self._fetch_em_kline("510300", "sh")
            sz = self._fetch_em_kline("159901", "sz")
            return {"sh": sh, "sz": sz}
        except Exception as e:
            LOG.error("[AShareFetcher] turnover fetch error: %s", e, exc_info=True)
            return {"sh": {}, "sz": {}}

    def _fetch_em_kline(self, code: str, market: str) -> Dict[str, Any]:
        secid = f"{'1.' if market == 'sh' else '0.'}{code}"
        url = (
            "https://push2.eastmoney.com/api/qt/stock/kline/get?"
            f"secid={secid}"
            "&fields1=f1,f2,f3,f4,f5,f6"
            "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59"
            "&klt=101&fqt=1&end=20500000&lmt=1"
        )
        r = requests.get(url, timeout=8)
        js = r.json()
        kl = js.get("data", {}).get("klines", [])
        if not kl:
            return {}
        row = kl[-1].split(",")
        return {
            "date": row[0],
            "close": float(row[1]),
            "volume": float(row[5]),
            "turnover": float(row[6]),
            "change_pct": float(row[8]),
        }

    # ---------- 两融 ----------
    def get_margin_lsdb(self) -> Dict[str, Any]:
        try:
            url = (
                "https://datacenter-web.eastmoney.com/api/data/v1/get?"
                "reportName=RPTA_RZRQ_LSDB&columns=ALL&source=WEB"
                "&sortColumns=DIM_DATE&sortTypes=-1&pageNumber=1&pageSize=50"
            )
            r = requests.get(url, timeout=8)
            js = r.json()
            data = js.get("result", {}).get("data", [])
            if not data:
                return {}
            row = data[0]
            return {
                "date": row.get("DIM_DATE"),
                "rzye": row.get("RZYE"),
                "rqye": row.get("RQYE"),
                "rzrqye": row.get("RZRQYE"),
            }
        except Exception as e:
            LOG.error("[AShareFetcher] margin lsdb error: %s", e, exc_info=True)
            return {}
