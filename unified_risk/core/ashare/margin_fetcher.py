# unifiedrisk/core/ashare/margin_fetcher.py

import json
from datetime import datetime
from pathlib import Path
import requests
import akshare as ak

from unifiedrisk.common.path_utils import day_cache_path
from unifiedrisk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Margin")

class MarginFetcher:

    def __init__(self, trade_date: str):
        self.trade_date = trade_date
        self.cache_file = day_cache_path(trade_date) / "margin.json"

    # -----------------------------
    # Public API
    # -----------------------------
    def load(self):
        """Return dict from cache"""
        if self.cache_file.exists():
            try:
                return json.loads(self.cache_file.read_text(encoding="utf-8"))
            except:
                pass
        return None

    def fetch_and_cache(self):
        """Main entry"""
        LOG.info(f"[Margin] Fetch margin for {self.trade_date}")
        data = (
            self._fetch_from_datacenter()
            or self._fetch_from_akshare()
            or self._fetch_from_push2()
        )

        if not data:
            LOG.error("[Margin] ❌ 全部接口失败，写入空结构")
            data = {"status": "fail"}

        self.cache_file.write_text(json.dumps(data, ensure_ascii=False))
        return data

    # -----------------------------
    # Source 1: EastMoney Datacenter
    # -----------------------------
    def _fetch_from_datacenter(self):
        try:
            url = (
                "https://datacenter-web.eastmoney.com/api/data/v1/get"
                "?reportName=RPTA_WEB_RZRQ_GPDJR"
                "&columns=ALL&sortColumns=RZRQYE"
                f"&filter=(TDATE='{self.trade_date}')"
            )
            r = requests.get(url, timeout=8)
            j = r.json()
            rows = j["result"]["data"]
            rz = sum([x["RZYE"] for x in rows])
            rq = sum([x["RQYE"] for x in rows])

            return {
                "trade_date": self.trade_date,
                "rz": rz,
                "rq": rq,
                "total": rz + rq,
                "source": "datacenter"
            }
        except Exception as e:
            LOG.warning(f"[Margin][datacenter] fail: {e}")
            return None

    # -----------------------------
    # Source 2: AkShare fallback
    # -----------------------------
    def _fetch_from_akshare(self):
        try:
            df = ak.stock_margin_sse()
            df2 = ak.stock_margin_szse()
            rz = df['融资余额(元)'].iloc[-1] + df2['融资余额(元)'].iloc[-1]
            rq = df['融券余额(元)'].iloc[-1] + df2['融券余额(元)'].iloc[-1]

            return {
                "trade_date": self.trade_date,
                "rz": rz,
                "rq": rq,
                "total": rz + rq,
                "source": "akshare"
            }
        except Exception as e:
            LOG.warning(f"[Margin][akshare] fail: {e}")
            return None

    # -----------------------------
    # Source 3: Push2 emergency fallback
    # -----------------------------
    def _fetch_from_push2(self):
        try:
            url = (
                "https://push2.eastmoney.com/api/qt/stock/get"
                "?secid=1.000001&fields=f161,f162"
            )
            r = requests.get(url, timeout=8).json()
            rz = r["data"]["f161"]
            rq = r["data"]["f162"]

            return {
                "trade_date": self.trade_date,
                "rz": rz,
                "rq": rq,
                "total": rz + rq,
                "source": "push2"
            }
        except:
            return None
