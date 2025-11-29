from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ...common import CacheManager, get_logger
from ...common.time_utils import fmt_date_compact, now_bj
from ...common.http_utils import get_json

LOG = get_logger("UnifiedRisk.AShareFetcher")

EM_NORTHBOUND_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EM_MARGIN_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EM_MAIN_FUND_URL = "https://push2.eastmoney.com/api/qt/clist/get"
EM_ETF_FLOW_URL = "https://push2.eastmoney.com/api/qt/clist/get"


@dataclass
class AShareRawData:
    turnover: Dict[str, Any]
    northbound: Dict[str, Any]
    margin: Dict[str, Any]
    main_fund: Dict[str, Any]
    etf_flow: Dict[str, Any]


class AShareDataFetcher:
    """A 股数据抓取 + day_cache 管理封装。""" 

    def __init__(self, cache: Optional[CacheManager] = None) -> None:
        self.cache = cache or CacheManager()

    def get_raw_data(self, date_str: Optional[str] = None) -> AShareRawData:
        if date_str is None:
            date_str = fmt_date_compact(now_bj())

        turnover = self._get_or_fetch_turnover(date_str)
        northbound = self._get_or_fetch_northbound(date_str)
        margin = self._get_or_fetch_margin(date_str)
        main_fund = self._get_or_fetch_main_fund(date_str)
        etf_flow = self._get_or_fetch_etf_flow(date_str)

        return AShareRawData(
            turnover=turnover,
            northbound=northbound,
            margin=margin,
            main_fund=main_fund,
            etf_flow=etf_flow,
        )

    def _get_or_fetch_turnover(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "ashare", "turnover")
        if cached is not None:
            return cached

        data = {
            "note": "turnover placeholder - 请在此对接你的成交额/换手率数据源",
            "date": date_str,
        }
        self.cache.write_key(date_str, "ashare", "turnover", data)
        return data

    def _get_or_fetch_northbound(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "ashare", "northbound")
        if cached is not None:
            return cached

        trade_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        params = {
            "reportName": "RPT_MUTUAL_DEALAMT",
            "columns": "ALL",
            "filter": f"(TRADE_DATE>='{trade_date}')",
            "pageNumber": 1,
            "pageSize": 50,
        }
        try:
            data = get_json(EM_NORTHBOUND_URL, params=params)
            LOG.info(f"Fetched northbound via EM datacenter for {date_str}")
        except Exception as e:
            LOG.error(f"Fetch northbound failed for {date_str}: {e}")
            data = {"error": str(e)}

        self.cache.write_key(date_str, "ashare", "northbound", data)
        return data

    def _get_or_fetch_margin(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "ashare", "margin")
        if cached is not None:
            return cached

        trade_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        params = {
            "reportName": "RPT_MARGIN",
            "columns": "ALL",
            "filter": f"(TRADE_DATE>='{trade_date}')",
            "pageNumber": 1,
            "pageSize": 50,
        }
        try:
            data = get_json(EM_MARGIN_URL, params=params)
            LOG.info(f"Fetched margin via EM datacenter for {date_str}")
        except Exception as e:
            LOG.error(f"Fetch margin failed for {date_str}: {e}")
            data = {"error": str(e)}

        self.cache.write_key(date_str, "ashare", "margin", data)
        return data

    def _get_or_fetch_main_fund(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "ashare", "main_fund")
        if cached is not None:
            return cached

        params = {
            "fltt": 2,
            "invt": 2,
            "fields": "f12,f14,f2,f3,f62",
            "fid": "f62",
            "po": 1,
            "pz": 50,
            "pn": 1,
            "np": 1,
            "fs": "m:1 t:2,m:1 t:3",
        }
        try:
            data = get_json(EM_MAIN_FUND_URL, params=params)
            LOG.info(f"Fetched main fund via EM push2 for {date_str}")
        except Exception as e:
            LOG.error(f"Fetch main fund failed for {date_str}: {e}")
            data = {"error": str(e)}

        self.cache.write_key(date_str, "ashare", "main_fund", data)
        return data

    def _get_or_fetch_etf_flow(self, date_str: str) -> Dict[str, Any]:
        cached = self.cache.read_key(date_str, "ashare", "etf_flow")
        if cached is not None:
            return cached

        params = {
            "fltt": 2,
            "invt": 2,
            "fields": "f12,f14,f2,f3,f62",
            "fid": "f62",
            "po": 1,
            "pz": 100,
            "pn": 1,
            "np": 1,
            "fs": "b:BK0501,b:BK0504",
        }
        try:
            data = get_json(EM_ETF_FLOW_URL, params=params)
            LOG.info(f"Fetched ETF flow via EM push2 for {date_str}")
        except Exception as e:
            LOG.error(f"Fetch ETF flow failed for {date_str}: {e}")
            data = {"error": str(e)}

        self.cache.write_key(date_str, "ashare", "etf_flow", data)
        return data
