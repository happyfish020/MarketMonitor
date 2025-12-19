# core/adapters/datasources/providers/provider_em.py
# UnifiedRisk V12 - EastMoney Provider (EM)

from __future__ import annotations

import time
import requests
from typing import List, Dict, Any

from core.adapters.providers.provider_base import ProviderBase
from core.utils.logger import get_logger

LOG = get_logger("Provider.EM")


class EMProvider(ProviderBase):
    """
    EastMoney Provider
    ------------------
    - 结构型数据 Provider（两融等）
    - 不提供通用行情 series
    """

    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Referer": "https://data.eastmoney.com/",
    }

    def __init__(self):
        # ⭐ 关键修复点
        super().__init__(name="em")

    # ------------------------------------------------------------------
    # 必须实现的抽象方法（但明确声明不支持）
    # ------------------------------------------------------------------
    def fetch_series_raw(self, *args, **kwargs):
        raise NotImplementedError(
            "EMProvider does not support fetch_series_raw(). "
            "Use EM-specific methods like fetch_margin_series()."
        )

    # ------------------------------------------------------------------
    # EM 专用方法：两融
    # ------------------------------------------------------------------
    def fetch_margin_series(self, days: int = 40) -> List[Dict[str, Any]]:
        params = {
            "reportName": "RPTA_RZRQ_LSDB",
            "columns": "ALL",
            "sortColumns": "DIM_DATE",
            "sortTypes": "-1",
            "pageNumber": 1,
            "pageSize": days,
            "source": "WEB",
            "_": int(time.time() * 1000),
        }

        rows = self._fetch_raw(params)
        if not rows:
            LOG.error("[EMProvider] empty margin data")
            return []

        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                date = str(r.get("DIM_DATE"))[:10]
                out.append(
                    {
                        "date": date,
                        "rz_balance": self._to_e8(r.get("TOTAL_RZYE")),
                        "rq_balance": self._to_e8(r.get("TOTAL_RQYE")),
                        "total": self._to_e8(r.get("TOTAL_RZRQYE")),
                        "rz_buy": self._to_e8(r.get("TOTAL_RZMRE")),
                        "total_chg": self._to_e8(r.get("TOTAL_RZRQYECZ")),
                        "rz_ratio": float(r.get("TOTAL_RZYEZB") or 0.0),
                    }
                )
            except Exception as e:
                LOG.error("[EMProvider] parse row failed: %s", e)

        out.sort(key=lambda x: x["date"])
        return out

    # ------------------------------------------------------------------
    def _fetch_raw(self, params: Dict[str, Any], retry: int = 3) -> List[Dict[str, Any]]:
        for i in range(retry):
            try:
                resp = requests.get(
                    self.BASE_URL,
                    params=params,
                    headers=self.HEADERS,
                    timeout=15,
                )
                resp.raise_for_status()
                js = resp.json()
                data = (js.get("result") or {}).get("data") or []
                if data:
                    return data
            except Exception as e:
                LOG.warning("[EMProvider] fetch retry=%s err=%s", i + 1, e)
                time.sleep(1)
        return []

    @staticmethod
    def _to_e8(v: Any) -> float:
        try:
            return round(float(v) / 1e8, 2)
        except Exception:
            return 0.0
