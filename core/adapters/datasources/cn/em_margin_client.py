# -*- coding: utf-8 -*-
"""
Eastmoney Margin Client — 统一单位版 + 3次重试机制
外部：元
内部：统一转换为 亿元(e9)
"""

import time
import requests
from typing import List, Dict, Any

from core.utils.logger import log
from core.utils.units import yuan_to_e9   # 必须引用这个！


class EastmoneyMarginClientCN:
    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def __init__(self) -> None:
        log("[IO] EastmoneyMarginClientCN init (no local cache)")

    # =====================================================================
    # 加强版：含 retry=3 + sleep(10) + timeout=20
    # =====================================================================
    def _request_with_retry(self, params: Dict[str, Any], headers: Dict[str, str]):
        """封装统一的网络请求 + 重试机制"""
        RETRY = 3
        SLEEP_SEC = 10

        for i in range(1, RETRY + 1):
            try:
                log(f"[IO] FETCH → Eastmoney RZRQ_LSDB (try {i}/{RETRY})")
                resp = requests.get(
                    self.BASE_URL,
                    params=params,
                    headers=headers,
                    timeout=20  # ★ read_timeout = 20
                )
                resp.raise_for_status()

                js = resp.json()
                return js

            except Exception as e:
                log(f"[IO] FAIL (try {i}/{RETRY}) ← {e}")
                if i < RETRY:
                    log(f"[IO] RETRY in {SLEEP_SEC}s...")
                    time.sleep(SLEEP_SEC)
                else:
                    log("[IO] GIVE UP after 3 retries.")
                    return None

    # =====================================================================
    # 主函数：获取近期 RZRQ 序列
    # =====================================================================
    def get_recent_series(self, max_days: int = 20) -> List[Dict[str, Any]]:
        params = {
            "reportName": "RPTA_RZRQ_LSDB",
            "columns": "ALL",
            "source": "WEB",
            "sortColumns": "DIM_DATE",
            "sortTypes": "-1",
            "pageNumber": 1,
            "pageSize": max_days,
        }

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://data.eastmoney.com/rzrq/",
        }

        js = self._request_with_retry(params, headers)
        if js is None:
            log("[IO] FETCH FAIL ← Eastmoney RZRQ_LSDB (all retries failed)")
            return []

        result = js.get("result") or {}
        data = result.get("data") or []
        rows: List[Dict[str, Any]] = []

        for row in data:
            try:
                date = str(row.get("DIM_DATE") or "")[:10]

                # 原始为元 → 必须转换为 亿(e9)
                rz_raw = float(row.get("RZYE") or 0.0)
                rq_raw = float(row.get("RQYE") or 0.0)
                rzrq_raw = float(row.get("RZRQYE") or (rz_raw + rq_raw))

                rz = yuan_to_e9(rz_raw)
                rq = yuan_to_e9(rq_raw)
                rzrq = yuan_to_e9(rzrq_raw)

                rows.append({
                    "date": date,
                    "rz": rz,
                    "rq": rq,
                    "rzrq": rzrq,
                })
            except Exception:
                continue

        rows.sort(key=lambda x: x["date"])
        log(f"[IO] FETCH OK ← Eastmoney RZRQ_LSDB (rows={len(rows)})")
        return rows
