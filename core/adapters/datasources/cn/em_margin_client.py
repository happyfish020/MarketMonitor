from __future__ import annotations

import os
import time
from datetime import datetime
from typing import List, Dict, Any

import requests
import json


class EastmoneyMarginClientCN:
    """
    东财两融日度数据客户端（RPTA_RZRQ_LSDB）
    - 只抓全市场合计数据，不逐股票
    - 做简单本地缓存，避免频繁请求
    - 供 MarginFactor 使用
    """

    BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def __init__(self, cache_root: str = "data/ashare", ttl_seconds: int = 6 * 60 * 60):
        self.cache_root = cache_root
        self.ttl_seconds = ttl_seconds
        os.makedirs(self.cache_root, exist_ok=True)

    # ---------- 公共方法 ----------

    def get_recent_series(self, max_days: int = 20) -> List[Dict[str, Any]]:
        """
        获取最近若干交易日的两融总量时间序列（按日期升序排列）

        返回每个元素类似：
        {
            "date": "2025-12-02",
            "rz": 123456.0,
            "rq": 23456.0,
            "rzrq": 146000.0
        }
        """
        cache_path = os.path.join(self.cache_root, "margin_lsdb.json")

        # 1. 尝试读缓存
        rows = self._load_cache(cache_path)
        if rows:
            return rows

        # 2. 无缓存或缓存过期，调用东财接口
        rows = self._fetch_from_remote(max_days=max_days)
        if rows:
            self._save_cache(cache_path, rows)

        return rows

    # ---------- 内部工具 ----------

    def _load_cache(self, path: str) -> List[Dict[str, Any]]:
        try:
            if not os.path.exists(path):
                return []
            # 简单 TTL 判断
            mtime = os.path.getmtime(path)
            if time.time() - mtime > self.ttl_seconds:
                return []

            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, list):
                return []

            # 按日期排序（升序）
            for row in data:
                if "date" in row and isinstance(row["date"], str):
                    # 只保留 YYYY-MM-DD
                    row["date"] = row["date"][:10]

            data.sort(key=lambda x: x.get("date", ""))
            return data
        except Exception:
            return []

    def _save_cache(self, path: str, rows: List[Dict[str, Any]]) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _fetch_from_remote(self, max_days: int = 20) -> List[Dict[str, Any]]:
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

        try:
            resp = requests.get(self.BASE_URL, params=params, headers=headers, timeout=8)
            resp.raise_for_status()
            js = resp.json()
        except Exception:
            return []

        result = js.get("result") or {}
        data = result.get("data") or []
        if not data:
            return []

        rows: List[Dict[str, Any]] = []
        for row in data:
            try:
                date = str(row.get("DIM_DATE") or "")[:10]
                rz = float(row.get("RZYE") or 0.0)
                rq = float(row.get("RQYE") or 0.0)
                rzrq = float(row.get("RZRQYE") or (rz + rq))
            except Exception:
                continue

            rows.append(
                {
                    "date": date,
                    "rz": rz,
                    "rq": rq,
                    "rzrq": rzrq,
                }
            )

        # 接口按日期倒序返回，这里改为升序
        rows.sort(key=lambda x: x["date"])
        return rows


if __name__ == "__main__":
    client = EastmoneyMarginClientCN()
    series = client.get_recent_series(max_days=15)
    print(f"rows={len(series)}")
    for r in series[-5:]:
        print(r)