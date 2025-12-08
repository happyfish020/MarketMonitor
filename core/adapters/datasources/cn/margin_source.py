# -*- coding: utf-8 -*-
# UnifiedRisk V12 - MarginDataSource (Enhanced, Clean, Fault-tolerant)

from __future__ import annotations

import os
import time
import requests
from typing import Dict, Any, List

from core.adapters.datasources.base import BaseDataSource
from core.adapters.cache.file_cache import load_json, save_json
from core.utils.datasource_config import DataSourceConfig
from core.utils.units import yuan_to_e9
from core.utils.logger import get_logger

LOG = get_logger("DS.Margin")

URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
CACHE_TTL = 600  # 10 minutes

 

class MarginDataSource(BaseDataSource):

    def __init__(self, trade_date: str):
        super().__init__("MarginDataSource")

        self.config = DataSourceConfig(market="cn", ds_name="margin")
        self.config.ensure_dirs()

        self.cache_path = os.path.join(self.config.cache_root, "margin_today.json")
        self.hist_path = os.path.join(self.config.history_root, "margin_series.json")

        LOG.info(f"[DS.Margin] Init: cache={self.config.cache_root}, history={self.config.history_root}")
        self.trade_date = trade_date
        LOG.info("Init: Trade_date%s", self.trade_date)
  

    def _is_valid_record(self, row: Dict[str, Any]) -> bool:
        """
        判断一条两融记录是否有效：
        - 日期不为空
        - TOTAL_RZRQYE > 0（最核心）
        """
        if not row.get("date"):
            return False
        if float(row.get("total", 0.0)) <= 0:
            return False
        return True


    # ---------------------------------------------------------
    # Cache
    # ---------------------------------------------------------
    def _load_cache(self):
        data = load_json(self.cache_path)
        if not data:
            return None
        if time.time() - data.get("ts", 0) > CACHE_TTL:
            return None
        LOG.info("[DS.Margin] Using cache")
        return data.get("data")

    def _save_cache(self, block):
        save_json(self.cache_path, {"ts": time.time(), "data": block})
        LOG.info("[DS.Margin] Cache saved")

    # ---------------------------------------------------------
    # History
    # ---------------------------------------------------------
    def _load_history(self):
        hist = load_json(self.hist_path)
        return hist if isinstance(hist, list) else []

    def _save_history(self, series):
        # 保留 400 日即可
        series = sorted(series, key=lambda x: x["date"])[-400:]
        save_json(self.hist_path, series)
        LOG.info(f"[DS.Margin] History saved rows={len(series)}")

    # ---------------------------------------------------------
    # Fetch from Eastmoney (with robust parsing)
    # ---------------------------------------------------------
    def _fetch_recent_rows(self, max_days=40) -> List[Dict[str, Any]]:
        params = {
            "reportName": "RPTA_RZRQ_LSDB",
            "columns": "ALL",
            "sortColumns": "DIM_DATE",
            "sortTypes": "-1",
            "pageNumber": 1,
            "pageSize": max_days,
            "source": "WEB",
            "_": int(time.time() * 1000),
        }

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Referer": "https://data.eastmoney.com/rzrq/",
        }

        def safe_num(v):
            if v is None:
                return 0.0
            try:
                return float(v)
            except:
                return 0.0

        for attempt in range(3):
            try:
                LOG.info(f"[DS.Margin] FETCH attempt={attempt+1}")

                resp = requests.get(URL, params=params, headers=headers, timeout=10)
                resp.raise_for_status()

                rows = (resp.json().get("result") or {}).get("data") or []
                if not rows:
                    LOG.warning("[DS.Margin] Empty response")
                    continue

                parsed = []

                for it in rows:
                    try:
                        date_raw = it.get("DIM_DATE")
                        if not date_raw:
                            continue
                        date = str(date_raw)[:10]

                        parsed.append({
                            "date": date,
                            "rz_balance": yuan_to_e9(safe_num(it.get("TOTAL_RZYE"))),
                            "rq_balance": yuan_to_e9(safe_num(it.get("TOTAL_RQYE"))),
                            "total": yuan_to_e9(safe_num(it.get("TOTAL_RZRQYE"))),
                            "rz_buy": yuan_to_e9(safe_num(it.get("TOTAL_RZMRE"))),
                            "total_chg": yuan_to_e9(safe_num(it.get("TOTAL_RZRQYECZ"))),
                            "rz_ratio": safe_num(it.get("TOTAL_RZYEZB")),
                        })

                    except Exception as e:
                        LOG.error(f"[DS.Margin] Parse row error: {e} row={it}")
                        continue

                parsed.sort(key=lambda x: x["date"])
                LOG.info(f"[DS.Margin] FETCH OK rows={len(parsed)}")

                return parsed

            except Exception as e:
                LOG.error(f"[DS.Margin] FETCH error: {e}")
                time.sleep(1)

        LOG.error("[DS.Margin] FETCH FAILED after retries")
        return []

    # ---------------------------------------------------------
    # Trend & Acceleration
    # ---------------------------------------------------------
    @staticmethod
    def _diff(series, window, key):
        if len(series) < window + 1:
            return 0.0

        try:
            v_now = float(series[-1].get(key, 0.0))
            v_prev = float(series[-1-window].get(key, 0.0))
            return v_now - v_prev
        except Exception:
            return 0.0

    # ---------------------------------------------------------
    # Risk zone
    # ---------------------------------------------------------
    @staticmethod
    def _risk_zone(total):
        if total >= 25000:
            return "高"
        if total >= 15000:
            return "中"
        return "低"

    # ---------------------------------------------------------
    # Normalize / clean history into unified schema
    # ---------------------------------------------------------
    def _normalize_history(self, hist: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned = []
        for row in hist:
            rz = float(row.get("rz_balance", row.get("rz", 0.0)))
            rq = float(row.get("rq_balance", row.get("rq", 0.0)))
            total = float(row.get("total", row.get("rzrq", rz + rq)))

            cleaned.append({
                "date": row.get("date"),
                "rz_balance": rz,
                "rq_balance": rq,
                "total": total,
                "rz_buy": float(row.get("rz_buy", 0.0)),
                "total_chg": float(row.get("total_chg", 0.0)),
                "rz_ratio": float(row.get("rz_ratio", 0.0)),
            })

        cleaned.sort(key=lambda x: x["date"])
        return cleaned

    # ---------------------------------------------------------
    # Main entry
    # ---------------------------------------------------------
    def get_margin_block(self, refresh=False) -> Dict[str, Any]:

        # 1) cache
        if not refresh:
            cached = self._load_cache()
            if isinstance(cached, dict):
                return cached

        # 2) fetch remote
        rows = self._fetch_recent_rows()
        if not rows:
            LOG.warning("[DS.Margin] Fallback to history")
            hist = self._load_history()
            hist = self._normalize_history(hist)

            if not hist:
                return {
                    "rz_balance": 0,
                    "rq_balance": 0,
                    "total": 0,
                    "trend_10d": 0,
                    "acc_3d": 0,
                    "risk_zone": "中",
                    "series": [],
                }

            last = hist[-1]
            block = {
                "rz_balance": last["rz_balance"],
                "rq_balance": last["rq_balance"],
                "total": last["total"],
                "trend_10d": 0,
                "acc_3d": 0,
                "risk_zone": self._risk_zone(last["total"]),
                "series": hist,
            }

            self._save_cache(block)
            return block

        # 3) merge + normalize history
        hist = self._load_history()
        existed_dates = {x.get("date") for x in hist}

        for r in rows:
            if r["date"] not in existed_dates:
                hist.append(r)

        hist = self._normalize_history(hist)
        self._save_history(hist)

        today = hist[-1]
        # ---- 如果最新的数据 total=0 或 None，则需要回退一天 ----
        if not self._is_valid_record(today):
            LOG.warning(f"[DS.Margin] Today record invalid: date={today.get('date')} total={today.get('total')}")
            
            fallback = None
            for row in reversed(hist[:-1]):  # 跳过最后一条，从倒数第二条往回找
                if self._is_valid_record(row):
                    fallback = row
                    break

            if fallback is None:
                LOG.error("[DS.Margin] No valid fallback record found, return neutral block")
                block = {
                    "rz_balance": 0,
                    "rq_balance": 0,
                    "total": 0,
                    "trend_10d": 0,
                    "acc_3d": 0,
                    "risk_zone": "中",
                    "series": hist,
                }
                self._save_cache(block)
                return block

            LOG.warning(f"[DS.Margin] Fallback to previous valid record: {fallback['date']}")
            today = fallback





        # 4) analytics
        trend_10d = self._diff(hist, 10, "total")
        acc_3d = self._diff(hist, 3, "total")

        block = {
            "rz_balance": today["rz_balance"],
            "rq_balance": today["rq_balance"],
            "total": today["total"],
            "rz_buy": today["rz_buy"],
            "total_chg": today["total_chg"],
            "rz_ratio": today["rz_ratio"],
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "risk_zone": self._risk_zone(today["total"]),
            "series": hist,
        }

        self._save_cache(block)

        LOG.info(
            f"[DS.Margin] block: total={today['total']:.2f}, "
            f"trend10={trend_10d:.2f}, acc3={acc_3d:.2f}, zone={block['risk_zone']}"
        )

        return block
