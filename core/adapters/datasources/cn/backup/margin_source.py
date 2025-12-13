# -*- coding: utf-8 -*-
# ============================================================
# UnifiedRisk V12 - Margin (两融) DataSource
# ------------------------------------------------------------
# 特点：
# - 遵循 refresh protocol（full / snapshot / none）
# - 从 DataSourceConfig 读取路径（无硬编码）
# - 所有异常必须 LOG.error
# - 完整保留旧版逻辑（fallback、merge history、趋势、加速度等）
# - 输出结构与 V12 因子完全兼容
# ============================================================

from __future__ import annotations

import os
import time
import requests
import json
from typing import Dict, Any, List

from core.datasources.datasource_base import BaseDataSource, DataSourceConfig
from core.utils.logger import get_logger
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.units import yuan_to_e9

LOG = get_logger("DS.Margin")


class MarginDataSource(BaseDataSource):

    def __init__(self, config: DataSourceConfig):
        """
        fetcher 必须传入 config:
            DataSourceConfig(market="cn", ds_name="margin")
        """
        super().__init__("MarginDataSource")

        self.config = config
        os.makedirs(config.cache_root, exist_ok=True)
        os.makedirs(config.history_root, exist_ok=True)

        self.cache_path = os.path.join(config.cache_root, "margin_today.json")
        self.history_path = os.path.join(config.history_root, "margin_series.json")

        LOG.info(
            f"[DS.Margin] Init: cache={self.cache_path}, history={self.history_path}"
        )

    # ============================================================
    # history load/save
    # ============================================================

    def _load_history(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.history_path):
            return []
        try:
            with open(self.history_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception as e:
            LOG.error(f"[DS.Margin] HistoryReadError: {e}")
            return []

    def _save_history(self, series: List[Dict[str, Any]]):
        try:
            with open(self.history_path, "w", encoding="utf-8") as f:
                json.dump(series, f, ensure_ascii=False, indent=2)
            LOG.info(f"[DS.Margin] HistorySaved rows={len(series)}")
        except Exception as e:
            LOG.error(f"[DS.Margin] HistorySaveError: {e}")

    # ============================================================
    # cache load/save
    # ============================================================

    def _load_cache(self):
        if not os.path.exists(self.cache_path):
            return None
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except Exception as e:
            LOG.error(f"[DS.Margin] CacheReadError: {e}")
            return None

    def _save_cache(self, block):
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
            LOG.info("[DS.Margin] CacheSaved")
        except Exception as e:
            LOG.error(f"[DS.Margin] CacheWriteError: {e}")

    # ============================================================
    # Fetch from EastMoney
    # ============================================================

    EM_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

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
                "Chrome/120 Safari/537.36"
            ),
            "Referer": "https://data.eastmoney.com/rzrq/",
        }

        def safe_num(v):
            try:
                return float(v) if v is not None else 0.0
            except Exception:
                return 0.0

        for attempt in range(3):
            try:
                LOG.info(f"[DS.Margin] FETCH attempt={attempt+1}")

                resp = requests.get(self.EM_URL, params=params, headers=headers, timeout=20)
                resp.raise_for_status()

                rows = (resp.json().get("result") or {}).get("data") or []
                if not rows:
                    LOG.warning("[DS.Margin] EmptyResponse")
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
                        LOG.error(f"[DS.Margin] RowParseError: {e}")

                parsed.sort(key=lambda x: x["date"])
                LOG.info(f"[DS.Margin] FETCH OK rows={len(parsed)}")

                return parsed

            except Exception as e:
                LOG.error(f"[DS.Margin] FetchError: {e}")
                time.sleep(1)

        LOG.error("[DS.Margin] FETCH FAILED 3 attempts")
        return []

    # ============================================================
    # Trend / Acceleration
    # ============================================================

    @staticmethod
    def _diff(series, window, key):
        if len(series) < window + 1:
            return 0.0
        try:
            return float(series[-1][key]) - float(series[-1-window][key])
        except Exception:
            return 0.0

    # ============================================================
    # Risk zone
    # ============================================================

    @staticmethod
    def _risk_zone(total):
        if total >= 25000:
            return "高"
        if total >= 15000:
            return "中"
        return "低"

    # ============================================================
    # 主入口：get_margin_block
    # ============================================================

    # ============================================================
    # 主入口：get_margin_block（必须与 fetcher 调用保持一致）
    # ============================================================
    def get_margin_block(self, trade_date: str, refresh_mode="none") -> Dict[str, Any]:
        """
        refresh_mode:
          - "full"      → 删 今日 cache + history
          - "snapshot"  → 删 今日 cache
          - "none"      → 不删任何文件（直接尝试读 cache）
        """

        # ---- 将 refresh_mode 规范化为字符串模式 ----
        if isinstance(refresh_mode, str):
            mode = refresh_mode.strip().lower() or "none"
        else:
            # bool 兼容
            mode = "snapshot" if refresh_mode else "none"

        # ------------------------------
        # Step 1 — 按协议清理（与 NPS 一致）
        # ------------------------------
        mode = apply_refresh_cleanup(
            refresh_mode=mode,
            cache_path=self.cache_path,
            history_path=self.history_path,
            spot_path=None,
        )

        LOG.info(f"[DS.Margin] Request trade_date={trade_date}, mode={mode}")

        # ------------------------------
        # Step 2 — cache 存在 → 直接返回
        # ------------------------------
        if mode == "none":
            cached = self._load_cache()
            if cached is not None:
                return cached

        # ------------------------------
        # Step 3 — 远程抓取
        # ------------------------------
        rows = self._fetch_recent_rows()
        if not rows:
            LOG.warning("[DS.Margin] NoRemote → fallback history")
            hist = self._load_history()
            if not hist:
                block = {
                    "rz_balance": 0.0,
                    "rq_balance": 0.0,
                    "total": 0.0,
                    "trend_10d": 0.0,
                    "acc_3d": 0.0,
                    "risk_zone": "中",
                    "series": [],
                }
                self._save_cache(block)
                return block

            last = hist[-1]
            block = {
                "rz_balance": last["rz_balance"],
                "rq_balance": last["rq_balance"],
                "total": last["total"],
                "trend_10d": 0.0,
                "acc_3d": 0.0,
                "risk_zone": self._risk_zone(last["total"]),
                "series": hist,
            }
            self._save_cache(block)
            return block

        # ------------------------------
        # Step 4 — merge + save history
        # ------------------------------
        hist = self._load_history()
        existed = {x["date"] for x in hist}

        for r in rows:
            if r["date"] not in existed:
                hist.append(r)

        hist.sort(key=lambda x: x["date"])
        hist = hist[-400:]
        self._save_history(hist)

        today = hist[-1]

        # ---- fallback if invalid ----
        if float(today["total"]) <= 0:
            fallback = None
            for row in reversed(hist[:-1]):
                if float(row["total"]) > 0:
                    fallback = row
                    break
            if fallback is None:
                LOG.error("[DS.Margin] NoValidFallback")
                today = {"rz_balance": 0, "rq_balance": 0, "total": 0}
            else:
                today = fallback

        # ------------------------------
        # Step 5 — compute features
        # ------------------------------
        trend_10d = self._diff(hist, 10, "total")
        acc_3d = self._diff(hist, 3, "total")

        block = {
            "rz_balance": today["rz_balance"],
            "rq_balance": today["rq_balance"],
            "total": today["total"],
            "rz_buy": today.get("rz_buy", 0.0),
            "total_chg": today.get("total_chg", 0.0),
            "rz_ratio": today.get("rz_ratio", 0.0),
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "risk_zone": self._risk_zone(today["total"]),
            "series": hist,
        }

        # ------------------------------
        # Step 6 — 写入 cache
        # ------------------------------
        self._save_cache(block)
        return block
 