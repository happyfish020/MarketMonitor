
from __future__ import annotations
from typing import Dict, Any
import logging
from datetime import datetime

from unified_risk.core.datasources.index_fetcher import (
    fetch_index_snapshot,
    get_a50_night_session,
)
from unified_risk.core.datasources.commodity_fetcher import get_commodity_snapshot

logger = logging.getLogger(__name__)

class AshareDataFetcher:

    def _log_raw(self, name: str, key: str, value: Any):
        logger.info(f"[RAW] {name:12s} | {key:12s}: {value:8.3f}" if isinstance(value,float) else f"[RAW] {name:12s} | {key}: {value}")

    # ---------------------------
    # 国债收益率（10Y/5Y）
    # ---------------------------
    def get_treasury_yield(self) -> Dict[str, float]:
        ten  = fetch_index_snapshot("^TNX")
        five = fetch_index_snapshot("^FVX")

        t_last  = ten.get("last", 0.0)
        f_last  = five.get("last", 0.0)
        diff = (t_last - f_last) * 100 if t_last and f_last else 0.0

        self._log_raw("Treasury(YF)", "10Y(%)", t_last)
        self._log_raw("Treasury(YF)", "5Y(%)", f_last)
        self._log_raw("Treasury(YF)", "Y.Curve(bps)", diff)

        return {"yield_jump": 0.0, "yield_curve_diff": diff}

    # ---------------------------
    # 美国股市
    # ---------------------------
    def get_us_equity_snapshot(self) -> Dict[str, Any]:
        ndx = fetch_index_snapshot("^IXIC")
        spy = fetch_index_snapshot("SPY")
        vix = fetch_index_snapshot("^VIX")

        self._log_raw("^IXIC", "Change%", ndx.get("pct",0))
        self._log_raw("SPY",   "Change%", spy.get("pct",0))
        self._log_raw("^VIX",  "Price",   vix.get("last",0))

        return {
            "nasdaq": {"price": ndx.get("last",0), "changePct": ndx.get("pct",0)},
            "spy":    {"price": spy.get("last",0), "changePct": spy.get("pct",0)},
            "vix":    {"price": vix.get("last",0), "changePct": vix.get("pct",0)},
        }

    # ---------------------------
    # 欧洲市场
    # ---------------------------
    def get_eu_futures(self) -> float:
        dax  = fetch_index_snapshot("^GDAXI")
        ftse = fetch_index_snapshot("^FTSE")

        self._log_raw("^GDAXI","Change%", dax.get("pct",0))
        self._log_raw("^FTSE","Change%",  ftse.get("pct",0))

        return dax.get("pct",0) or ftse.get("pct",0)

    # ---------------------------
    # 亚洲指数（日经/韩国）
    # ---------------------------
    def get_asian_market(self) -> Dict[str, float]:
        nk = fetch_index_snapshot("^N225")
        ks = fetch_index_snapshot("^KS11")
        return {"nikkei_vol": abs(nk.get("pct",0)), "kospi_vol": abs(ks.get("pct",0))}

    # ---------------------------
    # 上证 & 创业板指数
    # ---------------------------
    def get_china_index_snapshot(self) -> Dict[str, Any]:
        sh  = fetch_index_snapshot("000001.SS")
        cyb = fetch_index_snapshot("399006.SZ")

        self._log_raw("SH", "Change%", sh.get("pct",0))
        self._log_raw("CYB","Change%", cyb.get("pct",0))

        return {
            "sh":  {"price": sh.get("last",0),  "changePct": sh.get("pct",0)},
            "cyb": {"price": cyb.get("last",0), "changePct": cyb.get("pct",0)},
        }

    # ---------------------------
    # 大宗商品
    # ---------------------------
    def get_commodity(self) -> Dict[str, Any]:
        snap = get_commodity_snapshot()
        return snap

    # ---------------------------
    # A50 夜盘
    # ---------------------------
    def get_a50(self) -> Dict[str, Any]:
        return get_a50_night_session()

    # ----------------------------------------------------
    # 统一 A 股日级 snapshot（完全替代旧 prepare_daily_market_snapshot）
    # ----------------------------------------------------
    def prepare_daily_market_snapshot(self, bj_time: datetime) -> Dict[str, Any]:
        """
        返回完整 A 股日级快照：
         - 中国市场（上证 / 创业板）
         - 美国市场（纳指 / SPY / VIX）
         - 欧洲市场（DAX / FTSE）
         - 亚洲市场（日经 / 韩国）
         - 国债（10Y / 5Y）
         - 大宗商品（黄金 / 原油 / 铜 / 美元指数）
         - A50 夜盘指数
        """

        snapshot = {}

        # ---- China ----
        cn = self.get_china_index_snapshot()
        snapshot["sh"]  = cn["sh"]
        snapshot["cyb"] = cn["cyb"]

        # ---- US equities ----
        us = self.get_us_equity_snapshot()
        snapshot["us_nasdaq"] = us["nasdaq"]
        snapshot["us_spy"] = us["spy"]
        snapshot["us_vix"] = us["vix"]

        # ---- Europe ----
        snapshot["eu_futures"] = self.get_eu_futures()

        # ---- Asia ----
        snapshot["asia"] = self.get_asian_market()

        # ---- Treasury ----
        snapshot["treasury"] = self.get_treasury_yield()

        # ---- Commodity ----
        snapshot["commodity"] = self.get_commodity()

        # ---- A50 night session ----
        snapshot["a50_night"] = self.get_a50()

        return snapshot
