from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd

from unified_risk.common.yf_fetcher import YFETFClient
from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Factor.Northbound")

BJ_TZ = timezone(timedelta(hours=8))

ETF_WEIGHTS: Dict[str, float] = {
    "2800.HK": 0.20,
    "510300": 0.25,
    "159919": 0.20,
    "510500": 0.15,
    "159915": 0.10,
    "159901": 0.10,
}

HISTORY_PATH = Path("data") / "northbound" / "nps_history.csv"
HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class NorthboundSnapshot:
    date: str
    nps_today: float
    trend_3d: float
    northbound_score: float
    nb_nps_score: float
    level: str
    advise: str
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class NorthboundFactor:
    def __init__(self, yf_client: Optional[YFETFClient] = None) -> None:
        self.yf = yf_client or YFETFClient()

    def _get_2800_from_yf(self) -> Tuple[float, bool]:
        try:
            pct = self.yf.get_latest_change_pct("2800.HK")
            if pct is None:
                LOG.warning("[Northbound] YF 2800.HK returned None")
                return 0.0, False
            return float(pct), True
        except Exception as e:
            LOG.error("[Northbound] YF 2800.HK error: %s", e)
            return 0.0, False

    def compute(self, bj_time: Optional[datetime] = None) -> NorthboundSnapshot:
        bj_now = bj_time.astimezone(BJ_TZ) if bj_time else datetime.now(BJ_TZ)
        trade_date = self._get_trade_date(bj_now)

        LOG.info("[Northbound] Compute NPS for %s", trade_date)

        changes: Dict[str, float] = {}
        source_info: Dict[str, str] = {}

        pct_2800, ok_2800 = self._get_2800_from_yf()
        changes["2800.HK"] = pct_2800
        source_info["2800.HK"] = "YF_OK" if ok_2800 else "YF_FAIL"

        for sym in ETF_WEIGHTS:
            if sym == "2800.HK":
                continue
            chg = self.yf.get_latest_change_pct(sym)
            if chg is None:
                LOG.warning("[Northbound] %s change None, use 0.0", sym)
                changes[sym] = 0.0
                source_info[sym] = "YF_FAIL"
            else:
                changes[sym] = float(chg)
                source_info[sym] = "YF_OK"

        nps_today, detail_rows = self._calc_nps(changes)

        hist = self._load_history()
        hist = self._upsert_history(hist, trade_date, nps_today)
        self._save_history(hist)
        trend_3d = self._calc_trend_3d(hist, trade_date)

        northbound_score = self._score_direction(nps_today, trend_3d)
        nb_nps_score = self._score_strength(nps_today)
        level, advise = self._classify_view(northbound_score, nb_nps_score)

        snap = NorthboundSnapshot(
            date=trade_date,
            nps_today=nps_today,
            trend_3d=trend_3d,
            northbound_score=northbound_score,
            nb_nps_score=nb_nps_score,
            level=level,
            advise=advise,
            details={
                "etf_changes": changes,
                "etf_weights": ETF_WEIGHTS,
                "source_info": source_info,
                "rows": detail_rows,
                "history_tail": hist.tail(5).to_dict(orient="records"),
            },
        )
        LOG.info(
            "[Northbound] NPS=%.3f trend=%.3f score=%.1f strength=%.1f level=%s",
            nps_today,
            trend_3d,
            northbound_score,
            nb_nps_score,
            level,
        )
        return snap

    @staticmethod
    def _get_trade_date(bj_now: datetime) -> str:
        d = bj_now.date()
        if bj_now.weekday() == 5:
            d = d - timedelta(days=1)
        elif bj_now.weekday() == 6:
            d = d - timedelta(days=2)
        return d.strftime("%Y-%m-%d")

    def _calc_nps(self, changes: Dict[str, float]) -> Tuple[float, List[Dict[str, Any]]]:
        nps = 0.0
        rows: List[Dict[str, Any]] = []
        for sym, w in ETF_WEIGHTS.items():
            chg = changes.get(sym, 0.0)
            contrib = w * chg
            nps += contrib
            rows.append(
                {
                    "symbol": sym,
                    "weight": w,
                    "change_pct": chg,
                    "contrib": contrib,
                }
            )
        return nps, rows

    def _load_history(self) -> pd.DataFrame:
        if not HISTORY_PATH.exists():
            return pd.DataFrame(columns=["date", "nps"])
        try:
            df = pd.read_csv(HISTORY_PATH)
            if "date" not in df.columns or "nps" not in df.columns:
                return pd.DataFrame(columns=["date", "nps"])
            return df
        except Exception as e:
            LOG.error("[Northbound] load history error: %s", e)
            return pd.DataFrame(columns=["date", "nps"])

    def _upsert_history(self, hist: pd.DataFrame, date_str: str, nps_today: float) -> pd.DataFrame:
        if hist.empty:
            return pd.DataFrame([{"date": date_str, "nps": nps_today}])
        hist = hist[hist["date"] != date_str]
        hist = pd.concat(
            [hist, pd.DataFrame([{"date": date_str, "nps": nps_today}])],
            ignore_index=True,
        )
        try:
            hist["date_dt"] = pd.to_datetime(hist["date"])
            hist = hist.sort_values("date_dt").drop(columns=["date_dt"])
        except Exception:
            pass
        return hist

    def _save_history(self, hist: pd.DataFrame) -> None:
        try:
            HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
            hist.to_csv(HISTORY_PATH, index=False, encoding="utf-8-sig")
        except Exception as e:
            LOG.error("[Northbound] save history error: %s", e)

    @staticmethod
    def _calc_trend_3d(hist: pd.DataFrame, date_str: str) -> float:
        if hist.empty or len(hist) < 2:
            return 0.0
        try:
            hist = hist.copy()
            hist["date_dt"] = pd.to_datetime(hist["date"])
            hist = hist.sort_values("date_dt").drop(columns=["date_dt"])
        except Exception:
            pass

        if date_str not in set(hist["date"]):
            window = hist["nps"].tail(3)
        else:
            idx = hist.index[hist["date"] == date_str][-1]
            sub = hist.loc[:idx]
            window = sub["nps"].tail(3)

        if len(window) == 0:
            return 0.0
        ma3 = float(window.mean())
        return ma3 - float(window.iloc[-1])

    @staticmethod
    def _score_direction(nps_today: float, trend_3d: float) -> float:
        base = 0.0
        if nps_today >= 1.5:
            base += 2.0
        elif nps_today >= 0.5:
            base += 1.0
        elif nps_today <= -1.5:
            base -= 2.0
        elif nps_today <= -0.5:
            base -= 1.0

        if trend_3d >= 0.5:
            base += 1.0
        elif trend_3d <= -0.5:
            base -= 1.0

        return max(-3.0, min(3.0, base))

    @staticmethod
    def _score_strength(nps_today: float) -> float:
        x = abs(nps_today)
        if x >= 2.5:
            return 5.0
        if x >= 1.5:
            return 3.5
        if x >= 0.8:
            return 2.0
        if x >= 0.3:
            return 1.0
        return 0.0

    @staticmethod
    def _classify_view(northbound_score: float, nb_nps_score: float) -> Tuple[str, str]:
        if northbound_score >= 2:
            return "净流入偏强", "北向偏多，对指数有明显支撑。"
        if northbound_score <= -2:
            return "净流出偏强", "北向持续流出，需警惕系统性压力。"
        if -1 <= northbound_score <= 1 and nb_nps_score <= 1.0:
            return "中性偏弱", "北向整体影响有限，略偏弱。"
        return "中性偏多", "北向略偏多，对权重股有一定支撑。"
