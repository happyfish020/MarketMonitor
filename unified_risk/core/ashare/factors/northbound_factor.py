# unified_risk/core/ashare/factors/northbound_factor.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from unified_risk.common.yf_fetcher import YFETFClient
from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Factor.Northbound")

BJ_TZ = timezone(timedelta(hours=8))

# 北向代替用 ETF 列表 + 权重
ETF_WEIGHTS: Dict[str, float] = {
    "02800.HK": 0.20,   # 恒指 ETF
    "510300.SS": 0.25,  # 沪深300
    "159919.SZ": 0.20,  # 沪深300(SZ)
    "510500.SS": 0.15,  # 中证500
    "159915.SZ": 0.10,  # 创业板
    "159901.SZ": 0.10,  # 深成指
}

DEFAULT_HISTORY_PATH = Path("data") / "northbound" / "nps_history.csv"


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
    """
    v7.5.3 北向 NPS 因子：
      - 多只宽基 ETF 涨跌幅 → NPS 当日值
      - N 日历史 → 3 日 MA → 趋势 trend_3d
      - northbound_score：[-3, +3]
      - nb_nps_score：强度 [0, 5]
    """

    def __init__(
        self,
        yf_client: Optional[YFETFClient] = None,
        history_path: Path | str = DEFAULT_HISTORY_PATH,
    ) -> None:
        self.yf = yf_client or YFETFClient()
        self.history_path = Path(history_path)
        self.history_path.parent.mkdir(parents=True, exist_ok=True)

    def compute(self, bj_time: Optional[datetime] = None) -> NorthboundSnapshot:
        bj_now = bj_time.astimezone(BJ_TZ) if bj_time else datetime.now(BJ_TZ)
        trade_date = self._get_trade_date(bj_now)

        LOG.info("[Northbound] Compute NPS for trade_date=%s", trade_date)

        etf_changes = self.yf.get_multi_latest_change_pct(list(ETF_WEIGHTS.keys()))
        nps_today, detail = self._calc_nps_from_etf(etf_changes)

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
                "etf_changes": etf_changes,
                "etf_weights": ETF_WEIGHTS,
                "nps_detail": detail,
                "history_tail": hist.tail(5).to_dict(orient="records"),
            },
        )

        LOG.info(
            "[Northbound] NPS=%.3f trend_3d=%.3f score=%.1f strength=%.1f level=%s",
            nps_today,
            trend_3d,
            northbound_score,
            nb_nps_score,
            level,
        )
        return snap

    # ---------- trade_date ----------
    @staticmethod
    def _get_trade_date(bj_now: datetime) -> str:
        d = bj_now.date()
        if bj_now.weekday() == 5:   # 周六
            d = d - timedelta(days=1)
        elif bj_now.weekday() == 6: # 周日
            d = d - timedelta(days=2)
        return d.strftime("%Y-%m-%d")

    # ---------- NPS from ETF ----------
    def _calc_nps_from_etf(self, changes: Dict[str, Optional[float]]) -> Tuple[float, Dict[str, Any]]:
        nps = 0.0
        rows: List[Dict[str, Any]] = []

        for sym, w in ETF_WEIGHTS.items():
            chg = changes.get(sym)
            if chg is None:
                LOG.warning("[Northbound] ETF %s change None, treat as 0.", sym)
                chg_val = 0.0
            else:
                chg_val = float(chg)
            contrib = w * chg_val
            nps += contrib
            rows.append(
                {
                    "symbol": sym,
                    "weight": w,
                    "change_pct": chg_val,
                    "contrib": contrib,
                }
            )
        return nps, {"rows": rows, "sum_contrib": nps}

    # ---------- 历史 ----------
    def _load_history(self) -> pd.DataFrame:
        if not self.history_path.exists():
            return pd.DataFrame(columns=["date", "nps"])
        try:
            df = pd.read_csv(self.history_path)
            if "date" not in df.columns or "nps" not in df.columns:
                return pd.DataFrame(columns=["date", "nps"])
            return df
        except Exception as e:
            LOG.error("[Northbound] load history failed: %s", e, exc_info=True)
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
            self.history_path.parent.mkdir(parents=True, exist_ok=True)
            hist.to_csv(self.history_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            LOG.error("[Northbound] save history failed: %s", e, exc_info=True)

    # ---------- 趋势 ----------
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
            nps_today = float(hist.iloc[-1]["nps"])
            window = hist["nps"].tail(3)
        else:
            idx = hist.index[hist["date"] == date_str][-1]
            sub = hist.loc[:idx]
            nps_today = float(sub.iloc[-1]["nps"])
            window = sub["nps"].tail(3)

        if len(window) == 0:
            return 0.0
        ma3 = float(window.mean())
        return ma3 - nps_today

    # ---------- 评分 ----------
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
            return "净流入偏强", "北向相对偏多，短期对指数有托底/拉升作用。"
        if northbound_score <= -2:
            return "净流出偏强", "北向明显流出，注意外资集中撤退带来的系统性压力。"
        if -1 <= northbound_score <= 1 and nb_nps_score <= 1.0:
            return "中性偏弱", "北向整体中性略偏弱，对大盘方向影响有限。"
        return "中性偏多", "北向略偏多，对个别权重股有一定支撑。"
