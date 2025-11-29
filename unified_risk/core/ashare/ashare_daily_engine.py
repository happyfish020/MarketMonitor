from __future__ import annotations

from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, Optional

from unified_risk.common.logger import get_logger
from unified_risk.common.yf_fetcher import YFETFClient
from unified_risk.core.ashare.data_fetcher import AShareDataFetcher
from unified_risk.core.ashare.scorer import AShareRiskScorer
from unified_risk.core.ashare.factors.northbound_factor import NorthboundFactor

BJ_TZ = timezone(timedelta(hours=8))
LOG = get_logger("UnifiedRisk.Engine.AShareDaily")


class AShareDailyEngine:
    def __init__(self) -> None:
        self.yf_client = YFETFClient()
        self.fetcher = AShareDataFetcher(self.yf_client)
        self.nb_factor = NorthboundFactor(self.yf_client)
        self.scorer = AShareRiskScorer()

    def run(self, run_time: Optional[datetime | date] = None) -> Dict[str, Any]:
        if isinstance(run_time, date) and not isinstance(run_time, datetime):
            run_dt = datetime(run_time.year, run_time.month, run_time.day, 18, 0, tzinfo=BJ_TZ)
        elif isinstance(run_time, datetime):
            run_dt = run_time.astimezone(BJ_TZ)
        else:
            run_dt = datetime.now(BJ_TZ)

        LOG.info("[AShareDaily] Start run at %s", run_dt.isoformat())

        snapshot = self.fetcher.build_daily_snapshot(run_dt)
        nb_snap = self.nb_factor.compute(run_dt)
        scores = self.scorer.score_daily(snapshot, nb_snap)

        result: Dict[str, Any] = {
            "meta": {
                "bj_time": run_dt.isoformat(),
                "version": "UnifiedRisk_v7.5.8",
            },
            "raw": {
                "snapshot": snapshot,
                "northbound": nb_snap.to_dict(),
            },
            "scores": scores,
        }

        result.update(scores)
        result["northbound_score"] = nb_snap.northbound_score
        result["nb_nps_score"] = nb_snap.nb_nps_score

        LOG.info(
            "[AShareDaily] Finished: total_risk_score=%.3f",
            scores.get("total_risk_score", 0.0),
        )
        return result
