# -*- coding: utf-8 -*-
"""
External Validation Aggregator (MVP)
- Reads: ext_report_claim, ext_claim_verdict, ext_market_snapshot
- Writes: ext_claim_followup, ext_validation_summary
- No feedback into UnifiedRisk Core.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
 
class ExternalValidationByClaimType:
    def __init__(self, conn: sqlite3.Connection):
        if conn is None:
            raise ValueError("sqlite3.Connection is required")
        self._conn = conn

    def run(
        self,
        asof_date: str,
        lookback_days: int,
        horizons: List[int],
        persist: bool = True,
    ) -> Dict:
        """
        Aggregate validation results by claim_type for given horizons.
        Returns a nested dict; optionally persists to ext_validation_by_claim_type.
        """
        trade_dates = self._get_lookback_trade_dates(asof_date, lookback_days)
        if not trade_dates:
            return {}

        result: Dict = {}

        for h in horizons:
            rows = self._conn.execute(
                f"""
                SELECT
                    c.claim_type                                  AS claim_type,
                    v.verdict                                     AS verdict,
                    COUNT(1)                                      AS cnt,
                    AVG(f.ret_cum)                                AS avg_ret
                FROM ext_claim_verdict v
                JOIN ext_report_claim c
                  ON c.claim_id = v.claim_id
                LEFT JOIN ext_claim_followup f
                  ON f.trade_date = v.trade_date
                 AND f.claim_id  = v.claim_id
                 AND f.horizon   = ?
                WHERE v.trade_date IN ({",".join(["?"] * len(trade_dates))})
                  AND c.active = 1
                GROUP BY c.claim_type, v.verdict
                """,
                (h, *trade_dates),
            ).fetchall()

            bucket: Dict[str, Dict] = {}
            for claim_type, verdict, cnt, avg_ret in rows:
                d = bucket.setdefault(
                    claim_type,
                    {
                        "total": 0,
                        "SUPPORTED": {"cnt": 0, "avg_ret": None},
                        "WEAKENED": {"cnt": 0, "avg_ret": None},
                        "UNVERIFIABLE": {"cnt": 0, "avg_ret": None},
                    },
                )
                d["total"] += int(cnt)
                d[verdict]["cnt"] = int(cnt)
                d[verdict]["avg_ret"] = (
                    float(avg_ret) if avg_ret is not None else None
                )

            # finalize metrics
            for claim_type, d in bucket.items():
                sup_cnt = d["SUPPORTED"]["cnt"]
                d["supported_rate"] = (
                    sup_cnt / d["total"] if d["total"] > 0 else None
                )

                # avg_ret_all = weighted avg across all verdicts with available avg_ret
                d["avg_ret_all"] = self._weighted_avg(
                    [
                        (d[v]["cnt"], d[v]["avg_ret"])
                        for v in ("SUPPORTED", "WEAKENED", "UNVERIFIABLE")
                    ]
                )
                d["avg_ret_supported"] = d["SUPPORTED"]["avg_ret"]

                if persist:
                    self._upsert_row(
                        asof_date=asof_date,
                        lookback_days=lookback_days,
                        horizon=h,
                        claim_type=claim_type,
                        total_cnt=d["total"],
                        supported_cnt=d["SUPPORTED"]["cnt"],
                        weakened_cnt=d["WEAKENED"]["cnt"],
                        unverifiable_cnt=d["UNVERIFIABLE"]["cnt"],
                        supported_rate=d["supported_rate"],
                        avg_ret_all=d["avg_ret_all"],
                        avg_ret_supported=d["avg_ret_supported"],
                    )

            result[h] = bucket

        self._conn.commit()
        return result

    # -------------------------
    # helpers
    # -------------------------

    def _get_lookback_trade_dates(
        self,
        asof_date: str,
        lookback_days: int,
    ) -> List[str]:
        rows = self._conn.execute(
            """
            SELECT DISTINCT trade_date
            FROM ext_market_snapshot
            WHERE trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (asof_date, lookback_days),
        ).fetchall()
        return [r[0] for r in reversed(rows)]

    def _weighted_avg(self, xs: List[tuple]) -> Optional[float]:
        num = 0.0
        den = 0.0
        for cnt, avg in xs:
            if cnt and avg is not None:
                num += cnt * avg
                den += cnt
        if den == 0.0:
            return None
        return num / den

    def _upsert_row(
        self,
        asof_date: str,
        lookback_days: int,
        horizon: int,
        claim_type: str,
        total_cnt: int,
        supported_cnt: int,
        weakened_cnt: int,
        unverifiable_cnt: int,
        supported_rate: Optional[float],
        avg_ret_all: Optional[float],
        avg_ret_supported: Optional[float],
    ) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO ext_validation_by_claim_type
            (asof_date, lookback_days, horizon, claim_type,
             total_cnt, supported_cnt, weakened_cnt, unverifiable_cnt,
             supported_rate, avg_ret_all, avg_ret_supported)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asof_date,
                lookback_days,
                horizon,
                claim_type,
                total_cnt,
                supported_cnt,
                weakened_cnt,
                unverifiable_cnt,
                supported_rate,
                avg_ret_all,
                avg_ret_supported,
            ),
        )
