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
  


@dataclass(frozen=True)
class AggregationConfig:
    lookback_days: int = 20
    horizons: Tuple[int, ...] = (1, 3, 5)
    default_symbol: str = "HS300"  # claims without target_symbol use this


class ExternalValidationAggregator:
    def __init__(self, conn: sqlite3.Connection, cfg: Optional[AggregationConfig] = None):
        self._conn = conn
        self._cfg = cfg or AggregationConfig()

    def run(self, asof_date: str) -> None:
        """
        Compute:
        - ext_claim_followup for claims within lookback window ending at asof_date
        - ext_validation_summary (rates + avg follow-up returns)
        """
        trade_dates = self._get_lookback_trade_dates(asof_date, self._cfg.lookback_days)
        if not trade_dates:
            return

        claims = self._load_active_claims()
        claim_map = {c["claim_id"]: c for c in claims}

        # 1) follow-up returns per (trade_date, claim_id, horizon)
        for td in trade_dates:
            verdicts = self._load_verdicts(td)
            if not verdicts:
                continue

            for v in verdicts:
                claim_id = v["claim_id"]
                claim = claim_map.get(claim_id)
                if claim is None:
                    # claim removed/disabled after the fact → treat as skip (do not mutate history)
                    continue

                target_symbol = claim.get("target_symbol") or self._cfg.default_symbol
                close_t = self._get_close(td, target_symbol)
                if close_t is None:
                    # cannot compute follow-up, but still keep summary counts
                    continue

                for h in self._cfg.horizons:
                    td_h = self._shift_trade_date(td, target_symbol, h)
                    if td_h is None:
                        continue
                    close_h = self._get_close(td_h, target_symbol)
                    if close_h is None:
                        continue
                    ret_cum = (close_h / close_t) - 1.0
                    self._upsert_followup(td, claim_id, h, target_symbol, ret_cum)

        # 2) summary rows for each horizon at asof_date
        for h in self._cfg.horizons:
            self._upsert_summary(asof_date, trade_dates, horizon=h)

        self._conn.commit()

    # -------------------------
    # Reads
    # -------------------------

    def _load_active_claims(self) -> List[Dict]:
        rows = self._conn.execute(
            """
            SELECT claim_id, claim_type, target_symbol, expected_regime
            FROM ext_report_claim
            WHERE active=1
            """
        ).fetchall()
        out = []
        for claim_id, claim_type, target_symbol, expected_regime in rows:
            out.append(
                {
                    "claim_id": claim_id,
                    "claim_type": claim_type,
                    "target_symbol": target_symbol,
                    "expected_regime": expected_regime,
                }
            )
        return out

    def _load_verdicts(self, trade_date: str) -> List[Dict]:
        rows = self._conn.execute(
            """
            SELECT claim_id, verdict
            FROM ext_claim_verdict
            WHERE trade_date=?
            """,
            (trade_date,),
        ).fetchall()
        return [{"claim_id": cid, "verdict": v} for cid, v in rows]

    def _get_close(self, trade_date: str, symbol: str) -> Optional[float]:
        row = self._conn.execute(
            """
            SELECT close
            FROM ext_market_snapshot
            WHERE trade_date=? AND symbol=?
            """,
            (trade_date, symbol),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])

    def _get_lookback_trade_dates(self, asof_date: str, lookback_days: int) -> List[str]:
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
        # reverse to chronological for readability
        return [r[0] for r in reversed(rows)]

    def _shift_trade_date(self, trade_date: str, symbol: str, n: int) -> Optional[str]:
        """
        Find the trade_date shifted by +n within ext_market_snapshot for given symbol.
        Uses ordering by trade_date as the market calendar proxy.
        """
        rows = self._conn.execute(
            """
            SELECT trade_date
            FROM ext_market_snapshot
            WHERE symbol=?
            ORDER BY trade_date
            """,
            (symbol,),
        ).fetchall()
        dates = [r[0] for r in rows]
        try:
            i = dates.index(trade_date)
        except ValueError:
            return None
        j = i + n
        if j < 0 or j >= len(dates):
            return None
        return dates[j]

    # -------------------------
    # Writes
    # -------------------------

    def _upsert_followup(
        self,
        trade_date: str,
        claim_id: str,
        horizon: int,
        target_symbol: str,
        ret_cum: float,
    ) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO ext_claim_followup
            (trade_date, claim_id, horizon, target_symbol, ret_cum)
            VALUES (?, ?, ?, ?, ?)
            """,
            (trade_date, claim_id, horizon, target_symbol, float(ret_cum)),
        )

    def _upsert_summary(self, asof_date: str, trade_dates: List[str], horizon: int) -> None:
        # counts by verdict across the window
        placeholders = ",".join(["?"] * len(trade_dates))

        rows = self._conn.execute(
            f"""
            SELECT v.verdict, COUNT(1)
            FROM ext_claim_verdict v
            WHERE v.trade_date IN ({placeholders})
            GROUP BY v.verdict
            """,
            tuple(trade_dates),
        ).fetchall()

        counts = {"SUPPORTED": 0, "WEAKENED": 0, "UNVERIFIABLE": 0}
        for verdict, cnt in rows:
            if verdict in counts:
                counts[verdict] = int(cnt)

        total = sum(counts.values())
        supported_rate = (counts["SUPPORTED"] / total) if total > 0 else None

        # follow-up averages (all + supported)
        # join verdict to followup on horizon
        rows_all = self._conn.execute(
            f"""
            SELECT f.ret_cum
            FROM ext_claim_followup f
            WHERE f.horizon=?
              AND f.trade_date IN ({placeholders})
              AND f.ret_cum IS NOT NULL
            """,
            (horizon, *trade_dates),
        ).fetchall()

        rows_sup = self._conn.execute(
            f"""
            SELECT f.ret_cum
            FROM ext_claim_followup f
            JOIN ext_claim_verdict v
              ON v.trade_date=f.trade_date AND v.claim_id=f.claim_id
            WHERE f.horizon=?
              AND f.trade_date IN ({placeholders})
              AND v.verdict='SUPPORTED'
              AND f.ret_cum IS NOT NULL
            """,
            (horizon, *trade_dates),
        ).fetchall()

        avg_all = self._avg([float(r[0]) for r in rows_all])
        avg_sup = self._avg([float(r[0]) for r in rows_sup])

        self._conn.execute(
            """
            INSERT OR REPLACE INTO ext_validation_summary
            (asof_date, lookback_days, horizon,
             total_cnt, supported_cnt, weakened_cnt, unverifiable_cnt,
             supported_rate, avg_ret_all, avg_ret_supported)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                asof_date,
                int(self._cfg.lookback_days),
                int(horizon),
                int(total),
                int(counts["SUPPORTED"]),
                int(counts["WEAKENED"]),
                int(counts["UNVERIFIABLE"]),
                float(supported_rate) if supported_rate is not None else None,
                float(avg_all) if avg_all is not None else None,
                float(avg_sup) if avg_sup is not None else None,
            ),
        )

    def _avg(self, xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        return sum(xs) / len(xs)


 