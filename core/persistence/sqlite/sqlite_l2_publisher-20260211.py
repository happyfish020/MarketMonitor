# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, Optional, Tuple

from core.persistence.contracts.errors import AlreadyPublishedError, PersistenceError
from core.persistence.sqlite.sqlite_report_store import SqliteReportStore
from core.persistence.sqlite.sqlite_des_store import SqliteDecisionEvidenceStore
from core.persistence.sqlite.sqlite_uow import SqliteUnitOfWork


class SqliteL2Publisher:
    """Atomic L2 publisher (S1): report + DES + link + audit in one transaction."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._report_store = SqliteReportStore(conn)
        self._des_store = SqliteDecisionEvidenceStore(conn)

    def publish(
        self,
        trade_date: str,
        report_kind: str,
        report_text: str,
        des_payload: Dict[str, Any],
        engine_version: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, str]:
        uow = SqliteUnitOfWork(self._conn)
        try:
            uow.begin_immediate()

            # ---- P0-2 (Frozen): enforce run_id parity in L2 payloads ----
            run_id = None
            if isinstance(meta, dict):
                run_id = meta.get("run_id")
            if run_id:
                # avoid mutating caller
                meta = dict(meta)
                meta["run_id"] = run_id
                if isinstance(des_payload, dict):
                    rp = des_payload.get("report_meta")
                    if not isinstance(rp, dict):
                        rp = {}
                        des_payload["report_meta"] = rp
                    rp["run_id"] = run_id
            # -------------------------------------------------------------
            report_hash = self._report_store.save_report(
                trade_date=trade_date,
                report_kind=report_kind,
                content_text=report_text,
                meta=meta,
            )
            des_hash = self._des_store.save_des(
                trade_date=trade_date,
                report_kind=report_kind,
                engine_version=engine_version,
                des_payload=des_payload,
            )

            created_at_utc = int(time.time())
            self._conn.execute(
                """INSERT INTO ur_report_des_link
                   (trade_date, report_kind, report_hash, des_hash, created_at_utc)
                   VALUES (?, ?, ?, ?, ?);""",
                (trade_date, report_kind, report_hash, des_hash, created_at_utc),
            )

            # ---- P0-4 (Frozen): audit CREATED with run_id + denoise ----
            created_note = {
                "run_id": run_id,
                "engine_version": engine_version,
                "report_hash": report_hash,
                "des_hash": des_hash,
            }
            # same run_id may attempt to publish twice (retries / duplicate calls)
            # --- Regime Shift + Regime Stats Audit (P0 read-only evidence; does not affect decisions) ---

            try:

                # history: prev 20 days from snapshot + current in-memory payload

                rows = self._conn.execute(

                    """SELECT trade_date, des_payload_json

                         FROM ur_decision_evidence_snapshot

                        WHERE report_kind=? AND trade_date < ?

                        ORDER BY trade_date DESC

                        LIMIT 20;""",

                    (report_kind, trade_date),

                ).fetchall()


                def _dig(d, *path):

                    c = d

                    for k in path:

                        if not isinstance(c, dict):

                            return None

                        c = c.get(k)

                    return c


                def _as_float(x):

                    try:

                        return None if x is None else float(x)

                    except Exception:

                        return None


                def _detect_stage(trend: str, drs_sig: str | None, adv_ratio, amount_ratio) -> str:

                    if not trend or drs_sig is None:

                        return "UNKNOWN"

                    t = (trend or "").strip().lower()

                    s = (drs_sig or "").strip().upper()

                    adv = adv_ratio if adv_ratio is not None else 0.0

                    amt = amount_ratio if amount_ratio is not None else 0.0

                    if t == "intact" and s != "RED" and adv >= 0.55 and amt >= 0.9:

                        return "S1"

                    if t == "intact" and s == "YELLOW":

                        return "S2"

                    if t == "mixed" and amt < 0.9:

                        return "S3"

                    if t == "broken" and s != "RED":

                        return "S4"

                    if t == "broken" and s == "RED":

                        return "S5"

                    return "UNKNOWN"


                def _stage_key(stage: str) -> str:

                    return (stage or "UNKNOWN").strip().upper()


                def _stage_from_payload(payload: dict):

                    drs = _dig(payload, "governance", "drs")

                    drs_u = drs.strip().upper() if isinstance(drs, str) else None

                    trend = _dig(payload, "structure", "trend_in_force", "state")

                    trend_s = trend if isinstance(trend, str) else None

                    amt = _as_float(_dig(payload, "structure", "amount", "evidence", "amount_ratio"))

                    adv = _as_float(_dig(payload, "structure", "crowding_concentration", "evidence", "adv_ratio"))

                    stage = _stage_key(_detect_stage(trend_s or "", drs_u, adv, amt))

                    ev = {"drs": drs_u, "trend": trend_s, "amount_ratio": amt, "adv_ratio": adv}

                    return stage, ev


                def _shift_meta(prev_stage: str, cur_stage: str, prev_ev: dict, cur_ev: dict):

                    order = {"UNKNOWN": 0, "S1": 1, "S2": 2, "S3": 3, "S4": 4, "S5": 5}

                    prev_rank = order.get(prev_stage, 0)

                    cur_rank = order.get(cur_stage, 0)

                    shift_type = "RISK_ESCALATION" if cur_rank > prev_rank else "RISK_EASING"

                    if cur_stage == "S5":

                        severity = "HIGH"

                    elif cur_stage == "S4":

                        severity = "MED"

                    elif shift_type == "RISK_ESCALATION":

                        severity = "MED"

                    else:

                        severity = "LOW"

                    reason = []

                    if prev_ev.get("drs") != cur_ev.get("drs"):

                        reason.append(f"drs:{prev_ev.get('drs')}→{cur_ev.get('drs')}")

                    if prev_ev.get("trend") != cur_ev.get("trend"):

                        reason.append(f"trend:{prev_ev.get('trend')}→{cur_ev.get('trend')}")

                    if cur_ev.get("amount_ratio") is not None:

                        reason.append(f"amount_ratio:{cur_ev.get('amount_ratio'):.3f}")

                    if cur_ev.get("adv_ratio") is not None:

                        reason.append(f"adv_ratio:{cur_ev.get('adv_ratio'):.3f}")

                    return {"shift_type": shift_type, "severity": severity, "reason": reason}


                hist = []

                for r in reversed(rows or []):  # oldest -> newest

                    td = r[0]

                    try:

                        p = json.loads(r[1]) if r[1] else {}

                    except Exception:

                        p = {}

                    st, ev = _stage_from_payload(p)

                    hist.append({"trade_date": td, "stage": st, "ev": ev})


                cur_stage, cur_ev = _stage_from_payload(des_payload)

                hist.append({"trade_date": trade_date, "stage": cur_stage, "ev": cur_ev})


                # REGIME_SHIFT (replace same-day row to keep only one version)

                if len(hist) >= 2:

                    prev = hist[-2]

                    cur = hist[-1]

                    if prev["stage"] != cur["stage"]:

                        meta2 = _shift_meta(prev["stage"], cur["stage"], prev.get("ev", {}), cur.get("ev", {}))

                        note_obj = {

                            "from": prev["stage"],

                            "to": cur["stage"],

                            "shift_type": meta2.get("shift_type"),

                            "severity": meta2.get("severity"),

                            "reason": meta2.get("reason"),

                            "prev": {"trade_date": prev.get("trade_date"), **(prev.get("ev") or {})},

                            "curr": {"trade_date": trade_date, **(cur.get("ev") or {})},

                        }

                        uow.record_audit(trade_date, report_kind, "REGIME_SHIFT", json.dumps(note_obj, ensure_ascii=False))


                # REGIME_STATS (replace same-day row to keep only one version)

                last10 = hist[-10:]

                last20 = hist[-20:]

                dist20 = {}

                for x in last20:

                    s = x.get("stage")

                    if s:

                        dist20[s] = dist20.get(s, 0) + 1


                consec_s5 = 0

                for x in reversed(hist):

                    if x.get("stage") == "S5":

                        consec_s5 += 1

                    else:

                        break


                stats_note = {

                    "last_10d": [{"trade_date": x.get("trade_date"), "stage": x.get("stage")} for x in last10],

                    "consecutive_s5_days": consec_s5,

                    "stage_distribution_20d": dist20,

                    "asof_trade_date": trade_date,

                }

                uow.record_audit(trade_date, report_kind, "REGIME_STATS", json.dumps(stats_note, ensure_ascii=False))

            except Exception:

                pass

            # --- end Regime Shift + Regime Stats Audit ---

            # keep audit readable: write CREATED once per run_id
            if run_id:
                cur = self._conn.execute(
                    """SELECT 1
                         FROM ur_persistence_audit
                        WHERE trade_date=? AND report_kind=? AND event='CREATED'
                          AND note LIKE ?
                        LIMIT 1;""",
                    (trade_date, report_kind, f'%"run_id":"{run_id}"%'),
                )
                existed = cur.fetchone() is not None
                if not existed:
                    uow.record_audit(trade_date, report_kind, "CREATED", json.dumps(created_note, ensure_ascii=False))
            else:
                uow.record_audit(trade_date, report_kind, "CREATED", json.dumps(created_note, ensure_ascii=False))
            uow.commit()
            return report_hash, des_hash

        except AlreadyPublishedError as e:
            try:
                uow.rollback()

                # --- REGIME_SHIFT + REGIME_STATS audit even if already published (backfill-safe) ---

                try:

                    # Always replace same-day rows (Scheme-1 in code)

                    self._conn.execute(

                        """DELETE FROM ur_persistence_audit

                             WHERE trade_date=? AND report_kind=? AND event IN ('REGIME_SHIFT','REGIME_STATS');""",

                        (trade_date, report_kind),

                    )

                    self._conn.commit()


                    cur = self._conn.execute(

                        """SELECT des_payload_json FROM ur_decision_evidence_snapshot

                             WHERE trade_date=? AND report_kind=? LIMIT 1;""",

                        (trade_date, report_kind),

                    ).fetchone()

                    if cur and cur[0]:

                        try:

                            cur_payload = json.loads(cur[0])

                        except Exception:

                            cur_payload = {}


                        # reuse the same stage extractor as main block (minimal duplication)

                        def _dig(d, *path):

                            c = d

                            for k in path:

                                if not isinstance(c, dict):

                                    return None

                                c = c.get(k)

                            return c


                        def _as_float(x):

                            try:

                                return None if x is None else float(x)

                            except Exception:

                                return None


                        def _detect_stage(trend: str, drs_sig: str | None, adv_ratio, amount_ratio) -> str:

                            if not trend or drs_sig is None:

                                return "UNKNOWN"

                            t = (trend or "").strip().lower()

                            s = (drs_sig or "").strip().upper()

                            adv = adv_ratio if adv_ratio is not None else 0.0

                            amt = amount_ratio if amount_ratio is not None else 0.0

                            if t == "intact" and s != "RED" and adv >= 0.55 and amt >= 0.9:

                                return "S1"

                            if t == "intact" and s == "YELLOW":

                                return "S2"

                            if t == "mixed" and amt < 0.9:

                                return "S3"

                            if t == "broken" and s != "RED":

                                return "S4"

                            if t == "broken" and s == "RED":

                                return "S5"

                            return "UNKNOWN"


                        def _stage_key(stage: str) -> str:

                            return (stage or "UNKNOWN").strip().upper()


                        def _stage_from_payload(payload: dict):

                            drs = _dig(payload, "governance", "drs")

                            drs_u = drs.strip().upper() if isinstance(drs, str) else None

                            trend = _dig(payload, "structure", "trend_in_force", "state")

                            trend_s = trend if isinstance(trend, str) else None

                            amt = _as_float(_dig(payload, "structure", "amount", "evidence", "amount_ratio"))

                            adv = _as_float(_dig(payload, "structure", "crowding_concentration", "evidence", "adv_ratio"))

                            stage = _stage_key(_detect_stage(trend_s or "", drs_u, adv, amt))

                            ev = {"drs": drs_u, "trend": trend_s, "amount_ratio": amt, "adv_ratio": adv}

                            return stage, ev


                        def _shift_meta(prev_stage: str, cur_stage: str, prev_ev: dict, cur_ev: dict):

                            order = {"UNKNOWN": 0, "S1": 1, "S2": 2, "S3": 3, "S4": 4, "S5": 5}

                            prev_rank = order.get(prev_stage, 0)

                            cur_rank = order.get(cur_stage, 0)

                            shift_type = "RISK_ESCALATION" if cur_rank > prev_rank else "RISK_EASING"

                            if cur_stage == "S5":

                                severity = "HIGH"

                            elif cur_stage == "S4":

                                severity = "MED"

                            elif shift_type == "RISK_ESCALATION":

                                severity = "MED"

                            else:

                                severity = "LOW"

                            reason = []

                            if prev_ev.get("drs") != cur_ev.get("drs"):

                                reason.append(f"drs:{prev_ev.get('drs')}→{cur_ev.get('drs')}")

                            if prev_ev.get("trend") != cur_ev.get("trend"):

                                reason.append(f"trend:{prev_ev.get('trend')}→{cur_ev.get('trend')}")

                            if cur_ev.get("amount_ratio") is not None:

                                reason.append(f"amount_ratio:{cur_ev.get('amount_ratio'):.3f}")

                            if cur_ev.get("adv_ratio") is not None:

                                reason.append(f"adv_ratio:{cur_ev.get('adv_ratio'):.3f}")

                            return {"shift_type": shift_type, "severity": severity, "reason": reason}


                        # build history: prev 20 + current

                        rows = self._conn.execute(

                            """SELECT trade_date, des_payload_json FROM ur_decision_evidence_snapshot

                                 WHERE report_kind=? AND trade_date < ?

                                 ORDER BY trade_date DESC LIMIT 20;""",

                            (report_kind, trade_date),

                        ).fetchall()


                        hist = []

                        for r in reversed(rows or []):

                            td = r[0]

                            try:

                                p = json.loads(r[1]) if r[1] else {}

                            except Exception:

                                p = {}

                            st, ev = _stage_from_payload(p)

                            hist.append({"trade_date": td, "stage": st, "ev": ev})


                        cur_stage, cur_ev = _stage_from_payload(cur_payload)

                        hist.append({"trade_date": trade_date, "stage": cur_stage, "ev": cur_ev})


                        # write REGIME_SHIFT if stage changed

                        if len(hist) >= 2:

                            prev = hist[-2]

                            curx = hist[-1]

                            if prev["stage"] != curx["stage"]:

                                meta2 = _shift_meta(prev["stage"], curx["stage"], prev.get("ev", {}), curx.get("ev", {}))

                                note_obj = {

                                    "from": prev["stage"],

                                    "to": curx["stage"],

                                    "shift_type": meta2.get("shift_type"),

                                    "severity": meta2.get("severity"),

                                    "reason": meta2.get("reason"),

                                    "prev": {"trade_date": prev.get("trade_date"), **(prev.get("ev") or {})},

                                    "curr": {"trade_date": trade_date, **(curx.get("ev") or {})},

                                }

                                self._conn.execute(

                                    """INSERT INTO ur_persistence_audit(trade_date, report_kind, event, note, created_at)

                                         VALUES (?, ?, 'REGIME_SHIFT', ?, CURRENT_TIMESTAMP);""",

                                    (trade_date, report_kind, json.dumps(note_obj, ensure_ascii=False)),

                                )

                                self._conn.commit()


                        # write REGIME_STATS always

                        last10 = hist[-10:]

                        last20 = hist[-20:]

                        dist20 = {}

                        for x in last20:

                            s = x.get("stage")

                            if s:

                                dist20[s] = dist20.get(s, 0) + 1

                        consec_s5 = 0

                        for x in reversed(hist):

                            if x.get("stage") == "S5":

                                consec_s5 += 1

                            else:

                                break

                        stats_note = {

                            "last_10d": [{"trade_date": x.get("trade_date"), "stage": x.get("stage")} for x in last10],

                            "consecutive_s5_days": consec_s5,

                            "stage_distribution_20d": dist20,

                            "asof_trade_date": trade_date,

                        }

                        self._conn.execute(

                            """INSERT INTO ur_persistence_audit(trade_date, report_kind, event, note, created_at)

                                 VALUES (?, ?, 'REGIME_STATS', ?, CURRENT_TIMESTAMP);""",

                            (trade_date, report_kind, json.dumps(stats_note, ensure_ascii=False)),

                        )

                        self._conn.commit()

                except Exception:

                    pass

                # --- end REGIME_SHIFT + REGIME_STATS backfill ---

                # best-effort audit outside the transaction
                self._conn.execute(
                    """INSERT INTO ur_persistence_audit
                       (trade_date, report_kind, event, note, created_at_utc)
                       VALUES (?, ?, 'FAILED', ?, ?);""",
                    (trade_date, report_kind, json.dumps({"run_id": run_id, "engine_version": engine_version, "error_type": type(e).__name__, "error": str(e)}, sort_keys=True, ensure_ascii=False, separators=(",", ":")), int(time.time())),
                )
                self._conn.commit()
            except Exception:
                pass
            raise

        except Exception as e:
            try:
                uow.rollback()
                self._conn.execute(
                    """INSERT INTO ur_persistence_audit
                       (trade_date, report_kind, event, note, created_at_utc)
                       VALUES (?, ?, 'FAILED', ?, ?);""",
                    (trade_date, report_kind, json.dumps({"run_id": run_id, "engine_version": engine_version, "error_type": type(e).__name__, "error": repr(e)}, sort_keys=True, ensure_ascii=False, separators=(",", ":")), int(time.time())),                    
                )
                self._conn.commit()
            except Exception:
                pass
            if isinstance(e, PersistenceError):
                raise
            raise PersistenceError("L2 publish failed", e)

