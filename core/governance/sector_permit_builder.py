#-*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.utils.logger import get_logger

LOG = get_logger("SectorPermit")


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _get_in(d: Any, path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


@dataclass
class SectorPermitConfig:
    """
    SectorPermit（板块轮动参与允许层）——Frozen V1

    设计原则：
    - 不改 Gate（Gate 仍是制度主开关）
    - 仅在 Gate!=FREEZE 且非系统性破坏时，允许“结构性进攻”（基于 rotation_snapshot 的 Entry/Hold/Exit）
    - 输出用于：
      1) ActionHint 允许边界（在 CAUTION 下不再一刀切=0）
      2) 报告解释层（给出候选板块 + 退出优先级）
    """
    topn_candidates: int = 3


class SectorPermitBuilder:
    schema_version = "SECTOR_PERMIT_V1_2026Q1"

    def __init__(self, cfg: Optional[SectorPermitConfig] = None) -> None:
        self.cfg = cfg or SectorPermitConfig()

    def build(self, *, slots: Dict[str, Any], asof: str, gate: str) -> Dict[str, Any]:
        warnings: List[str] = []
        evidence: Dict[str, Any] = {"gate_final": str(gate).upper() if gate else None}

        rs = slots.get("rotation_snapshot")
        if not isinstance(rs, dict):
            return self._payload(
                asof=asof,
                permit="NO",
                mode="OFF",
                label="⛔ 无板块轮动快照（OFF）",
                candidates=[],
                exits={},
                evidence=evidence,
                warnings=["missing:rotation_snapshot"],
                constraints=["rotation_snapshot missing -> OFF"],
            )

        entry = rs.get("entry") if isinstance(rs.get("entry"), dict) else {}
        holding = rs.get("holding") if isinstance(rs.get("holding"), dict) else {}
        exitb = rs.get("exit") if isinstance(rs.get("exit"), dict) else {}

        entry_allowed = bool(entry.get("allowed")) if isinstance(entry, dict) else False
        entry_rows = entry.get("rows") if isinstance(entry.get("rows"), list) else []
        holding_rows = holding.get("rows") if isinstance(holding.get("rows"), list) else []
        exit_rows = exitb.get("rows") if isinstance(exitb.get("rows"), list) else []

        evidence["entry_allowed"] = bool(entry_allowed)
        evidence["entry_cnt"] = len(entry_rows) if isinstance(entry_rows, list) else 0
        evidence["holding_cnt"] = len(holding_rows) if isinstance(holding_rows, list) else 0
        evidence["exit_cnt"] = len(exit_rows) if isinstance(exit_rows, list) else 0

        # Systemic veto (strict)
        gate_u = str(gate).upper() if isinstance(gate, str) else ""
        trend_state = _get_in(slots, ["structure", "trend_in_force", "state"])
        exec_band = _get_in(slots, ["execution_summary", "band"])
        drs_sig = _get_in(slots, ["governance", "drs", "signal"]) or _get_in(slots, ["drs", "signal"])

        evidence["trend_state"] = trend_state
        evidence["execution_band"] = exec_band
        evidence["drs_signal"] = drs_sig

        if gate_u == "FREEZE":
            return self._payload(
                asof=asof,
                permit="NO",
                mode="OFF",
                label="⛔ Gate=FREEZE（全市场禁止进攻）",
                candidates=[],
                exits=self._pack_exits(holding_rows, exit_rows),
                evidence=evidence,
                warnings=warnings,
                constraints=["gate=FREEZE -> block_sector_attack"],
            )

        if isinstance(trend_state, str) and trend_state.strip().lower() == "broken":
            return self._payload(
                asof=asof,
                permit="NO",
                mode="OFF",
                label="⛔ Trend=broken（系统性破坏，禁止进攻）",
                candidates=[],
                exits=self._pack_exits(holding_rows, exit_rows),
                evidence=evidence,
                warnings=warnings,
                constraints=["trend_in_force=broken -> block_sector_attack"],
            )

        if isinstance(exec_band, str) and exec_band.upper() == "D3":
            return self._payload(
                asof=asof,
                permit="NO",
                mode="OFF",
                label="⛔ Execution=D3（执行摩擦极高，禁止进攻）",
                candidates=[],
                exits=self._pack_exits(holding_rows, exit_rows),
                evidence=evidence,
                warnings=warnings,
                constraints=["execution_band=D3 -> block_sector_attack"],
            )

        if not entry_allowed or not entry_rows:
            # still surface exits/holding
            return self._payload(
                asof=asof,
                permit="NO",
                mode="OFF",
                label="⛔ 无明确 Entry 信号（OFF）",
                candidates=[],
                exits=self._pack_exits(holding_rows, exit_rows),
                evidence=evidence,
                warnings=warnings,
                constraints=["no_entry_candidates -> OFF"],
            )

        # Mode selection
        mode = "ON"
        label = "🟢 板块轮动：可参与（分批，不追涨）"
        constraints: List[str] = [
            "仅限：EntryTop候选板块；建议分批（T+1/T+2）或回撤确认后参与；禁止追涨式加仓。",
        ]

        # Cap when execution friction high / drs red
        if isinstance(exec_band, str) and exec_band.upper() == "D2":
            mode = "LIGHT_ON"
            label = "🟡 板块轮动：轻参与（控制摩擦，不追涨）"
            constraints.append("Execution=D2：仅轻仓/底仓参与，优先控制执行摩擦。")
        if isinstance(drs_sig, str) and drs_sig.upper() == "RED":
            mode = "LIGHT_ON"
            label = "🟡 板块轮动：仅底仓/回撤确认（DRS=RED）"
            constraints.append("DRS=RED：仅底仓/回撤确认；若后续升级为 FREEZE/Trend broken 则自动 OFF。")

        candidates = self._pick_candidates(entry_rows)

        payload = self._payload(
            asof=asof,
            permit="YES",
            mode=mode,
            label=label,
            candidates=candidates,
            exits=self._pack_exits(holding_rows, exit_rows),
            evidence=evidence,
            warnings=warnings,
            constraints=constraints,
        )
        LOG.info("[SectorPermit] asof=%s gate=%s permit=%s mode=%s entry_cnt=%s",
                 asof, gate_u, payload.get("permit"), payload.get("mode"), len(candidates))
        return payload

    def _pick_candidates(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        # sort by ENTRY_RANK if possible
        def _rank(r: Dict[str, Any]) -> float:
            v = r.get("ENTRY_RANK")
            try:
                return float(v) if v is not None else 1e9
            except Exception:
                return 1e9

        for r in sorted([x for x in rows if isinstance(x, dict)], key=_rank)[: self.cfg.topn_candidates]:
            out.append({
                "SECTOR_TYPE": r.get("SECTOR_TYPE"),
                "SECTOR_ID": r.get("SECTOR_ID"),
                "SECTOR_NAME": r.get("SECTOR_NAME"),
                "ENTRY_RANK": r.get("ENTRY_RANK"),
                "WEIGHT_SUGGESTED": r.get("WEIGHT_SUGGESTED"),
                "SIGNAL_SCORE": r.get("SIGNAL_SCORE"),
                "ENERGY_PCT": r.get("ENERGY_PCT"),
                "ENERGY_TIER": r.get("ENERGY_TIER"),
                "STATE": r.get("STATE"),
                "TRANSITION": r.get("TRANSITION"),
            })
        return out

    def _pack_exits(self, holding_rows: List[Dict[str, Any]], exit_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        exit_allowed: List[Dict[str, Any]] = []
        exit_pending: List[Dict[str, Any]] = []

        for r in holding_rows or []:
            if not isinstance(r, dict):
                continue
            st = str(r.get("EXIT_EXEC_STATUS") or "").upper()
            if st == "EXIT_ALLOWED":
                exit_allowed.append({"SECTOR_NAME": r.get("SECTOR_NAME"), "EXIT_EXEC_STATUS": st, "NEXT_EXIT_ELIGIBLE_DATE": r.get("NEXT_EXIT_ELIGIBLE_DATE")})
            elif st == "EXIT_PENDING":
                exit_pending.append({"SECTOR_NAME": r.get("SECTOR_NAME"), "EXIT_EXEC_STATUS": st, "NEXT_EXIT_ELIGIBLE_DATE": r.get("NEXT_EXIT_ELIGIBLE_DATE")})

        # Exit snapshot rows (best-effort)
        for r in exit_rows or []:
            if not isinstance(r, dict):
                continue
            st = str(r.get("EXIT_EXEC_STATUS") or "").upper()
            if st == "EXIT_ALLOWED":
                exit_allowed.append({"SECTOR_NAME": r.get("SECTOR_NAME"), "EXIT_EXEC_STATUS": st, "EXEC_EXIT_DATE": r.get("EXEC_EXIT_DATE")})
            elif st == "EXIT_PENDING":
                exit_pending.append({"SECTOR_NAME": r.get("SECTOR_NAME"), "EXIT_EXEC_STATUS": st, "EXEC_EXIT_DATE": r.get("EXEC_EXIT_DATE")})

        # de-dup by name+status
        def _dedup(xs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            seen=set()
            out=[]
            for x in xs:
                k=(x.get("SECTOR_NAME"), x.get("EXIT_EXEC_STATUS"))
                if k in seen:
                    continue
                seen.add(k)
                out.append(x)
            return out

        return {
            "exit_allowed": _dedup(exit_allowed),
            "exit_pending": _dedup(exit_pending),
        }

    def _payload(
        self,
        *,
        asof: str,
        permit: str,
        mode: str,
        label: str,
        candidates: List[Dict[str, Any]],
        exits: Dict[str, Any],
        evidence: Dict[str, Any],
        warnings: List[str],
        constraints: List[str],
    ) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "asof": str(asof),
            "permit": str(permit).upper(),
            "mode": str(mode).upper(),
            "label": str(label),
            "candidates": candidates or [],
            "exits": exits or {},
            "constraints": constraints or [],
            "evidence": evidence or {},
            "warnings": warnings or [],
        }
