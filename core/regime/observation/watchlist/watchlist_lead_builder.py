# -*- coding: utf-8 -*-
"""WatchlistLeadBuilder (MVP)

Frozen Engineering rules:
- Build structured observation-only payload for report & persistence.
- Missing data is NOT an error: emit placeholder with warnings, never raise.
- MUST NOT change Gate / Execution / DRS decisions (read-only overlay only).

Output contract (append-only):
slots["watchlist_lead"] = {
  "schema": "WL_SLOT_MVP_2026Q1",
  "asof": "YYYY-MM-DD",
  "data_status": "OK|PARTIAL|MISSING|ERROR",
  "warnings": [...],
  "market_overlay": {"gate": "...", "execution": "...", "drs": "..."},
  "global_lead_t2": {"overall": "GREEN|YELLOW|ORANGE|RED", "reasons": [...], "note": "..."},
  "groups": { ... add group["lead_t2"] ... },
  "action_tips": {"lines": [...], "todo": [...]},
  "meta": {...},
}

"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


_LEVEL_ORDER = {"GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3, "MISSING": 4}
_VALID_LEVELS = set(_LEVEL_ORDER.keys())


def _norm_level(v: Any) -> str:
    if isinstance(v, str):
        u = v.strip().upper()
        if u in _VALID_LEVELS:
            return u
        # tolerate "LOW/HIGH/NEUTRAL" from some factor levels
        if u in {"LOW", "NEUTRAL", "HIGH"}:
            return {"LOW": "GREEN", "NEUTRAL": "YELLOW", "HIGH": "ORANGE"}[u]
    return "MISSING"


def _max_level(levels: List[str]) -> str:
    best = "GREEN"
    for lv in levels:
        nl = _norm_level(lv)
        if _LEVEL_ORDER.get(nl, 4) > _LEVEL_ORDER.get(best, 0):
            best = nl
    return best


def _get(d: Any, path: List[str], default=None):
    cur = d
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def _as_dict_factor(fr: Any) -> Dict[str, Any]:
    if isinstance(fr, dict):
        return fr
    return {
        "score": getattr(fr, "score", None),
        "level": getattr(fr, "level", None),
        "details": getattr(fr, "details", None),
        "name": getattr(fr, "name", None),
    }


def _to_pct(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        if abs(x) <= 1.0:
            return x * 100.0
        return x
    except Exception:
        return None


def _pct_le(v: Any, thr_pct: float) -> bool:
    p = _to_pct(v)
    if p is None:
        return False
    return p <= thr_pct


def _pct_ge(v: Any, thr_pct: float) -> bool:
    p = _to_pct(v)
    if p is None:
        return False
    return p >= thr_pct


@dataclass
class LeadCfg:
    breadth_new_low_ratio_pct: float = 6.0
    micro_weak_pct: float = -1.5
    micro_weak_k_orange: int = 2


class WatchlistLeadBuilder:
    """Build slots['watchlist_lead'] from factor details + overlays."""

    def build(self, slots: Dict[str, Any], cfg: Dict[str, Any], asof: Any) -> Dict[str, Any]:
        warnings: List[str] = []
        asof_s = str(asof)

        lead_cfg = self._parse_lead_cfg(cfg or {})

        # overlays (read-only)
        gate = _get(slots, ["governance", "gate", "final_gate"]) or _get(slots, ["governance", "gate", "gate_final"]) or _get(slots, ["gate_final"])
        if gate is None:
            gate = _get(slots, ["governance", "gate", "raw_gate"]) or _get(slots, ["gate_pre"])
        execution = _get(slots, ["governance", "execution", "band"]) or _get(slots, ["execution_summary", "band"])
        drs = _get(slots, ["governance", "drs", "signal"]) or _get(slots, ["drs", "signal"])

        overlay = {
            "gate": gate if isinstance(gate, str) else None,
            "execution": execution if isinstance(execution, str) else None,
            "drs": drs if isinstance(drs, str) else None,
        }

        factors = slots.get("factors") or {}
        frd = _as_dict_factor(factors.get("watchlist_lead"))
        details = frd.get("details") if isinstance(frd, dict) else None
        if not isinstance(details, dict):
            warnings.append("missing:watchlist_lead_factor_details")
            return self._default_payload(asof_s, overlay, warnings, data_status="MISSING")

        data_status = details.get("data_status")
        if not isinstance(data_status, str):
            data_status = "OK" if isinstance(details.get("groups"), dict) and details.get("groups") else "PARTIAL"
            warnings.append("infer:data_status")

        groups_in = details.get("groups")
        if not isinstance(groups_in, dict) or not groups_in:
            warnings.append("empty:groups")
            return self._default_payload(asof_s, overlay, warnings, data_status=str(data_status).upper() if data_status else "PARTIAL")

        # lead(T-2) env conditions
        exec_risk = isinstance(execution, str) and execution.upper() in {"D1", "D2"}

        breadth_fr = _as_dict_factor(factors.get("breadth"))
        new_low_ratio = _get(breadth_fr, ["details", "new_low_ratio"])
        breadth_bad = _pct_ge(new_low_ratio, lead_cfg.breadth_new_low_ratio_pct)

        sync_state = _get(slots, ["structure", "crowding_concentration", "state"]) or _get(slots, ["structure", "etf_index_sync", "state"])
        if not isinstance(sync_state, str):
            sync_state = _get(_as_dict_factor(factors.get("crowding_concentration")), ["details", "state"]) or _get(_as_dict_factor(factors.get("etf_spot_sync")), ["details", "interpretation", "participation"])
        sync_bad = isinstance(sync_state, str) and sync_state.lower() in {"sync_bad", "bad", "broken", "weak", "breakdown"}

        global_reasons: List[str] = []
        if exec_risk:
            global_reasons.append(f"exec={execution}")
        if breadth_bad:
            p = _to_pct(new_low_ratio)
            global_reasons.append(f"new_low_ratio>={p:.2f}%" if p is not None else f"new_low_ratio>={lead_cfg.breadth_new_low_ratio_pct}%")
        if sync_bad:
            global_reasons.append("sync=sync_bad")

        global_level = self._derive_env_level(exec_risk=exec_risk, breadth_bad=breadth_bad, sync_bad=sync_bad)
        global_lead_t2 = {"overall": global_level, "reasons": global_reasons, "note": "提前预警（未必已跌至阈值）"}

        groups_out: Dict[str, Any] = {}
        todo: List[Dict[str, Any]] = []

        for gkey, g in groups_in.items():
            if not isinstance(g, dict):
                continue
            g_level = _norm_level(g.get("level"))
            members = g.get("members") if isinstance(g.get("members"), list) else []
            micro_weak = 0
            shock_members: List[Dict[str, Any]] = []

            for m in members:
                if not isinstance(m, dict):
                    continue
                if _pct_le(m.get("pct_1d"), lead_cfg.micro_weak_pct):
                    micro_weak += 1
                trig = m.get("triggered")
                if isinstance(trig, dict) and (trig.get("orange_1d") or trig.get("red_1d") or trig.get("orange_2d") or trig.get("red_2d")):
                    shock_members.append(m)

            micro_level = "GREEN"
            micro_reason = None
            if micro_weak >= lead_cfg.micro_weak_k_orange:
                micro_level = "ORANGE"
                micro_reason = f"micro_weak>={micro_weak}"
            elif micro_weak >= 1:
                micro_level = "YELLOW"
                micro_reason = "micro_weak"

            lead_reasons = list(global_reasons)
            if micro_reason:
                lead_reasons.append(micro_reason)

            lead_level = _max_level([global_level, micro_level, g_level])

            if shock_members:
                for sm in shock_members:
                    todo.append({
                        "bucket": gkey,
                        "symbol": sm.get("symbol"),
                        "alias": sm.get("alias"),
                        "reason": "shock_confirmed",
                        "level": _norm_level(sm.get("level")),
                    })

            g2 = dict(g)
            g2["lead_t2"] = {"level": lead_level, "reasons": lead_reasons}
            groups_out[gkey] = g2

        overall_lv = _norm_level(_get(details, ["overall", "level"]))
        if overall_lv == "MISSING":
            overall_lv = _max_level([_norm_level(g.get("level")) for g in groups_out.values() if isinstance(g, dict)])

        overall = {"level": overall_lv, "score": _get(details, ["overall", "score"])}

        action_tips = self._build_action_tips(overlay=overlay, global_lead=global_lead_t2, todo=todo)

        payload = {
            "schema": "WL_SLOT_MVP_2026Q1",
            "asof": asof_s,
            "data_status": str(data_status).upper() if data_status else "PARTIAL",
            "warnings": warnings,
            "market_overlay": overlay,
            "global_lead_t2": global_lead_t2,
            "overall": overall,
            "groups": groups_out,
            "action_tips": action_tips,
            "meta": {
                "contribute_to_market_score": bool(_get(details, ["meta", "contribute_to_market_score"], False)),
                "source": "factor:watchlist_lead",
                "schema_factor": details.get("schema"),
            },
        }

        # append-only: supply_pressure (observation-only)
        sp = details.get("supply_pressure")
        if isinstance(sp, dict) and sp:
            payload["supply_pressure"] = sp

        return payload

    def _default_payload(self, asof: str, overlay: Dict[str, Any], warnings: List[str], data_status: str) -> Dict[str, Any]:
        return {
            "schema": "WL_SLOT_MVP_2026Q1",
            "asof": asof,
            "data_status": data_status,
            "warnings": warnings,
            "market_overlay": overlay,
            "global_lead_t2": {"overall": "MISSING", "reasons": [], "note": "提前预警（数据缺失）"},
            "overall": {"level": "MISSING", "score": None},
            "groups": {},
            "action_tips": {"lines": [], "todo": []},
            "meta": {"contribute_to_market_score": False, "source": "none"},
        }

    def _parse_lead_cfg(self, cfg: Dict[str, Any]) -> LeadCfg:
        d = cfg.get("lead_t2") if isinstance(cfg.get("lead_t2"), dict) else {}
        out = LeadCfg()
        v = d.get("breadth_new_low_ratio_pct")
        if isinstance(v, (int, float)) and v > 0:
            out.breadth_new_low_ratio_pct = float(v)
        v = d.get("micro_weak_pct")
        if isinstance(v, (int, float)) and v < 0:
            out.micro_weak_pct = float(v)
        v = d.get("micro_weak_k_orange")
        if isinstance(v, int) and v >= 1:
            out.micro_weak_k_orange = int(v)
        return out

    def _derive_env_level(self, exec_risk: bool, breadth_bad: bool, sync_bad: bool) -> str:
        if exec_risk and (breadth_bad or sync_bad):
            return "ORANGE"
        if exec_risk or breadth_bad or sync_bad:
            return "YELLOW"
        return "GREEN"

    def _build_action_tips(self, overlay: Dict[str, Any], global_lead: Dict[str, Any], todo: List[Dict[str, Any]]) -> Dict[str, Any]:
        gate = (overlay.get("gate") or "").upper()
        execution = (overlay.get("execution") or "").upper()

        lines: List[str] = []
        if gate in {"CAUTION", "D", "FREEZE"}:
            lines.append("基调：Gate=CAUTION → 禁止加仓/扩风险；仅允许减仓/降档/不动。")
        elif gate in {"NORMAL", "N"}:
            lines.append("基调：Gate=NORMAL → 不鼓励进攻性加仓；优先观察/分批执行，避免追价。")
        elif gate in {"ALLOW", "A"}:
            lines.append("基调：Gate=ALLOW → 允许按结构计划执行（仍需服从 Execution/DRS，不追价）。")
        else:
            lines.append("基调：Gate 未知 → 仅做观察提示，不构成行动依据。")

        if execution in {"D1", "D2"}:
            lines.append(f"执行：Execution={execution} → 执行摩擦偏高：优先“卖在反弹/分批降档”，避免追价与频繁换仓。")
        elif execution in {"A", "N"}:
            lines.append(f"执行：Execution={execution} → 执行环境相对顺畅，但仍需遵守 Gate/DRS 约束。")
        else:
            lines.append("执行：Execution 缺失 → 仅做占位，不构成行动依据。")

        glv = _norm_level(global_lead.get("overall"))
        reasons = global_lead.get("reasons") if isinstance(global_lead.get("reasons"), list) else []
        if glv in {"YELLOW", "ORANGE", "RED"}:
            r = ", ".join([str(x) for x in reasons if x]) if reasons else "（无）"
            lines.append(f"环境：GlobalLead(T-2)={glv}（{r}）→ 未来 1–2 天更易出现回撤/扩散：撤掉进攻挂单，降低暴露。")
        else:
            lines.append("环境：GlobalLead(T-2)=GREEN → 环境预警不显著，但仍以 Gate/Execution 为准。")

        if todo:
            names = []
            for x in todo:
                sym = x.get("symbol")
                alias = x.get("alias") or sym
                if sym:
                    names.append(f"{alias}({sym})")
            if names:
                lines.append("处理清单：" + "、".join(names) + " → 已发生回撤确认（Shock）。建议今天完成“降档1”（分批/不追价）。")

        lines.append("盘中窗口：反弹走强→在强势段完成减仓；冲高回落/翻绿→按规则执行，不拖延。")

        return {"lines": lines, "todo": todo}
