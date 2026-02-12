# -*- coding: utf-8 -*-
"""WatchlistLead（观察层）报告块。

展示 watchlist_lead（池/成员/触发），并计算 Lead(T-2) 先行预警（仅展示，不改 Gate/DRS）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock


def _to_str(x: Any) -> str:
    try:
        return "" if x is None else str(x)
    except Exception:
        return ""


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None



def _render_supply_pressure_section(wl: Any) -> List[str]:
    """Render supply pressure section (display-only).

    Contract (Frozen):
    - Read-only from slots['watchlist_lead'].
    - Missing supply data => show MISSING/UNKNOWN with counts, never raise.
    - skipped counts are ONLY for ETF and overseas watch symbols (not applicable for supply evaluation).
    - Keep output short: show bucket summary + top triggered members (max 3 per bucket).
    """
    lines: List[str] = []
    if not isinstance(wl, dict):
        return lines

    sp = wl.get("supply_pressure")
    if not isinstance(sp, dict) or not sp:
        return lines

    lines.append("")
    lines.append("## 供给压力（大宗 / 董监高增减持）")

    overall = sp.get("overall") if isinstance(sp.get("overall"), dict) else {}
    level = _to_str(overall.get("level") or overall.get("overall_supply_level") or "UNKNOWN").upper()
    triggered = int(overall.get("triggered") or 0)
    total = int(overall.get("total") or 0)
    missing = int(overall.get("missing") or 0)
    skipped = int(overall.get("skipped") or sp.get("skipped") or 0)

    # If there is nothing eligible (all ETF/overseas), show NA explicitly.
    if total == 0 and triggered == 0 and missing == 0 and skipped > 0:
        level = "NA"

    extra = ""
    if skipped > 0:
        extra = "，skipped=%d（仅 ETF / 海外观察标的不适用）" % skipped

    lines.append(f"- Overall: **{level}**（触发 {triggered}/{total}，缺失 {missing}{extra}）")

    # Determine max window for evidence summary
    windows = sp.get("windows") if isinstance(sp.get("windows"), list) else []
    wins: List[int] = []
    for w in windows:
        try:
            wi = int(w)
            if wi > 0:
                wins.append(wi)
        except Exception:
            pass
    maxw = str(max(wins) if wins else 20)

    def _rank(lv: str) -> int:
        m = {"NA": -2, "MISSING": -1, "GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}
        return int(m.get((lv or "").upper(), 0))

    def _is_ashare_stock(sym: str) -> bool:
        s = (sym or "").strip()
        # Accept both "300394" and "300394.SZ" styles
        if len(s) >= 6 and s[:6].isdigit():
            code = s[:6]
            return code[0] in ("0", "3", "6", "8")
        return False

    # Bucket summary + top triggered members
    groups = wl.get("groups")
    bucket_items: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(groups, dict):
        for gk in sorted(groups.keys()):
            g = groups.get(gk)
            if isinstance(g, dict):
                bucket_items.append((str(gk), g))
    elif isinstance(groups, list):
        for g in groups:
            if not isinstance(g, dict):
                continue
            gk = _to_str(g.get("key") or g.get("id") or g.get("name"))
            bucket_items.append((gk or "-", g))

    any_hit = False
    missing_syms: List[str] = []

    for gk, g in bucket_items:
        title = _to_str(g.get("title") or gk)

        # bucket supply level/stats: prefer group fields, fallback to panel.buckets
        bucket_lv = _to_str(g.get("bucket_supply_level") or _to_str((g.get("supply") or {}).get("level"))).upper() or "UNKNOWN"
        bstats = g.get("bucket_supply_stats") if isinstance(g.get("bucket_supply_stats"), dict) else (g.get("supply") if isinstance(g.get("supply"), dict) else {})
        if not isinstance(bstats, dict):
            bstats = {}

        if (not bucket_lv or bucket_lv == "UNKNOWN") and isinstance(sp.get("buckets"), dict) and isinstance(sp.get("buckets").get(gk), dict):
            b = sp["buckets"][gk]
            bucket_lv = _to_str(b.get("level") or "UNKNOWN").upper()
            for k in ("triggered", "missing", "total", "skipped"):
                if k not in bstats and k in b:
                    bstats[k] = b.get(k)

        b_trig = int(bstats.get("triggered") or 0)
        b_total = int(bstats.get("total") or 0)
        b_miss = int(bstats.get("missing") or 0)
        b_skip = int(bstats.get("skipped") or 0)

        # If this bucket has no eligible items but has skipped watch symbols, show NA.
        if b_total == 0 and b_trig == 0 and b_miss == 0 and b_skip > 0:
            bucket_lv = "NA"

        # Show bucket line only when it carries some information (eligible/missing/skip/trigger)
        if b_total > 0 or b_miss > 0 or b_trig > 0 or b_skip > 0:
            if bucket_lv == "NA":
                lines.append(f"- {title} ({gk}): **NA**（仅 ETF / 海外观察标的，不适用供给评估）")
            else:
                extra_b = f"，skipped={b_skip}" if b_skip > 0 else ""
                lines.append(f"- {title} ({gk}): **{bucket_lv}**（触发 {b_trig}/{b_total}，缺失 {b_miss}{extra_b}）")

        members = g.get("members") if isinstance(g.get("members"), list) else []
        hits: List[Tuple[str, float, Dict[str, Any], Dict[str, Any]]] = []
        for m in members:
            if not isinstance(m, dict):
                continue
            sup = m.get("supply") if isinstance(m.get("supply"), dict) else None
            if not isinstance(sup, dict):
                continue
            lv = _to_str(sup.get("supply_level") or sup.get("level") or "MISSING").upper()

            # collect missing eligible symbols for quick audit
            if lv == "MISSING":
                sym = _to_str(m.get("symbol"))
                if _is_ashare_stock(sym) and sym not in missing_syms and len(missing_syms) < 5:
                    missing_syms.append(sym)

            if lv not in ("YELLOW", "ORANGE", "RED"):
                continue
            w = _safe_float(m.get("weight")) or 0.0
            hits.append((lv, float(w), m, sup))

        hits.sort(key=lambda t: (_rank(t[0]), t[1]), reverse=True)

        # Show top 3 members per bucket (max), keep short
        for lv, w, m, sup in hits[:3]:
            any_hit = True
            symbol = _to_str(m.get("symbol"))
            alias = _to_str(m.get("alias") or m.get("key") or symbol)
            reasons = sup.get("reasons") if isinstance(sup.get("reasons"), list) else []
            reasons_s = "; ".join([_to_str(x) for x in reasons[:2] if _to_str(x)])

            ev = sup.get("evidence") if isinstance(sup.get("evidence"), dict) else {}
            ins = ev.get("insider") if isinstance(ev.get("insider"), dict) else {}
            bt = ev.get("block_trade") if isinstance(ev.get("block_trade"), dict) else {}
            ins_counts = ins.get("neg_counts") if isinstance(ins.get("neg_counts"), dict) else {}
            bt_counts = bt.get("counts") if isinstance(bt.get("counts"), dict) else {}
            bt_worst = bt.get("worst_discount_pct") if isinstance(bt.get("worst_discount_pct"), dict) else {}

            parts: List[str] = []
            try:
                v = ins_counts.get(maxw)
                if isinstance(v, (int, float)) and int(v) > 0:
                    parts.append(f"ins_neg_{maxw}d={int(v)}")
            except Exception:
                pass
            try:
                v = bt_counts.get(maxw)
                if isinstance(v, (int, float)) and int(v) > 0:
                    parts.append(f"dzjy_{maxw}d={int(v)}")
            except Exception:
                pass
            try:
                v = bt_worst.get(maxw)
                if isinstance(v, (int, float)):
                    parts.append(f"worst_disc_{maxw}d={float(v):+.1f}%")
            except Exception:
                pass

            ev_s = (" | " + ", ".join(parts)) if parts else ""
            if reasons_s:
                lines.append(f"  - {alias} {symbol} · **{lv}** — {reasons_s}{ev_s}")
            else:
                lines.append(f"  - {alias} {symbol} · **{lv}**{ev_s}")

    # Footer hints: help interpret GREEN quickly without extra debugging.
    if not any_hit and total > 0 and missing == 0:
        lines.append(f"- 结论：近 {maxw}D 未见供给事件触发（大宗/减持）。")
    if missing_syms:
        lines.append(f"- 缺失提示：{', '.join(missing_syms)}（可评估股票未匹配到供给数据；ETF/海外不适用不计入缺失）")

    return lines

def _pct_dec_to_str(x: Any) -> str:
    v = _safe_float(x)
    if v is None:
        return ""
    return f"{v * 100.0:+.2f}%"


def _level_rank(lv: str) -> int:
    m = {"GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}
    return int(m.get((lv or "").upper(), 0))


def _max_level(levels: List[str]) -> str:
    best = "GREEN"
    best_r = -1
    for lv in levels:
        r = _level_rank(lv)
        if r > best_r:
            best_r = r
            best = (lv or "GREEN").upper()
    return best



def _overlay_cap_level(gate: str, exe: str, drs: str) -> str:
    """Map governance overlay (Gate/Execution/DRS) to a cap level for WatchlistLead overall.

    This is display-governance only:
    - Prevents overall=GREEN when Gate/Execution indicate 'no add-risk' / 'high friction'.
    - Does NOT change Gate/DRS themselves.
    """
    levels: List[str] = []

    g = (gate or "").upper()
    if g in ("CAUTION",):
        levels.append("YELLOW")
    elif g in ("D",):
        levels.append("ORANGE")
    elif g in ("FREEZE",):
        levels.append("RED")

    e = (exe or "").upper()
    if e in ("D1",):
        levels.append("YELLOW")
    elif e in ("D2",):
        levels.append("ORANGE")
    elif e in ("D3", "D4", "D5", "D"):
        levels.append("RED")

    d = (drs or "").upper()
    if d in ("YELLOW", "ORANGE", "RED"):
        levels.append(d)

    return _max_level(levels) if levels else "GREEN"



def _get_factor_details(context: ReportContext, key: str) -> Optional[Dict[str, Any]]:
    """Get FactorResult.details from context.slots['factors'] with broad compatibility.

    In some pipelines, slots['factors'] may be:
    - dict: {factor_name: FactorResult|dict}
    - custom mapping-like container: supports .get() or __getitem__, but is not a dict
    """
    factors = context.slots.get("factors")
    if factors is None:
        return None

    fr = None
    # dict path
    if isinstance(factors, dict):
        fr = factors.get(key)
    else:
        # mapping-like path
        try:
            getter = getattr(factors, "get", None)
            if callable(getter):
                fr = getter(key)
            else:
                fr = factors[key]  # type: ignore[index]
        except Exception:
            fr = None

    if fr is None:
        return None

    # dict FactorResult-like
    if isinstance(fr, dict):
        d = fr.get("details") if isinstance(fr.get("details"), dict) else None
        if d is not None:
            return d
        # sometimes store details directly
        if any(k in fr for k in ("groups", "lead_panels", "tplus2_lead")):
            return fr
        return None

    # object FactorResult-like
    d2 = getattr(fr, "details", None)
    if isinstance(d2, dict):
        return d2
    return None


def _get_structure_fact(context: ReportContext, key: str) -> Optional[Dict[str, Any]]:
    st = context.slots.get("structure")
    if isinstance(st, dict):
        v = st.get(key)
        return v if isinstance(v, dict) else None
    return None


def _get_governance_overlay(context: ReportContext) -> Tuple[str, str, str]:
    """(gate, execution_band, drs_signal)"""
    gate = exe = drs = ""
    gov = context.slots.get("governance")
    if isinstance(gov, dict):
        g = gov.get("gate") if isinstance(gov.get("gate"), dict) else {}
        e = gov.get("execution") if isinstance(gov.get("execution"), dict) else {}
        d = gov.get("drs") if isinstance(gov.get("drs"), dict) else {}
        gate = _to_str(g.get("final_gate") or g.get("raw_gate") or "")
        exe = _to_str(e.get("band") or "")
        drs = _to_str(d.get("signal") or "")
    drs2 = context.slots.get("drs")
    if not drs and isinstance(drs2, dict):
        drs = _to_str(drs2.get("signal") or "")
    return (gate.upper() or "-", exe.upper() or "-", drs.upper() or "-")


# --- Lead(T-2) detector (display-only) ---
#
# Goal: provide "T-2 early warning" signals WITHOUT turning every bucket to ORANGE
# when the environment is globally weak.
#
# Strategy:
# 1) Compute a GlobalLead(T-2) from market-wide cues (exec/breadth/sync/north/global_lead).
# 2) Compute a BucketMicro signal from bucket members (micro weakness, not necessarily shock).
# 3) BucketLead(T-2) = combine(GlobalLead, BucketMicro) with "de-resonance" rules:
#    - If GlobalLead is ORANGE but bucket has no micro weakness => cap BucketLead to YELLOW
#    - If GlobalLead is YELLOW but bucket has no micro weakness => cap BucketLead to GREEN
#    - BucketMicro itself can elevate a bucket even if GlobalLead is GREEN.

def _global_lead_signals(context: ReportContext) -> Tuple[str, List[str]]:
    """Return (global_lead_level, reasons) for T-2. Display-only.

    IMPORTANT:
    - This is NOT a gate / decision module. It is a display-only early-warning overlay.
    - Reasons must show *threshold + actual value* to avoid confusion caused by rounding.
    """
    signals = 0
    reasons: List[str] = []

    # --- 1) Execution lead (governance) ---
    _gate, exe, _drs = _get_governance_overlay(context)
    if exe in ("D1", "D2", "D3", "D4", "D5", "D"):
        signals += 1
        reasons.append(f"exec={exe}")

    # --- 2) Breadth lead (structure or factor) ---
    br = _get_structure_fact(context, "breadth") or {}
    br_state = _to_str(br.get("state"))
    if br_state and br_state.lower() in ("breakdown", "damage", "weak", "broken"):
        signals += 1
        reasons.append(f"breadth={br_state}")
    else:
        brd = _get_factor_details(context, "breadth") or {}
        nlr = _safe_float(brd.get("new_low_ratio"))
        if nlr is not None and nlr >= 5.0:
            signals += 1
            reasons.append(f"new_low_ratio>=5.00% ({nlr:.2f}%)")

    # --- 3) Crowding lead (structure or factor) ---
    cc = _get_structure_fact(context, "crowding_concentration") or {}
    cc_state = _to_str(cc.get("state"))
    cc_state_l = (cc_state or "").lower()

    # Backward/forward compatible states:
    # - new: low/medium/high
    # - old: sync_good/sync_ok/sync_bad
    if cc_state_l in ("high", "crowding_high", "sync_bad", "bad", "broken", "diverge", "divergent"):
        signals += 1
        reasons.append(f"crowding={cc_state or 'high'}")
    else:
        cfd = _get_factor_details(context, "crowding_concentration") or {}
        sc = None  # avoid UnboundLocalError on fallback-only branches
        # Prefer liquidity_quality.details.top20_ratio for "Top20 成交集中度" semantics.
        # NOTE: crowding_concentration.details.top20_amount_ratio uses a DIFFERENT denominator (legacy proxy).
        lqd = _get_factor_details(context, "liquidity_quality") or {}
        TOP20_RATIO_TH = 0.12  # 12% of total market amount
        top20_ratio = _safe_float(lqd.get("top20_ratio"))
        if top20_ratio is not None and top20_ratio >= TOP20_RATIO_TH:
            signals += 1
            reasons.append(f"top20_ratio>={TOP20_RATIO_TH:.2f} ({top20_ratio:.3f})")
        else:
            # Legacy proxy (do NOT treat as Top20 ratio; denominator differs)
            top20_proxy = _safe_float(cfd.get("top20_amount_ratio"))
            if top20_proxy is not None and top20_proxy >= 0.72:
                signals += 1
                reasons.append(f"crowding_proxy_top20_amount_ratio>=0.72 ({top20_proxy:.3f})")
            else:
                # keep legacy score behavior (if present) as a weak fallback
                sc = _safe_float(cfd.get("score"))
            if sc is not None and sc <= 30.0:
                signals += 1
                reasons.append(f"crowding_score<=30 ({sc:.0f})")

    # --- 4) North proxy pressure lead (structure) ---
    npp = _get_structure_fact(context, "north_proxy_pressure") or {}
    ev = npp.get("evidence") if isinstance(npp.get("evidence"), dict) else {}
    pressure_level = _to_str(ev.get("pressure_level"))
    pressure_score = _safe_float(ev.get("pressure_score"))

    NORTH_TH = 40.0
    if pressure_score is not None and pressure_score >= NORTH_TH:
        signals += 1
        reasons.append(f"north_pressure_score>={NORTH_TH:.1f} ({pressure_score:.1f})")
    elif pressure_level and pressure_level.upper() not in ("LOW", "PRESSURE_LOW", "NEUTRAL"):
        signals += 1
        reasons.append(f"north_pressure={pressure_level.upper()}")

    # --- 5) Optional global lead (futures/US) ---
    gld = _get_factor_details(context, "global_lead") or {}
    gscore = _safe_float(gld.get("score"))
    if gscore is not None and gscore <= 45.0:
        signals += 1
        reasons.append(f"global_lead_score<=45 ({gscore:.0f})")

    # Map signals -> level
    # 0-1: GREEN, 2: YELLOW, >=3: ORANGE
    if signals >= 3:
        return "ORANGE", reasons
    if signals >= 2:
        return "YELLOW", reasons
    return "GREEN", reasons


def _bucket_micro_signal(group: Dict[str, Any]) -> Tuple[str, List[str]]:
    """Return (micro_level, reasons) based on bucket members only.

    This is designed to fire *before* a -3% shock, so thresholds are mild.
    """
    members = group.get("members") if isinstance(group.get("members"), list) else []
    reasons: List[str] = []

    micro_cnt = 0
    for m in members:
        if not isinstance(m, dict):
            continue
        p1 = _safe_float(m.get("pct_1d"))
        p2 = _safe_float(m.get("pct_2d"))

        # micro weakness thresholds (pre-shock)
        if (p1 is not None and p1 <= -0.015) or (p2 is not None and p2 <= -0.025):
            micro_cnt += 1

        # explicit yellow triggers also count as micro weakness evidence
        trig = m.get("triggered") if isinstance(m.get("triggered"), dict) else {}
        if trig.get("yellow_1d") or trig.get("yellow_2d"):
            micro_cnt += 1

    if micro_cnt >= 2:
        reasons.append("micro_weak>=2")
        return "ORANGE", reasons
    if micro_cnt == 1:
        reasons.append("micro_weak")
        return "YELLOW", reasons
    return "GREEN", reasons


def _combine_bucket_lead(global_level: str, micro_level: str) -> Tuple[str, List[str]]:
    """Combine global and micro to bucket lead with de-resonance caps."""
    gl = (global_level or "GREEN").upper()
    ml = (micro_level or "GREEN").upper()

    # Start from the worse of (global, micro)
    combined = _max_level([gl, ml])

    # De-resonance: global weak but bucket clean => cap
    if gl == "ORANGE" and ml == "GREEN":
        combined = "YELLOW"
    elif gl == "YELLOW" and ml == "GREEN":
        combined = "GREEN"

    reasons: List[str] = []
    if gl != "GREEN":
        reasons.append(f"global={gl}")
    if ml != "GREEN":
        reasons.append(f"micro={ml}")
    return combined, reasons



def _panel_level(panel: Dict[str, Any]) -> str:
    return _to_str(panel.get("level") or panel.get("overall") or "MISSING").upper() or "MISSING"


def _panel_status(panel: Dict[str, Any]) -> str:
    return _to_str(panel.get("data_status") or panel.get("status") or "MISSING").upper() or "MISSING"


def _fmt_kv_metrics(metrics: Any, limit: int = 5) -> str:
    if not isinstance(metrics, dict) or not metrics:
        return "-"
    parts: List[str] = []
    for k in list(metrics.keys())[:limit]:
        v = metrics.get(k)
        if v is None:
            continue
        # percentage formatting for *_pct or ratio-ish values in [0,1]
        if isinstance(v, (int, float)):
            if k.endswith("_pct") or k.endswith("_ratio_pct"):
                parts.append(f"{k}={v:.2f}%")
            else:
                # keep compact
                if abs(v) >= 1000:
                    parts.append(f"{k}={v:.0f}")
                elif abs(v) >= 10:
                    parts.append(f"{k}={v:.2f}")
                else:
                    parts.append(f"{k}={v:.4f}".rstrip("0").rstrip("."))
        else:
            parts.append(f"{k}={_to_str(v)}")
    return ", ".join(parts) if parts else "-"


def _default_panel_meaning(level: str, status: str) -> str:
    lv = _to_str(level).upper() or "MISSING"
    st = _to_str(status).upper() or "MISSING"
    if st in ("MISSING", "ERROR"):
        return "数据缺失/不可用（仅占位，不解读）。"
    if lv == "GREEN":
        return "结构健康/情绪平稳（可观察，不用急）。"
    if lv == "YELLOW":
        return "出现轻度偏热/分歧（追涨需更谨慎，控制节奏）。"
    if lv == "ORANGE":
        return "偏热或分歧大（追涨胜率下降，优先等确认/等回撤）。"
    if lv == "RED":
        return "过热或风险扩散（避免追涨与频繁换仓，优先防守/降摩擦）。"
    return "仅占位。"


def _render_tplus2_lead_section(wl: Dict[str, Any]) -> List[str]:
    t2 = wl.get("tplus2_lead") if isinstance(wl.get("tplus2_lead"), dict) else {}
    lv = _to_str(t2.get("overall") or t2.get("overall_level") or t2.get("level") or "MISSING").upper() or "MISSING"
    tag = _to_str(t2.get("overall_tag") or "")
    lv_disp = _to_str(t2.get("overall_display") or "")
    if not lv_disp:
        lv_disp = f"{lv}({tag})" if tag else lv

    # Compat: factor uses one_liner; older blocks may use one_line/summary
    one = _to_str(t2.get("one_liner") or t2.get("one_line") or t2.get("one_sentence") or t2.get("summary") or "")
    reasons = t2.get("reasons")
    if isinstance(reasons, str) and reasons:
        reasons_s = reasons
    elif isinstance(reasons, list):
        reasons_s = "; ".join([_to_str(x) for x in reasons if _to_str(x)])[:300]
    else:
        reasons_s = ""
    confirm = t2.get("confirm_signals") or t2.get("confirm") or t2.get("confirm_metrics")
    if isinstance(confirm, str) and confirm:
        confirm_s = confirm
    elif isinstance(confirm, dict):
        # flatten 3 keys max
        parts = []
        for k in list(confirm.keys())[:3]:
            parts.append(f"{k}={_to_str(confirm.get(k))}")
        confirm_s = " | ".join(parts)
    elif isinstance(confirm, list):
        confirm_s = " | ".join([_to_str(x) for x in confirm if _to_str(x)])[:200]
    else:
        confirm_s = ""

    lines: List[str] = []
    lines.append("")
    lines.append("【T+2 Lead（领先结构 · 观察层）】")
    lines.append(f"- overall={lv_disp}")
    if one:
        lines.append(f"- 一句话：{one}")
    if reasons_s:
        lines.append(f"- reasons: {reasons_s}")
    if confirm_s:
        lines.append(f"- confirm: {confirm_s}")

    # Heat Overlay note (observation-only)
    ov_all = wl.get('bucket_overlays') if isinstance(wl.get('bucket_overlays'), dict) else {}
    hov = ov_all.get('heat_overlay') if isinstance(ov_all, dict) else None
    if isinstance(hov, dict) and hov.get('applied') is True:
        tb = hov.get('target_buckets') if isinstance(hov.get('target_buckets'), list) else []
        tb_s = ','.join([_to_str(x) for x in tb if _to_str(x)])
        lines.append(f"- HeatOverlay: {hov.get('cap_code')} → 禁止 ADD-RISK/追涨；仅 HOLD/TRIM_ON_STRENGTH（反弹减仓） | targets={tb_s}")
        lines.append("  提醒：未来 1–2 天任意拥挤赛道可能被砸，不要等点名。")

    return lines


def _render_leading_panels_section(wl: Dict[str, Any]) -> List[str]:
    panels = wl.get("lead_panels") if isinstance(wl.get("lead_panels"), dict) else {}
    order = [
        ("market_sentiment", "A 情绪/参与度"),
        ("breadth_plus", "B 广度增强"),
        ("etf_flow", "C ETF 份额/净申购"),
        ("futures_basis", "D 期指基差/贴水"),
        ("options_risk", "E 期权风险定价"),
        ("liquidity_quality", "F 流动性质量"),
        ("margin_intensity", "G 两融强度"),
    ]
    lines: List[str] = []
    lines.append("")
    lines.append("【领先结构面板摘要（v1）】")

    for key, title in order:
        p = panels.get(key)
        if not isinstance(p, dict):
            # allow raw key name compatibility
            p = panels.get(f"{key}_raw") if isinstance(panels.get(f"{key}_raw"), dict) else None

        if not isinstance(p, dict):
            lines.append(f"- {title}: MISSING (MISSING) · missing:{key}_raw")
            lines.append("  - 含义：数据缺失/不可用（仅占位，不解读）。")
            continue

        lv = _panel_level(p)
        st = _panel_status(p)

        # metrics line
        km = p.get("key_metrics") if isinstance(p.get("key_metrics"), dict) else (p.get("metrics") if isinstance(p.get("metrics"), dict) else {})
        metric_limit = 6 if key == "market_sentiment" else 5
        metric_line = _fmt_kv_metrics(km, limit=metric_limit)

        # meaning: prefer factor-provided semantics
        meaning = _to_str(p.get("meaning") or p.get("interpretation") or p.get("summary") or "")
        if not meaning:
            meaning = _default_panel_meaning(lv, st)

        lines.append(f"- {title}: {lv} ({st}) · {metric_line}")

        # expand only if not GREEN OK
        expand = not (lv == "GREEN" and st == "OK")
        if expand:
            lines.append(f"  - 含义：{meaning}")
            # optional explain lines
            expl = p.get("explain") if isinstance(p.get("explain"), list) else None
            if isinstance(expl, list) and expl:
                for x in expl[:3]:
                    s = _to_str(x)
                    if s:
                        lines.append(f"  - 解读：{s}")

            reasons = p.get("reasons")
            if isinstance(reasons, list) and reasons:
                rs = "；".join([_to_str(x) for x in reasons if _to_str(x)])[:200]
                if rs:
                    lines.append(f"  - 触发点：{rs}")
            # warnings
            pw = p.get("warnings")
            if isinstance(pw, list):
                for w in pw[:3]:
                    sw = _to_str(w)
                    if sw:
                        lines.append(f"  - ⚠ {sw}")
    return lines


class WatchlistLeadBlock(ReportBlockRendererBase):
    block_alias = "watchlist.lead"
    title = "WatchlistLead（持仓/关注池 · 观察层）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        # slots preferred
        wl = context.slots.get("watchlist_lead")
        if not isinstance(wl, dict) or not wl:
            wl = _get_factor_details(context, "watchlist_lead") or {}

        if not isinstance(wl, dict) or not wl:
            warnings.append("empty:watchlist_lead")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload={
                    "meaning": [
                        "watchlist_lead 不可用（观察层不影响 Gate/DRS）。",
                        "请检查：watchlist_lead_raw 是否已接入；watchlist_lead factor 是否运行；以及 slots['factors'] 是否可被报告块读取。",
                    ],
                    "watchlist_lead": {},
                },
                warnings=warnings,
            )

        # overall / asof / data_status
        overall = wl.get("overall")
        if isinstance(overall, dict):
            o_lv = _to_str(overall.get("level") or overall.get("value") or "GREEN").upper() or "GREEN"
        else:
            o_lv = _to_str(overall or "GREEN").upper() or "GREEN"
        asof = _to_str(wl.get("asof") or context.trade_date)
        # P1-STABILITY-B: provide raw_asof alias to avoid NameError from legacy templates/config
        raw_asof = asof
        data_status = _to_str(wl.get("data_status") or "PARTIAL")

        base_w = wl.get("warnings")
        if isinstance(base_w, list):
            warnings.extend([_to_str(x) for x in base_w if _to_str(x)])
        # P1-STABILITY-B: sanitize legacy NameError noise (do not mask other errors)
        warnings = [w for w in warnings if "NameError:name 'raw_asof'" not in str(w)]

        gate, exe, drs = _get_governance_overlay(context)

        # overlay cap (display-only)
        overlay_cap = _overlay_cap_level(gate, exe, drs)
        disp_lv = _max_level([o_lv, overlay_cap])

        # groups list normalize
        groups_list: List[Dict[str, Any]] = []
        if isinstance(wl.get("groups"), list):
            groups_list = [g for g in wl.get("groups") if isinstance(g, dict)]
        elif isinstance(wl.get("groups"), dict):
            for gk in sorted(wl.get("groups").keys()):
                g = wl["groups"].get(gk)
                if isinstance(g, dict):
                    g2 = dict(g)
                    g2["key"] = gk
                    groups_list.append(g2)

        # GlobalLead(T-2)
        global_lv, global_reasons = _global_lead_signals(context)

        # Lead(T-2) per group
        lead_levels: List[str] = []
        lead_by_group: Dict[str, Any] = {}
        for g in groups_list:
            micro_lv, micro_reasons = _bucket_micro_signal(g)
            bucket_lv, _ = _combine_bucket_lead(global_lv, micro_lv)
            lead_levels.append(bucket_lv)

            gk = _to_str(g.get("key"))
            reasons2: List[str] = []
            if bucket_lv in ("YELLOW", "ORANGE", "RED") and global_lv != "GREEN":
                reasons2.extend(global_reasons)
            if micro_lv != "GREEN":
                reasons2.extend(micro_reasons)
            lead_by_group[gk] = {"level": bucket_lv, "micro": micro_lv, "reasons": reasons2[:8]}

        lead_overall = _max_level(lead_levels) if lead_levels else "GREEN"

        # -------- render lines --------
        lines: List[str] = []
        lines.append(f"As of: {asof} · overall={disp_lv} · data_status={data_status}")

        if overlay_cap != "GREEN":
            lines.append(f"Overlay cap applied: {overlay_cap} (Gate/Execution/DRS)")
            lines.append(f"Market overlay: Gate={gate} / Execution={exe} / DRS={drs}")

        if global_lv != "GREEN":
            rs = ", ".join(global_reasons[:6]) if global_reasons else "-"
            lines.append(f"GlobalLead(T-2): overall={global_lv} · reasons={rs}")

        # NEW: T+2 lead + leading panels (human semantics)
        lines.extend(_render_tplus2_lead_section(wl))
        lines.extend(_render_leading_panels_section(wl))


        # P1-STABILITY: if leading_panels has error/missing/partial, downgrade bucket display to MISSING
        lead_panels_err = False
        for w in warnings:
            if isinstance(w, str) and ("error:leading_panels" in w or "error:leading_panels" in w.lower()):
                lead_panels_err = True
                break
        # If lead_panels is empty or any key missing, treat as partial
        panels = wl.get("lead_panels") if isinstance(wl.get("lead_panels"), dict) else {}
        required_panel_keys = {"market_sentiment","breadth_plus","etf_flow","futures_basis","options_risk","liquidity_quality","margin_intensity"}
        if not panels:
            lead_panels_err = True
        else:
            present = set([k.replace("_raw","") for k in panels.keys() if isinstance(k, str)])
            if not (required_panel_keys & present):
                # nothing meaningful present
                lead_panels_err = True
        if str(data_status).upper() in ("PARTIAL","ERROR","MISSING","NA"):
            lead_panels_err = True
        
                # buckets
            for g in groups_list:
                gk = _to_str(g.get("key"))
                title = _to_str(g.get("title") or gk)
                shock_lv = _to_str(g.get("level_display") or g.get("level") or "GREEN") or "GREEN"
                if lead_panels_err:
                    shock_lv = "MISSING"
    
                counts = g.get("counts") if isinstance(g.get("counts"), dict) else {}
                red = int(_safe_float(counts.get("red")) or 0)
                orange = int(_safe_float(counts.get("orange")) or 0)
                yellow = int(_safe_float(counts.get("yellow")) or 0)
                total = int(_safe_float(counts.get("total")) or 0)
    
                lines.append("")
                lines.append(f"- {title} ({gk}): {shock_lv}  [red={red} orange={orange} yellow={yellow} total={total}]")
    
                aa = g.get("action_allowed") if isinstance(g.get("action_allowed"), list) else None
                af = g.get("action_forbidden") if isinstance(g.get("action_forbidden"), list) else None
                if isinstance(aa, list) and aa:
                    aa_s = ','.join([_to_str(x) for x in aa if _to_str(x)])
                    af_s = ','.join([_to_str(x) for x in af if _to_str(x)]) if isinstance(af, list) else ''
                    lines.append(f"  - ActionAllowed: {aa_s} | Forbidden: {af_s}")
    
                # show top triggered members when group shock elevated
                members = g.get("members") if isinstance(g.get("members"), list) else []
                risky = [m for m in members if isinstance(m, dict) and _to_str(m.get("level") or "GREEN").upper() in ("YELLOW", "ORANGE", "RED")]
                risky = sorted(
                    risky,
                    key=lambda m: (_level_rank(_to_str(m.get("level") or "GREEN").upper()), _safe_float(m.get("weight")) or 0.0),
                    reverse=True,
                )
                for m in risky[:3]:
                    alias = _to_str(m.get("alias") or m.get("key"))
                    symbol = _to_str(m.get("symbol"))
                    lv = _to_str(m.get("level") or "GREEN").upper()
                    p1 = _pct_dec_to_str(m.get("pct_1d"))
                    p2 = _pct_dec_to_str(m.get("pct_2d"))
                    lines.append(f"  - {alias} {symbol} · {lv} · 1D {p1} · 2D {p2}")
    
            # execution tips: use stored lines if present
            tips: List[str] = []
            tips.append("")
            tips.append("【执行提示（只读）】")
    
            stored_lines = None
            at = wl.get("action_tips") if isinstance(wl, dict) else None
            if isinstance(at, dict):
                stored_lines = at.get("lines")
    
            if isinstance(stored_lines, list) and stored_lines:
                for x in stored_lines:
                    s = str(x)
                    tips.append(s if s.lstrip().startswith("-") else f"- {s}")
            else:
                # fallback: keep short and consistent
                tips.append(f"- 基调：Gate={gate} → 禁止加仓/扩风险；仅允许减仓/降档/不动。")
                if exe in ("D1", "D2", "D"):
                    tips.append(f"- 执行：Execution={exe} → 执行摩擦偏高：优先“卖在反弹/分批降档”，避免追价与频繁换仓。")
                if global_lv in ("YELLOW", "ORANGE", "RED"):
                    tips.append(f"- 环境：GlobalLead(T-2)={global_lv}（{', '.join(global_reasons[:3])}）→ 未来 1–2 天更易轮动加速或出现回撤：撤掉进攻挂单，降低暴露。")
                if lead_overall in ("YELLOW", "ORANGE", "RED"):
                    tips.append("- 暂无回撤确认（Shock）命中成员：以预警为主，停止加仓，准备必要时降档。")
                tips.append("- 盘中窗口：反弹走强→在强势段完成减仓；冲高回落/翻绿→按规则执行，不拖延。")
    
            lines.extend(tips)
    
            # supply pressure panel (existing)
            lines.extend(_render_supply_pressure_section(wl))
    
            payload = {
                "meaning": lines,
                "watchlist_lead": wl,
                "lead_t2": {"global": {"level": global_lv, "reasons": global_reasons[:8]}, "overall": lead_overall, "by_group": lead_by_group},
            }
    
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

