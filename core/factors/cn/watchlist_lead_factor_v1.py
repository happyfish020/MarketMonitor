#-*- coding: utf-8 -*-
"""UnifiedRisk V12
Factor: watchlist_lead (Observation-only · MVP)

目标（冻结/只读）：
- 监控“持仓/关注桶”的局部风险（-2%~-3% 级别冲击提示）
- 不参与 GateDecision / DRS / MarketScore（默认权重=0，不写入结构事实 keys）
- 输出结构化 details，供 WatchlistLeadBuilder / 报告块使用

输入（冻结）：
- 优先：snapshot["watchlist_lead_input_raw"]（BlockBuilder 组合块：lead_raw + supply_raw）
- 兼容：snapshot["watchlist_lead_raw"]（旧链路）

输出（append-only）：
- score: 0~100（代表观察桶的风险严重度；仅用于展示）
- level: LOW/NEUTRAL/HIGH（用于展示）
- details: {groups/items/...}（稳定 schema，便于 replay diff）
- details.supply_pressure: 供给压力面板（observation-only，不进 Gate/DRS）
"""

from __future__ import annotations

import logging

from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult
from core.utils.config_loader import load_watchlist_lead

LOG = logging.getLogger(__name__)



def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None or isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def _lvl_to_score(lv: str) -> int:
    m = {"GREEN": 0, "YELLOW": 33, "ORANGE": 66, "RED": 100, "MISSING": 0}
    return int(m.get((lv or "").upper(), 0))


def _max_level(levels: List[str]) -> str:
    order = {"GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3, "MISSING": -1}
    best = "GREEN"
    best_v = -10
    for lv in levels:
        u = (lv or "").upper()
        v = order.get(u, -1)
        if v > best_v:
            best, best_v = u, v
    return best


def _parse_date_any(v: Any) -> Optional[date]:
    """Best-effort parse date/datetime/str -> date."""
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # allow 'YYYY-MM-DD' or 'YYYY/MM/DD' or 'YYYYMMDD'
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"):
            try:
                return datetime.strptime(s[:10], fmt).date()
            except Exception:
                pass
    return None


def _days_ago(asof: date, d: Optional[date]) -> Optional[int]:
    if d is None:
        return None
    try:
        return int((asof - d).days)
    except Exception:
        return None


def _stringify_row(row: Dict[str, Any]) -> str:
    parts: List[str] = []
    for k, v in (row or {}).items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    return " ".join(parts)


def _pick_first(row: Dict[str, Any], candidates: List[str]) -> Any:
    for k in candidates:
        if k in row:
            return row.get(k)
    return None


def _parse_pct_points(v: Any) -> Optional[float]:
    """Parse percent-like fields into pct points (e.g. '-8.3%' -> -8.3)."""
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    if isinstance(v, str):
        s = v.strip().replace("％", "%")
        if not s:
            return None
        s = s.replace(",", "")
        try:
            if s.endswith("%"):
                return float(s[:-1])
            return float(s)
        except Exception:
            return None
    return None


@dataclass(frozen=True)
class _Thr:
    yellow_1d: float
    orange_1d: float
    red_1d: float
    yellow_2d: float
    orange_2d: float
    red_2d: float


class WatchlistLeadFactor(FactorBase):
    def __init__(self) -> None:
        # Frozen Contract: FactorBase.__init__ only accepts `name`.
        # Do NOT pass non-standard kwargs like layer/desc.
        super().__init__(name="watchlist_lead")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        cfg = load_watchlist_lead() or {}
        warnings: List[str] = []

        # ---------------------------------------------------------
        # Prefer multi-DS combined input block (Frozen)
        # ---------------------------------------------------------
        lead_raw = None
        supply_raw = None

        inp = self.pick(input_block, "watchlist_lead_input_raw", None)
        if isinstance(inp, dict) and inp:
            lead_raw = inp.get("lead_raw")
            supply_raw = inp.get("supply_raw")
            iw = inp.get("warnings")
            if isinstance(iw, list) and iw:
                warnings.extend([f"input_raw:{w}" for w in iw if isinstance(w, str)])
        else:
            # compat fallback (explicit warning; no silent behavior)
            lead_raw = self.pick(input_block, "watchlist_lead_raw", None)
            supply_raw = self.pick(input_block, "watchlist_supply_raw", None)
            warnings.append("compat:missing_watchlist_lead_input_raw")

        raw = lead_raw
        if not isinstance(raw, dict) or not raw:
            return self._neutral("DATA_NOT_CONNECTED:watchlist_lead_raw", trade_date=asof_str)

        groups_cfg = (cfg.get("groups") or {}) if isinstance(cfg, dict) else {}
        symbols_cfg = (cfg.get("symbols") or {}) if isinstance(cfg, dict) else {}
        thr_cfg = (cfg.get("thresholds") or {}) if isinstance(cfg, dict) else {}

        profiles = ((thr_cfg.get("profiles") or {}) if isinstance(thr_cfg, dict) else {})
        agg = ((thr_cfg.get("group_aggregation") or {}) if isinstance(thr_cfg, dict) else {})

        yk = int(agg.get("yellow_k", 1) or 1)
        ok = int(agg.get("orange_k", 2) or 2)
        red_any_2d = bool(agg.get("red_any_2d", True))

        items = raw.get("items") if isinstance(raw.get("items"), dict) else None
        if not isinstance(items, dict):
            return self._neutral("INVALID_RAW:watchlist_lead_raw.items", trade_date=asof_str)

        out_groups: Dict[str, Any] = {}
        overall_levels: List[str] = []

        # ---------------- groups loop (original behavior) ----------------
        for gk, g in (groups_cfg or {}).items():
            if not isinstance(g, dict):
                continue
            title = g.get("title")
            note = g.get("note")
            its = g.get("items") if isinstance(g.get("items"), list) else []

            members: List[Dict[str, Any]] = []
            member_levels: List[str] = []
            red_cnt = 0
            orange_cnt = 0
            yellow_cnt = 0
            any_red_2d = False

            for it in its:
                if not isinstance(it, dict) or not isinstance(it.get("key"), str):
                    continue
                ik = it["key"].strip()
                weight = _to_float(it.get("weight"))
                role = str(it.get("role") or "")

                sc = symbols_cfg.get(ik) if isinstance(symbols_cfg, dict) else None
                profile_name = None
                alias = None
                symbol = None
                if isinstance(sc, dict):
                    profile_name = sc.get("threshold_profile")
                    alias = sc.get("alias")
                    symbol = sc.get("symbol")

                rec = items.get(ik) if isinstance(items, dict) else None
                pct1 = _to_float(rec.get("pct_1d")) if isinstance(rec, dict) else None
                pct2 = _to_float(rec.get("pct_2d")) if isinstance(rec, dict) else None

                thr = self._pick_profile(profiles, profile_name)
                level, triggered = self._classify(pct1, pct2, thr)

                if level == "RED":
                    red_cnt += 1
                elif level == "ORANGE":
                    orange_cnt += 1
                elif level == "YELLOW":
                    yellow_cnt += 1

                if triggered.get("red_2d"):
                    any_red_2d = True

                member_levels.append(level)
                members.append({
                    "key": ik,
                    "symbol": symbol,
                    "alias": alias,
                    "role": role,
                    "weight": weight,
                    "pct_1d": pct1,
                    "pct_2d": pct2,
                    "level": level,
                    "triggered": triggered,
                    "present": bool(isinstance(rec, dict) and rec.get("present") is True),
                    "stale": bool(isinstance(rec, dict) and rec.get("stale") is True),
                    "last_date": rec.get("last_date") if isinstance(rec, dict) else None,
                })

            group_level = self._aggregate_group(
                member_levels,
                yk=yk,
                ok=ok,
                any_red_2d=any_red_2d,
                red_any_2d=red_any_2d,
            )
            overall_levels.append(group_level)

            out_groups[gk] = {
                "title": title,
                "note": note,
                "level": group_level,
                "counts": {"red": red_cnt, "orange": orange_cnt, "yellow": yellow_cnt, "total": len(members)},
                "members": sorted(members, key=lambda x: (-(x.get("weight") or 0.0), x.get("key") or "")),
            }

        overall = _max_level(overall_levels) if overall_levels else "GREEN"
        score = _lvl_to_score(overall)
        level = "LOW" if overall == "GREEN" else ("NEUTRAL" if overall == "YELLOW" else "HIGH")

        # ---------------------------------------------------------
        # Supply Pressure Panel (observation-only, append-only)
        # ---------------------------------------------------------
        asof_str = None
        if isinstance(inp, dict):
            asof_str = (inp.get("asof") or {}).get("trade_date") if isinstance(inp.get("asof"), dict) else None
        if not asof_str and isinstance(raw.get("meta"), dict):
            asof_str = raw.get("meta", {}).get("trade_date")
        if not asof_str:
            asof_str = self.pick(input_block, "trade_date", None)
        asof_date = _parse_date_any(asof_str) or date.today()

        supply_panel = None
        if isinstance(inp, dict) and inp:
            supply_panel = self._apply_supply_pressure(
                out_groups=out_groups,
                supply_raw=supply_raw,
                cfg=cfg,
                asof_date=asof_date,
                warnings=warnings,
            )


        # ------------------------------
        # Leading-Structure DataPack v1 (observation-only)
        # ------------------------------
        lead_panels: Dict[str, Any] = {}
        tplus2_lead: Dict[str, Any] = {}

        try:
            # Prefer input_raw (BlockBuilder joined) but allow direct snapshot fallback for robustness.
            market_sentiment_raw = None
            breadth_plus_raw = None
            if isinstance(inp, dict):
                market_sentiment_raw = inp.get("market_sentiment_raw")
                breadth_plus_raw = inp.get("breadth_plus_raw")
                # BlockBuilder uses {} as placeholder when a raw block is missing.
                # Treat empty dict as missing to align with frozen schema: MISSING + warnings.
                if isinstance(market_sentiment_raw, dict) and not market_sentiment_raw:
                    market_sentiment_raw = None
                if isinstance(breadth_plus_raw, dict) and not breadth_plus_raw:
                    breadth_plus_raw = None

            if market_sentiment_raw is None:
                market_sentiment_raw = self.pick(input_block, "market_sentiment_raw", None)
            if breadth_plus_raw is None:
                breadth_plus_raw = self.pick(input_block, "breadth_plus_raw", None)

            # Retrieve optional panel raw blocks from the combined input.  Treat empty dicts as missing.
            etf_flow_raw = None
            if isinstance(inp, dict):
                etf_flow_raw = inp.get("etf_flow_raw")
                if isinstance(etf_flow_raw, dict) and not etf_flow_raw:
                    etf_flow_raw = None
            if etf_flow_raw is None:
                etf_flow_raw = self.pick(input_block, "etf_flow_raw", None)

            futures_basis_raw = None
            if isinstance(inp, dict):
                futures_basis_raw = inp.get("futures_basis_raw")
                if isinstance(futures_basis_raw, dict) and not futures_basis_raw:
                    futures_basis_raw = None
            if futures_basis_raw is None:
                futures_basis_raw = self.pick(input_block, "futures_basis_raw", None)

            # Retrieve liquidity quality raw from combined input or fallback
            liquidity_quality_raw = None
            if isinstance(inp, dict):
                liquidity_quality_raw = inp.get("liquidity_quality_raw")
                if isinstance(liquidity_quality_raw, dict) and not liquidity_quality_raw:
                    liquidity_quality_raw = None
            if liquidity_quality_raw is None:
                liquidity_quality_raw = self.pick(input_block, "liquidity_quality_raw", None)

            # Retrieve options risk raw from combined input or fallback
            options_risk_raw = None
            if isinstance(inp, dict):
                options_risk_raw = inp.get("options_risk_raw")
                if isinstance(options_risk_raw, dict) and not options_risk_raw:
                    options_risk_raw = None
            if options_risk_raw is None:
                options_risk_raw = self.pick(input_block, "options_risk_raw", None)

            # Retrieve margin intensity raw (Panel G). Prefer margin_intensity_raw; fallback to margin_raw (as-of lag is common).
            margin_intensity_raw = None
            if isinstance(inp, dict):
                margin_intensity_raw = inp.get("margin_intensity_raw") or inp.get("margin_intensity")
                if isinstance(margin_intensity_raw, dict) and not margin_intensity_raw:
                    margin_intensity_raw = None
            if margin_intensity_raw is None:
                margin_intensity_raw = self.pick(input_block, "margin_intensity_raw", None)

            if margin_intensity_raw is None:
                # compat fallback: margin_raw (DS Margin) is the historical source for margin intensity panel
                margin_raw = None
                if isinstance(inp, dict):
                    margin_raw = inp.get("margin_raw")
                    if isinstance(margin_raw, dict) and not margin_raw:
                        margin_raw = None
                if margin_raw is None:
                    margin_raw = self.pick(input_block, "margin_raw", None)
                if margin_raw is not None:
                    warnings.append("compat:margin_raw_used_for:margin_intensity_raw")
                    margin_intensity_raw = margin_raw

            lead_panels = self._build_lead_panels(
                cfg=cfg,
                market_sentiment_raw=market_sentiment_raw,
                breadth_plus_raw=breadth_plus_raw,
                etf_flow_raw=etf_flow_raw,
                futures_basis_raw=futures_basis_raw,
                liquidity_quality_raw=liquidity_quality_raw,
                options_risk_raw=options_risk_raw,
                margin_intensity_raw=margin_intensity_raw,
                warnings=warnings,
            )
            tplus2_lead = self._aggregate_tplus2_lead(lead_panels=lead_panels)
        except Exception as e:
            # Never silent: keep report stable
            warnings.append(f"error:leading_panels:{type(e).__name__}:{e}")


        details = {
            "schema": str(cfg.get("schema_version") or "WL_MVP"),
            "asof": asof_str,
            "data_status": raw.get("data_status"),
            "warnings": sorted(set((raw.get("warnings") or []) + warnings)),
            "meta": {
                "asof": asof_str,
                "contribute_to_market_score": False,
                "group_aggregation": {"yellow_k": yk, "orange_k": ok, "red_any_2d": red_any_2d},
            },
            "overall": {"level": overall, "score": score},
            "groups": {k: out_groups[k] for k in sorted(out_groups.keys())},
        }
        details["meaning"] = f"WatchlistLead overall={overall} (observation-only)"

        if isinstance(supply_panel, dict) and supply_panel:
            details["supply_pressure"] = supply_panel

        if isinstance(lead_panels, dict) and lead_panels:
            details["lead_panels"] = lead_panels
        if isinstance(tplus2_lead, dict) and tplus2_lead:
            details["tplus2_lead"] = tplus2_lead

        return FactorResult(
            name=self.name,
            score=float(score),
            level=level,
            details=details,
        )

    # ------------------------- existing logic (unchanged) -------------------------
    def _pick_profile(self, profiles: Dict[str, Any], profile_name: Optional[str]) -> _Thr:
        d = profiles.get(profile_name) if (isinstance(profile_name, str) and profile_name in profiles) else None
        if not isinstance(d, dict):
            d = profiles.get("default") if isinstance(profiles.get("default"), dict) else {}
        return _Thr(
            yellow_1d=float(d.get("yellow_1d", -2.0)),
            orange_1d=float(d.get("orange_1d", -3.0)),
            red_1d=float(d.get("red_1d", -4.0)),
            yellow_2d=float(d.get("yellow_2d", -3.0)),
            orange_2d=float(d.get("orange_2d", -4.5)),
            red_2d=float(d.get("red_2d", -6.0)),
        )

    def _classify(self, pct_1d: Optional[float], pct_2d: Optional[float], thr: _Thr) -> Tuple[str, Dict[str, Any]]:
        triggered = {"yellow_1d": False, "orange_1d": False, "red_1d": False, "yellow_2d": False, "orange_2d": False, "red_2d": False}
        levels: List[str] = []

        if pct_1d is not None:
            if pct_1d <= thr.red_1d:
                triggered["red_1d"] = True
                levels.append("RED")
            elif pct_1d <= thr.orange_1d:
                triggered["orange_1d"] = True
                levels.append("ORANGE")
            elif pct_1d <= thr.yellow_1d:
                triggered["yellow_1d"] = True
                levels.append("YELLOW")

        if pct_2d is not None:
            if pct_2d <= thr.red_2d:
                triggered["red_2d"] = True
                levels.append("RED")
            elif pct_2d <= thr.orange_2d:
                triggered["orange_2d"] = True
                levels.append("ORANGE")
            elif pct_2d <= thr.yellow_2d:
                triggered["yellow_2d"] = True
                levels.append("YELLOW")

        return (_max_level(levels) if levels else "GREEN"), triggered

    def _aggregate_group(self, member_levels: List[str], yk: int, ok: int, any_red_2d: bool, red_any_2d: bool) -> str:
        reds = sum(1 for x in member_levels if x == "RED")
        oranges = sum(1 for x in member_levels if x == "ORANGE")
        yellows = sum(1 for x in member_levels if x == "YELLOW")

        if reds > 0 and (not red_any_2d or any_red_2d):
            return "RED"
        if oranges >= ok:
            return "ORANGE"
        if yellows >= yk:
            return "YELLOW"
        return "GREEN"

    def _neutral(self, reason: str, trade_date: Optional[str] = None) -> FactorResult:
        return FactorResult(
            name=self.name,
            score=0.0,
            level="LOW",
            details={
                "schema": "WL_MVP",
                "asof": trade_date,
                "data_status": "MISSING",
                "warnings": [reason],
                "meta": {"asof": trade_date, "contribute_to_market_score": False},
                "overall": {"level": "MISSING", "score": None},
                "groups": {},
            },
        )

    # ------------------------- supply panel (new, observation-only) -------------------------

    def _apply_supply_pressure(
        self,
        out_groups: Dict[str, Any],
        supply_raw: Any,
        cfg: Dict[str, Any],
        asof_date: date,
        warnings: List[str],
    ) -> Dict[str, Any]:
        """Build observation-only supply pressure panel.

        Contract:
        - Input is raw-only (no business logic in DS).
        - Missing data => MISSING + warnings (no hard fail).
        - Empty records => OK/GREEN.
        - Append-only output. Adds compat fields expected by report blocks:
          - panel["overall"] = {"level", "triggered", "total", "missing", "counts"}
          - group["supply"] = {"level", "triggered", "total", "missing"}
          - member["supply"]["level"] alias of supply_level
        """
        # ------------------------------
        # 1) Normalize config (support both config styles)
        # ------------------------------
        supply_cfg_raw: Dict[str, Any] = {}
        if isinstance(cfg, dict):
            v = cfg.get("supply_eval")
            if isinstance(v, dict) and v:
                supply_cfg_raw = v
            else:
                thr = cfg.get("thresholds")
                if isinstance(thr, dict):
                    sp = thr.get("supply_pressure")
                    if isinstance(sp, dict) and sp:
                        supply_cfg_raw = sp

        supply_cfg = self._normalize_supply_cfg(supply_cfg_raw)

        windows = supply_cfg.get("windows") if isinstance(supply_cfg.get("windows"), list) else [5, 10, 20]
        windows = [int(x) for x in windows if isinstance(x, (int, float)) and int(x) > 0] or [5, 10, 20]

        # supply_raw items: support {"items": {sym:...}} OR {"symbols": {sym:...}}
        items: Optional[Dict[str, Any]] = None
        if isinstance(supply_raw, dict):
            if isinstance(supply_raw.get("items"), dict):
                items = supply_raw.get("items")
            elif isinstance(supply_raw.get("symbols"), dict):
                items = supply_raw.get("symbols")

        panel: Dict[str, Any] = {
            "schema": "WL_SUPPLY_PANEL_MVP_2026Q1",
            "asof": asof_date.isoformat(),
            "data_status": "OK",
            "warnings": [],
            "windows": windows,
            # primary (v2) keys
            "overall_supply_level": "GREEN",
            "buckets": {},
            "stats": {"bucket_total": 0, "member_total": 0, "triggered": 0, "missing": 0, "skipped": 0},
            # compat / analytics
            "counts": {"GREEN": 0, "YELLOW": 0, "ORANGE": 0, "RED": 0, "MISSING": 0},
        }

        if not isinstance(out_groups, dict) or not out_groups:
            panel["data_status"] = "MISSING"
            panel["overall_supply_level"] = "MISSING"
            panel["warnings"].append("missing:groups")
            panel["overall"] = {
                "level": "MISSING",
                "triggered": 0,
                "total": 0,
                "missing": 0,
                "counts": panel["counts"],
            }
            return panel

        if not isinstance(items, dict):
            # Not connected / DS missing: treat as MISSING (not alert)
            panel["data_status"] = "MISSING"
            panel["overall_supply_level"] = "MISSING"
            panel["warnings"].append("missing:supply_raw_items")
            # still annotate per bucket/member as MISSING
            for gk, g in out_groups.items():
                if not isinstance(g, dict):
                    continue
                ms = g.get("members") if isinstance(g.get("members"), list) else []
                g["bucket_supply_level"] = "MISSING"
                g["bucket_supply_stats"] = {"triggered": 0, "missing": len(ms), "total": len(ms)}
                g["supply"] = {"level": "MISSING", "triggered": 0, "missing": len(ms), "total": len(ms)}
                for m in ms:
                    if not isinstance(m, dict):
                        continue
                    m_supply = {
                        "data_status": "MISSING",
                        "supply_level": "MISSING",
                        "level": "MISSING",
                        "reasons": [],
                        "evidence": {},
                        "warnings": ["missing:supply_raw_items"],
                    }
                    m["supply"] = m_supply
                    panel["counts"]["MISSING"] += 1
                    panel["stats"]["member_total"] += 1
                    panel["stats"]["missing"] += 1
            panel["stats"]["bucket_total"] = len([1 for g in out_groups.values() if isinstance(g, dict)])
            panel["overall"] = {
                "level": "MISSING",
                "triggered": 0,
                "total": int(panel["stats"]["member_total"]),
                "missing": int(panel["stats"]["missing"]),
                "counts": panel["counts"],
            }
            return panel

        # ------------------------------
        # 2) Evaluate per member and roll up
        # ------------------------------
        bucket_levels: List[str] = []
        def _extract_code6(s: str) -> Optional[str]:
            base = s.split('.')[0] if '.' in s else s
            digits = ''.join(ch for ch in base if ch.isdigit())
            return digits[:6] if len(digits) >= 6 else None

        def _is_supply_eligible(sym: str) -> bool:
            """Eligible symbols for supply evaluation.

            Supply DS currently supports A-share STOCK symbols only.
            Excludes ETFs (codes often start with 1/5) and overseas tickers.
            """
            if not isinstance(sym, str) or not sym:
                return False
            code6 = _extract_code6(sym)
            if not code6 or len(code6) != 6:
                return False
            # CN suffix is expected for A-share, but tolerate plain 6-digit
            if sym.isalpha():
                return False
            # Heuristic: A-share STOCK codes mainly start with 0/3/6/8.
            # ETFs/funds/bonds often start with 1/5 (e.g., 159xxx / 512xxx).
            if code6[0] in ("0", "3", "6", "8"):
                return True
            return False

        def _supply_lookup(it: Dict[str, Any], sym: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
            """Lookup supply item with tolerant symbol normalization."""
            if not isinstance(sym, str) or not sym:
                return None, None
            candidates: List[str] = [sym]
            code6 = _extract_code6(sym)
            if code6 and code6 != sym:
                candidates.append(code6)
                for suf in ('.SZ', '.SS', '.SH', '.BJ'):
                    candidates.append(code6 + suf)
                for pre in ('SZ', 'SH', 'SS', 'BJ'):
                    candidates.append(pre + code6)
            seen: set = set()
            for k in candidates:
                if not k or k in seen:
                    continue
                seen.add(k)
                v = it.get(k) if isinstance(it, dict) else None
                if isinstance(v, dict):
                    return k, v
            return (candidates[0] if candidates else None), None

        for gk, g in out_groups.items():
            if not isinstance(g, dict):
                continue
            panel["stats"]["bucket_total"] += 1

            ms = g.get("members") if isinstance(g.get("members"), list) else []
            trig = 0
            miss = 0
            members_out: List[Dict[str, Any]] = []
            skipped = 0
            eligible_total = 0

            for m in ms:
                if not isinstance(m, dict):
                    continue
                sym = m.get("symbol")
                # member_total counts only eligible CN stocks (exclude ETFs/overseas)
                if not isinstance(sym, str) or not sym:
                    eligible_total += 1
                    panel["stats"]["member_total"] += 1
                    miss += 1
                    m_supply = {
                        "data_status": "MISSING",
                        "supply_level": "MISSING",
                        "level": "MISSING",
                        "reasons": [],
                        "evidence": {},
                        "warnings": ["missing:member_symbol"],
                    }
                    m["supply"] = m_supply
                    members_out.append(m)
                    panel["counts"]["MISSING"] += 1
                    panel["stats"]["missing"] += 1
                    continue

                # Decide eligibility (skip ETFs/overseas); keep NA in member.supply but exclude from stats.
                if not _is_supply_eligible(sym):
                    skipped += 1
                    panel["stats"]["skipped"] += 1
                    # Log every skip for auditability (only ETF & overseas watch symbols are skipped).
                    code6 = _extract_code6(sym) or ""
                    if not code6 or sym.isalpha():
                        reason = "overseas_watch_symbol"
                    elif code6 and code6[0] in ("1", "5"):
                        reason = "etf_watch_symbol"
                    else:
                        reason = "non_cn_stock_symbol"
                    LOG.info("[WatchlistLead] supply eval skipped for symbol=%s reason=%s (only skip ETF/overseas watch symbols)", sym, reason)
                    m_supply = {
                        "data_status": "NA",
                        "supply_level": "NA",
                        "level": "NA",
                        "reasons": ["not_applicable"],
                        "evidence": {"symbol": sym, "reason": reason},
                        "warnings": ["na:supply:skip_etf_or_overseas_watch_symbol"],
                    }
                    m["supply"] = m_supply
                    members_out.append(m)
                    continue

                eligible_total += 1
                panel["stats"]["member_total"] += 1

                used_key, sitem = _supply_lookup(items, sym)
                if not isinstance(sitem, dict):
                    miss += 1
                    m_supply = {
                        "data_status": "MISSING",
                        "supply_level": "MISSING",
                        "level": "MISSING",
                        "reasons": [],
                        "evidence": {},
                        "warnings": ["missing:supply_item"],
                    }
                    if used_key:
                        m_supply["evidence"]["supply_key"] = used_key
                    m["supply"] = m_supply
                    members_out.append(m)
                    panel["counts"]["MISSING"] += 1
                    panel["stats"]["missing"] += 1
                    continue

                m_supply = self._eval_supply_symbol(
                    sym=sym,
                    sitem=sitem,
                    supply_cfg=supply_cfg,
                    windows=windows,
                    asof_date=asof_date,
                )
                # alias for report compatibility
                if isinstance(m_supply, dict):
                    m_supply["level"] = m_supply.get("level") or m_supply.get("supply_level") or "MISSING"
                    # evidence: which key was used for supply lookup
                    ev = m_supply.get("evidence")
                    if not isinstance(ev, dict):
                        ev = {}
                        m_supply["evidence"] = ev
                    if used_key:
                        ev["supply_key"] = used_key
                m["supply"] = m_supply
                members_out.append(m)

                lv = (m_supply.get("supply_level") if isinstance(m_supply, dict) else None) or "MISSING"
                lv = str(lv).upper()
                if lv in panel["counts"]:
                    panel["counts"][lv] += 1
                else:
                    panel["counts"]["MISSING"] += 1

                if lv in ("YELLOW", "ORANGE", "RED"):
                    trig += 1
                    panel["stats"]["triggered"] += 1
                if lv == "MISSING":
                    miss += 1
                    panel["stats"]["missing"] += 1

            # bucket level = max member levels excluding MISSING
            member_lv_list = []
            for m in ms:
                if isinstance(m, dict) and isinstance(m.get("supply"), dict):
                    ds = str(m["supply"].get("data_status") or "").upper()
                    lv = str(m["supply"].get("supply_level") or "MISSING").upper()
                    if ds == "NA" or lv == "NA":
                        continue
                    member_lv_list.append(lv)
            bucket_lv = _max_level([x for x in member_lv_list if x != "MISSING"]) if member_lv_list else "GREEN"
            if member_lv_list and all(x == "MISSING" for x in member_lv_list):
                bucket_lv = "MISSING"

            g["bucket_supply_level"] = bucket_lv
            g["bucket_supply_stats"] = {"triggered": trig, "missing": miss, "total": eligible_total, "skipped": skipped}
            # compat key expected by some report renderers
            g["supply"] = {"level": bucket_lv, "triggered": trig, "missing": miss, "total": eligible_total, "skipped": skipped}

            panel["buckets"][gk] = {"level": bucket_lv, "triggered": trig, "missing": miss, "total": eligible_total, "skipped": skipped}
            bucket_levels.append(bucket_lv)

        overall_supply = _max_level([lv for lv in bucket_levels if lv != "MISSING"]) if bucket_levels else "GREEN"
        if bucket_levels and all(lv == "MISSING" for lv in bucket_levels):
            overall_supply = "MISSING"

        panel["overall_supply_level"] = overall_supply
        # compat "overall" expected by report renderers
        panel["overall"] = {
            "level": overall_supply,
            "triggered": int(panel["stats"]["triggered"]),
            "total": int(panel["stats"]["member_total"]),
            "missing": int(panel["stats"]["missing"]),
            "skipped": int(panel["stats"].get("skipped", 0)),
            "counts": panel["counts"],            "skipped_note": "仅skip ETF与海外观察标的（watch symbols 不适用）",

        }

        # merge warnings
        base_w = panel.get("warnings") if isinstance(panel.get("warnings"), list) else []
        panel["warnings"] = sorted(set(base_w + (warnings or [])))
        return panel


    def _normalize_supply_cfg(self, raw_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize supply config from either legacy `supply_eval` or `thresholds.supply_pressure` style."""
        if not isinstance(raw_cfg, dict):
            return {"windows": [5, 10, 20], "insider_change": {}, "block_trade": {}}

        # If looks like legacy supply_eval (has insider_change/block_trade), keep but ensure defaults.
        if "insider_change" in raw_cfg or "block_trade" in raw_cfg:
            out = dict(raw_cfg)
            out.setdefault("windows", [5, 10, 20])
            out.setdefault("insider_change", {})
            out.setdefault("block_trade", {})
            # default keywords if missing
            ins = out.get("insider_change") if isinstance(out.get("insider_change"), dict) else {}
            if "negative_keywords" not in ins:
                ins["negative_keywords"] = ["减持", "卖出", "拟减持", "计划减持", "退出", "套现"]
            if "positive_keywords" not in ins:
                ins["positive_keywords"] = ["增持", "买入", "拟增持"]
            if "event_thresholds" not in ins:
                ins["event_thresholds"] = {"yellow": 1, "orange": 2, "red": 3}
            out["insider_change"] = ins
            bt = out.get("block_trade") if isinstance(out.get("block_trade"), dict) else {}
            bt.setdefault("event_thresholds", {})
            bt.setdefault("discount_pct_thresholds", {"yellow": None, "orange": None, "red": None})
            out["block_trade"] = bt
            return out

        # thresholds.supply_pressure style
        windows = raw_cfg.get("windows") if isinstance(raw_cfg.get("windows"), list) else [5, 10, 20]
        windows = [int(x) for x in windows if isinstance(x, (int, float)) and int(x) > 0] or [5, 10, 20]
        ins_raw = raw_cfg.get("insider") if isinstance(raw_cfg.get("insider"), dict) else {}
        bt_raw = raw_cfg.get("block_trade") if isinstance(raw_cfg.get("block_trade"), dict) else {}

        # Insider thresholds: choose values from max window when available; fallback to any.
        maxw = str(max(windows))
        def _get_any(prefix: str) -> Optional[int]:
            # try exact window keys first
            for w in [maxw, "20", "10", "5"]:
                k = f"{prefix}_{w}"
                if k in ins_raw and isinstance(ins_raw.get(k), (int, float)):
                    return int(ins_raw.get(k))
            return None

        y = _get_any("sell_cnt_yellow") or 1
        o = _get_any("sell_cnt_orange") or 2
        r = _get_any("sell_cnt_red") or 3

        insider_change = {
            "negative_keywords": ["减持", "卖出", "拟减持", "计划减持", "退出", "套现"],
            "positive_keywords": ["增持", "买入", "拟增持"],
            "event_thresholds": {"yellow": int(y), "orange": int(o), "red": int(r)},
        }

        # Block trade thresholds per window (yellow/orange/red)
        ev_thr: Dict[str, Any] = {}
        for w in windows:
            k = str(w)
            yk = bt_raw.get(f"cnt_yellow_{k}")
            ok = bt_raw.get(f"cnt_orange_{k}")
            rk = bt_raw.get(f"cnt_red_{k}")
            d: Dict[str, Any] = {}
            if isinstance(yk, (int, float)):
                d["yellow"] = int(yk)
            if isinstance(ok, (int, float)):
                d["orange"] = int(ok)
            if isinstance(rk, (int, float)):
                d["red"] = int(rk)
            if d:
                ev_thr[k] = d

        disc_thr = {
            "yellow": _to_float(bt_raw.get("worst_discount_yellow")),
            "orange": _to_float(bt_raw.get("worst_discount_orange")),
            "red": _to_float(bt_raw.get("worst_discount_red")),
        }

        return {
            "windows": windows,
            "insider_change": insider_change,
            "block_trade": {"event_thresholds": ev_thr, "discount_pct_thresholds": disc_thr},
        }

    def _eval_supply_symbol(
        self,
        sym: str,
        sitem: Dict[str, Any],
        supply_cfg: Dict[str, Any],
        windows: List[int],
        asof_date: date,
    ) -> Dict[str, Any]:
        """Evaluate one symbol's supply pressure from raw rows."""
        # local helper: rank supply level (avoid NameError; keep minimal scope)
        def _level_rank(level: str) -> int:
            order = {"MISSING": -1, "GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}
            u = (level or "").upper()
            return int(order.get(u, -1))

        # raw rows
        bt_rows = sitem.get("block_trade") if isinstance(sitem.get("block_trade"), list) else (sitem.get("block_trade_rows") if isinstance(sitem.get("block_trade_rows"), list) else [])
        ins_rows = sitem.get("insider_change") if isinstance(sitem.get("insider_change"), list) else (sitem.get("insider_rows") if isinstance(sitem.get("insider_rows"), list) else [])

        def in_window(row: Dict[str, Any], w: int) -> bool:
            d = _parse_date_any(row.get("trade_date") or row.get("date") or row.get("公告日期") or row.get("公告日"))
            if d is None:
                return False
            return (asof_date - d).days <= int(w)

        # 1) insider counts (negative keyword hits)
        ins_cfg = supply_cfg.get("insider_change") if isinstance(supply_cfg.get("insider_change"), dict) else {}
        neg_kw = ins_cfg.get("negative_keywords") if isinstance(ins_cfg.get("negative_keywords"), list) else ["减持", "卖出", "拟减持", "计划减持", "退出", "套现"]

        def is_negative(row: Dict[str, Any]) -> bool:
            # best-effort: look across common fields
            text_fields = []
            for k in ("change_type", "方向", "变动方向", "变动类别", "title", "摘要", "reason", "内容"):
                v = row.get(k)
                if isinstance(v, str) and v.strip():
                    text_fields.append(v.strip())
            joined = " ".join(text_fields)
            return any(kw in joined for kw in neg_kw) if joined else False

        ins_counts: Dict[str, int] = {}
        for w in windows:
            rows_w = [r for r in ins_rows if isinstance(r, dict) and in_window(r, w)]
            negs = [r for r in rows_w if is_negative(r)]
            ins_counts[str(w)] = len(negs)

        # 2) block trade counts and worst discount (%)
        bt_counts: Dict[str, int] = {}
        bt_worst_disc: Dict[str, Optional[float]] = {}
        for w in windows:
            rows_w = [r for r in bt_rows if isinstance(r, dict) and in_window(r, w)]
            bt_counts[str(w)] = len(rows_w)
            worst = None
            for r in rows_w:
                # try several possible column names
                disc = _to_float(r.get("discount_pct") or r.get("折溢价率") or r.get("折价率") or r.get("溢价率") or r.get("折溢价"))
                if disc is None:
                    continue
                worst = disc if worst is None else min(worst, disc)
            bt_worst_disc[str(w)] = worst

        w_key = str(max(windows)) if windows else "20"

        supply_level = "GREEN"
        reasons: List[str] = []
        evidence: Dict[str, Any] = {
            "insider": {"neg_counts": ins_counts, "rows": len(ins_rows)},
            "block_trade": {"counts": bt_counts, "worst_discount_pct": bt_worst_disc, "rows": len(bt_rows)},
        }

        # insider thresholds (use max window)
        ins_thr = ins_cfg.get("event_thresholds") if isinstance(ins_cfg.get("event_thresholds"), dict) else {"yellow": 1, "orange": 2, "red": 3}
        y_thr = int((ins_thr.get("yellow") or 1))
        o_thr = int((ins_thr.get("orange") or 2))
        r_thr = int((ins_thr.get("red") or 3))
        neg_cnt = int(ins_counts.get(w_key, 0) or 0)
        if neg_cnt >= r_thr:
            supply_level = _max_level([supply_level, "RED"])
            reasons.append(f"insider_neg_events>={r_thr}({w_key}d)")
        elif neg_cnt >= o_thr:
            supply_level = _max_level([supply_level, "ORANGE"])
            reasons.append(f"insider_neg_events>={o_thr}({w_key}d)")
        elif neg_cnt >= y_thr:
            supply_level = _max_level([supply_level, "YELLOW"])
            reasons.append(f"insider_neg_events>={y_thr}({w_key}d)")

        # block trade count thresholds (per window may have yellow/orange/red)
        bt_cfg = supply_cfg.get("block_trade") if isinstance(supply_cfg.get("block_trade"), dict) else {}
        ev_thr = bt_cfg.get("event_thresholds") if isinstance(bt_cfg.get("event_thresholds"), dict) else {}
        best_lv = "GREEN"
        best_reason = None
        for w in windows:
            k = str(w)
            cnt = int(bt_counts.get(k, 0) or 0)
            th = ev_thr.get(k)
            lv = "GREEN"
            if isinstance(th, dict):
                ry = th.get("yellow")
                ro = th.get("orange")
                rr = th.get("red")
                if isinstance(rr, (int, float)) and cnt >= int(rr):
                    lv = "RED"
                    reason = f"block_trade_events>={int(rr)}({k}d)"
                elif isinstance(ro, (int, float)) and cnt >= int(ro):
                    lv = "ORANGE"
                    reason = f"block_trade_events>={int(ro)}({k}d)"
                elif isinstance(ry, (int, float)) and cnt >= int(ry):
                    lv = "YELLOW"
                    reason = f"block_trade_events>={int(ry)}({k}d)"
                else:
                    reason = None
            elif isinstance(th, (int, float)) and cnt >= int(th):
                lv = "YELLOW"
                reason = f"block_trade_events>={int(th)}({k}d)"
            else:
                reason = None

            if _level_rank(lv) > _level_rank(best_lv):
                best_lv = lv
                best_reason = reason

        if best_lv != "GREEN":
            supply_level = _max_level([supply_level, best_lv])
            if best_reason:
                reasons.append(best_reason)

        # block trade discount thresholds (use worst over max window)
        disc_thr = bt_cfg.get("discount_pct_thresholds") if isinstance(bt_cfg.get("discount_pct_thresholds"), dict) else {}
        worst = bt_worst_disc.get(w_key)
        y_disc = _to_float(disc_thr.get("yellow"))
        o_disc = _to_float(disc_thr.get("orange"))
        r_disc = _to_float(disc_thr.get("red"))
        if worst is not None and r_disc is not None and worst <= r_disc:
            supply_level = _max_level([supply_level, "RED"])
            reasons.append(f"block_trade_worst_discount<={r_disc}%({w_key}d)")
        elif worst is not None and o_disc is not None and worst <= o_disc:
            supply_level = _max_level([supply_level, "ORANGE"])
            reasons.append(f"block_trade_worst_discount<={o_disc}%({w_key}d)")
        elif worst is not None and y_disc is not None and worst <= y_disc:
            supply_level = _max_level([supply_level, "YELLOW"])
            reasons.append(f"block_trade_worst_discount<={y_disc}%({w_key}d)")

        reasons = reasons[:3]

        data_status = "OK"
        if len(bt_rows) == 0 and len(ins_rows) == 0:
            data_status = "OK"  # empty is OK/GREEN
        if (len(bt_rows) == 0 and len(ins_rows) > 0) or (len(bt_rows) > 0 and len(ins_rows) == 0):
            data_status = "PARTIAL"

        return {
            "data_status": data_status,
            "supply_level": supply_level,
            "level": supply_level,
            "reasons": reasons,
            "evidence": evidence,
            "warnings": [],
        }
# ==============================
    # Leading-Structure DataPack v1
    # ==============================

    @staticmethod
    def _level_rank(level: str) -> int:
        lv = (level or "MISSING").upper()
        order = {"MISSING": 0, "GREEN": 1, "YELLOW": 2, "ORANGE": 3, "RED": 4, "ERROR": 4}
        return order.get(lv, 0)

    @staticmethod
    def _coerce_raw_block(raw: Any) -> Tuple[str, List[str], Optional[str], Optional[str], Dict[str, Any]]:
        """Return (data_status, warnings, error_type, error_message, evidence_dict)."""
        if not isinstance(raw, dict):
            return ("MISSING", ["missing:raw_block"], None, None, {})
        data_status = str(raw.get("data_status") or "OK").upper()
        rw = raw.get("warnings")
        warnings = [str(x) for x in rw] if isinstance(rw, list) else []
        error_type = raw.get("error_type")
        error_message = raw.get("error_message")
        ev = raw.get("evidence")
        if isinstance(ev, dict):
            evidence = ev
        else:
            # backward-compat: raw itself contains metrics
            evidence = {k: v for k, v in raw.items() if k not in ("schema_version", "asof", "data_status", "warnings", "error_type", "error_message")}
        return (data_status, warnings, str(error_type) if error_type else None, str(error_message) if error_message else None, evidence)

    @staticmethod
    def _as_pct(x: Any) -> Optional[float]:
        v = _to_float(x)
        if v is None:
            return None
        # if it's a ratio (0~1.5), convert to pct points for display.
        return v * 100.0 if v <= 1.5 else v

    def _panel_missing(self, panel_key: str, warnings: List[str]) -> Dict[str, Any]:
        warnings.append(f"input_raw:missing:{panel_key}_raw")
        return {
            "level": "MISSING",
            "data_status": "MISSING",
            "warnings": [f"missing:{panel_key}_raw"],
            "key_metrics": {},
            "reasons": [],
            "meaning": "数据缺失/不可用（仅占位，不解读）。",
            "evidence": {},
        }

    def _build_lead_panels(
        self,
        cfg: Dict[str, Any],
        market_sentiment_raw: Any,
        breadth_plus_raw: Any,
        etf_flow_raw: Any,
        futures_basis_raw: Any,
        liquidity_quality_raw: Any,
        options_risk_raw: Any,
        margin_intensity_raw: Any,
        warnings: List[str],
    ) -> Dict[str, Any]:
        panels_cfg = self.pick(cfg, "leading_panels", {}) if isinstance(cfg, dict) else {}

        lead_panels: Dict[str, Any] = {}

        # A: market sentiment / participation
        if market_sentiment_raw is None:
            lead_panels["market_sentiment"] = self._panel_missing("market_sentiment", warnings)
        else:
            lead_panels["market_sentiment"] = self._panel_market_sentiment(
                raw=market_sentiment_raw,
                pcfg=self.pick(panels_cfg, "market_sentiment", {}),
                warnings=warnings,
            )

        # B: breadth plus
        if breadth_plus_raw is None:
            lead_panels["breadth_plus"] = self._panel_missing("breadth_plus", warnings)
        else:
            lead_panels["breadth_plus"] = self._panel_breadth_plus(
                raw=breadth_plus_raw,
                pcfg=self.pick(panels_cfg, "breadth_plus", {}),
                warnings=warnings,
            )

        # C: ETF flow
        # Use raw block if available, else mark missing.  Delegates parsing/level determination to _panel_etf_flow.
        if etf_flow_raw is None:
            lead_panels["etf_flow"] = self._panel_missing("etf_flow", warnings)
        else:
            lead_panels["etf_flow"] = self._panel_etf_flow(
                raw=etf_flow_raw,
                pcfg=self.pick(panels_cfg, "etf_flow", {}),
                warnings=warnings,
            )

        # D: Futures basis (期指基差)
        if futures_basis_raw is None:
            lead_panels["futures_basis"] = self._panel_missing("futures_basis", warnings)
        else:
            lead_panels["futures_basis"] = self._panel_futures_basis(
                raw=futures_basis_raw,
                pcfg=self.pick(panels_cfg, "futures_basis", {}),
                warnings=warnings,
            )

        # E: Liquidity quality (F block) - after futures basis
        if liquidity_quality_raw is None:
            lead_panels["liquidity_quality"] = self._panel_missing("liquidity_quality", warnings)
        else:
            lead_panels["liquidity_quality"] = self._panel_liquidity_quality(
                raw=liquidity_quality_raw,
                pcfg=self.pick(panels_cfg, "liquidity_quality", {}),
                warnings=warnings,
            )

        # E: Options risk (期权风险) - new panel
        if options_risk_raw is None:
            lead_panels["options_risk"] = self._panel_missing("options_risk", warnings)
        else:
            lead_panels["options_risk"] = self._panel_options_risk(
                raw=options_risk_raw,
                pcfg=self.pick(panels_cfg, "options_risk", {}),
                warnings=warnings,
            )
        # G: Margin intensity (两融强度)
        if margin_intensity_raw is None:
            lead_panels["margin_intensity"] = self._panel_missing("margin_intensity", warnings)
        else:
            lead_panels["margin_intensity"] = self._panel_margin_intensity(
                raw=margin_intensity_raw,
                pcfg=self.pick(panels_cfg, "margin_intensity", {}),
                warnings=warnings,
            )

        return lead_panels


    def _panel_market_sentiment(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        """Panel A: Market sentiment / participation (EOD, observation-only).

        Notes (Frozen):
        - Use snapshot raw block: market_sentiment_raw
        - Missing data must NOT break factor; return stable schema with warnings.
        - Ratios are treated as *percent* (0~100) unless explicitly documented otherwise.
        """
        data_status, rwarns, err_type, err_msg, ev = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)

        # hard missing / error
        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": ev,
            }

        evidence = ev if isinstance(ev, dict) else {}
        # evidence keys (best-effort)
        total = evidence.get("total_stocks") or evidence.get("total") or evidence.get("total_count")
        adv = evidence.get("adv") or evidence.get("up") or evidence.get("adv_count")
        dec = evidence.get("dec") or evidence.get("down") or evidence.get("down_count")
        flat = evidence.get("flat") or evidence.get("flat_count")

        limit_up = evidence.get("limit_up")
        limit_down = evidence.get("limit_down")
        max_consecutive = evidence.get("max_consecutive_limit_up") or evidence.get("max_consecutive")
        broken_std = evidence.get("broken_limit_rate_std") or evidence.get("broken_limit_std")

        # normalize numbers
        def _f(x: Any) -> Optional[float]:
            try:
                if x is None:
                    return None
                return float(x)
            except Exception:
                return None

        total_f = _f(total)
        adv_f = _f(adv)
        limit_up_f = _f(limit_up)
        limit_down_f = _f(limit_down)
        max_consecutive_f = _f(max_consecutive)
        broken_std_f = _f(broken_std)

        # ratios in percent (0~100)
        adv_ratio = evidence.get("adv_ratio")  # may already be percent
        adv_ratio_f = _f(adv_ratio)
        if adv_ratio_f is None and total_f and adv_f is not None:
            adv_ratio_f = adv_f * 100.0 / total_f

        lu_ratio = None
        ld_ratio = None
        if total_f and total_f > 0:
            if limit_up_f is not None:
                lu_ratio = limit_up_f * 100.0 / total_f
            if limit_down_f is not None:
                ld_ratio = limit_down_f * 100.0 / total_f

        # broken std display (std itself is 0~1; show as pct)
        broken_std_pct = None
        if broken_std_f is not None:
            broken_std_pct = broken_std_f * 100.0 if broken_std_f <= 1.5 else broken_std_f

        # thresholds (panel-level; safe floor to avoid unit mistakes)
        th = self.pick(pcfg, "thresholds", {}) if isinstance(pcfg, dict) else {}

        def _th(name: str, default: float) -> float:
            v = th.get(name, default)
            try:
                return float(v)
            except Exception:
                return float(default)

        # NOTE: limit_up/down ratios are in percent (0~100)
        # Floor is to prevent too-low config causing false 'panic' (e.g. 0.03 -> 0.03%).
        limit_up_ratio_yellow = _th("limit_up_ratio_yellow", 1.5)
        limit_up_ratio_orange = _th("limit_up_ratio_orange", 2.5)
        limit_up_ratio_red = _th("limit_up_ratio_red", 4.0)

        # more reasonable floors for limit-down (panic proxy)
        limit_down_ratio_yellow = max(_th("limit_down_ratio_yellow", 0.30), 0.10)
        limit_down_ratio_orange = max(_th("limit_down_ratio_orange", 0.50), 0.20)
        limit_down_ratio_red = max(_th("limit_down_ratio_red", 1.00), 0.50)
        # enforce monotonic
        limit_down_ratio_orange = max(limit_down_ratio_orange, limit_down_ratio_yellow)
        limit_down_ratio_red = max(limit_down_ratio_red, limit_down_ratio_orange)

        max_consecutive_orange = _th("max_consecutive_orange", 9.0)
        max_consecutive_red = _th("max_consecutive_red", 12.0)

        broken_std_orange = _th("broken_std_orange", 0.30)
        broken_std_red = _th("broken_std_red", 0.45)

        adv_ratio_orange = _th("adv_ratio_orange", 40.0)
        adv_ratio_red = _th("adv_ratio_red", 30.0)

        # panel status: OK -> PARTIAL if upstream warnings indicate partial coverage
        panel_status = str(data_status or "OK").upper()
        if panel_status == "OK":
            if any(str(w).startswith("missing:") or str(w).startswith("error:") for w in rwarns):
                panel_status = "PARTIAL"

        level = "GREEN"
        reasons: List[str] = []

        # 1) risk-off participation weakness (only if adv_ratio is low)
        if adv_ratio_f is not None and adv_ratio_f <= adv_ratio_red:
            level = _max_level([level, "RED"])
            reasons.append(f"上涨占比偏低 {adv_ratio_f:.2f}%")
        elif adv_ratio_f is not None and adv_ratio_f <= adv_ratio_orange:
            level = _max_level([level, "ORANGE"])
            reasons.append(f"上涨占比偏低 {adv_ratio_f:.2f}%")

        # 2) limit-down pressure (panic proxy) - percent thresholds
        if ld_ratio is not None and ld_ratio >= limit_down_ratio_red:
            level = _max_level([level, "RED"])
            reasons.append(f"跌停占比偏高 {ld_ratio:.2f}%")
        elif ld_ratio is not None and ld_ratio >= limit_down_ratio_orange:
            level = _max_level([level, "ORANGE"])
            reasons.append(f"跌停占比偏高 {ld_ratio:.2f}%")
        elif ld_ratio is not None and ld_ratio >= limit_down_ratio_yellow:
            level = _max_level([level, "YELLOW"])
            reasons.append(f"跌停占比偏高 {ld_ratio:.2f}%")

        # 3) limit-up heat (overheat proxy)
        if lu_ratio is not None and lu_ratio >= limit_up_ratio_red:
            level = _max_level([level, "RED"])
            reasons.append(f"涨停占比偏高 {lu_ratio:.2f}%")
        elif lu_ratio is not None and lu_ratio >= limit_up_ratio_orange:
            level = _max_level([level, "ORANGE"])
            reasons.append(f"涨停占比偏高 {lu_ratio:.2f}%")
        elif lu_ratio is not None and lu_ratio >= limit_up_ratio_yellow:
            level = _max_level([level, "YELLOW"])
            reasons.append(f"涨停占比偏高 {lu_ratio:.2f}%")

        # 4) broken board (seal instability)
        if broken_std_f is not None and broken_std_f >= broken_std_red:
            level = _max_level([level, "RED"])
            reasons.append(f"炸板率波动偏高 {broken_std_pct:.2f}%")
        elif broken_std_f is not None and broken_std_f >= broken_std_orange:
            level = _max_level([level, "ORANGE"])
            reasons.append(f"炸板率波动偏高 {broken_std_pct:.2f}%")

        # 5) max consecutive (leader heat)
        if max_consecutive_f is not None and max_consecutive_f >= max_consecutive_red:
            level = _max_level([level, "RED"])
            reasons.append(f"连板高度偏高 {int(max_consecutive_f)}")
        elif max_consecutive_f is not None and max_consecutive_f >= max_consecutive_orange:
            level = _max_level([level, "ORANGE"])
            reasons.append(f"连板高度偏高 {int(max_consecutive_f)}")

        # flags for human meaning
        panic_flag = (ld_ratio is not None and ld_ratio >= limit_down_ratio_orange) or any("跌停占比偏高" in r for r in reasons)
        overheat_flag = (
            (lu_ratio is not None and lu_ratio >= limit_up_ratio_orange)
            or (broken_std_f is not None and broken_std_f >= broken_std_orange)
            or (max_consecutive_f is not None and max_consecutive_f >= max_consecutive_orange)
        )

        # human meaning (avoid ambiguous "或")
        if panic_flag and overheat_flag:
            meaning = "情绪撕裂/分歧：上涨点火与下跌压力并存（两端极端化）。未来 1–2 天更易剧烈轮动或回撤，避免追涨，优先防守/等待。"
        elif panic_flag:
            meaning = "恐慌/下跌压力抬头：弱势反弹的胜率偏低，优先观望、防守，避免逆势加仓。"
        elif overheat_flag:
            meaning = "情绪点火/拥挤且封板不稳：追涨胜率下降，优先控制追高，等确认/等回撤再动。"
        else:
            meaning = "情绪平稳：红盘扩散正常、封板质量正常（可观察，不用急）。"

        if panel_status == "PARTIAL":
            meaning = f"{meaning}（部分子指标缺失/失败，仅按可用数据解读。）"

        

        # optional: down-limit lock proxy (EOD-only, derived from CN_STOCK_DAILY_PRICE)
        stuck_locked_ratio = None
        lock_proxy = evidence.get("down_limit_lock_proxy") if isinstance(evidence, dict) else None
        if isinstance(lock_proxy, dict):
            lock_ev = lock_proxy.get("evidence") if isinstance(lock_proxy.get("evidence"), dict) else {}
            stuck_locked_ratio = _f(lock_ev.get("stuck_locked_ratio_pct") or lock_ev.get("stuck_locked_ratio"))
        key_metrics = {
            "adv_ratio_pct": adv_ratio_f,
            "limit_up": limit_up_f,
            "limit_down": limit_down_f,
            "limit_up_ratio_pct": lu_ratio,
            "limit_down_ratio_pct": ld_ratio,
            "stuck_locked_ratio_pct": stuck_locked_ratio,
            "broken_limit_rate_std_pct": broken_std_pct,
            "max_consecutive_limit_up": max_consecutive_f,
        }

        # assumptions explicitly recorded
        pw.append("assumption:board_limit_pct_prefix_300_301_688_689_20_else_10")
        pw.append("assumption:limit_updown_by_limit_price_round2")

        return {
            "level": level,
            "data_status": panel_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
        }

    def _panel_breadth_plus(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        data_status, rwarns, err_type, err_msg, ev = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)

        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": ev,
            }

        # flexible fields
        pct_ma20 = self._as_pct(ev.get("pct_above_ma20") or ev.get("pct_ma20") or ev.get("above_ma20_pct"))
        pct_ma50 = self._as_pct(ev.get("pct_above_ma50") or ev.get("pct_ma50") or ev.get("above_ma50_pct"))

        new_high = _to_float(ev.get("new_high_20") or ev.get("new_high") or ev.get("nh20"))
        new_low = _to_float(ev.get("new_low_20") or ev.get("new_low") or ev.get("nl20"))
        hl_ratio = _to_float(ev.get("new_high_low_ratio_20") or ev.get("new_high_low_ratio") or ev.get("hl_ratio_20"))
        if hl_ratio is None and (new_high is not None) and (new_low is not None):
            hl_ratio = float(new_high) / max(float(new_low), 1.0)

        total = _to_float(ev.get("total_stocks") or ev.get("coverage") or ev.get("total") or ev.get("count"))
        new_low_ratio = None
        if total and total > 0 and new_low is not None:
            new_low_ratio = float(new_low) * 100.0 / float(total)

        ad = ev.get("ad_line") if isinstance(ev.get("ad_line"), dict) else {}
        ad_slope = _to_float(ad.get("slope_10d") or ad.get("delta_10d") or ad.get("chg_10d"))
        # if series exists
        if ad_slope is None and isinstance(ad.get("series"), list) and len(ad.get("series")) >= 2:
            try:
                ad_slope = float(ad["series"][-1]) - float(ad["series"][-2])
            except Exception:
                ad_slope = None

        th = self.pick(pcfg, "thresholds", {}) if isinstance(pcfg, dict) else {}
        t = {
            "pct_ma50_yellow": float(th.get("pct_ma50_yellow", 55.0)),
            "pct_ma50_orange": float(th.get("pct_ma50_orange", 45.0)),
            "pct_ma50_red": float(th.get("pct_ma50_red", 30.0)),
            "hl_ratio_orange": float(th.get("hl_ratio_orange", 0.6)),
            "hl_ratio_red": float(th.get("hl_ratio_red", 0.3)),
            "new_low_ratio_orange": float(th.get("new_low_ratio_orange", 2.0)),
            "new_low_ratio_red": float(th.get("new_low_ratio_red", 4.0)),
        }

        level = "GREEN"
        reasons: List[str] = []

        if pct_ma50 is not None:
            if pct_ma50 < t["pct_ma50_red"]:
                level = _max_level([level, "RED"])
                reasons.append(f">%MA50 偏低 {pct_ma50:.2f}%")
            elif pct_ma50 < t["pct_ma50_orange"]:
                level = _max_level([level, "ORANGE"])
                reasons.append(f">%MA50 偏低 {pct_ma50:.2f}%")
            elif pct_ma50 < t["pct_ma50_yellow"]:
                level = _max_level([level, "YELLOW"])
                reasons.append(f">%MA50 走弱 {pct_ma50:.2f}%")

        if hl_ratio is not None:
            if hl_ratio < t["hl_ratio_red"]:
                level = _max_level([level, "RED"])
                reasons.append(f"新高/新低比偏低 {hl_ratio:.2f}")
            elif hl_ratio < t["hl_ratio_orange"]:
                level = _max_level([level, "ORANGE"])
                reasons.append(f"新高/新低比偏低 {hl_ratio:.2f}")

        if new_low_ratio is not None:
            if new_low_ratio >= t["new_low_ratio_red"]:
                level = _max_level([level, "RED"])
                reasons.append(f"新低占比偏高 {new_low_ratio:.2f}%")
            elif new_low_ratio >= t["new_low_ratio_orange"]:
                level = _max_level([level, "ORANGE"])
                reasons.append(f"新低占比偏高 {new_low_ratio:.2f}%")

        if ad_slope is not None and ad_slope < 0:
            level = _max_level([level, "YELLOW"])
            reasons.append("A/D line 走弱")

        meaning = "广度健康：参与面较完整（更像扩散或健康轮动）。"
        if level == "YELLOW":
            meaning = "广度略走弱：扩散不足或轮动变快，未来 1–2 天更需要等确认。"
        elif level == "ORANGE":
            meaning = "广度受损：多数个股跌破中期均线/新低增多，趋势更容易回撤，避免追涨。"
        elif level in ("RED", "ERROR"):
            meaning = "广度显著受损：新低扩散/多数跌破关键均线，未来 1–2 天更易出现回撤或风险扩散，优先防守。"

        reasons = reasons[:3]

        key_metrics = {}
        if pct_ma20 is not None:
            key_metrics["pct_above_ma20"] = round(float(pct_ma20), 2)
        if pct_ma50 is not None:
            key_metrics["pct_above_ma50"] = round(float(pct_ma50), 2)
        if hl_ratio is not None:
            key_metrics["new_high_low_ratio"] = round(float(hl_ratio), 3)
        if new_low_ratio is not None:
            key_metrics["new_low_ratio_pct"] = round(float(new_low_ratio), 2)
        if ad_slope is not None:
            key_metrics["ad_slope_10d"] = round(float(ad_slope), 4)

        evidence = {
            "total_stocks": total,
            "pct_above_ma20_pct": pct_ma20,
            "pct_above_ma50_pct": pct_ma50,
            "new_high_20": new_high,
            "new_low_20": new_low,
            "new_high_low_ratio": hl_ratio,
            "new_low_ratio_pct": new_low_ratio,
            "ad_line": ad,
            "ad_slope_10d": ad_slope,
        }

        return {
            "level": level,
            "data_status": data_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
        }

    # Backward-compat alias (if someone temporarily renamed it)
    def _panel_breadth_plus_0(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        return self._panel_breadth_plus(raw=raw, pcfg=pcfg, warnings=warnings)

    def _panel_etf_flow(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        """Panel C: ETF flow (EOD, observation-only).

        Interpret net ETF flow data to provide an early signal. A positive
        total_change_amount together with a positive 10‑day trend implies
        sustained net creations (GREEN), whereas a negative flow with a
        negative trend implies sustained redemptions (RED). Mixed or weak
        signals are labelled YELLOW. The method is intentionally simple and
        avoids external configuration; it only inspects the raw data provided
        by ETFFlowDataSource. If the raw block is missing or malformed,
        appropriate missing/error semantics are returned.
        """
        # Coerce the raw block: unify data_status, warnings and evidence handling.
        data_status, rwarns, err_type, err_msg, evidence = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)

        # If missing or error, propagate status and include error info in warnings.
        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": evidence,
            }

        # Helper to safely convert to float, guarding booleans and None
        def _safe(v: Any) -> Optional[float]:
            try:
                if v is None or isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        # Extract core metrics from evidence
        total_change = _safe(evidence.get("total_change_amount"))
        trend_10d = _safe(evidence.get("trend_10d"))
        acc_3d = _safe(evidence.get("acc_3d"))
        flow_ratio = _safe(evidence.get("flow_ratio"))

        # Determine level: both positive => GREEN, both negative => RED, else YELLOW
        if total_change is None or trend_10d is None:
            level = "MISSING"
        else:
            if total_change > 0 and trend_10d > 0:
                level = "GREEN"
            elif total_change < 0 and trend_10d < 0:
                level = "RED"
            else:
                level = "YELLOW"

        panel_status = data_status if data_status else "OK"

        # Build concise metrics: use raw units with more precision instead of scaling to millions.
        key_metrics: Dict[str, Any] = {}
        if total_change is not None:
            try:
                key_metrics["flow"] = round(total_change, 3)
            except Exception:
                key_metrics["flow"] = total_change
        if trend_10d is not None:
            try:
                key_metrics["trend"] = round(trend_10d, 3)
            except Exception:
                key_metrics["trend"] = trend_10d
        if acc_3d is not None:
            try:
                key_metrics["accel"] = round(acc_3d, 3)
            except Exception:
                key_metrics["accel"] = acc_3d
        if flow_ratio is not None:
            # Show more decimals for tiny ratios
            try:
                key_metrics["ratio"] = round(flow_ratio, 6)
            except Exception:
                key_metrics["ratio"] = flow_ratio

        # Provide simple reasons for extreme cases
        reasons: List[str] = []
        try:
            if total_change is not None and trend_10d is not None:
                if total_change > 0 and trend_10d > 0:
                    reasons.append("持续净申购")
                elif total_change < 0 and trend_10d < 0:
                    reasons.append("持续净赎回")
        except Exception:
            pass

        # Human‑readable summary
        if level == "GREEN":
            meaning = "ETF份额净申购持续增加，资金流入主导，对行情支撑偏积极。"
        elif level == "RED":
            meaning = "ETF份额净赎回持续增加，资金流出压力偏大，或预示风险扩散。"
        elif level == "YELLOW":
            meaning = "ETF份额变化方向不一致或趋势不明显，需结合其他指标观察。"
        else:
            meaning = "数据缺失/不可用（仅占位，不解读）。"

        # Summarise evidence chain for report (optional).
        explain: Optional[List[str]] = None
        series = evidence.get("series") if isinstance(evidence, dict) else None
        if isinstance(series, list) and series:
            try:
                slen = len(series)
                # summarise last 3 entries of each metric
                last = series[-3:]
                # Build strings like "YYYY-MM-DD:value"
                def _fmt_series(lst: List[Dict[str, Any]], key: str) -> str:
                    parts: List[str] = []
                    for itm in lst:
                        try:
                            dt = str(itm.get("trade_date", ""))
                            val = itm.get(key)
                            if val is None:
                                parts.append(f"{dt}:NA")
                            else:
                                v = val
                                if isinstance(v, (int, float)):
                                    # For large numbers (>= 1e7) show in millions with two decimals
                                    if abs(float(v)) >= 1e7:
                                        parts.append(f"{dt}:{float(v)/1e6:.2f}M")
                                    else:
                                        # Use three decimals for small magnitudes
                                        parts.append(f"{dt}:{float(v):.3f}")
                                else:
                                    parts.append(f"{dt}:{v}")
                        except Exception:
                            parts.append("NA")
                    return "; ".join(parts)

                series_change = _fmt_series(last, "total_change_amount")
                series_vol = _fmt_series(last, "total_volume")
                series_amt = _fmt_series(last, "total_amount")
                explain = [
                    f"series_length={slen}",
                    f"近3日 total_change_amount: {series_change}",
                    f"近3日 total_volume: {series_vol}",
                    f"近3日 total_amount: {series_amt}",
                ]
            except Exception:
                explain = None

        return {
            "level": level,
            "data_status": panel_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
            **({"explain": explain} if explain else {}),
        }

    def _panel_futures_basis(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        """
        Panel D: Futures basis (期指基差/贴水) 解析。

        使用 FuturesBasisDataSource 提供的原始数据计算早期风险信号。
        规则：
            - avg_basis < 0 且 trend_10d < 0：RED（持续贴水扩大）
            - avg_basis > 0 且 trend_10d > 0：ORANGE（持续升水扩大）
            - 其它情况：YELLOW（方向不一致或趋势不明显）

        如果原始块缺失或错误，返回 MISSING/ERROR，附带 warnings。
        """
        data_status, rwarns, err_type, err_msg, evidence = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)
        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": evidence,
            }

        def _safe(v: Any) -> Optional[float]:
            try:
                if v is None or isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        avg_basis = _safe(evidence.get("avg_basis"))
        trend_10d = _safe(evidence.get("trend_10d"))
        acc_3d = _safe(evidence.get("acc_3d"))
        basis_ratio = _safe(evidence.get("basis_ratio"))
        # Determine level
        if avg_basis is None or trend_10d is None:
            level = "MISSING"
        else:
            if avg_basis < 0 and trend_10d < 0:
                level = "RED"
            elif avg_basis > 0 and trend_10d > 0:
                level = "ORANGE"
            else:
                level = "YELLOW"
        panel_status = data_status if data_status else "OK"
        # Key metrics
        key_metrics: Dict[str, Any] = {}
        if avg_basis is not None:
            try:
                key_metrics["basis"] = round(avg_basis, 3)
            except Exception:
                key_metrics["basis"] = avg_basis
        if trend_10d is not None:
            try:
                key_metrics["trend"] = round(trend_10d, 3)
            except Exception:
                key_metrics["trend"] = trend_10d
        if acc_3d is not None:
            try:
                key_metrics["accel"] = round(acc_3d, 3)
            except Exception:
                key_metrics["accel"] = acc_3d
        if basis_ratio is not None:
            try:
                key_metrics["ratio"] = round(basis_ratio, 6)
            except Exception:
                key_metrics["ratio"] = basis_ratio
        reasons: List[str] = []
        try:
            if avg_basis is not None and trend_10d is not None:
                if avg_basis < 0 and trend_10d < 0:
                    reasons.append("持续贴水扩大")
                elif avg_basis > 0 and trend_10d > 0:
                    reasons.append("持续升水扩大")
        except Exception:
            pass
        # Meaning summary
        if level == "RED":
            meaning = "期指价格持续低于现货指数且贴水扩大，市场悲观预期增强，需要警惕风险。"
        elif level == "ORANGE":
            meaning = "期指价格持续高于现货指数且升水扩大，市场过于乐观，需保持谨慎。"
        elif level == "YELLOW":
            meaning = "期指基差方向不一致或趋势不明显，需结合其他指标观察。"
        else:
            meaning = "数据缺失/不可用（仅占位，不解读）。"
        # Evidence summary for report (series analysis)
        explain: Optional[List[str]] = None
        series = evidence.get("series") if isinstance(evidence, dict) else None
        if isinstance(series, list) and series:
            try:
                slen = len(series)
                last = series[-3:]
                def _fmt_series(lst: List[Dict[str, Any]], key: str) -> str:
                    parts: List[str] = []
                    for itm in lst:
                        try:
                            dt = str(itm.get("trade_date", ""))
                            val = itm.get(key)
                            if val is None:
                                parts.append(f"{dt}:NA")
                            else:
                                v = val
                                if isinstance(v, (int, float)):
                                    parts.append(f"{dt}:{float(v):.3f}")
                                else:
                                    parts.append(f"{dt}:{v}")
                        except Exception:
                            pass
                    return "; ".join(parts)
                explain = []
                explain.append(f"series_length={slen}")
                explain.append(f"近3日 avg_basis: {_fmt_series(last, 'avg_basis')}")
                explain.append(f"近3日 basis_ratio: {_fmt_series(last, 'basis_ratio')}")
            except Exception:
                explain = None
        panel = {
            "level": level,
            "data_status": panel_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
        }
        if explain:
            panel["explain"] = explain
        return panel

    def _panel_liquidity_quality(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        """
        Panel F: Liquidity Quality (流动性质量) 解析。

        使用 LiquidityQualityDataSource 提供的原始数据生成流动性结构的早期信号。
        规则：
            - 若 top20_ratio > 0.3 且 top20_trend_10d > 0，认为成交集中度持续走高，流动性迅速收缩 → RED。
            - 若 big_small_ratio ≥ 2.0 或 ≤ 0.5，认为大小盘成交严重失衡 → ORANGE。
            - 若 down_low_ratio ≥ 0.5，认为下跌时缩量明显，杀跌意愿减弱 → GREEN。
            - 其它情况 → YELLOW。

        若原始块缺失或错误，返回 MISSING/ERROR，并附带 warnings。
        """
        data_status, rwarns, err_type, err_msg, evidence = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)
        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": evidence,
            }

        def _safe(v: Any) -> Optional[float]:
            try:
                if v is None or isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        # Extract metrics
        top20_ratio = _safe(evidence.get("top20_ratio"))
        big_small_ratio = _safe(evidence.get("big_small_ratio"))
        down_low_ratio = _safe(evidence.get("down_low_ratio"))
        top20_trend = _safe(evidence.get("top20_trend_10d"))
        big_small_trend = _safe(evidence.get("big_small_trend_10d"))
        down_low_trend = _safe(evidence.get("down_low_trend_10d"))

        # Determine level according to heuristics
        try:
            level = self._determine_liq_level(top20_ratio, top20_trend, big_small_ratio, down_low_ratio)
        except Exception:
            level = "MISSING"

        panel_status = data_status if data_status else "OK"

        # Build key metrics
        key_metrics: Dict[str, Any] = {}
        if top20_ratio is not None:
            try:
                key_metrics["top20"] = round(top20_ratio, 4)
            except Exception:
                key_metrics["top20"] = top20_ratio
        if big_small_ratio is not None:
            try:
                key_metrics["big_small"] = round(big_small_ratio, 4)
            except Exception:
                key_metrics["big_small"] = big_small_ratio
        if down_low_ratio is not None:
            try:
                key_metrics["down_low"] = round(down_low_ratio, 4)
            except Exception:
                key_metrics["down_low"] = down_low_ratio
        # also show primary trend for top20 and down_low to highlight direction
        if top20_trend is not None:
            try:
                key_metrics["trend"] = round(top20_trend, 4)
            except Exception:
                key_metrics["trend"] = top20_trend
        if down_low_trend is not None:
            try:
                key_metrics.setdefault("trend_down", round(down_low_trend, 4))
            except Exception:
                key_metrics.setdefault("trend_down", down_low_trend)

        # Reasons summarising key drivers
        reasons: List[str] = []
        try:
            if top20_ratio is not None and top20_trend is not None:
                if float(top20_ratio) > 0.3 and float(top20_trend) > 0:
                    reasons.append("成交集中度持续走高")
            if big_small_ratio is not None:
                br = float(big_small_ratio)
                if br >= 2.0 or br <= 0.5:
                    reasons.append("大/小盘成交显著失衡")
            if down_low_ratio is not None:
                dl = float(down_low_ratio)
                if dl >= 0.5:
                    reasons.append("下跌缩量比高")
        except Exception:
            pass

        # Meaning summary
        if level == "RED":
            meaning = "成交集中度明显上升，流动性收缩，风险偏高。"
        elif level == "ORANGE":
            meaning = "大盘与小盘成交显著失衡，流动性结构失衡，需要关注。"
        elif level == "GREEN":
            meaning = "下跌时缩量明显，杀跌意愿减弱，流动性环境较稳。"
        elif level == "YELLOW":
            meaning = "流动性结构无明显倾向，需结合其他指标观察。"
        else:
            meaning = "数据缺失/不可用（仅占位，不解读）。"

        # Evidence summary for report: series analysis
        explain: Optional[List[str]] = None
        series = evidence.get("series") if isinstance(evidence, dict) else None
        if isinstance(series, list) and series:
            try:
                slen = len(series)
                last = series[-3:]
                def _fmt_series(lst: List[Dict[str, Any]], key: str) -> str:
                    parts: List[str] = []
                    for itm in lst:
                        try:
                            dt = str(itm.get("trade_date", ""))
                            val = itm.get(key)
                            if val is None:
                                parts.append(f"{dt}:NA")
                            else:
                                v = val
                                if isinstance(v, (int, float)):
                                    parts.append(f"{dt}:{float(v):.3f}")
                                else:
                                    parts.append(f"{dt}:{v}")
                        except Exception:
                            pass
                    return "; ".join(parts)
                explain = []
                explain.append(f"series_length={slen}")
                explain.append(f"近3日 top20_ratio: {_fmt_series(last, 'top20_ratio')}")
                explain.append(f"近3日 big_small_ratio: {_fmt_series(last, 'big_small_ratio')}")
                explain.append(f"近3日 down_low_ratio: {_fmt_series(last, 'down_low_ratio')}")
            except Exception:
                explain = None

        panel = {
            "level": level,
            "data_status": panel_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
        }
        if explain:
            panel["explain"] = explain
        return panel

    def _panel_options_risk(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        """
        Panel E: Options risk (期权风险定价) 解析。

        使用 OptionsRiskDataSource 提供的原始数据计算期权风险信号。
        规则：
            - weighted_change < 0 且 trend_10d < 0：RED（持续下跌扩大）
            - weighted_change > 0 且 trend_10d > 0：ORANGE（持续上涨扩大）
            - 其它情况：YELLOW（方向不一致或趋势不明显）

        若原始块缺失或错误，返回 MISSING/ERROR，并附带 warnings。
        """
        data_status, rwarns, err_type, err_msg, evidence = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)
        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": evidence,
            }

        def _safe(v: Any) -> Optional[float]:
            try:
                if v is None or isinstance(v, bool):
                    return None
                return float(v)
            except Exception:
                return None

        weighted_change = _safe(evidence.get("weighted_change"))
        trend_10d = _safe(evidence.get("trend_10d"))
        acc_3d = _safe(evidence.get("acc_3d"))
        change_ratio = _safe(evidence.get("change_ratio"))

        # Heuristic: all-zero placeholders are common when upstream is not connected.
        # If all key fields are 0 and there is no series/volume evidence, treat as MISSING.
        try:
            _series = evidence.get("series")
            _has_series = isinstance(_series, list) and len(_series) > 0
            _tv = _safe(evidence.get("total_volume") or evidence.get("total") or 0.0)
            _is_zero_payload = (
                (weighted_change in (0.0, None)) and (trend_10d in (0.0, None)) and (acc_3d in (0.0, None)) and (change_ratio in (0.0, None))
            )
            if _is_zero_payload and (not _has_series) and (_tv is None or _tv == 0.0):
                pw.append("zero_placeholder:options_risk")
                return {
                    "level": "MISSING",
                    "data_status": "MISSING",
                    "warnings": pw,
                "asof": raw_asof,
                    "key_metrics": {},
                    "reasons": [],
                    "meaning": "数据缺失/不可用（检测到全 0 占位；不解读）。",
                    "evidence": evidence,
                }
        except Exception:
            pass

        # Determine level
        if weighted_change is None or trend_10d is None:
            level = "MISSING"
        else:
            if weighted_change < 0 and trend_10d < 0:
                level = "RED"
            elif weighted_change > 0 and trend_10d > 0:
                level = "ORANGE"
            else:
                level = "YELLOW"

        panel_status = data_status if data_status else "OK"
        key_metrics: Dict[str, Any] = {}
        if weighted_change is not None:
            try:
                key_metrics["change"] = round(weighted_change, 3)
            except Exception:
                key_metrics["change"] = weighted_change
        if trend_10d is not None:
            try:
                key_metrics["trend"] = round(trend_10d, 3)
            except Exception:
                key_metrics["trend"] = trend_10d
        if acc_3d is not None:
            try:
                key_metrics["accel"] = round(acc_3d, 3)
            except Exception:
                key_metrics["accel"] = acc_3d
        if change_ratio is not None:
            try:
                key_metrics["ratio"] = round(change_ratio, 6)
            except Exception:
                key_metrics["ratio"] = change_ratio
        reasons: List[str] = []
        try:
            if weighted_change is not None and trend_10d is not None:
                if weighted_change < 0 and trend_10d < 0:
                    reasons.append("持续下跌扩大")
                elif weighted_change > 0 and trend_10d > 0:
                    reasons.append("持续上涨扩大")
        except Exception:
            pass
        # Meaning summary
        if level == "RED":
            meaning = "期权整体价格持续下跌且跌幅扩大，市场避险情绪上升，需警惕风险。"
        elif level == "ORANGE":
            meaning = "期权整体价格持续上涨且涨幅扩大，市场可能过于乐观，需保持谨慎。"
        elif level == "YELLOW":
            meaning = "期权涨跌方向不一致或趋势不明显，需结合其他指标观察。"
        else:
            meaning = "数据缺失/不可用（仅占位，不解读）。"
        # Evidence summary for report (series analysis)
        explain: Optional[List[str]] = None
        series = evidence.get("series") if isinstance(evidence, dict) else None
        if isinstance(series, list) and series:
            try:
                slen = len(series)
                last = series[-3:]
                def _fmt_series(lst: List[Dict[str, Any]], key: str) -> str:
                    parts: List[str] = []
                    for itm in lst:
                        try:
                            dt = str(itm.get("trade_date", ""))
                            val = itm.get(key)
                            if val is None:
                                parts.append(f"{dt}:NA")
                            else:
                                v = val
                                if isinstance(v, (int, float)):
                                    parts.append(f"{dt}:{float(v):.3f}")
                                else:
                                    parts.append(f"{dt}:{v}")
                        except Exception:
                            pass
                    return "; ".join(parts)
                explain = []
                explain.append(f"series_length={slen}")
                explain.append(f"近3日 weighted_change: {_fmt_series(last, 'weighted_change')}")
                explain.append(f"近3日 change_ratio: {_fmt_series(last, 'change_ratio')}")
            except Exception:
                explain = None
        panel = {
            "level": level,
            "data_status": panel_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
        }
        if explain:
            panel["explain"] = explain
        return panel


    def _panel_margin_intensity(self, raw: Any, pcfg: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        """Panel G: Margin intensity (两融强度) — observation-only.

        Notes (Frozen):
        - Upstream raw block key is expected to be `margin_intensity_raw`.
        - For backward compatibility, callers may pass `margin_raw` content into this panel.
        - Margin data is often published with 1 trading-day lag. We must display it as-of date
          and NEVER mark it missing silently.
        """
        data_status, rwarns, err_type, err_msg, ev = self._coerce_raw_block(raw)
        pw: List[str] = []
        pw.extend(rwarns)

        # hard missing / error
        if data_status in ("MISSING", "ERROR"):
            if err_type or err_msg:
                pw.append(f"error:{err_type or 'unknown'}:{err_msg or ''}".strip(":"))
            return {
                "level": "MISSING" if data_status == "MISSING" else "ERROR",
                "data_status": data_status,
                "warnings": pw,
                "asof": raw_asof,
                "key_metrics": {},
                "reasons": [],
                "meaning": "数据缺失/不可用（仅占位，不解读）。" if data_status == "MISSING" else "数据错误（可见 warnings）。",
                "evidence": ev,
            }

        evidence = ev if isinstance(ev, dict) else {}

        # as-of lag handling (common)
        req_td = evidence.get("requested_trade_date")
        asof_td = evidence.get("trade_date") or evidence.get("asof_date")
        lag = evidence.get("asof_lag_days")
        try:
            lag_i = int(lag) if lag is not None else 0
        except Exception:
            lag_i = 0
        if lag_i > 0:
            pw.append(f"asof_lag_days={lag_i}")
        if req_td and asof_td and str(req_td) != str(asof_td):
            pw.append(f"asof_trade_date={asof_td} (requested={req_td})")

        def _f(x: Any) -> Optional[float]:
            try:
                if x is None or isinstance(x, bool):
                    return None
                return float(x)
            except Exception:
                return None

        rz_balance = _f(evidence.get("rz_balance"))
        rq_balance = _f(evidence.get("rq_balance"))
        total_bal = _f(evidence.get("total") or evidence.get("total_balance"))
        rz_buy = _f(evidence.get("rz_buy"))
        total_chg = _f(evidence.get("total_chg") or evidence.get("chg"))
        rz_ratio = _f(evidence.get("rz_ratio"))
        trend_10d = _f(evidence.get("trend_10d"))
        acc_3d = _f(evidence.get("acc_3d"))
        change_ratio = _f(evidence.get("change_ratio"))

        # Heuristic: all-zero placeholders (or empty series) should not trigger RED/ORANGE conclusions.
        try:
            _series = evidence.get("series")
            _series_len = evidence.get("series_length")
            _has_series = (isinstance(_series, list) and len(_series) > 0) or (isinstance(_series_len, int) and _series_len > 0)
            _is_zero_payload = True
            for _v in [rz_balance, rq_balance, total_bal, rz_buy, total_chg, rz_ratio, trend_10d, acc_3d, change_ratio]:
                if _v is None:
                    continue
                if float(_v) != 0.0:
                    _is_zero_payload = False
                    break
            if _is_zero_payload and (not _has_series):
                pw.append("zero_placeholder:margin_intensity")
                return {
                    "level": "MISSING",
                    "data_status": "MISSING",
                    "warnings": pw,
                "asof": raw_asof,
                    "key_metrics": {},
                    "reasons": [],
                    "meaning": "数据缺失/不可用（检测到全 0 占位；不解读）。",
                    "evidence": evidence,
                }
        except Exception:
            pass


        # thresholds (best-effort defaults; ratios are fractions unless clearly percent in upstream)
        th = self.pick(pcfg, "thresholds", {}) if isinstance(pcfg, dict) else {}

        def _th(name: str, default: float) -> float:
            v = th.get(name, default)
            try:
                return float(v)
            except Exception:
                return float(default)

        # change_ratio thresholds (fractional)
        red_chg = _th("change_ratio_red", -0.04)
        orange_chg = _th("change_ratio_orange", -0.02)
        yellow_chg = _th("change_ratio_yellow", -0.01)

        # deleveraging acceleration thresholds
        orange_acc = _th("acc_3d_orange", -0.01)
        red_acc = _th("acc_3d_red", -0.02)

        # normalize if upstream gave percent (heuristic)
        if change_ratio is not None and abs(change_ratio) > 2.0:
            change_ratio = change_ratio / 100.0

        level = "GREEN"
        reasons: List[str] = []

        # primary: day-over-day balance change
        if change_ratio is not None and change_ratio <= red_chg:
            level = _max_level([level, "RED"])
            reasons.append(f"两融余额下降幅度偏大 {change_ratio:.2%}")
        elif change_ratio is not None and change_ratio <= orange_chg:
            level = _max_level([level, "ORANGE"])
            reasons.append(f"两融余额下降 {change_ratio:.2%}")
        elif change_ratio is not None and change_ratio <= yellow_chg:
            level = _max_level([level, "YELLOW"])
            reasons.append(f"两融余额小幅下降 {change_ratio:.2%}")

        # secondary: acceleration (3d)
        if acc_3d is not None and acc_3d <= red_acc:
            level = _max_level([level, "RED"])
            reasons.append("去杠杆加速偏强（3D）")
        elif acc_3d is not None and acc_3d <= orange_acc:
            level = _max_level([level, "ORANGE"])
            reasons.append("去杠杆加速（3D）")

        meaning = "两融强度（滞后一日常见）：用于观察杠杆扩张/收缩对风险偏好的影响。"

        key_metrics = {
            "rz_balance": rz_balance,
            "rq_balance": rq_balance,
            "total_balance": total_bal,
            "rz_buy": rz_buy,
            "total_chg": total_chg,
            "rz_ratio": rz_ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "change_ratio": change_ratio,
            "asof_trade_date": asof_td,
        }

        # panel status
        panel_status = str(data_status or "OK").upper()
        if panel_status == "OK":
            if any(str(w).startswith("missing:") or str(w).startswith("error:") for w in rwarns):
                panel_status = "PARTIAL"

        return {
            "level": level,
            "data_status": panel_status,
            "warnings": pw,
                "asof": raw_asof,
            "key_metrics": key_metrics,
            "reasons": reasons,
            "meaning": meaning,
            "evidence": evidence,
        }


    def _determine_liq_level(
        self,
        top20_ratio: float | None,
        top20_trend: float | None,
        big_small_ratio: float | None,
        down_low_ratio: float | None,
    ) -> str:
        """
        根据流动性指标判断等级。

        此方法独立便于测试和阅读。
        """
        try:
            # RED: 高集中度且继续上升
            if top20_ratio is not None and top20_trend is not None:
                if float(top20_ratio) > 0.3 and float(top20_trend) > 0:
                    return "RED"
            # ORANGE: 大小盘严重失衡
            if big_small_ratio is not None:
                br = float(big_small_ratio)
                if br >= 2.0 or br <= 0.5:
                    return "ORANGE"
            # GREEN: 缩量下跌比高
            if down_low_ratio is not None:
                dl = float(down_low_ratio)
                if dl >= 0.5:
                    return "GREEN"
            # default
            return "YELLOW"
        except Exception:
            return "MISSING"

    def _aggregate_tplus2_lead(self, lead_panels: Dict[str, Any]) -> Dict[str, Any]:
        # Determine overall by the worst non-missing panel
        worst_lv = "MISSING"
        ok_cnt = 0
        miss_cnt = 0
        reasons: List[str] = []
        confirms: List[str] = []

        # Tagging (Frozen, append-only): distinguish "HEAT" vs "RISK" when overall=RED.
        # - HEAT: 情绪偏热/封板不稳导致的追涨红灯（风险侧未扩散）
        # - RISK: 风险扩散/广度受损导致的风险红灯（可能影响退出/回撤）
        # Keep overall_level unchanged for backward-compat; only add overall_tag/overall_display.
        heat_risk_tag: Optional[str] = None

        # risk-side proxy metrics (best-effort)
        _stuck_locked_ratio_pct: Optional[float] = None
        _limit_down_ratio_pct: Optional[float] = None
        _new_low_ratio_pct: Optional[float] = None
        _ad_slope_10d: Optional[float] = None
        _pct_above_ma50: Optional[float] = None

        for k, p in (lead_panels or {}).items():
            if not isinstance(p, dict):
                continue
            lv = str(p.get("level") or "MISSING").upper()
            ds = str(p.get("data_status") or "MISSING").upper()
            if lv in ("MISSING",) or ds in ("MISSING",):
                miss_cnt += 1
                continue
            ok_cnt += 1
            if self._level_rank(lv) > self._level_rank(worst_lv):
                worst_lv = lv

            rs = p.get("reasons")
            if isinstance(rs, list) and rs:
                # take first 2 for aggregation
                for x in rs[:2]:
                    reasons.append(f"{k}：{str(x)}")

            km = p.get("key_metrics")
            if isinstance(km, dict) and km:
                # compact confirm strings (panel-specific)
                if k == "market_sentiment":
                    b = km.get("broken_limit_rate_std_pct")
                    m = km.get("max_consecutive_limit_up")
                    d = km.get("limit_down")
                    if b is not None:
                        confirms.append(f"炸板率(std)={float(b):.2f}%")
                    if m is not None:
                        confirms.append(f"连板高度={int(m)}")
                    if d is not None:
                        # limit-down ratio (if available)
                        ev = p.get("evidence") if isinstance(p.get("evidence"), dict) else {}
                        ld_ratio = ev.get("limit_down_ratio_pct")
                        if ld_ratio is not None:
                            confirms.append(f"跌停占比={float(ld_ratio):.2f}%")
                elif k == "breadth_plus":
                    ma50 = km.get("pct_above_ma50") or km.get("pct_above_ma50_pct")
                    ev2 = p.get("evidence") if isinstance(p.get("evidence"), dict) else {}

                    # stash for tag decision
                    try:
                        _pct_above_ma50 = float(ma50) if ma50 is not None else _pct_above_ma50
                    except Exception:
                        pass
                    try:
                        _new_low_ratio_pct = float(ev2.get("new_low_ratio_pct")) if ev2.get("new_low_ratio_pct") is not None else _new_low_ratio_pct
                    except Exception:
                        pass

                    # MA50 breadth
                    if ma50 is not None:
                        confirms.append(f">%MA50={float(ma50):.2f}%")

                    # New High / New Low (prefer 50D counts + ratio, more stable)
                    nh50 = ev2.get("new_high_50") if ev2.get("new_high_50") is not None else ev2.get("new_high_50d")
                    nl50 = ev2.get("new_low_50") if ev2.get("new_low_50") is not None else ev2.get("new_low_50d")
                    hl = (
                        km.get("new_high_low_ratio")
                        if km.get("new_high_low_ratio") is not None
                        else ev2.get("new_high_low_ratio")
                    )
                    if nh50 is not None and nl50 is not None:
                        try:
                            nh50_i = int(nh50)
                            nl50_i = int(nl50)
                        except Exception:
                            nh50_i = nh50
                            nl50_i = nl50
                        if hl is not None:
                            confirms.append(f"NH/NL50={nh50_i}/{nl50_i} (r={float(hl):.2f})")
                        else:
                            confirms.append(f"NH/NL50={nh50_i}/{nl50_i}")
                    elif hl is not None:
                        confirms.append(f"新高/新低比={float(hl):.2f}")

                    # A/D line
                    ad = ev2.get("ad_line") if isinstance(ev2.get("ad_line"), dict) else {}
                    slope10 = ad.get("slope_10d")
                    net5 = ad.get("net_adv_5d")
                    try:
                        _ad_slope_10d = float(slope10) if slope10 is not None else _ad_slope_10d
                    except Exception:
                        pass
                    if slope10 is not None:
                        confirms.append(f"A/D斜率10D={float(slope10):.3f}")
                    elif net5 is not None:
                        confirms.append(f"A/D净扩散5D={int(net5)}")
            
            # panel-specific risk proxy stash for tag decision
            if k == "market_sentiment":
                evs = p.get("evidence") if isinstance(p.get("evidence"), dict) else {}
                try:
                    _limit_down_ratio_pct = float(evs.get("limit_down_ratio_pct")) if evs.get("limit_down_ratio_pct") is not None else _limit_down_ratio_pct
                except Exception:
                    pass
                # stuck_locked_ratio_pct is nested under down_limit_lock_proxy.evidence
                lock = evs.get("down_limit_lock_proxy") if isinstance(evs.get("down_limit_lock_proxy"), dict) else {}
                lock_ev = lock.get("evidence") if isinstance(lock.get("evidence"), dict) else {}
                try:
                    _stuck_locked_ratio_pct = float(lock_ev.get("stuck_locked_ratio_pct")) if lock_ev.get("stuck_locked_ratio_pct") is not None else _stuck_locked_ratio_pct
                except Exception:
                    pass

        data_status = "MISSING" if ok_cnt == 0 else ("PARTIAL" if miss_cnt > 0 else "OK")

        # Tag decision (best-effort, no external config):
        # If overall=RED, label as:
        # - RISK: risk-side confirms (new lows / A-D slope negative / locked-down stress)
        # - HEAT: otherwise (hot but breadth still strong)
        if worst_lv == "RED":
            risk_confirmed = False
            try:
                if _stuck_locked_ratio_pct is not None and _stuck_locked_ratio_pct >= 0.80:
                    risk_confirmed = True
                if _limit_down_ratio_pct is not None and _limit_down_ratio_pct >= 0.30:
                    risk_confirmed = True
                if _new_low_ratio_pct is not None and _new_low_ratio_pct >= 2.00:
                    risk_confirmed = True
                if _ad_slope_10d is not None and _ad_slope_10d < 0:
                    risk_confirmed = True
                # guard rail: if MA50 breadth already weak, treat as RISK even without other confirms
                if _pct_above_ma50 is not None and _pct_above_ma50 < 45.0 and (_new_low_ratio_pct is not None and _new_low_ratio_pct >= 1.0):
                    risk_confirmed = True
            except Exception:
                # never break; tag is optional
                risk_confirmed = False

            heat_risk_tag = "RISK" if risk_confirmed else "HEAT"

        # One-liner
        one_liner = "领先结构数据缺失（仅占位）。"
        if worst_lv == "GREEN":
            one_liner = "领先结构平稳：未来 1–2 天更可能延续或温和轮动（仍以 Gate/Execution 为准）。"
        elif worst_lv == "YELLOW":
            one_liner = "领先结构略有升温/或扩散不足：未来 1–2 天更可能轮动加快，进攻更看执行。"
        elif worst_lv == "ORANGE":
            one_liner = "领先结构撕裂/分歧加大或热度升温：未来 1–2 天更易轮动加速或出现回撤。情绪点火但封板不稳时，追涨胜率偏低，优先等确认/等回撤。"
        elif worst_lv in ("RED", "ERROR"):
            if heat_risk_tag == "HEAT":
                one_liner = "领先结构点火且封板不稳（追涨红灯）：未来 1–2 天更易震荡加剧或出现回撤。避免追高与高位换仓，优先等确认/等回撤。"
            elif heat_risk_tag == "RISK":
                one_liner = "领先结构风险扩散/广度受损（风险红灯）：未来 1–2 天更易回撤与扩散。优先防守/降档/减少摩擦，等待确认。"
            else:
                one_liner = "领先结构过热或广度显著受损：未来 1–2 天更易回撤/扩散。避免追涨与高位换仓，优先防守/降档/等待确认。"

        # keep compact
        reasons = reasons[:3]
        confirms = confirms[:4]

        out = {
            "overall_level": worst_lv,
            "overall_tag": heat_risk_tag,
            "overall_display": f"{worst_lv}({heat_risk_tag})" if isinstance(heat_risk_tag, str) and heat_risk_tag else worst_lv,
            "one_liner": one_liner,
            "reasons": reasons,
            "confirm_signals": confirms,
            "data_status": data_status,
            "warnings": [],
        }

        # Append-only: provide compact tag evidence for audit (optional fields)
        out["tag_evidence"] = {
            "stuck_locked_ratio_pct": _stuck_locked_ratio_pct,
            "limit_down_ratio_pct": _limit_down_ratio_pct,
            "new_low_ratio_pct": _new_low_ratio_pct,
            "ad_slope_10d": _ad_slope_10d,
            "pct_above_ma50": _pct_above_ma50,
        }
        return out
