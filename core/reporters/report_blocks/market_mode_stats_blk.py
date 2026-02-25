# #-*- coding: utf-8 -*-
"""Market Mode Stats Layer (EOD) · Report Block

Frozen scope:
- Read-only statistics & display for audit/replay.
- Must NOT change any decision logic (Gate/DRS/Rotation/ActionHint).
- Prefer Run→Persist / L2 persistence (SQLite unifiedrisk.db).
- Must gracefully degrade when history or fields are missing.

Output (frozen):
- Last 20 days distribution by market_mode
- Last 60 days grouped stats by market_mode
- data_status line + explicit fallbacks / missing fields

Key logs (frozen):
- [MarketModeStats] source=... days=... modes=... missing=...
- [MarketModeStats] fallback_reason=...
"""

from __future__ import annotations

import json
import datetime
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.config_loader import ROOT_DIR
from core.utils.logger import get_logger


LOG = get_logger("Block.MarketModeStats")


_RE_MODE = re.compile(r"当前阶段：\*\*(?P<mode>[A-Z_]+)\*\*")


def _dig(d: Any, *path: str) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _pct_fmt(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    return f"{v:.2f}%"


@dataclass
class _HistRow:
    trade_date: str
    mode: str
    ret_pct: Optional[float]
    mode_src: str
    ret_src: str
    drs_level: str


class MarketModeStatsBlock(ReportBlockRendererBase):
    block_alias = "governance.market_mode_stats"
    title = "市场制度统计（Market Mode Stats）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        db_path = self._resolve_unifiedrisk_db_path()
        source = "sqlite:unifiedrisk.db"
        rows: List[_HistRow] = []
        status_notes: List[str] = []

        if not db_path or not db_path.exists():
            LOG.warning("[MarketModeStats] fallback_reason=missing_db path=%s", db_path)
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=["stale:missing_unifiedrisk_db"],
                payload={
                    "data_status": "STALE",
                    "source": "none",
                    "content": [
                        "（未找到本地持久化 DB：./data/persistent/unifiedrisk.db；无法生成 MarketMode Stats）",
                        "- data_status=STALE",
                    ],
                },
            )

        try:
            rows, status_notes = self._load_history_from_sqlite(db_path=db_path, report_kind=context.kind)
        except Exception as e:
            LOG.exception("[MarketModeStats] sqlite read failed: %s", e)
            warnings.append("exception:market_mode_stats_sqlite_read")
            status_notes.append(f"exception_sqlite_read:{type(e).__name__}")
            rows = []

        data_status = self._judge_data_status(rows=rows, status_notes=status_notes)

        last20 = rows[:20]
        last60 = rows[:60]

        dist_lines, dist_meta = self._build_dist_20(last20)
        group_lines, group_meta = self._build_group_stats_60(last60)

        missing = {
            "mode_missing": sum(1 for r in last60 if r.mode == "UNKNOWN"),
            "ret_missing": sum(1 for r in last60 if r.ret_pct is None),
            "drs_missing": sum(1 for r in last60 if (not r.drs_level) or r.drs_level == "UNKNOWN"),
        }
        LOG.info(
            "[MarketModeStats] source=%s days=%s modes=%s missing=%s",
            source,
            len(last60),
            sorted(list(group_meta.get("modes", []))),
            missing,
        )

        lines: List[str] = []
        lines.append(f"- data_status={data_status} | source={source} | days={len(last60)}")
        if status_notes:
            lines.append("- notes: " + "; ".join(status_notes))
        if dist_meta.get("fallbacks"):
            lines.append("- fallbacks: " + "; ".join(dist_meta["fallbacks"]))
        extra_fb = [
            x for x in (group_meta.get("fallbacks") or []) if x not in (dist_meta.get("fallbacks") or [])
        ]
        if extra_fb:
            lines.append("- fallbacks+: " + "; ".join(extra_fb))

        lines.append("")
        lines.append("### 近20日制度分布")
        lines.extend(dist_lines)
        lines.append("")
        lines.append("### 近60日分组统计（按 mode）")
        lines.extend(group_lines)

        # A) Mode Performance Monitor (read-only)
        perf_lines, perf_meta = self._build_mode_performance_monitor(last60)
        lines.append("")
        lines.append("### 制度表现监控（Mode Performance Monitor · Read-only）")
        lines.extend(perf_lines)

        # A2) Mode Separation Check (read-only)
        sep_lines, sep_meta = self._build_mode_separation_check(last60)
        lines.append("")
        lines.append("### 制度区分能力检测（Mode Separation Check · Read-only）")
        lines.extend(sep_lines)
        # A3) Mode × DRS Cross Separation (read-only)
        cross_lines, cross_meta = self._build_mode_drs_cross_separation(last60)
        lines.append("")
        lines.append("### 制度×DRS 交叉区分能力（Mode×DRS Separation · Read-only）")
        lines.extend(cross_lines)

        # C1) Mode Transition Matrix (read-only)
        trans_lines, trans_meta = self._build_mode_transition_matrix(last60)
        lines.append("")
        lines.append("### 制度转换矩阵（Mode Transition Matrix · Read-only）")
        lines.extend(trans_lines)

        # C2) Rolling Stability (read-only)
        stab_lines, stab_meta = self._build_mode_separation_stability(last60)
        lines.append("")
        lines.append("### 区分能力稳定性（Rolling Stability · Read-only）")
        lines.extend(stab_lines)


        # B) Mode Duration Monitor (read-only)
        dur_lines, dur_meta = self._build_mode_duration_monitor(last60)
        lines.append("")
        lines.append("### 模式持续天数提醒（Mode Duration Monitor · Read-only）")
        lines.extend(dur_lines)

        # merge meta for audit
        try:
            group_meta["performance_monitor"] = perf_meta
            group_meta["separation_check"] = sep_meta
            group_meta["mode_drs_cross_separation"] = cross_meta
            group_meta["mode_transition_matrix"] = trans_meta
            group_meta["rolling_stability"] = stab_meta
            group_meta["duration_monitor"] = dur_meta
        except Exception:
            pass

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            warnings=warnings,
            payload={
                "data_status": data_status,
                "source": source,
                "content": lines,
                "meta": {
                    "dist_20": dist_meta,
                    "group_60": group_meta,
                    "status_notes": status_notes,
                },
            },
        )

    def _judge_data_status(self, *, rows: List[_HistRow], status_notes: List[str]) -> str:
        if not rows:
            status_notes.append("no_history_rows")
            return "STALE"
        n = len(rows)
        n_mode_known = sum(1 for r in rows if r.mode and r.mode != "UNKNOWN")
        n_ret_valid = sum(1 for r in rows if r.ret_pct is not None)
        n_drs_known = sum(1 for r in rows if r.drs_level and r.drs_level != "UNKNOWN")
        if n >= 40 and n_mode_known >= max(10, int(n * 0.6)) and n_ret_valid >= max(10, int(n * 0.6)) and n_drs_known >= max(10, int(n * 0.6)):
            return "OK"
        # PARTIAL
        if n < 60:
            status_notes.append(f"history_short:{n}")
        if n_mode_known < n:
            status_notes.append(f"mode_missing:{n-n_mode_known}")
        if n_ret_valid < n:
            status_notes.append(f"ret_missing:{n-n_ret_valid}")
        if n_drs_known < n:
            status_notes.append(f"drs_missing:{n-n_drs_known}")
        return "PARTIAL"

    # ----------------------------
    # history load
    # ----------------------------
    def _resolve_unifiedrisk_db_path(self) -> Optional[Path]:
        try:
            return Path(ROOT_DIR) / "data" / "persistent" / "unifiedrisk.db"
        except Exception:
            return None

    def _open_sqlite(self, db_path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(str(db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_history_from_sqlite(self, *, db_path: Path, report_kind: str) -> Tuple[List[_HistRow], List[str]]:
        rk = (report_kind or "EOD").strip() or "EOD"
        notes: List[str] = []
        rows: List[_HistRow] = []
        fallback_reasons: List[str] = []

        conn = self._open_sqlite(db_path)
        try:
            sql = (
                "SELECT d.trade_date, d.des_payload_json, r.content_text "
                "  FROM ur_decision_evidence_snapshot d "
                "  LEFT JOIN ur_report_artifact r "
                "    ON r.trade_date=d.trade_date AND r.report_kind=d.report_kind "
                " WHERE d.report_kind=? "
                " ORDER BY d.trade_date DESC "
                " LIMIT 60;"
            )
            q = conn.execute(sql, (rk,)).fetchall()
            if not q:
                return [], ["no_rows_in_ur_decision_evidence_snapshot"]

            for r in q:
                td = str(r["trade_date"])
                des_payload: Dict[str, Any] = {}
                try:
                    des_payload = json.loads(r["des_payload_json"]) if r["des_payload_json"] else {}
                except Exception:
                    des_payload = {}
                    notes.append(f"bad_json:des:{td}")

                report_text = r["content_text"] if ("content_text" in r.keys()) else None

                mode, mode_src = self._extract_market_mode(des_payload=des_payload, report_text=report_text)
                if mode_src and mode_src not in ("des_payload", "des_payload_compat"):
                    fb = f"mode_from:{mode_src}"
                    if fb not in fallback_reasons:
                        fallback_reasons.append(fb)

                ret_pct, ret_src = self._extract_index_ret_pct(des_payload=des_payload, report_text=report_text)
                if ret_src and ret_src not in ("des_payload", "des_payload_compat"):
                    fb = f"ret_from:{ret_src}"
                    if fb not in fallback_reasons:
                        fallback_reasons.append(fb)

                drs_level, drs_src = self._extract_drs_level(des_payload=des_payload, report_text=report_text)
                if drs_src and drs_src not in ("des_payload", "des_payload_compat"):
                    fb = f"drs_from:{drs_src}"
                    if fb not in fallback_reasons:
                        fallback_reasons.append(fb)

                rows.append(
                    _HistRow(
                        trade_date=td,
                        mode=mode,
                        ret_pct=ret_pct,
                        mode_src=mode_src,
                        ret_src=ret_src,
                        drs_level=drs_level,
                    )
                )

            if fallback_reasons:
                notes.append("fallback:" + ";".join(fallback_reasons))
                LOG.info("[MarketModeStats] fallback_reason=%s", ";".join(fallback_reasons))

            return rows, notes
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ----------------------------
    # extractors
    # ----------------------------
    def _extract_market_mode(self, *, des_payload: Dict[str, Any], report_text: Any) -> Tuple[str, str]:
        mode = _dig(des_payload, "governance", "market_mode", "mode")
        if isinstance(mode, str) and mode.strip():
            return mode.strip(), "des_payload"

        mode2 = _dig(des_payload, "governance", "market_mode_mode")
        if isinstance(mode2, str) and mode2.strip():
            return mode2.strip(), "des_payload_compat"

        if isinstance(report_text, str) and report_text:
            m = _RE_MODE.search(report_text)
            if m:
                mm = m.group("mode")
                if mm:
                    return mm.strip(), "report_text"

        return "UNKNOWN", "missing"


    def _extract_drs_level(self, *, des_payload: Dict[str, Any], report_text: Any) -> Tuple[str, str]:
        # prefer persisted governance/factor fields; fallback to report_text parsing.
        candidates = [
            _dig(des_payload, "governance", "drs", "level"),
            _dig(des_payload, "governance", "drs_level"),
            _dig(des_payload, "governance", "daily_risk_signal", "level"),
            _dig(des_payload, "factors", "drs", "level"),
            _dig(des_payload, "factors", "daily_risk_signal", "level"),
            _dig(des_payload, "factors", "drs", "details", "level"),
        ]
        for v in candidates:
            if isinstance(v, str) and v.strip():
                return v.strip().upper(), "des_payload"

        if isinstance(report_text, str) and report_text:
            m = re.search(r"DRS\s*=\s*(GREEN|YELLOW|RED)", report_text, flags=re.IGNORECASE)
            if m:
                return m.group(1).upper(), "report_text"

        return "UNKNOWN", "missing"

    def _extract_index_ret_pct(self, *, des_payload: Dict[str, Any], report_text: Any) -> Tuple[Optional[float], str]:
        v = _dig(des_payload, "factors", "index_tech", "details", "hs300_pct")
        ret = _as_float(v)
        if ret is not None:
            return ret, "des_payload"

        v2 = _dig(des_payload, "structure", "market_overview", "indices", "hs300", "pct")
        ret2 = _as_float(v2)
        if ret2 is not None:
            return ret2, "des_payload_compat"

        if isinstance(report_text, str) and report_text:
            m = re.search(r"沪深300\s*[：:]?\s*([+-]?\d+(?:\.\d+)?)%", report_text)
            if m:
                return _as_float(m.group(1)), "report_text"
            m2 = re.search(r"上证(?:指数)?\s*[：:]?\s*([+-]?\d+(?:\.\d+)?)%", report_text)
            if m2:
                return _as_float(m2.group(1)), "report_text_fallback_sh"

        return None, "missing"

    # ----------------------------
    # stats
    # ----------------------------
    def _build_dist_20(self, rows20: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        meta: Dict[str, Any] = {"window": 20, "fallbacks": []}
        if not rows20:
            return ["- （缺历史数据：近20日为空）"], meta

        total = len(rows20)
        counts: Dict[str, int] = {}
        for r in rows20:
            k = r.mode or "UNKNOWN"
            counts[k] = counts.get(k, 0) + 1

        cur_mode = rows20[0].mode or "UNKNOWN"
        streak = 0
        for r in rows20:
            if (r.mode or "UNKNOWN") == cur_mode:
                streak += 1
            else:
                break

        meta.update({"counts": counts, "current_mode": cur_mode, "current_streak": streak})

        fb: List[str] = []
        if any(r.mode_src == "report_text" for r in rows20):
            fb.append("mode:report_text")
        if any(r.mode == "UNKNOWN" for r in rows20):
            fb.append("mode:UNKNOWN")
        meta["fallbacks"] = fb

        lines: List[str] = []
        lines.append(f"- 当前 mode：**{cur_mode}** | 连续：{streak} 天")
        for mode, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            pct = (cnt / total) * 100.0 if total else 0.0
            lines.append(f"- {mode}: {cnt} 天（{pct:.1f}%）")

        return lines, meta

    def _build_group_stats_60(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        meta: Dict[str, Any] = {"window": 60, "modes": [], "fallbacks": []}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        groups: Dict[str, List[_HistRow]] = {}
        for r in rows60:
            k = r.mode or "UNKNOWN"
            groups.setdefault(k, []).append(r)
        meta["modes"] = sorted(list(groups.keys()))

        fb: List[str] = []
        if any(r.mode_src == "report_text" for r in rows60):
            fb.append("mode:report_text")
        if any(r.mode == "UNKNOWN" for r in rows60):
            fb.append("mode:UNKNOWN")
        if any(r.ret_src == "report_text" for r in rows60):
            fb.append("ret:report_text")
        if any(r.ret_pct is None for r in rows60):
            fb.append("ret:NA")
        meta["fallbacks"] = fb

        lines: List[str] = []

        def _order_key(m: str) -> Tuple[int, str]:
            return (-len(groups.get(m, [])), m)

        for mode in sorted(groups.keys(), key=_order_key):
            rs = groups[mode]
            total = len(rs)
            rets = [r.ret_pct for r in rs if r.ret_pct is not None]
            valid = len(rets)

            wins = sum(1 for x in rets if x > 0)
            win_pct = (wins / valid) * 100.0 if valid else None

            mean = (sum(rets) / valid) if valid else None
            if valid >= 2:
                mu = mean or 0.0
                var = sum((x - mu) ** 2 for x in rets) / valid
                std = var ** 0.5
            elif valid == 1:
                std = 0.0
            else:
                std = None

            contrib_sum = sum(rets) if valid else None

            if win_pct is None:
                lines.append(f"- **{mode}** | N={valid}/{total} | Win%=NA")
            else:
                lines.append(f"- **{mode}** | N={valid}/{total} | Win%={win_pct:.1f}%")
            lines.append(
                f"  - mean={_pct_fmt(mean)} | std={_pct_fmt(std)} | contrib(sum)={_pct_fmt(contrib_sum)}"
            )

        return lines, meta


    # ----------------------------
    # A) Mode Performance Monitor
    # ----------------------------
    def _build_mode_performance_monitor(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        """Read-only: show basic effectiveness/credibility of each mode from last 60 days."""
        meta: Dict[str, Any] = {"window": 60, "modes": []}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        groups: Dict[str, List[_HistRow]] = {}
        for r in rows60:
            k = r.mode or "UNKNOWN"
            groups.setdefault(k, []).append(r)

        meta["modes"] = sorted(list(groups.keys()))
        lines: List[str] = []
        lines.append("- 说明：本区块只做“制度有效性/可信度”统计，不参与任何 Gate/DRS/ActionHint 决策。")
        lines.append("- Sharpe_like=mean/std（按日，未年化；std=0 或 NA 时显示 NA）")

        def _confidence(n_valid: int) -> str:
            if n_valid >= 20:
                return "HIGH"
            if n_valid >= 10:
                return "MED"
            if n_valid >= 5:
                return "LOW"
            return "INSUFFICIENT"

        def _order_key(m: str) -> Tuple[int, str]:
            return (-len(groups.get(m, [])), m)

        rows_meta: List[Dict[str, Any]] = []
        for mode in sorted(groups.keys(), key=_order_key):
            rs = groups[mode]
            total = len(rs)
            rets = [r.ret_pct for r in rs if r.ret_pct is not None]
            valid = len(rets)

            win_pct: Optional[float]
            mean: Optional[float]
            std: Optional[float]
            sharpe_like: Optional[float]

            if valid:
                wins = sum(1 for x in rets if x > 0)
                win_pct = (wins / valid) * 100.0
                mean = sum(rets) / valid
                if valid >= 2:
                    mu = mean
                    var = sum((x - mu) ** 2 for x in rets) / valid
                    std = var ** 0.5
                else:
                    std = 0.0
                if std and std > 0:
                    sharpe_like = mean / std
                else:
                    sharpe_like = None
            else:
                win_pct = None
                mean = None
                std = None
                sharpe_like = None

            conf = _confidence(valid)
            rows_meta.append(
                {
                    "mode": mode,
                    "n_total": total,
                    "n_valid": valid,
                    "win_pct": win_pct,
                    "mean": mean,
                    "std": std,
                    "sharpe_like": sharpe_like,
                    "confidence": conf,
                }
            )

            if win_pct is None:
                lines.append(f"- **{mode}** | N={valid}/{total} | Win%=NA | mean=NA | std=NA | Sharpe_like=NA | conf={conf}")
            else:
                sl = "NA" if sharpe_like is None else f"{sharpe_like:.2f}"
                lines.append(
                    f"- **{mode}** | N={valid}/{total} | Win%={win_pct:.1f}% | mean={_pct_fmt(mean)} | std={_pct_fmt(std)} | Sharpe_like={sl} | conf={conf}"
                )

        meta["rows"] = rows_meta
        return lines, meta

    # ----------------------------
    # B) Mode Duration Monitor
    # ----------------------------
    def _build_mode_separation_check(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        """Read-only: check whether each mode has statistical separation vs overall baseline (effect size style)."""
        meta: Dict[str, Any] = {"window": 60, "baseline": {}, "rows": []}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        valid_all = [r.ret_pct for r in rows60 if r.ret_pct is not None]
        if not valid_all:
            return ["- （缺收益数据：近60日指数涨跌均缺失）"], meta

        mean_all = sum(valid_all) / len(valid_all)
        var_all = sum((x - mean_all) ** 2 for x in valid_all) / (len(valid_all) - 1) if len(valid_all) > 1 else 0.0
        std_all = var_all ** 0.5

        win_all = sum(1 for x in valid_all if x > 0) / len(valid_all) if valid_all else 0.0
        meta["baseline"] = {"n": len(valid_all), "mean": mean_all, "std": std_all, "win": win_all}

        groups: Dict[str, List[float]] = {}
        for r in rows60:
            k = r.mode or "UNKNOWN"
            if r.ret_pct is None:
                continue
            groups.setdefault(k, []).append(r.ret_pct)

        lines: List[str] = []
        lines.append("- 说明：只做“制度区分能力”监控：比较各 mode 的收益均值与全样本基线的差异（effect size），不参与任何制度决策。")
        lines.append("- 指标：delta=mode_mean-baseline_mean；d≈delta/pooled_std（简化 Cohen's d）；t_like≈delta/(mode_std/√n)。")
        lines.append(f"- 基线（近60日有效收益样本）：N={len(valid_all)} | mean={_pct_fmt(mean_all)} | std={_pct_fmt(std_all)} | Win%={win_all*100:.1f}%")

        def _confidence(n_valid: int) -> str:
            if n_valid >= 20:
                return "HIGH"
            if n_valid >= 10:
                return "MED"
            if n_valid >= 5:
                return "LOW"
            return "INSUFFICIENT"

        def _level(d: Optional[float], conf: str) -> str:
            if conf in ("INSUFFICIENT",):
                return "INSUFFICIENT"
            if d is None:
                return "WEAK"
            ad = abs(d)
            if conf in ("MED", "HIGH") and ad >= 0.5:
                return "STRONG"
            if ad >= 0.3:
                return "MODERATE"
            return "WEAK"

        rows_meta: List[Dict[str, Any]] = []
        # stable ordering: by sample size desc
        for mode in sorted(groups.keys(), key=lambda m: (-len(groups[m]), m)):
            xs = groups[mode]
            n = len(xs)
            mean_m = sum(xs) / n
            var_m = sum((x - mean_m) ** 2 for x in xs) / (n - 1) if n > 1 else 0.0
            std_m = var_m ** 0.5
            delta = mean_m - mean_all

            pooled = None
            d = None
            if std_all > 0 or std_m > 0:
                pooled = ((std_all ** 2 + std_m ** 2) / 2.0) ** 0.5
                if pooled and pooled > 0:
                    d = delta / pooled

            t_like = None
            if n >= 2 and std_m > 0:
                t_like = delta / (std_m / (n ** 0.5))

            win_m = sum(1 for x in xs if x > 0) / n if n else 0.0
            win_delta = win_m - win_all

            conf = _confidence(n)
            level = _level(d, conf)

            d_str = "NA" if d is None else f"{d:.2f}"
            t_str = "NA" if t_like is None else f"{t_like:.2f}"
            lines.append(
                f"- **{mode}** | N={n} | mean={_pct_fmt(mean_m)} | std={_pct_fmt(std_m)} | delta={_pct_fmt(delta)} | d={d_str} | t_like={t_str} | WinΔ={(win_delta*100):+.1f}% | level={level} | conf={conf}"
            )

            rows_meta.append(
                {
                    "mode": mode,
                    "n": n,
                    "mean": mean_m,
                    "std": std_m,
                    "delta": delta,
                    "d": d,
                    "t_like": t_like,
                    "win": win_m,
                    "win_delta": win_delta,
                    "level": level,
                    "conf": conf,
                }
            )

        meta["rows"] = rows_meta
        return lines, meta

    def _build_mode_duration_monitor(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        """Read-only: detect whether current mode streak is unusually long vs last 60d distribution."""
        meta: Dict[str, Any] = {"window": 60}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        cur_mode = rows60[0].mode or "UNKNOWN"
        cur_streak = 0
        for r in rows60:
            if (r.mode or "UNKNOWN") == cur_mode:
                cur_streak += 1
            else:
                break

        streaks_by_mode: Dict[str, List[int]] = {}
        i = 0
        n = len(rows60)
        while i < n:
            m = rows60[i].mode or "UNKNOWN"
            j = i
            while j < n and (rows60[j].mode or "UNKNOWN") == m:
                j += 1
            ln = j - i
            streaks_by_mode.setdefault(m, []).append(ln)
            i = j

        cur_hist = streaks_by_mode.get(cur_mode, [])
        meta["current_mode"] = cur_mode
        meta["current_streak"] = cur_streak
        meta["streaks_by_mode"] = streaks_by_mode

        def _pct(values: List[int], p: float) -> Optional[float]:
            if not values:
                return None
            vs = sorted(values)
            if len(vs) == 1:
                return float(vs[0])
            k = (len(vs) - 1) * p
            f = int(k)
            c = min(f + 1, len(vs) - 1)
            if f == c:
                return float(vs[f])
            return float(vs[f] + (vs[c] - vs[f]) * (k - f))

        p50 = _pct(cur_hist, 0.50)
        p80 = _pct(cur_hist, 0.80)
        p90 = _pct(cur_hist, 0.90)
        mx = max(cur_hist) if cur_hist else None

        meta.update({"p50": p50, "p80": p80, "p90": p90, "max": mx})

        level = "NORMAL"
        if p90 is not None and cur_streak >= max(7, int(round(p90))):
            level = "EXTREME"
        elif p80 is not None and cur_streak >= max(5, int(round(p80))):
            level = "ELEVATED"
        elif cur_streak >= 5:
            level = "ELEVATED"

        meta["level"] = level

        def _fmt(v: Optional[float]) -> str:
            return "NA" if v is None else f"{v:.0f}"

        lines: List[str] = []
        lines.append("- 说明：只做“模式持续天数异常”提醒，不参与任何制度决策。")
        lines.append(f"- 当前 mode：**{cur_mode}** | 当前连续：{cur_streak} 天 | level={level}")
        if mx is None:
            lines.append("- 近60日该 mode 的历史 streak 分布不足，无法计算分位数。")
        else:
            lines.append(f"- 近60日该 mode streak 统计：p50={_fmt(p50)} / p80={_fmt(p80)} / p90={_fmt(p90)} / max={_fmt(float(mx))}")
            lines.append("- 解读：ELEVATED/EXTREME 仅表示“持续时间异常偏长”。常见于去风险尾段或趋势单边阶段，提示提高节奏敏感度（非决策）。")

        return lines, meta



    def _build_mode_drs_cross_separation(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        """Read-only: cross separation check by (mode, drs_level) vs overall baseline."""
        meta: Dict[str, Any] = {"window": 60, "baseline": {}, "rows": []}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        valid_all = [r.ret_pct for r in rows60 if r.ret_pct is not None]
        if not valid_all:
            return ["- （缺收益数据：近60日指数涨跌均缺失）"], meta

        mean_all = sum(valid_all) / len(valid_all)
        var_all = sum((x - mean_all) ** 2 for x in valid_all) / (len(valid_all) - 1) if len(valid_all) > 1 else 0.0
        std_all = var_all ** 0.5
        win_all = sum(1 for x in valid_all if x > 0) / len(valid_all) if valid_all else 0.0
        meta["baseline"] = {"n": len(valid_all), "mean": mean_all, "std": std_all, "win": win_all}

        groups: Dict[Tuple[str, str], List[float]] = {}
        for r in rows60:
            if r.ret_pct is None:
                continue
            k = (r.mode or "UNKNOWN", r.drs_level or "UNKNOWN")
            groups.setdefault(k, []).append(r.ret_pct)

        def _conf(n: int) -> str:
            if n >= 20:
                return "HIGH"
            if n >= 10:
                return "MED"
            if n >= 5:
                return "LOW"
            return "INSUFFICIENT"

        lines: List[str] = []
        lines.append("- 说明：只做“制度×DRS 交叉区分能力”监控：比较各 (mode,DRS) 组合的收益均值与全样本基线的差异（effect size），不参与任何制度决策。")
        lines.append(f"- 基线（近60日有效收益样本）：N={len(valid_all)} | mean={_pct_fmt(mean_all)} | std={_pct_fmt(std_all)} | Win%={win_all*100:.1f}%")

        items = []
        for (mode, drs), xs in groups.items():
            n = len(xs)
            mean_g = sum(xs) / n
            var_g = sum((x - mean_g) ** 2 for x in xs) / (n - 1) if n > 1 else 0.0
            std_g = var_g ** 0.5
            win_g = sum(1 for x in xs if x > 0) / n if n else 0.0
            delta = mean_g - mean_all
            pooled = None
            if std_all > 0 and std_g > 0:
                pooled = ((std_all ** 2 + std_g ** 2) / 2.0) ** 0.5
            elif std_all > 0:
                pooled = std_all
            elif std_g > 0:
                pooled = std_g
            d = (delta / pooled) if (pooled and pooled > 0) else None
            t_like = (delta / (std_g / (n ** 0.5))) if (n > 1 and std_g > 0) else None
            conf = _conf(n)
            if conf == "INSUFFICIENT":
                level = "INSUFFICIENT"
            else:
                ad = abs(d) if d is not None else 0.0
                level = "STRONG" if ad >= 0.6 else ("MODERATE" if ad >= 0.3 else "WEAK")
            items.append((n, mode, drs, mean_g, std_g, delta, d, t_like, win_g, conf, level))

        items.sort(key=lambda x: (-x[0], x[1], x[2]))
        max_show = 12
        for it in items[:max_show]:
            n, mode, drs, mean_g, std_g, delta, d, t_like, win_g, conf, level = it
            d_str = f"{d:.2f}" if d is not None else "NA"
            t_str = f"{t_like:.2f}" if t_like is not None else "NA"
            lines.append(
                f"- **{mode} × {drs}** | N={n} | mean={_pct_fmt(mean_g)} | std={_pct_fmt(std_g)} | delta={_pct_fmt(delta)} | d={d_str} | t_like={t_str} | Win%={win_g*100:.1f}% | level={level} | conf={conf}"
            )
            meta["rows"].append(
                {"mode": mode, "drs": drs, "n": n, "mean": mean_g, "std": std_g, "delta": delta, "d": d, "t_like": t_like, "win": win_g, "conf": conf, "level": level}
            )

        if len(items) > max_show:
            lines.append(f"- …（其余 {len(items)-max_show} 个组合省略；样本不足时多为噪声）")
        return lines, meta

    def _build_mode_transition_matrix(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        """Read-only: transition matrix/statistics for market_mode."""
        meta: Dict[str, Any] = {"window": 60, "transitions": {}, "days": 0, "pairs": 0}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        # chronological order
        def _parse_td(s: str) -> Any:
            try:
                return datetime.datetime.strptime(s, "%Y-%m-%d").date()
            except Exception:
                return s

        seq = sorted(rows60, key=lambda r: _parse_td(r.trade_date))
        meta["days"] = len(seq)
        if len(seq) < 2:
            return ["- （历史天数不足：无法统计转换）"], meta

        mat: Dict[str, Dict[str, int]] = {}
        for a, b in zip(seq[:-1], seq[1:]):
            fm = a.mode or "UNKNOWN"
            tm = b.mode or "UNKNOWN"
            mat.setdefault(fm, {}).setdefault(tm, 0)
            mat[fm][tm] += 1

        meta["transitions"] = mat
        meta["pairs"] = sum(sum(v.values()) for v in mat.values())

        # summarize by each from-mode top3
        lines: List[str] = []
        lines.append("- 说明：只做“制度转换惯性”统计：统计 mode 在相邻交易日之间的转移概率，不参与任何制度决策。")
        lines.append(f"- 样本：days={len(seq)} | transitions={meta['pairs']}（相邻日对数）")

        # determine main modes by presence in seq
        counts: Dict[str, int] = {}
        for r in seq:
            counts[r.mode or "UNKNOWN"] = counts.get(r.mode or "UNKNOWN", 0) + 1
        main_modes = [k for k, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))][:8]

        for fm in main_modes:
            row = mat.get(fm, {})
            total = sum(row.values())
            if total <= 0:
                continue
            tops = sorted(row.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
            parts = []
            for tm, c in tops:
                p = c / total if total else 0.0
                parts.append(f"{tm}:{c}({p*100:.1f}%)")
            lines.append(f"- **{fm}** → " + " | ".join(parts) + f" | row_total={total}")
        return lines, meta

    def _build_mode_separation_stability(self, rows60: List[_HistRow]) -> Tuple[List[str], Dict[str, Any]]:
        """Read-only: rolling stability of separation effect size (recent windows)."""
        meta: Dict[str, Any] = {"windows": [20, 40, 60], "rows": []}
        if not rows60:
            return ["- （缺历史数据：近60日为空）"], meta

        # recent-first windows based on rows60 ordering (DESC by trade_date)
        def _calc_d(window_rows: List[_HistRow], mode: str) -> Tuple[Optional[float], int, int]:
            valid_all = [r.ret_pct for r in window_rows if r.ret_pct is not None]
            if len(valid_all) < 3:
                return None, len(valid_all), 0
            mean_all = sum(valid_all) / len(valid_all)
            var_all = sum((x - mean_all) ** 2 for x in valid_all) / (len(valid_all) - 1) if len(valid_all) > 1 else 0.0
            std_all = var_all ** 0.5
            xs = [r.ret_pct for r in window_rows if (r.ret_pct is not None and (r.mode or "UNKNOWN") == mode)]
            if len(xs) < 2 or std_all <= 0:
                return None, len(valid_all), len(xs)
            mean_g = sum(xs) / len(xs)
            delta = mean_g - mean_all
            return delta / std_all, len(valid_all), len(xs)

        # modes present
        counts: Dict[str, int] = {}
        for r in rows60:
            counts[r.mode or "UNKNOWN"] = counts.get(r.mode or "UNKNOWN", 0) + 1
        modes = [k for k, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))][:8]

        lines: List[str] = []
        lines.append("- 说明：只做“区分能力稳定性”监控：比较各 mode 在不同近期窗口下的 effect size(d) 是否稳定/漂移，不参与任何制度决策。")
        lines.append("- d_w 口径：d≈(mode_mean - baseline_mean)/baseline_std（简化），窗口为最近 W 日（最大 60）。")

        for mode in modes:
            d20, n20, nm20 = _calc_d(rows60[:20], mode)
            d40, n40, nm40 = _calc_d(rows60[:40], mode)
            d60, n60, nm60 = _calc_d(rows60[:60], mode)

            def _fmt(v: Optional[float]) -> str:
                return "NA" if v is None else f"{v:.2f}"

            drift = "NA"
            if d20 is not None and d60 is not None:
                if (d20 == 0) or (d60 == 0):
                    drift = "NO"
                else:
                    sign_change = (d20 > 0) != (d60 > 0)
                    big_shift = abs(d20 - d60) >= 0.5
                    drift = "YES" if (sign_change or big_shift) else "NO"

            conf = "INSUFFICIENT"
            if nm60 >= 20:
                conf = "HIGH"
            elif nm60 >= 10:
                conf = "MED"
            elif nm60 >= 5:
                conf = "LOW"

            lines.append(
                f"- **{mode}** | d20={_fmt(d20)}(n_mode={nm20}) | d40={_fmt(d40)}(n_mode={nm40}) | d60={_fmt(d60)}(n_mode={nm60}) | drift={drift} | conf={conf}"
            )
            meta["rows"].append(
                {"mode": mode, "d20": d20, "d40": d40, "d60": d60, "n_mode_20": nm20, "n_mode_40": nm40, "n_mode_60": nm60, "drift": drift, "conf": conf}
            )

        return lines, meta
