# -*- coding: utf-8 -*-
"""UnifiedRisk V12 - Market Overview block (read-only)."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

import yaml

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger


log = get_logger(__name__)


class MarketOverviewBlock(ReportBlockRendererBase):
    block_alias = "market.overview"
    title = "大盘概览（收盘事实）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        try:
            src = self._pick_src(context=context, warnings=warnings)
            if not src:
                warnings.append("empty:market_overview")
                return ReportBlock(
                    block_alias=self.block_alias,
                    title=self.title,
                    warnings=warnings,
                    payload={"content": ["（未提供市场概览数据，且无法从 factors/etf_spot_sync 兜底读取）"]},
                )

            lines: List[str] = []

            idx_line = self._fmt_indices(context=context, src=src, warnings=warnings)
            if idx_line:
                lines.append(idx_line)

            close_struct_line = self._fmt_close_structure(context=context, src=src)
            if close_struct_line:
                lines.append(close_struct_line)

            amt_line, top20_ratio = self._fmt_amount(context=context, src=src, warnings=warnings)
            if amt_line:
                lines.append(amt_line)

            amt_trend_line = self._fmt_amount_trend(context=context)
            if amt_trend_line:
                lines.append(amt_trend_line)

            br_line, adv_ratio = self._fmt_breadth(src=src, warnings=warnings)
            if br_line:
                lines.append(br_line)

            proxy_line = self._fmt_north_proxy(context=context)
            if proxy_line:
                lines.append(proxy_line)

            feel_line = self._fmt_feeling(adv_ratio=adv_ratio, top20_ratio=top20_ratio)
            if feel_line:
                lines.append(feel_line)

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={"content": lines},
            )

        except Exception as e:
            log.exception("MarketOverviewBlock.render failed: %s", e)
            warnings.append("exception:market_overview_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={"content": ["市场概览渲染异常（已捕获）。"]},
            )

    def _pick_src(self, *, context: ReportContext, warnings: List[str]) -> Dict[str, Any]:
        slots = context.slots if isinstance(context.slots, dict) else {}
        slot_v = slots.get("market_overview") if isinstance(slots.get("market_overview"), dict) else {}

        # use direct slot when complete enough
        if any(slot_v.get(k) for k in ("indices", "amount", "breadth")):
            return dict(slot_v)

        src: Dict[str, Any] = dict(slot_v) if slot_v else {}
        factors = slots.get("factors") if isinstance(slots.get("factors"), dict) else {}

        idx = self._extract_indices_from_index_tech(factors, warnings)
        if idx:
            src["indices"] = idx

        amt = self._extract_amount_from_amount_factor(factors, warnings)
        if amt:
            src["amount"] = amt

        br = self._extract_breadth_from_unified_emotion(factors, warnings)
        if not br:
            br = self._extract_breadth_from_participation(factors, warnings)
        if br:
            src["breadth"] = br

        top20 = self._extract_top20_ratio_from_liquidity_quality(factors)
        if top20 is not None:
            src["top20_ratio"] = top20
            src["top20_ratio_src"] = "liquidity_quality.details.top20_ratio"
        else:
            proxy, src_key = self._extract_top20_proxy_amount_ratio(factors)
            if proxy is not None:
                src["top20_amount_ratio_proxy"] = proxy
                src["top20_proxy_src"] = src_key

        etf = slots.get("etf_spot_sync")
        if isinstance(etf, dict):
            if "breadth" not in src and isinstance(etf.get("breadth"), dict):
                src["breadth"] = etf.get("breadth")
            if "top20_amount_ratio_proxy" not in src and isinstance(etf.get("top20_amount_ratio"), (int, float)):
                src["top20_amount_ratio_proxy"] = etf.get("top20_amount_ratio")
                src["top20_proxy_src"] = "etf_spot_sync"

        return src

    def _extract_indices_from_index_tech(self, factors: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        idx_tech = factors.get("index_tech") if isinstance(factors, dict) else None
        if not isinstance(idx_tech, dict):
            return {}
        details = idx_tech.get("details") if isinstance(idx_tech.get("details"), dict) else {}
        raw = details.get("_raw_data")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                warnings.append("invalid:index_tech_raw_data_json")
                return {}
        if not isinstance(raw, dict):
            return {}
        out: Dict[str, Any] = {}
        for k, v in raw.items():
            if k == "_meta":
                continue
            if isinstance(v, dict):
                out[k] = v
        return out

    def _extract_amount_from_amount_factor(self, factors: Dict[str, Any], warnings: List[str]) -> Optional[Dict[str, Any]]:
        a = factors.get("amount") if isinstance(factors, dict) else None
        if not isinstance(a, dict):
            return None
        details = a.get("details") if isinstance(a.get("details"), dict) else {}
        total = details.get("amount_total")
        delta = details.get("amount_delta_prev")
        if not isinstance(total, (int, float)):
            return None
        out = {"amount": float(total), "unit": "亿元"}
        if isinstance(delta, (int, float)):
            out["delta"] = float(delta)
        return out

    def _extract_breadth_from_unified_emotion(self, factors: Dict[str, Any], warnings: List[str]) -> Optional[Dict[str, Any]]:
        ue = factors.get("unified_emotion") if isinstance(factors, dict) else None
        if not isinstance(ue, dict):
            return None
        details = ue.get("details") if isinstance(ue.get("details"), dict) else {}
        raw = details.get("_raw_data")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                warnings.append("invalid:unified_emotion_raw_data_json")
                return None
        if not isinstance(raw, dict):
            return None
        ms = raw.get("market_sentiment") if isinstance(raw.get("market_sentiment"), dict) else None
        if not isinstance(ms, dict):
            return None
        out = {k: ms.get(k) for k in ["adv", "dec", "flat", "limit_up", "limit_down", "adv_ratio"] if k in ms}
        return out if out else None

    def _extract_breadth_from_participation(self, factors: Dict[str, Any], warnings: List[str]) -> Optional[Dict[str, Any]]:
        p = factors.get("participation") if isinstance(factors, dict) else None
        if not isinstance(p, dict):
            return None
        details = p.get("details") if isinstance(p.get("details"), dict) else {}
        raw = details.get("_raw_data")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                warnings.append("invalid:participation_raw_data_json")
                return None
        if not isinstance(raw, dict):
            return None
        out = {k: raw.get(k) for k in ["adv", "dec", "flat", "adv_ratio"] if k in raw}
        return out if out else None

    def _extract_top20_ratio_from_liquidity_quality(self, factors: Dict[str, Any]) -> Optional[float]:
        fr = factors.get("liquidity_quality") if isinstance(factors, dict) else None
        if not isinstance(fr, dict):
            return None
        details = fr.get("details") if isinstance(fr.get("details"), dict) else {}
        v = details.get("top20_ratio")
        return float(v) if isinstance(v, (int, float)) else None

    def _extract_top20_proxy_amount_ratio(self, factors: Dict[str, Any]) -> Tuple[Optional[float], str]:
        for k in ("crowding_concentration", "etf_spot_sync"):
            fr = factors.get(k) if isinstance(factors, dict) else None
            if not isinstance(fr, dict):
                continue
            details = fr.get("details") if isinstance(fr.get("details"), dict) else {}
            v = details.get("top20_amount_ratio")
            if isinstance(v, (int, float)):
                return float(v), k
        return None, "-"

    def _fmt_indices(self, *, context: ReportContext, src: Dict[str, Any], warnings: List[str]) -> Optional[str]:
        indices = src.get("indices")
        if not isinstance(indices, dict) or not indices:
            warnings.append("missing:indices")
            return None

        order = self._load_indices_order(context=context) or list(indices.keys())
        parts: List[str] = []
        for key in order:
            v = indices.get(key)
            if not isinstance(v, dict):
                continue
            close = v.get("close")
            pct = v.get("pct_1d") if "pct_1d" in v else v.get("chg_pct")
            if not isinstance(close, (int, float)) or not isinstance(pct, (int, float)):
                continue
            prev_close = v.get("prev_close")
            pct_pct = self._to_pct_return(pct_raw=pct, close=close, prev_close=prev_close)
            if pct_pct is None:
                continue
            parts.append(f"{key.upper()} {pct_pct:.2f}% 收{close:.2f}")

        if not parts:
            warnings.append("missing:indices_values")
            return None
        return "**指数表现：**" + " / ".join(parts) + "。"

    def _fmt_close_structure(self, *, context: ReportContext, src: Dict[str, Any]) -> Optional[str]:
        overlay = context.slots.get("intraday_overlay") if isinstance(context.slots, dict) else None
        if isinstance(overlay, dict):
            am = overlay.get("am_move")
            pm = overlay.get("pm_move")
            cp = overlay.get("close_pressure")
            if any(isinstance(x, (int, float, str)) for x in (am, pm, cp)):
                bits: List[str] = []
                if isinstance(am, (int, float)):
                    bits.append(f"am_move={float(am):+.2f}%")
                elif isinstance(am, str) and am.strip():
                    bits.append(f"am_move={am}")
                if isinstance(pm, (int, float)):
                    bits.append(f"pm_move={float(pm):+.2f}%")
                elif isinstance(pm, str) and pm.strip():
                    bits.append(f"pm_move={pm}")
                if isinstance(cp, (int, float)):
                    bits.append(f"close_pressure={float(cp):+.2f}%")
                elif isinstance(cp, str) and cp.strip():
                    bits.append(f"close_pressure={cp}")
                if bits:
                    return "**盘面分段：**" + " / ".join(bits)

        indices = src.get("indices") if isinstance(src.get("indices"), dict) else {}
        if not indices:
            return None

        strong = weak = mixed = samples = 0
        for _, v in indices.items():
            if not isinstance(v, dict):
                continue
            close = v.get("close")
            ma5 = v.get("ma5")
            ma10 = v.get("ma10")
            if not (isinstance(close, (int, float)) and isinstance(ma5, (int, float)) and isinstance(ma10, (int, float))):
                continue
            samples += 1
            c, m5, m10 = float(close), float(ma5), float(ma10)
            if c >= m5 and m5 >= m10:
                strong += 1
            elif c < m5 and m5 < m10:
                weak += 1
            else:
                mixed += 1

        if samples <= 0:
            return None
        return f"**收盘结构代理：** strong={strong} / weak={weak} / mixed={mixed}（close vs MA5/MA10）"

    def _fmt_amount(self, *, context: ReportContext, src: Dict[str, Any], warnings: List[str]) -> Tuple[Optional[str], Optional[float]]:
        amt = src.get("amount")
        total, delta, unit = self._extract_value_delta(amt)
        if total is None:
            warnings.append("missing:amount")
            return None, None

        top20_ratio = src.get("top20_ratio")
        top20_pct = self._to_pct(top20_ratio)
        top20_src = src.get("top20_ratio_src")

        proxy = src.get("top20_amount_ratio_proxy")
        proxy_pct = self._to_pct(proxy)
        proxy_src = src.get("top20_proxy_src")

        s = f"**成交额：**{total:.2f}{unit}"
        if delta is not None:
            s += f"（较前一日{delta:+.2f}{unit}）"
        if top20_pct is not None:
            s += f"；Top20成交集中度(top20_ratio) {top20_pct:.1f}% (src={top20_src})"
        elif proxy_pct is not None:
            s += f"；拥挤代理(top20_amount_ratio) {proxy_pct:.1f}% (src={proxy_src}, denom!=top20_ratio)"
        s += "。"
        return s, top20_pct

    def _fmt_amount_trend(self, *, context: ReportContext) -> Optional[str]:
        factors = context.slots.get("factors") if isinstance(context.slots, dict) else None
        if not isinstance(factors, dict):
            return None
        amount = factors.get("amount")
        if not isinstance(amount, dict):
            return None
        details = amount.get("details") if isinstance(amount.get("details"), dict) else {}

        signal = details.get("amount_trend_signal")
        slope3 = details.get("amount_ratio_slope_3d")
        ratio20 = details.get("amount_ratio")
        ratio60 = details.get("amount_ratio_ma60")
        if signal is None and slope3 is None and ratio20 is None and ratio60 is None:
            return None

        bits: List[str] = []
        if isinstance(signal, str) and signal.strip():
            bits.append(f"signal={signal}")
        if isinstance(ratio20, (int, float)):
            bits.append(f"ratio_ma20={float(ratio20):.3f}")
        if isinstance(ratio60, (int, float)):
            bits.append(f"ratio_ma60={float(ratio60):.3f}")
        if isinstance(slope3, (int, float)):
            bits.append(f"slope_3d={float(slope3):+.3f}")
        if not bits:
            return None
        return "**量能趋势(增强)：**" + " / ".join(bits)

    def _fmt_breadth(self, *, src: Dict[str, Any], warnings: List[str]) -> Tuple[Optional[str], Optional[float]]:
        br = src.get("breadth")
        if not isinstance(br, dict):
            warnings.append("missing:breadth")
            return None, None

        adv = int(br.get("adv")) if isinstance(br.get("adv"), (int, float)) else None
        dec = int(br.get("dec")) if isinstance(br.get("dec"), (int, float)) else None
        flat = int(br.get("flat")) if isinstance(br.get("flat"), (int, float)) else None
        lu = int(br.get("limit_up")) if isinstance(br.get("limit_up"), (int, float)) else None
        ld = int(br.get("limit_down")) if isinstance(br.get("limit_down"), (int, float)) else None
        adv_ratio_pct = self._to_pct(br.get("adv_ratio"))

        parts: List[str] = []
        if adv is not None and dec is not None and flat is not None:
            parts.append(f"上涨 {adv} / 下跌 {dec} / 平盘 {flat}")
        if adv_ratio_pct is not None:
            parts.append(f"上涨占比 {adv_ratio_pct:.2f}%")
        if lu is not None and ld is not None:
            parts.append(f"涨停 {lu} / 跌停 {ld}")
        if not parts:
            warnings.append("missing:breadth_values")
            return None, adv_ratio_pct
        return "**赚钱效应：**" + "；".join(parts) + "。", adv_ratio_pct

    def _fmt_north_proxy(self, *, context: ReportContext) -> Optional[str]:
        structure = context.slots.get("structure") if isinstance(context.slots, dict) else None
        if not isinstance(structure, dict):
            return None
        npp = structure.get("north_proxy_pressure")
        if not isinstance(npp, dict):
            return None
        ev = npp.get("evidence") if isinstance(npp.get("evidence"), dict) else {}
        level = ev.get("pressure_level") or npp.get("state")
        score = ev.get("pressure_score")
        if level is None and score is None:
            return None
        if isinstance(score, (int, float)):
            return f"**北向代理：**压力 {level}（score {float(score):.1f}）。"
        return f"**北向代理：**压力 {level}。"

    def _fmt_feeling(self, *, adv_ratio: Optional[float], top20_ratio: Optional[float]) -> Optional[str]:
        if top20_ratio is not None and top20_ratio >= 12:
            return "**一句话体感：**成交集中偏高，追价与轮动胜率偏低。"
        if adv_ratio is not None and 45 <= adv_ratio <= 55:
            return "**一句话体感：**多空均衡、轮动偏快，更适合观察或小仓位。"
        if adv_ratio is not None and adv_ratio < 40:
            return "**一句话体感：**下跌家数占优，优先控制回撤与执行摩擦。"
        if adv_ratio is not None and adv_ratio > 60:
            return "**一句话体感：**上涨扩散较好，但仍需量能与集中度验证。"
        return None

    def _load_indices_order(self, *, context: ReportContext) -> List[str]:
        cfg = context.slots.get("governance", {}).get("config", {}) if isinstance(context.slots, dict) else {}
        path = cfg.get("symbols_path") if isinstance(cfg, dict) else None
        candidates: List[str] = []
        if isinstance(path, str) and path.strip():
            candidates.append(path.strip())
        candidates.extend(["config/symbols.yaml", "symbols.yaml"])

        for p in candidates:
            doc = self._try_load_yaml(p)
            if not isinstance(doc, dict):
                continue
            idx_core = doc.get("index_core")
            if isinstance(idx_core, dict) and idx_core:
                return [str(k) for k in idx_core.keys()]
            idx = doc.get("indices")
            if isinstance(idx, list):
                return [str(x) for x in idx if isinstance(x, str)]
        return []

    def _try_load_yaml(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            p = path if os.path.isabs(path) else os.path.join(os.getcwd(), path)
            if not os.path.exists(p):
                return None
            with open(p, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _extract_value_delta(self, v: Any) -> Tuple[Optional[float], Optional[float], str]:
        unit = "亿元"
        if isinstance(v, dict):
            amount = v.get("amount") if v.get("amount") is not None else v.get("amount_total")
            delta = v.get("delta")
            if isinstance(v.get("unit"), str) and v.get("unit").strip():
                unit = v.get("unit")
            amount_f = float(amount) if isinstance(amount, (int, float)) else None
            delta_f = float(delta) if isinstance(delta, (int, float)) else None
            return amount_f, delta_f, unit
        if isinstance(v, (int, float)):
            return float(v), None, unit
        return None, None, unit

    def _to_pct(self, v: Any) -> Optional[float]:
        if isinstance(v, (int, float)):
            x = float(v)
            if 0 <= x <= 1:
                return x * 100.0
            return x
        return None

    def _to_pct_return(self, *, pct_raw: Any, close: Any, prev_close: Any) -> Optional[float]:
        if not isinstance(pct_raw, (int, float)):
            return None
        x = float(pct_raw)

        if isinstance(close, (int, float)) and isinstance(prev_close, (int, float)) and prev_close not in (0, 0.0):
            try:
                implied_pct = (float(close) / float(prev_close) - 1.0) * 100.0
                as_percent = x
                as_ratio_pct = x * 100.0
                return as_percent if abs(as_percent - implied_pct) <= abs(as_ratio_pct - implied_pct) else as_ratio_pct
            except Exception:
                pass

        if -1.0 <= x <= 1.0:
            if abs(x) >= 0.4:
                return x
            return x * 100.0
        return x
