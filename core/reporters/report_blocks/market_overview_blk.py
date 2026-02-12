# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Market Overview block (Frozen)

目标：
- 输出“收盘事实”的大盘概述：指数表现、成交与集中度、赚钱效应（上涨/下跌/涨停/跌停）
- **不依赖** slots['market_overview']（可能为空），自动从 factors / etf_spot_sync 兜底
- 不展示真实北向净流入（系统当前只有北向代理压力），避免 north_net 之类未定义字段

注意：这是解释层 block，不参与决策；所有异常必须被捕获并转为 warnings。
"""

from __future__ import annotations

import json
import os
from typing import Dict, Any, List, Optional, Tuple

import yaml

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger


log = get_logger(__name__)


class MarketOverviewBlock(ReportBlockRendererBase):
    block_alias = "market.overview"
    title = "大盘概述（收盘事实）"

    # NOTE: keep constructor minimal; block instances may be created dynamically.
    # Logger is module-level (log). Do not rely on instance attributes like self._logger.

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        try:
            src = self._pick_src(context=context, doc_partial=doc_partial, warnings=warnings)
            if not src:
                warnings.append("empty:market_overview")
                return ReportBlock(
                    block_alias=self.block_alias,
                    title=self.title,
                    warnings=warnings,
                    payload={
                        "content": [
                            "（未提供市场概述数据，且无法从 factors/etf_spot_sync 兜底读取）",
                        ],
                        #"note": "注：MarketOverview 只读展示；缺失时不影响 Gate / ActionHint。",
                    },
                )

            lines: List[str] = []

            idx_line = self._fmt_indices(context=context, src=src, warnings=warnings)
            if idx_line:
                lines.append(idx_line)

            amt_line, top20_ratio = self._fmt_amount(context=context, src=src, warnings=warnings)
            if amt_line:
                lines.append(amt_line)

            br_line, adv_ratio = self._fmt_breadth(src=src, warnings=warnings)
            if br_line:
                lines.append(br_line)

            proxy_line = self._fmt_north_proxy(context=context, warnings=warnings)
            if proxy_line:
                lines.append(proxy_line)

            feel_line = self._fmt_feeling(adv_ratio=adv_ratio, top20_ratio=top20_ratio)
            if feel_line:
                lines.append(feel_line)

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": lines,
                    #"note": "注：以上为收盘事实的读数汇总，用于理解盘面，不构成建议。",
                },
            )

        except Exception as e:
            log.exception("MarketOverviewBlock.render failed: %s", e)
            warnings.append("exception:market_overview_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": [
                        "市场概述渲染异常（已捕获）。",
                    ],
                    #"note": "注：异常已记录日志；本 block 不影响其它 block 生成。",
                },
            )

    # ----------------------------
    # data pick / fallback
    # ----------------------------
    def _pick_src(self, *, context: ReportContext, doc_partial: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        slots = context.slots if isinstance(context.slots, dict) else {}

        # prefer slot (if exists) — but ONLY if it is "complete enough".
        # Regression guard (Frozen): a partially-populated market_overview (e.g. only amount_ma20_ratio)
        # must NOT block fallback assembly, otherwise the block will emit missing:indices/amount/breadth.
        slot_v = slots.get("market_overview")
        use_slot_direct = False
        if isinstance(slot_v, dict) and slot_v:
            for k in ("indices", "amount", "breadth"):
                if k in slot_v and slot_v.get(k):
                    use_slot_direct = True
                    break

        if use_slot_direct:
            return slot_v

        # If slot exists but is partial, keep its extra fields as a base, then fill missing pieces via fallback.
        src: Dict[str, Any] = dict(slot_v) if isinstance(slot_v, dict) and slot_v else {}
        if isinstance(slot_v, dict) and slot_v:
            warnings.append("compat:market_overview_partial_slot_fallback")

        factors = slots.get("factors")

        # 1) indices from index_tech factor raw_data
        idx = self._extract_indices_from_index_tech(factors, warnings=warnings)
        if idx:
            src["indices"] = idx

        # 2) amount from amount factor raw_data
        a = self._extract_amount_from_amount_factor(factors, warnings=warnings)
        if a:
            src["amount"] = a

        # 3) breadth counts from market sentiment factors (preferred)
        br = self._extract_breadth_from_unified_emotion(factors, warnings=warnings)
        if not br:
            br = self._extract_breadth_from_participation(factors, warnings=warnings)
        if br:
            src["breadth"] = br
        # 4) Top20 concentration (strict semantics):
        #   Top20 成交集中度 = 全市场Top20个股成交额 / 全市场成交额
        # Prefer liquidity_quality.details.top20_ratio (Panel F + SQL-verified).
        top20_ratio = self._extract_top20_ratio_from_liquidity_quality(factors, warnings=warnings)
        if top20_ratio is not None:
            src["top20_ratio"] = top20_ratio
            src["top20_ratio_src"] = "liquidity_quality.details.top20_ratio"
        else:
            # Legacy proxy (different denominator): keep ONLY for compatibility & evidence, never as Top20 ratio.
            proxy, proxy_src = self._extract_top20_proxy_amount_ratio(factors)
            if proxy is not None:
                src["top20_amount_ratio_proxy"] = proxy
                src["top20_proxy_src"] = proxy_src
                if proxy >= 0.25:
                    warnings.append(
                        f"suspect:top20_amount_ratio_high_from:{proxy_src}={proxy:.4f} "
                        "(denominator differs; prefer liquidity_quality.top20_ratio)"
                    )
                else:
                    warnings.append(f"compat:top20_amount_ratio_from:{proxy_src}")

        # 5) extra fallback from etf_spot_sync slot (may carry feeling/top20/breadth)
        etf = slots.get("etf_spot_sync")
        if isinstance(etf, dict) and etf:
            if "breadth" not in src:
                br2 = etf.get("breadth") if isinstance(etf.get("breadth"), dict) else None
                if not br2:
                    det = etf.get("details") if isinstance(etf.get("details"), dict) else {}
                    br2 = det.get("breadth") if isinstance(det.get("breadth"), dict) else None
                    if not br2:
                        br2 = {k: det.get(k) for k in ["adv", "dec", "flat", "limit_up", "limit_down", "adv_ratio"] if k in det}
                if br2:
                    src["breadth"] = br2

            if "top20_amount_ratio" not in src and "top20_amount_ratio" in etf:
                src["top20_amount_ratio"] = etf.get("top20_amount_ratio")

            if "feeling" in etf and isinstance(etf.get("feeling"), str):
                src["feeling"] = etf.get("feeling")

        return src
    def _extract_top20_ratio_from_liquidity_quality(
        self,
        factors: Any,
        warnings: Optional[List[str]] = None,
    ) -> Optional[float]:
        """
        Strict semantics:
        Top20 成交集中度 = 全市场Top20个股成交额 / 全市场成交额。

        Source of truth:
        - liquidity_quality.details.top20_ratio (aligns with Panel F & Oracle SQL verification)

        Returns:
        - ratio in [0,1] if available, else None.
        """
        if warnings is None:
            warnings = []
        if not isinstance(factors, dict):
            return None
        fr = factors.get("liquidity_quality")
        if not isinstance(fr, dict):
            return None
        details = fr.get("details") if isinstance(fr.get("details"), dict) else {}
        v = details.get("top20_ratio")
        return float(v) if isinstance(v, (int, float)) else None

    def _extract_top20_proxy_amount_ratio(self, factors: Any) -> Tuple[Optional[float], str]:
        """
        Legacy proxy (different denominator; DO NOT interpret as Top20 ratio):
        - crowding_concentration.details.top20_amount_ratio
        - etf_spot_sync.details.top20_amount_ratio

        Returns:
        - (value, source_key) where source_key is the factor name that provided the proxy.
        """
        if not isinstance(factors, dict):
            return None, "-"
        for k in ("crowding_concentration", "etf_spot_sync"):
            fr = factors.get(k)
            if not isinstance(fr, dict):
                continue
            details = fr.get("details") if isinstance(fr.get("details"), dict) else {}
            v = details.get("top20_amount_ratio")
            if isinstance(v, (int, float)):
                return float(v), k
        return None, "-"



    def _extract_breadth_from_unified_emotion(self, factors: Any, warnings: List[str]) -> Optional[Dict[str, Any]]:
        """Extract breadth/market-sentiment counts from unified_emotion.details._raw_data.market_sentiment.

        Expected payload (example):
          details._raw_data = {"market_sentiment": {"adv":..., "dec":..., "flat":..., "limit_up":..., "limit_down":..., "adv_ratio":...}}
        """
        if not isinstance(factors, dict):
            return None
        ue = factors.get("unified_emotion")
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

    def _extract_breadth_from_participation(self, factors: Any, warnings: List[str]) -> Optional[Dict[str, Any]]:
        """Fallback breadth extraction from participation.details._raw_data (adv/dec/flat/adv_ratio)."""
        if not isinstance(factors, dict):
            return None
        p = factors.get("participation")
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

    def _extract_indices_from_index_tech(self, factors: Any, warnings: List[str]) -> Dict[str, Any]:
        if not isinstance(factors, dict):
            warnings.append("missing:factors")
            return {}

        idx_tech = factors.get("index_tech")
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

        # raw format: {sh:{close,pct_1d}, ... , _meta:{indices:[...]}}
        indices = {}
        for k, v in raw.items():
            if k == "_meta":
                continue
            if isinstance(v, dict):
                indices[k] = v
        return indices

    def _extract_amount_from_amount_factor(self, factors: Any, warnings: List[str]) -> Optional[Dict[str, Any]]:
        if not isinstance(factors, dict):
            return None
        a = factors.get("amount")
        if not isinstance(a, dict):
            return None

        details = a.get("details") if isinstance(a.get("details"), dict) else {}
        total = details.get("amount_total")

        # Prefer pre-computed vs-prev fields from AmountFactor (robust, avoids _raw_data truncation issues)
        delta = details.get("amount_delta_prev")
        if delta is None:
            # backward-compat: try derive from raw window if _raw_data is still valid JSON
            raw = details.get("_raw_data")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = None
            if isinstance(raw, dict):
                window = raw.get("window")
                if isinstance(window, list) and len(window) >= 2:
                    try:
                        # window may be latest-first (preferred) or oldest-first; sort by trade_date desc
                        rows = []
                        for r in window:
                            if isinstance(r, dict):
                                td = r.get("trade_date") or r.get("date") or r.get("dt")
                                tv = r.get("total_amount")
                                if td and isinstance(tv, (int, float)):
                                    rows.append((str(td), float(tv)))
                        if len(rows) >= 2:
                            rows.sort(key=lambda x: x[0], reverse=True)
                            delta = float(rows[0][1]) - float(rows[1][1])
                    except Exception:
                        pass

        if not isinstance(total, (int, float)):
            return None

        out = {"amount": float(total), "unit": "亿元"}
        if isinstance(delta, (int, float)):
            out["delta"] = float(delta)
        return out

    # ----------------------------
    # formatting
    # ----------------------------
    def _fmt_indices(self, *, context: ReportContext, src: Dict[str, Any], warnings: List[str]) -> Optional[str]:
        indices = src.get("indices")
        if not isinstance(indices, dict) or not indices:
            warnings.append("missing:indices")
            return None

        order = self._load_indices_order(context=context, warnings=warnings)
        if not order:
            # fallback to meta if exists
            meta = indices.get("_meta") if isinstance(indices.get("_meta"), dict) else {}
            order = meta.get("indices") if isinstance(meta.get("indices"), list) else list(indices.keys())

        parts: List[str] = []
        for key in order:
            if key not in indices:
                continue
            v = indices.get(key)
            if not isinstance(v, dict):
                continue
            close = v.get("close")
            pct = v.get("pct_1d") if "pct_1d" in v else v.get("chg_pct")
            if not isinstance(close, (int, float)) or not isinstance(pct, (int, float)):
                continue

            # pct may be stored either as ratio (0.0093 == 0.93%) or already-percent (0.93).
            # Use close/prev_close to disambiguate when possible (e.g., 0.9145 must be 0.9145%, not 91.45%).
            prev_close = v.get("prev_close")
            pct_pct = self._to_pct_return(pct_raw=pct, close=close, prev_close=prev_close)
            if pct_pct is None:
                continue
            parts.append(f"{key.upper()} {pct_pct:.2f}% 收 {close:.2f}")

        if not parts:
            warnings.append("missing:indices_values")
            return None

        return "**指数表现：**" + " / ".join(parts) + "。"

    def _fmt_amount(self, *, context: ReportContext, src: Dict[str, Any], warnings: List[str]) -> Tuple[Optional[str], Optional[float]]:
        amt = src.get("amount")
        total, delta, unit = self._extract_value_delta(amt)

        # fallback: if amount exists but not parsed, warn
        if total is None:
            warnings.append("missing:amount")
            return None, None

        # Top20 concentration (strict) + legacy proxy
        top20_ratio = src.get("top20_ratio")
        top20_pct = self._to_pct(top20_ratio)
        top20_src = src.get("top20_ratio_src")

        proxy = src.get("top20_amount_ratio_proxy")
        proxy_pct = self._to_pct(proxy)
        proxy_src = src.get("top20_proxy_src")

        # If missing, try proxy from etf_spot_sync slot (legacy)
        if top20_pct is None and proxy_pct is None:
            etf = context.slots.get("etf_spot_sync") if isinstance(context.slots, dict) else None
            if isinstance(etf, dict):
                proxy = etf.get("top20_amount_ratio")
                proxy_pct = self._to_pct(proxy)
                proxy_src = "etf_spot_sync"

        s = f"**成交额：**{total:.2f}{unit}"
        if delta is not None:
            s += f"（较前一日 {delta:+.2f}{unit}）"
        if top20_pct is not None:
            s += f"；Top20 成交集中度(top20_ratio) {top20_pct:.1f}% (src={top20_src})"
        elif proxy_pct is not None:
            s += f"；拥挤代理(top20_amount_ratio) {proxy_pct:.1f}% (src={proxy_src}, denom!=top20_ratio)"
        s += "。"
        return s, top20_pct

    def _fmt_breadth(self, *, src: Dict[str, Any], warnings: List[str]) -> Tuple[Optional[str], Optional[float]]:
        br = src.get("breadth")
        if not isinstance(br, dict):
            warnings.append("missing:breadth")
            return None, None

        adv = br.get("adv")
        dec = br.get("dec")
        flat = br.get("flat")
        limit_up = br.get("limit_up")
        limit_down = br.get("limit_down")
        adv_ratio = br.get("adv_ratio")

        adv_i = int(adv) if isinstance(adv, (int, float)) else None
        dec_i = int(dec) if isinstance(dec, (int, float)) else None
        flat_i = int(flat) if isinstance(flat, (int, float)) else None
        lu_i = int(limit_up) if isinstance(limit_up, (int, float)) else None
        ld_i = int(limit_down) if isinstance(limit_down, (int, float)) else None

        adv_ratio_pct = self._to_pct(adv_ratio)

        parts: List[str] = []
        if adv_i is not None and dec_i is not None and flat_i is not None:
            parts.append(f"上涨 {adv_i} 家 / 下跌 {dec_i} 家 / 平盘 {flat_i} 家")
        if adv_ratio_pct is not None:
            parts.append(f"上涨占比 {adv_ratio_pct:.2f}%")
        if lu_i is not None and ld_i is not None:
            parts.append(f"涨停 {lu_i} / 跌停 {ld_i}")

        if not parts:
            warnings.append("missing:breadth_values")
            return None, adv_ratio_pct

        return "**赚钱效应：**" + "；".join(parts) + "。", adv_ratio_pct

    def _fmt_north_proxy(self, *, context: ReportContext, warnings: List[str]) -> Optional[str]:
        # Current system only provides proxy pressure, not real north_net.
        structure = context.slots.get("structure") if isinstance(context.slots, dict) else None
        if not isinstance(structure, dict):
            return None
        npp = structure.get("north_proxy_pressure")
        if not isinstance(npp, dict):
            return None
        state = npp.get("state")
        ev = npp.get("evidence") if isinstance(npp.get("evidence"), dict) else {}
        score = ev.get("pressure_score")
        level = ev.get("pressure_level") or state
        if score is None and level is None:
            return None
        if isinstance(score, (int, float)):
            return f"**北向代理：**压力 {level}（score {float(score):.1f}）。"
        return f"**北向代理：**压力 {level}。"

    def _fmt_feeling(self, *, adv_ratio: Optional[float], top20_ratio: Optional[float]) -> Optional[str]:
        # prefer explicit feeling if exists
        # (do not force warnings here; feeling is optional)
        # adv_ratio/top20_ratio are already % numbers
        if top20_ratio is not None and top20_ratio >= 12:
            return "**一句话体感：**成交集中度偏高（窄领涨/拥挤），追价与热点轮动的胜率偏低。"
        if adv_ratio is not None and 45 <= adv_ratio <= 55:
            return "**一句话体感：**多空均衡、轮动偏快，按制度更适合“观望/小仓试错”而非追价。"
        if adv_ratio is not None and adv_ratio < 40:
            return "**一句话体感：**下跌家数占优，情绪偏弱，优先控制回撤与执行摩擦。"
        if adv_ratio is not None and adv_ratio > 60:
            return "**一句话体感：**上涨扩散较好，但仍需看成交与集中度是否支持持续性。"
        return None

    # ----------------------------
    # helpers
    # ----------------------------
    def _load_indices_order(self, *, context: ReportContext, warnings: List[str]) -> List[str]:
        # configurable via governance.config.symbols_path; fallback toJM: config/symbols.yaml
        cfg = context.slots.get("governance", {}).get("config", {}) if isinstance(context.slots, dict) else {}
        path = cfg.get("symbols_path") if isinstance(cfg, dict) else None
        candidates = []
        if isinstance(path, str) and path.strip():
            candidates.append(path.strip())
        candidates.extend(["config/symbols.yaml", "symbols.yaml"])

        sym = None
        for p in candidates:
            sym = self._try_load_yaml(p)
            if isinstance(sym, dict):
                break
        if not isinstance(sym, dict):
            warnings.append("missing:symbols_yaml")
            return []

        idx_core = sym.get("index_core")
        if isinstance(idx_core, dict) and idx_core:
            return list(idx_core.keys())

        idx = sym.get("indices")
        if isinstance(idx, list):
            return [str(x) for x in idx if isinstance(x, str)]
        return []

    def _try_load_yaml(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            if not path:
                return None
            # allow relative path
            p = path
            if not os.path.isabs(p):
                p = os.path.join(os.getcwd(), p)
            if not os.path.exists(p):
                return None
            with open(p, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except Exception:
            return None

    def _extract_value_delta(self, v: Any) -> Tuple[Optional[float], Optional[float], str]:
        unit = "亿元"
        if isinstance(v, dict):
            # common keys
            amount = v.get("amount")
            if amount is None:
                amount = v.get("amount_total")
            delta = v.get("delta")
            if isinstance(v.get("unit"), str):
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
            # treat 0~1 as ratio, others as already percent
            if 0 <= x <= 1:
                return x * 100.0
            return x
        return None

    def _to_pct_return(self, *, pct_raw: Any, close: Any, prev_close: Any) -> Optional[float]:
        """Convert daily return to percent with disambiguation.

        Some upstream sources store daily change as:
        - ratio: 0.0091 means +0.91%
        - percent: 0.91 means +0.91%

        For small values (0~1) these overlap. If prev_close is available, we compute implied_pct
        and pick the interpretation that matches implied_pct better.
        """
        if not isinstance(pct_raw, (int, float)):
            return None

        x = float(pct_raw)

        # If prev_close is available, use it to infer the correct unit
        if isinstance(close, (int, float)) and isinstance(prev_close, (int, float)) and prev_close not in (0, 0.0):
            try:
                implied_pct = (float(close) / float(prev_close) - 1.0) * 100.0
                as_percent = x                  # interpret x as already-percent
                as_ratio_pct = x * 100.0        # interpret x as ratio then convert to percent
                # Choose whichever is closer to implied_pct
                if abs(as_percent - implied_pct) <= abs(as_ratio_pct - implied_pct):
                    return as_percent
                return as_ratio_pct
            except Exception:
                pass

        # Fallback: heuristic
        # Values in [-1, 1] are most often ratios, but to avoid 0.9 -> 90% type explosions,
        # treat unusually large ratios as already-percent.
        if -1.0 <= x <= 1.0:
            if abs(x) >= 0.4:  # 40% daily ratio is impossible for A-share indices; must be already-percent
                return x
            return x * 100.0
        return x
