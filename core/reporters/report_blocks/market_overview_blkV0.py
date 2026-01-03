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
                        "note": "注：MarketOverview 只读展示；缺失时不影响 Gate / ActionHint。",
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
                    "note": "注：以上为收盘事实的读数汇总，用于理解盘面，不构成建议。",
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
                    "note": "注：异常已记录日志；本 block 不影响其它 block 生成。",
                },
            )

    # ----------------------------
    # data pick / fallback
    # ----------------------------
    def _pick_src(self, *, context: ReportContext, doc_partial: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
        slots = context.slots if isinstance(context.slots, dict) else {}

        src: Dict[str, Any] = {}

        # prefer slot (if exists) but still allow filling missing fields from other sources
        slot_v = slots.get("market_overview")
        if isinstance(slot_v, dict) and slot_v:
            src.update(slot_v)


        # 1) indices from index_tech factor raw_data
        idx = self._extract_indices_from_index_tech(slots.get("factors"), warnings=warnings)
        if "indices" not in src and idx:
            src["indices"] = idx

        # 2) amount + top20 ratio + breadth from etf_spot_sync slot
        etf = slots.get("etf_spot_sync")
        if isinstance(etf, dict) and etf:
            # breadth could be nested or flat
            br = etf.get("breadth") if isinstance(etf.get("breadth"), dict) else None
            if not br:
                br = {k: etf.get(k) for k in ["adv", "dec", "flat", "limit_up", "limit_down", "adv_ratio"] if k in etf}
            if br:
                src["breadth"] = br

            amt = etf.get("amount") if isinstance(etf.get("amount"), dict) else None
            if "amount" not in src and amt:
                src["amount"] = amt

            if "top20_amount_ratio" not in src and "top20_amount_ratio" in etf:
                src["top20_amount_ratio"] = etf.get("top20_amount_ratio")

            if "feeling" not in src and "feeling" in etf and isinstance(etf.get("feeling"), str):
                src["feeling"] = etf.get("feeling")

        # 2b) breadth (adv/dec/limit) from snapshot.market_sentiment (EOD best-effort)
        # NOTE: market_sentiment is a "facts" block (counts), not a structure semantic.
        if "breadth" not in src:
            ms = slots.get("market_sentiment")
            if isinstance(ms, dict) and ms:
                br = {k: ms.get(k) for k in ["adv", "dec", "flat", "limit_up", "limit_down", "adv_ratio"] if k in ms}
                if "breadth" not in src and br:
                    src["breadth"] = br


        # 3) amount fallback from amount factor raw_data
        if "amount" not in src:
            a = self._extract_amount_from_amount_factor(slots.get("factors"), warnings=warnings)
            if a:
                src["amount"] = a

        return src

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
        raw = details.get("_raw_data")

        delta = None
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = None
        if isinstance(raw, dict):
            window = raw.get("window")
            if isinstance(window, list) and len(window) >= 2:
                # assume window sorted by date asc or desc; we try detect by comparing trade_date
                try:
                    w0 = window[0]
                    w1 = window[1]
                    t0 = str(w0.get("trade_date", ""))
                    t1 = str(w1.get("trade_date", ""))
                    # If t0 < t1 then ascending, latest is last
                    if t0 and t1 and t0 < t1:
                        latest = window[-1].get("total_amount")
                        prev = window[-2].get("total_amount")
                    else:
                        latest = w0.get("total_amount")
                        prev = w1.get("total_amount")
                    if isinstance(latest, (int, float)) and isinstance(prev, (int, float)):
                        total = total if isinstance(total, (int, float)) else latest
                        delta = float(latest) - float(prev)
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
            parts.append(f"{key.upper()} {pct:.2f}% 收 {close:.2f}")

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

        # top20 ratio
        top20 = src.get("top20_amount_ratio")
        top20_pct = self._to_pct(top20)

        # If top20 missing, try from etf_spot_sync factor details (if any)
        if top20_pct is None:
            etf = context.slots.get("etf_spot_sync") if isinstance(context.slots, dict) else None
            if isinstance(etf, dict):
                top20_pct = self._to_pct(etf.get("top20_amount_ratio"))

        s = f"**成交额：**{total:.2f}{unit}"
        if delta is not None:
            s += f"（较前一日 {delta:+.2f}{unit}）"
        if top20_pct is not None:
            s += f"；Top20 成交占比 {top20_pct:.1f}%"
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
        if top20_ratio is not None and top20_ratio >= 65:
            return "**一句话体感：**成交高度集中（窄领涨/拥挤），追价与热点轮动的胜率偏低。"
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
