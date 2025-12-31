# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class MarketOverviewBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Market Overview（收盘事实）

    设计原则：
    - 只读 context / doc_partial，不抛异常、不返回 None
    - 优先使用 slots["market_overview"]，但允许从 observations / doc_partial 兜底取数
    - 不编造事实：缺字段就明确缺失（warning），而不是“猜”
    """

    block_alias = "market.overview"
    title = "大盘概述（收盘事实）"

    # -------------------------
    # entry
    # -------------------------
    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        src = self._pick_src(context, doc_partial)

        if not isinstance(src, dict) or not src:
            warnings.append("empty:market_overview")
            payload = (
                "（未提供大盘收盘事实数据：market_overview slot 为空或无法兜底读取）\n"
                "建议：在 Snapshot/DS 层补齐指数/量能/赚钱效应/资金流字段。"
            )
            return ReportBlock(self.block_alias, self.title, payload=payload, warnings=warnings)

        # normalize sub-sections
        indices = src.get("indices") if isinstance(src.get("indices"), dict) else {}
        turnover = src.get("turnover") if isinstance(src.get("turnover"), dict) else {}
        breadth = src.get("breadth") if isinstance(src.get("breadth"), dict) else {}
        fundflow = src.get("fundflow") if isinstance(src.get("fundflow"), dict) else {}
        feeling = src.get("feeling")

        lines: List[str] = []

        # 1) 指数
        idx_line = self._fmt_indices(indices, warnings)
        if idx_line:
            lines.append(idx_line)
        else:
            warnings.append("missing:indices")

        # 2) 量能
        t_line = self._fmt_turnover(turnover, warnings)
        if t_line:
            lines.append(t_line)

        # 3) 赚钱效应 / 扩散
        b_line = self._fmt_breadth(breadth, warnings)
        if b_line:
            lines.append(b_line)

        # 4) 资金流
        f_line = self._fmt_fundflow(fundflow, warnings)
        if f_line:
            lines.append(f_line)

        # 5) 一句话体感
        if isinstance(feeling, str) and feeling.strip():
            lines.append(f"**一句话体感：**{feeling.strip()}")
        else:
            # 尽力拼“中性体感”，不写具体数值
            adv = breadth.get("adv_ratio")
            top20 = turnover.get("top20_turnover_ratio") or breadth.get("top20_turnover_ratio")
            if isinstance(adv, (int, float)) and adv <= 0.42:
                lines.append("**一句话体感：**指数可能较稳，但涨跌扩散偏弱（涨少跌多）——盘面更像调仓轮动，而非全面风险偏好抬升。")
            elif isinstance(top20, (int, float)) and top20 >= 0.70:
                lines.append("**一句话体感：**成交高度集中（窄领涨/拥挤），追价与热点轮动的胜率偏低。")
            else:
                lines.append("**一句话体感：**盘面偏结构性分化；需结合制度（Gate/Execution）判断是否适合进攻性执行。")
            warnings.append("missing:feeling")

        payload = "\n".join(lines).strip()
        return ReportBlock(self.block_alias, self.title, payload=payload, warnings=warnings)

    # -------------------------
    # source picking / normalization
    # -------------------------
    def _pick_src(self, context: ReportContext, doc_partial: Dict[str, Any]) -> Dict[str, Any]:
        """
        取数优先级（只读，不改写 context）：
        1) slots["market_overview" | "market_close_facts" | "close_facts"]
        2) doc_partial["market_overview"]
        3) observations["market_overview" | "market" | "close_facts" | "market_close_facts"]
        4) 从 observations 中拼装（indices/turnover/breadth/fundflow）
        """
        # 1) direct slots
        for k in ("market_overview", "market_close_facts", "close_facts"):
            v = context.slots.get(k)
            if isinstance(v, dict) and v:
                return v

        # 2) doc_partial
        v = doc_partial.get("market_overview")
        if isinstance(v, dict) and v:
            return v

        # 3) observations direct
        obs = context.slots.get("observations")
        if isinstance(obs, dict) and obs:
            for k in ("market_overview", "market", "close_facts", "market_close_facts"):
                vv = obs.get(k)
                if isinstance(vv, dict) and vv:
                    # allow nested market dict directly as src if it already has sub-sections
                    if any(x in vv for x in ("indices", "turnover", "breadth", "fundflow", "feeling")):
                        return vv
                    # or wrap it
                    return {"_raw": vv}

            # 4) assemble from observations keys
            assembled: Dict[str, Any] = {}
            # indices candidates
            idx = obs.get("indices") or obs.get("index") or obs.get("index_facts")
            if isinstance(idx, dict) and idx:
                assembled["indices"] = idx

            # turnover candidates
            t = obs.get("turnover") or obs.get("amount") or obs.get("liquidity")
            if isinstance(t, dict) and t:
                assembled["turnover"] = t

            # breadth candidates
            b = obs.get("breadth") or obs.get("adv_dec") or obs.get("market_breadth")
            if isinstance(b, dict) and b:
                assembled["breadth"] = b

            # fundflow candidates
            ff = obs.get("fundflow") or obs.get("money_flow") or obs.get("main_fundflow")
            if isinstance(ff, dict) and ff:
                assembled["fundflow"] = ff

            if assembled:
                return assembled

        return {}

    # -------------------------
    # formatters
    # -------------------------
    def _fmt_indices(self, indices: Dict[str, Any], warnings: List[str]) -> Optional[str]:
        if not indices:
            return None

        # accept various keys; keep order stable
        keys = [
            ("sh", "沪指"),
            ("sz", "深成指"),
            ("cyb", "创业板"),
            ("kcb50", "科创50"),
            ("hs300", "沪深300"),
        ]

        parts: List[str] = []
        for k, label in keys:
            v = indices.get(k)
            if isinstance(v, dict):
                parts.append(self._fmt_one_index(label, v))
            elif isinstance(v, (int, float)):
                # v is pct only
                parts.append(f"{label} {self._fmt_pct(v)}")
        # fallback: if indices dict is already {name: {...}}
        if not parts:
            for name, v in indices.items():
                if isinstance(v, dict):
                    parts.append(self._fmt_one_index(str(name), v))
        if not parts:
            warnings.append("missing:indices_fields")
            return None
        return "**指数：**" + "；".join(parts) + "。"

    def _fmt_one_index(self, label: str, v: Dict[str, Any]) -> str:
        pct = v.get("pct")
        close = v.get("close")
        s = f"{label} {self._fmt_pct(pct)}"
        if isinstance(close, (int, float)):
            s += f" 收 {self._fmt_close(close)}"
        return s

    def _fmt_turnover(self, turnover: Dict[str, Any], warnings: List[str]) -> Optional[str]:
        if not turnover:
            return None

        # common fields
        amount = turnover.get("amount") or turnover.get("total_amount") or turnover.get("amt")
        delta = turnover.get("delta") or turnover.get("chg") or turnover.get("amount_delta")
        unit = turnover.get("unit") or "亿元"

        # top20 concentration (optional)
        top20 = turnover.get("top20_turnover_ratio")
        top20_part = ""
        if isinstance(top20, (int, float)):
            top20_part = f"；Top20 成交占比 {top20:.3f}"

        if isinstance(amount, (int, float)):
            line = f"**量能：**两市成交额约 {self._fmt_num(amount)}{unit}"
            if isinstance(delta, (int, float)):
                # delta sign: + / -
                sign = "+" if delta >= 0 else ""
                line += f"，较上一交易日 {'放量' if delta >= 0 else '缩量'}约 {sign}{self._fmt_num(abs(delta))}{unit}"
            line += f"。{top20_part}"
            return line

        # if only ratio exists
        if top20_part:
            warnings.append("missing:turnover_amount")
            return f"**量能：**（成交额缺失）{top20_part.lstrip('；')}。"

        warnings.append("missing:turnover")
        return None

    def _fmt_breadth(self, breadth: Dict[str, Any], warnings: List[str]) -> Optional[str]:
        if not breadth:
            return None
        up = breadth.get("up") or breadth.get("adv") or breadth.get("adv_count")
        down = breadth.get("down") or breadth.get("dec") or breadth.get("dec_count")
        median = breadth.get("median") or breadth.get("median_ret") or breadth.get("median_pct")
        adv_ratio = breadth.get("adv_ratio")

        parts: List[str] = []
        if isinstance(up, (int, float)) and isinstance(down, (int, float)):
            parts.append(f"上涨约 {int(up)} 家，下跌约 {int(down)} 家")
        if isinstance(median, (int, float)):
            parts.append(f"中位数 {self._fmt_pct(median)}")
        if isinstance(adv_ratio, (int, float)) and not parts:
            parts.append(f"上涨占比 {adv_ratio:.4f}")
        if not parts:
            warnings.append("missing:breadth_fields")
            return None
        return "**赚钱效应：**" + "；".join(parts) + "。"

    def _fmt_fundflow(self, fundflow: Dict[str, Any], warnings: List[str]) -> Optional[str]:
        if not fundflow:
            return None

        # accept common names
        main = fundflow.get("main_net") or fundflow.get("main_net_inflow") or fundflow.get("main")
        north = fundflow.get("north_net") or fundflow.get("northbound") or fundflow.get("north")

        unit = fundflow.get("unit") or "亿元"

        parts: List[str] = []
        if isinstance(main, (int, float)):
            sign = "+" if main >= 0 else ""
            parts.append(f"主力净{'流入' if main >= 0 else '流出'} {sign}{self._fmt_num(abs(main))}{unit}")
        if isinstance(north, (int, float)):
            sign = "+" if north >= 0 else ""
            parts.append(f"北向净{'买入' if north >= 0 else '卖出'} {sign}{self._fmt_num(abs(north))}{unit}")

        if not parts:
            warnings.append("missing:fundflow_fields")
            return None
        return "**资金面：**" + "；".join(parts) + "。"

    # -------------------------
    # helpers
    # -------------------------
    def _fmt_pct(self, v: Any) -> str:
        if not isinstance(v, (int, float)):
            return ""
        # v may be in [-1,1] or already in percentage
        if abs(v) <= 1.5:
            return f"{v * 100:.2f}%"
        return f"{v:.2f}%"

    def _fmt_close(self, v: Any) -> str:
        if not isinstance(v, (int, float)):
            return ""
        return f"{v:.2f}"

    def _fmt_num(self, v: Any) -> str:
        if not isinstance(v, (int, float)):
            return ""
        # keep 2 decimals for large
        if abs(v) >= 100:
            return f"{v:.0f}"
        return f"{v:.2f}"
