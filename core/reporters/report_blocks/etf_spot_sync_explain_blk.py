# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class EtfSpotSyncExplainBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · ETF Spot Sync Explain（核心字段解释）

    设计原则：
    - 只读 context / doc_partial，不抛异常
    - 优先 slots["etf_spot_sync"]，允许从 intraday_overlay / observations 兜底读取
    """

    block_alias = "etf_spot_sync.explain"
    title = "核心字段解释（拥挤/不同步/参与度）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        src = self._pick_src(context, doc_partial)

        if not isinstance(src, dict) or not src:
            warnings.append("empty:etf_spot_sync")
            payload = "（未提供 etf_spot_sync / intraday_overlay 数据：该区块仅用于占位）"
            return ReportBlock(self.block_alias, self.title, payload=payload, warnings=warnings)

        # Many FactorResult wrappers use {"details": {...}, "score":..., "level":...}
        details: Dict[str, Any]
        score = None
        level = None

        if "details" in src and isinstance(src.get("details"), dict):
            details = src.get("details")  # type: ignore[assignment]
            score = src.get("score")
            level = src.get("level")
        else:
            details = src

        interp = details.get("interpretation") if isinstance(details.get("interpretation"), dict) else {}
        lines: List[str] = []

        snap = details.get("snapshot_type")
        if isinstance(snap, str) and snap:
            lines.append(f"- snapshot_type: **{snap}**")

        # Key fields (raw)
        adv_ratio = details.get("adv_ratio")
        top20 = details.get("top20_turnover_ratio")
        dispersion = details.get("dispersion")
        same_dir = details.get("same_direction")

        if isinstance(adv_ratio, (int, float)):
            lines.append(f"- adv_ratio: **{adv_ratio:.4f}**")
        if isinstance(top20, (int, float)):
            lines.append(f"- top20_turnover_ratio: **{top20:.3f}**")
        if isinstance(dispersion, (int, float)):
            lines.append(f"- dispersion: **{dispersion:.4f}**")
        if isinstance(same_dir, bool):
            lines.append(f"- same_direction: **{same_dir}**")

        # Interpretation explanation (requested semantics)
        if interp:
            crowding = interp.get("crowding")
            direction = interp.get("direction")
            participation = interp.get("participation")
            divergence = interp.get("divergence")
            disp = interp.get("dispersion")

            if crowding:
                lines.append(f"\n**crowding: {crowding}**")
                if str(crowding).lower() in ("high", "very_high"):
                    lines.append("拥挤度高：资金集中在少数热点/少数票里，容易出现冲高回落、轮动很快、追涨胜率下降。")
                else:
                    lines.append("拥挤度不高：资金集中度可控，追价摩擦相对更低（仍需看 Gate/Execution）。")

            if direction:
                lines.append(f"\n**direction: {direction}**")
                if str(direction).lower() in ("diverged", "diverge", "mixed"):
                    lines.append("方向不一致/不同步：ETF/代理与对照在方向上不一致，容易造成执行摩擦上升（买在错误一侧/买到没跟上的那边）。")
                else:
                    lines.append("方向一致：ETF/代理与对照同步性更好（仍需结合拥挤度与参与度）。")

            if participation:
                lines.append(f"\n**participation: {participation}**")
                if str(participation).lower() in ("weak", "low"):
                    lines.append("参与度弱：涨跌扩散不足，常见于“指数能稳住，但多数个股没跟上”的盘面，进攻成功率偏低。")
                else:
                    lines.append("参与度不弱：扩散较好，若 Gate/Execution 允许，进攻胜率更可期。")

            if disp:
                lines.append(f"\n**dispersion: {disp}**")
                lines.append("分化描述：用于判断是“温和分化/轮动”还是“撕裂式失衡”。")

            if divergence:
                lines.append(f"\n**divergence: {divergence}**")
                lines.append("偏离幅度：用于区分“方向不同步但幅度不大”与“剧烈背离”。")

        # Factor header (optional)
        if score is not None or level is not None:
            lines.append("\n（Factor）")
            if level is not None:
                lines.append(f"- level: **{level}**")
            if score is not None:
                lines.append(f"- score: **{score}**")

        payload = "\n".join(lines).strip()
        return ReportBlock(self.block_alias, self.title, payload=payload, warnings=warnings)

    def _pick_src(self, context: ReportContext, doc_partial: Dict[str, Any]) -> Dict[str, Any]:
        # 1) direct slot
        v = context.slots.get("etf_spot_sync")
        if isinstance(v, dict) and v:
            return v

        # 2) intraday overlay container (common pattern: overlay["etf_spot_sync"] / ["etf_index_sync"])
        overlay = context.slots.get("intraday_overlay") or context.slots.get("intraday") or context.slots.get("overlay")
        if isinstance(overlay, dict) and overlay:
            for k in ("etf_spot_sync", "etf_index_sync", "etf_index_sync_daily", "etf_spot_sync_raw"):
                vv = overlay.get(k)
                if isinstance(vv, dict) and vv:
                    return vv
            # if overlay itself is the details dict
            if "interpretation" in overlay or "top20_turnover_ratio" in overlay:
                return overlay

        # 3) doc_partial
        v = doc_partial.get("etf_spot_sync")
        if isinstance(v, dict) and v:
            return v

        # 4) observations fallback
        obs = context.slots.get("observations")
        if isinstance(obs, dict) and obs:
            for k in ("etf_spot_sync", "etf_index_sync", "intraday_overlay", "overlay"):
                vv = obs.get(k)
                if isinstance(vv, dict) and vv:
                    return vv

        return {}
