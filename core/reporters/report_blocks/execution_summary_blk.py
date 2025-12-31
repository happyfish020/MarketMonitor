# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class ExecutionSummaryBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Execution Summary Block（冻结版 · 单值口径 v3）

    v3 changes:
    - 对用户可见文本：只输出 execution_band（A/N/D/D1/D2），永不展示 legacy code 字段。
    - 若 band 与 legacy code 不一致：静默忽略（不放 warnings，避免在报告里出现“⚠️ note:execution_code_diff”）。
      审计需要请在上游统一口径（band=唯一真值）。
    - 输入缺失/格式异常不崩溃：warnings + 占位说明。
    """

    block_alias = "execution.summary"
    title = "执行层评估（Execution · 2–5D）"

    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []
        execu = context.slots.get("execution_summary")

        band = self._extract_band(execu, warnings)

        if not band:
            warnings.append("missing:execution_band")
            payload = "未生成 Execution Summary（不影响制度裁决）。"
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        lines: List[str] = []
        lines.append(f"执行评级：{band}")

        # Minimal, non-actionable semantics (still “只解释不决策”)
        if band in ("D1", "D2", "D"):
            lines.append("含义：执行摩擦偏高/胜率下降阶段，追价与进攻性调仓的成功率偏低。")
            lines.append(
                "定义：执行摩擦偏高 = 信号在纸面上可做，但真实成交与事后效果更容易变差（滑点、追价回撤、轮动踩空），"
                "导致实际胜率往往低于表观胜率。"
            )
            # evidence summary (best-effort, no guesses)
            ev = self._extract_friction_evidence(context)
            if ev:
                lines.append("常见触发证据（当日观测）：")
                for s in ev:
                    lines.append(f"- {s}")
            else:
                lines.append("常见触发证据：未接入/不可用（可在 etf_spot_sync / market_overview slot 补齐相关字段）。")
        elif band == "N":
            lines.append("含义：执行环境中性，需严格服从 Gate/DRS 的制度约束。")
        else:  # A
            lines.append("含义：执行环境较顺，但仍需在 Gate 放行前提下行动。")

        lines.append(
            "制度说明：Execution 仅评估在 Gate 允许前提下的执行摩擦，"
            "不构成任何新增、调整或进攻行为的依据。"
        )
        lines.append(
            "若盘中/短期出现波动或滑点风险，应优先遵守 Gate/DRS 的制度约束，"
            "Execution 不得被用作“放行进攻”的理由。"
        )

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines),
            warnings=warnings,
        )


    # -----------------------------
    # Evidence (best-effort)
    # -----------------------------
    def _extract_friction_evidence(self, context: ReportContext) -> List[str]:
        """
        从已接入的 slots 中提取“执行摩擦”证据（只读，不推断、不编造）。

        当前支持：
        - etf_spot_sync（或 intraday_overlay 内嵌 etf_index_sync）：adv_ratio / top20_turnover_ratio / same_direction / interpretation.*
        - market_overview：breadth.up/down/adv_ratio、turnover.top20_turnover_ratio

        返回为“可读短句”，用于报告解释。
        """
        details = self._extract_etf_sync_details(context)
        interp = details.get("interpretation") if isinstance(details.get("interpretation"), dict) else {}

        adv_ratio = self._pick_number(
            details.get("adv_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("breadth", "adv_ratio")), None),
        )
        top20 = self._pick_number(
            details.get("top20_turnover_ratio"),
            self._pick_number(self._get_nested(context.slots.get("market_overview"), ("turnover", "top20_turnover_ratio")), None),
        )
        same_direction = details.get("same_direction")
        crowding = str(interp.get("crowding", "")).lower()
        direction = str(interp.get("direction", "")).lower()
        participation = str(interp.get("participation", "")).lower()

        out: List[str] = []

        # participation / breadth
        if participation in ("weak", "low"):
            out.append("参与度弱（participation=weak）：涨跌扩散不足，指数稳住但多数个股没跟上。")
        elif isinstance(adv_ratio, (int, float)) and adv_ratio <= 0.42:
            out.append(f"上涨占比偏低（adv_ratio={adv_ratio:.4f}）：扩散弱，进攻胜率偏低。")

        # crowding / concentration
        if crowding in ("high", "very_high"):
            out.append("拥挤度高（crowding=high）：资金集中在少数热点/少数票里，轮动快、追涨胜率下降。")
        elif isinstance(top20, (int, float)) and top20 >= 0.70:
            out.append(f"成交高度集中（top20_turnover_ratio={top20:.3f}）：窄领涨特征明显，追价更容易踩空。")

        # divergence / sync
        if direction in ("diverged", "mixed", "diverge"):
            out.append("方向不同步（direction=diverged）：ETF/代理与对照方向不一致，容易买在错误一侧。")
        elif same_direction is False:
            out.append("不同步（same_direction=false）：同步性下降，执行摩擦上升。")

        return out

    def _extract_etf_sync_details(self, context: ReportContext) -> Dict[str, Any]:
        """
        提取 etf_spot_sync / etf_index_sync 的 details（best-effort）。
        允许数据放在：
        - slots["etf_spot_sync"]
        - slots["intraday_overlay"]["etf_spot_sync"/"etf_index_sync"]
        - slots["observations"][...]
        """
        # direct
        v = context.slots.get("etf_spot_sync")
        d = self._unwrap_details(v)
        if d:
            return d

        # overlay container
        overlay = context.slots.get("intraday_overlay") or context.slots.get("overlay") or context.slots.get("intraday")
        if isinstance(overlay, dict):
            for k in ("etf_spot_sync", "etf_index_sync", "etf_index_sync_daily"):
                vv = overlay.get(k)
                dd = self._unwrap_details(vv)
                if dd:
                    return dd
            # overlay itself may be details dict
            dd = self._unwrap_details(overlay)
            if dd:
                return dd

        # observations
        obs = context.slots.get("observations")
        if isinstance(obs, dict):
            for k in ("etf_spot_sync", "etf_index_sync", "intraday_overlay", "overlay"):
                vv = obs.get(k)
                dd = self._unwrap_details(vv)
                if dd:
                    return dd

        return {}

    @staticmethod
    def _unwrap_details(obj: Any) -> Dict[str, Any]:
        if not isinstance(obj, dict) or not obj:
            return {}
        if isinstance(obj.get("details"), dict):
            return obj.get("details")  # type: ignore[return-value]
        # already details-like
        keys = ("adv_ratio", "top20_turnover_ratio", "interpretation", "same_direction")
        if any(k in obj for k in keys):
            return obj
        return {}

    @staticmethod
    def _pick_number(*vals: Any) -> Optional[float]:
        for v in vals:
            if isinstance(v, (int, float)):
                return float(v)
        return None

    @staticmethod
    def _get_nested(obj: Any, path: Tuple[str, ...]) -> Optional[Any]:
        cur = obj
        for k in path:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur

    # -----------------------------
    # Helpers
    # -----------------------------
    @staticmethod
    def _get_field(obj: Any, key: str) -> Optional[Any]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def _extract_band(self, execu: Any, warnings: List[str]) -> Optional[str]:
        band = self._get_field(execu, "band")
        if band is None:
            band = self._get_field(execu, "code")

        if band is None:
            return None

        try:
            s = str(band).strip().upper().replace(" ", "")
        except Exception:
            warnings.append("invalid:execution_band_type")
            return None

        # forbid combined strings
        if "/" in s:
            warnings.append("invalid:execution_band_combined")
            s = s.split("/", 1)[0].strip().upper().replace(" ", "")

        allowed = {"A", "N", "D", "D1", "D2"}
        if s not in allowed:
            warnings.append("invalid:execution_band_value")
            return s  # frozen: show something rather than blank

        return s
