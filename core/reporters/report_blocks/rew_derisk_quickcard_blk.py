# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · P0-36 · REW→De-risk 执行速查卡（冻结版）

目标：
- 只读 slots（governance.rew / governance.execution / governance.gate / governance.drs）
- 输出“REW→De-risk 执行速查卡”，把 REW × Scope × Execution 映射为去风险执行建议

冻结铁律：
- slot 缺失 ≠ 错误：返回 warnings + 占位 payload（可审计）
- 永不抛异常 / 不返回 None
- 不写入 slots / 不影响 GateDecision / DRS / 全市场评分
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger


log = get_logger(__name__)


class RewDeriskQuickcardBlock(ReportBlockRendererBase):
    """P0-36: REW→De-risk 执行速查卡（只读）。"""

    block_alias = "rew.derisk_quickcard"
    title = "REW→De-risk 执行速查卡（只读）"

    # Frozen schema version (for report_dump / replay)
    _SCHEMA_VERSION = "P0-36.REW_DERISK_QUICKCARD.V1"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []
        try:
            slots = context.slots if isinstance(context.slots, dict) else {}
            gov = slots.get("governance") if isinstance(slots.get("governance"), dict) else {}

            gate_final = self._extract_gate_final(gov)
            exec_band = self._extract_execution_band(gov, slots)
            drs_level = self._extract_drs_level(gov, slots)

            rew_level, rew_scope, rew_reasons = self._extract_rew(gov, slots, warnings)

            # Map to a deterministic matrix key (even when missing)
            key = self._matrix_key(
                rew_level=rew_level,
                rew_scope=rew_scope,
                exec_band=exec_band,
            )

            plan_level, plan_actions = self._map_plan(
                rew_level=rew_level,
                rew_scope=rew_scope,
                exec_band=exec_band,
                gate_final=gate_final,
                drs_level=drs_level,
                warnings=warnings,
            )

            lines: List[str] = []
            # Header line (human)
            lines.append(
                f"REW={rew_level or 'MISSING'}"
                f" · Scope={rew_scope or 'MISSING'}"
                f" · Execution={exec_band or 'MISSING'}"
                f" · Gate={gate_final or 'MISSING'}"
                f" · DRS={drs_level or 'MISSING'}"
            )
            lines.append(f"MatrixKey: {key}")
            lines.append(f"PlanLevel: {plan_level}")

            if rew_reasons:
                lines.append("")
                lines.append("REW reasons（最多 3 条）：")
                for r in rew_reasons[:3]:
                    rr = self._safe_str(r)
                    if rr:
                        lines.append(f"- {rr}")

            lines.append("")
            lines.append("去风险行动建议（只读）：")
            for a in (plan_actions or [])[:12]:
                aa = self._safe_str(a)
                if aa:
                    lines.append(f"- {aa}")

            payload = {
                "schema_version": self._SCHEMA_VERSION,
                "inputs": {
                    "rew_level": rew_level or "MISSING",
                    "rew_scope": rew_scope or "MISSING",
                    "execution_band": exec_band or "MISSING",
                    "gate_final": gate_final or "MISSING",
                    "drs_level": drs_level or "MISSING",
                },
                "matrix_key": key,
                "plan_level": plan_level,
                "action_plan": list(plan_actions or []),
                "content": lines,
                "note": "只读：该卡片不参与 Gate/Execution/DRS 计算，仅把治理结论映射为去风险执行建议。",
            }

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload=payload,
                warnings=warnings,
            )

        except Exception as e:
            log.exception("RewDeriskQuickcardBlock.render failed: %s", e)
            warnings.append("exception:rew_derisk_quickcard_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload={
                    "schema_version": self._SCHEMA_VERSION,
                    "content": [
                        "BLOCK_EXCEPTION (captured)",
                        f"{type(e).__name__}: {e}",
                    ],
                    "note": "异常已记录日志；本 block 不影响其它 block 生成。",
                },
                warnings=warnings,
            )

    # ----------------------------
    # Extractors (best-effort)
    # ----------------------------
    @staticmethod
    def _safe_str(v: Any) -> Optional[str]:
        if v is None:
            return None
        try:
            s = str(v).strip()
        except Exception:
            return None
        return s or None

    @staticmethod
    def _extract_gate_final(gov: Dict[str, Any]) -> Optional[str]:
        g = gov.get("gate") if isinstance(gov.get("gate"), dict) else None
        if isinstance(g, dict):
            v = g.get("final_gate") or g.get("final") or g.get("gate")
            s = RewDeriskQuickcardBlock._safe_str(v)
            return s.upper() if isinstance(s, str) else None
        return None

    @staticmethod
    def _extract_execution_band(gov: Dict[str, Any], slots: Dict[str, Any]) -> Optional[str]:
        # V12 canonical: governance.execution.band
        ex = gov.get("execution") if isinstance(gov.get("execution"), dict) else None
        if isinstance(ex, dict):
            s = RewDeriskQuickcardBlock._safe_str(ex.get("band") or ex.get("code"))
            if s:
                return s.upper()
        # legacy: slots.execution_summary.band
        es = slots.get("execution_summary")
        if isinstance(es, dict):
            s = RewDeriskQuickcardBlock._safe_str(es.get("band") or es.get("code"))
            if s:
                return s.upper()
        if es is not None:
            try:
                s = getattr(es, "band", None) or getattr(es, "code", None)
                s = RewDeriskQuickcardBlock._safe_str(s)
                if s:
                    return s.upper()
            except Exception:
                pass
        return None

    @staticmethod
    def _extract_drs_level(gov: Dict[str, Any], slots: Dict[str, Any]) -> Optional[str]:
        # prefer governance.drs
        drs = gov.get("drs") if isinstance(gov.get("drs"), dict) else None
        if isinstance(drs, dict):
            s = RewDeriskQuickcardBlock._safe_str(drs.get("band") or drs.get("level") or drs.get("signal"))
            if s:
                return s.upper()
        # legacy slots['drs']['signal']
        d = slots.get("drs") if isinstance(slots.get("drs"), dict) else None
        if isinstance(d, dict):
            s = RewDeriskQuickcardBlock._safe_str(d.get("signal") or d.get("level"))
            if s:
                return s.upper()
        return None

    def _extract_rew(
        self,
        gov: Dict[str, Any],
        slots: Dict[str, Any],
        warnings: List[str],
    ) -> Tuple[Optional[str], Optional[str], List[str]]:
        """Extract REW (Regime Early Warning) best-effort.

        Supported shapes (best-effort):
        - slots['governance']['rew'] = {level/band/signal, scope, reasons}
        - slots['rew'] / slots['regime_early_warning']
        """

        cand = None
        for k in ("rew", "regime_early_warning"):
            v = gov.get(k) if isinstance(gov.get(k), dict) else None
            if isinstance(v, dict):
                cand = v
                break

        if cand is None:
            for k in ("rew", "regime_early_warning"):
                v = slots.get(k) if isinstance(slots.get(k), dict) else None
                if isinstance(v, dict):
                    cand = v
                    break

        if not isinstance(cand, dict):
            warnings.append("missing:rew")
            return None, None, []

        lvl = self._safe_str(cand.get("level") or cand.get("band") or cand.get("signal"))
        lvl_u = lvl.upper() if isinstance(lvl, str) else None
        # Allow GREEN as a valid “no early warning” state.
        if lvl_u not in {"GREEN", "YELLOW", "ORANGE", "RED"}:
            warnings.append("invalid:rew_level")
            lvl_u = None

        scope = self._safe_str(cand.get("scope") or cand.get("domain") or cand.get("kind"))
        scope_u = scope.upper() if isinstance(scope, str) else None
        if scope_u in {"LOCAL", "CN", "A", "ASHARE"}:
            scope_u = "LOCAL"
        elif scope_u in {"GLOBAL", "OVERSEAS", "US"}:
            scope_u = "GLOBAL"
        elif scope_u is not None:
            warnings.append("invalid:rew_scope")
            scope_u = None

        reasons_raw = cand.get("reasons")
        reasons: List[str] = []
        if isinstance(reasons_raw, list):
            for r in reasons_raw:
                s = self._safe_str(r)
                if s:
                    reasons.append(s)
        elif isinstance(reasons_raw, str) and reasons_raw.strip():
            reasons.append(reasons_raw.strip())

        return lvl_u, scope_u, reasons

    # ----------------------------
    # Matrix & Plan
    # ----------------------------
    @staticmethod
    def _matrix_key(*, rew_level: Optional[str], rew_scope: Optional[str], exec_band: Optional[str]) -> str:
        lvl = (rew_level or "MISSING").upper()
        scope = (rew_scope or "MISSING").upper()
        ex = (exec_band or "MISSING").upper()
        return f"{lvl}_{scope}_{ex}"

    @staticmethod
    def _level_rank(level: Optional[str]) -> int:
        if not isinstance(level, str):
            return 0
        u = level.strip().upper()
        return {"GREEN": 0, "YELLOW": 1, "ORANGE": 2, "RED": 3}.get(u, 0)

    def _map_plan(
        self,
        *,
        rew_level: Optional[str],
        rew_scope: Optional[str],
        exec_band: Optional[str],
        gate_final: Optional[str],
        drs_level: Optional[str],
        warnings: List[str],
    ) -> Tuple[str, List[str]]:
        """Deterministic mapping to a de-risk plan.

        Frozen contract:
        - only uses already-computed governance states (rew/gate/execution/drs)
        - never throws
        """

        lvl_rank = self._level_rank(rew_level)
        scope = (rew_scope or "").upper()
        ex = (exec_band or "").upper()
        gate = (gate_final or "").upper()
        drs = (drs_level or "").upper()

        # Fallback: if REW missing/invalid, we still emit a conservative plan based on Gate/Execution.
        # Note: GREEN is a valid REW state and should NOT trigger fallback.
        if rew_level is None:
            warnings.append("fallback:rew_missing_use_gate_exec")

        # Compute an "intensity" score (0~5) for selecting a plan template.
        intensity = max(lvl_rank, 0)
        if scope == "GLOBAL" and intensity > 0:
            intensity += 1
        if ex == "D2":
            intensity += 2
        elif ex == "D1":
            intensity += 1
        if gate in {"FREEZE", "D", "D1", "D2"}:
            intensity = max(intensity, 4)
        if gate == "CAUTION":
            intensity = max(intensity, 3)

        # DRS is not a primary driver here, but can slightly tilt messaging.
        if drs in {"YELLOW", "RED"}:
            intensity = max(intensity, 2)

        # Plan templates (deterministic)
        if intensity >= 5:
            level = "RED"
            actions = [
                "立刻停止所有新增风险敞口（撤销 ADD-RISK/进攻挂单）",
                "优先降低高β/高拥挤暴露：从主题卫星 → 次核心 → 核心逐级",
                "执行摩擦大时（D1/D2）：只在反弹/放量段分批减，避免追跌",
                "提高现金与防御资产占比（必要时降档到 A 档/对冲池）",
                "若出现结构破坏/失败率上升：允许更激进的去风险（一次性降档）",
            ]
            return level, actions

        if intensity >= 4:
            level = "ORANGE"
            actions = [
                "停止加仓/扩敞口；将计划切换为“去风险优先”",
                "优先处理高β/高波动持仓：分批减仓或用反弹做再平衡",
                "若执行不顺（D1/D2）：减少换仓频率，避免追价与冲动单",
                "把仓位降到可接受回撤区（预留 1–2 天的流动性安全垫）",
            ]
            return level, actions

        if intensity >= 3:
            level = "YELLOW"
            actions = [
                "保持观望/不主动扩风险；把新增计划改为“等确认/等回撤”",
                "可在强势反弹段做小幅降档（以降低波动与执行摩擦）",
                "检查高拥挤与高估值标的：不追高，挂单/分批为主",
            ]
            return level, actions

        # intensity 0~2
        level = "GREEN"
        actions = [
            "未触发 REW 去风险矩阵（或数据缺失）：保持制度边界执行",
            "若 Execution=D1/D2 或 Gate=CAUTION：仍禁止加仓，优先降低摩擦",
        ]
        return level, actions
