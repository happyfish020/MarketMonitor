# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class SummaryANDBlock(ReportBlockRendererBase):
    \"\"\"
    UnifiedRisk V12 · Summary (A / N / D) Block（语义一致性冻结版）

    Fixes (Frozen Engineering):
    - Never output combined Execution bands like \"A/D1\" (band must be single value).
    - Summary must explicitly show Gate → Permission mapping when Gate semantics differ.
    - Missing/invalid inputs MUST NOT crash report rendering; use warnings + placeholders.
    \"\"\"

    block_alias = "summary"
    title = "简要总结（Summary · A / N / D）"

    # -----------------------------
    # Public API
    # -----------------------------
    def render(
        self,
        context: ReportContext,
        doc_partial: Dict[str, Any],
    ) -> ReportBlock:
        warnings: List[str] = []

        summary_code = doc_partial.get("summary")
        if summary_code not in ("A", "N", "D"):
            warnings.append("invalid:summary_code")
            summary_code = "N"

        actionhint = doc_partial.get("actionhint")
        reason = (
            actionhint.get("reason")
            if isinstance(actionhint, dict) and actionhint.get("reason")
            else "结构进入谨慎区间，制度上不支持主动扩大风险敞口。"
        )

        # Gate mapping (C fix)
        gate_pre = context.slots.get("gate_pre")
        gate_final = context.slots.get("gate_final")
        gate_for_perm = gate_final or gate_pre
        if not gate_for_perm:
            warnings.append("empty:gate")
        gate_perm = self._gate_permission_text(gate_for_perm) if gate_for_perm else None

        # Compose lines (avoid raising; keep stable formatting)
        lines: List[str] = []

        # Header line: Code + Gate permission mapping (if available)
        if gate_for_perm and gate_perm:
            lines.append(f"Code:{summary_code}（Gate={gate_for_perm}：{gate_perm}）")
        else:
            lines.append(f"Code:{summary_code}")

        # Main meaning
        lines.append(reason)
        lines.append("趋势结构仍在，但成功率下降，制度不支持主动扩大风险敞口。")

        # DRS（只读）
        drs = context.slots.get("drs")
        if isinstance(drs, dict):
            sig = drs.get("signal")
            m = drs.get("meaning")
            if sig or m:
                lines.append(f"【DRS · 日度风险信号】{sig} —— {m}")
            else:
                warnings.append("invalid:drs_payload")
        elif drs is not None:
            # unexpected format
            warnings.append("invalid:drs_format")

        # Execution (B fix): band must be single value, never \"A/D1\"
        execu = context.slots.get("execution_summary")
        exec_band = self._extract_execution_band(execu, warnings)
        if exec_band:
            lines.append(
                f"【Execution · 2–5D】{exec_band} —— "
                "Execution 仅评估执行摩擦；在 Gate 约束下不构成放行或进攻依据。"
            )
        elif execu is not None:
            warnings.append("invalid:execution_summary_format")
        else:
            warnings.append("empty:execution_summary")

        # Gate block (keep for readability; do not overload Execution)
        if gate_pre or gate_final:
            lines.append("【制度权限（Gate）】")
            if gate_pre:
                lines.append(f"- 原始 Gate：{gate_pre}")
            else:
                warnings.append("empty:gate_pre")
                lines.append("- 原始 Gate：N/A")
            if gate_final:
                lines.append(f"- 执行后 Gate：{gate_final}")
            else:
                warnings.append("empty:gate_final")
                lines.append("- 执行后 Gate：N/A")

        payload = "\\n".join(lines)

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload=payload,
            warnings=warnings,
        )

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

    def _extract_execution_band(self, execu: Any, warnings: List[str]) -> Optional[str]:
        \"\"\"
        Extract a single execution band string.

        Priority:
        - execu.band
        - execu.code (fallback)
        \"\"\"
        band = self._get_field(execu, "band")
        if not band:
            band = self._get_field(execu, "code")

        if band is None:
            return None

        if not isinstance(band, str):
            try:
                band = str(band)
            except Exception:
                warnings.append("invalid:execution_band_type")
                return None

        s = band.strip().upper()

        # Hard rule: never allow combined strings like \"A/D1\" or \"A / D1\"
        if "/" in s:
            warnings.append("invalid:execution_band_combined")
            s = s.split("/", 1)[0].strip().upper()

        # Common normalizations
        s = s.replace(" ", "")

        allowed = {"A", "N", "D", "D1", "D2"}
        if s not in allowed:
            warnings.append("invalid:execution_band_value")
            # keep raw but still return something to avoid empty UX
            return s

        return s

    def _gate_permission_text(self, gate: Any) -> str:
        if gate is None:
            return "权限未知"

        g = str(gate).strip().upper()

        mapping = {
            "ALLOW": "允许 ADD-RISK / REBALANCE（受 Execution 约束）",
            "A": "允许 ADD-RISK / REBALANCE（受 Execution 约束）",
            "NORMAL": "允许 HOLD；ADD-RISK 不鼓励/需满足附加条件",
            "N": "允许 HOLD；ADD-RISK 不鼓励/需满足附加条件",
            "CAUTION": "允许 HOLD / DEFENSE；禁止 ADD-RISK",
            "C": "允许 HOLD / DEFENSE；禁止 ADD-RISK",
            "PLANB": "以防守为主；禁止 ADD-RISK（仅允许结构性切换/降风险）",
            "FREEZE": "必须 DEFENSE / DE-RISK；禁止一切进攻",
            "D": "必须 DEFENSE / DE-RISK；禁止 ADD-RISK",
        }

        return mapping.get(g, "权限未知")
