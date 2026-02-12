# -*- coding: utf-8 -*-
"""UnifiedRisk V12 · Rotation Switch block (Frozen)

目标：
- 在报告中明确展示：当前交易日是否“适合/不适合”启用板块轮动策略
- 必须给出理由（结构化 reasons），并展示数据缺失情况

注意：
- 本 block 只读展示，不参与 Gate / ActionHint 决策
- 任何异常都必须捕获为 warnings，避免影响其它区块
"""

from __future__ import annotations

from typing import Any, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase
from core.utils.logger import get_logger


log = get_logger(__name__)


class RotationSwitchBlock(ReportBlockRendererBase):
    block_alias = "rotation.switch"
    title = "板块轮动开关（Rotation Switch）"

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        try:
            rs = context.slots.get("rotation_switch") if isinstance(context.slots, dict) else None
            if not isinstance(rs, dict) or not rs:
                warnings.append("empty:rotation_switch")
                return ReportBlock(
                    block_alias=self.block_alias,
                    title=self.title,
                    warnings=warnings,
                    payload={
                        "content": ["（未生成 rotation_switch：该区块仅用于占位）"],
                        "note": "注：RotationSwitch 只读展示；缺失不影响其它 block。",
                    },
                )

            mode = rs.get("mode")
            verdict = rs.get("verdict")
            conf = rs.get("confidence")

            gating = rs.get("gating") if isinstance(rs.get("gating"), dict) else {}
            gate = gating.get("gate")
            execution = gating.get("execution")
            drs = gating.get("drs")

            lines: List[str] = []
            header = f"- **今日结论：{mode}**"
            if isinstance(conf, (int, float)):
                header += f"（conf={float(conf):.2f}）"
            if isinstance(verdict, str) and verdict.strip():
                header += f" · {verdict.strip()}"
            lines.append(header)

            # Governance context
            ctx_bits: List[str] = []
            if isinstance(gate, str) and gate:
                ctx_bits.append(f"Gate={gate}")
            if isinstance(execution, str) and execution:
                ctx_bits.append(f"Execution={execution}")
            if isinstance(drs, str) and drs:
                ctx_bits.append(f"DRS={drs}")
            if ctx_bits:
                lines.append("- 制度背景：" + " / ".join(ctx_bits))

            # Execution meaning (must be explicit)
            meaning = None
            if isinstance(mode, str):
                m = mode.strip().upper()
                if m == "OFF":
                    meaning = "禁止板块轮动/换仓进攻；仅允许 HOLD 或按计划小幅降摩擦（不追涨、不扩风险）。"
                elif m == "PARTIAL":
                    meaning = "仅限低频/只做确认段：禁止 IGNITE 追涨；优先 CONFIRM/KEEP，控制换手。"
                elif m == "ON":
                    meaning = "允许启用板块轮动（仍受 Gate/Execution/DRS 约束）；建议持仓≥3天，避免高频切换。"
            if meaning:
                lines.append(f"- 执行含义：{meaning}")

            # Top reasons
            reasons = rs.get("reasons") if isinstance(rs.get("reasons"), list) else []
            if reasons:
                lines.append("- 理由（Top）：")
                for r in reasons[:8]:
                    if not isinstance(r, dict):
                        continue
                    code = r.get("code")
                    level = r.get("level")
                    msg = r.get("msg")
                    if isinstance(code, str) and isinstance(msg, str):
                        lv = f"{level} " if isinstance(level, str) and level else ""
                        lines.append(f"  - {lv}{code}: {msg}")

            # Data status
            ds = rs.get("data_status") if isinstance(rs.get("data_status"), dict) else {}
            cov = ds.get("coverage")
            missing = ds.get("missing") if isinstance(ds.get("missing"), list) else []
            if cov == "PARTIAL":
                warnings.append("partial:rotation_switch")
                lines.append(f"- ⚠ 数据不全：missing={missing}")

            # Execution constraints
            cons = rs.get("constraints") if isinstance(rs.get("constraints"), dict) else {}
            if cons:
                mh = cons.get("min_hold_days")
                ms = cons.get("max_switch_per_week")
                ac = cons.get("allow_chase")
                bits: List[str] = []
                if mh is not None:
                    bits.append(f"min_hold_days={mh}")
                if ms is not None:
                    bits.append(f"max_switch_per_week={ms}")
                if ac is not None:
                    bits.append(f"allow_chase={ac}")
                if bits:
                    lines.append("- 执行边界：" + ", ".join(bits))

            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": lines,
                    "raw": rs,
                    "note": "注：RotationSwitch 仅作为策略开关与解释层展示，不直接改变 Gate/Execution/DRS。",
                },
            )

        except Exception as e:
            log.exception("RotationSwitchBlock.render failed: %s", e)
            warnings.append("exception:rotation_switch_render")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                warnings=warnings,
                payload={
                    "content": ["RotationSwitch 渲染异常（已捕获）。"],
                    "note": "注：异常已记录日志；本 block 不影响其它 block。",
                },
            )
