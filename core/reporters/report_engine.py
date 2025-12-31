from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from core.reporters.report_context import ReportContext
from core.reporters.report_types import ReportBlock, ReportDocument

LOG = logging.getLogger("ReportEngine")

BlockBuilder = Callable[[ReportContext, Dict[str, Any]], ReportBlock]


@dataclass(frozen=True)
class BlockSpec:
    block_alias: str
    title: str


# 🔒 冻结：顺序即制度顺序（不再使用 block_id）
#Todo  -- if add block
BLOCK_SPECS: List[BlockSpec] = [
    # 🔒 冻结：顺序即制度顺序（事实 → 结构 → 解释 → 总结 → 执行）
    #BlockSpec("market.overview", "大盘概述（收盘事实）"),
    BlockSpec("structure.facts", "结构事实（Fact → 含义）"),
    #BlockSpec("etf_spot_sync.explain", "核心字段解释（拥挤/不同步/参与度）"),
    BlockSpec("summary", "简要总结（A / N / D）"),
    BlockSpec("execution.summary", "执行层评估（Execution · 2–5D）"),
    BlockSpec("exit.readiness", "出场准备度（Exit Readiness · Governance）"),
    #BlockSpec("context.overnight", "隔夜维度"),
    #BlockSpec("watchlist.sectors", "观察板块对象"),
    #BlockSpec("execution.timing",  "执行时点校验（风险敞口变更行为）"),
    #BlockSpec("exposure.boundary",  "下一交易日（T+1）风险敞口行为边界"),
    #BlockSpec("scenarios.forward", "T+N 情景说明"),
    BlockSpec("execution_quick_reference", "执行速查参考"),
    #BlockSpec("dev.evidence", "审计证据链"),
]
class ReportEngine:
    """
    UnifiedRisk V12 · Phase-3 ReportEngine（冻结修正版）

    铁律：
    - Summary 必须存在（不可为 None）
    - Block 顺序只由 block_alias 决定
    - block_id 不参与排序与制度语义
    """

    def __init__(
        self,
        *,
        market: str,
        actionhint_service: Any,
        #summary_mapper: Any,
        block_builders: Dict[str, BlockBuilder],  # key = block_alias
    ) -> None:
        self.market = market
        self.actionhint_service = actionhint_service
        #self.summary_mapper = summary_mapper
        self._builders_by_alias = dict(block_builders)

    def build_report_no(self, *, context: ReportContext) -> ReportDocument:
        meta = {
            "market": self.market,
            "trade_date": context.trade_date,
            "kind": context.kind,

            # "mode": context.mode,  # "DEV" or "PROD"
        }

        # -------- ActionHint --------

        actionhint = context.actionhint
        if actionhint is None:
            raise ValueError("ReportContext missing actionhint (forbidden by V12)")

        gate = actionhint.get("gate")
        if gate is None:
            raise ValueError("ActionHint missing gate")

        # -------- Summary（强制不为 None）--------
        #summary = self.summary_mapper.map_gate_to_summary(gate=gate)
        #if summary is None:
        #    raise ValueError("Summary mapping returned None (forbidden by V12)")

        summary = actionhint.get("summary")
        if summary is None:
            raise ValueError("ActionHint missing summary (forbidden by V12)")

        # -------- Blocks（按 alias 顺序）--------
        doc_partial = {"actionhint": actionhint, "summary": summary}
        blocks: List[ReportBlock] = []

        for spec in BLOCK_SPECS:
            builder = self._builders_by_alias.get(spec.block_alias)
            if builder is None:
                LOG.warning("missing block builder: %s", spec.block_alias)
                blocks.append(
                    ReportBlock(
                        block_alias=spec.block_alias,
                        title=spec.title,
                        payload={"note": "BLOCK_NOT_IMPLEMENTED"},
                        warnings=[f"missing_builder:{spec.block_alias}"],
                    )
                )
            else:
                blk = builder(context, doc_partial)
                if blk.block_alias != spec.block_alias:
                    raise ValueError(
                        f"block_alias mismatch: {blk.block_alias} != {spec.block_alias}"
                    )
                blocks.append(blk)

        return ReportDocument(meta, actionhint, summary, blocks)


###
    def build_report(self,  context: ReportContext) -> ReportDocument:
        meta = {
            "market": self.market,
            "trade_date": context.trade_date,
            "kind": context.kind,
        }

        # -------- slots（事实层）--------
        slots = context.slots
        if not isinstance(slots, dict):
            raise ValueError("ReportContext.slots must be dict (V12 invariant)")

        # ---- V12: prefer governance.gate.* ; legacy slots['gate'] only for transition ----
        gov = slots.get("governance")
        gate_pre = None
        gate_final_precomputed = None

        if isinstance(gov, dict):
            g = gov.get("gate")
            if isinstance(g, dict):
                # prefer raw_gate, fallback to final_gate
                gate_pre = g.get("raw_gate") or g.get("final_gate")
                gate_final_precomputed = g.get("final_gate")

        if gate_pre is None:
            # legacy fallback (fixtures / old pipeline)
            gate_pre = slots.get("gate")

        if gate_pre is None:
            raise ValueError("gate missing: expected slots['governance']['gate'] or legacy slots['gate']")

        structure = slots.get("structure") or {}
        observations = slots.get("observations")
        if not isinstance(observations, dict):
            observations = {}

        # 标准化 DRS（兼容两套字段命名：signal / level）
        # - V12 canonical: slots["governance"]["drs"] may carry {"signal": "..."} or {"level": "..."}
        # - legacy blocks often read slots["drs"]["signal"]
        drs_signal = None

        # 1) prefer slots["drs"]
        drs_slot = slots.get("drs")
        if isinstance(drs_slot, dict):
            drs_signal = drs_slot.get("signal") or drs_slot.get("level")

        # 2) fallback to governance.drs
        if drs_signal is None and isinstance(gov, dict):
            gov_drs = gov.get("drs")
            if isinstance(gov_drs, dict):
                drs_signal = gov_drs.get("signal") or gov_drs.get("level")
                # mirror to legacy slot shape if needed
                if "drs" not in slots and isinstance(drs_signal, str) and drs_signal:
                    slots["drs"] = {"signal": drs_signal}
                # ensure governance also has "signal" for downstream
                if isinstance(drs_signal, str) and drs_signal and "signal" not in gov_drs:
                    gov_drs["signal"] = drs_signal

        # 3) final: ensure slots["drs"]["signal"] if we have drs_signal
        if isinstance(drs_signal, str) and drs_signal:
            cur = slots.get("drs")
            if isinstance(cur, dict) and "signal" not in cur:
                cur["signal"] = drs_signal
            elif cur is None:
                slots["drs"] = {"signal": drs_signal}

        # trend_state
        trend = structure.get("trend_in_force") if isinstance(structure, dict) else None
        trend_state = trend.get("state") if isinstance(trend, dict) else None

        # execution_band（兼容 V12 新旧字段）
        # - legacy: slots["execution_summary"]["band"] 或 ExecutionSummaryObj.band
        # - V12 canonical (fixtures): slots["governance"]["execution"]["band"]
        execution_band = None

        execution_summary = slots.get("execution_summary")
        if isinstance(execution_summary, dict):
            execution_band = execution_summary.get("band") or execution_summary.get("code")
        elif execution_summary is not None:
            execution_band = getattr(execution_summary, "band", None) or getattr(execution_summary, "code", None)

        # fallback to governance.execution.band
        if execution_band is None and isinstance(gov, dict):
            gov_exec = gov.get("execution")
            if isinstance(gov_exec, dict):
                execution_band = gov_exec.get("band") or gov_exec.get("code")

        # normalize string
        if execution_band is not None and not isinstance(execution_band, str):
            try:
                execution_band = str(execution_band)
            except Exception:
                execution_band = None

        if isinstance(execution_band, str):
            execution_band = execution_band.strip().upper()

        # mirror to legacy slot shape so Summary/Blocks can render consistently
        if isinstance(execution_band, str) and execution_band:
            if not isinstance(slots.get("execution_summary"), dict):
                slots["execution_summary"] = {"band": execution_band}
            else:
                if "band" not in slots["execution_summary"]:
                    slots["execution_summary"]["band"] = execution_band

            # ensure governance.execution exists and has "band"
            if isinstance(gov, dict):
                gov_exec = gov.setdefault("execution", {})
                if isinstance(gov_exec, dict) and "band" not in gov_exec:
                    gov_exec["band"] = execution_band

        # -------- ① GateOverlay（只允许降级）--------
        from core.governance.gate_overlay import GateOverlay

        if isinstance(gate_final_precomputed, str) and gate_final_precomputed:
            gate_final = gate_final_precomputed
            overlay_reasons = []
            overlay_evidence = {"note": "gate_final precomputed in slots['governance']['gate']"}
        else:
            overlay = GateOverlay().apply(
                gate_pre=gate_pre,
                trend_state=trend_state,
                drs_signal=drs_signal,
                execution_band=execution_band,
            )
            gate_final = overlay.gate_final
            overlay_reasons = overlay.reasons
            overlay_evidence = overlay.evidence

        # 写回 slots（供 Summary 展示 + 新契约落位）
        slots["gate_pre"] = gate_pre
        slots["gate_final"] = gate_final
        slots["gate_overlay"] = {"reasons": overlay_reasons, "evidence": overlay_evidence}

        # V12 canonical placement
        gov = slots.setdefault("governance", {}) if isinstance(slots, dict) else {}
        if isinstance(gov, dict):
            g = gov.setdefault("gate", {})
            if isinstance(g, dict):
                g["raw_gate"] = gate_pre
                g["final_gate"] = gate_final
            gov["gate_overlay"] = {"reasons": overlay_reasons, "evidence": overlay_evidence}

        # -------- ② Rebound-only Observation（只读）--------
        from core.regime.observation.rebound_only.rebound_only_observation import ReboundOnlyObservation
        rebound_only = ReboundOnlyObservation().build(
            trend_state=trend_state,
            drs_signal=drs_signal,
            execution_band=execution_band,
            asof=context.trade_date,
        )
        observations["rebound_only"] = rebound_only
        slots["observations"] = observations
        slots["rebound_only"] = rebound_only.get("observation")  # 便于 block 未来直接展示

        from core.governance.exit_readiness_validator import ExitReadinessValidator
        slots["exit_readiness"] = ExitReadinessValidator().evaluate(slots=slots, asof=context.trade_date)

        # -------- ③ ActionHint（唯一生成点；只传已支持参数）--------
        # 注意：ActionHintService 不接收 observations / execution_summary
        actionhint = self.actionhint_service.build_actionhint(
            gate=gate_final,
            structure=structure if isinstance(structure, dict) else None,
            watchlist=slots.get("watchlist") if isinstance(slots.get("watchlist"), dict) else None,
            conditions_runtime=slots.get("conditions_runtime"),
        )

        if not isinstance(actionhint, dict):
            raise ValueError("ActionHint must be dict (V12 invariant)")

        summary = actionhint.get("summary")
        if summary is None:
            raise ValueError("ActionHint missing summary (forbidden by V12)")

        # -------- Blocks（按 alias 顺序）--------
        doc_partial = {
            "actionhint": actionhint,
            "summary": summary,
        }

        blocks: List[ReportBlock] = []

        for spec in BLOCK_SPECS:
            builder = self._builders_by_alias.get(spec.block_alias)
            if builder is None:
                LOG.warning("missing block builder: %s", spec.block_alias)
                blocks.append(
                    ReportBlock(
                        block_alias=spec.block_alias,
                        title=spec.title,
                        payload={"note": "BLOCK_NOT_IMPLEMENTED"},
                        warnings=[f"missing_builder:{spec.block_alias}"],
                    )
                )
            else:
                blk = builder(context, doc_partial)
                if blk.block_alias != spec.block_alias:
                    raise ValueError(
                        f"block_alias mismatch: {blk.block_alias} != {spec.block_alias}"
                    )
                blocks.append(blk)

        from core.reporters.cn.semantic_guard import SemanticGuard

        # 假设你已经有：
        # gate_final: str
        # report_blocks: List[ReportBlock]

        guard = SemanticGuard(mode="WARN")  # 先用 WARN
        warnings = guard.check(
            gate_final=gate_final,
            blocks={b.block_alias: b.payload for b in blocks},
        )

        for w in warnings:
            LOG.warning(w)


        return ReportDocument(meta, actionhint, summary, blocks)

##