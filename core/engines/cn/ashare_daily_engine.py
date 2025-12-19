# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - CN AShare Daily Engine

职责（V12 终态）：
- 作为系统 orchestration 层
- 组织 Fetcher → Snapshot → Factors → Prediction → Reporter
- 不解析业务结果
- 不输出人类可读文本
- 不向 main.py 返回业务数据
"""

from __future__ import annotations

from typing import Dict, Any
from datetime import datetime

from core.utils.logger import get_logger
 
##########
from core.snapshot.ashare_snapshot  import AshareSnapshotBuilder
from core.policy.cn.ashare_policy_compute import AsharePolicyCompute
from core.actionhint.cn.ashare_actionhint_builder import AshareActionHintBuilder
from core.reporters.cn.ashare_report_pipeline import AshareReportPipeline




LOG = get_logger("Engine.AshareDaily")





def _normalize_trade_date(trade_date: str | None) -> str:
    if trade_date:
        s = str(trade_date).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        return s
    return datetime.now().strftime("%Y-%m-%d")


# =========================================================
# Report helpers (naming frozen)
# =========================================================

from core.reporters.report_context import ReportContext
from core.reporters.report_engine import ReportEngine
from core.reporters.renderers.markdown_renderer import MarkdownRenderer
from core.reporters.report_writer import ReportWriter

from core.actions.actionhint_service import ActionHintService
from core.actions.summary_mapper import SummaryMapper

from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock
from core.reporters.report_blocks.summary_a_n_d_blk import SummaryANDBlock
from core.reporters.report_blocks.watchlist_sectors_blk import WatchlistSectorsBlock
from core.reporters.report_blocks.context_overnight_blk import ContextOvernightBlock
from core.reporters.report_blocks.conditions_runtime_blk import ConditionsRuntimeBlock
from core.reporters.report_blocks.dev_evidence_blk import DevEvidenceBlock
from core.reporters.report_blocks.scenarios_forward_blk import ScenariosForwardBlock


####

def _prepare_report_slots(*, gate_decision, factors_bound) -> dict:
    """
    Prepare Phase-3 report slots.

    原则：
    - 不引入新的因子或评分
    - 仅对 Phase-2 结果做解释性聚合
    - slot 缺失允许，但尽量提供可解释内容
    """

    # 1️⃣ Gate（必须）
    gate = gate_decision.level

    # 2️⃣ Structure（Phase-2 冻结结构）
    # 若 Phase-2 已提供 structure，则直接使用
    # 否则 fallback 到 factors_bound（兼容旧结构）
    structure = factors_bound.get("structure", factors_bound)

    # 3️⃣ Watchlist（结构来源说明，而非推荐）
    watchlist = factors_bound.get("watchlist")
    if watchlist is None:
        watchlist = {
            "note": (
                "watchlist 未在 Phase-2 显式提供。"
                "当前报告未基于 sector_rotation 形成观察对象。"
            )
        }

    # 4️⃣ Conditions Runtime（执行前校验条件）
    # 当前版本不引入强制校验，仅提供占位说明
    conditions_runtime = {
        "status": "not_enforced",
        "note": (
            "即时验证条件尚未启用强制校验。"
            "当前 ActionHint 仅受 Gate 与结构性因素约束。"
        ),
    }

    # 5️⃣ Overnight（隔夜环境：解释性，不参与裁决）
    # 从 Phase-2 已有全局因子中提取“可读摘要”
    overnight = {}

    # 常见全局代理（存在才取）
    global_lead = factors_bound.get("global_lead", {})
    index_global = factors_bound.get("index_global", {})

    if isinstance(global_lead, dict):
        for k in ("a50", "hsi"):
            if k in global_lead:
                overnight[f"{k}_proxy"] = global_lead.get(k)

    if isinstance(index_global, dict):
        for k in ("spx", "ndx", "vix", "dxy"):
            if k in index_global:
                overnight[f"{k}_proxy"] = index_global.get(k)

    if not overnight:
        overnight = {
            "note": (
                "未能从 Phase-2 全局代理中提取隔夜环境摘要。"
                "该信息仅用于背景说明，不影响 Gate / ActionHint。"
            )
        }

    return {
        "gate": gate,
        "structure": structure,
        "watchlist": watchlist,
        "conditions_runtime": conditions_runtime,
        "overnight": overnight,
        # scenarios.forward / dev.evidence 由 block 内部基于现有 slot 生成
    }

###??
 
def _build_report_context(
    *,
    trade_date: str,
    slots: dict,
    kind: str,
) -> ReportContext:
    """
    构造只读 ReportContext（防污染边界）。
    """
    return ReportContext(
        kind=kind,
        trade_date=trade_date,
        slots=slots,
    )
 
def _build_report_engine() -> ReportEngine:
    """
    构造 ReportEngine（只做 wiring，不接数据）。
    """
    return ReportEngine(
        market="CN",
        actionhint_service=ActionHintService(),
        
        block_builders={
            # 🚨 只能用 block_alias
            "structure.facts": StructureFactsBlock().render,
            "summary": SummaryANDBlock().render,
            "context.overnight": ContextOvernightBlock().render,
            "watchlist.sectors": WatchlistSectorsBlock().render,
            "conditions.runtime": ConditionsRuntimeBlock().render,
            "scenarios.forward": ScenariosForwardBlock().render,
            "dev.evidence": DevEvidenceBlock().render,
        },
    )  
##### report section to  be added above  


def _execute_report_pipeline1(
    *,
    trade_date: str,
    gate_decision,
    factors_bound,
    kind: str,
) -> str:
    """
    执行完整 Report pipeline：
    slots → context → engine → document → render → write
    """
    slots = _prepare_report_slots(
        gate_decision=gate_decision,
        factors_bound=factors_bound,
    )

    context = _build_report_context(
        trade_date=trade_date,
        slots=slots,
        kind=kind,
    )

    engine = _build_report_engine()

    # 1️⃣ build document
    report_doc = engine.build_report(context=context)

    # 2️⃣ render
    renderer = MarkdownRenderer()
    text = renderer.render(report_doc)

    # 3️⃣ write（🚨 base_dir 必须来自 paths.yaml）
    writer = ReportWriter()
    report_path = writer.write(doc=report_doc, text=text)

    return report_path


#######33
def run_cn_ashare_daily(trade_date: str | None = None, refresh_mode: str = "auto") -> None:
    trade_date = _normalize_trade_date(trade_date)

    LOG.info(
        "Run CN AShare Daily | trade_date=%s refresh=%s",
        trade_date,
        refresh_mode,
    )

    # ===============================
    # 构建 Policy 计算器（制度层）
    # ===============================
    from core.policy.cn.ashare_policy_compute import AsharePolicyCompute
    from core.policy.cn.ashare_factor_compute import AshareFactorCompute
    from core.policy.cn.ashare_regime_compute import AshareRegimeCompute
    from core.policy.cn.ashare_gate_compute import AshareGateCompute

    factor_compute = AshareFactorCompute()

    policy_compute = AsharePolicyCompute(
        factor_compute=factor_compute,
        regime_compute=AshareRegimeCompute(),
        gate_compute=AshareGateCompute(),
    )

    # ===============================
    # Snapshot Pipeline
    # ===============================
    from core.snapshot.cn.ashare_snapshot_pipeline import AshareSnapshotPipeline
    snapshot_pipeline = AshareSnapshotPipeline()

    # ===============================
    # ActionHint / Report Pipeline
    # ===============================
    from core.actionhint.cn.ashare_actionhint_builder import AshareActionHintBuilder
    from core.reporters.cn.ashare_report_pipeline import AshareReportPipeline

    # ===============================
    # 构建 Orchestration Engine
    # ===============================
    engine = AshareDailyEngine(
        snapshot_builder=snapshot_pipeline.build,          # ✅ 只负责 snapshot
        factor_compute=factor_compute.compute,             # ✅ 注入 factor 计算
        policy_compute=policy_compute.compute,             # ✅ 只读 snapshot["factors"]
        actionhint_builder=AshareActionHintBuilder().build,
        report_pipeline=AshareReportPipeline(),
    )

    # ===============================
    # 执行（Engine 内完成完整编排）
    # ===============================
    engine.run(
        trade_date=trade_date,
        refresh_mode=refresh_mode,
    )

    LOG.info("CN AShare Daily finished successfully.")

####################
 


"""
UnifiedRisk V12 FULL
A-share Daily Engine (Orchestration Only)

本文件职责（冻结）：
- 仅承担系统编排（Orchestration）
- 不包含任何制度计算逻辑
- 不拼装报告字段
- 不做 Gate / Regime / Factor 判断
- 只负责按顺序调用外部注入的功能模块，并传递对象

设计原则：
- Interface First
- Dependency Injection（不假设任何实现存在）
- 单向数据流
"""

from typing import Callable, Dict, Any, Optional


class AshareDailyEngine:
    """
    A股日度运行编排器（Orchestrator）

    ⚠️ 注意：
    - 本类不感知任何制度细节
    - 所有功能模块必须由外部注入
    """

    def __init__(
        self,
        *,
        snapshot_builder: Callable[..., Any],
        factor_compute,  
        policy_compute: Callable[..., Any],
        actionhint_builder: Callable[..., Any],
        report_pipeline: Callable[..., Any],
    ) -> None:
        """
        参数说明（全部为依赖注入）：

        snapshot_builder:
            - 负责构建 MarketSnapshot
            - Engine 不关心其内部实现

        policy_compute:
            - 负责制度计算（Factor / Regime / Gate）
            - 返回 PolicyDecisionBundle

        actionhint_builder:
            - 负责生成 ActionHintResult（仅解释与建议）

        report_pipeline:
            - 负责生成最终 DailyReport（表达层）
        """
        self._snapshot_builder = snapshot_builder
        self._factor_compute = factor_compute
        self._policy_compute = policy_compute
        self._actionhint_builder = actionhint_builder
        self._report_pipeline = report_pipeline

    def run(
        self,
        *,
        trade_date: str,
        refresh_mode: str,
        market: str = "CN_A",
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        运行一次 A 股日度流程（Orchestration）

        输入参数（冻结）：
        - trade_date: 交易日（YYYY-MM-DD）
        - refresh_mode: 数据刷新模式（如 full / incremental / cache_only）
        - market: 市场标识（默认 CN_A）
        - context: 运行上下文（可选，Engine 只透传，不解析）

        输出（冻结）：
        - Dict[str, Any]：
            {
                "trade_date": ...,
                "market": ...,
                "snapshot": MarketSnapshot,
                "policy_result": PolicyDecisionBundle,
                "action_hint": ActionHintResult,
                "report": DailyReport,
            }
        """

        # -------- 1. 构建结构事实（Snapshot）--------
        snapshot = self._snapshot_builder(
            trade_date=trade_date,
            refresh_mode=refresh_mode,
            market=market,
            context=context,
        )

        # -------- 2. 制度计算（Policy / Regime / Gate）--------
        policy_result = self._policy_compute(
            snapshot=snapshot,
            trade_date=trade_date,
            market=market,
            context=context,
        )

        # -------- 3. 行为建议构建（ActionHint）--------
        action_hint = self._actionhint_builder(
            snapshot=snapshot,
            policy_result=policy_result,
            trade_date=trade_date,
            market=market,
            context=context,
        )

        # -------- 4. 报告表达（Report Pipeline）--------
        report = self._report_pipeline(
            snapshot=snapshot,
            policy_result=policy_result,
            action_hint=action_hint,
            trade_date=trade_date,
            market=market,
            context=context,
        )

        # -------- 5. 汇总输出（Engine 只做对象封装）--------
        return {
            "trade_date": trade_date,
            "market": market,
            "snapshot": snapshot,
            "policy_result": policy_result,
            "action_hint": action_hint,
            "report": report,
        }



##############


