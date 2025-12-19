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
