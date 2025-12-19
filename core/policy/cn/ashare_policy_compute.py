"""
UnifiedRisk V12 FULL
A-share Policy Compute Aggregator

制度定位（冻结）：
- 制度计算层的唯一聚合入口
- 负责顺序触发：
    1) Factor 计算
    2) Regime 判定
    3) Gate 决策
- 不关心任何数据来源
- 不参与任何表达 / 报告拼装
"""

from typing import Dict, Any, Callable, Optional


class AsharePolicyCompute:
    """
    A股制度计算聚合器（Policy Compute）

    ⚠️ 铁律：
    - 本类是 Gate / Regime / Factor 的唯一产生源
    - 所有制度模块通过依赖注入提供
    """

    def __init__(
        self,
        *,
        factor_compute: Callable[..., Dict[str, Any]],
        regime_compute: Callable[..., Any],
        gate_compute: Callable[..., Any],
    ) -> None:
        """
        依赖注入说明（不假设任何实现存在）：

        factor_compute:
            - 输入 snapshot
            - 输出 Dict[str, FactorResult]

        regime_compute:
            - 输入 factor_results + snapshot
            - 输出 RegimeResult

        gate_compute:
            - 输入 regime_result + factor_results
            - 输出 GateDecision
        """
        self._factor_compute = factor_compute
        self._regime_compute = regime_compute
        self._gate_compute = gate_compute

    def compute(
        self,
        *,
        snapshot: Any,
        trade_date: str,
        market: str = "CN_A",
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        执行一次完整制度计算（冻结流程）

        输入（冻结）：
        - snapshot: MarketSnapshot（结构事实，只读）
        - trade_date: 交易日
        - market: 市场标识
        - context: 运行上下文（仅透传）

        输出（冻结）：
        - PolicyDecisionBundle（Dict[str, Any]）
        """

        # ---------------------------
        # 1. Factor 计算（制度证据）
        # ---------------------------
        factor_results = self._factor_compute(
            snapshot=snapshot,
            trade_date=trade_date,
            market=market,
            context=context,
        )

        # ---------------------------
        # 2. Regime 判定（制度状态）
        # ---------------------------
        regime_result = self._regime_compute(
            snapshot=snapshot,
            factor_results=factor_results,
            trade_date=trade_date,
            market=market,
            context=context,
        )

        # ---------------------------
        # 3. Gate 决策（制度门控）
        # ---------------------------
        gate_decision = self._gate_compute(
            regime_result=regime_result,
            factor_results=factor_results,
            trade_date=trade_date,
            market=market,
            context=context,
        )

        # ---------------------------
        # 4. 制度裁决结果封装（不解释、不加工）
        # ---------------------------
        policy_result: Dict[str, Any] = {
            "factor_results": factor_results,
            "regime_result": regime_result,
            "gate_decision": gate_decision,
            "policy_notes": {
                "engine": "AsharePolicyCompute",
                "version": "V12",
            },
        }

        return policy_result
