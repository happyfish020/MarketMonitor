# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
CN A-Share PolicyCompute (Policy Orchestrator)

职责（冻结）：
- 作为 Policy 层唯一编排入口
- 顺序调用：
    1) FactorCompute  → PolicySlots
    2) RegimeCompute  → RegimeResult
    3) GateCompute    → GateDecision
- 不参与任何制度判断
- 不拼装 Report / ActionHint
- 对 Engine 暴露稳定的 compute() 接口
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from core.utils.logger import get_logger

from core.policy.cn.ashare_factor_compute import AshareFactorCompute
from core.policy.cn.ashare_regime_compute import AshareRegimeCompute
from core.policy.cn.ashare_gate_compute import AshareGateCompute

LOG = get_logger("Policy.AsharePolicyCompute")


class AsharePolicyCompute:
    """
    A股 PolicyCompute（制度编排器）

    构造器（冻结）：
        factor_compute : AshareFactorCompute
        regime_compute : AshareRegimeCompute
        gate_compute   : AshareGateCompute
    """

    def __init__(
        self,
        *,
        factor_compute: AshareFactorCompute,
        regime_compute: AshareRegimeCompute,
        gate_compute: AshareGateCompute,
    ) -> None:
        self._factor_compute = factor_compute
        self._regime_compute = regime_compute
        self._gate_compute = gate_compute

    def compute(
        self,
        *,
        snapshot: Dict[str, Any],
        trade_date: str,
        market: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        执行完整 Policy 计算流程

        输入（冻结）：
        - snapshot: MarketSnapshot（只读，必须已包含 factors）
        - trade_date: 交易日
        - market: 市场标识
        - context: 运行上下文（仅透传）

        输出（冻结）：
        PolicyDecisionBundle = {
            "policy_slots": Dict[str, Any],
            "regime_result": Dict[str, Any],
            "gate_decision": Any,
        }
        """
        if not isinstance(snapshot, dict):
            raise TypeError("snapshot must be dict")

        factors = snapshot.get("factors")
        if not isinstance(factors, dict):
            raise ValueError("snapshot['factors'] must exist and be dict")

        # ==================================================
        # 1️⃣ FactorCompute → PolicySlots
        # ==================================================
        policy_slots = self._factor_compute.compute(factors=factors)

        # ==================================================
        # 2️⃣ RegimeCompute → RegimeResult
        # ==================================================
        regime_result = self._regime_compute.compute(
            factors=factors,
            policy_slots=policy_slots,
        )

        # ==================================================
        # 3️⃣ GateCompute → GateDecision
        # ==================================================
        gate_decision = self._gate_compute.compute(
            snapshot=snapshot,
            policy_slots=policy_slots,
            regime_result=regime_result,
        )

        LOG.info(
            "[AsharePolicyCompute] done | gate=%s",
            getattr(gate_decision, "level", None),
        )

        return {
            "policy_slots": policy_slots,
            "regime_result": regime_result,
            "gate_decision": gate_decision,
        }

    # 支持 callable 注入（给 Engine 用）
    def __call__(
        self,
        *,
        snapshot: Dict[str, Any],
        trade_date: str,
        market: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self.compute(
            snapshot=snapshot,
            trade_date=trade_date,
            market=market,
            context=context,
        )
