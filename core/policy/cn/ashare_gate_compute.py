# -*- coding: utf-8 -*-
"""
UnifiedRisk V12
CN A-Share GateCompute (Decision Layer)

职责（冻结）：
- 基于 RegimeResult + PolicySlots 进行制度裁决（Gate）
- 不参与结构判断（structure 已完成）
- 不生成 ActionHint
- 不拼装 Report
- 当前版本仅包裹既有 ASharesGateDecider（过渡实现）
"""

from __future__ import annotations

from typing import Any, Dict

from core.utils.logger import get_logger
from core.regime.ashares_gate_decider import ASharesGateDecider

LOG = get_logger("Policy.AshareGateCompute")


class AshareGateCompute:
    """
    A股 GateCompute（制度裁决器）

    输出（冻结）：
    GateDecision = {
        "level": str,
        "reasons": list[str],
        "constraints": dict,
        "evidence": dict,
    }
    """

    def __init__(self, *, gate_decider: ASharesGateDecider | None = None) -> None:
        # 允许外部注入（测试 / 将来替换）
        self._decider = gate_decider or ASharesGateDecider()

    def compute(
        self,
        *,
        snapshot: Dict[str, Any],
        policy_slots: Dict[str, Any],
        regime_result: Dict[str, Any],
    ) -> Any:
        """
        执行 Gate 裁决

        注意：
        - snapshot 仅用于兼容旧 GateDecider 的接口
        - GateDecider 内部仍然只读 structure / watchlist 等制度槽位
        """
        if not isinstance(snapshot, dict):
            raise TypeError("snapshot must be dict")
        if not isinstance(policy_slots, dict):
            raise TypeError("policy_slots must be dict")
        if not isinstance(regime_result, dict):
            raise TypeError("regime_result must be dict")

        # 兼容旧 GateDecider 的输入形式：
        # 旧接口：decide(snapshot, factors_bound)
        # 这里将 policy_slots + structure 合并为 factors_bound 形态
        factors_bound = dict(policy_slots)

        structure = regime_result.get("structure")
        if structure is not None:
            factors_bound["structure"] = structure

        try:
            gate_decision = self._decider.decide(snapshot, factors_bound)
        except Exception:
            LOG.exception("[AshareGateCompute] gate decider failed")
            raise

        # gate_decision 保持原样（不强制改结构）
        LOG.info(
            "[AshareGateCompute] gate decided | level=%s",
            getattr(gate_decision, "level", None),
        )

        return gate_decision

    # 支持 callable 注入
    def __call__(
        self,
        *,
        snapshot: Dict[str, Any],
        policy_slots: Dict[str, Any],
        regime_result: Dict[str, Any],
    ) -> Any:
        return self.compute(
            snapshot=snapshot,
            policy_slots=policy_slots,
            regime_result=regime_result,
        )
