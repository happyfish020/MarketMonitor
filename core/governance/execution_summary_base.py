from abc import ABC, abstractmethod
from typing import Dict, Any


class ExecutionSummaryBuilderBase(ABC):
    """
    ExecutionSummaryBuilderBase

    职责：
    - 只读 governance_facts（来自 Phase-2 的治理解释层）
    - 将其解释为短期（2–5D）可执行风险等级
    - 不生成新事实、不计算因子、不参与 GateDecision

    输入：
    - governance_facts: Dict[str, Any]
        由 GovernanceFactsBuilder 产出

    输出：
    - execution_summary: Dict[str, Any]
        用于 GateOverlay / ReportBuilder
    """

    def __init__(self, governance_facts: Dict[str, Any]):
        self.governance_facts = governance_facts

    @abstractmethod
    def build(self) -> Dict[str, Any]:
        """
        Build execution summary from governance facts.

        Returns:
            Dict[str, Any]: execution_summary
        """
        raise NotImplementedError
