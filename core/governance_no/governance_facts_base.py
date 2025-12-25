from abc import ABC, abstractmethod
from typing import Dict, Any


class GovernanceFactsBuilderBase(ABC):
    """
    GovernanceFactsBuilderBase

    职责：
    - 只读 Phase-2 Factor outputs（FactorResult）
    - 将其解释为“治理事实（governance facts）”
    - 不产生新信号，不参与评分，不反向影响 Factor / Gate

    输入：
    - factor_results: Dict[str, Any]
        key   = factor_name
        value = FactorResult（或等价结构）

    输出：
    - governance_facts: Dict[str, Any]
        只包含布尔 / 枚举 / 标签类事实
    """

    def __init__(self, factor_results: Dict[str, Any]):
        self.factor_results = factor_results

    @abstractmethod
    def build(self) -> Dict[str, Any]:
        """
        Build governance facts from Phase-2 factor outputs.

        Returns:
            Dict[str, Any]: governance_facts
        """
        raise NotImplementedError
