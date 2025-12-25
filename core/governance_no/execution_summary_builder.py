from typing import Dict, Any, List
from core.governance.execution_summary_base import ExecutionSummaryBuilderBase


class ExecutionSummaryBuilder(ExecutionSummaryBuilderBase):
    """
    ExecutionSummaryBuilder

    说明：
    - 只读 governance_facts（由 GovernanceFactsBuilder 产出）
    - 以及 Phase-2 FactorResult 的“level / details.data_status”
    - 不依赖 score，不引入新计算
    """

    def build(self) -> Dict[str, Any]:
        """
        governance_facts 预期结构（最小约定）：

        {
            "factors": Dict[str, FactorResult],
            "sources": Dict[str, str]   # 可选，用于审计
        }
        """

        factors: Dict[str, Any] = self.governance_facts.get("factors", {})
        drivers: List[str] = []

        high_risk_count = 0
        data_missing = False

        for name, result in factors.items():
            # -------- 1. 风险语义（唯一可靠） --------
            if result.level == "HIGH":
                high_risk_count += 1
                drivers.append(f"{name}:HIGH")

            # -------- 2. 数据缺失 → 保守偏防守 --------
            details = result.details or {}
            if details.get("data_status") == "DATA_NOT_CONNECTED":
                data_missing = True
                drivers.append(f"{name}:DATA_MISSING")

        # -------- 3. D1 / D2 / D3 判定（冻结规则） --------
        if high_risk_count >= 3:
            code = "D3"
            risk = "-4% ~ -6%"
            meaning = (
                "多项核心因子处于高风险状态，"
                "短期内存在较大回撤或系统性风险，"
                "应以保护本金为优先目标。"
            )

        elif high_risk_count == 2:
            code = "D2"
            risk = "-2.5% ~ -4%"
            meaning = (
                "存在多项风险因子共振，"
                "未来数个交易日内回撤风险显著，"
                "制度上要求主动降低风险敞口。"
            )

        elif high_risk_count == 1 or data_missing:
            code = "D1"
            risk = "-1.5% ~ -2.5%"
            meaning = (
                "已出现风险预警信号，"
                "短期存在回撤可能，"
                "建议提前进行防守性调整。"
            )

        else:
            code = "A"
            risk = "-0% ~ -1%"
            meaning = (
                "当前未观察到显著短期风险信号，"
                "无需提前调整风险敞口。"
            )

        return {
            "code": code,
            "horizon": "2-5D",
            "risk_estimate": risk,
            "meaning": meaning,
            "drivers": drivers,
        }
