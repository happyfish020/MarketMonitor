from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class FactorResult:
    """统一的因子输出结果容器（松耦合版）。

    - name: 因子名称（如 'north_nps'）
    - score: 0~100 的标准化得分
    - signal: 一句话信号总结（用于简表 / UI）
    - raw:   因子内部的原始数据明细（仅供调试或后续扩展使用）
    - report_block: 该因子在文本报告中的完整展示块（由因子内部封装）
    """

    name: str
    score: float
    signal: str
    raw: Dict[str, Any]
    report_block: Optional[str] = None

    def ensure_report_block(self) -> str:
        """如果因子未提供自定义 report_block，则回退为通用格式。"""
        if self.report_block:
            return self.report_block

        # 通用兜底格式：只显示名称、得分和 signal
        return f"  - {self.name}: {self.score:.2f}（{self.signal}）\n"
