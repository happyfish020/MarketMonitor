# core/factors/base.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from core.models.factor_result import FactorResult
 
class BaseFactor(ABC):
    """
    全因子基类（V12 通用版）

    要求实现：
        compute(snapshot) → FactorResult
    """

    name: str = "base_factor"

    @abstractmethod
    def compute(self, snapshot: dict) -> FactorResult:
        raise NotImplementedError("Factor must implement compute()")

    # 工具：用于简化返回结果
    def result(self, score: float, detail: str = "") -> FactorResult:
        return FactorResult(self.name, score, detail)

    #def __init__(self, name: str):
    #    self.name = name

    
