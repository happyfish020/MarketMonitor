# core/factors/base_factor.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Literal
from core.factors.factor_result import FactorResult

RiskLevel = Literal["LOW", "NEUTRAL", "HIGH"]

 
class FactorBase(ABC):
    """
    V12 Factor 基类（铁律 A/B/C/D）：

    ✅ Factor 只能接收标准化 input_block(dict) 并返回 FactorResult
    ❌ 不得访问 DataSource / cache / history / snapshot 细节 / 外部 API / 文件系统
    """

    #: 默认风险阈值（可被子类覆盖，但不允许改接口）
    #: 例：score>=70 => HIGH, score<=30 => LOW，否则 NEUTRAL
    HIGH_THRESHOLD: float = 70.0
    LOW_THRESHOLD: float = 30.0

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Factor name must be a non-empty str")
        self._name = name.strip()

    @property
    def name(self) -> str:
        return self._name

    def __call__(self, input_block: Dict[str, Any]) -> FactorResult:
        """
        允许 predictor 以函数方式调用 factor，但不改变统一入口。
        """
        return self.compute(input_block)

    @abstractmethod
    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        """
        统一接口（铁律 B）：子类必须实现且不得改签名。
        """
        raise NotImplementedError

    # ---------------------------
    # helpers (不触碰铁律的最小公共能力)
    # ---------------------------

    @staticmethod
    def _require_dict(input_block: Any, *, factor_name: Optional[str] = None) -> Dict[str, Any]:
        if not isinstance(input_block, dict):
            prefix = f"[{factor_name}] " if factor_name else ""
            raise TypeError(f"{prefix}input_block must be a dict")
        return input_block

    @staticmethod
    def clamp_score(score: float) -> float:
        """
        将分数裁剪到 [0, 100]。用于子类内部安全处理。
        """
        if not isinstance(score, (int, float)):
            raise TypeError("score must be a number")
        if score < 0:
            return 0.0
        if score > 100:
            return 100.0
        return float(score)

    @classmethod
    def level_from_score(cls, score: float) -> RiskLevel:
        """
        按阈值将 score 映射为 level（LOW/NEUTRAL/HIGH）。
        """
        s = cls.clamp_score(score)
        if s >= cls.HIGH_THRESHOLD:
            return "HIGH"
        if s <= cls.LOW_THRESHOLD:
            return "LOW"
        return "NEUTRAL"

    def build_result(
        self,
        *,
        score: float,
        level: Optional[RiskLevel] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> FactorResult:
        """
        构造 FactorResult 的统一出口：
        - 自动 clamp_score
        - level 未传则由阈值生成
        - details 默认 {}
        """
        s = self.clamp_score(score)
        lv: RiskLevel = level if level is not None else self.level_from_score(s)
        dt = details if details is not None else {}
        return FactorResult(name=self.name, score=s, level=lv, details=dt)

    @staticmethod
    def pick(mapping: Mapping[str, Any], key: str, default: Any = None) -> Any:
        """
        读取 input_block 常用安全取值（不对结构做假设，仅提供工具）。
        """
        if not isinstance(mapping, Mapping):
            return default
        return mapping.get(key, default)
