from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class FactorResult:
    """通用因子结果封装"""
    name: str
    score: float
    level: str
    desc: str
    details: Dict[str, Any]
