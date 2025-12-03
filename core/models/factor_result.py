from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class FactorResult:
    name: str
    score: float
    signal: str
    raw: Dict[str, Any]
