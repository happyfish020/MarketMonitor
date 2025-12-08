# core/models/factor_result.py
"""
UnifiedRisk V12
FactorResult 统一数据结构：
  - score: 因子得分（0~100）
  - desc:  因子简述（报告摘要用途）
  - detail: 因子详细信息（报告全文用途）
"""

from dataclasses import dataclass


@dataclass
class FactorResult:
    name = ""
    score: float
    desc: str = ""
    detail: str = ""

    def to_dict(self):
        return {
            "score": self.score,
            "desc": self.desc,
            "detail": self.detail,
        }

    def __init__(self):
        pass  
    