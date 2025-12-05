# -*- coding: utf-8 -*-
"""
统一因子输出容器（UnifiedRisk V11.7 FINAL）
-------------------------------------------
该类是所有因子的标准返回数据包装器（松耦合）。
完全兼容旧版，同时支持新版扩展字段。
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional


@dataclass
class FactorResult:
    name: str                       # 因子名称
    score: float                    # 标准化得分 0~100
    level: Optional[str] = None     # 文本区间（强、中性、弱）
    signal: Optional[str] = None    # 简短信号
    details: Dict[str, Any] = field(default_factory=dict)   # 结构化因子明细
    raw: Dict[str, Any] = field(default_factory=dict)       # 因子内部原始数据
    report_block: Optional[str] = None                      # 报告文本（可自定义）

    # ============================================================
    # 自定义构造函数（兼容旧接口）
    # ============================================================
    def __init__(
        self,
        name: str,
        score: float,
        details: Dict[str, Any],
        level: Optional[str] = None,
        signal: Optional[str] = None,
        raw: Optional[Dict[str, Any]] = None,
        report_block: Optional[str] = None,
    ):
        self.name = name
        self.score = float(score)
        self.details = details or {}
        self.level = level or self.details.get("level") or ""
        self.signal = signal or ""
        self.raw = raw or {}
        self.report_block = report_block

    # ============================================================
    # 兜底报告生成：因子未提供 report_block 时使用
    # ============================================================
    def ensure_report_block(self) -> str:
        if self.report_block:
            return self.report_block

        # 默认报告结构
        lines = []
        lines.append(f"  - {self.name}: {self.score:.2f}（{self.level}）")

        # 展开 details 字段
        for k, v in self.details.items():
            if k == "level":
                continue
            lines.append(f"      · {k}: {v}")

        lines.append("")  # 空行分隔
        return "\n".join(lines)
