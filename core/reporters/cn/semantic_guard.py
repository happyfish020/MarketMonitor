# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, List, Tuple
import re

class SemanticViolation(Exception):
    """语义一致性违规（可选 FAIL 模式）"""


class SemanticGuard:
    """
    UnifiedRisk V12 · Semantic Guard（语义一致性校验）

    职责：
    - 在 Gate ≠ A 时，扫描所有 ReportBlock 文本
    - 发现“进攻 / 调仓 / 扩仓”等暗示性词汇 → 记录告警或失败
    """

    # Gate 允许进攻的唯一值（与你系统一致）
    ALLOW_GATES = {"ALLOW", "A"}

    # 默认禁用词（可扩展）
    FORBIDDEN_PATTERNS: List[Tuple[str, str]] = [
        # (pattern, reason)
        (r"加仓|扩仓|追高", "explicit_add_risk"),
        (r"调仓|换仓|切换至", "rebalance_hint"),
        (r"进攻|进取|积极参与", "offensive_language"),
        (r"动能改善|趋势走强|放量上涨", "momentum_language"),
        (r"可执行|按计划执行|择机执行", "execution_as_permission"),
        (r"建议买入|适合介入", "explicit_buy"),
    ]

    def __init__(
        self,
        mode: str = "WARN",  # WARN | FAIL
        extra_patterns: List[Tuple[str, str]] | None = None,
    ) -> None:
        assert mode in ("WARN", "FAIL")
        self.mode = mode
        self.patterns = list(self.FORBIDDEN_PATTERNS)
        if extra_patterns:
            self.patterns.extend(extra_patterns)

    def check(
        self,
        gate_final: str,
        blocks: Dict[str, str],
    ) -> List[str]:
        """
        执行语义校验

        参数：
        - gate_final: 最终 Gate
        - blocks: {block_alias: block_payload_text}

        返回：
        - warnings: 语义违规描述列表
        """
        warnings: List[str] = []

        # Gate 允许进攻 → 不校验
        if gate_final in self.ALLOW_GATES:
            return warnings

        for alias, text in blocks.items():
            if not isinstance(text, str) or not text.strip():
                continue

            for pattern, reason in self.patterns:
                if re.search(pattern, text):
                    msg = (
                        f"[semantic_guard] gate={gate_final} "
                        f"block={alias} hit='{pattern}' reason={reason}"
                    )
                    warnings.append(msg)

        if warnings and self.mode == "FAIL":
            raise SemanticViolation("\n".join(warnings))

        return warnings
