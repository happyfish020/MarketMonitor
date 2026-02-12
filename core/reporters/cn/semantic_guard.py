# -*- coding: utf-8 -*-
from __future__ import annotations

"""
UnifiedRisk V12 · Semantic Guard（语义一致性校验）

职责（冻结版）：
- Gate ≠ ALLOW/A 时，扫描 ReportBlock 文本，发现“进攻/加仓/调仓”等暗示性词汇
- 输出 WARN（默认）或 FAIL（抛异常）

兼容性：
- report_engine.py 可能调用 SemanticGuard(mode="WARN")，因此必须支持带参构造
- check(...) 目前在 report_engine 中以 check(gate_final=..., blocks=...) 调用
"""

from typing import Any, Dict, List, Tuple, Optional
import re


class SemanticViolation(Exception):
    """语义一致性违规（FAIL 模式）"""


class SemanticGuard:
    # Gate 允许“进攻语义出现”的唯一值（系统兼容）
    ALLOW_GATES = {"ALLOW", "A"}

    # 默认禁用词（可扩展）
    DEFAULT_PATTERNS: List[Tuple[str, str]] = [
        # (pattern, reason)
        (r"加仓|扩仓|追高", "explicit_add_risk"),
        (r"调仓|换仓|切换至", "rebalance_hint"),
        (r"进攻|进取|积极参与", "offensive_language"),
        (r"动能改善|趋势走强|放量上涨", "momentum_language"),
        (r"可执行|按计划执行|择机执行", "execution_as_permission"),
        (r"建议买入|适合介入", "explicit_buy"),
    ]

    # 简单否定词窗口：如果命中词附近出现这些否定词，则不报警（例如“禁止加仓/不追高”）
    _NEGATIONS = ("不", "禁止", "不可", "不允许", "严禁")

    def __init__(
        self,
        mode: str = "WARN",  # WARN | FAIL
        patterns: Optional[List[Tuple[str, str]]] = None,
        allow_gates: Optional[set] = None,
        **_: Any,
    ) -> None:
        # 允许传入多余参数，防止未来签名扩展导致报错
        self.mode = (mode or "WARN").upper()
        if self.mode not in ("WARN", "FAIL"):
            self.mode = "WARN"
        self.patterns = patterns or list(self.DEFAULT_PATTERNS)
        self.allow_gates = allow_gates or set(self.ALLOW_GATES)

    def check(self, *, gate_final: str, blocks: Dict[str, Any], **kwargs: Any) -> List[str]:
        gate_final = (gate_final or "").upper()
        if gate_final in self.allow_gates:
            return []

        # AttackPermit is a read-only DOS overlay. If it explicitly grants limited/full
        # permission, then “进攻/加仓/可执行”等文字在报告中不应被当作语义违规。
        ap_yes = False
        gov = kwargs.get("governance")
        if isinstance(gov, dict):
            ap = gov.get("attack_permit")
            if isinstance(ap, dict) and str(ap.get("permit") or "").upper() == "YES":
                ap_yes = True

        patterns = self.patterns
        if ap_yes:
            # Keep high-risk “suggest to buy” & momentum marketing language checks,
            # but ignore operational permission / add-risk wording.
            ignore_reasons = {
                "explicit_add_risk",
                "rebalance_hint",
                "offensive_language",
                "execution_as_permission",
            }
            patterns = [(p, r) for (p, r) in self.patterns if r not in ignore_reasons]

        warnings: List[str] = []
        for alias, payload in (blocks or {}).items():
            text = self._payload_to_text(payload)
            if not text.strip():
                continue

            for pattern, reason in patterns:
                for m in re.finditer(pattern, text):
                    if self._is_negated(text, m.start()):
                        continue
                    msg = f"[semantic_guard] gate={gate_final} block={alias} hit='{pattern}' reason={reason}"
                    warnings.append(msg)
                    # 同一 pattern 命中一次即可，避免刷屏
                    break

        if warnings and self.mode == "FAIL":
            raise SemanticViolation("\n".join(warnings))
        return warnings

    @staticmethod
    def _payload_to_text(payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, str):
            return payload
        if isinstance(payload, dict):
            # 常见字段优先
            for k in ("text", "md", "markdown", "content"):
                v = payload.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            # 兜底：拼接所有字符串值（避免把复杂对象 toString 过长）
            parts: List[str] = []
            for v in payload.values():
                if isinstance(v, str) and v.strip():
                    parts.append(v)
            return "\n".join(parts)
        return str(payload)

    @classmethod
    def _is_negated(cls, text: str, idx: int) -> bool:
        # 向左看 6 个字符窗口：出现否定词则认为是“禁止/不允许”的语境
        left = text[max(0, idx - 6): idx]
        return any(n in left for n in cls._NEGATIONS)
