from __future__ import annotations

from typing import Dict, Any, List


class CaseValidationError(Exception):
    pass


# =========================
# 语义规则（冻结）
# =========================

NEGATION_PREFIXES = [
    "不",
    "未",
    "无",
    "禁止",
    "避免",
    "不支持",
    "不构成",
    "不适合",
    "不允许",
]

FORBIDDEN_ACTION_PHRASES = [
    "进攻",
    "加仓",
    "扩大风险敞口",
    "追高",
]


def _is_negated(text: str, keyword: str) -> bool:
    """
    判断 keyword 是否处于否定语义中
    规则：keyword 前若干字符内出现否定前缀
    """
    idx = text.find(keyword)
    if idx == -1:
        return False

    window = text[max(0, idx - 6): idx]
    return any(neg in window for neg in NEGATION_PREFIXES)


def validate_case(
    *,
    case_path: str,
    gate_final: str,
    summary_code: str,
    structure: Dict[str, Any],
    report_text: str,
) -> None:
    """
    Case 校验（制度冻结）

    - Gate / Summary / Structure 一致性
    - 语义约束（支持否定语义）
    """

    import yaml

    with open(case_path, "r", encoding="utf-8") as f:
        case = yaml.safe_load(f)

    expected = case.get("expected", {})

    # =========================
    # Gate 校验
    # =========================
    exp_gate = expected.get("gate", {}).get("final")
    if exp_gate and gate_final != exp_gate:
        raise CaseValidationError(
            f"[CASE] gate mismatch: expect={exp_gate}, got={gate_final}"
        )

    # =========================
    # Summary 校验
    # =========================
    exp_summary = expected.get("action_hint", {}).get("summary_code")
    if exp_summary and summary_code != exp_summary:
        raise CaseValidationError(
            f"[CASE] summary mismatch: expect={exp_summary}, got={summary_code}"
        )

    # =========================
    # Structure 校验
    # =========================
    exp_structs = expected.get("structure", {})
    for key, exp_struct in exp_structs.items():
        actual = structure.get(key)
        if not isinstance(actual, dict):
            raise CaseValidationError(f"[CASE] missing structure key: {key}")

        for field, exp_val in exp_struct.items():
            act_val = actual.get(field)
            if act_val != exp_val:
                raise CaseValidationError(
                    f"[CASE] structure mismatch: {key}.{field} "
                    f"expect={exp_val}, got={act_val}"
                )

    # =========================
    # 语义校验（关键修正点）
    # =========================
    for keyword in FORBIDDEN_ACTION_PHRASES:
        if keyword in report_text:
            if _is_negated(report_text, keyword):
                # 属于“不要进攻”这类解释性语句 → 允许
                continue

            raise CaseValidationError(
                f"[CASE] forbidden action semantic detected: '{keyword}'"
            )
