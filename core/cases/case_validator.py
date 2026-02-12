from __future__ import annotations

from typing import Dict, Any, List, Optional
import re


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
    """判断 keyword 是否处于否定语义中。

    规则：keyword 前 6 个字符窗口内出现否定前缀 → 认为是“禁止/不允许/不支持”的解释语境。
    """
    idx = text.find(keyword)
    if idx == -1:
        return False
    window = text[max(0, idx - 6): idx]
    return any(neg in window for neg in NEGATION_PREFIXES)


def _parse_pct(report_text: str, patterns: List[str]) -> Optional[float]:
    """从 report_text 中按 patterns 解析百分比数值（返回 float，例如 71.6）。"""
    for pat in patterns:
        m = re.search(pat, report_text)
        if m:
            try:
                return float(m.group(1))
            except Exception:
                return None
    return None


def _has_attack_permit_yes(report_text: str) -> bool:
    """判断报告文本中是否明确出现 AttackPermit=YES（LIMITED/FULL）。"""
    # accept multiple render styles
    if re.search(r"permit\s*=\s*YES", report_text):
        return True
    if "AttackPermit=🟡" in report_text or "AttackPermit=🟢" in report_text:
        return True
    if re.search(r"AttackPermit：.*YES", report_text):
        return True
    return False


def validate_case(
    *,
    case_path: str,
    gate_final: str,
    summary_code: str,
    structure: Dict[str, Any],
    report_text: str,
) -> None:
    """Case 校验（制度冻结）

    - Gate / Summary / Structure 一致性
    - 语义约束（支持否定语义）
    - 回退防线（Regression Guards, P0）
    """
    import yaml

    with open(case_path, "r", encoding="utf-8") as f:
        case = yaml.safe_load(f)

    expected = case.get("expected", {}) if isinstance(case, dict) else {}

    # =========================
    # Gate 校验
    # =========================
    exp_gate = _safe_get(expected, ["gate", "final"])
    if exp_gate and gate_final != exp_gate:
        raise CaseValidationError(
            f"[CASE] gate mismatch: expect={exp_gate}, got={gate_final}"
        )

    # =========================
    # Summary 校验
    # =========================
    exp_summary = _safe_get(expected, ["action_hint", "summary_code"])
    if exp_summary and summary_code != exp_summary:
        raise CaseValidationError(
            f"[CASE] summary mismatch: expect={exp_summary}, got={summary_code}"
        )

    # =========================
    # Structure 校验
    # =========================
    exp_structs = expected.get("structure", {}) if isinstance(expected, dict) else {}
    if isinstance(exp_structs, dict):
        for key, exp_struct in exp_structs.items():
            actual = structure.get(key)
            if not isinstance(actual, dict):
                raise CaseValidationError(f"[CASE] missing structure key: {key}")

            if isinstance(exp_struct, dict):
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
    # 如果报告明确给出 AttackPermit=YES（LIMITED/FULL），则“进攻/加仓”等词汇可能出现在
    # “进攻许可/允许/禁止”结构化段落中，不应被当作违规语义。
    ap_yes = _has_attack_permit_yes(report_text)

    if not ap_yes:
        for keyword in FORBIDDEN_ACTION_PHRASES:
            if keyword in report_text:
                if _is_negated(report_text, keyword):
                    continue
                raise CaseValidationError(
                    f"[CASE] forbidden action semantic detected: '{keyword}'"
                )

    # =========================
    # 回退防线（Regression Guards）· P0
    # =========================
    # 当“上涨占比高 + Top20 集中度低”满足进攻许可典型条件时，
    # 报告必须显式包含 AttackPermit 与关键动作枚举，否则认为链路回退。
    adv_pct = _parse_pct(
        report_text,
        patterns=[
            r"上涨占比\s*[：:]\s*([0-9.]+)%",
            r"adv_ratio\s*[：:]\s*([0-9.]+)%",
            r"-\s*adv_ratio\s*[：:]\s*([0-9.]+)%",
        ],
    )
    top20_pct = _parse_pct(
        report_text,
        patterns=[
            r"Top20\s*成交集中度\(top20_ratio\)\s*([0-9.]+)%",
            r"top20_ratio（成交集中度）：\s*([0-9.]+)%",
            r"-\s*top20_ratio[^\n]*\s([0-9.]+)%",
        ],
    )

    if adv_pct is not None and top20_pct is not None:
        if adv_pct >= 68.0 and top20_pct <= 16.0:
            required = [
                "AttackPermit",
                "BASE_ETF_ADD",
                "PULLBACK_ADD",
                "覆盖提示：AttackPermit",
            ]
            missing = [k for k in required if k not in report_text]
            if missing:
                raise CaseValidationError(
                    "[CASE] regression guard failed: expected offensive permit signals missing: "
                    + ", ".join(missing)
                    + f" (adv_pct={adv_pct}, top20_pct={top20_pct})"
                )


def _safe_get(d: Any, path: List[str]) -> Optional[Any]:
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur
