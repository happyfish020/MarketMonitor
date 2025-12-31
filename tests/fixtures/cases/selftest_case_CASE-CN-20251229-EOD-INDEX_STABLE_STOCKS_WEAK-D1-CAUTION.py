# -*- coding: utf-8 -*-
"""Selftest regression for CASE-CN-20251229-EOD-INDEX_STABLE_STOCKS_WEAK-D1-CAUTION

Usage:
- Generate today's report text (EOD) to a variable `text`
- Then call `assert_case(text)`
"""
import json
from pathlib import Path

CASE_PATH = Path(r"/mnt/data/CASE-CN-20251229-EOD-INDEX_STABLE_STOCKS_WEAK-D1-CAUTION.json")

def _count_substr(text: str, sub: str) -> int:
    i = 0
    n = 0
    while True:
        j = text.find(sub, i)
        if j < 0:
            return n
        n += 1
        i = j + len(sub)

def assert_case(report_text: str) -> None:
    case = json.loads(CASE_PATH.read_text(encoding="utf-8"))
    exp = case["expected"]

    for bad in exp["must_not_contain"]:
        assert bad not in report_text, f"forbidden token found: {bad}"

    # Execution must be single value
    assert "执行评级：D1" in report_text or "执行评级： D1" in report_text, "execution band D1 missing"

    # Gate hint
    assert "Gate=CAUTION" in report_text, "Gate=CAUTION missing in summary"

    # Caution hint should not duplicate within a single meaning line
    hint = "（提示：该状态不等于允许进攻。）"
    if exp.get("caution_hint_no_duplicate"):
        assert _count_substr(report_text, hint) <= 2, "caution hint appears too many times (duplicate likely)"

    # Must contain at least one 'feel' token
    ok = any(t in report_text for t in exp["must_contain_any"])
    assert ok, "feel tokens missing (expected at least one of: " + ", ".join(exp["must_contain_any"]) + ")"
