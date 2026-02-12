# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · Audit Diff
- Deterministic, schema-friendly JSON diff
- Designed for persisted report_dump / slots / gate_decision comparison

Key design:
- Path format: JSON Pointer-ish, e.g. /structure/turnover/state
- No silent coercions
- Float tolerance support
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import fnmatch
import math


Json = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


@dataclass(frozen=True)
class DiffItem:
    path: str
    kind: str   # 'missing' | 'extra' | 'value' | 'type'
    a: Any
    b: Any

    def to_dict(self) -> Dict[str, Any]:
        return {"path": self.path, "kind": self.kind, "a": self.a, "b": self.b}


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _float_equal(a: Any, b: Any, atol: float) -> bool:
    if not (_is_number(a) and _is_number(b)):
        return False
    af = float(a); bf = float(b)
    if math.isnan(af) and math.isnan(bf):
        return True
    return abs(af - bf) <= atol


def _ignored(path: str, ignore_globs: Sequence[str]) -> bool:
    for g in ignore_globs:
        if fnmatch.fnmatch(path, g):
            return True
    return False


def diff_json(
    a: Json,
    b: Json,
    *,
    float_atol: float = 0.0,
    ignore_globs: Optional[Sequence[str]] = None,
    _path: str = "",
) -> List[DiffItem]:
    """
    Compare two JSON-compatible structures and return a list of DiffItems.
    """
    ignore_globs = list(ignore_globs or [])

    # Always compare root even if ignored? Convention: if root ignored, ignore all.
    if _ignored(_path or "/", ignore_globs):
        return []

    # Type mismatch (but allow int/float mix)
    if type(a) != type(b):
        if _is_number(a) and _is_number(b) and float_atol >= 0.0:
            if _float_equal(a, b, float_atol):
                return []
            return [DiffItem(path=_path or "/", kind="value", a=a, b=b)]
        return [DiffItem(path=_path or "/", kind="type", a=type(a).__name__, b=type(b).__name__)]

    # Dict
    if isinstance(a, dict):
        diffs: List[DiffItem] = []
        a_keys = set(a.keys())
        b_keys = set(b.keys())
        for k in sorted(a_keys - b_keys):
            p = f"{_path}/{k}" if _path else f"/{k}"
            if _ignored(p, ignore_globs):
                continue
            diffs.append(DiffItem(path=p, kind="missing", a=a.get(k), b=None))
        for k in sorted(b_keys - a_keys):
            p = f"{_path}/{k}" if _path else f"/{k}"
            if _ignored(p, ignore_globs):
                continue
            diffs.append(DiffItem(path=p, kind="extra", a=None, b=b.get(k)))
        for k in sorted(a_keys & b_keys):
            p = f"{_path}/{k}" if _path else f"/{k}"
            diffs.extend(diff_json(a.get(k), b.get(k), float_atol=float_atol, ignore_globs=ignore_globs, _path=p))
        return diffs

    # List
    if isinstance(a, list):
        diffs: List[DiffItem] = []
        if len(a) != len(b):
            diffs.append(DiffItem(path=_path or "/", kind="value", a=f"len={len(a)}", b=f"len={len(b)}"))
        n = min(len(a), len(b))
        for i in range(n):
            p = f"{_path}/{i}" if _path else f"/{i}"
            diffs.extend(diff_json(a[i], b[i], float_atol=float_atol, ignore_globs=ignore_globs, _path=p))
        # remaining tail is already captured by len diff, but include items for visibility
        for i in range(n, len(a)):
            p = f"{_path}/{i}" if _path else f"/{i}"
            if _ignored(p, ignore_globs):
                continue
            diffs.append(DiffItem(path=p, kind="missing", a=a[i], b=None))
        for i in range(n, len(b)):
            p = f"{_path}/{i}" if _path else f"/{i}"
            if _ignored(p, ignore_globs):
                continue
            diffs.append(DiffItem(path=p, kind="extra", a=None, b=b[i]))
        return diffs

    # Scalars
    if _is_number(a) and float_atol >= 0.0:
        if _float_equal(a, b, float_atol):
            return []
        if a == b:
            return []
        return [DiffItem(path=_path or "/", kind="value", a=a, b=b)]

    if a != b:
        return [DiffItem(path=_path or "/", kind="value", a=a, b=b)]
    return []


def diffs_to_markdown(diffs: Sequence[DiffItem], *, title: str = "Audit Diff") -> str:
    lines: List[str] = [f"# {title}", ""]
    if not diffs:
        lines += ["✅ No differences.", ""]
        return "\n".join(lines)

    lines += [f"- Total diffs: **{len(diffs)}**", ""]
    lines += ["| Path | Kind | A | B |", "|---|---|---|---|"]
    for d in diffs[:500]:
        a = repr(d.a)
        b = repr(d.b)
        # Avoid giant blobs
        if len(a) > 120: a = a[:117] + "..."
        if len(b) > 120: b = b[:117] + "..."
        lines.append(f"| `{d.path}` | {d.kind} | `{a}` | `{b}` |")
    if len(diffs) > 500:
        lines += ["", f"> Truncated. Showing first 500 diffs of {len(diffs)}."]
    lines.append("")
    return "\n".join(lines)
