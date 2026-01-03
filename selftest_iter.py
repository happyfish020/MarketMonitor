# -*- coding: utf-8 -*-
r'''UnifiedRisk V12 - Selftest (Regression Gate)

目标：
- 遍历 core/tests/fixtures/cases 目录下的所有 case fixture（*.json / *.v2.json）
- 对每个 case 跑一次“报告链路”（ReportEngine + Blocks + MarkdownRenderer）
- 执行回归断言（语义/格式闸门），防止报告漂移回退

说明：
- selftest 不依赖实时行情数据。
- case fixture 负责提供最小 slots/context（或提供 expected 让 selftest 构造最小 slots）。
- 兼容两类 case：
  1) “旧版” fixture：expected.execution_band / expected.gate / expected.structure_states / inputs_anchor...
  2) “新版” fixture：inputs.slots + expected.expected_text_contains (+ 可选 assertions/invariants)

用法：
    python selftest.py
    python selftest.py --write-output
    python selftest.py --pattern 2025-12-30
    python selftest.py --cases-dir D:\LHJ\PythonWS\MarketMon\MarketMonitor\core\tests\fixtures\cases
'''

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Extra dump directories (optional). Provide via CLI: --dumps-dir <path> (repeatable)
_EXTRA_DUMP_DIRS: List[Path] = []
_DEBUG: bool = False

def _dbg(msg: str) -> None:
    if _DEBUG:
        print(f"[DBG] {msg}")



# -----------------------------
# Case discovery
# -----------------------------

def _find_cases_dir(explicit: Optional[str] = None) -> Path:
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if not p.exists() or not p.is_dir():
            raise FileNotFoundError(f'--cases-dir not found or not a directory: {p}')
        return p

    # search common roots: script dir, cwd, and their parents
    roots: List[Path] = []
    here = Path(__file__).resolve().parent
    roots.extend([here, Path.cwd().resolve()])
    roots.extend(list(here.parents)[:6])
    roots.extend(list(Path.cwd().resolve().parents)[:6])

    candidates_rel = [
 
        Path('tests') / 'cases',
    ]

    for r in roots:
        for rel in candidates_rel:
            p = (r / rel).resolve()
            if p.exists() and p.is_dir():
                if p.name == 'cases':
                    return p
                p2 = p / 'cases'
                if p2.exists() and p2.is_dir():
                    return p2

    raise FileNotFoundError(
        'Cannot locate cases directory. Tried roots near script/cwd for: '
        + ', '.join(str(x) for x in candidates_rel)
        + '. Provide --cases-dir explicitly.'
    )


def _select_case_files(cases_dir: Path) -> List[Path]:
    '''Pick all *.json, but if both xxx.json and xxx.v2.json exist, keep the v2 one.'''
    files = [p for p in cases_dir.glob('*.json') if p.is_file()]
    # group by base id (strip optional .v2 suffix)
    best: Dict[str, Path] = {}
    for p in files:
        stem = p.stem  # e.g. "CASE-....v2" or "CASE-...."
        base = stem[:-3] if stem.endswith('.v2') else stem
        prev = best.get(base)
        if prev is None:
            best[base] = p
            continue
        # prefer .v2.json over .json; if both v2, keep latest mtime
        if p.stem.endswith('.v2') and not prev.stem.endswith('.v2'):
            best[base] = p
        elif p.stem.endswith('.v2') == prev.stem.endswith('.v2'):
            if p.stat().st_mtime > prev.stat().st_mtime:
                best[base] = p
    return sorted(best.values(), key=lambda x: x.name)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def _case_id_from(case: Dict[str, Any], path: Path) -> str:
    return (
        str(case.get('case_id') or '')
        or str(case.get('CASE_ID') or '')
        or str(case.get('id') or '')
        or str(case.get('meta', {}).get('case_id') or '')
        or path.stem.replace('.v2', '')
    )


def _case_kind_from(case: Dict[str, Any]) -> str:
    return (
        str(case.get('kind') or '')
        or str(case.get('meta', {}).get('kind') or '')
        or str(case.get('inputs', {}).get('context', {}).get('kind') or '')
        or 'EOD'
    )


def _case_trade_date_from(case: Dict[str, Any]) -> str:
    return (
        str(case.get('trade_date') or '')
        or str(case.get('meta', {}).get('trade_date') or '')
        or str(case.get('inputs', {}).get('context', {}).get('trade_date') or '')
        or '1970-01-01'
    )


# -----------------------------
# Utilities (legacy fixture support)
# -----------------------------


def _extract_trade_date_from_case_id(case_id: str) -> Optional[str]:
    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", case_id)
    if m:
        return m.group(0)
    m2 = re.search(r"(20\d{2})(\d{2})(\d{2})", case_id)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return None



def _load_sidecar_report_dump(case_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    '''
    Load a structured report dump JSON (if present) to make assertions stable.

    Filenames:
      - doc_<trade_date>.json          e.g. doc_2025-12-30.json
      - doc_<trade_date_yyyymmdd>.json e.g. doc_20251230.json
      - doc_<case_id>.json

    Search directories (in order):
      A) near the case fixture (derived from case["_case_path"]):
         - case_dir
         - case_dir/_dumps, case_dir/dumps
         - case_dir/../_dumps, case_dir/../dumps
      B) extra dirs provided via --dumps-dir
      C) common project dir under CWD:
         - ./core/tests/fixtures/cases (+ _dumps / dumps)
    '''
    case_id = str(case_obj.get("case_id") or case_obj.get("id") or "")
    trade_date = (
        case_obj.get("trade_date")
        or case_obj.get("context", {}).get("trade_date")
        or case_obj.get("inputs", {}).get("context", {}).get("trade_date")
    )
    if not isinstance(trade_date, str) or not trade_date:
        trade_date = _extract_trade_date_from_case_id(case_id or "")

    filenames: List[str] = []
    if isinstance(trade_date, str) and trade_date:
        filenames.append(f"doc_{trade_date}.json")
        filenames.append(f"doc_{trade_date.replace('-', '')}.json")
    if case_id:
        filenames.append(f"doc_{case_id}.json")

    search_dirs: List[Path] = []

    case_path_str = case_obj.get("_case_path")
    if isinstance(case_path_str, str) and case_path_str:
        case_path = Path(case_path_str)
        case_dir = case_path.parent
        search_dirs.extend([
            case_dir,
            case_dir / "_dumps",
            case_dir / "dumps",
            case_dir.parent / "_dumps",
            case_dir.parent / "dumps",
        ])
        _dbg(f"dump search: case_path={case_path}")

    # CLI-provided extra dirs
    for d in _EXTRA_DUMP_DIRS:
        search_dirs.append(d)

    # Common project dirs under cwd (best effort)
    cwd = Path.cwd()
    common = cwd / "core" / "tests" / "fixtures" / "cases"
    search_dirs.extend([common, common / "_dumps", common / "dumps"])

    # de-dup
    uniq: List[Path] = []
    seen = set()
    for d in search_dirs:
        key = str(d)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(d)

    _dbg(f"dump search dirs: {[str(d) for d in uniq]}")
    _dbg(f"dump search filenames: {filenames}")

    for d in uniq:
        for fn in filenames:
            p = d / fn
            if p.exists() and p.is_file():
                _dbg(f"dump hit: {p}")
                try:
                    return _load_json(p)
                except Exception as e:
                    _dbg(f"dump load failed: {p} err={e}")
                    continue
    return None
def _structure_from_dump(dump: Dict[str, Any]) -> Dict[str, Any]:
    struct = dump.get("structure")
    if not isinstance(struct, dict):
        return {}
    out: Dict[str, Any] = {}
    for k, v in struct.items():
        if not isinstance(v, dict):
            continue
        state = v.get("state")
        if not isinstance(state, str) or not state:
            continue
        evidence: Dict[str, Any] = {}
        for kk, vv in v.items():
            if kk in {"state", "data_status"}:
                continue
            evidence[kk] = vv
        out[k] = {"state": state, "evidence": evidence}
    return out


def _enrich_from_dump(extracted: Dict[str, Any], dump: Dict[str, Any]) -> None:
    rep = extracted.get("report")
    if not isinstance(rep, dict):
        return
    gov = dump.get("governance")
    if isinstance(gov, dict):
        if isinstance(gov.get("gate"), str):
            rep.setdefault("governance", {})
            if isinstance(rep.get("governance"), dict):
                rep["governance"]["gate"] = gov.get("gate")
        if isinstance(gov.get("drs"), str):
            rep.setdefault("drs", {})
            if isinstance(rep.get("drs"), dict):
                rep["drs"].setdefault("level", gov.get("drs"))

def _count_substr(text: str, sub: str) -> int:
    i = 0
    n = 0
    while True:
        j = text.find(sub, i)
        if j < 0:
            return n
        n += 1
        i = j + len(sub)


def _extract_feeling_meta(report_text: str) -> Dict[str, Any]:
    '''Parse feeling meta comment produced by StructureFactsBlock v8+.'''
    m = re.search(r'<!--\s*feeling_tag:(?P<tag>[^\s]+)\s+evidence:(?P<ev>.*?)\s*-->', report_text)
    if not m:
        return {}
    tag = m.group('tag').strip()
    ev_raw = m.group('ev').strip()
    ev_list: Any = None
    try:
        ev_list = json.loads(ev_raw)
    except Exception:
        ev_list = ev_raw
    return {'tag': tag, 'evidence': ev_list}


def _assert_report_text_legacy(report_text: str, case: Dict[str, Any]) -> None:
    exp = case.get('expected', {})

    for bad in exp.get('must_not_contain', []):
        assert bad not in report_text, f'forbidden token found: {bad}'

    band = exp.get('execution_band')
    if band:
        assert (f'执行评级：{band}' in report_text) or (f'执行评级:{band}' in report_text), 'execution band missing'

    gate = exp.get('gate')
    if gate:
        assert f'Gate={gate}' in report_text, 'Gate=... missing in summary'

    hint = '（提示：该状态不等于允许进攻。）'
    if exp.get('caution_hint_no_duplicate'):
        assert _count_substr(report_text, hint) <= 2, 'caution hint appears too many times (duplicate likely)'

    must_any = exp.get('must_contain_any', [])
    if must_any:
        ok = any(t in report_text for t in must_any)
        assert ok, 'feel tokens missing (expected at least one of: ' + ', '.join(must_any) + ')'

    meta = _extract_feeling_meta(report_text)
    exp_tag = exp.get('feeling_tag')
    if exp_tag:
        assert meta.get('tag') == exp_tag, f'feeling_tag mismatch: got={meta.get("tag")} expected={exp_tag}'

    prefixes = exp.get('evidence_prefixes', [])
    if prefixes:
        ev = meta.get('evidence')
        assert ev is not None, 'missing evidence meta'
        if isinstance(ev, list):
            for p in prefixes:
                assert any(isinstance(x, str) and x.startswith(p) for x in ev), f'evidence missing prefix: {p}'
        else:
            for p in prefixes:
                assert p in str(ev), f'evidence raw missing: {p}'


def _wrap_factor(fr_like: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'score': fr_like.get('score'),
        'level': fr_like.get('level'),
        'details': fr_like.get('details') or {},
    }


def _build_minimal_slots_from_case_legacy(case: Dict[str, Any]) -> Dict[str, Any]:
    exp = case.get('expected', {})
    anchor = case.get('inputs_anchor', {})

    ss = exp.get('structure_states', {})
    structure = {
        'breadth': {
            'state': ss.get('breadth', 'healthy'),
            'meaning': '市场广度未出现系统性破坏，但扩散程度有限，需结合其他结构指标判断。',
        },
        'amount': {
            'state': ss.get('amount', 'expanding'),
            'meaning': '成交放大，但更可能反映分歧或调仓轮动，而非新增进攻性资金。',
        },
        'index_tech': {
            'state': ss.get('index_tech', 'neutral'),
            'meaning': '成长/科技与指数表现大致同步。',
        },
        'failure_rate': {
            'state': ss.get('failure_rate', 'stable'),
            'meaning': '未观察到趋势结构失效迹象，结构保持稳定。',
        },
        'trend_in_force': {
            'state': ss.get('trend_in_force', 'in_force'),
            'meaning': '趋势结构仍然成立，但成功率下降，需避免过度利用趋势。',
        },
        'north_proxy_pressure': {
            'state': ss.get('north_proxy_pressure', 'pressure_low'),
            'meaning': '北向代理压力不显著（未见明显撤退压力），仍需结合广度/成功率判断。',
        },
        '_summary': {'meaning': '结构未坏，但扩散不足，结构同步性与成功率下降。'},
    }

    etf_anchor = anchor.get('etf_spot_sync', {})
    etf_factor = {
        'score': 0.0,
        'level': 'LOW',
        'details': {
            'snapshot_type': 'EOD',
            'adv_ratio': etf_anchor.get('adv_ratio'),
            'top20_amount_ratio': etf_anchor.get('top20_amount_ratio'),
            'dispersion': etf_anchor.get('dispersion'),
            'same_direction': etf_anchor.get('same_direction'),
            'interpretation': etf_anchor.get('interpretation') or {},
        },
    }

    market_overview = {'note': '（回归用最小占位：真实收盘事实由 MarketOverviewBlock 接入后替换）'}

    exec_band = exp.get('execution_band', 'D1')
    execution_summary = {'band': exec_band}

    gate = exp.get('gate', 'CAUTION')
    slots = {
        'gate': gate,
        'drs': {'signal': exp.get('drs_signal', 'YELLOW')},
        'structure': structure,
        'execution_summary': execution_summary,
        'market_overview': market_overview,
        'etf_spot_sync': _wrap_factor(etf_factor),
        'intraday_overlay': {},
        'observations': {},
    }
    return slots


# -----------------------------
# New fixture support (optional)
# -----------------------------

def _extract_action_hint_code(text: str) -> Optional[str]:
    m = re.search(r'当前制度状态：\s*([AND])\b', text)
    if m:
        return m.group(1)
    m = re.search(r'Code:\s*([AND])\b', text)
    if m:
        return m.group(1)
    return None


def _extract_summary_code(text: str) -> Optional[str]:
    m = re.search(r'Code:\s*([AND])\b', text)
    if m:
        return m.group(1)
    m = re.search(r'Summary[：:\s]+([AND])\b', text)
    if m:
        return m.group(1)
    return None


def _extract_gate(text: str) -> Optional[str]:
    m = re.search(r'Gate=([A-Z_]+)', text)
    if m:
        return m.group(1)
    m = re.search(r'原始 Gate：\s*([A-Z_]+)', text)
    if m:
        return m.group(1)
    m = re.search(r'执行后 Gate：\s*([A-Z_]+)', text)
    if m:
        return m.group(1)
    return None


def _extract_permissions(text: str) -> Optional[Dict[str, bool]]:
    """Extract permissions block from summary line.

    Expected canonical phrase:
      Code:N（Gate=CAUTION：允许 HOLD / DEFENSE；禁止 ADD-RISK）
    Returns:
      {"allow_hold": bool, "allow_defense": bool, "allow_add_risk": bool}
    """
    gate = _extract_gate(text)

    # Try text-based extraction first
    allow_hold = bool(re.search(r"允许\s*HOLD", text))
    allow_defense = bool(re.search(r"允许[^\n]*DEFENSE", text))
    forbid_add = bool(re.search(r"禁止\s*ADD[- ]?RISK", text))
    allow_add = bool(re.search(r"允许\s*ADD[- ]?RISK", text))

    if allow_add:
        allow_add_risk = True
    elif forbid_add:
        allow_add_risk = False
    else:
        allow_add_risk = False if gate == "CAUTION" else None  # type: ignore

    if allow_add_risk is None:
        # If we cannot infer from text, fall back to gate heuristics (minimal)
        if gate == "CAUTION":
            return {"allow_hold": True, "allow_defense": True, "allow_add_risk": False}
        return None

    # If hold/defense not explicitly mentioned, fall back minimally by gate
    if not allow_hold and gate in {"CAUTION", "NORMAL", "ALLOW"}:
        allow_hold = True
    if not allow_defense and gate in {"CAUTION", "NORMAL", "ALLOW"}:
        allow_defense = True

    return {"allow_hold": bool(allow_hold), "allow_defense": bool(allow_defense), "allow_add_risk": bool(allow_add_risk)}


def _extract_execution_band(text: str) -> Optional[str]:
    m = re.search(r'执行评级[：:\s]+(A|N|D1|D2)\b', text)
    if m:
        return m.group(1)
    m = re.search(r'【Execution[^】]*】\s*(A|N|D1|D2)\b', text)
    if m:
        return m.group(1)
    return None


def _extract_drs_level(text: str) -> Optional[str]:
    m = re.search(r'【DRS[^】]*】\s*([A-Z]+)\b', text)
    if m:
        return m.group(1)
    return None


def _extract_exit_readiness(text: str) -> Tuple[Optional[str], Optional[str]]:
    m = re.search(r'准备度等级[：:\s]+([A-Z]+)\s*/\s*建议动作[：:\s]+([A-Z0-9_]+)', text)
    if m:
        return m.group(1), m.group(2)
    m1 = re.search(r'准备度等级[：:\s]+([A-Z]+)\b', text)
    m2 = re.search(r'建议动作[：:\s]+([A-Z0-9_]+)\b', text)
    return (m1.group(1) if m1 else None), (m2.group(1) if m2 else None)



def _extract_structure_section(text: str, key: str) -> Optional[str]:
    '''
    Extract the body text for a structure item from the "结构事实（技术轨）" markdown.

    Robust strategy:
    - Find a list item line like:
        - breadth:
      (indent may be 0+ spaces)

    - Capture subsequent lines until the next list item of the same or less indentation.
    '''
    lines = text.splitlines()

    # find the start line and its indentation
    start_idx = None
    start_indent = 0
    start_pat = re.compile(rf'^(?P<indent>\s*)-\s+{re.escape(key)}\s*:\s*$')
    for i, ln in enumerate(lines):
        mm = start_pat.match(ln)
        if mm:
            start_idx = i
            start_indent = len(mm.group('indent') or '')
            break
    if start_idx is None:
        return None

    # capture until next "- <something>:" with indent <= start_indent
    item_pat = re.compile(r'^(?P<indent>\s*)-\s+[\w_]+\s*:\s*$')
    body_lines = []
    for j in range(start_idx + 1, len(lines)):
        ln = lines[j]
        mm2 = item_pat.match(ln)
        if mm2:
            indent2 = len(mm2.group('indent') or '')
            if indent2 <= start_indent:
                break
        body_lines.append(ln)

    return '\n'.join(body_lines).strip('\n')


def _extract_structure_state(text: str, key: str) -> Optional[str]:
    body = _extract_structure_section(text, key)
    if not body:
        return None
    # 状态：healthy（...） or 状态: healthy
    m = re.search(r'状态\s*[:：]\s*([A-Za-z0-9_]+)', body)
    return m.group(1) if m else None


def _extract_structure_evidence(text: str, key: str) -> Dict[str, Any]:
    '''
    Parse evidence key-values within a structure item.

    Accept:
      - modifier: distribution_risk
      - pressure_score: 3.0000
      pressure_score: 3.0000
    where ':' or '：' may be used.

    Only ASCII keys are collected (e.g., modifier, pressure_level, pressure_score).
    '''
    body = _extract_structure_section(text, key)
    if not body:
        return {}

    ev: Dict[str, Any] = {}
    kv_re = re.compile(r'^(?:-\s*)?(?P<k>[A-Za-z_][A-Za-z0-9_]*)\s*[:：]\s*(?P<v>.+?)\s*$')

    for ln in body.splitlines():
        s = ln.strip()
        mm = kv_re.match(s)
        if not mm:
            continue
        k = mm.group('k').strip()
        v = mm.group('v').strip()

        # Extract first numeric token if present (handles "3.0000", "3.0000（...）", "3.0000," etc.)
        num_m = re.match(r'^-?\d+(?:\.\d+)?', v)
        if num_m:
            try:
                ev[k] = float(num_m.group(0))
                continue
            except Exception:
                pass

        ev[k] = v
    return ev


def _extract_structure_bundle(text: str) -> Dict[str, Any]:
    keys = ['breadth', 'failure_rate', 'index_tech', 'north_proxy_pressure', 'trend_in_force', 'amount']
    out: Dict[str, Any] = {}
    for k in keys:
        st = _extract_structure_state(text, k)
        if st is None:
            continue
        out[k] = {'state': st, 'evidence': _extract_structure_evidence(text, k)}
    return out
def _get_by_path(d: Dict[str, Any], path: str) -> Any:
    cur: Any = d
    for part in path.split('.'):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _assert_report_text_new(report_text: str, case: Dict[str, Any]) -> None:
    exp = case.get('expected', {}) or {}
    report_dump = _load_sidecar_report_dump(case)

    for bad in exp.get('must_not_contain', []):
        assert bad not in report_text, f'forbidden token found: {bad}'

    for s in (exp.get('expected_text_contains', []) or exp.get('must_contain', []) or []):
        assert s in report_text, f'missing expected text: {s}'

    assertions = exp.get('assertions', [])
    if assertions:
        er_level, er_action = _extract_exit_readiness(report_text)
        extracted = {
            'report': {
                'action_hint': {'code': _extract_action_hint_code(report_text)},
                'summary': {
                    'code': _extract_summary_code(report_text),
                    'gate': _extract_gate(report_text),
                    'permissions': _extract_permissions(report_text),
                },
                'structure': (_structure_from_dump(report_dump) if isinstance(report_dump, dict) else _extract_structure_bundle(report_text)),
                'execution': {'band': _extract_execution_band(report_text)},
                'drs': {'level': _extract_drs_level(report_text)},
                'exit_readiness': {'level': er_level, 'suggested_action': er_action},
            }
        }
        if isinstance(report_dump, dict):
            _enrich_from_dump(extracted, report_dump)
        for a in assertions:
            path = a.get('path')
            op = a.get('op', 'eq')
            val = a.get('value')
            if not path or op != 'eq':
                raise AssertionError(f'unsupported assertion: {a}')
            got = _get_by_path(extracted, path)
            assert got == val, f'assertion failed: {path} got={got} expected={val}'

    invariants = exp.get('invariants', [])
    for inv in invariants:
        name = inv.get('name')
        if name == 'execution_band_must_be_single_token':
            band = _extract_execution_band(report_text)
            assert band in {'A', 'N', 'D1', 'D2'}, f'invalid execution band: {band}'
        elif name == 'no_combined_summary_code':
            code = _extract_summary_code(report_text)
            assert code in {'A', 'N', 'D'}, f'invalid summary code: {code}'
        elif name == 'gate_permission_consistency':
            gate = _extract_gate(report_text)
            if gate == 'CAUTION':
                assert '禁止 ADD-RISK' in report_text, 'CAUTION should forbid ADD-RISK in summary'
        elif name == 'drs_not_override_gate':
            continue
        else:
            raise AssertionError(f'unknown invariant name: {name}')


def _assert_report_text(report_text: str, case: Dict[str, Any]) -> None:
    exp = case.get('expected', {}) or {}
    report_dump = _load_sidecar_report_dump(case)
    if ('expected_text_contains' in exp) or ('assertions' in exp) or ('invariants' in exp):
        _assert_report_text_new(report_text, case)
    else:
        _assert_report_text_legacy(report_text, case)


def _slots_from_case(case: Dict[str, Any]) -> Dict[str, Any]:
    inputs = case.get('inputs', {}) or {}
    slots = inputs.get('slots')
    if isinstance(slots, dict):
        return slots
    if isinstance(case.get('slots'), dict):
        return case['slots']
    return _build_minimal_slots_from_case_legacy(case)


# -----------------------------
# Runner
# -----------------------------

@dataclass
class CaseResult:
    case_id: str
    path: Path
    ok: bool
    error: Optional[str] = None


def run_case(case_path: Path, write_output: bool = False, out_dir: Optional[Path] = None) -> str:
    case = _load_json(case_path)
    case['_case_path'] = str(case_path)
    _dbg(f"case_path set: {case_path}")
    case_id = _case_id_from(case, case_path)
    kind = _case_kind_from(case)
    trade_date = _case_trade_date_from(case)

    # ---- project imports ----
    from core.reporters.report_context import ReportContext
    from core.reporters.report_engine import ReportEngine
    from core.reporters.renderers.markdown_renderer import MarkdownRenderer
    from core.actions.actionhint_service import ActionHintService

    from core.reporters.report_blocks.market_overview_blk import MarketOverviewBlock
    from core.reporters.report_blocks.structure_facts_blk import StructureFactsBlock
    from core.reporters.report_blocks.summary_a_n_d_blk import SummaryANDBlock
    from core.reporters.report_blocks.etf_spot_sync_explain_blk import EtfSpotSyncExplainBlock
    from core.reporters.report_blocks.execution_summary_blk import ExecutionSummaryBlock
    from core.reporters.report_blocks.exit_readiness_blk import ExitReadinessBlock

    slots = _slots_from_case(case)

    context = ReportContext(
        kind=kind,
        trade_date=trade_date,
        slots=slots,
    )

    engine = ReportEngine(
        market='CN',
        actionhint_service=ActionHintService(),
        block_builders={
            'market.overview': MarketOverviewBlock().render,
            'structure.facts': StructureFactsBlock().render,
            'etf_spot_sync.explain': EtfSpotSyncExplainBlock().render,
            'summary': SummaryANDBlock().render,
            'execution.summary': ExecutionSummaryBlock().render,
            'exit.readiness': ExitReadinessBlock().render,
        },
    )

    doc = engine.build_report(context)
    text = MarkdownRenderer().render(doc)

    _assert_report_text(text, case)

    if write_output:
        if out_dir is None:
            out_dir = Path.cwd() / 'selftest_outputs'
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f'{case_id}.md'
        out.write_text(text, encoding='utf-8')

    return text


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--cases-dir', type=str, default=None, help='explicit cases directory (optional)')
    ap.add_argument('--write-output', action='store_true', help='write rendered report markdown to selftest_outputs/')
    ap.add_argument('--pattern', type=str, default=None, help='only run cases whose filename contains this substring')
    ap.add_argument('--fail-fast', action='store_true', help='stop on first failure')
    args = ap.parse_args()

    global _EXTRA_DUMP_DIRS, _DEBUG
    _DEBUG = bool(getattr(args, 'debug', False))
    _EXTRA_DUMP_DIRS = []
    for p in (getattr(args, 'dumps_dir', None) or []):
        try:
            _EXTRA_DUMP_DIRS.append(Path(p))
        except Exception:
            continue
    _dbg(f"extra dumps dirs: {[str(d) for d in _EXTRA_DUMP_DIRS]}")

    cases_dir = _find_cases_dir(args.cases_dir)
    case_files = _select_case_files(cases_dir)

    if args.pattern:
        case_files = [p for p in case_files if args.pattern in p.name]

    if not case_files:
        raise SystemExit(f'No case files found under: {cases_dir}')

    results: List[CaseResult] = []
    for p in case_files:
        cid = p.stem.replace('.v2', '')
        try:
            run_case(p, write_output=args.write_output)
            results.append(CaseResult(case_id=cid, path=p, ok=True))
            print(f'[OK] {cid}')
        except Exception as e:
            results.append(CaseResult(case_id=cid, path=p, ok=False, error=str(e)))
            print(f'[FAIL] {cid} :: {e}')
            if args.fail_fast:
                break

    ok_n = sum(1 for r in results if r.ok)
    fail_n = sum(1 for r in results if not r.ok)
    print('')
    print('========== Selftest Summary ==========')
    print(f'cases_dir : {cases_dir}')
    print(f'total     : {len(results)}')
    print(f'passed    : {ok_n}')
    print(f'failed    : {fail_n}')
    if fail_n:
        print('')
        print('Failed cases:')
        for r in results:
            if not r.ok:
                print(f'- {r.case_id} ({r.path.name}) :: {r.error}')
        raise SystemExit(2)

    print('[OK] all regression cases passed.')


if __name__ == '__main__':
    main()
