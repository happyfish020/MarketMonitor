# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 · Replay Smoke (Frozen)

Purpose:
- Regression gate for report rendering: recompute persisted runs and diff vs stored report_dump.
- Default ignores /rendered (text) diffs; focuses on structured des_payload parity.

Design:
- Calls existing replay_run.py via subprocess to avoid import-path coupling.
- Uses replay_run.py --list to obtain recent runs, then filters status=COMPLETED.

Usage (Windows):
  python scripts\replay_smoke.py --db data/persistent/unifiedrisk.db --block-specs config/report_blocks.yaml --limit 20

Strict rendered (include /rendered in diff):
  python scripts\replay_smoke.py --db data/persistent/unifiedrisk.db --block-specs config/report_blocks.yaml --limit 20 --strict-rendered
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _find_replay_run_py() -> Path:
    here = Path(__file__).resolve()
    # If placed in scripts/, root is parent; if placed in root, it's next to this script
    cands = [
        here.parent / "replay_run.py",
        here.parent.parent / "replay_run.py",
    ]
    for p in cands:
        if p.exists() and p.is_file():
            return p
    raise FileNotFoundError(
        "replay_run.py not found next to this script or in parent directory. "
        "Place replay_smoke.py under project root/scripts/ or project root/."
    )


def _run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str, str]:
    p = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out, err = p.communicate()
    return p.returncode, out, err


def _parse_json_from_stdout(stdout: str) -> Any:
    """
    replay_run.py prints a JSON object/array. We parse stdout accordingly.
    """
    s = stdout.strip()
    if not s:
        raise ValueError("empty stdout")
    try:
        return json.loads(s)
    except Exception:
        pass
    # fallback: attempt parse from last '{' or '['
    last_obj = max(s.rfind("{"), s.rfind("["))
    if last_obj == -1:
        raise ValueError("no JSON found in stdout")
    return json.loads(s[last_obj:])


def _load_summary(out_dir: Path, stdout: str) -> Dict[str, Any]:
    cand = out_dir / "summary.json"
    if cand.exists():
        return json.loads(cand.read_text(encoding="utf-8"))
    obj = _parse_json_from_stdout(stdout)
    if isinstance(obj, dict):
        return obj
    raise ValueError("summary.json missing and stdout is not a dict")


def _first_diff_hint(out_dir: Path) -> str:
    diffs_json = out_dir / "diffs.json"
    if diffs_json.exists():
        try:
            items = json.loads(diffs_json.read_text(encoding="utf-8"))
            if isinstance(items, list) and items:
                d0 = items[0]
                return f"{d0.get('path')} ({d0.get('kind')})"
        except Exception:
            pass
    diff_md = out_dir / "diff.md"
    if diff_md.exists():
        for line in diff_md.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.strip().startswith("| `"):
                return line.strip()
    return "(no diff detail)"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite DB path, e.g. data/persistent/unifiedrisk.db")
    ap.add_argument("--block-specs", required=True, help="Report blocks specs yaml, e.g. config/report_blocks.yaml")
    ap.add_argument("--limit", type=int, default=20, help="Number of COMPLETED runs to check (default 20)")
    ap.add_argument("--scan-limit", type=int, default=200, help="How many recent runs to scan to find COMPLETED (default 200)")
    ap.add_argument("--out-root", default="out/replay_smoke", help="Output root dir (default out/replay_smoke)")
    ap.add_argument("--strict-rendered", dest="strict_rendered", action="store_true",
                    help="Include /rendered in diffs (default ignores /rendered).")
    ap.add_argument("--stop-on-first-fail", action="store_true", help="Stop immediately on first diff/error")
    args = ap.parse_args()

    replay_run_py = _find_replay_run_py()
    project_root = replay_run_py.parent

    # 1) list recent runs
    cmd_list = [
        sys.executable,
        str(replay_run_py),
        "--db",
        args.db,
        "--list",
        "--limit",
        str(args.scan_limit),
    ]
    rc, out, err = _run_cmd(cmd_list, cwd=project_root)
    if rc != 0:
        print("replay_smoke: FAILED to list runs")
        print(err or out)
        return 2

    runs = _parse_json_from_stdout(out)
    if not isinstance(runs, list):
        print("replay_smoke: unexpected --list output (not a list)")
        print(out)
        return 2

    completed = [r for r in runs if isinstance(r, dict) and str(r.get("status", "")).upper() == "COMPLETED"]
    if not completed:
        print("replay_smoke: no COMPLETED runs found in recent list")
        return 3

    target = completed[: max(0, int(args.limit))]

    out_root = (project_root / args.out_root).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    print("========== Replay Smoke ==========")
    print("db       :", args.db)
    print("block    :", args.block_specs)
    print("scan     :", len(runs), "runs (recent)")
    print("target   :", len(target), "COMPLETED runs")
    print("strict   :", bool(args.strict_rendered))
    print("out_root :", str(out_root))
    print("---------------------------------")

    failures: List[Dict[str, Any]] = []
    passes = 0

    for i, r in enumerate(target, start=1):
        run_id = str(r.get("run_id") or "")
        if not run_id:
            continue

        out_dir = out_root / f"{i:03d}_{run_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            sys.executable,
            str(replay_run_py),
            "--db",
            args.db,
            "--run-id",
            run_id,
            "--mode",
            "recompute",
            "--block-specs",
            args.block_specs,
            "--out",
            str(out_dir),
        ]
        if args.strict_rendered:
            cmd.append("--strict-rendered")

        rc, stdout, stderr = _run_cmd(cmd, cwd=project_root)
        if rc != 0:
            failures.append(
                {"run_id": run_id, "kind": "runtime_error", "detail": (stderr or stdout)[-2000:]}
            )
            print(f"[{i}/{len(target)}] FAIL run_id={run_id} :: runtime_error")
            if args.stop_on_first_fail:
                break
            continue

        try:
            summary = _load_summary(out_dir, stdout)
            diff_count = int(summary.get("diff_count", 0))
        except Exception as e:
            failures.append({"run_id": run_id, "kind": "summary_parse_error", "detail": str(e)})
            print(f"[{i}/{len(target)}] FAIL run_id={run_id} :: summary_parse_error")
            if args.stop_on_first_fail:
                break
            continue

        if diff_count != 0:
            hint = _first_diff_hint(out_dir)
            failures.append({"run_id": run_id, "kind": "diff", "diff_count": diff_count, "first_diff": hint})
            print(f"[{i}/{len(target)}] FAIL run_id={run_id} :: diff_count={diff_count} :: {hint}")
            if args.stop_on_first_fail:
                break
        else:
            passes += 1
            print(f"[{i}/{len(target)}] PASS run_id={run_id}")

    print("---------------------------------")
    print("PASS:", passes, "/", len(target))
    if failures:
        print("FAIL:", len(failures))
        (out_root / "failures.json").write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
        print("details:", str((out_root / "failures.json").resolve()))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
