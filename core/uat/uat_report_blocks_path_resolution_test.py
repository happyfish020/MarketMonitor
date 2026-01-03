# -*- coding: utf-8 -*-
"""UAT · ReportBlocks path resolution (cwd-independent)

Goal:
- When report_blocks_path is relative (e.g. 'config/report_blocks.yaml'),
  ReportEngine must resolve it against repository ROOT_DIR first (not cwd),
  to avoid loading a wrong/stale config or 'missing blocks' symptoms.
"""

from __future__ import annotations

import os
from pathlib import Path

from core.reporters.report_engine import ReportEngine
from core.utils.config_loader import ROOT_DIR


def main() -> int:
    root = Path(ROOT_DIR).resolve()
    assert root.exists(), f"ROOT_DIR not found: {root}"

    # Simulate running from a non-root working directory (common in Windows IDEs)
    prev_cwd = Path.cwd()
    os.chdir((root / "core").as_posix())

    try:
        engine = ReportEngine(
            market="CN",
            actionhint_service=object(),  # not used in this test
            block_specs_path="config/report_blocks.yaml",  # relative on purpose
        )

        doc = engine._load_blocks_doc(kind="EOD", slots={})  # type: ignore[attr-defined]
        assert isinstance(doc, dict), "report_blocks.yaml must load into dict"
        reports = (doc.get("reports") or {})
        eod = (reports.get("EOD") or {})
        blocks = eod.get("blocks") or []
        assert isinstance(blocks, list), "reports.EOD.blocks must be list"

        # Critical invariant for 'structure block visible'
        assert "structure.facts" in blocks, f"structure.facts missing in EOD blocks: {blocks}"

        print("OK: report_blocks.yaml loaded (cwd-independent) and includes 'structure.facts'.")
        return 0

    finally:
        os.chdir(prev_cwd.as_posix())


if __name__ == "__main__":
    raise SystemExit(main())
