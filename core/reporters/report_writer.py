from __future__ import annotations

import os
import logging
from datetime import datetime
from typing import Optional

from core.reporters.report_types import ReportDocument
from core.utils.config_loader import load_paths


LOG = logging.getLogger("ReportWriter")


class ReportWriter:
    def __init__(self) -> None:
        _paths = load_paths()
        self.base_dir = _paths.get("cn_report_dir", "data/reports/cn/daily")
        

    def write(self, *, doc: ReportDocument, text: str) -> str:
        """
        Write rendered report text to disk.
    
        Design-B rules:
        - Writer does NOT decide directory structure
        - base_dir is provided by paths.yaml (cn_report_dir)
        - filename is deterministic: <kind>_<trade_date>.md
        """
    
        meta = doc.meta or {}
    
        trade_date = str(meta.get("trade_date"))
        kind = str(meta.get("kind"))
    
        if not trade_date or not kind:
            raise ValueError(
                f"[ReportWriter] missing required meta fields: "
                f"trade_date={trade_date}, kind={kind}"
            )
    
        # base_dir MUST come from paths.yaml (already injected)
        out_dir = self.base_dir
        os.makedirs(out_dir, exist_ok=True)
    
        fname = f"{kind.lower()}_{trade_date}.md"
        path = os.path.join(out_dir, fname)
    
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
        except Exception:
            LOG.exception("[ReportWriter] failed to write report: %s", path)
            raise
    
        LOG.info("[ReportWriter] report written: %s", path)
        return path
     




