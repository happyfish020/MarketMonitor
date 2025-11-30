from __future__ import annotations
from datetime import datetime
from pathlib import Path
from unified_risk.common.logging_utils import get_logger

LOG = get_logger("UnifiedRisk.ReportWriter")

REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def write_ashare_daily_report(result: dict, bj_time: datetime):
    """
    将 run_ashare_daily 的文本报告写入 reports/*.txt
    """

    # 文件名：UnifiedRisk_ashare_daily_2025-11-30.txt
    filename = REPORT_DIR / f"UnifiedRisk_ashare_daily_{bj_time.date()}.txt"

    summary_text = result.get("summary", "")
    if not summary_text:
        summary_text = "（未生成 summary）\n"

    # 写入
    with filename.open("w", encoding="utf-8") as f:
        f.write(summary_text)

    LOG.info(f"[Report] 写入报告: {filename}")
    return str(filename)
