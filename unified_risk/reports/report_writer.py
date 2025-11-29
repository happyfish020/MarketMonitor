from pathlib import Path
from ..common.logger import get_logger
from ..common.config_loader import get_path

LOG = get_logger("UnifiedRisk.Report")

# 从配置获取报告目录
REPORT_DIR: Path = get_path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


class DailyReportWriter:
    def write_daily_report(self, date: str, ashare: dict, global_: dict) -> Path:
        """
        统一写出日级报告文件。

        后续可以在这里拼接更丰富的文本内容，现在先保证路径完全由配置控制。
        """
        fpath = REPORT_DIR / f"UnifiedRisk_Daily_{date}.txt"

        # TODO: 这里先写一个简单占位内容，你以后可以替换成完整报告模板
        text = "UnifiedRisk Daily Report\n"
        text += f"Date: {date}\n"
        text += "\n[A-Share]\n"
        text += str(ashare.get("result", {})) + "\n"
        text += "\n[Global]\n"
        text += str(global_.get("result", {})) + "\n"

        fpath.write_text(text, encoding="utf-8")
        LOG.info(f"Daily report written to {fpath}")
        return fpath
