from pathlib import Path
from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Writer.Daily")

class DailyReportWriter:
    def __init__(self, report_dir: Path):
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def write(self, payload: dict):
        date_str = payload["date"]
        path = self.report_dir / f"Ashare_DailyRisk_{date_str}.txt"
        lines = [
            "=== A股日级别风险量化报告（UnifiedRisk v7.1） ===",
            f"日期：{date_str}",
            "",
            f"综合风险评分: {payload['total_risk_score']}",
            f"风险等级: {payload['risk_level']}",
            f"建议: {payload['advise']}",
            "",
            "【因子得分】",
        ]
        for k,v in payload["factor_scores"].items():
            lines.append(f"  {k}: {v}")
        path.write_text("\n".join(lines), encoding="utf-8")
        LOG.info(f"[Report] Written {path}")
        return path
