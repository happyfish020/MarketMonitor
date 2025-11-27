
from __future__ import annotations
import datetime as _dt
from pathlib import Path
from typing import Any, Dict

BJ_TZ = _dt.timezone(_dt.timedelta(hours=8))
def _now_bj():
    return _dt.datetime.now(BJ_TZ)

class DailyReportWriter:
    def __init__(self, base_dir: Path | None = None):
        # 改为项目根目录的 reports：.../MarketMonitor/reports
        if base_dir is None:
            # __file__ = .../unifiedrisk/reporting/daily_writer.py
            # parents[0] = reporting
            # parents[1] = unifiedrisk
            # parents[2] = MarketMonitor (项目根)
            project_root = Path(__file__).resolve().parents[2]
            base_dir = project_root / "reports"
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write_daily_report(self, payload: Dict[str, Any]) -> Path:
        raw = payload.get("raw", {})
        score = payload.get("score", {})
        meta = raw.get("meta", {})
        trade_date = meta.get("trade_date") or _now_bj().date().isoformat()
        bj_time_str = meta.get("bj_time") or _now_bj().isoformat()
        total = score.get("total_score", 0.0)
        level = score.get("risk_level","中性")
        desc = score.get("risk_description","")
        factor_scores = score.get("factor_scores",{})
        ts = _now_bj().strftime("%Y%m%d-%H%M%S")
        fn = f"AShareDaily-{trade_date.replace('-','')}-{ts}.txt"
        fp = self.base_dir / fn
        lines=[]
        lines.append("=== A股日级别风险量化报告 (UnifiedRisk v4.0) ===")
        lines.append(f"生成时间（北京）: {bj_time_str}")
        lines.append(f"交易日: {trade_date}")
        lines.append("")
        lines.append(f"综合风险评分: {total:.2f}")
        lines.append(f"风险等级: {level}")
        if desc: lines.append(f"风险描述: {desc}")
        lines.append("")
        lines.append("【关键因子得分】")
        for k,v in factor_scores.items():
            lines.append(f"- {k}: {v:+d}")
        fp.write_text("\n".join(lines),encoding="utf-8")
        return fp

# --- old API compatibility ---
def write_daily_report(raw: Dict[str,Any], score: Dict[str,Any]):
    payload={"raw": raw, "score": score}
    writer = DailyReportWriter()
    return writer.write_daily_report(payload)
