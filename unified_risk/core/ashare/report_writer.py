from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from unified_risk.common.logger import get_logger

LOG = get_logger("UnifiedRisk.Writer.AShareDaily")

REPORT_DIR = Path("reports")
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _risk_level_and_advice(score: float) -> tuple[str, str]:
    if score <= -3:
        return "Critical Risk", "偏高风险，建议防守为主。"
    if score <= -1.5:
        return "High Risk", "风险偏高，适当减仓、避免追高。"
    if score <= 1.5:
        return "Neutral", "中性，可维持当前仓位。"
    if score <= 3:
        return "Positive", "偏多，可适度增加高质量标的。"
    return "Extreme Positive", "极度乐观，警惕过热可能带来的回调。"


def write_daily_report(result: Dict[str, Any]) -> Path:
    meta = result.get("meta", {})
    raw = result.get("raw", {})
    scores = result.get("scores", {})

    bj_time = meta.get("bj_time")
    version = meta.get("version", "UnifiedRisk")
    if bj_time:
        d = datetime.fromisoformat(bj_time)
        date_str = d.strftime("%Y-%m-%d")
    else:
        date_str = raw.get("snapshot", {}).get("date", "N/A")

    total_score = scores.get("total_risk_score", 0.0)
    level, advice = _risk_level_and_advice(total_score)

    nb_score = scores.get("northbound_score", 0.0)
    nb_strength = scores.get("nb_nps_score", 0.0)
    turnover_score = scores.get("turnover_score", 0.0)
    margin_score = scores.get("margin_score", 0.0)
    global_score = scores.get("global_score", 0.0)

    lines = []
    lines.append(f"=== A股日级别风险量化报告（{version}） ===")
    lines.append(f"日期：{date_str}")
    lines.append("")
    lines.append(f"综合风险评分: {total_score:.1f}")
    lines.append(f"风险等级: {level}")
    lines.append(f"建议: {advice}")
    lines.append("")

    nb_details = raw.get("northbound", {}).get("details", {})
    source_info = nb_details.get("source_info", {})
    src_2800 = source_info.get("2800.HK", "YF_FAIL")
    if src_2800 != "YF_OK":
        lines.append("⚠ 警告：2800.HK 数据源【YF】失效，已按 0.0%（中性）处理。")
        lines.append("")

    lines.append("【因子得分】")
    lines.append(f"  northbound_score: {nb_score:.1f}")
    lines.append(f"  nb_nps_score: {nb_strength:.1f}")
    lines.append(f"  turnover_score: {turnover_score:.1f}")
    lines.append(f"  margin_score: {margin_score:.1f}")
    lines.append(f"  global_score: {global_score:.1f}")
    lines.append("")

    filepath = REPORT_DIR / f"Ashare_DailyRisk_{date_str}.txt"
    filepath.write_text("\n".join(lines), encoding="utf-8-sig")
    LOG.info("[Report] Written %s", filepath)
    return filepath
