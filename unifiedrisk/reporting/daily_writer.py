
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Any, Dict

BJ_TZ = _dt.timezone(_dt.timedelta(hours=8))


def _now_bj() -> _dt.datetime:
    return _dt.datetime.now(BJ_TZ)


class DailyReportWriter:
    def __init__(self, base_dir: Path | None = None) -> None:
        if base_dir is None:
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

        total_score = float(score.get("total_score", 0.0))
        risk_level = score.get("risk_level", "中性")
        risk_desc = score.get("risk_description", "")
        factor_scores = score.get("factor_scores", {})

        ts = _now_bj().strftime("%Y%m%d-%H%M%S")
        filename = f"AShareDaily-{trade_date.replace('-', '')}-{ts}.txt"
        filepath = self.base_dir / filename

        lines: list[str] = []
        lines.append("=== A股日级别风险量化报告 (UnifiedRisk v4.2) ===")
        lines.append(f"生成时间（北京）: {bj_time_str}")
        lines.append(f"交易日: {trade_date}")
        lines.append("")
        lines.append(f"综合风险评分: {total_score:.2f}")
        lines.append(f"风险等级: {risk_level}")
        if risk_desc:
            lines.append(f"风险描述: {risk_desc}")
        lines.append("")
        lines.append("【关键因子得分】")
        for name, val in factor_scores.items():
            lines.append(f"- {name}: {val:+d}")
        lines.append("")

        nb_block = raw.get("northbound") or {}
        if nb_block.get("ok"):
            nb = nb_block.get("data") or {}
            lines.append("【北向资金概览】")
            lines.append(
                f"- 来源: {nb.get('source','?')}  净买入: {nb.get('net', 0.0):,.0f}  成交额: {nb.get('deal',0.0):,.0f}"
            )
            lines.append(
                f"- NPS: {nb.get('nps', 0.0):+.3f} | NPS_20d: {nb.get('nps_20d',0.0):+.3f} | NPS_30d: {nb.get('nps_30d',0.0):+.3f}"
            )
            lines.append(
                f"- 趋势: 3v20={nb.get('trend_3v20',0.0):+.3f}  5v30={nb.get('trend_5v30',0.0):+.3f}  组合={nb.get('combined_trend',0.0):+.3f}"
            )
            lines.append(
                f"- 参与度: strength20={nb.get('strength_20',0.0):.2f}  strength30={nb.get('strength_30',0.0):.2f}"
            )
            lines.append(
                f"- 行为: pattern={nb.get('pattern','?')}  pos_streak={nb.get('pos_streak',0)}  neg_streak={nb.get('neg_streak',0)}  accel={nb.get('accel',0.0):+.3f}"
            )
            lines.append("")

        margin_block = raw.get("margin") or {}
        if margin_block.get("ok"):
            mg = margin_block.get("data") or {}
            lines.append("【两融资金概览】")
            lines.append(
                f"- 融资买入额: {mg.get('fin_buy',0.0):,.0f}  融券净卖出(占位): {mg.get('sec_sell',0.0):,.0f}"
            )
            lines.append(
                f"- 余额: 融资={mg.get('fin_balance',0.0):,.0f}  融券={mg.get('sec_balance',0.0):,.0f}  总计={mg.get('total_balance',0.0):,.0f}"
            )
            lines.append(
                f"- 趋势: fin_5v20={mg.get('fin_trend_5v20',0.0):+.3f}  sec_5v20={mg.get('sec_trend_5v20',0.0):+.3f}  杠杆分位={mg.get('balance_pct_60d',0.0):.3f}"
            )
            lines.append(f"- 分型: pattern={mg.get('pattern','?')}")
            lines.append("")

        lines.append("【说明】")
        lines.append("1. 本报告为 UnifiedRisk v4.2 测试版，当前核心接入：北向成交 + 两融总量趋势。")
        lines.append("2. 北向接口样本中尚未找到明确净买入字段，NPS 方向性目前为占位（net=0），")
        lines.append("   等你在接口测试中确认准确字段后，只需修改 data_fetcher._fetch_northbound_series_em 即可。")
        lines.append("3. 两融部分基于 TOTAL_RZMRE / TOTAL_RZYE / TOTAL_RQYE / TOTAL_RZRQYE 计算 5v20 趋势与杠杆水平分位，")
        lines.append("   若你后续确认了融券净卖出字段，也可在该函数中补全 sec_sell。")

        filepath.write_text("\n".join(lines), encoding="utf-8")
        return filepath


def write_daily_report(raw: Dict[str, Any], score: Dict[str, Any]):
    payload = {"raw": raw, "score": score}
    writer = DailyReportWriter()
    return writer.write_daily_report(payload)
