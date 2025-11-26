
from datetime import datetime
from typing import Dict, Any

from unifiedrisk.utils.paths import get_reports_dir, get_data_dir

def _parse_bj_date(raw: Dict[str, Any]) -> str:
    bj_time = raw.get("meta", {}).get("bj_time")
    if not bj_time:
        return datetime.now().strftime("%Y-%m-%d")
    return bj_time[:10]

def _risk_level_zh(level: str) -> str:
    return {
        "Low": "极低风险 - 可考虑加仓",
        "Medium": "中性风险 - 以观察为主",
        "High": "偏高风险 - 建议减仓",
        "Extreme": "极高风险 - 建议大幅降仓",
    }.get(level, level)

def _format_turnover_block(idx: Dict[str, Any]) -> str:
    def fmt_one(name: str, key: str) -> str:
        if key not in idx:
            return f"- {name}: 数据缺失"
        t = idx[key].get("turnover", 0.0)
        return f"- {name}: {t/1e8:.2f} 亿元"

    lines = [
        fmt_one("上证ETF(510300)", "shanghai"),
        fmt_one("深证ETF(159901)", "shenzhen"),
        fmt_one("创业板ETF(159915)", "chi_next"),
    ]
    return "\n".join(lines)

def _format_tplus1_block(raw: Dict[str, Any], score: Dict[str, Any]) -> str:
    g = raw.get("global", {})
    nas = g.get("nasdaq", {}).get("change_pct", 0.0)
    spy = g.get("spy", {}).get("change_pct", 0.0)
    vix = g.get("vix", {}).get("last", 0.0)

    total = float(score.get("total_score", 0.0))

    if total >= 4:
        base_view = "T+1 日大概率偏强，上涨概率约 65%~75%。"
    elif total >= 2:
        base_view = "T+1 日略偏强，震荡上行概率略高。"
    elif total >= 0:
        base_view = "T+1 日大概率维持震荡格局。"
    elif total >= -3:
        base_view = "T+1 日偏弱，下跌概率偏高。"
    else:
        base_view = "T+1 日存在较大下跌风险，需谨慎规避高贝塔标的。"

    global_comment = f"当前外围环境：纳指 {nas:.2f}%，SPY {spy:.2f}%，VIX {vix:.2f}。"

    if nas > 1 and spy > 0.5 and vix < 18:
        global_view = "外围整体偏暖，对 A 股 T+1 有正向支撑。"
    elif nas < -1 or spy < -0.5:
        global_view = "外围存在一定压力，可能拖累 A 股短线情绪。"
    else:
        global_view = "外围整体中性，A 股更多取决于内生资金与政策。"

    lines = [
        base_view,
        global_comment,
        global_view,
        "以上判断仅为量化模型的方向性参考，并不构成任何投资建议。",
    ]
    return "\n".join(lines)

def write_daily_report(raw: Dict[str, Any], score: Dict[str, Any]):
    reports_dir = get_reports_dir()
    data_dir = get_data_dir()
    history_dir = data_dir / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    history_file = history_dir / "ashare_risk_score.csv"

    date_str = _parse_bj_date(raw)
    total = float(score.get("total_score", 0.0))
    level = score.get("risk_level", "Medium")
    advise = score.get("advise", "")

    prev_score = None
    if history_file.exists():
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                lines = [x.strip() for x in f.readlines() if x.strip()]
            if lines:
                last = lines[-1].split(",")
                if len(last) >= 2 and last[0] != date_str:
                    prev_score = float(last[1])
        except Exception:
            prev_score = None

    with open(history_file, "a", encoding="utf-8") as f:
        f.write(f"{date_str},{total}\n")

    if prev_score is not None:
        diff = total - prev_score
        if diff > 0:
            trend_desc = f"风险上升 ({diff:+.2f} 分)"
        elif diff < 0:
            trend_desc = f"风险下降 ({diff:+.2f} 分)"
        else:
            trend_desc = "风险持平 (0.00 分)"
        prev_str = f"{prev_score:.2f}"
    else:
        trend_desc = "暂无昨日记录，无法计算趋势"
        prev_str = "N/A"

    risk_level_desc = _risk_level_zh(level)

    factor_lines = [
        f"・ Turnover Score: {score.get('turnover_score', 0)}",
        f"・ Global Score: {score.get('global_score', 0)}",
        f"・ Northbound Proxy Score: {score.get('north_score', 0)}",
        f"・ Liquidity Score: {score.get('liquidity_score', 0)}",
    ]
    factor_block = "\n".join(factor_lines)

    idx_block = _format_turnover_block(raw.get("index_turnover", {}))

    tplus1_block = _format_tplus1_block(raw, score)

    content = f"""=== A股日级别风险量化报告 ===
日期：{date_str}（使用最新收盘数据）

综合风险评分 (T0): {total:.2f}
昨日风险评分 (T-1): {prev_str}
风险趋势: {trend_desc}

风险等级: {risk_level_desc}
风险描述: {risk_level_desc}（模型结论：{advise}）

关键因子触发：
{factor_block}

=== 成交额与流动性概览 ===
{idx_block}

=== 下一交易日（T+1）行情预测 ===
{tplus1_block}

"""

    report_path = reports_dir / f"{date_str}_Ashare_DailyRisk.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)

    return report_path
