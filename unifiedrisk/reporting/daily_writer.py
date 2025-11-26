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


def _format_macro_block(raw: Dict[str, Any]) -> str:
    macro = raw.get("macro", {})
    if not macro:
        return "当前版本未成功获取美元 / 大宗商品数据。"

    def fmt_one(label: str, key: str) -> str:
        item = macro.get(key)
        if not item:
            return f"- {label}: 数据缺失"
        last = item.get("last", 0.0)
        pct = item.get("change_pct", 0.0)
        return f"- {label}: {last:.2f} ({pct:+.2f}%)"

    lines = [
        fmt_one("美元指数", "usd"),
        fmt_one("COMEX 黄金", "gold"),
        fmt_one("WTI 原油", "oil"),
        fmt_one("COMEX 期铜", "copper"),
    ]
    return "\n".join(lines)


def _format_tplus1_block(raw: Dict[str, Any], score: Dict[str, Any]) -> str:
    g = raw.get("global", {})
    macro = raw.get("macro", {})

    nas = g.get("nasdaq", {}).get("change_pct", 0.0)
    spy = g.get("spy", {}).get("change_pct", 0.0)
    vix = g.get("vix", {}).get("last", 0.0)

    usd = macro.get("usd", {}).get("change_pct", 0.0)
    gold = macro.get("gold", {}).get("change_pct", 0.0)
    oil = macro.get("oil", {}).get("change_pct", 0.0)
    copper = macro.get("copper", {}).get("change_pct", 0.0)

    total = float(score.get("total_score", 0.0))

    # 方向性 + 概率
    if total >= 4:
        dir_text = "🟢 下一交易日显著偏多，强反弹概率较高（约 70%）"
    elif total >= 2:
        dir_text = "🟢 下一交易日偏多，震荡上行概率略高（约 60%）"
    elif total >= 0:
        dir_text = "🟡 下一交易日大概率维持震荡格局，上下空间有限"
    elif total >= -3:
        dir_text = "🔴 下一交易日偏空，下跌概率偏高（约 60%）"
    else:
        dir_text = "🔴 下一交易日存在较大下跌风险，需谨慎规避高贝塔标的"

    global_comment = f"跨夜外围：纳指 {nas:.2f}%，SPY {spy:.2f}%，VIX {vix:.2f}。"

    # 外围情绪归纳
    if nas > 1 and spy > 0.5 and vix < 18:
        global_view = "外围整体偏暖，对 A 股 T+1 有正向支撑。"
    elif nas < -1 or spy < -0.5:
        global_view = "外围存在一定压力，可能拖累 A 股短线情绪。"
    else:
        global_view = "外围整体中性，A 股更多取决于内生资金与政策。"

    # 跨资产信号
    cross_asset = []
    if gold > 0.8 and vix > 20:
        cross_asset.append("黄金走强 + VIX 偏高 → 风险偏好回落，利好贵金属 / 资源，压制高估值成长。")
    elif gold < -0.5 and vix < 18:
        cross_asset.append("黄金走弱 + VIX 低位 → 风险偏好改善，利好科技 / 权重反弹。")
    if usd > 0.5:
        cross_asset.append("美元指数偏强 → 对以出口为主的板块有一定压力。")
    elif usd < -0.5:
        cross_asset.append("美元指数走弱 → 对新兴市场与大宗商品相对友好。")
    if copper > 1.0:
        cross_asset.append("期铜明显走强 → 对周期 / 有色板块情绪偏正面。")
    if not cross_asset:
        cross_asset.append("跨资产信号整体中性，暂未看到极端风险 / 机会。")

    # 指数层面拆分（大盘 vs 创业板）
    if total >= 2:
        index_view = (
            "大盘指数（上证50 / 沪深300）：反弹概率偏高；"
            "创业板 / 小盘：有望跟随反弹，但弹性取决于资金偏好。"
        )
    elif total >= 0:
        index_view = (
            "大盘指数：以箱体震荡为主；"
            "创业板 / 小盘：进攻性略强，但同时回撤风险也更大。"
        )
    else:
        index_view = (
            "大盘指数：下跌概率偏高；"
            "创业板 / 小盘：若前期涨幅较大，需警惕放量回落。"
        )

    lines = [
        f"预测方向：{dir_text}",
        f"T+1 模型综合评分: {total:.2f}",
        "",
        "【跨夜全球市场概览】",
        global_comment,
        global_view,
        "",
        "【美元 / 大宗商品信号】",
        *["- " + x for x in cross_asset],
        "",
        "【指数层面拆分（大盘 vs 创业板）】",
        index_view,
        "",
        "（T+1 跨夜预测不参与 T0 综合评分，仅用于提前预警）",
    ]
    return "\n".join(lines)


def _format_factor_detail(score: Dict[str, Any]) -> str:
    t = score.get("turnover_score", 0)
    g = score.get("global_score", 0)
    n = score.get("north_score", 0)
    l = score.get("liquidity_score", 0)

    liquidity_alert = l < 0
    macro_reflection = g  # 暂时用 global_score 作为宏观反射占位

    lines = [
        f"・ Turnover Score: {t}",
        f"・ Global Score: {g}",
        f"・ Northbound Proxy Score: {n}",
        f"・ Liquidity Score: {l}",
        f"・ Liquidity Alert: {liquidity_alert}",
        f"・ Macro Reflection Risk: {macro_reflection}",
        "・ Style Switch Risk: 暂未接入（预留因子）",
        "・ Volume-Price Risk: 暂未接入（预留因子）",
        "・ Margin Speed Risk: 暂未接入（预留因子）",
        "・ Bear Trap Score: 暂未接入（预留因子）",
        "・ Tech Pattern Risk: 暂未接入（预留因子）",
        "・ Policy ETF Score: 暂未接入（预留因子）",
    ]
    return "\n".join(lines)


def _format_sector_outlook(raw: Dict[str, Any], score: Dict[str, Any]) -> str:
    level = score.get("risk_level", "Medium")
    macro = raw.get("macro", {})
    gold = macro.get("gold", {}).get("change_pct", 0.0)
    copper = macro.get("copper", {}).get("change_pct", 0.0)
    oil = macro.get("oil", {}).get("change_pct", 0.0)

    def base_view():
        if level == "Low":
            return "T+1/T+2 整体偏多，大多数行业以反弹为主。"
        if level == "Medium":
            return "T+1/T+2 整体以震荡为主，行业间分化取决于政策与盈利预期。"
        if level == "High":
            return "T+1/T+2 整体偏弱，建议控制高波动板块仓位。"
        return "T+1/T+2 存在较大系统性风险，优先考虑防御与现金。"

    lines = [base_view(), ""]

    lines.append("- 金融 / 银行：大概率跟随大盘，偏向稳定风格，适合作为波动缓冲。")
    lines.append("- 券商：对情绪与成交额敏感，在放量反弹环境下弹性更大。")

    if gold > 0.8:
        lines.append("- 贵金属 / 有色：受黄金走强带动，短期防御属性增强，回撤压力相对较小。")
    elif gold < -0.5:
        lines.append("- 贵金属 / 有色：黄金走弱背景下，需警惕避险情绪降温后的回吐风险。")
    else:
        lines.append("- 贵金属 / 有色：整体中性，更多跟随美元与利率预期波动。")

    if copper > 1.0:
        lines.append("- 周期（有色 / 化工 / 建材）：期铜走强，若叠加国内稳增长预期，T+1/T+2 有望偏强。")
    else:
        lines.append("- 周期（有色 / 化工 / 建材）：暂未看到明确趋势信号，以结构性机会为主。")

    if oil > 1.0:
        lines.append("- 能源 / 石油石化：油价上行时盈利预期改善，但需警惕高油价对整体经济的压制。")
    else:
        lines.append("- 能源 / 石油石化：油价平稳或回落，有利于下游制造与消费成本端。")

    lines.append("- 科技 / 半导体：对全球流动性与风险偏好敏感，在风险等级偏低时弹性最大，但回撤也最快。")
    lines.append("- 医药 / 消费：医药具备一定防御属性；消费板块中，必选消费更稳健，可选消费对宏观预期和利率更敏感。")
    lines.append("- 新能源车 / 高端制造：在风险偏低 + 风险偏好修复的环境中具备更高弹性，但需警惕政策与海外需求变化。")

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

    # 读取历史评分，算趋势
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

    # 追加今日记录
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

    factor_block = _format_factor_detail(score)
    idx_block = _format_turnover_block(raw.get("index_turnover", {}))
    macro_block = _format_macro_block(raw)
    tplus1_block = _format_tplus1_block(raw, score)
    sector_block = _format_sector_outlook(raw, score)

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

=== 跨资产视角：美元 / 黄金 / 原油 / 铜 ===
{macro_block}

=== 下一交易日（T+1）行情预测（跨夜全球市场 → A股） ===
{tplus1_block}

=== 各行业 T+1 / T+2 结构性展望（定性） ===
{sector_block}

"""

    report_path = reports_dir / f"{date_str}_Ashare_DailyRisk.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)

    return report_path
