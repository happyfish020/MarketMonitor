from datetime import datetime
from pathlib import Path


# ===========================
# 格式化辅助函数
# ===========================

def _fmt(v):
    if v is None:
        return "N/A"
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


# ===========================
# T+1 预测模块
# ===========================

from unifiedrisk.core.ashare.t1_model import build_t1_view   # 你已有此文件（上传过）


# ===========================
# 日报写入函数（主入口）
# ===========================

def write_daily_report(raw, score):
    """
    生成日级风险报告（TXT）
    """
    bj = raw["meta"]["bj_time"][:10]          # '2025-11-27'
    version = raw["meta"]["version"]
    date_str = bj.replace("-", "")

    # 报告输出目录
    report_dir = Path("reports")
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f"{bj}_Ashare_DailyRisk.txt"

    lines = []

    # ===========================
    # 1) Header
    # ===========================
    lines.append("=== A股日级风险量化报告（UnifiedRisk） ===")
    lines.append(f"日期：{bj}")
    lines.append(f"版本：{version}")
    lines.append("")

    # ===========================
    # 2) 成交额
    # ===========================
    lines.append("=== A股三大指数成交额 ===")
    for k, v in raw["index_turnover"].items():
        lines.append(
            f"- {k} | 成交额：{_fmt(v['turnover'])} | 价格：{_fmt(v['price'])} | 成交量：{_fmt(v['volume'])}"
        )
    lines.append("")

    # ===========================
    # 3) 全球与宏观市场
    # ===========================
    lines.append("=== 全球与宏观市场 ===")
    g = raw.get("global", {})
    m = raw.get("macro", {})

    def _gp(name):
        d = g.get(name, {})
        return f"{_fmt(d.get('last'))} ({_fmt(d.get('change_pct'))}%)"

    def _mp(name):
        d = m.get(name, {})
        return f"{_fmt(d.get('last'))} ({_fmt(d.get('change_pct'))}%)"

    lines.append(f"纳指：{_gp('nasdaq')}")
    lines.append(f"标普SPY：{_gp('spy')}")
    lines.append(f"VIX：{_gp('vix')}")
    lines.append("")
    lines.append(f"美元指数：{_mp('usd')}")
    lines.append(f"黄金：{_mp('gold')}")
    lines.append(f"原油：{_mp('oil')}")
    lines.append(f"铜：{_mp('copper')}")
    lines.append("")

    # ===========================
    # 4) 风险评分
    # ===========================
    lines.append("=== 风险评分 ===")
    lines.append(f"综合风险总分：{score.get('total_score')}")
    lines.append(f"风险等级：{score.get('risk_level')}")
    lines.append(f"建议：{score.get('advise')}")
    lines.append("")

    # ===========================
    # 5) 关键因子触发（含你要新增的所有因子）
    # ===========================
    lines.append("=== 关键因子触发 ===")

    lines.append(f"・ Turnover Score: {score.get('turnover_score')}")
    lines.append(f"・ Global Score: {score.get('global_score')}")
    lines.append(f"・ Northbound Proxy Score: {score.get('north_score')}")
    lines.append(f"・ Liquidity Score: {score.get('liquidity_score')}")
    lines.append("")

    # === 新增的扩展因子 ===
    lines.append(f"・ Macro Reflection Risk: {score.get('macro_reflection_risk', 0)}")
    lines.append(f"・ Style Switch Risk: {score.get('style_switch', '暂未接入')}")
    lines.append(f"・ Volume-Price Risk: {score.get('vp_risk', '暂未接入')}")
    lines.append(f"・ Margin Speed Risk: {score.get('margin_speed', '暂未接入')}")
    lines.append(f"・ Bear Trap Score: {score.get('bear_trap', '暂未接入')}")
    lines.append(f"・ Tech Pattern Risk: {score.get('tech_pattern', '暂未接入')}")
    lines.append(f"・ Policy ETF Score: {score.get('policy_etf', '暂未接入')}")
    lines.append("")

    # 解释字段
    explanation = score.get("explanation", "")
    lines.append(explanation)
    lines.append("")

    # ===========================
    # 6) 下一交易日（T+1）增强版预测
    # ===========================
    lines.append("=== 下一交易日（T+1）行情预测（跨夜全球市场 → A股） ===")

    try:
        t1_block = build_t1_view(raw, score)
        lines.append(t1_block)
    except Exception as e:
        lines.append(f"[T+1 模块错误] {e}")

    lines.append("")

    # ===========================
    # 7) 行业 T+1 / T+2 展望
    # ===========================
    sectors = raw.get("sectors", {})
    if sectors:
        lines.append("=== 各行业 T+1 / T+2 展望 ===")
        for name, info in sectors.items():
            lines.append(f"- {name}: 今日 {info['today']}%， T+1：{info['t1']}；T+2：{info['t2']}")
        lines.append("")

    # ===========================
    # 写入TXT
    # ===========================
    report_file.write_text("\n".join(lines), encoding="utf-8")

    return str(report_file)
