# unifiedrisk/core/ashare/t1_model.py
from __future__ import annotations

from typing import Dict, Any, Tuple


def _safe_pct(d: Dict[str, Any], key: str) -> float:
    try:
        return float(d.get(key, {}).get("change_pct", 0.0) or 0.0)
    except Exception:
        return 0.0


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _build_t1_direction_and_prob(t1_score: float) -> Tuple[str, str]:
    """
    根据 t1_score 粗略映射方向 & 概率区间（仅供参考）
    """
    if t1_score >= 3.5:
        direction = "🟢 下一交易日偏多（强反弹概率高）"
        prob = "强反弹概率：约 60%–70%"
    elif 1.5 <= t1_score < 3.5:
        direction = "🟢 下一交易日略偏多（温和反弹概率较高）"
        prob = "温和上涨概率：约 55%–60%"
    elif -1.5 < t1_score < 1.5:
        direction = "🟡 下一交易日大概率震荡（方向不明朗）"
        prob = "震荡概率：约 50%–55%"
    elif -3.5 < t1_score <= -1.5:
        direction = "🔴 下一交易日略偏空（回调概率偏高）"
        prob = "回调概率：约 55%–60%"
    else:
        direction = "🔴 下一交易日偏空（明显调整概率较高）"
        prob = "明显调整概率：约 60%–70%"
    return direction, prob


def _build_sector_view_lines() -> str:
    """
    这里暂时不接入真实行业涨跌幅，只给一个结构化的“展望模板”。
    后续可以从 T-RiskMonitor / akshare 行业指数接入真实数据。
    """
    lines = []
    lines.append("=== 各行业 T+1 / T+2 结构性展望（试验版） ===")
    lines.append("（当前版本尚未接入真实行业涨跌幅，仅给出方向性判断模板）")
    lines.append("- 金融: 今日涨跌: 暂未接入，T+1：大概率震荡/跟随大盘；T+2：延续 T+1 大方向")
    lines.append("- 券商: 今日涨跌: 暂未接入，T+1：大概率震荡偏强；T+2：若放量，则存在加速可能")
    lines.append("- 医药: 今日涨跌: 暂未接入，T+1：大概率震荡；T+2：以防御属性为主")
    lines.append("- 半导体: 今日涨跌: 暂未接入，T+1：跟随纳指 & 科技情绪波动；T+2：取决于海外科技走势")
    lines.append("- 新能源车: 今日涨跌: 暂未接入，T+1：跟随大盘，受政策与海外电车情绪影响较大")
    lines.append("- 煤炭: 今日涨跌: 暂未接入，T+1：偏震荡，跟随商品价格；T+2：看大宗与需求预期")
    lines.append("- 军工: 今日涨跌: 暂未接入，T+1：若前一日大涨，易出现冲高回落；T+2：以情绪为主")
    lines.append("- 消费: 今日涨跌: 暂未接入，T+1：必选消费偏稳，可选消费对利率与收入预期更敏感")
    return "\n".join(lines)


def build_t1_view(raw: Dict[str, Any], score: Dict[str, Any]) -> str:
    """
    构造类似 T-RiskMonitor 风格的 T+1 预测 & 行业展望文本块。
    不改变现有 total_score，只在报告中提供“跨夜参考”。
    """
    global_data = raw.get("global", {}) or {}
    macro_data = raw.get("macro", {}) or {}

    # 1) 提取核心因子
    nas_pct = _safe_pct(global_data, "nasdaq")
    spy_pct = _safe_pct(global_data, "spy")
    vix_pct = _safe_pct(global_data, "vix")

    usd_pct = _safe_pct(macro_data, "usd")
    gold_pct = _safe_pct(macro_data, "gold")
    oil_pct = _safe_pct(macro_data, "oil")
    copper_pct = _safe_pct(macro_data, "copper")

    total_score = float(score.get("total_score", 0.0) or 0.0)
    turnover_score = float(score.get("turnover_score", 0.0) or 0.0)
    north_score = float(score.get("north_score", 0.0) or 0.0)

    # 2) 外围情绪因子：美股 + VIX
    global_bias = 0.0
    if nas_pct > 1.0:
        global_bias += 1.0
    elif nas_pct < -1.0:
        global_bias -= 1.0

    if spy_pct > 0.8:
        global_bias += 0.8
    elif spy_pct < -0.8:
        global_bias -= 0.8

    # VIX：上涨 → 风险厌恶；下降 → 风险偏好改善
    if vix_pct > 8.0:
        global_bias -= 1.0
    elif vix_pct < -8.0:
        global_bias += 0.8

    # 3) 宏观 & 大宗商品因子
    macro_bias = 0.0

    # 风险偏好组合：
    # - 铜大涨、油反弹、美元走弱 → 周期 / 有色 / 顺周期受益
    if copper_pct > 2.0:
        macro_bias += 1.0
    if oil_pct > 2.0:
        macro_bias += 0.5
    if usd_pct < -0.3:
        macro_bias += 0.5

    # 避险偏好组合：
    # - 黄金大涨、美元走强 → 风险偏好下降
    if gold_pct > 1.5:
        macro_bias -= 1.0
    if usd_pct > 0.5:
        macro_bias -= 0.5

    # 4) 成交额 & 北向作为“确认因子”
    confirm = 0.0
    confirm += 0.4 * turnover_score
    confirm += 0.4 * north_score

    # 5) 综合成一个 t1_score（范围大致 -5 ~ +5）
    t1_score = 0.8 * total_score + global_bias + macro_bias + confirm
    t1_score = _clamp(t1_score, -5.0, 5.0)

    direction, prob_text = _build_t1_direction_and_prob(t1_score)

    # === 文本组装 ===
    lines = []
    lines.append("=== 下一交易日（T+1）行情预测（跨夜全球市场 → A股） ===")
    lines.append(f"综合 T+1 情绪强度 (t1_score)：{t1_score:.2f}")
    lines.append(direction)
    lines.append(prob_text)
    lines.append("")
    lines.append("【驱动因子拆解】")
    lines.append(f"- 美股：纳指 {nas_pct:.3f}%，SPY {spy_pct:.3f}%")
    lines.append(f"- VIX：{vix_pct:.3f}% → VIX 下跌代表风险偏好改善，上涨则代表避险情绪升温")
    lines.append(
        f"- 大宗商品：黄金 {gold_pct:.3f}% / 期铜 {copper_pct:.3f}% / 原油 {oil_pct:.3f}% / 美元指数 {usd_pct:.3f}%"
    )
    lines.append(
        f"- A股内部：成交额得分 {turnover_score:+.1f}，北向得分 {north_score:+.1f}，T0 总分 {total_score:+.2f}"
    )
    lines.append("")
    lines.append("【结构性解读（示意版）】")
    if t1_score >= 2.5:
        lines.append("🟢 大盘指数（上证50 / 沪深300）：偏多，若开盘不高开过度，强反弹概率较大。")
        lines.append("🟡 创业板 / 小盘：跟随反弹，但若前期涨幅已大，可能出现“高开低走”。")
    elif t1_score <= -2.5:
        lines.append("🔴 大盘指数（上证50 / 沪深300）：偏空，外盘或宏观偏冷，需防范系统性回调。")
        lines.append("🟡 创业板 / 小盘：波动会更剧烈，高估值板块承压更大。")
    else:
        lines.append("🟡 大盘指数（上证50 / 沪深300）：偏震荡，更多是存量资金博弈。")
        lines.append("🟡 创业板 / 小盘：风格切换可能较快，追高与杀跌都需谨慎。")

    # 宏观方向对行业的简要指引
    lines.append("")
    lines.append("【宏观-行业映射（简要）】")
    if copper_pct > 2.0:
        lines.append("- 期铜大涨 → 有利于有色金属、化工、资源周期板块的阶段性表现。")
    if gold_pct > 1.5:
        lines.append("- 黄金明显走强 → 避险情绪抬头，通常不利于高贝塔成长股，对防御类板块相对有利。")
    if usd_pct < -0.3:
        lines.append("- 美元走弱 → 通常对新兴市场与大宗商品友好，对 A 股整体情绪偏正面。")
    if oil_pct < -2.0:
        lines.append("- 原油大跌 → 对航空、物流等成本敏感行业偏利好，对上游油气板块偏利空。")

    lines.append("")
    # 加入行业展望模板（含“消费”）
    lines.append(_build_sector_view_lines())

    lines.append("")
    lines.append("（说明：T+1 跨夜预测不参与 T0 综合评分，仅作为提前预警和结构性参考。）")

    return "\n".join(lines)
