from pathlib import Path
from typing import Any, Dict

from .engine import AShareDailyEngine
from .data_fetcher import BJ_TZ
from datetime import datetime


def write_daily_report(payload: Dict[str, Any], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    meta = payload.get("meta", {})
    date_str = meta.get("date") or datetime.now(BJ_TZ).date().isoformat()
    bj_time = meta.get("bj_time") or datetime.now(BJ_TZ).isoformat(timespec="seconds")
    version = meta.get("version", "UnifiedRisk_v4.3.7")

    fname = f"A-RiskReport-{date_str}.txt"
    fpath = out_dir / fname

    factor_scores = payload.get("factor_scores", {})
    factor_explain = payload.get("factor_explain", {})

    lines = []
    lines.append("=== A股日级别风险量化报告（UnifiedRisk v4.3.7） ===")
    lines.append(f"日期：{date_str}")
    lines.append(f"运行时间（北京）：{bj_time}")
    lines.append(f"引擎版本：{version}")
    lines.append("")
    lines.append(f"综合风险评分 (T0): {payload.get('total_risk_score', 0):+.2f}")
    lines.append(f"风险等级: {payload.get('risk_level', 'N/A')}")
    lines.append(f"风险描述: {payload.get('risk_description', '')}")
    lines.append(f"操作建议: {payload.get('risk_advice', '')}")
    lines.append("")
    lines.append("[关键因子打分]")
    lines.append("")
    for name, score in factor_scores.items():
        lines.append(f"・ {name}: {score:+.2f}")
        explain = factor_explain.get(name, "")
        if explain:
            lines.append(f"    - {explain}")
    lines.append("")

    # 简要市场概览
    raw = payload.get("raw", {})
    etf = raw.get("etf") or {}
    sh = etf.get("sh") or {}
    sz = etf.get("sz") or {}
    margin = raw.get("margin") or {}
    north = raw.get("north") or []
    market = raw.get("market") or {}

    lines.append("[市场概览（代理 + clist 快照）]")
    lines.append("")

    # ETF 成交额合计
    total_turn = float(sh.get("turnover_100m") or 0.0) + float(sz.get("turnover_100m") or 0.0)
    if total_turn > 0:
        lines.append(f"・ ETF 代理合计成交额约：{total_turn:.0f} 亿元（510300 + 159901）")

    # 两融
    rz = margin.get("两融余额_亿")
    rz_delta = margin.get("两融余额增减_亿")
    if rz is not None:
        lines.append(
            f"・ 两融余额约：{float(rz):.2f} 亿元，日变动：{float(rz_delta or 0.0):+.2f} 亿元"
        )

    # 北向
    if north:
        net_sum = sum(float(r.get("northbound_net") or 0.0) for r in north)
        tot_sum = sum(float(r.get("northbound_total") or 0.0) for r in north) or 1.0
        ratio = net_sum / tot_sum
        lines.append(f"・ 北向净买入：{net_sum:.2e}（占成交 {ratio:.2%}）")

    # clist 大盘快照
    if market:
        up = market.get("up")
        down = market.get("down")
        flat = market.get("flat")
        breadth_val = market.get("breadth")
        mean_change = market.get("mean_change")
        total_amt_100m = market.get("total_amt_100m")
        sample_size = market.get("sample_size")

        if up is not None and down is not None:
            lines.append(
                f"・ 主板样本（fs=b:MK0010）上涨 {up} 家，下跌 {down} 家，平盘 {flat} 家，"
                f"宽度={breadth_val}（样本数={sample_size}）"
            )
        if mean_change is not None:
            lines.append(f"・ 主板平均涨跌幅：{float(mean_change):.2f}%")
        if total_amt_100m is not None:
            lines.append(f"・ 主板样本成交额合计约：{float(total_amt_100m):.0f} 亿元")

    lines.append("")

    # TOP 列表展示（简版）
    if market:
        top_g = market.get("top_gainers") or []
        top_l = market.get("top_losers") or []
        top_a = market.get("top_amplitude") or []

        def _fmt_stock(s):
            return f"{s['code']} {s['name']} 涨跌:{s['change_pct']:.2f}%, 成交:{s['amount_100m']:.1f}亿"

        lines.append("[Top 涨幅股票（样本内，前 5）]")
        for s in top_g[:5]:
            lines.append(f"  - {_fmt_stock(s)}")
        lines.append("")

        lines.append("[Top 跌幅股票（样本内，前 5）]")
        for s in top_l[:5]:
            lines.append(f"  - {_fmt_stock(s)}")
        lines.append("")

        lines.append("[高振幅股票（样本内，前 5）]")
        for s in top_a[:5]:
            lines.append(
                f"  - {s['code']} {s['name']} 振幅:{s['amp_pct']:.2f}%, "
                f"涨跌:{s['change_pct']:.2f}%, 成交:{s['amount_100m']:.1f}亿"
            )
        lines.append("")

    with fpath.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return fpath


def run_and_write(out_dir: Path) -> Path:
    engine = AShareDailyEngine()
    payload = engine.run()
    return write_daily_report(payload, out_dir)
