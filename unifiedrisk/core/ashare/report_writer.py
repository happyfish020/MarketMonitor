# -*- coding: utf-8 -*-
"""
UnifiedRisk v4.3.8 - Daily Report Writer
----------------------------------------
负责将 engine.run() 结果写入 txt 报告。

最终输出格式：
    reports/A-RiskReport-YYYY-MM-DD.txt
"""

import logging
from pathlib import Path

LOG = logging.getLogger(__name__)


# ================================================================
# 工具：格式化函数
# ================================================================
def fmt_pct(x):
    """格式化涨跌幅"""
    try:
        return f"{float(x):+.2f}%"
    except:
        return "-"


def fmt_billion(x):
    """亿级数字"""
    try:
        return f"{float(x):,.2f}"
    except:
        return "-"


# ================================================================
# 主函数：写报告
# ================================================================
def write_daily_report(payload: dict, out_dir: Path):
    """
    生成：
        A-RiskReport-YYYY-MM-DD.txt
    """

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    date = payload["meta"].get("date")
    filename = f"A-RiskReport-{date}.txt"
    out_path = out_dir / filename

    LOG.info(f"Writing daily report → {out_path}")

    score = payload["score"]
    raw = payload["raw"]

    # ------------------------------
    # Header
    # ------------------------------
    lines = []
    lines.append("=== A股日级别风险量化报告 ===")
    lines.append(f"日期：{date}")
    lines.append(f"运行时间：{payload['meta']['run_time']}")
    lines.append("")

    # ------------------------------
    # Ⅰ. 综合风险等级
    # ------------------------------
    lines.append("[综合风险评分]")
    lines.append(f"总分：{score['total_score']:+.2f}")
    lines.append(f"风险等级：{score['risk_level']}")
    lines.append("")
    lines.append(score["comment"])
    lines.append("")

    # ------------------------------
    # Ⅱ. 指数表现
    # ------------------------------
    idx = raw.get("index", {})
    sh = idx.get("sh000001")
    sz = idx.get("sz399001")
    cy = idx.get("sz399006")

    lines.append("[指数表现]")
    if sh:
        pct = (sh["close"] - sh["open"]) / max(1e-6, sh["open"]) * 100
        lines.append(f"上证指数： {sh['close']:.2f}  ({pct:+.2f}%)")

    if sz:
        pct = (sz["close"] - sz["open"]) / max(1e-6, sz["open"]) * 100
        lines.append(f"深证成指： {sz['close']:.2f}  ({pct:+.2f}%)")

    if cy:
        pct = (cy["close"] - cy["open"]) / max(1e-6, cy["open"]) * 100
        lines.append(f"创业板指： {cy['close']:.2f}  ({pct:+.2f}%)")

    lines.append("")

    # ------------------------------
    # Ⅲ. 市场情绪 Breadth
    # ------------------------------
    br = raw.get("breadth", {})
    lines.append("[市场宽度 / 情绪]")
    lines.append(f"上涨家数：{br.get('advancers','-')}")
    lines.append(f"下跌家数：{br.get('decliners','-')}")
    #lines.append(f"平均涨跌幅：{br.get('market_avg_change','-'):+.2f}%")
    # 取值
    avg_chg = br.get("market_avg_change")
    
    # 防止 None 或 '-' 崩溃
    if isinstance(avg_chg, (int, float)):
        avg_chg_str = f"{avg_chg:+.2f}%"
    else:
        avg_chg_str = "-"
    
    lines.append(f"平均涨跌幅：{avg_chg_str}")
    
    lines.append("")

    # ------------------------------
    # Ⅳ. 上交所 / 深交所成交与市值
    # ------------------------------
    sse = raw.get("sse", {})
    szse = raw.get("szse", {})

    lines.append("[成交额 / 市值]")
    if sse:
        lines.append(f"上交所成交额：{fmt_billion(sse.get('turnover'))} 亿元")
        lines.append(f"上交所流通市值：{fmt_billion(sse.get('float_mv'))} 亿元")
    if szse:
        lines.append(f"深交所成交额：{fmt_billion(szse.get('turnover'))} 亿元")
        lines.append(f"深交所流通市值：{fmt_billion(szse.get('float_mv'))} 亿元")
    lines.append("")

    # ------------------------------
    # Ⅴ. 北向资金
    # ------------------------------
    north = raw.get("north", {}).get("north", [])
    lines.append("[北向资金]")
    if north:
        t = north[-1]
        lines.append(f"当日净买入：{fmt_billion(t.get('fund_net')/1e8)} 亿元")
        lines.append(f"沪股通净买入：{fmt_billion(t.get('hk2sh')/1e8)} 亿元")
        lines.append(f"深股通净买入：{fmt_billion(t.get('hk2sz')/1e8)} 亿元")
    else:
        lines.append("数据缺失")
    lines.append("")

    # ------------------------------
    # Ⅵ. 两融余额
    # ------------------------------
    margin = raw.get("margin", {}).get("margin", [])
    lines.append("[两融余额]")
    if margin:
        m = margin[-1]
        lines.append(f"两融余额：{fmt_billion(m.get('rzrqye_100m'))} 亿元")
        lines.append(f"融资余额：{fmt_billion(m.get('rzye_100m'))} 亿元")
        lines.append(f"融券余额：{fmt_billion(m.get('rqye_100m'))} 亿元")
    else:
        lines.append("数据缺失")
    lines.append("")

    # ------------------------------
    # Ⅶ. 主力资金流向
    # ------------------------------
    main = raw.get("mainflow", {})
    lines.append("[大盘主力资金]")
    if main:
        lines.append(f"主力净流入：{fmt_billion(main.get('main_net')/1e8)} 亿元")
        lines.append(f"超大单净流入：{fmt_billion(main.get('super_net')/1e8)} 亿元")
    else:
        lines.append("数据缺失")
    lines.append("")

    # ------------------------------
    # Ⅷ. 行业主力资金前 5
    # ------------------------------
    sector = raw.get("sector", {}).get("sectors", [])
    lines.append("[行业主力资金（前5）]")
    if sector:
        top5 = sorted(sector, key=lambda x: x.get("main_100m", 0.0), reverse=True)[:5]
        for s in top5:
            lines.append(
                f"{s['name']}： 主力 {fmt_billion(s['main_100m'])} 亿，涨幅 {s['change_pct']:+.2f}%"
            )
    else:
        lines.append("数据缺失")

    lines.append("")

    # ------------------------------
    # 保存
    # ------------------------------
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    LOG.info(f"Report written → {out_path}")
    return out_path


# ================================================================
# 调度器入口（外部调用用）
# ================================================================
def run_and_write(out_dir="reports"):
    """
    engine.run() → report_writer → 写入文件
    用于 run_ashare_daily.py 脚本
    """
    from .engine import AShareDailyEngine
    eng = AShareDailyEngine()
    payload = eng.run()
    return write_daily_report(payload, Path(out_dir))

    def _write_sector_rotation(self, raw, lines):
        sr = (raw.get('sector_rotation') or {}).get('summary_lines', [])
        lines.append('')
        lines.append('Ⅸ. 行业轮动 / 板块强弱')
        for s in sr:
            lines.append(f'- {s}')