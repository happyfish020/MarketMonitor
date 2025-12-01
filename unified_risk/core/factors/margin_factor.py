
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

from unified_risk.common.logging_utils import log_info, log_warning
from unified_risk.core.datasources.margin_fetcher import fetch_margin_agg

BJ_TZ = timezone(timedelta(hours=8))


class MarginFactor:
    """两融因子：-10 ~ +10"""

    def __init__(self, cache_manager=None):
        self.cache = cache_manager

    def _get_trade_date(self):
        d = datetime.now(BJ_TZ).date() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return datetime(d.year, d.month, d.day, tzinfo=BJ_TZ)

    def compute(self) -> Dict[str, Any]:
        today = self._get_trade_date()
        prev = today - timedelta(days=1)
        while prev.weekday() >= 5:
            prev -= timedelta(days=1)

        cur = fetch_margin_agg(today)
        pre = fetch_margin_agg(prev)

        if not cur or not pre:
            log_warning("[MARGIN] missing data, neutral.")
            return {
                "margin_score": 0.0,
                "direction": "中性",
                "description": "两融数据缺失（中性）。",
                "raw": {},
            }

        tb = cur["total_balance"]
        pb = pre["total_balance"]
        net_buy = cur["net_buy"]

        chg = (tb - pb) / pb * 100.0 if pb else 0.0
        base = max(min(chg / 3.0 * 10.0, 10.0), -10.0)
        ratio = net_buy / max(tb, 1.0)
        flow_adj = max(min(ratio / 0.005 * 5.0, 5.0), -5.0)

        score = max(min(base * 0.7 + flow_adj * 0.3, 10.0), -10.0)

        if score > 4:
            direction = "杠杆明显加仓"
        elif score > 1:
            direction = "杠杆小幅加仓"
        elif score < -4:
            direction = "杠杆明显减仓"
        elif score < -1:
            direction = "杠杆小幅减仓"
        else:
            direction = "杠杆变化不大"

        desc = (
            f"两融余额变动 {chg:.2f}%，融资买入 {net_buy:,.0f}。"
            f"判定：{direction}，得分 {score:.1f}/10。"
        )

        log_info(f"[MARGIN] score={score:.2f}, chg={chg:.2f}%, net_buy={net_buy:.0f}")
        return {
            "margin_score": score,
            "direction": direction,
            "description": desc,
            "raw": {"today": cur, "prev": pre},
        }
