# -*- coding: utf-8 -*-
"""
UnifiedRisk v4.3.8 - RiskScorer
--------------------------------
基于 DataFetcher 提供的 raw 数据，计算 7 大核心风险因子：
    • Price 因子（大盘价格 & 指数趋势）
    • Liquidity 因子（上/深成交额 & 市值）
    • Northbound 因子（北向趋势 & 大小）
    • Margin 因子（两融余额趋势）
    • Mainflow 因子（主力/超大单主攻情况）
    • Emotion 因子（市场涨跌家数 & 情绪）
    • SectorFlow 因子（板块主力资金轮动）

最终输出：
    {
        "price": x,
        "liquidity": x,
        "north": x,
        "margin": x,
        "mainflow": x,
        "emotion": x,
        "sector": x,

        "total_score": y,
        "risk_level": "...",
        "comment": "...",
    }
"""

import logging
LOG = logging.getLogger(__name__)

from statistics import mean


class RiskScorer:
    def __init__(self, raw: dict):
        """
        raw 由 DataFetcher.fetch_daily_snapshot 返回。
        用于计算所有因子。
        """
        self.raw = raw
        self.scores = {}

    # --------------------------------------------------------------
    # 工具：安全取值
    # --------------------------------------------------------------
    def _safe(self, d: dict, key: str, default=0.0):
        try:
            return float(d.get(key, default))
        except:
            return default

    # --------------------------------------------------------------
    # Ⅰ. Price 因子：指数涨跌幅 & 趋势
    # --------------------------------------------------------------
    def compute_price_factor(self):
        index = self.raw.get("index", {})
        sh = index.get("sh000001")
        if not sh:
            return 0.0

        close = self._safe(sh, "close")
        open_ = self._safe(sh, "open")
        if open_ <= 0:
            return 0.0

        pct = (close - open_) / open_ * 100

        # 简单映射：涨跌幅 → 分数
        if pct > 1.5:
            score = 2
        elif pct > 0.5:
            score = 1
        elif pct > -0.5:
            score = 0
        elif pct > -1.5:
            score = -1
        else:
            score = -2

        # 深成指 / 创业板相对走势增强 → 微调（增强成长风格因子）
        sz = index.get("sz399001")
        cy = index.get("sz399006")
        if sz and cy:
            sz_pct = (sz["close"] - sz["open"]) / max(1e-6, sz["open"]) * 100
            cy_pct = (cy["close"] - cy["open"]) / max(1e-6, cy["open"]) * 100
            if sz_pct > pct + 0.8 and cy_pct > pct + 1.2:
                score += 0.5

        return score

    # --------------------------------------------------------------
    # Ⅱ. Liquidity 因子：成交额 / 换手率
    # --------------------------------------------------------------
    def compute_liquidity_factor(self):
        """
        使用：
            sse.turnover   上交所成交额（亿元）
            szse.turnover  深交所成交额（亿元）

        简化成：全市场成交额（亿元）衡量总体流动性
        """
        sse = self.raw.get("sse", {})
        szse = self.raw.get("szse", {})

        sse_turn = self._safe(sse, "turnover", 0.0)
        szse_turn = self._safe(szse, "turnover", 0.0)

        total_turnover = sse_turn + szse_turn

        # 市场水位判断：经验区间（可调）
        if total_turnover > 12000:      # > 1.2 万亿
            score = 1
        elif total_turnover > 8000:     # > 8000 亿
            score = 0
        else:
            score = -1

        return score

    # --------------------------------------------------------------
    # Ⅲ. Northbound 因子：北向趋势 + 当日规模
    # --------------------------------------------------------------
    def compute_north_factor(self):
        north = self.raw.get("north", {}).get("north", [])
        if not north:
            return 0.0

        # 只看最近 5 日
        last5 = north[-5:]
        net_list = [x.get("fund_net", 0.0) for x in last5]
        avg5 = mean(net_list)
        today = last5[-1].get("fund_net", 0.0)

        # 趋势分支
        if today > avg5 * 1.5:
            trend = 1.5
        elif today > avg5 * 1.1:
            trend = 1
        elif today > avg5 * 0.8:
            trend = 0
        else:
            trend = -1

        # 规模分支 —— 当日绝对值
        if today > 80e8:        # > 80 亿
            size = 1
        elif today > 10e8:
            size = 0.5
        elif today > 0:
            size = 0
        else:
            size = -0.5

        return trend + size
    # --------------------------------------------------------------
    # Ⅳ. Margin 因子：两融余额趋势（杠杆风险）
    # --------------------------------------------------------------
    def compute_margin_factor(self):
        """
        使用两融余额：rzrqye_100m（亿元）
        趋势向下 → 市场风险下降（加分）
        趋势向上 → 杠杆上升（减分）
        """

        margin = self.raw.get("margin", {}).get("margin", [])
        if len(margin) < 5:
            return 0.0

        # 最近 5 天
        last5 = margin[-5:]
        vals = [x["rzrqye_100m"] for x in last5]

        # 简单趋势（最后一天 - 第一天）
        diff = vals[-1] - vals[0]

        # 趋势判断
        if diff > 50:      # 最近 5 日两融上升 50 亿以上 → 风险
            score = -1.5
        elif diff > 10:
            score = -0.5
        elif diff > -10:
            score = 0
        elif diff > -50:
            score = 0.5
        else:
            score = 1

        return score

    # --------------------------------------------------------------
    # Ⅴ. Mainflow 因子：大盘主力/超大单净流入
    # --------------------------------------------------------------
    def compute_mainflow_factor(self):
        main = self.raw.get("mainflow")
        if not main:
            return 0.0

        main_net = self._safe(main, "main_net")
        super_net = self._safe(main, "super_net")

        # 主力资金（亿元级别）。值非常大，因为 push2 返回真实元级别。
        main_100m = main_net / 1e8
        super_100m = super_net / 1e8

        # 主力 → 风险偏多/偏空
        score = 0

        if main_100m > 100:
            score += 1
        elif main_100m < -100:
            score -= 1

        if super_100m > 80:
            score += 0.5
        elif super_100m < -80:
            score -= 0.5

        return score

    # --------------------------------------------------------------
    # Ⅵ. Emotion 因子：涨跌家数、中位数涨跌幅
    # --------------------------------------------------------------
    def compute_emotion_factor(self):
        """
        情绪因子结合：
            • advancers（上涨公司数）
            • decliners（下跌公司数）
            • market_avg_change（全市场平均涨跌幅）
        """

        br = self.raw.get("breadth", {})

        adv = self._safe(br, "advancers", 0)
        dec = self._safe(br, "decliners", 0)
        avg = self._safe(br, "market_avg_change", 0)

        # 涨跌家数比例
        total = adv + dec + 1e-9
        ratio = (adv - dec) / total

        # 情绪分数
        score = 0

        # 涨跌比
        if ratio > 0.3:
            score += 1
        elif ratio < -0.3:
            score -= 1

        # 平均涨跌幅
        if avg > 0.6:
            score += 1
        elif avg > 0.2:
            score += 0.5
        elif avg < -0.6:
            score -= 1
        elif avg < -0.2:
            score -= 0.5

        return score

    # --------------------------------------------------------------
    # Ⅶ. SectorFlow 因子：行业主力资金轮动（你测试成功）
    # --------------------------------------------------------------
    def compute_sector_factor(self):
        sectors = self.raw.get("sector", {}).get("sectors", [])
        if not sectors:
            return 0.0

        # 主力净流入从大到小排序
        large = sorted(sectors, key=lambda x: x.get("main_100m", 0.0), reverse=True)

        # 取前 10 个行业，反映“主攻方向”
        top10 = large[:10]
        vals = [x.get("main_100m", 0.0) for x in top10]

        if not vals:
            return 0.0

        avg_top10 = mean(vals)

        # 映射为情绪/风格分数
        if avg_top10 > 50:       # 前 10 行业主力 > 50 亿
            score = 1
        elif avg_top10 > 10:
            score = 0.5
        elif avg_top10 > -10:
            score = 0
        elif avg_top10 > -50:
            score = -1
        else:
            score = -2

        return score

    # --------------------------------------------------------------
    # Ⅷ. 总分：权重整合
    # --------------------------------------------------------------
    def compute_total_score(self, subscores):
        """
        当前统一权重（可以日后外置 YAML 配置）：
            Price:       20%
            Liquidity:   15%
            North:       20%
            Margin:      10%
            Mainflow:    10%
            Emotion:     10%
            Sector:      15%
        """
        w = {
            "price": 0.20,
            "liquidity": 0.15,
            "north": 0.20,
            "margin": 0.10,
            "mainflow": 0.10,
            "emotion": 0.10,
            "sector": 0.15,
        }

        total = 0
        for k, sc in subscores.items():
            total += sc * w.get(k, 0)

        return total

    # --------------------------------------------------------------
    # Ⅸ. 风险等级
    # --------------------------------------------------------------
    def determine_risk_level(self, score):
        if score >= 1.2:
            return "低风险（偏多）"
        elif score >= 0.3:
            return "中性偏低（中性偏多）"
        elif score >= -0.3:
            return "中性"
        elif score >= -1.0:
            return "偏高风险"
        else:
            return "高风险（建议减仓）"

    # --------------------------------------------------------------
    # Ⅹ. 解释生成
    # --------------------------------------------------------------
    def build_comment(self, subscores, level):
        lines = []
        lines.append(f"风险等级：{level}")
        lines.append("关键因子：")

        for k, v in subscores.items():
            flag = "↑" if v > 0 else ("↓" if v < 0 else "-")
            lines.append(f"・{k.capitalize():10s}: {v:+.2f} {flag}")

        return "\n".join(lines)

    # --------------------------------------------------------------
    # Ⅺ. 主入口
    # --------------------------------------------------------------
    def score_all(self):
        subscores = {}

        subscores["price"] = self.compute_price_factor()
        subscores["liquidity"] = self.compute_liquidity_factor()
        subscores["north"] = self.compute_north_factor()
        subscores["margin"] = self.compute_margin_factor()
        subscores["mainflow"] = self.compute_mainflow_factor()
        subscores["emotion"] = self.compute_emotion_factor()
        subscores["sector"] = self.compute_sector_factor()

        total = self.compute_total_score(subscores)
        level = self.determine_risk_level(total)
        comment = self.build_comment(subscores, level)

        return {
            "subscores": subscores,
            "total_score": total,
            "risk_level": level,
            "comment": comment,
        }
