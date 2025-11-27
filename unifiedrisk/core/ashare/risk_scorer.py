from typing import Dict, Any, List


class RiskScorer:
    """UnifiedRisk v4.0 RiskScorer

    在原有 4 个因子的基础上，预留并接入更多扩展因子：
    - macro_reflection_risk: 基于美元、黄金、原油、铜等宏观资产
    - style_switch: 风格切换（成长 vs 价值），从 payload.get("factors", {}) 中读取
    - vp_risk: 量价结构风险，同上
    - margin_speed: 两融节奏，同上
    - bear_trap: 假跌破陷阱，同上
    - tech_pattern: 技术形态风险，同上
    - policy_etf: 政策 ETF 资金流，同上

    说明：
    - 如果 payload 中没有提供对应扩展因子，则记为 0 分，描述为“未提供”，不影响总分。
    - 这样可以保证与你现有数据流兼容，新因子可以逐步接入。
    """

    def score(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        idx = payload.get("index_turnover", {})
        g = payload.get("global", {})
        macro = payload.get("macro", {})
        fact = payload.get("factors", {}) or {}

        # 原有 4 个因子
        t_s, t_d = self._turnover(idx)
        g_s, g_d = self._global(g)
        n_s, n_d = self._north(idx)
        l_s, l_d = self._liq(idx)

        # 新增：宏观反射因子（直接从 raw.macro 计算）
        m_s, m_d = self._macro(macro)

        # 扩展预留因子：从 payload["factors"] 透传，如果没有就为 0
        style_s, style_d = self._style_switch(fact)
        vp_s, vp_d = self._vp_risk(fact)
        margin_s, margin_d = self._margin_speed(fact)
        bear_s, bear_d = self._bear_trap(fact)
        tech_s, tech_d = self._tech_pattern(fact)
        policy_s, policy_d = self._policy_etf(fact)

        total = (
            t_s
            + g_s
            + n_s
            + l_s
            + m_s
            + style_s
            + vp_s
            + margin_s
            + bear_s
            + tech_s
            + policy_s
        )

        level = self._level(total)
        advice = self._adv(level)
        expl = self._expl(
            total,
            level,
            t_d,
            g_d,
            n_d,
            l_d,
            m_d,
            style_d,
            vp_d,
            margin_d,
            bear_d,
            tech_d,
            policy_d,
        )

        return {
            "turnover_score": t_s,
            "global_score": g_s,
            "north_score": n_s,
            "liquidity_score": l_s,
            "macro_reflection_risk": m_s,
            "style_switch": style_s,
            "vp_risk": vp_s,
            "margin_speed": margin_s,
            "bear_trap": bear_s,
            "tech_pattern": tech_s,
            "policy_etf": policy_s,
            "total_score": total,
            "risk_level": level,
            "advise": advice,
            "explanation": expl,
        }

    # ========== 原有逻辑 ==========
    def _turnover(self, idx):
        vals = [
            idx[k]["turnover"]
            for k in ["shanghai", "shenzhen", "chi_next"]
            if k in idx and "turnover" in idx[k]
        ]
        if not vals:
            return 0, ["成交额缺失"]
        total = sum(vals)
        d: List[str] = []
        s = 0
        if total > 7e10:
            s += 3
            d.append("全市场放量")
        elif total > 5e10:
            s += 1
            d.append("成交额偏强")
        elif total < 3e10:
            s -= 2
            d.append("明显缩量")
        else:
            d.append("成交额正常")
        return s, d

    def _global(self, g):
        s = 0
        d: List[str] = []
        nas = g.get("nasdaq", {}).get("change_pct", 0)
        spy = g.get("spy", {}).get("change_pct", 0)
        vix = g.get("vix", {}).get("last", 0)
        if nas < -1:
            s -= 2
            d.append(f"纳指下跌{nas}%")
        if spy < -0.5:
            s -= 1
            d.append(f"SPY下跌{spy}%")
        if vix > 22:
            s -= 2
            d.append(f"VIX={vix}")
        return s, d

    def _north(self, idx):
        cyb = idx.get("chi_next", {}).get("turnover", 0)
        if cyb > 3e9:
            return 1, ["北向偏强"]
        if cyb < 1e9:
            return -1, ["北向偏弱"]
        return 0, ["北向中性"]

    def _liq(self, idx):
        vol = idx.get("chi_next", {}).get("volume", 0)
        if vol < 3e8:
            return -2, ["创业板流动性下降"]
        return 0, ["流动性正常"]

    def _level(self, t):
        if t >= 4:
            return "Low"
        if t >= 0:
            return "Medium"
        if t >= -3:
            return "High"
        return "Extreme"

    def _adv(self, l):
        return {
            "Low": "加仓",
            "Medium": "观察",
            "High": "减仓",
            "Extreme": "规避",
        }[l]

    def _expl(self, total, level, *ds):
        lines = [f"风险等级：{level}（{total}分）", "", "【因子解读】"]
        for sec in ds:
            for x in sec:
                lines.append("- " + x)
        return "\n".join(lines)

    # ========== 新增扩展因子实现/占位 ==========
    def _macro(self, macro: Dict[str, Any]):
        """宏观反射因子：简单用美元、黄金、原油、铜来衡量风险偏好。

        规则（可日后再细化）：
        - 美元指数上行 + 黄金上行 → 风险偏好下降 → -1 分
        - 美元下行 + 黄金调整 → 风险偏好改善 → +1 分
        - 极端大涨大跌时，额外 +-1
        """
        s = 0
        d: List[str] = []

        usd = macro.get("usd", {}).get("change_pct", 0) or 0
        gold = macro.get("gold", {}).get("change_pct", 0) or 0
        oil = macro.get("oil", {}).get("change_pct", 0) or 0
        copper = macro.get("copper", {}).get("change_pct", 0) or 0

        if usd > 0.5 and gold > 0.5:
            s -= 1
            d.append(f"美元({usd}%) + 黄金({gold}%) 同涨 → 风险偏好下降")
        elif usd < -0.5 and gold < 0.5:
            s += 1
            d.append(f"美元({usd}%) 回落 + 黄金温和 → 风险偏好改善")

        if oil < -2:
            s -= 1
            d.append(f"油价大跌({oil}%) → 需求担忧")
        if copper > 2:
            s += 1
            d.append(f"铜价大涨({copper}%) → 经济预期改善")

        if not d:
            d.append("宏观情绪中性")
        return s, d

    def _style_switch(self, fact: Dict[str, Any]):
        v = fact.get("style_switch_score")
        if v is None:
            return 0, ["风格切换因子未提供"]
        desc = "风格偏成长" if v > 0 else "风格偏价值" if v < 0 else "风格均衡"
        return v, [f"Style Switch: {v}（{desc}）"]

    def _vp_risk(self, fact: Dict[str, Any]):
        v = fact.get("volume_price_score")
        if v is None:
            return 0, ["量价结构因子未提供"]
        return v, [f"量价结构风险: {v}"]

    def _margin_speed(self, fact: Dict[str, Any]):
        v = fact.get("margin_speed_score")
        if v is None:
            return 0, ["两融节奏因子未提供"]
        desc = "两融快速扩张" if v > 0 else "两融快速收缩" if v < 0 else "两融节奏平稳"
        return v, [f"Margin Speed: {v}（{desc}）"]

    def _bear_trap(self, fact: Dict[str, Any]):
        v = fact.get("bear_trap_score")
        if v is None:
            return 0, ["Bear Trap 因子未提供"]
        return v, [f"Bear Trap 信号: {v}"]

    def _tech_pattern(self, fact: Dict[str, Any]):
        v = fact.get("tech_pattern_score")
        if v is None:
            return 0, ["技术形态因子未提供"]
        return v, [f"技术形态风险: {v}"]

    def _policy_etf(self, fact: Dict[str, Any]):
        v = fact.get("policy_etf_score")
        if v is None:
            return 0, ["政策 ETF 因子未提供"]
        return v, [f"政策 ETF 资金流: {v}"]
