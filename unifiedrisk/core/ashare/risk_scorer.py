
"""
unifiedrisk/core/ashare/risk_scorer.py

Simplified risk scoring module for UnifiedRisk v4.

- 输入: raw dict, 结构与 UnifiedRisk main 输出中的 "raw" 一致
- 输出: score dict, 包含各个因子得分、总分、风险等级、操作建议与中文解释说明

本版本重点：
1）在原有成交额 / 北向 / 流动性得分基础上，加入宏观反射因子 macro_reflection_risk
2）预留风格切换 / 量价结构 / 两融节奏 / Bear Trap / 技术形态 / 政策 ETF 等因子
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Tuple


@dataclass
class RiskScoreResult:
    turnover_score: int = 0
    global_score: int = 0
    north_score: int = 0
    liquidity_score: int = 0

    # v4 新增 / 预留因子
    macro_reflection_risk: int = 0
    style_switch: int = 0
    vp_risk: int = 0
    margin_speed: int = 0
    bear_trap: int = 0
    tech_pattern: int = 0
    policy_etf: int = 0

    total_score: int = 0
    risk_level: str = "Medium"
    advise: str = "观察"
    explanation: str = ""


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


class RiskScorer:
    """
    统一的 A 股日级风险打分器。

    使用方式（示例）::

        scorer = RiskScorer(raw_payload)
        score_dict = scorer.run()
    """

    def __init__(self, raw: Dict[str, Any]) -> None:
        self.raw = raw or {}
        self.result = RiskScoreResult()
        self._explanation_parts: list[str] = []

    # === 对外主入口 ===

    def run(self) -> Dict[str, Any]:
        """主流程：计算各项因子分数 → 聚合成总分与风险等级。"""
        self._score_turnover()
        self._score_global()
        self._score_north()
        self._score_liquidity()
        self._score_macro_reflection()
        self._aggregate()

        # 生成中文解释
        self.result.explanation = self._build_explanation()
        return asdict(self.result)

    # === 各单项因子 ===

    def _score_turnover(self) -> None:
        """
        成交额因子：
        - 使用 510300 / 159901 / 159915 的成交额简单求和作为当日市场活跃度 proxy
        - 目前先用绝对阈值（后续可以接入历史均值进行标准化）
        """
        idx = self.raw.get("index_turnover") or {}

        def get_turn(name: str) -> float:
            node = idx.get(name) or {}
            val = node.get("turnover")
            if val is None:
                # 某些场景只给了 price + volume
                price = _safe_float(node.get("price")) or 0.0
                volume = _safe_float(node.get("volume")) or 0.0
                return float(price * volume)
            return float(val)

        sh = get_turn("shanghai")
        sz = get_turn("shenzhen")
        cyb = get_turn("chi_next")
        total = sh + sz + cyb

        score = 0
        if total >= 60e9:
            score = 2
            self._explanation_parts.append("成交额明显放量")
        elif total >= 50e9:
            score = 1
            self._explanation_parts.append("成交额偏强")
        elif total >= 30e9:
            score = 0
            self._explanation_parts.append("成交额中性")
        else:
            score = -2
            self._explanation_parts.append("明显缩量")

        self.result.turnover_score = int(score)

    def _score_global(self) -> None:
        """
        外围市场因子（当前版本先简化为 0，后续可接入完整的 GlobalRisk 信号）.
        """
        # 这里保留接口，方便未来接 GlobalRisk 的 us_daily / macro_scoring 结果
        self.result.global_score = 0

    def _score_north(self) -> None:
        """
        北向资金因子：
        - 当前 A 股不再披露实时北向，v4 暂不直接打分，统一返回 0（中性）
        - 后续可以接入 ETF 流入替代（如 510300 / 159915 等）再调整
        """
        self.result.north_score = 0

    def _score_liquidity(self) -> None:
        """
        流动性因子：
        - 预留接口，当前统一视为“流动性正常”，得分为 0
        - 后续可接入融资余额 / 质押风险 / 小盘成交占比等信号
        """
        self.result.liquidity_score = 0

    def _score_macro_reflection(self) -> None:
        """
        宏观 / 大宗对 A 股的反射风险（macro_reflection_risk）：

        设计思路：
        - 黄金大涨 → 偏避险，单独来看对股市是压力（-1）
        - 期铜大涨 → 经济预期改善，对周期 / 权重股偏利好（+2）
        - 美元走强 → 流动性偏紧，对新兴市场略偏空（-1）
        - 美元走弱 → 有利于全球风险资产（+1）
        - 原油大涨 / 大跌主要影响通胀与成本，这里只在极端时略作调整

        最终分数限制在 [-2, +2] 区间。
        """
        macro = self.raw.get("macro") or {}

        def get_pct(key: str) -> float | None:
            node = macro.get(key) or {}
            return _safe_float(node.get("change_pct"))

        dxy = get_pct("usd")
        gold = get_pct("gold")
        oil = get_pct("oil")
        copper = get_pct("copper")

        score = 0

        # 美元指数
        if dxy is not None:
            if dxy > 0.8:
                score -= 1
            elif dxy < -0.8:
                score += 1

        # 黄金变动：避险情绪
        if gold is not None:
            if gold > 1.5:
                # 黄金大涨，偏避险
                score -= 1
            elif gold < -1.5:
                score += 1

        # 期铜：对经济预期的反射更强，权重稍大
        if copper is not None:
            if copper > 2.0:
                score += 2
            elif copper < -2.0:
                score -= 2

        # 原油极端波动时略作调整
        if oil is not None:
            if oil > 4.0:
                score -= 1
            elif oil < -4.0:
                score += 1

        # 裁剪到 [-2, 2]
        if score > 2:
            score = 2
        elif score < -2:
            score = -2

        self.result.macro_reflection_risk = int(score)

        # 解释文案（只在有可用数据时输出）
        if copper is not None and abs(copper) >= 2.0:
            if copper > 0:
                self._explanation_parts.append(f"铜价大涨({copper:.3f}%) → 经济预期改善")
            else:
                self._explanation_parts.append(f"铜价大跌({copper:.3f}%) → 周期风险上升")

    # === 聚合 & 文案 ===

    def _aggregate(self) -> None:
        """
        汇总所有因子得分，并给出风险等级与操作建议。

        当前简单规则：
        - total <= -4  : Extreme / 规避
        - -4 < total <= -1 : High / 减仓
        - -1 < total <= 2 : Medium / 观察
        - total > 2   : Low / 持有
        """
        # 聚合所有显式列出的因子（方便以后增加新因子）
        factor_scores = [
            self.result.turnover_score,
            self.result.global_score,
            self.result.north_score,
            self.result.liquidity_score,
            self.result.macro_reflection_risk,
            self.result.style_switch,
            self.result.vp_risk,
            self.result.margin_speed,
            self.result.bear_trap,
            self.result.tech_pattern,
            self.result.policy_etf,
        ]
        total = int(sum(x for x in factor_scores if x is not None))
        self.result.total_score = total

        if total <= -4:
            level = "Extreme"
            advise = "规避"
        elif total <= -1:
            level = "High"
            advise = "减仓"
        elif total <= 2:
            level = "Medium"
            advise = "观察"
        else:
            level = "Low"
            advise = "持有"

        self.result.risk_level = level
        self.result.advise = advise

    def _build_explanation(self) -> str:
        """
        生成最终中文解释文本，格式示例：

        风险等级：High（-1分）

        【因子解读】
        - 明显缩量
        - 北向中性
        - 流动性正常
        - 铜价大涨(2.902%) → 经济预期改善
        - 风格切换因子未提供
        ...
        """
        lines: list[str] = []

        # 1) 头部总览
        lines.append(f"风险等级：{self.result.risk_level}（{self.result.total_score}分）")
        lines.append("")
        lines.append("【因子解读】")

        # 2) 核心三大因子（成交额 / 北向 / 流动性）
        # Turnover
        if self.result.turnover_score <= -2:
            lines.append("- 明显缩量")
        elif self.result.turnover_score >= 2:
            lines.append("- 成交额明显放量")
        elif self.result.turnover_score == 1:
            lines.append("- 成交额偏强")
        else:
            lines.append("- 成交额中性")

        # Northbound
        if self.result.north_score > 0:
            lines.append("- 北向偏强")
        elif self.result.north_score < 0:
            lines.append("- 北向偏弱")
        else:
            lines.append("- 北向中性")

        # Liquidity
        if self.result.liquidity_score < 0:
            lines.append("- 流动性偏紧")
        elif self.result.liquidity_score > 0:
            lines.append("- 流动性宽松")
        else:
            lines.append("- 流动性正常")

        # 3) 宏观 / 大宗说明（如果 _score_macro_reflection 已经加入）
        for part in self._explanation_parts:
            lines.append(f"- {part}")

        # 4) 预留因子说明
        lines.append("- 风格切换因子未提供")
        lines.append("- 量价结构因子未提供")
        lines.append("- 两融节奏因子未提供")
        lines.append("- Bear Trap 因子未提供")
        lines.append("- 技术形态因子未提供")
        lines.append("- 政策 ETF 因子未提供")

        return "\n".join(lines)


def score_ashare_risk(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    对外的便捷函数封装，便于在 main.py / engine 中直接调用。

    示例::

        from unifiedrisk.core.ashare.risk_scorer import score_ashare_risk

        result = score_ashare_risk(raw_payload)
    """
    return RiskScorer(raw).run()
