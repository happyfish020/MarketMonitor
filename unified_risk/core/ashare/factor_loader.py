from __future__ import annotations
from ...common.config_loader import get_etf_wide, get_etf_sector
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...common.cache_manager import CacheManager
from ...common.logger import get_logger
from ...common.time_utils import fmt_date_compact, now_bj

LOG = get_logger("UnifiedRisk.AShareFactor")


@dataclass
class AShareFactors:
    """承载 A 股日级因子结果的简单数据结构。"""
    values: Dict[str, float] = field(default_factory=dict)


class AShareFactorLoader:
    """
    A 股日级因子计算模块（v6.2f）

    设计要点：
    1. 所有原始数据必须先由 DataFetcher 写入 day_cache 后再使用；
    2. 本模块只做“从 raw → 因子得分”的映射；
    3. 因子符号约定（非常重要）：
       - > 0  表示“风险上升 / 偏空”
       - < 0  表示“风险下降 / 偏多”
    4. 所有因子得分建议控制在 [-3, +3] 区间，方便与其它模块兼容。

    支持的因子列表（初版）：
    - turnover_risk           : 成交额 / 换手率风险（占位版，可日后精细化）
    - northbound_risk         : 北向当日净流入方向因子
    - northbound_trend_risk   : 北向 T-1/T-2/T-3 三日趋势因子
    - margin_risk             : 两融（融资余额绝对水平 + 短期变化）
    - main_fund_risk          : 全市场主力资金净流向因子（push2 f62）
    - etf_wide_risk           : 宽基 ETF 资金流向因子
    - etf_sector_risk         : 行业/主题 ETF 资金流向因子
    """
 
    def __init__(self, cache: Optional[CacheManager] = None) -> None:
        # 独立的 CacheManager，指向同一 day_cache 目录即可
        self.cache = cache or CacheManager()
        self.WIDE_ETFS = set(get_etf_wide())
        self.SECTOR_ETFS = set(get_etf_sector())
    
    
    # ======================================================================
    # 外部主入口
    # ======================================================================
    def build_factors(self, raw: Any, date_str: Optional[str] = None) -> AShareFactors:
        """
        从原始数据构建 A 股日级因子。

        参数
        ----
        raw : 任意
            一般为 AShareRawData（dataclass）或 dict，要求至少含有：
            - turnover
            - northbound
            - margin
            - main_fund
            - etf_flow
        date_str : Optional[str]
            评估日期，格式 YYYYMMDD。
            如果为 None，则使用“今天的北京时间”。

        说明
        ----
        - 如果引擎目前只调用 build_factors(raw)，没有传 date_str，
          则本函数会使用“当前北京时间对应的 YYYYMMDD”来推算
          T-1/T-2/T-3；大部分“盘后跑今天”的场景是 OK 的。
        - 如果你后续想对任意历史日回算，建议在 AShareDailyEngine 中
          改为：self.factor_loader.build_factors(raw, date_str=date_str)
        """
        if date_str is None:
            date_str = fmt_date_compact(now_bj())

        # 兼容 dataclass 与 dict
        turnover = self._get_field(raw, "turnover")
        northbound = self._get_field(raw, "northbound")
        margin = self._get_field(raw, "margin")
        main_fund = self._get_field(raw, "main_fund")
        etf_flow = self._get_field(raw, "etf_flow")

        vals: Dict[str, float] = {}

        # 成交额 / 换手（目前仍是占位逻辑，等待你接入真实指标）
        vals["turnover_risk"] = self._score_turnover(turnover)

        # 北向当日方向 + T-1/T-2/T-3 趋势
        nb_today = self._compute_nb_netbuy_total(northbound)
        vals["northbound_risk"] = self._score_northbound_today(nb_today)

        nb_trend_score = self._score_northbound_trend(date_str, nb_today)
        vals["northbound_trend_risk"] = nb_trend_score

        # 两融（融资余额绝对水平 + 变化）
        vals["margin_risk"] = self._score_margin(date_str, margin)

        # 主力资金（全市场 f62 总和）
        vals["main_fund_risk"] = self._score_main_fund(main_fund)

        # ETF：宽基 & 行业/主题
        wide_score, sector_score = self._score_etf_flows(etf_flow)
        vals["etf_wide_risk"] = wide_score
        vals["etf_sector_risk"] = sector_score

        LOG.debug(f"AShare factors for {date_str}: {vals}")
        return AShareFactors(values=vals)

    # ======================================================================
    # 通用工具
    # ======================================================================
    def _get_field(self, raw: Any, name: str) -> Dict[str, Any]:
        """兼容 dataclass / 对象 / dict 的字段获取。"""
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw.get(name, {}) or {}
        # dataclass / 对象
        return getattr(raw, name, {}) or {}

    def _get_em_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        EM datacenter 返回格式兼容：
        - {"result": {"data": [ {...}, ... ]}}
        - {"data": [ {...}, ... ]}
        """
        if not isinstance(data, dict):
            return []
        if isinstance(data.get("result"), dict) and isinstance(data["result"].get("data"), list):
            return data["result"]["data"]
        if isinstance(data.get("data"), list):
            return data["data"]
        return []

    def _get_push2_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        push2 返回格式兼容：
        - {"data": {"diff": [ {...}, ... ]}}
        - {"data": [ {...}, ... ]}
        """
        if not isinstance(data, dict):
            return []
        d = data.get("data")
        if isinstance(d, dict) and isinstance(d.get("diff"), list):
            return d["diff"]
        if isinstance(d, list):
            return d
        return []

    def _clamp(self, v: float, lo: float = -3.0, hi: float = 3.0) -> float:
        return max(lo, min(hi, v))

    # ======================================================================
    # 1. 成交额 / 换手因子（占位逻辑）
    # ======================================================================
    def _score_turnover(self, turnover_data: Dict[str, Any]) -> float:
        """
        成交额 / 换手率风险因子（暂时占位版）。

        当前实现：
        - 尝试从 EM 风格数据里取出“绝对值最大的数值”作为代表成交规模；
        - 规模越大 → 短期情绪越亢奋 → 风险越高（正分）；
        - 规模较小则接近 0。

        TODO（你可以后续增强）：
        - 接入全市场成交额 / 换手率的分位数；
        - 明确区分“地量”与“放量”的不同含义（有时地量也意味着中期机会）。
        """
        items = self._get_em_items(turnover_data)
        if not items:
            return 0.0

        nums: List[float] = []
        for rec in items:
            if not isinstance(rec, dict):
                continue
            for v in rec.values():
                if isinstance(v, (int, float)):
                    nums.append(float(v))
                else:
                    try:
                        nums.append(float(v))
                    except Exception:
                        continue
        if not nums:
            return 0.0

        val = max(nums, key=lambda x: abs(x))
        score = abs(val) / 1e11  # 1e11 级别 ≈ 1 分
        return self._clamp(score, 0.0, 3.0)

    # ======================================================================
    # 2. 北向当日净流入因子
    # ======================================================================
    def _compute_nb_netbuy_total(self, nb_data: Dict[str, Any]) -> float:
        """
        计算当日北向净买入总额（单位统一为“元”）。

        兼容字段：
        - HSNETBUYAMT / SZNETBUYAMT（常见命名）
        - NETBUY_AMT_S / NETBUY_AMT_H（另一套命名）
        - 兜底：凡是 key 中包含 "NETBUY" 的数值字段都会累加。
        """
        items = self._get_em_items(nb_data)
        if not items:
            return 0.0

        total = 0.0
        for rec in items:
            if not isinstance(rec, dict):
                continue
            # 优先识别常用字段
            for key in ("HSNETBUYAMT", "SZNETBUYAMT", "NETBUY_AMT_S", "NETBUY_AMT_H"):
                if key in rec:
                    try:
                        total += float(rec[key])
                    except Exception:
                        pass

            # 兜底：任何包含 NETBUY 的字段
            for k, v in rec.items():
                if "NETBUY" in k.upper():
                    try:
                        total += float(v)
                    except Exception:
                        continue

        return float(total)

    def _score_northbound_today(self, nb_total: float) -> float:
        """
        北向当日净流入方向因子。

        约定：
        - 净流入（>0） → 视为“外资偏多” → 风险下降（负分）
        - 净流出（<0） → 视为“外资偏空” → 风险上升（正分）
        """
        if nb_total == 0:
            return 0.0

        base = abs(nb_total) / 3e9  # 3e9 级别 ≈ 满分 3
        base = self._clamp(base, 0.0, 3.0)
        if nb_total > 0:
            return -base
        else:
            return base

    # ======================================================================
    # 3. 北向 T-1/T-2/T-3 趋势因子
    # ======================================================================
    def _score_northbound_trend(self, date_str: str, nb_today: float) -> float:
        """
        北向三日趋势因子（T-1/T-2/T-3）。

        逻辑：
        1. 回看最近 3 个“自然日”的 day_cache（含当日），各自的 netbuy_total；
        2. 构造序列 [T-3, T-2, T-1, T]，如数据不足则按已有长度；
        3. 评估：
           - 总体方向：sum(netbuy)
           - 变化趋势：近几天是放大还是减弱
        4. 方向一致且放大 → 趋势明确 → 放大得分；
           方向混乱 → 得分接近 0。
        """
        # 先把当日值存入缓存，方便未来使用
        try:
            derived = {"netbuy_total": nb_today}
            self.cache.write_key(date_str, "ashare", "northbound_derived", derived)
        except Exception as e:
            LOG.debug(f"write northbound_derived failed: {e}")

        # 收集最近 4 天（T-3~T）的 netbuy_total
        dates: List[str] = []
        try:
            base_date = datetime.strptime(date_str, "%Y%m%d")
        except Exception:
            base_date = datetime.strptime(fmt_date_compact(now_bj()), "%Y%m%d")

        for offset in range(0, 4):  # 0: T, 1: T-1, 2: T-2, 3: T-3
            d = base_date - timedelta(days=offset)
            dates.append(d.strftime("%Y%m%d"))

        series: List[float] = []
        for ds in reversed(dates):  # [T-3, T-2, T-1, T]
            cache_day = self.cache.load_day_cache(ds)
            nb_derived = (
                cache_day.get("ashare", {})
                .get("northbound_derived", {})
            )
            if isinstance(nb_derived, dict) and "netbuy_total" in nb_derived:
                series.append(float(nb_derived["netbuy_total"]))
            else:
                # 尝试从 raw northbound 现算
                nb_raw = (
                    cache_day.get("ashare", {})
                    .get("northbound", {})
                )
                val = self._compute_nb_netbuy_total(nb_raw)
                series.append(val)

        if len(series) < 3:
            return 0.0

        # 只拿最后 4 个中的 T-3, T-2, T-1, T
        nb_T3, nb_T2, nb_T1, nb_T = series[0], series[1], series[2], series[3]

        # 三日总和
        nb_3d_sum = nb_T3 + nb_T2 + nb_T1

        # 简单趋势：最近三天的“加速度”
        diff1 = nb_T2 - nb_T3
        diff2 = nb_T1 - nb_T2
        trend = diff1 + diff2

        # 方向一致性：如果 3 日合计方向与趋势方向一致，则加强信号
        dir_score = 0.0
        if nb_3d_sum > 0 and trend > 0:
            dir_score = 1.0
        elif nb_3d_sum < 0 and trend < 0:
            dir_score = 1.0
        elif nb_3d_sum * trend < 0:
            # 方向相反 → 明显“反转” → 减弱信号
            dir_score = -0.5

        # 基础强度：取三日合计绝对值
        base = abs(nb_3d_sum) / 5e9  # 5e9 级别 ≈ 2.5 分
        base = min(base, 2.5)

        # 最终得分
        # 总方向：三日合计净流入 → 趋势偏多 → 负分；净流出 → 正分
        if nb_3d_sum > 0:
            score = -base
        elif nb_3d_sum < 0:
            score = base
        else:
            score = 0.0

        # 加上方向一致性修正
        score += dir_score
        return self._clamp(score, -3.0, 3.0)

    # ======================================================================
    # 4. 两融因子（RPT_MARGIN）
    # ======================================================================
    def _compute_rzye_total(self, margin_data: Dict[str, Any]) -> float:
        """
        融资余额总量（RZYE）。

        从 EM margin 报表中：
        - 遍历所有记录，凡是 key 含 "RZYE" 的字段累加。
        """
        items = self._get_em_items(margin_data)
        if not items:
            return 0.0

        total = 0.0
        for rec in items:
            if not isinstance(rec, dict):
                continue
            for k, v in rec.items():
                if "RZYE" in k.upper():
                    try:
                        total += float(v)
                    except Exception:
                        continue
        return float(total)

    def _score_margin(self, date_str: str, margin_data: Dict[str, Any]) -> float:
        """
        两融风险因子：融资余额绝对水平 + 短期变化。

        思路：
        - 绝对水平：RZYE 当前总量，越大说明系统杠杆越高 → 风险 +；
        - 单日变化：RZYE_T - RZYE_T-1，快速加杠杆 → 额外加分。
        """
        rz_today = self._compute_rzye_total(margin_data)

        # 写入 derived，方便未来复用
        try:
            self.cache.write_key(
                date_str, "ashare", "margin_derived", {"rzye_total": rz_today}
            )
        except Exception as e:
            LOG.debug(f"write margin_derived failed: {e}")

        # 获取昨日 rzye
        try:
            base_date = datetime.strptime(date_str, "%Y%m%d")
        except Exception:
            base_date = datetime.strptime(fmt_date_compact(now_bj()), "%Y%m%d")
        yest_str = (base_date - timedelta(days=1)).strftime("%Y%m%d")

        cache_yest = self.cache.load_day_cache(yest_str)
        rz_yest = 0.0
        m_derived = (
            cache_yest.get("ashare", {})
            .get("margin_derived", {})
        )
        if isinstance(m_derived, dict) and "rzye_total" in m_derived:
            try:
                rz_yest = float(m_derived["rzye_total"])
            except Exception:
                rz_yest = 0.0
        else:
            m_raw = (
                cache_yest.get("ashare", {})
                .get("margin", {})
            )
            rz_yest = self._compute_rzye_total(m_raw)

        # 风险打分
        # 1）绝对水平（越高风险越大）
        level = abs(rz_today) / 5e11 * 2.0  # 5e11 级别 ≈ 2 分
        level = max(0.0, min(level, 2.0))

        # 2）变化量（快速加杠杆 → 额外 +1）
        delta = rz_today - rz_yest
        delta_score = 0.0
        if delta > 0:
            delta_score = min(delta / 1e10, 1.0)  # 每增加 1e10，+1 分封顶
        # 杠杆下降不直接给负分，只是少加分

        score = level + max(delta_score, 0.0)
        return self._clamp(score, 0.0, 3.0)

    # ======================================================================
    # 5. 主力资金因子（push2 f62）
    # ======================================================================
    def _sum_f62(self, data: Dict[str, Any]) -> float:
        items = self._get_push2_items(data)
        if not items:
            return 0.0
        total = 0.0
        for rec in items:
            if not isinstance(rec, dict):
                continue
            v = rec.get("f62")
            if v in (None, ""):
                continue
            try:
                total += float(v)
            except Exception:
                continue
        return float(total)

    def _score_main_fund(self, main_fund_data: Dict[str, Any]) -> float:
        """
        主力资金净流向因子（全市场 f62 汇总）。

        约定：
        - 净流入 → 主力偏多 → 风险下降（负分）
        - 净流出 → 主力偏空 → 风险上升（正分）
        """
        total = self._sum_f62(main_fund_data)
        if total == 0:
            return 0.0
        base = abs(total) / 3e9  # 3e9 ≈ 3 分
        base = self._clamp(base, 0.0, 3.0)
        return -base if total > 0 else base

    # ======================================================================
    # 6. ETF 因子：宽基 + 行业
    # ======================================================================
    def _score_etf_flows(self, etf_data: Dict[str, Any]) -> (float, float):
        """
        ETF 资金流向因子（宽基 + 行业/主题）。

        - 宽基 ETF：代表整体市场/主要指数的资金偏好；
        - 行业/主题 ETF：代表结构性抱团或资金撤退。
        """
        items = self._get_push2_items(etf_data)
        if not items:
            return 0.0, 0.0

        wide_total = 0.0
        sector_total = 0.0

        for rec in items:
            if not isinstance(rec, dict):
                continue
            code = rec.get("f12")
            if not code:
                continue
            code = str(code).strip()

            v = rec.get("f62")
            if v in (None, ""):
                continue
            try:
                amt = float(v)
            except Exception:
                continue

            if code in self.WIDE_ETFS:
                wide_total += amt
            if code in self.SECTOR_ETFS:
                sector_total += amt

        # 宽基：整体风险方向
        wide_score = 0.0
        if wide_total != 0:
            base = abs(wide_total) / 2e9  # 2e9 ≈ 3 分（更敏感）
            base = self._clamp(base, 0.0, 3.0)
            wide_score = -base if wide_total > 0 else base

        # 行业：结构性风险/机会
        sector_score = 0.0
        if sector_total != 0:
            base = abs(sector_total) / 1.5e9  # 1.5e9 ≈ 3 分，更敏感
            base = self._clamp(base, 0.0, 3.0)
            sector_score = -base if sector_total > 0 else base

        return wide_score, sector_score
