# unified_risk/core/ashare/fetcher.py

from __future__ import annotations
from datetime import datetime
from typing import Dict, Any

from unified_risk.common.cache_manager import DayCacheManager
from unified_risk.common.logging_utils import get_logger
from unified_risk.core.global_market.fetcher import GlobalMarketFetcher

LOG = get_logger("UnifiedRisk.AShareFetcher")


class AShareFetcher:
    """
    A股数据汇总入口：
      - 指数涨跌（sh_pct / cyb_pct）
      - 市场宽度（advance / decline）
      - 成交额 & 流动性（sh_yi / sz_yi / drying）
      - 北向代理因子（mode C）
      - 全球市场（global 子字段）
    """

    def __init__(self) -> None:
        self.cache = DayCacheManager()
        self.global_fetcher = GlobalMarketFetcher()

    # ---------------- 主入口 ----------------

    def build_ashare_snapshot(self, bj_now: datetime) -> Dict[str, Any]:
        """
        统一输出 raw 结构（供 Engine / 因子 / 报告使用）
        """
        d = bj_now.date()

        # 尝试整包读取缓存（可选）
        cached = self.cache.read_json(d, "ashare_snapshot.json")
        if cached is not None:
            return cached

        # 各部分数据
        index_data = self._get_index_changes()
        breadth_data = self._get_breadth()
        turnover_data = self._get_turnover()
        north_data = self._get_northbound_proxy(bj_now)
        trend_data = self._get_ashare_trend()
        global_data = self.global_fetcher.build_global_snapshot(bj_now)

        snapshot: Dict[str, Any] = {
            "index": index_data,
            "breadth": breadth_data,
            "turnover": turnover_data,
            "north": north_data,
            "ashare_trend": trend_data,
            "global": global_data,
        }

        # 写入缓存
        self.cache.write_json(d, "ashare_snapshot.json", snapshot)
        return snapshot

    # ---------------- 指数涨跌 ----------------

    def _get_index_changes(self) -> Dict[str, float]:
        """
        返回：
            sh_pct  : 上证指数日涨跌（%）
            cyb_pct : 创业板指数日涨跌（%）
        """
        try:
            sh_pct = self._fetch_shanghai_index_change()
            cyb_pct = self._fetch_chuangye_index_change()
        except Exception as e:
            LOG.warning(f"[AShareFetcher] _get_index_changes error: {e}")
            sh_pct = 0.0
            cyb_pct = 0.0

        LOG.info(f"[RAW] SH | Change% : {sh_pct:8.3f}")
        LOG.info(f"[RAW] CYB| Change% : {cyb_pct:8.3f}")

        return {
            "sh_pct": sh_pct,
            "cyb_pct": cyb_pct,
        }

    def _fetch_shanghai_index_change(self) -> float:
        """
        真正获取上证指数涨跌的逻辑（推给你现有实现）：
        例如：
          - EastMoney push2 指数接口
          - 或者用 510300 ETF 收盘涨跌代替
        """
        # TODO: 填充你自己的实现
        return 0.0

    def _fetch_chuangye_index_change(self) -> float:
        """
        获取创业板指数涨跌
        """
        # TODO
        return 0.0

    # ---------------- 市场宽度 ----------------

    def _get_breadth(self) -> Dict[str, int]:
        """
        宽度：上涨家数 / 下跌家数
        """
        try:
            adv, dec = self._fetch_advance_decline()
        except Exception as e:
            LOG.warning(f"[AShareFetcher] _get_breadth error: {e}")
            adv, dec = 0, 0

        LOG.info(f"[RAW] ADV | Count : {adv:8d}")
        LOG.info(f"[RAW] DEC | Count : {dec:8d}")

        return {
            "advance": adv,
            "decline": dec,
        }

    def _fetch_advance_decline(self) -> tuple[int, int]:
        """
        从东财 push2 / akshare 获取全市场涨跌家数。
        """
        # TODO: 你原来做 breadth 的逻辑挪过来
        return 0, 0

    # ---------------- 成交额 & 流动性 ----------------

    def _get_turnover(self) -> Dict[str, Any]:
        """
        返回：
          sh_yi : 上海成交额（亿元）
          sz_yi : 深圳成交额（亿元）
          drying: 是否流动性枯竭（True/False）
        """
        try:
            sh_yi, sz_yi = self._fetch_turnover()
            drying = self._detect_liquidity_drying(sh_yi, sz_yi)
        except Exception as e:
            LOG.warning(f"[AShareFetcher] _get_turnover error: {e}")
            sh_yi, sz_yi, drying = 0.0, 0.0, False

        LOG.info(f"[RAW] Turnover | Shanghai(Yi): {sh_yi:10.3e}")
        LOG.info(f"[RAW] Turnover | Shenzhen(Yi): {sz_yi:10.3e}")
        LOG.info(f"[LIQ] drying={drying}")

        return {
            "sh_yi": sh_yi,
            "sz_yi": sz_yi,
            "drying": drying,
        }

    def _fetch_turnover(self) -> tuple[float, float]:
        """
        成交额获取逻辑（你可以用 ETF 替代 or 直接用东财市场成交额接口）。
        """
        # TODO: 填入你自己的实现
        return 0.0, 0.0

    def _detect_liquidity_drying(self, sh_yi: float, sz_yi: float) -> bool:
        """
        根据最近 20 日均值判断流动性是否枯竭。
        这里只保留接口，你可以在后续版本接入完整 LV 因子。
        """
        # TODO: 使用你已有的 vol_3d/20d 逻辑
        return False

    # ---------------- 北向代理（模式 C） ----------------

    def _get_northbound_proxy(self, bj_now: datetime) -> Dict[str, Any]:
        """
        返回 NPS 因子需要的 north 字段：
            mode
            etf_flow_yi
            nf_deal_amt
            hs300_pct
            a50_night_pct
            prev_etf_flow_yi
        """
        d = bj_now.date()

        # 先看今天有没有单独的 northbound_etf.json
        cached = self.cache.read_json(d, "northbound_etf.json")
        if cached is not None:
            # 确保字段存在
            cached.setdefault("mode", "C")
            cached.setdefault("nf_deal_amt", 0.0)
            cached.setdefault("hs300_pct", 0.0)
            cached.setdefault("a50_night_pct", 0.0)
            # 补充 T-1 ETF
            prev = self._load_prev_etf_flow(bj_now)
            cached["prev_etf_flow_yi"] = prev
            return cached

        # 没缓存 → 调用实际数据接口
        try:
            etf_flow_yi = self._fetch_etf_proxy_flow()
            nf_deal_amt = self._fetch_northbound_deal_amount()
            hs300_pct = self._fetch_hs300_change()
            a50_night_pct = self.global_fetcher.get_a50_night_change()
        except Exception as e:
            LOG.warning(f"[AShareFetcher] _get_northbound_proxy error: {e}")
            etf_flow_yi, nf_deal_amt, hs300_pct, a50_night_pct = 0.0, 0.0, 0.0, 0.0

        north = {
            "mode": "C",
            "etf_flow_yi": etf_flow_yi,
            "nf_deal_amt": nf_deal_amt,
            "hs300_pct": hs300_pct,
            "a50_night_pct": a50_night_pct,
        }

        # 写单独 northbound_etf.json，方便其它模块复用
        self.cache.write_json(d, "northbound_etf.json", north)

        # 再补 T-1 ETF
        north["prev_etf_flow_yi"] = self._load_prev_etf_flow(bj_now)
        return north

    def _load_prev_etf_flow(self, bj_now: datetime) -> float | None:
        """
        从前一日 northbound_etf.json 里读取 etf_flow_yi
        """
        from datetime import timedelta

        prev_date = bj_now.date() - timedelta(days=1)
        cached = self.cache.read_json(prev_date, "northbound_etf.json")
        if not cached:
            return None
        try:
            return float(cached.get("etf_flow_yi", 0.0))
        except Exception:
            return None

    def _fetch_etf_proxy_flow(self) -> float:
        """
        ETF 宽基流入代理：
        例如：
          - 510300.SS / 159901.SZ 等宽基 ETF 主力资金净流入合计
        """
        # TODO: 把你 T-RiskMonitor / UnifiedRisk 原有 ETF 流入逻辑迁移到这里
        return 0.0

    def _fetch_northbound_deal_amount(self) -> float:
        """
        北向总成交额 NF_DEAL_AMT（亿元），如果你有新的东财 datacenter 接口，可在此实现。
        """
        # TODO
        return 0.0

    def _fetch_hs300_change(self) -> float:
        """
        沪深300 日涨跌幅（%），可用 akshare/index 或 YF ETF 替代。
        """
        # TODO
        return 0.0

    # ---------------- A 股中短期趋势 ----------------

    def _get_ashare_trend(self) -> Dict[str, float]:
        """
        提供因子 a_short / a_mid 所需的：
          core_etf_ma5_pct
          shanghai_4w_pct
        """
        try:
            ma5 = self._fetch_core_etf_ma5()
            sh4w = self._fetch_shanghai_4w_change()
        except Exception as e:
            LOG.warning(f"[AShareFetcher] _get_ashare_trend error: {e}")
            ma5, sh4w = 0.0, 0.0

        return {
            "core_etf_ma5_pct": ma5,
            "shanghai_4w_pct": sh4w,
        }

    def _fetch_core_etf_ma5(self) -> float:
        """
        通过核心宽基 ETF（如 510300/159915/159901）计算近 5 日平均涨跌。
        """
        # TODO: 你已有 get_etf_daily / ma5 逻辑可以直接封装到这里
        return 0.0

    def _fetch_shanghai_4w_change(self) -> float:
        """
        上证指数近 4 周的累计涨跌幅（%）。
        """
        # TODO
        return 0.0
