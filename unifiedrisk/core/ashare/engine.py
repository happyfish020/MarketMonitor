from typing import Any, Dict, Optional
import datetime as dt

from unifiedrisk.common.scoring import (
    normalize_score,
    aggregate_factor_scores,
    classify_level_with_advice,
)
from .data_fetcher import DataFetcher, BJ_TZ


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


class AShareDailyEngine:
    """
    v4.3.7 日级风险引擎：
      - Turnover: ETF 代理的沪深成交额（510300+159901）
      - Volume: ETF 代理的成交量
      - Breadth: ETF 涨跌方向宽度 proxy
      - VolumeRatio: ETF 简易 5 日量比
      - Northbound: 北向净买入占成交
      - Margin: 两融余额增减（RZRQYECZ）
      - MarketBreadth: push2.clist 的上涨/下跌家数宽度
      - MarketTrend: clist 的平均涨跌幅
    """

    def __init__(self, fetcher: Optional[DataFetcher] = None):
        self.fetcher = fetcher or DataFetcher()

    def run(self, date: Optional[dt.date] = None, as_dict: bool = False) -> Dict[str, Any]:
        d = date or dt.datetime.now(BJ_TZ).date()
        raw = self.fetcher.fetch_daily_snapshot(d)

        etf = raw.get("etf") or {}
        sh = etf.get("sh") or {}
        sz = etf.get("sz") or {}

        margin = raw.get("margin") or {}
        north_list = raw.get("north") or []
        market = raw.get("market") or {}

        factor_scores: Dict[str, float] = {}
        factor_explain: Dict[str, str] = {}

        # --- 1) 成交额因子（ETF Turnover，单位：亿元） ---
        sh_turn = float(sh.get("turnover_100m") or 0.0)
        sz_turn = float(sz.get("turnover_100m") or 0.0)
        total_turn = sh_turn + sz_turn

        if total_turn <= 0:
            sc_turn = 0.0
            factor_explain["Turnover"] = "ETF 成交额数据缺失或为 0，按 0 分处理。"
        else:
            # 这里依然用 2000~12000 亿元作为区间
            sc_turn = normalize_score(total_turn, lo=2000.0, hi=12000.0)
            factor_explain["Turnover"] = (
                f"ETF 代理估算的沪深合计成交额约 {total_turn:.0f} 亿元，映射得分 {sc_turn:+.2f}。"
            )
        factor_scores["Turnover"] = sc_turn

        # --- 2) 成交量因子（Volume） ---
        sh_vol = float(sh.get("volume") or 0.0)
        sz_vol = float(sz.get("volume") or 0.0)
        total_vol = sh_vol + sz_vol

        if total_vol <= 0:
            sc_vol = 0.0
            factor_explain["Volume"] = "ETF 成交量数据缺失或为 0，按 0 分处理。"
        else:
            # 区间可以后续根据实盘再调，这里先给一个大致范围
            sc_vol = normalize_score(total_vol, lo=5e7, hi=3e8)
            factor_explain["Volume"] = (
                f"两只 ETF 成交量合计约 {total_vol:.2e}，映射得分 {sc_vol:+.2f}。"
            )
        factor_scores["Volume"] = sc_vol

        # --- 3) ETF Breadth：根据两只 ETF 涨跌方向构造简化宽度 ---
        sh_close = float(sh.get("close") or 0.0)
        sh_prev = float(sh.get("prev_close") or 0.0)
        sz_close = float(sz.get("close") or 0.0)
        sz_prev = float(sz.get("prev_close") or 0.0)

        sh_dir = _sign(sh_close - sh_prev)
        sz_dir = _sign(sz_close - sz_prev)
        width_proxy = sh_dir + sz_dir  # ∈ [-2, 2]

        if sh_close == 0 and sz_close == 0:
            sc_breadth = 0.0
            factor_explain["Breadth"] = "ETF 涨跌方向数据缺失，按 0 分处理。"
        else:
            sc_breadth = normalize_score(width_proxy, lo=-2.0, hi=2.0)
            factor_explain["Breadth"] = (
                f"ETF 涨跌方向宽度 proxy={width_proxy:.0f}（两只 ETF 涨跌组合），得分 {sc_breadth:+.2f}。"
            )
        factor_scores["Breadth"] = sc_breadth

        # --- 4) VolumeRatio：两只 ETF 量比平均值 ---
        sh_vr = float(sh.get("volume_ratio") or 0.0)
        sz_vr = float(sz.get("volume_ratio") or 0.0)
        if sh_vr <= 0 and sz_vr <= 0:
            sc_vr = 0.0
            factor_explain["VolumeRatio"] = "ETF 量比数据缺失，按 0 分处理。"
        else:
            vr = 0.0
            cnt = 0
            if sh_vr > 0:
                vr += sh_vr
                cnt += 1
            if sz_vr > 0:
                vr += sz_vr
                cnt += 1
            if cnt > 0:
                vr /= cnt
            sc_vr = normalize_score(vr, lo=0.5, hi=2.5)
            factor_explain["VolumeRatio"] = (
                f"ETF 平均量比约 {vr:.2f}，映射得分 {sc_vr:+.2f}。"
            )
        factor_scores["VolumeRatio"] = sc_vr

        # --- 5) 北向因子（净流入占成交之比） ---
        if north_list:
            net_sum = sum(float(r.get("northbound_net") or 0.0) for r in north_list)
            tot_sum = sum(float(r.get("northbound_total") or 0.0) for r in north_list) or 1.0
            ratio = net_sum / tot_sum
            sc_nb = normalize_score(ratio, lo=-0.05, hi=0.05)
            factor_explain["Northbound"] = (
                f"北向净买入 {net_sum:.2e}，占成交 {ratio:.2%}，得分 {sc_nb:+.2f}。"
            )
        else:
            sc_nb = 0.0
            factor_explain["Northbound"] = "北向数据缺失或为空，按 0 分处理。"
        factor_scores["Northbound"] = sc_nb

        # --- 6) 两融因子（两融余额增减_亿） ---
        rz_delta = float(margin.get("两融余额增减_亿") or 0.0)
        if rz_delta == 0.0:
            sc_margin = 0.0
            factor_explain["Margin"] = "两融余额增减为 0 或数据缺失，按 0 分处理。"
        else:
            # -50 亿 ~ +50 亿 映射到 [-3, 3]
            sc_margin = normalize_score(rz_delta, lo=-50.0, hi=50.0)
            factor_explain["Margin"] = (
                f"两融余额当日变动约 {rz_delta:.2f} 亿元，映射得分 {sc_margin:+.2f}。"
            )
        factor_scores["Margin"] = sc_margin

        # --- 7) MarketBreadth：来自 clist 的上涨/下跌家数宽度 ---
        up = float(market.get("up") or 0.0)
        down = float(market.get("down") or 0.0)
        flat = float(market.get("flat") or 0.0)
        breadth_val = float(market.get("breadth") or 0.0)
        if up == 0 and down == 0 and flat == 0:
            sc_mbreadth = 0.0
            factor_explain["MarketBreadth"] = "clist 市场宽度数据缺失，按 0 分处理。"
        else:
            # -1500 ~ +1500 对应 [-3, 3]，后续可以根据实盘再调
            sc_mbreadth = normalize_score(breadth_val, lo=-1500.0, hi=1500.0)
            factor_explain["MarketBreadth"] = (
                f"上涨家数 {up:.0f} / 下跌家数 {down:.0f} / 平盘 {flat:.0f}，"
                f"宽度={breadth_val:.0f}，得分 {sc_mbreadth:+.2f}。"
            )
        factor_scores["MarketBreadth"] = sc_mbreadth

        # --- 8) MarketTrend：clist 平均涨跌幅 ---
        mean_change = float(market.get("mean_change") or 0.0)
        if up == 0 and down == 0 and flat == 0:
            sc_mtrend = 0.0
            factor_explain["MarketTrend"] = "clist 平均涨跌幅数据缺失，按 0 分处理。"
        else:
            # -3% ~ +3% 映射 [-3, 3]
            sc_mtrend = normalize_score(mean_change, lo=-3.0, hi=3.0)
            factor_explain["MarketTrend"] = (
                f"市场平均涨跌幅约 {mean_change:.2f}% ，得分 {sc_mtrend:+.2f}。"
            )
        factor_scores["MarketTrend"] = sc_mtrend

        # --- 汇总 ---
        total_score = aggregate_factor_scores(factor_scores)
        level_info = classify_level_with_advice(total_score)

        payload: Dict[str, Any] = {
            "meta": {
                "date": raw.get("date"),
                "bj_time": raw.get("bj_time"),
                "version": raw.get("version", "UnifiedRisk_v4.3.7"),
            },
            "factor_scores": factor_scores,
            "factor_explain": factor_explain,
            "total_risk_score": total_score,
            **level_info,
            "raw": raw,
        }
        return payload
