
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from .config import VERSION, REPORT_DIR
from .utils.logging_utils import setup_logger
from .utils.time_utils import now_bj
from .utils.history_utils import save_daily_snapshot, load_recent_snapshots
from .datasources.ashare_bridge import get_daily_snapshot
from .scoring.macro_scoring import GlobalMacroSnapshot, score_global_macro
from .scoring.ashare_daily_scoring import AShareDailyInputs, compute_ashare_daily_score

logger = setup_logger(__name__)


@dataclass
class GlobalRiskResult:
    version: str
    macro_score: float
    macro_level: str
    macro_desc: str
    ashare_daily_total: float
    ashare_daily_level: str
    ashare_daily_detail: Dict[str, Any]
    snapshot_raw: Dict[str, Any]


class GlobalRiskEngine:
    """
    v5.4.full 统一引擎：
      - run_daily() 同时计算：外围宏观 + A 股日级风险 + T-5 提前预警
      - 不再依赖 python -m 调用，可直接 import 使用
    """

    def __init__(self) -> None:
        self.version = VERSION

    # ------------------------------------------------------------------
    #  从 AshareDataFetcher 的日级快照构建宏观 / A 股输入
    # ------------------------------------------------------------------
    def _build_macro_snapshot_from_daily(self, daily: Dict[str, Any]) -> GlobalMacroSnapshot:
        treasury = (daily.get("treasury") or {}) if daily else {}
        us_eq = (daily.get("us_equity") or {}) if daily else {}
        a50 = (daily.get("a50_night") or {}) if daily else {}
        eu = daily.get("eu_futures") if daily else None

        ycurve_bps = treasury.get("yield_curve_diff")

        nas = (us_eq.get("nasdaq") or {}).get("changePct")
        spy = (us_eq.get("spy") or {}).get("changePct")
        vix_price = (us_eq.get("vix") or {}).get("price")

        dax_pct: Optional[float] = eu
        ftse_pct: Optional[float] = None

        a50_ret = a50.get("ret") if isinstance(a50, dict) else None
        a50_pct = a50_ret * 100.0 if a50_ret is not None else None
        a50_src = a50.get("source") if isinstance(a50, dict) else None

        snap = GlobalMacroSnapshot(
            treasury_5y=None,
            treasury_10y=None,
            ycurve_bps=ycurve_bps,
            nasdaq_pct=nas,
            spy_pct=spy,
            vix_last=vix_price,
            dax_pct=dax_pct,
            ftse_pct=ftse_pct,
            a50_night_pct=a50_pct,
            a50_night_proxy=a50_src,
        )
        return snap

    def _build_ashare_daily_inputs_from_daily(self, daily: Dict[str, Any]):
        index = (daily.get("index") or {}) if daily else {}
        advdec = (daily.get("advdec") or {}) if daily else {}
        liq = (daily.get("liquidity") or {}) if daily else {}

        sh_change = index.get("sh_change")
        cyb_change = index.get("cyb_change")
        adv = advdec.get("advance")
        dec = advdec.get("decline")

        liq_wrapper = {
            "liquidity_risk": bool(liq.get("liquidity_risk", False)),
            "raw": liq,
        }

        inputs = AShareDailyInputs(
            sh_change_pct=sh_change,
            cyb_change_pct=cyb_change,
            adv_count=adv,
            dec_count=dec,
            liquidity=liq_wrapper,
        )

        raw = {
            "sh_change_pct": sh_change,
            "cyb_change_pct": cyb_change,
            "adv": adv,
            "dec": dec,
            "turnover_yi": daily.get("turnover") if daily else None,
            "liquidity": liq,
        }
        return inputs, raw

    # ------------------------------------------------------------------
    #  T-5 历史趋势预警
    # ------------------------------------------------------------------
    def _compute_t5_from_history(self, history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """根据最近 N 天历史快照计算 T-5 提前预警得分。"""
        if not history or len(history) < 2:
            return {
                "t5_score": None,
                "t5_level": "数据不足",
                "t5_desc": "历史样本不足，暂不生成 T-5 提前预警",
                "t5_days": len(history),
            }

        sh_list: List[float] = []
        adv_ratios: List[float] = []
        liq_flags: List[bool] = []

        for h in history:
            snap = h.get("snapshot", {})
            ashares = snap.get("ashares", {})
            sh = ashares.get("sh_change_pct")
            adv = ashares.get("adv")
            dec = ashares.get("dec")
            liq = ashares.get("liquidity", {})

            if isinstance(sh, (int, float)):
                sh_list.append(float(sh))

            if isinstance(adv, (int, float)) and isinstance(dec, (int, float)) and (adv + dec) > 0:
                adv_ratios.append(adv / (adv + dec))

            liq_flag = bool(liq.get("liquidity_risk", False))
            liq_flags.append(liq_flag)

        sh_5d = sum(sh_list) if sh_list else 0.0
        avg_adv_ratio = sum(adv_ratios) / len(adv_ratios) if adv_ratios else None
        liq_bad_days = sum(1 for x in liq_flags if x)

        score = 50.0
        reasons: List[str] = []

        # 1) 上证 5 日累计
        if sh_5d < -3.0:
            score -= 10
            reasons.append(f"上证指数 5 日累计下跌 {sh_5d:.2f}%")
        elif sh_5d < -1.0:
            score -= 5
            reasons.append(f"上证指数 5 日累计小幅下跌 {sh_5d:.2f}%")
        elif sh_5d > 3.0:
            score += 8
            reasons.append(f"上证指数 5 日累计上涨 {sh_5d:.2f}%")
        elif sh_5d > 1.0:
            score += 4
            reasons.append(f"上证指数 5 日累计小幅上涨 {sh_5d:.2f}%")

        # 2) 涨跌家数走势
        if avg_adv_ratio is not None:
            if avg_adv_ratio < 0.45:
                score -= 8
                reasons.append(f"过去 {len(adv_ratios)} 日内，下跌家数占优（平均上涨占比 {avg_adv_ratio:.2f}）")
            elif avg_adv_ratio < 0.5:
                score -= 3
                reasons.append(f"过去 {len(adv_ratios)} 日内，略偏空（平均上涨占比 {avg_adv_ratio:.2f}）")
            elif avg_adv_ratio > 0.6:
                score += 5
                reasons.append(f"过去 {len(adv_ratios)} 日内，上涨家数明显占优（平均上涨占比 {avg_adv_ratio:.2f}）")
            elif avg_adv_ratio > 0.55:
                score += 2
                reasons.append(f"过去 {len(adv_ratios)} 日内，略偏多（平均上涨占比 {avg_adv_ratio:.2f}）")

        # 3) 流动性连续性
        if liq_bad_days >= 3:
            score -= 8
            reasons.append(f"过去 {len(liq_flags)} 日中有 {liq_bad_days} 日出现流动性风险")
        elif liq_bad_days >= 1:
            score -= 3
            reasons.append(f"过去 {len(liq_flags)} 日中有 {liq_bad_days} 日流动性偏弱")
        else:
            score += 2
            reasons.append("过去数日流动性整体正常")

        score = max(0.0, min(100.0, score))

        if score >= 70:
            level = "T-5 偏安全"
        elif score >= 55:
            level = "T-5 中性偏安全"
        elif score >= 40:
            level = "T-5 中性偏谨慎"
        elif score >= 25:
            level = "T-5 偏高风险"
        else:
            level = "T-5 高风险预警"

        desc = "；".join(reasons) if reasons else "近 5 日无显著趋势信号"
        return {
            "t5_score": score,
            "t5_level": level,
            "t5_desc": desc,
            "t5_days": len(history),
            "t5_sh_5d": sh_5d,
            "t5_avg_adv_ratio": avg_adv_ratio,
            "t5_liq_bad_days": liq_bad_days,
        }

    def _update_history_and_t5(self, result: GlobalRiskResult, bj_time):
        """保存当日快照并计算 T-5 提前预警，写回 result.ashare_daily_detail。"""
        scores_for_save = {
            "ashare_daily_total": result.ashare_daily_total,
            "ashare_daily_level": result.ashare_daily_level,
        }
        save_daily_snapshot(bj_time, result.snapshot_raw, scores_for_save)

        history = load_recent_snapshots(max_days=5)
        t5 = self._compute_t5_from_history(history)
        result.ashare_daily_detail.update(t5)
        return t5

    # ------------------------------------------------------------------
    #  主流程
    # ------------------------------------------------------------------
    def run_daily(self) -> GlobalRiskResult:
        bj_time = now_bj()
        logger.info("[Version] %s", self.version)
        logger.info("Beijing time: %s", bj_time.strftime("%Y-%m-%d %H:%M:%S"))
    
        daily = get_daily_snapshot(bj_time)
    
        macro_snap = self._build_macro_snapshot_from_daily(daily)
        macro_score = score_global_macro(macro_snap)
    
        as_inputs, raw_inputs = self._build_ashare_daily_inputs_from_daily(daily)
    
        core_etf_5d_change = -0.36
        sh_4w_change = -0.52
        as_scores = compute_ashare_daily_score(as_inputs, core_etf_5d_change, sh_4w_change)
        
        # ============================
        # 原始 macro snapshot
        # ============================
        snapshot_raw = {
            "macro": {
                "treasury_5y": macro_snap.treasury_5y,
                "treasury_10y": macro_snap.treasury_10y,
                "ycurve_bps": macro_snap.ycurve_bps,
                "nasdaq_pct": macro_snap.nasdaq_pct,
                "spy_pct": macro_snap.spy_pct,
                "vix_last": macro_snap.vix_last,
                "dax_pct": macro_snap.dax_pct,
                "ftse_pct": macro_snap.ftse_pct,
                "a50_night_pct": macro_snap.a50_night_pct,
                "a50_night_proxy": macro_snap.a50_night_proxy,
            },
            "ashares": raw_inputs,
        }
    
        # ============================
        # ★★★ 在这里加入大宗商品因子 ★★★
        # ============================
        commodities = daily.get("commodities", {})        #  ← snapshot 中的黄金/原油/铜/美元指数
        snapshot_raw["macro"].update(commodities)         #  ← merge 到 macro
    
        # ============================
        # 构造最终 result
        # ============================
        result = GlobalRiskResult(
            version=self.version,
            macro_score=macro_score.total_score,
            macro_level=macro_score.risk_level,
            macro_desc=macro_score.description,
            ashare_daily_total=as_scores.total_score,
            ashare_daily_level=as_scores.risk_level,
            ashare_daily_detail={
                "emotion_score": as_scores.emotion_score,
                "emotion_desc": as_scores.emotion_desc,
                "short_term_score": as_scores.short_term_score,
                "short_term_desc": as_scores.short_term_desc,
                "mid_term_score": as_scores.mid_term_score,
                "mid_term_desc": as_scores.mid_term_desc,
            },
            snapshot_raw=snapshot_raw,
        )
    
        # T-5: 更新历史
        self._update_history_and_t5(result, bj_time)
    
        # 生成文本报告
        self._write_report_text(result, bj_time)
        return result

    def _write_report_text(self, result: GlobalRiskResult, bj_time):
        date_str = bj_time.strftime("%Y%m%d-%H%M%S")
        report_path = REPORT_DIR / f"GlobalRiskReport-ashare_daily-{date_str}.txt"
        with report_path.open("w", encoding="utf-8") as f:
            f.write(f"[A股日级] 综合得分 {result.ashare_daily_total:.1f}/100（{result.ashare_daily_level}）\n")
            f.write(
                f"- 情绪：市场情绪得分 {result.ashare_daily_detail['emotion_score']:.1f} / 20，"
                f"{result.ashare_daily_detail['emotion_desc']}。\n"
            )
            f.write(
                f"- 短期：{result.ashare_daily_detail['short_term_desc']}，"
                f"得分 {result.ashare_daily_detail['short_term_score']:.1f}/20。\n"
            )
            f.write(
                f"- 中期：{result.ashare_daily_detail['mid_term_desc']}，"
                f"得分 {result.ashare_daily_detail['mid_term_score']:.1f}/20。\n"
            )

            # T-5 段落（如果有）
            t5_score = result.ashare_daily_detail.get("t5_score")
            t5_level = result.ashare_daily_detail.get("t5_level")
            t5_desc = result.ashare_daily_detail.get("t5_desc")
            t5_days = result.ashare_daily_detail.get("t5_days")

            if t5_level is not None:
                if t5_score is None:
                    f.write(
                        f"- T-5 提前预警：{t5_level}（{t5_desc}，样本天数={t5_days}）。\n"
                    )
                else:
                    f.write(
                        f"- T-5 提前预警：得分 {float(t5_score):.1f}/100（{t5_level}），"
                        f"窗口 {t5_days} 天，{t5_desc}。\n"
                    )

        logger.info("A-share daily report written to %s", report_path)
