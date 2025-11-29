from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

import requests

from unified_risk.common.config_manager import CONFIG
from unified_risk.common.logger import get_logger
from unified_risk.core.cache.day_cache import DayCache

LOG = get_logger("UnifiedRisk.Factor.Turnover")


@dataclass
class TurnoverSnapshot:
    trade_date: date
    turnover_ratio: float      # 相对20日均成交额的偏离 (%)
    today_amt_e9: float        # 当日成交额（亿元）
    mean20_amt_e9: float       # 过去20日平均成交额（亿元）
    score: int                 # -2 ~ +2


class TurnoverFactor:
    """日级流动性 / 换手因子（v7.3，基于东财 push2his HS300 日K）。

    指数选用：沪深300（000300），secid=1.000300
    数据源：push2his 日K接口，字段中包含成交额（元）。
    计算逻辑：
      - 取截至 trade_date 的最近 21 个交易日：
          * 当日：today_amt
          * 前 20 日：计算均值 mean20_amt
      - turnover_ratio = (today_amt / mean20_amt - 1) * 100
      - 根据 ratio 区间映射到 -2 ~ +2。
    """

    SECID = "1.000300"

    def __init__(self) -> None:
        cache_root = CONFIG.get_path("cache_dir") / "turnover"
        self.cache = DayCache(cache_root)

    # --------- 内部：请求 EastMoney 日K ---------
    def _fetch_kline(self, d: date, limit: int = 60) -> Optional[List[str]]:
        """从东财 push2his 获取沪深300日K，返回 klines 列表。

        每个元素形如: 'YYYY-MM-DD,open,close,high,low,volume,amount,...'
        具体字段数量可能略有变化，我们只使用:
          - 日期 (index 0)
          - 成交额 amount = 最后一个或倒数第1/2个字段
        """
        end_str = d.strftime("%Y%m%d")
        url = (
            "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            f"?secid={self.SECID}"
            "&fields1=f1%2Cf2%2Cf3%2Cf4"
            "&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57"
            "&klt=101&fqt=1"
            f"&end={end_str}&lmt={limit}"
        )
        LOG.info(f"[Turnover] Fetch HS300 kline from EastMoney for end={end_str}")
        try:
            resp = requests.get(url, timeout=8)
            resp.raise_for_status()
            j = resp.json()
            data = j.get("data") or {}
            klines = data.get("klines") or []
            if not klines:
                LOG.warning("[Turnover] HS300 kline: empty klines.")
                return None
            return klines
        except Exception as e:
            LOG.error(f"[Turnover] HS300 kline fetch error: {e}")
            return None

    def _parse_amount_from_kline(self, rec: str) -> Optional[float]:
        parts = rec.split(",")
        if len(parts) < 6:
            return None
        # 通常最后一个字段为成交额或倒数第二个，取最后一个可解析数字
        for x in reversed(parts[1:]):
            try:
                v = float(x)
                if v >= 0:
                    return v
            except Exception:
                continue
        return None

    def _extract_turnover_for_date(self, d: date) -> Optional[TurnoverSnapshot]:
        klines = self._fetch_kline(d, limit=60)
        if not klines:
            return None

        # 过滤出日期 <= d 的记录
        d_str = d.strftime("%Y-%m-%d")
        valid: List[tuple[str, float]] = []
        for rec in klines:
            parts = rec.split(",")
            if not parts:
                continue
            ds = parts[0]  # 日期
            if ds > d_str:
                continue
            amt = self._parse_amount_from_kline(rec)
            if amt is None:
                continue
            valid.append((ds, amt))

        if len(valid) < 2:
            LOG.warning("[Turnover] 有效日K记录不足 2 天，使用中性 0。")
            return TurnoverSnapshot(
                trade_date=d,
                turnover_ratio=0.0,
                today_amt_e9=0.0,
                mean20_amt_e9=0.0,
                score=0,
            )

        # 按日期排序，取最后 21 条（含当日）
        valid.sort(key=lambda x: x[0])
        recent = valid[-21:]
        # 当日记录为最后一条
        today_ds, today_amt = recent[-1]

        # 过去20日
        past20 = [amt for (_ds, amt) in recent[:-1]][-20:]
        if not past20:
            LOG.warning("[Turnover] 历史成交额不足 20 日，使用中性 0。")
            mean20 = today_amt
        else:
            mean20 = sum(past20) / len(past20)

        if mean20 <= 0:
            LOG.warning("[Turnover] 20 日平均成交额异常，使用中性 0。")
            ratio = 0.0
        else:
            ratio = (today_amt / mean20 - 1.0) * 100.0

        score = self._map_ratio_to_score(ratio)

        return TurnoverSnapshot(
            trade_date=d,
            turnover_ratio=ratio,
            today_amt_e9=today_amt / 1e8,
            mean20_amt_e9=mean20 / 1e8,
            score=score,
        )

    @staticmethod
    def _map_ratio_to_score(ratio: float) -> int:
        """简单区间打分，你后续可以按经验调参。"""
        if ratio >= 40:
            return 2
        if ratio >= 15:
            return 1
        if ratio <= -40:
            return -2
        if ratio <= -15:
            return -1
        return 0

    # --------- 主入口 ---------
    def compute_for_date(self, d: date) -> TurnoverSnapshot:
        cached_ratio = self.cache.get(d, "TURNOVER_RATIO")
        cached_today = self.cache.get(d, "TURNOVER_AMT_E9")
        cached_mean20 = self.cache.get(d, "TURNOVER_MEAN20_E9")
        cached_score = self.cache.get(d, "TURNOVER_SCORE")
        if (
            cached_ratio is not None
            and cached_today is not None
            and cached_mean20 is not None
            and cached_score is not None
        ):
            return TurnoverSnapshot(
                trade_date=d,
                turnover_ratio=float(cached_ratio),
                today_amt_e9=float(cached_today),
                mean20_amt_e9=float(cached_mean20),
                score=int(cached_score),
            )

        snap = self._extract_turnover_for_date(d)
        if snap is None:
            LOG.warning("[Turnover] 无法从东财获取成交额数据，使用中性 0。")
            snap = TurnoverSnapshot(
                trade_date=d,
                turnover_ratio=0.0,
                today_amt_e9=0.0,
                mean20_amt_e9=0.0,
                score=0,
            )

        # 写缓存
        self.cache.set(d, "TURNOVER_RATIO", snap.turnover_ratio)
        self.cache.set(d, "TURNOVER_AMT_E9", snap.today_amt_e9)
        self.cache.set(d, "TURNOVER_MEAN20_E9", snap.mean20_amt_e9)
        self.cache.set(d, "TURNOVER_SCORE", snap.score)
        return snap

    def as_factor_dict(self, d: date) -> Dict[str, Any]:
        snap = self.compute_for_date(d)
        return {
            "turnover_score": snap.score,
            "turnover_ratio": snap.turnover_ratio,
            "turnover_today_amt_e9": snap.today_amt_e9,
            "turnover_mean20_amt_e9": snap.mean20_amt_e9,
        }
