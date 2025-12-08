# core/factors/cn/index_tech_factor.py

from typing import Dict, Any

from core.factors.base import BaseFactor
from core.models.factor_result import FactorResult
from core.utils.logger import get_logger

LOG = get_logger("Factor.IndexTech")


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


class IndexTechFactor(BaseFactor):
    """
    指数技术结构因子（简版）：
      - 使用 snapshot["index_core"] 中：
          hs300, zz500, kc50 等的当日涨跌 pct
      - 作为技术结构的 proxy（未来可接 K 线/均线等）
    """

    def compute(self, snapshot: Dict[str, Any]) -> FactorResult:
        idx = snapshot.get("index_core", {}) or {}

        def _get_pct(name):
            info = idx.get(name) or {}
            return _safe_float(info.get("pct"))

        hs300_pct = _get_pct("hs300")
        zz500_pct = _get_pct("zz500")
        kc50_pct = _get_pct("kc50")

        LOG.info(
            "Compute IndexTech: hs300=%.2f%% zz500=%.2f%% kc50=%.2f%%",
            hs300_pct, zz500_pct, kc50_pct
        )

        # 简单平均
        vals = [v for v in [hs300_pct, zz500_pct, kc50_pct] if v is not None]
        if not vals:
            avg = 0.0
        else:
            avg = sum(vals) / len(vals)

        # 将平均涨跌幅映射到 0-100：-3% -> 20，0 -> 50，+3% -> 80
        score = 50 + avg * 10  # 1% ≈ 10 分
        score = max(0.0, min(100.0, score))

        if score >= 70:
            desc = "核心指数技术结构偏强，多头占优"
        elif score <= 30:
            desc = "核心指数技术结构偏弱，短期承压"
        else:
            desc = "核心指数技术结构中性或震荡"

        detail_lines = [
            f"沪深300：{hs300_pct:.2f}%",
            f"中证500：{zz500_pct:.2f}%",
            f"科创50：{kc50_pct:.2f}%",
            f"平均涨跌幅：{avg:.2f}%",
        ]
        detail = "\n".join(detail_lines)

        LOG.info("IndexTechFactor: score=%.2f desc=%s", score, desc)

        return FactorResult(score=score, desc=desc, detail=detail)
