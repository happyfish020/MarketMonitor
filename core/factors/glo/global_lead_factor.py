from typing import Dict, Any
import json
from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class GlobalLeadFactor(FactorBase):
    """
    V12 GlobalLeadFactor（瘦因子版）

    输入：
      input_block["global_lead"] = {
          a50, es, nq, hsi, btc : { pct / pct_chg / last, prev_close ... }
      }
    """

    def __init__(self):
        super().__init__("global_lead")

    @staticmethod
    def _safe_pct(info: Dict[str, Any]) -> float:
        if not isinstance(info, dict):
            return 0.0
        for k in ("pct", "pct_chg", "change_pct", "chg"):
            if isinstance(info.get(k), (int, float)):
                return float(info[k])
        last = info.get("last") or info.get("close")
        prev = info.get("prev_close") or info.get("pre_close")
        try:
            if last is not None and prev:
                return (float(last) / float(prev) - 1.0) * 100.0
        except Exception:
            pass
        return 0.0

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "global_lead_raw", {})
        if not data:
            return FactorResult(
                name = self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "global_lead_raw data missing",
                } 
            )

        a50 = self._safe_pct(data.get("a50", {}))
        es  = self._safe_pct(data.get("es", {}))
        nq  = self._safe_pct(data.get("nq", {}))
        hsi = self._safe_pct(data.get("hsi", {}))
        btc = self._safe_pct(data.get("btc", {}))

        signal = (
            0.30 * a50
            + 0.25 * es
            + 0.25 * nq
            + 0.10 * hsi
            + 0.10 * btc
        )

        score = 50.0 + (signal / 3.0) * 50.0

        return self.build_result(
            score=score,
            details={
                "a50_pct": a50,
                "es_pct": es,
                "nq_pct": nq,
                "hsi_pct": hsi,
                "btc_pct": btc,
                "lead_signal": signal,
                "data_status": "OK",
                "_raw_data": json.dumps(data)[:160] + "...",
            },
        )
