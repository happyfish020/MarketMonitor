from typing import Dict, Any
import json
from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class GlobalMacroFactor(FactorBase):
    """
    V12 GlobalMacroFactor（瘦因子版）

    输入：
      input_block["global_macro"] = {
          bond10, bond05, dxy, nas
      }
    """

    def __init__(self):
        super().__init__("global_macro_raw")

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
        data = self.pick(input_block, "global_macro_raw", {})
        if not data:
            return FactorResult(
                name = self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "global_macro_raw data missing",
                } 
            )


        bond10 = self._safe_pct(data.get("bond10", {}))
        bond05 = self._safe_pct(data.get("bond05", {}))
        dxy    = self._safe_pct(data.get("dxy", {}))
        nas    = self._safe_pct(data.get("nas", {}))

        signal = (
            -0.40 * bond10
            -0.20 * bond05
            -0.20 * dxy
            +0.20 * nas
        )

        score = 50.0 + (signal / 3.0) * 50.0

        return self.build_result(
            score=score,
            details={
                "bond10_pct": bond10,
                "bond05_pct": bond05,
                "dxy_pct": dxy,
                "nas_pct": nas,
                "macro_signal": signal,
                "data_status": "OK",
                "_raw_data": json.dumps(data)[:160] + "...",
            },
        )
