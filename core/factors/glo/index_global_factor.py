from typing import Dict, Any
import json

from core.factors.factor_base import FactorBase
from core.factors.factor_result import FactorResult


class IndexGlobalFactor(FactorBase):
    """
    V12 IndexGlobalFactor（瘦因子版）

    输入：
      input_block["index_global"] = {
          spx, vix, dxy
      }
    """

    def __init__(self):
        super().__init__("index_global")

    def compute(self, input_block: Dict[str, Any]) -> FactorResult:
        data = self.pick(input_block, "index_global_raw", {})
        if not data:
            return FactorResult(
                name = self.name,
                score=50.0,
                level="NEUTRAL",
                details={
                    "data_status": "DATA_NOT_CONNECTED",
                    "reason": "index_global_raw data missing",
                } 
            )    
        # 统一 key 为小写
        ig = {str(k).lower(): v for k, v in data.items()} if isinstance(data, dict) else {}

        def pct(info):
            if not isinstance(info, dict):
                return 0.0
            for k in ("pct", "pct_change", "chg"):
                if isinstance(info.get(k), (int, float)):
                    return float(info[k])
            return 0.0

        spx = pct(ig.get("spx", {}))
        vix = pct(ig.get("vix", {}))
        dxy = pct(ig.get("dxy", {}))

        # 规则仍然在因子内：这是“事实到 score 的映射”，不是权重
        score_spx = 50.0 + spx * 10.0
        score_vix = 50.0 - vix * 5.0
        score_dxy = 50.0 - dxy * 5.0

        score = (score_spx + score_vix + score_dxy) / 3.0

        return self.build_result(
            score=score,
            details={
                "spx_pct": spx,
                "vix_pct": vix,
                "dxy_pct": dxy,
                "components": {
                    "spx_score": score_spx,
                    "vix_score": score_vix,
                    "dxy_score": score_dxy,
                },
                "data_status": "OK",
                "_raw_data": json.dumps(data)[:160] + "...",
            },
        )
