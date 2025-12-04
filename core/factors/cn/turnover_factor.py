from __future__ import annotations

from typing import Dict, Any, Optional

from core.models.factor_result import FactorResult


class TurnoverFactor:
    """A股市场成交额因子（唯一数据源：zh_spot.amount 汇总）"""

    name = "turnover"

    def compute_from_daily(
        self,
        processed: Dict[str, Any],
        snapshot: Optional[Dict[str, Any]] = None,
    ) -> FactorResult:
        f = processed.get("features", {}) or {}

        # ---- 1. 从 snapshot.zh_spot 计算成交额（唯一来源） ----
        df = None
        if snapshot is not None:
            df = snapshot.get("zh_spot")

        sh = sz = total = 0.0

        if df is not None and hasattr(df, "empty") and (not df.empty) and ("amount" in df.columns):
            # 总成交额（亿元）
            total = float(df["amount"].sum()) / 1e9

            # 尝试按市场拆分：优先用 market 字段，其次用 code 规则
            sh_amt = sz_amt = 0.0
            try:
                if "market" in df.columns:
                    m = df["market"].astype(str).str.upper()
                    sh_mask = m.str.startswith("SH")
                    sz_mask = m.str.startswith("SZ")
                    sh_amt = float(df.loc[sh_mask, "amount"].sum()) / 1e9
                    sz_amt = float(df.loc[sz_mask, "amount"].sum()) / 1e9
                elif "code" in df.columns:
                    code_str = df["code"].astype(str)
                    sh_mask = code_str.str.startswith("6")  # 粗略规则：6打头当作沪市
                    sh_amt = float(df.loc[sh_mask, "amount"].sum()) / 1e9
                    sz_amt = total - sh_amt
                else:
                    sh_amt = sz_amt = 0.0
            except Exception:
                sh_amt = sz_amt = 0.0

            sh = sh_amt
            sz = sz_amt
        else:
            # 如果 zh_spot 异常，作为兜底：从 features 里拿已有的（一般为 0）
            sh = float(f.get("turnover_sh", 0.0) or 0.0)
            sz = float(f.get("turnover_sz", 0.0) or 0.0)
            total = float(f.get("turnover_total", 0.0) or 0.0)

        # 宽基 ETF 成交额（来自 etf_proxy）
        etf_turnover = float(f.get("turnover_etf", 0.0) or 0.0)

        if total <= 0:
            score = 50.0
            signal = "成交额数据缺失或异常（视为中性）"
            raw = {
                "turnover_sh": sh,
                "turnover_sz": sz,
                "turnover_total": total,
                "turnover_etf": etf_turnover,
                "etf_ratio": 0.0,
            }
            return FactorResult(name=self.name, score=score, signal=signal, raw=raw)

        # ---- 2. 成交额绝对强度打分 ----
        # 这里区间只是一个软参考，你后面可以按实际成交额分布调参数
        if total >= 4000:
            base = 1.0   # 极度放量
        elif total >= 2500:
            base = 0.7   # 明显放量
        elif total >= 1500:
            base = 0.4   # 正常偏上
        elif total >= 800:
            base = 0.1   # 正常偏弱
        elif total >= 400:
            base = -0.2  # 明显缩量
        else:
            base = -0.5  # 极度缩量

        # ---- 3. 宽基 ETF 吸金度 ----
        etf_ratio = etf_turnover / total if total > 0 else 0.0
        if etf_ratio >= 0.25:
            etf_score = 0.6
            etf_desc = "宽基ETF显著吸金"
        elif etf_ratio >= 0.15:
            etf_score = 0.3
            etf_desc = "宽基ETF温和吸金"
        elif etf_ratio >= 0.05:
            etf_score = 0.0
            etf_desc = "宽基ETF参与一般"
        else:
            etf_score = -0.2
            etf_desc = "宽基ETF参与度较低"

        # ---- 4. 综合原始分数 & 映射到 0–100 ----
        raw_combined = 0.8 * base + 0.2 * etf_score
        score = self._map_to_0_100(raw_combined)

        if score >= 70:
            desc = "成交活跃，流动性充足"
        elif score >= 55:
            desc = "成交正常偏暖"
        elif score >= 45:
            desc = "成交中性"
        elif score >= 30:
            desc = "成交偏冷，风险偏回避"
        else:
            desc = "成交极度低迷或异常，需谨慎"

        signal = (
            f"{desc}（total={total:.1f}亿, ETF={etf_turnover:.1f}亿, "
            f"etf_ratio={etf_ratio:.2f}，{etf_desc}）"
        )

        return FactorResult(
            name=self.name,
            score=score,
            signal=signal,
            raw={
                "turnover_sh": sh,
                "turnover_sz": sz,
                "turnover_total": total,
                "turnover_etf": etf_turnover,
                "etf_ratio": etf_ratio,
                "base_component": base,
                "etf_component": etf_score,
            },
        )

    @staticmethod
    def _map_to_0_100(raw: float) -> float:
        # 将 [-1, 1] 映射到 [0, 100]
        raw_clamped = max(-1.0, min(1.0, raw))
        return round(50.0 + raw_clamped * 50.0, 2)
