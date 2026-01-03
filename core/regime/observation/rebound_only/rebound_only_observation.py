from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from core.utils.logger import get_logger

LOG = get_logger("Obs.ReboundOnly")


@dataclass(frozen=True)
class ReboundOnlyResult:
    flag: bool
    severity: str  # LOW / NEUTRAL / HIGH
    meaning: str
    drivers: Dict[str, Any]


class ReboundOnlyObservation:
    """
    UnifiedRisk V12 · Rebound-only Observation（冻结 MVP）

    目的：
    - 解决“反弹并不等于可买”的制度化解释（避免盘中追涨/被噪声牵引）
    - MVP 不依赖“是否真的反弹”的价格检测（先做结构守门）
      —— 只要趋势破坏/DRS=RED/执行分档偏高，就给出“反弹不可追”的守门提示
    """

    def build(
        self,
        *,
        trend_state: Optional[str],
        drs_signal: Optional[str],
        execution_band: Optional[str],
        asof: str,
    ) -> Dict[str, Any]:
        try:
            res = self._build_impl(
                trend_state=trend_state,
                drs_signal=drs_signal,
                execution_band=execution_band,
            )
            return {
                "meta": {"kind": "rebound_only", "asof": asof, "status": "ok"},
                "observation": {
                    "flag": res.flag,
                    "severity": res.severity,
                    "meaning": res.meaning,
                },
                "evidence": res.drivers,
            }
        except Exception as e:
            LOG.error("[ReboundOnly] build failed: %s", e, exc_info=True)
            return {
                "meta": {"kind": "rebound_only", "asof": asof, "status": "error"},
                "observation": {
                    "flag": False,
                    "severity": "LOW",
                    "meaning": "Rebound-only 构建失败（不影响主流程），请查看日志。",
                },
                "evidence": {"error": str(e)},
            }

    def _build_impl(
        self,
        *,
        trend_state: Optional[str],
        drs_signal: Optional[str],
        execution_band: Optional[str],
    ) -> ReboundOnlyResult:
        drivers = {
            "trend_state": trend_state,
            "drs_signal": drs_signal,
            "execution_band": execution_band,
        }

        # HIGH：趋势破坏或 DRS=RED 且执行分档偏高（D2/D3）
        if (trend_state == "broken" or drs_signal == "RED") and (execution_band in ("D2", "D3")):
            return ReboundOnlyResult(
                flag=True,
                severity="HIGH",
                meaning="反弹不可追：制度风险或趋势结构未修复，短期更适合防守执行（减仓/控敞口）。",
                drivers=drivers,
            )

        # MEDIUM：趋势破坏或 DRS=RED（即便执行分档不高，也提示谨慎）
        if trend_state == "broken" or drs_signal == "RED":
            return ReboundOnlyResult(
                flag=True,
                severity="NEUTRAL",
                meaning="反弹谨慎：趋势结构或制度风险尚未解除，反弹更可能是噪声而非趋势修复。",
                drivers=drivers,
            )

        # LOW：未触发
        return ReboundOnlyResult(
            flag=False,
            severity="LOW",
            meaning="未触发 Rebound-only 守门条件。",
            drivers=drivers,
        )
