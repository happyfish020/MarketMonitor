# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, Any, List
from datetime import datetime
import os
import json
import glob
from collections import defaultdict


class RollingBiasSummary:
    """
    UnifiedRisk · Rolling Bias Summary

    职责：
    - 汇总最近 N 天的 recon 结果
    - 统计系统性偏差（不是单日噪音）
    - 为阈值 / 因子 / 连续性规则提供证据
    """

    def __init__(
        self,
        *,
        recon_dir: str = "runs/recon",
        window: int = 20,
    ):
        self.recon_dir = recon_dir
        self.window = window

    # ==================================================
    # Public API
    # ==================================================
    def run(self) -> Dict[str, Any]:
        files = self._load_recent_files()
        records = [self._load_file(fp) for fp in files]

        stats = self._aggregate(records)
        result = {
            "window": self.window,
            "asof": datetime.now().strftime("%Y-%m-%d"),
            "file_count": len(records),
            "stats": stats,
        }

        self._write_outputs(result)
        return result

    # ==================================================
    # Load
    # ==================================================
    def _load_recent_files(self) -> List[str]:
        pattern = os.path.join(self.recon_dir, "recon_*.json")
        files = sorted(glob.glob(pattern))
        return files[-self.window :]

    def _load_file(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ==================================================
    # Aggregate core
    # ==================================================
    def _aggregate(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        核心统计逻辑：
        - 统计“系统判断 vs 真实行情”的偏差频率
        """

        counters = defaultdict(lambda: defaultdict(int))
        totals = defaultdict(int)

        for rec in records:
            table = rec.get("table", [])
            for row in table:
                dim = row.get("dimension")
                truth = row.get("truth")
                system = row.get("system")

                if dim is None:
                    continue

                totals[dim] += 1

                # ---------- 北向 ----------
                if dim == "北向资金" and isinstance(truth, (int, float)):
                    if truth > 0 and system == "neutral":
                        counters["north_nps"]["neutral_when_inflow"] += 1
                    if truth < 0 and system == "neutral":
                        counters["north_nps"]["neutral_when_outflow"] += 1

                # ---------- 成交 ----------
                if dim == "成交额" and isinstance(truth, (int, float)):
                    if truth > 1.1 and system != "expanding":
                        counters["turnover"]["miss_expanding"] += 1
                    if truth < 0.9 and system == "expanding":
                        counters["turnover"]["false_expanding"] += 1

                # ---------- 广度 ----------
                if dim == "市场广度" and isinstance(truth, (int, float)):
                    if truth > 0.6 and system != "healthy":
                        counters["breadth"]["miss_healthy"] += 1
                    if truth < 0.4 and system == "healthy":
                        counters["breadth"]["false_healthy"] += 1

                # ---------- Gate ----------
                if dim == "制度 Gate":
                    if system == "CAUTION":
                        counters["gate"]["caution_days"] += 1

        # ---------- 汇总成可读统计 ----------
        summary = {}

        for k, sub in counters.items():
            summary[k] = {}
            for metric, cnt in sub.items():
                summary[k][metric] = {
                    "count": cnt,
                    "ratio": round(cnt / max(1, len(records)), 3),
                }

        return summary

    # ==================================================
    # Output
    # ==================================================
    def _write_outputs(self, result: Dict[str, Any]) -> None:
        out_json = os.path.join(
            self.recon_dir, f"bias_summary_{self.window}d.json"
        )
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        out_md = os.path.join(
            self.recon_dir, f"bias_summary_{self.window}d.md"
        )
        with open(out_md, "w", encoding="utf-8") as f:
            f.write(self._render_md(result))

    def _render_md(self, result: Dict[str, Any]) -> str:
        lines = [
            f"# Rolling Bias Summary · {result['window']}D",
            "",
            f"- 样本数：{result['file_count']}",
            "",
        ]

        stats = result.get("stats", {})
        if not stats:
            lines.append("无可用统计结果。")
            return "\n".join(lines)

        for dim, metrics in stats.items():
            lines.append(f"## {dim}")
            for name, v in metrics.items():
                lines.append(
                    f"- {name}: {v['count']} 次 "
                    f"({int(v['ratio'] * 100)}%)"
                )
            lines.append("")

        return "\n".join(lines)
