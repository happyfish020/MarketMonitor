# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, Any, List
from datetime import datetime
import json
import os


class ReconciliationEngine:
    """
    UnifiedRisk · ReconciliationEngine（真实对账表程序化版）

    目标：
    - 把“人工真实对账表”完全程序化
    - 事实 vs 系统 并排呈现
    - 只提示偏差，不下结论
    """

    def __init__(self, *, output_dir: str = "runs/recon"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    # ==================================================
    # Public API
    # ==================================================
    def run(
        self,
        *,
        trade_date: str,
        snapshot: Dict[str, Any],
        factors: Dict[str, Any],
        structure: Dict[str, Any],
        gate_level: str,
    ) -> Dict[str, Any]:

         

        truth = self._extract_truth(snapshot)
        factor_view = self._extract_factors(factors)
        system = self._extract_system(structure, gate_level)
        
        table = self._build_table(
            truth=truth,
            factors=factor_view,
            system=system,
        )
        
        diff = self._diff(table)

        result = {
            "trade_date": trade_date,
            "table": table,
            "diff": diff,
            "meta": {
                "generated_at": datetime.now().isoformat(),
            },
        }

        self._write_outputs(trade_date, result)
        return result


    def _build_table(
        self,
        truth: Dict[str, Any],
        factors: Dict[str, Any],
        system: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
    
        def _fmt(v):
            if v is None:
                return "DATA_MISSING"
            if isinstance(v, dict):
                s = v.get("state")
                sc = v.get("score")
                return f"{s} / {sc}"
            return v
    
        rows: List[Dict[str, Any]] = []
    
        rows.append({
            "dimension": "北向资金",
            "raw": _fmt(truth.get("north")),
            "factor": _fmt(factors.get("north_nps")),
            "structure": system["structure"].get("north_nps", {}).get("state"),
            "gate": system["gate"],
        })
    
        rows.append({
            "dimension": "成交额",
            "raw": _fmt(truth.get("turnover")),
            "factor": _fmt(factors.get("turnover")),
            "structure": system["structure"].get("turnover", {}).get("state"),
            "gate": system["gate"],
        })
    
        rows.append({
            "dimension": "市场广度",
            "raw": _fmt(truth.get("breadth")),
            "factor": _fmt(factors.get("breadth")),
            "structure": system["structure"].get("breadth", {}).get("state"),
            "gate": system["gate"],
        })
    
        rows.append({
            "dimension": "指数技术",
            "raw": _fmt(truth.get("index_tech")),
            "factor": _fmt(factors.get("index_tech")),
            "structure": system["structure"].get("index_tech", {}).get("state"),
            "gate": system["gate"],
        })
    
        rows.append({
            "dimension": "趋势结构",
            "raw": _fmt(truth.get("trend")),
            "factor": _fmt(factors.get("trend_in_force")),
            "structure": system["structure"].get("trend_in_force", {}).get("state"),
            "gate": system["gate"],
        })
    
        return rows
    
    def _latest(self, series: Any) -> Dict[str, Any] | None:
        if isinstance(series, list) and series:
            last = series[-1]
            if isinstance(last, dict):
                return last
        return None
    

    def _extract_factors(self, factors: Dict[str, Any]) -> Dict[str, Any]:
        """
        Factor 层事实（直接来自 FactorResult）
        """
        result = {}
    
        for name, fr in factors.items():
            result[name] = {
                "score": fr.score,
                "level": fr.level,
                "details": fr.details,
            }
    
        return result
    
    # ==================================================
    # Truth side（真实行情事实）
    # ==================================================
    def _extract_truth(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        def _pick_raw(key: str):
            v = snapshot.get(key)
            if isinstance(v, dict):
                return {
                    "state": v.get("state"),
                    "score": v.get("score"),
                    "data_status": v.get("data_status"),
                }
            return None
    
        return {
            "north": _pick_raw("north_nps_raw"),
            "turnover": _pick_raw("turnover_raw"),
            "breadth": _pick_raw("breadth_raw"),
            "index_tech": snapshot.get("index_tech"),
            "trend": snapshot.get("trend_in_force"),
        }
    
    # ==================================================
    # System side（系统判断）
    # ==================================================
    def _extract_system(
        self,
        structure: Dict[str, Any],
        gate_level: str,
    ) -> Dict[str, Any]:

        system: Dict[str, Any] = {
            "gate": gate_level,
            "structure": {},
        }

        for key in [
            "trend_in_force",
            "turnover",
            "north_nps",
            "breadth",
            "index_tech",
        ]:
            if key in structure:
                system["structure"][key] = {
                    "state": structure[key].get("state"),
                    "modifier": structure[key].get("modifier"),
                }

        return system

    # ==================================================
    # 对账表（核心）
    # ==================================================
    def _extract_truth(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        Truth = raw inputs（事实输入），而非价格行情
        """
        truth: Dict[str, Any] = {}
    
        def _pick_raw(key: str):
            v = snapshot.get(key)
            if isinstance(v, dict):
                return {
                    "state": v.get("state"),
                    "score": v.get("score"),
                    "data_status": v.get("data_status"),
                }
            return None
    
        truth["north"] = _pick_raw("north_nps_raw")
        truth["turnover"] = _pick_raw("turnover_raw")
        truth["breadth"] = _pick_raw("breadth_raw")
        truth["index_tech"] = snapshot.get("index_tech")
        truth["trend"] = snapshot.get("trend_in_force")
    
        return truth
    
    # ==================================================
    # 差异提示（不裁决）
    # ==================================================
    def _diff(self, table: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
    对账差异分析（只做解释性校验，不做裁决）

    校验对象：
    - raw → factor 是否一致
    - factor → structure 是否被放大/压缩
    - structure → gate 是否被治理性收紧
    """
    
        notes: List[str] = []
    
        for row in table:
            dim = row.get("dimension")
            raw = row.get("raw")
            factor = row.get("factor")
            structure = row.get("structure")
            gate = row.get("gate")
    
            # ---------- 数据缺失 ----------
            if raw == "DATA_MISSING":
                notes.append(
                    f"{dim}：raw 输入缺失，无法完成链路对账。"
                )
                continue
    
            # ---------- raw → factor ----------
            if isinstance(raw, str) and isinstance(factor, str):
                if raw.split(" / ")[0] != factor.split(" / ")[0]:
                    notes.append(
                        f"{dim}：raw 状态为 {raw}，"
                        f"但 factor 解释为 {factor}，"
                        f"请检查 factor 计算或阈值。"
                    )
    
            # ---------- factor → structure ----------
            if isinstance(factor, str) and structure:
                factor_state = factor.split(" / ")[0]
                if factor_state != structure:
                    notes.append(
                        f"{dim}：factor 状态为 {factor_state}，"
                        f"但 structure 判定为 {structure}，"
                        f"存在解释差异。"
                    )
    
            # ---------- structure → gate（只解释，不否定） ----------
            if structure and gate == "CAUTION":
                notes.append(
                    f"{dim}：结构状态为 {structure}，"
                    f"但 Gate 为 CAUTION（治理性收紧），"
                    f"属于制度约束而非结构错误。"
                )
    
        return {"notes": notes}
    
    # ==================================================
    # Output
    # ==================================================
    def _write_outputs(self, trade_date: str, result: Dict[str, Any]) -> None:
        json_path = os.path.join(self.output_dir, f"recon_{trade_date}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        md_path = os.path.join(self.output_dir, f"recon_{trade_date}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(self._render_md(result))

    def _render_md(self, result: Dict[str, Any]) -> str:
        """
        渲染对账 Markdown（新四层契约）
        """
    
        lines: List[str] = []
    
        lines.append(f"# 真实对账表 · {result['trade_date']}")
        lines.append("")
        lines.append("| 维度 | Raw 输入 | Factor 结果 | Structure | Gate |")
        lines.append("|---|---|---|---|---|")
    
        for row in result.get("table", []):
            lines.append(
                f"| {row.get('dimension')} "
                f"| {row.get('raw', '')} "
                f"| {row.get('factor', '')} "
                f"| {row.get('structure', '')} "
                f"| {row.get('gate', '')} |"
            )
    
        lines.append("")
    
        notes = result.get("diff", {}).get("notes", [])
        if notes:
            lines.append("## 差异提示")
            for n in notes:
                lines.append(f"- {n}")
        else:
            lines.append("## 差异提示")
            lines.append("- 未发现显著链路差异")
    
        return "\n".join(lines)
    