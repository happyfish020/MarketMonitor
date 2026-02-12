# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from core.reporters.report_types import ReportBlock
from core.reporters.report_context import ReportContext
from core.reporters.report_blocks.report_block_base import ReportBlockRendererBase


class SectorProxyBlock(ReportBlockRendererBase):
    """
    UnifiedRisk V12 · Sector Proxy Block（UAT-P0 · Validation · MVP）

    目标：
    - 展示 SectorProxyFactor 的关键 evidence，并给出“只读解释”（更友好）
    - 不参与 Gate/Execution/DRS 的任何计算（只解释，不决策）
    - 输入缺失/格式异常不崩溃：warnings + 占位

    读取优先级（best-effort）：
    1) slots["factors"]["sector_proxy"]
    2) slots["phase2"]["factors"]["sector_proxy"]
    3) slots["sector_proxy"]
    """

    def __init__(self, block_alias: str = "sector.proxy", title: str = "板块代理验证（Sector Proxy · Validation）") -> None:
        self.block_alias = "sector.proxy"
        self.title = "板块代理验证（Sector Proxy · Validation）"
        #super().__init__(block_alias=block_alias, title=title)

    def render(self, context: ReportContext, doc_partial: Dict[str, Any]) -> ReportBlock:
        warnings: List[str] = []

        fr = self._pick_factor(context)

        if fr is None:
            warnings.append("missing:sector_proxy")
            return ReportBlock(
                block_alias=self.block_alias,
                title=self.title,
                payload="未接入 sector_proxy 因子结果（仅影响解释区块，不影响制度裁决）。",
                warnings=warnings,
            )

        score = self._get_num(self._get_field(fr, "score"))
        level = self._get_field(fr, "level")
        details = self._get_field(fr, "details")
        if not isinstance(details, dict):
            details = {}

        evidence = details.get("evidence") if isinstance(details.get("evidence"), dict) else {}

        # risk_score should be available for header rendering (avoid UnboundLocalError)

        r_score = self._get_num((evidence or {}).get("risk_score"))

        lines: List[str] = []
        lines.append("（只读解释）Sector Proxy 用一组板块/主题代理，衡量“指数走势是否被板块参与所确认”。")
        lines.append("注意：该区块仅用于结构验证展示，不构成进攻/调仓依据；最终以 Gate / Execution / DRS 为准。")
        lines.append("⚠️ 重要：Validation（如 STRONG）仅表示结构未被证伪，不等于“允许进攻/加仓”。进攻许可请以 Attack Window + Gate 为准。")

        
        v_level = (evidence or {}).get("validation_level")
        # 表述层优化：显性化“STRONG ≠ 进攻许可”
        v_level_disp = v_level
        if isinstance(v_level, str):
            if v_level.upper() == "STRONG":
                v_level_disp = "STRONG（结构有效，但不构成进攻许可）"
            elif v_level.upper() in ("MED", "MEDIUM"):
                v_level_disp = "MED（结构尚可，不构成进攻许可）"
            elif v_level.upper() in ("WEAK", "SOFT"):
                v_level_disp = "WEAK（结构偏弱）"
                r_level = (evidence or {}).get("risk_level")
                if v_level or r_level:
                    lines.append(
                        f"(Factor) validation_level: {v_level_disp or 'NA'}    validation_score: {score if score is not None else 'NA'}"
                        f"    risk_level: {r_level or (level if isinstance(level, str) else 'NA')}    risk_score: {r_score if r_score is not None else 'NA'}"
                    )
                elif isinstance(level, str) and level:
                    lines.append(f"(Factor) level: {level}    score: {score if score is not None else 'NA'}")
                else:
                    lines.append(f"(Factor) score: {score if score is not None else 'NA'}")
        
        
                v_score = self._get_num(evidence.get("validation_score"))
                r_score = self._get_num(evidence.get("risk_score"))
                leaders_ratio = self._get_num(evidence.get("leaders_ratio_10d"))
                sector_cnt = self._get_num(evidence.get("sector_count"))
                avg_rs10 = self._get_num(evidence.get("avg_rs_10d"))
                stdev_rs10 = self._get_num(evidence.get("stdev_rs_10d"))
                span_rs10 = self._get_num(evidence.get("span_rs_10d"))
                dd10_bad = evidence.get("dd10_bad")
        
                parts: List[str] = []
                if v_score is not None:
                    parts.append(f"validation_score={v_score:.2f}")
                if r_score is not None:
                    parts.append(f"risk_score={r_score:.2f}")
                if leaders_ratio is not None:
                    parts.append(f"leaders_ratio_10d={leaders_ratio:.4f}")
                if sector_cnt is not None:
                    parts.append(f"sector_count={int(sector_cnt)}")
                if avg_rs10 is not None:
                    parts.append(f"avg_rs10={avg_rs10:.4f}")
                if stdev_rs10 is not None:
                    parts.append(f"stdev_rs10={stdev_rs10:.4f}")
                if span_rs10 is not None:
                    parts.append(f"span_rs10={span_rs10:.4f}")
                if isinstance(dd10_bad, (int, float)):
                    parts.append(f"dd10_bad={int(dd10_bad)}")
                if parts:
                    lines.append("关键证据：" + " ; ".join(parts))
        
                bench = details.get("benchmark")
                if isinstance(bench, dict):
                    bsym = bench.get("symbol")
                    b10 = self._get_num(bench.get("ret_10d"))
                    b20 = self._get_num(bench.get("ret_20d"))
                    bps: List[str] = []
                    if isinstance(bsym, str) and bsym:
                        bps.append(f"symbol={bsym}")
                    if b10 is not None:
                        bps.append(f"ret_10d={b10:.4%}")
                    if b20 is not None:
                        bps.append(f"ret_20d={b20:.4%}")
                    if bps:
                        lines.append("基准：" + " ; ".join(bps))
        
                sectors = details.get("sectors")
                rows: List[str] = []
                rs_pairs: List[Tuple[str, float]] = []
                if isinstance(sectors, dict) and sectors:
                    rows, rs_pairs = self._build_rows(sectors)
                    if rows:
                        lines.append("")
                        lines.append("板块相对强弱（RS_10d vs 基准，降序）：")
                        show = rows[:8]
                        for r in show:
                            lines.append(f"- {r}")
                        if len(rows) > len(show):
                            lines.append(f"(其余 {len(rows)-len(show)} 个略)")
                else:
                    warnings.append("missing:sector_proxy_sectors")
        
                reasons = details.get("reasons")
                if isinstance(reasons, list) and reasons:
                    rr = [str(x) for x in reasons[:10] if str(x)]
                    if rr:
                        lines.append("")
                        lines.append("模型理由（截断）：")
                        for x in rr:
                            lines.append(f"- {x}")
        
                ds = details.get("data_status")
                if isinstance(ds, str) and ds:
                    lines.append("")
                    lines.append(f"data_status: {ds}")
        
                lines.extend(self._interpretation_lines(
                    validation_score=v_score,
                    risk_score=r_score,
                    leaders_ratio_10d=leaders_ratio,
                    span_rs10=span_rs10,
                    stdev_rs10=stdev_rs10,
                    dd10_bad=dd10_bad,
                    rs_pairs=rs_pairs,
                ))

        return ReportBlock(
            block_alias=self.block_alias,
            title=self.title,
            payload="\n".join(lines),
            warnings=warnings,
        )

    def _interpretation_lines(
        self,
        *,
        validation_score: Optional[float],
        risk_score: Optional[float],
        leaders_ratio_10d: Optional[float],
        span_rs10: Optional[float],
        stdev_rs10: Optional[float],
        dd10_bad: Any,
        rs_pairs: List[Tuple[str, float]],
    ) -> List[str]:
        lines: List[str] = []
        lines.append("")
        lines.append("解读（只读，不决策）：")

        overall = []
        if validation_score is not None:
            if validation_score >= 80:
                overall.append("确认度较高（板块对指数走势的“跟随/确认”更充分）")
            elif validation_score >= 60:
                overall.append("确认度中等（部分板块确认，仍存在拖后/缺位）")
            else:
                overall.append("确认度偏弱（指数走势更可能“靠少数方向支撑”）")

        if risk_score is not None:
            if risk_score <= 25:
                overall.append("风险压力偏低（结构警报不高）")
            elif risk_score <= 40:
                overall.append("风险压力中等（需关注分化/回撤项）")
            else:
                overall.append("风险压力偏高（结构分化/回撤提示更强）")

        if overall:
            lines.append("- 总体： " + "；".join(overall))

        portrait = []
        if span_rs10 is not None:
            if span_rs10 >= 0.06:
                portrait.append(f"分化偏大（span_rs10={span_rs10:.2%}）→ 更像结构性轮动而非全面行情")
            else:
                portrait.append(f"分化较可控（span_rs10={span_rs10:.2%}）→ 更接近广泛参与")
        if stdev_rs10 is not None:
            if stdev_rs10 >= 0.025:
                portrait.append(f"强弱离散偏高（stdev_rs10={stdev_rs10:.2%}）→ 个别方向“很强/很弱”更突出")
            else:
                portrait.append(f"强弱离散较低（stdev_rs10={stdev_rs10:.2%}）→ 板块同步性更好")
        if portrait:
            lines.append("- 盘面画像： " + "；".join(portrait))

        if leaders_ratio_10d is not None:
            if leaders_ratio_10d >= 0.75:
                lines.append(f"- 参与面：leaders_ratio_10d={leaders_ratio_10d:.2f}（多数代理为正，相对“有参与”）")
            elif leaders_ratio_10d >= 0.55:
                lines.append(f"- 参与面：leaders_ratio_10d={leaders_ratio_10d:.2f}（参与中等，仍有明显拖后）")
            else:
                lines.append(f"- 参与面：leaders_ratio_10d={leaders_ratio_10d:.2f}（参与不足，更容易出现“指数稳、个股弱”）")

        if isinstance(dd10_bad, (int, float)):
            if int(dd10_bad) > 0:
                lines.append(f"- 回撤提示：dd10_bad={int(dd10_bad)}（存在至少一个代理回撤偏差，常见于轮动/兑现盘）")
            else:
                lines.append("- 回撤提示：dd10_bad=0（代理组回撤项未出现明显异常）")

        if rs_pairs:
            rs_pairs_sorted = sorted(rs_pairs, key=lambda x: x[1], reverse=True)
            top = rs_pairs_sorted[:3]
            bottom = rs_pairs_sorted[-2:] if len(rs_pairs_sorted) >= 2 else rs_pairs_sorted[-1:]

            top_s = ", ".join([f"{k}(RS10 {v:+.2%})" for k, v in top])
            bottom_s = ", ".join([f"{k}(RS10 {v:+.2%})" for k, v in bottom])

            lines.append(f"- 确认者TOP：{top_s}")
            lines.append(f"- 拖后者：{bottom_s}")

        lines.append("- 使用方式：用于解释“这是全面行情还是结构性轮动/分化”，不作为单独的买卖信号。")

        return lines

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _pick_factor(self, context: ReportContext) -> Optional[Any]:
        factors = context.slots.get("factors")
        fr = self._get_dict_item(factors, "sector_proxy")
        if fr is not None:
            return fr

        ph2 = context.slots.get("phase2")
        if isinstance(ph2, dict):
            fr = self._get_dict_item(ph2.get("factors"), "sector_proxy")
            if fr is not None:
                return fr

        fr = self._get_dict_item(context.slots, "sector_proxy")
        if fr is not None:
            return fr

        return None

    @staticmethod
    def _get_dict_item(obj: Any, key: str) -> Optional[Any]:
        if isinstance(obj, dict):
            return obj.get(key)
        return None

    @staticmethod
    def _get_field(obj: Any, key: str) -> Optional[Any]:
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    @staticmethod
    def _get_num(v: Any) -> Optional[float]:
        if isinstance(v, (int, float)):
            return float(v)
        try:
            if isinstance(v, str) and v.strip():
                return float(v.strip())
        except Exception:
            return None
        return None

    def _build_rows(self, sectors: Dict[str, Any]) -> Tuple[List[str], List[Tuple[str, float]]]:
        items: List[Tuple[float, str]] = []
        rs_pairs: List[Tuple[str, float]] = []

        for k, v in sectors.items():
            if not isinstance(v, dict):
                continue
            sym = v.get("symbol")
            rs10 = self._get_num(v.get("rs_10d"))
            ret10 = self._get_num(v.get("ret_10d"))
            dd10 = self._get_num(v.get("dd10"))

            rs10_show = f"{rs10:+.4%}" if rs10 is not None else "NA"
            ret10_show = f"{ret10:+.4%}" if ret10 is not None else "NA"
            dd10_show = f"{dd10:+.4%}" if dd10 is not None else "NA"

            sym_show = sym if isinstance(sym, str) and sym else ""
            label = f"{k} {('('+sym_show+')') if sym_show else ''}  RS10={rs10_show}  ret10={ret10_show}  dd10={dd10_show}".strip()

            sort_key = rs10 if rs10 is not None else -999.0
            items.append((sort_key, label))
            if rs10 is not None:
                rs_pairs.append((k, rs10))

        items.sort(key=lambda x: x[0], reverse=True)
        return [x[1] for x in items], rs_pairs
