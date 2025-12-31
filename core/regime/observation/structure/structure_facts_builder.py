from __future__ import annotations

from typing import Dict, Optional, Any

from core.factors.factor_result import FactorResult

# ===============================
# Modifier 定义（命名规范合规）
# ===============================
MOD_NONE = None
MOD_SUCCESS_RATE_DECLINING = "success_rate_declining"
MOD_DISTRIBUTION_RISK = "distribution_risk"
MOD_HIGH_EXECUTION_RISK = "high_execution_risk"

class StructureFactsBuilder:
    """
    UnifiedRisk V12 · StructureFactsBuilder（state-only · v4）

    职责：
    - FactorResult → 结构事实（state）
    - 制度上下文（distribution / drs）→ modifier
    - state + modifier + evidence → 由报告层统一解释（meaning 仅在 Block）

    v2 变更（语义对齐 2025-12-29 盘面体感）：
    - 彻底弃用 north_nps 映射（避免“单日/未算趋势默认 neutral”误导）
    - 新增 north_proxy_pressure 映射（可进入 Gate & 报告结构解释）
    - turnover/breadth 默认文案去“偏乐观”措辞：扩大成交更偏“分歧/调仓/轮动”，
      breadth healthy 只表示“未坏”，并允许根据 adv_ratio 提示“涨少跌多”的体感。
    - build() 兼容 keys：turnover / turnover_raw，breadth / breadth_raw
    """

    # ===============================
    # Public API
    # ===============================
    def build(
        self,
        *,
        factors: Dict[str, FactorResult],
        distribution_risk_active: bool = False,
        drs_signal: Optional[str] = None,  # GREEN / YELLOW / RED
    ) -> Dict[str, Dict[str, Any]]:
        """
        参数：
        - factors: Phase-2 FactorResult
        - distribution_risk_active: 是否存在结构性分布风险
        - drs_signal: 日度风险信号（GREEN / YELLOW / RED）
        """

        modifier = self._resolve_modifier(
            distribution_risk_active=distribution_risk_active,
            drs_signal=drs_signal,
        )

        structure: Dict[str, Dict[str, str]] = {}

        # ---- index_tech ----
        fr = factors.get("index_tech")
        if fr:
            structure["index_tech"] = self._map_index_tech(fr, modifier)

        # ---- turnover (兼容 turnover_raw) ----
        fr = factors.get("turnover") or factors.get("turnover_raw")
        if fr:
            structure["turnover"] = self._map_turnover(fr, modifier)

        # ---- breadth (兼容 breadth_raw) ----
        fr = factors.get("breadth") or factors.get("breadth_raw")
        if fr:
            structure["breadth"] = self._map_breadth(fr, modifier)

        # ---- north proxy pressure (replaces north_nps) ----
        fr = factors.get("north_proxy_pressure")
        if fr:
            structure["north_proxy_pressure"] = self._map_north_proxy_pressure(fr, modifier)

        # ---- trend / frf ----
        fr = factors.get("trend_in_force")
        if fr:
            structure["trend_in_force"] = self._map_trend_in_force(fr, modifier)

        fr = factors.get("failure_rate")
        if fr:
            structure["failure_rate"] = self._map_failure_rate(fr)

        # ---- 结构总结（永远制度中性）----
        structure["_summary"] = self._build_summary(structure, modifier)

        return structure

    # ===============================
    # Modifier 解析（命名规范合规）
    # ===============================
    def _resolve_modifier(
        self,
        *,
        distribution_risk_active: bool,
        drs_signal: Optional[str],
    ) -> Optional[str]:
        """
        modifier 优先级（高 → 低）：
        1. distribution_risk
        2. drs_signal = RED
        3. drs_signal = YELLOW
        """
        if distribution_risk_active:
            return MOD_DISTRIBUTION_RISK

        if drs_signal == "RED":
            return MOD_HIGH_EXECUTION_RISK

        if drs_signal == "YELLOW":
            return MOD_SUCCESS_RATE_DECLINING

        return MOD_NONE

    # ===============================
    # Factor → Structure 映射
    # ===============================
    def _map_index_tech(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, Any]:
        """index_tech：指数技术状态（只输出 state，不输出 meaning）。"""
        ds = self._data_status(fr)
        if fr.level == "HIGH":
            return {"state": "strong", "modifier": modifier, "data_status": ds}
        if fr.level == "LOW":
            return {"state": "weak", "modifier": modifier, "data_status": ds}
        return {"state": "neutral", "modifier": modifier, "data_status": ds}
    def _map_turnover(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, Any]:
        """turnover：成交状态（只输出 state，不输出 meaning）。"""
        ds = self._data_status(fr)
        if fr.level == "HIGH":
            return {"state": "expanding", "modifier": modifier, "data_status": ds}
        if fr.level == "LOW":
            return {"state": "contracting", "modifier": modifier, "data_status": ds}
        return {"state": "neutral", "modifier": modifier, "data_status": ds}
    def _map_breadth(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, Any]:
        """breadth：只输出 state + modifier + evidence（不输出 meaning）。"""
        ds = self._data_status(fr)
        adv_ratio = self._extract_adv_ratio(fr)

        if fr.level == "HIGH":
            out: Dict[str, Any] = {"state": "healthy", "modifier": modifier, "data_status": ds}
        elif fr.level == "LOW":
            out = {"state": "not_broken", "modifier": modifier, "data_status": ds}
        else:
            out = {"state": "neutral", "modifier": modifier, "data_status": ds}

        if isinstance(adv_ratio, (int, float)):
            # canonical: nest evidence (V12 contract)
            ev = out.get("evidence")
            if not isinstance(ev, dict):
                ev = {}
                out["evidence"] = ev
            ev["adv_ratio"] = float(adv_ratio)
            # compatibility: keep top-level for legacy readers (will be removed after stabilization)
            out["adv_ratio"] = float(adv_ratio)

        return out
    def _map_north_proxy_pressure(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, Any]:
        """north_proxy_pressure：只输出 state + data_status + 可用证据字段（不输出 meaning）。"""
        ds = self._data_status(fr)
        det: Any = getattr(fr, "details", None) or {}
        out: Dict[str, Any] = {"modifier": modifier, "data_status": ds}

        lvl = (fr.level or "").upper()
        if lvl == "HIGH":
            out["state"] = "pressure_high"
        elif lvl == "LOW":
            out["state"] = "pressure_low"
        else:
            out["state"] = "pressure_medium"

        if isinstance(det, dict):
            ev: Dict[str, Any] = {}
            for k in ("pressure_level", "pressure_score", "quality_score", "reasons", "proxy_used"):
                if k in det:
                    v = det.get(k)
                    if k in ("pressure_score", "quality_score") and isinstance(v, (int, float)):
                        v = float(v)
                    ev[k] = v
                    # compatibility: keep top-level (legacy) until stabilized
                    out[k] = v
            if ev:
                out["evidence"] = ev

        return out
    def _map_trend_in_force(
        self,
        fr: FactorResult,
        modifier: Optional[str],
    ) -> Dict[str, Any]:
        ds = self._data_status(fr)
        if fr.level == "HIGH":
            return {"state": "in_force", "modifier": modifier, "data_status": ds}
        if fr.level == "LOW":
            return {"state": "broken", "modifier": modifier, "data_status": ds}
        return {"state": "weakening", "modifier": modifier, "data_status": ds}
    def _map_failure_rate(self, fr: FactorResult) -> Dict[str, Any]:
        ds = self._data_status(fr)
        if fr.level == "HIGH":
            return {"state": "elevated_risk", "data_status": ds}
        if fr.level == "NEUTRAL":
            return {"state": "watch", "data_status": ds}
        return {"state": "stable", "data_status": ds}

    @staticmethod
    def _data_status(fr: FactorResult) -> str:
        det: Any = getattr(fr, "details", None)
        if isinstance(det, dict):
            ds = det.get("data_status")
            if isinstance(ds, str) and ds:
                return ds
        return "OK"

    @staticmethod
    def _extract_adv_ratio(fr: FactorResult) -> Optional[float]:
        det: Any = getattr(fr, "details", None)
        if not isinstance(det, dict):
            return None
        v = det.get("adv_ratio")
        return float(v) if isinstance(v, (int, float)) else None

    # ===============================
    # Summary（制度中性）
    # ===============================
    
    def _build_summary(
        self,
        structure: Dict[str, Dict[str, str]],
        modifier: Optional[str],
    ) -> Dict[str, Any]:
        """生成制度中性的“结构摘要标签”。
    
        注意：
        - 不输出人类可读 meaning（避免多处口径漂移）
        - 只输出 tags，供 StructureFactsBlock 做统一解释与渲染
        """
        tags = []
    
        br = structure.get("breadth")
        if br:
            st = br.get("state")
            if st == "healthy":
                tags.append("breadth_not_broken")
            elif st == "not_broken":
                tags.append("breadth_weak_not_broken")
            elif st == "neutral":
                tags.append("breadth_neutral")
    
        tv = structure.get("turnover")
        if tv:
            st = tv.get("state")
            if st == "expanding":
                tags.append("turnover_expanding")
            elif st == "contracting":
                tags.append("turnover_contracting")
            elif st == "neutral":
                tags.append("turnover_neutral")
    
        frf = structure.get("failure_rate")
        if frf:
            st = frf.get("state")
            if st == "elevated_risk":
                tags.append("failure_rate_elevated")
            elif st == "watch":
                tags.append("failure_rate_watch")
            elif st == "stable":
                tags.append("failure_rate_stable")
    
        tif = structure.get("trend_in_force")
        if tif:
            st = tif.get("state")
            if st == "in_force":
                tags.append("trend_in_force")
            elif st == "weakening":
                tags.append("trend_weakening")
            elif st == "broken":
                tags.append("trend_broken")
    
        npp = structure.get("north_proxy_pressure")
        if npp:
            st = npp.get("state")
            if st == "pressure_high":
                tags.append("north_pressure_high")
            elif st == "pressure_medium":
                tags.append("north_pressure_medium")
            elif st == "pressure_low":
                tags.append("north_pressure_low")
    
        it = structure.get("index_tech")
        if it:
            st = it.get("state")
            if st == "strong":
                tags.append("index_tech_strong")
            elif st == "weak":
                tags.append("index_tech_weak")
            elif st == "neutral":
                tags.append("index_tech_neutral")
    
        if modifier == MOD_DISTRIBUTION_RISK:
            tags.append("modifier_distribution_risk")
        elif modifier == MOD_SUCCESS_RATE_DECLINING:
            tags.append("modifier_success_rate_declining")
        elif modifier == MOD_HIGH_EXECUTION_RISK:
            tags.append("modifier_high_execution_risk")
    
        return {"tags": tags}
