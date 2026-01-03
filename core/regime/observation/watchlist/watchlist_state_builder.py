from __future__ import annotations

from typing import Dict, Any

from core.factors.factor_result import FactorResult


class WatchlistStateBuilder:
    """
    UnifiedRisk V12 · WatchlistStateBuilder（Phase-2 / 冻结）

    职责（严格限定）：
    - 基于 FactorResult + StructureFacts
      为“观察对象”生成【观察状态 + 人话解释】
    - 只用于约束与解释，不产生交易信号
    - breadth 仅作为“安全阀”（只能收紧，不能放宽）
    """

    # ===============================
    # Public API
    # ===============================
    def build(
        self,
        *,
        factors: Dict[str, FactorResult],
        structure: Dict[str, Dict[str, str]],
        watchlist_config: Dict[str, Any],
    ) -> Dict[str, Dict[str, str]]:
        """
        输出结构（冻结）：

        {
          "<object_id>": {
              "title": "...",
              "state": "NOT_ALLOWED | OBSERVE",
              "summary": "...",
              "detail": "..."
          }
        }
        """
        states: Dict[str, Dict[str, str]] = {}

        for obj_id, cfg in watchlist_config.items():
            obj_type = cfg.get("type")

            if obj_type == "ETF":
                states[obj_id] = self._eval_etf(
                    obj_id=obj_id,
                    factors=factors,
                    structure=structure,
                    cfg=cfg,
                )
            elif obj_type == "STOCK":
                states[obj_id] = self._eval_stock(
                    obj_id=obj_id,
                    factors=factors,
                    structure=structure,
                    cfg=cfg,
                )
            elif obj_type == "INDEX":
                states[obj_id] = self._eval_index(
                    obj_id=obj_id,
                    factors=factors,
                    structure=structure,
                    cfg=cfg,
                )
            else:
                states[obj_id] = {
                    "title": cfg.get("title", obj_id),
                    "state": "OBSERVE",
                    "summary": "未知观察对象类型",
                    "detail": "该观察对象类型未被系统识别，仅作为占位观察。",
                }

        return states

    # ===============================
    # ETF / 主线代理（最严格）
    # ===============================
    def _eval_etf(
        self,
        *,
        obj_id: str,
        factors: Dict[str, FactorResult],
        structure: Dict[str, Dict[str, str]],
        cfg: Dict[str, Any],
    ) -> Dict[str, str]:
        title = cfg.get("title", obj_id)

        amount = structure.get("amount", {}).get("state")
        breadth = structure.get("breadth", {}).get("state")

        # ---------------------------------
        # 1️⃣ 广度安全阀（最高优先级）
        # ---------------------------------
        if breadth in ("broken", "damaged"):
            return {
                "title": title,
                "state": "NOT_ALLOWED",
                "summary": "市场广度出现趋势性破坏",
                "detail": (
                    "当前市场广度已出现趋势性破坏信号，"
                    "整体结构不支持任何主线参与行为，"
                    "该观察对象仅用于风险监控。"
                ),
            }

        # ---------------------------------
        # 2️⃣ 动能否决（常见限制条件）
        # ---------------------------------
        if amount == "contracting":
            return {
                "title": title,
                "state": "NOT_ALLOWED",
                "summary": "主线动能不足",
                "detail": (
                    "当前市场成交持续缩量，进攻动能不足，"
                    "主线仍处于止跌与结构验证阶段，"
                    "暂不具备被允许参与的条件。"
                ),
            }

        # ---------------------------------
        # 3️⃣ 结构未破坏，但未确认
        # ---------------------------------
        return {
            "title": title,
            "state": "OBSERVE",
            "summary": "主线处于观察阶段",
            "detail": (
                "市场结构尚未出现趋势性破坏，"
                "但主线动能与同步性仍需进一步确认，"
                "当前阶段仅作结构观察。"
            ),
        }

    # ===============================
    # 个股观察（风险提示，不否决）
    # ===============================
    def _eval_stock(
        self,
        *,
        obj_id: str,
        factors: Dict[str, FactorResult],
        structure: Dict[str, Dict[str, str]],
        cfg: Dict[str, Any],
    ) -> Dict[str, str]:
        title = cfg.get("title", obj_id)

        breadth = structure.get("breadth", {}).get("state")
        amount = structure.get("amount", {}).get("state")

        # breadth 仅用于风险提示，不直接否决个股
        if breadth in ("broken", "damaged"):
            return {
                "title": title,
                "state": "OBSERVE",
                "summary": "整体环境走弱，关注个股风险",
                "detail": (
                    "当前市场广度走弱，系统风险上升，"
                    "该个股仅用于观察波动与风险变化，"
                    "不作为参与依据。"
                ),
            }

        if amount == "contracting":
            return {
                "title": title,
                "state": "OBSERVE",
                "summary": "市场动能不足",
                "detail": (
                    "当前市场成交动能不足，"
                    "个股处于修复与观察阶段，"
                    "不支持主动参与。"
                ),
            }

        return {
            "title": title,
            "state": "OBSERVE",
            "summary": "个股风险观察",
            "detail": (
                "该个股用于观察权重股或高波动个股的稳定性，"
                "作为结构与风险对照参考，不构成参与依据。"
            ),
        }

    # ===============================
    # 指数观察（风格 / 背景参照）
    # ===============================
    def _eval_index(
        self,
        *,
        obj_id: str,
        factors: Dict[str, FactorResult],
        structure: Dict[str, Dict[str, str]],
        cfg: Dict[str, Any],
    ) -> Dict[str, str]:
        title = cfg.get("title", obj_id)

        breadth = structure.get("breadth", {}).get("state")

        if breadth in ("broken", "damaged"):
            return {
                "title": title,
                "state": "OBSERVE",
                "summary": "指数结构走弱",
                "detail": (
                    "当前市场广度出现破坏迹象，"
                    "指数用于观察整体趋势变化，"
                    "不作为参与判断依据。"
                ),
            }

        return {
            "title": title,
            "state": "OBSERVE",
            "summary": "指数结构参照",
            "detail": (
                "指数作为风格与趋势的整体参照对象，"
                "用于判断市场环境变化，"
                "不直接参与操作判断。"
            ),
        }
