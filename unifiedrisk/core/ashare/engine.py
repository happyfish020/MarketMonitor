# -*- coding: utf-8 -*-
"""
UnifiedRisk v5.0.2 - AShareDailyEngine
--------------------------------------
入口：engine.run(date=None) → payload

payload 结构：
    {
        "meta": {...},          # 日期 / 版本 / 运行时间
        "raw":  {...},          # A 股日级别快照（指数 / 市场宽度 / 北向 / 两融 / 主力 / 行业）
        "score": {...},         # RiskScorer 计算出的总分 & 分项
    }

兼容：
    - 现有 DataFetcher.fetch_daily_snapshot()
    - 现有 RiskScorer(raw)
    - 现有 report_writer.write_daily_report(payload, out_dir)
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any

import logging

LOG = logging.getLogger(__name__)

from .data_fetcher import DataFetcher
from .risk_scorer import RiskScorer

# 尝试导入 v5.0.2 的 schema / format 辅助函数（如不存在则降级为 no-op）
try:
    from .data_schema import ensure_meta, ensure_daily_snapshot  # type: ignore
except Exception:  # pragma: no cover
    ensure_meta = None           # type: ignore
    ensure_daily_snapshot = None # type: ignore

try:
    from .data_format import (   # type: ignore
        normalize_mainflow,
        normalize_margin,
        normalize_north,
        normalize_sector,
    )
except Exception:  # pragma: no cover
    normalize_mainflow = None    # type: ignore
    normalize_margin = None      # type: ignore
    normalize_north = None       # type: ignore
    normalize_sector = None      # type: ignore

# 北向趋势 & 行业轮动视图
try:
    from .north_trend import build_north_trend_view  # type: ignore
except Exception:  # pragma: no cover
    build_north_trend_view = None  # type: ignore

try:
    from .sector_rotation import build_sector_rotation_view  # type: ignore
except Exception:  # pragma: no cover
    build_sector_rotation_view = None  # type: ignore


class AShareDailyEngine:
    """
    A 股日级别核心引擎。

    使用方式：
        eng = AShareDailyEngine()
        payload = eng.run()              # 默认使用“今天”
        payload = eng.run("2025-11-29")  # 指定某个交易日
    """

    def __init__(self, date: Optional[str] = None):
        self.default_date = date
        self.fetcher = DataFetcher()

    # ----------------------------------------------------------
    # 主入口
    # ----------------------------------------------------------
    def run(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        执行完整的 A 股日级别数据抓取 + 风险评分。

        参数:
            date: 可选，形如 "2025-11-29" 的日期字符串。
                  如果为 None，则使用 DataFetcher 内部的“今天”。

        返回:
            payload: dict，供 report_writer 与外部系统使用。
        """
        trade_date = date or self.default_date

        # 1) 抓取原始快照
        raw = self.fetcher.fetch_daily_snapshot(trade_date)

        # 2) 元信息补全
        meta = raw.get("meta") or {}
        # 如果有 ensure_meta，则使用 schema 统一化
        if callable(ensure_meta):
            try:
                meta = ensure_meta(meta)  # type: ignore
            except Exception as e:  # pragma: no cover
                LOG.warning("[AShareEngine] ensure_meta 失败: %s", e)

        # 增加 run_time 字段
        meta.setdefault("run_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # 升级版本号（如已有则保留现有）
        meta.setdefault("version", "UnifiedRisk_v5.0.2")
        raw["meta"] = meta

        # 3) 统一日级快照结构（如 data_schema 存在）
        snapshot: Dict[str, Any]
        if callable(ensure_daily_snapshot):
            try:
                snapshot = ensure_daily_snapshot(raw)  # type: ignore
            except Exception as e:  # pragma: no cover
                LOG.warning("[AShareEngine] ensure_daily_snapshot 失败，退回 raw: %s", e)
                snapshot = raw
        else:
            snapshot = raw

        # 4) 字段规范化（如 data_format 存在）
        try:
            if callable(normalize_mainflow):
                snapshot["mainflow"] = normalize_mainflow(snapshot.get("mainflow"))  # type: ignore
            if callable(normalize_margin):
                snapshot["margin"] = normalize_margin(snapshot.get("margin"))        # type: ignore
            if callable(normalize_north):
                snapshot["north"] = normalize_north(snapshot.get("north"))          # type: ignore
            if callable(normalize_sector):
                snapshot["sector"] = normalize_sector(snapshot.get("sector"))        # type: ignore
        except Exception as e:  # pragma: no cover
            LOG.warning("[AShareEngine] normalize_* 过程中出错: %s", e)

        # 5) 北向趋势视图（Step 3）
        if callable(build_north_trend_view):
            try:
                snapshot["north_trend"] = build_north_trend_view(snapshot.get("north") or {}, meta)
            except Exception as e:  # pragma: no cover
                LOG.warning("[AShareEngine] 计算 north_trend 失败: %s", e)
                snapshot["north_trend"] = {}
        else:
            snapshot.setdefault("north_trend", {})

        # 6) 行业轮动视图（Step 4）
        if callable(build_sector_rotation_view):
            try:
                # 优先使用 sector["sectors"]（标准化后），否则尝试 raw 的 sector_flow 列表
                sectors_raw = None
                sector_block = snapshot.get("sector")
                if isinstance(sector_block, dict) and "sectors" in sector_block:
                    sectors_raw = sector_block.get("sectors")
                if sectors_raw is None:
                    # 兼容旧字段名
                    sectors_raw = snapshot.get("sector_flow") or []

                snapshot["sector_rotation"] = build_sector_rotation_view(sectors_raw or [])
            except Exception as e:  # pragma: no cover
                LOG.warning("[AShareEngine] 计算 sector_rotation 失败: %s", e)
                snapshot["sector_rotation"] = {
                    "table": [],
                    "top_strong": [],
                    "top_weak": [],
                    "summary_lines": ["行业轮动：计算失败。"],
                }
        else:
            snapshot.setdefault(
                "sector_rotation",
                {
                    "table": [],
                    "top_strong": [],
                    "top_weak": [],
                    "summary_lines": ["行业轮动：未启用。"],
                },
            )

        # 7) 风险评分
        scorer = RiskScorer(snapshot)
        score = scorer.score_all()

        # 8) 构造最终 payload
        payload: Dict[str, Any] = {
            "meta": {
                "date": meta.get("date"),
                "bj_time": meta.get("bj_time"),
                "version": meta.get("version"),
                "run_time": meta.get("run_time"),
            },
            "raw": snapshot,
            "score": score,
        }

        return payload
