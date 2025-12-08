# core/snapshot/ashare_snapshot.py

"""
UnifiedRisk V12
Snapshot Builder - 正式版

功能：
  - 接收 fetcher 的 raw_snapshot
  - 构建标准化 snapshot_v12
  - 自动补齐字段
  - 日志化所有步骤
"""

from typing import Dict, Any
from core.utils.logger import get_logger

LOG = get_logger("Snapshot.Builder")


class SnapshotBuilder:
    """
    Snapshot 统一结构化模块（V12）
    """

    # V12 Snapshot Schema（因子系统依赖的字段）
    REQUIRED_KEYS = [
        "meta",
        "index_core",
        "turnover",
        "sentiment",
        "emotion",
        "margin",
        "spot",
        "global_lead",
        "north_nps"
    ]

    def __init__(self):
        LOG.info("SnapshotBuilderV12 初始化")

    # ------------------------------------------------------------------
    def build(self, raw_snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据 raw_snapshot 构建 V12 标准化结构。

        raw_snapshot 来自 AshareFetcher.build_daily_snapshot_v12()
        """

        LOG.info("SnapshotBuilderV12 开始构建 snapshot_v12")

        snapshot = {}

        # ====== 1. 主字段结构化 ======
        for key in self.REQUIRED_KEYS:
            if key in raw_snapshot:
                snapshot[key] = raw_snapshot[key]
                LOG.info("Snapshot 字段载入: %s (type=%s)",
                         key, type(raw_snapshot[key]).__name__)
            else:
                # 自动补空
                snapshot[key] = {} if key != "meta" else {"trade_date": None}
                LOG.warning("Snapshot 字段缺失，已补齐默认值: %s", key)

        # ====== 2. 强化 meta 结构 ======
        self._fix_meta(snapshot)

        # ====== 3. 强化 index_core ======
        self._fix_index_core(snapshot)

        # ====== 4. 强化 turnover ======
        self._fix_turnover(snapshot)

        # ====== 5. 强化 sentiment ======
        self._fix_sentiment(snapshot)

        # ====== 6. 强化 northbound ======
        self._fix_northbound(snapshot)

        # ====== 7. 强化 margin ======
        self._fix_margin(snapshot)

        # ====== 8. 强化 spot ======
        self._fix_spot(snapshot)

        # ====== 9. 强化 global_lead ======
        self._fix_global_lead(snapshot)

        LOG.info("SnapshotBuilderV12 构建完成: trade_date=%s",
                 snapshot["meta"].get("trade_date"))

        return snapshot

    # ------------------------------------------------------------------
    # 子结构修复区
    # ------------------------------------------------------------------

    def _fix_meta(self, snapshot: Dict[str, Any]):
        meta = snapshot["meta"]
        LOG.info("修正 meta 字段: %s", meta)

        # trade_date 必须存在
        if "trade_date" not in meta:
            LOG.warning("meta.trade_date 缺失，自动补齐 None")
            meta["trade_date"] = None

        snapshot["meta"] = meta

    # ------------------------------------------------------------------
    def _fix_index_core(self, snapshot):
        d = snapshot["index_core"]
        if not isinstance(d, dict):
            LOG.error("index_core 格式异常，重置为空 dict")
            snapshot["index_core"] = {}
            return

        # 每个指数必须含 {last, pct}
        for name, info in d.items():
            if "last" not in info:
                LOG.warning("index_core.%s.last 缺失，设置为 None", name)
                info["last"] = None
            if "pct" not in info:
                LOG.warning("index_core.%s.pct 缺失，设置为 None", name)
                info["pct"] = None

    # ------------------------------------------------------------------
    def _fix_turnover(self, snapshot):
        d = snapshot["turnover"]
        if not isinstance(d, dict):
            LOG.error("turnover 格式异常，重置为空 dict")
            snapshot["turnover"] = {}
            return

        # 标准字段
        d.setdefault("sh", None)
        d.setdefault("sz", None)
        d.setdefault("total", None)

    # ------------------------------------------------------------------
    def _fix_sentiment(self, snapshot):
        d = snapshot["sentiment"]
        if not isinstance(d, dict):
            LOG.error("sentiment 格式异常，重置为空 dict")
            snapshot["sentiment"] = {}
            return

        d.setdefault("adv", None)
        d.setdefault("dec", None)
        d.setdefault("ratio", None)

    # ------------------------------------------------------------------
    def _fix_northbound(self, snapshot):
        d = snapshot["north_nps"]
    
        if not isinstance(d, dict):
            LOG.error("north_nps 格式异常，重置为空 dict")
            snapshot["north_nps"] = {}
            return
    
        # === V12 正式字段（以 DS 输出为准） ===
        required_keys = [
            "strength_today",
            "turnover_today_e9",
            "trend_3d",
            "trend_5d",
            "zone",
            "anomaly",
        ]
    
        for k in required_keys:
            if k not in d:
                LOG.warning(f"[SnapshotBuilder] north_nps 缺失字段: {k}")
    
        # 不修改 / 不覆盖 north_nps block（保持 DS 原样）
        LOG.info("[SnapshotBuilder] north_nps 结构校验完成")
 
    # ------------------------------------------------------------------
    def _fix_margin(self, snapshot):
        d = snapshot["margin"]
        if not isinstance(d, dict):
            LOG.error("margin 格式异常，重置为空 dict")
            snapshot["margin"] = {}
            return

        d.setdefault("rz_balance", None)
        d.setdefault("rq_balance", None)
        d.setdefault("total", None)
        d.setdefault("trend_10d", None)
        d.setdefault("acc_3d", None)
        d.setdefault("risk_zone", None)

    # ------------------------------------------------------------------
    def _fix_spot(self, snapshot):
        d = snapshot["spot"]
        if not isinstance(d, dict):
            LOG.error("spot 格式异常，重置为空 dict")
            snapshot["spot"] = {}
            return

        d.setdefault("adv", None)
        d.setdefault("dec", None)
        d.setdefault("limit_up", None)
        d.setdefault("limit_down", None)
        d.setdefault("hs300_pct", None)

    # ------------------------------------------------------------------
    def _fix_global_lead(self, snapshot):
        d = snapshot["global_lead"]
        if not isinstance(d, dict):
            LOG.error("global_lead 格式异常，重置为空 dict")
            snapshot["global_lead"] = {}
            return

        # 每个 symbol 应至少有 close/pct
        for group, items in d.items():
            for sym, info in items.items():
                if "close" not in info:
                    LOG.warning("global_lead.%s.%s.close 缺失", group, sym)
                    info["close"] = None
                if "pct" not in info:
                    LOG.warning("global_lead.%s.%s.pct 缺失", group, sym)
                    info["pct"] = None
 