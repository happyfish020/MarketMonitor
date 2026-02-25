# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Futures Basis DataSource (D Block)

璁捐鐩殑锛?
    浠庢湰鍦?Oracle 鏁版嵁搴撶殑鑲℃寚鏈熻揣鏃ヨ鎯呰〃锛圕N_FUT_INDEX_HIS锛夊拰鎸囨暟鏃ヨ鎯呰〃锛圕N_INDEX_DAILY_PRICE锛?
    鑱氬悎璁＄畻鑲℃寚鏈熻揣鍩哄樊锛堟湡璐х粨绠椾环 - 鎸囨暟鏀剁洏浠凤級鍙婂叾璧板娍锛岀敤浜庨闄╃洃娴嬨€傝繑鍥炲師濮嬫椂闂村簭鍒椼€?
    鍩哄樊鍧囧€笺€佽秼鍔垮拰鍔犻€熷害銆?

绾︽潫锛?
    - 浠呬緷璧?DBOracleProvider锛屼笉璁块棶澶栭儴 API銆?
    - 涓嶅畾涔夋柊鐨?provider 鎺ュ彛锛岀洿鎺ヨ皟鐢?provider 灞傛彁渚涚殑鑱氬悎鏂规硶 fetch_futures_basis_series銆?
    - 鎸夋棩鏋勫缓鏃堕棿搴忓垪锛寃indow 榛樿 60 澶┿€?

杈撳嚭瀛楁锛?
    trade_date: 浜ゆ槗鏃ユ湡锛堟渶鏂颁竴涓氦鏄撴棩瀛楃涓诧級
    avg_basis:  鎸夋垚浜ら噺鍔犳潈鐨勫熀宸潎鍊硷紙鏈熻揣 - 鎸囨暟锛夛紝姝ｅ€间负鍗囨按锛岃礋鍊间负璐存按
    total_basis: 鎸夊悎绾︾畝鍗曟眰鍜岀殑鍩哄樊锛堣緟鍔╋級
    basis_ratio: 鍩哄樊鐩稿浜庡姞鏉冩寚鏁版敹鐩樹环鐨勬瘮鍊?
    trend_10d:   杩?10 鏃ュ熀宸彉鍖栵紙鍩哄樊鍧囧€煎樊锛?
    acc_3d:     杩?3 鏃ュ熀宸彉鍖栵紙鍩哄樊鍧囧€煎樊锛?
    series: 鍘嗗彶搴忓垪鍒楄〃锛屾瘡椤瑰寘鍚?trade_date銆乤vg_basis銆乼otal_basis銆乥asis_ratio銆亀eighted_future_price銆亀eighted_index_price銆乼otal_volume

褰撴暟鎹己澶辨垨寮傚父鏃讹紝杩斿洖 neutral_block銆?
"""

from __future__ import annotations

import os
import json
from typing import Dict, Any, List

import pandas as pd

from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger
from core.adapters.providers.db_provider_mysql_market import DBOracleProvider

LOG = get_logger("DS.FuturesBasis")


class FuturesBasisDataSource(DataSourceBase):
    """
    Futures Basis DataSource

    鑱氬悎鑲℃寚鏈熻揣鍜屾寚鏁版棩琛屾儏琛ㄧ殑鏁版嵁锛岃绠楀姞鏉冨熀宸簭鍒楀強鍏惰秼鍔?鍔犻€熷害銆?
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        # 鍥哄畾鍚嶇О锛屼究浜庢棩蹇楄瘑鍒?
        super().__init__(name="DS.FuturesBasis")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # 缂撳瓨鍜屽巻鍙茶矾寰?
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 鍗曟棩 cache 鏂囦欢鍚?
        self.cache_file = os.path.join(self.cache_root, "futures_basis_today.json")
        self.history_file = os.path.join(self.history_root, "futures_basis_series.json")

        LOG.info(
            "[DS.FuturesBasis] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        鏋勫缓鏈熸寚鍩哄樊鍘熷鏁版嵁鍧椼€?

        鍙傛暟锛?
            trade_date: 瀛楃涓诧紝璇勪及鏃ユ湡锛堥€氬父涓?T 鎴?T-1锛?
            refresh_mode: 鍒锋柊绛栫暐锛屾敮鎸?none/readonly/full
        """
        # 娓呯悊缂撳瓨渚濇嵁 refresh_mode
        apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        # 鍛戒腑缂撳瓨鐩存帴杩斿洖
        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.FuturesBasis] load cache error: %s", exc)

        # 璇诲彇鑱氬悎鏁版嵁
        try:
            df: pd.DataFrame = self.db.fetch_futures_basis_series(
                start_date=trade_date,
                look_back_days=self.window,
            )
        except Exception as exc:
            LOG.error("[DS.FuturesBasis] fetch_futures_basis_series error: %s", exc)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.warning("[DS.FuturesBasis] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        df_sorted = df.sort_index(ascending=True)
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            dt_str = idx.strftime("%Y-%m-%d")
            try:
                avg_basis = float(row.get("avg_basis", 0.0)) if pd.notna(row.get("avg_basis")) else 0.0
            except Exception:
                avg_basis = 0.0
            try:
                total_basis = float(row.get("total_basis", 0.0)) if pd.notna(row.get("total_basis")) else 0.0
            except Exception:
                total_basis = 0.0
            try:
                basis_ratio = row.get("basis_ratio")
                basis_ratio = float(basis_ratio) if basis_ratio is not None and pd.notna(basis_ratio) else 0.0
            except Exception:
                basis_ratio = 0.0
            try:
                w_fut = float(row.get("weighted_future_price", 0.0)) if pd.notna(row.get("weighted_future_price")) else 0.0
            except Exception:
                w_fut = 0.0
            try:
                w_idx = float(row.get("weighted_index_price", 0.0)) if pd.notna(row.get("weighted_index_price")) else 0.0
            except Exception:
                w_idx = 0.0
            try:
                total_volume = float(row.get("total_volume", 0.0)) if pd.notna(row.get("total_volume")) else 0.0
            except Exception:
                total_volume = 0.0
            series.append({
                "trade_date": dt_str,
                "avg_basis": avg_basis,
                "total_basis": total_basis,
                "basis_ratio": basis_ratio,
                "weighted_future_price": w_fut,
                "weighted_index_price": w_idx,
                "total_volume": total_volume,
            })

        merged_series = self._merge_history(series)
        trend_10d, acc_3d = self._calc_trend(merged_series)
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.FuturesBasis] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        avg_basis = latest.get("avg_basis", 0.0)
        total_basis = latest.get("total_basis", 0.0)
        ratio = latest.get("basis_ratio", 0.0)

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "avg_basis": avg_basis,
            "total_basis": total_basis,
            "basis_ratio": ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": merged_series,
        }

        # 淇濆瓨鍘嗗彶鍜岀紦瀛?
        try:
            self._save(self.history_file, merged_series)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.FuturesBasis] save error: %s", exc)

        return block

    # ------------------------------------------------------------------
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        鍚堝苟鍘嗗彶涓庡綋鍓嶇獥鍙ｏ紝淇濊瘉闀垮害鍥哄畾涓?window銆?
        """
        old: List[Dict[str, Any]] = []
        if os.path.exists(self.history_file):
            try:
                old = self._load(self.history_file)
            except Exception:
                old = []
        buf: Dict[str, Dict[str, Any]] = {r["trade_date"]: r for r in old}
        for r in recent:
            buf[r["trade_date"]] = r
        out = sorted(buf.values(), key=lambda x: x["trade_date"])
        return out[-self.window:]

    # ------------------------------------------------------------------
    def _calc_trend(self, series: List[Dict[str, Any]]) -> tuple[float, float]:
        """
        璁＄畻 10 鏃ヨ秼鍔垮拰 3 鏃ュ姞閫熷害锛堝熀宸潎鍊煎樊锛夈€?
        """
        if len(series) < 2:
            return 0.0, 0.0
        values = [s.get("avg_basis", 0.0) or 0.0 for s in series]
        try:
            t10 = values[-1] - values[-11] if len(values) >= 11 else 0.0
            a3 = values[-1] - values[-4] if len(values) >= 4 else 0.0
            return round(t10, 2), round(a3, 2)
        except Exception:
            return 0.0, 0.0

    # ------------------------------------------------------------------
    @staticmethod
    def _load(path: str) -> Any:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    @staticmethod
    def _save(path: str, obj: Any) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _neutral_block(trade_date: str) -> Dict[str, Any]:
        """
        杩斿洖绌?涓€у潡锛屾墍鏈夋寚鏍囦负 0.0锛宻eries 涓虹┖銆?
        """
        return {
            "trade_date": trade_date,
            "avg_basis": 0.0,
            "total_basis": 0.0,
            "basis_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
        }
