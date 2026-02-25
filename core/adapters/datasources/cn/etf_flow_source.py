# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ETF Flow DataSource (C Block)

璁捐鐩殑锛?
    浠庢湰鍦?Oracle 鏁版嵁搴撶殑鍩洪噾 ETF 鏃ヨ鎯呰〃锛圕N_FUND_ETF_HIST_EM锛?
    鑱氬悎璁＄畻 ETF 浠介鍙樺寲浠ｇ悊鎸囨爣锛屾彁渚涘師濮嬬獥鍙ｅ簭鍒楀拰瓒嬪娍鎸囨爣銆?

绾︽潫锛?
    - 浠呬緷璧?DBOracleProvider锛屼笉璁块棶澶栭儴 API
    - 涓嶅畾涔夋柊鐨?provider 鎺ュ彛锛岀洿鎺ヨ皟鐢?provider 灞傛彁渚涚殑鑱氬悎鏂规硶
    - 鎸夋棩鏋勫缓鏃堕棿搴忓垪锛寃indow 榛樿 60 澶?

杈撳嚭瀛楁锛?
    trade_date: 浜ゆ槗鏃ユ湡锛堟渶鏂颁竴涓氦鏄撴棩瀛楃涓诧級
    total_change_amount: 褰撴棩鎵€鏈?ETF price change 涔嬪拰
    total_volume: 褰撴棩 ETF 鎴愪氦閲忎箣鍜?
    total_amount: 褰撴棩 ETF 鎴愪氦棰濅箣鍜?
    flow_ratio: 褰撴棩浠锋牸娑ㄨ穼棰濅笌鎴愪氦閲忕殑姣斿€硷紙proxy锛?
    trend_10d: 10 鏃ョ疮璁″彉鍖栵紙鎬?price change锛?
    acc_3d: 3 鏃ョ疮璁″彉鍖栵紙鎬?price change锛?
    series: 浠庢棫鍒版柊鐨勫巻鍙插簭鍒楀垪琛紝姣忛」鍖呭惈 trade_date銆乼otal_change_amount銆乼otal_volume銆乼otal_amount

褰撴暟鎹己澶辨垨寮傚父鏃讹紝杩斿洖 neutral_block
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

LOG = get_logger("DS.ETFFlow")


class ETFFlowDataSource(DataSourceBase):
    """
    ETF Flow DataSource

    鑱氬悎 ETF 鏃ヨ鎯呰〃鐨?price change / volume / amount 鏁版嵁锛?
    閫氳繃 10 澶╁拰 3 澶╃疮绉€兼彁渚涜秼鍔垮拰鍔犻€熷害淇℃伅銆?
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        # 浣跨敤鍥哄畾鍚嶇О锛屼究浜庢棩蹇楄瘑鍒?
        super().__init__(name="DS.ETFFlow")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # cache 鍜?history 璺緞
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 鍗曟棩 cache 缁熶竴鍛藉悕锛岄伩鍏嶄娇鐢?trade_date 浣滀负鏂囦欢鍚?
        self.cache_file = os.path.join(self.cache_root, "etf_flow_today.json")
        # 鎸佷箙鍖栧巻鍙插簭鍒?
        self.history_file = os.path.join(self.history_root, "etf_flow_series.json")

        LOG.info(
            "[DS.ETFFlow] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        涓诲叆鍙ｏ細鏋勫缓 ETF flow 鍘熷鏁版嵁鍧椼€?

        鍙傛暟锛?
            trade_date: 瀛楃涓诧紝璇勪及鏃ユ湡锛堥€氬父涓?T 鎴?T-1锛?
            refresh_mode: 鍒锋柊绛栫暐锛屾敮鎸?none/readonly/full
        """
        # 鎸?refresh_mode 娓呯悊缂撳瓨鏂囦欢
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
                LOG.error("[DS.ETFFlow] load cache error: %s", exc)

        # 璇诲彇鑱氬悎鏁版嵁
        try:
            df: pd.DataFrame = self.db.fetch_etf_hist_series(
                start_date=trade_date,
                look_back_days=self.window,
            )
        except Exception as exc:
            LOG.error("[DS.ETFFlow] fetch_etf_hist_series error: %s", exc)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.warning("[DS.ETFFlow] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        # 纭繚鏈夊簭锛氭寜鏃ユ湡鍗囧簭锛堟棫鈫掓柊锛?
        df_sorted = df.sort_index(ascending=True)

        # 灏?DataFrame 杞负鍒楄〃 [{trade_date, total_change_amount, ...}]
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            series.append({
                "trade_date": idx.strftime("%Y-%m-%d"),
                "total_change_amount": float(row["total_change_amount"]) if pd.notna(row["total_change_amount"]) else 0.0,
                "total_volume": float(row["total_volume"]) if pd.notna(row["total_volume"]) else 0.0,
                "total_amount": float(row["total_amount"]) if pd.notna(row["total_amount"]) else 0.0,
            })

        # 鍚堝苟鍘嗗彶锛堜繚璇佹粦绐楅暱搴﹀浐瀹氾紝鍚戝悗琛ラ綈锛?
        merged_series = self._merge_history(series)

        # 璁＄畻瓒嬪娍/鍔犻€熷害
        trend_10d, acc_3d = self._calc_trend(merged_series)

        # 鏈€鏂拌褰?
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.ETFFlow] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        total_change_amount = latest.get("total_change_amount")
        total_volume = latest.get("total_volume")
        total_amount = latest.get("total_amount")
        # 姣斿€硷細閬垮厤闄ら浂
        flow_ratio = 0.0
        try:
            flow_ratio = round(total_change_amount / total_volume, 4) if total_volume else 0.0
        except Exception:
            flow_ratio = 0.0

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "total_change_amount": total_change_amount,
            "total_volume": total_volume,
            "total_amount": total_amount,
            "flow_ratio": flow_ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": merged_series,
        }

        # 淇濆瓨鍒板巻鍙插拰缂撳瓨
        try:
            # 鎸佷箙鍖栧巻鍙?
            self._save(self.history_file, merged_series)
            # 缂撳瓨褰撳ぉ鍧?
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.ETFFlow] save error: %s", exc)

        return block

    # ------------------------------------------------------------------
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        鍚堝苟鍘嗗彶搴忓垪銆?

        recent: 褰撳墠鏌ヨ绐楀彛鍐呯殑鍒楄〃锛堝崌搴忥級銆?
        history_file 涓繚鐣欐洿涔呰繙鐨勫巻鍙茶褰曪紝涓?recent 鍚堝苟鍚庢埅鍙?window 闀垮害銆?
        """
        old = []
        if os.path.exists(self.history_file):
            try:
                old = self._load(self.history_file)
            except Exception:
                old = []
        # 鏋勫缓瀛楀吀浠ユ棩鏈熷幓閲?
        buf: Dict[str, Dict[str, Any]] = {r["trade_date"]: r for r in old}
        for r in recent:
            buf[r["trade_date"]] = r
        out = sorted(buf.values(), key=lambda x: x["trade_date"])
        return out[-self.window:]

    # ------------------------------------------------------------------
    def _calc_trend(self, series: List[Dict[str, Any]]) -> tuple[float, float]:
        """
        璁＄畻 10 澶╄秼鍔垮拰 3 澶╁姞閫熷害銆?
        trend_10d = last.total_change_amount - total_change_amount[-11]
        acc_3d   = last.total_change_amount - total_change_amount[-4]
        鑻ラ暱搴︿笉澶燂紝鍒欒繑鍥?0.0
        """
        if len(series) < 2:
            return 0.0, 0.0
        values = [s.get("total_change_amount", 0.0) or 0.0 for s in series]
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
        杩斿洖绌?涓€у潡銆?
        """
        return {
            "trade_date": trade_date,
            "total_change_amount": 0.0,
            "total_volume": 0.0,
            "total_amount": 0.0,
            "flow_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
        }
