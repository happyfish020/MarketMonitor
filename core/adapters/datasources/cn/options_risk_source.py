# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Options Risk DataSource (E Block)

璁捐鐩殑锛?
    鑱氬悎 ETF 鏈熸潈鏃ヨ鎯呮暟鎹紝璁＄畻鍔犳潈娑ㄨ穼棰濄€佹€绘定璺岄銆佸姞鏉冩敹鐩樹环浠ュ強鍏跺彉鍖栬秼鍔裤€?
    姝ゆ暟鎹簮涓烘湡鏉冮闄╁垎鏋愭彁渚涘熀纭€鍘熷鏁版嵁锛岀敤浜庡悗缁洜瀛愭墦鍒嗗拰鎶ュ憡灞曠ず銆?

绾︽潫锛?
    - 浠呬緷璧?DBOracleProvider锛屼笉璁块棶澶栭儴 API銆?
    - 浠呰仛鍚堜竴缁勫浐瀹氱殑 ETF 鏈熸潈鏍囩殑锛堜節鍙狤TF锛夛紝鏍规嵁閰嶇疆鍙皟鏁淬€?
    - 鎸夋棩鏋勫缓鏃堕棿搴忓垪锛岄粯璁ゅ洖婧?60 鏃ャ€?

杈撳嚭瀛楁锛?
    trade_date: 鏈€鏂颁氦鏄撴棩鏈燂紙瀛楃涓诧級
    weighted_change: 鎸夋垚浜ら噺鍔犳潈鐨勬定璺岄鍧囧€?
    total_change:    鎵€鏈夊悎绾︽定璺岄姹傚拰
    total_volume:    鎬绘垚浜ら噺
    weighted_close:  鎸夋垚浜ら噺鍔犳潈鐨勬敹鐩樹环
    change_ratio:    weighted_change / weighted_close锛堣嫢鏀剁洏浠蜂负 0锛屽垯涓?0 鎴?None锛?
    trend_10d:       杩?10 鏃?weighted_change 鍙樺寲
    acc_3d:         杩?3 鏃?weighted_change 鍙樺寲
    series: 鍘嗗彶搴忓垪鍒楄〃锛屾瘡椤瑰寘鍚?trade_date, weighted_change, total_change, total_volume,
            weighted_close, change_ratio

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

LOG = get_logger("DS.OptionsRisk")


class OptionsRiskDataSource(DataSourceBase):
    """
    Options Risk DataSource

    鑱氬悎 ETF 鏈熸潈鏃ヨ鎯呮暟鎹紝璁＄畻鍔犳潈娑ㄨ穼棰濆強鍏惰秼鍔?鍔犻€熷害銆?
    """

    def __init__(self, config: DataSourceConfig, window: int = 60) -> None:
        # 鍥哄畾鍚嶇О锛屼究浜庢棩蹇楄瘑鍒?
        super().__init__(name="DS.OptionsRisk")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBOracleProvider()

        # 缂撳瓨鍜屽巻鍙茶矾寰?
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 鍗曟棩 cache 鏂囦欢鍚?
        self.cache_file = os.path.join(self.cache_root, "options_risk_today.json")
        self.history_file = os.path.join(self.history_root, "options_risk_series.json")

        LOG.info(
            "[DS.OptionsRisk] Init: market=%s ds_name=%s cache_root=%s history_root=%s window=%s",
            config.market,
            config.ds_name,
            self.cache_root,
            self.history_root,
            self.window,
        )

    # ------------------------------------------------------------------
    def build_block(self, trade_date: str, refresh_mode: str = "none") -> Dict[str, Any]:
        """
        鏋勫缓鏈熸潈椋庨櫓鍘熷鏁版嵁鍧椼€?

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
                LOG.error("[DS.OptionsRisk] load cache error: %s", exc)

        # 璋冪敤 DB provider 鑱氬悎鏁版嵁
        try:
            df: pd.DataFrame = self.db.fetch_options_risk_series(
                start_date=trade_date,
                look_back_days=self.window,
            )
        except Exception as exc:
            LOG.error("[DS.OptionsRisk] fetch_options_risk_series error: %s", exc)
            return self._neutral_block(trade_date)

        if df is None or df.empty:
            LOG.warning("[DS.OptionsRisk] no data returned for %s", trade_date)
            return self._neutral_block(trade_date)

        df_sorted = df.sort_index(ascending=True)
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            dt_str = idx.strftime("%Y-%m-%d")
            try:
                wchg = float(row.get("weighted_change", 0.0)) if pd.notna(row.get("weighted_change")) else 0.0
            except Exception:
                wchg = 0.0
            try:
                tchg = float(row.get("total_change", 0.0)) if pd.notna(row.get("total_change")) else 0.0
            except Exception:
                tchg = 0.0
            try:
                tv = float(row.get("total_volume", 0.0)) if pd.notna(row.get("total_volume")) else 0.0
            except Exception:
                tv = 0.0
            try:
                wclose = float(row.get("weighted_close", 0.0)) if pd.notna(row.get("weighted_close")) else 0.0
            except Exception:
                wclose = 0.0
            try:
                ratio = row.get("change_ratio")
                ratio = float(ratio) if ratio is not None and pd.notna(ratio) else 0.0
            except Exception:
                ratio = 0.0
            series.append({
                "trade_date": dt_str,
                "weighted_change": wchg,
                "total_change": tchg,
                "total_volume": tv,
                "weighted_close": wclose,
                "change_ratio": ratio,
            })

        merged_series = self._merge_history(series)
        trend_10d, acc_3d = self._calc_trend(merged_series)
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.OptionsRisk] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        weighted_change = latest.get("weighted_change", 0.0)
        total_change = latest.get("total_change", 0.0)
        total_volume = latest.get("total_volume", 0.0)
        weighted_close = latest.get("weighted_close", 0.0)
        change_ratio = latest.get("change_ratio", 0.0)

        block: Dict[str, Any] = {
            "trade_date": latest_date,
            "weighted_change": weighted_change,
            "total_change": total_change,
            "total_volume": total_volume,
            "weighted_close": weighted_close,
            "change_ratio": change_ratio,
            "trend_10d": trend_10d,
            "acc_3d": acc_3d,
            "series": merged_series,
            # 鏍囪鏁版嵁鐘舵€佷负 OK锛岃〃鏄庢暟鎹潵婧愭甯?
            "data_status": "OK",
            # 榛樿鏃?warnings锛涜嫢涓婂眰闇€瑕佸彲瑕嗙洊
            "warnings": [],
        }

        # 淇濆瓨鍘嗗彶鍜岀紦瀛?
        try:
            self._save(self.history_file, merged_series)
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.OptionsRisk] save error: %s", exc)

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
        璁＄畻 10 鏃ヨ秼鍔垮拰 3 鏃ュ姞閫熷害锛堝熀浜?weighted_change锛夈€?
        """
        if len(series) < 2:
            return 0.0, 0.0
        values = [s.get("weighted_change", 0.0) or 0.0 for s in series]
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

        娉ㄦ剰锛氬綋鏁版嵁缂哄け鎴栨棤娉曞姞杞芥椂锛岄渶鏄庣‘鏍囨敞 data_status 涓?"MISSING"銆傚鏋滅渷鐣ユ瀛楁锛?
        涓婂眰鍥犲瓙鍜屾姤鍛婁細榛樿璁や负鏁版嵁姝ｅ父锛?OK"锛夛紝浠庤€岀粰鍑轰笉鍑嗙‘鐨勬彁绀恒€傛澶勬垜浠槑纭?
        璁剧疆 data_status 涓?"MISSING" 浠ヤ究 WatchlistLeadFactor 鑳芥纭瘑鍒暟鎹己澶辨儏鍐点€?
        """
        return {
            "trade_date": trade_date,
            "weighted_change": 0.0,
            "total_change": 0.0,
            "total_volume": 0.0,
            "weighted_close": 0.0,
            "change_ratio": 0.0,
            "trend_10d": 0.0,
            "acc_3d": 0.0,
            "series": [],
            "data_status": "MISSING",
            # 鎻愪緵涓€涓?warnings 瀛楁浠ヤ究涓婂眰闈㈡澘璁板綍缂哄け鍘熷洜
            "warnings": ["missing:options_risk_series"],
        }
