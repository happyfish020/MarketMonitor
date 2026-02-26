# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Futures Basis DataSource (D Block)

鐠佹崘顓搁惄顔炬畱閿?
    娴犲孩婀伴崷?Oracle 閺佺増宓佹惔鎾舵畱閼测剝瀵氶張鐔绘彛閺冦儴顢戦幆鍛般€冮敍鍦昇_FUT_INDEX_HIS閿涘鎷伴幐鍥ㄦ殶閺冦儴顢戦幆鍛般€冮敍鍦昇_INDEX_DAILY_PRICE閿?
    閼辨艾鎮庣拋锛勭暬閼测剝瀵氶張鐔绘彛閸╁搫妯婇敍鍫熸埂鐠愌呯波缁犳ぞ鐜?- 閹稿洦鏆熼弨鍓佹磸娴犲嚖绱氶崣濠傚従鐠ф澘濞嶉敍宀€鏁ゆ禍搴棑闂勨晝娲冨ù瀣ㄢ偓鍌濈箲閸ョ偛甯慨瀣闂傛潙绨崚妞尖偓?
    閸╁搫妯婇崸鍥р偓绗衡偓浣界Ъ閸斿灝鎷伴崝鐘烩偓鐔峰閵?

缁撅附娼敍?
    - 娴犲懍绶风挧?DBOracleProvider閿涘奔绗夌拋鍧楁６婢舵牠鍎?API閵?
    - 娑撳秴鐣炬稊澶嬫煀閻?provider 閹恒儱褰涢敍宀€娲块幒銉ㄧ殶閻?provider 鐏炲倹褰佹笟娑氭畱閼辨艾鎮庨弬瑙勭《 fetch_futures_basis_series閵?
    - 閹稿妫╅弸鍕紦閺冨爼妫挎惔蹇撳灙閿涘瘍indow 姒涙顓?60 婢垛斂鈧?

鏉堟挸鍤€涙顔岄敍?
    trade_date: 娴溿倖妲楅弮銉︽埂閿涘牊娓堕弬棰佺娑擃亙姘﹂弰鎾存）鐎涙顑佹稉璇х礆
    avg_basis:  閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕唨瀹割喖娼庨崐纭风礄閺堢喕鎻?- 閹稿洦鏆熼敍澶涚礉濮濓絽鈧棿璐熼崡鍥ㄦ寜閿涘矁绀嬮崐闂磋礋鐠愬瓨鎸?
    total_basis: 閹稿鎮庣痪锔剧暆閸楁洘鐪伴崪宀€娈戦崺鍝勬▕閿涘牐绶熼崝鈺嬬礆
    basis_ratio: 閸╁搫妯婇惄绋款嚠娴滃骸濮為弶鍐╁瘹閺佺増鏁归惄妯圭幆閻ㄥ嫭鐦崐?
    trend_10d:   鏉?10 閺冦儱鐔€瀹割喖褰夐崠鏍电礄閸╁搫妯婇崸鍥р偓鐓庢▕閿?
    acc_3d:     鏉?3 閺冦儱鐔€瀹割喖褰夐崠鏍电礄閸╁搫妯婇崸鍥р偓鐓庢▕閿?
    series: 閸樺棗褰舵惔蹇撳灙閸掓銆冮敍灞剧槨妞ょ懓瀵橀崥?trade_date閵嗕工vg_basis閵嗕辜otal_basis閵嗕攻asis_ratio閵嗕簚eighted_future_price閵嗕簚eighted_index_price閵嗕辜otal_volume

瑜版挻鏆熼幑顔惧繁婢惰鲸鍨ㄥ鍌氱埗閺冭绱濇潻鏂挎礀 neutral_block閵?
"""

from __future__ import annotations

import os
import json
from typing import Dict, Any, List

import pandas as pd

from core.datasources.datasource_base import DataSourceConfig, DataSourceBase
from core.utils.ds_refresh import apply_refresh_cleanup
from core.utils.logger import get_logger
from core.adapters.providers.db_provider_mysql_market import DBMySQLMarketProvider

LOG = get_logger("DS.FuturesBasis")


class FuturesBasisDataSource(DataSourceBase):
    """
    Futures Basis DataSource

    閼辨艾鎮庨懖鈩冨瘹閺堢喕鎻ｉ崪灞惧瘹閺佺増妫╃悰灞惧剰鐞涖劎娈戦弫鐗堝祦閿涘矁顓哥粻妤€濮為弶鍐ㄧ唨瀹割喖绨崚妤€寮烽崗鎯扮Ъ閸?閸旂娀鈧喎瀹抽妴?
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        # 閸ュ搫鐣鹃崥宥囆為敍灞肩┒娴滃孩妫╄箛妤勭槕閸?
        super().__init__(name="DS.FuturesBasis")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBMySQLMarketProvider()

        # 缂傛挸鐡ㄩ崪灞藉坊閸欒尪鐭惧?
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 閸楁洘妫?cache 閺傚洣娆㈤崥?
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
        閺嬪嫬缂撻張鐔稿瘹閸╁搫妯婇崢鐔奉潗閺佺増宓侀崸妞尖偓?

        閸欏倹鏆熼敍?
            trade_date: 鐎涙顑佹稉璇х礉鐠囧嫪鍙婇弮銉︽埂閿涘牓鈧艾鐖舵稉?T 閹?T-1閿?
            refresh_mode: 閸掗攱鏌婄粵鏍殣閿涘本鏁幐?none/readonly/full
        """
        # 濞撳懐鎮婄紓鎾崇摠娓氭繃宓?refresh_mode
        apply_refresh_cleanup(
            refresh_mode=refresh_mode,
            cache_path=self.cache_file,
            history_path=self.history_file,
            spot_path=None,
        )

        # 閸涙垝鑵戠紓鎾崇摠閻╁瓨甯存潻鏂挎礀
        if refresh_mode in ("none", "readonly") and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                LOG.error("[DS.FuturesBasis] load cache error: %s", exc)

        # 鐠囪褰囬懕姘値閺佺増宓?
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

        # 娣囨繂鐡ㄩ崢鍡楀蕉閸滃瞼绱︾€?
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
        閸氬牆鑻熼崢鍡楀蕉娑撳骸缍嬮崜宥囩崶閸欙綇绱濇穱婵婄槈闂€鍨閸ュ搫鐣炬稉?window閵?
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
        鐠侊紕鐣?10 閺冦儴绉奸崝鍨嫲 3 閺冦儱濮為柅鐔峰閿涘牆鐔€瀹割喖娼庨崐鐓庢▕閿涘鈧?
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
        鏉╂柨娲栫粚?娑擃厽鈧冩健閿涘本澧嶉張澶嬪瘹閺嶅洣璐?0.0閿涘eries 娑撹櫣鈹栭妴?
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

