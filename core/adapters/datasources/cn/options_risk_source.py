# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - Options Risk DataSource (E Block)

鐠佹崘顓搁惄顔炬畱閿?
    閼辨艾鎮?ETF 閺堢喐娼堥弮銉攽閹懏鏆熼幑顕嗙礉鐠侊紕鐣婚崝鐘虫綀濞戙劏绌兼０婵勨偓浣光偓缁樺畾鐠哄矂顤傞妴浣稿閺夊啯鏁归惄妯圭幆娴犮儱寮烽崗璺哄綁閸栨牞绉奸崝瑁も偓?
    濮濄倖鏆熼幑顔界爱娑撶儤婀￠弶鍐棑闂勨晛鍨庨弸鎰絹娓氭稑鐔€绾偓閸樼喎顫愰弫鐗堝祦閿涘瞼鏁ゆ禍搴℃倵缂侇厼娲滅€涙劖澧﹂崚鍡楁嫲閹躲儱鎲＄仦鏇犮仛閵?

缁撅附娼敍?
    - 娴犲懍绶风挧?DBOracleProvider閿涘奔绗夌拋鍧楁６婢舵牠鍎?API閵?
    - 娴犲懓浠涢崥鍫滅缂佸嫬娴愮€规氨娈?ETF 閺堢喐娼堥弽鍥╂畱閿涘牅绡€閸欑嫟TF閿涘绱濋弽瑙勫祦闁板秶鐤嗛崣顖濈殶閺佹番鈧?
    - 閹稿妫╅弸鍕紦閺冨爼妫挎惔蹇撳灙閿涘矂绮拋銈呮礀濠?60 閺冦儯鈧?

鏉堟挸鍤€涙顔岄敍?
    trade_date: 閺堚偓閺傞姘﹂弰鎾存）閺堢噦绱欑€涙顑佹稉璇х礆
    weighted_change: 閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕畾鐠哄矂顤傞崸鍥р偓?
    total_change:    閹碘偓閺堝鎮庣痪锔藉畾鐠哄矂顤傚Ч鍌氭嫲
    total_volume:    閹粯鍨氭禍銈夊櫤
    weighted_close:  閹稿鍨氭禍銈夊櫤閸旂姵娼堥惃鍕暪閻╂ü鐜?
    change_ratio:    weighted_change / weighted_close閿涘牐瀚㈤弨鍓佹磸娴犺渹璐?0閿涘苯鍨稉?0 閹?None閿?
    trend_10d:       鏉?10 閺?weighted_change 閸欐ê瀵?
    acc_3d:         鏉?3 閺?weighted_change 閸欐ê瀵?
    series: 閸樺棗褰舵惔蹇撳灙閸掓銆冮敍灞剧槨妞ょ懓瀵橀崥?trade_date, weighted_change, total_change, total_volume,
            weighted_close, change_ratio

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

LOG = get_logger("DS.OptionsRisk")


class OptionsRiskDataSource(DataSourceBase):
    """
    Options Risk DataSource

    閼辨艾鎮?ETF 閺堢喐娼堥弮銉攽閹懏鏆熼幑顕嗙礉鐠侊紕鐣婚崝鐘虫綀濞戙劏绌兼０婵嗗挤閸忔儼绉奸崝?閸旂娀鈧喎瀹抽妴?
    """

    def __init__(self, config: DataSourceConfig, window: int = 60) -> None:
        # 閸ュ搫鐣鹃崥宥囆為敍灞肩┒娴滃孩妫╄箛妤勭槕閸?
        super().__init__(name="DS.OptionsRisk")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBMySQLMarketProvider()

        # 缂傛挸鐡ㄩ崪灞藉坊閸欒尪鐭惧?
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 閸楁洘妫?cache 閺傚洣娆㈤崥?
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
        閺嬪嫬缂撻張鐔告綀妞嬪酣娅撻崢鐔奉潗閺佺増宓侀崸妞尖偓?

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
                LOG.error("[DS.OptionsRisk] load cache error: %s", exc)

        # 鐠嬪啰鏁?DB provider 閼辨艾鎮庨弫鐗堝祦
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
            # 閺嶅洩顔囬弫鐗堝祦閻樿埖鈧椒璐?OK閿涘矁銆冮弰搴㈡殶閹诡喗娼靛┃鎰劀鐢?
            "data_status": "OK",
            # 姒涙顓婚弮?warnings閿涙稖瀚㈡稉濠傜湴闂団偓鐟曚礁褰茬憰鍡欐磰
            "warnings": [],
        }

        # 娣囨繂鐡ㄩ崢鍡楀蕉閸滃瞼绱︾€?
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
        鐠侊紕鐣?10 閺冦儴绉奸崝鍨嫲 3 閺冦儱濮為柅鐔峰閿涘牆鐔€娴?weighted_change閿涘鈧?
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
        鏉╂柨娲栫粚?娑擃厽鈧冩健閿涘本澧嶉張澶嬪瘹閺嶅洣璐?0.0閿涘eries 娑撹櫣鈹栭妴?

        濞夈劍鍓伴敍姘秼閺佺増宓佺紓鍝勩亼閹存牗妫ゅ▔鏇炲鏉炶姤妞傞敍宀勬付閺勫海鈥橀弽鍥ㄦ暈 data_status 娑?"MISSING"閵嗗倸顩ч弸婊呮阜閻ｃ儲顒濈€涙顔岄敍?
        娑撳﹤鐪伴崶鐘茬摍閸滃本濮ら崨濠佺窗姒涙顓荤拋銈勮礋閺佺増宓佸锝呯埗閿?OK"閿涘绱濇禒搴も偓宀€绮伴崙杞扮瑝閸戝棛鈥橀惃鍕絹缁€鎭掆偓鍌涱劃婢跺嫭鍨滄禒顒佹绾?
        鐠佸墽鐤?data_status 娑?"MISSING" 娴犮儰绌?WatchlistLeadFactor 閼宠姤顒滅涵顔跨槕閸掝偅鏆熼幑顔惧繁婢惰鲸鍎忛崘鐐光偓?
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
            # 閹绘劒绶垫稉鈧稉?warnings 鐎涙顔屾禒銉ょ┒娑撳﹤鐪伴棃銏℃緲鐠佹澘缍嶇紓鍝勩亼閸樼喎娲?
            "warnings": ["missing:options_risk_series"],
        }

