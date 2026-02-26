# -*- coding: utf-8 -*-
"""
UnifiedRisk V12 - ETF Flow DataSource (C Block)

鐠佹崘顓搁惄顔炬畱閿?
    娴犲孩婀伴崷?Oracle 閺佺増宓佹惔鎾舵畱閸╂椽鍣?ETF 閺冦儴顢戦幆鍛般€冮敍鍦昇_FUND_ETF_HIST_EM閿?
    閼辨艾鎮庣拋锛勭暬 ETF 娴犱粙顤傞崣妯哄娴狅絿鎮婇幐鍥ㄧ垼閿涘本褰佹笟娑樺斧婵鐛ラ崣锝呯碍閸掓鎷扮搾瀣◢閹稿洦鐖ｉ妴?

缁撅附娼敍?
    - 娴犲懍绶风挧?DBOracleProvider閿涘奔绗夌拋鍧楁６婢舵牠鍎?API
    - 娑撳秴鐣炬稊澶嬫煀閻?provider 閹恒儱褰涢敍宀€娲块幒銉ㄧ殶閻?provider 鐏炲倹褰佹笟娑氭畱閼辨艾鎮庨弬瑙勭《
    - 閹稿妫╅弸鍕紦閺冨爼妫挎惔蹇撳灙閿涘瘍indow 姒涙顓?60 婢?

鏉堟挸鍤€涙顔岄敍?
    trade_date: 娴溿倖妲楅弮銉︽埂閿涘牊娓堕弬棰佺娑擃亙姘﹂弰鎾存）鐎涙顑佹稉璇х礆
    total_change_amount: 瑜版挻妫╅幍鈧張?ETF price change 娑斿鎷?
    total_volume: 瑜版挻妫?ETF 閹存劒姘﹂柌蹇庣閸?
    total_amount: 瑜版挻妫?ETF 閹存劒姘︽０婵呯閸?
    flow_ratio: 瑜版挻妫╂禒閿嬬壐濞戙劏绌兼０婵呯瑢閹存劒姘﹂柌蹇曟畱濮ｆ柨鈧》绱檖roxy閿?
    trend_10d: 10 閺冦儳鐤拋鈥冲綁閸栨牭绱欓幀?price change閿?
    acc_3d: 3 閺冦儳鐤拋鈥冲綁閸栨牭绱欓幀?price change閿?
    series: 娴犲孩妫崚鐗堟煀閻ㄥ嫬宸婚崣鎻掔碍閸掓鍨悰顭掔礉濮ｅ繘銆嶉崠鍛儓 trade_date閵嗕辜otal_change_amount閵嗕辜otal_volume閵嗕辜otal_amount

瑜版挻鏆熼幑顔惧繁婢惰鲸鍨ㄥ鍌氱埗閺冭绱濇潻鏂挎礀 neutral_block
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

LOG = get_logger("DS.ETFFlow")


class ETFFlowDataSource(DataSourceBase):
    """
    ETF Flow DataSource

    閼辨艾鎮?ETF 閺冦儴顢戦幆鍛般€冮惃?price change / volume / amount 閺佺増宓侀敍?
    闁俺绻?10 婢垛晛鎷?3 婢垛晝鐤粔顖氣偓鍏煎絹娓氭稖绉奸崝鍨嫲閸旂娀鈧喎瀹虫穱鈩冧紖閵?
    """

    def __init__(self, config: DataSourceConfig, window: int = 60):
        # 娴ｈ法鏁ら崶鍝勭暰閸氬秶袨閿涘奔绌舵禍搴㈡）韫囨鐦戦崚?
        super().__init__(name="DS.ETFFlow")
        self.config = config
        self.window = int(window) if window and window > 0 else 60
        self.db = DBMySQLMarketProvider()

        # cache 閸?history 鐠侯垰绶?
        self.cache_root = config.cache_root
        self.history_root = config.history_root
        os.makedirs(self.cache_root, exist_ok=True)
        os.makedirs(self.history_root, exist_ok=True)

        # 閸楁洘妫?cache 缂佺喍绔撮崨钘夋倳閿涘矂浼╅崗宥勫▏閻?trade_date 娴ｆ粈璐熼弬鍥︽閸?
        self.cache_file = os.path.join(self.cache_root, "etf_flow_today.json")
        # 閹镐椒绠欓崠鏍у坊閸欐彃绨崚?
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
        娑撹鍙嗛崣锝忕窗閺嬪嫬缂?ETF flow 閸樼喎顫愰弫鐗堝祦閸фぜ鈧?

        閸欏倹鏆熼敍?
            trade_date: 鐎涙顑佹稉璇х礉鐠囧嫪鍙婇弮銉︽埂閿涘牓鈧艾鐖舵稉?T 閹?T-1閿?
            refresh_mode: 閸掗攱鏌婄粵鏍殣閿涘本鏁幐?none/readonly/full
        """
        # 閹?refresh_mode 濞撳懐鎮婄紓鎾崇摠閺傚洣娆?
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
                LOG.error("[DS.ETFFlow] load cache error: %s", exc)

        # 鐠囪褰囬懕姘値閺佺増宓?
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

        # 绾喕绻氶張澶婄碍閿涙碍瀵滈弮銉︽埂閸楀洤绨敍鍫熸＋閳帗鏌婇敍?
        df_sorted = df.sort_index(ascending=True)

        # 鐏?DataFrame 鏉烆兛璐熼崚妤勩€?[{trade_date, total_change_amount, ...}]
        series: List[Dict[str, Any]] = []
        for idx, row in df_sorted.iterrows():
            series.append({
                "trade_date": idx.strftime("%Y-%m-%d"),
                "total_change_amount": float(row["total_change_amount"]) if pd.notna(row["total_change_amount"]) else 0.0,
                "total_volume": float(row["total_volume"]) if pd.notna(row["total_volume"]) else 0.0,
                "total_amount": float(row["total_amount"]) if pd.notna(row["total_amount"]) else 0.0,
            })

        # 閸氬牆鑻熼崢鍡楀蕉閿涘牅绻氱拠浣圭拨缁愭鏆辨惔锕€娴愮€规熬绱濋崥鎴濇倵鐞涖儵缍堥敍?
        merged_series = self._merge_history(series)

        # 鐠侊紕鐣荤搾瀣◢/閸旂娀鈧喎瀹?
        trend_10d, acc_3d = self._calc_trend(merged_series)

        # 閺堚偓閺傛媽顔囪ぐ?
        latest = merged_series[-1] if merged_series else None
        if latest is None:
            LOG.warning("[DS.ETFFlow] merged_series empty")
            return self._neutral_block(trade_date)

        latest_date = latest.get("trade_date")
        total_change_amount = latest.get("total_change_amount")
        total_volume = latest.get("total_volume")
        total_amount = latest.get("total_amount")
        # 濮ｆ柨鈧》绱伴柆鍨帳闂勩倝娴?
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

        # 娣囨繂鐡ㄩ崚鏉垮坊閸欐彃鎷扮紓鎾崇摠
        try:
            # 閹镐椒绠欓崠鏍у坊閸?
            self._save(self.history_file, merged_series)
            # 缂傛挸鐡ㄨぐ鎾炽亯閸?
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(block, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            LOG.error("[DS.ETFFlow] save error: %s", exc)

        return block

    # ------------------------------------------------------------------
    def _merge_history(self, recent: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        閸氬牆鑻熼崢鍡楀蕉鎼村繐鍨妴?

        recent: 瑜版挸澧犻弻銉嚄缁愭褰涢崘鍛畱閸掓銆冮敍鍫濆磳鎼村骏绱氶妴?
        history_file 娑擃厺绻氶悾娆愭纯娑斿懓绻欓惃鍕坊閸欒尪顔囪ぐ鏇礉娑?recent 閸氬牆鑻熼崥搴㈠焻閸?window 闂€鍨閵?
        """
        old = []
        if os.path.exists(self.history_file):
            try:
                old = self._load(self.history_file)
            except Exception:
                old = []
        # 閺嬪嫬缂撶€涙鍚€娴犮儲妫╅張鐔峰箵闁?
        buf: Dict[str, Dict[str, Any]] = {r["trade_date"]: r for r in old}
        for r in recent:
            buf[r["trade_date"]] = r
        out = sorted(buf.values(), key=lambda x: x["trade_date"])
        return out[-self.window:]

    # ------------------------------------------------------------------
    def _calc_trend(self, series: List[Dict[str, Any]]) -> tuple[float, float]:
        """
        鐠侊紕鐣?10 婢垛晞绉奸崝鍨嫲 3 婢垛晛濮為柅鐔峰閵?
        trend_10d = last.total_change_amount - total_change_amount[-11]
        acc_3d   = last.total_change_amount - total_change_amount[-4]
        閼汇儵鏆辨惔锔跨瑝婢剁噦绱濋崚娆掔箲閸?0.0
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
        鏉╂柨娲栫粚?娑擃厽鈧冩健閵?
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

