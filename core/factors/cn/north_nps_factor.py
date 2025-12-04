
from datetime import date as Date, timedelta
from typing import Dict, Any, List, Tuple
from core.models.factor_result import FactorResult
from core.utils.logger import log
from core.adapters.cache.file_cache import load_json
from core.adapters.fetchers.cn.ashare_fetcher import get_daily_cache_path
from core.utils.trade_calendar import is_trading_day

class NorthNPSFactor:
    name="north_nps"

    @staticmethod
    def _safe_float(v): 
        try: return float(v or 0.0)
        except: return 0.0

    def _calc_strength(self, flow, turnover, hs300_pct):
        return 0.5*flow + 0.3*turnover + 0.2*(hs300_pct*turnover)

    def _load_strength(self, d:Date):
        path=get_daily_cache_path(d)
        snap=load_json(path)
        if not snap: return False,0.0
        p=snap.get("etf_proxy",{})
        f=self._safe_float(p.get("net_etf_flow"))
        t=self._safe_float(p.get("turnover_etf"))
        hs=self._safe_float(p.get("hs300_pct"))
        return True, self._calc_strength(f,t,hs)

    def _series(self, trade_date:Date, today_strength:float):
        s=[(trade_date,today_strength)]
        d=trade_date
        while len(s)<3:
            d=d-timedelta(days=1)
            if not is_trading_day(d): continue
            ok,val=self._load_strength(d)
            if not ok: break
            s.append((d,val))
        return s

    @staticmethod
    def _band(sc):
        if sc>=80: return "强"
        if sc>=60: return "偏强"
        if sc>=40: return "中性"
        if sc>=20: return "偏弱"
        return "极弱"

    def compute_from_daily(self, processed):
        raw = processed.get("raw", {}) or {}
        f = processed.get("features", {}) or {}

        td = raw.get("trade_date")
        if isinstance(td, Date):
            trade_date = td
        elif isinstance(td, str):
            try:
                trade_date = Date.fromisoformat(td)
            except Exception:
                trade_date = Date.today()
        else:
            trade_date = Date.today()
        flow=self._safe_float(f.get("net_etf_flow"))
        turnover=self._safe_float(f.get("turnover_etf"))
        hs=self._safe_float(f.get("hs300_pct"))

        strength_today=self._calc_strength(flow,turnover,hs)
        series=self._series(trade_date,strength_today)
        vals=[v for _,v in series]
        ma1=vals[0]
        ma3=sum(vals)/len(vals)
        trend=ma3-ma1

        strength_norm=max(-100,min(100, strength_today/6.0))
        trend_norm=max(-40,min(40, trend/3.0))

        raw_score=strength_norm + 0.8*trend_norm
        raw_score=max(-100,min(100, raw_score))
        factor_score=(raw_score+100)/2

        band=self._band(factor_score)
        trend_txt="趋势持平"
        if trend>0: trend_txt="3日趋势向上"
        elif trend<0: trend_txt="3日趋势向下"

        signal=f"北向资金：{band}（{band}，{trend_txt}，score={factor_score:.1f}）"

        return FactorResult(
            name=self.name,
            score=factor_score,
            signal=signal,
            raw={
                "trade_date":trade_date.isoformat(),
                "strength_today":strength_today,
                "series":[{"date":d.isoformat(),"strength":v} for d,v in series],
                "ma1":ma1,"ma3":ma3,"trend":trend,
                "strength_norm":strength_norm,
                "trend_norm":trend_norm,
                "raw_score":raw_score,
                "factor_score":factor_score,
                "band":band,
                "debug_flag":raw.get("debug_flag")
            }
        )
