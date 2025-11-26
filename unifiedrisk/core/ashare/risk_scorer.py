# risk_scorer v2
class RiskScorer:
    def score(self,payload):
        idx=payload.get("index_turnover",{})
        g=payload.get("global",{})
        t_s,t_d=self._turnover(idx)
        g_s,g_d=self._global(g)
        n_s,n_d=self._north(idx)
        l_s,l_d=self._liq(idx)
        total=t_s+g_s+n_s+l_s
        level=self._level(total)
        advice=self._adv(level)
        expl=self._expl(total,level,t_d,g_d,n_d,l_d)
        return {
            "turnover_score":t_s,"global_score":g_s,
            "north_score":n_s,"liquidity_score":l_s,
            "total_score":total,"risk_level":level,
            "advise":advice,"explanation":expl
        }

    def _turnover(self,idx):
        vals=[idx[k]["turnover"] for k in ["shanghai","shenzhen","chi_next"]
              if k in idx and "turnover" in idx[k]]
        if not vals: return 0,["成交额缺失"]
        total=sum(vals); d=[]
        s=0
        if total>7e10: s+=3; d.append("全市场放量")
        elif total>5e10: s+=1; d.append("成交额偏强")
        elif total<3e10: s-=2; d.append("明显缩量")
        else: d.append("成交额正常")
        return s,d

    def _global(self,g):
        s=0;d=[]
        nas=g.get("nasdaq",{}).get("change_pct",0)
        spy=g.get("spy",{}).get("change_pct",0)
        vix=g.get("vix",{}).get("last",0)
        if nas<-1: s-=2; d.append(f"纳指下跌{nas}%")
        if spy<-0.5: s-=1; d.append(f"SPY下跌{spy}%")
        if vix>22: s-=2; d.append(f"VIX={vix}")
        return s,d

    def _north(self,idx):
        cyb=idx.get("chi_next",{}).get("turnover",0)
        if cyb>3e9: return 1,["北向偏强"]
        if cyb<1e9: return -1,["北向偏弱"]
        return 0,["北向中性"]

    def _liq(self,idx):
        vol=idx.get("chi_next",{}).get("volume",0)
        if vol<3e8: return -2,["创业板流动性下降"]
        return 0,["流动性正常"]

    def _level(self,t):
        if t>=4: return "Low"
        if t>=0: return "Medium"
        if t>=-3: return "High"
        return "Extreme"

    def _adv(self,l):
        return {"Low":"加仓","Medium":"观察","High":"减仓","Extreme":"规避"}[l]

    def _expl(self,total,level,*ds):
        lines=[f"风险等级：{level}（{total}分）","","【因子解读】"]
        for sec in ds:
            lines+=["- "+x for x in sec]
        return "\n".join(lines)
