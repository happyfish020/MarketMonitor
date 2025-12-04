class AshareProcessor:
    def build_from_daily(self, snapshot):
        """
        将日级 snapshot 统一转换为因子可用的 processed 结构：
        {
            "raw": { ... },
            "features": { ... }
        }
        """
        df = snapshot.get("zh_spot")

        # ---- 个股层面的基础列表（当前因子暂未使用，但保留结构） ----
        features = {
            "pct": df["pct"].tolist() if df is not None else [],
            "volume": df["volume"].tolist() if df is not None else [],
            "amount": df["amount"].tolist() if df is not None else [],
            "price": df["price"].tolist() if df is not None else [],
            "pre_close": df["pre_close"].tolist() if df is not None else [],
            "change": df["change"].tolist() if df is not None else [],
        }

        # ---- 基于宽基 ETF 的北向代理（etf_proxy） ----
        etf_proxy = snapshot.get("etf_proxy") or {}
        try:
            features["net_etf_flow"] = float(etf_proxy.get("net_etf_flow") or 0.0)
        except Exception:
            features["net_etf_flow"] = 0.0

        try:
            features["turnover_etf"] = float(etf_proxy.get("turnover_etf") or 0.0)
        except Exception:
            features["turnover_etf"] = 0.0

        try:
            features["hs300_pct"] = float(etf_proxy.get("hs300_pct") or 0.0)
        except Exception:
            features["hs300_pct"] = 0.0

        # ---- 成交额聚合（turnover） ----
        turnover = snapshot.get("turnover") or {}
        try:
            features["turnover_sh"] = float(turnover.get("turnover_sh") or 0.0)
            features["turnover_sz"] = float(turnover.get("turnover_sz") or 0.0)
            features["turnover_total"] = float(turnover.get("turnover_total") or 0.0)
        except Exception:
            features.setdefault("turnover_sh", 0.0)
            features.setdefault("turnover_sz", 0.0)
            features.setdefault("turnover_total", 0.0)

        # ---- 市场宽度（breadth） ----
        breadth = snapshot.get("breadth") or {}
        try:
            features["adv"] = int(breadth.get("adv") or 0)
            features["dec"] = int(breadth.get("dec") or 0)
            features["limit_up"] = int(breadth.get("limit_up") or 0)
            features["limit_down"] = int(breadth.get("limit_down") or 0)
            features["total"] = int(breadth.get("total") or 0)
        except Exception:
            features.setdefault("adv", 0)
            features.setdefault("dec", 0)
            features.setdefault("limit_up", 0)
            features.setdefault("limit_down", 0)
            features.setdefault("total", 0)

        raw = {
            "trade_date": snapshot.get("trade_date"),
            "debug_flag": snapshot.get("debug_flag"),
            "meta": snapshot.get("meta") or {},
        }

        return {
            "raw": raw,
            "features": features,
        }
