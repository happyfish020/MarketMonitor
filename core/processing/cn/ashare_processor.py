from typing import Dict, Any


class AshareProcessor:
    """A 股数据加工层：把 snapshot 统一转换为 features，供日级 / 盘中因子使用。"""

    # ===== 日级：北向 + 成交额 + 市场情绪 =====
    def build_from_daily(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        # 1) 北向 ETF 代理
        proxy = snapshot.get("etf_proxy", {}) or {}
        etf_flow_e9 = float(proxy.get("etf_flow_e9", 0.0) or 0.0)
        etf_turnover_e9 = float(proxy.get("total_turnover_e9", 0.0) or 0.0)
        hs300_pct = float(proxy.get("hs300_proxy_pct", 0.0) or 0.0)

        # 2) 成交额（来自 zh_spot 聚合）
        turnover = snapshot.get("turnover", {}) or {}
        sh_turnover_e9 = float(turnover.get("sh_turnover_e9", 0.0) or 0.0)
        sz_turnover_e9 = float(turnover.get("sz_turnover_e9", 0.0) or 0.0)
        total_turnover_e9 = float(turnover.get("total_turnover_e9", 0.0) or 0.0)

        # 3) 市场情绪（Breadth）
        breadth = snapshot.get("breadth", {}) or {}
        adv = int(breadth.get("adv", 0) or 0)
        dec = int(breadth.get("dec", 0) or 0)
        limit_up = int(breadth.get("limit_up", 0) or 0)
        limit_down = int(breadth.get("limit_down", 0) or 0)
        total_stocks = int(breadth.get("total", 0) or 0)

        return {
            "type": "daily",
            "raw": snapshot,
            "features": {
                # 北向 NPS 代理用
                "etf_flow_e9": etf_flow_e9,
                "etf_turnover_e9": etf_turnover_e9,
                "hs300_proxy_pct": hs300_pct,
                # 成交额因子用
                "sh_turnover_e9": sh_turnover_e9,
                "sz_turnover_e9": sz_turnover_e9,
                "total_turnover_e9": total_turnover_e9,
                # 市场情绪因子用
                "adv": adv,
                "dec": dec,
                "limit_up": limit_up,
                "limit_down": limit_down,
                "total_stocks": total_stocks,
            },
        }

    # ===== 盘中：指数涨跌等短线情绪 =====
    def build_from_intraday(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        index = snapshot.get("index", {}) or {}
        sh_change = float(index.get("sh_change", 0.0) or 0.0)
        cyb_change = float(index.get("cyb_change", 0.0) or 0.0)

        return {
            "type": "intraday",
            "raw": snapshot,
            "features": {
                "index_sh_change": sh_change,
                "index_cyb_change": cyb_change,
            },
        }