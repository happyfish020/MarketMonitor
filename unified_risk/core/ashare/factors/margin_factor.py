from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

import re
import json
import time
import random
import requests

from unified_risk.common.config_manager import CONFIG
from unified_risk.common.logger import get_logger
from unified_risk.core.cache.day_cache import DayCache

LOG = get_logger("UnifiedRisk.Factor.Margin")


@dataclass
class MarginSnapshot:
    trade_date: date
    rzye_e9: float          # 融资余额（亿元）
    rz_buy_e9: float        # 融资买入额（亿元）
    rz_change_5d: float     # 近5个交易日融资余额变化（%）
    score: int              # -2 ~ +2


class MarginFactor:
    """两融（日级）因子 v7.4.2，使用 EastMoney LSDB 接口：RPTA_RZRQ_LSDB。

    接口特点：
      - 支持海外访问；
      - JSONP 返回，需要去除 callback；
      - 字段包括 DIM_DATE, RZYE, RQYE, RZRQYE, RZMRE, RQYL 等；
      - 我们只关心融资余额 RZYE 与融资买入额 RZMRE。

    计算逻辑：
      - 针对 trade_date，获取最近若干页数据（按 DIM_DATE 降序）；
      - 过滤出 DIM_DATE <= trade_date 的记录，按日期升序排列；
      - 取最后一条作为“当日/最近交易日”；
      - 取第倒数第 6 条作为“5 个交易日前”，若不足 6 条则用第一条代替；
      - 计算 5 日融资余额变化幅度：
          rz_change_5d = (rzye_last / rzye_ref - 1) * 100
      - 根据变化幅度映射到 [-2,2]。
    """

    REPORT_NAME = "RPTA_RZRQ_LSDB"

    def __init__(self) -> None:
        cache_root = CONFIG.get_path("cache_dir") / "margin"
        self.cache = DayCache(cache_root)

    # ---------- HTTP 工具 ----------
    def _build_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/129.0.0.0 Safari/537.36"
            ),
            "Referer": "https://data.eastmoney.com/rzrq/",
            "Accept": "*/*",
        }

    def _build_url(self, page: int = 1) -> str:
        ts = int(time.time() * 1000)
        callback = f"datatable{ts % 1000000 + random.randint(0,9999)}"
        params = {
            "callback": callback,
            "reportName": self.REPORT_NAME,
            "columns": "ALL",
            "source": "WEB",
            "sortColumns": "DIM_DATE",
            "sortTypes": "-1",
            "pageNumber": page,
            "pageSize": 50,
            "_": ts,
        }
        from urllib.parse import urlencode
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urlencode(params)
        return url

    def _fetch_pages(self, max_pages: int = 5) -> List[Dict[str, Any]]:
        """获取最近若干页两融数据（按日期降序），合并为列表。"""
        headers = self._build_headers()
        all_rows: List[Dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            url = self._build_url(page)
            LOG.info(f"[Margin] LSDB fetch page {page}: {url}")
            try:
                resp = requests.get(url, headers=headers, timeout=15)
                resp.raise_for_status()
                text = resp.text
                # 解析 JSONP： callback(...json...)
                json_str = re.sub(r"^[^(]*\((.*)\)[^)]*$", r"\1", text)
                data = json.loads(json_str)
                result = data.get("result") or {}
                rows = result.get("data") or []
                if not rows:
                    LOG.info(f"[Margin] LSDB no data on page {page}, stop paging.")
                    break
                all_rows.extend(rows)
                pages_total = result.get("pages") or None
                LOG.info(f"[Margin] LSDB page {page} fetched: {len(rows)} rows, total_pages={pages_total}")
                # 如果当前返回记录少于 pageSize，说明已经到结尾
                if len(rows) < 50:
                    break
            except Exception as e:
                LOG.error(f"[Margin] LSDB page {page} error: {e}")
                break
        return all_rows

    def _extract_rzye(self, row: Dict[str, Any]) -> float:
        for key in ["RZYE", "rzye"]:
            if key in row and row[key] is not None:
                try:
                    return float(row[key])
                except Exception:
                    continue
        return 0.0

    def _extract_rzmre(self, row: Dict[str, Any]) -> float:
        for key in ["RZMRE", "rzmre", "RZ_MRJE", "RZMRJE"]:
            if key in row and row[key] is not None:
                try:
                    return float(row[key])
                except Exception:
                    continue
        return 0.0

    # ---------- 主计算 ----------
    def _compute_snapshot(self, d: date) -> MarginSnapshot:
        rows = self._fetch_pages(max_pages=5)
        if not rows:
            LOG.warning("[Margin] LSDB 未获取到任意两融数据，使用中性 0。")
            return MarginSnapshot(
                trade_date=d,
                rzye_e9=0.0,
                rz_buy_e9=0.0,
                rz_change_5d=0.0,
                score=0,
            )

        d_str = d.strftime("%Y-%m-%d")
        # DIM_DATE 一般形如 "2025-11-28 00:00:00" 或 "2025-11-28"
        def _get_date_str(row: Dict[str, Any]) -> str:
            ds = str(row.get("DIM_DATE", ""))[:10]
            return ds

        # 过滤出日期 <= trade_date 的记录
        rows_valid = [r for r in rows if _get_date_str(r) <= d_str]
        if not rows_valid:
            LOG.warning("[Margin] LSDB 历史数据中没有 <= trade_date 的记录，使用中性 0。")
            return MarginSnapshot(
                trade_date=d,
                rzye_e9=0.0,
                rz_buy_e9=0.0,
                rz_change_5d=0.0,
                score=0,
            )

        # 按日期升序排序
        rows_valid.sort(key=_get_date_str)
        last = rows_valid[-1]
        rzye_last = self._extract_rzye(last)
        rz_buy_last = self._extract_rzmre(last)

        # 5 个交易日前的记录：若足够，则用倒数第 6 条，否则用第一条
        if len(rows_valid) >= 6:
            ref = rows_valid[-6]
        else:
            ref = rows_valid[0]
        rzye_ref = self._extract_rzye(ref)

        if rzye_ref > 0:
            rz_change_5d = (rzye_last / rzye_ref - 1.0) * 100.0
        else:
            rz_change_5d = 0.0

        score = self._map_change_to_score(rz_change_5d)

        snap = MarginSnapshot(
            trade_date=d,
            rzye_e9=rzye_last / 1e8,
            rz_buy_e9=rz_buy_last / 1e8,
            rz_change_5d=rz_change_5d,
            score=score,
        )
        return snap

    @staticmethod
    def _map_change_to_score(chg: float) -> int:
        """根据 5 日融资余额变化幅度映射得分。"""
        if chg >= 8:
            return 2
        if chg >= 3:
            return 1
        if chg <= -8:
            return -2
        if chg <= -3:
            return -1
        return 0

    # ---------- 对外入口 ----------
    def compute_for_date(self, d: date) -> MarginSnapshot:
        cached_rzye = self.cache.get(d, "MARGIN_RZYE_E9")
        cached_rzbuy = self.cache.get(d, "MARGIN_RZBUY_E9")
        cached_chg5 = self.cache.get(d, "MARGIN_RZ_CHANGE5")
        cached_score = self.cache.get(d, "MARGIN_SCORE")
        if (
            cached_rzye is not None
            and cached_rzbuy is not None
            and cached_chg5 is not None
            and cached_score is not None
        ):
            return MarginSnapshot(
                trade_date=d,
                rzye_e9=float(cached_rzye),
                rz_buy_e9=float(cached_rzbuy),
                rz_change_5d=float(cached_chg5),
                score=int(cached_score),
            )

        snap = self._compute_snapshot(d)

        # 写入缓存
        self.cache.set(d, "MARGIN_RZYE_E9", snap.rzye_e9)
        self.cache.set(d, "MARGIN_RZBUY_E9", snap.rz_buy_e9)
        self.cache.set(d, "MARGIN_RZ_CHANGE5", snap.rz_change_5d)
        self.cache.set(d, "MARGIN_SCORE", snap.score)
        return snap

    def as_factor_dict(self, d: date) -> Dict[str, Any]:
        snap = self.compute_for_date(d)
        return {
            "margin_score": snap.score,
            "margin_rzye_e9": snap.rzye_e9,
            "margin_rz_change_5d": snap.rz_change_5d,
            "margin_rz_buy_e9": snap.rz_buy_e9,
        }
