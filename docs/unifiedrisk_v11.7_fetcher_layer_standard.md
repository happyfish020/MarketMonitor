# UnifiedRisk V11.7
# Fetcher 层设计规范

版本：V11.7
更新时间：2025-12-06

目录（TOC）

Fetcher 的定位

Fetcher 的职责（最重要）

Snapshot 的创建规则

snapshot 中必须包含的字段

缓存（cache）规则

Fetcher 日志规范

Fetcher 模板（完整示例）

禁止行为

# 1. Fetcher 层定位

Fetcher 是整个系统中最关键的桥梁：

层级	内容
datasource → fetcher	原始数据来自多个数据源
fetcher → snapshot	fetcher 合并所有数据，写一个 JSON 文件
factor → snapshot	因子层只读 snapshot

Fetcher 是 唯一能写 snapshot 文件的模块。

# 2. Fetcher 的核心职责

Fetcher 必须做三件事：

✔ 2.1 调用各 datasource

如：

etf_proxy = EtfNorthProxy().fetch(...)
index_series = IndexSeriesClient().fetch(...)
global_lead = GlobalLeadClient().fetch(...)
margin = MarginClient().get_recent_series(...)
breadth = reader.get_breadth_summary()

✔ 2.2 整合 snapshot 数据结构

snapshot 必须有统一字段：

snapshot = {
    "meta": {...},
    "etf_proxy": etf_proxy,
    "turnover": turnover,
    "breadth": breadth,
    "margin": margin,
    "index_series": index_series,
    "global_lead": global_lead,
}

✔ 2.3 写入 snapshot 文件

即：

data/cache/day_cn/YYYYMMDD/ashare_daily_snapshot.json


以该文件为因子层的唯一输入。

# 3. Snapshot 的创建规则
✔ Snapshot 必须是一次性写入

不可 “分段更新”；不可 “多次写入”。

✔ Snapshot 名字必须统一

只能是：

ashare_daily_snapshot.json

✔ Snapshot 写入前必须创建目录结构

确保：

data/cache/day_cn/YYYYMMDD/


存在。

# 4. snapshot 必须包含的字段

以下字段必须存在：

字段	说明
meta	日期、版本等信息
etf_proxy	ETF 北向代理（来自 datasource）
turnover	沪深成交额
breadth	adv/dec 等宽度
margin	融资融券
index_series	上证/深证/HS300
global_lead	海外引导指数
# 5. 缓存（cache）规则

Fetcher 不写 symbolcache，理由：

symbolcache 是 datasource 的职责

snapshot 是 fetcher 的职责

Fetcher 只写 snapshot，一个文件。

# 6. Fetcher 日志规范

示例：

log("[Fetcher] Start building daily snapshot")
log("[Fetcher] ETF Proxy OK")
log("[Fetcher] GlobalLead OK")
log("[Fetcher] Writing snapshot → path")


禁止：

❌ logging.getLogger

❌ print

❌ 在 datasource 内写 snapshot

# 7. Fetcher 模板（完整示例）
class AshareFetcher:

    def get_daily_snapshot(self, trade_date, force_refresh=False):
        log(f"[Fetcher] Building snapshot → {trade_date}")

        reader = MarketDataReaderCN(trade_date)

        etf_proxy = EtfNorthProxy().fetch(trade_date, force_refresh)
        turnover = reader.get_turnover_summary()
        breadth = reader.get_breadth_summary()
        margin = EastmoneyMarginClientCN().get_recent_series(20)
        index_series = IndexSeriesClient().fetch(trade_date)
        global_lead = GlobalLeadClient().fetch(trade_date)

        snapshot = {
            "meta": {"trade_date": trade_date.isoformat()},
            "etf_proxy": etf_proxy,
            "turnover": turnover,
            "breadth": breadth,
            "margin": margin,
            "index_series": index_series,
            "global_lead": global_lead,
        }

        save_json(snapshot_path, snapshot)
        log(f"[Fetcher] Snapshot written → {snapshot_path}")

        return snapshot

# 8. 禁止行为（必须遵守）

❌ fetcher 写 symbolcache
❌ datasource 写 snapshot
❌ datasource 写自己的 json（global_lead.json 等）
❌ 使用 yfinance（必须在 datasource 内部使用 yf_client_cn）
❌ 多次写 snapshot
❌ snapshot 字段不规范（乱命名、不统一）
 