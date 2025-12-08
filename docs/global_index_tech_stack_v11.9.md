全球指数 & GlobalLead 技术栈规范（含 120 日历史）

> 适用范围：A 股日度引擎（cn/ashare_daily_engine）及所有使用  
> `global_lead` / `index_series` / `index_tech` / `index_global` / `nps` 等因子的模块。

---

## 1. 设计目标

本规范定义 UnifiedRisk V11.9 中，**全球指数 & 宏观指数相关的数据流与接口标准**，包括：

- 数据源层（DataSource / Client）
- 缓存 & FORCE 刷新语义
- Snapshot 结构（含 120 日历史）
- 因子层接口规范（GlobalLeadFactor / IndexGlobalFactor / IndexTechFactor 等）
- 扩展方法（如何新增标的 / 新因子）

### 核心原则

1. **所有外部数据（YF / EastMoney / 等）必须先进入本地缓存，再被因子使用。**  
2. **Fetcher / Client 层统一做缓存与 FORCE 控制；Factor 层不直接访问网络。**  
3. **Snapshot 是 Engine 与 Factor 之间唯一的数据桥接层。**  
4. **日快照（T）与历史序列（T-0…T-\(window-1\)）都走统一接口。**  
5. **扩展时优先修改 Client，而不是 Factor，保持因子松耦合。**

---

## 2. 数据源基础：`yf_client_cn.py`

### 2.1 已有的日级接口

文件：`core/adapters/datasources/cn/yf_client_cn.py` 

- `get_etf_daily(symbol: str, trade_date: date) -> Optional[Dict[str, Any]]`  
  - market = `"cn"`, kind = `"etf"`
  - 尝试 `load_symbol_daily("cn", trade_date, symbol, kind="etf")`
  - 缓存 miss 时调用 `yfinance.download()`（默认窗口：T-15 ~ T+1）
  - 从 df 中选择 **精确 T** 或最近 <T 的交易日 fallback
  - 计算：
    - `close`
    - `prev_close`
    - `volume`
    - `pct_change`（百分比，×100）  
  - 写入缓存：`save_symbol_daily("cn", trade_date, symbol, kind="etf", data=data)`

- `get_macro_daily(symbol: str, trade_date: date) -> Optional[Dict[str, Any]]`  
  - market = `"global"`, kind = `"macro"`  
  - 与 `get_etf_daily` 逻辑一致，但带 retry/timeout，并返回：
    - `close`
    - `prev_close`
    - `pct_change`（**注意：这里是比例，不是百分比**，后续因子要统一处理）

- `get_index_daily(symbol: str, trade_date: date) -> Optional[Dict[str, Any]]`  
  - market = `"cn"`, kind = `"index"`  
  - 同样通过 `yfinance` 获取数据，并统一写入 cn/index 缓存。  

> **约定**：  
> - 所有日级数据都通过 `get_*_daily()` 进入 `symbol_cache`，并且所有上层只通过这些函数拿数据。  
> - 不允许在 Factor / Engine 直接调用 `yfinance`。

### 2.2 历史序列接口（V11.9 设计）

为了支撑技术类因子（如 IndexTechFactor），在 `yf_client_cn.py` 中增设：

- `get_macro_history_series(symbol: str, trade_date: date, window: int = 120) -> Optional[Dict[str, Any]]`
- `get_index_history_series(symbol: str, trade_date: date, window: int = 120) -> Optional[Dict[str, Any]]`

#### 设计要点

1. **只通过 `get_macro_daily` / `get_index_daily` 获取数据**，不直接打 YF。
2. 从 `trade_date` 向前逐日扫描，最多回溯 `window * 3` 天：
   - 若当天有 snapshot，加入 `history` 列表；
   - 若无 snapshot，则 `get_*_daily` 会自动触发 YF 下载并写缓存（首次补齐）。
3. 收集到 `window` 条记录后停止；若不足则在可用数据范围内返回实际长度。
4. 按 `date` 升序排序，返回结构：

```python
{
    "symbol": symbol,
    "end_date": trade_date.isoformat(),
    "window": window,
    "history": [
        {
            "symbol": ...,
            "date": "YYYY-MM-DD",
            "close": ...,
            "prev_close": ...,
            "volume": ...,
            "pct_change": ...,
        },
        ...
    ]
}
首次运行就会自动补齐 120 日历史（通过回溯调用 daily 接口），不依赖系统运行 120 天。

3. GlobalLeadClient（全球宏观引导指标）
文件：core/adapters/datasources/global/global_lead_client.py

3.1 作用
为 GlobalLead 因子提供 美债 / 美元 / 纳指 等的统一 access，并实现：

FORCE 删除缓存

每 Symbol 仅一次 FORCE

面向 Engine 的 OOP 封装

3.2 日级接口：get_global_lead
python
Copy code
def get_global_lead(
    symbol: str,
    trade_date: date,
    force_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    - kind="lead" 的 global 日级数据
    - FORCE: 删除当日该 symbol 的 lead 缓存 JSON（仅一次）
    - 实际数据由 get_macro_daily(symbol, trade_date) 提供
    """
缓存路径示例：

text
Copy code
data/cache/day_global/20251205/lead_^TNX.json
3.3 OOP 包装：GlobalLeadClient
python
Copy code
class GlobalLeadClient:
    SYMBOLS = ["^TNX", "^FVX", "DX-Y.NYB", "^IXIC"]

    def fetch(
        self,
        trade_date: date,
        force_refresh: bool = False,
        history_window: int = 0,  # V11.9 扩展
    ) -> Dict[str, Any]:
        ...
返回结构（V11.9）
历史窗口 = 0（默认），保持兼容：

python
Copy code
{
    "^TNX": { ... snapshot ... },
    "^FVX": { ... },
    ...
}
历史窗口 > 0：附带 120 日 history

python
Copy code
{
    "^TNX": {
        "last": { ... snapshot ... },
        "history": [ ... 120 日 ... ]
    },
    "^FVX": {
        "last": { ... },
        "history": [ ... ]
    },
    ...
}
4. IndexSeriesClient（A 股指数序列）
文件：core/adapters/datasources/global/index_series_client.py

4.1 作用
为 A 股核心指数提供统一接口（上证 / 深证 / 创业板 / 沪深300）：

python
Copy code
class IndexSeriesClient:

    SYMBOLS = {
        "sh": "000001.SS",
        "sz": "399001.SZ",
        "cyb": "399006.SZ",
        "hs300": "000300.SS",
    }

    def fetch(
        self,
        trade_date: Date,
        force_refresh: bool = False,
        history_window: int = 0,  # V11.9 扩展
    ) -> Dict[str, Any]:
        ...
4.2 返回结构
历史窗口 = 0：

python
Copy code
{
    "sh":    { ... snapshot ... },
    "sz":    { ... },
    "cyb":   { ... },
    "hs300": { ... },
}
历史窗口 > 0：

python
Copy code
{
    "sh": {
        "last":    { ... snapshot ... },
        "history": [ ... 120 日 ... ],
    },
    "sz": {
        "last":    { ... },
        "history": [ ... ],
    },
    ...
}
snapshot 的内容即为 get_index_daily(symbol, trade_date) 的返回。

5. Snapshot 结构规范（与 Engine 对接）
5.1 原有字段（示意）
在 cn/ashare_daily_engine 中构建的 daily_snapshot 一般包含：

python
Copy code
daily_snapshot = {
    "meta": {...},
    "etf_proxy": {...},        # etf_north_proxy
    "turnover": {...},
    "breadth": {...},
    "margin": {...},
    "index_series": {...},     # A 股指数当日数据
    "global_lead": {...},      # GlobalLead 当日数据
    "index_global": {...},     # A50/VIX/SPX 当日数据
    ...
}
其中：

etf_proxy 由 get_etf_north_proxy 提供，内部使用 get_etf_daily。

index_series 由 IndexSeriesClient.fetch(trade_date) 提供。

global_lead 由 GlobalLeadClient.fetch(trade_date) 提供。

5.2 V11.9 新增：历史序列视图（推荐）
为了支持 IndexTechFactor 等技术指标因子，可在 snapshot 中增加只读视图，例如：

python
Copy code
daily_snapshot = {
    ...
    "index_series": index_series_block,               # 日快照视图（兼容旧因子）
    "index_series_history": index_series_history,     # 120 日历史视图（可选）

    "global_lead": global_lead_block,                 # 日快照
    "global_lead_history": global_lead_history,       # 120 日历史
    ...
}
index_series_history 可以直接复用 IndexSeriesClient.fetch(history_window=120) 的结果；

global_lead_history 同理来自 GlobalLeadClient.fetch(history_window=120)。

推荐做法：
Engine 内部并不强制将 history 写入 snapshot。也可以在 Factor 内直接调用 client 的 fetch(..., history_window=120)，以减少 snapshot 体积。两种方式都符合架构原则，关键点是：

Factor 不直接访问 yfinance / 外部 API，只访问 Client 或 Snapshot。

6. 因子接口规范（GlobalLead / IndexGlobal / IndexTech）
6.1 Factor 基类
所有因子继承自统一基类，如：

python
Copy code
class BaseFactor:
    name: str
    def compute(self, snapshot: Dict[str, Any]) -> FactorResult: ...
FactorResult 的基本结构：

python
Copy code
@dataclass
class FactorResult:
    name: str
    score: float         # 0~100
    level: str           # "偏多" / "中性" / "偏空" 等
    details: Dict[str, Any]
    factor_obj: BaseFactor
6.2 GlobalLeadFactor
输入：snapshot["global_lead"] 或 GlobalLeadClient.fetch(trade_date)。

使用的 Symbol：["^TNX", "^FVX", "DX-Y.NYB", "^IXIC"]。

使用字段：

pct_change（注意有的是百分比，有的是比例，Factor 内部要标准化）

典型逻辑：

利率上行 / 美元走强 / 纳指走弱 → “外部宏观偏空”

综合得分 → 0~100，并定义 level。

6.3 IndexGlobalFactor（海外指数强弱：A50 夜盘 / VIX / SPX）
输入：snapshot["index_global"]

结构示例：

python
Copy code
"index_global": {
    "a50_future": { "last":..., "pct_change":... },
    "vix":        { "last":..., "pct_change":... },
    "spx":        { "last":..., "pct_change":... },
}
逻辑：

A50 夜盘涨跌 → 次日 A 股情绪预判

VIX 涨跌 → 风险偏好

SPX 涨跌 → 全球股市 beta

输出：FactorResult(name="index_global", score=..., level=..., details=...)

6.4 IndexTechFactor（技术类因子，V11.9 新增）
目标：
对 A 股核心指数（sh/sz/cyb/hs300）及若干全球指数（如 SPX/NDX 等）进行：

MA5/10/20/60 趋势判断

动能（momentum）

加速度（acceleration）

多周期共振

强弱区间划分

推荐输入：

A 股：IndexSeriesClient.fetch(trade_date, history_window=120)

Global：GlobalLeadClient.fetch(trade_date, history_window=120) 或单独的 GlobalIndexClient。

推荐输出：

python
Copy code
FactorResult(
    name="index_tech",
    score=final_score,
    level="偏弱 / 中性 / 偏强",
    details={
        "global_signal": "...",
        "global_breadth": "0.33",
        "indexes": {
            "hs300": "72.5（长期多头 + 动能增强）",
            "sh":    "48.0（震荡）",
            ...
        }
    },
    factor_obj=self,
)
7. 缓存与 FORCE 刷新规范
7.1 日级缓存：symbol_cache
所有日快照统一通过：

python
Copy code
load_symbol_daily(market, trade_date, symbol, kind)
save_symbol_daily(market, trade_date, symbol, kind, data)
常见 kind：

"etf"

"index"

"macro"

"lead"（global_lead_client）

7.2 FORCE 刷新语义
在单次进程运行中，每个 Symbol 仅允许执行一次物理删除，由 Client 层控制。

示例：GlobalLeadClient：

python
Copy code
_GLOBAL_LEAD_REFRESHED: Dict[str, bool] = {}

if force_refresh and not _GLOBAL_LEAD_REFRESHED.get(symbol):
    _force_delete_lead_cache(trade_date, symbol)
    _GLOBAL_LEAD_REFRESHED[symbol] = True
IndexSeriesClient 同理有 _INDEX_REFRESHED。

7.3 历史序列函数与 FORCE 的关系
get_macro_history_series / get_index_history_series 不直接删除缓存，只调用 get_*_daily。

真正的 FORCE 行为仍然由 daily 层的 Client (GlobalLeadClient / IndexSeriesClient) 控制。

这样保证：

FORCE 逻辑只存在一处；

所有历史序列构建都在“已完成 FORCE 后的缓存视图”上进行。

8. 扩展指南
8.1 新增一个 Global 指标（例如 JPY Index 或 BTC）
在 GlobalLeadClient.SYMBOLS 中添加 YF 符号；

确保 get_macro_daily(new_symbol, trade_date) 能正常工作（YF 上有此标的）；

重跑 Engine，一次性补齐 120 日历史；

在因子层（如 GlobalLeadFactor / CIBRFactor / YenFactor）中使用：

snapshot["global_lead"][new_symbol]（单日），或

GlobalLeadClient.fetch(..., history_window=120)[new_symbol]["history"]（历史）。

8.2 新增一个 A 股指数（如 中证500 指数）
在 IndexSeriesClient.SYMBOLS 中增加映射：

"zz500": "<YF symbol>";

get_index_daily(<YF symbol>, trade_date) 自动处理日数据与缓存；

需要历史时，使用 IndexSeriesClient.fetch(history_window=120)；

在因子层扩展 IndexTechFactor 对 "zz500" 的评分逻辑。

8.3 新增一个技术类因子（例：GlobalTrendFactor）
新建文件 core/factors/glo/global_trend_factor.py；

在因子中：

使用 GlobalLeadClient.fetch(trade_date, history_window=120)；

从 history 中计算 MA / 动能 / 加速度；

返回 FactorResult，并在 Engine 中注册该因子；

如需进入统一评分 / 预测，在 score_unified.py / prediction_engine.py 中为该因子配置权重。

9. 总结
yf_client_cn 负责 日级数据 的获取与缓存，是所有外部数据的唯一入口。

GlobalLeadClient / IndexSeriesClient 负责 将日级数据组织为因子可用的结构，并统一管理 FORCE 刷新。

历史序列（120 日）由 *_history_series 工具函数通过多次 get_*_daily 调用自动补齐，不依赖系统运行时长。

Snapshot 是 Engine 与 Factor 的桥梁，可以选择：

将 history 嵌入 snapshot；或

在 Factor 内直接通过 Client 获取 history。

所有因子必须基于 snapshot 或 Client，不得直接访问外部 API。

这套 V11.9 规范为后续以下模块提供统一基础能力：

IndexTechFactor（指数技术结构）

GlobalLeadFactor（宏观利率 / 美元 / 成长性）

IndexGlobalFactor（A50 夜盘 / VIX / SPX）

YenFactor（日元因子）

CIBRFactor（Crypto Bubble Risk）

更复杂的板块轮动 / 多周期趋势因子

只要遵守本规范，未来扩展新的指数 / 因子只需要少量配置 + 轻量级逻辑即可完成，无需再改底层。