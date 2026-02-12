# UnifiedRisk V12 · Top20口径统一修复（Hotfix v2）验证指南

## 目标
确保所有表达“Top20 成交集中度（全市场 Top20 个股成交额 / 全市场成交额）”的引用统一为：

- **canonical**：`liquidity_quality_raw.top20_ratio`（报告中应标注 `src=liquidity_quality.details.top20_ratio` 语义来源）

并且：
- `crowding_concentration.details.top20_amount_ratio` 仅作为**拥挤代理**（分母不同），不得再被当作“Top20成交占比”使用或触发 Gate 阈值。

---

## 1) Oracle SQL 校验（canonical top20_ratio）

> 口径：全市场当日成交额 TOP20 之和 / 全市场成交额之和

```sql
-- 以 SECOPR.CN_STOCK_DAILY_PRICE 为例（字段名按你的表为准：SYMBOL, TRADE_DATE, AMOUNT）
-- 注意：请确保 AMOUNT 单位一致（元/万元/亿元），比值不受单位影响。

WITH t AS (
  SELECT
    symbol,
    amount
  FROM SECOPR.CN_STOCK_DAILY_PRICE
  WHERE trade_date = TO_DATE(:trade_date, 'YYYY-MM-DD')
    AND amount IS NOT NULL
),
tot AS (
  SELECT SUM(amount) AS total_amount
  FROM t
),
top20 AS (
  SELECT SUM(amount) AS top20_amount
  FROM (
    SELECT amount
    FROM t
    ORDER BY amount DESC
    FETCH FIRST 20 ROWS ONLY
  )
)
SELECT
  top20.top20_amount / NULLIF(tot.total_amount, 0) AS top20_ratio
FROM top20, tot;
```

期望：
- 结果应与报告中的 `top20_ratio` 近似一致（小数 4 位以内差异可接受；取决于你在 DS 层是否做了过滤：ST/北交所/异常股等）。

---

## 2) 报告字段对照（检查修复是否生效）

生成 `eod_YYYY-MM-DD.md` 后检查：

### A. MarketOverview（大盘概述）
应出现类似（canonical）：
- `Top20 成交占比 X.X%（3D: ...）（src=liquidity_quality.details.top20_ratio）`

并且不应再出现（proxy/误用）：
- `拥挤代理(top20pct)=...（src=crowding_concentration...）`
- `suspect:top20_amount_ratio_high_from:...`

### B. GateDecision / GlobalLead / T+2 Lead
- 当 `liquidity_quality_raw.top20_ratio` 可用时：
  - 不应再把 `top20_amount_ratio≈0.72` 当作“Top20成交占比”或作为 Gate 触发依据
  - 不应再出现 `suspect:top20_amount_ratio_high_from:crowding_concentration=...`
- 仅在 `liquidity_quality_raw` 缺失时，允许出现 warning：
  - `suspect:top20_amount_ratio_high_from:crowding_concentration=...`
  - 且必须明确“**不用于 Gate 阈值触发**”。

### C. WatchlistLead（领先结构）
- 顶部 warnings 不应再包含 `suspect:top20_amount_ratio_high_from:crowding_concentration=...`（在 liquidity_quality_raw 存在时）
- F 面板继续展示：
  - `top20=...` 与 `top20(3D)=...`

---

## 3) 为什么会出现 0.72（解释）
- `crowding_concentration.details.top20_amount_ratio` 的分母**不是全市场成交额**（常见是“Top20成交额 / TopN成交额”或“Top20在某个子样本中的占比”），因此数值可能达到 0.7+。
- canonical `top20_ratio` 的分母是**全市场成交额**，因此常见范围约 0.06~0.20（随市场结构变化波动）。

---

## 4) 回放/测试建议
- 用同一天（如 2026-01-23）生成报告，对照三处一致性：
  1) Oracle SQL 的 `top20_ratio`
  2) 报告 MarketOverview 的 `Top20 成交占比`
  3) WatchlistLead F 面板的 `top20` 与 `top20(3D)`

满足以上条件，即验证通过。
