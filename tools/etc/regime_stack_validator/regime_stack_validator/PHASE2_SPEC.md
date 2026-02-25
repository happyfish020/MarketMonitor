Phase-2 RegimeStack Validator 规范文档

（UnifiedRisk V12 / tools/regime_stack_validator）

0. 本文目的

Phase-2 的目标不是“预测涨跌”，而是建立一套可审计、可回放、可复现的市场风险制度：
将市场风险从“主观判断”变成“结构化信号 → 行为约束（Gate）→ 自动体检（Health）”。

本文记录：

需求来源与范围边界（Phase-2 只做制度验证）

关键名词定义（统一口径）

分层设计与解耦规则（V12 铁律延伸）

Phase-2 实测过程与问题闭环（FAIL → HEALTHY）

Phase-2 冻结规则（后续升级不得破坏）

1. Phase-2 需求与范围
1.1 需求背景（从 sector_rotation 的矛盾回到制度）

在做 sector_rotation 时暴露出典型矛盾：

Snapshot/BlockBuilder 是否能承载多 symbol / 多序列聚合？

如果把逻辑塞进 Snapshot，系统会“过度智能、难审计”；

如果把逻辑散落在各处，又会“紧耦合、不可复现”。

因此 Phase-2 明确目标：
用最少的信号 + 最严格的行为纪律，验证 V12 架构承载性与制度可运行性。

1.2 Phase-2 锁定的 3 个最高优先级因子（不新增）

Market Breadth Damage（结构损伤）

Index–Sector Correlation Regime（相关性收敛/发散，Phase-2 可先关闭）

Market Participation / Breadth Expansion（参与度）

Phase-2 禁区：资金网页、ETF 份额、两融、任何“第四个因子”、任何爬虫实现。

1.3 Phase-2 输出目标（不是交易信号）

Phase-2 的产物不是买卖点，而是：

每日离散状态（State）

每日风险门控（Gate：Normal/Caution/PlanB/Freeze）

自动体检（Health：制度是否自洽、有纪律、无横跳）

2. 分层设计与解耦规范（V12 延伸）

Phase-2 工具必须保持可独立运行，并且未来可插拔入 UnifiedRisk。

2.1 分层职责（禁止越界）

DataLoader（数据层）

只负责取数据与标准化字段

允许派生 prev_close（shift）

不允许评分、不允许判状态、不允许做 Gate

Metrics（指标层）

只负责“逐股数据 → 按日聚合的数值统计量”

不允许阈值判断、不允许输出 state、不允许输出 gate

State Mapper（状态层）

只负责“Metrics 数值 → 离散状态”

规则必须可配置（rules.yaml），且顺序优先匹配

不允许看 yesterday_gate，不允许做任何行为修正

Gate Engine（行为层）

只负责“状态组合 → Gate 行为等级”

可以包含“行为纪律”（例如恢复慢放、反横跳）

不允许回看价格/成交额等数值（只读 state）

Health Check（体检层）

只负责审计 Gate 序列是否符合制度红线

不参与 Gate 生成，不允许反向修正 Gate

3. 名词解释（Glossary）
3.1 数据字段

SYMBOL / EXCHANGE：股票代码与交易所

TRADE_DATE：交易日（不是自然日）

CLOSE：收盘价（本阶段前复权即可）

prev_close：前一交易日收盘价（由 DataLoader 按 code shift 派生）

3.2 Metrics（按日聚合数值）

adv_ratio（上涨比例）
当日 close > prev_close 的股票数 ÷ 有效股票数（忽略 prev_close NaN）

median_return（中位数收益）
当日所有股票 close/prev_close - 1 的中位数（忽略 NaN）

new_low_ratio（新低占比）
当日满足 close == rolling_min(close, N) 的股票占比
其中 N = new_low_window

new_low_persistence（新低持续性）
new_low_ratio 连续触发的天数计数（Phase-2 简化定义：>0 则累加，否则归零）

3.3 State（离散状态）

Breadth Damage State（结构损伤状态）

Healthy：无明显结构损伤

Early：出现裂纹（新低占比达到 early）

Confirmed：裂纹被确认（占比 + 连续天数）

Breakdown：系统性损伤（更高占比 + 更长持续）

Participation State（参与度状态）

BroadUp：上涨家数广泛 + 中位数为正

Narrow：上涨集中（adv_ratio 未达 broad_up，但 median_return ≥ 0）

HiddenWeakness：隐性疲弱（adv_ratio 低 & median_return < 0）

Neutral：不满足关键形态的常态

Correlation Regime State（相关性状态）
Phase-2 可关闭，默认 Stable（不影响 Gate/Health 主线）

3.4 Gate（行为门控）

Normal：正常风险暴露允许

Caution：谨慎（不加仓/减速）

PlanB：防御模式（切换防守策略）

Freeze：冻结（停止进攻性操作）

Gate 是风险暴露上限，不是交易指令。

3.5 Health（制度体检）

zigzag_detected：横跳检测（PlanB/Freeze → Normal → PlanB/Freeze）

breadth_consistency_ok：Breadth 明确恶化时不允许输出 Normal

hidden_weakness_capture_ratio：HiddenWeakness 是否被 PlanB/Freeze 捕捉

HEALTHY/WARNING/FAIL：制度稳定性总评

4. Phase-2 规则定义（冻结版）
4.1 new_low_window（核心口径）

默认：50（对应 50D New Lows 的中期结构锚点）

可做对照组：63（≈一季交易日），但 Phase-2 主线不替换 50

指标窗口不随回测窗口长度变化。回测窗口只是验证范围，N 代表结构周期。

4.2 State Mapper（rules.yaml 口径）

Breadth Damage（示例阈值）：

Early：new_low_ratio ≥ 3%

Confirmed：new_low_ratio ≥ 7% 且 persistence ≥ 3

Breakdown：new_low_ratio ≥ 12% 且 persistence ≥ 5

Participation（示例阈值）：

BroadUp：adv_ratio ≥ 0.60 且 median_return ≥ 0

HiddenWeakness：adv_ratio ≤ 0.40 且 median_return < 0

Narrow：adv_ratio < 0.60 且 median_return ≥ 0

Neutral：兜底

Phase-2 的阈值是“制度阈值”，不是为回测成绩调优。

4.3 Gate 棋谱（Phase-2 冻结）

Breadth Breakdown → Freeze

Breadth Confirmed → PlanB

HiddenWeakness → PlanB

Early 或 Narrow → Caution

Healthy + (BroadUp/Neutral) → Normal

其他 → Caution

4.4 行为纪律补丁（Phase-2 最关键冻结项）

反横跳恢复粘性（anti-zigzag）：

若 yesterday_gate == PlanB 且 today_raw_gate == Normal
则 today_gate 强制为 Caution

目的：禁止 PlanB → Normal → PlanB 横跳。
这条规则属于行为层纪律，不改变信号本身。

5. 实测闭环：FAIL → 归因 → HEALTHY
5.1 首次实测 FAIL 的真实原因

在真实 daily_gate.csv 中出现大量模式：

HiddenWeakness → PlanB

次日短暂 BroadUp/Neutral → Normal

随后再次 HiddenWeakness → PlanB

触发 Health 红线：zigzag_detected = True
而 Breadth Damage 全程 Healthy（说明不是结构性崩坏，是参与度驱动的疲弱/分化）

结论：

FAIL 不是因子错、不是窗口错、不是 new_low_window 错
而是 Gate “恢复过快、无纪律”。

5.2 最小修正（只改行为，不改信号）

在 Gate Engine 加入恢复粘性：
PlanB 不允许直接回 Normal，必须过 Caution。

5.3 修正后 HEALTHY 的意义

信号仍保持敏感（HiddenWeakness 仍触发 PlanB）

但行为具备纪律（消灭横跳）

制度可长期运行、可审计、可复现

6. 回测窗口选择规范（避免 hindsight bias）
6.1 Anchor Date（事件锚点）

例如你观察到 2025-11-13 是第一天下跌：
它只能作为锚点，不能作为回测起点。

6.2 Event-Anchored 回测规则（推荐固化）

给定 anchor_date：

start_date = anchor_date - max(new_low_window, 40)（至少保证结构指标有历史上下文）

end_date = anchor_date + 20~40（验证期）

不允许直接从“下跌第一天”开始回测，否则会制造“假滞后”。

7. 输出文件规范（Artifacts Contract）
7.1 daily_gate.csv（必需字段）

date

breadth_damage_state

participation_state

correlation_regime_state

H4_gate

7.2 health_report.json

gate_distribution（四类 Gate 天数）

max_consecutive（PlanB/Freeze 连续天数）

zigzag_detected（红线）

breadth_consistency_ok（红线）

hidden_weakness_capture_ratio

summary.status（HEALTHY/WARNING/FAIL）

8. 运行与复现规范
8.1 输入数据（CSV Snapshot）

CSV 可包含额外字段（SOURCE/WINDOW_START/CREATED_AT），但 DataLoader 必须只读取：

SYMBOL / EXCHANGE / TRADE_DATE / CLOSE

8.2 复现纪律

任何 HEALTHY/FAIL 必须可用同一份 CSV 快照复现。
制度层（Metrics/State/Gate/Health）不得依赖 DB 状态或外部网络。

9. Phase-2 冻结清单（后续不得破坏）
9.1 冻结项（不可随意改）

分层边界与越界禁令（尤其制度层不碰数据源）

new_low_window 主线 = 50

Gate 反横跳恢复粘性规则（PlanB→Normal 禁止直跳）

Health 红线定义（zigzag 为 FAIL）

9.2 允许变更项（Phase-3 或后续）

Correlation Regime 的启用与规则（Phase-2 可关闭）

阈值的“制度级版本迭代”（必须通过同样的 Health 审计）

输出增加 gate_reason / debug_report（不改变判定）

10. 下一阶段入口（仅记录，不在 Phase-2 展开）

Phase-3 目标：多周期一致性（中期与更长期趋势/结构的确认与冲突处理）。
Phase-2 已证明：信号可用 + 行为有纪律 + 制度可审计。

如果你愿意，我可以再补两份“配套文档”让你后面做 Phase-3 更顺：

PHASE2_CHANGELOG.md：记录从 FAIL → HEALTHY 的唯一改动（便于 code review）

PHASE2_RULES_REFERENCE.md：把 rules.yaml / gate 棋谱 / health 红线做成“速查表”

你回我想要哪两个，我就直接输出同样可落库的 Markdown。

-----
