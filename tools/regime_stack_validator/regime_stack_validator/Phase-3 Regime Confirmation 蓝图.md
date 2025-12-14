Phase-3 Regime Confirmation 蓝图

(UnifiedRisk V12 – RegimeStack)

0. Phase-3 的一句话定义（先给结论）

Phase-3 的目标不是“更早预警”，
而是“确认 Phase-2 的风险信号在更大时间尺度上是否成立”。

Phase-3 解决的问题是：

当 Phase-2 进入 Caution / PlanB 时：
这是“短期扰动”，还是“中期/长期 Regime 变化”？

1. 为什么 Phase-3 是“必需的”（从 Phase-2 的边界说起）
1.1 Phase-2 已经解决了什么（回顾）

Phase-2 已经明确完成：

中短期结构风险的感知（T-5 ~ T+5 级别）

将“信号”变成“行为约束（Gate）”

解决了：

信号是否有效

行为是否有纪律

是否避免 zigzag

但 Phase-2 有一个刻意保留的限制：

⚠️ Phase-2 不回答：
“这个风险，是不是会演变成一个更大的 Regime？”

1.2 如果没有 Phase-3，会发生什么？

如果你只用 Phase-2：

每一次 PlanB：

你都会面临同一个问题
👉「这是一次洗盘，还是趋势切换？」

系统层面：

Phase-2 会显得“太敏感”

容易被误解为“过度防守”

Phase-3 的使命，就是解决这个“确认问题”。

2. Phase-3 的核心需求（Requirements）
2.1 核心需求（冻结表述）

Phase-3 必须回答且只回答下面 3 个问题：

Phase-2 的风险信号，是否被更长期结构所支持？

当前市场处于哪一种 Regime？

Phase-2 的 Gate 行为，是否需要被“放大 / 约束 / 中和”？

2.2 Phase-3 不允许做的事（Scope 边界）

Phase-3 明确不做：

❌ 不预测价格涨跌

❌ 不引入新的高频噪音指标

❌ 不替代 Phase-2 的 Gate

❌ 不对 Phase-2 的判断“打分否定”

Phase-3 的角色是：确认器（Confirmation Layer），不是裁判。

3. Phase-3 的输出不是信号，而是“Regime”
3.1 Regime 的定义（不是行情判断）

在 Phase-3 中，Regime ≠ 牛市 / 熊市。

Regime 是一个行为语义：

“在当前结构背景下，
Phase-2 的风险信号，应该被如何对待？”

3.2 Phase-3 的最小 Regime 集合（建议冻结）
Risk-On        （趋势支持风险）
Neutral        （结构中性）
Risk-Off       （趋势压制风险）
Distribution   （顶部/分化型风险）


Phase-3 不追求多，追求语义稳定。

4. Phase-3 解决的“关键冲突场景”

下面是 Phase-3 存在的真实理由。

场景 A（Phase-2 单独使用的问题）
Phase-2 = PlanB
但更长期趋势 = 强势上行


如果没有 Phase-3：

你只能被迫“全面防守”

但实际上可能只是：

上涨过程中的一次健康调整

👉 Phase-3 要识别：
这是 Risk-On 中的 PlanB，而不是全面 Risk-Off。

场景 B（更危险的一类）
Phase-2 = Normal
但更长期结构已经开始恶化


如果没有 Phase-3：

系统会“放松警惕”

直到 Phase-2 被动触发

👉 Phase-3 要提供 背景预警。

5. Phase-3 的设计思路（不写代码，只讲结构）
5.1 Phase-3 的信息来源（允许的）

Phase-3 只允许使用“慢变量”，例如：

更长窗口的 Breadth 结构（例如 100D/120D）

中长期趋势稳定性（例如方向一致性，而非拐点）

结构性扩散/收敛（是否持续集中在少数板块）

⚠️ 关键原则：

Phase-3 绝不和 Phase-2 竞争“谁更快”

5.2 Phase-3 的处理方式（抽象）

Phase-3 不输出“买/卖”，只输出：

Regime ∈ {Risk-On, Neutral, Risk-Off, Distribution}


然后通过一个 Regime × Gate 解释矩阵，影响行为。

6. Phase-3 与 Phase-2 的关系（最重要的一节）
6.1 Phase-3 永远不直接修改 Phase-2 Gate

Phase-2 Gate 是底层安全机制，不可被覆盖。

Phase-3 只能做三件事之一：

确认（Confirm）

约束（Cap）

放大（Amplify）

6.2 示例：Gate × Regime 行为解释
Phase-2 Gate	Phase-3 Regime	行为含义
Normal	Risk-On	正常执行策略
PlanB	Risk-On	减速而非反向
PlanB	Risk-Off	防守确认
Normal	Distribution	提前警惕
Caution	Risk-Off	不允许激进

注意：
Phase-3 解释行为，不重写 Gate。

7. Phase-3 的 Scope 冻结（非常重要）
7.1 Phase-3 必须保持的约束

Phase-3 不接管 Gate

Phase-3 不使用日内数据

Phase-3 不引入资金网页类信号

Phase-3 不做微观择时

7.2 Phase-3 的时间尺度
Phase-2 : T-5 ~ T+5
Phase-3 : 周级 / 月级

8. Phase-3 的成功标准（不是“看对行情”）

Phase-3 成功的标志不是：

“更早预测到顶部或底部”

而是：

减少 Phase-2 的误判成本

让防守行为“有背景、有分寸”

让风险暴露的变化“符合大结构”

9. Phase-3 与 UnifiedRisk V12 架构的关系

Phase-3 天然适合：

作为 Transformer → Snapshot → Factor 的“上层解释模块”

输出到：

PredictionEngine（作为权重/约束）

Reporter（作为文字解释）

但 Phase-3 本身仍然是“制度层”，不是预测层。

10. Phase-3 之前的冻结声明（承接 Phase-2）

在进入 Phase-3 实现前，以下结论被视为 不可推翻前提：

Phase-2 Gate 行为纪律已冻结

new_low_window = 50 的中期结构锚点成立

HiddenWeakness 是有效的中短期风险信号

zigzag 被视为制度级 FAIL

Phase-3 只能在此基础上“叠加解释”，不能推翻。

11. Phase-3 下一步（Implementation 之前）

在写任何代码之前，Phase-3 必须先完成：

Regime 定义文档（离散语义）

Regime × Gate 行为解释表

Phase-3 的 Health / Consistency 规则（避免 Regime 横跳）

12. Phase-3 的一句话总结（给未来的你）

Phase-2 让系统“知道什么时候要慢下来”，
Phase-3 让系统“知道慢下来是暂时的，还是必须的”。

如果你愿意，下一步我可以 严格按这个蓝图，继续帮你做三选一：

1️⃣ 写 Phase-3 Regime 的离散定义草案
2️⃣ 设计 Phase-3 的 Health / Anti-Zigzag 规则（避免 Regime 级别乱跳）
3️⃣ 把 Phase-3 映射回 UnifiedRisk V12 的 Factor / Predictor 接口规范