# UnifiedRisk V11.7
# 因子层（Factor Layer）设计规范

版本：V11.7
更新时间：2025-12-06

目录（TOC）

因子层定位

因子输入来源（数据约束）

因子内部结构规范

因子输出格式规范

因子计算通用规则

因子之间的依赖规范

因子日志规范

因子示例结构（标准模板）

常见错误与禁止行为

# 1. 因子层定位

Factor 层是 UnifiedRisk 的核心分析模块，其职责是：

从 snapshot 中读取数据

进行计算（趋势、加速度、区间、评分等）

输出一个 标准格式的因子对象（score + details）

特点：

项目	说明
只读 snapshot	❌ 不访问 symbolcache，❌ 不访问 datasource
独立	每个因子必须逻辑独立，不得相互耦合
松耦合	因子接口统一，便于“拼装风险报告”
可组合	用于 unified score 或 multi-horizon score
# 2. 因子输入来源（数据约束）

因子 唯一允许读取的数据源：

data/cache/day_cn/YYYYMMDD/ashare_daily_snapshot.json


读取方式：

snap = snapshot["xxx"]


因子禁止读取：

禁止内容	原因
❌ symbolcache	symbolcache 是 datasource 层数据，不属于因子
❌ datasource（如 get_macro_daily）	会破坏 V11 分层结构
❌ yfinance / requests	因子层必须是纯计算层
# 3. 因子内部结构规范

每个因子必须是一个 类：

class XxxFactor:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def compute(self) -> Dict[str, Any]:
        ...


必要部分：

部分	描述
init(snapshot)	接收 snapshot（或 snapshot 中的某一部分）
compute()	返回 score + explain 结构
私有函数	_calc_trend() _calc_accel() 等

禁止：

❌ 全局变量

❌ 写文件

❌ 调用 datasource

❌ 网络请求

# 4. 因子输出格式规范

所有因子必须输出以下统一结构：

{
    "name": "turnover",
    "score": 40.0,
    "level": "中性偏弱",
    "details": {
        "sh": ...,
        "sz": ...,
        "trend_3": ...,
        "accel_3": ...,
    }
}


强制要求：

字段	含义
name	因子名（统一命名规范）
score	0~100（越高越强）
level	文本区间（“强 / 中性 / 弱”）
details	任意结构，只要可读
# 5. 因子计算通用规则
⭐ 5.1 分数必须归一化到 0~100

所有因子必须遵循统一评分模型：

100 = 极强

50 = 中性

0 = 极弱

禁止使用：（避免不统一）

❌ -1~1

❌ True/False

❌ 0/1

❌ ±100

⭐ 5.2 趋势（Trend）

趋势的标准公式：

trend_N = MA_N(value_series) - MA_1(value_series)


趋势方向：

0 代表增强

< 0 代表减弱

⭐ 5.3 加速度（Acceleration）
accel_3 = val[-1] - val[-2]


或：

accel_3 = trend_3 - trend_3_prev

⭐ 5.4 区间（Regime）

每个因子必须定义自己的区间，例如：

围度	说明
> 70	强区间
30–70	中性区间
< 30	弱区间
# 6. 因子之间的依赖规范

因子之间必须 完全独立：

禁止依赖	原因
❌ 因子 A 调用因子 B	增强耦合
❌ 用“共享状态”传递	不可复现
❌ 修改 snapshot	数据源不可篡改

因子之间唯一允许的组合方式：

由 scorer 层统一加权融合

因子自身完全独立

# 7. 因子日志规范

必须使用：

log(f"[Factor] XXX ...")


禁止：

❌ print

❌ logging.getLogger

❌ debug=False/True 控制台输出

因子日志要尽量“轻”：

✔ 允许：趋势结果、加速度结果
❌ 禁止：大 DataFrame 输出、symbolcache 路径

# 8. 因子模板（标准模板，所有因子必须遵循）
class ExampleFactor:
    def __init__(self, snapshot):
        self.data = snapshot["example"]

    def compute(self):
        val = self.data["value"]
        trend = self._trend()
        accel = self._accel()

        score = self._score(trend, accel)

        return {
            "name": "example",
            "score": score,
            "level": self._level(score),
            "details": {
                "value": val,
                "trend": trend,
                "accel": accel,
            }
        }

    def _trend(self):
        ...

    def _accel(self):
        ...

    def _score(self, trend, accel):
        ...

    def _level(self, score):
        ...

# 9. 禁止行为总结（必须遵守）

❌ 因子中读 symbolcache
❌ 因子中调 datasource
❌ 因子直接使用 yfinance
❌ 因子写 snapshot
❌ 因子使用全局变量
❌ 因子返回非 0~100 分数
❌ 因子输出格式不统一