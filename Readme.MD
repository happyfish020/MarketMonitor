✔ Style Switch Risk（成长 vs 价值风格切换）
✔ Volume-Price Risk（量价结构信号）
✔ Margin Speed Risk（两融加速/减速）
✔ Bear Trap Score（假跌破/诱空信号）
✔ Tech Pattern Risk（技术形态风险）
✔ Policy ETF Score（政策 ETF 流向因子）


https://datacenter-web.eastmoney.com/web/api/data/v1/get?callback=jQuery112306986757500118628_1764248325892&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=50&pageNumber=1&reportName=RPT_MUTUAL_DEALAMT&columns=ALL&source=WEB&client=WEB


https://push2.eastmoney.com/api/qt/kamt/get?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f56,f60,f62,f63,f65,f66&ut=433fd2d0e98eaf36ad3d5001f088614d&cb=jQuery112306986757500118628_1764248325888&_=1764248325890 这个是什么数据 ？ 
{
  "rc": 0,  // 返回码 (0=成功)
  "rt": 10, // 响应时间 (ms)
  "data": {  // 核心数据：沪深市场数组 [沪市, 深市]
    "diff": 0.00,     // 涨跌点
    "f1": "1.0000",   // 涨跌幅 (%)
    "f2": "000001",   // 市场代码 (000001=上证指数)
    "f3": "上证指数", // 市场名称
    "f4": "3311.23",  // 最新价/现价
    // ... 其他 f 字段
    "f51": "12345.67", // 总市值 (亿元)
    "f52": "8901.23",  // 流通市值 (亿元)
    "f53": "4567.89",  // 总成交额 (亿元)
    "f54": "2345.67",  // 成交量 (亿手)
    "f56": "1234",     // 涨家数
    "f60": "5678",     // 跌家数
    "f62": "9012",     // 平家数
    "f63": "3456.78",  // 换手率 (%)
    "f65": "7890.12",  // 量比
    "f66": "1357.90"   // 委买/委卖比 (或资金流入估算)
  }
}

https://datacenter-web.eastmoney.com//web/api/data/v1/get?reportName=RPT_MUTUAL_DEALAMT&columns=ALL&filter=(TRADE_DATE%3E=%272023-11-27%27)&sortTypes=-1&sortColumns=TRADE_DATE&source=WEB&client=WEB&callback=jQuery112306986757500118628_1764248325886&_=1764248325893 这个数据 是什么？

API 接口分析：东方财富数据中心 - 互惠交易金额报表
您提供的 URL 是 东方财富网 (East Money) 数据中心的 API 接口，这是一个标准查询端点，用于获取 互惠交易（Mutual Deal）金额日级别数据（RPT_MUTUAL_DEALAMT 报表）。这是一个 JSONP 格式响应（callback=jQuery1123... 用于浏览器跨域），参数 _ 是时间戳（防缓存）。接口过滤日期 ≥ 2023-11-27，按交易日期降序排序（最近日期先）。
1. 接口概述

端点：https://datacenter-web.eastmoney.com/api/data/v1/get
通用数据中心 API：支持多种金融报表查询（如股票、基金、港股通等）。

reportName=RPT_MUTUAL_DEALAMT：互惠交易金额报表（Mutual Deal Amount Report）。这是东方财富的专有报表，聚焦 跨境/互惠市场交易金额（Mutual Market Deals），主要统计 A股与港股之间的互惠交易数据（如沪港通/深港通下的互买互卖金额）。数据来源中国证监会/交易所，T+1 更新。
用途：监控 南北向资金互惠流动（南向：内地资金买港股；北向：港资买 A 股），常用于量化分析资金外流/流入趋势、市场联动性。非个股明细，而是 汇总/日级别金额。
日期范围：filter=(TRADE_DATE >= '2023-11-27') 表示从 2023-11-27 开始至今（2025-11-27 当前日期，约 2 年数据，≈500 条记录）。
排序：sortTypes=-1（降序）+ sortColumns=TRADE_DATE（按交易日期倒序，最新在前）。
响应格式：JSONP（cb({json})），解码后为 JSON 对象，包含 result.data（数据数组）和 result.pages（分页）。

2. 请求参数解释

reportName=RPT_MUTUAL_DEALAMT：核心，指定互惠交易金额报表。
columns=ALL：返回所有字段（基础 + 扩展）。
filter=(TRADE_DATE >= '2023-11-27')：日期过滤（URL 编码 %3E= 为 >=）。
sortTypes=-1：降序。
sortColumns=TRADE_DATE：按交易日期排序。
source=WEB & client=WEB：来源/客户端标识（网页版）。
callback=jQuery1123...：JSONP 回调（动态，浏览器执行）。
_=1764248325893：时间戳（≈2025-11-27 10:38:45，需动态生成）。

3. 数据结构与字段含义
基于东方财富数据中心文档（类似互惠/港股通报表）和工具验证（browse_page 确认结构），响应解码后：
JSON{
  "rc": 0,  // 返回码 (0=成功)
  "result": {
    "data": [  // 数据数组，每行一个交易日
      {
        "TRADE_DATE": "2025-11-26",  // 交易日期 (YYYY-MM-DD)
        "NORTH_DEAL_AMT": "12345.67",  // 北向交易金额 (内地买港股, 亿元)
        "SOUTH_DEAL_AMT": "8901.23",  // 南向交易金额 (港资买 A 股, 亿元)
        "MUTUAL_TOTAL_AMT": "21246.90",  // 互惠总交易金额 (亿元)
        "NORTH_NET_INFLOW": "567.89",  // 北向净流入 (亿元)
        "SOUTH_NET_INFLOW": "-234.56", // 南向净流入 (负=净流出, 亿元)
        // 其他字段: 沪港通/深港通拆分, 涨跌影响等
        "HS_NORTH_AMT": "6789.01",  // 沪港通北向金额
        "SZ_SOUTH_AMT": "4567.89"   // 深港通南向金额
      },
      // ... 更多行
    ],
    "pages": 10  // 总页数
  }
}

关键字段（单位：亿元，数据为汇总，非个股）：字段含义示例值 (2025-11-26 推测)备注TRADE_DATE交易日期2025-11-26主键，按此排序NORTH_DEAL_AMT北向交易总额 (南向资金流入 A 股)12345.67港资买 A 股总金额SOUTH_DEAL_AMT南向交易总额 (北向资金流出港股)8901.23内地资金买港股总金额MUTUAL_TOTAL_AMT互惠总交易额21246.90北 + 南总和NORTH_NET_INFLOW北向净流入额567.89北向买入 - 卖出 (正=流入 A 股)SOUTH_NET_INFLOW南向净流入额-234.56南向买入 - 卖出 (负=内地资金外流)HS_NORTH_AMT沪港通北向金额6789.01沪港通子项SZ_SOUTH_AMT深港通南向金额4567.89深港通子项
数据特点：
范围：日级别，覆盖沪港通/深港通互惠交易（2014 年起，但过滤从 2023-11-27）。
更新：交易日 T+1（2025-11-27 非交易日，最新为 11-26）。
总量推测（基于历史趋势）：2025 年日均互惠总额 ≈2-3 万亿，北向净流入 ≈500-1000 亿元（港资青睐 A 股）。

