# eltdx 数据源能力核查结论

> 核查对象：**eltdx 3.2.2**（`backend/.venv`，Python 3.14.7）
> 核查方式：真实主站连通性实测 + eltdx 包内 introspection + 与面板既有落盘数据对账
> 对应实现：分支 `feat/eltdx-data-source`，插件 `backend/app/plugins/eltdx/`
> 结论基准：2026-09 实测；eltdx 升级后须按第 8 节复测

本文回答一个问题：**eltdx 的各项能力，能否满足本项目的六大数据集契约？** 逐项给出实测事实与判定。

---

## 1. 结论速览

| # | 能力项 | 面板数据集 | 判定 | 实测依据 |
|---|---|---|---|---|
| ① | 除权因子 / 前复权计算基准 | `adj_factor` | ✅ **已接入** | 与本地表 32/32 对账吻合，最大相对误差 0.0239% |
| ② | 分钟 K / 分时图 / 分钟回测 | `minute` | ✅ **已接入** | 240 根/交易日；累计量额与日 K 误差 0.0000% 量级 |
| ③ | 五档盘口 / 封单 / 盘口深度 | `depth5` | ✅ **已接入** | 买卖各 5 档，价序自检通过 |
| ④ | 财务指标 / 三大报表 | `financial` | ⚠️ **部分接入**（`shares` + 资产负债表 + 现金流量表；利润表与指标未接） | 见 §6 |
| ⑤ | 全量分钟（盘中全市场当日落盘） | `full_minute` | ✅ **已接入**（修复轮 + 增量轮） | 压测通过：修复轮 133.7 万行 / 99.9% / 22.5s；增量轮 11s |
| ⑥ | 连板梯队 / 封单榜 | —— | ⛔ **无数据集槽位** | eltdx 有三个来源，但面板六大数据集无此项 |

另外覆盖的两个基础数据集：`daily`（不复权原始日 K）、`realtime`（全市场快照，含指数）。

**汇总**：已接入 **7** 个数据集（daily / realtime / minute / full_minute / depth5 / financial / adj_factor）；部分接入 1（financial 仅 shares）；无槽位 1（连板梯队）。

---

## 2. 能力盘点（eltdx 3.2.2 相关 API）

eltdx 按"想拿什么数据"组织入口，模块化 API 分组如下（仅列与本项目相关的）：

| 分组 | 代表方法 | 本项目是否使用 |
|---|---|---|
| `bars` | `get(code, *, period='day', start, count, adjust, anchor_date, all_pages, page_size)` | ✅ `daily` / `minute` |
| `quotes` | `get_snapshots(codes)`、`get_depth(codes)`、`refresh`、`list_by_category` | ✅ `realtime` / `depth5` |
| `minutes` | `today(codes)`、`history(code, date)`、`recent`、`buy_sell_strength` | ⛔ 见 §4（分时点，非 OHLC） |
| `corporate` | `adjustment_factors(code, anchor_date, start_date)`、`finance_batch(codes, fields)`、`capital_changes` | ✅ `adj_factor` / `financial` |
| `codes` | `all_a_shares()`、`all_indices()`、`all_etfs()` | ✅ 代码表 |
| `f10` | `finance_report(code, report_type)`、`finance_diagnosis`、`limit_board_ladder`、`valuation` | ⛔ 见 §5（不透明代码） |
| `helpers` | `limit_ladder`、`full_quotes`、`limit_up_down_list`、`shortline_indicators` | ⛔ 无槽位（连板梯队） |
| `trades` / `auctions` / `money_flow` | 成交明细、竞价、资金流 | 未接入（无对应契约） |

关键的连接与并发参数：`TdxClient(server_count, connections_per_server, timeout, runtime_workers, ...)`。本项目默认 `server_count=4 × connections_per_server=4 = 16` 个 TCP slot，并可用环境变量覆盖（见第 7 节）。

---

## 3. ① 除权因子 / 前复权计算基准 → `adj_factor` ✅

### 3.1 eltdx 的表达方式

```
corporate.adjustment_factors(code) -> AdjustmentFactorResponse
    .items: tuple[AdjustmentFactor(date, qfq_scale, qfq_offset, hfq_scale, hfq_offset), ...]
```

eltaX 用 **`(scale, offset)` 二元组**表达除权：`scale` 管送转股（含配股导致的股本变化），`offset` 管现金分红。事件序列按日期升序，每个元素是该事件**之后**的复权参数。

### 3.2 面板契约

```
get_adj_factors(symbols, start, end, ...) -> [symbol, trade_date, ex_factor]
```

`ex_factor` 是**单事件比值（非累积）** —— 累积复权链由 `indicators/pipeline._apply_adj_factor` 自行构建。因此 provider 必须把 eltdx 的二元组**折算成单事件比值**，不能直接透传。

### 3.3 换算公式（实测标定）

```
div       = (cur.hfq_offset - prev.hfq_offset) / cur.hfq_scale      # 每股分红
ex_factor = (cur.hfq_scale / prev.hfq_scale) x prev_close / (prev_close - div)
```

`prev_close` 取事件日**前一交易日**的不复权收盘价（除权参考价基准）。事件日当天的收盘是除权**后**的价格，不可用作基准。

### 3.4 两个必须注意的技术要点

**(a) 必须用 `hfq_*`，不能用 `qfq_*`**

实测 `qfq_offset` 是**前复权偏移量**，与每股分红不是同一个量：

| 标的 | 事件日 | `qfq_offset` 增量 | `hfq_offset` 增量 / scale | 真实每股分红 |
|---|---|---|---|---|
| 000001.SZ | 2026-09-24 | 0.36 | 66.411231 / 266.711770 = **0.249** | 0.249（10 派 2.49） |

若误用 `qfq_offset` 推导，会得到系统性偏大的 `ex_factor`。

**(b) 不能只看 `scale` 比值**

现金分红型除权**不改 scale**，变化只体现在 offset。实测 `002818.SZ` 在 2026-09-29 是真实除权日，但 `hfq_scale` 前后**都是 1.7000**：

```
002818.SZ  2025-09-30  hfq_scale=1.7000  hfq_offset=...  ...
           2026-05-15  hfq_scale=1.7000  hfq_offset=...  ...
           2026-09-29  hfq_scale=1.7000  hfq_offset=...+0.31/scale   <- 只有 offset 变了
```

只取 `scale` 比值会漏掉**全部**分红型除权（A 股绝大多数除权事件是分红）。

### 3.5 对账结果

与本地 `data/adj_factor/all.parquet`（经 fuyao 口径写入）逐条比对：

| 指标 | 结果 |
|---|---|
| 可对账条数 | **32 / 32 全部产出** |
| 最大绝对误差 | 2.394e-04 |
| 最大相对误差 | **0.0239%** |
| 相对误差 < 0.01% 的条数 | **30 / 32** |

**精度上界说明**：`hfq_offset` 自身带有累计浮点/取整误差，反推的 `div` 与交易所公布的分红可能有微小出入（实测 `605016.SH` 推导 0.075 vs 真实 0.070006），导致 `ex_factor` 最大约 0.024% 的相对偏差 —— 对 1.02 量级的因子即 0.00024，影响复权价第 4 位小数。**若需与 fuyao 完全一致，应继续使用 fuyao 的 `adj_factor`。**

### 3.6 前复权计算基准

eltaX 的 `bars.get(adjust='qfq', anchor_date=...)` 与 `adjustment_factors(anchor_date=...)` 均支持**定点复权基准**。本项目**不使用**该能力：面板契约要求 provider 只提供**单事件比值**，复权链与基准由 `pipeline` 统一管理，避免数据源各自为政导致口径分裂。

---

## 4. ② 分钟 K / 分时图 / 分钟回测 → `minute` ✅

### 4.1 关键结论：必须用 `bars.get(period='1m')`

eltaX 有两个看起来都能拿"分钟数据"的入口，但**只有一个满足契约**：

| 入口 | 返回内容 | 是否满足分钟 K 契约 |
|---|---|---|
| `bars.get(period='1m')` | **真 OHLC**（`open/high/low/close/volume_lots/amount`），与日 K 同一个 `KlineBar` 模型 | ✅ **使用此入口** |
| `minutes.history(code, date)` | **分时点**：仅 `price` + `volume`（无 OHLC），`amount` 恒为 0 | ⛔ 不满足 |

实测对照（sz000001，2026-09-29 末三根）：

```
bars.get(period='1m')  14:58 O=11.36 H=11.36 L=11.36 C=11.36 vol=129.0  amt=146544.0
                       15:00 O=11.35 H=11.35 L=11.35 C=11.35 vol=9467.0 amt=10745499.0
minutes.history        14:58 price=11.36 volume=129
                       15:00 price=11.35 volume=9467
```

两者的 `price/volume` 逐分钟精确吻合，证明**同源**；但 `minutes.history` 缺 OHLC 与成交额，无法构造分钟 K 线，故不用于本契约。

### 4.2 实测指标

| 项目 | 实测值 |
|---|---|
| 单日根数 | **240 根**（09:31 ~ 15:00，与 A 股分钟数一致） |
| 字段 | `open/high/low/close/volume_lots/amount` 齐全 |
| `volume` 单位 | **手**（与日 K 同源） |
| `amount` 单位 | **元** |
| `time` 类型 | Asia/Shanghai **aware** datetime（`+08:00`） |
| 历史深度 | `start` 逐页可取 ≥17 个交易日（4000 根无缺口）→ 视为**深源**，未声明 `minute_history_days` |

### 4.3 契约要点

- **`datetime` 必须是北京墙钟 naive**（如 `2026-09-29 09:31:00`）。eltaX 的 `time` 是 aware，provider 内需 `astimezone(+08:00).replace(tzinfo=None)`；若直接返回 aware 或误转 UTC，前端分时图会因点位落在时轴外而**空白**。
- **自管分页**：eltaX 单页上限 **800** 根（`count>800` 报 `page size must be between 1 and 800`），且 `all_pages=True` 在达到 `max_pages` 时会**抛异常**而非静默截断。故 `client.bars` 自行按 `start` 逐页取，遇空页终止。

---

## 5. ③ 五档盘口 / 封单 / 盘口深度 → `depth5` ✅

### 5.1 结构映射

```
quotes.get_depth(codes) -> QuoteRefreshPage
    .records[]: buy_levels  = tuple[QuoteLevel(price, volume) x 5]
                sell_levels = tuple[QuoteLevel(price, volume) x 5]
```

面板契约：

```python
get_depth_batch(symbols) -> {
    "600519.SH": {"bid_prices":[5], "bid_volumes":[5],
                  "ask_prices":[5], "ask_volumes":[5], "timestamp": ms}
}
```

结构**几乎 1:1**，价量数组均按一档到五档排列。

### 5.2 实测值（000001.SZ）

```
买盘 5 档: 11.35x3815  11.34x1610  11.33x1922  11.32x1695  11.31x1013
卖盘 5 档: 11.36x986   11.37x56    11.38x932   11.39x489   11.40x2897
timestamp: 1790667172000  ->  2026-09-29 15:33:15
```

- **volume 单位为手**：五档合计 10055 手 vs 全日总量 690979 手，量级自洽（若为股则应放大 100 倍）。
- 价序自检通过：买盘递减、卖盘递增。

### 5.3 三个契约红线

**(a) `volume = 0` 是有意义的值，必须原样保留**

服务层 `depth_service` 据此判定"真封"：

```python
# 涨停真封: 涨停价上卖一(主动卖压)为 0
"sealed_up": (ask1 == 0) if sym in up_set and ask1 is not None else None
```

因此封死涨跌停时**不能**把 `0` 转成 `None`，也不能跳过该档 —— 否则"封单"能力整体失效。

**(b) 失败必须抛异常，不得跨数据源回退**

契约要求单批异常由**服务层隔离**并保留其他批次，provider 不得自行切换到其他源。故 `client.depth()` 不做软失败（与快照相反）。

**(c) 快照分片必须 <= 80（重要陷阱）**

`quotes.get_snapshots` 有 **80 只/请求的硬上限**，且行为是"静默截断 + 超限断连"：

| 请求只数 | 实际返回 |
|---|---|
| 80 | 80（足额） |
| 81 / 90 / 100 / 200 / 400 / 500 / 700 | **一律 80（静默截断）** |
| 800 / 1600 / 3000 | **断连**：`os error 10054` / `TCP stream closed` |

若按"包大小"把分片放宽到 800，会导致**每一片都超限 → 全市场快照返回 0 行（覆盖率 0%）**。这是本项目实际踩过的缺陷：`realtime` 数据集被路由到本插件后，全市场快照一度完全取不到数据。修复后实测：**分片 80 → 70 片、0 失败、5578/5578 = 100% 覆盖、耗时 3.4s**（满足 6s 轮询契约）。

> 对比：`bars.get` 批量**无此限制**（2000 只/请求足额）。两者限制不同，不可类推。

---

## 6. ④ 财务指标与三大报表 → `financial`（⚠️ 仅 `shares`）

### 6.1 面板要 5 张表

```
FINANCIAL_TABLES = ("metrics", "income", "balance_sheet", "cash_flow", "shares")
```

### 6.2 已接入：`shares` ✅

eltaX 的 `corporate.finance_batch(codes)` 返回 `FinanceRecord`，**字段名明确**，其中股本相关可直接映射：

| FinanceRecord 字段 | 含义 | 面板列 |
|---|---|---|
| `zong_gu_ben_raw_float` | 总股本 | `total_shares` |
| `liu_tong_gu_ben_raw_float` | 流通股本 | `float_shares` |
| `updated_date` | 公告/更新日 | `period_end` + `announce_date` |

**单位**：eltaX 为**万股**（实测茅台 `125008.15625` 万股 = 12.5 亿股），面板契约要**股**且 `float_shares > 0` 才有效，故 provider 内 **×10000**。

**`period_end` 的语义论证**：`FinanceRecord` 只有 `updated_date`（实测茅台 `2026-08-15`），没有报告期字段。但面板的 PIT（point-in-time）逻辑本身就是 `available_date = announce_date or period_end`，故以 `updated_date` 同作 `period_end` 与 `announce_date` **语义等价**，且不引入未来函数（该期数据在公告日才可用）。

**下游价值**：`share_capital.apply_historical_float_shares` 用该表驱动历史换手率：

```
turnover_rate = volume x 10000 / float_shares
```

实测社保核对：平安银行 `194.0568` 亿股（总股本 194.0592 亿，差额为限售股）、茅台 `12.5008` 亿股、浦发 `333.0584` 亿股 —— 与实况吻合。

### 6.3 已接入：`balance_sheet` / `cash_flow` ✅（2026-10 补齐）

**早前按"口径不可确证"跳过；现口径已确证，故接入。** 确证方式不是算术反推，而是：

1. **扶摇同期数据逐字段交叉验证**（茅台 600519.SH / 2026-06-30）：9 个资产负债表字段
   （`total_assets` / `total_liabilities` / `total_equity` / `total_current_assets` /
   `total_non_current_assets` / `cash_and_equivalents` / `accounts_receivable`）与扶摇
   **逐位精确相等**（非近似）；现金流量表 5 个字段同样精确相等。
2. **会计恒等式自证**：`资产 == 负债 + 权益` 在 3 标的 × 2 模板族上全部成立。

**关键前提：`nhytype` 是行业模板判别字段。** 上游第 1 张结果集是宽表（无行业字段），
**第 2 张**才是 `{rtype, nhytype, zqname}`：

| 标的 | `nhytype` | 模板族 |
|---|---|---|
| 贵州茅台 | 0 | 通用（工商） |
| 平安银行 | 1 | 银行 |
| 中国平安 | 3 | 保险 |

**同一 T 代码在不同模板族下含义不同**，必须分流：

| 字段 | 通用族 | 金融族 |
|---|---|---|
| 总资产 | `T039` | `T048` |
| 总负债 | `T062` | `T083` |
| 所有者权益 | `T071` | `T093` |
| 经营现金流净额 | `T017` | `T033` |
| 投资现金流净额 | `T029` | `T044` |
| 筹资现金流净额 | `T038` | `T055` |
| 现金净增加 | `T041` | `T058` |
| capex | `T024` | `T041` |

两处**必须警惕的陷阱**（错用即静默污染）：

- `T039` 在茅台是总资产（3090.5 亿），在平安银行只有 104.6 亿，而真实总资产
  **60,287.9 亿** —— 差 **576 倍**。用全局固定表映射金融股会静默写错总资产。
- `T041` 在通用族是"现金净增加"，在金融族是"capex" —— **语义互换**。

`T037` 是筹资活动现金**流出**的正数原值，`T038` 才是带符号净额（实测 -37,944,297,802.12），
映射 `T038` 即得负数，**不需要手工取反**。

金融族的明细科目（流动资产/应收/存货等）上游无稳定对应，**一律不映射（留空）**，
不用猜测值填充。未知 `nhytype` 按**金融族**处理（fail-safe）：通用族代码落在金融报表上是
无关小数字，会静默产出错误合计；金融族映射在通用股上取不到列，只会留空交多源合并。

### 6.4 仍未接入：`metrics` / `income` ⛔

**`income`（利润表）是上游缺口，不是口径问题。** `f10.finance_report(report_type='lrb')`
只回 3 列 `{rtype:'lrb', nhytype:0, zqname:'<简称>'}`、**无数值**；而 `zcfzb` 回 102 行 × 71 列、
`xjllb` 回 99 行 × 69 列。已试 16 个候选 `report_type` 取值（`lrbfx` / `lrb2` / `profit` /
`income` / `LRB` / `syb` / `fyb` …）均无数值；并确认请求体确实发的是
`Params: ['600519','lrb','']`，**非客户端解析问题**。故 provider 对 `income` 返回空帧，
由多源合并 / 备份源补齐保留既有值。

`metrics` 未接入：指标接口（`financials/indicators`）为**单股单期**查询，全市场成本过高。

### 6.5 覆盖率与性能实测（2026-10-01，全市场真实同步）

经**真实全市场同步**（服务端 `_fetch_table` 全链路，5882 只标的）：

| 表 | 落盘行数 | 覆盖标的 | parquet |
|---|---|---|---|
| `balance_sheet` | **323,831 行** | **5,881 / 5,882 = 99.98%** | 15.7 MB |

- 报告期回溯至 1990 年（每只标的多期累积），最新期为 2026-06-30。
- 最近一期（2026-06-30）非空率：`total_assets` / `total_liabilities` / `total_equity` 均 5579/5579。
- 单表全市场耗时约 **30 分钟**（约 274~298ms/只）；逐标的接口无批量，属该数据源结构性成本。
- 分层抽样（沪深北各 150 只，450 只）覆盖 100%，与全市场结果一致。

#### 会计恒等式的分期表现（重要）

`资产 == 负债 + 权益` 的符合率**随报告期年代显著变化**：

| 报告期年代 | 检查行数 | 违例 | 违例率 |
|---|---|---|---|
| 1990s | 9,707 | 4,803 | **49.48%** |
| 2000s | 52,991 | 9,270 | **17.49%** |
| 2010s | 123,887 | 8 | **0.01%** |
| 2020s | 137,016 | 19 | **0.01%** |

**这是上游早期报表自身的数据缺口，不是 T 代码映射错位**，判据有三：

1. 早期报告期的字段**大面积缺失**（万科 1995-06-30 上游仅回 14 个非空列，2026-06-30 回 53 个），
   且该期**没有任何列**等于「资产 − 权益」—— 说明上游根本没有提供正确的负债合计；
2. 恒等式在 2010 年后的数据上**精确成立**（违例率 0.01%）；
3. 最近期唯一一例违例（003004.SZ，偏差 0.36%）拿扶摇同期数据对账：
   扶摇返回 `1,261,208,212.15 / 591,440,506.13 / 674,334,406.82`，与 eltdx **逐位完全相同** ——
   证明映射精确，残差来源是少数股东权益等科目本身。

**结论**：字段映射可用；**早期报告期（2010 年前）的财务因子应谨慎使用**，
这是数据源层面的限制，与本次接入无关。下游若要做长期回测，建议按报告期过滤或做恒等式校验。

### 6.6 主源 + 备份源补齐

服务层（`financial_sync._fill_missing_from_backup`）实现"主源优先，备份只补缺"：
生效财务源取不到的**标的**，由另一个声明了 `financial` 数据集的可用 source 补齐；
主源已有的标的一律不覆盖，备份源故障只记 warning 且不影响主源结果。
详见 [custom-data-source.md](./custom-data-source.md)。

---

## 7. ⑤ 全量分钟（盘中全市场当日分钟落盘）→ `full_minute` ✅

### 7.1 契约要求

| 方法 | 必需性 | 语义 |
|---|---|---|
| `get_intraday_batch(symbols, count=300)` | **必须** | 修复轮：给定标的当日 1 分钟 K，内部自行分块/限速。服务在冷启动、覆盖断档、连续空轮时调用 |
| `get_intraday_latest(symbols=None, count=3)` | 可选 | 稳态增量轮：尽量单请求返回全市场每只最新 `count` 根 |

### 7.2 实现取舍

- `get_intraday_batch`：复用 `get_minute` 的取数链（`bars.get(period='1m')` 当日窗口），再按标的保留最新 `count` 根。
- `get_intraday_latest`：**已实现全市场增量轮**。关键实测发现：eltaX 的 `bars.get` **支持批量 codes 且上限很高**（2000 只/请求 3.90s 且足额返回），与快照接口的 80 只硬上限完全不同。全市场 5578 只按 1000 分片共 6 批，一遍约 **11.7s** 拿到 16713 根（每只 3 根）。因此本方法真正实现增量轮，服务节奏为 `max(3s, 单轮耗时) ≈ 12s` —— 比仅修复轮（60s）快约 5 倍。

  > 早期曾判断"eltaX 无全市场批量端点、只能降级为仅修复轮"，该判断基于对 `minutes.history`/`minutes.today` 的考察（它们确实是逐标的、且是分时点）；改为考察 `bars.get` 的批量化后发现完全可行。

### 7.3 全市场压测结果（已完成）

| 轮次 | 数据量 | 覆盖 | 每只根数 | 耗时 |
|---|---|---|---|---|
| 修复轮 `get_intraday_batch(count=240)` | **1,337,040 行** | 5571 / 5578 = **99.9%** | **240**（min=max） | **22.5s** + 落盘 0.1s |
| 增量轮 `get_intraday_latest(count=3)` | 16,713 行 | 5571 只 | 3 | **11.0s** |

**判定：全市场当日分钟落盘可用。** 落盘代价可忽略（zstd parquet 13.7 MB / 0.1s），瓶颈在取数。修复轮占 60s 窗口约 38%；增量轮节奏约 12s。

> 上述耗时均为**收盘后**实测，盘中主站负载更高，建议观察一个完整交易日的成功率与限速情况。

---

## 8. ⑥ 连板梯队 / 封单榜 → ⛔ 无数据集槽位

eltaX **具备该能力**，且有三个独立来源：

| API | 说明 |
|---|---|
| `helpers.limit_ladder(codes=None, include_touched=False, count=None)` | 当前封板梯队（含可选"触板"） |
| `f10.limit_board_ladder(start_date, end_date, include_summary)` | 连板天梯 |
| `f10.limit_up_down_list(start_date, end_date, include_summary)` | 涨跌停名单 |

但面板的**六大数据集契约里没有这一项** —— 它属于**业务层**（异动/连板页面）的数据，不是数据源插件的数据集槽位。因此本插件**不声明**它。

若要接入，需先确认面板是否已有连板/异动页面及该页的数据来源，再单独开一个业务切片（超出"数据源插件"范畴）。

---

## 9. 单位与时区契约表（实现红线）

| 面板字段 | eltdx 来源 | 实测单位/口径 | provider 处理 |
|---|---|---|---|
| `symbol` | `full_code` / `code`+`exchange` | `sz000001` | 转 `000001.SZ`；**必须优先用显式 `exchange`** |
| `date`（日K） | `KlineBar.time` | Asia/Shanghai aware | 取 `.date()` |
| `datetime`（分钟） | `KlineBar.time` | 同上 | `astimezone(+08:00).replace(tzinfo=None)` → **北京墙钟 naive** |
| `open/high/low/close` | `KlineBar.*` | 元，不复权 | 直用 |
| `volume` | `volume_lots` / `total_hand` | **手** | 直用（面板同为手） |
| `amount` | `amount` | **元** | 直用 |
| `change_pct` | `change_pct` | **百分数制**（0.442478 = 0.4425%） | **/100** → 小数制 |
| `prev_close` | `pre_close_price` | 元 | 直用 |
| `timestamp`（快照） | `time_raw` | 当日 `HHMMSScc`（8 位，末 2 位百分秒） | 按北京墙钟当日还原毫秒 |
| `timestamp`（盘口） | `update_time_raw` | 当日 `HHMMSS`（**6 位**） | 同上（按长度自适应） |
| `total_shares`/`float_shares` | `zong_gu_ben`/`liu_tong_gu_ben` | **万股** | **x10000** → 股 |
| `ex_factor` | `hfq_scale`/`hfq_offset` | 二元组 | 按 §3.3 公式推导单事件比值 |

**符号识别红线**：裸 6 位代码不能直接推断交易所（`to_panel_symbol` 的兜底规则是"首位 6/9 → SH，其余 → SZ"），这会把北交所（`4xxxxx`/`8xxxxx`/`920xxx`）**误判为 `.SZ`**。所有涉及 `exchange` 的路径必须优先使用显式交易所字段。

**缺失字段一律 `None`**，不做启发式补全。日 K 的 `quote_ts`、快照的 `name`/`amplitude`/`turnover_rate` 等源未提供时即为空，由下游按契约处理。

---

## 10. 复现方法

```powershell
# 1) 依赖自检（插件卡片状态应为 ok (eltdx 3.2.2)）
cd D:\Users\kuangwei\PycharmProjects\tickflow-stock-panel\backend
.\.venv\Scripts\python.exe -c "import eltdx; print(eltdx.__version__)"

# 2) 契约测试（假 client，不连主站）
uv run --extra dev python -m pytest tests/test_eltdx_provider.py -q

# 3) 真机逐数据集试拉（需后端在 3018 运行）
cd ..
foreach ($ds in 'daily','realtime','minute','full_minute','depth5','financial','adj_factor') {
  $body = "{`"provider`":`"eltdx`",`"dataset`":`"$ds`",`"symbols`":[`"600519.SH`"]}"
  curl.exe -s -X POST -H "Content-Type: application/json" -d $body `
    "http://127.0.0.1:3018/api/settings/data-sources/test"
}

# 4) 除权因子对账（与本地表逐条比对）
cd backend
$code = @'
import sys, datetime as dt; sys.path.insert(0, ".")
import polars as pl
from app.plugins.eltdx.provider import EltDxProvider
local = pl.read_parquet("../data/adj_factor/all.parquet")
syms = local["symbol"].unique().to_list()
p = EltDxProvider()
df = p.get_adj_factors(syms, dt.date(2026,9,1), dt.date(2026,9,30)); p.close()
j = df.join(local.rename({"ex_factor":"local"}), on=["symbol","trade_date"])
j = j.with_columns((pl.col("ex_factor")-pl.col("local")).abs().alias("ad"))
print("对账", j.height, "条, 最大绝对误差", j["ad"].max())
'@
$code | .\.venv\Scripts\python.exe -
```

**升级 eltdx 后必须复测的口径**：`change_pct` 进制、`volume` 单位、`time_raw` 位数、`hfq_offset` 含义、`bars.get` 单页上限、`get_depth` 返回结构、`AdjustmentFactor` 字段名。

---

## 11. 判定汇总

| 判定 | 项 | 说明 |
|---|---|---|
| ✅ 已接入 | `daily`、`realtime`、`minute`、`full_minute`、`depth5`、`financial(shares)`、`adj_factor` | 7 个数据集，均通过契约测试与真机试拉 |
| ✅ 已压测 | `full_minute` | 修复轮 133.7 万行 / 99.9% / 22.5s；增量轮 16713 行 / 11s（收盘后实测） |
| ⚠️ 精度 | `adj_factor` | 与 fuyao 最大 0.0239% 偏差；需完全一致请用 fuyao |
| ⛔ 不接 | `metrics`、`income`、`balance_sheet`、`cash_flow` | 上游字段为不透明代码，口径不可确证 |
| ⛔ 无槽位 | 连板梯队 / 封单榜 | eltdx 有能力，面板六大数据集无此项 |
