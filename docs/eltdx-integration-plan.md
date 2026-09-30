# eltdx 数据源接入方案与实施记录

> 对应分支：`feat/eltdx-data-source`（已推送）
> 插件位置：`backend/app/plugins/eltdx/`（`runtime: python`，依赖 `eltdx>=3.2.2`）
> 能力核查依据见 [eltdx-capability-audit.md](./eltdx-capability-audit.md)
> 插件开发规范见 [plugin-development.md](./plugin-development.md)

---

## 1. 目标与范围

**目标**：把 eltdx（通达信在线行情协议客户端）接入为面板的可选数据源插件，覆盖面板的六大数据集契约。

**已交付**：7 个数据集。

| 数据集 | 状态 | 说明 |
|---|---|---|
| `daily` | ✅ | 不复权原始日 K |
| `realtime` | ✅ | 全市场快照 + 指数快照 |
| `minute` | ✅ | 1 分钟 K（真 OHLC） |
| `full_minute` | ✅ | 全量分钟修复轮（当日窗口批量） |
| `depth5` | ✅ | 五档盘口（封单/盘口深度） |
| `financial` | ⚠️ 部分 | 仅 `shares` 表 |
| `adj_factor` | ✅ | 除权因子（单事件比值） |

**明确不做**（理由见 §6）：`metrics` / `income` / `balance_sheet` / `cash_flow` 四张财务表；连板梯队（面板无数据集槽位）。

**设计约束**：不改动任何 `services/`、`api/` 代码 —— 插件机制的意义就是零集成改动；未声明的数据集自动回退 TickFlow。

---

## 2. 覆盖矩阵

| 面板数据集 | 契约方法 | eltdx 数据来源 | 关键换算 / 要点 |
|---|---|---|---|
| `daily` | `get_daily` / `iter_daily` | `bars.get(code, period='day', adjust=None)` | 不复权；`volume_lots`=手、`amount`=元 直用；`time` aware → 取 `.date()` |
| `realtime` | `get_realtime` / `get_realtime_indices` | `quotes.get_snapshots(codes)` | `change_pct` **百分数→/100**；`total_hand`=手；`time_raw` 8 位 `HHMMSScc` |
| `minute` | `get_minute` | `bars.get(code, period='1m')` | **必须用 bars 而非 minutes.history**；`datetime` 转**北京墙钟 naive** |
| `full_minute` | `get_intraday_batch` / `get_intraday_latest` | 同上（当日窗口 / 批量最新 N 根） | 增量轮走 `bars.get` 批量（1000 只/片），实测全市场 ~11.7s |
| `depth5` | `get_depth_batch` | `quotes.get_depth(codes)` | 各 5 档；**量为 0 须保留**；失败**抛异常**不回退 |
| `financial` | `get_financials` | `corporate.finance_batch(codes)` | 仅 `shares`；股本**万股→x10000**；`period_end` 取 `updated_date` |
| `adj_factor` | `get_adj_factors` | `corporate.adjustment_factors(code)` | `(scale, offset)` → **单事件比值**（公式见 §4.4） |
| （代码表） | 内部使用 | `codes.all_a_shares()` / `all_indices()` | 代码格式转换 `sz000001` ↔ `000001.SZ` |

---

## 3. 架构

### 3.1 文件职责

```
backend/app/plugins/eltdx/
├── plugin.yaml        31 行   清单：runtime/entry/check/datasets/install_hint/合规提示
├── requirements.txt    3 行   依赖声明（eltdx>=3.2.2），设置页「安装依赖」读取
├── __init__.py         1 行
├── client.py         358 行   适配层：连接池 / 代码双向转换 / 自管分页 / 软失败语义
└── provider.py       993 行   契约层：字段映射 / 单位换算 / 试拉 / 可用性自检
```

**分层原则**：`client.py` 只与 eltdx 打交道（负责"怎么拿到数据"），`provider.py` 只与面板契约打交道（负责"数据长什么样"）。口径换算全部集中在 provider，便于对账与审计。

### 3.2 `client.py` —— 适配层

| 职责 | 说明 |
|---|---|
| 连接池 | 惰性建连 `TdxClient(server_count, connections_per_server, timeout)`；`close()` 由 loader 重建注册表时调用；建连动作加锁 |
| **代码双向转换** | `to_panel_symbol`（`sz000001` → `000001.SZ`）/ `to_eltdx_code`（反向）。**注意**：裸 6 位代码的交易所推断不可靠（北交所），涉及 `exchange` 的路径必须优先用显式交易所 |
| **自管分页** | eltdx 单页上限 800，且 `all_pages=True` 达 `max_pages` 会**抛异常**；故按 `start` 逐页取 + 空页终止 |
| 分批并发 | `iter_bars_batches` 按 `batch_size` 有界分批 + 线程池；批次内结果排序保证可复现 |
| 软失败语义分级 | 快照类 → 返回 `[]`（不阻断轮询线程）；盘口类 → **抛异常**（由服务按批隔离） |

### 3.3 `provider.py` —— 契约层

方法清单：

```
get_daily / iter_daily                    日K（含全市场历史同步优先消费的有界分批路径）
get_minute                                1 分钟 K
get_intraday_batch / get_intraday_latest  全量分钟（修复轮 / 稳态增量轮）
get_realtime / get_realtime_indices       全市场快照 / 指数快照
get_depth_batch                           五档盘口
get_financials                            财务（仅 shares）
get_adj_factors                           除权因子
test_dataset                              设置页「试拉测试」
close                                     资源释放
```

### 3.4 并发与限速参数（均可用环境变量覆盖）

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `ELTDX_SERVER_COUNT` | 4 | 选用的主站数量 |
| `ELTDX_CONNECTIONS_PER_SERVER` | 4 | 每主站 TCP 连接数（合计 16 slot） |
| `ELTDX_TIMEOUT` | 8.0 | 单请求超时（秒） |
| `ELTDX_DAILY_BATCH` | 200 | 日 K 每批标的数 |
| `ELTDX_MINUTE_WORKERS` | 8 | 分钟取数并发 |
| `ELTDX_MINUTE_MAX_BARS` | 12000 | 单标的分钟根数上限（≈50 交易日） |
| `ELTDX_SNAPSHOT_WORKERS` | 4 | 快照分片并发 |
| `ELTDX_ADJ_WORKERS` | 8 | 除权因子并发 |
| `ELTDX_ADJ_LOOKBACK_BARS` | 120 | 除权基准价回看余量（会随请求区间放大） |
| `ELTDX_FINANCE_BATCH` | 75 | `finance_batch` 每批标的数（对齐官方默认） |

常量：`_SNAPSHOT_BATCH = 800`（快照单次请求代码数）、`_MAX_PAGE_SIZE = 800`（eltdx 单页上限）。

> 调参建议：全市场日 K 首次同步对主站压力较大，建议保守起步（默认 16 slot），观察成功率后再考虑提高；eltdx 官方给出的"中等并发"参考为 4 台 x 8 连接 = 32 slot。

---

## 4. 实施顺序与各步验证手段

按**风险从低到高**推进，每步独立验证后再进入下一步：

### 4.1 `daily` + `realtime`（基础）
- **验证**：与本地经 fuyao 写入的日 K 主档**逐字段比对** → OHLC/量额完全一致
- 这一步建立了后续所有工作的口径基准

### 4.2 `minute` + `full_minute`
- 先实测确认 eltdx 的两个候选入口（`bars.get(period='1m')` vs `minutes.history`），确认前者才是真 OHLC
- **验证**：分钟累计量额 vs 同日日 K → 误差 0.0000% 量级（240 根/日）

### 4.3 `depth5`
- **验证**：5 档结构 + 买卖价序自检 + `0` 值保留 + 失败抛异常语义

### 4.4 `financial`（缩范围为 `shares`）
- 先读面板财务 schema（`data/financials/*`）与 `share_capital` 消费逻辑，再决定映射范围
- **验证**：股本与实况核对（平安银行 194.06 亿股）；端到端经 `apply_historical_float_shares` 产出换手率；与既有列多源合并保留原值

### 4.5 `adj_factor`（最高风险，最后做）
- 先做**离线对账**：推导公式 → 与本地 `data/adj_factor` 表逐条比对
- **换算公式**：
  ```
  div       = (cur.hfq_offset - prev.hfq_offset) / cur.hfq_scale
  ex_factor = (cur.hfq_scale / prev.hfq_scale) x prev_close / (prev_close - div)
  ```
  `prev_close` = 事件日**前一交易日**的不复权收盘
- **验证**：32/32 条对账吻合，最大相对误差 0.0239%（详见核查文档 §3.5）
- 对账未通过则不接入（宁可回退 TickFlow，也不接错口径）

### 4.6 契约测试（贯穿）
每个切片都补契约测试，范本 `backend/tests/test_fuyao_provider.py`，**不依赖真实网络**（假 client 注入）。当前 **112 个**测试用例。

---

## 5. 交付清单

| 文件 | 行数 |
|---|---|
| `backend/app/plugins/eltdx/provider.py` | 993 |
| `backend/app/plugins/eltdx/client.py` | 358 |
| `backend/app/plugins/eltdx/plugin.yaml` | 31 |
| `backend/app/plugins/eltdx/requirements.txt` | 3 |
| `backend/app/plugins/eltdx/__init__.py` | 1 |
| `backend/tests/test_eltdx_provider.py` | 1516（81 个 test 函数） |
| `docs/plugin-development.md` | +51（eltdx 条目与口径要点） |
| `docs/eltdx-capability-audit.md` | 本文档配套 |
| `docs/eltdx-integration-plan.md` | 本文件 |

**提交历史**（8 个提交，已快进合并入 `main` 并推送至 `origin`）：

```
3a43483  docs(eltdx): 补充全市场压测结果（修复轮 133.7 万行/22.5s，增量轮 11s）
dbefab1  docs(eltdx): 补充能力核查结论与接入方案文档
a9b2247  fix(plugins): 修复 eltdx 全市场快照分片超限导致 0 行 + 实现全量分钟增量轮
50490c3  feat(plugins): eltdx 接入 adj_factor(除权因子, 单事件比值推导)          5 files, +704 -19
843c1a3  feat(plugins): eltdx 接入 financial(shares 表)并修复北交所 symbol 误标   5 files, +403 -12
a4463ed  feat(plugins): eltdx 接入 depth5(五档盘口/封单/盘口深度)                 5 files, +275 -10
4fbe377  feat(plugins): eltdx 接入 minute 与 full_minute(1分钟K + 全量分钟修复轮) 5 files, +409 -38
07112dd  feat(plugins): 新增 eltdx(通达信)数据源插件(daily/realtime)              7 files, +1138
```

**质量门禁**：插件契约测试 **112 passed** ｜ 全量后端 2570 passed / 6 skipped ｜ `ruff check` clean ｜ `ruff format --check` clean。

---

## 6. 未完成项与取舍（如实列出）

### 6.1 全市场压测结果（已完成）

| 轮次 | 数据量 | 覆盖 | 耗时 | 说明 |
|---|---|---|---|---|
| **修复轮** `get_intraday_batch(count=240)` | **1,337,040 行** | 5571 / 5578 = **99.9%** | **22.5s** + 落盘 0.1s | 每只 **min=max=240 根**；zstd parquet 13.7 MB |
| **增量轮** `get_intraday_latest(count=3)` | 16,713 行 | 5571 只 | **11.0s** | 每只 3 根 |

**结论：可用于盘中落盘。** 修复轮占 60s 窗口约 38%；增量轮节奏约 12s，优于仅修复轮的 60s。取数耗时是瓶颈，落盘可忽略（0.1s）。

### 6.1b 盘中真实表现（2026-09-30 完整交易日实测，已销项）

在**真实交易时段**跑了完整一天（HTTP 网关模式），结果与收盘后压测一致：

| 项 | 盘中实测 | 结论 |
|---|---|---|
| 增量轮耗时 | 中位 **~11.5s**（区间 10.4~23s） | 与收盘后 11.0s 基本一致，**未因盘中负载劣化** |
| 覆盖率 | **5572 只 / 轮**（100% 稳定） | 无标的丢失 |
| 成功率 | **0 错误**（午后 100+ 轮） | 无失败轮 |
| 冷启动修复轮 | 首轮 `full` 约 94s（含铺底当日全量），随后自动转 `increment` | 符合设计 |
| **depth5 定版** | 15:02 准时生成 `data/depth5/date=YYYY-MM-DD/part.parquet` | 80 只（33 封涨停 / 5 封跌停） |
| **封板 `0` 语义** | `ask1_vol==0` 恰为 33 只、`bid1_vol==0` 恰为 5 只，与 `sealed_up/sealed_down` **完全一致** | 证明 0 未被当缺失值丢弃 |
| 分时数据量 | 全天累计 **117.9 万行** | — |

> depth5 盘中轮询依赖「连板梯队监控」开关（`limit_ladder_monitor_enabled`）与实时行情开关
> **同时开启**；默认关闭时轮询不启动，`data/depth5/` 会一直为空（面板显示为「需适配」）。
> 另外它只拉**涨跌停名单**（当日约 70~80 只），不是全市场 —— 这是设计（盘口只服务于封板判定）。

**盘中事故与修复**（同日实测暴露，均已提交并加回归测试）：
1. eltaX 运行时崩溃(`runtime command channel is closed`)后**永不自愈** → 加 `_call` 自愈(重建池+重试一次)；
2. 并发 2 线程过低 → 提到 8(实测 **>=12 线程锁死在 ~10.1s 的地板**, 该地板是上游 7709 串行处理速率, 本端并发无法突破)；
3. 改 HTTP 网关后暴露三个接入坑：**端口耗尽**(TIME_WAIT 3269/上限 16384)、**keep-alive 竞态**(uvicorn 5s vs 轮询 6~12s)、**K线 time 为 ISO8601 字符串**(旧解析静默返回 None → 全市场取数 **0 行**)。

### 6.2 其余未完成项

| 项 | 状态 | 说明 |
|---|---|---|
| `metrics` / `income` / `balance_sheet` / `cash_flow` | ⛔ 不接 | `f10.finance_report` 返回不透明 `T***` 代码，包内无代码→名称字典；面板合并逻辑用 `drop_nulls().last()` **无法用 null 修正错误值** → 口径不明确不接 |
| 连板梯队 / 封单榜 | ⛔ 无槽位（按裁定留槽位即可，不做） | eltdx 有三个来源（`helpers.limit_ladder` / `f10.limit_board_ladder` / `f10.limit_up_down_list`），但面板六大数据集无此项，属业务层 |
| `adj_factor` 精度 | ⚠️ 0.0239% 偏差 | `hfq_offset` 累计浮点/取整精度所致；需与 fuyao 完全一致时请用 fuyao |
| `trades` / `auctions` / `money_flow` | 未接入 | 面板无对应数据集契约 |
| ~~盘中真实表现~~ | ✅ **已销项**(2026-09-30) | 完整交易日实测：增量轮 ~11.5s、5572 只、0 错误；depth5 于 15:02 准时定版。详见 §6.1b |
| HTTP 网关为**默认传输** | ✅ 已切换 | 进程隔离；网关需单独启动(`eltdx-http`)。**不自动回退**进程内 —— 网关不可达时按不可用处理(设置页会展示原因) |

**依赖管理注意**：eltdx 由设置页「安装依赖」按钮装入 `backend/.venv`，**不在 `uv.lock`**。若执行 `uv sync --frozen`，需重新点击「安装依赖」（这是面板插件机制的既定约定，与 fuyao/stocksdk 一致）。

---

## 7. 风险与回滚

| 风险 | 缓解 |
|---|---|
| 上游 eltdx 升级导致字段/单位变化 | 口径全部集中在 provider 且注释标"实测基线"；升级后按核查文档 §10 的复测清单逐项验证 |
| 全市场同步对主站压力 | 连接池保守默认（16 slot）+ 有界分批 + 环境变量可调；单标的失败软失败不拖垮整批 |
| 财务/复权口径错误（静默污染） | 除权因子经 32/32 对账才接入；三大报表宁可不接；财务只接字段名明确的 `shares` |
| eltdx 许可 | **Research-Only License**（仅限研究，禁用商业用途），已在 `plugin.yaml` 与文档显式标注，对齐 stocksdk 的处理方式 |
| 回滚 | 各数据集独立声明；在设置页切回原数据源即可（未声明项自动回退 TickFlow）。插件整体卸载只需删除 `backend/app/plugins/eltdx/` 目录 |

---

## 8. 复现与验证命令

```powershell
cd D:\Users\kuangwei\PycharmProjects\tickflow-stock-panel\backend

# 依赖（已在 .venv 中；重装用）
uv pip install --python .\.venv\Scripts\python.exe "eltdx>=3.2.2"

# 契约测试
uv run --extra dev python -m pytest tests/test_eltdx_provider.py -q

# lint / format
uv run --extra dev ruff check app/plugins/eltdx tests/test_eltdx_provider.py
uv run --extra dev ruff format --check app/plugins/eltdx tests/test_eltdx_provider.py

# 全量回归
uv run --extra dev python -m pytest -q

# 起后端（3018）后，设置页 → 数据源 → eltdx 卡片 → 试拉测试
# 或命令行逐数据集试拉见 eltdx-capability-audit.md §10
```

---

## 9. 后续可选项（需另行决策）

1. **`full_minute` 压测**：确认全市场当日窗口的实际耗时，决定是否启用修复轮与是否保留该数据集声明。
2. **连板梯队业务切片**：需先确认面板是否已有连板/异动页面及其数据来源，再决定接入形态（这不是数据源插件的范畴）。
3. **`etdx-http` 网关路线**：eltdx 3.2.2 自带 `eltdx-http`（`POST /rpc` JSON-RPC + WebSocket + 实时订阅，需 `pip install "eltdx[http]"`）。当前**未采用** —— Python 插件直连少一跳、无额外进程。若将来需要非 Python 客户端调用，可另评估该路线（注意其是 RPC 信封形态，不满足面板 `custom-data-source.md` 的 REST 契约，无法用 YAML 声明式接入）。
