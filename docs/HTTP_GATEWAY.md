# eltdx HTTP 网关

`ELTDX_TRANSPORT=http`（**默认模式**）下，插件不直接连行情主站，而是连一个独立的
`eltdx-http` 网关进程。后端只是这个网关的 HTTP 客户端。

## 为什么需要网关

进程内直连（`inproc`）有两个结构性风险，来自 2026-09-30 盘中事故的直接结论：

1. **运行时是进程级单例**。el taX 的内部运行时（Rust）一旦进入 closed 状态
   （上游一次 `response timed out during connect` 就能触发），**本进程内所有数据集**
   （行情 / 分钟 / 盘口 / 代码表 / 财务）会一起失效。eltaX 未暴露健康状态字段，
   只能靠错误文本识别并重建连接池 —— 且**不保证自愈**。
2. **多进程互相踩踏**。服务运行期间，任何独立 eltdx 脚本都会与服务争夺同一个 7709
   运行时。实测曾把服务连接打死（单分钟 29434 次 `runtime command channel is closed`）。

改用网关后：连接池与运行时**归网关所有**，后端重启不影响 eltdx；网关崩溃可独立重启；
外部脚本只能走 HTTP 这一个入口。实测**性能无损耗**（1000 只 `bars.get` 2692ms
vs 进程内 2675ms）。

## 快速开始

### 1. 安装网关依赖

网关是 eltdx 的可选 extra，不在默认依赖里：

```bash
cd backend
uv pip install --python .venv/Scripts/python.exe "eltdx[http]>=3.2.2"
```

> 注意：`uv sync --frozen` 会清掉未记录进 `uv.lock` 的包。执行后可能需要重新安装
> 本插件的依赖（`app/plugins/eltdx/requirements.txt`）。

### 2. 启动网关

```bash
eltdx-http
```

默认监听 `127.0.0.1:8000`。验证：

```bash
curl http://127.0.0.1:8000/health
```

### 3. 确认插件转为可用

后端日志出现这行即成功：

```
内置插件 eltdx 已注册 (runtime=python)
```

设置 → 数据源 Beta 里 eltdx 卡片不再灰显。

## 协议口径

- 端点：`POST {base_url}/rpc`
- 载荷：`{"id": <n>, "method": <m>, "params": {...}}`
- 成功：`{"id":..,"ok":true,"result":<...>}`
- 失败：`{"ok":false, "error":<...>}`
- HTTP 状态码：`400` 参数错 / `404` 未知方法 / `502` 主站错 / `500` 网关内部错

方法名与 Python API 同名，常用：

| 方法 | 用途 |
| --- | --- |
| `codes.all_a_shares` | A 股代码表 |
| `codes.all_indices` | 指数代码表 |
| `bars.get` | K 线（日 K / 1 分钟） |
| `bars.get_intraday_batch` | 全量分钟修复轮 |
| `quotes.get_snapshots` | 全市场实时快照 |
| `quotes.get_depth` | 五档盘口 |
| `corporate.adjustment_factors` | 除权因子 |
| `corporate.finance_batch` | 股本（shares） |
| `f10.finance_report` | 资产负债表 / 现金流量表 |

返回的 dataclass 会序列化为**同名字段的 JSON**（`volume_lots` / `amount` /
`buy_levels` / `time` 等与进程内对象一致），客户端再包装成带同名属性的轻量对象，
使 provider 的解析逻辑无需区分传输方式。

## 配置项

全部通过 `.env` 传给后端：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ELTDX_TRANSPORT` | `http` | `http` 或 `inproc` |
| `ELTDX_HTTP_URL` | `http://127.0.0.1:8000` | 网关地址 |
| `ELTDX_HTTP_TIMEOUT` | `120.0` | 网关请求超时（秒）。全市场批量（1000 只）本身 ~2.7s，需留余量 |
| `ELTDX_HTTP_WORKERS` | `8` | 并发工作线程 |
| `ELTDX_CODE_TTL` | `300.0` | 代码表缓存 TTL（秒），0 = 每轮回源 |
| `ELTDX_CODE_STALE_MAX` | `600.0` | 降级清单的陈旧度上界（秒），回源失败时最多沿用多久前的清单 |

网关进程本身的并发/服务器数由 `ELTDX_SERVER_COUNT`、`ELTDX_CONNECTIONS_PER_SERVER`
控制 —— 这些是**网关侧**参数，通过网关启动环境传入，不由后端 `.env` 决定。

## 两种模式怎么选

| | `http`（默认） | `inproc` |
| --- | --- | --- |
| 额外进程 | 需要起网关 | 不需要 |
| 崩溃影响面 | 只影响 eltdx 自己 | Rust 内核崩溃会牵连全部数据集 |
| 多进程 | 外部脚本只能走 HTTP，不会踩踏 | 独立脚本会与服务争抢运行时 |
| 适用 | 长期运行、生产 | 诊断、离线、低能力档位 |

`inproc` 启动时后端会打 WARNING，提示存在上述风险 —— 这是预期噪音，不是错误。

## 排查

**卡片灰显 / 日志报「网关不可达」**

```
eltaX HTTP 网关不可达(http://127.0.0.1:8000): ... WinError 10061
```

按序检查：网关进程是否在跑 → `curl http://127.0.0.1:8000/health` 是否有响应 →
`ELTDX_HTTP_URL` 是否与网关实际监听地址一致。

**切了 inproc 后想切回 http**

改 `.env` 的 `ELTDX_TRANSPORT=http`，**重启后端**（该变量在插件注册时读取，
热重载不生效）。

## 许可

eltdx 为 **Research-Only License**：仅供研究使用，**禁止商业用途**。数据来自通达信
公开行情主站，启用即视为使用者知悉并自行承担合规责任。