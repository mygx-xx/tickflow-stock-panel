"""eltdx **HTTP 网关**传输层 —— 与进程内 ``TdxClient`` 完全隔离。

为什么需要它(2026-09-30 盘中事故的直接结论)
--------------------------------------------------------------------------
进程内直连 eltdx 有两个结构性风险:

1. **运行时是进程级单例**: eltaX 的内部运行时(Rust)一旦进入 closed 状态
   (上游一次 ``response timed out during connect`` 即可触发), 本进程内**所有**
   数据集(行情/分钟/盘口/代码表/财务)会一起失效, 且 eltaX 未暴露任何健康状态
   字段可供探测, 只能靠错误文本识别并重建连接池。
2. **多进程互相踩踏**: 服务运行期间, 任何独立的 eltdx 脚本都会与服务争夺同一个
   7709 运行时, 实测直接把服务的连接打死(10:35 单分钟 29434 次
   ``runtime command channel is closed``)。

改用 ``eltdx-http`` 网关(独立进程, 见 eltdx 的 ``docs/HTTP_GATEWAY.md``)后:

* 连接池与运行时**归网关所有**; 后端只是 HTTP 客户端 —— 后端重启不影响 eltdx 连接。
* 网关崩溃可**独立重启**, 不必重启面板。
* 外部脚本无法再与服务互相踩踏(只有 HTTP 一个入口)。
* 实测**性能无损耗**: 1000 只 bars.get 2692ms(进程内 2675ms);
  全市场 5578 只并发 ~10.1s(与进程内同为上游物理地板)。

调用口径
--------------------------------------------------------------------------
* 端点: ``POST {base_url}/rpc``, 载荷 ``{"id": <n>, "method": <m>, "params": {...}}``。
* 方法名与 Python API 同名(``bars.get`` / ``quotes.get_snapshots`` / ...)。
* 成功: ``{"id":..,"ok":true,"result":<...>}``; 失败: ``ok:false`` + ``error``;
  HTTP 层面 400 参数错 / 404 未知方法 / 502 主站错 / 500 网关内部错。
* 返回的 dataclass 会被序列化成 **同名字段的 JSON**(实测 ``volume_lots`` /
  ``amount`` / ``buy_levels`` / ``time`` 等与进程内对象一致), 故本模块把 JSON
  **再包装成带同名属性的轻量对象**, 使 provider.py 的解析逻辑无需改动。
"""

from __future__ import annotations

import contextlib
import http.client
import json
import logging
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

from app.market_time import cn_today
from app.plugins.eltdx.client import (
    _MAX_PAGE_SIZE,
    to_eltdx_code,
    to_panel_symbol,
)

logger = logging.getLogger(__name__)

DEFAULT_HTTP_URL = "http://127.0.0.1:8000"
# 网关侧上游超时默认 8s; 但全市场批量(1000 只)本身要 ~2.7s, 大请求要留足余量。
DEFAULT_HTTP_TIMEOUT_S = 120.0

# 代码表缓存 TTL(秒)。依据(2026-09-30 复核实测, 网关 3.2.2): codes.all_a_shares
# 每次回源约 1.48~1.64s(连续 5 次), 缓存命中仅 0.030ms —— 差约 4.9 万倍, 故按 TTL 缓存。
# 代码表当日几乎不变(实测连续两次集合完全一致, 5578 只无差异), 缓存语义安全。
# 取 5 分钟: 覆盖数十轮刷新, 同时把盘中新上市/退市的可见滞后限制在 5 分钟内。
# 另按「北京日期」跨日强制失效(见 all_a_shares), 不依赖 TTL。
# 注: 早期记载的「每次 2.1~6.5s(中位 4.3s)、占单轮七成以上」为接入网关前的旧观测,
# 现已不复现, 不应再据此评估收益。
# 可用 ``ELTDX_CODE_TTL`` 覆盖(见 provider._make_transport); 设为 0 等价于"每轮都回源"。
DEFAULT_CODE_TTL_S = 300.0

# 兜底清单(失败时沿用当日旧清单)的**陈旧度上界**(秒)。旧清单只用于决定"拉哪些
# 标的", 但无限陈旧仍不妥: 交易时段内的新股/退市/代码变更会一直不可见。故给一个
# 宽松上界(默认 2xTTL = 10 分钟); 超过则宁可本轮返回空, 让下一轮重新取干净的清单。
# 取 2xTTL 的理由: 单次回源约 1.5s, 允许"连续两次回源失败"仍能降级服务, 同时把
# 最坏陈旧度控制在 10 分钟内(而非此前实测可达的数小时)。
DEFAULT_CODE_STALE_MAX_S = 2 * DEFAULT_CODE_TTL_S


def _code_entry_symbol(item: Any) -> str | None:
    """代码表条目 → 面板 symbol; 无法确定交易所时返回 None(**不推断**)。

    实测(2026-09-30, 网关 3.2.2): ``codes.all_a_shares`` 返回**全部为 str** 且
    **100% 带交易所前缀** —— 5578 条实测分布 ``sh`` 2320 / ``sz`` 2907 / ``bj`` 351,
    无裸 6 位代码、无 ``{"code","exchange"}`` 形态。故 dict 分支是**纵深防御**
    (JSON 序列化形态若变化时的兜底), 当前上游走不到。

    不推断的理由: ``to_panel_symbol`` 对裸 6 位按首位猜(6/9→SH, 其余→SZ), 会把
    沪市指数 000001 猜成 ``000001.SZ``、北交所 920xxx 猜成 ``920xxx.SH``。
    该退化仅在条目**缺失交易所信息**时发生 —— 当前网关版本未观测到此情形,
    因此这是异常路径的防护而非活跃缺陷。
    """
    if isinstance(item, str):
        raw = item.strip()
        # 已带交易所标识: 前缀式(sz000001, 8 位) 或后缀式(000001.SZ)
        if "." in raw or len(raw) == 8:
            return to_panel_symbol(raw)
        return None
    ex = getattr(item, "exchange", None)
    num = getattr(item, "code", None)
    if ex and num:
        return to_panel_symbol(f"{ex}{num}")
    return None


class _Obj:
    """把 JSON dict 包装成"带同名属性"的对象, 以复用 provider 的属性访问逻辑。

    例: ``{"volume_lots": 707.0}`` → ``obj.volume_lots == 707.0``。
    缺失的键返回 ``None``(与进程内对象的 Optional 字段语义一致)。
    """

    __slots__ = ("_d",)

    def __init__(self, d: dict[str, Any]) -> None:
        self._d = d

    def __getattr__(self, name: str) -> Any:
        try:
            return self._d[name]
        except KeyError:
            return None

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return f"_Obj({self._d!r})"


def _wrap(value: Any) -> Any:
    """递归把 dict 包装成 _Obj, list 逐项包装, 其余原样。

    注意: **日期/时间保持字符串**。provider 侧已经用过 ``_as_datetime`` /
    ``_bar_date`` 处理 ISO8601(实测网关返回 ``2026-09-30T10:50:00+08:00``),
    这里不擅自转 datetime, 避免与进程内路径(double-aware)行为不一致。
    """
    if isinstance(value, dict):
        return _Obj({k: _wrap(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_wrap(v) for v in value]
    return value


class _GatewayError(RuntimeError):
    """网关返回的业务错误(ok=false)或 HTTP 层错误。"""


class _ConnPool:
    """极简 HTTP/1.1 keep-alive 连接池(线程安全)。

    为什么不用 urllib: 它**每个请求新建 TCP 连接**, 高频轮询下连接进入 TIME_WAIT
    并迅速耗尽本机动态端口 —— 实测触发
      ``WinError 10055 由于系统缓冲区空间不足或队列已满``
    (当时 TIME_WAIT 3269 条, Windows 动态端口仅 16384)。复用连接后 TIME_WAIT 不再增长。

    **空闲连接会过期**: 网关(uvicorn)默认 ``timeout_keep_alive=5s``, 超时即单方面
    关闭连接。而面板轮询间隔是 6~12s > 5s, 意味着**几乎每次复用都撞上已死连接**
    (实测 WinError 10053)。故这里给空闲连接记录"闲置起始时刻", 超过
    ``_IDLE_TTL_S`` 直接丢弃不用(避免明知已死还去发请求)。

    语义: ``acquire()`` 取一条**新鲜**连接, 用完 ``release()`` 归还;
    出错则销毁(不归还), 由下一次 acquire 重建。
    """

    # 小于网关的 keep-alive 超时(uvicorn 默认 5s), 留出安全余量
    _IDLE_TTL_S = 4.0

    def __init__(self, *, host: str, port: int, size: int, timeout: float) -> None:
        self._host = host
        self._port = port
        self._size = max(1, int(size))
        self._timeout = float(timeout)
        self._idle: list[tuple[http.client.HTTPConnection, float]] = []
        self._lock = threading.Lock()
        self._created = 0  # 同时存活的连接数(含借出中)

    def acquire(self) -> http.client.HTTPConnection:
        now = time.monotonic()
        stale: list[http.client.HTTPConnection] = []
        with self._lock:
            while self._idle:
                conn, since = self._idle.pop()
                if now - since <= self._IDLE_TTL_S:
                    return conn
                stale.append(conn)
                self._created = max(0, self._created - 1)
            if self._created < self._size:
                self._created += 1
                return http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
        for c in stale:  # 锁外关闭, 避免持锁做 IO
            with contextlib.suppress(Exception):
                c.close()
        # 池已满: 让调用方新建一条临时连接(不计数), 用完即弃。
        # 这样并发尖峰不会阻塞, 也不会让常驻连接数无限增长。
        return http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)

    def release(self, conn: http.client.HTTPConnection, *, reusable: bool) -> None:
        if not reusable:
            with self._lock:
                self._created = max(0, self._created - 1)
            with contextlib.suppress(Exception):
                conn.close()
            return
        with self._lock:
            if len(self._idle) < self._size:
                self._idle.append((conn, time.monotonic()))
                return
            self._created = max(0, self._created - 1)
        with contextlib.suppress(Exception):
            conn.close()

    def close_all(self) -> None:
        with self._lock:
            conns, self._idle, self._created = self._idle, [], 0
        for c, _ in conns:
            with contextlib.suppress(Exception):
                c.close()


class HttpTransport:
    """eltaX HTTP 网关客户端, 与 :class:`~app.plugins.eltdx.client.EltDxClient` 同接口。

    仅实现 provider 实际用到的方法; 每个方法签名与进程内版本保持一致, 便于
    provider 无感切换(见 ``ELTDX_TRANSPORT``)。
    """

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_HTTP_URL,
        timeout: float = DEFAULT_HTTP_TIMEOUT_S,
        max_workers: int = 8,
        pool_size: int | None = None,
        code_ttl_s: float = DEFAULT_CODE_TTL_S,
        code_stale_max_s: float = DEFAULT_CODE_STALE_MAX_S,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self._max_workers = max(1, int(max_workers))
        # 代码表缓存 TTL(秒)。0 表示不缓存(每轮都回源), 便于排障或需要最小时滞时用。
        self._code_ttl_s = max(0.0, float(code_ttl_s))
        # 兜底清单陈旧度上界(秒)。<=0 表示不作上界限制(仅受"当日"约束)。
        self._code_stale_max_s = float(code_stale_max_s)
        self._lock = threading.Lock()
        self._seq = 0
        # 代码表缓存: 由 quote 线程与 minute-refresh 线程共享(provider 为模块级单例),
        # 故缓存读写与「单飞」都在同一把条件变量下协调。**网络请求严格在锁外**
        # (见 all_a_shares), 避免持锁做 IO 阻塞另一个线程的实时路径。
        #
        # 为什么用 Condition 而不是 Lock + sleep 轮询:
        #   轮询实现有两个结构性缺陷 —— 等待者超时后只能**递归重入**取数(无层数上限,
        #   持有者一旦卡死等待者即永久卡死), 且无法区分「同世代成功/失败」,
        #   于是失败时每个等待者各自再拉一次(单飞退化为 N 次并发回源)。
        #   条件变量让等待者被精确唤醒、且能读到该世代的结果, 两处缺陷一并消除。
        self._code_cv = threading.Condition()
        self._code_symbols: list[str] | None = None
        self._code_day: date | None = None
        self._code_at: float = 0.0
        # 单飞: generation 标识「在途世代」, None 表示当前无人在回源。
        self._code_generation = 0
        self._code_inflight: int | None = None
        # 最近一次回源结果(只保留**一个**槽位)。等待者只关心「自己进入时正在拉的那
        # 一个世代」, 故不需要按世代累积 —— 累积会让字典随运行时长无界增长。
        self._code_last_gen: int | None = None
        self._code_last_result: tuple[str, list[str] | None] | None = None
        # 数据新鲜度(与 _code_at 分离): 记录清单**真实取回**的时刻。
        # 为什么必须与 _code_at 分开: _code_at 是"缓存有效期"的起点(降级时会刷新,
        # 让旧清单再顶一轮), 而陈旧度上界必须看数据**真实年龄** —— 若共用 _code_at,
        # 每次降级都把年龄归零, 陈旧上界将永不触发(实测: 上界 1s 连续 4 轮仍返回数据)。
        self._code_data_at: float = 0.0
        host, port = self._host_port()
        self._pool = _ConnPool(
            host=host,
            port=port,
            size=max(1, int(pool_size or self._max_workers)),
            timeout=self._timeout,
        )

    def _host_port(self) -> tuple[str, int]:
        from urllib.parse import urlparse

        u = urlparse(self._base_url)
        return (u.hostname or "127.0.0.1", int(u.port or 80))

    # ---- 基础调用 -------------------------------------------------------

    def _next_id(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    def _request(self, method_http: str, path: str, payload: bytes | None) -> Any:
        """走 keep-alive 池发一次 HTTP 请求, 返回解析后的 JSON。

        连接出错即销毁(不归还)。**幂等重试一次**: 即便做了空闲过期判断, 连接仍可能
        在"取出到发出"之间被网关关闭(keep-alive 竞态, 实测 WinError 10053);
        重试用的是全新连接, 因此能真正恢复。只重试一次, 避免上游故障时打成风暴。
        """
        last_err: Exception | None = None
        for attempt in range(2):
            conn = self._pool.acquire()
            headers = {"Content-Type": "application/json"} if payload is not None else {}
            reusable = False
            try:
                conn.request(method_http, path, body=payload, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                status = resp.status
                # 服务端未要求关闭即可复用(HTTP/1.1 默认 keep-alive)
                reusable = resp.will_close is False and status < 500
                if status >= 400:
                    # 4xx 是业务错误, 重试无意义
                    raise _GatewayError(
                        f"HTTP {status} on {path}: {raw.decode('utf-8', 'replace')[:200]}"
                    )
                return json.loads(raw.decode("utf-8"))
            except _GatewayError:
                raise
            except Exception as e:
                last_err = e
                if attempt == 0:
                    logger.debug("eltdx-http %s 连接失效, 换新连接重试: %s", path, str(e)[:80])
                    continue
            finally:
                self._pool.release(conn, reusable=reusable)
        raise _GatewayError(f"{type(last_err).__name__} on {path}: {str(last_err)[:150]}")

    def _rpc(self, method: str, params: dict[str, Any]) -> Any:
        """发一次 JSON-RPC 调用, 返回已包装的 result; 失败抛 :class:`_GatewayError`。"""
        payload = json.dumps(
            {"id": self._next_id(), "method": method, "params": params},
            ensure_ascii=False,
        ).encode("utf-8")
        try:
            body = self._request("POST", "/rpc", payload)
        except _GatewayError as e:
            raise _GatewayError(f"{e} [method={method}]") from e

        if not body.get("ok"):
            err = body.get("error") or {}
            raise _GatewayError(
                f"{err.get('type', 'Error')} on {method}: {str(err.get('message', ''))[:150]}"
            )
        return _wrap(body.get("result"))

    def health(self) -> dict[str, Any]:
        """``GET /health``: 网关存活与版本(供 availability/诊断使用)。"""
        try:
            return self._request("GET", "/health", None)
        except _GatewayError as e:
            raise _GatewayError(f"gateway health failed: {str(e)[:120]}") from e

    # ---- 连接管理(与进程内同形, 供 loader/provider 无感复用) ------------

    def _invalidate_code_cache(self) -> None:
        """清空代码表缓存, 并作废在途回源(其世代号因此失效)。

        递增 ``_code_generation`` 是关键: 在途请求返回时世代号已不匹配, 结果被
        丢弃而**不会**把旧清单写回刚清空的缓存(否则 close 的清缓存形同虚设)。
        同时唤醒所有等待者 —— 否则它们会一直等到自己的 deadline。
        """
        with self._code_cv:
            self._code_generation += 1
            self._code_inflight = None
            self._code_last_gen = None
            self._code_last_result = None
            self._code_symbols = None
            self._code_day = None
            self._code_at = 0.0
            self._code_cv.notify_all()

    def close(self) -> None:
        """关闭全部 keep-alive 连接并清空代码表缓存(与 EltDxClient 同接口)。

        清缓存是必需的: loader 重建 provider 时会先 close 再丢弃实例, 但同一
        provider 若被复用于不同配置(如数据源切换), 残留的旧清单会指向已变更的
        标的集合。
        """
        self._pool.close_all()
        self._invalidate_code_cache()

    def reset(self, *, reason: str = "") -> None:
        """丢弃现有 keep-alive 连接, 下次调用重建(网关重启后的自愈)。

        同样作废在途回源并清空缓存: 回源请求本身是通过被重置的那条连接路径发出的,
        其成败已不可信; 更重要的是**清除单飞占位** —— 否则网关故障期间卡住的
        回源会让 ``_code_inflight`` 永久非空, 所有后续调用只能等到超时才返回,
        使自愈路径失去意义。
        """
        self._pool.close_all()
        self._invalidate_code_cache()
        if reason:
            logger.warning("eltdx-http 连接已重置(原因: %s)", reason)

    @property
    def connected(self) -> bool:
        return True

    # ---- 代码表 ---------------------------------------------------------

    def all_a_shares(self) -> list[str]:
        """全市场 A 股(面板格式); 失败返回 ``[]``(但可能降级为当日旧清单, 见下)。

        带 TTL 缓存(见 ``DEFAULT_CODE_TTL_S`` / ``ELTDX_CODE_TTL``): 命中返回副本,
        未命中回源。失效条件 = 北京日期变化 或 TTL 过期 —— 跨日必须重拉, 不靠 TTL 兜底。

        并发契约(provider 是模块级单例, quote 线程与 minute-refresh 线程共用本实例):

        * **单飞**: 同一时刻最多一个线程回源, 其余等这一世代的结果。
        * **有界**: 任何调用都在 ``timeout`` 内返回, 绝不无限阻塞(等待者结构上不递归)。
        * **降级**: 回源失败/超时时, 若存在**当日且未超陈旧上界**的旧清单, 返回旧清单;
          否则返回 ``[]``。返回空时会向上放大为本轮行情/分钟为空, 故旧清单优先。
        * **锁外 IO**: 网络请求在条件变量之外发出, 不阻塞另一个线程的实时路径。

        注意 ``ELTDX_CODE_TTL=0`` 的语义是"每轮都回源", **不是**"失败即返回空":
        降级兜底仍会用内存中的上次成功清单(受陈旧上界约束)。

        已知行为: ``close()``/``reset()`` 若打断在途回源, 该次调用返回 ``[]`` ——
        即便上游其实已成功返回(世代已作废, 结果按不可信丢弃), 属预期语义。
        """
        cached = self._cached_code_symbols()
        if cached is not None:
            return cached
        return self._fetch_and_cache_code_symbols()

    def _cached_code_symbols(self) -> list[str] | None:
        """命中则返回**副本**(防止调用方原地修改污染缓存); 未命中返回 None。"""
        with self._code_cv:
            return self._cached_code_symbols_locked()

    def _cached_code_symbols_locked(self) -> list[str] | None:
        """``_cached_code_symbols`` 的无锁版本(调用方须已持有 ``_code_cv``)。"""
        if self._code_symbols is None or self._code_day is None:
            return None
        if self._code_day != cn_today():
            return None  # 跨日: 立即失效, 不等 TTL
        if (time.monotonic() - self._code_at) >= self._code_ttl_s:
            return None
        logger.debug("eltdx-http 代码表缓存命中: %d 只", len(self._code_symbols))
        return list(self._code_symbols)

    def _stale_code_symbols_locked(self) -> list[str] | None:
        """可兜底的旧清单(仅**当日**且未超陈旧上界), 供回源失败时降级; 无则 None。

        为什么失败时返回旧清单而非 ``[]``: 代码表只用于**决定拉哪些标的**, 不是
        行情数据本身。旧清单用于取数时的后果是"覆盖面略窄" —— 退市标的快照取不到
        会被 ``_snapshot_row`` 丢弃, 新标的下一轮补上, **不产生错误行**; 而返回
        ``[]`` 会让整轮行情/分钟为空。故有旧清单时用旧清单更优(与 quote_service 的
        "指数本轮获取失败, 沿用上轮缓存"同一思路)。

        两道约束, 缺一不可:
        * **必须当日**: 隔夜标的存在上市/退市/代码变更, 且面板当日分区已切换,
          用昨日清单取到的数据会写进今天的上下文。
        * **不得超过 ``_code_stale_max_s``**: 否则上游长时间故障时会一直用数小时前的
          清单, 期间新股/退市完全不可见。超界宁可本轮返回空, 让下一轮取干净清单。
        """
        if self._code_symbols is None or self._code_day != cn_today():
            return None
        # 用数据**真实取回时刻**判断年龄(不随降级刷新), 否则上界永不触发。
        if (
            self._code_stale_max_s > 0
            and (time.monotonic() - self._code_data_at) > self._code_stale_max_s
        ):
            return None
        return list(self._code_symbols)

    def _fetch_and_cache_code_symbols(self) -> list[str]:
        """回源拉代码表并写缓存; 单飞: 并发时只放行一个请求, 其余等该世代结果。"""
        with self._code_cv:
            # 「判断 + 占位」在同一次持锁内完成, 结构上不存在 TOCTOU 空窗。
            if self._code_inflight is not None:
                waiting_for = self._code_inflight
                i_am_holder = False
            else:
                self._code_generation += 1
                waiting_for = self._code_generation
                self._code_inflight = waiting_for
                self._code_last_gen = None
                self._code_last_result = None
                i_am_holder = True

            if not i_am_holder:
                return self._await_code_generation_locked(waiting_for)

        # 只有持有者走到这里; 以下是**锁外**的网络请求。
        try:
            started = time.perf_counter()
            symbols = self._request_a_shares()
            elapsed_ms = (time.perf_counter() - started) * 1000
        except BaseException:
            with self._code_cv:
                self._code_last_gen = waiting_for
                self._code_last_result = ("fail", None)
                if self._code_inflight == waiting_for:
                    self._code_inflight = None
                self._code_cv.notify_all()
            raise

        with self._code_cv:
            if symbols and waiting_for == self._code_generation:
                now = time.monotonic()
                self._code_symbols = list(symbols)
                self._code_day = cn_today()
                self._code_at = now
                self._code_data_at = now  # 真实取回时刻(陈旧度上界的基准)
                self._code_last_gen = waiting_for
                self._code_last_result = ("ok", list(symbols))
                logger.info(
                    "eltdx-http 代码表回源: %d 只, 耗时 %.0fms (缓存 %.0fs)",
                    len(symbols), elapsed_ms, self._code_ttl_s,
                )
                outcome = list(symbols)
            elif not symbols and waiting_for == self._code_generation:
                # 回源失败(软失败返回空): 优先降级用**当日且未超陈旧上界**的旧清单,
                # 避免把一次回源失败放大成整轮行情/分钟为空。
                stale = self._stale_code_symbols_locked()
                self._code_last_gen = waiting_for
                if stale is not None:
                    # 刷新 TTL: 降级结果按"一轮有效缓存"使用, 否则上游持续故障时每轮
                    # 都会真打一次网关(__code_at 不变 → 缓存始终判过期), 既无退避也刷警告。
                    # 代价: 上游恢复最多晚 TTL 被发现 —— 对当日近乎不变的代码表可接受。
                    self._code_at = time.monotonic()
                    self._code_last_result = ("ok", list(stale))
                    logger.warning(
                        "eltdx-http 代码表回源失败, 降级使用当日旧清单(%d 只, 缓存 %.0fs)",
                        len(stale), self._code_ttl_s,
                    )
                    outcome = stale
                else:
                    self._code_last_result = ("fail", None)
                    outcome = []
            else:
                # 世代已失效(close/reset 期间返回) → 结果与旧清单都不可信, 一律丢弃。
                if symbols:
                    logger.debug("eltdx-http 代码表回源结果已过期, 丢弃 (%d 只)", len(symbols))
                self._code_last_gen = waiting_for
                self._code_last_result = ("fail", None)
                outcome = []
            if self._code_inflight == waiting_for:
                self._code_inflight = None
            self._code_cv.notify_all()
            return outcome

    def _await_code_generation_locked(self, waiting_for: int) -> list[str]:
        """等待某个在途世代出结果(调用方须已持有 ``_code_cv``)。

        退出条件必须同时看**结果是否就绪**, 不能只看 ``_code_inflight`` —— 持有者是
        先写结果、再清占位, 若只等占位清空会「结果已就绪却读不到」而误返回 ``[]``
        (实测: 单飞正常但 6 个并发里 5 个拿到空)。

        本函数**结构上不递归**: 无论成功/失败/超时/被作废, 都在此处直接返回,
        故调用者必然有界返回。旧实现在超时后递归重入取数, 持有者卡死时等待者
        会无限递归(实测 4 个线程全部无法退出)。

        失败/超时时**优先返回当日的过期旧清单**(见 ``_stale_code_symbols_locked``),
        避免把一次回源失败放大成整轮行情为空; 无旧清单才返回 ``[]``。
        """
        deadline = time.monotonic() + self._timeout
        while True:
            if self._code_last_gen == waiting_for and self._code_last_result is not None:
                kind, syms = self._code_last_result
                if kind == "ok":
                    return list(syms or [])
                stale = self._stale_code_symbols_locked()
                if stale is not None:
                    logger.debug(
                        "eltdx-http 代码表同世代回源失败, 兜底使用当日旧清单(%d 只)", len(stale)
                    )
                    return stale
                logger.debug("eltdx-http 代码表同世代回源失败且无旧清单, 本轮返回空")
                return []  # 无旧清单: 不各自重拉, 由下一轮统一回源
            if self._code_inflight != waiting_for:
                # 在途世代已被 close/reset 作废 → 本轮返回空(缓存已被清, 旧清单不可信)
                return []
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                stale = self._stale_code_symbols_locked()
                if stale is not None:
                    logger.warning(
                        "eltdx-http 代码表等待回源超时(%.0fs), 兜底使用当日旧清单(%d 只)",
                        self._timeout, len(stale),
                    )
                    return stale
                logger.warning(
                    "eltdx-http 代码表等待回源超时(%.0fs)且无旧清单, 本轮返回空", self._timeout
                )
                return []
            self._code_cv.wait(timeout=min(remaining, 0.25))

    def _request_a_shares(self) -> list[str]:
        """真正发起一次代码表请求(软失败返回 [], 失败**不写缓存**以便下轮重试)。"""
        try:
            raw = self._rpc("codes.all_a_shares", {})
        except Exception as e:
            logger.warning("eltdx-http all_a_shares 失败: %s", e)
            return []
        # 网关可能返回 ["sz000001",...] 或 [{"code":..,"exchange":..},...]
        out: list[str] = []
        for item in raw or []:
            sym = _code_entry_symbol(item)
            if sym:
                out.append(sym)
        if not out:
            logger.warning("eltdx-http all_a_shares 返回空或无法识别")
        return out

    def all_indices(self) -> list[str]:
        """全市场指数(面板格式); 失败返回 []。"""
        try:
            raw = self._rpc("codes.all_indices", {})
        except Exception as e:
            logger.warning("eltdx-http all_indices 失败: %s", e)
            return []
        out: list[str] = []
        for item in raw or []:
            sym = _code_entry_symbol(item)
            if sym:
                out.append(sym)
        return out

    # ---- K 线 -----------------------------------------------------------

    def bars(self, symbol: str, *, period: str = "day", count: int) -> list[Any]:
        """单标的 K 线(自带分页, 单页上限 800 根); 失败返回 []。"""
        code = to_eltdx_code(symbol)
        if code is None:
            logger.warning("eltdx-http bars: 无法识别的 symbol %r", symbol)
            return []
        want = max(0, int(count))
        if want == 0:
            return []
        out: list[Any] = []
        for start in range(0, want, _MAX_PAGE_SIZE):
            take = min(_MAX_PAGE_SIZE, want - start)
            try:
                series = self._rpc(
                    "bars.get",
                    {"code": code, "period": period, "count": take, "start": start, "adjust": None},
                )
            except Exception as e:
                logger.warning(
                    "eltdx-http bars 失败 %s(period=%s start=%d): %s", symbol, period, start, e
                )
                break
            page = list(getattr(series, "bars", None) or [])
            if not page:
                break  # 空页终止
            out.extend(page)
            if len(page) < take:
                break  # 不足一页 = 已到最早
        return out

    def bars_multi(
        self, symbols: list[str], *, period: str = "day", count: int
    ) -> list[tuple[str, list[Any]]]:
        """批量 K 线 → ``[(面板symbol, bars), ...]``。

        实测网关的 ``bars.get`` 支持 codes 数组且**无 80 上限**(80/200/500/1000 只
        均足额返回; 那个 80 是 ``quotes.get_snapshots`` 的专属硬上限)。
        """
        codes: list[tuple[str, str]] = []
        for s in symbols:
            c = to_eltdx_code(s)
            if c is not None:
                codes.append((c, s))
        if not codes:
            return []
        try:
            resp = self._rpc(
                "bars.get",
                {
                    "code": [c for c, _ in codes],
                    "period": period,
                    "count": max(1, int(count)),
                    "adjust": None,
                },
            )
        except Exception as e:
            logger.warning(
                "eltdx-http bars_multi 失败(%d 只, period=%s): %s", len(codes), period, e
            )
            return []
        out: list[tuple[str, list[Any]]] = []
        if isinstance(resp, _Obj):
            # 批量 → {eltdx_code: series}; 单个 code 时也可能直接是 series
            data = resp._d
            bars = getattr(resp, "bars", None)
            if bars is not None and not any(k in data for k, _ in codes):
                only = list(bars or [])
                return [(codes[0][1], only)] if only else []
            for code, panel_sym in codes:
                series = data.get(code)
                page = list(getattr(series, "bars", None) or []) if series is not None else []
                if page:
                    out.append((panel_sym, page))
        return out

    def iter_bars_batches(
        self, symbols: list[str], *, period: str = "day", count: int, batch_size: int
    ) -> Iterator[list[tuple[str, list[Any]]]]:
        """有界分批(bars_multi 包装); 契约: 每批有明确上界。"""
        step = max(1, int(batch_size))
        for i in range(0, len(symbols), step):
            yield self.bars_multi(symbols[i : i + step], period=period, count=count)

    # ---- 实时快照 -------------------------------------------------------

    def snapshots(self, symbols: list[str], *, batch_size: int) -> list[Any]:
        """批量实时快照; **软失败返回 []**(不阻断面板轮询线程)。

        警告: ``quotes.get_snapshots`` 的**单请求硬上限是 80 只**(实测请求 81/100
        只静默截断为 80; 800 只直接断连)。故 batch_size 必须 <=80。
        """
        codes = [c for c in (to_eltdx_code(s) for s in symbols) if c]
        if not codes:
            logger.warning("eltdx-http snapshots: 无有效代码(入参 %d 个)", len(symbols))
            return []
        step = max(1, int(batch_size))
        chunks = [codes[i : i + step] for i in range(0, len(codes), step)]
        out: list[Any] = []

        def _one(chunk: list[str]) -> list[Any]:
            return list(self._rpc("quotes.get_snapshots", {"codes": chunk}) or [])

        try:
            if len(chunks) == 1:
                return _one(chunks[0])
            with ThreadPoolExecutor(max_workers=min(self._max_workers, len(chunks))) as pool:
                futures = [pool.submit(_one, ch) for ch in chunks]
                for fut in as_completed(futures):
                    try:
                        out.extend(fut.result() or [])
                    except Exception as e:
                        logger.warning("eltdx-http snapshots 分片失败(已隔离): %s", e)
        except Exception as e:
            logger.warning("eltdx-http snapshots 整体失败(软失败返回空): %s", e)
            return []
        if not out:
            logger.warning("eltdx-http snapshots 返回空(symbols=%d)", len(symbols))
        return out

    # ---- 除权因子 -------------------------------------------------------

    def adjustment_factors(self, code: str) -> list[Any]:
        """单标的除权事件列表; 失败返回 []。"""
        try:
            resp = self._rpc("corporate.adjustment_factors", {"code": code})
        except Exception as e:
            logger.warning("eltdx-http adjustment_factors 失败 %s: %s", code, e)
            return []
        items = list(getattr(resp, "items", None) or [])
        return sorted(items, key=lambda it: getattr(it, "date", None) or "")

    # ---- 财务 -----------------------------------------------------------

    def finance_batch(self, codes: list[str]) -> Any:
        """批量基础财务信息; 失败抛异常(由调用方按批隔离)。

        上游对**单次请求的代码组合**敏感(实测批大小需 <=20), 见 provider 的
        ``_FINANCE_BATCH`` 与重试/拆分兜底。
        """
        if not codes:
            return None
        return self._rpc("corporate.finance_batch", {"codes": list(codes)})

    # ---- 五档盘口 -------------------------------------------------------

    def depth(self, symbols: list[str]) -> Any:
        """五档盘口; **失败抛异常**(契约要求服务层按批隔离, 不跨源回退)。"""
        codes = [c for c in (to_eltdx_code(s) for s in symbols) if c]
        if not codes:
            return None
        return self._rpc("quotes.get_depth", {"codes": codes})
