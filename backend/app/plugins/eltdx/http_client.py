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

# 代码表缓存 TTL(秒)。依据(实测): 全市场 5578 只快照并发 8 约 1.4s, 而
# codes.all_a_shares 每次 2.1~6.5s(中位 4.3s), 占单轮总耗时(5.6~7.9s)七成以上;
# 代码表当日几乎不变(实测连续多次集合完全一致), 故按 TTL 缓存。
# 取 5 分钟: 覆盖数十轮刷新, 同时把盘中新上市/退市的可见滞后限制在 5 分钟内。
# 另按「北京日期」跨日强制失效(见 all_a_shares), 不依赖 TTL。
_CODE_CACHE_TTL_S = 300.0


def _code_entry_symbol(item: Any) -> str | None:
    """代码表条目 → 面板 symbol; 无法确定交易所时返回 None(**不推断**)。

    条目有两种形态: ``"sz000001"``(带 2 位前缀)或 ``{"code","exchange"}``。
    两者都拿不到交易所信息时拒绝按首位猜 —— 沪市指数 000001 会被猜成
    ``000001.SZ``, 北交所 920000 会被猜成 ``920000.SH``, 这些错码会被当作
    股票写进 kline_daily, 与真实标的撞成重复行。
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
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self._max_workers = max(1, int(max_workers))
        self._lock = threading.Lock()
        self._seq = 0
        # 代码表缓存: 由 quote 线程与 minute-refresh 线程共享(provider 为模块级单例),
        # 故读写与「单飞」都需加锁。网络请求严格在锁外(见 all_a_shares)。
        self._code_lock = threading.Lock()
        self._code_symbols: list[str] | None = None
        self._code_day: date | None = None
        self._code_at: float = 0.0
        self._code_fetching = False
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

    def close(self) -> None:
        """关闭全部 keep-alive 连接并清空代码表缓存(与 EltDxClient 同接口)。

        清缓存是必需的: loader 重建 provider 时会先 close 再丢弃实例, 但同一
        provider 若被复用于不同配置(如数据源切换), 残留的旧清单会指向已变更的
        标的集合。
        """
        self._pool.close_all()
        with self._code_lock:
            self._code_symbols = None
            self._code_day = None
            self._code_at = 0.0
            self._code_fetching = False

    def reset(self, *, reason: str = "") -> None:
        """丢弃现有 keep-alive 连接, 下次调用重建(网关重启后的自愈)。"""
        self._pool.close_all()
        if reason:
            logger.warning("eltdx-http 连接已重置(原因: %s)", reason)

    @property
    def connected(self) -> bool:
        return True

    # ---- 代码表 ---------------------------------------------------------

    def all_a_shares(self) -> list[str]:
        """全市场 A 股(面板格式); 失败返回 []。

        带 TTL 缓存(见 ``_CODE_CACHE_TTL_S``): 命中直接返回副本, 未命中回源。
        失效条件 = 北京日期变化 或 TTL 过期 —— 跨日必须重拉, 不靠 TTL 兜底。

        并发: provider 是模块级单例, quote 线程与 minute-refresh 线程共用本实例。
        锁只保护缓存读写与「单飞」占位, **网络请求在锁外**(避免持锁做 IO 阻塞
        另一个线程的实时路径)。
        """
        cached = self._cached_code_symbols()
        if cached is not None:
            return cached
        return self._fetch_and_cache_code_symbols()

    def _cached_code_symbols(self) -> list[str] | None:
        """命中则返回**副本**(防止调用方原地修改污染缓存); 未命中返回 None。"""
        with self._code_lock:
            if self._code_symbols is None or self._code_day is None:
                return None
            if self._code_day != cn_today():
                return None  # 跨日: 立即失效, 不等 TTL
            if (time.monotonic() - self._code_at) >= _CODE_CACHE_TTL_S:
                return None
            logger.debug("eltdx-http 代码表缓存命中: %d 只", len(self._code_symbols))
            return list(self._code_symbols)

    def _fetch_and_cache_code_symbols(self) -> list[str]:
        """回源拉代码表并写缓存; 单飞语义: 并发时只放行一个请求, 其余等结果。"""
        with self._code_lock:
            if self._code_fetching:
                # 已有线程在拉: 等它写完缓存(不重复回源), 超时则自行回源。
                wait_for_other = True
            else:
                self._code_fetching = True
                wait_for_other = False

        if wait_for_other:
            deadline = time.monotonic() + self._timeout
            while time.monotonic() < deadline:
                time.sleep(0.05)
                cached = self._cached_code_symbols()
                if cached is not None:
                    return cached
                with self._code_lock:
                    if not self._code_fetching:
                        break  # 对方已结束但仍未写入(失败) → 由本线程回源
            return self._fetch_and_cache_code_symbols()

        try:
            started = time.perf_counter()
            symbols = self._request_a_shares()
            elapsed_ms = (time.perf_counter() - started) * 1000
            if symbols:
                with self._code_lock:
                    self._code_symbols = list(symbols)
                    self._code_day = cn_today()
                    self._code_at = time.monotonic()
                logger.info(
                    "eltdx-http 代码表回源: %d 只, 耗时 %.0fms (缓存 %.0fs)",
                    len(symbols), elapsed_ms, _CODE_CACHE_TTL_S,
                )
            return symbols
        finally:
            with self._code_lock:
                self._code_fetching = False

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
