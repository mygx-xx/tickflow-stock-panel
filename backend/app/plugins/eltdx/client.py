"""eltdx 客户端封装 —— 连接池、代码格式转换、分批并发、软失败。

本模块是 eltdx(TdxClient) 与面板内部契约之间的**适配层**, 只做三件事:

1. **代码格式转换**  eltdx 用 ``sz000001`` / ``sh600000``(小写交易所前缀 + 6 位);
   面板内部统一 ``000001.SZ`` / ``600000.SH``(6 位 + 大写交易所后缀)。
2. **分批并发取数**  eltdx 是逐标的接口(``bars.get`` 一次一个 code), 而面板全市场
   日 K 同步要 5000+ 标的。这里用 ``TdxClient`` 的连接池(``server_count x
   connections_per_server`` 个 TCP slot)+ 线程池把请求打满, 且**批次有上界**
   (契约要求: 不得先全量收集再 concat)。
3. **软失败**  契约要求实时快照失败返回 ``[]`` 且不抛异常(不阻断面板轮询线程)。

实测口径基线(eltdx 3.2.2, 2026-09 实测, 单位换算依据):
    * ``quotes.get_snapshots`` 行即快照对象:
      - ``total_hand``  int, 单位 **手** —— 自验 ``amount/(last_pricextotal_hand)≈100``
        (100 股/手), 与面板 volume 契约(手)一致, **无需换算**。
      - ``amount``      float, 单位 **元**。
      - ``change_pct``  float, **百分数制**(如 0.442478 表示 0.4425%) ——
        实测 4/4 标的满足 ``change_pct ≈ change/pre_closex100``; 面板契约是
        **小数制**, 故 provider 必须显式 /100(见 provider.py)。
      - ``open_price/high_price/low_price/pre_close_price/last_price`` float, 元。
    * ``bars.get(...).bars`` 每根 KlineBar:
      - ``volume_lots`` float, 单位 **手**(实测 690979.12); ``amount`` float, 元。
      - ``time`` 是 **Asia/Shanghai 时区的 aware datetime**(日线固定挂 15:00) ——
        面板日 K 契约要 naive ``date``, 故取 ``.date()``。
      - ``adjust=None`` 为不复权原始价(面板契约要求 provider 不得自行复权)。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

# 交易所后缀(面板) ↔ 前缀(eltdx)
_SUFFIX_TO_PREFIX = {"SH": "sh", "SZ": "sz", "BJ": "bj"}

# 分批并发的默认参数。eltdx 官方建议中等并发 = server_count 4 x 每台 8 连接 = 32 slot;
# 这里用更保守的默认值起步(面板日 K 同步是长任务, 稳优先), 可由环境变量覆盖。
DEFAULT_SERVER_COUNT = 4
DEFAULT_CONNECTIONS_PER_SERVER = 4
DEFAULT_TIMEOUT_S = 8.0
# 单次 bars.get 最大请求根数(实测 count>800 报 "page size must be between 1 and 800",
# 故深层历史由本客户端自管分页, 不依赖 SDK 的 all_pages)
MAX_BARS_PER_REQUEST = 800
_MAX_PAGE_SIZE = MAX_BARS_PER_REQUEST


def to_panel_symbol(code: str) -> str | None:
    """``sz000001`` → ``000001.SZ``; 已是面板格式则原样返回; 无法识别返回 None。

    契约: 面板 symbol 统一带交易所后缀且大写(``600519.SH``/``000001.SZ``/ETF、指数同格式)。
    """
    if not code:
        return None
    raw = str(code).strip()
    if not raw:
        return None
    # 已是面板格式: 6 位 + "." + 2 位交易所
    if "." in raw:
        left, _, right = raw.partition(".")
        if len(left) == 6 and left.isdigit() and right.upper() in _SUFFIX_TO_PREFIX:
            return f"{left}.{right.upper()}"
        return None
    # eltdx 格式: 2 位交易所前缀 + 6 位数字
    if len(raw) == 8:
        prefix, digits = raw[:2].lower(), raw[2:]
        if prefix in _SUFFIX_TO_PREFIX.values() and digits.isdigit():
            return f"{digits}.{prefix.upper()}"
    # 裸 6 位数字: 无法判定交易所, 按 A 股规则推导(6/9 开头沪市, 其余深市)
    if len(raw) == 6 and raw.isdigit():
        return f"{raw}.{'SH' if raw[0] in ('6', '9') else 'SZ'}"
    return None


def to_eltdx_code(symbol: str) -> str | None:
    """``000001.SZ`` → ``sz000001``; 无法识别返回 None。"""
    if not symbol:
        return None
    raw = str(symbol).strip()
    if "." not in raw:
        return None
    code, _, suffix = raw.partition(".")
    prefix = _SUFFIX_TO_PREFIX.get(suffix.upper())
    if prefix is None or len(code) != 6 or not code.isdigit():
        return None
    return f"{prefix}{code}"


class EltDxClient:
    """``TdxClient`` 的薄封装: 惰性建连、线程安全、分批并发、软失败。

    线程安全: ``TdxClient`` 自带连接池, 多线程并发调用其 API 是设计内用法;
    本类只额外保护"惰性建连"这一次性动作。
    """

    def __init__(
        self,
        *,
        server_count: int = DEFAULT_SERVER_COUNT,
        connections_per_server: int = DEFAULT_CONNECTIONS_PER_SERVER,
        timeout: float = DEFAULT_TIMEOUT_S,
        max_workers: int | None = None,
    ) -> None:
        self._server_count = max(1, int(server_count))
        self._connections_per_server = max(1, int(connections_per_server))
        self._timeout = float(timeout)
        # 并发线程数默认对齐连接池 slot 数(多了只会在池上排队)
        self._max_workers = int(max_workers or self._server_count * self._connections_per_server)
        self._client: Any = None
        self._lock = threading.Lock()

    # ---- 连接管理 -------------------------------------------------------

    def _ensure(self) -> Any:
        """惰性建连(首次调用时探测主站并建池); 建连失败抛异常, 由上层决定软/硬失败。"""
        client = self._client
        if client is not None:
            return client
        with self._lock:
            if self._client is None:
                from eltdx import TdxClient  # 延迟 import: 依赖缺失时 availability() 先拦

                self._client = TdxClient(
                    server_count=self._server_count,
                    connections_per_server=self._connections_per_server,
                    timeout=self._timeout,
                )
            return self._client

    def close(self) -> None:
        """关闭连接池(loader 重建注册表时会调用)。"""
        with self._lock:
            client, self._client = self._client, None
        if client is not None:
            try:
                client.close()
            except Exception as e:
                logger.debug("eltdx close failed: %s", e)

    @property
    def connected(self) -> bool:
        return self._client is not None

    # ---- 代码表 ---------------------------------------------------------

    def all_a_shares(self) -> list[str]:
        """全市场 A 股代码(面板格式); 失败返回 []。"""
        try:
            raw = self._ensure().codes.all_a_shares()
        except Exception as e:
            logger.warning("eltdx all_a_shares 失败: %s", e)
            return []
        out = [s for s in (to_panel_symbol(c) for c in (raw or [])) if s]
        if not out:
            logger.warning("eltdx all_a_shares 返回空或全部无法识别(接口结构可能变化)")
        return out

    def all_indices(self) -> list[str]:
        """全市场指数代码(面板格式); 失败返回 []。"""
        try:
            raw = self._ensure().codes.all_indices()
        except Exception as e:
            logger.warning("eltdx all_indices 失败: %s", e)
            return []
        return [s for s in (to_panel_symbol(c) for c in (raw or [])) if s]

    # ---- K 线 -----------------------------------------------------------

    def bars(self, symbol: str, *, period: str = "day", count: int) -> list[Any]:
        """单标的 K 线列表(KlineBar); 失败返回 []。``adjust=None`` 即不复权原始价。

        eltdx 单页上限 800 根(实测 ``count>800`` 报 "page size must be between 1 and 800"),
        故深层历史按 ``start`` 逐页取; 空页即终止(契约要求空页终止条件)。
        ``all_pages`` 在本版本会因 max_pages 抛异常, 故这里自管分页而非交给 SDK。
        """
        code = to_eltdx_code(symbol)
        if code is None:
            logger.warning("eltdx bars: 无法识别的 symbol %r", symbol)
            return []
        want = max(0, int(count))
        if want == 0:
            return []
        out: list[Any] = []
        page = _MAX_PAGE_SIZE
        for start in range(0, want, page):
            take = min(page, want - start)
            try:
                series = self._ensure().bars.get(
                    code,
                    period=period,
                    count=take,
                    start=start,
                    adjust=None,  # 面板契约: K 线必须不复权原始价, 复权交给 adj_factor+enriched
                )
            except Exception as e:
                logger.warning(
                    "eltdx bars 失败 %s(period=%s start=%d): %s", symbol, period, start, e
                )
                break
            bars = list(getattr(series, "bars", ()) or ())
            if not bars:
                break  # 空页终止
            out.extend(bars)
            if len(bars) < take:
                break  # 不足一页 = 已到最早
        return out

    def bars_multi(
        self, symbols: list[str], *, period: str = "day", count: int
    ) -> list[tuple[str, list[Any]]]:
        """**批量**取 K 线: 一次请求多个 code, 返回 ``[(面板symbol, bars), ...]``。

        实测 eltdx 的 ``bars.get`` 支持批量 codes 且上限很高(2000 只/请求 3.9s 足额),
        远优于逐标的并发 —— 全市场分钟增量轮依赖此路径(见 provider.get_intraday_latest)。

        与单标的 ``bars`` 的差别: 批量返回 ``dict{eltdx_code: KlineSeries}``, 且这里
        **不做分页**(批量场景只取最新 ``count`` 根, 分页由调用方按需分批标的数)。
        返回的 symbol 已转成面板格式; 无法识别的 code 丢弃。
        """
        codes: list[tuple[str, str]] = []  # (eltdx_code, panel_symbol)
        for s in symbols:
            code = to_eltdx_code(s)
            if code is not None:
                codes.append((code, s))
        if not codes:
            return []
        try:
            resp = self._ensure().bars.get(
                [c for c, _ in codes], period=period, count=max(1, int(count)), adjust=None
            )
        except Exception as e:
            logger.warning("eltdx bars_multi 失败(%d 只, period=%s): %s", len(codes), period, e)
            return []
        if not isinstance(resp, dict):
            # 单 code 入参时 eltdx 返回 KlineSeries, 这里统一包一层
            only = list(getattr(resp, "bars", ()) or ())
            return [(codes[0][1], only)] if only else []
        out: list[tuple[str, list[Any]]] = []
        for code, panel_sym in codes:
            series = resp.get(code)
            if series is None:
                continue
            bars = list(getattr(series, "bars", ()) or ())
            if bars:
                out.append((panel_sym, bars))
        return out

    def iter_bars_batches(
        self,
        symbols: list[str],
        *,
        period: str = "day",
        count: int,
        batch_size: int,
    ) -> Iterator[list[tuple[str, list[Any]]]]:
        """按 ``batch_size`` 分批并发取 K 线, 逐批 yield ``[(symbol, bars), ...]``。

        契约要求: 批次必须有明确上界、不得先全量收集再 concat; 由调用方边收边转换落盘。
        逐标的失败以空列表形式返回(软失败), 由调用方按空批处理。
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        step = max(1, int(batch_size))
        for start in range(0, len(symbols), step):
            chunk = symbols[start : start + step]
            results: list[tuple[str, list[Any]]] = []
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                futures = {pool.submit(self.bars, s, period=period, count=count): s for s in chunk}
                for fut in as_completed(futures):
                    sym = futures[fut]
                    try:
                        results.append((sym, fut.result()))
                    except Exception as e:
                        logger.warning("eltdx bars 线程异常 %s: %s", sym, e)
                        results.append((sym, []))
            # 稳定输出顺序(并发完成顺序不确定, 排序让测试与日志可复现)
            results.sort(key=lambda item: item[0])
            yield results

    # ---- 实时快照 -------------------------------------------------------

    def snapshots(self, symbols: list[str], *, batch_size: int) -> list[Any]:
        """批量实时快照(QuoteSnapshot 对象列表); **软失败返回 []**。

        契约: ``get_realtime`` 必须软失败(返回空 + warning), 不阻断面板轮询线程。
        eltdx 单次快照请求的代码数有上限, 故内部按 batch_size 分片串行/并发取回。
        """
        codes = [c for c in (to_eltdx_code(s) for s in symbols) if c]
        if not codes:
            logger.warning("eltdx snapshots: 无有效代码(入参 %d 个)", len(symbols))
            return []
        step = max(1, int(batch_size))
        chunks = [codes[i : i + step] for i in range(0, len(codes), step)]
        out: list[Any] = []
        try:
            if len(chunks) == 1:
                return list(self._ensure().quotes.get_snapshots(chunks[0]) or [])
            # 多片: 并发取, 单片的异常不影响其他片(隔离而非整批失败)
            with ThreadPoolExecutor(max_workers=min(self._max_workers, len(chunks))) as pool:
                futures = [pool.submit(self._ensure().quotes.get_snapshots, ch) for ch in chunks]
                for fut in as_completed(futures):
                    try:
                        out.extend(list(fut.result() or []))
                    except Exception as e:
                        logger.warning("eltdx snapshots 分片失败(已隔离): %s", e)
        except Exception as e:
            logger.warning("eltdx snapshots 整体失败(软失败返回空): %s", e)
            return []
        if not out:
            logger.warning("eltdx snapshots 返回空(symbols=%d)", len(symbols))
        return out

    # ---- 除权因子 -------------------------------------------------------

    def adjustment_factors(self, code: str) -> list[Any]:
        """单标的除权事件列表(``AdjustmentFactor``); 失败返回 []。

        每个事件含 ``(date, qfq_scale, qfq_offset, hfq_scale, hfq_offset)``。
        注意: **推导面板 ex_factor 要用 hfq_* 而非 qfq_*** —— 实测 qfq_offset 是
        前复权偏移量, hfq_offset 增量除以 hfq_scale 才是真实每股分红(见 provider)。
        """
        try:
            resp = self._ensure().corporate.adjustment_factors(code)
        except Exception as e:
            logger.warning("eltdx adjustment_factors 失败 %s: %s", code, e)
            return []
        items = list(getattr(resp, "items", ()) or ())
        return sorted(items, key=lambda it: getattr(it, "date", None) or date.min)

    # ---- 财务(基础财务信息) ---------------------------------------------

    def finance_batch(self, codes: list[str]) -> Any:
        """批量基础财务信息(``FinanceBatch``, 取 ``.records``)。

        ``codes`` 为 eltdx 代码(如 ``sh600519``)。失败抛异常, 由调用方按批隔离。
        """
        if not codes:
            return None
        return self._ensure().corporate.finance_batch(list(codes))

    # ---- 五档盘口 -------------------------------------------------------

    def depth(self, symbols: list[str]) -> Any:
        """五档盘口页(``QuoteRefreshPage``, 取 ``.records``)。

        与快照不同, 盘口契约要求失败由**服务层按批隔离**且不跨数据源回退,
        故这里不做软失败 —— 异常直接上抛, 交由调用方/服务处理。
        """
        codes = [c for c in (to_eltdx_code(s) for s in symbols) if c]
        if not codes:
            return None
        return self._ensure().quotes.get_depth(codes)
