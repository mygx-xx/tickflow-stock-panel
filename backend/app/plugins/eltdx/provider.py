"""eltdx Provider —— 通达信行情数据源(面板数据源插件契约实现)。

只声明并实现 ``daily`` 与 ``realtime`` 两个数据集; 未声明的数据集
(adj_factor / minute / full_minute / depth5 / financial) 由
``provider_has_dataset`` 返回 False, 面板自动回退 TickFlow。

**口径依据**(全部为 eltdx 3.2.2 实测, 详见 client.py 模块 docstring 与
docs/plugin-development.md「内部数据契约」):

===========  ====================  ==========================  ==============
面板字段      eltdx 来源            实测单位/口径                处理
===========  ====================  ==========================  ==============
symbol       ``full_code``         ``sz000001``                转 ``000001.SZ``
date         KlineBar.time         Asia/Shanghai aware dt      取 ``.date()``
open/high/low/close  KlineBar.*    元(不复权)                   直用
volume       KlineBar.volume_lots  手                           直用
amount       KlineBar.amount       元                           直用
last_price   last_price            元                           直用
prev_close   pre_close_price       元                           直用
change_pct   change_pct            **百分数制**(0.4425=0.4425%)  **/100**
===========  ====================  ==========================  ==============

缺失字段一律 ``None``, 不做启发式补全(契约红线); ``change_amount`` 用
``last_price - prev_close`` 按固定口径推导。
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl

from app.data_providers.normalizer import DAILY_COLS, normalize_daily
from app.plugins.eltdx.client import (
    DEFAULT_CONNECTIONS_PER_SERVER,
    DEFAULT_SERVER_COUNT,
    DEFAULT_TIMEOUT_S,
    EltDxClient,
    to_panel_symbol,
)

logger = logging.getLogger(__name__)

_DATASETS = ("daily", "realtime")

# 日 K 列(面板 canonical 9 列, 含 quote_ts; 由 normalizer.DAILY_COLS 单源定义)
_DAILY_COLUMNS = DAILY_COLS

# 实时快照: eltdx 单次请求的代码数上限(保守值; 官方未给硬上限, 分片既限并发也限单包大小)
_SNAPSHOT_BATCH = 800
# 快照分片并发上限(独立于日 K 的连接池, 避免瞬时把池占满影响其他数据集)
_SNAPSHOT_WORKERS = int(os.environ.get("ELTDX_SNAPSHOT_WORKERS", "4"))

# 日 K 分批大小(契约: iter_daily 每批要有上界; 调用方边收边落盘)
_DAILY_BATCH = int(os.environ.get("ELTDX_DAILY_BATCH", "200"))


def availability() -> tuple[bool, str]:
    """插件可用性自检(后端启动时调用): eltdx 依赖是否可 import。

    契约: 返回 ``(是否可用, 原因)``, 不抛异常。不可用时设置页灰显并展示 install_hint。
    """
    try:
        import eltdx
    except Exception as e:  # 依赖缺失: 记录原因供设置页灰显, 不上抛
        return False, f"未安装 eltdx 依赖({e}); 请点击卡片「安装依赖」按钮"
    version = getattr(eltdx, "__version__", "unknown")
    return True, f"ok (eltdx {version})"


def _to_float(value: Any) -> float | None:
    """安全转 float; 非法/缺失返回 None(不伪造)。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    # NaN/Inf 不进面板(读回会序列化成 null, 但源头就该干净)
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _bar_date(value: Any) -> date | None:
    """KlineBar.time → ``date``(北京墙钟 aware datetime, 取日期部分)。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value[:19], fmt).date()
            except ValueError:
                continue
    return None


def _kline_row(symbol: str, bar: Any) -> dict | None:
    """单根 KlineBar → 面板日 K 行; 关键字段缺失则丢弃该根(不伪造)。"""
    d = _bar_date(getattr(bar, "time", None))
    o = _to_float(getattr(bar, "open", None))
    h = _to_float(getattr(bar, "high", None))
    low = _to_float(getattr(bar, "low", None))
    c = _to_float(getattr(bar, "close", None))
    if d is None or o is None or h is None or low is None or c is None:
        return None
    return {
        "symbol": symbol,
        "date": d,
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        # volume_lots 实测为"手"(契约单位), amount 为元 —— 均直用, 不换算
        "volume": _to_float(getattr(bar, "volume_lots", None)),
        "amount": _to_float(getattr(bar, "amount", None)),
    }


def _snapshot_row(snap: Any) -> dict | None:
    """单个 QuoteSnapshot → 面板 realtime 行; 必需字段缺失则丢弃(不伪造)。"""
    symbol = to_panel_symbol(getattr(snap, "full_code", None) or getattr(snap, "code", None))
    if symbol is None:
        return None
    last = _to_float(getattr(snap, "last_price", None))
    prev = _to_float(getattr(snap, "pre_close_price", None))
    if last is None or prev is None:
        return None
    pct = _to_float(getattr(snap, "change_pct", None))
    change_amt = _to_float(getattr(snap, "change", None))
    if change_amt is None and prev is not None:
        # 固定口径推导(契约允许): change_amount = last - prev_close
        change_amt = round(last - prev, 6)
    return {
        "symbol": symbol,
        "last_price": last,
        "prev_close": prev,
        "open": _to_float(getattr(snap, "open_price", None)),
        "high": _to_float(getattr(snap, "high_price", None)),
        "low": _to_float(getattr(snap, "low_price", None)),
        # total_hand 实测为"手"(amount/(lastxhand)≈100 自验), 契约要求手 → 直用
        "volume": _to_float(getattr(snap, "total_hand", None)),
        "amount": _to_float(getattr(snap, "amount", None)),
        # ⚠️ eltdx change_pct 是百分数制(0.4425 = 0.4425%), 面板契约是小数制 → /100
        "change_pct": pct / 100.0 if pct is not None else None,
        "change_amount": change_amt,
        "name": None,  # 快照无名称, 下游用标的维表关联(契约允许置 None)
        "amplitude": None,  # eltdx 未直接提供, 置 None 由 pipeline 重算, 不启发式伪造
        "turnover_rate": None,
        "timestamp": _snapshot_ts(getattr(snap, "time_raw", None)),
    }


def _snapshot_ts(time_raw: Any) -> int | None:
    """快照时间 → 毫秒时间戳。

    eltdx ``time_raw`` 是主站的"当日 HHMMSSmmm"紧凑整数(如 15330366 = 15:33:03.366),
    无日期部分。契约允许缺失时退本地时间, 故这里按**北京墙钟当日**还原为毫秒戳;
    无法解析时返回 None(下游退本地时间)。
    """
    if time_raw is None:
        return None
    try:
        raw = int(time_raw)
    except (TypeError, ValueError):
        return None
    if raw < 0:
        return None
    # 主站紧凑时间戳: 时/分/秒 + 毫秒/百分秒。位数不固定(实测 8 位 = HHMMSScc,
    # 即百分秒), 故按"去掉小数部分再解 HHMMSS"的统一口径处理, 越界即判非法。
    text = str(raw)
    if len(text) < 6:
        return None
    head, frac = text[:-2], text[-2:]  # 末 2 位 = 百分秒
    if len(head) != 6 or not head.isdigit():
        return None
    hour, minute, sec = int(head[0:2]), int(head[2:4]), int(head[4:6])
    if hour > 23 or minute > 59 or sec > 59:
        return None
    ms = int(frac) * 10  # 百分秒 → 毫秒
    today = date.today()
    try:
        dt = datetime(today.year, today.month, today.day, hour, minute, sec, ms * 1000)
    except ValueError:
        return None
    return int(dt.timestamp() * 1000)


class _EltDxConfig:
    """伪 config: 契约要求 provider.config.datasets 存在, 供 provider_has_dataset 路由。"""

    def __init__(self) -> None:
        self.datasets: dict[str, Any] = dict.fromkeys(_DATASETS)
        self.display_name = "eltdx (通达信)"


class EltDxProvider:
    """eltdx 数据源 Provider(无需继承基类, 方法签名对齐 GenericHTTPProvider)。"""

    name = "eltdx"
    builtin = True

    def __init__(self) -> None:
        self.config = _EltDxConfig()
        self._client = EltDxClient(
            server_count=int(os.environ.get("ELTDX_SERVER_COUNT", DEFAULT_SERVER_COUNT)),
            connections_per_server=int(
                os.environ.get("ELTDX_CONNECTIONS_PER_SERVER", DEFAULT_CONNECTIONS_PER_SERVER)
            ),
            timeout=float(os.environ.get("ELTDX_TIMEOUT", DEFAULT_TIMEOUT_S)),
        )

    # ---- 生命周期 -------------------------------------------------------

    def close(self) -> None:
        """关闭连接池(loader 重建注册表时对每个 provider 调用)。"""
        self._client.close()

    # ---- 日 K -----------------------------------------------------------

    def _daily_frame(self, rows: list[dict]) -> pl.DataFrame:
        """行 → 面板日 K 帧(复用官方 normalize_daily: 统一 cast/filter_halt_days/列序)。

        归一化走 normalizer 单源, 保证与 fuyao/tickflow 同口径(停牌日过滤等)。
        """
        if not rows:
            return pl.DataFrame()
        return normalize_daily(pl.DataFrame(rows), source="eltdx")

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | date,
        end_time: datetime | date,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        """日 K(不复权原始价): ``[symbol, date, open, high, low, close, volume, amount]``。

        eltdx 只能逐标的取 K 线, 故此处按 start/end 反推所需根数上限后并发拉取,
        再按区间过滤。区间很大的全市场同步请走 ``iter_daily``(有界分批)。
        """
        start_d = start_time.date() if isinstance(start_time, datetime) else start_time
        end_d = end_time.date() if isinstance(end_time, datetime) else end_time
        # 根数上界: 自然日 x 0.75(A 股年均交易日占比) + 余量; 用于 limit 单标的请求深度
        span_days = max(1, (end_d - start_d).days)
        count = min(int(span_days * 0.8) + 30, 8000)

        frames: list[pl.DataFrame] = []
        total = len(symbols)
        done = 0
        for batch in self._client.iter_bars_batches(
            symbols, period="day", count=count, batch_size=_DAILY_BATCH
        ):
            rows: list[dict] = []
            for symbol, bars in batch:
                for bar in bars:
                    row = _kline_row(symbol, bar)
                    if row is None:
                        continue
                    if start_d <= row["date"] <= end_d:
                        rows.append(row)
            done += len(batch)
            if on_chunk_done is not None:
                on_chunk_done(done, total)  # 契约: 空批次也要覆盖, 保证最终 cur == total
            df = self._daily_frame(rows)
            if df.height:
                frames.append(df)
        if not frames:
            return self._daily_frame([])
        return pl.concat(frames).sort(["symbol", "date"])

    def iter_daily(
        self,
        symbols: list[str],
        start_time: datetime | date,
        end_time: datetime | date,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> Any:
        """有界分批返回日 K(与 get_daily 同形); 面板全市场历史同步优先消费本方法。

        契约: 每批有明确上界、不得先全量 concat; ``on_chunk_done(cur, total)``
        必须覆盖空批次且最终 ``cur == total``。
        """
        start_d = start_time.date() if isinstance(start_time, datetime) else start_time
        end_d = end_time.date() if isinstance(end_time, datetime) else end_time
        span_days = max(1, (end_d - start_d).days)
        count = min(int(span_days * 0.8) + 30, 8000)

        total = len(symbols)
        done = 0
        for batch in self._client.iter_bars_batches(
            symbols, period="day", count=count, batch_size=_DAILY_BATCH
        ):
            rows: list[dict] = []
            for symbol, bars in batch:
                for bar in bars:
                    row = _kline_row(symbol, bar)
                    if row is None:
                        continue
                    if start_d <= row["date"] <= end_d:
                        rows.append(row)
            done += len(batch)
            if on_chunk_done is not None:
                on_chunk_done(done, total)
            yield self._daily_frame(rows)

    # ---- 实时快照 -------------------------------------------------------

    def get_realtime(self) -> list[dict]:
        """全市场实时快照 → ``list[dict]``。**软失败返回 []**(不抛异常)。"""
        symbols = self._client.all_a_shares()
        if not symbols:
            logger.warning("eltdx get_realtime: 取不到代码表, 本轮返回空")
            return []
        snaps = self._client.snapshots(symbols, batch_size=_SNAPSHOT_BATCH)
        out: list[dict] = []
        for snap in snaps:
            row = _snapshot_row(snap)
            if row is not None:
                out.append(row)
        if not out:
            logger.warning("eltdx get_realtime: 快照全部无法映射(上游结构可能变化), 返回空")
        return out

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照 → ``list[dict]``(行字段同 get_realtime)。

        契约: 失败返回 None(保留上轮缓存), 成功无数据返回 []。
        eltdx 快照接口可同时返回指数, 故与 A 股同路径。
        """
        if not symbols:
            return []
        try:
            snaps = self._client.snapshots(symbols, batch_size=_SNAPSHOT_BATCH)
        except Exception as e:  # 软失败: 返回 None 让服务保留上一轮有效缓存
            logger.warning("eltdx 指数快照失败(软失败, 保留上轮缓存): %s", e)
            return None
        if not snaps:
            return None  # 软失败: 让服务保留上一轮有效缓存
        out: list[dict] = []
        for snap in snaps:
            row = _snapshot_row(snap)
            if row is not None:
                out.append(row)
        return out

    # ---- 设置页「试拉」 -------------------------------------------------

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页「试拉测试」: 返回 {provider, dataset, rows, columns, preview, error?}。"""
        if dataset not in _DATASETS:
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "error": f"eltdx 未声明数据集 {dataset!r}, 该数据集会回退 TickFlow",
            }
        try:
            if dataset == "daily":
                syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
                df = self.get_daily(syms, datetime.now() - timedelta(days=30), datetime.now())
                return self._preview(dataset, df)
            syms = [s for s in (symbols or [])][:5]
            rows = self.get_realtime_indices(syms) or [] if syms else self.get_realtime()
            df = pl.DataFrame(rows[:20]) if rows else pl.DataFrame()
            return self._preview(dataset, df)
        except Exception as e:
            return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}

    @staticmethod
    def _preview(dataset: str, df: pl.DataFrame) -> dict:
        head = df.head(5).to_dicts()
        for row in head:  # date/datetime → ISO 字符串, 保证 JSON 可序列化
            for k, v in list(row.items()):
                if isinstance(v, (date, datetime)):
                    row[k] = v.isoformat()
        return {
            "provider": "eltdx",
            "dataset": dataset,
            "rows": df.height,
            "columns": df.columns,
            "preview": head,
        }
