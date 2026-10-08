"""eltdx 数据源插件契约测试。

按 docs/plugin-development.md「测试要求」的 7 项覆盖, **不依赖真实网络与主站**:
用假 client(替身对象)注入, 验证字段映射、单位换算、软失败、能力声明与 loader 集成。

范本: backend/tests/test_fuyao_provider.py
"""

from __future__ import annotations

import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from types import ModuleType, SimpleNamespace
from unittest.mock import ANY

import polars as pl
import pytest

from app.plugins.eltdx import client as eltdx_client
from app.plugins.eltdx.client import EltDxClient, to_eltdx_code, to_panel_symbol
from app.plugins.eltdx.provider import (
    _CN_TZ,
    EltDxProvider,
    _snapshot_ts,
    availability,
)

# ---------------------------------------------------------------------------
# 代码格式转换
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("sz000001", "000001.SZ"),
        ("sh600000", "600000.SH"),
        ("bj430047", "430047.BJ"),
        ("000001.SZ", "000001.SZ"),
        ("600000.sh", "600000.SH"),
        ("000001", "000001.SZ"),  # 裸码按 A 股规则推导: 0 开头 → 深市
        ("600519", "600519.SH"),  # 6 开头 → 沪市
        ("", None),
        ("abc", None),
        ("sz00001", None),  # 位数不对
        ("xx000001", None),  # 交易所前缀非法
    ],
)
def test_to_panel_symbol(raw: str, expected: str | None) -> None:
    assert to_panel_symbol(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("000001.SZ", "sz000001"),
        ("600000.SH", "sh600000"),
        ("430047.BJ", "bj430047"),
        ("000001", None),
        ("sz000001", None),
        ("000001.XX", None),
    ],
)
def test_to_eltdx_code(raw: str, expected: str | None) -> None:
    assert to_eltdx_code(raw) == expected


# ---------------------------------------------------------------------------
# 假替身: 构造 eltdx 返回对象的最小形状
# ---------------------------------------------------------------------------


def _bar(
    dt: datetime,
    *,
    o: float = 10.0,
    h: float = 10.5,
    low: float = 9.5,
    c: float = 10.2,
    volume_lots: float = 1234.0,
    amount: float = 1_234_000.0,
) -> SimpleNamespace:
    return SimpleNamespace(
        time=dt,
        open=o,
        high=h,
        low=low,
        close=c,
        volume_lots=volume_lots,
        amount=amount,
    )


def _snap(
    full_code: str,
    *,
    last: float = 10.0,
    prev: float = 9.9,
    pct: float = 1.0101,  # 百分数制: 1.0101 表示 1.0101%
    change: float | None = 0.1,
    total_hand: int = 5000,
    amount: float = 5_000_000.0,
    time_raw: int | None = 15330366,
) -> SimpleNamespace:
    return SimpleNamespace(
        full_code=full_code,
        last_price=last,
        pre_close_price=prev,
        open_price=9.95,
        high_price=10.2,
        low_price=9.8,
        change_pct=pct,
        change=change,
        total_hand=total_hand,
        amount=amount,
        time_raw=time_raw,
    )


class _FakeClient:
    """假 EltDxClient: 记录调用并按预设返回, 不连主站。"""

    def __init__(
        self, *, bars=None, shares=None, snapshots=None, fail_bars=False, minute_bars=None
    ):
        self._bars = bars or {}
        self._minute_bars = minute_bars or {}
        self._shares = shares or []
        self._snapshots = snapshots or []
        self._fail_bars = fail_bars
        self.calls: list[tuple] = []

    def all_a_shares(self):
        self.calls.append(("all_a_shares",))
        return list(self._shares)

    def all_indices(self):
        return []

    def bars(self, symbol, *, period="day", count):
        self.calls.append(("bars", symbol, period, count))
        if self._fail_bars:
            raise RuntimeError("boom")
        # 分钟: 用 minute_bars 提供(与日K分开, 便于断言 period 路由)
        if period == "1m" and self._minute_bars:
            return list(self._minute_bars.get(symbol, []))[:count]
        return list(self._bars.get(symbol, []))[:count]

    def iter_bars_batches(self, symbols, *, period="day", count, batch_size):
        """与真实实现同形的有界分批(yield [(symbol, bars)])。"""
        step = max(1, batch_size)
        for i in range(0, len(symbols), step):
            chunk = symbols[i : i + step]
            out = [(s, list(self._bars.get(s, []))) for s in chunk]
            out.sort(key=lambda item: item[0])
            yield out

    def bars_multi(self, symbols, *, period="day", count):
        """批量取 K 线(真实实现一次请求多个 code); 这里按 minute_bars 返回。"""
        self.calls.append(("bars_multi", tuple(symbols), period, count))
        out = []
        for s in symbols:
            bars = self._minute_bars.get(s) if period == "1m" else self._bars.get(s)
            bars = list(bars or [])[-max(1, count) :]
            if bars:
                out.append((s, bars))
        return out

    def snapshots(self, symbols, *, batch_size):
        self.calls.append(("snapshots", len(symbols)))
        return list(self._snapshots)

    def depth(self, symbols):
        self.calls.append(("depth", tuple(symbols)))
        return None  # 默认无盘口(需要盘口的用例自行覆写)

    def finance_batch(self, codes):
        """默认无财务数据(返回 None); 财务用例通过子类覆写本方法。"""
        self.calls.append(("finance_batch", tuple(codes)))
        return None

    def adjustment_factors(self, code):
        """除权事件列表; 默认返回 [](既有用例不受影响)。

        ``_adj_events`` 由 :func:`_adj_client` 就地挂上(不改本类 ``__init__`` 签名,
        避免既有子类覆写 ``__init__`` 时漏传新参数)。``raise_for`` 里的代码抛异常,
        用于验证 get_adj_factors 的单标的隔离。
        """
        self.calls.append(("adj", code))
        if code in getattr(self, "_adj_raise", ()):
            raise RuntimeError("station down")
        return list(getattr(self, "_adj_events", {}).get(code, []))

    def close(self):
        self.calls.append(("close",))


def _provider(fake: _FakeClient) -> EltDxProvider:
    p = EltDxProvider()
    p._client = fake
    return p


# ---------------------------------------------------------------------------
# 1) 字段映射与单位转换
# ---------------------------------------------------------------------------


def test_daily_field_mapping_and_units() -> None:
    """日K: 不复权 OHLC 直用; volume 取 volume_lots(手); date 取北京墙钟日期。"""
    today = datetime(2026, 9, 29, 15, 0)
    fake = _FakeClient(bars={"000001.SZ": [_bar(today, volume_lots=690979.12, amount=784632832.0)]})
    df = _provider(fake).get_daily(["000001.SZ"], today - timedelta(days=5), today)

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["symbol"] == "000001.SZ"
    assert row["date"] == date(2026, 9, 29)
    assert row["close"] == 10.2
    assert row["volume"] == 690979.12  # 手, 不换算
    assert row["amount"] == 784632832.0  # 元
    assert "quote_ts" not in df.columns  # 源未提供则不出现(未伪造)


def test_realtime_change_pct_converted_from_percent_to_decimal() -> None:
    """契约红线: eltdx change_pct 是百分数制, 面板要小数制 → 必须 /100。"""
    fake = _FakeClient(snapshots=[_snap("sz000001", last=11.35, prev=11.3, pct=0.442478)])
    rows = _provider(fake).get_realtime_indices(["000001.SZ"])

    assert rows is not None and len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "000001.SZ"
    assert row["last_price"] == 11.35
    assert row["prev_close"] == 11.3
    # 0.442478% → 0.00442478(小数制)
    assert row["change_pct"] == pytest.approx(0.442478 / 100.0)
    assert row["change_pct"] == pytest.approx(0.05 / 11.3, rel=1e-3)


def test_realtime_volume_taken_from_total_hand_as_lots() -> None:
    """total_hand 实测为"手"(amount/(last x hand)≈100 自验) → 直用不换算。"""
    fake = _FakeClient(
        snapshots=[_snap("sz000001", total_hand=690979, amount=784632832.0, last=11.35)]
    )
    row = _provider(fake).get_realtime_indices(["000001.SZ"])[0]
    assert row["volume"] == 690979


def test_realtime_change_amount_derived_when_missing() -> None:
    """change_amount 缺失时按固定口径 last - prev_close 推导(契约允许)。"""
    fake = _FakeClient(snapshots=[_snap("sz000001", last=11.35, prev=11.3, change=None)])
    row = _provider(fake).get_realtime_indices(["000001.SZ"])[0]
    assert row["change_amount"] == pytest.approx(0.05)


def test_missing_fields_are_none_not_fabricated() -> None:
    """缺失字段一律 None: amplitude/turnover_rate 不启发式补全, name 置 None。"""
    fake = _FakeClient(snapshots=[_snap("sz000001")])
    row = _provider(fake).get_realtime_indices(["000001.SZ"])[0]
    assert row["name"] is None
    assert row["amplitude"] is None
    assert row["turnover_rate"] is None


def test_snapshot_ts_parsing_and_invalid(monkeypatch) -> None:
    """time_raw = 当日 HHMMSScc 紧凑整数(实测 8 位, 末 2 位为百分秒)。

    真机样例: 15330366 → 15:33:03.66(当日最后一笔, 收盘后 15:33 时刻)。
    非法值(时/分/秒越界、位数不足)返回 None, 由下游退本地时间。

    还原时刻**必须显式指定 _CN_TZ**: 契约是"A 股墙钟(北京时间)", 而
    ``datetime.fromtimestamp(ts)`` 会用**运行机器**的本地时区 —— 本机
    UTC+8 恰好等于北京时区所以通过, GitHub runner 是 UTC, 于是同一份代码
    在 CI 上得出 07:33 而非 15:33, 长期只有 CI 红。
    """
    _assume_trading_day(monkeypatch, True)
    assert _CN_TZ.utcoffset(None) == timedelta(hours=8), "A 股墙钟必须是北京时间"

    ts = _snapshot_ts(15330366)  # 15:33:03.66
    assert ts is not None
    dt = datetime.fromtimestamp(ts / 1000, tz=_CN_TZ)
    assert (dt.hour, dt.minute, dt.second) == (15, 33, 3)
    assert _snapshot_ts(None) is None
    assert _snapshot_ts("abc") is None
    assert _snapshot_ts(12345) is None  # 位数不足 6
    assert _snapshot_ts(99999999) is None  # hour=99 越界
    assert _snapshot_ts(15609999) is None  # minute=60 越界


def _assume_trading_day(monkeypatch, verdict) -> None:
    """固定交易日探针结论, 隔离真实网络 (探针自身另有 test_trading_day.py 覆盖)。"""
    from app.services import trading_day

    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: verdict)


def test_snapshot_ts_withheld_on_holiday(monkeypatch) -> None:
    """休市日回归: 上游 time_raw 不含日期, 不得用本地当日伪造归属。

    2026-10-01 国庆实测: eltdx 全市场快照冻结在 09-30 14:59:59.990, 而 time_raw
    (14595999) 仍被按"本地当日"还原成 10-01 14:59:59.990。该伪时间戳同时击穿:
      1. _build_daily 按 quote_ts 过滤非当日记录 (专治停牌股回归) 的防线;
      2. final 定版边界比较 → 陈旧快照被当定版落盘, 再用 cn_today() 打戳, 造出与
         上一交易日逐行相同的假分区 (5561 只 OHLC 全等, 全市场涨跌幅归零)。
    故休市日必须返回 None (日期未知), 交由服务层的 filter_halt_days 等防线兜底。
    """
    _assume_trading_day(monkeypatch, False)
    assert _snapshot_ts(14595999) is None
    assert _snapshot_ts(15330366) is None
    # 盘口记录走同一纪律
    from app.plugins.eltdx.provider import _hhmmss_ts

    assert _hhmmss_ts(153252) is None


def test_snapshot_ts_unknown_verdict_keeps_restoring(monkeypatch) -> None:
    """探针未知 (未配 fuyao 且 tickflow 不可用) 时维持还原, 不误伤盘中真实行情。"""
    _assume_trading_day(monkeypatch, None)
    assert _snapshot_ts(15330366) is not None


# ---------------------------------------------------------------------------
# 2) 响应结构变体: 异常/空数据
# ---------------------------------------------------------------------------


def test_daily_drops_bars_with_missing_ohlc() -> None:
    """单根缺 OHLC → 丢弃该根(不伪造), 不影响同批其他根。"""
    today = datetime(2026, 9, 29, 15, 0)
    good = _bar(today)
    bad = SimpleNamespace(
        time=today, open=None, high=None, low=None, close=None, volume_lots=1.0, amount=1.0
    )
    fake = _FakeClient(bars={"000001.SZ": [good, bad]})
    df = _provider(fake).get_daily(["000001.SZ"], today - timedelta(days=5), today)
    assert df.height == 1


def test_daily_filters_out_of_range_rows() -> None:
    """区间外的 K 线必须过滤掉(源返回深度大于请求区间)。"""
    fake = _FakeClient(
        bars={
            "000001.SZ": [
                _bar(datetime(2026, 9, 20, 15, 0)),
                _bar(datetime(2026, 9, 29, 15, 0)),
            ]
        }
    )
    df = _provider(fake).get_daily(["000001.SZ"], datetime(2026, 9, 25), datetime(2026, 9, 30))
    assert df.height == 1
    assert df.row(0, named=True)["date"] == date(2026, 9, 29)


def test_realtime_snapshot_row_without_symbol_is_dropped() -> None:
    """快照行识别不出 symbol → 丢弃; 全部丢弃时返回空列表并告警。"""
    fake = _FakeClient(snapshots=[_snap("zzzzzz"), _snap("sz000001")])
    rows = _provider(fake).get_realtime_indices(["000001.SZ"])
    assert rows is not None
    assert [r["symbol"] for r in rows] == ["000001.SZ"]


def test_realtime_snapshot_bare_code_without_exchange_is_dropped() -> None:
    """交易所缺失时**不得**按首位猜后缀。

    纵深防御(机制已复现, 触发前提未观测到): 快照的 ``exchange`` 为空时, SDK 的
    ``full_code`` 属性(``f"{exchange}{code}"``)退化成裸 6 位代码(如 "000001"),
    早期实现直接交给 ``to_panel_symbol`` 按首位推断 —— 上证指数 ``000001.SH``
    于是被写成 ``000001.SZ``, 与真实的平安银行撞在同一个 symbol 上, 在 kline_daily
    留下重复行, 令矩阵构建报 "MarketDataMatrix requires unique timestamp/symbol rows"。
    正确行为: 丢弃该行(少一行好过把指数值写成股票价)。

    注(2026-09-30, 网关 3.2.2 实测): 真实快照**不含 full_code 字段**, 且 ``exchange``
    从不缺失或为空(抽样 80 只全为 ``sh``)。故本用例是人为构造异常输入来钉住防护行为,
    **不代表**当前上游会走到该分支。
    """
    bare = SimpleNamespace(
        full_code="000001",  # exchange 为空 → full_code 无交易所标识
        last_price=3840.83,
        pre_close_price=3820.0,
        open_price=3839.25,
        high_price=3847.68,
        low_price=3833.09,
        change_pct=0.5,
        change=20.83,
        total_hand=100,
        amount=1.0,
        time_raw=15330366,
    )
    fake = _FakeClient(snapshots=[bare, _snap("sz000001")])
    rows = _provider(fake).get_realtime_indices(["000001.SZ"])

    assert rows is not None
    # 裸代码行被丢弃, 只留带交易所信息的真实深市标的
    assert [r["symbol"] for r in rows] == ["000001.SZ"]


def test_realtime_snapshot_honours_explicit_exchange() -> None:
    """显式 ``exchange`` 优先: 沪市指数在 exchange='sh' 时必须得到 .SH。"""
    snap = SimpleNamespace(
        exchange="sh",
        code="000001",
        last_price=3840.83,
        pre_close_price=3820.0,
        open_price=3839.25,
        high_price=3847.68,
        low_price=3833.09,
        change_pct=0.5,
        change=20.83,
        total_hand=100,
        amount=1.0,
        time_raw=15330366,
    )
    fake = _FakeClient(snapshots=[snap])
    rows = _provider(fake).get_realtime_indices(["000001.SH"])

    assert rows is not None
    assert [r["symbol"] for r in rows] == ["000001.SH"]


# ---------------------------------------------------------------------------
# 3) 分批: iter_daily 有界分批 + on_chunk_done 覆盖
# ---------------------------------------------------------------------------


def test_iter_daily_batches_bounded_and_callback_totals() -> None:
    """iter_daily 每批有界; on_chunk_done 覆盖所有批次且末次 cur == total。"""
    today = datetime(2026, 9, 29, 15, 0)
    syms = ["000001.SZ", "600000.SH", "300750.SZ"]
    fake = _FakeClient(bars={s: [_bar(today)] for s in syms})
    seen: list[tuple[int, int]] = []
    batches = list(
        _provider(fake).iter_daily(
            syms, today - timedelta(days=5), today, on_chunk_done=lambda c, t: seen.append((c, t))
        )
    )
    assert sum(b.height for b in batches) == 3
    assert seen[-1] == (3, 3)  # 末次回调覆盖到 total


def test_iter_daily_yields_empty_frames_for_empty_batches() -> None:
    """空批也要回调(契约): 源无数据时仍推进 cur, 最终 cur == total。"""
    today = datetime(2026, 9, 29, 15, 0)
    fake = _FakeClient(bars={})  # 全部标的都取不到
    seen: list[tuple[int, int]] = []
    frames = list(
        _provider(fake).iter_daily(
            ["000001.SZ", "600000.SH"],
            today - timedelta(days=5),
            today,
            on_chunk_done=lambda c, t: seen.append((c, t)),
        )
    )
    assert all(f.height == 0 for f in frames)
    assert seen[-1] == (2, 2)


# ---------------------------------------------------------------------------
# 4) 软失败
# ---------------------------------------------------------------------------


def test_get_realtime_soft_fails_to_empty_list() -> None:
    """契约: realtime 失败必须返回 [] 且不抛(不阻断面板轮询线程)。"""

    class _Boom(_FakeClient):
        def snapshots(self, symbols, *, batch_size):
            raise RuntimeError("station down")

    p = _provider(_Boom())
    assert p.get_realtime_indices(["000001.SZ"]) is None  # 指数: None 保留上轮缓存
    # 全市场路径: 代码表取不到 → []
    assert _provider(_FakeClient(shares=[])).get_realtime() == []


def test_get_realtime_maps_all_shares() -> None:
    """全市场路径: 代码表 → 快照 → 行(含 symbol 归一)。"""
    fake = _FakeClient(
        shares=["000001.SZ", "600000.SH"], snapshots=[_snap("sz000001"), _snap("sh600000")]
    )
    rows = _provider(fake).get_realtime()
    assert [r["symbol"] for r in rows] == ["000001.SZ", "600000.SH"]
    assert ("all_a_shares",) in fake.calls


# ---------------------------------------------------------------------------
# 5) 能力声明
# ---------------------------------------------------------------------------


def test_declared_datasets_only() -> None:
    """声明的数据集 = 已实现的集合(未声明的 provider_has_dataset 为 False → 回退)。"""
    p = EltDxProvider()
    ds = p.config.datasets
    assert set(ds) == {
        "daily",
        "realtime",
        "minute",
        "full_minute",
        "depth5",
        "auction",
        "financial",
        "adj_factor",
    }


def test_test_dataset_reports_error_for_undeclared() -> None:
    """试拉未声明的数据集名 → 返回 error 说明会回退, 不抛异常。"""
    out = EltDxProvider().test_dataset("not_a_dataset")
    assert out["rows"] == 0
    assert "回退" in out["error"]


# ---------------------------------------------------------------------------
# 6) 可用性两态
# ---------------------------------------------------------------------------


def test_availability_true_when_eltdx_importable(monkeypatch) -> None:
    """inproc 模式: 依赖可 import 即可用。

    eltdx 是**可选依赖**(未在 pyproject/lock 中声明), 所以这里不能假设它已安装 ——
    否则 CI 上(只装 dev extra)必然失败。改为用 import 注入构造"已安装"态,
    断言契约本身, 而不是断言某个环境事实。
    """
    monkeypatch.setenv("ELTDX_TRANSPORT", "inproc")
    monkeypatch.setitem(sys.modules, "eltdx", ModuleType("eltdx"))

    ok, reason = availability()

    assert ok is True
    assert "eltdx" in reason


def test_availability_false_when_import_fails(monkeypatch) -> None:
    """inproc 模式: 依赖缺失 → (False, 安装提示), 不抛异常。"""
    import builtins

    monkeypatch.setenv("ELTDX_TRANSPORT", "inproc")
    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "eltdx":
            raise ImportError("No module named 'eltdx'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    ok, reason = availability()
    assert ok is False
    assert "安装依赖" in reason


def test_availability_http_mode_requires_reachable_gateway(monkeypatch) -> None:
    """http 模式: 网关不可达必须判为**不可用**(因为不会自动回退进程内)。

    这是刻意的严格口径: 若网关挂了却仍报"可用", 用户会看到面板静默无数据,
    与本次事故的观感完全一样。
    """
    monkeypatch.setenv("ELTDX_TRANSPORT", "http")
    monkeypatch.setenv("ELTDX_HTTP_URL", "http://127.0.0.1:9")  # 必然拒连
    monkeypatch.setenv("ELTDX_HTTP_TIMEOUT", "2")
    ok, reason = availability()
    assert ok is False
    assert "网关不可达" in reason
    assert "eltdx-http" in reason, "提示里要告诉用户怎么启动网关"


# ---------------------------------------------------------------------------
# 7) loader 集成
# ---------------------------------------------------------------------------


def test_manifest_parses_and_entry_loads() -> None:
    """plugin.yaml 可解析、字段齐备, entry/check 指向的类与函数真实存在。"""
    import yaml

    from app.data_providers.custom.loader import plugin_manifest

    manifest = plugin_manifest("eltdx")
    assert manifest is not None, "plugin.yaml 未被识别"
    assert manifest["name"] == "eltdx"
    assert manifest["runtime"] == "python"
    assert set(manifest["datasets"]) == {
        "daily",
        "realtime",
        "minute",
        "full_minute",
        "depth5",
        "auction",
        "financial",
        "adj_factor",
    }
    # manifest 与 provider 的声明必须一致, 否则设置页展示与运行时路由会脱节
    assert set(manifest["datasets"]) == set(EltDxProvider().config.datasets)
    assert manifest["install_hint"]
    # entry/check 可解析到真实对象
    from app.data_providers.custom.loader import _load_entry

    cls = _load_entry(manifest["entry"])
    assert cls is EltDxProvider
    chk = _load_entry(manifest["check"])
    assert chk is availability
    assert yaml.safe_load is not None


# ---------------------------------------------------------------------------
# client 层: 分批边界与并发参数
# ---------------------------------------------------------------------------


def test_iter_bars_batches_rejects_bad_batch_size() -> None:
    """batch_size 必须为正(防调用方传 0 造成死循环)。"""
    c = EltDxClient()
    with pytest.raises(ValueError, match="batch_size"):
        next(c.iter_bars_batches(["000001.SZ"], count=10, batch_size=0))


def test_client_symbols_batch_splits() -> None:
    """snapshots 按 batch_size 分片; 无有效代码时返回 [](软失败)。"""
    c = EltDxClient()
    assert c.snapshots(["bad-symbol"], batch_size=10) == []
    assert c.snapshots([], batch_size=10) == []


def test_snapshot_batch_size_respects_eltdx_hard_limit() -> None:
    """回归防护: 快照分片必须 <=80。

    实测 eltdx 的 quotes.get_snapshots 硬上限为 **80 只/请求** —— 请求 81/100/400/700
    均被**静默截断为 80**; 请求 800/1600/3000 直接断连(os error 10054)。
    早期实现按"包大小"取 800, 导致每一片都超限 -> 全市场快照返回 0 行(覆盖率 0%),
    而 realtime 数据集若被路由到本插件, 面板将拿不到任何实时行情。
    """
    from app.plugins.eltdx.provider import _SNAPSHOT_BATCH

    assert _SNAPSHOT_BATCH <= 80, "超过 80 会被上游截断/断连, 全市场将返回空"


def test_panel_daily_columns_single_source() -> None:
    """插件列定义复用 normalizer.DAILY_COLS(单源, 避免双份漂移)。"""
    from app.data_providers.normalizer import DAILY_COLS

    assert eltdx_client is not None
    assert DAILY_COLS[0] == "symbol"
    assert "quote_ts" in DAILY_COLS


def test_daily_returns_empty_frame_when_no_rows() -> None:
    """无数据返回空帧(契约: 不抛异常), 列可缺省。"""
    p = _provider(_FakeClient(bars={}))
    df = p.get_daily(["000001.SZ"], datetime(2026, 9, 1), datetime(2026, 9, 2))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


# ---------------------------------------------------------------------------
# 分钟 K: 契约 [symbol, datetime(北京墙钟 naive), o/h/l/c, volume(手), amount(元)]
# ---------------------------------------------------------------------------

_CN = timezone(timedelta(hours=8))


def _mbar(
    hh: int,
    mm: int,
    *,
    sec: int = 0,
    c: float = 10.2,
    vol: float = 100.0,
    amt: float = 102000.0,
    day: date | None = None,
):
    """1m KlineBar 替身: time 为 Asia/Shanghai aware(与 eltdx 实测一致)。

    ``day`` 默认**当天**: ``get_intraday_batch`` / ``get_intraday_latest`` 内部用
    ``date.today()` 构造当日窗口, 硬编码日期会让行被区间过滤掉。需要指定日期
    (如测区间过滤)时传 ``day=``。
    """
    d = day or date.today()
    return _bar(
        datetime(d.year, d.month, d.day, hh, mm, sec, tzinfo=_CN),
        o=c,
        h=c + 0.01,
        low=c - 0.01,
        c=c,
        volume_lots=vol,
        amount=amt,
    )


def test_minute_field_mapping_and_naive_beijing_wallclock() -> None:
    """契约红线: datetime 必须是北京时间墙钟 **naive**(非 UTC、无 tzinfo)。"""
    d = date(2026, 9, 29)
    fake = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31, day=d), _mbar(9, 32, day=d)]})
    df = _provider(fake).get_minute(["000001.SZ"], d, d)

    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert df.height == 2
    assert df.schema["datetime"].time_zone is None, "必须是 naive(无时区)"
    first = df["datetime"][0]
    assert (first.hour, first.minute) == (9, 31), "必须是北京墙钟(若误转 UTC 会变 01:31)"


def test_minute_uses_1m_period_route() -> None:
    """分钟必须走 period='1m'(不是 day, 也不是分时接口)。"""
    d = date(2026, 9, 29)
    fake = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31, day=d)]})
    _provider(fake).get_minute(["000001.SZ"], d, d)
    assert ("bars", "000001.SZ", "1m", ANY) in [
        (c[0], c[1], c[2], ANY) for c in fake.calls if c[0] == "bars"
    ]


def test_minute_filters_out_of_range_bars() -> None:
    """区间外分钟必须过滤(源可能返回超过请求区间的深度)。"""
    d = date(2026, 9, 29)
    fake = _FakeClient(
        minute_bars={
            "000001.SZ": [
                _bar(datetime(2026, 9, 25, 10, 0, tzinfo=_CN)),  # 区间外(前一交易日)
                _mbar(9, 31, day=d),
            ]
        }
    )
    df = _provider(fake).get_minute(["000001.SZ"], d, d)
    assert df.height == 1
    assert df["datetime"][0].day == 29


def test_minute_request_count_scales_by_240_bars_per_day() -> None:
    """回归防护: 分钟请求根数必须按「每日 240 根」折算, 不能把自然日数当根数。

    曾实测到的缺陷: 实现写成 ``int(span_days * 0.8) + 10``(5 天 -> 14 根),
    14 根只覆盖不到 1 天, 于是 5 天的同步只落盘最后一天(数据页「分钟K」显示 1)。
    正确: 5 自然日 -> 约 2~3 交易日 -> 需 >=720 根。
    """
    fake = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31)]})
    _provider(fake).get_minute(["000001.SZ"], date(2026, 9, 24), date(2026, 9, 29))

    calls = [c for c in fake.calls if c[0] == "bars" and c[2] == "1m"]
    assert calls, "分钟应走 bars(period='1m')"
    count = calls[0][3]
    # 5 自然日 ~ 2-3 交易日; 至少要能覆盖这些天的每日 240 根
    assert count >= 2 * 240, f"5 自然日只请求 {count} 根, 不足 2 个交易日"
    assert count <= 12000, "不得超过 _MINUTE_MAX_BARS 上限"


def test_minute_request_count_scales_with_range() -> None:
    """更长区间的请求根数必须单调增大(否则长区间会静默截断成短区间)。"""
    fake1 = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31)]})
    _provider(fake1).get_minute(["000001.SZ"], date(2026, 9, 28), date(2026, 9, 29))
    short = next(c for c in fake1.calls if c[0] == "bars" and c[2] == "1m")[3]

    fake2 = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31)]})
    _provider(fake2).get_minute(["000001.SZ"], date(2026, 8, 1), date(2026, 9, 29))
    long = next(c for c in fake2.calls if c[0] == "bars" and c[2] == "1m")[3]

    assert long > short, "长区间的请求根数必须更大"


def test_minute_single_symbol_failure_isolated() -> None:
    """单标的异常隔离: 一只失败不影响其他标的(分钟走并发, 需逐个兜底)。"""

    class _Flaky(_FakeClient):
        def bars(self, symbol, *, period="day", count):
            # symbol 是 eltdx 代码(如 sh600000)
            if symbol == to_eltdx_code("600000.SH"):
                raise RuntimeError("station error")
            return super().bars(symbol, period=period, count=count)

    d = date(2026, 9, 29)
    fake = _Flaky(minute_bars={"000001.SZ": [_mbar(9, 31, day=d)]})
    df = _provider(fake).get_minute(["000001.SZ", "600000.SH"], d, d)
    assert df["symbol"].unique().to_list() == ["000001.SZ"]


def test_minute_empty_returns_typed_empty_frame() -> None:
    """无数据返回空帧但**列齐全**(下游按列消费, 不能缺列)。"""
    p = _provider(_FakeClient(minute_bars={}))
    df = p.get_minute(["000001.SZ"], date(2026, 9, 29), date(2026, 9, 29))
    assert df.height == 0
    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


# ---------------------------------------------------------------------------
# 全量分钟(full_minute): 修复轮 + 稳态增量轮
# ---------------------------------------------------------------------------


def test_intraday_batch_keeps_latest_count_per_symbol() -> None:
    """修复轮: 每只标的只保留最新 count 根(当日窗口语义)。"""
    bars = [_mbar(9, 30 + i) for i in range(1, 11)]  # 10 根
    fake = _FakeClient(minute_bars={"000001.SZ": bars, "600000.SH": bars})
    df = _provider(fake).get_intraday_batch(["000001.SZ", "600000.SH"], count=3)

    assert df.height == 6  # 2 标的 x 3 根
    for sym in ("000001.SZ", "600000.SH"):
        sub = df.filter(pl.col("symbol") == sym).sort("datetime")
        assert sub.height == 3
        assert sub["datetime"][-1].minute == 40  # 保留的是最新 3 根


def test_intraday_latest_full_market_uses_batch_endpoint() -> None:
    """全市场(symbols=None)走 `bars_multi` **批量**端点, 而非逐标的拉取。

    实测依据: eltdx 的 bars.get 支持批量 codes 且上限很高(2000 只/请求 3.9s 足额),
    全市场一遍约 11s, 可支撑稳态增量轮; 快照类接口的 80 只硬上限不适用于此路径。
    """
    fake = _FakeClient(
        shares=["000001.SZ", "600000.SH"],
        minute_bars={"000001.SZ": [_mbar(9, 31), _mbar(9, 32)]},
    )
    df = _provider(fake).get_intraday_latest(None, count=3)

    assert df.height > 0, "全市场应真正取数(批量端点可用), 不再降级为空帧"
    assert [c for c in fake.calls if c[0] == "bars_multi"], "必须走 bars_multi 批量路径"


def test_intraday_latest_works_on_bounded_pool() -> None:
    """传入受限标的池(监控池)时正常返回每只最新 N 根。"""
    bars = [_mbar(9, 30 + i) for i in range(1, 6)]
    fake = _FakeClient(minute_bars={"000001.SZ": bars})
    df = _provider(fake).get_intraday_latest(["000001.SZ"], count=2)
    assert df.height == 2
    assert df["datetime"][-1].minute == 35


def test_full_minute_declared_in_datasets() -> None:
    """full_minute 与 minute 都必须出现在 config.datasets(否则路由回退)。"""
    ds = EltDxProvider().config.datasets
    assert "minute" in ds
    assert "full_minute" in ds


# ---------------------------------------------------------------------------
# 五档盘口(depth5): 价量各 5 档 + 毫秒时间戳; 失败抛异常(不跨源回退)
# ---------------------------------------------------------------------------


def _depth_rec(code: str, *, ask1_vol: float = 100.0, bid1_vol: float = 200.0, time_raw=153252):
    """QuoteRefreshPage.records[] 替身: buy_levels/sell_levels 各 5 档。"""
    bids = tuple(
        SimpleNamespace(
            price=10.0 - i * 0.01, volume=(bid1_vol if i == 0 else 100.0 + i), price_delta_raw=0
        )
        for i in range(5)
    )
    asks = tuple(
        SimpleNamespace(
            price=10.01 + i * 0.01, volume=(ask1_vol if i == 0 else 100.0 + i), price_delta_raw=1
        )
        for i in range(5)
    )
    return SimpleNamespace(
        full_code=code, buy_levels=bids, sell_levels=asks, update_time_raw=time_raw
    )


class _FakeDepthPage:
    def __init__(self, records):
        self.records = tuple(records)


def test_depth_batch_contract_structure() -> None:
    """契约: {symbol: {bid_prices[5], bid_volumes[5], ask_prices[5], ask_volumes[5], timestamp}}。"""

    class _C(_FakeClient):
        def depth(self, symbols):
            return _FakeDepthPage([_depth_rec("sz000001"), _depth_rec("sh600000")])

    out = _provider(_C()).get_depth_batch(["000001.SZ", "600000.SH"])

    assert set(out) == {"000001.SZ", "600000.SH"}
    for row in out.values():
        assert set(row) == {"bid_prices", "bid_volumes", "ask_prices", "ask_volumes", "timestamp"}
        assert len(row["bid_prices"]) == len(row["bid_volumes"]) == 5
        assert len(row["ask_prices"]) == len(row["ask_volumes"]) == 5
        assert all(isinstance(v, float) for v in row["bid_volumes"])


def test_depth_zero_ask1_preserved_for_sealed_limit_up() -> None:
    """契约红线: 封死涨停时卖一量为 0, **0 必须保留**(服务据此判定"真封")。"""

    class _C(_FakeClient):
        def depth(self, symbols):
            return _FakeDepthPage([_depth_rec("sz000001", ask1_vol=0)])

    out = _provider(_C()).get_depth_batch(["000001.SZ"])
    assert out["000001.SZ"]["ask_volumes"][0] == 0, "0 不能被丢弃或写成 None"


def test_depth_volume_unit_is_lots_passthrough() -> None:
    """volume 单位为手, 与面板契约一致 → 直用不换算。"""

    class _C(_FakeClient):
        def depth(self, symbols):
            return _FakeDepthPage([_depth_rec("sz000001", ask1_vol=986, bid1_vol=3815)])

    row = _provider(_C()).get_depth_batch(["000001.SZ"])["000001.SZ"]
    assert row["ask_volumes"][0] == 986
    assert row["bid_volumes"][0] == 3815


def test_depth_raises_on_failure_no_cross_source_fallback() -> None:
    """契约: 盘口失败必须抛出(由服务按批隔离), **不得**自行回退其他数据源。"""

    class _C(_FakeClient):
        def depth(self, symbols):
            raise RuntimeError("station down")

    with pytest.raises(RuntimeError, match="station down"):
        _provider(_C()).get_depth_batch(["000001.SZ"])


def test_depth_empty_symbols_and_invalid_codes() -> None:
    """空入参/无效代码 → 返回 {} 且不触发网络调用。"""

    class _C(_FakeClient):
        def depth(self, symbols):
            raise AssertionError("不应调用 depth")

    p = _provider(_C())
    assert p.get_depth_batch([]) == {}
    assert p.get_depth_batch(["not-a-symbol"]) == {}


def test_depth_skips_records_without_levels() -> None:
    """无档位的记录丢弃(不产出空行)。"""

    class _C(_FakeClient):
        def depth(self, symbols):
            return _FakeDepthPage(
                [
                    SimpleNamespace(full_code="sz000001", buy_levels=(), sell_levels=()),
                    _depth_rec("sh600000"),
                ]
            )

    out = _provider(_C()).get_depth_batch(["000001.SZ", "600000.SH"])
    assert set(out) == {"600000.SH"}


@pytest.mark.parametrize(
    ("raw", "expect_hm"),
    [
        (153252, (15, 32, 52)),  # 6 位 HHMMSS(盘口实测形态)
        (15330366, (15, 33, 3)),  # 8 位 HHMMSScc(快照形态)
        (93100, (9, 31, 0)),
        (999999, None),  # hour=99 越界
        (None, None),
        ("abc", None),
    ],
)
def test_hhmmss_ts_parses_both_widths(raw, expect_hm, monkeypatch) -> None:
    """时间戳解析需兼容 6 位(盘口)与 8 位(快照)两种紧凑形态。

    休市日该函数返回 None (日期不可归属, 见 test_snapshot_ts_withheld_on_holiday),
    故这里固定"交易日"以免测试结果随运行日漂移。还原时刻必须显式带 _CN_TZ
    (北京时间), 不能依赖运行机器的本地时区 —— 见 test_snapshot_ts_parsing_and_invalid。
    """
    from app.plugins.eltdx.provider import _hhmmss_ts

    _assume_trading_day(monkeypatch, True)
    ts = _hhmmss_ts(raw)
    if expect_hm is None:
        assert ts is None
    else:
        dt = datetime.fromtimestamp(ts / 1000, tz=_CN_TZ)
        assert (dt.hour, dt.minute, dt.second) == expect_hm


def test_depth5_declared_in_datasets() -> None:
    """depth5 必须已声明(否则服务层 provider_has_dataset 判 False 而回退)。"""
    assert "depth5" in EltDxProvider().config.datasets


# ---------------------------------------------------------------------------
# 财务(shares 表): 单位换算 / 非正值剔除 / 未接入表返回空帧
# ---------------------------------------------------------------------------

# 面板 shares 契约列序(与 provider._SHARES_COLUMNS 一致)
_SHARES_COLS = ["symbol", "period_end", "announce_date", "total_shares", "float_shares"]


def _fin(
    code: str,
    exchange: str,
    *,
    total: float | None = 125008.15625,
    float_: float | None = 125008.15625,
    updated=date(2026, 8, 15),
) -> SimpleNamespace:
    """FinanceRecord 替身: 股本字段为 eltdx 的**万股**原始值。"""
    return SimpleNamespace(
        code=code,
        exchange=exchange,
        zong_gu_ben_raw_float=total,
        liu_tong_gu_ben_raw_float=float_,
        updated_date=updated,
    )


class _FinPage:
    """corporate.finance_batch 响应替身(只用到 records)。"""

    def __init__(self, records):
        self.records = tuple(records)


class _FinClient(_FakeClient):
    """财务假 client: finance_batch 返回给定记录。"""

    def __init__(self, records=()):
        super().__init__()
        self._records = list(records)

    def finance_batch(self, codes):
        self.calls.append(("finance_batch", tuple(codes)))
        return _FinPage(self._records)


def _fin_full_code(full_code: str) -> SimpleNamespace:
    """FinanceRecord 替身: 只带 ``full_code``(无 code/exchange)的记录。"""
    return SimpleNamespace(
        full_code=full_code,
        zong_gu_ben_raw_float=125008.15625,
        liu_tong_gu_ben_raw_float=125008.15625,
        updated_date=date(2026, 8, 15),
    )


def test_financials_shares_column_contract_and_order() -> None:
    """shares 表列必须**恰好**是契约 5 列且顺序一致(实现走 .select(keep))。"""
    p = _provider(_FinClient([_fin("600519", "sh")]))
    df = p.get_financials("shares", ["600519.SH"])

    assert isinstance(df, pl.DataFrame)
    assert df.columns == _SHARES_COLS, "列序即契约, 下游按下标/列名双消费"


def test_financials_shares_converts_wan_to_ge() -> None:
    """契约红线: eltdx 股本单位为**万股**, 面板要**股** → 必须 x10000。"""
    p = _provider(_FinClient([_fin("600519", "sh", total=125008.15625, float_=125008.15625)]))
    df = p.get_financials("shares", ["600519.SH"])

    row = df.row(0, named=True)
    # 茅台: 125008.15625 万股 = 1,250,081,562.5 股
    assert row["total_shares"] == pytest.approx(1250081562.5)
    assert row["float_shares"] == pytest.approx(1250081562.5)


def test_financials_shares_drops_non_positive_float_shares() -> None:
    """float_shares <= 0 或缺失 → 整行丢弃(面板 apply_historical_float_shares 会丢)。"""
    recs = [
        _fin("000001", "sz", total=1940591.0, float_=0.0),  # 0 → 丢
        _fin("600000", "sh", total=2935208.0, float_=-1.0),  # 负 → 丢
        _fin("000002", "sz", total=1000.0, float_=None),  # 缺失 → 丢
        _fin("600519", "sh"),  # 唯一有效
    ]
    df = _provider(_FinClient(recs)).get_financials("shares", ["000001.SZ", "600519.SH"])

    assert df["symbol"].to_list() == ["600519.SH"], "非正/缺失流通股本的行必须整行消失"


def test_financials_shares_total_falls_back_to_float() -> None:
    """总股本缺失或非正 → 回退用流通股本(不留 null, 下游免二次兜底)。"""
    recs = [
        _fin("000001", "sz", total=None, float_=1940591.0),  # 缺失
        _fin("600000", "sh", total=0.0, float_=2935208.0),  # 非正值
    ]
    df = _provider(_FinClient(recs)).get_financials("shares", ["000001.SZ", "600000.SH"])

    assert df.height == 2
    for row in df.to_dicts():
        assert row["total_shares"] is not None
        assert row["total_shares"] == pytest.approx(row["float_shares"])


def test_financials_shares_period_and_announce_from_updated_date() -> None:
    """period_end / announce_date 同取 updated_date, 序列化为 ISO ``YYYY-MM-DD`` 字符串。"""
    p = _provider(_FinClient([_fin("600519", "sh", updated=date(2026, 8, 15))]))
    row = p.get_financials("shares", ["600519.SH"]).row(0, named=True)

    assert row["period_end"] == "2026-08-15"
    assert row["announce_date"] == "2026-08-15"
    assert isinstance(row["period_end"], str), "必须是 ISO 字符串(不是 date 对象)"


def test_financials_shares_drops_row_without_updated_date() -> None:
    """无可用 updated_date → 丢弃(无 period_end 会被面板合并逻辑判为无效帧)。"""
    recs = [
        _fin("000001", "sz", updated=None),
        _fin("600000", "sh", updated=""),  # 空串同样不可用
        _fin("600519", "sh", updated=date(2026, 8, 15)),
    ]
    df = _provider(_FinClient(recs)).get_financials("shares", ["000001.SZ", "600519.SH"])

    assert df["symbol"].to_list() == ["600519.SH"]


@pytest.mark.parametrize(
    ("code", "exchange", "expected"),
    [
        ("600519", "sh", "600519.SH"),
        ("000001", "sz", "000001.SZ"),
        ("300750", "sz", "300750.SZ"),
        ("688981", "sh", "688981.SH"),
    ],
)
def test_financials_shares_symbol_normalization(code, exchange, expected) -> None:
    """记录只带 code(6 位) + exchange('sh'/'sz'/'bj') → 归一为 ``600519.SH`` 形态。"""
    p = _provider(_FinClient([_fin(code, exchange)]))
    df = p.get_financials("shares", [expected])

    assert df["symbol"].to_list() == [expected]


def test_financials_shares_normalizes_from_full_code() -> None:
    """记录带 ``full_code``(``bj430047`` 形态)时同样归一为面板格式。"""
    p = _provider(_FinClient([_fin_full_code("bj430047")]))
    df = p.get_financials("shares", ["430047.BJ"])

    assert df["symbol"].to_list() == ["430047.BJ"]


@pytest.mark.parametrize(
    ("code", "exchange", "want"),
    [
        ("430047", "bj", "430047.BJ"),
        ("830799", "bj", "830799.BJ"),
        ("920012", "bj", "920012.BJ"),
        ("600519", "sh", "600519.SH"),
        ("000001", "sz", "000001.SZ"),
    ],
)
def test_financials_shares_symbol_honours_explicit_exchange(code, exchange, want) -> None:
    """契约: 显式 ``exchange`` 必须优先于裸代码的交易所推断。

    回归防护(曾实测到的缺陷): 早期实现先用裸 ``code`` 调 ``to_panel_symbol``, 该函数
    按首位数字猜交易所(6/9 → SH, 其余 → SZ), 于是北交所(4xxxxx/8xxxxx/920xxx)被错标
    成 ``.SZ``, 而 ``exchange='bj'`` 分支成了不可达代码。沪/深因启发式恰好一致而掩盖
    了该缺陷, 故这里三市一并钉死。
    """
    p = _provider(_FinClient([_fin(code, exchange)]))
    df = p.get_financials("shares", [want])

    assert df["symbol"].to_list() == [want]


@pytest.mark.parametrize("table", ["metrics", "income", "balance_sheet", "cash_flow"])
def test_financials_unimplemented_tables_return_empty_frame(table) -> None:
    """契约红线: 未接入的表返回**空帧**(空 = 无意见), 让多源合并保留 TickFlow 值。"""
    fake = _FinClient([_fin("600519", "sh")])
    df = _provider(fake).get_financials(table, ["600519.SH"])

    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()
    assert not [c for c in fake.calls if c[0] == "finance_batch"], "未接入的表不应发起请求"


def test_financials_shares_empty_symbols_no_request() -> None:
    """空标的列表 → 直接返回空帧, 不调用 client(避免无谓网络往返)。"""
    fake = _FinClient([_fin("600519", "sh")])
    df = _provider(fake).get_financials("shares", [])

    assert df.is_empty()
    assert not [c for c in fake.calls if c[0] == "finance_batch"]


def test_financials_shares_batch_exception_keeps_partial_result() -> None:
    """单批失败不拖垮整表: 返回已取到的行, 绝不向上抛异常。"""
    from app.plugins.eltdx.provider import _FINANCE_BATCH

    ok_codes = ("sz000001",)

    class _Flaky(_FakeClient):
        def finance_batch(self, codes):
            # 只让**恰好等于** ok 代码的那一批成功, 其余(含重试/拆分)全炸
            if tuple(codes) == ok_codes:
                return _FinPage([_fin("000001", "sz")])
            raise RuntimeError("station down")

    # 正好一批: 该批含 ok 代码(独立成批) + 一批全炸的
    symbols = ["000001.SZ", *[f"{i:06d}.SZ" for i in range(2, _FINANCE_BATCH + 2)]]
    df = _provider(_Flaky()).get_financials("shares", symbols)

    assert df.height == 1, "成功那批的结果必须保留"
    assert df["symbol"].to_list() == ["000001.SZ"]
    assert df.columns == _SHARES_COLS


def test_financials_shares_all_batches_fail_returns_empty_not_raise() -> None:
    """全部批次失败 → 返回空帧(软失败), 不抛异常。"""

    class _Dead(_FakeClient):
        def finance_batch(self, codes):
            raise RuntimeError("station down")

    df = _provider(_Dead()).get_financials("shares", ["600519.SH"])
    assert isinstance(df, pl.DataFrame)
    assert df.is_empty()


def test_financials_shares_none_response_returns_empty() -> None:
    """client 返回 None(无 records 属性) → 空帧, 不 AttributeError。"""
    df = _provider(_FakeClient()).get_financials("shares", ["600519.SH"])
    assert df.is_empty()


def test_financial_declared_in_datasets() -> None:
    """financial 必须在 config.datasets 中(否则 provider_has_dataset 判 False 而回退)。"""
    assert "financial" in EltDxProvider().config.datasets


# ---------------------------------------------------------------------------
# 三大报表(f10): T 代码口径表 + nhytype 模板族分流
# ---------------------------------------------------------------------------


def _f10_response(
    columns: list[str], rows: list[dict], *, nhytype: int | None = 0
) -> SimpleNamespace:
    """f10.finance_report 响应替身: result_sets[0] 宽表 + result_sets[1] 带 nhytype。

    实测形态: 第 1 张结果集是报表宽表(无行业字段), 第 2 张是一行
    ``{rtype, nhytype, zqname}``。模板族判别必须读第 2 张。
    """
    sets = [
        SimpleNamespace(
            key="table0",
            columns=columns,
            rows=[SimpleNamespace(**r) for r in rows],
        )
    ]
    if nhytype is not None:
        sets.append(
            SimpleNamespace(
                key="table1",
                columns=["rtype", "nhytype", "zqname"],
                rows=[SimpleNamespace(rtype="zcfzb", nhytype=nhytype, zqname="测试")],
            )
        )
    return SimpleNamespace(result_sets=sets)


class _F10Client(_FakeClient):
    """f10 假 client: 按 (eltdx_code, report_type) 返回预设响应。"""

    def __init__(self, responses: dict[tuple[str, str], object], *, raise_for: set | None = None):
        super().__init__()
        self._responses = responses
        self._raise_for = raise_for or set()

    def finance_report(self, code, report_type):
        self.calls.append(("finance_report", code, report_type))
        if (code, report_type) in self._raise_for:
            raise RuntimeError("station down")
        resp = self._responses.get((code, report_type))
        if resp is None:
            raise RuntimeError("no such report")
        return resp


def test_f10_template_family_mapping() -> None:
    """nhytype → 模板族: 0 为通用, 其余(银行 1 / 保险 3 / 未知) 一律金融族。

    未知值按金融族是 **fail-safe**: 通用族的 T039 落在金融报表上是个无关小数字,
    会静默产出错误总资产; 而金融族映射在通用股上取不到列 → 留空由多源合并保留。
    """
    from app.plugins.eltdx.provider import _f10_template_family

    assert _f10_template_family(0) == "general"
    assert _f10_template_family(1) == "financial"
    assert _f10_template_family(3) == "financial"
    assert _f10_template_family(None) == "financial"
    assert _f10_template_family("2") == "financial"


def test_f10_balance_sheet_general_family() -> None:
    """通用族(nhytype=0): T039/T062/T071 → 资产/负债/权益。"""
    resp = _f10_response(
        ["rq", "T039", "T062", "T071", "T020", "T038", "T007", "T010"],
        [{
            "rq": "2026-06-30",
            "T039": 309050784569.31,
            "T062": 46954432394.95,
            "T071": 262096352174.36,
            "T020": 260724668103.4,
            "T038": 48326116465.91,
            "T007": 53518798979.08,
            "T010": 570895.04,
        }],
        nhytype=0,
    )
    p = _provider(_F10Client({("sh600519", "zcfzb"): resp}))
    df = p.get_financials("balance_sheet", ["600519.SH"], latest_only=True)

    assert df.height == 1
    row = df.to_dicts()[0]
    assert row["symbol"] == "600519.SH"
    assert row["period_end"] == "2026-06-30"
    assert row["total_assets"] == pytest.approx(309050784569.31)
    assert row["total_liabilities"] == pytest.approx(46954432394.95)
    assert row["total_equity"] == pytest.approx(262096352174.36)
    assert row["cash_and_equivalents"] == pytest.approx(53518798979.08)
    # 会计恒等式自证
    assert row["total_assets"] == pytest.approx(
        row["total_liabilities"] + row["total_equity"]
    )


def test_f10_balance_sheet_financial_family_uses_different_codes() -> None:
    """金融族(nhytype=1): 必须走 T048/T083/T093, **不得**误用通用族 T039。

    回归防护(实测): 平安银行 T039=104.6 亿而真实总资产 60287.9 亿, 差 576 倍。
    """
    resp = _f10_response(
        ["rq", "T039", "T048", "T083", "T093"],
        [{
            "rq": "2026-06-30",
            "T039": 10464000000,  # 通用族代码在金融报表里是无关数字
            "T048": 6028785000000.0,
            "T083": 5480571000000.0,
            "T093": 548214000000.0,
        }],
        nhytype=1,
    )
    p = _provider(_F10Client({("sz000001", "zcfzb"): resp}))
    df = p.get_financials("balance_sheet", ["000001.SZ"], latest_only=True)

    row = df.to_dicts()[0]
    assert row["total_assets"] == pytest.approx(6028785000000.0)
    assert row["total_assets"] != pytest.approx(10464000000)  # 未误用 T039
    assert row["total_liabilities"] == pytest.approx(5480571000000.0)
    assert row["total_equity"] == pytest.approx(548214000000.0)
    assert row["total_assets"] == pytest.approx(
        row["total_liabilities"] + row["total_equity"]
    )


def test_f10_balance_sheet_omits_unmapped_detail_for_financial() -> None:
    """金融族不映射明细科目(上游无稳定对应) → 该字段不落盘, 不用猜测值填充。"""
    resp = _f10_response(
        ["rq", "T048", "T083", "T093", "T020"],
        [{
            "rq": "2026-06-30",
            "T048": 6028785000000.0,
            "T083": 5480571000000.0,
            "T093": 548214000000.0,
            "T020": 999.0,
        }],
        nhytype=1,
    )
    p = _provider(_F10Client({("sz000001", "zcfzb"): resp}))
    row = p.get_financials("balance_sheet", ["000001.SZ"], latest_only=True).to_dicts()[0]

    assert "total_current_assets" not in row  # 未声明列根本不落盘


def test_f10_cash_flow_general_vs_financial_t041_semantics() -> None:
    """T041 在通用族是"现金净增加", 在金融族是"capex" —— 语义互换, 必须分流。

    这是最危险的一处: 若沿用通用族映射, 金融股的 capex 会被写成现金净增加。
    """
    general = _f10_response(
        ["rq", "T017", "T029", "T038", "T041", "T024"],
        [{"rq": "2026-06-30", "T017": 70690750119.06, "T029": 25640543520.6,
          "T038": -37944297802.12, "T041": 58385486034.9, "T024": 832142752.28}],
        nhytype=0,
    )
    financial = _f10_response(
        ["rq", "T033", "T044", "T055", "T058", "T041"],
        [{"rq": "2026-06-30", "T033": 215012000000.0, "T044": -70495000000.0,
          "T055": -196012000000.0, "T058": -53384000000.0, "T041": 666000000.0}],
        nhytype=1,
    )
    p = _provider(_F10Client({
        ("sh600519", "xjllb"): general,
        ("sz000001", "xjllb"): financial,
    }))
    df = p.get_financials("cash_flow", ["600519.SH", "000001.SZ"], latest_only=True)
    by_sym = {r["symbol"]: r for r in df.to_dicts()}

    # 通用族: T041 = 现金净增加
    assert by_sym["600519.SH"]["net_cash_change"] == pytest.approx(58385486034.9)
    assert by_sym["600519.SH"]["capex"] == pytest.approx(832142752.28)
    # 金融族: T041 = capex(不是现金净增加), 现金净增加走 T058
    assert by_sym["000001.SZ"]["capex"] == pytest.approx(666000000.0)
    assert by_sym["000001.SZ"]["net_cash_change"] == pytest.approx(-53384000000.0)
    assert by_sym["000001.SZ"]["net_cash_change"] != pytest.approx(666000000.0)


def test_f10_cash_flow_keeps_signed_net_values() -> None:
    """筹资/投资净额必须保留上游符号, 不做 abs 或取反。

    ``T037`` 是筹资活动现金**流出**的正数原值, ``T038`` 才是带符号净额;
    映射 T038 即得负数, 无需手工取反(实测茅台 -37,944,297,802.12)。
    """
    resp = _f10_response(
        ["rq", "T017", "T029", "T038", "T041", "T024", "T037"],
        [{"rq": "2026-06-30", "T017": 70690750119.06, "T029": 25640543520.6,
          "T038": -37944297802.12, "T041": 58385486034.9, "T024": 832142752.28,
          "T037": 37944297802.12}],  # 同额正数: 证明选的是 T038 而非 T037
        nhytype=0,
    )
    p = _provider(_F10Client({("sh600519", "xjllb"): resp}))
    row = p.get_financials("cash_flow", ["600519.SH"], latest_only=True).to_dicts()[0]

    assert row["net_financing_cash_flow"] < 0
    assert row["net_financing_cash_flow"] == pytest.approx(-37944297802.12)


def test_f10_income_returns_empty_frame() -> None:
    """利润表: 上游 lrb 无数值 → 恒返回空帧(交多源合并保留 fuyao 值)。"""
    p = _provider(_F10Client({}))
    assert p.get_financials("income", ["600519.SH"]).is_empty()


def test_f10_lrb_nameless_row_is_discarded() -> None:
    """即使上游对 lrb 回了 3 列名称行, 也必须被丢弃(无 rq/无数值)。"""
    from app.plugins.eltdx.provider import _f10_statement_rows

    resp = _f10_response(
        ["rtype", "nhytype", "zqname"],
        [{"rtype": "lrb", "nhytype": 0, "zqname": "贵州茅台"}],
        nhytype=None,
    )
    assert _f10_statement_rows(resp, "balance_sheet", "600519.SH") == []


def test_f10_single_symbol_failure_is_isolated() -> None:
    """单标的请求失败只隔离该只, 不拖垮整表(备份源补齐正依赖此语义)。"""
    resp = _f10_response(
        ["rq", "T039", "T062", "T071"],
        [{"rq": "2026-06-30", "T039": 100.0, "T062": 40.0, "T071": 60.0}],
        nhytype=0,
    )
    p = _provider(_F10Client(
        {("sh600519", "zcfzb"): resp},
        raise_for={("sz000001", "zcfzb")},
    ))
    df = p.get_financials("balance_sheet", ["600519.SH", "000001.SZ"], latest_only=True)

    assert df.height == 1
    assert df.to_dicts()[0]["symbol"] == "600519.SH"


def test_f10_latest_only_selects_newest_period() -> None:
    """latest_only=True 时每只标的只保留最新报告期。"""
    resp = _f10_response(
        ["rq", "T039", "T062", "T071"],
        [
            {"rq": "2025-12-31", "T039": 1.0, "T062": 1.0, "T071": 1.0},
            {"rq": "2026-06-30", "T039": 2.0, "T062": 2.0, "T071": 2.0},
        ],
        nhytype=0,
    )
    p = _provider(_F10Client({("sh600519", "zcfzb"): resp}))

    latest = p.get_financials("balance_sheet", ["600519.SH"], latest_only=True)
    assert latest.to_dicts()[0]["period_end"] == "2026-06-30"

    full = p.get_financials("balance_sheet", ["600519.SH"], latest_only=False)
    assert full.height == 2
    assert sorted(full["period_end"].to_list()) == ["2025-12-31", "2026-06-30"]


def test_f10_statement_uses_eltdx_code_and_correct_report_type() -> None:
    """请求前必须转 eltdx 代码, 且表名 → report_type 映射正确。"""
    resp = _f10_response(
        ["rq", "T039", "T062", "T071"],
        [{"rq": "2026-06-30", "T039": 1.0, "T062": 1.0, "T071": 1.0}],
        nhytype=0,
    )
    fake = _F10Client({("sh600519", "zcfzb"): resp})
    p = _provider(fake)
    p.get_financials("balance_sheet", ["600519.SH"], latest_only=True)

    assert ("finance_report", "sh600519", "zcfzb") in fake.calls


def test_f10_metrics_still_not_implemented() -> None:
    """metrics 仍未接入 → 空帧(其来源是指标接口, 与三大报表不同路径)。"""
    p = _provider(_F10Client({}))
    assert p.get_financials("metrics", ["600519.SH"]).is_empty()


# ---------------------------------------------------------------------------
# 除权因子(adj_factor): [symbol, trade_date, ex_factor] 单事件比值
# ---------------------------------------------------------------------------


def _adj_event(
    d: date,
    *,
    scale: float = 1.0,
    offset: float = 0.0,
    qfq_scale: float = 1.0,
    qfq_offset: float = 0.0,
) -> SimpleNamespace:
    """AdjustmentFactor 替身: 只有 ``(date, qfq_*, hfq_*)`` 被 provider 消费。

    ``qfq_*`` 默认与 ``hfq_*`` 同值: 顺带证明推导**不读** qfq(改了 qfq 结果不变)。
    """
    return SimpleNamespace(
        date=d,
        hfq_scale=scale,
        hfq_offset=offset,
        qfq_scale=qfq_scale,
        qfq_offset=qfq_offset,
    )


def _adj_client(
    events: dict[str, list[SimpleNamespace]] | None = None,
    *,
    bars: dict[str, list[SimpleNamespace]] | None = None,
    raise_for: set[str] | None = None,
) -> _FakeClient:
    """除权因子假 client: 事件表 + 日 K 表 + 指定标的抛异常(单标的隔离用)。

    事件表按**面板 symbol** 给出(``000001.SZ``), 这里转成 eltdx 代码(``sz000001``)存,
    因为 provider 调的是 ``adjustment_factors(to_eltdx_code(sym))`` —— 顺带把"请求前
    必须转代码"这条钉进假 client。日 K 表仍按面板 symbol 存(provider 传原 symbol)。

    不改动 ``_FakeClient.__init__`` 的签名(既有用例全部按关键字调用, 且子类各自
    覆写 ``__init__`` 时不会串味); 除权因子需要的两个状态在这里就地挂上。
    """
    fake = _FakeClient(bars=bars or {})
    fake._adj_events = {to_eltdx_code(s): v for s, v in (events or {}).items()}
    fake._adj_raise = {to_eltdx_code(s) for s in (raise_for or set())}
    fake._adj_panel = dict(events or {})
    return fake


def _adj_events(fake: _FakeClient, sym: str) -> list[SimpleNamespace]:
    """按面板 symbol 取假事件表(测试内的便捷读取, 不触发 client 调用)。"""
    return list(fake._adj_panel.get(sym, []))


# 标的: 面板格式(000001.SZ -> eltdx 代码 sz000001)
_SYM = "000001.SZ"
_SYM2 = "600000.SH"

# 事件日与其前一交易日的固定日期(交易日历无关, 只需严格先后)
_ADJ_PREV_DAY = date(2026, 6, 9)
_ADJ_EVENT_DAY = date(2026, 6, 10)
_ADJ_AFTER_DAY = date(2026, 6, 11)
_ADJ_START = date(2026, 1, 1)
_ADJ_END = date(2026, 12, 31)


def _adj_bars(sym: str, prev_close: float | None = 10.0, event_close: float | None = None):
    """事件日前一交易日(以及可选的事件日)的日 K 替身。"""
    if prev_close is None:
        bars = []
    else:
        bars = [_bar(datetime(2026, 6, 9, 15, 0, tzinfo=_CN), c=prev_close)]
    if event_close is not None:
        bars.append(_bar(datetime(2026, 6, 10, 15, 0, tzinfo=_CN), c=event_close))
    return {sym: bars}


def test_adj_factors_column_contract_and_order() -> None:
    """列必须**恰好**是 [symbol, trade_date, ex_factor](单事件比值, 非累积)。"""
    events = {_SYM: [_adj_event(_ADJ_PREV_DAY, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)]}
    fake = _adj_client(events, bars=_adj_bars(_SYM))
    df = _provider(fake).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert df.columns == ["symbol", "trade_date", "ex_factor"]
    assert df.height >= 1, "有事件的用例必须真产出数据(否则列断言会空转)"
    assert fake.calls.count(("adj", "sz000001")) == 1, "面板 symbol 必须转成 eltdx 代码再请求"


def test_adj_factors_symbol_normalization_roundtrip() -> None:
    """symbol 归一: 面板格式 ``000001.SZ`` 进就 ``000001.SZ`` 出(不是 sz000001)。"""
    events = {_SYM: [_adj_event(_ADJ_PREV_DAY), _adj_event(_ADJ_EVENT_DAY, offset=0.1)]}
    df = _provider(_adj_client(events, bars=_adj_bars(_SYM))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )

    assert df["symbol"].to_list() == [_SYM]


def test_adj_factors_accepts_gateway_string_dates() -> None:
    """事件 ``date`` 为 ISO 字符串时(HTTP 网关形态)必须仍能推导, 不得整批失败。

    回归防护(实测缺陷): 网关把 ``AdjustmentFactor.date`` 序列化成 ``'2002-07-25'``
    字符串, 而实现直接写 ``start_d <= cur.date <= end_d`` 与 date 端点比较, 抛
    ``TypeError: '<=' not supported between instances of 'datetime.date' and 'str'``。
    该异常被单标的软失败吞掉, 表现为**全市场 5584 只逐条 warning、sync_adj 报告
    "no new factors"**, 除权因子静默停更而管道仍报成功 —— 最难发现的一类失败。
    """
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY.isoformat(), offset=0.0),
            _adj_event(_ADJ_EVENT_DAY.isoformat(), offset=0.5),
        ]
    }
    fake = _adj_client(events, bars=_adj_bars(_SYM, prev_close=10.0))
    df = _provider(fake).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert df.height == 1, "字符串事件日必须与 date 端点同样被接受"
    assert df["trade_date"].to_list() == [_ADJ_EVENT_DAY]
    # 与 date 形态产出**同值**, 证明只是类型收口而非公式变化
    events_date = {
        _SYM: [_adj_event(_ADJ_PREV_DAY, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)]
    }
    df_date = _provider(_adj_client(events_date, bars=_adj_bars(_SYM, prev_close=10.0))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )
    assert df["ex_factor"].to_list() == df_date["ex_factor"].to_list()


def test_adj_factors_string_dates_still_filtered_by_range() -> None:
    """字符串事件日同样要受区间过滤, 不能因类型收口而全部放行。"""
    events = {
        _SYM: [
            _adj_event(date(2025, 12, 1).isoformat(), offset=0.0),  # 区间外(首个事件)
            _adj_event(_ADJ_EVENT_DAY.isoformat(), offset=0.2),     # 区间内
            _adj_event(date(2027, 3, 1).isoformat(), offset=0.4),   # 区间外
        ]
    }
    df = _provider(_adj_client(events, bars=_adj_bars(_SYM))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )

    assert df["trade_date"].to_list() == [_ADJ_EVENT_DAY]


def test_adj_factors_sorted_by_symbol_and_trade_date() -> None:
    """输出按 (symbol, trade_date) 升序(下游 join_asof 依赖有序)。"""
    later = date(2026, 6, 20)
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, offset=0.0),
            _adj_event(later, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, offset=0.0),
        ],
        _SYM2: [
            _adj_event(_ADJ_PREV_DAY, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, offset=0.0),
        ],
    }
    bars = {**_adj_bars(_SYM), **_adj_bars(_SYM2)}
    df = _provider(_adj_client(events, bars=bars)).get_adj_factors(
        [_SYM2, _SYM], _ADJ_START, _ADJ_END
    )

    got = [(r["symbol"], r["trade_date"]) for r in df.to_dicts()]
    assert got == [
        (_SYM, _ADJ_EVENT_DAY),
        (_SYM, later),
        (_SYM2, _ADJ_EVENT_DAY),
    ]


# ---------------------------------------------------------------------------
# 除权因子: 区间过滤 / 首事件跳过
# ---------------------------------------------------------------------------


def test_adj_factors_filters_events_outside_range() -> None:
    """区间外的除权事件必须过滤(源会一次返回全历史事件)。"""
    events = {
        _SYM: [
            _adj_event(date(2025, 12, 1), offset=0.0),  # 区间外(且是首个事件)
            _adj_event(_ADJ_EVENT_DAY, offset=0.2),  # 区间内
            _adj_event(date(2027, 3, 1), offset=0.4),  # 区间外
        ]
    }
    df = _provider(_adj_client(events, bars=_adj_bars(_SYM))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )

    assert df["trade_date"].to_list() == [_ADJ_EVENT_DAY]


def test_adj_factors_range_bounds_are_inclusive() -> None:
    """区间端点包含边界: 事件日 == start 或 == end 都算在区间内。"""
    events = {_SYM: [_adj_event(_ADJ_PREV_DAY), _adj_event(_ADJ_EVENT_DAY, offset=0.2)]}
    p = _provider(_adj_client(events, bars=_adj_bars(_SYM, prev_close=10.0)))

    assert p.get_adj_factors([_SYM], _ADJ_EVENT_DAY, _ADJ_EVENT_DAY).height == 1
    assert p.get_adj_factors([_SYM], _ADJ_EVENT_DAY, _ADJ_AFTER_DAY).height == 1
    assert p.get_adj_factors([_SYM], _ADJ_START, _ADJ_PREV_DAY).height == 0


def test_adj_factors_skips_first_event_so_n_minus_one_rows() -> None:
    """N 个事件只产出 N-1 行: 首个事件无前序参照(累积链从第 2 个起有定义)。"""
    events = {
        _SYM: [
            _adj_event(date(2026, 3, 2), offset=0.0),
            _adj_event(date(2026, 4, 2), offset=0.1),
            _adj_event(date(2026, 5, 4), offset=0.2),
            _adj_event(date(2026, 6, 1), offset=0.3),
        ]
    }
    bars = {
        _SYM: [
            _bar(datetime(2026, 3, 1, 15, 0, tzinfo=_CN), c=10.0),
            _bar(datetime(2026, 4, 1, 15, 0, tzinfo=_CN), c=10.0),
            _bar(datetime(2026, 5, 1, 15, 0, tzinfo=_CN), c=10.0),
        ]
    }
    df = _provider(_adj_client(events, bars=bars)).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert len(events[_SYM]) == 4, "N == 4 个事件"
    assert df.height == 3, "首个事件必须被跳过(无前序参照), 故只有 N-1 行"
    assert df["trade_date"].to_list() == [date(2026, 4, 2), date(2026, 5, 4), date(2026, 6, 1)]
    assert date(2026, 3, 2) not in df["trade_date"].to_list()


# ---------------------------------------------------------------------------
# 除权因子: 公式(纯分红 / 送转 / 复合)
# ---------------------------------------------------------------------------


def test_adj_factors_pure_cash_dividend_formula() -> None:
    """纯现金分红(scale 不变, offset 增大): ex_factor = prev_close / (prev_close - div)。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=0.5),
        ]
    }
    df = _provider(_adj_client(events, bars=_adj_bars(_SYM, prev_close=10.0))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )

    assert df.height == 1
    row = df.row(0, named=True)
    assert row["trade_date"] == _ADJ_EVENT_DAY
    # div = (0.5 - 0.0) / 1.0 -> ex = (1.0 / 1.0) x 10.0 / (10.0 - 0.5)
    assert row["ex_factor"] == pytest.approx(10.0 / 9.5)
    assert row["ex_factor"] == pytest.approx(1.0526315789473684)


def test_adj_factors_split_formula_scale_changed() -> None:
    """送转(scale 变化, offset 不变): div=0 -> ex_factor = cur_scale / prev_scale。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=2.0, offset=0.0),
        ]
    }
    row = (
        _provider(_adj_client(events, bars=_adj_bars(_SYM, prev_close=10.0)))
        .get_adj_factors([_SYM], _ADJ_START, _ADJ_END)
        .row(0, named=True)
    )

    assert row["ex_factor"] == pytest.approx(2.0), "10 送 10 的除权比值应为 2.0"


def test_adj_factors_composite_scale_and_offset() -> None:
    """复合(scale 与 offset 同时变): 两类效应按公式相乘复合, 不是简单相加。"""
    prev_scale, cur_scale = 2.0, 3.0
    prev_offset, cur_offset = 0.4, 1.0
    prev_close = 20.0
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=prev_scale, offset=prev_offset),
            _adj_event(_ADJ_EVENT_DAY, scale=cur_scale, offset=cur_offset),
        ]
    }
    row = (
        _provider(_adj_client(events, bars=_adj_bars(_SYM, prev_close=prev_close)))
        .get_adj_factors([_SYM], _ADJ_START, _ADJ_END)
        .row(0, named=True)
    )

    div = (cur_offset - prev_offset) / cur_scale  # = 0.2
    want = (cur_scale / prev_scale) * prev_close / (prev_close - div)
    assert div == pytest.approx(0.2)
    assert row["ex_factor"] == pytest.approx(want)
    assert row["ex_factor"] == pytest.approx(1.5 * 20.0 / 19.8)
    # 单纯相加会把分红项算错(少乘 scale 比), 这里钉死不是相加
    assert row["ex_factor"] != pytest.approx((cur_scale / prev_scale) + div)


def test_adj_factors_uses_hfq_not_qfq() -> None:
    """契约红线: 只读 hfq_*, qfq_* 改了也不影响结果(实测 qfq_offset 是前复权偏移)。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0, qfq_scale=7.7, qfq_offset=99.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=0.5, qfq_scale=3.3, qfq_offset=-42.0),
        ]
    }
    row = (
        _provider(_adj_client(events, bars=_adj_bars(_SYM, prev_close=10.0)))
        .get_adj_factors([_SYM], _ADJ_START, _ADJ_END)
        .row(0, named=True)
    )

    assert row["ex_factor"] == pytest.approx(10.0 / 9.5)


# ---------------------------------------------------------------------------
# 除权因子: 基准价(prev_close)相关红线
# ---------------------------------------------------------------------------


def test_adj_factors_basis_close_is_strictly_before_event_date() -> None:
    """回归防护: 基准价必须取事件日**严格之前**的收盘, 事件日当天收盘(除权后)不可用。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=0.5),
        ]
    }
    bars = _adj_bars(_SYM, prev_close=10.0, event_close=5.5)  # 事件日不复权价大跌(除权后)
    row = (
        _provider(_adj_client(events, bars=bars))
        .get_adj_factors([_SYM], _ADJ_START, _ADJ_END)
        .row(0, named=True)
    )

    assert row["ex_factor"] == pytest.approx(10.0 / 9.5), "应基于 6-09 的 10.0"
    assert row["ex_factor"] != pytest.approx(5.5 / 5.0), "绝不能拿 6-10 的 5.5 当基准"


def test_adj_factors_missing_basis_price_skips_row() -> None:
    """无事件日之前的 K 线(基准价缺失) -> 该事件不产出行(不伪造基准价)。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=0.5),
        ]
    }
    fake = _adj_client(events, bars={})  # 一根 K 线都没有
    df = _provider(fake).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert df.height == 0, "无基准价不得伪造出一行"
    assert not df.columns


def test_adj_factors_basis_bar_on_event_date_only_is_not_used() -> None:
    """只有事件日当天的 K 线(无更早的) -> 基准价缺失, 该事件同样跳过。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=0.5),
        ]
    }
    bars = {_SYM: [_bar(datetime(2026, 6, 10, 15, 0, tzinfo=_CN), c=10.0)]}
    df = _provider(_adj_client(events, bars=bars)).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert df.height == 0, "事件日当天的 K 线不能充当前一交易日基准价"


def test_adj_factors_basis_bar_with_non_positive_close_ignored() -> None:
    """基准 K 线收盘价 <= 0 视为不可用 -> 回退更早的有效收盘, 实在没有就跳过。"""
    earlier = date(2026, 6, 5)
    mid = date(2026, 6, 9)
    events = {_SYM: [_adj_event(mid, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)]}
    bars = {
        _SYM: [
            _bar(datetime(2026, 6, 5, 15, 0, tzinfo=_CN), c=8.0),
            _bar(datetime(2026, 6, 9, 15, 0, tzinfo=_CN), c=0.0),
        ]
    }
    row = (
        _provider(_adj_client(events, bars=bars))
        .get_adj_factors([_SYM], _ADJ_START, _ADJ_END)
        .row(0, named=True)
    )

    assert earlier < mid  # 回退到 6-05 的 8.0
    assert row["ex_factor"] == pytest.approx(8.0 / 7.5)


def test_adj_factors_skips_row_when_denominator_non_positive() -> None:
    """denom = prev_close - div <= 0(数据异常) -> 跳过该行, 不产出负/发散的比值。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=1.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=50.0),  # div = 50 > prev_close = 10
        ]
    }
    df = _provider(_adj_client(events, bars=_adj_bars(_SYM, prev_close=10.0))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )

    assert df.height == 0, "denom <= 0 必须跳过(否则会得到负因子)"


def test_adj_factors_skips_row_when_prev_scale_invalid() -> None:
    """前序事件 scale 为 0/None -> 比值不可求, 跳过该行(不抛异常)。"""
    events = {
        _SYM: [
            _adj_event(_ADJ_PREV_DAY, scale=0.0, offset=0.0),
            _adj_event(_ADJ_EVENT_DAY, scale=1.0, offset=0.0),
        ]
    }
    df = _provider(_adj_client(events, bars=_adj_bars(_SYM))).get_adj_factors(
        [_SYM], _ADJ_START, _ADJ_END
    )

    assert df.height == 0


# ---------------------------------------------------------------------------
# 除权因子: 事件数不足 / 回调 / 失败隔离 / 能力声明
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("n_events", [0, 1])
def test_adj_factors_fewer_than_two_events_returns_empty(n_events) -> None:
    """0 或 1 个事件 -> 空帧(height 0), 且不发起日 K 请求(无需基准价)。"""
    events = {_SYM: [_adj_event(_ADJ_EVENT_DAY, offset=0.5)][:n_events]}
    fake = _adj_client(events, bars=_adj_bars(_SYM))
    df = _provider(fake).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert not [c for c in fake.calls if c[0] == "bars"], "事件不足 2 个不应取日 K"


def test_adj_factors_empty_symbols_returns_empty() -> None:
    """空标的列表 -> 空帧, 不触发任何 client 调用。"""
    fake = _adj_client({})
    df = _provider(fake).get_adj_factors([], _ADJ_START, _ADJ_END)

    assert df.height == 0
    assert fake.calls == []


def test_adj_factors_on_chunk_done_covers_all_symbols() -> None:
    """on_chunk_done 覆盖每个标的(含无数据的), 末次为 (total, total)。"""
    events = {
        _SYM: [_adj_event(_ADJ_PREV_DAY), _adj_event(_ADJ_EVENT_DAY, offset=0.5)],
        _SYM2: [],  # 无事件也必须被回调覆盖
    }
    bars = {**_adj_bars(_SYM), **_adj_bars(_SYM2)}
    seen: list[tuple[int, int]] = []
    _provider(_adj_client(events, bars=bars)).get_adj_factors(
        [_SYM, _SYM2], _ADJ_START, _ADJ_END, on_chunk_done=lambda c, t: seen.append((c, t))
    )

    assert len(seen) == 2, "每个标的回调一次"
    assert seen[-1] == (2, 2)
    assert {c for c, _ in seen} == {1, 2}, "cur 必须覆盖 1..total"


def test_adj_factors_per_symbol_failure_isolated() -> None:
    """单标的异常隔离: 一只抛异常不影响其他标的, 异常不上抛。"""
    events = {
        _SYM: [_adj_event(_ADJ_PREV_DAY, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)],
        _SYM2: [_adj_event(_ADJ_PREV_DAY, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)],
    }
    bars = {**_adj_bars(_SYM), **_adj_bars(_SYM2)}
    df = _provider(_adj_client(events, bars=bars, raise_for={_SYM2})).get_adj_factors(
        [_SYM, _SYM2], _ADJ_START, _ADJ_END
    )

    assert df["symbol"].to_list() == [_SYM], "失败标的无行, 成功标的照常产出"


def test_adj_factors_invalid_symbol_contributes_no_rows() -> None:
    """非法 symbol(无法转 eltdx 代码) -> 不发起请求、不产出行、不崩溃。"""
    events = {_SYM: [_adj_event(_ADJ_PREV_DAY, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)]}
    fake = _adj_client(events, bars=_adj_bars(_SYM))
    df = _provider(fake).get_adj_factors(["not-a-symbol", _SYM], _ADJ_START, _ADJ_END)

    assert df["symbol"].to_list() == [_SYM]
    assert _adj_events(fake, "not-a-symbol") == [], "非法代码不会命中任何事件表"
    assert [c for c in fake.calls if c[0] == "adj"] == [] or all(
        c[1] != "not-a-symbol" for c in fake.calls if c[0] == "adj"
    ), "非法代码不得转成 eltdx 代码去请求"


def test_adj_factors_declared_in_datasets() -> None:
    """adj_factor 必须在 config.datasets 中(否则 provider_has_dataset 判 False 而回退)。"""
    assert "adj_factor" in EltDxProvider().config.datasets


def test_adj_factors_empty_frame_has_no_adj_columns() -> None:
    """无行时返回裸空帧(与文件内 daily/minute 空帧口径一致), 不伪造列。"""
    df = _provider(_adj_client({})).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert df.columns == []


class _FakeClientBarCountLimit(_FakeClient):
    """日 K 假 client: 断言 count 上界被如实透传。"""

    def bars(self, symbol, *, period="day", count):
        self.calls.append(("bars", symbol, period, count))
        return list(self._bars.get(symbol, []))[:count]


def test_adj_factors_requests_daily_bars_for_basis_price() -> None:
    """基准价来自 client.bars(symbol, period='day', count=回看窗口), 且 count 随区间放大。

    回归防护: 回看根数必须覆盖整个请求区间, 否则老事件找不到基准价会被整批跳过
    (实测固定取 120 根时, 11 年跨度只剩 6 行)。
    """
    from app.plugins.eltdx.provider import _ADJ_LOOKBACK_BARS

    events = {_SYM: [_adj_event(_ADJ_PREV_DAY, offset=0.0), _adj_event(_ADJ_EVENT_DAY, offset=0.5)]}
    fake = _FakeClientBarCountLimit(bars=_adj_bars(_SYM))
    fake._adj_events = {to_eltdx_code(_SYM): events[_SYM]}
    fake._adj_raise = set()
    _provider(fake).get_adj_factors([_SYM], _ADJ_START, _ADJ_END)

    bar_calls = [c for c in fake.calls if c[0] == "bars"]
    assert len(bar_calls) == 1
    _, sym, period, count = bar_calls[0]
    assert (sym, period) == (_SYM, "day")
    # 窄区间: 仍不小于基础回看窗口; 且不得超过 eltdx 的 8000 根上限
    assert count >= _ADJ_LOOKBACK_BARS
    assert count <= 8000
    # 宽区间必须放大回看根数(否则老事件无基准价)
    fake2 = _FakeClientBarCountLimit(bars=_adj_bars(_SYM))
    fake2._adj_events = {to_eltdx_code(_SYM): events[_SYM]}
    fake2._adj_raise = set()
    _provider(fake2).get_adj_factors([_SYM], date(2015, 1, 1), date(2026, 9, 30))
    wide = next(c for c in fake2.calls if c[0] == "bars")[3]
    assert wide > count, "宽区间的回看根数必须更大"


# ---------------------------------------------------------------------------
# 财务 shares: 批大小上限 + 重试 + 二分拆分(实测服务内 40% 批会失败)
# ---------------------------------------------------------------------------


class _FlakyFinClient(_FinClient):
    """按"毒批"集合模拟上游: 命中毒批就抛 ``invalid ASCII response code``。

    实测现象: eltdx 的 finance_batch 对单次请求的**代码组合**敏感 —— 即使批=20 也有
    ~40% 的批失败, 且**可复现**(同批代码重复跑结果一致)。这里的 fake 复刻该行为,
    用于验证 provider 的重试 + 二分拆分能把它兜住。
    """

    def __init__(self, poison_prefixes=(), records=()):
        super().__init__(records)
        self._poison = {tuple(p) for p in poison_prefixes}
        self.attempts: list[tuple] = []

    def finance_batch(self, codes):
        self.calls.append(("finance_batch", tuple(codes)))
        self.attempts.append(tuple(codes))
        # 命中毒组合(要求该组合被完整包含) -> 抛上游那个真实错误
        if any(t and t[0] == codes[0] and set(t) <= set(codes) for t in self._poison):
            raise RuntimeError("invalid ASCII response code")
        # 正常返回: 按 eltdx 代码前缀还原 (code, exchange)
        out = []
        for c in codes:
            ex, num = c[:2], c[2:]
            out.append(_fin(num, ex))
        return _FinPage(out)


def test_finance_batch_size_is_conservative() -> None:
    """回归防护: 财务批大小必须 <=20。

    实测(真实面板代码序): n=20 成功、n=25 起全部报 ``invalid ASCII response code``;
    eltdx 官方默认 batch_size=75 在实际代码序下不可用 —— 会成批失败,
    只落 ~1210/5578 只(覆盖率 21.7%)。降到 20 后配合重试/拆分可达 94.7%。
    """
    from app.plugins.eltdx.provider import _FINANCE_BATCH

    assert _FINANCE_BATCH <= 20, "财务批大小超过 20 会成批失败"


def test_finance_retry_recovers_transient_failure() -> None:
    """首次失败 -> 重试成功: 该批数据必须被取回(不能因一次抖动丢弃)。"""

    class _OnceFail(_FinClient):
        def __init__(self, records=()):
            super().__init__(records)
            self._n = 0

        def finance_batch(self, codes):
            self._n += 1
            if self._n == 1:
                raise RuntimeError("invalid ASCII response code")
            return super().finance_batch(codes)

    fake = _OnceFail(records=[_fin("600519", "sh")])
    df = _provider(fake).get_financials("shares", ["600519.SH"])

    assert df.height == 1, "重试后应拿到数据"
    assert df["symbol"].to_list() == ["600519.SH"]


def test_finance_split_recovers_poison_batch() -> None:
    """毒批(固定失败) -> **二分拆分**后必须能取到非毒部分的数据。

    这是实测里最关键的兜底: 有 104 批是靠拆分救回来的。
    """
    # 组合(sh600519,sh600000)是"毒", 但拆开后各自可用
    poison = [("sh600519", "sh600000")]
    fake = _FlakyFinClient(poison_prefixes=poison)
    df = _provider(fake).get_financials("shares", ["600519.SH", "600000.SH"])

    assert set(df["symbol"].to_list()) == {"600519.SH", "600000.SH"}, "毒批拆分后应取回全部可用标的"
    # 必须真的发生过拆分: 存在单只请求
    assert [a for a in fake.attempts if len(a) == 1], "应当退化为单只请求(拆分兜底)"


def test_finance_split_recursion_terminates_on_single_poison_symbol() -> None:
    """单只也失败(真毒) -> 不递归死循环, 只丢该只, 其余照常返回。"""

    class _SinglePoison(_FinClient):
        def finance_batch(self, codes):
            self.calls.append(("finance_batch", tuple(codes)))
            if list(codes) == ["sz000001"]:
                raise RuntimeError("invalid ASCII response code")
            return _FinPage([_fin("600519", "sh")])

    fake = _SinglePoison()
    df = _provider(fake).get_financials("shares", ["600519.SH", "000001.SZ"])

    assert "600519.SH" in df["symbol"].to_list()
    # 单只也失败 -> 该只被丢弃, 且**不得**无限递归(调用次数有限)
    assert len(fake.calls) < 50, "单只毒代码不得导致无限拆分"


# ---------------------------------------------------------------------------
# 运行时崩溃自愈(2026-09-30 盘中事故的回归防护)
# ---------------------------------------------------------------------------

_DEAD_MSG = "7709 runtime command channel is closed"


def test_runtime_dead_detected_by_message() -> None:
    """崩溃识别: 只有 "runtime command channel is closed" 才算运行时崩溃。

    2026-09-30 盘中实测: 上游一次 `response timed out during connect` 之后,
    eltaX 内部运行时进入 closed 状态, 此后**所有**接口(代码表/快照/K线/盘口)
    一律报此错且**永不自愈**, 面板表现为全线静默为空 + 无限空转。
    普通业务错误(如 invalid code)不得误判为崩溃而重置连接池。
    """
    from app.plugins.eltdx.client import EltDxClient

    assert EltDxClient._is_runtime_dead(RuntimeError(_DEAD_MSG)) is True
    assert EltDxClient._is_runtime_dead(RuntimeError("invalid code: sh999999")) is False
    assert EltDxClient._is_runtime_dead(RuntimeError("station error")) is False


def test_call_rebuilds_pool_and_retries_once_on_runtime_death() -> None:
    """崩溃后必须**重建连接池并重试一次** —— 否则旧池永久不可用, 服务无限空转。

    这是本次事故的核心修复: 换新池后能立刻恢复取数。
    """
    from app.plugins.eltdx.client import EltDxClient

    c = EltDxClient()
    calls: list[int] = []

    class _Boom:
        """首次调用抛"运行时崩溃", 第二次成功(模拟重建后的新池)。"""

        def __call__(self):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError(_DEAD_MSG)
            return "ok"

    assert c._call(_Boom()) == "ok"
    assert len(calls) == 2, "必须在重建后重试一次"
    assert c._client is None, "崩溃后旧池必须被作废(下次调用重建)"


def test_call_retries_only_once_under_sustained_failure() -> None:
    """上游持续崩溃时**只重试一次**, 不得打成重试风暴。"""
    from app.plugins.eltdx.client import EltDxClient

    c = EltDxClient()
    calls: list[int] = []

    def _always_dead():
        calls.append(1)
        raise RuntimeError(_DEAD_MSG)

    with pytest.raises(RuntimeError):
        c._call(_always_dead)
    assert len(calls) == 2, f"应只尝试 2 次(原始+重试), 实际 {len(calls)}"


def test_call_does_not_retry_business_errors() -> None:
    """普通业务错误不触发重建/重试(否则会对限流类错误雪上加霜)。"""
    from app.plugins.eltdx.client import EltDxClient

    c = EltDxClient()
    sentinel = object()
    c._client = sentinel
    calls: list[int] = []

    def _biz_error():
        calls.append(1)
        raise RuntimeError("invalid code: sh999999")

    with pytest.raises(RuntimeError):
        c._call(_biz_error)
    assert len(calls) == 1, "业务错误不得重试"
    assert c._client is sentinel, "业务错误不得作废连接池"


# ---------------------------------------------------------------------------
# HTTP 网关传输(进程隔离; 2026-09-30 事故后的架构收敛)
# ---------------------------------------------------------------------------


def _http_transport(handler):
    """构造一个把 /rpc 交给 handler 的 HttpTransport(不真连网络)。

    handler 返回**原始 JSON 结构**; 这里按真实 ``_rpc`` 的行为做 ``_wrap``,
    以便测试覆盖"JSON dict -> 属性对象"这层转换。
    """
    from app.plugins.eltdx.http_client import HttpTransport, _wrap

    t = HttpTransport(base_url="http://test.local", timeout=5)
    calls: list[tuple[str, dict]] = []

    def _rpc(method, params):
        calls.append((method, params))
        return _wrap(handler(method, params))

    t._rpc = _rpc  # type: ignore[method-assign]
    return t, calls


def test_http_transport_wraps_json_into_attribute_objects() -> None:
    """JSON dict 必须包装成**属性可访问**的对象, 以复用 provider 的解析逻辑。

    网关把 dataclass 序列化成同名字段的 JSON(实测 volume_lots / amount /
    buy_levels 等与进程内对象一致), provider 侧却是按属性访问的 —— 这层包装
    是"两套传输共用同一份 provider 代码"的前提。
    """
    from app.plugins.eltdx.http_client import _wrap

    o = _wrap({"volume_lots": 707.0, "nested": {"price": 11.48}, "items": [{"volume": 1}]})
    assert o.volume_lots == 707.0
    assert o.nested.price == 11.48
    assert o.items[0].volume == 1
    assert o.not_there is None, "缺失字段必须返回 None(对应契约的缺失语义)"


def test_http_transport_bars_uses_paging_and_stops_on_short_page() -> None:
    """单标的 bars 必须自管分页(单页上限 800), 且短页即终止。"""
    pages = {
        0: [{"time": f"2026-09-30T10:{i:02d}:00+08:00", "close": 1.0} for i in range(800)],
        800: [{"time": "2026-09-30T11:00:00+08:00", "close": 2.0}],  # 短页 -> 终止
    }

    def h(method, params):
        assert method == "bars.get"
        assert params["adjust"] is None, "面板契约: K 线必须不复权"
        return {"bars": pages.get(params["start"], [])}

    t, calls = _http_transport(h)
    out = t.bars("000001.SZ", period="day", count=2000)
    assert len(out) == 801, "800 满页 + 1 短页"
    assert [c[1]["start"] for c in calls] == [0, 800], "短页后不得继续翻页"


def test_http_transport_bars_multi_maps_codes_back_to_panel_symbols() -> None:
    """批量返回必须按请求顺序还原成面板 symbol(网关返回的 key 是 eltdx 代码)。"""

    def h(method, params):
        assert method == "bars.get"
        return {
            "sz000001": {"bars": [{"close": 11.5}]},
            "sh600519": {"bars": [{"close": 1238.0}, {"close": 1239.0}]},
        }

    t, _ = _http_transport(h)
    got = t.bars_multi(["000001.SZ", "600519.SH"], period="1m", count=3)
    assert [s for s, _ in got] == ["000001.SZ", "600519.SH"]
    assert [len(b) for _, b in got] == [1, 2]


def test_http_transport_snapshots_soft_fails() -> None:
    """快照必须**软失败**(返回 [] 不抛), 否则会打断面板轮询线程。"""

    def h(method, params):
        raise RuntimeError("HTTP 502 on quotes.get_snapshots")

    t, _ = _http_transport(h)
    assert t.snapshots(["000001.SZ"], batch_size=80) == []


def test_http_transport_depth_hard_fails() -> None:
    """盘口契约相反: 必须**抛异常**(由服务层按批隔离, 不跨源回退)。"""

    def h(method, params):
        raise RuntimeError("HTTP 502 on quotes.get_depth")

    t, _ = _http_transport(h)
    with pytest.raises(RuntimeError):
        t.depth(["000001.SZ"])


def test_http_transport_all_a_shares_soft_fails() -> None:
    """代码表失败软返回 [](与进程内同语义)。"""

    def h(method, params):
        raise RuntimeError("gateway down")

    t, _ = _http_transport(h)
    assert t.all_a_shares() == []


# ---------------------------------------------------------------------------
# 代码表 TTL 缓存(单轮 5.6~7.9s 中约 4.3s 花在 codes.all_a_shares 上)
# ---------------------------------------------------------------------------


def _codes_transport(symbols: list[str] | None = None):
    """代码表专用替身: 记录 codes.all_a_shares 的调用次数。"""
    payload = symbols if symbols is not None else ["sz000001", "sh600000"]
    counter = {"n": 0}

    def h(method, params):
        assert method == "codes.all_a_shares"
        counter["n"] += 1
        return payload

    t, _ = _http_transport(h)
    return t, counter


def test_code_table_cached_across_calls() -> None:
    """本改动的核心断言: 连续调用只回源**一次**。

    回归防护(实测): 每次调用 all_a_shares 都真拉上游, 耗时 2.1~6.5s(中位 4.3s),
    而同一份清单从内存读取仅需 0.011ms。实时轮询每 6s 一拍、分钟增量每 ~12s 一拍,
    不清缓存等于每轮都把大部分时间花在一份几乎不变的全市场清单上。
    """
    t, counter = _codes_transport()
    first = t.all_a_shares()
    second = t.all_a_shares()
    third = t.all_a_shares()

    assert counter["n"] == 1, f"应只回源 1 次, 实际 {counter['n']} 次"
    assert first == second == third == ["000001.SZ", "600000.SH"]


def test_code_table_returns_copy_not_cache_itself() -> None:
    """返回的必须是副本: 调用方原地修改不得污染缓存。

    get_intraday_latest 会对 syms 做切片, get_realtime 会把它传给 snapshots;
    共享同一个 list 对象会让"某个调用方的就地改动"影响另一个线程的后续轮次。
    """
    t, counter = _codes_transport()
    first = t.all_a_shares()
    first.append("FAKE.SH")
    first.clear()  # 恶意清空

    again = t.all_a_shares()
    assert again == ["000001.SZ", "600000.SH"], "缓存被调用方污染"
    assert counter["n"] == 1, "污染检查不应触发额外回源"


def test_code_table_invalidated_on_new_day(monkeypatch) -> None:
    """跨北京日期必须立即失效(不依赖 TTL)。

    日期必须由 ``hc.cn_today`` **自身**推进 (而非硬编码某个具体日期): 硬编码时
    一旦运行日恰好等于该日期, "跨日"这一步就退化成同日, 断言 counter==2 恒失败
    —— 2026-10-01 实测即如此 (原写法 ``date(2026,10,1)`` 与当天 cn_today() 相同)。
    这里用「当天 + 1 天」构造跨日, 与运行日无关。
    """
    from app.plugins.eltdx import http_client as hc

    t, counter = _codes_transport()
    t.all_a_shares()
    assert counter["n"] == 1

    # 同一天内仍命中
    t.all_a_shares()
    assert counter["n"] == 1

    # 跨日 -> 重拉 (相对当天推进, 不写死具体日期)
    tomorrow = hc.cn_today() + timedelta(days=1)
    monkeypatch.setattr(hc, "cn_today", lambda: tomorrow)
    t.all_a_shares()
    assert counter["n"] == 2, "跨日未失效"


def test_code_table_invalidated_after_ttl(monkeypatch) -> None:
    """TTL 过期后必须重拉, 以限制盘中新上市/退市的可见滞后。

    用**实例**的 ``_code_ttl_s`` 计算偏移(而非模块常量): 实例 TTL 可配
    (``code_ttl_s`` / ``ELTDX_CODE_TTL``), 绑定模块常量会在改配后失配。
    """
    from app.plugins.eltdx import http_client as hc

    t, counter = _codes_transport()
    ttl = t._code_ttl_s
    assert ttl > 1, "默认 TTL 应显著大于 1s"
    t.all_a_shares()
    assert counter["n"] == 1

    # 推进到 TTL 之内: 仍命中
    # base 必须取自 **hc.time**(被测代码用的那个时钟函数): 用本模块的
    # time.monotonic() 只是恰好同源才成立, 一旦被测模块的 time 被替换
    # (如本地复现 CI 的 fresh-boot 插件)两边量级就对不上, 断言随即失效。
    base = hc.time.monotonic()
    monkeypatch.setattr(hc.time, "monotonic", lambda: base + ttl - 1)
    t.all_a_shares()
    assert counter["n"] == 1, "TTL 未到不应回源"

    # 超过 TTL: 重拉
    monkeypatch.setattr(hc.time, "monotonic", lambda: base + ttl + 1)
    t.all_a_shares()
    assert counter["n"] == 2, "TTL 过期未失效"


def test_code_table_failure_is_not_cached() -> None:
    """失败**不得写入缓存**: 否则一次上游抖动会让清单空到 TTL 结束。"""
    state = {"fail": True, "n": 0}

    def h(method, params):
        state["n"] += 1
        if state["fail"]:
            raise RuntimeError("gateway down")
        return ["sz000001"]

    t, _ = _http_transport(h)
    assert t.all_a_shares() == [], "失败应软返回 []"

    state["fail"] = False
    assert t.all_a_shares() == ["000001.SZ"], "失败结果被缓存了, 未能重试"
    assert state["n"] == 2


def test_code_table_concurrent_callers_fetch_once() -> None:
    """单飞: 多线程并发时只放行一次回源(provider 是模块级单例, 两线程共享实例)。"""
    started = threading.Event()
    counter = {"n": 0}

    def h(method, params):
        counter["n"] += 1
        started.set()
        time.sleep(0.2)  # 拉长窗口, 让其他线程有机会并发进入
        return ["sz000001", "sh600000"]

    t, _ = _http_transport(h)
    results: list[list[str]] = []
    lock = threading.Lock()

    def worker():
        out = t.all_a_shares()
        with lock:
            results.append(out)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=10)

    assert counter["n"] == 1, f"并发下应只回源 1 次, 实际 {counter['n']} 次"
    assert len(results) == 6
    assert all(r == ["000001.SZ", "600000.SH"] for r in results)


def test_code_table_cache_cleared_on_close() -> None:
    """close() 必须清缓存: provider 重建/数据源切换后不得残留旧清单。"""
    t, counter = _codes_transport()
    t.all_a_shares()
    assert counter["n"] == 1

    t.close()
    t.all_a_shares()
    assert counter["n"] == 2, "close 后仍命中旧缓存"


def _codes_transport_with_timeout(handler, *, timeout: float):
    """构造一个可指定 ``timeout`` 的代码表替身(用于"等待超时"类用例)。

    直接覆写 ``_rpc``(而非真实 HTTP), 故不连网络; 但保留真实的
    ``all_a_shares`` 缓存/单飞逻辑, 以便用例验证并发语义本身。
    """
    from app.plugins.eltdx.http_client import HttpTransport, _wrap

    t = HttpTransport(base_url="http://test.local", timeout=timeout)
    counter = {"n": 0}

    def _rpc(method, params):
        counter["n"] += 1
        return _wrap(handler(method, params))

    t._rpc = _rpc  # type: ignore[method-assign]
    return t, counter


# ── 代码表缓存"强制过期" ──────────────────────────────────────────────
# 为什么不能写 `t._code_at = 0.0`(2026-10-08 CI 修复):
#   过期判定是 `time.monotonic() - _code_at >= _code_ttl_s`, 而
#   **time.monotonic() 是机器/容器的运行时长**。开发机常已开机数天
#   (monotonic ≈ 数十万秒), 远超 TTL(默认 300s), 于是 0.0 恰好构成过期;
#   GitHub Actions 容器可能只启动了几十~几百秒, 0.0 - 30s 之差**根本没到 TTL**,
#   缓存被判命中、完全不回源 —— 4 个用例在 CI 上稳定报"回源 0 次 / 1 次",
#   而本机全绿。按当前 monotonic 往回推 TTL+1 秒即与 uptime 无关。
def _expire_code_cache(t) -> None:
    """把代码表缓存强制置为"已过期"(任何机器 uptime 下都成立)。

    基准时钟取 ``hc.time.monotonic`` —— **被测代码看的那个**。用本测试模块的
    ``time.monotonic()`` 只是"两边恰好是同一个模块对象"才等价, 一旦被测模块的
    time 被替换(本地复现 CI 的 fresh-boot 插件)量级就对不上, 断言随之失效。
    """
    from app.plugins.eltdx import http_client as hc

    t._code_at = hc.time.monotonic() - t._code_ttl_s - 1.0


def _age_out_code_data(t) -> None:
    """把代码表**数据**年龄推到陈旧上界之外(基准时钟同 ``_expire_code_cache``)。"""
    from app.plugins.eltdx import http_client as hc

    t._code_data_at = hc.time.monotonic() - (t._code_stale_max_s + 1)


# ── 卡死模拟的占位工具 ────────────────────────────────────────────────
# 为什么不用 time.sleep(30) 直接占位(2026-10-08 CI 修复):
#   这些用例的意图是"持有者卡死时, 等待者必须在 timeout 内有界返回"。
#   硬 sleep 的占位线程是 daemon 线程, 只有**整个 pytest 进程结束**才被杀,
#   于是它会在后续所有用例期间持续存活; 单跑该文件 17s / 跑全量更久。
#   断言又是硬上限(elapsed < 2.0 甚至 < 0.15) —— 在 GitHub runner 的
#   2 核环境上, 多个 sleep(30) 线程 + 严格时间断言叠加, 必然出现
#   调度延迟导致的偶发失败(这正是 CI 连续 10 次红的可疑根因)。
#
# 改成 Event.wait: 语义等价("卡死直到本用例结束"), 但用例一结束就能立刻
# 唤醒退出, 不给后续用例留垃圾。同样保留"远大于 timeout"的占位时长。
_BLOCK_UNTIL_RELEASE = 60.0


def _block_until(release: threading.Event, result: list[str], started: threading.Event | None = None):
    """占位 handler: 阻塞直到 ``release`` 被 set, 返回 ``result``。

    timeout 取 ``_BLOCK_UNTIL_RELEASE`` 而非无限 —— 若某个用例忘了 set
    release, 也会在 60s 后自行退出, 不会把整个测试会话拖死。
    ``started`` 用于让调用方确认"持有者确实进来了"(替代原先 sleep 前
    的 started.set())。
    """

    def _handler(method, params):
        if started is not None:
            started.set()
        release.wait(timeout=_BLOCK_UNTIL_RELEASE)
        return list(result)

    return _handler


def test_code_table_waiter_returns_bounded_when_holder_stalls() -> None:
    """**回归防护(实测缺陷)**: 持有者卡死时, 等待者必须在 timeout 内有界返回。

    旧实现里等待者 deadline 用尽后会**递归重入** ``_fetch_and_cache_code_symbols``,
    而 ``_code_fetching`` 仍为 True → 再进一轮等待 → 无限递归。实测(持有者永久
    阻塞, timeout=0.5, 4 并发): 4 个线程全部无法退出。

    更糟的是这不只是理论边界: ``_request`` 内含 2 次重试, 单次 socket 超时
    ``timeout``, 故持有者最坏耗时 ≈ 2*timeout > 等待者 deadline —— 生产中
    "等待者先到期再递归"是**常态**, 表现为 /quote 轮询与分钟增量一起静默假死。

    正确行为: 等待者超时返回 [](软失败), 由下一轮轮询自然重试。
    """
    started = threading.Event()
    release = threading.Event()

    t, _ = _codes_transport_with_timeout(
        _block_until(release, ["sz000001"], started), timeout=0.3
    )
    holder = threading.Thread(target=t.all_a_shares, daemon=True)
    holder.start()
    try:
        assert started.wait(timeout=5), "持有者未启动"

        t0 = time.perf_counter()
        result = t.all_a_shares()  # 本线程是等待者
        elapsed = time.perf_counter() - t0

        assert result == [], "等待者超时应软失败返回空"
        assert elapsed < 2.0, f"等待者必须按 timeout 有界返回, 实际 {elapsed:.2f}s"
    finally:
        # 必须释放: 否则持有者线程会滞留到 pytest 进程结束, 占用 CPU 并
        # 放大后续用例的调度延迟(2 核 runner 上尤其明显)。
        release.set()
        holder.join(timeout=5)


def test_code_table_reset_releases_stalled_single_flight() -> None:
    """**回归防护(实测缺陷)**: reset() 必须清除单飞占位, 否则自愈路径失效。

    旧实现的 ``reset()`` 只关连接池, **不清** ``_code_fetching``: 网关故障期间卡住的
    回源占位会一直为 True, 之后每次 ``all_a_shares()`` 都只能空等到 timeout 才返回
    (实测 timeout=0.3 时每次调用耗时 0.3s), 而 reset() 正是网关重启后的自愈入口。
    """
    started = threading.Event()
    release = threading.Event()

    t, _ = _codes_transport_with_timeout(
        _block_until(release, ["sz000001"], started), timeout=0.3
    )
    holder = threading.Thread(target=t.all_a_shares, daemon=True)
    holder.start()
    try:
        assert started.wait(timeout=5), "持有者未启动"

        t.reset(reason="网关重启")
        assert t._code_inflight is None, "reset 后单飞占位必须已清除"

        # 自愈后必须能立刻成功回源(而不是被旧占位拖到 timeout)
        t._rpc = lambda method, params: ["sz000001"]  # type: ignore[method-assign]
        t0 = time.perf_counter()
        assert t.all_a_shares() == ["000001.SZ"]
        elapsed = time.perf_counter() - t0
        # 上界取 timeout(0.3) 而非 0.15: 本用例要证明的是"没有白等满
        # timeout", 而非精确耗时; 0.15 在 2 核 runner 上会因调度抖动偶发失败。
        assert elapsed < 0.3, f"reset 后应立即回源, 实际 {elapsed:.2f}s(旧占位未清除)"
    finally:
        release.set()
        holder.join(timeout=5)


def test_code_table_inflight_result_discarded_after_close() -> None:
    """**回归防护(实测缺陷)**: close() 之后, 在途回源的结果不得写回缓存。

    旧实现的 ``close()`` 只把 ``_code_fetching`` 置 False, 但在途请求返回后仍会
    无条件写缓存(实测: close 后 ``_code_symbols`` 被在途结果重新填成旧清单)——
    于是 close 的清缓存形同虚设。
    """
    started = threading.Event()
    release = threading.Event()

    def h(method, params):
        started.set()
        release.wait(timeout=10)
        return ["sz000001", "sh600000"]

    t, _ = _codes_transport_with_timeout(h, timeout=5)
    holder = threading.Thread(target=t.all_a_shares, daemon=True)
    holder.start()
    assert started.wait(timeout=5), "持有者未启动"

    t.close()          # 作废在途世代
    release.set()      # 放行在途请求(其世代已失效)
    holder.join(timeout=10)
    assert not holder.is_alive(), "在途请求应已结束"

    assert t._code_symbols is None, "close 后不得被在途结果重新填充"


def test_code_table_failure_does_not_multiply_fetches() -> None:
    """**回归防护(实测缺陷)**: 回源失败时不得让每个调用者各自重拉(单飞退化)。

    旧实现在持有者失败后, 等待者会 break 出去**自行回源**, 于是 N 个并发调用者
    打出 N 次请求(实测 2/4/8 并发分别回源 2/4/8 次)—— 恰是单飞要消除的放大,
    方向上反了。正确行为: 等待者读到"同世代失败"即软返回, 由下一轮统一重试。

    本用例**无旧清单**(冷启动即失败), 故只能返回空; 有当日旧清单时的兜底行为
    见 ``test_code_table_falls_back_to_stale_list_on_failure``。
    """

    def h(method, params):
        time.sleep(0.2)  # 贴合真实失败量级(连接错误/超时, 非瞬时)
        raise RuntimeError("gateway down")

    t, counter = _codes_transport_with_timeout(h, timeout=5)
    results: list[list[str]] = []
    lock = threading.Lock()

    def worker():
        out = t.all_a_shares()
        with lock:
            results.append(out)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    assert counter["n"] == 1, f"失败时应只回源 1 次, 实际 {counter['n']} 次(单飞退化)"
    assert len(results) == 8
    assert all(r == [] for r in results), "无旧清单时失败应全部软返回空"
    # 失败不写缓存: 下一轮仍能成功后恢复
    t._rpc = lambda method, params: ["sz000001"]  # type: ignore[method-assign]
    assert t.all_a_shares() == ["000001.SZ"], "失败后下一轮应能恢复"


def test_code_table_falls_back_to_stale_list_on_failure() -> None:
    """**行为契约**: 回源失败时若有**当日旧清单**, 必须兜底返回它而不是 ``[]``。

    理由: 代码表只用于决定"拉哪些标的", 不是行情数据本身。旧清单(最多 TTL 时长
    之前)的后果是**覆盖面略窄** —— 退市标的快照取不到会被 ``_snapshot_row`` 丢弃、
    新标的下一轮补上, 不产生错误行; 而返回 ``[]`` 会让整轮行情/分钟为空
    (``get_realtime`` 直接返回空、``get_intraday_latest`` 返回空帧)。
    故有旧清单时用旧清单严格更优(与 quote_service "指数本轮获取失败, 沿用上轮缓存"
    同一思路)。

    覆盖**持有者**与**等待者**两条路径: 前者自己回源失败, 后者等到的是同世代失败。
    """
    state = {"fail": False}

    def h(method, params):
        if state["fail"]:
            time.sleep(0.2)
            raise RuntimeError("gateway down")
        return ["sz000001", "sh600000"]

    t, counter = _codes_transport_with_timeout(h, timeout=5)
    fresh = t.all_a_shares()
    assert fresh == ["000001.SZ", "600000.SH"]

    # ---- 持有者路径: 单线程, 回源失败 ----
    _expire_code_cache(t)
    state["fail"] = True
    assert t.all_a_shares() == fresh, "持有者路径应兜底当日旧清单"

    # ---- 等待者路径: 4 并发同时等到同世代失败 ----
    _expire_code_cache(t)
    counter["n"] = 0
    results: list[list[str]] = []
    lock = threading.Lock()

    def worker():
        out = t.all_a_shares()
        with lock:
            results.append(out)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    assert counter["n"] == 1, f"单飞仍应只回源 1 次, 实际 {counter['n']}"
    assert results and all(r == fresh for r in results), "等待者路径应兜底当日旧清单"


def test_code_table_stale_fallback_is_same_day_only() -> None:
    """跨日的旧清单**不得**兜底: 隔夜有上市/退市/代码变更, 且当日分区已切换。

    无当日旧清单时仍返回 ``[]``(软失败, 不伪造数据)。
    """
    from datetime import date

    t, _ = _codes_transport()
    t.all_a_shares()
    assert t._stale_code_symbols_locked() is not None, "当日清单应可兜底"

    t._code_day = date(2020, 1, 1)  # 伪造成历史日期
    assert t._stale_code_symbols_locked() is None, "跨日旧清单不得兜底"


def test_code_table_stale_fallback_has_age_bound() -> None:
    """旧清单**超过陈旧上界**后不得再兜底, 否则长时间故障会一直用数小时前的清单。

    上界之所以必须看**数据真实取回时刻**(``_code_data_at``)而不是缓存有效期起点
    (``_code_at``): 降级会刷新 ``_code_at``(让旧清单再顶一轮), 若上界也用它,
    每次降级都把年龄归零, 上界将**永不触发**(实测: 上界 1s 时连续 4 轮仍返回数据)。
    """
    state = {"fail": False}

    def h(method, params):
        if state["fail"]:
            raise RuntimeError("gateway down")
        return ["sz000001"]

    t, _ = _codes_transport_with_timeout(h, timeout=5)
    t.all_a_shares()
    assert t._stale_code_symbols_locked() is not None, "刚取回应在界内"

    # 把数据年龄推到上界之外(不改 _code_at, 模拟"降级刷新过缓存有效期")
    _age_out_code_data(t)
    assert t._stale_code_symbols_locked() is None, "超过陈旧上界不得兜底"

    # 且此时回源失败应返回空(而非降级)
    _expire_code_cache(t)
    state["fail"] = True
    assert t.all_a_shares() == [], "超出陈旧上界后失败应返回空"


def test_code_table_stale_fallback_backs_off() -> None:
    """**行为契约**: 降级使用旧清单后应刷新缓存有效期, 避免每轮都真打网关。

    否则上游持续故障时(每轮回源都失败、``_code_at`` 不变 → 缓存始终判过期),
    每轮都会发一次真实请求并记一条 warning —— 既不退避, 也对已故障的上游持续施压。
    实测修复前后: 连续 4 轮由"回源 4 次"降为"回源 1 次"。
    """
    state = {"fail": False}

    def h(method, params):
        if state["fail"]:
            raise RuntimeError("gateway down")
        return ["sz000001", "sh600000"]

    t, counter = _codes_transport_with_timeout(h, timeout=5)
    fresh = t.all_a_shares()
    assert fresh == ["000001.SZ", "600000.SH"]

    _expire_code_cache(t)
    state["fail"] = True
    counter["n"] = 0
    for _ in range(4):
        assert t.all_a_shares() == fresh, "降级应持续返回旧清单"
    assert counter["n"] == 1, (
        f"降级后应退避(只回源 1 次), 实际 {counter['n']} 次 —— 每轮都在打网关"
    )


def test_code_table_ttl_is_configurable() -> None:
    """``ELTDX_CODE_TTL`` / ``code_ttl_s``: TTL=0 等价"每轮回源", 正数生效。"""
    from app.plugins.eltdx.http_client import HttpTransport, _wrap

    calls = {"n": 0}

    def _rpc(method, params):
        calls["n"] += 1
        return _wrap(["sz000001"])

    # TTL=0: 每次调用都回源
    t0 = HttpTransport(base_url="http://test.local", timeout=5, code_ttl_s=0.0)
    t0._rpc = _rpc  # type: ignore[method-assign]
    for _ in range(3):
        t0.all_a_shares()
    assert calls["n"] == 3, f"TTL=0 应每轮回源, 实际 {calls['n']} 次"

    # TTL>0: 命中缓存
    calls["n"] = 0
    t1 = HttpTransport(base_url="http://test.local", timeout=5, code_ttl_s=300.0)
    t1._rpc = _rpc  # type: ignore[method-assign]
    for _ in range(3):
        t1.all_a_shares()
    assert calls["n"] == 1, f"TTL=300 应只回源 1 次, 实际 {calls['n']} 次"


def test_code_table_interrupted_fetch_returns_empty() -> None:
    """**已知行为**: close()/reset() 打断在途回源时, 该次调用返回 ``[]``。

    即便上游其实已成功返回, 结果也按"世代已作废 → 不可信"丢弃。这是预期语义
    (reset 期间的连接路径已重置, 数据不可信), 与 close() 一致; 但要钉住,
    否则后人看到"请求成功了却返回空"会误当 bug 改掉。
    """
    started = threading.Event()
    release = threading.Event()

    def h(method, params):
        started.set()
        release.wait(timeout=10)
        return ["sz000001", "sh600000"]

    t, _ = _codes_transport_with_timeout(h, timeout=5)
    out: dict[str, list[str]] = {}

    def holder():
        out["r"] = t.all_a_shares()

    th = threading.Thread(target=holder, daemon=True)
    th.start()
    assert started.wait(timeout=5), "持有者未启动"

    t.reset(reason="mid-flight")  # 作废在途世代
    release.set()                 # 上游其实成功返回了
    th.join(timeout=10)
    assert not th.is_alive()

    assert out["r"] == [], "被 reset 打断的调用应返回空(结果不可信)"
    assert t._code_symbols is None, "被打断的结果不得写入缓存"


def test_code_table_stays_correct_after_many_refetches() -> None:
    """长跑正确性: 反复 TTL 失效后仍返回正确结果, 且状态不随回源次数累积。

    结果槽位只保留**一个世代**。若按世代累积, 长期运行下容器会无界增长; 这里用
    行为断言(结果正确 + 单飞仍生效)覆盖, 避免直接钉住实现细节。
    """
    t, counter = _codes_transport()
    for _ in range(50):
        assert t.all_a_shares() == ["000001.SZ", "600000.SH"]
        _expire_code_cache(t)  # 强制 TTL 过期, 触发真实回源
    assert counter["n"] == 50, f"每次 TTL 失效应各回源一次, 实际 {counter['n']}"
    assert t.all_a_shares() == ["000001.SZ", "600000.SH"]


def test_transport_factory_defaults_to_http_and_never_falls_back(monkeypatch) -> None:
    """默认必须走 HTTP 网关; 且 **http 模式不因网关不可达而回退 inproc**。

    回退会静默退回"运行时崩溃牵连全部数据集 + 多进程踩踏"的已知风险路径,
    这正是本次事故要消除的东西 —— 故宁可显式失败。
    """
    from app.plugins.eltdx import provider as pv
    from app.plugins.eltdx.http_client import HttpTransport

    monkeypatch.delenv("ELTDX_TRANSPORT", raising=False)
    assert isinstance(pv._make_transport(), HttpTransport), "默认必须是 HTTP 网关"

    monkeypatch.setenv("ELTDX_HTTP_URL", "http://127.0.0.1:9999")
    t = pv._make_transport()
    assert isinstance(t, HttpTransport), "网关不可达也不得回退 inproc"
    assert t._base_url == "http://127.0.0.1:9999"


def test_transport_factory_inproc_is_explicit_opt_in(monkeypatch) -> None:
    """inproc 必须**显式选择**(诊断/离线用), 且会打印风险警告。"""
    from app.plugins.eltdx import provider as pv
    from app.plugins.eltdx.client import EltDxClient

    monkeypatch.setenv("ELTDX_TRANSPORT", "inproc")
    assert isinstance(pv._make_transport(), EltDxClient)


# ---------------------------------------------------------------------------
# HTTP 连接池: 两个实测踩到的坑(端口耗尽 / keep-alive 竞态)
# ---------------------------------------------------------------------------


def _pool(size=4, ttl=None):
    from app.plugins.eltdx.http_client import _ConnPool

    p = _ConnPool(host="127.0.0.1", port=1, size=size, timeout=1)
    if ttl is not None:
        p._IDLE_TTL_S = ttl
    return p


def test_conn_pool_reuses_connection_instead_of_new_tcp() -> None:
    """必须复用连接: urllib 式"每请求新建 TCP"会耗尽动态端口。

    实测事故: 高频轮询下 TIME_WAIT 累积 3269 条(Windows 动态端口仅 16384),
    触发 ``WinError 10055 由于系统缓冲区空间不足或队列已满``。
    """
    p = _pool(size=2)
    c1 = p.acquire()
    p.release(c1, reusable=True)
    c2 = p.acquire()
    assert c2 is c1, "空闲连接必须被复用, 而不是新建"
    assert p._created == 1, "复用不应增加连接计数"


def test_conn_pool_discards_stale_idle_connections() -> None:
    """空闲超过 TTL 的连接必须被丢弃 —— 网关(uvicorn)默认 5s 就单方面关闭它。

    实测: 面板轮询间隔 6~12s > keep-alive 5s, 几乎每次复用都撞上死连接
    (``WinError 10053 你的主机中的软件中止了一个已建立的连接``)。
    """
    p = _pool(size=2, ttl=0.05)
    c1 = p.acquire()
    p.release(c1, reusable=True)
    import time as _t

    _t.sleep(0.12)
    c2 = p.acquire()
    assert c2 is not c1, "过期连接必须被丢弃, 不得复用"
    assert p._created == 1, "丢弃过期连接后计数应回到 1(新连接)"


def test_conn_pool_drops_broken_connection() -> None:
    """出错连接不得归还(``reusable=False``), 否则后续请求继续踩雷。"""
    p = _pool(size=2)
    c1 = p.acquire()
    p.release(c1, reusable=False)
    assert p._created == 0, "坏连接必须销毁并减计数"
    c2 = p.acquire()
    assert c2 is not c1


def test_request_retries_once_on_stale_connection(monkeypatch) -> None:
    """keep-alive 竞态: 连接可能在"取出到发出"之间死掉 → 必须换新连接重试一次。

    空闲过期只能挡住"放置很久"的情况; 竞态下仍需重试兜底。
    """
    from app.plugins.eltdx.http_client import HttpTransport

    t = HttpTransport(base_url="http://127.0.0.1:1", timeout=1)
    attempts: list[int] = []

    class _FlakyConn:
        def __init__(self, n):
            self._n = n

        def request(self, *a, **k):
            attempts.append(self._n)
            if self._n == 1:
                raise ConnectionAbortedError("软件中止了一个已建立的连接")

        def getresponse(self):
            class _R:
                status = 200
                will_close = False

                def read(self):
                    return b'{"ok":true,"result":{"v":42}}'

            return _R()

        def close(self):
            pass

    seq = iter([_FlakyConn(1), _FlakyConn(2)])
    monkeypatch.setattr(t._pool, "acquire", lambda: next(seq))
    monkeypatch.setattr(t._pool, "release", lambda c, *, reusable: None)

    assert t._request("POST", "/rpc", b"{}") == {"ok": True, "result": {"v": 42}}
    assert attempts == [1, 2], "必须用新连接重试一次"


# ---------------------------------------------------------------------------
# K 线时间解析: 必须同时认 ISO8601(HTTP 网关)与 datetime(进程内)
# ---------------------------------------------------------------------------


def test_bar_time_parsing_accepts_gateway_iso8601() -> None:
    """回归防护: HTTP 网关把 time 序列化成 ISO8601 **字符串**, 必须解析成功。

    实测事故(2026-09-30 切 HTTP 后): 旧实现只认 ``'%Y-%m-%d %H:%M:%S'``(空格分隔),
    对 ISO 的 ``T`` + ``+08:00`` 偏移**静默返回 None** → 所有分钟行被区间过滤掉
    → ``get_intraday_batch`` 全市场返回 **0 行**, 服务无限"返回空数据"空转。

    两种传输的 time 形态:
      * 进程内: aware ``datetime``
      * HTTP  : ``'2026-09-30T11:25:00+08:00'``
    """
    from app.plugins.eltdx.provider import _bar_date, _bar_datetime

    iso = "2026-09-30T11:25:00+08:00"
    assert _bar_date(iso) == date(2026, 9, 30), "ISO8601 必须能取到日期"
    dt = _bar_datetime(SimpleNamespace(time=iso))
    assert dt is not None and (dt.year, dt.month, dt.day, dt.hour) == (2026, 9, 30, 11)

    # ISO 无偏移 / 空格分隔 / aware datetime 都不得回归
    assert _bar_date("2026-09-30T11:25:00") == date(2026, 9, 30)
    assert _bar_date("2026-09-30 11:25:00") == date(2026, 9, 30)
    assert _bar_date(datetime(2026, 9, 30, 11, 25)) == date(2026, 9, 30)
    # 无法解析的输入仍返回 None(不得伪造)
    assert _bar_date("not-a-time") is None
    assert _bar_date(None) is None


def test_get_intraday_batch_accepts_iso8601_bars() -> None:
    """端到端: bars 的 time 为 ISO 字符串时, get_intraday_batch 仍须返回数据。

    这是把上面那个"0 行"缺陷钉死在接口层 —— 单测 _bar_date 还不够,
    必须证明整个修复轮的取数路径不再被过滤空。
    """
    today = date.today().isoformat()
    fake = _FakeClient(
        minute_bars={
            "000001.SZ": [
                _bar(datetime.fromisoformat(f"{today}T09:31:00+08:00")),
                _bar(datetime.fromisoformat(f"{today}T09:32:00+08:00")),
            ]
        }
    )
    # 模拟 HTTP 网关: 把 time 换成 ISO 字符串
    for bars in fake._minute_bars.values():
        for b in bars:
            b.time = b.time.isoformat()

    df = _provider(fake).get_intraday_batch(["000001.SZ"], count=240)
    assert df.height == 2, "ISO8601 时间不得被区间过滤掉"
    assert df["datetime"][0].hour == 9
