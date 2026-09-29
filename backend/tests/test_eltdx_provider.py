"""eltdx 数据源插件契约测试。

按 docs/plugin-development.md「测试要求」的 7 项覆盖, **不依赖真实网络与主站**:
用假 client(替身对象)注入, 验证字段映射、单位换算、软失败、能力声明与 loader 集成。

范本: backend/tests/test_fuyao_provider.py
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import ANY

import polars as pl
import pytest

from app.plugins.eltdx import client as eltdx_client
from app.plugins.eltdx.client import EltDxClient, to_eltdx_code, to_panel_symbol
from app.plugins.eltdx.provider import EltDxProvider, _snapshot_ts, availability

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


def test_snapshot_ts_parsing_and_invalid() -> None:
    """time_raw = 当日 HHMMSScc 紧凑整数(实测 8 位, 末 2 位为百分秒)。

    真机样例: 15330366 → 15:33:03.66(当日最后一笔, 收盘后 15:33 时刻)。
    非法值(时/分/秒越界、位数不足)返回 None, 由下游退本地时间。
    """
    ts = _snapshot_ts(15330366)  # 15:33:03.66
    assert ts is not None
    dt = datetime.fromtimestamp(ts / 1000)
    assert (dt.hour, dt.minute, dt.second) == (15, 33, 3)
    assert _snapshot_ts(None) is None
    assert _snapshot_ts("abc") is None
    assert _snapshot_ts(12345) is None  # 位数不足 6
    assert _snapshot_ts(99999999) is None  # hour=99 越界
    assert _snapshot_ts(15609999) is None  # minute=60 越界


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


def test_availability_ok_when_eltdx_installed() -> None:
    ok, reason = availability()
    assert ok is True
    assert "eltdx" in reason


def test_availability_false_when_import_fails(monkeypatch) -> None:
    """依赖缺失 → (False, 安装提示), 不抛异常。"""
    import builtins

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "eltdx":
            raise ImportError("No module named 'eltdx'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    ok, reason = availability()
    assert ok is False
    assert "安装依赖" in reason


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
    hh: int, mm: int, *, sec: int = 0, c: float = 10.2, vol: float = 100.0, amt: float = 102000.0
):
    """1m KlineBar 替身: time 为 Asia/Shanghai aware(与 eltdx 实测一致)。"""
    return _bar(
        datetime(2026, 9, 29, hh, mm, sec, tzinfo=_CN),
        o=c,
        h=c + 0.01,
        low=c - 0.01,
        c=c,
        volume_lots=vol,
        amount=amt,
    )


def test_minute_field_mapping_and_naive_beijing_wallclock() -> None:
    """契约红线: datetime 必须是北京时间墙钟 **naive**(非 UTC、无 tzinfo)。"""
    fake = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31), _mbar(9, 32)]})
    df = _provider(fake).get_minute(["000001.SZ"], date(2026, 9, 29), date(2026, 9, 29))

    assert df.columns == ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    assert df.height == 2
    assert df.schema["datetime"].time_zone is None, "必须是 naive(无时区)"
    first = df["datetime"][0]
    assert (first.hour, first.minute) == (9, 31), "必须是北京墙钟(若误转 UTC 会变 01:31)"


def test_minute_uses_1m_period_route() -> None:
    """分钟必须走 period='1m'(不是 day, 也不是分时接口)。"""
    fake = _FakeClient(minute_bars={"000001.SZ": [_mbar(9, 31)]})
    _provider(fake).get_minute(["000001.SZ"], date(2026, 9, 29), date(2026, 9, 29))
    assert ("bars", "000001.SZ", "1m", ANY) in [
        (c[0], c[1], c[2], ANY) for c in fake.calls if c[0] == "bars"
    ]


def test_minute_filters_out_of_range_bars() -> None:
    """区间外分钟必须过滤(源可能返回超过请求区间的深度)。"""
    fake = _FakeClient(
        minute_bars={
            "000001.SZ": [
                _bar(datetime(2026, 9, 25, 10, 0, tzinfo=_CN)),  # 区间外(前一交易日)
                _mbar(9, 31),
            ]
        }
    )
    df = _provider(fake).get_minute(["000001.SZ"], date(2026, 9, 29), date(2026, 9, 29))
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
            if symbol == "600000.SH":
                raise RuntimeError("station error")
            return super().bars(symbol, period=period, count=count)

    fake = _Flaky(minute_bars={"000001.SZ": [_mbar(9, 31)]})
    df = _provider(fake).get_minute(
        ["000001.SZ", "600000.SH"], date(2026, 9, 29), date(2026, 9, 29)
    )
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
def test_hhmmss_ts_parses_both_widths(raw, expect_hm) -> None:
    """时间戳解析需兼容 6 位(盘口)与 8 位(快照)两种紧凑形态。"""
    from app.plugins.eltdx.provider import _hhmmss_ts

    ts = _hhmmss_ts(raw)
    if expect_hm is None:
        assert ts is None
    else:
        dt = datetime.fromtimestamp(ts / 1000)
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
    batch_size = 75  # provider._FINANCE_BATCH 默认值
    seen: list[list[str]] = []

    class _Flaky(_FakeClient):
        def finance_batch(self, codes):
            seen.append(list(codes))
            if len(seen) == 1:
                return _FinPage([_fin("000001", "sz")])  # 首批成功
            raise RuntimeError("station down")  # 次批炸

    # 76 只标的 → 2 批(75 + 1)
    symbols = [f"{i:06d}.SZ" for i in range(1, batch_size + 2)]
    df = _provider(_Flaky()).get_financials("shares", symbols)

    assert len(seen) == 2, "应切成 2 批请求"
    assert df.height == 1, "首批结果必须保留"
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
