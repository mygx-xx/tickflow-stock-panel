"""eltdx 数据源插件契约测试。

按 docs/plugin-development.md「测试要求」的 7 项覆盖, **不依赖真实网络与主站**:
用假 client(替身对象)注入, 验证字段映射、单位换算、软失败、能力声明与 loader 集成。

范本: backend/tests/test_fuyao_provider.py
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

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

    def __init__(self, *, bars=None, shares=None, snapshots=None, fail_bars=False):
        self._bars = bars or {}
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
        return list(self._bars.get(symbol, []))

    def iter_bars_batches(self, symbols, *, period="day", count, batch_size):
        """与真实实现同形的有界分批(yield [(symbol, bars)])。"""
        step = max(1, batch_size)
        for i in range(0, len(symbols), step):
            chunk = symbols[i : i + step]
            out = [(s, list(self._bars.get(s, []))) for s in chunk]
            out.sort(key=lambda item: item[0])
            yield out

    def snapshots(self, symbols, *, batch_size):
        self.calls.append(("snapshots", len(symbols)))
        return list(self._snapshots)

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
    """只声明 daily/realtime; 其余数据集 provider_has_dataset 语义为 False(回退 TickFlow)。"""
    p = EltDxProvider()
    ds = p.config.datasets
    assert set(ds) == {"daily", "realtime"}
    for not_supported in ("adj_factor", "minute", "full_minute", "depth5", "financial"):
        assert not_supported not in ds


def test_test_dataset_reports_error_for_undeclared() -> None:
    """试拉未声明数据集 → 返回 error 说明会回退, 不抛异常。"""
    out = EltDxProvider().test_dataset("minute")
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
    assert set(manifest["datasets"]) == {"daily", "realtime"}
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
