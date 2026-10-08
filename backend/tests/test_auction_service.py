"""竞价服务层: 路由/按批隔离/落盘合并/榜聚合单位 的契约测试。

单位锚点(手→股 ×100)与小数制竞价涨幅在这里钉死: 这两处一旦改错,
竞价榜的「匹配额」会差两个数量级、涨幅列会静默失真, 而代码看起来仍然合理。
"""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import polars as pl
import pytest

from app.data_providers.base import AUCTION_COLUMNS, AUCTION_SCHEMA
from app.services import auction_service as auction_module
from app.services.auction_service import AuctionService
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

D = date(2026, 10, 8)


def _rows(symbol: str, *, seg: str = "open", n: int = 2, price: float = 10.0) -> list[dict]:
    """构造某标的的竞价逐点行(末点的量价决定榜单读数)。"""
    times = {"open": [(9, 24, 57), (9, 25, 0)], "close": [(14, 57, 0), (14, 59, 57), (15, 0, 0)]}[seg]
    out = []
    for i in range(n):
        hh, mm, ss = times[min(i, len(times) - 1)]
        out.append({
            "symbol": symbol,
            "trade_date": D,
            "segment": seg,
            "datetime": datetime(2026, 10, 8, hh, mm, ss),  # 契约: 北京墙钟 naive
            "price": price,
            "matched_volume": 1000.0 + i,
            "unmatched_volume": 500.0,
            "unmatched_side": "buy",
        })
    return out


class _Store:
    def __init__(self, data_dir) -> None:
        self.data_dir = data_dir


class _Repo:
    """最小 repo: 只提供竞价服务用到的 store.data_dir、instruments 维表与日级昨收缓存。"""

    def __init__(self, data_dir, symbols: list[str], history: pl.DataFrame | None = None) -> None:
        self.store = _Store(data_dir)
        self._symbols = symbols
        self._history = history

    def get_instruments(self) -> pl.DataFrame:
        return pl.DataFrame({
            "symbol": self._symbols,
            "name": [f"名称{s}" for s in self._symbols],
        })

    def get_enriched_history_span(self):
        if self._history is None or self._history.is_empty():
            return None
        return self._history["date"].min(), self._history["date"].max()

    def get_enriched_range(self, start, end, symbols=None, columns=None):
        if self._history is None:
            return None
        return self._history.filter((pl.col("date") >= start) & (pl.col("date") <= end))


def _service(
    tmp_path, symbols: list[str], *, batch: int = 2,
    quotes: pl.DataFrame | None = None, session_date: date = D, history: pl.DataFrame | None = None,
):
    svc = AuctionService()
    svc.set_repo(_Repo(tmp_path, symbols, history=history))
    qs = SimpleNamespace(
        # 服务用「行情缓存会话日 + prev_close」判日期: 桩里默认快照就是 D 当天的
        get_enriched_today=MagicMock(return_value=(pl.DataFrame() if quotes is None else quotes, session_date))
    )
    svc.set_app_state(SimpleNamespace(
        capabilities=CapabilitySet({Cap.AUCTION_BATCH: CapabilityLimits(batch=batch)}),
        quote_service=qs,
    ))
    return svc


def _frame(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=AUCTION_SCHEMA).select(AUCTION_COLUMNS)


def _route(monkeypatch, provider) -> None:
    monkeypatch.setattr("app.services.preferences.get_auction_data_provider", lambda: "eltdx")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "eltdx" and dataset == "auction",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)
    monkeypatch.setattr(auction_module, "sleep_between_batches", MagicMock())


# ---------------------------------------------------------------- 路由与隔离
def test_sweep_routes_to_provider_and_chunks(monkeypatch, tmp_path):
    provider = SimpleNamespace(get_auction_batch=MagicMock(
        side_effect=lambda syms, d: _frame([r for s in syms for r in _rows(s)])
    ))
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH", "B.SH", "C.SH"], batch=2)

    stats = svc.sweep(D)

    assert provider.get_auction_batch.call_args_list == [call(["A.SH", "B.SH"], D), call(["C.SH"], D)]
    assert stats["ok"] and stats["symbols"] == 3 and stats["persisted"]
    assert stats["provider"] == "eltdx"


def test_batch_failure_isolated_without_fallback(monkeypatch, tmp_path):
    """单批失败只丢该批标的; 不得回退到其它源(TickFlow 根本没有竞价接口)。"""
    provider = SimpleNamespace(get_auction_batch=MagicMock(
        side_effect=[RuntimeError("boom"), _frame(_rows("C.SH"))]
    ))
    _route(monkeypatch, provider)
    monkeypatch.setattr(
        "app.data_providers.registry.get_provider",
        lambda name: pytest.fail("失败批不得跨源回退"),
    )
    svc = _service(tmp_path, ["A.SH", "B.SH", "C.SH"], batch=2)

    stats = svc.sweep(D)

    assert stats["failed_batches"] == 1
    assert stats["symbols"] == 1
    got = svc._read_partition(D)["symbol"].to_list()
    assert set(got) == {"C.SH"} and len(got) == len(_rows("C.SH"))


def test_sweep_without_capability_does_not_touch_provider(monkeypatch, tmp_path):
    svc = _service(tmp_path, ["A.SH"])
    svc.set_app_state(SimpleNamespace(capabilities=CapabilitySet({}), quote_service=None))
    provider = SimpleNamespace(get_auction_batch=MagicMock())
    _route(monkeypatch, provider)

    stats = svc.sweep(D)

    provider.get_auction_batch.assert_not_called()
    assert not stats["ok"] and "竞价数据源" in stats["msg"]


def test_empty_points_are_not_a_failure(monkeypatch, tmp_path):
    """空帧是正常状态(休市/无竞价/超回溯窗口), 不计失败批、不落盘、不伪造。"""
    provider = SimpleNamespace(get_auction_batch=MagicMock(
        side_effect=lambda syms, d: pl.DataFrame(schema=AUCTION_SCHEMA)
    ))
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH"])

    stats = svc.sweep(D)

    assert stats["failed_batches"] == 0 and stats["rows"] == 0 and not stats["persisted"]
    assert "无竞价数据" in stats["msg"]


# ---------------------------------------------------------------- 落盘与合并
def test_persist_roundtrip_keeps_contract_columns(tmp_path, monkeypatch):
    rows = _rows("A.SH") + _rows("A.SH", seg="close", n=3)
    provider = SimpleNamespace(get_auction_batch=MagicMock(
        side_effect=lambda syms, d: _frame(rows)
    ))
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH"])

    svc.sweep(D)
    df = svc._read_partition(D)

    assert df.columns == list(AUCTION_COLUMNS)
    assert set(df["segment"].to_list()) == {"open", "close"}
    assert df.filter(pl.col("segment") == "close").height == 3


def test_degraded_sweep_never_shrinks_history(tmp_path, monkeypatch):
    """第二轮只拿到部分标的时, 已落盘的其余标的必须保留(合并而不是覆盖)。"""
    calls = {"n": 0}

    def fetch(syms, d):  # 分批内容不参与判定, 只按轮次区分退化/完整
        calls["n"] += 1
        if calls["n"] == 1:
            return _frame(_rows("A.SH") + _rows("B.SH"))
        return _frame(_rows("A.SH", price=11.0))

    provider = SimpleNamespace(get_auction_batch=MagicMock(side_effect=fetch))
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH", "B.SH"], batch=5)

    svc.sweep(D)
    svc.sweep(D)
    df = svc._read_partition(D)

    assert sorted(df["symbol"].to_list()) == ["A.SH", "A.SH", "B.SH", "B.SH"]
    # 本轮标的覆盖旧行(价格 10→11), 本轮没拿到的标的保留旧行
    assert df.filter(pl.col("symbol") == "A.SH")["price"].unique().to_list() == [11.0]
    assert df.filter(pl.col("symbol") == "B.SH")["price"].unique().to_list() == [10.0]


# ---------------------------------------------------------------- 竞价榜读数
def test_board_uses_last_point_per_segment_and_lot_unit(tmp_path, monkeypatch):
    """榜取每段末点; 匹配额 = 价(元) × 量(手) × 100 → 元(跨边界单位换算钉死)。"""
    rows = _rows("A.SH", n=2, price=10.0) + _rows("A.SH", seg="close", n=3, price=12.0)
    provider = SimpleNamespace(get_auction_batch=MagicMock(side_effect=lambda s, d: _frame(rows)))
    _route(monkeypatch, provider)
    quotes = pl.DataFrame({"symbol": ["A.SH"], "prev_close": [12.5]})
    svc = _service(tmp_path, ["A.SH"], quotes=quotes)
    svc.sweep(D)

    board = svc.board(D, segment="open")

    assert board["total"] == 1  # 开盘榜不受同日收盘段干扰
    item = board["items"][0]
    assert item["price"] == 10.0 and item["matched_volume"] == 1001.0  # 末点, 非首点
    assert item["matched_amount"] == pytest.approx(10.0 * 1001 * 100)
    assert item["unmatched_amount"] == pytest.approx(10.0 * 500 * 100)
    assert item["name"] == "名称A.SH"
    # 竞价涨幅是**小数制**(-0.2 = -20%), 与同页百分数制的 auction_pct 刻意区分
    assert item["auction_change_ratio"] == pytest.approx(-0.2)

    assert svc.board(D, segment="close")["items"][0]["matched_volume"] == 1002.0


def test_board_change_ratio_null_without_prev_close(tmp_path, monkeypatch):
    """取不到昨收 → 涨幅为 null, 不拿虚拟价自己编基准。"""
    provider = SimpleNamespace(get_auction_batch=MagicMock(
        side_effect=lambda s, d: _frame(_rows("A.SH"))
    ))
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH"])
    svc.sweep(D)

    item = svc.board(D)["items"][0]

    assert item["prev_close"] is None and item["auction_change_ratio"] is None


def test_board_missing_date_is_not_ready(tmp_path):
    svc = _service(tmp_path, ["A.SH"])
    out = svc.board(D)
    assert out["items"] == [] and out["total"] == 0 and not out["ready"]


def test_board_prev_close_anchored_to_its_own_session(tmp_path, monkeypatch):
    """竞价涨幅只能用**该竞价日**的昨收: 快照会话日不同 → null; 日级缓存覆盖该日 → 用该日的。

    拿另一天的昨收冒充会算出一个看起来完全合理的错误涨跌幅, 是金融读数错误不是显示瑕疵。
    """
    provider = SimpleNamespace(get_auction_batch=MagicMock(
        side_effect=lambda s, d: _frame(_rows("A.SH", price=10.0))
    ))
    _route(monkeypatch, provider)
    quotes = pl.DataFrame({"symbol": ["A.SH"], "prev_close": [12.5]})

    svc = _service(tmp_path, ["A.SH"], quotes=quotes, session_date=date(2026, 10, 9))
    svc.sweep(D)
    assert svc.board(D)["items"][0]["auction_change_ratio"] is None

    history = pl.DataFrame({"symbol": ["A.SH"], "date": [D], "prev_close": [8.0]})
    svc2 = _service(tmp_path, ["A.SH"], quotes=quotes, session_date=date(2026, 10, 9), history=history)
    svc2.sweep(D)
    item = svc2.board(D)["items"][0]
    assert item["prev_close"] == 8.0
    assert item["auction_change_ratio"] == pytest.approx(0.25)


# ---------------------------------------------------------------- 单股序列
def test_series_reads_partition_then_falls_back_to_provider(tmp_path, monkeypatch):
    fetch = MagicMock(side_effect=lambda syms, d: _frame([r for s in syms for r in _rows(s)]))
    provider = SimpleNamespace(get_auction_batch=fetch)
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH"], quotes=pl.DataFrame({"symbol": ["A.SH"], "prev_close": [12.5]}))
    svc.sweep(D)  # 分区里只有 A.SH

    part = svc.get_series("A.SH", D)
    missing = svc.get_series("510300.SH", D)

    assert part["source"] == "parquet" and len(part["points"]) == 2
    assert missing["source"].startswith("provider:") and len(missing["points"]) == 2
    assert fetch.call_args_list[-1] == call(["510300.SH"], D)  # 单标的回源, 不拉全市场
    # 昨收跟着序列一起给出(个股图画昨收线/涨跌幅轴的基准); 股票缓存里没有 ETF → null, 不臆造
    assert part["prev_close"] == 12.5
    assert missing["prev_close"] is None


def test_series_provider_error_returns_empty_not_fake(tmp_path, monkeypatch):
    provider = SimpleNamespace(get_auction_batch=MagicMock(side_effect=RuntimeError("down")))
    _route(monkeypatch, provider)
    svc = _service(tmp_path, ["A.SH"])

    out = svc.get_series("A.SH", D)

    assert out["points"] == [] and out["msg"] and "无竞价记录" in out["msg"]
