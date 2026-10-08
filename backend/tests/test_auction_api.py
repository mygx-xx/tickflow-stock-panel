"""竞价 API 的降级表达与入参边界。

这里锁的是「前端能不能分清三种状态」: 数据源没配 / 当天确实没竞价 / 有数据。
把 no_data 画成 0、或把 source_unavailable 说成「今天没数据」都是错误金融读数。
"""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.auction import router
from app.data_providers.base import AUCTION_COLUMNS, AUCTION_SCHEMA
from app.services.auction_service import AuctionService
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet

D = date(2026, 10, 8)


class _Store:
    def __init__(self, data_dir) -> None:
        self.data_dir = data_dir


class _Repo:
    def __init__(self, data_dir) -> None:
        self.store = _Store(data_dir)

    def get_instruments(self) -> pl.DataFrame:
        return pl.DataFrame({"symbol": ["600519.SH"], "name": ["贵州茅台"]})

    # 日级 enriched 缓存不提供: 历史日昨收取不到 → 榜的竞价涨幅应为 null
    def get_enriched_history_span(self):
        return None

    def get_enriched_range(self, start, end, symbols=None, columns=None):
        return None


def _frame(symbol: str, *, price: float = 10.0, matched: float = 1000.0) -> pl.DataFrame:
    rows = [
        {
            "symbol": symbol,
            "trade_date": D,
            "segment": "open",
            "datetime": datetime(2026, 10, 8, 9, 24 + i, 0),
            "price": price,
            "matched_volume": matched + i,
            "unmatched_volume": 500.0,
            "unmatched_side": "buy",
        }
        for i in (0, 1)
    ]
    return pl.DataFrame(rows, schema=AUCTION_SCHEMA).select(AUCTION_COLUMNS)


def _client(tmp_path, monkeypatch, *, provider_rows=None, capable: bool = True):
    provider = SimpleNamespace(
        get_auction_batch=MagicMock(
            return_value=_frame("600519.SH") if provider_rows is None else provider_rows
        )
    )
    monkeypatch.setattr("app.services.preferences.get_auction_data_provider", lambda: "eltdx")
    monkeypatch.setattr(
        "app.data_providers.custom.provider_has_dataset",
        lambda name, dataset: name == "eltdx" and dataset == "auction",
    )
    monkeypatch.setattr("app.data_providers.custom.get_provider", lambda name: provider)

    svc = AuctionService()
    svc.set_repo(_Repo(tmp_path))
    caps = CapabilitySet({Cap.AUCTION_BATCH: CapabilityLimits(batch=200)}) if capable else CapabilitySet({})
    svc.set_app_state(SimpleNamespace(
        capabilities=caps,
        quote_service=SimpleNamespace(
            get_enriched_today=MagicMock(return_value=(pl.DataFrame(), D))
        ),
    ))

    app = FastAPI()
    app.include_router(router)
    app.state.repo = _Repo(tmp_path)
    app.state.auction_service = svc
    return TestClient(app), svc, provider


def test_board_ok_after_sweep(tmp_path, monkeypatch):
    client, svc, _ = _client(tmp_path, monkeypatch)
    svc.sweep(D)

    body = client.get("/api/auction/board", params={"date": D.isoformat()}).json()

    assert body["state"] == "ok" and body["ready"]
    item = body["items"][0]
    assert item["symbol"] == "600519.SH" and item["name"] == "贵州茅台"
    assert item["matched_volume"] == 1001.0  # 末点
    assert item["matched_amount"] == 10.0 * 1001 * 100

    # 缺省日期回落到最近已落盘日
    assert client.get("/api/auction/board").json()["trade_date"] == D.isoformat()


def test_board_without_data_is_no_data_not_zero(tmp_path, monkeypatch):
    client, _, _ = _client(tmp_path, monkeypatch)
    body = client.get("/api/auction/board", params={"date": D.isoformat()}).json()

    assert body["state"] == "no_data" and body["items"] == [] and body["total"] == 0
    assert "无竞价落盘数据" in body["message"]


def test_board_without_source_is_unavailable(tmp_path, monkeypatch):
    client, _, provider = _client(tmp_path, monkeypatch, capable=False)
    body = client.get("/api/auction/board").json()

    assert body["state"] == "source_unavailable"
    assert "数据源配置" in body["message"]
    provider.get_auction_batch.assert_not_called()


def test_series_symbol_missing_falls_back_to_provider(tmp_path, monkeypatch):
    client, svc, provider = _client(tmp_path, monkeypatch)
    svc.sweep(D)

    body = client.get("/api/auction/series", params={"symbol": "510300.SH", "date": D.isoformat()}).json()

    assert body["state"] == "ok" and len(body["points"]) == 2
    assert body["source"].startswith("provider:")
    assert provider.get_auction_batch.call_args_list[-1].args[0] == ["510300.SH"]


def test_series_without_points_explains_why(tmp_path, monkeypatch):
    client, _, _ = _client(tmp_path, monkeypatch, provider_rows=pl.DataFrame(schema=AUCTION_SCHEMA))
    body = client.get("/api/auction/series", params={"symbol": "830799.BJ", "date": D.isoformat()}).json()

    assert body["state"] == "no_data" and body["points"] == []
    assert "830799.BJ" in body["msg"] and "无竞价记录" in body["msg"]


def test_series_without_source_says_so(tmp_path, monkeypatch):
    """源没配时不能说成「这天没有竞价」—— 两种降级在前端必须是不同提示。"""
    client, _, provider = _client(tmp_path, monkeypatch, capable=False)
    body = client.get("/api/auction/series", params={"symbol": "600519.SH", "date": D.isoformat()}).json()

    assert body["state"] == "source_unavailable" and body["points"] == []
    assert "数据源配置" in body["msg"]
    provider.get_auction_batch.assert_not_called()


def test_status_and_manual_sweep(tmp_path, monkeypatch):
    client, _, provider = _client(tmp_path, monkeypatch)

    st = client.get("/api/auction/status").json()
    assert st["state"] == "ok" and st["usable"] and st["provider"] == "eltdx"

    out = client.post("/api/auction/sweep", params={"date": D.isoformat()}).json()
    assert out["state"] == "ok" and out["persisted"] and out["symbols"] == 1
    provider.get_auction_batch.assert_called_once()


def test_bad_date_is_400_and_bad_segment_is_422(tmp_path, monkeypatch):
    client, _, _ = _client(tmp_path, monkeypatch)

    # 注意: date.fromisoformat 接受 20261008 这种紧凑写法, 所以用真非法值断言 400
    assert client.get("/api/auction/board", params={"date": "2026-13-45"}).status_code == 400
    assert client.get("/api/auction/board", params={"segment": "noon"}).status_code == 422
