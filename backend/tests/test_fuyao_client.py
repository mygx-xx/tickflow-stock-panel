"""fuyao 客户端连接池自愈回归测试 (2026-10-01 利润表事故)。

事故: 长期复用的 httpx keep-alive 连接池一次瞬断后整体坏死, 此后每个请求都是
``WinError 10061`` 且**永不自愈**; 该次利润表同步 55 分钟、1555 次请求全部失败,
最终 0 行落盘, 前端却显示"已同步"。修复: 连接层错误时重建连接池并重试一次。
本测试锁定该行为, 防止回归到"坏池一直用、失败被当无数据"。
"""

from __future__ import annotations

import httpx
import pytest

from app.plugins.fuyao import client as fc
from app.plugins.fuyao.client import FuyaoClient, FuyaoError


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload


class _FakeHttp:
    """``httpx.Client`` 替身: 记录 get 次数与是否被 close, 可切换成连接层失败。"""

    def __init__(self, *, fail: bool = False, payload: dict | None = None) -> None:
        self.fail = fail
        self.payload = payload or {}
        self.gets = 0
        self.closed = False

    def get(self, path: str, params: dict | None = None) -> _FakeResponse:
        self.gets += 1
        if self.fail:
            raise httpx.ConnectError("connection refused")
        return _FakeResponse(self.payload)

    def close(self) -> None:
        self.closed = True


def _install(monkeypatch, *clients: _FakeHttp) -> list[_FakeHttp]:
    """按构造顺序把 FuyaoClient 的 httpx.Client 换成给定替身。"""
    created: list[_FakeHttp] = []

    def factory(**kwargs) -> _FakeHttp:
        created.append(clients[len(created)])
        return created[-1]

    monkeypatch.setattr(fc.httpx, "Client", factory)
    return created


def test_connection_error_rebuilds_pool_and_retries(monkeypatch):
    created = _install(
        monkeypatch,
        _FakeHttp(fail=True),
        _FakeHttp(payload={"code": 0, "data": {"ok": 1}}),
    )
    client = FuyaoClient(api_key="k")

    assert client._get("/x", {}) == {"ok": 1}
    assert created[0].closed, "坏死的连接池必须被关闭"
    assert client._http is created[1], "重试必须走重建后的连接池"


def test_retry_failure_raises_fuyao_error(monkeypatch):
    _install(monkeypatch, _FakeHttp(fail=True), _FakeHttp(fail=True))
    client = FuyaoClient(api_key="k")

    with pytest.raises(FuyaoError):
        client._get("/x", {})


def test_business_error_does_not_rebuild_pool(monkeypatch):
    created = _install(
        monkeypatch,
        _FakeHttp(payload={"code": 1002, "message": "Unknown thscode"}),
    )
    client = FuyaoClient(api_key="k")

    with pytest.raises(FuyaoError) as excinfo:
        client._get("/x", {})

    assert "1002" in str(excinfo.value)
    assert len(created) == 1, "业务错误不是连接层故障, 不应重建连接池"
