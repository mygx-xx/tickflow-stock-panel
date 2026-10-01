"""财务数据源「主源 + 备份源补齐」路由契约测试。

覆盖: 主源优先不覆盖、缺失标的回退补齐、无备份源时行为不变、
备份源故障隔离、跨源合并保留主源列。
"""
from __future__ import annotations

import polars as pl

from app.services import financial_sync
from app.tickflow.capabilities import CapabilitySet


class _StubProvider:
    """按标的返回预设行的 provider 替身; ``have`` 之外的标的"取不到"。"""

    def __init__(self, rows: dict[str, dict], *, raise_exc: Exception | None = None):
        self._rows = rows
        self._raise = raise_exc
        self.calls: list[tuple[str, list[str]]] = []

    def get_financials(self, table, symbols, latest_only=True):
        self.calls.append((table, list(symbols)))
        if self._raise is not None:
            raise self._raise
        out = [
            {"symbol": s, "period_end": "2026-06-30", **self._rows[s]}
            for s in symbols
            if s in self._rows
        ]
        return pl.DataFrame(out) if out else pl.DataFrame()


def _patch_providers(monkeypatch, mapping: dict[str, object]) -> None:
    """把 custom_sources.get_provider 换成分派到替身(缺省回退真实实现)。"""
    from app.data_providers import custom as custom_sources

    real = custom_sources.get_provider

    def dispatch(name):
        return mapping.get(name) or real(name)

    monkeypatch.setattr(custom_sources, "get_provider", dispatch)


def _patch_primary(monkeypatch, name: str) -> None:
    """把生效财务源固定为 name。

    ``get_financial_provider`` 在 ``_fetch_table`` 内**局部导入** ``app.services.preferences``,
    故 patch 必须打在 preferences 模块自身的方法上(而非 financial_sync 的属性)。
    """
    from app.services import preferences

    monkeypatch.setattr(preferences, "get_financial_provider", lambda: name)


def test_primary_only_uses_when_complete(monkeypatch):
    """主源覆盖全部标的时不触碰备份源。"""
    primary = _StubProvider({"600519.SH": {"total_assets": 1.0}})
    backup = _StubProvider({"600519.SH": {"total_assets": 999.0}})
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary, "b1": backup})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: "b1")
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table("balance_sheet", ["600519.SH"], CapabilitySet())

    assert df.to_dicts()[0]["total_assets"] == 1.0
    assert backup.calls == [], "主源已覆盖全部标的, 不应请求备份源"


def test_missing_symbols_filled_from_backup(monkeypatch):
    """主源缺的标的由备份源补齐, 主源已有的一律不被覆盖。"""
    primary = _StubProvider({"600519.SH": {"total_assets": 1.0}})
    backup = _StubProvider({
        "000001.SZ": {"total_assets": 2.0},
        "600519.SH": {"total_assets": 999.0},  # 不该被采用
    })
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary, "b1": backup})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: "b1")
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table(
        "balance_sheet", ["600519.SH", "000001.SZ"], CapabilitySet()
    )
    by_sym = {r["symbol"]: r["total_assets"] for r in df.to_dicts()}

    assert by_sym == {"600519.SH": 1.0, "000001.SZ": 2.0}
    # 备份源只被问及缺失的那一只
    assert backup.calls == [("balance_sheet", ["000001.SZ"])]


def test_empty_primary_falls_back_entirely(monkeypatch):
    """主源整表为空(如 eltdx 的利润表) → 全部由备份源提供。"""
    primary = _StubProvider({})
    backup = _StubProvider({
        "600519.SH": {"revenue": 100.0},
        "000001.SZ": {"revenue": 200.0},
    })
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary, "b1": backup})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: "b1")
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table("income", ["600519.SH", "000001.SZ"], CapabilitySet())

    assert df.height == 2
    assert sorted(df["symbol"].to_list()) == ["000001.SZ", "600519.SH"]


def test_no_backup_keeps_primary_result(monkeypatch):
    """没有可用备份源时行为与改造前一致: 缺的就是缺的, 不报错。"""
    primary = _StubProvider({"600519.SH": {"total_assets": 1.0}})
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: None)
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table(
        "balance_sheet", ["600519.SH", "000001.SZ"], CapabilitySet()
    )

    assert df.height == 1
    assert df.to_dicts()[0]["symbol"] == "600519.SH"


def test_backup_failure_is_isolated(monkeypatch):
    """备份源抛异常只记 warning, 主源已取到的数据必须原样返回。"""
    primary = _StubProvider({"600519.SH": {"total_assets": 1.0}})
    backup = _StubProvider({}, raise_exc=RuntimeError("backup down"))
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary, "b1": backup})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: "b1")
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table(
        "balance_sheet", ["600519.SH", "000001.SZ"], CapabilitySet()
    )

    assert df.height == 1
    assert df.to_dicts()[0]["total_assets"] == 1.0


def test_primary_provider_failure_still_allows_backup(monkeypatch):
    """主源整体抛异常时, 全部标的转由备份源提供(不整表放弃)。"""
    primary = _StubProvider({}, raise_exc=RuntimeError("primary down"))
    backup = _StubProvider({"600519.SH": {"total_assets": 7.0}})
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary, "b1": backup})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: "b1")
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table("balance_sheet", ["600519.SH"], CapabilitySet())

    assert df.height == 1
    assert df.to_dicts()[0]["total_assets"] == 7.0


def test_backup_rows_outside_missing_set_are_dropped(monkeypatch):
    """备份源返回超出缺失集合的标的时必须丢弃, 防止反向覆盖主源。"""
    primary = _StubProvider({"600519.SH": {"total_assets": 1.0}})
    backup = _StubProvider({
        "000001.SZ": {"total_assets": 2.0},
        "999999.SZ": {"total_assets": 999.0},  # 未被请求
    })
    _patch_primary(monkeypatch, "p1")
    _patch_providers(monkeypatch, {"p1": primary, "b1": backup})
    monkeypatch.setattr(financial_sync, "_financial_backup_provider", lambda _p: "b1")
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)

    df = financial_sync._fetch_table(
        "balance_sheet", ["600519.SH", "000001.SZ"], CapabilitySet()
    )

    assert "999999.SZ" not in df["symbol"].to_list()


def test_backup_selection_skips_primary_and_unavailable(monkeypatch):
    """备份源选取: 跳过主源自身、跳过不可用插件、要求声明 financial 数据集。"""
    from app.data_providers import custom as custom_sources

    monkeypatch.setattr(custom_sources, "list_plugins", lambda: [
        {"name": "primary_one", "available": True, "datasets": ["financial"]},
        {"name": "broken_one", "available": False, "datasets": ["financial"]},
        {"name": "no_finance", "available": True, "datasets": ["daily"]},
        {"name": "good_backup", "available": True, "datasets": ["financial"]},
    ])
    monkeypatch.setattr(
        custom_sources, "provider_has_dataset",
        lambda n, d: d == "financial" and n in {"primary_one", "broken_one", "good_backup"},
    )

    assert financial_sync._financial_backup_provider("primary_one") == "good_backup"
    # 主源是 good_backup 时, 应回落到 primary_one
    assert financial_sync._financial_backup_provider("good_backup") == "primary_one"
