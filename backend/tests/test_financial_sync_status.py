"""财务同步状态语义: 0 行写入不得伪装成"已同步"(2026-10-01 利润表事故)。

那次事故里 fuyao 连接池坏死导致整表 0 行, 但 last_sync 照常前进, 前端看起来
同步成功。这里锁定: 只有写入数据才推进 last_sync; 0 行保持原状并显式告警。
"""

from __future__ import annotations

from app.services import financial_sync
from app.services.financial_sync import FinancialScheduler
from app.tickflow.capabilities import CapabilitySet


def _scheduler(tmp_path) -> FinancialScheduler:
    fs = FinancialScheduler()
    fs._data_dir = tmp_path
    fs._capset = CapabilitySet()
    return fs


def test_zero_rows_does_not_advance_last_sync(monkeypatch, tmp_path):
    fs = _scheduler(tmp_path)
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)
    monkeypatch.setattr(financial_sync, "sync_income", lambda data_dir, capset: 0)

    result = fs.run_now("income")

    assert result == {"income": 0}
    assert "income" not in fs.last_sync


def test_positive_rows_advances_last_sync(monkeypatch, tmp_path):
    fs = _scheduler(tmp_path)
    monkeypatch.setattr(financial_sync, "_financial_is_custom", lambda: True)
    monkeypatch.setattr(financial_sync, "sync_income", lambda data_dir, capset: 7)
    from app.services import preferences

    monkeypatch.setattr(preferences, "set_financial_sync_time", lambda table, ts: None)

    result = fs.run_now("income")

    assert result == {"income": 7}
    assert fs.last_sync.get("income")
