"""定时任务节假日门控测试。

CronTrigger 只能表达 mon-fri, 覆盖不到调休/长假; 这些用例锁定「探针确定休市时
调度入口不建任务、不执行」的行为 (2026-10-01 国庆当天 instruments job 照跑的回归)。
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from app.jobs import daily_pipeline
from app.services import trading_day


def test_holiday_skip_semantics(monkeypatch):
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)
    assert daily_pipeline._holiday_skip("x") is True
    # 未知 (None) 不拦 —— 与全项目交易日探针同语义
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: None)
    assert daily_pipeline._holiday_skip("x") is False
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: True)
    assert daily_pipeline._holiday_skip("x") is False


def test_scheduled_job_skips_on_holiday(monkeypatch):
    called = []
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)
    ok = daily_pipeline._scheduled_job(lambda on_progress=None: called.append(1), "test")
    assert ok is False
    assert called == []


def test_scheduled_job_runs_when_trading(monkeypatch):
    seen = []

    def _fake_tracked(fn, label):
        seen.append((fn, label))
        return True

    def _fn(on_progress=None):
        return None

    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: True)
    monkeypatch.setattr(daily_pipeline, "_run_tracked", _fake_tracked)
    assert daily_pipeline._scheduled_job(_fn, "test") is True
    assert seen == [(_fn, "test")]


def _state_with_depth(finalized: list):
    return SimpleNamespace(
        depth_service=SimpleNamespace(finalize=lambda: finalized.append(1)),
    )


def test_scheduled_depth_finalize_skips_on_holiday(monkeypatch):
    finalized = []
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)
    monkeypatch.setattr(daily_pipeline, "_get_app_state", lambda: _state_with_depth(finalized))
    daily_pipeline._scheduled_depth_finalize()
    assert finalized == []


def test_scheduled_depth_finalize_runs_when_trading(monkeypatch):
    finalized = []
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: True)
    monkeypatch.setattr(daily_pipeline, "_get_app_state", lambda: _state_with_depth(finalized))
    daily_pipeline._scheduled_depth_finalize()
    assert finalized == [1]


def test_scheduled_review_skips_on_holiday(monkeypatch):
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)
    # 休市 → 直接返回, 不触达 AI Key / 复盘生成 (repo=None 也不会报错)
    asyncio.run(daily_pipeline._run_scheduled_review(None))
