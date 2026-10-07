"""rebuild_enriched 端点的智能模式选择。

端点原本无条件走全量 —— 在 1454 天 / 720 万行的库上就是 49 秒 + 全量重写,
而运维点它时通常只想补一天。这里锚定三件事:
1. ?mode=plan 只探测不执行, 且返回的依据能解释"这次该做什么"。
2. auto 模式下 run_pipeline 拿到的参数确实来自探测结果(而不是又偷偷全量)。
3. 已对齐时直接判 noop, 不再白跑一次全量。

为什么不用 TestClient: 端点用 asyncio.create_task 起后台任务, TestClient 的事件循环
在请求返回后就关闭, 任务永远跑不到完成。改为直接 asyncio.run 端点协程 + 让后台任务
跑完, 这才是该端点在生产里的真实执行路径。
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from app.api import kline, pipeline
from app.services import pipeline_jobs


def _mk(root: Path, which: str, dates: list[str]) -> None:
    base = root / ("kline_daily" if which == "daily" else "kline_daily_enriched")
    for d in dates:
        (base / f"date={d}").mkdir(parents=True, exist_ok=True)


def _request(tmp_path: Path, monkeypatch, query: str = "") -> dict:
    """构造最小 app, 直接跑端点协程并等待其后台任务收尾。"""
    app = FastAPI()
    app.include_router(pipeline.router)
    app.include_router(kline.router)
    app.state.repo = SimpleNamespace(
        store=SimpleNamespace(data_dir=tmp_path),
        db=SimpleNamespace(execute=lambda *args: None),
    )

    parts = query.split("=", 1)
    req = SimpleNamespace(
        app=app,
        query_params=SimpleNamespace(get=lambda _k, _d=None: (parts[1] if len(parts) > 1 else None)),
    )

    async def run():
        resp = await kline.rebuild_enriched(req)
        # 端点内部 create_task(task()); asyncio.run 只等协程, 这里给后台任务机会完成
        for _ in range(300):
            await asyncio.sleep(0.01)
        return resp

    return asyncio.run(run())


@pytest.fixture
def jobs(tmp_path: Path, monkeypatch):
    """每次用独立的磁盘 store_dir —— 默认 store_dir 指向真实 data/, 会读到历史 job。"""
    fresh = pipeline_jobs.JobStore(store_dir=tmp_path / "job_store")
    monkeypatch.setattr(pipeline_jobs, "job_store", fresh)
    monkeypatch.setattr("app.api.pipeline.job_store", fresh)
    monkeypatch.setattr(pipeline_jobs, "try_acquire_run_slot", lambda *_: True)
    monkeypatch.setattr(pipeline_jobs, "release_run_slot", lambda *_: None)
    monkeypatch.setattr(pipeline_jobs, "run_with_capacity", lambda _j, fn, *a, **k: fn(*a, **k))
    monkeypatch.setattr("app.api.data.invalidate_storage_cache", lambda: None)
    return fresh


def test_plan模式只探测不执行(tmp_path: Path, monkeypatch, jobs):
    """运维想先看差异再决定, 不能一调就开跑。"""
    _mk(tmp_path, "daily", ["2026-01-01", "2026-01-02"])
    _mk(tmp_path, "enriched", ["2026-01-01"])

    body = _request(tmp_path, monkeypatch, "mode=plan")

    assert body["status"] == "planned"
    plan = body["plan"]
    assert plan["mode"] == "forward"
    assert plan["missing_dates"] == ["2026-01-02"]
    assert plan["run_kwargs"] == {"new_dates_only": True, "symbols": None}
    # 关键: 一个 job 都没创建
    assert jobs.list_recent() == []


def test_auto模式把探测结果传给run_pipeline(tmp_path: Path, monkeypatch, jobs):
    """端点必须真的按计划走增量, 而不是表面选了增量内部仍全量。"""
    _mk(tmp_path, "daily", ["2026-01-01", "2026-01-02", "2026-01-03"])
    _mk(tmp_path, "enriched", ["2026-01-01", "2026-01-02"])

    seen: dict = {}
    monkeypatch.setattr(
        "app.indicators.pipeline.run_pipeline",
        lambda **k: (seen.update(k), 1)[1],
    )

    body = _request(tmp_path, monkeypatch)
    assert body["status"] == "started"
    job = jobs.get(body["job_id"])

    assert job["status"] == "succeeded", job.get("error")
    assert seen["new_dates_only"] is True
    assert seen["symbols"] is None
    assert job["result"]["mode"] == "forward"


def test已对齐时判noop不调用run_pipeline(tmp_path: Path, monkeypatch, jobs):
    """已对齐却全量 = 在 720 万行上白跑 49 秒, 这是本改动的主要收益点。"""
    ds = ["2026-01-01", "2026-01-02", "2026-01-03"]
    _mk(tmp_path, "daily", ds)
    _mk(tmp_path, "enriched", ds)

    called: list = []
    monkeypatch.setattr("app.indicators.pipeline.run_pipeline", lambda **k: called.append(k))

    body = _request(tmp_path, monkeypatch)
    job = jobs.get(body["job_id"])

    assert called == [], "已对齐时不应调用 run_pipeline"
    assert job["status"] == "succeeded"
    assert job["result"]["mode"] == "noop"
    assert job["result"]["enriched_rows"] == 0


def test_force_full绕过noop(tmp_path: Path, monkeypatch, jobs):
    """运维明确要全量重写时必须照做。"""
    ds = ["2026-01-01"]
    _mk(tmp_path, "daily", ds)
    _mk(tmp_path, "enriched", ds)

    seen: dict = {}
    monkeypatch.setattr(
        "app.indicators.pipeline.run_pipeline",
        lambda **k: (seen.update(k), 1)[1],
    )

    body = _request(tmp_path, monkeypatch, "mode=full")
    job = jobs.get(body["job_id"])

    assert seen["new_dates_only"] is False
    assert seen["symbols"] is None
    assert job["result"]["mode"] == "full"


def test_job结果附带决策依据(tmp_path: Path, monkeypatch, jobs):
    """前端要解释"为什么这么快", 所以 reason/savings/elapsed 必须进结果。"""
    _mk(tmp_path, "daily", ["2026-01-01"])
    monkeypatch.setattr("app.indicators.pipeline.run_pipeline", lambda **k: 1)

    body = _request(tmp_path, monkeypatch)
    res = jobs.get(body["job_id"])["result"]

    assert res["mode"] == "full"
    assert "首次" in res["reason"]
    assert res["savings"] == "全量重写"
    assert isinstance(res["elapsed_sec"], (int, float))
