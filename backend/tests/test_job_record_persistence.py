"""回归测试: job 记录跨进程死亡持久化(「数据在、记录丢」补丁)。

背景(用户反馈): 全市场同步 12:11~12:42 成功结束后 0.7s, uvicorn --reload
检测到代码变更杀死 worker, 恰好落在管道完成与 job_store.succeed() 落盘之间
—— 数据已写盘但同步历史无任何记录。旧实现 pending/running 仅存内存、终态才
落盘, 存在整段丢失窗口。

修复后契约:
  - create()/start() 即落盘 pending/running 快照;
  - 下次进程启动(= 新 JobStore 实例, 同目录)把遗留的 pending/running
    孤儿记录补标为 failed(中断), finished_at 取文件 mtime;
  - 终态记录不受补录影响; 终态写入覆盖 running 快照(同一文件)。
均为纯逻辑, 不触网。
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from app.services.pipeline_jobs import JobStore


def _read_disk(d, jid: str) -> dict:
    return json.loads((d / f"{jid}.json").read_text("utf-8"))


# ── 创建/启动即落盘 ──────────────────────────────────────────────────────

def test_create_writes_pending_snapshot_to_disk(tmp_path):
    d = tmp_path / "jobs"
    store = JobStore(store_dir=d)
    jid, _ = store.create(timeout_s=60)

    disk = _read_disk(d, jid)
    assert disk["status"] == "pending"
    assert disk["stage"] == "init"


def test_start_updates_disk_snapshot_to_running(tmp_path):
    d = tmp_path / "jobs"
    store = JobStore(store_dir=d)
    jid, _ = store.create(timeout_s=60)
    store.start(jid)

    disk = _read_disk(d, jid)
    assert disk["status"] == "running"
    assert disk["started_at"] is not None


# ── 进程死亡 → 下次启动补录 ──────────────────────────────────────────────

def test_orphan_running_record_is_reaped_on_next_boot(tmp_path):
    """核心场景: 进程死在 running(甚至工作已做完但未终态), 记录必须可见。"""
    d = tmp_path / "jobs"
    dead = JobStore(store_dir=d)
    jid, _ = dead.create(timeout_s=60)
    dead.start(jid)
    dead.progress(jid, "sync", 50, "halfway")  # 进度只更新内存

    # 新进程 = 同目录新实例(内存为空, 只有磁盘)
    revived = JobStore(store_dir=d)
    j = revived.get(jid)
    assert j is not None
    assert j["status"] == "failed"
    assert "中断" in j["error"]
    assert j["finished_at"] is not None
    # finished_at 基于文件 mtime(≈ start 时刻), 时长不得虚增为负或巨大
    assert j["duration_s"] is not None
    assert 0 <= j["duration_s"] <= 60
    # 同步历史列表可见
    assert any(x["id"] == jid for x in revived.list_recent())


def test_orphan_pending_record_is_reaped(tmp_path):
    """进程死在 create() 与 start() 之间: 记录同样可见, 时长为 None。"""
    d = tmp_path / "jobs"
    dead = JobStore(store_dir=d)
    jid, _ = dead.create(timeout_s=60)
    # 未 start 即死亡

    revived = JobStore(store_dir=d)
    j = revived.get(jid)
    assert j["status"] == "failed"
    assert j["duration_s"] is None


def test_reap_does_not_touch_terminal_records(tmp_path):
    d = tmp_path / "jobs"
    store = JobStore(store_dir=d)
    jid, _ = store.create(timeout_s=60)
    store.start(jid)
    store.succeed(jid, {"daily_rows": 100})

    revived = JobStore(store_dir=d)
    j = revived.get(jid)
    assert j["status"] == "succeeded"
    assert j["result"] == {"daily_rows": 100}


def test_reap_allows_new_job_after_dead_orphan(tmp_path):
    """补录后旧 job 已 failed: 新进程 create() 不被死孤儿阻塞(单飞只看内存)。"""
    d = tmp_path / "jobs"
    dead = JobStore(store_dir=d)
    old_jid, _ = dead.create(timeout_s=60)
    dead.start(old_jid)

    revived = JobStore(store_dir=d)
    new_jid, is_new = revived.create(timeout_s=60)
    assert is_new is True
    assert new_jid != old_jid


# ── 终态覆盖快照 ─────────────────────────────────────────────────────────

def test_terminal_write_replaces_running_snapshot(tmp_path):
    d = tmp_path / "jobs"
    store = JobStore(store_dir=d)
    jid, _ = store.create(timeout_s=60)
    store.start(jid)
    store.fail(jid, "boom")

    files = list(d.glob("*.json"))
    assert len(files) == 1
    disk = _read_disk(d, jid)
    assert disk["status"] == "failed"
    assert disk["error"] == "boom"


# ── 落盘原子性(并发读者不得看到半截 JSON) ──────────────────────────────

def test_terminal_write_swaps_file_atomically(tmp_path, monkeypatch):
    """终态落盘必须经「临时文件 + os.replace」原子换入, 不得原地 truncate。

    旧实现用 ``path.write_text``: 先截断再写, 并发读者(轮询线程里的
    ``_read_file``)会读到半截 JSON → 解析失败 → 返回 None, 与「任务不存在」
    无法区分。CI(2 核 runner)上 ``test_api_jobs_wait_before_computing`` 即因此
    报 ``'NoneType' object is not subscriptable`` —— 本机 22 核的轮询间隔极少
    落进这个亚毫秒窗口, 所以长期只红在 CI。

    这里的判定不靠"压概率": 直接把 ``os.replace`` 换成探针, 断言
    (a) 终态确实经 rename 换入, (b) rename 发生的瞬间目标文件仍是**完整的**
    旧快照(说明此前从未被 truncate), (c) 临时文件不残留且不被
    ``glob("*.json")`` 扫到。
    """
    d = tmp_path / "jobs"
    store = JobStore(store_dir=d)
    jid, _ = store.create(timeout_s=60)
    store.start(jid)
    dst = d / f"{jid}.json"
    assert json.loads(dst.read_text("utf-8"))["status"] == "running"

    swaps = []
    real_replace = os.replace

    def spy_replace(src, target):
        # rename 之前, 目标必须仍是完整的旧快照 —— 原地写的话这里早已是空壳
        assert json.loads(Path(target).read_text("utf-8"))["status"] == "running"
        swaps.append((Path(src), Path(target)))
        return real_replace(src, target)

    monkeypatch.setattr(os, "replace", spy_replace)
    store.succeed(jid, {"rows": 3})

    assert swaps, "终态必须经 os.replace 原子换入(原地写会让读者读到半截文件)"
    assert swaps[-1][1] == dst
    assert json.loads(dst.read_text("utf-8"))["status"] == "succeeded"
    # 临时文件不得残留, 也不得混进 job 列表/清理逻辑的 glob("*.json")
    assert not list(d.glob("*.tmp.*")), "临时文件残留"
    assert [f.name for f in d.glob("*.json")] == [f"{jid}.json"]
