"""_write_lock 锁区间瘦身的回归测试。

背景: polars 并发执行存在死锁风险 (app.polars_guard), 若 polars 读/合并/排序
悬死在 _write_lock 内, 全局写锁被永久持有 → 所有写路径排队冻结 (2026-09-07
线上全站冻结事故的放大器)。乐观并发模式把重活移到锁外, 此处验证:
- 并发 upsert 不丢行 (乐观重试的正确性);
- 合并计算期间 _write_lock 可被其他线程获取 (重活确实不在锁内);
- 乐观校验用的基底指纹与基底内容同源 (否则重试判据失效, 见文末用例)。
"""
from __future__ import annotations

import threading
from datetime import date
from pathlib import Path

import polars as pl

from app.tickflow.repository import DataStore, KlineRepository


def _frame(symbols: list[str], dt: date = date(2026, 9, 7)) -> pl.DataFrame:
    n = len(symbols)
    return pl.DataFrame({
        "symbol": symbols,
        "date": [dt] * n,
        "close": [10.0 + i for i in range(n)],
    })


def test_concurrent_upserts_do_not_lose_rows(tmp_path: Path) -> None:
    repo = KlineRepository(DataStore(tmp_path))

    groups = [[f"{i:03d}{j:04d}.SZ" for j in range(8)] for i in range(6)]
    errors: list[BaseException] = []

    def worker(symbols: list[str]) -> None:
        try:
            for _ in range(3):  # 每线程多轮写, 提高乐观重试路径命中
                repo.merge_live_daily_asset("stock", _frame(symbols))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(g,), daemon=True) for g in groups]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not errors, errors
    assert all(not t.is_alive() for t in threads)

    out = tmp_path / "kline_daily" / "date=2026-09-07" / "part.parquet"
    final = pl.read_parquet(out)
    assert final["symbol"].n_unique() == sum(len(g) for g in groups)  # 无一丢失
    assert final["symbol"].to_list() == sorted(final["symbol"].to_list())


def test_heavy_merge_runs_outside_write_lock(tmp_path: Path, monkeypatch) -> None:
    repo = KlineRepository(DataStore(tmp_path))
    # 预置旧分区内容, 让 upsert 走「读旧 + concat 合并」路径。
    repo.merge_live_daily_asset("stock", _frame(["000001.SZ"]))

    merge_entered = threading.Event()
    original_concat = pl.concat

    def slow_concat(*args, **kwargs):
        merge_entered.set()
        import time

        time.sleep(0.4)  # 模拟重合并耗时; 期间 _write_lock 必须是空闲的
        return original_concat(*args, **kwargs)

    monkeypatch.setattr(pl, "concat", slow_concat)

    done = threading.Event()

    def upsert() -> None:
        repo.merge_live_daily_asset("stock", _frame(["000002.SZ"]))
        done.set()

    t = threading.Thread(target=upsert, daemon=True)
    t.start()
    assert merge_entered.wait(timeout=5), "合并路径未被触发"

    acquired = repo._write_lock.acquire(timeout=1.0)
    assert acquired, "合并计算期间 _write_lock 被占用 — 重活仍在锁内"
    repo._write_lock.release()

    assert done.wait(timeout=5)
    t.join(timeout=5)
    out = tmp_path / "kline_daily" / "date=2026-09-07" / "part.parquet"
    assert pl.read_parquet(out)["symbol"].to_list() == ["000001.SZ", "000002.SZ"]


def test_optimistic_base_fingerprint_describes_the_base_it_merged(tmp_path: Path, monkeypatch) -> None:
    """乐观校验的指纹必须描述它实际合并的那一版基底, 否则重试判据形同虚设。

    复现: 基底是"先按路径读内容, 再 stat 取指纹"两步。若另一路写入者在这两步之间
    完成落盘, 指纹描述的是新文件而内容还是旧文件 —— 锁内比对必然通过, 对方刚写入的
    行被静默覆盖。这里用一次同步注入把窗口确定化 (不靠线程时序碰运气)。

    同一时序窗口也是套件里 test_concurrent_upserts_do_not_lose_rows 偶发
    `parquet: File out of specification: The page header reported the wrong page
    size` 的根: polars 按活路径分多次取 (footer → 列块), 落在中间的替换会让它把
    两版的元数据与页混读, 除了丢行还会直接抛错/panic。修法是把基底与指纹收进同
    一次 open 的不可变快照。
    """
    repo = KlineRepository(DataStore(tmp_path))
    repo.merge_live_daily_asset("stock", _frame(["000001.SZ"]))

    original_read = pl.read_parquet
    calls: list[object] = []

    def racing_read(source, *args, **kwargs):
        df = original_read(source, *args, **kwargs)
        calls.append(source)
        if len(calls) == 1:  # 对手写入者恰好在这一步之后发布新版
            repo.merge_live_daily_asset("stock", _frame(["000002.SZ"]))
        return df

    monkeypatch.setattr(pl, "read_parquet", racing_read)
    repo.merge_live_daily_asset("stock", _frame(["000003.SZ"]))

    out = tmp_path / "kline_daily" / "date=2026-09-07" / "part.parquet"
    assert original_read(out)["symbol"].to_list() == ["000001.SZ", "000002.SZ", "000003.SZ"]
