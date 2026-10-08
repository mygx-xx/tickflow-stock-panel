"""_write_lock 锁区间瘦身的回归测试。

背景: polars 并发执行存在死锁风险 (app.polars_guard), 若 polars 读/合并/排序
悬死在 _write_lock 内, 全局写锁被永久持有 → 所有写路径排队冻结 (2026-09-07
线上全站冻结事故的放大器)。乐观并发模式把重活移到锁外, 此处验证:
- 并发 upsert 不丢行 (乐观重试的正确性);
- 合并计算期间 _write_lock 可被其他线程获取 (重活确实不在锁内);
- 乐观校验用的基底指纹与基底内容同源 (否则重试判据失效, 见文末用例)。

同一套纪律现在收在 fs_utils.optimistic_partition_write, 除日分区外还覆盖
kline_sync 的分钟分区与 adj_factor 合并 (盘后同步 / 盘中全量分钟刷新 / 个股补齐
会并发写同一文件), 下半部分的用例验证这三条通路。
"""
from __future__ import annotations

import threading
from datetime import date, datetime
from pathlib import Path

import polars as pl

from app.services import fs_utils, kline_sync
from app.services.fs_utils import atomic_write_parquet
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


# ---------- 分钟分区 / adj_factor: 同一纪律的另外两条写通路 ----------


def _minute_frame(symbols: list[str], ts: datetime = datetime(2026, 9, 7, 9, 31)) -> pl.DataFrame:
    n = len(symbols)
    return pl.DataFrame({
        "symbol": symbols,
        "datetime": [ts] * n,
        "close": [10.0 + i for i in range(n)],
    })


def _adj_frame(symbol: str, factor: float, td: date = date(2026, 6, 26)) -> pl.DataFrame:
    return pl.DataFrame([{"symbol": symbol, "trade_date": td, "ex_factor": factor}])


def test_staging_files_are_writer_exclusive(tmp_path: Path, monkeypatch) -> None:
    """每次落盘用自己的暂存名 —— 固定 `<name>.tmp` 是并发写者共享的。

    A 写完暂存、B 再把半截写进同一个名字、A 执行 replace → 发布出去的是 B 的字节,
    正式分区当场变成半截 parquet (之后 scan_parquet 整条链路报错)。改前实测: 8 个线程
    并发写同一分钟分区 × 15 轮, 120 次写入只有 15 次落到最终文件, 并出现 64 次
    `FileNotFoundError: ...part.parquet.tmp` (自己的暂存文件被对方 replace 走)。
    """
    seen: list[Path] = []
    real_write = pl.DataFrame.write_parquet

    def spy(self, file, *args, **kwargs):
        seen.append(Path(file))
        return real_write(self, file, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", spy)
    target = tmp_path / "part.parquet"

    atomic_write_parquet(pl.DataFrame({"v": [1]}), target)
    atomic_write_parquet(pl.DataFrame({"v": [2]}), target)

    assert len(seen) == 2
    assert seen[0] != seen[1], f"两次写入共用暂存名: {seen}"
    assert pl.read_parquet(target)["v"].to_list() == [2]
    assert [p.name for p in tmp_path.iterdir()] == ["part.parquet"]  # 不留孤儿暂存


def test_minute_partition_injected_race_keeps_late_writer(tmp_path: Path, monkeypatch) -> None:
    """盘后同步与盘中全量分钟刷新会同时写当日分钟分区, 插队的一方不能被子抹掉。

    读旧与合并跑在锁外, 对手可能正好落在「基底快照已读、锁内校验还没跑」之间。
    锁内的指纹校验必须发现文件已换版并重读重算, 否则本次发布把对方刚写入的 symbol
    整片覆盖 (丢分钟 K → 分时图缺柱、分钟策略漏信号)。这里用一次同步注入把交错
    确定化, 不靠线程时序碰运气。

    改前实测 (8 个线程各写 1 个 symbol 进同一分区 × 15 轮, HEAD d9234f1): 15/15 轮
    丢行、合计丢 105/120 次写入, 另有 64 次 FileNotFoundError 打在共享的
    `part.parquet.tmp` 上; 接入 write_lock + 独占暂存后同一压测 0 丢行 0 异常。
    """
    lock = threading.Lock()
    minute_dir = tmp_path / "kline_minute"
    kline_sync._write_minute_partition(
        _minute_frame(["000001.SZ"]), minute_dir, write_lock=lock
    )

    real_snapshot = fs_utils.read_parquet_snapshot
    injected: list[int] = []

    def racing_snapshot(path: Path):
        snap = real_snapshot(path)
        if not injected:
            injected.append(1)
            kline_sync._write_minute_partition(
                _minute_frame(["000002.SZ"]), minute_dir, write_lock=lock
            )
        return snap

    monkeypatch.setattr(fs_utils, "read_parquet_snapshot", racing_snapshot)
    kline_sync._write_minute_partition(
        _minute_frame(["000003.SZ"]), minute_dir, write_lock=lock
    )

    final = pl.read_parquet(minute_dir / "date=2026-09-07" / "part.parquet")
    assert final["symbol"].to_list() == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert injected, "注入未生效, 用例没真正跑到并发窗口"


def test_adj_factor_merge_injected_race_keeps_late_event(tmp_path: Path, monkeypatch) -> None:
    """adj_factor 的 all.parquet 是全市场单文件: 插队写入的除权事件不能被抹掉。

    丢一条除权事件的后果不是报错而是静默算错 —— 该标的之后的复权价按漏掉的因子算。
    改前实测 (同 8×15 压测): 15/15 轮丢事件、合计丢 105/120 条, 61 次暂存文件被对手
    搬走后的 FileNotFoundError。
    """
    lock = threading.Lock()
    out = tmp_path / "adj_factor" / "all.parquet"
    assert kline_sync._merge_adj_factor_store(out, _adj_frame("600000.SH", 1.5), write_lock=lock) == (1, ["600000.SH"])

    real_snapshot = fs_utils.read_parquet_snapshot
    injected: list[int] = []

    def racing_snapshot(path: Path):
        snap = real_snapshot(path)
        if not injected:
            injected.append(1)
            kline_sync._merge_adj_factor_store(
                out, _adj_frame("000001.SZ", 1.2), write_lock=lock
            )
        return snap

    monkeypatch.setattr(fs_utils, "read_parquet_snapshot", racing_snapshot)
    added, changed = kline_sync._merge_adj_factor_store(
        out, _adj_frame("600036.SH", 1.8), write_lock=lock
    )

    assert added == 1 and changed == ["600036.SH"]
    final = pl.read_parquet(out)
    assert sorted(final["symbol"].to_list()) == ["000001.SZ", "600000.SH", "600036.SH"]
    assert injected


def test_minute_merge_runs_outside_write_lock(tmp_path: Path, monkeypatch) -> None:
    """分钟分区的读旧+合并在 _write_lock 外跑: 锁不能用来兜住整段 polars 计算。

    全市场当日分钟分区实测 16 MB / 132.5 万行, 一次读-改-写 27.7 ms; 让它在锁内跑,
    polars 一旦悬死就把全局写锁永久占住 (2026-09-07 全站冻结的形态)。
    """
    lock = threading.Lock()
    minute_dir = tmp_path / "kline_minute"
    kline_sync._write_minute_partition(_minute_frame(["000001.SZ"]), minute_dir, write_lock=lock)

    merge_entered = threading.Event()
    done = threading.Event()
    original_concat = pl.concat

    def slow_concat(*args, **kwargs):
        merge_entered.set()
        import time

        time.sleep(0.4)  # 模拟重合并耗时; 期间锁必须空闲
        return original_concat(*args, **kwargs)

    def write() -> None:
        kline_sync._write_minute_partition(_minute_frame(["000002.SZ"]), minute_dir, write_lock=lock)
        done.set()

    monkeypatch.setattr(pl, "concat", slow_concat)
    t = threading.Thread(target=write, daemon=True)
    t.start()
    acquired = False
    try:
        assert merge_entered.wait(timeout=5), "合并路径未被触发"
        acquired = lock.acquire(timeout=1.0)
        assert acquired, "合并计算期间写锁被占用 — 重活仍在锁内"
    finally:
        if acquired:
            lock.release()  # 先交还: 写线程只剩锁内这一步
        monkeypatch.undo()
    assert done.wait(timeout=5)
    t.join(timeout=5)

    final = pl.read_parquet(minute_dir / "date=2026-09-07" / "part.parquet")
    assert final["symbol"].to_list() == ["000001.SZ", "000002.SZ"]
