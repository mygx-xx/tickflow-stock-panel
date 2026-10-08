"""文件系统小工具 — 原子写、分区 parquet 的乐观并发读写等。

历史遗留: json_report_store / strategy_cache / kline_sync 等模块里各有一份内联的
同款原子写。新代码统一用本模块的 atomic_write_text / atomic_write_parquet,
一处实现一处维护。

parquet 分区同时有读侧 (polars scan / DuckDB 视图) 和其他写入者, 所以本模块把
「怎么写才不会把分区写成半截 / 不会覆盖掉对手刚落盘的行」收成三个原语:
  parquet_fingerprint      — 版本指纹 (mtime_ns, size)
  read_parquet_snapshot    — 基底内容 + *同一版* 的指纹 (一次 open 内取)
  optimistic_partition_write — 读旧/合并在锁外, 锁内只做版本校验 + 原子替换
"""
from __future__ import annotations

import contextlib
import io
import os
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # 只做类型标注; JSON 原子写的调用方 (preferences/secrets) 不必加载 polars
    import polars as pl


def atomic_write_text(path: Path, text: str, *, mode: int | None = None) -> None:
    """临时文件 + os.replace 原子替换, 避免读侧读到半截 JSON。

    `mode` 在替换*之前*打到临时文件上。凭证类文件如果先落盘再 chmod, 中间有一段
    以默认权限存在的窗口; 先改临时文件就没有这个窗口。Windows 上 chmod 只影响
    只读位, 失败不该让写入失败, 所以吞掉 OSError, 与原调用处的处理一致。
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    if mode is not None:
        try:
            os.chmod(tmp, mode)
        except OSError:
            pass
    os.replace(tmp, path)


def _staging_path(path: Path) -> Path:
    """本次写入独占的暂存文件名 (与 enriched_generation / backtest 同一约定)。

    固定名 `<name>.tmp` 在两个写入者同时落同一分区时会互相覆盖: A 写完暂存、B 又把
    自己的半截写进同一个名字, A 的 replace 就把 B 的半截文件搬成正式分区。改前实测 8 个
    线程并发写同一分钟分区 × 15 轮: 120 次写入只有 15 次留在最终文件里, 另有 64 次
    暂存文件被对方 replace 走后剩 FileNotFoundError。唯一名后同一压测 0 丢行 0 异常。
    """
    return path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")


def atomic_write_parquet(df: pl.DataFrame, path: Path) -> None:
    """parquet 版原子写: 先写独占暂存文件再替换, 与 repository / kline_sync 的
    `_atomic_write_parquet` 同语义。

    直接 `df.write_parquet(path)` 在进程被 kill (dev.sh 清端口用 kill -9)、断电或
    磁盘写满时会留下半截文件, 之后读侧 `read_parquet` / `scan_parquet` 整条报错。
    暂存名带随机后缀且以 `.tmp` 结尾, 既不匹配 `*.parquet` glob 不会被视图误读,
    也不会和并发写入者的暂存文件撞名 (见 _staging_path)。Windows 下目标正被并发
    读取时由 `replace_with_retry` 短退避穿过; 替换没成功就清掉自己的暂存文件,
    不留孤儿。
    """
    from app.tickflow.repository import replace_with_retry  # 惰性导入, 避免模块级环

    tmp = _staging_path(path)
    try:
        df.write_parquet(tmp)
        replace_with_retry(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):  # Windows 上暂存文件可能正被自己持有
            tmp.unlink(missing_ok=True)
        raise


def parquet_fingerprint(path: Path) -> tuple[int, int] | None:
    """分区文件的版本指纹 (mtime_ns, size); 不存在返回 None。"""
    try:
        st = path.stat()
    except FileNotFoundError:
        return None
    return (st.st_mtime_ns, st.st_size)


def read_parquet_snapshot(path: Path) -> tuple[pl.DataFrame, tuple[int, int] | None]:
    """基底数据帧 + *同一版* 的版本指纹, 二者在同一次 open 内取。

    分成「按路径读内容」+「再 stat 取指纹」两步会开出并发窗口:
    - 期间另一路写入者完成替换后, 指纹描述新版而合并用的还是旧版, 锁内比对照样
      通过 → 对方刚落盘的行被静默覆盖 (丢行)。
    - polars 按活路径读要分多次取 (footer → 列块), 替换落在中间就混读两版的元数据
      与页, 直接抛 `ComputeError: parquet: File out of specification`, 还会 panic 掉
      polars 线程池的工作线程 (Windows 实测: 6142 次读 11 次命中; 分钟分区并发下
      首轮 4/4 写者全部 `PanicException: range end index 124 out of range for slice of length 0`)。

    一次 open + fstat + 全量 read 把该版文件钉成不可变快照, polars 之后只解析内存
    缓冲, 不存在第三种状态; 句柄只在顺序读期间持有, 不比现状多阻塞替换。
    内存代价实测: 16 MB / 132.5 万行的全市场分钟分区, 快照比按路径读多 ~16 MB,
    而它自己的合并步骤峰值就有 ~104 MB, 所以复制一份原始字节不是瓶颈。
    """
    import polars as pl  # 惰性: JSON 原子写的调用方不必加载 polars

    try:
        with path.open("rb") as fh:
            st = os.fstat(fh.fileno())
            raw = fh.read()
    except FileNotFoundError:
        return pl.DataFrame(), None
    return pl.read_parquet(io.BytesIO(raw)), (st.st_mtime_ns, st.st_size)


def optimistic_partition_write(
    path: Path,
    build: Callable[[pl.DataFrame], pl.DataFrame],
    write_lock: threading.Lock | None,
    *,
    retries: int = 3,
) -> pl.DataFrame:
    """分区 merge-upsert 的乐观并发执行: polars 的读与算在锁外, 锁内只做版本校验 +
    原子替换。返回真正落盘的那一版数据帧。

    为什么不能整段持锁: polars 并发执行有死锁风险 (见 app.polars_guard), 重活悬在
    全局写锁里会把所有写路径冻住 (2026-09-07 线上全站冻结事故的放大形态)。全市场
    分钟分区实测 16 MB / 132.5 万行, 一次读-改-写 27.7 ms, 让它在锁内跑就是拿
    全局写锁赌 polars 不悬死。
    为什么必须校验版本: 锁外的基底可能已被对手写过 —— 不校验就直接落盘会把对方
    刚落盘的行覆盖掉。校验成立的前提是「指纹与基底同源」, 由 read_parquet_snapshot
    保证。

    `build` 在乐观重试下会被调用多次, 必须是纯函数 (只读 existing、不累加外部状态);
    需要从中取回的量 (变化 symbol 等) 每次整体覆盖写, 以最后一次为准。

    `write_lock=None` 表示没有并发对手 (单线程调用/测试), 直接读-改-写一次。
    重试耗尽 (高频并发写同一分区, 罕见) 退回锁内全量模式: 正确性优先, 牺牲隔离性。
    """
    if write_lock is None:
        existing, _fp = read_parquet_snapshot(path)
        merged = build(existing)
        atomic_write_parquet(merged, path)
        return merged
    for _attempt in range(retries):
        existing, base_fp = read_parquet_snapshot(path)
        merged = build(existing)
        with write_lock:
            if parquet_fingerprint(path) != base_fp:
                continue  # 基底被并发写入者改过, 出锁重读重算
            atomic_write_parquet(merged, path)
            return merged
    with write_lock:
        existing, _fp = read_parquet_snapshot(path)
        merged = build(existing)
        atomic_write_parquet(merged, path)
        return merged
