"""进程级历史缓存的跨窗口复用测试。

背景
----
策略池里有 13 个不同的 LOOKBACK_DAYS (10/25/.../260)。原实现把 lookback_days
放进缓存 key, 导致每个窗口各算一次 (~3.5s) 且槽位上限 10 会互相逐出 —— 首次
加载策略页要 ~45s, 切页还要重算。

改为按 (asset_type, target_date) 缓存**最大窗口的完整帧**, 命中后按窗口裁剪。
本文件锁定三条不变量:
  1. 不同窗口命中同一份帧, 且返回的窗口与"各自独立计算"完全一致;
  2. 帧比请求窗口短时必须丢弃重算, 不能静默少给数据;
  3. 裁剪必须按**目标日及之前**的交易日计数 (与 test_enriched_history_past_target 同源)。
"""
from __future__ import annotations

import math
import time
from datetime import date, timedelta

import polars as pl
import pytest

from app.services import screener as screener_mod
from app.services.screener import ScreenerService, _trim_history
from app.tickflow.repository import DataStore, KlineRepository


def _trading_days(n: int = 320) -> list[date]:
    days: list[date] = []
    d = date(2024, 1, 1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def _frame(days: list[date]) -> pl.DataFrame:
    rows = []
    for symbol, phase in (("600001.SH", 0.0), ("000002.SZ", 1.0)):
        for i, day in enumerate(days):
            rows.append((symbol, day, round(10 + math.sin(i * 0.2 + phase), 2)))
    return pl.DataFrame(rows, schema=["symbol", "date", "close"], orient="row").sort(["symbol", "date"])


def _repo(tmp_path, cache: pl.DataFrame | None) -> KlineRepository:
    repo = KlineRepository(DataStore(tmp_path))
    repo._enriched_history_cache = cache  # None -> 强制走进程级缓存路径
    return repo


@pytest.fixture(autouse=True)
def _clean_process_cache():
    """每个用例前清空进程级缓存, 避免用例间串味。"""
    ScreenerService.clear_history_cache()
    yield
    ScreenerService.clear_history_cache()


# ── _trim_history 的纯函数行为 ────────────────────────────────────────────

@pytest.mark.parametrize("lookback", [5, 20, 60])
def test_trim_matches_independent_window(lookback):
    """裁剪结果必须等于"只喂该窗口"时的结果 (窗口语义一致)。"""
    days = _trading_days(200)
    frame = _frame(days)
    target = days[-1]

    trimmed = _trim_history(frame, target, lookback)
    assert trimmed is not None
    want = [d for d in days if d <= target][-(lookback + 1):]
    assert trimmed["date"].unique().sort().to_list() == want


def test_trim_none_when_frame_too_short():
    """帧不足时必须返回 None, 让调用方重算 —— 静默返回短窗口会少给数据。"""
    days = _trading_days(30)
    frame = _frame(days)

    assert _trim_history(frame, days[-1], 60) is None


def test_trim_ignores_days_after_target():
    """晚于目标日的交易日不得计入窗口 (选股页可选历史日期)。"""
    days = _trading_days(200)
    frame = _frame(days)
    target = days[-40]

    trimmed = _trim_history(frame, target, 20)
    assert trimmed is not None
    assert trimmed["date"].max() <= target
    assert trimmed["date"].n_unique() == 21


# ── 缓存跨窗口复用 ────────────────────────────────────────────────────────

def test_different_windows_share_one_cached_frame(tmp_path):
    """同一目标日的多个窗口: 只应产生一份缓存帧, 且各窗口结果正确。"""
    days = _trading_days(320)
    repo = _repo(tmp_path, None)
    svc = ScreenerService(repo)
    target = days[-1]

    # 预置一份"最大帧"到进程缓存 (模拟慢路径已算过)
    cache_key = (svc.asset_type, target)
    screener_mod._history_cache[cache_key] = (
        time.monotonic(),
        _frame(days[-300:]),
    )

    for lookback in (10, 25, 60, 120, 260):
        out = svc._load_enriched_history(target, lookback)
        assert not out.is_empty(), f"lookback={lookback} 不应为空"
        assert out["date"].n_unique() == lookback + 1, f"lookback={lookback} 窗口长度不对"

    # 关键: 只应有一份帧, 没有为每个窗口各存一份
    assert len(screener_mod._history_cache) == 1, "不同窗口不应各占一个缓存槽"


def test_short_cached_frame_is_discarded_not_served(tmp_path):
    """缓存帧比请求窗口短时, 必须丢弃而不是返回短帧。"""
    days = _trading_days(320)
    repo = _repo(tmp_path, None)
    svc = ScreenerService(repo)
    target = days[-1]

    # 只缓存 30 个交易日, 但请求 120 —— 不够用
    screener_mod._history_cache[(svc.asset_type, target)] = (
        time.monotonic(),
        _frame(days[-30:]),
    )

    # 该帧不足以服务 120; 函数应丢弃它 (随后走慢路径, 这里数据目录为空会返回空帧)
    out = svc._load_enriched_history(target, 120)
    # 关键断言: 不能返回那个短帧里的 30 天窗口
    if not out.is_empty():
        assert out["date"].n_unique() != 30, "不得把 30 天的短帧当成 120 天窗口下发"


def test_trim_consistent_with_repo_cache_path(tmp_path):
    """repo 缓存命中与进程缓存命中, 同一 as_of+窗口 结果必须一致。"""
    days = _trading_days(200)
    target = days[-60]
    lookback = 20

    # a: 走 repo 级缓存
    full = _frame(days)
    repo_a = _repo(tmp_path / "a", full)
    out_a = ScreenerService(repo_a)._load_enriched_history(target, lookback)

    # b: 走进程级缓存 (repo 缓存关掉)
    repo_b = _repo(tmp_path / "b", None)
    screener_mod._history_cache[("stock", target)] = (
        time.monotonic(),
        _frame([d for d in days if d <= target]),
    )
    out_b = ScreenerService(repo_b)._load_enriched_history(target, lookback)

    assert out_a["date"].unique().sort().to_list() == out_b["date"].unique().sort().to_list()
