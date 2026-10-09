from __future__ import annotations

import json
import os

from app.services import strategy_cache


def _result(*symbols: str) -> dict:
    return {
        "total": len(symbols),
        "as_of": "2026-07-20",
        "rows": [{"symbol": symbol, "close": index + 1.0} for index, symbol in enumerate(symbols)],
    }


def test_same_day_partial_writes_merge_strategy_results(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_b": _result("600000.SH")})

    cached = strategy_cache.read_cache(tmp_path)

    assert set(cached["results"]) == {"strategy_a", "strategy_b"}
    assert cached["results"]["strategy_a"]["rows"][0]["symbol"] == "000001.SZ"
    assert cached["results"]["strategy_b"]["rows"][0]["symbol"] == "600000.SH"


def test_same_day_update_replaces_only_target_strategy_and_keeps_ever_rows(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {
        "strategy_a": _result("000001.SZ"),
        "strategy_b": _result("600000.SH"),
    })
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000002.SZ")})

    cached = strategy_cache.read_cache(tmp_path)

    assert [row["symbol"] for row in cached["results"]["strategy_a"]["rows"]] == ["000002.SZ"]
    assert [row["symbol"] for row in cached["results"]["strategy_b"]["rows"]] == ["600000.SH"]
    assert set(cached["today_ever_rows"]["strategy_a"]) == {"000001.SZ", "000002.SZ"}


def test_new_date_resets_results_and_ever_rows(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    next_day = _result("600000.SH")
    next_day["as_of"] = "2026-07-21"

    strategy_cache.write_cache(tmp_path, "2026-07-21", {"strategy_b": next_day})
    cached = strategy_cache.read_cache(tmp_path)

    assert cached["as_of"] == "2026-07-21"
    assert set(cached["results"]) == {"strategy_b"}
    assert set(cached["today_ever_rows"]) == {"strategy_b"}


def test_失败原因同日按策略累积_重跑成功即撤销(tmp_path):
    """渐进式逐策略写缓存: 失败原因要留住给卡片显示, 但重跑修好后不能继续挂错。"""
    strategy_cache.write_cache(tmp_path, "2026-07-20", {}, {"strategy_a": '缺少列 "pb_latest"'})
    strategy_cache.write_cache(
        tmp_path, "2026-07-20", {"strategy_b": _result("600000.SH")}, {"strategy_c": "类型错误"}
    )

    cached = strategy_cache.read_cache(tmp_path)
    assert cached["errors"] == {"strategy_a": '缺少列 "pb_latest"', "strategy_c": "类型错误"}
    # 失败写入不动其它策略结果
    assert set(cached["results"]) == {"strategy_b"}

    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})

    assert strategy_cache.read_cache(tmp_path)["errors"] == {"strategy_c": "类型错误"}


def test_换日重置失败原因(tmp_path):
    strategy_cache.write_cache(
        tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")}, {"strategy_b": "缺少列"}
    )
    next_day = _result("600000.SH")
    next_day["as_of"] = "2026-07-21"

    strategy_cache.write_cache(tmp_path, "2026-07-21", {"strategy_a": next_day})

    assert strategy_cache.read_cache(tmp_path)["errors"] == {}


# ── 进程内 memo: 16MB 缓存整体 json.loads 一次实测 218ms, 策略页首屏与 2s 轮询
# 都要重付, 所以 (mtime_ns, size) 命中即复用。以下用例钉 memo 的四条边界。
def test_memo_冷读只回盘一次(tmp_path, monkeypatch):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    strategy_cache._memo.clear()

    seen: list = []
    real = strategy_cache._load_from_disk
    monkeypatch.setattr(
        strategy_cache, "_load_from_disk", lambda p: (seen.append(p), real(p))[1]
    )

    first = strategy_cache.read_cache(tmp_path)
    second = strategy_cache.read_cache(tmp_path)

    assert len(seen) == 1
    assert second is first


def test_memo_写入后直接刷新_读不回盘(tmp_path, monkeypatch):
    """写侧在锁内已知本次内容, 落盘即刷 memo —— 同一天连续增量写不再回盘解析。"""
    seen: list = []
    real = strategy_cache._load_from_disk
    monkeypatch.setattr(
        strategy_cache, "_load_from_disk", lambda p: (seen.append(p), real(p))[1]
    )

    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_b": _result("600000.SH")})

    cached = strategy_cache.read_cache(tmp_path)

    assert seen == [], "写入后 memo 应直接命中, 不该回盘"
    assert cached["results"]["strategy_b"]["rows"][0]["symbol"] == "600000.SH"


def test_外部改写缓存后_memo_按_stat_失效(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    path = strategy_cache._cache_path(tmp_path)
    foreign = {"as_of": "2026-07-19", "results": {"strategy_z": _result("300001.SZ")}}
    path.write_text(json.dumps(foreign, ensure_ascii=False), encoding="utf-8")

    cached = strategy_cache.read_cache(tmp_path)

    assert cached["as_of"] == "2026-07-19"
    assert set(cached["results"]) == {"strategy_z"}


def test_同刻同尺寸连续写_memo_仍反映最后一笔(tmp_path):
    """Windows 时钟粒度粗, 连续写可能撞出同一个 mtime。

    只靠 (mtime, size) 判据在此刻会返回旧内容, 所以写侧落盘后直接刷新 memo;
    这里把两笔的 mtime 钉成同一定宽值, 让 stat 完全无法区分。
    """
    pinned = (1_700_000_000_000_000_000, 1_700_000_000_000_000_000)
    path = strategy_cache._cache_path(tmp_path)

    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    os.utime(path, ns=pinned)
    size_before = path.stat().st_size

    # 换日重置 + 等长键/等长代码/等长日期 → 文件字节数与上一笔完全相同
    next_day = _result("600000.SH")
    next_day["as_of"] = "2026-07-21"
    strategy_cache.write_cache(tmp_path, "2026-07-21", {"strategy_b": next_day})
    os.utime(path, ns=pinned)

    assert path.stat().st_size == size_before, "前提失效: 两笔尺寸不同, stat 就能区分"

    cached = strategy_cache.read_cache(tmp_path)
    assert cached["as_of"] == "2026-07-21"
    assert set(cached["results"]) == {"strategy_b"}


def test_clear_后_memo_不残留旧内容(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})

    strategy_cache.clear_cache(tmp_path)

    assert strategy_cache.read_cache(tmp_path) is None


def test_单跑刷新不改写已发出的缓存对象(tmp_path):
    """read_cache 返回进程内共享对象: 上游就地塞键会污染之后所有读取。"""
    from app.api import screener as screener_api

    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    handed_out = strategy_cache.read_cache(tmp_path)

    screener_api._update_cache_strategy(
        tmp_path, "2026-07-20", "strategy_b", _result("600000.SH")
    )

    assert "strategy_b" not in handed_out["results"]
    assert "strategy_b" in strategy_cache.read_cache(tmp_path)["results"]


# ── 摘要侧文件: 卡片摘要的数据源, 不碰全量明细 ──────────────────────────────
def test_摘要剥掉明细但留住计数与原因字段(tmp_path):
    strategy_cache.write_cache(
        tmp_path,
        "2026-07-20",
        {"strategy_a": {**_result("000001.SZ", "000002.SZ"), "computed_at": 111}},
        {"strategy_b": "缺少列"},
    )

    summary = strategy_cache.read_summary(tmp_path)

    assert summary["as_of"] == "2026-07-20"
    assert summary["results"]["strategy_a"] == {
        "total": 2,
        "as_of": "2026-07-20",
        "computed_at": 111,
    }
    assert "rows" not in summary["results"]["strategy_a"]
    # ever 要留完整 symbol 列表 (不是只留计数): 端点叠加实时轮要和它做并集
    assert summary["today_ever_matched"]["strategy_a"] == ["000001.SZ", "000002.SZ"]
    assert summary["errors"] == {"strategy_b": "缺少列"}
    # 摘要必须真的比全量小两个量级, 否则这条改造没有意义
    full = strategy_cache._cache_path(tmp_path).stat().st_size
    assert strategy_cache._summary_path(tmp_path).stat().st_size < full


def test_摘要随每次写入一起更新(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_b": _result("600000.SH")})

    summary = strategy_cache.read_summary(tmp_path)

    assert set(summary["results"]) == {"strategy_a", "strategy_b"}


def test_clear_同时删掉摘要侧文件(tmp_path):
    strategy_cache.write_cache(tmp_path, "2026-07-20", {"strategy_a": _result("000001.SZ")})
    assert strategy_cache.read_summary(tmp_path) is not None

    strategy_cache.clear_cache(tmp_path)

    assert strategy_cache.read_summary(tmp_path) is None
