"""命中日报 (hits_archive) — 按日归档 + 跨日差分。

核心口径: 「新增/剔除」是相对**上一归档日**的差分, 所以断言全部锚定集合运算:
  added   = 当日命中 - 前日命中
  dropped = 前日命中 - 当日命中
首日没有前一日 → baseline=True 且 added/dropped 为空数组 (不是 0, 避免前端误显示"无变化")。
"""
from __future__ import annotations

import json
from pathlib import Path

from app.services import hits_archive


def _archive(tmp_path: Path, as_of: str, matched: dict[str, list[str]]) -> None:
    hits_archive.archive(tmp_path, as_of, matched)


def test_归档往返(tmp_path: Path):
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH", "000001.SZ"]})

    got = hits_archive.read(tmp_path, "2026-09-30")

    assert got is not None
    assert got["date"] == "2026-09-30"
    assert got["matched"]["a"] == ["000001.SZ", "600000.SH"]   # 排序落盘, 保证可比
    assert hits_archive.list_dates(tmp_path) == ["2026-09-30"]


def test_读不存在的日期返回None(tmp_path: Path):
    assert hits_archive.read(tmp_path, "1999-01-01") is None
    assert hits_archive.list_dates(tmp_path) == []


def test_首日无基准_baseline为True(tmp_path: Path):
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH", "000001.SZ"], "b": ["600519.SH"]})

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {"a": "甲", "b": "乙"})

    assert rep["baseline"] is True
    assert rep["prev_date"] is None
    assert rep["strategies"][0]["name"] == "甲"          # 命中数 2 > 1, 降序
    assert rep["strategies"][0]["matched"] == 2
    assert rep["strategies"][0]["prev_matched"] == 0
    assert rep["strategies"][0]["added"] == []            # 无基准, 不谎报"新增 2"
    assert rep["strategies"][0]["dropped"] == []
    assert rep["universe"]["current"] == 3


def test_跨日新增与剔除(tmp_path: Path):
    """集合刻意非对称 (增 2 丢 1) —— 若差分方向写反, 数量与内容都会变, 断言必挂。
    对称的 1↔1 交换会让"方向反了但数量相同"蒙混过关。"""
    _archive(tmp_path, "2026-09-29", {"a": ["600000.SH", "000001.SZ"]})
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH", "002671.SZ", "300750.SZ"]})

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {"a": "甲"})

    assert rep["baseline"] is False
    assert rep["prev_date"] == "2026-09-29"
    row = rep["strategies"][0]
    assert row["matched"] == 3
    assert row["prev_matched"] == 2
    assert row["added"] == ["002671.SZ", "300750.SZ"]       # 新进 2 只
    assert row["dropped"] == ["000001.SZ"]                  # 掉出 1 只
    u = rep["universe"]
    assert u["added"] == 2 and u["dropped"] == 1 and u["held"] == 1


def test_多策略并集口径(tmp_path: Path):
    """universe 是所有策略命中的并集; 同一只票被两个策略命中只算一只。"""
    _archive(tmp_path, "2026-09-29", {"a": ["600000.SH"], "b": ["600000.SH", "000001.SZ"]})
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH"], "b": ["000001.SZ"]})

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {})
    u = rep["universe"]

    assert u["prev"] == 2          # 前日并集 {600000, 000001}
    assert u["current"] == 2
    assert u["held"] == 2
    assert u["added"] == 0 and u["dropped"] == 0


def test_只取严格早于当日的基准(tmp_path: Path):
    """归档里有未来日期时不能被当成基准。"""
    _archive(tmp_path, "2026-09-28", {"a": ["600000.SH"]})
    _archive(tmp_path, "2026-09-29", {"a": ["600000.SH", "000001.SZ"]})
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH"]})
    _archive(tmp_path, "2026-10-01", {"a": ["600000.SH", "999999.SH"]})

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {})

    assert rep["prev_date"] == "2026-09-29"     # 而非 10-01 或 09-28


def test_基准日策略缺失_不报错(tmp_path: Path):
    """前日有、今日没跑的策略, 在今日报表里不出现 (而不是给一行 0 变化)。"""
    _archive(tmp_path, "2026-09-29", {"a": ["600000.SH"], "gone": ["000001.SZ"]})
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH"]})

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {})

    assert [r["strategy_id"] for r in rep["strategies"]] == ["a"]
    # 但 universe 层面仍能看出少了什么
    assert rep["universe"]["dropped"] == 1


def test_损坏归档文件不崩(tmp_path: Path):
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH"]})
    (hits_archive._dir(tmp_path) / "2026-09-29.json").write_text("{坏 json", encoding="utf-8")

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {})

    assert rep["strategies"][0]["matched"] == 1     # 基准读失败当无基准
    assert hits_archive.list_dates(tmp_path) == ["2026-09-29", "2026-09-30"]


def test_归档缺失时用策略缓存补基准(tmp_path: Path):
    """功能上线当天: 缓存是上线前写的, 补一份基准让日报立即可用。"""
    cache_dir = tmp_path / "user_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "strategy_cache.json").write_text(
        json.dumps({
            "as_of": "2026-09-30",
            "results": {},
            "today_ever_matched": {"a": ["600000.SH", "000001.SZ"]},
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {"a": "甲"})

    assert rep["strategies"][0]["matched"] == 2
    assert hits_archive.list_dates(tmp_path) == ["2026-09-30"]   # 基准已落盘, 明天可对比


def test_缓存日期不匹配不补基准(tmp_path: Path):
    cache_dir = tmp_path / "user_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "strategy_cache.json").write_text(
        json.dumps({"as_of": "2026-09-28", "today_ever_matched": {"a": ["600000.SH"]}}),
        encoding="utf-8",
    )

    rep = hits_archive.daily_report(tmp_path, "2026-09-30", {})

    assert rep["strategies"] == []
    assert hits_archive.list_dates(tmp_path) == []


def test_同日重复归档内容相同则跳过(tmp_path: Path):
    """run_all 过程中 write_cache 被多次调用, 归档不应每次都重写 (白费 IO)。"""
    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH"]})
    first = (hits_archive._dir(tmp_path) / "2026-09-30.json").stat().st_mtime_ns

    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH"]})   # 内容相同
    assert (hits_archive._dir(tmp_path) / "2026-09-30.json").stat().st_mtime_ns == first

    _archive(tmp_path, "2026-09-30", {"a": ["600000.SH", "000001.SZ"]})  # 变了 → 要重写
    assert (hits_archive._dir(tmp_path) / "2026-09-30.json").stat().st_mtime_ns != first
    assert hits_archive.read(tmp_path, "2026-09-30")["matched"]["a"] == ["000001.SZ", "600000.SH"]
