"""enriched 重建模式决策的测试。

锚定的是"选错模式 = 静默产出不一致 enriched"的几条红线:
- 中间/开头的缺口**必须**走全量 (增量只补末尾, 缺口会永远留着)
- 发布未完成**必须**走全量 (增量会与半成品混合)
- 已对齐时**必须**是 noop, 不能白跑 49 秒全量
- 孤儿分区要如实报告, 不能装作无事发生
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.services import enriched_rebuild_plan as P


def _mk(root: Path, which: str, dates: list[str]) -> None:
    base = root / ("kline_daily" if which == "daily" else "kline_daily_enriched")
    for d in dates:
        (base / f"date={d}").mkdir(parents=True, exist_ok=True)


def _days(n: int, start: str = "2026-01-01") -> list[str]:
    from datetime import date, timedelta

    y, m, d = (int(x) for x in start.split("-"))
    base = date(y, m, d)
    return [(base + timedelta(days=i)).isoformat() for i in range(n)]


# ---------- 首次 / 强制 ----------

def test_首次建库走全量(tmp_path: Path):
    _mk(tmp_path, "daily", _days(5))

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "full"
    assert "首次" in plan["reason"]
    assert plan["run_kwargs"] == {"new_dates_only": False, "symbols": None}


def test_force_full覆盖任何状态(tmp_path: Path):
    _mk(tmp_path, "daily", _days(5))
    _mk(tmp_path, "enriched", _days(5))

    plan = P.plan_rebuild(tmp_path, force_full=True)

    assert plan["mode"] == "full"
    assert plan["run_kwargs"]["new_dates_only"] is False


def test_发布未完成必须全量不能增量(tmp_path: Path):
    """发布中断留下的半成品与增量结果混合 = 数据不一致。"""
    _mk(tmp_path, "daily", _days(10, "2026-02-01"))
    _mk(tmp_path, "enriched", _days(10, "2026-02-01"))

    plan = P.plan_rebuild(tmp_path, publication_incomplete=True)

    assert plan["mode"] == "full"
    assert "发布未完成" in plan["reason"]


def test_无日K数据走noop(tmp_path: Path):
    """已建好的库, daily 被清空 → 无事可做(不是"首次建库")。"""
    _mk(tmp_path, "enriched", _days(5))

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "noop"
    assert plan["daily_days"] == 0
    assert "无日K数据" in plan["reason"]


def test_全新空库走全量而非noop(tmp_path: Path):
    """两者都是 full/noop 的边界: enriched 也空时是首次建库, 该全量。"""
    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "full"
    assert "首次" in plan["reason"]


# ---------- 已对齐 ----------

def test_完全对齐时不白跑全量(tmp_path: Path):
    """实测 1454 天 / 720 万行全量要 49 秒 —— 已对齐却全量是纯浪费。"""
    _mk(tmp_path, "daily", _days(30))
    _mk(tmp_path, "enriched", _days(30))

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "noop"
    assert plan["missing_total"] == 0
    assert plan["orphan_total"] == 0


# ---------- 末尾新增: 增量 ----------

def test_末尾新增走向前增量(tmp_path: Path):
    old = _days(10, "2026-01-01")
    _mk(tmp_path, "daily", old + ["2026-01-11", "2026-01-12"])
    _mk(tmp_path, "enriched", old)

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "forward"
    assert plan["run_kwargs"] == {"new_dates_only": True, "symbols": None}
    assert plan["missing_dates"] == ["2026-01-11", "2026-01-12"]
    assert "2/12" in plan["savings"]


def test_末尾新增且有个股除权变更_增量并带上symbols(tmp_path: Path):
    """新分区与旧分区的复权口径必须一致 → 受影响个股要一起重算历史。"""
    old = _days(10, "2026-01-01")
    _mk(tmp_path, "daily", old + ["2026-01-11"])
    _mk(tmp_path, "enriched", old)

    plan = P.plan_rebuild(tmp_path, affected_symbols=["600000.SH", "000001.SZ"])

    assert plan["mode"] == "forward"
    assert plan["run_kwargs"] == {"new_dates_only": True, "symbols": ["000001.SZ", "600000.SH"]}
    assert "除权因子变更" in plan["reason"]


# ---------- 向前缺口: 必须全量 ----------

def test_向前缺口必须全量而非增量(tmp_path: Path):
    """这是本模块最关键的一条: 增量模式只看"末尾新增", 中间缺口它永远看不见。"""
    _mk(tmp_path, "daily", _days(10, "2026-01-01"))
    # 中间挖掉第 3 天, 末尾仍在
    _mk(tmp_path, "enriched", [d for d in _days(10, "2026-01-01") if d != "2026-01-03"])

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "full"
    assert "向前的缺口" in plan["reason"]
    assert plan["earliest_missing"] == "2026-01-03"


def test_开头缺口必须全量(tmp_path: Path):
    _mk(tmp_path, "daily", _days(10, "2026-01-01"))
    _mk(tmp_path, "enriched", _days(5, "2026-01-06"))

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "full"
    assert "向前" in plan["reason"]


# ---------- 仅除权变更: 局部重算 ----------

def test_仅除权变更走局部重算(tmp_path: Path):
    ds = _days(10)
    _mk(tmp_path, "daily", ds)
    _mk(tmp_path, "enriched", ds)

    plan = P.plan_rebuild(tmp_path, affected_symbols=["600000.SH"])

    assert plan["mode"] == "local"
    assert plan["run_kwargs"] == {"new_dates_only": False, "symbols": ["600000.SH"]}


def test_局部重算个股过多改走全量(tmp_path: Path):
    """逐只重算全部历史, 500 只反而比一次全量慢。"""
    ds = _days(10)
    _mk(tmp_path, "daily", ds)
    _mk(tmp_path, "enriched", ds)

    plan = P.plan_rebuild(tmp_path, affected_symbols=[f"s{i}" for i in range(P.LOCAL_RECOMPUTE_SYMBOL_THRESHOLD + 1)])

    assert plan["mode"] == "full"
    assert "比全量更慢" in plan["reason"]


# ---------- 孤儿分区 ----------

def test_孤儿分区如实报告而非当作无事(tmp_path: Path):
    """enriched 多出 daily 没有的分区(数据被删) —— 增量不删分区, 不能静默。"""
    _mk(tmp_path, "daily", _days(5))
    _mk(tmp_path, "enriched", _days(8))

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "noop"
    assert plan["orphan_total"] == 3
    assert "孤儿分区" in plan["reason"]


def test_孤儿与末尾缺口并存时走增量(tmp_path: Path):
    """daily 比 enriched 多 3 天(末尾连续) + enriched 多 3 天(孤儿) → 增量补缺口。"""
    _mk(tmp_path, "daily", _days(8))
    _mk(tmp_path, "enriched", _days(5))

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "forward"
    assert plan["missing_total"] == 3
    assert plan["orphan_total"] == 0      # _days(5) 是 _days(8) 的子集, 无孤儿


def test_前向缺口与孤儿并存时必须全量(tmp_path: Path):
    """中间挖洞 + enriched 多出尾部 → 缺口不在末尾, 全量才能填。"""
    ds = _days(8)
    _mk(tmp_path, "daily", ds)
    _mk(tmp_path, "enriched", [d for d in ds if d != "2026-01-03"] + ["2025-12-30"])

    plan = P.plan_rebuild(tmp_path)

    assert plan["mode"] == "full"
    assert plan["missing_total"] == 1
    assert plan["orphan_total"] == 1


# ---------- 辅助 ----------

def test_分区名解析不误收非日期目录(tmp_path: Path):
    """``date=`` 这种空分区名要忽略, 不能当成一个日期。"""
    ds = _days(3)
    _mk(tmp_path, "daily", ds)
    _mk(tmp_path, "enriched", ds)
    (tmp_path / "kline_daily_enriched" / "date=").mkdir(parents=True, exist_ok=True)

    plan = P.plan_rebuild(tmp_path)

    assert plan["daily_days"] == 3
    assert plan["enriched_days"] == 3


def test_计划含完整诊断字段(tmp_path: Path):
    """前端要显示"依据", 所以诊断字段不能省。"""
    _mk(tmp_path, "daily", _days(5))
    _mk(tmp_path, "enriched", _days(4))

    plan = P.plan_rebuild(tmp_path)

    for key in (
        "mode", "reason", "savings", "run_kwargs", "daily_days", "enriched_days",
        "missing_dates", "missing_total", "orphan_dates", "orphan_total",
        "earliest_missing", "affected_symbols", "adj_dates",
    ):
        assert key in plan, f"缺少字段 {key}"
