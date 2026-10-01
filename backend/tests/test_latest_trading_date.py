"""休市日回归 (读取侧): latest_trading_date 必须拒绝"打戳日实为休市日"的假分区。

背景 (2026-10-01 国庆实测): 实时行情用 cn_today() 给每行打戳 (quote_service.
_build_daily), 一旦在休市日跑成 (成因见 tests/test_final_sync_confirmation.py 与
tests/test_eltdx_provider.py), 就会在 kline_daily_enriched 下留下
date=<休市日> 的分区, 内容与上一交易日逐行相同 (实测 5561 只 OHLC 全等,
全市场 change_pct 归零)。

读取侧原本一律取 max(date), 于是这个假分区成为全应用的 as_of: 看板把它当交易日
渲染并显示 as_of=休市日, 选股/策略/监控/异动也全部继承错误日期。

修复: ScreenerService.latest_trading_date() 在"候选 == 今天 且 探针确认今天休市"时
回溯到不晚于今天的最近分区; 探针未知 (None) 或候选早于今天时不拦截。
"""
from __future__ import annotations

from datetime import date

import pytest

import app.market_time as market_time
from app.services import trading_day
from app.services.screener import ScreenerService

TODAY = date(2026, 10, 1)      # 国庆休市
PREV = date(2026, 9, 30)       # 上一交易日


class _StubRepo:
    """最小仓库桩: 固定 max(date) 与按 cutoff 回溯的结果。"""

    def __init__(self, latest: date | None, floored: date | None) -> None:
        self._latest = latest
        self._floored = floored
        self.queries: list[tuple[str, list | None]] = []

    # stock 分支走 enriched_latest_date (优先), 为空时回退 DuckDB
    def enriched_latest_date(self) -> date | None:
        return self._latest

    def execute_one(self, sql: str, params: list | None = None):
        self.queries.append((sql, params))
        if "date <=" in sql:
            return (self._floored,) if self._floored else None
        return (self._latest,) if self._latest else None


@pytest.fixture
def holiday(monkeypatch):
    """固定"今天 = 10-01 且休市", 隔离真实时钟与网络探针。"""
    monkeypatch.setattr(market_time, "cn_today", lambda: TODAY)
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)


def _svc(repo) -> ScreenerService:
    return ScreenerService(repo)


def test_holiday_partition_rolls_back_to_previous_trading_day(holiday):
    """候选 == 今天(休市) → 回溯到上一交易日, 假分区不得成为 as_of。"""
    svc = _svc(_StubRepo(latest=TODAY, floored=PREV))

    assert svc.latest_trading_date() == PREV


def test_rollback_uses_date_le_cutoff_query(holiday):
    """回溯必须走 SQL 谓词 (不能靠 Python 过滤已读入的假行)。"""
    repo = _StubRepo(latest=TODAY, floored=PREV)

    _svc(repo).latest_trading_date()

    cutoff_queries = [p for sql, p in repo.queries if "date <=" in sql]
    assert cutoff_queries, "未走 cutoff 回溯查询"
    assert cutoff_queries[0] == [TODAY]


def test_trading_day_candidate_passes_through(monkeypatch):
    """今天是交易日 → 如实返回候选, 不做回溯 (不得误伤正常路径)。"""
    monkeypatch.setattr(market_time, "cn_today", lambda: TODAY)
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: True)
    repo = _StubRepo(latest=TODAY, floored=PREV)

    assert _svc(repo).latest_trading_date() == TODAY
    assert repo.queries == [], "交易日不应触发回溯查询"


def test_unknown_verdict_keeps_candidate(monkeypatch):
    """探针未知 (None) → 维持原口径, 不擅自回溯。

    探针不可用时"今天"可能就是真实交易日, 回溯会丢当日数据。
    """
    monkeypatch.setattr(market_time, "cn_today", lambda: TODAY)
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: None)
    repo = _StubRepo(latest=TODAY, floored=PREV)

    assert _svc(repo).latest_trading_date() == TODAY
    assert repo.queries == []


def test_past_candidate_untouched_on_holiday(holiday):
    """候选早于今天 (节后首次运行) → 即使今天休市也直接返回, 不回溯。"""
    repo = _StubRepo(latest=PREV, floored=date(2026, 9, 29))

    assert _svc(repo).latest_trading_date() == PREV
    assert repo.queries == []


def test_no_data_returns_none(holiday):
    assert _svc(_StubRepo(latest=None, floored=None)).latest_trading_date() is None


def test_rollback_keeps_candidate_when_no_earlier_partition(holiday):
    """回溯查不到更早分区时保留候选, 避免把 as_of 变成 None 静默清空看板。"""
    svc = _svc(_StubRepo(latest=TODAY, floored=None))

    assert svc.latest_trading_date() == TODAY
