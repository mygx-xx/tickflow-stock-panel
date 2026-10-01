"""交易日探针 (trading_day oracle) 与两个消费方接入的测试。

不依赖真实网络: 探测函数 (_probe_fuyao / _probe_tickflow) 全部 monkeypatch。
覆盖: 周末零成本直判、探测链优先级 (fuyao 日历权威, 无开盘缓冲问题)、
tickflow 戳的 OR 语义与开盘缓冲窗、失败/无权限 → None、TTL 缓存、
实时行情门控与分钟增量 gate_reason 的 holiday 分支。
"""

from __future__ import annotations

from datetime import date, datetime, time as dt_time, timezone, timedelta

import pytest

from app.services import trading_day
from app.services.trading_day import is_trading_day, reset_cache

CN = timezone(timedelta(hours=8))


@pytest.fixture(autouse=True)
def _clean_cache():
    trading_day.set_calendar_store(None)  # 默认关闭持久化, 保持无磁盘副作用
    reset_cache()
    yield
    trading_day.set_calendar_store(None)
    reset_cache()


def _no_probes(monkeypatch):
    """探测函数替换为爆炸 — 用于验证未被打到。"""
    monkeypatch.setattr(trading_day, "_probe_fuyao", lambda now: (_ for _ in ()).throw(AssertionError("不应探测")))
    monkeypatch.setattr(trading_day, "_probe_tickflow", lambda now: (_ for _ in ()).throw(AssertionError("不应探测")))


# ---- 周末零成本直判 ----

def test_weekend_returns_false_without_probing(monkeypatch):
    sat = datetime(2026, 8, 29, 10, 0, tzinfo=CN)  # 周六
    sun = datetime(2026, 8, 30, 10, 0, tzinfo=CN)  # 周日
    _no_probes(monkeypatch)
    assert is_trading_day(sat) is False
    assert is_trading_day(sun) is False


# ---- 探测链优先级 ----

def test_fuyao_calendar_is_authoritative_even_before_open(monkeypatch):
    """fuyao 日历结论无时段依赖: 开盘缓冲窗内也直接生效。"""
    holiday_mon = datetime(2026, 9, 7, 9, 31, tzinfo=CN)  # 周一 (缓冲窗内)
    monkeypatch.setattr(trading_day, "_probe_fuyao", lambda now: False)
    monkeypatch.setattr(
        trading_day, "_probe_tickflow",
        lambda now: (_ for _ in ()).throw(AssertionError("fuyao 已有结论不应继续探测")),
    )
    assert is_trading_day(holiday_mon) is False


def test_chain_falls_through_to_tickflow_when_fuyao_unknown(monkeypatch):
    monday = datetime(2026, 9, 7, 10, 0, tzinfo=CN)
    monkeypatch.setattr(trading_day, "_probe_fuyao", lambda now: None)
    monkeypatch.setattr(trading_day, "_probe_tickflow", lambda now: True)
    assert is_trading_day(monday) is True


def test_all_probes_unknown_returns_none(monkeypatch):
    monday = datetime(2026, 9, 7, 10, 0, tzinfo=CN)
    monkeypatch.setattr(trading_day, "_probe_fuyao", lambda now: None)
    monkeypatch.setattr(trading_day, "_probe_tickflow", lambda now: None)
    assert is_trading_day(monday) is None


# ---- tickflow 戳语义 ----

def test_tickflow_stale_stamp_before_buffer_is_unknown(monkeypatch):
    """开盘缓冲窗内戳停在昨日: 可能是集合竞价未翻新, 保守判未知。"""
    monday_935 = datetime(2026, 9, 7, 9, 35, tzinfo=CN)
    import app.tickflow.client as tf_client_mod

    class _FakeQuotes:
        def get(self, symbols):
            # 周一 9:35 拉到上周五 15:30 的戳
            return [{"symbol": s, "timestamp": 1787902251001} for s in symbols]

    class _FakeClient:
        quotes = _FakeQuotes()

    monkeypatch.setattr(tf_client_mod, "get_client", lambda: _FakeClient())
    monkeypatch.setattr(trading_day, "_probe_fuyao", lambda now: None)
    assert trading_day._probe_tickflow(monday_935) is None


def test_tickflow_stale_stamp_after_buffer_is_holiday(monkeypatch):
    monday_1041 = datetime(2026, 9, 7, 10, 41, tzinfo=CN)
    import app.tickflow.client as tf_client_mod

    class _FakeQuotes:
        def get(self, symbols):
            return [{"symbol": s, "timestamp": 1787902251001} for s in symbols]  # 上周五

    class _FakeClient:
        quotes = _FakeQuotes()

    monkeypatch.setattr(tf_client_mod, "get_client", lambda: _FakeClient())
    assert trading_day._probe_tickflow(monday_1041) is False


def test_tickflow_fresh_stamp_is_trading_anytime(monkeypatch):
    """缓冲窗内只要戳是今天就判交易日 (OR 语义, 任一翻新即可)。"""
    monday_920 = datetime(2026, 9, 7, 9, 20, tzinfo=CN)
    import app.tickflow.client as tf_client_mod

    class _FakeQuotes:
        def get(self, symbols):
            # 一只翻新 + 其余停在周五 → max 为今日
            fresh_ms = int(monday_920.timestamp() * 1000)
            return [
                {"symbol": "000001.SZ", "timestamp": fresh_ms},
                {"symbol": "600519.SH", "timestamp": 1787902251001},
            ]

    class _FakeClient:
        quotes = _FakeQuotes()

    monkeypatch.setattr(tf_client_mod, "get_client", lambda: _FakeClient())
    assert trading_day._probe_tickflow(monday_920) is True


def test_tickflow_error_returns_none(monkeypatch):
    monday = datetime(2026, 9, 7, 10, 41, tzinfo=CN)
    import app.tickflow.client as tf_client_mod

    def _boom():
        raise RuntimeError("无实时权限 (free 档)")

    monkeypatch.setattr(tf_client_mod, "get_client", _boom)
    assert trading_day._probe_tickflow(monday) is None


# ---- TTL 缓存 ----

def test_verdict_cached_within_ttl(monkeypatch):
    monday = datetime(2026, 9, 7, 10, 0, tzinfo=CN)
    calls = {"n": 0}

    def _counting_probe(now):
        calls["n"] += 1
        return True

    monkeypatch.setattr(trading_day, "_probe_fuyao", _counting_probe)
    assert is_trading_day(monday) is True
    assert is_trading_day(monday) is True
    assert is_trading_day(monday) is True
    assert calls["n"] == 1  # 命中缓存, 只探一次


def test_unknown_verdict_retries_after_short_ttl(monkeypatch):
    monday = datetime(2026, 9, 7, 10, 0, tzinfo=CN)
    calls = {"n": 0}

    def _counting_probe(now):
        calls["n"] += 1
        return None

    monkeypatch.setattr(trading_day, "_probe_fuyao", _counting_probe)
    monkeypatch.setattr(trading_day, "_probe_tickflow", lambda now: None)  # 隔离真实网络
    assert is_trading_day(monday) is None
    # 手动把缓存时间拨回 10 分钟前 (超过 unknown TTL 300s) → 重探
    with trading_day._CACHE_LOCK:
        trading_day._CACHE.probed_at -= 600
    assert is_trading_day(monday) is None
    assert calls["n"] == 2


# ---- 消费方 1: 实时行情门控 ----

def test_quote_service_holiday_gate_blocks_polling(monkeypatch):
    from app.services.quote_service import QuoteService

    qs = QuoteService()
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)
    assert qs._should_poll_for_phase("morning") is False
    assert qs._should_poll_for_phase("preopen") is False
    # final 定版同样被剔除: 休市日没有需要定版的当日行情
    assert qs._should_poll_for_phase("close_final") is False


def test_quote_service_polls_when_trading_or_unknown(monkeypatch):
    from app.services.quote_service import QuoteService

    qs = QuoteService()
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: True)
    assert qs._should_poll_for_phase("morning") is True
    # 未知 → 维持现状 (周几近似)
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: None)
    assert qs._should_poll_for_phase("morning") is True


# ---- 消费方 2: 分钟增量 gate_reason ----

def _minute_service(monkeypatch):
    from app.services.minute_refresh import MinuteRefreshService

    svc = MinuteRefreshService.__new__(MinuteRefreshService)
    svc._app_state = None
    monkeypatch.setattr(
        "app.services.minute_refresh.preferences.get_minute_refresh_enabled",
        lambda: True,
    )
    monkeypatch.setattr(svc, "capability_ok", lambda: True)
    return svc


def test_minute_refresh_gate_returns_holiday(monkeypatch):
    """周几+时段门控放行 (周一盘中) 但探针判休市 → holiday。"""
    svc = _minute_service(monkeypatch)
    monday_1030 = datetime(2026, 9, 7, 10, 30, tzinfo=CN)
    monkeypatch.setattr(
        "app.services.minute_refresh._in_continuous_session", lambda now=None: True
    )
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: False)
    assert svc._gate_reason() == "holiday"


def test_minute_refresh_gate_passes_when_trading(monkeypatch):
    svc = _minute_service(monkeypatch)
    monkeypatch.setattr(
        "app.services.minute_refresh._in_continuous_session", lambda now=None: True
    )
    monkeypatch.setattr(trading_day, "is_trading_day", lambda now=None: True)
    assert svc._gate_reason() is None


# ---- fuyao 日历解析 ----

def test_fuyao_provider_trading_days_conversion(monkeypatch):
    from app.plugins.fuyao.provider import FuyaoProvider
    from app.plugins.fuyao import provider as fp

    class _CalClient:
        def trading_days(self):
            # 上海零点戳按 provider 的 _ms_of_date 口径构造
            def _midnight_ms(d):
                import calendar
                return (calendar.timegm(d.timetuple()) - 28800) * 1000

            d4, d7 = date(2026, 9, 4), date(2026, 9, 7)
            return [
                {"date": "20260904", "date_ms": _midnight_ms(d4)},
                {"date": "20260907", "date_ms": _midnight_ms(d7)},
            ]

    monkeypatch.setattr(
        fp, "fuyao_client", type("M", (), {"FuyaoClient": lambda **kw: _CalClient()})
    )
    monkeypatch.setattr(fp, "get_api_key", lambda: "test-key")
    days = FuyaoProvider().trading_days()
    assert days == {date(2026, 9, 4), date(2026, 9, 7)}


def test_unknown_verdict_is_cached_within_short_ttl(monkeypatch):
    """未知结论也要按 _TTL_UNKNOWN_S 缓存: 轮询每拍重探会重复打 tickflow 请求。"""
    monday = datetime(2026, 9, 7, 10, 0, tzinfo=CN)
    calls = {"fuyao": 0, "tickflow": 0}

    def _fuyao(now):
        calls["fuyao"] += 1
        return None

    def _tickflow(now):
        calls["tickflow"] += 1
        return None

    monkeypatch.setattr(trading_day, "_probe_fuyao", _fuyao)
    monkeypatch.setattr(trading_day, "_probe_tickflow", _tickflow)

    assert is_trading_day(monday) is None
    assert is_trading_day(monday) is None
    assert is_trading_day(monday) is None
    assert calls == {"fuyao": 1, "tickflow": 1}

# ---- 统一交易日历 (任意日期 / 前后交易日 / 区间 / 单一缓存) ----

def test_is_trading_day_for_date_uses_calendar(monkeypatch):
    days = {date(2026, 9, 4), date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 10)}
    monkeypatch.setattr(trading_day, "trading_calendar", lambda: days)
    assert trading_day.is_trading_day(date(2026, 9, 4)) is True
    assert trading_day.is_trading_day(date(2026, 9, 8)) is True
    assert trading_day.is_trading_day(date(2026, 9, 9)) is False  # 窗口内工作日但休市
    assert trading_day.is_trading_day(date(2026, 9, 5)) is False  # 周六
    assert trading_day.is_trading_day(date(2026, 9, 6)) is False  # 周日
    # 超出日历窗口 → 未知, 不能误判成休市
    assert trading_day.is_trading_day(date(2026, 9, 11)) is None
    assert trading_day.is_trading_day(date(2026, 9, 3)) is None


def test_is_trading_day_for_date_returns_none_without_calendar(monkeypatch):
    monkeypatch.setattr(trading_day, "trading_calendar", lambda: None)
    assert trading_day.is_trading_day(date(2026, 9, 8)) is None


def test_is_trading_day_date_today_still_uses_probe_chain(monkeypatch):
    """date 口径遇到「今天」时必须仍走探测链 (tickflow 兜底只回答今天)。"""
    monday = datetime(2026, 9, 7, 10, 0, tzinfo=CN)
    monkeypatch.setattr(trading_day, "cn_now", lambda: monday)
    monkeypatch.setattr(trading_day, "_probe_fuyao", lambda now: True)
    assert trading_day.is_trading_day(monday.date()) is True


def test_prev_next_and_range_trading_days(monkeypatch):
    days = {date(2026, 9, 4), date(2026, 9, 7), date(2026, 9, 8)}
    monkeypatch.setattr(trading_day, "trading_calendar", lambda: days)
    assert trading_day.prev_trading_day(date(2026, 9, 8)) == date(2026, 9, 7)
    assert trading_day.prev_trading_day(date(2026, 9, 4)) is None
    assert trading_day.next_trading_day(date(2026, 9, 4)) == date(2026, 9, 7)
    assert trading_day.next_trading_day(date(2026, 9, 8)) is None
    assert trading_day.trading_days(date(2026, 9, 4), date(2026, 9, 8)) == [
        date(2026, 9, 4), date(2026, 9, 7), date(2026, 9, 8),
    ]
    assert trading_day.trading_days(date(2026, 9, 5), date(2026, 9, 6)) == []


def test_trading_calendar_caches_fetch(monkeypatch):
    calls = {"n": 0}

    def _fetch():
        calls["n"] += 1
        return {date(2026, 9, 7)}

    monkeypatch.setattr(trading_day, "_fetch_fuyao_calendar", _fetch)
    assert trading_day.trading_calendar() == {date(2026, 9, 7)}
    assert trading_day.trading_calendar() == {date(2026, 9, 7)}
    assert calls["n"] == 1


def test_trading_calendar_serves_stale_on_error(monkeypatch):
    monkeypatch.setattr(trading_day, "_fetch_fuyao_calendar", lambda: {date(2026, 9, 7)})
    assert trading_day.trading_calendar() == {date(2026, 9, 7)}
    # 模拟 TTL 过期 + fuyao 抖动 (取数返回 None): 沿用上次成功结果
    monkeypatch.setattr(trading_day, "_fetch_fuyao_calendar", lambda: None)
    with trading_day._CAL_LOCK:
        trading_day._CAL = (0.0, trading_day._CAL[1])
    assert trading_day.trading_calendar() == {date(2026, 9, 7)}


# ---- 交易日历持久化 (冷启动/离线兜底 + 并集累积 + 变更才写) ----

def test_calendar_persists_and_reloads_offline(tmp_path, monkeypatch):
    path = tmp_path / "trading_calendar.json"
    trading_day.set_calendar_store(path)
    days = {date(2026, 9, 7), date(2026, 9, 8)}
    monkeypatch.setattr(trading_day, "_fetch_fuyao_calendar", lambda: set(days))
    assert trading_day.trading_calendar() == days

    # 模拟重启: 清内存缓存 (保留 _CAL_FILE), 上游不可用 → 从本地文件恢复
    trading_day.reset_cache()
    monkeypatch.setattr(trading_day, "_fetch_fuyao_calendar", lambda: None)
    assert trading_day.trading_calendar() == days


def test_calendar_union_accumulates_over_window(tmp_path, monkeypatch):
    path = tmp_path / "trading_calendar.json"
    trading_day.set_calendar_store(path)
    monkeypatch.setattr(
        trading_day, "_fetch_fuyao_calendar", lambda: {date(2026, 9, 7)},
    )
    assert trading_day.trading_calendar() == {date(2026, 9, 7)}

    # 上游一年窗口滑动后只剩 09-08; 本地并集必须保留 09-07
    trading_day.reset_cache()
    monkeypatch.setattr(
        trading_day, "_fetch_fuyao_calendar", lambda: {date(2026, 9, 8)},
    )
    assert trading_day.trading_calendar() == {date(2026, 9, 7), date(2026, 9, 8)}


def test_calendar_only_writes_when_changed(tmp_path, monkeypatch):
    path = tmp_path / "trading_calendar.json"
    trading_day.set_calendar_store(path)
    days = {date(2026, 9, 7), date(2026, 9, 8)}
    monkeypatch.setattr(trading_day, "_fetch_fuyao_calendar", lambda: set(days))
    writes = {"n": 0}

    def _counting_replace(src, dst):
        writes["n"] += 1
        trading_day.os.replace(src, dst)

    monkeypatch.setattr(trading_day, "_replace_file", _counting_replace)
    assert trading_day.trading_calendar() == days
    assert writes["n"] == 1
    # 集合不变 → 即使 TTL 过期重取也不再写盘
    with trading_day._CAL_LOCK:
        trading_day._CAL = (0.0, trading_day._CAL[1])
    assert trading_day.trading_calendar() == days
    assert writes["n"] == 1


def test_calendar_store_disabled_does_not_write(tmp_path, monkeypatch):
    path = tmp_path / "trading_calendar.json"
    trading_day.set_calendar_store(path)
    trading_day.set_calendar_store(None)  # 关闭持久化
    monkeypatch.setattr(
        trading_day, "_fetch_fuyao_calendar", lambda: {date(2026, 9, 7)},
    )
    assert trading_day.trading_calendar() == {date(2026, 9, 7)}
    assert not path.exists()
