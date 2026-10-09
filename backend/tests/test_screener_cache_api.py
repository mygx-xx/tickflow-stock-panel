from __future__ import annotations

from types import SimpleNamespace

from app.api import screener as screener_api


class _MonitorEngine:
    def __init__(self, results=None):
        self.results = results or {}

    def latest_strategy_results(self):
        return self.results


def _request(tmp_path, monitor_results=None):
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    state = SimpleNamespace(repo=repo, monitor_engine=_MonitorEngine(monitor_results))
    return SimpleNamespace(app=SimpleNamespace(state=state))


def test_cached_summary_omits_rows_and_counts_realtime_expirations(monkeypatch, tmp_path):
    cached = {
        "as_of": "2026-07-20",
        "results": {
            "strategy_a": {
                "as_of": "2026-07-20",
                "total": 2,
                "rows": [{"symbol": "000001.SZ"}, {"symbol": "000002.SZ"}],
            },
        },
        "today_ever_rows": {
            "strategy_a": {
                "000001.SZ": {"symbol": "000001.SZ"},
                "600000.SH": {"symbol": "600000.SH"},
            },
        },
        "updated_at": 1,
    }
    realtime = {
        "strategy_a": {
            "as_of": "2026-07-20",
            "total": 2,
            "rows": [{"symbol": "000002.SZ"}, {"symbol": "300001.SZ"}],
        },
    }
    monkeypatch.setattr(screener_api.strategy_cache, "read_cache", lambda *_args: cached)

    payload = screener_api.get_cached_summary(_request(tmp_path, realtime))

    assert payload["results"] == {
        "strategy_a": {"total": 2, "as_of": "2026-07-20", "computed_at": None}
    }
    assert payload["today_ever_counts"] == {"strategy_a": 4}
    assert "rows" not in payload["results"]["strategy_a"]


def test_cached_summary_透出缺数据策略的失败原因(monkeypatch, tmp_path):
    """跑挂的策略要把原因带给卡片: 否则只剩空白, 用户看不出是策略引用了缺失列。"""
    reason = '策略引用了面板未提供的数据列 "pb_latest"'
    cached = {
        "as_of": "2026-07-20",
        "results": {
            "strategy_ok": {"as_of": "2026-07-20", "total": 1, "rows": [{"symbol": "000001.SZ"}]},
            # 同日重跑才失败: 旧结果还在, 但原因必须一并透出
            "strategy_stale": {"as_of": "2026-07-20", "total": 3, "rows": []},
        },
        "errors": {"strategy_broken": reason, "strategy_stale": reason},
        "today_ever_rows": {},
        "updated_at": 1,
    }
    monkeypatch.setattr(screener_api.strategy_cache, "read_cache", lambda *_args: cached)

    payload = screener_api.get_cached_summary(_request(tmp_path))

    assert payload["results"]["strategy_stale"]["error"] == reason
    # 只有失败没有结果的策略也要有条目, 前端才有地方挂原因
    assert payload["results"]["strategy_broken"] == {
        "total": 0,
        "as_of": "2026-07-20",
        "computed_at": None,
        "error": reason,
    }
    assert "error" not in payload["results"]["strategy_ok"]
    # 失败策略不进「今日曾命中」差分, 卡片不会同时冒出 -N 失效数
    assert payload["today_ever_counts"] == {"strategy_ok": 1, "strategy_stale": 0}


def test_cached_result_returns_only_requested_rows_with_ext_and_strategy_membership(monkeypatch, tmp_path):
    cached = {
        "as_of": "2026-07-20",
        "results": {
            "strategy_a": {
                "as_of": "2026-07-20",
                "total": 1,
                "rows": [{"symbol": "000001.SZ"}],
            },
            "strategy_b": {
                "as_of": "2026-07-20",
                "total": 2,
                "rows": [{"symbol": "000001.SZ"}, {"symbol": "600000.SH"}],
            },
        },
        "today_ever_rows": {
            "strategy_a": {
                "000001.SZ": {"symbol": "000001.SZ"},
                "000002.SZ": {"symbol": "000002.SZ"},
            },
        },
        "updated_at": 1,
    }
    monkeypatch.setattr(screener_api.strategy_cache, "read_cache", lambda *_args: cached)
    monkeypatch.setattr(
        screener_api,
        "_load_ext_value_maps",
        lambda *_args: {"concept.concept": {"000001.SZ": "银行", "000002.SZ": "科技"}},
    )

    payload = screener_api.get_cached_result(
        "strategy_a",
        _request(tmp_path),
        ext_columns="concept.concept",
    )

    assert payload["result"]["strategy"] == "strategy_a"
    assert payload["result"]["rows"] == [{"symbol": "000001.SZ", "concept.concept": "银行"}]
    assert payload["today_ever_rows"]["000002.SZ"]["concept.concept"] == "科技"
    assert payload["strategy_ids_by_symbol"] == {"000001.SZ": ["strategy_a", "strategy_b"]}


# ── #303 数据充足性提示的读侧透传 ──────────────────────────────────────────
# run_all / 单跑会把 coverage_warnings 写进缓存 (results[sid]["warnings"]), 但
# /cached-result 会重建 result 对象; 若不显式透传, 策略页首屏(走缓存)在薄库时
# 只会显示「今日无命中」而没有任何解释。

_WARNING = "本地数据仅覆盖 3 个交易日, 低于本次计算所需约 60 天暖机窗口"


def _cached_with_warnings(warnings=None) -> dict:
    result = {
        "as_of": "2026-07-20",
        "total": 0,
        "rows": [],
    }
    if warnings is not None:
        result["warnings"] = warnings
    return {
        "as_of": "2026-07-20",
        "results": {"strategy_a": result},
        "today_ever_rows": {},
        "updated_at": 1,
    }


def test_cached_result_carries_coverage_warnings(monkeypatch, tmp_path):
    monkeypatch.setattr(
        screener_api.strategy_cache, "read_cache", lambda *_args: _cached_with_warnings([_WARNING])
    )

    payload = screener_api.get_cached_result("strategy_a", _request(tmp_path), ext_columns=None)

    assert payload["result"]["warnings"] == [_WARNING]


def test_cached_result_omits_warnings_when_cache_has_none(monkeypatch, tmp_path):
    monkeypatch.setattr(
        screener_api.strategy_cache, "read_cache", lambda *_args: _cached_with_warnings()
    )

    payload = screener_api.get_cached_result("strategy_a", _request(tmp_path), ext_columns=None)

    assert "warnings" not in payload["result"]


def test_cached_keeps_coverage_warnings_with_and_without_ext_columns(monkeypatch, tmp_path):
    """两条返回路径(带/不带扩展列)都必须保住 warnings —— 带扩展列会走
    _cache_payload_with_ext 重建 results, 是最容易漏掉提示的分支。"""
    monkeypatch.setattr(
        screener_api.strategy_cache, "read_cache", lambda *_args: _cached_with_warnings([_WARNING])
    )

    plain = screener_api.get_cached(_request(tmp_path), ext_columns=None)
    assert plain["results"]["strategy_a"]["warnings"] == [_WARNING]

    monkeypatch.setattr(
        screener_api,
        "_load_ext_value_maps",
        lambda *_args: {"concept.concept": {"000001.SZ": "银行"}},
    )
    with_ext = screener_api.get_cached(_request(tmp_path), ext_columns="concept.concept")
    assert with_ext["results"]["strategy_a"]["warnings"] == [_WARNING]
