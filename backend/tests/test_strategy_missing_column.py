"""策略引用了面板未提供的数据列 → 收口成中文 ValueError, 不是 500。

回归: 自定义策略过滤引用 pb_latest (metrics 表只有最新一期财报, 日线选股帧里没有该列),
polars 抛 ColumnNotFoundError; run_preset 只捕 ValueError → 一路 500, 且异常原文会把
整张表的列名全量 dump 出来 (上百列), 不能进 API detail。
"""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest
from fastapi import HTTPException

from app.api import screener as screener_api
from app.strategy.engine import StrategyDataContext, StrategyDef, StrategyEngine


def _pb_strategy(sid: str = "custom_pb") -> StrategyDef:
    """引用缺失列 pb_latest 的策略 (polars_expr 后端, filter 返回表达式)。"""
    return StrategyDef(
        meta={"id": sid, "name": sid, "params": [], "scoring": {}, "limit": 100},
        basic_filter={"enabled": False},
        entry_signals=[],
        exit_signals=[],
        stop_loss=None,
        trailing_stop=None,
        trailing_take_profit_activate=None,
        trailing_take_profit_drawdown=None,
        max_hold_days=None,
        filter_fn=lambda df, params: pl.col("pb_latest") < 1.5,
        filter_history_fn=None,
        lookback_days=1,
        source="custom",
    )


def _engine_with(*strategies: StrategyDef) -> StrategyEngine:
    engine = StrategyEngine(strategy_dirs=[])
    for s in strategies:
        engine._strategies[s.meta["id"]] = s
    return engine


def _context() -> StrategyDataContext:
    return StrategyDataContext(
        asset_type="stock",
        timeframe="1d",
        as_of=date(2026, 9, 4),
        current=pl.DataFrame({"symbol": ["000001.SZ", "600000.SH"], "close": [10.0, 8.0]}),
    )


def test_缺失列策略_引擎收口为带列名的中文ValueError():
    engine = _engine_with(_pb_strategy())

    with pytest.raises(ValueError) as excinfo:
        engine.run("custom_pb", _context())

    message = str(excinfo.value)
    assert "pb_latest" in message
    assert "面板未提供的数据列" in message
    # polars 原文会列出全部合法列名, 不能顺着异常文本外泄
    assert "valid columns" not in message


def test_缺失列策略_run_preset返回400而不是500(monkeypatch, tmp_path):
    class _FakeService:
        def __init__(self, repo, asset_type="stock"):
            pass

        def latest_trading_date(self):
            return date(2026, 9, 4)

        def build_strategy_context(self, engine, as_of, strategy_ids, **kwargs):
            return _context()

    monkeypatch.setattr(screener_api, "ScreenerService", _FakeService)
    monkeypatch.setattr(screener_api, "_load_ext_value_maps", lambda *_args: {})
    monkeypatch.setattr(screener_api.strategy_config, "load_override", lambda *_args: {})
    repo = SimpleNamespace(store=SimpleNamespace(data_dir=tmp_path))
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
        repo=repo, strategy_engine=_engine_with(_pb_strategy()))))

    with pytest.raises(HTTPException) as excinfo:
        screener_api.run_preset(
            screener_api.PresetRequest(strategy_id="custom_pb", as_of=date(2026, 9, 4)),
            request,
        )

    assert excinfo.value.status_code == 400
    assert "pb_latest" in excinfo.value.detail
    assert "valid columns" not in excinfo.value.detail


def test_失败原因压平成一句话_不带堆栈与超长文本():
    """run_all 把原因原样写进缓存并透出给卡片, 入库前必须收敛长度。"""
    assert screener_api._strategy_failure_reason(ValueError('缺少列\n  "pb_latest"')) == '缺少列 "pb_latest"'
    assert len(screener_api._strategy_failure_reason(RuntimeError("x" * 300))) == 200
    assert screener_api._strategy_failure_reason(RuntimeError("")) == "RuntimeError"


def test_正常策略不受缺失列收口影响():
    ok = StrategyDef(
        meta={"id": "custom_ok", "name": "custom_ok", "params": [], "scoring": {}, "limit": 100},
        basic_filter={"enabled": False},
        entry_signals=[],
        exit_signals=[],
        stop_loss=None,
        trailing_stop=None,
        trailing_take_profit_activate=None,
        trailing_take_profit_drawdown=None,
        max_hold_days=None,
        filter_fn=lambda df, params: pl.col("close") > 9.0,
        filter_history_fn=None,
        lookback_days=1,
        source="custom",
    )

    result = _engine_with(ok).run("custom_ok", _context())

    assert [row["symbol"] for row in result.rows] == ["000001.SZ"]
