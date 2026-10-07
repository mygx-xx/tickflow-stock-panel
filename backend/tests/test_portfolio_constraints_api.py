"""组合约束的配置链路契约测试。

组合约束字段要穿过四层才能真正生效:

    API 端点 → StrategyBacktestConfig → MatcherConfig → engine 仓位分配

任何一层漏传, 都会表现为「参数传了但回测结果没变」—— 这是最隐蔽的失败
形态: 不报错, 只是约束静默失效。因此本文件锁定每一段的传递契约。

背景见 `app/backtest/portfolio_constraints.py` 模块 docstring。
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from app.api.backtest import (
    _OPT_BT_CONSTRAINT_FIELDS,
    _OPT_BT_FIELDS,
    _make_job_key,
    _opt_backtest_kwargs,
)
from app.backtest.strategy import StrategyBacktestConfig, StrategyBacktestService
from app.backtest.engine import MatcherConfig


_CONSTRAINT_FIELDS = ("max_position_weight", "max_industry_weight", "max_correlation")


def _cfg(**kwargs) -> StrategyBacktestConfig:
    base = dict(
        strategy_id="s1",
        symbols=None,
        start=date(2026, 1, 1),
        end=date(2026, 2, 1),
    )
    base.update(kwargs)
    return StrategyBacktestConfig(**base)


class TestDefaultDisabled:
    """默认必须全关且逐位一致 —— 否则存量回测结果会变。"""

    def test_strategy_config_defaults_none(self):
        cfg = _cfg()
        for f in _CONSTRAINT_FIELDS:
            assert getattr(cfg, f) is None, f"{f} 默认应为 None"

    def test_matcher_config_defaults_none(self):
        mc = MatcherConfig()
        for f in _CONSTRAINT_FIELDS:
            assert getattr(mc, f) is None

    def test_opt_kwargs_omit_constraints_when_unset(self):
        """未启用时 kwargs 里不应出现约束键 —— 多余的 None 键会让
        backtest_kwargs 校验与缓存签名无谓地变化。"""
        kw = _opt_backtest_kwargs(
            "open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "equal", "position", 5,
        )
        for f in _CONSTRAINT_FIELDS:
            assert f not in kw


class TestConfigPropagation:
    def test_strategy_to_matcher_fields_exist(self):
        """StrategyBacktestConfig 与 MatcherConfig 字段名必须一致,
        否则 strategy.py 的显式传参会静默漏掉。"""
        cfg = _cfg(max_position_weight=0.15, max_industry_weight=0.3, max_correlation=0.8)
        for f in _CONSTRAINT_FIELDS:
            assert hasattr(cfg, f)
            assert hasattr(MatcherConfig(), f)

    def test_config_snapshot_includes_constraints(self):
        """配置快照(回测结果 meta)需含约束, 否则结果无法复现。"""
        from app.backtest.strategy import StrategyBacktestService

        snap = StrategyBacktestService._config_to_dict(
            _cfg(max_position_weight=0.15, max_correlation=0.8)
        )
        assert snap["max_position_weight"] == 0.15
        assert snap["max_correlation"] == 0.8
        assert snap["max_industry_weight"] is None

    def test_replace_preserves_constraints(self):
        """dataclasses.replace 传新时间区间时必须保住约束。"""
        cfg = _cfg(max_position_weight=0.15, max_correlation=0.8)
        moved = replace(cfg, start=date(2026, 3, 1), end=date(2026, 4, 1))
        assert moved.max_position_weight == 0.15
        assert moved.max_correlation == 0.8


class TestOptimizerPath:
    """优化/走查路径最容易丢约束 —— 它们走 backtest_kwargs 白名单。"""

    def test_kwargs_carries_constraints(self):
        kw = _opt_backtest_kwargs(
            "open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "score_weight", "position", 5,
            max_position_weight=0.15, max_industry_weight=0.3, max_correlation=0.8,
        )
        assert kw["max_position_weight"] == 0.15
        assert kw["max_industry_weight"] == 0.3
        assert kw["max_correlation"] == 0.8

    def test_kwargs_fields_accepted_by_config(self):
        """优化器会校验 backtest_kwargs 键合法性, 约束键必须被接受。"""
        from app.backtest.optimizer import OptimizeConfig

        kw = _opt_backtest_kwargs(
            "open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "equal", "position", 5,
            max_position_weight=0.15,
        )
        oc = OptimizeConfig(
            strategy_id="s1", symbols=None,
            start=date(2026, 1, 1), end=date(2026, 2, 1),
            param_grid={"lookback": [5, 10]},
            backtest_kwargs=kw,
        )
        assert oc.backtest_kwargs["max_position_weight"] == 0.15

    def test_bt_signature_includes_constraints(self):
        """缓存签名必须含约束 —— 漏掉会表现为「改了约束没反应」。"""
        kw = _opt_backtest_kwargs(
            "open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "equal", "position", 5,
            max_position_weight=0.15, max_correlation=0.8,
        )
        sig = "|".join(
            [f"{k}={kw[k]}" for k in _OPT_BT_FIELDS]
            + [f"{k}={kw.get(k)}" for k in _OPT_BT_CONSTRAINT_FIELDS]
        )
        assert "max_position_weight=0.15" in sig
        assert "max_correlation=0.8" in sig

    def test_signature_differs_by_constraint(self):
        """不同约束必须产生不同签名。"""
        def _sig(**kw):
            base = _opt_backtest_kwargs(
                "open_t+1", 0.0002, None, None, 5.0, 10, 1.0, 1e6, "equal", "position", 5,
                **kw,
            )
            return "|".join(
                [f"{k}={base[k]}" for k in _OPT_BT_FIELDS]
                + [f"{k}={base.get(k)}" for k in _OPT_BT_CONSTRAINT_FIELDS]
            )

        assert _sig() != _sig(max_position_weight=0.15)
        assert _sig(max_position_weight=0.15) != _sig(max_position_weight=0.2)


class TestJobKeyCaching:
    """回测任务键必须含约束, 否则换约束命中旧任务。"""

    def _key(self, **kw):
        base = dict(
            strategy_id="s1", symbols=None, start="2026-01-01", end="2026-02-01",
            matching="open_t+1", entry_fill=None, exit_fill=None,
            fees_pct=0.0002, slippage_bps=5.0, max_positions=10, max_exposure_pct=1.0,
            initial_capital=1e6, position_sizing="equal",
            params=None, overrides=None,
        )
        base.update(kw)
        return _make_job_key(**base)

    def test_key_differs_by_constraint(self):
        assert self._key() != self._key(max_position_weight=0.15)
        assert self._key(max_position_weight=0.15) != self._key(max_position_weight=0.2)
        assert self._key(max_position_weight=0.15) != self._key(max_correlation=0.8)
        assert self._key(max_industry_weight=0.3) != self._key(max_industry_weight=0.4)

    def test_key_stable_when_all_none(self):
        """全 None 时键应与改动前一致（向后兼容既有前端链接）。"""
        assert self._key() == self._key(
            max_position_weight=None, max_industry_weight=None, max_correlation=None
        )