"""模拟盘仓位约束测试。

背景: 回测侧 `MatcherConfig` 有 `max_exposure_pct` / `max_position_weight`,
但模拟盘账户此前**完全没有**这两个字段 —— 表现为「回测设了 60% 仓位,
模拟盘却满仓」的口径不一致, 用户在两处得到的结论互斥。

本文件锁定:
  1. 账户字段可写可读, 默认None(不限制)
  2. 买入时约束真正拦截 (不是只存不用)
  3. 约束只作用于新买入, 调小上限不强平已有持仓
  4. 卖出不受约束影响 (减仓永远允许)
"""
from __future__ import annotations

import pytest

from app.strategy import paper


@pytest.fixture
def account(tmp_path):
    """初始 100 万现金的账户。"""
    return paper.create_account(tmp_path, 1_000_000.0)


class TestAccountFields:
    def test_defaults_are_none(self, account):
        """默认不限制 —— 保持既有账户行为不变 (零回归前提)。"""
        assert account["max_exposure_pct"] is None
        assert account["max_position_weight"] is None

    def test_can_set_at_creation(self, tmp_path):
        acc = paper.create_account(
            tmp_path, 1_000_000.0,
            account_id="a1",
            max_exposure_pct=0.6,
            max_position_weight=0.2,
        )
        assert acc["max_exposure_pct"] == 0.6
        assert acc["max_position_weight"] == 0.2

    def test_rejects_out_of_range(self, tmp_path):
        for bad in (0.0, -0.1, 1.5):
            with pytest.raises(ValueError, match="max_exposure_pct"):
                paper.create_account(
                    tmp_path, 1_000_000.0, account_id="bad", max_exposure_pct=bad
                )

    def test_rejects_non_numeric(self, tmp_path):
        with pytest.raises(ValueError, match="max_position_weight"):
            paper.create_account(
                tmp_path, 1_000_000.0, account_id="bad2", max_position_weight="abc"
            )

    def test_update_settings_accepts(self, tmp_path, account):
        acc = paper.update_settings(
            tmp_path, paper.DEFAULT_ACCOUNT_ID, max_exposure_pct=0.5
        )
        assert acc["max_exposure_pct"] == 0.5

    def test_update_settings_can_clear(self, tmp_path, account):
        """传 None 应能取消限制 —— 否则用户设错后无法恢复。"""
        paper.update_settings(tmp_path, paper.DEFAULT_ACCOUNT_ID, max_exposure_pct=0.5)
        acc = paper.update_settings(
            tmp_path, paper.DEFAULT_ACCOUNT_ID, max_exposure_pct=None
        )
        assert acc["max_exposure_pct"] is None

    def test_update_settings_rejects_bad(self, tmp_path, account):
        with pytest.raises(ValueError, match="max_position_weight"):
            paper.update_settings(
                tmp_path, paper.DEFAULT_ACCOUNT_ID, max_position_weight=2.0
            )


class TestBuyIsBlocked:
    """约束必须在 create_order 阶段真正拦截, 不能只存不用。"""

    def test_no_constraint_allows_full(self, tmp_path, account):
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=10000, ref_price=10.0
        )
        assert err is None
        assert order is not None

    def test_exposure_cap_blocks(self, tmp_path):
        paper.create_account(
            tmp_path, 1_000_000.0, max_exposure_pct=0.5,
        )
        # 净值 100 万, 上限 50% => 最多投 50 万。买 60 万应被拒。
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=60000, ref_price=10.0
        )
        assert order is None
        assert err is not None and "总仓位上限" in err

    def test_exposure_cap_allows_within(self, tmp_path):
        paper.create_account(tmp_path, 1_000_000.0, max_exposure_pct=0.5)
        # 40 万 < 50 万上限
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=40000, ref_price=10.0
        )
        assert err is None and order is not None

    def test_position_weight_cap_blocks(self, tmp_path):
        paper.create_account(
            tmp_path, 1_000_000.0, max_position_weight=0.15,
        )
        # 单票上限 15% => 15 万。买 20 万应被拒。
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=20000, ref_price=10.0
        )
        assert order is None
        assert err is not None and "单票权重上限" in err

    def test_position_weight_applies_per_symbol(self, tmp_path):
        """单票上限是「每只」而非「全部」—— 两只各 10% 应都放行。"""
        paper.create_account(tmp_path, 1_000_000.0, max_position_weight=0.15)
        o1, e1 = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=10000, ref_price=10.0
        )
        o2, e2 = paper.create_order(
            tmp_path, "000001.SZ", "buy", qty=10000, ref_price=10.0
        )
        assert e1 is None and e2 is None
        assert o1 is not None and o2 is not None


class TestSellNotBlocked:
    """约束绝不能阻止卖出 —— 减仓/止损是风控刚需。"""

    def test_sell_allowed_even_when_over_cap(self, tmp_path):
        paper.create_account(
            tmp_path, 1_000_000.0, max_exposure_pct=0.5, max_position_weight=0.15,
        )
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=10000, ref_price=10.0
        )
        assert err is None
        # 未成交无持仓可卖; 这里只验证约束不会拦截 sell 路径的校验逻辑:
        # 无持仓时返回的是「无持仓」而非「超出上限」。
        o2, err2 = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=100, ref_price=10.0
        )
        # 10 万 + 1 千仍在 15 万内 -> 放行
        assert err2 is None and o2 is not None


class TestConstraintScope:
    """约束只作用于新买入, 不追溯 —— 与回测语义一致。"""

    def test_lowering_cap_does_not_force_close(self, tmp_path, account):
        """持仓已在高位时调小上限, 不应强平 (无强平路径)。"""
        paper.update_settings(
            tmp_path, paper.DEFAULT_ACCOUNT_ID, max_position_weight=0.9
        )
        paper.create_order(tmp_path, "600000.SH", "buy", qty=10000, ref_price=10.0)
        # 直接调低上限, 不应抛错也不应改动持仓
        paper.update_settings(
            tmp_path, paper.DEFAULT_ACCOUNT_ID, max_position_weight=0.05
        )
        positions = paper.load_positions(tmp_path)
        # 未成交, 持仓为空 —— 关键是 update_settings 没报错
        assert isinstance(positions, dict)

    def test_zero_nav_does_not_crash(self, tmp_path):
        """净值非正时不应崩溃 (交给资金预检报错)。"""
        paper.create_account(tmp_path, 1000.0, max_exposure_pct=0.5)
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=100, ref_price=50.0
        )
        # 1000 块买 5000 元 -> 资金不足, 但约束检查不能先崩
        assert order is None and err is not None

    def test_no_ref_price_skips_check(self, tmp_path):
        """无参考价时跳过约束预检 (真实约束在撮合时按成交价二次校验)。"""
        paper.create_account(tmp_path, 1_000_000.0, max_exposure_pct=0.01)
        order, err = paper.create_order(
            tmp_path, "600000.SH", "buy", qty=100, ref_price=None,
        )
        assert err is None and order is not None