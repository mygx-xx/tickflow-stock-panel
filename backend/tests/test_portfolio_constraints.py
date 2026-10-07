"""组合约束层测试 — 权重投影与约束执行。

背景（CONTRIBUTING 分层要求: 服务层不依赖 HTTP 类型, 纯逻辑单测）:

现状仓位分配在 `backtest/engine.py` 两处（矩阵路径 L2093 / 标量路径 L2640）：

    target_value = equity * max_exposure_pct / max_positions
    weights = equal(1/N) 或 score 归一
    allocation = min(budget * weight, target_value, cash, capacity)

其中 `target_value` 是**按固定 K值**算出的单票上限。它在 `score_weight`
模式下会二次压制评分权重, 且被截断的资金**不会**再分配给次优候选。

本模块抽出纯函数 `build_target_weights`, 用「迭代投影 + 重新归一」替代
「截断但不重分配」, 并补上组合级约束（单票上限 / 行业暴露 / 相关性去重）。

设计裁定: 分类映射可注入且可缺省 —— `instruments.parquet` 无 industry 字段,
行业成员来自运行时 repo（见 services/rps_rotation._load_concept_map_df）,
回测是纯离线重放不应联网, 故不传 `category_of` 时行业约束降级为不启用,
绝不因分类缺失而让回测失败。
"""
from __future__ import annotations

import math
from dataclasses import replace

import numpy as np
import pytest

from app.backtest.portfolio_constraints import (
    Candidate,
    PortfolioConstraintSpec,
    build_target_weights,
    correlation_matrix,
)


def _cands(scores: list[float], ids: list[str] | None = None) -> list[Candidate]:
    ids = ids or [f"S{i}" for i in range(len(scores))]
    return [
        Candidate(asset_id=i, symbol=sid, score=sc)
        for i, (sid, sc) in enumerate(zip(ids, scores))
    ]


# ================================================================
# 默认关闭: 必须与现状行为逐位一致
# ================================================================

class TestDefaultOff:
    """spec 全默认时输出与现状 equal 档一致 —— 保证 2687 个存量测试不回归。"""

    def test_equal_weight_matches_legacy(self):
        cands = _cands([5.0, 4.0, 3.0])
        got = build_target_weights(cands, PortfolioConstraintSpec())
        assert got.weights == pytest.approx({0: 1 / 3, 1: 1 / 3, 2: 1 / 3})

    def test_sum_is_one(self):
        cands = _cands([9.0, 1.0, 5.0])
        got = build_target_weights(cands, PortfolioConstraintSpec())
        assert got.total_weight == pytest.approx(1.0)

    def test_empty_candidates(self):
        got = build_target_weights([], PortfolioConstraintSpec())
        assert got.weights == {}
        assert got.total_weight == 0.0

    def test_single_candidate_gets_full(self):
        got = build_target_weights(_cands([3.0]), PortfolioConstraintSpec())
        assert got.weights == {0: pytest.approx(1.0)}


# ================================================================
# 核心缺陷 A: score_weight 被固定K 上限二次压制
# ================================================================

class TestScoreWeightNotSuppressed:
    """现状 bug: target_value 按 max_positions 算, 把score_weight 权重压回去。

    10 候选 / max_positions=10 场景下现状实测 5 只被截断、最高分票权重从
    0.1818 压到 0.1000, 且截断资金不重分配 → 总仓位只用约 73%。
    开启 score_weight 后, 权重必须严格按评分比例, 且资金用满。
    """

    def test_weights_strictly_proportional_to_score(self):
        scores = [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
        got = build_target_weights(
            _cands(scores), PortfolioConstraintSpec(position_sizing="score_weight")
        )
        total = sum(scores)
        for cand in got.selected:
            assert got.weights[cand.asset_id] == pytest.approx(scores[cand.asset_id] / total)

    def test_highest_score_keeps_its_share(self):
        """最高分票不该被压到 1/max_positions —— 这是缺陷的核心。"""
        scores = [10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        spec = PortfolioConstraintSpec(position_sizing="score_weight")
        got = build_target_weights(_cands(scores), spec)
        assert got.weights[0] == pytest.approx(10.0 / 19.0)
        # 现状会被压到 0.1；此处必须显著大于 0.1
        assert got.weights[0] > 0.5

    def test_budget_actually_fully_used(self):
        """总仓位应接近满仓 —— 现状截断后剩约 27% 闲置。"""
        scores = [10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
        spec = PortfolioConstraintSpec(position_sizing="score_weight", total_budget=1_000_000)
        got = build_target_weights(_cands(scores), spec)
        assert got.total_weight == pytest.approx(1.0)
        assert got.allocated_value == pytest.approx(1_000_000, rel=1e-6)

    def test_negative_scores_fall_back_to_equal(self):
        """评分全负时 score_weight 无意义, 应降级等权而非崩溃或全零。"""
        spec = PortfolioConstraintSpec(position_sizing="score_weight")
        got = build_target_weights(_cands([-1.0, -2.0, -3.0]), spec)
        assert got.total_weight == pytest.approx(1.0)
        assert len(got.weights) == 3

    def test_zero_scores_fall_back_to_equal(self):
        spec = PortfolioConstraintSpec(position_sizing="score_weight")
        got = build_target_weights(_cands([0.0, 0.0, 0.0]), spec)
        assert got.total_weight == pytest.approx(1.0)


# ================================================================
# 单票权重上限
# ================================================================

class TestMaxWeight:
    def test_single_position_capped(self):
        spec = PortfolioConstraintSpec(position_sizing="score_weight", max_weight=0.15)
        scores = [10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        got = build_target_weights(_cands(scores), spec)
        assert max(got.weights.values()) <= 0.15 + 1e-9

    def test_cap_redistributes_not_truncates(self):
        """封顶后必须重新归一到 1, 否则资金流失（现状缺陷的根因）。"""
        spec = PortfolioConstraintSpec(
            position_sizing="score_weight", max_weight=0.15, total_budget=1_000_000
        )
        scores = [10.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        got = build_target_weights(_cands(scores), spec)
        assert got.total_weight == pytest.approx(1.0)
        assert got.allocated_value == pytest.approx(1_000_000, rel=1e-6)

    def test_cap_too_small_to_fill_is_reported(self):
        """10 只各封顶 10% = 100% 刚好; 封顶 5% 时总额只有 50%, 需如实暴露。"""
        spec = PortfolioConstraintSpec(max_weight=0.05, total_budget=1_000_000)
        got = build_target_weights(_cands([5.0, 4.0, 3.0, 2.0, 1.0]), spec)
        assert got.total_weight == pytest.approx(0.25, rel=1e-6)
        assert got.allocated_value == pytest.approx(250_000, rel=1e-6)

    def test_max_weight_none_no_cap(self):
        spec = PortfolioConstraintSpec(position_sizing="score_weight", max_weight=None)
        got = build_target_weights(_cands([10.0, 1.0, 1.0]), spec)
        assert max(got.weights.values()) == pytest.approx(10.0 / 12.0)


# ================================================================
# 行业暴露上限
# ================================================================

class TestIndustryCap:
    def _two_industry_spec(self, cap: float) -> PortfolioConstraintSpec:
        mapping = {"S0": "银行", "S1": "银行", "S2": "银行", "S3": "白酒", "S4": "白酒"}
        return PortfolioConstraintSpec(max_weight=1.0, industry_max=cap, category_of=mapping.get)

    def test_industry_group_capped(self):
        cands = _cands([5.0, 4.0, 3.0, 2.0, 1.0])
        got = build_target_weights(cands, self._two_industry_spec(0.4))
        bank = sum(got.weights[i] for i in (0, 1, 2))
        assert bank == pytest.approx(0.4, rel=1e-6)

    def test_industry_cap_and_weight_cap_combine(self):
        cands = _cands([5.0, 4.0, 3.0, 2.0, 1.0])
        spec = replace(self._two_industry_spec(0.6), max_weight=0.25)
        got = build_target_weights(cands, spec)
        assert max(got.weights.values()) <= 0.25 + 1e-9
        assert sum(got.weights[i] for i in (0, 1, 2)) <= 0.6 + 1e-9

    def test_missing_category_degrades_not_fails(self):
        """分类缺失(映射返回 None)时该票不进任何行业桶, 约束降级不报错。"""

        def only_some(symbol: str) -> str | None:
            return "银行" if symbol in ("S0", "S1", "S2") else None

        spec = PortfolioConstraintSpec(max_weight=1.0, industry_max=0.4, category_of=only_some)
        got = build_target_weights(_cands([5.0, 4.0, 3.0, 2.0, 1.0]), spec)
        assert sum(got.weights[i] for i in (0, 1, 2)) == pytest.approx(0.4, rel=1e-6)
        assert got.total_weight == pytest.approx(1.0)

    def test_no_category_mapping_disables_industry_constraint(self):
        """不传 category_of → 行业约束不启用（instruments 无 industry 字段）。"""
        cands = _cands([5.0, 4.0, 3.0, 2.0, 1.0])
        got = build_target_weights(cands, PortfolioConstraintSpec(industry_max=0.4))
        assert got.total_weight == pytest.approx(1.0)

    def test_unmapped_everything_degrades_to_unconstrained(self):
        spec = PortfolioConstraintSpec(
            industry_max=0.4, category_of=lambda s: None
        )
        got = build_target_weights(_cands([5.0, 4.0, 3.0]), spec)
        assert got.total_weight == pytest.approx(1.0)


# ================================================================
# 相关性去重
# ================================================================

class TestCorrelationDedup:
    def test_highly_correlated_pair_keeps_higher_score(self):
        #两只近同步走势,相关性 0.95 → 只保留评分高的
        corr = {"S0": {"S0": 1.0, "S1": 0.95}, "S1": {"S0": 0.95, "S1": 1.0}}
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: corr.get(s))
        got = build_target_weights(_cands([5.0, 4.0], ids=["S0", "S1"]), spec)
        assert set(got.weights) == {0}

    def test_low_correlation_both_kept(self):
        corr = {"S0": {"S0": 1.0, "S1": 0.2}, "S1": {"S0": 0.2, "S1": 1.0}}
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: corr.get(s))
        got = build_target_weights(_cands([5.0, 4.0], ids=["S0", "S1"]), spec)
        assert set(got.weights) == {0, 1}

    def test_dedup_then_renormalize(self):
        """去掉一只后, 剩下的必须重新归一到满仓, 不能留空。"""
        corr = {"S0": {"S0": 1.0, "S1": 0.9, "S2": 0.1},
                "S1": {"S0": 0.9, "S1": 1.0, "S2": 0.1},
                "S2": {"S0": 0.1, "S1": 0.1, "S2": 1.0}}
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: corr.get(s))
        got = build_target_weights(_cands([5.0, 4.0, 3.0], ids=["S0", "S1", "S2"]), spec)
        assert set(got.weights) == {0, 2}
        assert got.total_weight == pytest.approx(1.0)

    def test_no_correlation_data_skips_dedup(self):
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: None)
        got = build_target_weights(_cands([5.0, 4.0, 3.0], ids=["S0", "S1", "S2"]), spec)
        assert len(got.weights) == 3

    def test_correlation_matrix_from_returns(self):
        """从收益率矩阵算相关系数。"""
        returns = np.asarray([
            [0.01, 0.02, -0.01, 0.00],
            [0.02, 0.01, -0.02, 0.01],
            [-0.01, -0.02, 0.01, -0.01],
            [0.00, 0.01, -0.01, 0.02],
        ], dtype=float)
        got = correlation_matrix(["A", "B", "C", "D"], returns)
        assert got["A"]["A"] == pytest.approx(1.0)
        # A/B 同向但不共线（样本 4 点, 相关系数不会到 0.99）
        assert got["A"]["B"] > 0.5
        # A/C 反向
        assert got["A"]["C"] < -0.5
        # 对称性是硬性质：去重结果不该依赖查哪一行。
        assert got["A"]["B"] == pytest.approx(got["B"]["A"])
        assert got["A"]["C"] == pytest.approx(got["C"]["A"])

    def test_constant_series_correlation_is_zero(self):
        """零方差序列不能产生 NaN（会静默污染去重判定）。"""
        returns = np.asarray([[0.01, 0.02], [0.01, 0.03], [0.01, 0.01]], dtype=float)
        got = correlation_matrix(["A", "B"], returns)
        for row in got.values():
            for v in row.values():
                assert not math.isnan(v)


# ================================================================
# 上限约束与既有仓位的衔接
# ================================================================

class TestExistingPositions:
    def test_respects_current_holdings(self):
        """已在持仓的票不重复买入, 但权重需计入总账。"""
        got = build_target_weights(
            _cands([5.0, 4.0]), PortfolioConstraintSpec(), current_weights={0: 0.3}
        )
        assert got.weights[0] >= 0.3

    def test_total_includes_existing(self):
        spec = PortfolioConstraintSpec(total_budget=1_000_000)
        got = build_target_weights(_cands([5.0, 4.0]), spec, current_weights={0: 0.2})
        assert got.total_weight == pytest.approx(1.0)


# ================================================================
# 边界与鲁棒
# ================================================================

class TestEdgeCases:
    def test_all_scores_tied(self):
        got = build_target_weights(_cands([1.0, 1.0, 1.0, 1.0]), PortfolioConstraintSpec())
        assert got.total_weight == pytest.approx(1.0)

    def test_spec_iteration_bounded(self):
        """极端参数下必须收敛或触达迭代上限, 不能死循环。"""
        spec = PortfolioConstraintSpec(
            position_sizing="score_weight", max_weight=0.02, max_iter=20
        )
        got = build_target_weights(_cands([10.0, 9.0, 8.0, 7.0, 6.0]), spec)
        assert len(got.weights) <= 5
        assert got.iterations <= 20

    def test_repeated_assets_ignored(self):
        got = build_target_weights(_cands([5.0, 4.0], ids=["S0", "S0"]), PortfolioConstraintSpec())
        assert len(got.weights) == 1

    def test_result_exposes_constraint_report(self):
        """结果需能回答「为什么这只没买」, 供 UI 与归因使用。"""
        spec = PortfolioConstraintSpec(max_weight=0.1)
        got = build_target_weights(_cands([5.0, 4.0, 3.0, 2.0]), spec)
        assert hasattr(got, "dropped")
        assert isinstance(got.dropped, tuple)

    def test_partial_row_falls_back_to_lookup(self):
        """provider 只给「自己那一行」时, 靠 correlation_lookup 二元补齐。"""
        # 真实数据源常只返回候选自身的行, 不含已保留标的。
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: {"S1": {"S1": 1.0}})
        lookup = lambda a, b: 0.95 if {a, b} == {"S0", "S1"} else 0.1
        got = build_target_weights(
            _cands([5.0, 4.0], ids=["S0", "S1"]), spec, correlation_lookup=lookup
        )
        assert set(got.weights) == {0}
        assert any(d.symbol == "S1" for d in got.dropped)

    def test_no_corr_data_is_conservative(self):
        """两侧都拿不到相关度时保守保留 —— 宁可少去重不可误杀。"""
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: None)
        got = build_target_weights(_cands([5.0, 4.0], ids=["S0", "S1"]), spec)
        assert set(got.weights) == {0, 1}

    def test_dropped_records_reason(self):
        corr = {
            "S0": {"S0": 1.0, "S1": 0.99},
            "S1": {"S0": 0.99, "S1": 1.0},
        }
        spec = PortfolioConstraintSpec(max_corr=0.8, correlation_of=lambda s: corr.get(s))
        got = build_target_weights(_cands([5.0, 4.0], ids=["S0", "S1"]), spec)
        assert any(d.symbol == "S1" for d in got.dropped)

    def test_zero_budget(self):
        spec = PortfolioConstraintSpec(total_budget=0.0)
        got = build_target_weights(_cands([5.0, 4.0]), spec)
        assert got.allocated_value == 0.0

    def test_none_budget_weights_still_computed(self):
        """预算未知时仍应给出权重（回测里equity 已由撮合器管）。"""
        got = build_target_weights(_cands([5.0, 4.0]), PortfolioConstraintSpec(total_budget=None))
        assert got.total_weight == pytest.approx(1.0)
        assert got.allocated_value == 0.0