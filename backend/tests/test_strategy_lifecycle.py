"""策略生命周期状态机 + 绩效判定测试。

对标 factors/store.py 的 STATUSES 四态, 但策略是 `.py` 文件存盘(状态在 META 里),
所以本测试只覆盖**语义层**(合法迁移/默认值/可写性)与**判定层**;
文件改写与回滚在 api 层, 由 _set_meta_string_field 保证。

判定层的测试重点是**弃权路径** —— 误杀好策略比漏杀坏策略代价高得多,
所以「数据不足时必须弃权而非判定」是本文件的核心断言。
"""
from __future__ import annotations

import pytest

from app.services.strategy_lifecycle import (
    MIN_FOLDS_FOR_JUDGEMENT,
    PSR_FLOOR,
    Verdict,
    judge,
)
from app.strategy.lifecycle import (
    AUTOMATIC_TRANSITIONS,
    DEFAULT_STATUS,
    STATUSES,
    TRANSITIONS,
    can_transition,
    describe_status,
    is_selectable,
    is_transition_automatic,
    is_visible,
    normalize_status,
)


# =====================================================================
# 状态机: 语义层
# =====================================================================


def test_statuses_与因子侧一致():
    """状态集合必须与 factors 侧同构, 不能各自漂移。"""
    from app.factors.store import STATUSES as FACTOR_STATUSES

    assert STATUSES == FACTOR_STATUSES == {"draft", "active", "watch", "retired"}
    assert DEFAULT_STATUS == "draft"


def test_normalize_未声明取默认():
    """META 里没有 status 字段(历史策略)应视为 draft。

    关键: 不能默认 active —— 那会让所有存量策略一上线就进自动选股池。
    """
    assert normalize_status(None) == "draft"
    assert normalize_status("") == "draft"
    assert normalize_status("   ") == "draft"


def test_normalize_大小写与空白容错():
    assert normalize_status("  Active ") == "active"
    assert normalize_status("WATCH") == "watch"


def test_normalize_非法值回退draft_而非原样透传():
    """非法值必须回退 draft, 不能让脏值渗进迁移图。"""
    assert normalize_status("bogus") == "draft"
    assert normalize_status("activee") == "draft"


def test_迁移图覆盖全部状态且无非法边():
    assert set(TRANSITIONS) == set(STATUSES)
    for src, targets in TRANSITIONS.items():
        assert targets <= set(STATUSES), f"{src} 的迁移目标含非法状态"
        assert src not in targets, f"{src} 不应能迁回自身"


def test_自动降级仅允许_active_to_watch():
    """自动判定只能降级, 绝不自动晋升或永久淘汰。

    这是整个状态机的核心约束: 绩效指标可能抖动, 一次坏结果不该枪毙好策略,
    一次好结果也不该放行未验证策略。
    """
    assert AUTOMATIC_TRANSITIONS == {("active", "watch")}

    # 晋升与淘汰都必须是人工路径
    assert not is_transition_automatic("draft", "active")
    assert not is_transition_automatic("watch", "active")
    assert not is_transition_automatic("active", "retired")
    assert not is_transition_automatic("watch", "retired")
    assert not is_transition_automatic("draft", "retired")


def test_can_transition_人工路径合法():
    # 人工可以跳过激活直接归档
    assert can_transition("draft", "watch")
    assert can_transition("draft", "retired")
    assert can_transition("draft", "active")
    # 人工复核通过可回升
    assert can_transition("watch", "active")
    # retired 只能回 draft(重新走检验)
    assert can_transition("retired", "draft")
    assert not can_transition("retired", "active")


def test_retired_是终态():
    """retired 除回 draft 外无出路, 防止绕过检验直接复活。"""
    assert TRANSITIONS["retired"] == {"draft"}


def test_active_不能直接回draft():
    """active 要退草稿必须先降级或归档, 不能一步回退抹掉衰减历史。"""
    assert not can_transition("active", "draft")


def test_is_selectable_只有active进自动池():
    """只有 active 参与自动选股 —— status 的核心业务语义。"""
    assert is_selectable("active")
    assert not is_selectable("draft")
    assert not is_selectable("watch")
    assert not is_selectable("retired")
    assert not is_selectable(None)


def test_watch_仍然可见():
    """watch 是"待复核"不是"隐藏" —— 人工要能看到它才能复核。"""
    assert is_visible("watch")
    assert is_visible("draft")
    assert is_visible("active")
    assert not is_visible("retired")


def test_describe_status_覆盖全部状态且有中文说明():
    for status in STATUSES:
        desc = describe_status(status)
        assert desc and desc != "未知状态", f"{status} 缺中文说明"


def test_describe_status_非法值先归一再取说明():
    """describe_status 内部走 normalize_status, 非法值回退 draft 后取 draft 说明。

    这是刻意的: 与 normalize_status 保持同一口径, 不会出现"归一后的状态"
    和"展示的说明"不一致。
    """
    assert describe_status("bogus") == describe_status("draft")


# =====================================================================
# 绩效判定: 构造 walkforward 结果
# =====================================================================


def _wf(
    *,
    degradation: float | None,
    n_folds: int = 4,
    objective: str = "sortino",
    avg_is: float | None = 2.0,
    avg_oos: float | None = 1.5,
    consistency: float = 0.75,
    trades_per_fold: int | None = None,
) -> dict:
    """构造一个 WalkForwardService.run() 形状的结果(只填用到的字段)。

    trades_per_fold: 每折样本外交易笔数。**不放顶层** —— 真实结构里
    run() 不返回顶层 n_trades, 笔数只在 folds[].oos_stats.n_trades
    (见 _total_oos_trades 的docstring), 这里必须如实复刻该形状。
    """
    folds = [
        {
            "index": i + 1,
            "test_end": f"2025-0{(i % 9) + 1}-30",
            "is_score": avg_is,
            "oos_objective": avg_oos,
            "oos_stats": {"total_return": 0.02, "n_trades": trades_per_fold},
        }
        for i in range(n_folds)
    ]
    return {
        "objective": objective,
        "direction": "min" if objective == "avg_holding_days" else "max",
        "n_folds": n_folds,
        "folds": folds,
        "summary": {
            "n_folds": n_folds,
            "avg_is_objective": avg_is,
            "avg_oos_objective": avg_oos,
            "degradation": degradation,
            "consistency": consistency,
        },
    }


# ---------- 前置门禁 ----------


@pytest.mark.parametrize("status", ["draft", "watch", "retired"])
def test_非active不参与判定(status):
    """只有 active 会被降级判定, 其余状态直接短路。"""
    v = judge(strategy_id="s1", walkforward_result=_wf(degradation=9.9), current_status=status)
    assert not v.should_degrade
    assert v.target_status is None


def test_未声明状态等同draft_不判定():
    """current_status=None 归一为 draft, 不应误判为 active 而降级。"""
    v = judge(strategy_id="s1", walkforward_result=_wf(degradation=9.9), current_status=None)
    assert not v.should_degrade
    assert v.current_status == "draft"


def test_无结果弃权():
    v = judge(strategy_id="s1", walkforward_result=None, current_status="active")
    assert not v.should_degrade
    assert "walkforward_missing" in v.abstained


def test_空字典结果弃权():
    v = judge(strategy_id="s1", walkforward_result={}, current_status="active")
    assert not v.should_degrade


@pytest.mark.parametrize("n_folds", [0, 1])
def test_折数不足弃权_不因高退化降级(n_folds):
    """1 折退化 99 也不能降级 —— 单折噪声远大于信号, 误杀代价高。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=99.0, n_folds=n_folds),
        current_status="active",
    )
    assert not v.should_degrade
    assert "insufficient_folds" in v.abstained
    assert v.target_status is None


def test_折数恰好达线_参与判定():
    """边界: 恰好等于下限时应正常判定, 不被 <= 误伤。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=99.0, n_folds=MIN_FOLDS_FOR_JUDGEMENT),
        current_status="active",
    )
    assert v.should_degrade


# ---------- degradation 判据 ----------


def test_退化超阈值_建议降级():
    # sortino 阈值 0.6, degradation 1.2 明显超
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=1.2, objective="sortino"),
        current_status="active",
    )
    assert v.should_degrade
    assert v.target_status == "watch"
    assert "degradation" in v.reason


def test_退化未超阈值_不降级():
    # sortino 阈值 0.6, degradation 0.2 温和
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=0.2, objective="sortino"),
        current_status="active",
    )
    assert not v.should_degrade
    assert v.target_status is None


def test_degradation为None_弃权不当作零():
    """空折时 walkforward 返回 None 而非 0.0 —— None 是"无数据", 不是"无退化"。

    若误当作 0, 会把完全没跑出结果的策略判为"健康"。
    """
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=None, objective="sortino"),
        current_status="active",
    )
    assert not v.should_degrade
    assert "degradation_missing" in v.abstained


def test_未知objective_弃权而非猜阈值():
    """未登记阈值的 objective 必须弃权, 不能用兜底数字误判。

    degradation 量纲随 objective 变(见模块 docstring), 猜阈值等于瞎判。
    """
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=99.0, objective="some_new_metric"),
        current_status="active",
    )
    assert not v.should_degrade
    assert "degradation_no_threshold" in v.abstained


@pytest.mark.parametrize(
    ("objective", "degradation", "should_degrade"),
    [
        # 有界比率类: 胜率上限 1.0, 退化 0.1 已显著
        ("win_rate", 0.10, True),
        ("win_rate", 0.03, False),
        # 无界风险调整类: sortino 退化 0.1 不算事
        ("sortino", 0.10, False),
        ("sortino", 1.00, True),
        # min 类目标: 退化同为正值(已由 walkforward 归一), 阈值 3 天
        ("avg_holding_days", 5.0, True),
        ("avg_holding_days", 1.0, False),
    ],
)
def test_阈值按objective分别定(objective, degradation, should_degrade):
    """同一 degradation 数值在不同目标下结论必须不同。

    这是本设计的核心: 用单一阈值会系统性误判
    (对 sortino 太严 / 对 win_rate 太松)。
    """
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=degradation, objective=objective),
        current_status="active",
    )
    assert v.should_degrade is should_degrade, (
        f"{objective} degradation={degradation} 结论错误: {v.reason}"
    )


# ---------- deflated Sharpe 判据 ----------


def test_psr触发_低夏普低交易数():
    """给足 EM 基准但夏普很差 -> PSR 触发。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(
            degradation=0.0, objective="sharpe", avg_oos=0.05, avg_is=0.05,
            n_folds=4, trades_per_fold=20,
        ),
        current_status="active",
        n_trials=50,
        sharpe_variance=0.25,
    )
    assert v.metrics.get("deflated_sharpe_psr") is not None
    assert v.should_degrade
    assert "deflated_sharpe_psr" in v.reason


def test_n_trades_从folds求和而非顶层():
    """交易笔数必须来自 folds[].oos_stats —— 真实结构无顶层 n_trades。

    回归防护: 曾写成 result.get("n_trades") 恒为 None → PSR 全程弃权 →
    判据静默失效(永不触发也不报错)。
    """
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(
            degradation=0.0, objective="sharpe", n_folds=4, trades_per_fold=25,
        ),
        current_status="active",
        n_trials=50,
        sharpe_variance=0.25,
    )
    assert v.metrics["n_trades"] == 100, "4 折 × 25笔应汇总为 100"


def test_psr缺EM基准_弃权而非退化成普通psr():
    """没有 n_trials/方差 时 PSR 必须弃权。

    若传expected_max_sharpe=0.0 会得到未校正的普通 PSR —— 看起来有判据,
    实则没做多次试验校正, 会放行纯靠运气的策略。
    """
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(
            degradation=0.0, objective="sharpe", avg_oos=0.05, avg_is=0.05,
            trades_per_fold=20,
        ),
        current_status="active",
        n_trials=None,
        sharpe_variance=None,
    )
    assert v.metrics.get("deflated_sharpe_psr") is None
    assert "psr_unavailable" in v.abstained


def test_psr交易数不足_弃权():
    """交易笔数不是独立观测数, 太少时 PSR 无意义(stats_v2 自身在 n<5 返回 None)。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(
            degradation=0.0, objective="sharpe", n_folds=2, trades_per_fold=2,
        ),
        current_status="active",
        n_trials=50,
        sharpe_variance=0.25,
    )
    # 4 笔 < MIN_TRADES_FOR_PSR=5 -> 弃权
    assert v.metrics.get("deflated_sharpe_psr") is None
    assert "psr_unavailable" in v.abstained


def test_psr不触发_健康策略():
    """强夏普 + 足够样本 -> PSR 不触发, 且 degradation 也温和 -> 整体不降级。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(
            degradation=0.0, objective="sharpe", avg_is=2.0, avg_oos=1.9,
            n_folds=6, trades_per_fold=40,
        ),
        current_status="active",
        n_trials=50,
        sharpe_variance=0.25,
    )
    assert v.metrics["deflated_sharpe_psr"] >= PSR_FLOOR
    assert not v.should_degrade


def test_两判据任一触发即降级():
    """degradation 温和但 PSR 极差 -> 仍应降级, 不要求两者同时触发。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(
            degradation=0.0, objective="sharpe", avg_is=0.05, avg_oos=0.05,
            trades_per_fold=20,
        ),
        current_status="active",
        n_trials=50,
        sharpe_variance=0.25,
    )
    assert v.should_degrade


# ---------- 汇总行为 ----------


def test_全部弃权时reason明确标注():
    """全弃权必须与"判定为不退级"在 reason 上可区分, 否则巡检记录会骗人。"""
    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=None, objective="unknown_obj"),
        current_status="active",
    )
    assert not v.should_degrade
    assert "弃权" in v.reason


def test_verdict_不可变():
    """判定结果会被记入巡检记录并对外暴露, 不该被就地改写。"""
    v = Verdict(
        strategy_id="s", current_status="active", target_status=None,
        should_degrade=False, reason="x",
    )
    with pytest.raises(Exception):
        v.should_degrade = True  # type: ignore[misc]


def test_verdict_as_dict_可序列化():
    """结果要能进 JSON 巡检记录与 API 响应。"""
    import json

    v = judge(
        strategy_id="s1",
        walkforward_result=_wf(degradation=1.2),
        current_status="active",
    )
    payload = json.dumps(v.as_dict(), ensure_ascii=False)
    assert "should_degrade" in payload
    restored = json.loads(payload)
    assert restored["target_status"] == "watch"


def test_judge_不接受active以外的自动降级():
    """性质断言: 输出 target_status 只可能是 None 或 "watch"(从 active)。

    保证本模块永远不产出"晋升"或"永久淘汰"的建议。
    """
    cases = [
        _wf(degradation=99.0),
        _wf(degradation=-99.0),
        _wf(degradation=None),
        _wf(degradation=0.5, n_folds=1),
        {},
    ]
    for wf in cases:
        v = judge(strategy_id="s", walkforward_result=wf, current_status="active")
        assert v.target_status in (None, "watch"), f"非法建议: {v.as_dict()}"
        if v.should_degrade:
            assert v.current_status == "active"
            assert v.target_status == "watch"