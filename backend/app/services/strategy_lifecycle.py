"""策略绩效自动判定 — 用 walkforward 退化度与 deflated Sharpe 决定是否降级。

方向二的核心算子。本模块**只做判定与解释**, 不跑回测、不碰文件 I/O:
输入是walkforward 的结果(经`WalkForwardService.run()` 返回), 输出是
「该策略是否应从 active 降级为 watch」+ 可审计的理由。

## 为什么用这两个指标

单一指标都会误判, 组合判定:

1. **degradation**(walkforward.summary) = IS 目标均值 − OOS 目标均值, 正值即
   样本外退化。它抓的是**过拟合**: 样本内选出的参数样本外失效。
   局限: 纯绝对差值, 没有样本量概念 —— 两折退化 0.02 可能比十折退化 0.3 更
   值得警惕(前者样本太少, 方差极大)。故必须配合下面的折数下限。
2. **deflated_sharpe_psr**(backtest.stats_v2) = 剔除「多次试验里最好那个」的
   运气之后的夏普显著性。它抓的是**运气好**: 夏普本身可能不错, 但放在
   同一批策略里比较就不显著。
   局限: 需要 expected_max_sharpe(n_trials, 方差) 才算得出deflated 值,
   单个策略算不出 → 无此信息时该判据必须弃权(返回 None), **不能退化成
   普通 PSR 假装有校正**。

## 阈值为什么必须按 objective 分别定

`walkforward.aggregate_oos` 的 degradation 是**绝对差值, 量纲随 objective 变**:

- `sortino` 可达 2~4, 0.3 的退化算温和
- `win_rate` 上限就是 1.0, 0.02 的退化已很显著
- `avg_holding_days`(唯一的 min 类)量级是个位数天数

用同一个阈值卡全部目标会系统性误判 —— 对 sortino 太严(永远不降级)、
对 win_rate 太松(全降级)。故按 objective 查表, 未知 objective **弃权**
(不降级) 而非用默认值: 宁可漏判也不误杀。

## 保守性设计(与 lifecycle.AUTOMATIC_TRANSITIONS 一致)

判定**只**输出「建议降级 active → watch」, 从不输出晋升:
- 有效折数不足 → 不判定 (insufficient_folds)
- 指标为 None(无数据) → 该判据弃权, 不当作 0
- 两者中任一**明确**触发即建议降级; 全部弃权 → 明确不降级
- 永远不产出 retired 建议(淘汰必须人工复核)
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from typing import Any

from app.backtest.optimizer import _MINIMIZE_OBJECTIVES
from app.backtest.stats_v2 import deflated_sharpe_psr, expected_max_sharpe
from app.strategy.lifecycle import normalize_status

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_DEGRADATION_TOLERANCE",
    "MIN_FOLDS_FOR_JUDGEMENT",
    "MIN_TRADES_FOR_PSR",
    "PSR_FLOOR",
    "Verdict",
    "judge",
    "sharpe_baseline",
    "sweep_strategy_lifecycle",
]

# ---- 全局门限 ------------------------------------------------------------

#: 有效折数下限。低于此数 degradation 的采样噪声大于信号, 不判定。
#: walkforward 默认 train252/test63/step63, 一年约 4 折; 2 折是"至少跨过一个
#: 完整牛熊或两段行情"的最低要求。
MIN_FOLDS_FOR_JUDGEMENT = 2

#: PSR 所需的最小观测数。stats_v2.deflated_sharpe_psr 自身在 n_obs < 5 时
#: 返回 None, 这里提前拦一道, 避免用交易笔数冒充独立观测数。
MIN_TRADES_FOR_PSR = 5

#: deflated Sharpe 低于此值即视为"不显著"。0.95 对应单尾 5% 显著性。
PSR_FLOOR = 0.95

#: 未在 _DEGRADATION_TOLERANCE 中登记的 objective 使用的兜底阈值。
#: 刻意取None 而非一个数字 —— 见模块 docstring「阈值为什么必须按 objective
#: 分别定」。填数字会让未知目标被用一个可能错 10 倍的阈值误判。
DEFAULT_DEGRADATION_TOLERANCE: float | None = None

#: 按优化目标定的 degradation 阈值(归一空间, 正值=退化)。
#: 取值依据: 对每个目标, 阈值设在"该指标单折标准差的量级"上——
#: sharpe/sortino 类无界指标可以容忍绝对值较大的退化; 有界比率类
#: (win_rate/profit_factor) 阈值必须小得多。
_DEGRADATION_TOLERANCE: dict[str, float] = {
    # 无界/可高可低的风险调整指标: 2.0~2.5 的退化是显著的
    "sharpe": 0.5,
    "sortino": 0.6,
    "calmar": 0.6,
    # 有界比率: 胜率 0~1, 0.08 的退化已经很明显
    "win_rate": 0.08,
    "profit_factor": 0.5,
    # 收益类: 年化 20% 的策略掉 10 个百分点是实质性衰退
    "total_return": 0.10,
    "annual_return": 0.10,
    # 回撤类为负值, 最大化带符号值 = 回撤越小越好; 退化体现为更负
    "max_drawdown": 0.10,
    "mc_maxdd_p50": 0.10,
    "mc_maxdd_p95": 0.10,
    "avg_pnl": 0.05,
    "median_pnl": 0.05,
    # 持仓天数: 唯一 min 类, 量级为个位数天
    "avg_holding_days": 3.0,
}


@dataclass(frozen=True)
class Verdict:
    """一次判定的结果。

    用 frozen dataclass 而非 dict: 判定结果会被记入巡检记录并在API 暴露,
    不该被调用方就地改写。
    """

    strategy_id: str
    current_status: str
    #:建议的目标状态, None 表示无建议(数据不足/全部弃权)。
    target_status: str | None
    #: 是否建议降级。唯一为 True 时 target_status 必为 "watch"。
    should_degrade: bool
    #: 人类可读的判定理由, 直接落日志与审计。
    reason: str
    #: 参与判定的原始指标, 便于事后复核(复算不需要重跑回测)。
    metrics: dict[str, Any] = field(default_factory=dict)
    #: 因数据不足而弃权的判据名。弃权不等于通过, 需与"判定为不退化"区分。
    abstained: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "current_status": self.current_status,
            "target_status": self.target_status,
            "should_degrade": self.should_degrade,
            "reason": self.reason,
            "metrics": self.metrics,
            "abstained": list(self.abstained),
        }


def _tolerance_for(objective: str) -> float | None:
    """该 objective 的 degradation 阈值; 未登记返回 None(弃权)。"""
    return _DEGRADATION_TOLERANCE.get(objective, DEFAULT_DEGRADATION_TOLERANCE)


def _direction_of(objective: str) -> str:
    """与 optimizer.default_direction 同源, 避免两处漂移。

    注意 max_drawdown 是**负值**, 最大化带符号值即回撤越小越好, 所以它是
    max 类(见 optimizer._MINIMIZE_OBJECTIVES 注释)。
    """
    return "min" if objective in _MINIMIZE_OBJECTIVES else "max"


def _deg_triggered(degradation: float | None, objective: str) -> tuple[bool | None, float | None]:
    """degradation 判据。返回 (是否触发, 阈值)。

    触发 = 归一空间的退化量超过该 objective 的阈值。
    degradation 已由 walkforward 的 _norm 做过方向归一, 正值恒为退化, 故
    此处不再按 direction 取负 —— 重复归一会把 min 类目标判反。
    """
    if degradation is None:
        return None, None
    tolerance = _tolerance_for(objective)
    if tolerance is None:
        # 阈值缺失 -> 弃权。不能拿 DEFAULT(None) 当 0 用, 也不能猜一个数。
        return None, None
    return degradation > tolerance, tolerance


def _psr_triggered(
    sharpe: float | None,
    n_trades: int | None,
    n_trials: int | None,
    sharpe_variance: float | None,
) -> tuple[bool | None, float | None]:
    """deflated Sharpe 判据。返回 (是否触发, psr 值)。

    弃权条件(返回 (None, None)):
    - sharpe 缺失或非有限
    - 交易笔数不足 MIN_TRADES_FOR_PSR(独立观测数不够, PSR 无意义)
    - 缺 n_trials / sharpe_variance: 无 EM 基准。若此时调
      deflated_sharpe_psr(expected_max_sharpe=0.0) 会得到**未校正的普通 PSR**,
      看起来有判据实则没有 —— 这正是 stats_v2.expected_max_sharpe 在
      n_trials<=1 时返回 0.0 表达"不校正"的意思, 不能当成有效基准。
    """
    if sharpe is None or not math.isfinite(sharpe):
        return None, None
    if n_trades is None or n_trades < MIN_TRADES_FOR_PSR:
        return None, None
    if not n_trials or not sharpe_variance:
        return None, None

    em = expected_max_sharpe(n_trials=n_trials, variance_sharpes=sharpe_variance)
    psr = deflated_sharpe_psr(sharpe=sharpe, n_obs=n_trades, expected_max_sharpe=em)
    if psr is None:
        return None, None
    return psr < PSR_FLOOR, psr


def _total_oos_trades(walkforward_result: dict[str, Any]) -> int | None:
    """汇总各折样本外交易笔数 —— PSR 的 n_obs 由此而来。

    **为什么不能读顶层字段**: `WalkForwardService.run()` 的返回结构里
    **没有** `n_trades`, 交易笔数只存在于每折的 `folds[].oos_stats.n_trades`
    (`backtest/engine.py:2942` 产出)。写成 `result.get("n_trades")` 会恒为
    None → PSR 全程弃权 → 判据静默失效(不报错, 只是永不触发), 是典型的
    "看起来在跑其实没生效"的坑。

    样本外笔数求和而非样本内: PSR 衡量的是**样本外**业绩是否显著,
    拿样本内笔数会高估显著性(样本内是挑出来的最优参数)。

    返回 None 表示折数据缺失或全为 0 —— 交给调用方弃权, 不返回 0
    (0 会被 deflated_sharpe_psr 以n_obs<5 挡下, 但显式 None 语义更清楚)。
    """
    folds = walkforward_result.get("folds") or []
    total = 0
    seen = False
    for fold in folds:
        stats = (fold or {}).get("oos_stats") or {}
        value = stats.get("n_trades")
        if isinstance(value, (int, float)):
            total += int(value)
            seen = True
    return total if seen else None


def judge(
    *,
    strategy_id: str,
    walkforward_result: dict[str, Any] | None,
    current_status: str | None = None,
    n_trials: int | None = None,
    sharpe_variance: float | None = None,
) -> Verdict:
    """判定一个策略是否应降级为 watch。

    参数
    ----
    strategy_id: 仅用于日志与结果标识。
    walkforward_result: `WalkForwardService.run()` 的返回值; None 或结构不符
        时视为无数据(弃权), 不抛异常 —— 巡检要能跳过跑不动的策略而非中断。
    current_status: 策略当前状态。只有 active 会被判定降级; 其余状态
        (draft/watch/retired) 直接返回"不降级", 因为:
        - draft 本来就没进池, 降级无意义
        - watch 已是观察态
        - retired 是终态
    n_trials / sharpe_variance: EM 基准所需, 来自同批策略的夏普分布。
        缺失时 PSR 判据弃权(见 _psr_triggered)。

    判定规则(任一触发即降级):
      - degradation 超过该 objective 的阈值
      - deflated Sharpe 概率低于 PSR_FLOOR
    全部弃权 -> 不降级, 但在 reason 里标明"弃权"以便审计。
    """
    status = normalize_status(current_status)
    if status != "active":
        return Verdict(
            strategy_id=strategy_id,
            current_status=status,
            target_status=None,
            should_degrade=False,
            reason=f"当前状态 {status} 不参与自动降级判定",
            metrics={},
        )

    if not walkforward_result:
        return Verdict(
            strategy_id=strategy_id,
            current_status=status,
            target_status=None,
            should_degrade=False,
            reason="无 walkforward 结果, 判定弃权(不降级)",
            metrics={},
            abstained=("walkforward_missing",),
        )

    summary = walkforward_result.get("summary") or {}
    objective = str(walkforward_result.get("objective") or "sortino")

    # ---------- 前置: 有效折数 ----------
    n_folds = int(summary.get("n_folds") or 0)
    if n_folds < MIN_FOLDS_FOR_JUDGEMENT:
        return Verdict(
            strategy_id=strategy_id,
            current_status=status,
            target_status=None,
            should_degrade=False,
            reason=(
                f"有效折数 {n_folds} < {MIN_FOLDS_FOR_JUDGEMENT}, "
                "样本量不足以判断衰退, 判定弃权(不降级)"
            ),
            metrics={"n_folds": n_folds, "objective": objective},
            abstained=("insufficient_folds",),
        )

    # ---------- 判据 1: degradation ----------
    degradation = summary.get("degradation")
    degradation = float(degradation) if isinstance(degradation, (int, float)) else None
    deg_hit, tolerance = _deg_triggered(degradation, objective)
    consistency = summary.get("consistency")

    # ---------- 判据 2: deflatedSharpe ----------
    # 夏普: objective 就是 sharpe 时, OOS 夏普均值就是 summary 里的
    # avg_oos_objective; 其他 objective 下 summary 没有夏普, 弃权(见下)。
    sharpe = summary.get("avg_oos_objective") if objective == "sharpe" else summary.get("sharpe")
    n_trades = _total_oos_trades(walkforward_result)
    psr_hit, psr_value = _psr_triggered(sharpe, n_trades, n_trials, sharpe_variance)

    metrics: dict[str, Any] = {
        "objective": objective,
        "direction": _direction_of(objective),
        "n_folds": n_folds,
        "degradation": degradation,
        "degradation_tolerance": tolerance,
        "consistency": consistency,
        "avg_is_objective": summary.get("avg_is_objective"),
        "avg_oos_objective": summary.get("avg_oos_objective"),
        "sharpe": sharpe,
        "n_trades": n_trades,
        "deflated_sharpe_psr": psr_value,
        "n_trials": n_trials,
    }

    abstained: list[str] = []
    if deg_hit is None:
        abstained.append("degradation_missing" if degradation is None else "degradation_no_threshold")
    if psr_hit is None:
        abstained.append("psr_unavailable")

    # ---------- 汇总 ----------
    if deg_hit or psr_hit:
        triggers: list[str] = []
        if deg_hit:
            triggers.append(
                f"degradation={degradation} > 阈值 {tolerance}(objective={objective})"
            )
        if psr_hit:
            triggers.append(f"deflated_sharpe_psr={psr_value:.4f} < {PSR_FLOOR}")
        return Verdict(
            strategy_id=strategy_id,
            current_status=status,
            target_status="watch",
            should_degrade=True,
            reason="绩效衰退: " + "; ".join(triggers),
            metrics=metrics,
            abstained=tuple(abstained),
        )

    # 全部可用判据均未触发
    if len(abstained) == 2:
        reason = "所有判据均弃权, 无法评估衰退, 维持 active(需人工复核)"
    else:
        checked = []
        if deg_hit is False:
            checked.append(
                f"degradation={degradation} <= 阈值 {tolerance}"
            )
        if psr_hit is False:
            checked.append(f"deflated_sharpe_psr={psr_value:.4f} >= {PSR_FLOOR}")
        reason = "绩效正常: " + "; ".join(checked)

    return Verdict(
        strategy_id=strategy_id,
        current_status=status,
        target_status=None,
        should_degrade=False,
        reason=reason,
        metrics=metrics,
        abstained=tuple(abstained),
    )


# =====================================================================
# EM 基准: 同批策略的夏普分布
# =====================================================================


def sharpe_baseline(data_dir: Any) -> tuple[int | None, float | None]:
    """统计同批策略的样本外夏普分布, 作为 deflated Sharpe 的 EM 基准。

    EM 校正回答的是「在N 次试验里, 最好的那个夏普本来该有多高」
    (`stats_v2.expected_max_sharpe`)。所以基准必须来自**同一批策略**的夏普
    分布 —— 单个策略算不出方差(`variance_sharpes<=0` 时该函数直接返回 0.0,
    即"不校正")。

    只统计 `objective == "sharpe"` 的缓存: 用别的 objective 的
    `avg_oos_objective`(可能是 sortino/胜率/回撤) 冒充夏普会让 EM 基准完全
    失真, 进而算出错误的 PSR。

    返回 (n_trials, variance_sharpes); 样本不足或方差为 0 时返回
    (None, None), 调用方据此让 PSR 判据整体弃权 —— 只剩 degradation 单判据。
    刻意不猜默认值: 错误的 EM 基准比没有基准更危险(会给出看似严谨的错结论)。
    """
    MIN_SAMPLES = 3
    directory = data_dir / "strategy_lifecycle"
    if not directory.exists():
        return None, None
    sharpes: list[float] = []
    # 只扫一级目录, 不递归: claim 存在 _claims/ 子目录下, 结构上就够隔离。
    for file in sorted(directory.glob("*.json")):
        # 判据是「内容里的 strategy_id 与文件 stem 一致」, 而不是文件名前缀 ——
        # 策略 id 的校验正则 [A-Za-z0-9_-]+ **允许** 以下划线开头, 用
        # `name.startswith("_")` 当过滤条件会把这类合法策略误跳过(实测确认)。
        try:
            payload = json.loads(file.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("夏普基准: 跳过损坏缓存 %s: %s", file.name, exc)
            continue
        if not isinstance(payload, dict):
            continue
        # 缓存文件必然带 strategy_id; claim 等元数据文件没有 -> 天然被排除。
        if payload.get("strategy_id") != file.stem:
            continue
        if payload.get("objective") != "sharpe":
            continue
        value = (payload.get("summary") or {}).get("avg_oos_objective")
        if isinstance(value, (int, float)) and math.isfinite(value):
            sharpes.append(float(value))
    if len(sharpes) < MIN_SAMPLES:
        return None, None
    mean = sum(sharpes) / len(sharpes)
    variance = sum((x - mean) ** 2 for x in sharpes) / len(sharpes)
    if variance <= 0:
        return None, None
    return len(sharpes), variance


# =====================================================================
# 批量巡检: 遍历 active 策略, 逐个判定并落盘降级
# =====================================================================


def sweep_strategy_lifecycle(
    data_dir: Any,
    strategy_engine: Any,
    *,
    n_trials: int | None = None,
    sharpe_variance: float | None = None,
) -> dict[str, Any]:
    """巡检所有 active 策略, 输出降级建议(**只出报告, 不改策略状态**)。

    ## 为什么不直接落盘

    降级要改 `strategy.py` 的 META 文本, 必须走
    `app.api.strategy.set_strategy_status` 的四段式安全写入
    (改写 → reload 断言生效 → 失败回滚 → 二次 reload), 而那套逻辑依赖
    FastAPI Request / 策略引擎上下文。本模块是纯判定层, 不依赖 web 框架,
    也不反向 import api(否则 services ↔ api 循环依赖)。

    故职责切分为: **本函数出建议 → api 层的巡检端点执行落盘**。
    这样手工触发巡检永远不会意外改掉策略状态。

    ## 为什么要逐策略 try/except

    `WalkForwardService` 对非支持 backend 是 **fail-closed 直接 raise**
    (`walkforward.py:161-167`: composite / minute_filter 不支持步进优化)。
    巡检必须**跳过并记录**而不是让整轮失败 —— 否则一个 composite 策略
    就会让本轮所有策略的绩效判定全部落空(静默失效)。

    ## EM 基准的取数

    `n_trials` / `sharpe_variance` 来自**同批策略的夏普分布**, 由调用方
    (api 巡检端点)统计后传入。缺失时 PSR 判据整体弃权, 只剩 degradation
    单判据生效 —— 这是刻意的保守: 宁可少一个判据, 不用错误的基准。
    """
    from app.services import walkforward_store  # 局部导入避免循环依赖

    degraded: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    healthy: list[dict[str, Any]] = []

    try:
        metas = strategy_engine.list_strategies(include_research=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("生命周期巡检: 枚举策略失败: %s", exc)
        return {"status": "failed", "error": str(exc), "degraded": [], "skipped": []}

    for meta in metas:
        sid = str(meta.get("id") or "")
        if not sid:
            continue
        status = normalize_status(meta.get("status"))
        # 只判active —— draft/watch/retired 各有自己的语义(见 judge docstring)
        if status != "active":
            skipped.append({"strategy_id": sid, "reason": f"状态为 {status}, 不参与判定"})
            continue
        try:
            wf_result = walkforward_store.load_walkforward_result(data_dir, sid)
            verdict = judge(
                strategy_id=sid,
                walkforward_result=wf_result,
                current_status=status,
                n_trials=n_trials,
                sharpe_variance=sharpe_variance,
            )
        except Exception as exc:  # noqa: BLE001
            # 单个策略的判定异常不得中断整轮巡检
            logger.warning("生命周期巡检: 策略 %s 判定失败: %s", sid, exc)
            skipped.append({"strategy_id": sid, "reason": f"判定异常: {exc}"})
            continue

        record = verdict.as_dict()
        if verdict.should_degrade:
            degraded.append(record)
            logger.info(
                "生命周期巡检: %s 建议降级 %s → %s: %s",
                sid, verdict.current_status, verdict.target_status, verdict.reason,
            )
        else:
            healthy.append(record)

    return {
        "status": "ok",
        "examined": len(metas),
        "degraded": degraded,
        "healthy": healthy,
        "skipped": skipped,
        "applied": False,  # 本函数永不落盘, 恒为 False; 落盘由 api 巡检端点执行
        "n_trials": n_trials,
        "sharpe_variance": sharpe_variance,
    }