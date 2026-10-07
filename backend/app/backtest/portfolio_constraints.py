"""组合约束层 — 把「候选 + 评分」翻译成「合规的目标权重」。

职责: 纯函数式权重投影。不认识 HTTP、不读文件、不读配置, 只接受数据返回数据。
不知道: 撮合、成本、撮合时点、账务、持仓对象 —— 那些属于 `backtest/engine.py`。

存在的原因（现状缺陷, 已实测）:

`backtest/engine.py` 两处仓位分配（L2093 矩阵路径 / L2640 标量路径）都是:

    target_value = equity * max_exposure_pct / max_positions
    weights = equal(1/N) 或 score 归一
    allocation = min(budget * weight, target_value, cash, capacity)

问题有三层:

1. **评分只决定「选谁」, 不决定「买多少」**。K 值是 `max_positions` 这个固定整数,
   候选池再大也只能进固定只数, 无法表达「前3 只各 30%」这类集中意图。
2. **`score_weight` 被固定K 上限二次压制**。`target_value` 按 `1/max_positions` 算,
   在 10 候选 / max_positions=10 / 评分 10..1 的实测场景下, 最高分票权重从 0.1818
   被压到 0.1000, 且 5 只被截断 —— 截断掉的资金**不会**再分配给次优候选,
   结果总仓位只用约 73%, 剩 27% 闲置现金。两头都不对: 好票没拿到应得份额,
   钱也没补给次优票。
3. **无任何组合级约束**。单票权重、行业暴露、相关性集中度全部没有约束点。

本模块的做法是「迭代投影 + 重新归一」: 约束收缩权重后**缩放回满仓**,
而不是截断剩余资金。这正是缺陷 2 的根因修复。

算法（projection, 不引入凸优化求解器 —— 见模块 docstring 末段）:

    1. 权重归一 -> sum(w) = 1
    2. 循环至收敛 (max_iter):
         a. 单票封顶   w_i = min(w_i, max_weight)
         b. 行业封顶   组内按比例缩放到 industry_max
         c. 相关性去重 剔除与已保留标的相关性超max_corr 的候选
         d. 重新归一   sum(w) = 1（缩放, 不截断）
    3. 输出权重; 若约束过紧导致 sum(w) < 1, 剩余为现金并如实报告

不引入 cvxpy/风险平价/Black-Litterman 的理由:
组合层缺的不是最优解求解器, 而是**约束表达与资金守恒**。在 10 只量级上,
迭代投影与均值-方差的结果差异远小于数据噪声, 而引入求解器会带来
跨平台安装负担（Windows/Linux、PyInstaller 冻结）与性能不确定性。
真需要协方差最优化时, 应先证明投影方案在真实数据上不够用。

行业分类的可用性（重要）:

`data/instruments/instruments.parquet` 共 5882 行 13 列
(symbol/name/code/exchange/region/type/listing_date/total_shares/
float_shares/tick_size/limit_up/limit_down/as_of) —— **无 industry 字段**。

行业成员来自 `services/rps_rotation._load_concept_map_df(repo, "industry")`,
依赖运行时数据源插件 + 600s 缓存, 回测是纯离线重放不应联网查插件。
因此 `category_of` 采用**可注入 + 可缺省**设计:
不传则行业约束自动降级为不启用; 映射返回 None 的标的视为「不属于任何已知
行业」不参与分组。分类缺失只让约束变弱, 绝不让回测失败 ——
这是「fail-open 于约束强度, fail-closed 于数据正确性」。

相关性口径: `correlation_matrix` 从收益率矩阵（行=时间, 列=标的）算截面
Pearson。零方差序列相关度定义为 0（而非 NaN）—— NaN 会静默污染去重判定。
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

# 权重投影的浮点容差。低于此差值视为已收敛（避免为极小残差空转迭代）。
_EPS = 1e-9


@dataclass(frozen=True)
class Candidate:
    """一个买入候选。

    asset_id: 调用方内部的整数标识（矩阵路径下是资产下标, 标量路径下是行号）。
    symbol:业务标识, 用于分类/相关性查询与结果可读性。
    score: 策略评分。score_weight 档位下按其比例分配权重。
    """

    asset_id: int
    symbol: str
    score: float


@dataclass(frozen=True)
class DroppedCandidate:
    """被约束剔除的候选 —— 回答「为什么这只没买」, 供 UI 与归因使用。"""

    symbol: str
    reason: str
    detail: str = ""


@dataclass(frozen=True)
class PortfolioConstraintSpec:
    """组合约束参数快照（纯数据, 便于快照测试与跨层传递）。

    默认全关: 所有约束为 None 且 position_sizing="equal" 时,
    `build_target_weights` 的输出与现状 engine 的等权逻辑**逐位一致**,
    保证存量回测不因引入本模块而回归。
    """

    # 权重分配档位: "equal" 现状等权 / "score_weight" 按评分比例 /
    # "constrained" 走完整约束投影（仅在此档下max_weight 等才生效）。
    position_sizing: str = "equal"

    # 单票权重上限（占组合总资产比例）。None = 不限。
    # 注意: 该上限作用于**目标权重**, 与撮合器的整手取整无关。
    max_weight: float | None = None

    # 单一行业暴露上限。None = 不限。需配合 category_of 使用。
    industry_max: float | None = None

    # 两两相关性上限。超过则剔除评分较低者（保留信息量更大的高分标的）。
    # 需配合 correlation_of 使用。None = 不去重。
    max_corr: float | None = None

    # 可注入的分类/相关性提供者:
    #   category_of(symbol) -> 行业名 | None
    #   correlation_of(symbol) -> {symbol: float} | None（相关性矩阵的一行）
    category_of: Callable[[str], str | None] | None = None
    correlation_of: Callable[[str], Mapping[str, float] | None] | None = None

    # 投影迭代上限。极端参数下用于保证收敛（不会死循环）。
    max_iter: int = 10

    # 组合预算（元）。None 时只产出权重不折算金额（equity 由撮合器管辖）。
    total_budget: float | None = None

    def has_constraints(self) -> bool:
        """是否含任何需要投影的约束。equal 且无上限时走快速路径。"""
        return any(v is not None for v in (self.max_weight, self.industry_max, self.max_corr))


@dataclass(frozen=True)
class WeightResult:
    """权重投影结果。

    weights: asset_id -> 目标权重（占组合总资产比例）。被剔除者不在其中。
    selected: 实际保留的候选（与 weights 同序, 便于展示）。
    dropped: 被剔除的候选及原因。
    total_weight: 权重之和。约束过紧时 < 1, 差额为现金（不隐式截断）。
    allocated_value: total_budget * total_weight（budget 为 None 时为 0）。
    iterations: 实际迭代次数（用于诊断收敛性）。
    """

    weights: dict[int, float] = field(default_factory=dict)
    selected: tuple[Candidate, ...] = ()
    dropped: tuple[DroppedCandidate, ...] = ()
    total_weight: float = 0.0
    allocated_value: float = 0.0
    iterations: int = 0


def correlation_matrix(
    symbols: Sequence[str],
    returns: np.ndarray,
) -> dict[str, dict[str, float]]:
    """从收益率矩阵算截面 Pearson 相关。

    returns: 形状 (n_periods, n_symbols), 行=时间, 列=与 symbols 对齐。
    零方差序列的相关度定义为 0.0 而非 NaN —— NaN 会让去重判定静默失效
    (`NaN > max_corr` 恒为 False, 于是所有高相关对都会被放过)。

    返回 {symbol: {symbol: corr}}; 对称, 对角线为 1.0（除零方差自身为 0）。
    """
    arr = np.asarray(returns, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"returns 必须是二维 (时间x标的), 实际维度 {arr.ndim}")
    n_syms = arr.shape[1]
    if n_syms != len(symbols):
        raise ValueError(f"returns 列数 {arr.shape[1]} 与 symbols 长度 {len(symbols)} 不一致")

    # 各标的方差; 为 0 的维度单独记下来, 相关度按 0 处理。
    std = arr.std(axis=0)
    safe_std = np.where(std > _EPS, std, 1.0)
    # (n_syms, n_periods) @ (n_periods, n_syms) -> (n_syms, n_syms)
    centered = (arr - arr.mean(axis=0)).T
    corr = centered @ centered.T / (n_periods := arr.shape[0])
    corr = corr / np.outer(safe_std, safe_std)

    degenerate = std <= _EPS
    if degenerate.any():
        corr[degenerate, :] = 0.0
        corr[:, degenerate] = 0.0
    # 数值噪声可能让相关度略微越界 [-1, 1]。
    corr = np.clip(corr, -1.0, 1.0)
    # 对角线：对有方差的标的强制 1.0, 零方差保持 0.0。
    good = ~degenerate
    idx = np.arange(n_syms)
    corr[idx[good], idx[good]] = 1.0

    return {
        sym: {s2: float(corr[i, j]) for j, s2 in enumerate(symbols)}
        for i, sym in enumerate(symbols)
    }


def build_target_weights(
    candidates: Sequence[Candidate],
    spec: PortfolioConstraintSpec,
    *,
    current_weights: Mapping[int, float] | None = None,
    correlation_lookup: Callable[[str, str], float | None] | None = None,
) -> WeightResult:
    """把候选 + 评分投影为合规的目标权重。

    candidates 须已按评分降序排列（调用方负责排序：本模块不重排, 保持
    「先到先得」的取舍语义, 与现状 `candidates.sort(...)` 后的行为一致）。

    current_weights: 已在持仓的asset_id -> 当前权重。用于让总账包含存量,
    避免把「已重仓的票」当作未配置处理。默认 None = 空仓视角。

    correlation_lookup: 可选的二元相关查询 (a, b) -> corr, 用于对称补齐
    `correlation_of` 只给出单行的情况（真实数据源常只提供候选自身那一行）。
    两者都缺失时该对视为不相关（保守保留, 不误杀候选）。
    """
    current = dict(current_weights or {})

    # ---- 1. 去重并保序（重复 asset_id 或 symbol 只保留首个 = 评分最高的）----
    # 两个维度都要查: 矩阵路径下 asset_id 是资产下标, 标量路径下是行号,
    # 同一 symbol 可能对应不同 asset_id（数据源重复），但买入只能算一次。
    seen_ids: set[int] = set()
    seen_symbols: set[str] = set()
    ordered: list[Candidate] = []
    duplicates: list[DroppedCandidate] = []
    for cand in candidates:
        if cand.asset_id in seen_ids or cand.symbol in seen_symbols:
            duplicates.append(
                DroppedCandidate(symbol=cand.symbol, reason="duplicate", detail="标的重复")
            )
            continue
        seen_ids.add(cand.asset_id)
        seen_symbols.add(cand.symbol)
        ordered.append(cand)

    if not ordered:
        return WeightResult(
            dropped=tuple(duplicates), total_weight=0.0, allocated_value=0.0
        )

    # ---- 2. 初始化权重 ----
    weights = _initial_weights(ordered, spec)

    # ---- 3. 相关性去重（先做, 减少后续投影的规模）----
    kept, dropped = _apply_correlation_dedup(ordered, spec, correlation_lookup)
    dropped = duplicates + dropped

    # 剔除后重新初始化权重（去重可能已改变成员集合）。
    if len(kept) != len(ordered):
        weights = _initial_weights(kept, spec)

    # ---- 4. 迭代投影（单票上限 + 行业上限 + 归一）----
    iterations = 0
    if _needs_projection(kept, spec):
        weights, iterations = _project(kept, spec, weights)

    # ---- 5. 叠加存量后收尾 ----
    for asset_id, w in current.items():
        if asset_id not in weights:
            weights[asset_id] = w

    total = float(sum(weights.values()))
    allocated = (spec.total_budget * total) if spec.total_budget is not None else 0.0
    kept_ids = {c.asset_id for c in kept}
    return WeightResult(
        weights=weights,
        selected=tuple(c for c in kept),
        dropped=tuple(dropped),
        total_weight=total,
        allocated_value=allocated,
        iterations=iterations,
    )


# ================================================================
# 内部实现
# ================================================================

def _initial_weights(
    candidates: Sequence[Candidate],
    spec: PortfolioConstraintSpec,
) -> dict[int, float]:
    """按档位产出初始权重（已归一到 sum=1）。"""
    if spec.position_sizing == "score_weight" and len(candidates) > 1:
        raw = np.array([max(c.score, 0.0) for c in candidates], dtype=float)
        if raw.sum() > _EPS:
            w = raw / raw.sum()
            return {c.asset_id: float(w[i]) for i, c in enumerate(candidates)}
        # 评分全零或全负 → score_weight 无意义, 降级等权而非崩溃/全零。
    n = len(candidates)
    return {c.asset_id: 1.0 / n for c in candidates}


def _needs_projection(
    candidates: Sequence[Candidate],
    spec: PortfolioConstraintSpec,
) -> bool:
    """是否需要迭代投影。无上限约束时直接返回（快速路径）。"""
    if not candidates:
        return False
    return spec.max_weight is not None or spec.industry_max is not None


def _project(
    candidates: Sequence[Candidate],
    spec: PortfolioConstraintSpec,
    weights: dict[int, float],
) -> tuple[dict[int, float], int]:
    """迭代投影: 反复「封顶 → 归一」直至收敛或触达迭代上限。"""
    categories = _category_map(candidates, spec)
    iterations = 0
    for iterations in range(1, max(int(spec.max_iter), 1) + 1):
        before = sum(weights.values())

        categories = _category_map(candidates, spec)
    iterations = 0
    for iterations in range(1, max(int(spec.max_iter), 1) + 1):
        before = sum(weights.values())

        # 每轮都重算「当前可用的自由额度」。关键: 归一不能破坏封顶 ——
        # 全局缩放会把刚封顶的值重新推高(实测 0.15 上限被归一成 0.2405)。
        # 正确做法是分组水填充: 被封顶的组保持上限, 剩余额度由自由组按比例瓜分。
        weights = _waterfill(weights, categories, spec)

        if abs(sum(weights.values()) - before) <= _EPS:
            break

    return weights, iterations


def _waterfill(
    weights: dict[int, float],
    categories: dict[int, str],
    spec: PortfolioConstraintSpec,
) -> dict[int, float]:
    """带上限的水填充投影 —— 满足所有上限约束且总额尽可能接近 1。

    流程:
      1. 单票封顶 (硬上限, 触顶者不再参与瓜分)
      2. 行业封顶 (组内按比例缩放到 industry_max, 组内相对比例不变)
      3. 水填充: 把「剩余额度」按当前比例分给未触顶的单票, 直至用尽或全部触顶

    与「封顶后直接归一」的本质区别: 归一是全局等比缩放, 会让封顶值越界;
    水填充只给还有空间的标的加仓, 因此上限始终成立。

    若所有标的都触顶 (max_weight * n < 1), 总额 < 1 —— 此时差额为现金,
    如实返回而不是突破上限。这是「上限优先于满仓」的业务取舍。
    """
    if not weights:
        return weights

    single_cap = (
        max(float(spec.max_weight), 0.0) if spec.max_weight is not None else None
    )
    industry_cap = (
        max(float(spec.industry_max), 0.0) if spec.industry_max is not None else None
    )

    out = dict(weights)

    # ---- 1. 单票硬封顶 ----
    if single_cap is not None:
        for k in out:
            if out[k] > single_cap:
                out[k] = single_cap

    # ---- 2. 行业封顶 ----
    if industry_cap is not None and categories:
        out = _cap_industries(out, categories, industry_cap)

    # ---- 3. 水填充: 用满剩余额度 ----
    # target 是「当前可达的最大总额」: 触顶票算上限, 未触顶票不设限。
    # 若 target <= 当前总额说明已满, 直接返回。
    for _ in range(64):  # 内层迭代上限, 防御性(每次至少收敛一票)
        total = sum(out.values())
        capped = {
            k for k in out
            if single_cap is not None and out[k] >= single_cap - _EPS
        }
        free = [k for k in out if k not in capped]
        if not free:
            break

        # 每个未触顶标的还能再吃多少。
        headroom = {}
        for k in free:
            room = float("inf") if single_cap is None else single_cap - out[k]
            # 行业上限下还需留出行业剩余额度; 简化处理: 行业组已达上限时
            # 该组内所有成员都不再瓜分。
            name = categories.get(k)
            if name is not None and industry_cap is not None:
                group_total = sum(out[j] for j in out if categories.get(j) == name)
                room = min(room, max(industry_cap - group_total, 0.0))
            if room > _EPS:
                headroom[k] = room

        if not headroom:
            break

        need = max(1.0 - total, 0.0)
        if need <= _EPS:
            break

        # 按当前权重比例分配 need, 但不越过各自 headroom。
        denom = sum(out[k] for k in headroom)
        if denom <= _EPS:
            # 自由组权重全为 0 → 均分。
            share = need / len(headroom)
            for k in headroom:
                out[k] += min(share, headroom[k])
            continue

        for k in headroom:
            out[k] += min(out[k] / denom * need, headroom[k])

    return out


def _category_map(
    candidates: Sequence[Candidate],
    spec: PortfolioConstraintSpec,
) -> dict[int, str]:
    """解析 asset_id -> 行业名。无 provider 或全部未命中时返回空 dict。"""
    if spec.category_of is None or spec.industry_max is None:
        return {}
    out: dict[int, str] = {}
    for cand in candidates:
        name = spec.category_of(cand.symbol)
        if isinstance(name, str) and name:
            out[cand.asset_id] = name
    return out


def _cap_industries(
    weights: dict[int, float],
    categories: dict[int, str],
    cap: float,
) -> dict[int, float]:
    """把每个行业组的权重和压到cap 以内（组内按比例缩放）。"""
    groups: dict[str, list[int]] = {}
    for asset_id, name in categories.items():
        if asset_id in weights:
            groups.setdefault(name, []).append(asset_id)

    for members in groups.values():
        total = sum(weights[a] for a in members)
        if total > cap + _EPS and total > _EPS:
            scale = cap / total
            for a in members:
                weights[a] *= scale
    return weights


def _pair_corr(
    cand_symbol: str,
    kept_symbol: str,
    spec: PortfolioConstraintSpec,
    lookup: Callable[[str, str], float | None] | None,
) -> float | None:
    """取两个标的间的相关度, 按可靠性依次降级。

    真实数据源常只提供「候选自身那一行」的相关度（不含已保留标的），
    因此三级降级: correlation_of 单行 → correlation_lookup 二元查询 → None。
    三级都拿不到时返回 None, 调用方按「不相关」处理 —— 保守保留候选,
    宁可少去重也不误杀（误杀会静默减少可投资产, 比漏去重更难发现）。
    """
    if spec.correlation_of is not None:
        row = spec.correlation_of(cand_symbol)
        if row:
            value = row.get(kept_symbol)
            if value is not None and np.isfinite(value):
                return float(value)
    if lookup is not None:
        value = lookup(cand_symbol, kept_symbol)
        if value is not None and np.isfinite(value):
            return float(value)
    return None


def _apply_correlation_dedup(
    candidates: Sequence[Candidate],
    spec: PortfolioConstraintSpec,
    lookup: Callable[[str, str], float | None] | None = None,
) -> tuple[list[Candidate], list[DroppedCandidate]]:
    """按评分优先保留, 剔除与已保留标的相关性超阈值的候选。

    只看「排在前面且已保留」的标的, 因此结果与候选顺序强相关 ——
    这正是想要的语义: 高分标的优先占用名额。
    """
    if spec.correlation_of is None and lookup is None:
        return list(candidates), []
    if spec.max_corr is None:
        return list(candidates), []

    threshold = float(spec.max_corr)
    kept: list[Candidate] = []
    dropped: list[DroppedCandidate] = []
    kept_syms: list[str] = []

    for cand in candidates:
        conflict_with = None
        for kept_sym in kept_syms:
            corr = _pair_corr(cand.symbol, kept_sym, spec, lookup)
            if corr is None:
                continue
            if abs(corr) > threshold:
                conflict_with = kept_sym
                break

        if conflict_with is None:
            kept.append(cand)
            kept_syms.append(cand.symbol)
        else:
            dropped.append(
                DroppedCandidate(
                    symbol=cand.symbol,
                    reason="correlation",
                    detail=f"与 {conflict_with} 相关性超 {threshold}",
                )
            )

    return kept, dropped