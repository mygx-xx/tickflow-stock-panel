"""enriched 重建模式决策 —— 先探测差异, 再决定全量还是增量。

背景
----
`run_pipeline` 早已支持三种模式 (全量 / new_dates_only 增量 / 按 symbols 局部重算),
日终管道 (`app/jobs/daily_pipeline.py`) 也已按"首次·往前扩展·往后新增·仅除权变更"
四态自动选择。但 `POST /api/pipeline/rebuild_enriched` 这个手动运维接口
**无条件走全量** —— 在已有 1454 天 / 720 万行的库上, 一次点击就是 49 秒 + 全量重写,
而实际往往只想补一天。

为什么不能"直接用增量就行"
------------------------
增量的正确性依赖两个前提, 满足不了就会静默产出**不一致的 enriched**:
1. 已有的日期分区必须是可信的。若某天算错了(算力中断/除权因子后来才补),
   增量只补新日期, 错的那天永远不会被修正。
2. 除权因子链不能发生"影响历史比例"的变化。有新除权因子时, 受影响个股的**全部历史日期**
   都要重算, 增量模式必须显式带上 symbols, 否则新分区与旧分区的复权口径会打架。

所以本模块只做一件事: **如实算出"这次该用哪个模式、依据是什么"**, 把判断依据一并返回,
让调用方(端点/日志/前端)能解释, 而不是给一个来源不明的模式名。
决策逻辑不碰任何数据, 是纯函数, 可直接单测。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# 局部重算(仅除权因子变更)的个股数超过此值时, 逐只重算全部历史会比全量还慢,
# 因为局部路径要按 symbol 反复读取 parquet。此时改走全量。
LOCAL_RECOMPUTE_SYMBOL_THRESHOLD = 200


def _partition_dates(root: Path) -> set[str]:
    """列出 ``date=YYYY-MM-DD`` 分区目录的日期集合。"""
    if not root.exists():
        return set()
    out: set[str] = set()
    for p in root.glob("date=*"):
        if not p.is_dir():
            continue
        _, _, ds = p.name.partition("=")
        if ds:
            out.add(ds)
    return out


def _adj_factor_dates(factor_path: Path) -> set[str]:
    """复权因子文件里出现的日期集合 (用于判断是否影响历史比例)。"""
    if not factor_path.exists():
        return set()
    try:
        import polars as pl

        lf = pl.scan_parquet(str(factor_path))
        if "date" not in lf.collect_schema().names():
            return set()
        return set(lf.select("date").unique().collect().get_column("date").to_list())
    except Exception as e:  # noqa: BLE001
        # 读不到就当"可能变了", 交由调用方走保守路径
        logger.warning("读取复权因子日期失败 %s: %s", factor_path, e)
        return set()


def plan_rebuild(
    data_dir: Path,
    *,
    symbols: list[str] | None = None,
    affected_symbols: list[str] | None = None,
    publication_incomplete: bool = False,
    force_full: bool = False,
) -> dict[str, Any]:
    """探测 daily / enriched 分区差异, 给出重建计划 (纯函数, 不写任何文件)。

    返回::

        {
          "mode": "full" | "forward" | "local" | "noop",
          "reason": str,               # 人可读依据, 会进日志与接口返回
          "run_kwargs": {...},         # 直接可展开传给 run_pipeline 的参数
          "daily_days": int,
          "enriched_days": int,
          "missing_dates": [...],      # daily 有而 enriched 缺 (漏算)
          "orphan_dates": [...],       # enriched 有而 daily 缺 (孤儿, 增量删不掉)
          "earliest_missing": str|None,
          "affected_symbols": int,
          "savings": str,              # 相对全量的定性描述
        }
    """
    d = Path(data_dir)
    daily_dir = d / "kline_daily"
    enriched_dir = d / "kline_daily_enriched"
    factor_path = d / "adj_factor" / "all.parquet"

    daily = _partition_dates(daily_dir)
    enriched = _partition_dates(enriched_dir)

    missing = sorted(daily - enriched)      # 漏算: 增量必须补
    orphan = sorted(enriched - daily)       # 孤儿: enriched 里 daily 已无, 增量模式不会清理
    sym_list = sorted(set(affected_symbols or symbols or []))

    base: dict[str, Any] = {
        "daily_days": len(daily),
        "enriched_days": len(enriched),
        "missing_dates": missing[:50],
        "missing_total": len(missing),
        "orphan_dates": orphan[:20],
        "orphan_total": len(orphan),
        "earliest_missing": missing[0] if missing else None,
        "affected_symbols": len(sym_list),
        "adj_dates": len(_adj_factor_dates(factor_path)),
    }

    def done(mode: str, reason: str, savings: str, **kw: Any) -> dict[str, Any]:
        return {**base, "mode": mode, "reason": reason, "savings": savings, "run_kwargs": kw}

    # ── 1. 显式要求全量 / 发布未完成 → 全量 ──
    if force_full:
        return done("full", "调用方显式要求全量", "全量重写", new_dates_only=False, symbols=None)

    if publication_incomplete:
        # run_pipeline 内部也会因此降级为全量, 这里提前说清楚, 免得日志显示"增量"实际全量
        return done(
            "full",
            "上一次 enriched 发布未完成, 增量会与半成品混合",
            "全量重写",
            new_dates_only=False,
            symbols=None,
        )

    # ── 2. 首次建库 → 全量 ──
    if not enriched:
        return done(
            "full",
            "enriched 为空 (首次建库)",
            "全量重写",
            new_dates_only=False,
            symbols=None,
        )

    # ── 3. daily 数据为��� → 无事可做 ──
    if not daily:
        return done("noop", "无日K数据", "无需重算", new_dates_only=True, symbols=None)

    # ── 4. 往前扩展(缺口不在末尾连续段) → 必须全量 ──
    # 增量模式只认"enriched 最后一天之后的连续新日期"。若缺失区间里
    # 存在 <= enriched 最大日期的日子, 说明中间有洞 —— 增量看不见, 会永远留着。
    # 注意不能写成 `missing[0] != max(enriched)`: 末尾新增时 missing[0] 恰恰
    # 就是 max(enriched) 的次日, 那个判断会把正常的末尾新增误判成向前缺口。
    if missing:
        enriched_max = max(enriched)
        backward = [m for m in missing if m <= enriched_max]
        if backward:
            return done(
                "full",
                f"存在向前的缺口 (最早 {backward[0]}, enriched 末尾 {enriched_max}) —— "
                "增量只补末尾, 中间缺口需全量才能填上",
                "全量重写",
                new_dates_only=False,
                symbols=None,
            )

    # ── 5. 局部重算个股过多 → 全量更快 ──
    if sym_list and not missing and len(sym_list) > LOCAL_RECOMPUTE_SYMBOL_THRESHOLD:
        return done(
            "full",
            f"{len(sym_list)} 只个股除权因子变更 (> {LOCAL_RECOMPUTE_SYMBOL_THRESHOLD}) —— "
            "逐只重算全部历史比全量更慢",
            "全量重写",
            new_dates_only=False,
            symbols=None,
        )

    # ── 6. 末尾新增日期 → 向前增量 (可带受影响个股) ──
    if missing:
        reason = f"新增 {len(missing)} 个日期分区 ({missing[0]}…{missing[-1]})"
        if sym_list:
            reason += f", 另有 {len(sym_list)} 只个股除权因子变更需重算全部历史"
        return done(
            "forward",
            reason,
            f"只算 {len(missing)}/{len(daily)} 天" + (f" + {len(sym_list)} 只个股全历史" if sym_list else ""),
            new_dates_only=True,
            symbols=sym_list or None,
        )

    # ── 7. 仅除权因子变更 → 局部重算 ──
    if sym_list:
        return done(
            "local",
            f"无新日期, {len(sym_list)} 只个股除权因子变更",
            f"只重算 {len(sym_list)} 只个股",
            new_dates_only=False,
            symbols=sym_list,
        )

    # ── 8. 孤儿分区: 增量模式不会清理, 如实报告而非假装无事 ──
    if orphan:
        return done(
            "noop",
            f"daily 与 enriched 已对齐, 但 enriched 多出 {len(orphan)} 个孤儿分区 "
            f"(如 {orphan[0]}) —— 增量模式不删分区, 需人工确认或走全量重建",
            "无需重算",
            new_dates_only=True,
            symbols=None,
        )

    return done("noop", "daily 与 enriched 完全对齐, 无需重算", "无需重算", new_dates_only=True, symbols=None)
