"""系统自检 — 把"服务/数据/策略/缓存/监控"健康度聚合成一次请求。

为什么要有这个端点: 排查"页面没数据/策略不更新/服务挂了"时, 原本要分别打
/health、/api/data/status、/api/strategies、/api/screener/cached-summary、
/api/monitor-rules 五个接口, 再人工拼时间戳对比。前端做聚合要么串 5 个请求,
要么只能显示第一个成功的结果 —— 都不利于定位。这里一次返回, 且任何子检查
失败都降级为该��的 error 字段, 绝不让整页 500。
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/diagnostics", tags=["diagnostics"])

# 进程启动时刻 (用于算 uptime)
_BOOT_TS = time.time()


def _app_version() -> str:
    try:
        from app import __version__

        return __version__
    except Exception:  # noqa: BLE001
        return "unknown"


def _check(name: str, fn) -> tuple[dict, str | None]:
    """跑一个子检查; 异常降级为 {'error': msg}, 不影响其它检查。"""
    try:
        return fn(), None
    except Exception as e:  # noqa: BLE001
        logger.warning("诊断子项失败 %s: %s", name, e)
        return {"error": f"{type(e).__name__}: {e}"[:200]}, f"{type(e).__name__}: {e}"[:200]


def _server() -> dict:
    return {
        "ok": True,
        "app_version": _app_version(),
        "python": f"{__import__('sys').version_info.major}.{__import__('sys').version_info.minor}",
        "uptime_sec": int(time.time() - _BOOT_TS),
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def _data(request: Request) -> dict:
    """复用 /api/data/status 的实现 (它只依赖 request.app.state), 口径与数据页完全一致。

    不自己再扫一遍 parquet: 一旦 _safe_aggregate_* 的统计口径调整, 自检页会
    悄悄显示过期数字, 而数据页显示新的 —— 这种不一致比没有自检更糟。
    """
    from app.api import data as data_api

    status = data_api.status(request)
    latest = max(
        (str(v.get("latest_date") or "") for v in status.values() if isinstance(v, dict)),
        default="",
    )
    datasets = [
        {
            "name": k,
            "rows": v.get("rows"),
            "symbols": v.get("symbols_covered"),
            "latest_date": v.get("latest_date"),
            "trading_days": v.get("trading_days"),
        }
        for k, v in status.items()
        if isinstance(v, dict)
    ]
    problems: list[str] = []
    if not latest:
        problems.append("没有任何数据集的最新日期")
    else:
        lag = _lag_days(latest)
        # 7 天以内属正常 (含周末与节假日); 超过多半是盘后管道没跑
        if lag is not None and lag > 10:
            problems.append(f"数据最新日期 {latest} 距今 {lag} 天, 盘后管道可能未运行")
    # 注: 不把 rows==0 列为问题 —— data/status 的行数走聚合缓存, 数据未变更时
    # 会停在 0(与 latest_date 矛盾), 报出来是误报, 会掩盖真问题。
    return {
        "latest_date": latest or None,
        "lag_days": _lag_days(latest),
        "datasets": datasets,
        "problems": problems,
    }


def _lag_days(latest: str | None) -> int | None:
    if not latest:
        return None
    try:
        d = date.fromisoformat(str(latest)[:10])
    except ValueError:
        return None
    return (date.today() - d).days


def _strategies(request: Request) -> dict:
    from app.strategy.lifecycle import describe_status, is_selectable, normalize_status

    engine = getattr(request.app.state, "strategy_engine", None)
    if engine is None:
        return {"error": "策略引擎未初始化", "total": 0, "by_status": {}}
    metas = engine.list_strategies(include_research=True)
    by_status: dict[str, int] = {}
    by_source: dict[str, int] = {}
    selectable = 0
    for m in metas:
        # 状态口径必须与 /api/strategies 一致 (未声明 → draft), 否则自检页
        # 显示"全部 unknown"而策略页显示 draft, 排查时两边对不上。
        st = normalize_status(m.get("status"))
        by_status[st] = by_status.get(st, 0) + 1
        by_source[m.get("source", "unknown")] = by_source.get(m.get("source", "unknown"), 0) + 1
        if is_selectable(st):
            selectable += 1
    return {
        "total": len(metas),
        "by_status": by_status,
        "by_source": by_source,
        "status_labels": {k: describe_status(k) for k in by_status},
        "selectable": selectable,
        "research_only": sum(1 for m in metas if m.get("research_only")),
        "load_errors": [
            str(e)[:200] for e in (engine.load_errors() or [])
        ][:20],
    }


def _cache(data_dir: Path) -> dict:
    from app.services import strategy_cache

    cached = strategy_cache.read_cache(data_dir)
    if cached is None:
        return {"exists": False, "note": "策略结果缓存为空 (策略页会回退到全量重算)"}
    updated_at = cached.get("updated_at")
    age_min = None
    if isinstance(updated_at, (int, float)):
        age_min = int((time.time() * 1000 - updated_at) / 60000)
    ever = cached.get("today_ever_matched") or {}
    return {
        "exists": True,
        "as_of": cached.get("as_of"),
        "lag_days": _lag_days(cached.get("as_of")),
        "updated_at": updated_at,
        "age_minutes": age_min,
        "strategies": len(cached.get("results") or {}),
        "ever_matched_symbols": sum(len(v) for v in ever.values()),
        "file_mb": round(_cache_file_mb(data_dir), 2),
    }


def _cache_file_mb(data_dir: Path) -> float:
    p = data_dir / "user_data" / "strategy_cache.json"
    try:
        return p.stat().st_size / 1024 / 1024
    except OSError:
        return 0.0


def _monitor(data_dir: Path) -> dict:
    from app.strategy import monitor_rules

    rules = monitor_rules.load_all(data_dir)
    strat = [r for r in rules if r.get("type") == "strategy"]
    return {
        "rules": len(rules),
        "enabled": sum(1 for r in rules if r.get("enabled")),
        "strategy_rules": len(strat),
        "by_type": {
            t: sum(1 for r in rules if r.get("type") == t)
            for t in sorted({str(r.get("type")) for r in rules})
        },
    }


def _auto_follow(data_dir: Path) -> dict:
    """自动跟单规则数 (规则文件按账户分目录, 这里只统计文件数)。"""
    base = data_dir / "user_data" / "paper"
    if not base.exists():
        return {"accounts": 0, "rule_files": 0}
    files = list(base.glob("*/auto_rules/*.json"))
    return {"accounts": sum(1 for p in base.iterdir() if p.is_dir()), "rule_files": len(files)}


@router.get("")
def diagnostics(request: Request) -> dict:
    """一次拿全系统健康度。子项失败只标记该项 error, 不影响其余。"""
    data_dir = request.app.state.repo.store.data_dir
    checks = {
        "server": _server,
        "data": lambda: _data(request),
        "strategies": lambda: _strategies(request),
        "cache": lambda: _cache(data_dir),
        "monitor": lambda: _monitor(data_dir),
        "auto_follow": lambda: _auto_follow(data_dir),
    }
    out: dict[str, Any] = {}
    failures: list[str] = []
    for name, fn in checks.items():
        payload, err = _check(name, fn)
        out[name] = payload
        if err:
            failures.append(f"{name}: {err}")
    out["failures"] = failures
    out["healthy"] = not failures
    return out


# ── 策略自述验证结论 ────────────────────────────────────────────────────
# 独立 prefix 而非挂在 router 上: /api/diagnostics 的语义是"本机健康度",
# 而这是"策略库里记录的验证结论", 生命周期不同, 也便于独立演进。


verdict_router = APIRouter(prefix="/api/strategy-verdicts", tags=["strategies"])


@verdict_router.get("")
def strategy_verdicts_report(request: Request):
    """返回清单里记录的自述验证结论 + 统计。

    注意口径: 这是**清单里人工/脚本汇总的自述结论**, 不是本系统跑的回测。
    前端必须如实标注来源, 不得呈现为"已验证"(含 verified 标记)。
    """
    from app.services import strategy_verdicts as svc

    data_dir: Path = request.app.state.repo.store.data_dir
    engine = getattr(request.app.state, "strategy_engine", None)
    known: set[str] | None = None
    if engine is not None:
        try:
            known = {m["id"] for m in engine.list_strategies(include_research=True)}
        except Exception as e:  # noqa: BLE001
            logger.warning("读取已加载策略 id 失败: %s", e)
            known = None
    return svc.summarize(data_dir, known)
