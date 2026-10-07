"""walkforward 结果的轻量落盘缓存 — `data/strategy_lifecycle/{strategy_id}.json`。

## 为什么需要它

`WalkForwardService.run()` 的结果**只经 SSE 流返回，从不落盘**
（`app/api/backtest.py` 里无任何 save/write，`backtest_results/` 目录
也只存普通回测的 parquet 汇总）。这让「基于绩效的自动降级」无从落地:

- 后台巡检任务**读不到**任何历史 walkforward 结果 → 每次判定都因
  「无数据」弃权 → 判据静默失效(不报错, 只是永不触发)。
- 用户手动跑一次 walkforward 后关掉页面, 结果就丢了。

故补一层最小落盘: 只存判定必需的字段, 不存逐折明细(folds 里的
`oos_stats` 可能有大量序列, 全量落盘会快速膨胀)。

## 字段裁剪的取舍

保留: objective / direction / summary(判定直接依赖 degradation 与
一致性) / 每折的 n_trades(PSR 的 n_obs 要靠求和, 不能丢)。
丢弃: `cache_telemetry` / `shared_market_data*` / `elapsed_ms`
(遥测与运行时信息, 判定用不到)、`best_params`(参数快照, 对生命周期判定
无意义)。

`oos_stats` 只留 `n_trades` —— 这是唯一进判定的字段, 其余(逐笔序列等)
一律丢弃, 单文件因此稳定在KB 级。

## 单文件损坏只跳过(对齐 CONTRIBUTING 插件隔离要求)

与 factors/store.load_all 一致: 坏JSON 只告警并返回 None, 不抛异常中断
巡检 —— 一个策略的缓存损坏不该让整个巡检任务失败。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from app.services.fs_utils import atomic_write_text

logger = logging.getLogger(__name__)

__all__ = ["save_walkforward_result", "load_walkforward_result", "clear_result"]


def _dir(data_dir: Path) -> Path:
    directory = data_dir / "strategy_lifecycle"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _path(data_dir: Path, strategy_id: str) -> Path:
    return _dir(data_dir) / f"{strategy_id}.json"


def _slim_folds(folds: list[dict] | None) -> list[dict]:
    """只留判定需要的每折字段。

    `oos_stats` 里除 n_trades 外一律丢弃 —— 逐折 stats 可能含净值序列/
    逐笔明细, 全留会让单文件随折数与标的数快速膨胀。
    """
    slim: list[dict] = []
    for fold in folds or []:
        if not isinstance(fold, dict):
            continue
        stats = fold.get("oos_stats") or {}
        n_trades = stats.get("n_trades")
        slim.append(
            {
                "index": fold.get("index"),
                "test_end": fold.get("test_end"),
                "is_score": fold.get("is_score"),
                "oos_objective": fold.get("oos_objective"),
                "oos_degraded": fold.get("oos_degraded"),
                "oos_stats": {"n_trades": n_trades} if n_trades is not None else {},
            }
        )
    return slim


#: summary 里允许落盘的字段白名单。
#: 不能整体透传 —— `aggregate_oos` 返回的 `oos_equity_curve` 是逐折净值序列,
#: 折数 × 标的数规模, 落盘会让单文件膨胀到 MB 级; 而判定只需要标量。
SUMMARY_KEEP_FIELDS = (
    "n_folds",
    "avg_is_objective",
    "avg_oos_objective",
    "degradation",
    "consistency",
)


def _slim_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    """按白名单裁剪 summary, 丢弃 oos_equity_curve 等序列字段。"""
    raw = summary or {}
    return {k: raw[k] for k in SUMMARY_KEEP_FIELDS if k in raw}


def save_walkforward_result(data_dir: Path, strategy_id: str, result: dict[str, Any]) -> Path | None:
    """把 walkforward 结果裁剪后落盘。失败返回 None(不抛, 巡检继续)。"""
    if not result:
        return None
    try:
        payload = {
            "strategy_id": strategy_id,
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "objective": result.get("objective"),
            "direction": result.get("direction"),
            "n_folds": result.get("n_folds"),
            "folds": _slim_folds(result.get("folds")),
            "summary": _slim_summary(result.get("summary")),
        }
        target = _path(data_dir, strategy_id)
        atomic_write_text(target, json.dumps(payload, ensure_ascii=False, indent=2))
        return target
    except Exception as exc:  # noqa: BLE001
        logger.warning("walkforward 结果落盘失败 %s: %s", strategy_id, exc)
        return None


def load_walkforward_result(data_dir: Path, strategy_id: str) -> dict[str, Any] | None:
    """读取缓存的 walkforward 结果; 不存在或损坏返回 None。

    返回的 dict 形状与 `WalkForwardService.run()` 一致(至少含 summary/folds),
    可直接喂给 `app.services.strategy_lifecycle.judge`。
    """
    target = _path(data_dir, strategy_id)
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("walkforward 缓存读取失败 %s: %s", target.name, exc)
        return None
    if not isinstance(payload, dict):
        return None
    # 补齐 judge 依赖的键; 旧版本缓存缺 folds 时给空列表而不是崩。
    payload.setdefault("folds", [])
    payload.setdefault("summary", {})
    return payload


def clear_result(data_dir: Path, strategy_id: str) -> bool:
    target = _path(data_dir, strategy_id)
    if target.exists():
        target.unlink()
        return True
    return False