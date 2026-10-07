"""命中日报 — 策略命中结果按日归档, 供「今天选了什么 / 昨天选的还在不在」对比。

背景: strategy_cache.json 只保留当日 (as_of) 的结果, 隔日即被覆盖, 无法回答
「相比上一交易日, 哪些票是新进的、哪些掉了」。本模块在缓存写入时顺手把当日的
曾命中集合按日期落一份归档, 日报接口据此做差分。

文件: data/user_data/strategy_hits/{as_of}.json
  {
    "date": "2026-09-30",
    "archived_at": 1705324800000,
    "matched": { strategy_id: [symbol, ...] }   # 当日曾命中 (与 strategy_cache.today_ever_matched 同源)
  }

纯文件存储, 一日一文件, 写入用临时文件 + os.replace 原子替换 (镜像 strategy_cache)。
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DIRNAME = "strategy_hits"


def _dir(data_dir: Path) -> Path:
    d = data_dir / "user_data" / _DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _path(data_dir: Path, as_of: str) -> Path:
    return _dir(data_dir) / f"{as_of}.json"


def archive(data_dir: Path, as_of: str, matched: dict[str, Any]) -> None:
    """把当日曾命中集合按日期归档。同日重复写入覆盖 (取并集后写入由调用方保证)。

    内容与已存归档一致时直接跳过: run_all 过程中 write_cache 会被调用多次
    (每个策略算完一次), 每次都重写同一份归档是白费的 IO。
    失败只记日志不抛: 归档是旁路能力, 不能因为它让策略缓存写入失败。
    """
    normalized = {sid: sorted(set(syms)) for sid, syms in matched.items()}
    path = _path(data_dir, as_of)
    existing = read(data_dir, as_of)
    if existing is not None and (existing.get("matched") or {}) == normalized:
        return
    payload = {
        "date": as_of,
        "archived_at": int(time.time() * 1000),
        "matched": normalized,
    }
    try:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
    except Exception as e:  # noqa: BLE001
        logger.warning("命中归档写入失败 %s: %s", as_of, e)


def list_dates(data_dir: Path) -> list[str]:
    """已归档的日期列表 (升序)。损坏文件跳过。"""
    try:
        return sorted(p.stem for p in _dir(data_dir).glob("*.json"))
    except Exception as e:  # noqa: BLE001
        logger.warning("命中归档目录读取失败: %s", e)
        return []


def read(data_dir: Path, as_of: str) -> dict | None:
    """读某日归档; 不存在或损坏返回 None。"""
    path = _path(data_dir, as_of)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning("命中归档读取失败 %s: %s", as_of, e)
        return None


def _prev_date(dates: list[str], as_of: str) -> str | None:
    """严格早于 as_of 的最近归档日。"""
    earlier = [d for d in dates if d < as_of]
    return earlier[-1] if earlier else None


def _backfill_from_cache(data_dir: Path, as_of: str) -> dict | None:
    """用 strategy_cache 的今日曾命中集合补一份归档。日期不匹配或无缓存则返回 None。"""
    try:
        from app.services import strategy_cache

        cached = strategy_cache.read_cache(data_dir)
    except Exception as e:  # noqa: BLE001
        logger.warning("命中归档补基准失败(读缓存): %s", e)
        return None
    if not cached or cached.get("as_of") != as_of:
        return None
    matched = cached.get("today_ever_matched") or {}
    if not matched:
        return None
    archive(data_dir, as_of, matched)
    logger.info("命中归档已用 strategy_cache 补基准: %s (%d 策略)", as_of, len(matched))
    return read(data_dir, as_of)


def daily_report(data_dir: Path, as_of: str, strategy_names: dict[str, str] | None = None) -> dict:
    """命中日报 = 当日各策略命中 + 相对上一归档日的新增/剔除。

    两层变动, 语义不同, 都要给:
    - intraday_drop: 当日曾命中但当前已不在 (日内被剔除, 走 today_ever_rows 对比 results)
      不依赖历史, 首次部署即可用
    - added / dropped: 相对上一归档日 (跨日)。首个归档日无基准, baseline=True 标明

    strategy_names: {strategy_id: 显示名}, 供调用方补名 (本模块不读策略定义, 保持纯数据层)
    """
    names = strategy_names or {}
    cur = read(data_dir, as_of)
    if cur is None:
        # 兜底: 当日还没归档 (功能上线当天, 缓存是上线前写的) → 用 strategy_cache
        # 里的 today_ever_matched 补一份基准归档。读时写是刻意的: 只在归档缺失
        # 时触发一次, 写的是本模块自己的文件, 不碰业务缓存, 让日报立即可用且
        # 明天起就有可比基准。
        cur = _backfill_from_cache(data_dir, as_of)
    if cur is None:
        return {
            "date": as_of,
            "baseline": True,
            "reason": "no_archive",
            "prev_date": None,
            "strategies": [],
            "universe": {"added": 0, "dropped": 0, "held": 0, "prev": 0},
        }

    cur_matched: dict[str, list[str]] = cur.get("matched") or {}
    dates = list_dates(data_dir)
    prev_date = _prev_date(dates, as_of)
    prev_matched: dict[str, list[str]] = {}
    if prev_date:
        prev = read(data_dir, prev_date) or {}
        prev_matched = prev.get("matched") or {}

    def _sym_union(m: dict[str, list[str]]) -> set[str]:
        out: set[str] = set()
        for syms in m.values():
            out.update(syms)
        return out

    cur_u, prev_u = _sym_union(cur_matched), _sym_union(prev_matched)
    added = sorted(cur_u - prev_u) if prev_date else []
    dropped = sorted(prev_u - cur_u) if prev_date else []

    rows = []
    for sid, syms in cur_matched.items():
        cur_set = set(syms)
        prev_set = set(prev_matched.get(sid, [])) if prev_date else set()
        rows.append({
            "strategy_id": sid,
            "name": names.get(sid, sid),
            "matched": len(cur_set),
            "prev_matched": len(prev_set),
            "added": sorted(cur_set - prev_set) if prev_date else [],
            "dropped": sorted(prev_set - cur_set) if prev_date else [],
        })
    rows.sort(key=lambda r: (-r["matched"], r["name"]))

    return {
        "date": as_of,
        "baseline": prev_date is None,
        "prev_date": prev_date,
        "strategies": rows,
        "universe": {
            "added": len(added),
            "dropped": len(dropped),
            "held": len(cur_u & prev_u) if prev_date else 0,
            "prev": len(prev_u),
            "current": len(cur_u),
            "added_symbols": added[:200],
            "dropped_symbols": dropped[:200],
        },
    }
