"""策略结果缓存 — 写入本地文件，供策略页面秒加载。

缓存结构:
  {
    "as_of": "2024-01-15",
    "results": { strategy_id: { total, as_of, rows } },
    "today_ever_matched": { strategy_id: [symbol, ...] },    // 今日曾命中 symbol 并集
    "today_ever_rows": { strategy_id: { symbol: row_data } },// 今日曾命中的完整行数据
    "errors": { strategy_id: "失败原因" },                  // 今日跑挂的策略 (缺数据等)
    "updated_at": 1705324800000  # Unix ms
  }

文件路径: data/user_data/strategy_cache.json

旁路文件: data/user_data/strategy_cache_summary.json — 上面 payload 的**无行数据投影**
(results 剥掉 rows, 另带 today_ever_matched / errors / as_of / updated_at)。策略页卡片的
摘要端点读它 (实测 151 策略约 55KB vs 全量 15–23MB), 由 write_cache 与全量同批原子替换;
缺失时端点回退解析全量, 口径不变 (有逐字段等价用例)。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any


def _json_default(obj: Any) -> Any:
    """处理 date/datetime 等 JSON 不认识的类型。"""
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


logger = logging.getLogger(__name__)

_CACHE_FILENAME = "strategy_cache.json"
# 摘要侧文件: 全量缓存的无行数据投影 (每策略 total/as_of/computed_at + 曾命中 symbol
# 列表), 实测 151 策略约 45KB vs 全量 22.7MB。策略页卡片的摘要读这个, 不再解析全量。
_SUMMARY_FILENAME = "strategy_cache_summary.json"

# 读写同一 JSON 文件的进程内锁: write_cache 的 read-modify-write 与并发 read_cache
# 无锁会丢更新/读到半写文件。read_cache 与 write_cache 共用此锁; write 内部复用
# _read_cache_unlocked 避免自死锁。写入用临时文件 + os.replace 做到原子替换。
_file_lock = threading.Lock()

# 已解析结果的进程内复用: 缓存文件实测 22.7MB (151 策略 / 4643 行), 整体 json.loads
# 一次 250–270ms, 而 cached-summary 只留 151 条摘要 —— 策略页首屏与 pendingRun 期间
# 每 2s 的轮询都要重付这 270ms, 且全程攥着 _file_lock (write 侧在锁内 read-modify-write
# 整份文件, 实测 570ms, 期间所有读排队)。按 (mtime_ns, size) 命中即复用, 命中与否内容
# 完全一致。
#
# 只在 _file_lock 内读写, 无需额外加锁。key 为缓存文件绝对路径。
# 返回值与其嵌套内容一律视为**只读**: memo 把同一对象发给多个调用方, 就地改会污染
# 后续所有读取 (write_cache 的 merge 读也走 memo)。
_MEMO_MAX_ENTRIES = 8
_memo: dict[str, tuple[int, int, dict | None]] = {}


def _cache_path(data_dir: Path) -> Path:
    return data_dir / "user_data" / _CACHE_FILENAME


def _summary_path(data_dir: Path) -> Path:
    return data_dir / "user_data" / _SUMMARY_FILENAME


def summary_of(payload: dict) -> dict:
    """全量缓存 → 摘要投影 (剥掉每策略的 rows 明细, 其余字段原样保留)。

    today_ever_matched 保留完整 symbol 列表而非只留计数: 端点叠加监控引擎实时结果时
    要用它和实时行做并集 (实时轮可能命中此前没命中的票)。
    """
    results = payload.get("results") or {}
    return {
        "as_of": payload.get("as_of"),
        "updated_at": payload.get("updated_at"),
        "results": {
            sid: {k: v for k, v in r.items() if k != "rows"}
            for sid, r in results.items()
            if isinstance(r, dict)
        },
        "today_ever_matched": payload.get("today_ever_matched") or {},
        "errors": payload.get("errors") or {},
    }


def _enriched_parquet_path(data_dir: Path, as_of: str) -> Path:
    """返回 enriched parquet 文件路径。"""
    return data_dir / "kline_daily_enriched" / f"date={as_of}" / "part.parquet"


def _get_enriched_mtime(data_dir: Path, as_of: str) -> float | None:
    """返回 enriched parquet 文件的 mtime (秒)。文件不存在返回 None。"""
    p = _enriched_parquet_path(data_dir, as_of)
    try:
        return p.stat().st_mtime
    except FileNotFoundError:
        return None


def read_cache(data_dir: Path) -> dict | None:
    """读取策略缓存文件。返回 None 表示无缓存或读取失败。

    说明: 原先有 enriched mtime 过期校验 (数据文件变化 → 判过期返回 None),
    但在有实时行情的系统里, enriched parquet 每轮被刷新 → mtime 必然变化 →
    缓存被永久判死, 策略页读不到数据。且判过期后不触发重算, 只能让用户手动重跑,
    保护价值有限。故移除: 盘后缓存总能读出, 实时新鲜度由 /api/screener/cached
    端点叠加监控引擎的内存实时结果 (latest_strategy_results) 来保证。

    返回值来自进程内 memo, 视为只读; 需要改就先复制 (见 _read_cache_unlocked)。
    """
    with _file_lock:
        return _read_cache_unlocked(data_dir)


def read_summary(data_dir: Path) -> dict | None:
    """读取摘要侧文件。返回 None 表示没有摘要 (旧缓存/写失败/已被清), 调用方须回退全量。

    不套 memo: 45KB 解析远小于加判据的成本, 且它每次写都必然跟着换。
    """
    path = _summary_path(data_dir)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        logger.warning("读取策略摘要失败, 调用方将回退全量缓存: %s", e)
        return None


def clear_cache(data_dir: Path) -> None:
    """删除策略结果缓存；策略代码 reload 后避免继续展示旧公式结果。"""
    import traceback

    # 运维可见性: 策略页依赖本缓存秒加载, 被清空即整页回退到全量重算。
    # 记录调用链 (最近 5 帧), 排查"缓存莫名消失"类问题不需要复现现场。
    frames = traceback.extract_stack()[:-1]
    chain = " <- ".join(
        f"{f.filename.rsplit('/', 1)[-1]}:{f.lineno}:{f.name}" for f in frames[-5:]
    )
    logger.warning("策略缓存被清除, 调用链: %s", chain)
    path = _cache_path(data_dir)
    summary = _summary_path(data_dir)
    with _file_lock:
        path.unlink(missing_ok=True)
        path.with_name(path.name + ".tmp").unlink(missing_ok=True)
        summary.unlink(missing_ok=True)
        summary.with_name(summary.name + ".tmp").unlink(missing_ok=True)
        _memo.pop(str(path), None)


def _read_cache_unlocked(data_dir: Path) -> dict | None:
    """带 memo 的读取 (调用方须已持 _file_lock)。返回值只读, 不得就地修改。

    供 read_cache 与 write_cache 复用, 避免重入死锁。
    """
    path = _cache_path(data_dir)
    try:
        stat = path.stat()
    except FileNotFoundError:
        _memo.pop(str(path), None)
        return None

    key = str(path)
    hit = _memo.get(key)
    if hit is not None and hit[0] == stat.st_mtime_ns and hit[1] == stat.st_size:
        return hit[2]

    cached = _load_from_disk(path)
    if len(_memo) >= _MEMO_MAX_ENTRIES:
        # 正常只有一个 data_dir; 上限只防测试里逐用例换 tmp_path 把表撑大
        _memo.clear()
    _memo[key] = (stat.st_mtime_ns, stat.st_size, cached)
    return cached


def _load_from_disk(path: Path) -> dict | None:
    """从磁盘整体解析缓存文件。返回 None 表示无缓存或读取失败。"""
    try:
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            return None
        return json.loads(text)
    except Exception as e:  # noqa: BLE001
        logger.warning("读取策略缓存失败: %s", e)
        return None


def _rows_to_symbol_map(rows: list[dict]) -> dict[str, dict]:
    """将 rows 列表转为 {symbol: row_data} 映射。"""
    result: dict[str, dict] = {}
    for row in rows:
        sym = row.get("symbol")
        if sym:
            result[sym] = row
    return result


def write_cache(
    data_dir: Path,
    as_of: str,
    results: dict[str, Any],
    errors: dict[str, str] | None = None,
) -> None:
    """将策略结果写入缓存文件，同时更新今日曾命中集合。

    - 日期变更时重置 today_ever_matched 和 today_ever_rows
    - 同一天内合并 (并集) 之前曾命中的 symbol，并用最新行数据更新
    - errors: {策略: 失败原因}。同一策略本轮成功 (出现在 results 里) 即撤销其旧错误，
      避免重跑修好后卡片继续挂「缺数据」。
    - 旁路: 把当日曾命中集合按日期归档, 供命中日报做跨日差分 (失败不影响主流程)
    """
    path = _cache_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    # 整个 read-modify-write 持锁: 避免并发 write 丢更新, 也避免与 read_cache 撕裂
    with _file_lock:
        payload = _write_cache_locked(path, data_dir, as_of, results, errors or {})

    # 归档在锁外做: 它写的是另一个文件, 不该拖长主缓存的锁持有时间
    if payload is not None:
        try:
            from app.services import hits_archive

            hits_archive.archive(data_dir, as_of, payload.get("today_ever_matched") or {})
        except Exception as e:  # noqa: BLE001
            logger.warning("命中归档旁路失败 (不影响缓存写入): %s", e)


def _write_summary_locked(data_dir: Path, summary: dict) -> None:
    """摘要落盘 (调用方须已持 _file_lock)。同样用临时文件 + os.replace 原子替换。"""
    path = _summary_path(data_dir)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(summary, ensure_ascii=False, default=_json_default), encoding="utf-8"
    )
    os.replace(tmp, path)


def _write_cache_locked(
    path: Path,
    data_dir: Path,
    as_of: str,
    results: dict[str, Any],
    errors: dict[str, str],
) -> dict | None:
    """持 _file_lock 后的实际写入逻辑 (read-merge-write + 原子替换)。

    返回落盘 payload (供调用方做旁路归档), 写盘失败返回 None。
    """
    # 读取旧缓存 (已持锁, 走不重入的 _read_cache_unlocked)
    old = _read_cache_unlocked(data_dir)
    old_as_of = old.get("as_of") if old else None
    old_ever_rows: dict[str, dict[str, dict]] = old.get("today_ever_rows", {}) if old else {}

    if old_as_of == as_of:
        merged_results = {**(old.get("results") or {}), **results}
    else:
        merged_results = results

    # 失败原因按日累计: 换日只留本轮, 同日增量写要保住其它策略已记的错误;
    # 本轮成功的策略从错误表里撤销 (它已有结果, 再报错就是过期信息)。
    old_errors = (old.get("errors") or {}) if old_as_of == as_of else {}
    merged_errors = {k: v for k, v in {**old_errors, **errors}.items() if k not in results}

    # 当前命中的行数据 → symbol 映射
    current_row_maps: dict[str, dict[str, dict]] = {}
    for sid, r in results.items():
        current_row_maps[sid] = _rows_to_symbol_map(r.get("rows", []))

    if old_as_of and old_as_of == as_of and old_ever_rows:
        # 同一天: 合并 — 用当前行数据更新旧数据 (保持最新价格等)
        merged_rows: dict[str, dict[str, dict]] = {}
        all_keys = set(old_ever_rows.keys()) | set(current_row_maps.keys())
        for sid in all_keys:
            old_map = old_ever_rows.get(sid, {})
            cur_map = current_row_maps.get(sid, {})
            # 以旧数据为基础，用当前数据覆盖 (当前数据更新鲜)
            combined = {**old_map, **cur_map}
            merged_rows[sid] = combined
        today_ever_rows = merged_rows
    else:
        # 新的一天或首次写入
        today_ever_rows = current_row_maps

    # 从 ever_rows 提取 symbol 列表 (用于快速计数)
    today_ever_matched = {sid: sorted(maps.keys()) for sid, maps in today_ever_rows.items()}

    # enriched_mtime: 盘后缓存写入时记录 (向后兼容旧字段)。read_cache 已不再用它
    # 做过期校验, 实时新鲜度改由 /cached 端点叠加监控引擎内存结果保证。
    enriched_mtime = _get_enriched_mtime(data_dir, as_of)

    payload = {
        "as_of": as_of,
        "results": merged_results,
        "today_ever_matched": today_ever_matched,
        "today_ever_rows": today_ever_rows,
        "errors": merged_errors,
        "enriched_mtime": enriched_mtime,
        "updated_at": int(time.time() * 1000),
    }
    try:
        # 原子写: 先写临时文件再 os.replace, 避免读侧读到半写的 JSON
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, default=_json_default), encoding="utf-8")
        os.replace(tmp, path)
        # 写侧直接刷新 memo: 同一次写的内容本机已知, 下一笔增量写 (run_all 逐策略落盘)
        # 的 merge 读即命中, 整写实测 570ms → 285ms, 锁占用减半; 也不依赖 (mtime, size)
        # 判据 —— Windows 时钟粒度粗, 同刻连续写会撞车使 memo 判命中而返回旧内容。
        # 外部进程改写仍由 stat 变化兜住 (size 几乎必变)。
        try:
            stat = path.stat()
            _memo[str(path)] = (stat.st_mtime_ns, stat.st_size, payload)
        except FileNotFoundError:
            _memo.pop(str(path), None)
        # 摘要跟着全量一起换 (同一份 payload 派生, 不会算两遍)。
        # 写失败就删掉旧摘要: 宁可让端点回退解析全量, 也不让它端出一份比全量旧的摘要。
        # 残留窗口: 两次 replace 之间进程被杀 → 摘要比全量旧一轮, 下一次写即自愈。
        try:
            _write_summary_locked(data_dir, summary_of(payload))
        except Exception as e:  # noqa: BLE001
            logger.warning("写入策略摘要失败, 已删除旧摘要让端点回退全量: %s", e)
            _summary_path(data_dir).unlink(missing_ok=True)
        total_rows = sum(len(r.get("rows", [])) for r in merged_results.values())
        total_ever = sum(len(v) for v in today_ever_matched.values())
        logger.info("策略缓存已写入: %s, %d 策略, %d 命中, %d 曾命中", as_of, len(merged_results), total_rows, total_ever)
        return payload
    except Exception as e:  # noqa: BLE001
        logger.warning("写入策略缓存失败: %s", e)
        return None
