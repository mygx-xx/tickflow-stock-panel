"""交易日探针 (oracle) + 统一交易日历 — A 股交易日的唯一权威。

消费方 (实时行情轮询 / 盘中分钟增量 / 快照时间戳归属 / 读取侧 as_of /
数据完整性扫描 / 定时任务门控) 在周几+时段门控之后调用, 用于把「工作日但
休市」的节假日从轮询与调度窗口里剔除; 返回 None (未知) 时调用方维持现状
行为 (周几近似 + 快照新鲜度判据兜底), 不引入新依赖。

「今天是否交易日」判定链 (按确定性排序, 先到先得):
  1. fuyao 交易日历 (已配置 fuyao 时): GET /api/a-share/calendar/trading-days,
     今天在近一年交易日列表内 ⇔ 交易日。权威日历, 无时段依赖, 无开盘缓冲问题。
  2. tickflow 实时行情时间戳: 拉一篮流动性票快照 (单请求), max(timestamp)
     日期 == 今天 ⇔ 交易日。非交易日全市场戳停在上一交易日 (2026-08-29 周六
     实测 5551/5551, 含停牌股 — 戳是快照定版时刻, 非最后成交时刻);
     交易日集合竞价阶段 (9:15-9:30) 戳是否已翻新未实测 → 开盘缓冲窗内
     戳过期不作数, 保守视为未知。
  3. 均不可用 → None: 调用方按周几近似继续。

任意日期 (is_trading_day(date) / prev_trading_day / next_trading_day /
trading_days) 只能由 fuyao 日历回答 —— tickflow 戳只描述「今天」。日历带
TTL 缓存与 stale-while-error, 全项目共用一份 (trading_calendar), 不再各自
拉取 (data_integrity 等直接复用)。

安全约束:
  - 周末直接返回 False (周几判断零成本, 不打任何请求)。
  - 探针是纯读: 只产出布尔判定/日历集合, 不落盘、不进行情管道、不碰归属链路。
  - 只用于「降档」(休市不轮询/不调度); 休市结论 TTL 较短 (30 分钟) 定期复探,
    探针误判最坏损失一段快照且可自愈; 未知结论短 TTL (5 分钟) 防止
    轮询循环每拍重打失败的探测。
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from pathlib import Path

from app.market_time import CN_TZ, cn_now

logger = logging.getLogger(__name__)

# tickflow 戳探针的开盘缓冲窗: 此时刻之前戳仍是上一交易日属正常 (集合竞价),
# 不据此判休市。周一实测竞价戳翻新时机后可收紧。仅上午首个窗口需要。
_STALE_BUFFER_UNTIL = dt_time(9, 40)

# 一篮流动性票: 探 max(timestamp), 任一戳为今日即交易日 (OR 语义)。
# 大盘蓝筹同日全部停牌 = 市场性事件, 与休市同处理无碍。
_BASKET = ("000001.SZ", "600519.SH", "600036.SH", "601318.SH", "000651.SZ")

_TTL_TRADING_S = 3600.0   # 交易日结论每小时复探 (跨日天然失效)
_TTL_HOLIDAY_S = 1800.0   # 休市结论 30 分钟复探, 误判自愈上限
_TTL_UNKNOWN_S = 300.0    # 未知结论 5 分钟后重试探测
_CAL_TTL_S = 1800.0       # 交易日历集合 30 分钟复取 (节假日表近乎不变)

_CACHE_LOCK = threading.Lock()
_CAL_LOCK = threading.Lock()


@dataclass
class _Cache:
    day: object | None = None
    verdict: bool | None = None
    probed_at: float = 0.0


_CACHE = _Cache()
# (取数时刻, 交易日集合); 取数失败沿用上次成功结果 (stale-while-error)
_CAL: tuple[float, set[date] | None] = (0.0, None)

# 交易日历持久化: 路径由应用启动时注入 (main lifespan); 未注入则不落盘 (单测默认)。
# 日历是低频小数据 (~250 行/年), 用 JSON 比 parquet 更轻且可读; 写入走临时文件 + 原子替换。
_CAL_FILE: Path | None = None
_PERSISTED: set[date] | None = None  # 本地已存集合; None = 尚未从磁盘加载


def set_calendar_store(path: str | Path | None) -> None:
    """注入交易日历持久化文件路径 (应用启动时调用); None = 关闭持久化。

    关闭时 trading_calendar() 只走内存, 不读写磁盘 —— 单测依赖该默认值保持无副作用。
    注入后立即预载本地集合, 使冷启动/离线也能判定, 不再从 None 起步。
    """
    global _CAL_FILE, _PERSISTED
    with _CAL_LOCK:
        _CAL_FILE = Path(path) if path is not None else None
        _PERSISTED = None
    if _CAL_FILE is not None:
        _load_persisted_calendar()


def _replace_file(src: Path, dst: Path) -> None:
    """原子替换 (独立函数便于测试计数)。"""
    os.replace(src, dst)


def _load_persisted_calendar() -> set[date] | None:
    """读取本地日历集合; 未启用 / 文件缺失 / 损坏 → None (按未知处理)。

    读取失败静默降级 (只告警), 绝不因持久化问题影响实时轮询。
    """
    global _PERSISTED
    with _CAL_LOCK:
        if _PERSISTED is not None:
            return _PERSISTED or None
        path = _CAL_FILE
    if path is None:
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        days = {date.fromisoformat(str(x)) for x in raw}
    except Exception as e:  # 坏文件不应阻断日历判定 (磁盘问题只降级)
        logger.warning("交易日历持久化读取失败 (%s), 按无本地日历处理: %s", path, e)
        days = set()
    with _CAL_LOCK:
        _PERSISTED = days
    return days or None


def _save_persisted_calendar(days: set[date]) -> None:
    """把集合写入本地日历文件; 未启用 / 内容未变化则跳过。

    先写临时文件再原子替换, 失败只告警: 磁盘问题不得影响行情主流程。
    """
    global _PERSISTED
    with _CAL_LOCK:
        path = _CAL_FILE
        unchanged = path is None or (_PERSISTED is not None and days == _PERSISTED)
    if unchanged:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(
            json.dumps(sorted(d.isoformat() for d in days)), encoding="utf-8",
        )
        _replace_file(tmp, path)
    except Exception as e:  # 持久化失败不影响内存日历 (只告警)
        logger.warning("交易日历持久化写入失败 (%s): %s", path, e)
        return
    with _CAL_LOCK:
        _PERSISTED = set(days)


def reset_cache() -> None:
    """清空探针/日历内存缓存与已加载的持久化副本 (测试用)。"""
    global _CAL, _PERSISTED
    with _CACHE_LOCK:
        _CACHE.day = None
        _CACHE.verdict = None
        _CACHE.probed_at = 0.0
    with _CAL_LOCK:
        _CAL = (0.0, None)
        _PERSISTED = None


def _fetch_fuyao_calendar() -> set[date] | None:
    """近一年 A 股交易日集合; 未配置 fuyao / 失败 → None。"""
    try:
        from app.data_providers import custom as custom_sources

        if not custom_sources.is_custom_provider("fuyao"):
            return None
        days = custom_sources.get_provider("fuyao").trading_days()
        return set(days) if days else None
    except Exception:  # noqa: BLE001 — 日历不可用按未知处理, 不上抛
        return None


def trading_calendar() -> set[date] | None:
    """A 股交易日集合 (fuyao 近一年), 全项目唯一来源; 从未取到 → None。

    需要**任意历史日**判定的消费方 (数据完整性扫描 / 前后交易日 / 区间查询)
    直接消费整年集合。失败时沿用上一次成功结果 (stale-while-error): 长假期内
    日历近乎不变, 陈旧日历仍远好于退回周几近似 (会把休市日误报为缺失, 造成
    实时门禁 409 死锁)。线程安全 (轮询 / 完整性 / 调度共用)。
    """
    global _CAL
    now = time.monotonic()
    with _CAL_LOCK:
        fetched_at, cached = _CAL
        if fetched_at and now - fetched_at < _CAL_TTL_S:
            return cached
    fetched = _fetch_fuyao_calendar()
    if fetched is None:
        # stale-while-error: 本进程上次成功结果; 冷启动则用本地已存日历 (离线兜底)
        base = cached if cached is not None else _load_persisted_calendar()
        with _CAL_LOCK:
            _CAL = (now, base)
            return base
    # 与本地已存集合取并集: 既能累积超过 fuyao 一年窗口的历史, 也避免上游窗口滑动丢数据
    persisted = _load_persisted_calendar() or set()
    merged = persisted | fetched
    _save_persisted_calendar(merged)
    with _CAL_LOCK:
        _CAL = (now, merged)
        return merged


def _probe_fuyao(now: datetime) -> bool | None:
    """fuyao 交易日历: 今天在列表内 ⇔ 交易日。未配置 fuyao / 失败 → None。"""
    days = trading_calendar()
    if days is None:
        return None
    return now.date() in days


def _probe_tickflow(now: datetime) -> bool | None:
    """tickflow 行情时间戳: max(timestamp) 日期 == 今天 ⇔ 交易日。

    戳停在上一交易日: 开盘缓冲窗内 → None (可能是竞价未翻新), 窗后 → False。
    无实时权限 / 网络失败 / 无有效戳 → None。
    """
    try:
        from app.tickflow.client import get_client

        rows = get_client().quotes.get(symbols=list(_BASKET)) or []
        stamps = [r.get("timestamp") for r in rows if isinstance(r, dict)]
        valid = [int(t) for t in stamps if isinstance(t, (int, float)) and t]
        if not valid:
            return None
        latest_day = datetime.fromtimestamp(max(valid) / 1000, tz=CN_TZ).date()
        if latest_day == now.date():
            return True
        if now.time() < _STALE_BUFFER_UNTIL:
            return None
        return False
    except Exception:  # noqa: BLE001 — 无权限/网络失败按未知处理
        return None


def _ttl_of(verdict: bool | None) -> float:
    if verdict is True:
        return _TTL_TRADING_S
    if verdict is False:
        return _TTL_HOLIDAY_S
    return _TTL_UNKNOWN_S


def _today_verdict(now: datetime) -> bool | None:
    """「今天」判定: 周末零成本直判, 工作日走探测链并按 TTL 缓存结论。"""
    if now.weekday() >= 5:
        return False

    with _CACHE_LOCK:
        # 「未知」(None) 也是一个结论, 同样按 TTL 缓存 —— 它正是 _TTL_UNKNOWN_S 要
        # 挡住的场景 (未配 fuyao 且 tickflow 不可用时, 轮询每拍都会重打一次探测)。
        # _CACHE.day 只在探测写回时设置, 因此「当天已探过」用它判定即可。
        if (
            _CACHE.day == now.date()
            and (time.monotonic() - _CACHE.probed_at) < _ttl_of(_CACHE.verdict)
        ):
            return _CACHE.verdict

    verdict = _probe_fuyao(now)
    if verdict is None:
        verdict = _probe_tickflow(now)

    with _CACHE_LOCK:
        _CACHE.day = now.date()
        _CACHE.verdict = verdict
        _CACHE.probed_at = time.monotonic()
    return verdict


def is_trading_day(now: datetime | date | None = None) -> bool | None:
    """是否 A 股交易日。True=交易日, False=确定休市, None=未知 (维持周几近似)。

    - datetime / None: 「今天」口径, 走探测链 (fuyao 日历 → tickflow 时间戳)。
    - date: 任意日期口径 (is_trading_day_on)。周末零成本 False; 今天仍走
      探测链 (tickflow 只能回答今天); 其余日期只查统一日历, 不可用 → None。
    """
    if isinstance(now, datetime):
        return _today_verdict(now)
    if isinstance(now, date):
        return is_trading_day_on(now)
    return _today_verdict(cn_now())


def is_trading_day_on(d: date) -> bool | None:
    """指定日期是否交易日 (日历口径); 日历不可用 / 超出日历窗口 → None。

    今天走完整探测链; 历史/未来日只查日历, 落在日历窗口外时返回 None
    (不能把「窗口外」误判成「休市」)。
    """
    if d.weekday() >= 5:
        return False
    today = cn_now()
    if d == today.date():
        return _today_verdict(today)
    days = trading_calendar()
    if days is None or not (min(days) <= d <= max(days)):
        return None
    return d in days


def trading_days(start: date, end: date) -> list[date] | None:
    """[start, end] 闭区间内的交易日 (升序); 日历不可用 → None, 区间内无交易日 → []。"""
    days = trading_calendar()
    if days is None:
        return None
    return sorted(d for d in days if start <= d <= end)


def prev_trading_day(d: date) -> date | None:
    """严格早于 d 的最近交易日; 日历不可用 / 超出窗口 → None。"""
    days = trading_calendar()
    if days is None or d <= min(days):
        return None
    earlier = [x for x in days if x < d]
    return max(earlier) if earlier else None


def next_trading_day(d: date) -> date | None:
    """严格晚于 d 的最近交易日; 日历不可用 / 超出窗口 → None。"""
    days = trading_calendar()
    if days is None or d >= max(days):
        return None
    later = [x for x in days if x > d]
    return min(later) if later else None
