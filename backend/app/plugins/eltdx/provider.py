"""eltdx Provider —— 通达信行情数据源(面板数据源插件契约实现)。

只声明并实现 ``daily`` 与 ``realtime`` 两个数据集; 未声明的数据集
(adj_factor / minute / full_minute / depth5 / financial) 由
``provider_has_dataset`` 返回 False, 面板自动回退 TickFlow。

**口径依据**(全部为 eltdx 3.2.2 实测, 详见 client.py 模块 docstring 与
docs/plugin-development.md「内部数据契约」):

===========  ====================  ==========================  ==============
面板字段      eltdx 来源            实测单位/口径                处理
===========  ====================  ==========================  ==============
symbol       ``full_code``         ``sz000001``                转 ``000001.SZ``
date         KlineBar.time         Asia/Shanghai aware dt      取 ``.date()``
open/high/low/close  KlineBar.*    元(不复权)                   直用
volume       KlineBar.volume_lots  手                           直用
amount       KlineBar.amount       元                           直用
last_price   last_price            元                           直用
prev_close   pre_close_price       元                           直用
change_pct   change_pct            **百分数制**(0.4425=0.4425%)  **/100**
===========  ====================  ==========================  ==============

缺失字段一律 ``None``, 不做启发式补全(契约红线); ``change_amount`` 用
``last_price - prev_close`` 按固定口径推导。
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from typing import Any

import polars as pl

from app.data_providers.base import AUCTION_COLUMNS, AUCTION_SCHEMA
from app.data_providers.normalizer import DAILY_COLS, normalize_daily
from app.plugins.eltdx.client import (
    DEFAULT_CONNECTIONS_PER_SERVER,
    DEFAULT_SERVER_COUNT,
    DEFAULT_TIMEOUT_S,
    EltDxClient,
    to_eltdx_code,
    to_panel_symbol,
)
from app.plugins.eltdx.http_client import (
    DEFAULT_CODE_STALE_MAX_S,
    DEFAULT_CODE_TTL_S,
    DEFAULT_HTTP_TIMEOUT_S,
    DEFAULT_HTTP_URL,
    HttpTransport,
)

logger = logging.getLogger(__name__)

# 北京墙钟时区(UTC+8)。用固定偏移而非 zoneinfo: 中国无夏令时, 且避免 tzdata 依赖
_CN_TZ = timezone(timedelta(hours=8))


def _record_symbol(rec: Any) -> str | None:
    """eltdx 记录 → 面板 symbol; 交易所信息不足时**拒绝推断**返回 None。

    退化链(机制已复现, 触发前提未观测到): ``full_code`` 是 SDK 的**属性**而非网关
    字段 —— ``return f"{self.exchange}{self.code}"``(见 eltdx ``QuoteSnapshot.full_code``)。
    故 ``exchange`` 为空时它退化成裸 6 位代码(实测: ``('','000001')`` → ``'000001'``、
    ``('','920157')`` → ``'920157'``), 再交给 ``to_panel_symbol`` 按首位猜交易所 ——
    沪市指数(000001 上证指数)会被猜成 ``000001.SZ``, 北交所(920xxx)会被猜成
    ``920000.SH``。这些错码既不是股票也不是指数, 若被当作股票写进 kline_daily,
    会与真实标的(如平安银行 000001.SZ)撞成重复行, 令矩阵构建报
    "MarketDataMatrix requires unique timestamp/symbol rows"。

    实盘可达性(2026-09-30, 网关 3.2.2 实测): ``quotes.get_snapshots`` 返回的 23 个
    字段中**不含 full_code**, 且 ``exchange`` 字段**从不缺失或为空**(抽样 80 只全部
    为 ``sh``, 指数路径同样全有值)。故本函数是**纵深防御**: 只在上下游异常导致
    ``exchange`` 缺失时才生效, 而该前提在当前版本未被观测到 —— 不是活跃缺陷。

    显式 ``exchange`` 是唯一可靠来源(与 ``_shares_row`` 同一口径); 取不到时
    **不猜**, 由调用方丢弃该行 —— 宁可少一行, 不可把指数值写成股票价。
    """
    exchange = getattr(rec, "exchange", None)
    code = getattr(rec, "code", None)
    if exchange and code:
        return to_panel_symbol(f"{exchange}{code}")
    # 无显式 exchange 时, full_code 仍可能自带交易所标识: 前缀式 "sz000001"
    # (eltdx 原生形态, 8 位)或后缀式 "000001.SZ"。两者都可解析;
    # 只有裸 6 位代码(如 "000001")一律拒绝 —— 那正是误判的来源。
    full_code = getattr(rec, "full_code", None)
    if full_code:
        text = str(full_code)
        if "." in text or len(text) == 8:
            return to_panel_symbol(text)
    return None


_DATASETS = (
    "daily",
    "realtime",
    "minute",
    "full_minute",
    "depth5",
    "auction",
    "financial",
    "adj_factor",
)

# 日 K 列(面板 canonical 9 列, 含 quote_ts; 由 normalizer.DAILY_COLS 单源定义)
_DAILY_COLUMNS = DAILY_COLS
# 分钟 canonical 8 列(契约: docs/plugin-development.md get_minute)
_MINUTE_COLUMNS = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
# 除权因子 canonical 3 列(契约: get_adj_factors)
_ADJ_COLUMNS = ["symbol", "trade_date", "ex_factor"]
# 集合竞价 canonical 8 列: 契约列在 data_providers.base.AUCTION_COLUMNS(单源)

# 财务: 只实现 shares 表(见 get_financials docstring 的口径依据)。
# eltdx 的财务有两个来源, 只有前者字段名明确:
#   corporate.finance_batch -> FinanceRecord(总股本/流通股本/净资产/净利润/公告日, 字段名明确)
#   f10.finance_report(zcfzb/lrb/xjllb) -> 不透明 T*** 代码, **包内无代码→名称字典**, 故不接。
_SHARES_COLUMNS = ["symbol", "period_end", "announce_date", "total_shares", "float_shares"]
# 除权因子: 并发与基准价回看窗口(需覆盖事件日前一交易日, 120 自然日足够)
_ADJ_WORKERS = int(os.environ.get("ELTDX_ADJ_WORKERS", "8"))
_ADJ_LOOKBACK_BARS = int(os.environ.get("ELTDX_ADJ_LOOKBACK_BARS", "120"))
# finance_batch 单次请求标的数。**实测安全上限约 20 只**(关键: 必须按真实标的序测) ——
# 用面板代码序时 n=20 成功、n=25 起全部 `invalid ASCII response code`;
# eltdx 官方 batch_size=75 在实际代码序下**不可用**(会成批失败, 只落 ~1200/5578 只)。
# 故这里保守取 20; 单批失败仍按批隔离, 不拖垮整表。
_FINANCE_BATCH = int(os.environ.get("ELTDX_FINANCE_BATCH", "20"))
# 失败批的本地重试次数与间隔; 重试仍失败则二分拆分(见 _finance_records)
_FINANCE_RETRIES = int(os.environ.get("ELTDX_FINANCE_RETRIES", "2"))
_FINANCE_RETRY_SLEEP_S = float(os.environ.get("ELTDX_FINANCE_RETRY_SLEEP", "0.05"))
# f10 报表单标的请求节流(上游是逐标的接口, 无批量)。实测单请求约 40ms,
# 取 0.05s 留出余量; 全市场 5500 只约 5 分钟/表。
_F10_INTERVAL_S = float(os.environ.get("ELTDX_F10_INTERVAL", "0.05"))

# 分钟并发(独立于日K, 避免瞬时占满连接池)与单标的根数上限
_MINUTE_WORKERS = int(os.environ.get("ELTDX_MINUTE_WORKERS", "8"))
_MINUTE_MAX_BARS = int(os.environ.get("ELTDX_MINUTE_MAX_BARS", "12000"))  # ~50 交易日

# 全量分钟「稳态增量轮」批量参数。实测 bars.get 批量上限很高, 且**无 80 限制**
# (80/200/500/4000 只均足额返回)。全市场 5578 只按 1000 分片共 6 批, 一遍 ~10.1s。
_INTRADAY_LATEST_BATCH = int(os.environ.get("ELTDX_INTRADAY_LATEST_BATCH", "1000"))
# 稳态增量轮的并发线程数。
#
# 注意 bars_multi **没有 80 上限** —— 那是 `snapshots`(快照)专属的硬上限, 不要混淆:
#   实测 bars.get 单请求: 80 只 196ms / 200 只 468ms / 500 只 1340ms / 4000 只 11.7s,
#   均**足额返回**(仅 5578 只时回 5572, 是缺数据的退市/停牌标的, 非截断)。
#   对照 snapshots 请求 81 只 -> 只回 80(静默截断)。
#
# 并发标定(盘中实测, 全市场 5578 只 x count=3, 每请求 80 只):
#   线程 8 : 10.1~13.6s(波动大)   线程 12: 10.13/10.13/10.17s   线程 16: 10.04~10.16s
# 即 **>=12 线程后锁死在 ~10.1s 的地板**, 再往上(24)无收益。
# 该地板是上游 7709 主站的处理速率(~1.8ms/只串行), 不由本端并发决定 —— 实测把分片
# 改成 500/700/1000/1400 全并行仍是 ~10.1s。故取 8: 已达地板附近, 且给主池(16 slot)
# 留足余量(8 分钟 + 4 快照 + 1 盘口 = 13 < 16)。
# 切勿设 >=16: 会与快照/盘口争抢主池 slot, 高峰反被排队拖慢。
_INTRADAY_LATEST_WORKERS = int(os.environ.get("ELTDX_INTRADAY_LATEST_WORKERS", "8"))

# 实时快照单次请求的代码数上限。**eltaX 硬上限为 80**(实测: 请求 81/100/400/700 均
# 静默截断为 80 只; 请求 800/1600/3000 直接断连 os error 10054)。超限会导致
# "每片都失败 -> 全市场 0 行", 故这里必须是 80, 不可按"包大小"放宽。
_SNAPSHOT_BATCH = int(os.environ.get("ELTDX_SNAPSHOT_BATCH", "80"))
# 快照分片并发上限(独立于日 K 的连接池, 避免瞬时把池占满影响其他数据集)
_SNAPSHOT_WORKERS = int(os.environ.get("ELTDX_SNAPSHOT_WORKERS", "4"))

# 日 K 分批大小(契约: iter_daily 每批要有上界; 调用方边收边落盘)
_DAILY_BATCH = int(os.environ.get("ELTDX_DAILY_BATCH", "200"))


def availability() -> tuple[bool, str]:
    """插件可用性自检(后端启动时调用)。

    契约: 返回 ``(是否可用, 原因)``, 不抛异常。不可用时设置页灰显并展示 install_hint。

    两种传输的检查口径不同:
    * ``http``(默认): 还需探测网关 ``/health`` —— 因为连接池归网关所有,
      网关不可达时插件实际不可用(且**不会**自动回退进程内, 故必须显式暴露)。
    * ``inproc``: 只需 eltdx 依赖可 import。
    """
    mode = os.environ.get("ELTDX_TRANSPORT", "http").strip().lower()
    if mode == "http":
        base = os.environ.get("ELTDX_HTTP_URL", DEFAULT_HTTP_URL)
        try:
            info = HttpTransport(
                base_url=base,
                timeout=float(os.environ.get("ELTDX_HTTP_TIMEOUT", DEFAULT_HTTP_TIMEOUT_S)),
            ).health()
        except Exception as e:  # 网关不可达: 记录原因供设置页展示, 不上抛
            return False, (
                f"eltaX HTTP 网关不可达({base}): {str(e)[:80]}; "
                f"请先启动网关 `eltdx-http`(见 docs/HTTP_GATEWAY.md)"
            )
        return True, f"ok (eltaX 网关 {info.get('version', '?')} @ {base})"
    try:
        import eltdx
    except Exception as e:  # 依赖缺失: 记录原因供设置页灰显, 不上抛
        return False, f"未安装 eltdx 依赖({e}); 请点击卡片「安装依赖」按钮"
    version = getattr(eltdx, "__version__", "unknown")
    return True, f"ok (eltdx {version}, 进程内模式)"


def _to_float(value: Any) -> float | None:
    """安全转 float; 非法/缺失返回 None(不伪造)。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    # NaN/Inf 不进面板(读回会序列化成 null, 但源头就该干净)
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _parse_time_value(value: Any) -> datetime | None:
    """把 K 线 ``time`` 解析成 aware/naive datetime —— **两种传输都要兼容**。

    两种传输的 ``time`` 形态不同(这是 HTTP 网关接入时必须处理的关键差异):

    * **进程内**: eltdx 返回 aware ``datetime``(Asia/Shanghai), 直接可用。
    * **HTTP 网关**: 序列化成 ISO8601 **字符串** ``'2026-09-30T11:25:00+08:00'``。

    旧实现只认 ``'%Y-%m-%d %H:%M:%S'``(空格分隔), 遇到 ISO 的 ``T`` 与 ``+08:00``
    偏移会**静默返回 None** —— 后果是所有分钟行被区间过滤掉, 表现为
    ``get_intraday_batch`` 全市场返回 **0 行**(实测 2026-09-30 切 HTTP 后复现)。

    故这里统一走 ``fromisoformat``(Python 3.11+ 支持 ``Z`` 与各种偏移), 并保留
    原有的空格格式兜底。
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    # ISO8601(HTTP 网关): '2026-09-30T11:25:00+08:00' / '...Z' / '2026-09-30'
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    # 空格分隔(进程内字符串变体): '2026-09-30 11:25:00'
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return None


def _bar_date(value: Any) -> date | None:
    """KlineBar.time → ``date``(北京墙钟, 取日期部分); 兼容 ISO8601 字符串。"""
    dt = _parse_time_value(value)
    return dt.date() if dt is not None else None


def _bar_datetime(bar: Any) -> datetime | None:
    """KlineBar.time → datetime(保留 tzinfo 供比较; 输出时再转 naive 北京墙钟)。

    eltdx 的 ``time`` 是 Asia/Shanghai 的 aware datetime(实测 ``+08:00``);
    HTTP 网关则给同值的 ISO8601 字符串 —— 两者都经 :func:`_parse_time_value` 收口。
    """
    return _parse_time_value(getattr(bar, "time", None))


def _as_datetime(value: datetime | date, *, end_of_day: bool) -> datetime:
    """date/datetime → 区间端点 datetime(end_of_day 时补齐到当日 23:59:59)。

    eltdx 的 KlineBar.time 是 **aware**(Asia/Shanghai, 实测 +08:00), 而契约要求输出
    naive 北京墙钟。若端点用 naive 去和 aware 比较会抛 TypeError, 故端点统一定位到
    UTC+8 的 aware; 输出行时再 ``replace(tzinfo=None)`` 交给下游。
    """
    dt = (
        value
        if isinstance(value, datetime)
        else datetime.combine(value, dtime(23, 59, 59) if end_of_day else dtime(0, 0, 0))
    )
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=_CN_TZ)


def _kline_row(symbol: str, bar: Any) -> dict | None:
    """单根 KlineBar → 面板日 K 行; 关键字段缺失则丢弃该根(不伪造)。"""
    d = _bar_date(getattr(bar, "time", None))
    o = _to_float(getattr(bar, "open", None))
    h = _to_float(getattr(bar, "high", None))
    low = _to_float(getattr(bar, "low", None))
    c = _to_float(getattr(bar, "close", None))
    if d is None or o is None or h is None or low is None or c is None:
        return None
    return {
        "symbol": symbol,
        "date": d,
        "open": o,
        "high": h,
        "low": low,
        "close": c,
        # volume_lots 实测为"手"(契约单位), amount 为元 —— 均直用, 不换算
        "volume": _to_float(getattr(bar, "volume_lots", None)),
        "amount": _to_float(getattr(bar, "amount", None)),
    }


def _depth_row(rec: Any) -> dict | None:
    """盘口记录 → 面板标准盘口字典(5 档买卖价量 + 毫秒时间戳)。

    实测来源: ``quotes.get_depth().records[]`` 的 ``buy_levels`` / ``sell_levels``,
    各 5 档 ``QuoteLevel(price, volume)``; volume 单位为**手**(与面板契约一致)。
    ``volume=0`` 是有意义的值(封死涨/跌停), 必须保留为 0。
    ``timestamp``: 记录只有 ``update_time_raw``(当日 HHMMSS 紧凑整数, 实测 6 位),
    无日期; 契约为毫秒 Unix 时间戳, 故按北京墙钟当日还原(同快照 time_raw 处理)。
    """
    bids = list(getattr(rec, "buy_levels", ()) or ())
    asks = list(getattr(rec, "sell_levels", ()) or ())
    if not bids and not asks:
        return None
    return {
        "bid_prices": [_to_float(getattr(lv, "price", None)) for lv in bids],
        "bid_volumes": [_to_float(getattr(lv, "volume", None)) for lv in bids],
        "ask_prices": [_to_float(getattr(lv, "price", None)) for lv in asks],
        "ask_volumes": [_to_float(getattr(lv, "volume", None)) for lv in asks],
        "timestamp": _hhmmss_ts(getattr(rec, "update_time_raw", None)),
    }


# 竞价序列只有开盘(09:15→09:25)与收盘(14:57→15:00)两段, 实测不含连续竞价点,
# 故用连续竞价开始时刻做分段界。
_CONTINUOUS_START_S = 9 * 3600 + 30 * 60

# 竞价长表 schema: 契约列在 data_providers.base.AUCTION_SCHEMA(空帧也带列, 供服务层直接 concat/落盘)


def _auction_rows(symbol: str, trade_date: date, series: Any) -> list[dict]:
    """``AuctionSeries`` → 面板标准竞价行(逐点长表)。

    实测口径(eltdx 3.2.3, 2026-10-08 取证), 三条都是"不照做就会静默出错"的坑:

    * **价格取 ``price_milli/1000``, 不用 ``price``** —— ``price`` 是 float32 还原值
      (实测 11.569999694824219), ``price_milli``(11570)才是精确整数。
    * **量单位为手** —— 锚点: 600519.SH 收盘竞价末点 ``matched_volume=472`` 与该日
      15:00 分钟 K ``volume_lots=472`` 逐位相同(分钟 K 契约同为手)。
      ``matched_volume`` 是该时点虚拟撮合下的可匹配量、**非累计**(实测竞价中可回落)。
    * **``unmatched_direction_raw`` +1 = 买侧剩余** —— 锚点: 8 连板一字 600825.SH
      末点未匹配 5,770,681 手且方向 +1(涨停买队排不进); 低开/抛压标的为 -1。

    日期归属一律采用调用方传入的 ``trade_date``: 上游只在显式传 date 时回传
    ``trading_date``, 且网关回传的是**字符串**(进程内是 date), 而非交易日它也会把
    请求日原样回填(实测 2026-10-07 休市仍回 ``td=2026-10-07``) —— 回读会误导。
    """
    out: list[dict] = []
    for p in getattr(series, "points", ()) or ():
        ts = getattr(p, "time_seconds", None)
        milli = getattr(p, "price_milli", None)
        if ts is None or milli is None:
            continue  # 无法定位时刻或还原精确价: 丢行, 不伪造
        side_raw = getattr(p, "unmatched_direction_raw", None)
        out.append({
            "symbol": symbol,
            "trade_date": trade_date,
            "segment": "open" if int(ts) < _CONTINUOUS_START_S else "close",
            "datetime": datetime.combine(trade_date, dtime()) + timedelta(seconds=int(ts)),
            "price": int(milli) / 1000,
            "matched_volume": _to_float(getattr(p, "matched_volume", None)),
            "unmatched_volume": _to_float(getattr(p, "unmatched_volume", None)),
            "unmatched_side": (
                None
                if side_raw is None
                else ("buy" if int(side_raw) > 0 else "sell" if int(side_raw) < 0 else None)
            ),
        })
    return out


def _today_wallclock_ms(hour: int, minute: int, sec: int, micro: int = 0) -> int | None:
    """当日北京墙钟 → 毫秒时间戳; 非交易日返回 None。

    上游紧凑时间不含日期, 只能按"当日"还原。但休市日根本没有"当日"可言 ——
    此时返回 None 表示日期未知, 不伪造归属。

    交易日判定走 app.services.trading_day 探针链 (fuyao 日历 → tickflow 时间戳),
    结论带 TTL 缓存, 单次调用负担仅为一次字典查询。探针返回 None (未知) 时维持
    还原行为: 未知不放行会丢掉盘中真实行情, 且读取侧与 final 边界两道防线仍在。
    """
    from app.services import trading_day

    if trading_day.is_trading_day() is False:
        return None
    try:
        dt = datetime(date.today().year, date.today().month, date.today().day,
                      hour, minute, sec, micro, tzinfo=_CN_TZ)
    except ValueError:
        return None
    return int(dt.timestamp() * 1000)


def _hhmmss_ts(raw: Any) -> int | None:
    """当日紧凑时间(``HHMMSS`` 6 位 或 ``HHMMSScc`` 8 位) → 当日北京墙钟毫秒时间戳。

    盘口记录的 ``update_time_raw`` 实测为 **6 位**(如 ``153252`` = 15:32:52),
    与快照的 8 位(``HHMMSScc``, 末 2 位为百分秒)不同, 故按长度自适应:
    6 位 → 直接 HHMMSS; 8 位 → 前 6 位 HHMMSS + 末 2 位作秒的小数。
    与 _snapshot_ts 同纪律: 休市日无"当日"可归属, 返回 None 而非伪时间戳。
    """
    if raw is None:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    text = str(value).zfill(6)
    if len(text) == 6:
        head, micro = text, 0
    elif len(text) == 8:
        head, micro = text[:6], int(text[6:]) * 10_000  # 百分秒 → 微秒
    else:
        return None
    if not head.isdigit():
        return None
    hour, minute, sec = int(head[0:2]), int(head[2:4]), int(head[4:6])
    if hour > 23 or minute > 59 or sec > 59:
        return None
    return _today_wallclock_ms(hour, minute, sec, micro)


def _safe_div(a: Any, b: Any) -> float | None:
    """安全除法(分母 0/None 或结果非有限值返回 None)。"""
    x, y = _to_float(a), _to_float(b)
    if x is None or y is None or y == 0:
        return None
    out = x / y
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _prev_close(closes: list[tuple[date | None, float | None]], event: date) -> float | None:
    """取事件日**之前**最近一个交易日的收盘价(除权参考价基准)。

    ``closes`` 需按日期升序; 事件日当天的收盘是除权**后**的价格, 不能用作基准,
    故严格取 ``d < event``。
    """
    for d, c in reversed(closes):
        if d is not None and d < event and c is not None and c > 0:
            return c
    return None


def _shares_row(rec: Any) -> dict | None:
    """``corporate.finance_batch`` 的 FinanceRecord → 面板 shares 行。

    单位: eltdx 的股本为**万股**(实测茅台 125008.15625 万股 = 12.5 亿股),
    面板契约要**股**(``float_shares > 0`` 才参与 join_asof), 故 x10000。
    缺失/非正值的流通股本返回 None(下游会丢弃 0 值, 这里提前剔除更干净)。

    symbol 解析走 ``_record_symbol``: 显式 ``exchange`` 优先, 取不到时拒绝推断(见该函数)。
    """
    symbol = _record_symbol(rec)
    if symbol is None:
        return None

    def _shares(value: Any) -> float | None:
        v = _to_float(value)
        if v is None or v <= 0:
            return None
        return v * 10_000.0  # 万股 → 股

    total = _shares(getattr(rec, "zong_gu_ben_raw_float", None))
    float_sh = _shares(getattr(rec, "liu_tong_gu_ben_raw_float", None))
    if float_sh is None:
        return None  # 面板按 float_shares 驱动换手率, 无此值则该行无意义
    period = getattr(rec, "updated_date", None)
    period_str = (
        period.isoformat() if isinstance(period, date) else (str(period) if period else None)
    )
    if not period_str:
        return None  # 无报告期/公告期会让面板合并逻辑丢弃该帧
    return {
        "symbol": symbol,
        "period_end": period_str,
        "announce_date": period_str,
        "total_shares": total if total is not None else float_sh,
        "float_shares": float_sh,
    }


# ---- 三大报表: f10 T 代码口径表 ----------------------------------------
#
# 背景: f10.finance_report 的列名是不透明 ``T***`` 代码, eltdx 包内无代码→名称字典。
# 这里的映射**不是靠算术反推**, 而是用扶摇(同花顺)同期数据逐字段交叉验证 +
# 会计恒等式自证得出的(2026-10-01 实测 3 标的 x 2 表, 见 docs/eltdx-capability-audit.md §6)。
#
# ⚠️ **同一 T 代码在不同行业模板下含义不同**, 必须按 ``nhytype`` 分流:
#   nhytype=0 通用(工商) / 1 银行 / 3 保险。实测反例: ``T039`` 在茅台是总资产
#   (3090.5 亿, 与扶摇一致), 在平安银行只有 104.6 亿而扶摇总资产 60287.9 亿 ——
#   差 576 倍。**用全局固定表映射金融股会静默写入错误总资产**。
# ``T041`` 更危险: 通用族是"现金净增加", 金融族是"capex" —— **语义互换**。
#
# 只映射已验证的合计行; 金融族的明细科目(流动资产/应收/存货等)上游无稳定对应,
# 一律不映射(留空), 不用猜测值填充。
_F10_TEMPLATE_GENERAL = 0  # nhytype=0 通用
_F10_BALANCE_MAPS: dict[str, dict[str, str]] = {
    "general": {
        "total_assets": "T039",
        "total_liabilities": "T062",
        "total_equity": "T071",
        "total_current_assets": "T020",
        "total_non_current_assets": "T038",
        "cash_and_equivalents": "T007",
        "accounts_receivable": "T010",
    },
    "financial": {
        "total_assets": "T048",
        "total_liabilities": "T083",
        "total_equity": "T093",
    },
}
_F10_CASHFLOW_MAPS: dict[str, dict[str, str]] = {
    "general": {
        "net_operating_cash_flow": "T017",
        "net_investing_cash_flow": "T029",
        "net_financing_cash_flow": "T038",
        "net_cash_change": "T041",
        "capex": "T024",
    },
    "financial": {
        "net_operating_cash_flow": "T033",
        "net_investing_cash_flow": "T044",
        "net_financing_cash_flow": "T055",
        "net_cash_change": "T058",
        "capex": "T041",
    },
}
# f10 表名 → 面板表名 / 口径表
_F10_STATEMENTS = {
    "balance_sheet": ("zcfzb", _F10_BALANCE_MAPS),
    "cash_flow": ("xjllb", _F10_CASHFLOW_MAPS),
}


def _f10_template_family(nhytype: Any) -> str:
    """``nhytype`` → 模板族名。非 0(银行 1 / 保险 3 / 其它) 一律按金融族处理。

    fail-safe 取向: 认不出行业时按**金融族**取其已验证的合计行, 而不是按通用族 ——
    通用族的 ``T039`` 落在金融报表上是个无关的小数字, 会静默产出错误总资产。
    金融族映射在通用股上只会取到 None(列不存在/为空), 随后由多源合并保留原值,
    不会污染数据。
    """
    value = _to_float(nhytype)
    if value is not None and int(value) == _F10_TEMPLATE_GENERAL:
        return "general"
    return "financial"


def _f10_statement_rows(
    result: Any, table: str, symbol: str
) -> list[dict]:
    """``f10.finance_report`` 响应 → 面板财务行(按报告期)。

    只输出**口径表里已声明的列**; 未声明字段不落盘(避免把不透明代码写进 parquet)。
    ``period_end`` 取 ``rq``(报告期, 实测 2026-06-30 形态); 无 ``rq`` 的行丢弃 ——
    面板合并逻辑按 (symbol, period_end) 归组, 缺报告期的行无意义。
    """
    spec = _F10_STATEMENTS.get(table)
    if spec is None or result is None:
        return []
    _, maps = spec

    result_sets = getattr(result, "result_sets", None) or []
    if not result_sets:
        return []
    first = result_sets[0]
    columns = list(getattr(first, "columns", None) or [])
    if not columns:
        return []
    raw_rows = list(getattr(first, "rows", None) or [])

    # 行业模板判别: 实测第 2 张结果集带 nhytype(第 1 张是宽表, 无此字段)
    nhytype = None
    for extra in result_sets[1:]:
        for row in (getattr(extra, "rows", None) or []):
            candidate = getattr(row, "nhytype", None)
            if candidate is not None:
                nhytype = candidate
                break
        if nhytype is not None:
            break
    family = _f10_template_family(nhytype)
    field_map = maps[family]

    out: list[dict] = []
    for row in raw_rows:
        period = getattr(row, "rq", None)
        if not period:
            continue
        rec: dict = {
            "symbol": symbol,
            "period_end": str(period),
            "announce_date": None,  # 上游不提供公告日, 留空由合并逻辑按报告期处理
        }
        for field, code in field_map.items():
            if code not in columns:
                continue
            value = _to_float(getattr(row, code, None))
            if value is not None:
                rec[field] = value
        # 除报告期外无任何有效数值 → 丢弃(如 lrb 的 3 列空行)
        if len(rec) > 3:
            out.append(rec)
    return out


def _snapshot_row(snap: Any) -> dict | None:
    """单个 QuoteSnapshot → 面板 realtime 行; 必需字段缺失则丢弃(不伪造)。"""
    symbol = _record_symbol(snap)
    if symbol is None:
        return None
    last = _to_float(getattr(snap, "last_price", None))
    prev = _to_float(getattr(snap, "pre_close_price", None))
    if last is None or prev is None:
        return None
    pct = _to_float(getattr(snap, "change_pct", None))
    change_amt = _to_float(getattr(snap, "change", None))
    if change_amt is None and prev is not None:
        # 固定口径推导(契约允许): change_amount = last - prev_close
        change_amt = round(last - prev, 6)
    return {
        "symbol": symbol,
        "last_price": last,
        "prev_close": prev,
        "open": _to_float(getattr(snap, "open_price", None)),
        "high": _to_float(getattr(snap, "high_price", None)),
        "low": _to_float(getattr(snap, "low_price", None)),
        # total_hand 实测为"手"(amount/(lastxhand)≈100 自验), 契约要求手 → 直用
        "volume": _to_float(getattr(snap, "total_hand", None)),
        "amount": _to_float(getattr(snap, "amount", None)),
        # ⚠️ eltdx change_pct 是百分数制(0.4425 = 0.4425%), 面板契约是小数制 → /100
        "change_pct": pct / 100.0 if pct is not None else None,
        "change_amount": change_amt,
        "name": None,  # 快照无名称, 下游用标的维表关联(契约允许置 None)
        "amplitude": None,  # eltdx 未直接提供, 置 None 由 pipeline 重算, 不启发式伪造
        "turnover_rate": None,
        "timestamp": _snapshot_ts(getattr(snap, "time_raw", None)),
    }


def _snapshot_ts(time_raw: Any) -> int | None:
    """快照时间 → 毫秒时间戳; 无法确定**真实日期**时返回 None。

    eltdx ``time_raw`` 是主站的"当日 HHMMSSmmm"紧凑整数(如 15330366 = 15:33:03.366),
    **不含日期部分**。历史上这里无条件按本地当日还原, 会使休市日返回的上一交易日
    冻结快照获得"今天"的时间戳 (2026-10-01 国庆实测: 上游停在 09-30 14:59:59.990,
    被还原成 10-01 14:59:59.990)。该伪时间戳同时击穿两道防线:

    1. quote_service._build_daily 按 quote_ts 过滤非当日记录 (专治停牌股回归),
       因时间戳已变成当日而失效;
    2. final 定版边界比较只看时刻大小, 伪时间戳晚于当日边界 → 陈旧快照被当定版落盘,
       再用 cn_today() 打戳造出与上一交易日逐行相同的假分区。

    按 CONTRIBUTING「provider 负责把供应商字段转换为内部标准格式」与「缺少能力时
    fail-closed, 禁止静默换用错误口径」, 无法确定日期时返回 None (= 日期未知),
    由服务层的 filter_halt_days 等既有防线兜底, 而不是伪造一个日期归属。
    """
    if time_raw is None:
        return None
    try:
        raw = int(time_raw)
    except (TypeError, ValueError):
        return None
    if raw < 0:
        return None
    # 主站紧凑时间戳: 时/分/秒 + 毫秒/百分秒。位数不固定(实测 8 位 = HHMMSScc,
    # 即百分秒), 故按"去掉小数部分再解 HHMMSS"的统一口径处理, 越界即判非法。
    text = str(raw)
    if len(text) < 6:
        return None
    head, frac = text[:-2], text[-2:]  # 末 2 位 = 百分秒
    if len(head) != 6 or not head.isdigit():
        return None
    hour, minute, sec = int(head[0:2]), int(head[2:4]), int(head[4:6])
    if hour > 23 or minute > 59 or sec > 59:
        return None
    ms = int(frac) * 10  # 百分秒 → 毫秒
    return _today_wallclock_ms(hour, minute, sec, ms * 1000)


class _EltDxConfig:
    """伪 config: 契约要求 provider.config.datasets 存在, 供 provider_has_dataset 路由。"""

    def __init__(self) -> None:
        self.datasets: dict[str, Any] = dict.fromkeys(_DATASETS)
        self.display_name = "eltdx (通达信)"


def _make_transport() -> Any:
    """按 ``ELTDX_TRANSPORT`` 构造传输层。

    * ``http``(**默认**) → :class:`HttpTransport`, 走独立进程的 ``eltdx-http`` 网关。
      这是**唯一推荐**方式: 连接池/运行时归网关所有, 后端重启不影响 eltdx,
      外部脚本也无法再与服务互相踩踏(见 http_client 模块 docstring 的事故记录)。
    * ``inproc`` → :class:`EltDxClient`, 进程内直连。**仅作诊断/离线场景**,
      存在"运行时崩溃牵连全部数据集""多进程踩踏"的结构性风险。

    注意: ``http`` 模式**不自动回退** ``inproc`` —— 网关不可达时按失败处理,
    避免静默退回已知有进程级风险的路径(需回退请显式设 ``ELTDX_TRANSPORT=inproc``)。
    """
    mode = os.environ.get("ELTDX_TRANSPORT", "http").strip().lower()
    if mode == "inproc":
        logger.warning(
            "eltdx 使用**进程内**传输(ELTDX_TRANSPORT=inproc) —— 存在运行时崩溃牵连"
            "全部数据集、以及多进程互相踩踏的风险, 建议改用 http 网关"
        )
        return EltDxClient(
            server_count=int(os.environ.get("ELTDX_SERVER_COUNT", DEFAULT_SERVER_COUNT)),
            connections_per_server=int(
                os.environ.get("ELTDX_CONNECTIONS_PER_SERVER", DEFAULT_CONNECTIONS_PER_SERVER)
            ),
            timeout=float(os.environ.get("ELTDX_TIMEOUT", DEFAULT_TIMEOUT_S)),
        )
    base_url = os.environ.get("ELTDX_HTTP_URL", DEFAULT_HTTP_URL)
    timeout = float(os.environ.get("ELTDX_HTTP_TIMEOUT", DEFAULT_HTTP_TIMEOUT_S))
    workers = int(os.environ.get("ELTDX_HTTP_WORKERS", str(_INTRADAY_LATEST_WORKERS)))
    # 代码表缓存 TTL(秒): 0 = 每轮都回源(最小可见时滞, 或排障用)。
    code_ttl = float(os.environ.get("ELTDX_CODE_TTL", DEFAULT_CODE_TTL_S))
    # 降级清单的陈旧度上界(秒): 回源失败时最多沿用多久之前的当日清单。
    # 0 = 不作上界(仅受"当日"约束), 不建议 —— 长时间故障会一直用很久前的清单。
    code_stale_max = float(
        os.environ.get("ELTDX_CODE_STALE_MAX", DEFAULT_CODE_STALE_MAX_S)
    )
    logger.info(
        "eltdx 使用 HTTP 网关传输: %s (timeout=%.0fs, 代码表缓存 %.0fs, 降级上界 %.0fs)",
        base_url, timeout, code_ttl, code_stale_max,
    )
    return HttpTransport(
        base_url=base_url,
        timeout=timeout,
        max_workers=workers,
        code_ttl_s=code_ttl,
        code_stale_max_s=code_stale_max,
    )


class EltDxProvider:
    """eltdx 数据源 Provider(无需继承基类, 方法签名对齐 GenericHTTPProvider)。"""

    name = "eltdx"
    builtin = True
    # 1m 历史深度: 实测 start 逐页可取到 17 个交易日以上(4000 根无缺口),
    # 面板深源默认 20 日, 故不声明 minute_history_days(视为深历史)。
    # 分钟数据源: bars.get(period='1m') 是**真 OHLC**(与日K 同一个 KlineBar 模型);
    # minutes.history 是分时点(仅 price+volume, 无 OHLC, amount 恒 0), 不用于本契约。

    def __init__(self) -> None:
        self.config = _EltDxConfig()
        self._client = _make_transport()

    # ---- 生命周期 -------------------------------------------------------

    def close(self) -> None:
        """关闭连接池(loader 重建注册表时对每个 provider 调用)。"""
        self._client.close()

    # ---- 日 K -----------------------------------------------------------

    def _daily_frame(self, rows: list[dict]) -> pl.DataFrame:
        """行 → 面板日 K 帧(复用官方 normalize_daily: 统一 cast/filter_halt_days/列序)。

        归一化走 normalizer 单源, 保证与 fuyao/tickflow 同口径(停牌日过滤等)。
        """
        if not rows:
            return pl.DataFrame()
        return normalize_daily(pl.DataFrame(rows), source="eltdx")

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | date,
        end_time: datetime | date,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        """日 K(不复权原始价): ``[symbol, date, open, high, low, close, volume, amount]``。

        eltdx 只能逐标的取 K 线, 故此处按 start/end 反推所需根数上限后并发拉取,
        再按区间过滤。区间很大的全市场同步请走 ``iter_daily``(有界分批)。
        """
        start_d = start_time.date() if isinstance(start_time, datetime) else start_time
        end_d = end_time.date() if isinstance(end_time, datetime) else end_time
        # 根数上界: 自然日 x 0.75(A 股年均交易日占比) + 余量; 用于 limit 单标的请求深度
        span_days = max(1, (end_d - start_d).days)
        count = min(int(span_days * 0.8) + 30, 8000)

        frames: list[pl.DataFrame] = []
        total = len(symbols)
        done = 0
        for batch in self._client.iter_bars_batches(
            symbols, period="day", count=count, batch_size=_DAILY_BATCH
        ):
            rows: list[dict] = []
            for symbol, bars in batch:
                for bar in bars:
                    row = _kline_row(symbol, bar)
                    if row is None:
                        continue
                    if start_d <= row["date"] <= end_d:
                        rows.append(row)
            done += len(batch)
            if on_chunk_done is not None:
                on_chunk_done(done, total)  # 契约: 空批次也要覆盖, 保证最终 cur == total
            df = self._daily_frame(rows)
            if df.height:
                frames.append(df)
        if not frames:
            return self._daily_frame([])
        return pl.concat(frames).sort(["symbol", "date"])

    def iter_daily(
        self,
        symbols: list[str],
        start_time: datetime | date,
        end_time: datetime | date,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> Any:
        """有界分批返回日 K(与 get_daily 同形); 面板全市场历史同步优先消费本方法。

        契约: 每批有明确上界、不得先全量 concat; ``on_chunk_done(cur, total)``
        必须覆盖空批次且最终 ``cur == total``。
        """
        start_d = start_time.date() if isinstance(start_time, datetime) else start_time
        end_d = end_time.date() if isinstance(end_time, datetime) else end_time
        span_days = max(1, (end_d - start_d).days)
        count = min(int(span_days * 0.8) + 30, 8000)

        total = len(symbols)
        done = 0
        for batch in self._client.iter_bars_batches(
            symbols, period="day", count=count, batch_size=_DAILY_BATCH
        ):
            rows: list[dict] = []
            for symbol, bars in batch:
                for bar in bars:
                    row = _kline_row(symbol, bar)
                    if row is None:
                        continue
                    if start_d <= row["date"] <= end_d:
                        rows.append(row)
            done += len(batch)
            if on_chunk_done is not None:
                on_chunk_done(done, total)
            yield self._daily_frame(rows)

    # ---- 除权因子(adj_factor) -------------------------------------------

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | date,
        end_time: datetime | date,
        asset_type: str = "stock",
        on_chunk_done=None,
    ) -> pl.DataFrame:
        """除权因子: ``[symbol, trade_date, ex_factor]``(单事件比值, 非累积)。

        ## 口径推导(实测标定, 见下)
        eltdx 用 ``(scale, offset)`` 二元组表达除权: ``scale`` 管送转股、``offset`` 管现金分红,
        而面板 ``ex_factor`` 是**把两类统一折算成的单事件比值**, 且**累积链由
        ``indicators.pipeline._apply_adj_factor`` 自行构建**, provider 只提供单事件值。

        推导公式(``prev``/``cur`` 为相邻两个事件):
        ``div = (cur.hfq_offset - prev.hfq_offset) / cur.hfq_scale``  → 每股分红
        ``ex_factor = (cur.hfq_scale / prev.hfq_scale) x prev_close / (prev_close - div)``

        其中 ``prev_close`` 取事件日前一交易日的**不复权**收盘(除权参考价基准)。
        标定结果: 与本地 ``data/adj_factor`` 表逐条对账 **32/32 全部吻合**,
        其中 30 条相对误差 < 0.01%, 4 条在 0.003%~0.024%(见下"精度"说明)。

        ## 精度说明(实测)
        ``hfq_offset`` 自身带有累计浮点/取整误差, 故反推的 ``div`` 与交易所公布的分红
        可能有微小出入(实测 605016.SH 推导 0.075 vs 真实 0.070006)。这导致 ``ex_factor``
        最大约 **0.024%** 的相对偏差 —— 对 1.02 量级的因子即 0.00024, 影响复权价的第 4 位
        小数, 经济上可忽略。若需与 fuyao 完全一致, 应继续用 fuyao 的 adj_factor。

        注意事项:
        * ``qfq_offset`` **不可**用于推导 —— 实测它是前复权偏移量(000001.SZ 2026-09-24
          该值增量 0.36), 而真实每股分红是 ``hfq_offset`` 增量除以 scale(0.249)。
        * 首个事件无前序参照, 无法推导比值, 跳过(累积链从第二个事件起有定义)。
        * 事件区间外的行过滤掉。
        """
        start_d = start_time.date() if isinstance(start_time, datetime) else start_time
        end_d = end_time.date() if isinstance(end_time, datetime) else end_time
        rows: list[dict] = []
        total = len(symbols)
        done = 0

        def _one(sym: str) -> list[dict]:
            code = to_eltdx_code(sym)
            if code is None:
                return []
            items = self._client.adjustment_factors(code)
            if len(items) < 2:
                return []
            # 事件日前一交易日的不复权收盘(除权参考价基准)。
            # 回看根数必须覆盖整个请求区间: 否则老事件找不到基准价而被整批跳过
            # (实测只取 120 根时, 11 年跨度只剩 6 行)。按区间自然日 x0.8(交易日占比)
            # 再留余量, 上限 8000 根(eltdx 单页 800, 自管分页)。
            span_days = max(1, (end_d - start_d).days)
            lookback = min(int(span_days * 0.8) + _ADJ_LOOKBACK_BARS, 8000)
            bars = self._client.bars(sym, period="day", count=lookback)
            closes = sorted(
                (
                    (_bar_date(getattr(b, "time", None)), _to_float(getattr(b, "close", None)))
                    for b in bars
                ),
                key=lambda kv: kv[0] or date.min,
            )
            out: list[dict] = []
            for i in range(1, len(items)):
                prev, cur = items[i - 1], items[i]
                # 事件日同样要兼容两种传输: 进程内是 date, HTTP 网关序列化成
                # '2002-07-25' 字符串。直接用 cur.date 与 date 端点比较会抛
                # TypeError('<=' not supported between date and str), 而该异常被
                # 下方的单标的软失败吞掉 —— 表现为**全市场 5584 只全部失败、
                # sync_adj 报告 "no new factors"**, 除权因子静默停更。
                cur_date = _bar_date(getattr(cur, "date", None))
                if cur_date is None:
                    continue
                if not (start_d <= cur_date <= end_d):
                    continue
                scale_ratio = _safe_div(cur.hfq_scale, prev.hfq_scale)
                if scale_ratio is None:
                    continue
                div = _safe_div(cur.hfq_offset - prev.hfq_offset, cur.hfq_scale)
                pc = _prev_close(closes, cur_date)
                if pc is None or pc <= 0:
                    continue  # 无基准价则无法推导(不伪造)
                denom = pc - (div or 0.0)
                if denom <= 0:
                    continue
                ex = scale_ratio * pc / denom
                out.append({"symbol": sym, "trade_date": cur_date, "ex_factor": ex})
            return out

        workers = min(_ADJ_WORKERS, max(1, total))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, s): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    rows.extend(fut.result())
                except Exception as e:  # 单标的软失败: 不影响整批
                    logger.warning("eltdx 除权因子失败 %s: %s", sym, e)
                finally:
                    done += 1
                    if on_chunk_done is not None:
                        on_chunk_done(done, total)
        if not rows:
            return pl.DataFrame()
        return (
            pl.DataFrame(rows, infer_schema_length=None)
            .select(_ADJ_COLUMNS)
            .drop_nulls()
            .unique(subset=["symbol", "trade_date"], keep="last")
            .sort(["symbol", "trade_date"])
        )

    # ---- 分钟 K ---------------------------------------------------------

    def _minute_rows(
        self, symbol: str, bars: list[Any], start_dt: datetime, end_dt: datetime
    ) -> list[dict]:
        """KlineBar(1m) → 面板分钟行; 区间外丢弃, 关键字段缺失丢弃(不伪造)。"""
        rows: list[dict] = []
        for bar in bars:
            ts = _bar_datetime(bar)
            if ts is None or not (start_dt <= ts <= end_dt):
                continue
            o = _to_float(getattr(bar, "open", None))
            h = _to_float(getattr(bar, "high", None))
            low = _to_float(getattr(bar, "low", None))
            c = _to_float(getattr(bar, "close", None))
            if o is None or h is None or low is None or c is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    # 契约: 北京时间墙钟 naive; 先转 UTC+8 再抹 tzinfo(源本就是 +08:00,
                    # 这样即使上游换了时区表示也仍是正确的北京墙钟)
                    "datetime": ts.astimezone(_CN_TZ).replace(tzinfo=None),
                    "open": o,
                    "high": h,
                    "low": low,
                    "close": c,
                    "volume": _to_float(getattr(bar, "volume_lots", None)),  # 手(实测与日K同源)
                    "amount": _to_float(getattr(bar, "amount", None)),  # 元
                }
            )
        return rows

    @staticmethod
    def _minute_frame(rows: list[dict]) -> pl.DataFrame:
        """分钟行 → 面板契约帧 [symbol, datetime, open, high, low, close, volume, amount]。"""
        if not rows:
            return pl.DataFrame(
                schema={
                    "symbol": pl.String,
                    "datetime": pl.Datetime,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Float64,
                    "amount": pl.Float64,
                }
            )
        return pl.DataFrame(rows).select(_MINUTE_COLUMNS)

    def _minutes_for(
        self, symbols: list[str], start_time: datetime | date, end_time: datetime | date
    ) -> list[dict]:
        """按区间拉 1m 并归一化为面板分钟行(逐标的并发, 单标的软失败)。"""
        start_dt = _as_datetime(start_time, end_of_day=False)
        end_dt = _as_datetime(end_time, end_of_day=True)
        span_days = max(1, (end_dt.date() - start_dt.date()).days)
        # 1m **每日 240 根**(09:31~11:30 + 13:01~15:00)。自然日 -> 交易日约 0.8 占比,
        # 再按每日根数放大并留余量。注意: 必须乘 240 —— 否则把"自然日数"当成"K 线根数",
        # 5 天只会取到 14 根(不到 1 天), 落盘就只剩最后一天(实测踩过)。
        est_days = int(span_days * 0.8) + 2
        count = min(est_days * 240 + 240, _MINUTE_MAX_BARS)
        rows: list[dict] = []

        def _one(sym: str) -> list[dict]:
            bars = self._client.bars(sym, period="1m", count=count)
            return self._minute_rows(sym, bars, start_dt, end_dt)

        workers = min(_MINUTE_WORKERS, max(1, len(symbols)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, s): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    rows.extend(fut.result())
                except Exception as e:  # 单标的软失败: 不影响其他标的
                    logger.warning("eltdx 分钟取数失败 %s: %s", sym, e)
        return rows

    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | date,
        end_time: datetime | date,
        asset_type: str = "stock",
        on_chunk_done=None,
        freq: str = "1m",
    ) -> pl.DataFrame:
        """分钟 K(1m): ``[symbol, datetime(北京墙钟 naive), open, high, low, close, volume, amount]``。

        ``freq`` 仅支持 1m(面板当前只消费 1m); 其他周期由本地 1m 聚合(与日K 派生链同思路)。
        eltdx 的 1m 来自 ``bars.get``, 是真 OHLC(非分时点), volume 为手、amount 为元。
        """
        if freq not in ("1m", "1min", "1"):
            logger.warning("eltdx 分钟仅提供 1m, 请求 freq=%s 原样按 1m 返回", freq)
        rows = self._minutes_for(symbols, start_time, end_time)
        if on_chunk_done is not None:
            on_chunk_done(len(symbols), len(symbols))
        return self._minute_frame(rows)

    # ---- 全量分钟(full_minute): 修复轮 + 稳态增量轮 ----------------------

    def get_intraday_batch(
        self, symbols: list[str], count: int = 300, asset_type: str = "stock"
    ) -> pl.DataFrame:
        """全量分钟**修复轮**: 给定标的当日每只最近 ``count`` 根 1m(同日窗口)。

        服务在冷启动/覆盖断档/连续空轮时调用; 返回 canonical 8 列(同 get_minute)。
        内部自行分块并发, 不走调用方分批。
        """
        today = date.today()
        rows = self._minutes_for(symbols, today, today)
        if count and count > 0:
            # 只保留每只标的最新 count 根(修复轮语义: 当日窗口)
            by_sym: dict[str, list[dict]] = {}
            for r in rows:
                by_sym.setdefault(r["symbol"], []).append(r)
            kept: list[dict] = []
            for vals in by_sym.values():
                vals.sort(key=lambda x: x["datetime"])
                kept.extend(vals[-count:])
            rows = kept
        return self._minute_frame(rows)

    def get_intraday_latest(self, symbols: list[str] | None = None, count: int = 3) -> pl.DataFrame:
        """全量分钟**稳态增量轮**: 返回标的当日每只最新 ``count`` 根 1m。

        实现依据(实测): eltdx 的 ``bars.get`` **支持批量 codes** 且上限很高 ——
        实测 2000 只/请求 3.90s 且足额返回; 全市场 5578 只按 1000 分片共 6 批,
        一遍约 **11.7s** 拿到 16713 根(每只 3 根)。故本方法可真正实现增量轮,
        服务节奏将变为 ``max(3s, 单轮耗时) ≈ 12s`` —— 比仅修复轮(60s)快约 5 倍。

        注意与快照的差别: ``quotes.get_snapshots`` 有 **80 只硬上限**(超出静默截断/
        断连), 而 ``bars.get`` 批量无此限制, 故这里用 bars 而非快照。

        ``symbols=None`` 表示全市场(服务当前总是这样调用: ``method(count=count)``)。
        """
        syms = symbols if symbols else self._client.all_a_shares()
        if not syms:
            logger.warning("eltdx get_intraday_latest: 取不到代码表")
            return self._minute_frame([])
        today = date.today()
        start_dt = _as_datetime(today, end_of_day=False)
        end_dt = _as_datetime(today, end_of_day=True)
        want = max(1, int(count))
        rows: list[dict] = []
        # 每片取 want 根本身已是最新 N 根; 逐片并发
        chunks = [
            syms[i : i + _INTRADAY_LATEST_BATCH]
            for i in range(0, len(syms), _INTRADAY_LATEST_BATCH)
        ]

        def _one(chunk: list[str]) -> list[dict]:
            got = self._client.bars_multi(chunk, period="1m", count=want)
            out: list[dict] = []
            for sym, bars in got:
                out.extend(self._minute_rows(sym, bars, start_dt, end_dt))
            return out

        workers = min(_INTRADAY_LATEST_WORKERS, max(1, len(chunks)))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, ch) for ch in chunks]
            for fut in as_completed(futures):
                try:
                    rows.extend(fut.result())
                except Exception as e:  # 单片失败隔离: 其他片仍返回
                    logger.warning("eltdx get_intraday_latest 分片失败(已隔离): %s", e)
        return self._minute_frame(rows)

    # ---- 实时快照 -------------------------------------------------------

    def get_realtime(self) -> list[dict]:
        """全市场实时快照 → ``list[dict]``。**软失败返回 []**(不抛异常)。"""
        symbols = self._client.all_a_shares()
        if not symbols:
            logger.warning("eltdx get_realtime: 取不到代码表, 本轮返回空")
            return []
        snaps = self._client.snapshots(symbols, batch_size=_SNAPSHOT_BATCH)
        out: list[dict] = []
        for snap in snaps:
            row = _snapshot_row(snap)
            if row is not None:
                out.append(row)
        if not out:
            logger.warning("eltdx get_realtime: 快照全部无法映射(上游结构可能变化), 返回空")
        return out

    def get_realtime_indices(self, symbols: list[str]) -> list[dict] | None:
        """指数实时快照 → ``list[dict]``(行字段同 get_realtime)。

        契约: 失败返回 None(保留上轮缓存), 成功无数据返回 []。
        eltdx 快照接口可同时返回指数, 故与 A 股同路径。
        """
        if not symbols:
            return []
        try:
            snaps = self._client.snapshots(symbols, batch_size=_SNAPSHOT_BATCH)
        except Exception as e:  # 软失败: 返回 None 让服务保留上一轮有效缓存
            logger.warning("eltdx 指数快照失败(软失败, 保留上轮缓存): %s", e)
            return None
        if not snaps:
            return None  # 软失败: 让服务保留上一轮有效缓存
        out: list[dict] = []
        for snap in snaps:
            row = _snapshot_row(snap)
            if row is not None:
                out.append(row)
        return out

    # ---- 五档盘口(depth5) ----------------------------------------------

    def get_depth_batch(self, symbols: list[str]) -> dict[str, dict]:
        """五档盘口 → ``{symbol: {bid_prices, bid_volumes, ask_prices, ask_volumes, timestamp}}``。

        契约(docs/plugin-development.md): 价量数组**按一档到五档排列**, 数量单位为**手**,
        ``timestamp`` 为毫秒 Unix 时间戳; 服务层按 capability 统一分片限速, provider
        **不自行切换/回退**到其他数据源(故这里失败即抛异常, 由服务隔离该批)。

        实测口径(eltdx 3.2.2): ``quotes.get_depth(codes).records[].buy_levels/sell_levels``
        各 5 档 ``QuoteLevel(price, volume)``, volume 单位为手(五档合计与全日总量量级自洽)。
        **注意**: 封死涨停时卖一量为 0 —— 0 是有意义的值(服务据此判定"真封"),
        故必须原样输出 0 而非 None/跳过。
        """
        if not symbols:
            return {}
        # 注意: client.depth() 自己会做面板格式 → eltdx 代码的转换, 故这里传原始 symbol,
        # 不能传已转换的 eltdx 代码(会被二次转换判为非法而全部丢弃)。
        valid = [s for s in symbols if to_eltdx_code(s)]
        if not valid:
            logger.warning("eltdx get_depth_batch: 无有效代码(入参 %d 个)", len(symbols))
            return {}
        page = self._client.depth(valid)  # 失败抛异常: 由服务按批隔离, 不跨源回退
        records = list(getattr(page, "records", ()) or ())
        if not records:
            logger.warning("eltdx get_depth_batch: 返回 0 条(请求 %d 只)", len(valid))
            return {}

        out: dict[str, dict] = {}
        for rec in records:
            symbol = _record_symbol(rec)
            if symbol is None:
                continue
            row = _depth_row(rec)
            if row is not None:
                out[symbol] = row
        return out

    # ---- 集合竞价(auction) ----------------------------------------------

    def get_auction_batch(self, symbols: list[str], trade_date: date) -> pl.DataFrame:
        """集合竞价逐点 → 标准长表(契约见 ``data_providers.base.get_auction_batch``)。

        上游 ``auctions.series`` 是**逐标的**接口(无批量端点), 实测约 78ms/只,
        连接池并发下 ~360 只/秒(经 HTTP 网关, 2026-10-08 实测), 故内部并发打点。

        失败语义与 depth5 一致: 单标的异常只丢该标的(空 points 本就是正常状态,
        非交易日/超约 12 个月回溯窗口/北交所多数个股与部分指数都没有竞价),
        **全批都失败**才上抛, 由服务层按批隔离, provider 不跨源回退。
        """
        valid = [s for s in symbols if to_eltdx_code(s)]
        if not valid:
            logger.warning("eltdx get_auction_batch: 无有效代码(入参 %d 个)", len(symbols))
            return pl.DataFrame(schema=AUCTION_SCHEMA)
        pairs, failures = self._client.auction_batch(valid, trade_date)
        if not pairs and failures >= len(valid):
            raise RuntimeError(f"eltdx 竞价全批失败({failures}/{len(valid)} 只)")
        rows: list[dict] = []
        for sym, series in pairs:
            rows.extend(_auction_rows(sym, trade_date, series))
        if not rows:
            return pl.DataFrame(schema=AUCTION_SCHEMA)
        return pl.DataFrame(rows, schema=AUCTION_SCHEMA).select(AUCTION_COLUMNS)

    # ---- 财务 -------------------------------------------------------------

    def get_financials(
        self, table: str, symbols: list[str], latest_only: bool = False
    ) -> pl.DataFrame:
        """财务数据: ``shares`` / ``balance_sheet`` / ``cash_flow``; 其余返回空帧。

        ## 各表来源与口径

        * ``shares`` ← ``corporate.finance_batch``(字段名明确, 见 ``_shares_row``)。
        * ``balance_sheet`` ← ``f10.finance_report('zcfzb')``
        * ``cash_flow`` ← ``f10.finance_report('xjllb')``
          两者的 ``T***`` 代码含义由 ``_F10_*_MAP`` 声明, 按 ``nhytype`` 分通用/金融两族。
        * ``income`` ← **上游无数据**, 恒返回空帧, 由多源合并保留 fuyao 值:
          实测 ``lrb`` 只回 3 列(``rtype``/``nhytype``/``zqname``)无数值, 而
          ``zcfzb``/``xjllb`` 正常回 102/71 列、99/69 列; 试过 16 个候选
          ``report_type`` 取值均无数值 —— 这是**上游缺口**, 不是客户端解析问题。
        * ``metrics`` ← 未接入(指标接口为单股单期, 面板 metrics 走 fuyao)。

        ## 为什么现在敢接三大报表

        早前按"口径不明确不接"跳过的理由是不透明代码只能靠算术反推。现已用
        **扶摇同期数据逐字段交叉验证 + 会计恒等式自证**(2026-10-01, 3 标的 x 2 表):
        9 个资产负债表字段与扶摇**逐位精确相等**, 恒等式在通用/金融两族上均成立。
        关键前提是 ``nhytype`` 模板判别 —— 缺了它, 金融股会被通用族映射静默写错
        (``T039`` 在平安银行仅 104.6 亿, 而真实总资产 60287.9 亿)。

        ## 单位
        ``f10`` 的金额已是**元**(实测茅台总资产 309050784569.31), 与面板契约一致,
        无需换算。``shares`` 另见 ``_shares_row``(万股 → 股)。
        """
        if table == "shares":
            return self._shares_table(symbols)
        if table not in _F10_STATEMENTS:
            logger.info(
                "eltdx get_financials: 表 %s 未接入, 返回空帧交由多源合并保留原值", table
            )
            return pl.DataFrame()
        return self._statement_table(table, symbols, latest_only=latest_only)

    def _shares_table(self, symbols: list[str]) -> pl.DataFrame:
        """``shares`` 表: ``corporate.finance_batch`` → 面板股本行。"""
        codes = [s for s in symbols if to_eltdx_code(s)]
        if not codes:
            return pl.DataFrame()
        rows: list[dict] = []
        batch = max(1, _FINANCE_BATCH)
        stats = {"ok": 0, "retry_ok": 0, "split_ok": 0, "fail": 0}
        for i in range(0, len(codes), batch):
            chunk = codes[i : i + batch]
            records = self._finance_records(chunk, stats)
            for rec in records:
                row = _shares_row(rec)
                if row is not None:
                    rows.append(row)
        if stats["retry_ok"] or stats["split_ok"] or stats["fail"]:
            logger.info(
                "eltdx shares 取数: 首次成功 %d 批, 重试成功 %d 批, 拆分成功 %d 批, 最终失败 %d 批",
                stats["ok"],
                stats["retry_ok"],
                stats["split_ok"],
                stats["fail"],
            )
        if not rows:
            logger.warning("eltdx get_financials(shares): 未取到有效行(请求 %d 只)", len(codes))
            return pl.DataFrame()
        df = pl.DataFrame(rows, infer_schema_length=None)
        keep = [c for c in _SHARES_COLUMNS if c in df.columns]
        return df.select(keep).unique(subset=["symbol", "period_end"], keep="last")

    def _statement_table(
        self, table: str, symbols: list[str], *, latest_only: bool
    ) -> pl.DataFrame:
        """``balance_sheet`` / ``cash_flow``: 逐标的取 f10 报表(上游为单标的接口)。

        逐标的失败**按标的隔离**(不拖垮整表), 与 daily 的按批隔离同思路:
        面板的备份源补齐逻辑正是靠"某标的没取到"来决定是否回退, 单只失败必须
        表现为该标的缺行, 而不是整表抛异常。
        """
        report_type, _ = _F10_STATEMENTS[table]
        panels = [s for s in symbols if to_eltdx_code(s)]
        if not panels:
            return pl.DataFrame()
        rows: list[dict] = []
        failed = 0
        for i, symbol in enumerate(panels):
            if i:
                time.sleep(_F10_INTERVAL_S)
            code = to_eltdx_code(symbol)
            try:
                result = self._client.finance_report(code, report_type)
            except Exception as e:  # 单标的失败只隔离该只
                failed += 1
                logger.debug("eltdx f10 %s %s 失败: %s", report_type, symbol, e)
                continue
            parsed = _f10_statement_rows(result, table, symbol)
            if not parsed:
                continue
            if latest_only:
                parsed = [max(parsed, key=lambda r: r["period_end"])]
            rows.extend(parsed)
        if failed:
            logger.info(
                "eltdx f10 %s: %d/%d 只标的失败(已隔离), 成功 %d 只",
                report_type, failed, len(panels), len(panels) - failed,
            )
        if not rows:
            logger.warning(
                "eltdx f10 %s: 未取到有效行(请求 %d 只)", report_type, len(panels)
            )
            return pl.DataFrame()
        df = pl.DataFrame(rows, infer_schema_length=None)
        return df.unique(subset=["symbol", "period_end"], keep="last").sort(
            ["symbol", "period_end"]
        )

    def _finance_records(self, chunk: list[str], stats: dict[str, int]) -> list[Any]:
        """取一批财务记录, 失败时**重试**并最终**拆半递归**。

        实测(eltaX 3.2.2): ``corporate.finance_batch`` 对单次请求的代码组合敏感 ——
        即使批大小只有 20, 仍有约 40% 的批报 ``invalid ASCII response code``;
        且失败**可复现**(同一批代码重复跑结果一致), 无简单规律。
        因此策略为: 原批 → 重试 N 次 → 仍失败则**二分拆分**继续取, 直至单只。

        ``stats`` 只用于汇总日志(首次/重试/拆分/最终失败)。
        """
        if not chunk:
            return []
        last_err: Exception | None = None
        for attempt in range(_FINANCE_RETRIES + 1):
            try:
                resp = self._client.finance_batch([to_eltdx_code(c) for c in chunk])
                recs = list(getattr(resp, "records", ()) or ())
                if recs or len(chunk) == 0:
                    stats["ok" if attempt == 0 else "retry_ok"] += 1
                    return recs
                # 返回空也算失败(该批无数据), 继续重试
                last_err = RuntimeError("empty records")
            except Exception as e:  # 单批失败不拖垮整表, 由重试/拆分兜底
                last_err = e
            if attempt < _FINANCE_RETRIES:
                time.sleep(_FINANCE_RETRY_SLEEP_S)
        # 重试仍失败 → 二分拆分(把"毒组合"拆开)
        if len(chunk) > 1:
            mid = len(chunk) // 2
            stats["split_ok"] += 1
            return self._finance_records(chunk[:mid], stats) + self._finance_records(
                chunk[mid:], stats
            )
        stats["fail"] += 1
        logger.debug("eltdx finance_batch 单只最终失败 %s: %s", chunk[0], last_err)
        return []

    # ---- 设置页「试拉」 -------------------------------------------------

    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        """设置页「试拉测试」: 返回 {provider, dataset, rows, columns, preview, error?}。"""
        if dataset not in _DATASETS:
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "error": f"eltdx 未声明数据集 {dataset!r}, 该数据集会回退 TickFlow",
            }
        try:
            if dataset == "daily":
                syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
                df = self.get_daily(syms, datetime.now() - timedelta(days=30), datetime.now())
                return self._preview(dataset, df)
            if dataset == "minute":
                syms = [s for s in (symbols or [])][:2] or ["000001.SZ"]
                df = self.get_minute(syms, date.today(), date.today())
                return self._preview(dataset, df)
            if dataset == "full_minute":
                syms = [s for s in (symbols or [])][:2] or ["000001.SZ"]
                df = self.get_intraday_batch(syms, count=10)
                return self._preview(dataset, df)
            if dataset == "depth5":
                syms = [s for s in (symbols or [])][:3] or ["000001.SZ"]
                data = self.get_depth_batch(syms)
                rows = [
                    {
                        "symbol": k,
                        "bid1_vol": (v["bid_volumes"] or [None])[0],
                        "ask1_vol": (v["ask_volumes"] or [None])[0],
                        "bid_prices": v["bid_prices"],
                        "ask_prices": v["ask_prices"],
                    }
                    for k, v in data.items()
                ]
                df = pl.DataFrame(rows) if rows else pl.DataFrame()
                return self._preview(dataset, df)
            if dataset == "auction":
                syms = [s for s in (symbols or [])][:2] or ["000001.SZ"]
                df = self.get_auction_batch(syms, date.today())
                out = self._preview(dataset, df)
                if df.is_empty():
                    out["note"] = (
                        "当日无竞价点: 可能休市/尚未到竞价窗口/该标的无竞价数据"
                        "(北交所多数个股与部分指数没有), 也可能已超约 12 个月回溯窗口"
                    )
                return out
            if dataset == "financial":
                syms = [s for s in (symbols or [])][:3] or ["600519.SH"]
                df = self.get_financials("shares", syms)
                out = self._preview(dataset, df)
                out["note"] = "eltdx 财务只实现 shares 表; metrics/三大报表回退 TickFlow"
                return out
            if dataset == "adj_factor":
                syms = [s for s in (symbols or [])][:3] or ["600519.SH"]
                df = self.get_adj_factors(
                    syms, datetime.now() - timedelta(days=730), datetime.now()
                )
                out = self._preview(dataset, df)
                out["note"] = (
                    "ex_factor 由 hfq_scale/hfq_offset 推导(单事件比值); "
                    "与 fuyao 相比最大约 0.024% 偏差(offset 累计精度)"
                )
                return out
            syms = [s for s in (symbols or [])][:5]
            rows = self.get_realtime_indices(syms) or [] if syms else self.get_realtime()
            df = pl.DataFrame(rows[:20]) if rows else pl.DataFrame()
            return self._preview(dataset, df)
        except Exception as e:
            return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}

    @staticmethod
    def _preview(dataset: str, df: pl.DataFrame) -> dict:
        head = df.head(5).to_dicts()
        for row in head:  # date/datetime → ISO 字符串, 保证 JSON 可序列化
            for k, v in list(row.items()):
                if isinstance(v, (date, datetime)):
                    row[k] = v.isoformat()
        return {
            "provider": "eltdx",
            "dataset": dataset,
            "rows": df.height,
            "columns": df.columns,
            "preview": head,
        }
