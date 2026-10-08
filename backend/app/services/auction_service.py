"""集合竞价服务 — 全市场竞价轮、按日落盘、竞价榜聚合。

架构(与 depth sealed 同为独立旁路线, 不写回 enriched 14 列):
  - 数据集 `auction` 走标准路由(`preferences.get_auction_data_provider`),
    列契约单源在 `app.data_providers.base.AUCTION_COLUMNS/SCHEMA`
  - 长表逐点落盘 `data/auction/date=YYYY-MM-DD/part.parquet`
  - 单股图读逐点, 全市场榜读聚合(每标的每段末点)

**为什么是两次扫描而不是盘中轮询**: 上游 `auctions.series` 返回的是**已完成的
竞价段**(09:15→09:25 开盘段 / 14:57→15:00 收盘段), 段内点位不可变、窗口外不再
增长。09:26 扫一次拿到开盘段(盘前决策窗口当天即可用), 15:01 再扫一次拿到含收盘段
的全天序列并覆盖。轮询只会重复拉同一批不可变数据、白烧通达信连接。

**为什么必须落盘**: 实测回溯窗口只有约 12 个月(2025-10-09 有、2025-06-03 已空),
不落盘就没有竞价历史。

失败语义(照 depth 的按批隔离, provider 不跨源回退):
  - 单标的异常/空 points 由 provider 丢弃该标的 —— **空是正常状态**(非交易日、
    超回溯窗口、北交所多数个股都没有竞价), 不能当失败
  - 单批失败只丢该批, 其余批照常
  - 落盘按标的合并: 本轮拿到的标的覆盖旧行, 本轮缺失的标的保留旧行 →
    退化轮永远不会缩短历史覆盖
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date
from datetime import time as dt_time
from pathlib import Path

import polars as pl

from app.data_providers.base import AUCTION_COLUMNS, AUCTION_SCHEMA
from app.market_time import cn_now, cn_today
from app.tickflow.capabilities import Cap
from app.tickflow.rate_limits import chunked, resolve_limit, sleep_between_batches

logger = logging.getLogger(__name__)

# 服务层分批粒度: 上游是逐标的接口(provider 内部按连接池 slot 并发), 这里只决定
# "一批失败丢多少标的"。实测网关 ~360 只/秒 → 200 只/批 ≈ 0.6s, 全市场约 30 批。
_BATCH = int(os.environ.get("AUCTION_BATCH_SIZE", "200"))

# 竞价段: 开盘集合竞价 / 收盘集合竞价
_SEGMENTS = ("open", "close")

# 榜默认排序键 → polars 表达式(降序)。unmatched_amount 带方向符号排序会偏,
# 故未匹配额按**额本身**排, 方向由 unmatched_side 呈现。
_BOARD_SORTS = {
    "matched_amount": "matched_amount",
    "unmatched_amount": "unmatched_amount",
    "matched_volume": "matched_volume",
    "change_ratio": "auction_change_ratio",
}

# 已落盘日的聚合榜缓存(按日, LRU 上限): 榜只随扫描变化, 不必每次请求重算
_BOARD_CACHE_MAX = 6


class AuctionService:
    """集合竞价服务 — 单例。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 全市场扫描会写同一份按日 parquet: 手动触发 / 定时任务 / 启动补跑可能并发,
        # 网络阶段在锁外, 合并+落盘在锁内(照 depth_service._fetch_lock 的做法)。
        self._sweep_lock = threading.Lock()
        self._repo = None
        self._app_state = None

        self._board_cache: dict[date, pl.DataFrame] = {}
        self._last_sweep: dict = {}          # 最近一次扫描的统计(供状态端点/排障)
        self._sweeping = False

    # ================================================================
    # 注入
    # ================================================================

    def set_repo(self, repo) -> None:
        self._repo = repo

    def set_app_state(self, app_state) -> None:
        self._app_state = app_state

    # ================================================================
    # 生命周期
    # ================================================================

    def boot_check(self) -> None:
        """启动补跑: 今天(交易日)无分区时后台扫一次, 不阻塞启动(全市场约 15~30s)。"""
        if not self.available():
            logger.info("auction: 竞价数据源不可用, 跳过启动补跑")
            return
        today = cn_today()
        if self._persisted_for_date(today):
            return
        if not self._sweep_due(today):
            return
        from app.services import trading_day

        if trading_day.is_trading_day(today) is False:
            return
        threading.Thread(
            target=self._catch_up, args=(today,), daemon=True, name="auction-boot"
        ).start()

    def _catch_up(self, today: date) -> None:
        """启动补跑线程体: 收盘段要等 15:01 之后才有, 之前只补开盘段。"""
        try:
            self.sweep(today)
        except Exception as e:  # noqa: BLE001
            logger.warning("auction 启动补跑失败: %s", e)

    def _sweep_due(self, d: date) -> bool:
        """该交易日的竞价窗口是否已经产生数据(09:25 之后)。

        09:26 之前扫只能拿到空序列, 白烧通达信连接; 定时任务本身就在 09:26/15:01,
        这里只给启动补跑用(且今天本轮还没扫成功过)。
        """
        if d != cn_today():
            return False
        if cn_now().time() < dt_time(9, 26):
            return False
        last = self._last_sweep
        return last.get("trade_date") != d.isoformat() or not last.get("ok")

    # ================================================================
    # 能力 / 路由
    # ================================================================

    def available(self) -> bool:
        """竞价能力是否就绪: 路由到的源真实提供该数据集(fail-closed, 不跨源回退)。

        用 `Cap.AUCTION_BATCH` 而非 TickFlow 档位: 该能力由
        `policy._augment_custom_sources` 在"路由到的第三方源声明 auction"时补授,
        因此它对插件源与 TickFlow 口径一致; TickFlow SDK 没有竞价接口, 不会补授。
        """
        capset = self._get_capset()
        return capset.has(Cap.AUCTION_BATCH)

    def _get_capset(self):
        if self._app_state:
            cs = getattr(self._app_state, "capabilities", None)
            if cs:
                return cs
        from app.tickflow.policy import detect_capabilities

        return detect_capabilities()

    def _resolve_provider(self):
        """路由偏好 → provider 实例; 不可用时返回 (None, 原因)。"""
        from app.services import preferences

        name = preferences.get_auction_data_provider()
        provider = None
        if name == "tickflow":
            from app.data_providers.registry import get_provider

            provider = get_provider("tickflow")
        else:
            from app.data_providers import custom as custom_sources

            if not custom_sources.provider_has_dataset(name, "auction"):
                return None, f"数据源 {name} 未声明 auction 数据集"
            provider = custom_sources.get_provider(name)

        if not callable(getattr(provider, "get_auction_batch", None)):
            return None, f"数据源 {name} 未实现 get_auction_batch"
        return provider, name

    def _universe(self) -> list[str]:
        """全市场 A 股标的(instruments 维表, 与 minute_refresh._universe 同一来源)。

        榜只收股票: 指数的 price 单位是「点」、volume 单位「手」无意义, 混进竞价额
        榜会产出错误金融读数。ETF/指数的单股竞价走 `get_series` 的缺失回源路径。
        """
        if not self._repo:
            return []
        inst = self._repo.get_instruments()
        if inst.is_empty() or "symbol" not in inst.columns:
            return []
        return inst["symbol"].cast(pl.Utf8).unique().sort().to_list()

    # ================================================================
    # 扫描 + 落盘
    # ================================================================

    def sweep(self, trade_date: date | None = None, *, persist: bool = True) -> dict:
        """全市场竞价扫描 → 长表 →(可选)按日落盘。

        返回统计 dict(请求/命中标的数、行数、是否落盘、耗时、错误), 供手动触发与
        状态端点展示; 不抛异常 —— 竞价是旁路线, 失败不得影响行情管道。
        """
        d = trade_date or cn_today()
        started = time.perf_counter()
        stats: dict = {
            "trade_date": d.isoformat(),
            "requested": 0,
            "symbols": 0,
            "rows": 0,
            "persisted": False,
            "ok": False,
            "msg": "",
        }
        if not self.available():
            stats["msg"] = "无竞价数据源(数据源配置里把「集合竞价」指向可用源)"
            return stats
        provider, route = self._resolve_provider()
        stats["provider"] = route
        if provider is None:
            stats["msg"] = route
            return stats

        symbols = self._universe()
        if not symbols:
            stats["msg"] = "标的池为空(instruments 维表未同步)"
            return stats
        stats["requested"] = len(symbols)

        fetch_batch = provider.get_auction_batch
        limit = resolve_limit(self._get_capset(), Cap.AUCTION_BATCH, default_batch=_BATCH)
        batch_size = limit.batch or _BATCH
        chunks = chunked(symbols, batch_size)

        self._sweeping = True
        frames: list[pl.DataFrame] = []
        failed_batches = 0
        try:
            for i, chunk in enumerate(chunks):
                sleep_between_batches(i, limit.rpm, default_interval=0.0)
                try:
                    df = fetch_batch(chunk, d)
                except Exception as e:  # noqa: BLE001
                    failed_batches += 1
                    logger.warning("auction 第 %d 批失败(%d 只): %s", i + 1, len(chunk), e)
                    continue  # 按批隔离: 单批失败不拖垮整轮
                if df is None or df.is_empty():
                    continue  # 空是正常状态(非交易日/无竞价的标的), 不是失败
                if not isinstance(df, pl.DataFrame):
                    logger.warning("auction 第 %d 批返回非 DataFrame, 已跳过", i + 1)
                    continue
                frames.append(df.select(AUCTION_COLUMNS))

            with self._sweep_lock:
                fetched = (
                    pl.concat(frames, how="vertical") if frames
                    else pl.DataFrame(schema=AUCTION_SCHEMA)
                )
                stats["symbols"] = fetched["symbol"].n_unique() if not fetched.is_empty() else 0
                stats["rows"] = fetched.height
                if persist and not fetched.is_empty():
                    stats["persisted"] = self._persist(d, fetched)
                self._board_cache.pop(d, None)  # 覆盖写后失效, 下次查询重算
        finally:
            self._sweeping = False

        stats["ok"] = stats["symbols"] > 0
        stats["failed_batches"] = failed_batches
        stats["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
        if not stats["ok"]:
            stats["msg"] = (
                f"{d} 无竞价数据(休市日/超回溯窗口/全部 {failed_batches} 批失败)"
                if not failed_batches
                else f"全部 {failed_batches} 批取数失败"
            )
        self._last_sweep = stats
        logger.info(
            "auction 扫描: %s 请求 %d 只 → 命中 %d 只/%d 行, 失败批 %d, 落盘=%s, %.0fms",
            d, stats["requested"], stats["symbols"], stats["rows"],
            failed_batches, stats["persisted"], stats["elapsed_ms"],
        )
        return stats

    def _persist(self, d: date, new: pl.DataFrame) -> bool:
        """按日合并落盘: 本轮标的覆盖旧行, 本轮缺失的标的保留旧行。

        退化轮(网络抖动只拿到部分标的)因此只会补全、不会缩短历史覆盖。
        """
        if not self._repo:
            return False
        out = self._partition_path(d)
        out.parent.mkdir(parents=True, exist_ok=True)
        df = new
        if out.exists():
            try:
                old = pl.read_parquet(out).select(AUCTION_COLUMNS)
                kept = old.filter(~pl.col("symbol").is_in(new["symbol"].unique().to_list()))
                if kept.height:
                    df = pl.concat([new, kept], how="vertical")
            except Exception as e:  # noqa: BLE001
                logger.warning("auction 旧分区读取失败(按无历史覆盖): %s", e)
        df = df.select(AUCTION_COLUMNS).sort(["symbol", "segment", "datetime"])
        # 原子写: 临时文件 + os.replace, 读侧不会看到半写文件
        tmp = out.with_name(out.name + ".tmp")
        df.write_parquet(tmp)
        os.replace(tmp, out)
        logger.info("auction 落盘: %d 行 → %s", df.height, out)
        return True

    def _partition_path(self, d: date) -> Path:
        return self._repo.store.data_dir / "auction" / f"date={d.isoformat()}" / "part.parquet"

    def _persisted_for_date(self, d: date) -> bool:
        if not self._repo:
            return False
        return self._partition_path(d).exists()

    # ================================================================
    # 读取
    # ================================================================

    def _read_partition(self, d: date) -> pl.DataFrame:
        """读某日逐点长表(无文件返回空帧)。"""
        if not self._repo:
            return pl.DataFrame()
        path = self._partition_path(d)
        if not path.exists():
            return pl.DataFrame()
        try:
            return pl.read_parquet(path).select(AUCTION_COLUMNS)
        except Exception as e:  # noqa: BLE001
            logger.warning("auction 分区读取失败 %s: %s", path, e)
            return pl.DataFrame()

    def get_series(self, symbol: str, trade_date: date) -> dict:
        """单股竞价逐点序列(个股竞价图)。

        返回 {"symbol", "trade_date", "prev_close", "points": [...], "source", "msg"}:
        points 按 datetime 升序, 空 points 是正常状态(该日休市/该标的无竞价/
        超约 12 个月回溯窗口), msg 说明原因, 不伪造数据。
        prev_close 为**该竞价日**的上一交易日收盘(见 `_prev_close_frame`), 取不到
        或非股票标的(ETF/指数不在股票 enriched 缓存里)为 null —— 前端据此决定
        是否画昨收线与涨跌幅轴, 没有基准就不画。

        本地分区缺失该标的时**回源**取当日(覆盖 ETF/指数, 也补股票漏扫)。
        """
        df = self._read_partition(trade_date)
        rows = (
            df.filter(pl.col("symbol") == symbol).sort("datetime").to_dicts()
            if not df.is_empty() else []
        )
        source = "parquet"
        if not rows:
            fetched, source = self._fetch_one(symbol, trade_date)
            rows = fetched
        return {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "prev_close": self._prev_close_for(symbol, trade_date),
            "points": rows,
            "source": source,
            "msg": "" if rows else f"{trade_date} {symbol} 无竞价记录(休市/该标的无竞价/超出回溯窗口)",
        }

    def _prev_close_for(self, symbol: str, d: date) -> float | None:
        prev = self._prev_close_frame(d)
        if prev.is_empty():
            return None
        hit = prev.filter(pl.col("symbol") == symbol)
        if hit.is_empty():
            return None
        value = hit["prev_close"][0]
        return float(value) if value is not None else None

    def _fetch_one(self, symbol: str, trade_date: date) -> tuple[list[dict], str]:
        """单标的回源(provider 直取), 不落盘 —— 落盘只由扫描/定时任务负责。"""
        if not self.available():
            return [], "unavailable"
        provider, route = self._resolve_provider()
        if provider is None:
            logger.warning("auction 回源失败: %s", route)
            return [], "unavailable"
        try:
            df = provider.get_auction_batch([symbol], trade_date)
        except Exception as e:  # noqa: BLE001
            logger.warning("auction 回源 %s 失败: %s", symbol, e)
            return [], "fetch-error"
        if df is None or df.is_empty():
            return [], "empty"
        rows = df.select(AUCTION_COLUMNS).sort("datetime").to_dicts()
        # 回源拿到的点不写分区: 单点写入会让"当轮覆盖"语义变得不可追溯
        return rows, f"provider:{route}"

    def get_board(self, trade_date: date, *, segment: str = "open") -> pl.DataFrame:
        """某日竞价末点榜(每标的一行): 虚拟价/匹配量/未匹配量与方向/匹配额。

        只含股票标的池(见 `_universe` 的口径说明); 未匹配额与匹配额的单位换算
        在这里显式完成: 量(手) × 100 → 股, 再 × 价(元) = 元。
        """
        df = self._board_df(trade_date)
        if df.is_empty():
            return df
        return df.filter(pl.col("segment") == segment)

    def _board_df(self, d: date) -> pl.DataFrame:
        """聚合 + 名称/昨收 JOIN, 按日缓存聚合结果(昨收按日现取, 见 `_prev_close_frame`)。"""
        with self._lock:
            cached = self._board_cache.get(d)
        if cached is not None:
            base = cached
        else:
            pts = self._read_partition(d)
            if pts.is_empty():
                return pts
            base = (
                pts.sort(["symbol", "segment", "datetime"])
                .unique(subset=["symbol", "segment"], keep="last")
                .with_columns(
                    (pl.col("price") * pl.col("matched_volume") * 100).alias("matched_amount"),
                    (pl.col("price") * pl.col("unmatched_volume") * 100).alias("unmatched_amount"),
                )
                .select([*AUCTION_COLUMNS, "matched_amount", "unmatched_amount"])
            )
            with self._lock:
                if len(self._board_cache) >= _BOARD_CACHE_MAX:
                    self._board_cache.pop(next(iter(self._board_cache)))
                self._board_cache[d] = base
        return self._with_context(base, d)

    def _with_context(self, base: pl.DataFrame, d: date) -> pl.DataFrame:
        """JOIN 名称与昨收, 算竞价涨幅(**小数制**)。

        列名刻意用 `auction_change_ratio` 而不是 `auction_pct`: 同页已有的扶摇
        竞价异动卡 `auction_pct` 是**百分数制**, 同名不同单位正是 CONTRIBUTING §3.1
        禁止的那类坑。昨收取不到时输出 null, 不伪造。
        """
        out = base
        names = self._name_map()
        if not names.is_empty():
            out = out.join(names, on="symbol", how="left")
        prev = self._prev_close_frame(d)
        if not prev.is_empty():
            out = out.join(prev, on="symbol", how="left")
        elif "prev_close" not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("prev_close"))
        return out.with_columns(
            pl.when(
                pl.col("prev_close").is_not_null()
                & (pl.col("prev_close") > 0)
                & pl.col("price").is_not_null()
            )
            .then(pl.col("price") / pl.col("prev_close") - 1)
            .cast(pl.Float64)
            .alias("auction_change_ratio")
        )

    def _name_map(self) -> pl.DataFrame:
        if not self._repo:
            return pl.DataFrame()
        inst = self._repo.get_instruments()
        if inst.is_empty() or "name" not in inst.columns:
            return pl.DataFrame()
        return inst.select(["symbol", "name"]).unique(subset=["symbol"], keep="last")

    def _prev_close_frame(self, d: date) -> pl.DataFrame:
        """该竞价日的「上一交易日收盘」表 (symbol, prev_close); 取不到返回空表。

        竞价涨幅只有在昨收属于**该竞价日**时才成立, 所以按日期分两种来源:
          - d == 行情缓存会话日 → 直接用 enriched 快照的 prev_close(盘中新鲜,
            与自选/异动页同源);
          - 其他 d(翻看历史竞价日) → 从日级 enriched 缓存按日取。只在缓存**确实覆盖**
            该日时读, 否则宁可返回空表 —— `get_enriched_range` 冷缓存时会同步
            全量重算(50s+), 不能把它放到请求路径上。

        拿今天的昨收冒充历史竞价日会算出一个看起来合理的错误涨跌幅, 所以取不到
        就输出 null(见 `_with_context`)。
        """
        qs = getattr(self._app_state, "quote_service", None) if self._app_state else None
        if qs is None:
            return pl.DataFrame()
        try:
            latest, session_date = qs.get_enriched_today()
        except Exception as e:  # noqa: BLE001
            logger.warning("auction 取昨收失败(榜不显示竞价涨幅): %s", e)
            return pl.DataFrame()
        if latest.is_empty() or "prev_close" not in latest.columns:
            return pl.DataFrame()
        if session_date == d:
            return latest.select(["symbol", "prev_close"]).unique(subset=["symbol"], keep="last")
        span = self._repo.get_enriched_history_span() if self._repo else None
        if span is None or not (span[0] <= d <= span[1]):
            return pl.DataFrame()
        hist = self._repo.get_enriched_range(d, d, columns=["symbol", "prev_close"])
        if hist is None or hist.is_empty() or "prev_close" not in hist.columns:
            return pl.DataFrame()
        return hist.select(["symbol", "prev_close"]).unique(subset=["symbol"], keep="last")

    def board(
        self,
        trade_date: date,
        *,
        segment: str = "open",
        sort_by: str = "matched_amount",
        limit: int = 100,
        symbols: list[str] | None = None,
    ) -> dict:
        """竞价榜(带排序/截断), 供 API 层直接返回。"""
        key = _BOARD_SORTS.get(sort_by, "matched_amount")
        df = self.get_board(trade_date, segment=segment)
        if symbols:
            df = df.filter(pl.col("symbol").is_in(symbols))
        total = df.height
        rows = (
            df.sort(key, descending=True, nulls_last=True).head(max(1, int(limit))).to_dicts()
            if not df.is_empty() else []
        )
        return {
            "trade_date": trade_date.isoformat(),
            "segment": segment,
            "sort_by": sort_by if sort_by in _BOARD_SORTS else "matched_amount",
            "total": total,
            "items": rows,
            "ready": total > 0,
        }

    def available_dates(self, last_n: int = 15) -> list[str]:
        """已落盘的交易日(倒序), 供前端日期选择与覆盖率展示。"""
        if not self._repo:
            return []
        root: Path = self._repo.store.data_dir / "auction"
        if not root.exists():
            return []
        days = []
        for p in root.iterdir():
            if p.is_dir() and p.name.startswith("date="):
                days.append(p.name.removeprefix("date="))
        return sorted(days, reverse=True)[:last_n]

    def status(self) -> dict:
        """服务状态(能力/路由/最近扫描/已落盘日期), 供状态端点与前端降级判定。"""
        capset = self._get_capset()
        provider, route = (None, "")
        if capset.has(Cap.AUCTION_BATCH):
            provider, route = self._resolve_provider()
        return {
            "usable": provider is not None,
            "provider": route,
            "capability": "auction.batch" if capset.has(Cap.AUCTION_BATCH) else None,
            "sweeping": self._sweeping,
            "last_sweep": self._last_sweep,
            "dates": self.available_dates(),
            "segments": list(_SEGMENTS),
        }
