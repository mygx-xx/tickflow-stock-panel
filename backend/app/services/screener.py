"""Screener 服务(§6.3)。

性能优化:
  - enriched parquet 仅存 14 列基础数据, 指标和信号即时计算
  - preset 策略: 从内存缓存或即时计算获取完整指标, ~10-50ms
  - custom SQL: DuckDB (用户传 SQL WHERE 字符串), ~10-50ms
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from app.parquet import scan_enriched_parquet
from app.tickflow.repository import KlineRepository

logger = logging.getLogger(__name__)

# 常用指标 (MA5/10/20、BOLL(20)、量比等) 的近似最小暖机窗口。
# 用于没有精确回看需求的场景 (自定义 SQL 选股、盘后管道) 的数据充足性提示 (#303);
# 策略运行用 engine.required_history_bars 的精确值。
MIN_INDICATOR_WARMUP_DAYS = 30


def enriched_history_days(data_dir, asset_type: str = "stock", as_of: date | None = None) -> int:
    """本地 enriched 在 as_of 及之前覆盖的交易日数 (#303)。

    按 date=* 分区目录名计数 (目录列举 O(天数)), 不读 parquet 内容 —
    只做数据充足性提示, 不进入指标计算路径。
    """
    from app.tickflow.repository import enriched_dirname

    root = Path(data_dir) / enriched_dirname(asset_type)
    if not root.exists():
        return 0
    days = [d.name[5:] for d in root.glob("date=*") if d.is_dir() and len(d.name) > 5]
    if as_of is not None:
        as_of_s = as_of.isoformat()
        days = [d for d in days if d <= as_of_s]
    return len(days)


# ── 进程级历史数据缓存 (避免 run_all 每次重新扫描 parquet + 计算指标) ──
#
# key 只含 (asset_type, target_date), **不含 lookback_days**。
# 原因: 慢路径算出的宽帧对所有窗口都是同一份, 只是裁剪长度不同。
# 把 lookback 放进 key 会导致每个窗口各占一个槽位、各算一次 (~3.5s/次):
# 策略池里有 13 个不同 LOOKBACK_DAYS (120/130/140/200/260...), 首次加载要算 13 次
# ≈45s, 且槽位上限 10 会让它们互相逐出 -> 每次切页都重新计算。
# 改为按日期缓存"最大窗口的完整帧", 命中后按请求窗口裁剪, 首次只需算 1 次。
_history_cache: dict[tuple[str, date], tuple[float, pl.DataFrame]] = {}
_HISTORY_CACHE_TTL = 120.0  # 秒
# 单个缓存帧保留的最大交易日数: 取策略池里最大的 LOOKBACK_DAYS 再留余量。
# 命中后统一从这份帧上裁剪, 所以它必须 >= 任何请求窗口, 否则会少给数据。
_HISTORY_CACHE_MAX_BARS = 400

# load_prior_consecutive 最多回看多少个已存在的日分区 (缺列时继续往前找的上限)
_PRIOR_PARTITION_SCAN = 10


@dataclass
class ScreenerResult:
    as_of: date
    strategy: str | None
    rows: list[dict] = field(default_factory=list)
    total: int = 0
    elapsed_ms: float = 0.0


def _trim_history(df: pl.DataFrame, target_date: date, lookback_days: int) -> pl.DataFrame | None:
    """把完整帧裁成"目标日及之前最近 lookback_days 个交易日"。

    与慢路径的裁剪口径**必须完全一致**(按交易日计数, 不用自然日),
    否则缓存命中与未命中会给出不同窗口 —— test_enriched_history_past_target.py
    锁定的正是这个不变量。

    返回 None 表示这份帧不够长(缓存是在更小窗口下算的), 调用方应丢弃重算:
    静默返回短窗口会让策略拿到不足的历史, 算出错误信号。
    """
    if "date" not in df.columns:
        return df
    upto = df.filter(pl.col("date") <= target_date)
    if upto.is_empty():
        return None
    trading_dates = upto["date"].unique().sort()
    # 需要 lookback_days + 1 个交易日(起点+窗口), 不足则这份帧不可用
    if len(trading_dates) < lookback_days + 1:
        return None
    if len(trading_dates) > lookback_days:
        lookback_start = trading_dates[-(lookback_days + 1)]
        upto = upto.filter(pl.col("date") >= lookback_start)
    return upto.sort(["symbol", "date"])


class ScreenerService:
    def __init__(self, repo: KlineRepository, asset_type: str = "stock") -> None:
        self.repo = repo
        self.asset_type = asset_type
        from app.tickflow.repository import enriched_dirname
        self._enriched_dirname = enriched_dirname(asset_type)

    @staticmethod
    def clear_history_cache() -> None:
        """清空进程级 _history_cache (TTL 缓存)。

        清除数据后调用, 避免内存里的旧历史窗口残留导致策略/看板仍命中旧数据。
        """
        _history_cache.clear()

    def _load_enriched_for_date(self, target_date: date) -> pl.DataFrame:
        """从 enriched parquet 读取指定日期的基础数据并即时计算完整指标+信号。

        enriched parquet 仅存 14 列。读取后需要即时计算 ma/ema/macd/kdj/rsi/boll/momentum/signal 等列。
        对于最新日, 优先使用内存缓存 (已包含完整指标)。
        """
        # 优先使用 repo 最新日缓存
        cache, cache_date = self.repo.get_enriched_latest_asset(self.asset_type)
        if cache is not None and not cache.is_empty() and cache_date == target_date:
            df = cache
            # JOIN instruments
            df_i = self.repo.get_instruments_asset(self.asset_type)
            if not df_i.is_empty():
                inst_cols = [c for c in ["symbol", "name", "total_shares", "float_shares"] if c in df_i.columns]
                if "name" not in df.columns:
                    df = df.join(df_i.select(inst_cols), on="symbol", how="left")
            return df

        # 尝试从 repo 级预计算历史缓存中提取目标日期 (仅 stock: 该缓存为股票专用)
        if self.asset_type == "stock":
            cached_hist = self.repo.get_enriched_history(target_date, 1)
            if cached_hist is not None and not cached_hist.is_empty() and "date" in cached_hist.columns:
                df = cached_hist.filter(pl.col("date") == target_date)
                if not df.is_empty():
                    logger.debug("_load_enriched_for_date: repo history cache for %s", target_date)
                    # JOIN instruments
                    df_i = self.repo.get_instruments_asset(self.asset_type)
                    if not df_i.is_empty():
                        inst_cols = [c for c in ["symbol", "name", "total_shares", "float_shares"] if c in df_i.columns]
                        if "name" not in df.columns:
                            df = df.join(df_i.select(inst_cols), on="symbol", how="left")
                    return df

        # 历史日期: 从 parquet 读取 14 列, 即时计算指标 (慢路径)
        enriched_dir = self.repo.store.data_dir / self._enriched_dirname
        ds = target_date.isoformat()
        target_parquet = enriched_dir / f"date={ds}" / "part.parquet"

        if not target_parquet.exists():
            return pl.DataFrame()

        try:
            df = pl.read_parquet(target_parquet)
        except Exception as e:  # noqa: BLE001
            logger.warning("load_enriched_for_date failed: %s", e)
            return pl.DataFrame()

        if df.is_empty():
            return df

        # 即时计算指标: 需要加载历史窗口作 warmup
        df_full = self._compute_enriched_full(df, target_date)
        return df_full

    def load_prior_consecutive(self, as_of: date, consec_col: str) -> pl.DataFrame:
        """窄读: 仅取前一交易日的 [symbol, consec_col] 两列 (谓词下推到单日 parquet)。

        consecutive_limit_ups / consecutive_limit_downs 是 enriched 的存储列,
        可直接从 parquet 读取, 无需 _load_enriched_for_date 的全量指标重算
        (历史日期该慢路径最坏会触发 9 次全市场 compute_enriched_full)。

        由近到远取 as_of 之前已存在的日分区 (即前一交易日), 读取其
        symbol + consec_col。存储列的值与重算值逐位一致 (连板计数为 run-length,
        150 天 warmup 完全覆盖 A 股最长连板, 二者相等)。

        返回列: symbol, prev_consec。找不到前一交易日时返回空 DataFrame。
        """
        enriched_dir = self.repo.store.data_dir / self._enriched_dirname
        for candidate in self._prior_partition_dates(as_of, _PRIOR_PARTITION_SCAN):
            target_parquet = enriched_dir / f"date={candidate.isoformat()}" / "part.parquet"
            if not target_parquet.exists():
                continue
            try:
                lf = pl.scan_parquet(target_parquet)
                cols = lf.collect_schema().names()
            except Exception as e:  # noqa: BLE001
                logger.warning("load_prior_consecutive scan failed for %s: %s", candidate, e)
                return pl.DataFrame()
            # 存储列理论上必含 consec_col; 若该分区缺列则继续向前找 (与旧循环一致)
            if "symbol" not in cols or consec_col not in cols:
                continue
            try:
                return lf.select(
                    "symbol",
                    pl.col(consec_col).alias("prev_consec"),
                ).collect()
            except Exception as e:  # noqa: BLE001
                logger.warning("load_prior_consecutive read failed for %s: %s", candidate, e)
                return pl.DataFrame()
        return pl.DataFrame()

    def _prior_partition_dates(self, as_of: date, limit: int) -> list[date]:
        """enriched 目录里早于 as_of 的分区日期, 由近到远最多 limit 个。

        枚举分区目录而不是按自然日回看固定天数: 春节长假连着调休周末,
        相邻两个交易日能隔 10~11 个自然日 (如 2024-02-08 → 2024-02-19),
        固定窗口会整段落空。与 auction_benchmark._prev_trading_day
        「本地日K分区日期 = 已知交易日集合」同口径。
        """
        enriched_dir = self.repo.store.data_dir / self._enriched_dirname
        days: list[date] = []
        try:
            entries = list(enriched_dir.iterdir())
        except OSError:
            return []
        for part in entries:
            if not part.name.startswith("date="):
                continue
            try:
                day = date.fromisoformat(part.name[5:])
            except ValueError:
                continue
            if day < as_of:
                days.append(day)
        days.sort(reverse=True)
        return days[:limit]

    def _compute_enriched_full(self, df_target: pl.DataFrame, target_date: date) -> pl.DataFrame:
        """从 14 列基础数据即时计算完整 enriched (含全部指标和信号)。

        读取历史数据作为指标计算的 warmup, 计算完成后只返回目标日期的行。
        """
        from app.indicators.pipeline import (
            compute_indicators,
            compute_limit_signals,
            compute_signals,
        )

        # 加载 warmup 历史 (目标日期前 ~120 天)
        enriched_dir = self.repo.store.data_dir / self._enriched_dirname
        start = target_date - timedelta(days=150)
        # turnover_rate 是 enriched 存储列, 必须随行透传: 否则即时计算后该列
        # 丢失, 自定义 SQL 用它做条件会 Binder Error 被吞成空结果 (#187)
        read_cols = ["symbol", "date", "open", "high", "low", "close", "volume",
                     "amount", "raw_close", "raw_high", "raw_low", "turnover_rate"]

        try:
            lf = (
                scan_enriched_parquet(str(enriched_dir / "**" / "*.parquet"))
                .filter(
                    (pl.col("date") >= start)
                    & (pl.col("date") <= target_date)
                )
                .sort(["symbol", "date"])
            )
            available = [c for c in read_cols if c in lf.schema]
            df_hist = lf.select(available).collect()
        except Exception as e:  # noqa: BLE001
            logger.warning("warmup history load failed: %s", e)
            df_hist = df_target

        if df_hist.is_empty():
            df_hist = df_target

        # 计算指标
        df_full = compute_indicators(df_hist)
        df_full = compute_signals(df_full)

        # 计算涨跌停信号 (需要 instruments; 涨停为股票专有, ETF 跳过)
        instruments = self.repo.get_instruments_asset(self.asset_type)
        if self.asset_type == "stock" and instruments is not None and not instruments.is_empty():
            df_full = compute_limit_signals(
                df_full,
                instruments,
                historical_shares=self.repo.get_historical_shares(),
            )

        # 只保留目标日期
        df_result = df_full.filter(pl.col("date") == target_date)

        # JOIN instruments (name, total_shares, float_shares)
        if not instruments.is_empty():
            inst_cols = [c for c in ["symbol", "name", "total_shares", "float_shares"] if c in instruments.columns]
            if "name" not in df_result.columns:
                df_result = df_result.join(instruments.select(inst_cols), on="symbol", how="left")

        return df_result

    def _load_enriched_history(self, target_date: date, lookback_days: int) -> pl.DataFrame:
        """读取目标日期之前的基础行情数据, 供历史窗口策略使用。

        优先从 repo 内存缓存获取 (启动时已预计算), 命中时 0ms。
        缓存 miss 时走 scan_parquet + compute_indicators 慢路径。

        进程级缓存按 (asset_type, target_date) 存**最大窗口的完整帧**,
        命中后按 lookback_days 裁剪下发 —— 见模块顶部 _history_cache 的说明。
        """
        # 优先级 1: repo 级预计算缓存 (启动时 _refresh_enriched 已计算完整历史; 仅 stock)
        t0 = time.perf_counter()
        if self.asset_type == "stock":
            cached = self.repo.get_enriched_history(target_date, lookback_days)
            if cached is not None and not cached.is_empty():
                # JOIN instruments (repo 缓存不含 name 等列)
                instruments = self.repo.get_instruments_asset(self.asset_type)
                if instruments is not None and not instruments.is_empty() and "name" not in cached.columns:
                    inst_cols = [c for c in ["symbol", "name", "total_shares", "float_shares"]
                                 if c in instruments.columns]
                    cached = cached.join(instruments.select(inst_cols), on="symbol", how="left")
                elapsed = (time.perf_counter() - t0) * 1000
                logger.info("_load_enriched_history(%s, %d): repo cache hit, %.1fms, %d rows",
                            target_date, lookback_days, elapsed, len(cached))
                return cached

        # 优先级 2: 进程级 history_cache —— 按日期存完整帧, 按窗口裁剪
        cache_key = (self.asset_type, target_date)
        now = time.monotonic()
        ttl_cached = _history_cache.get(cache_key)
        if ttl_cached is not None:
            ts, cached_df = ttl_cached
            if now - ts < _HISTORY_CACHE_TTL:
                trimmed = _trim_history(cached_df, target_date, lookback_days)
                if trimmed is not None:
                    logger.debug("history TTL cache hit: %s lookback=%d -> %d rows",
                                 target_date, lookback_days, len(trimmed))
                    return trimmed
                # 缓存帧比请求窗口短(说明缓存是在更小的窗口下算的), 丢弃重算,
                # 否则会静默少给数据 —— 策略拿到的历史不足会算出错误的信号。
                del _history_cache[cache_key]
            else:
                del _history_cache[cache_key]

        # 优先级 3: scan_parquet + compute_indicators (慢路径, ~5s)
        # 一次算到最大窗口, 让同一日期的所有策略共用这一帧
        compute_bars = max(lookback_days, _HISTORY_CACHE_MAX_BARS)
        logger.warning("_load_enriched_history cache miss, computing indicators (%s, %d->%d)...",
                       target_date, lookback_days, compute_bars)
        from app.indicators.pipeline import (
            compute_indicators,
            compute_limit_signals,
            compute_signals,
        )

        warmup = 60
        # 按**交易日**需求反推自然日跨度: 原实现硬编码上限 180 自然日(≈120 交易日),
        # 导致任何 >120 的窗口(如 200/260)都取不到足够历史 —— 帧被裁到 120 后,
        # 缓存命中判定为"帧不足"而反复重算(实测大窗口每次都花 2.8~3.3s)。
        # 用 1.5 倍自然日/交易日比(含周末, 不含长假) + 30 天余量, 并保留原上限
        # 以兼容历史行为; 该跨度只影响 scan 范围, 返回前仍按交易日精确裁剪。
        calendar_days = int((compute_bars + warmup) * 1.5) + 30
        start = target_date - timedelta(days=max(calendar_days, 180))

        enriched_dir = self.repo.store.data_dir / self._enriched_dirname
        # 同 _compute_enriched_full: turnover_rate 存储列随行透传 (#187)
        read_cols = ["symbol", "date", "open", "high", "low", "close", "volume",
                     "amount", "raw_close", "raw_high", "raw_low", "turnover_rate"]

        try:
            lf = (
                scan_enriched_parquet(str(enriched_dir / "**" / "*.parquet"))
                .filter((pl.col("date") >= start) & (pl.col("date") <= target_date))
                .sort(["symbol", "date"])
            )
            available = [c for c in read_cols if c in lf.collect_schema().names()]
            df_hist = lf.select(available).collect()
        except Exception as e:  # noqa: BLE001
            logger.warning("load_enriched_history failed: %s", e)
            return pl.DataFrame()

        if df_hist.is_empty():
            return pl.DataFrame()

        df_full = compute_indicators(df_hist)
        df_full = compute_signals(df_full)

        instruments = self.repo.get_instruments_asset(self.asset_type)
        if self.asset_type == "stock" and instruments is not None and not instruments.is_empty():
            df_full = compute_limit_signals(
                df_full,
                instruments,
                historical_shares=self.repo.get_historical_shares(),
            )

        if instruments is not None and not instruments.is_empty():
            inst_cols = [c for c in ["symbol", "name", "total_shares", "float_shares"] if c in instruments.columns]
            if "name" not in df_full.columns:
                df_full = df_full.join(instruments.select(inst_cols), on="symbol", how="left")

        # 裁剪掉 warmup 部分, 只保留 compute_bars 范围 (减少 group_by 开销)。
        # 按交易日计数: 从数据里实际存在的交易日序列取最后 N 个交易日,
        # 不能用 timedelta(days=N) (自然日), 否则周末/节假日会让窗口偏少, 与回测不一致。
        #
        # 注意这里裁到 compute_bars(=max(请求窗口, _HISTORY_CACHE_MAX_BARS)),
        # 存进缓存的是这一份完整帧; 命中其它窗口时由 _trim_history 再裁。
        if "date" in df_full.columns:
            trading_dates = df_full["date"].unique().sort()
            if len(trading_dates) > compute_bars:
                lookback_start = trading_dates[-(compute_bars + 1)]
            else:
                lookback_start = trading_dates[0]
            df_full = df_full.filter(pl.col("date") >= lookback_start)

        df_full = df_full.sort(["symbol", "date"])

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info("_load_enriched_history(%s, %d): computed in %.1fms, %d rows (frame bars=%d)",
                    target_date, lookback_days, elapsed, len(df_full), compute_bars)

        _history_cache[cache_key] = (now, df_full)
        # TTL 先清一轮; 仍超上限时按最旧无条件淘汰。
        # key 现在只含 (asset_type, date), 槽位需求从"窗口数"降到"日期数",
        # 上限 10 足够容纳 10 个不同查询日; 逐出策略仍保留以防历史日期被批量扫描。
        if len(_history_cache) > 10:
            expired = [k for k, (ts, _) in _history_cache.items() if now - ts > _HISTORY_CACHE_TTL]
            for k in expired:
                del _history_cache[k]
            while len(_history_cache) > 10:
                oldest = min(_history_cache, key=lambda k: _history_cache[k][0])
                del _history_cache[oldest]

        # 返回值必须与"未命中缓存"时的窗口口径一致; _trim_history 返回 None 只会在
        # 帧不足时发生(这里刚按 compute_bars 裁过, 正常不会), 兜底返回完整帧。
        # 注意不能用 `a or b`: Polars DataFrame 的真值判断会抛 TypeError。
        trimmed = _trim_history(df_full, target_date, lookback_days)
        return df_full if trimmed is None else trimmed

    def run(
        self,
        as_of: date,
        conditions: list[str],
        order_by: str | None = None,
        limit: int = 30,
        pool: list[str] | None = None,
    ) -> ScreenerResult:
        """自定义 SQL 条件选股。

        先通过 Polars 即时计算完整指标, 再用 DuckDB 做 SQL WHERE 过滤。
        kline_enriched DuckDB 视图只有 14 列, 不能直接用于指标过滤。
        """
        t0 = time.perf_counter()

        if not conditions:
            return ScreenerResult(as_of=as_of, strategy=None)

        # 从即时计算获取完整 enriched 数据
        df = self._load_enriched_for_date(as_of)
        if df.is_empty():
            return ScreenerResult(as_of=as_of, strategy=None)

        # Pool 过滤
        if pool:
            df = df.filter(pl.col("symbol").is_in(pool))

        # 用 DuckDB 做 SQL 过滤 (注册临时视图)
        # 用独立的 :memory: 连接 (而非复用 repo 共享连接的 cursor): conditions 是用户
        # 传入的 SQL 片段, 隔离连接下注入至多能碰 read_csv/read_parquet 文件; 若复用共享
        # 连接则会把 app 已注册的真实业务表也暴露给注入, 扩大攻击面。隔离连接创建开销极低。
        # 再关闭 external_access, 让注入的文件读写函数 (read_parquet/COPY 等) 直接报错,
        # 视图数据仍通过 con.register 注入, 不受该开关影响 (#224)。
        con = None
        try:
            import duckdb
            con = duckdb.connect(
                database=":memory:", config={"enable_external_access": False}
            )
            con.register("enriched", df.to_arrow())
            where = " AND ".join(f"({c})" for c in conditions)
            sql = f"SELECT * FROM enriched WHERE {where}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            if limit:
                sql += f" LIMIT {limit}"
            df_result = con.execute(sql).pl()
        except Exception as e:  # noqa: BLE001
            logger.warning("screener SQL query failed: %s", e)
            df_result = pl.DataFrame()
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:  # noqa: BLE001
                    pass

        rows = df_result.to_dicts() if not df_result.is_empty() else []
        elapsed = (time.perf_counter() - t0) * 1000

        return ScreenerResult(
            as_of=as_of,
            strategy=None,
            rows=rows,
            total=len(rows),
            elapsed_ms=elapsed,
        )

    def build_strategy_context(
        self,
        engine,
        as_of: date,
        strategy_ids: list[str],
        *,
        timeframe: str = "1d",
        params_map: dict[str, dict] | None = None,
        overrides_map: dict[str, dict] | None = None,
        current: pl.DataFrame | None = None,
        market=None,
        cache_key: str | None = None,
    ):
        """按调用方要求装配标准策略数据上下文，不解释策略公式。"""
        from app.strategy.engine import StrategyDataContext

        if current is None:
            current = self._load_enriched_for_date(as_of)
        if timeframe == "1m":
            # 分钟策略数据源是本地当日分钟K分区 (单分区文件直读), 与日线
            # enriched 历史窗口无关, 不走 required_history_bars 日线路径。
            history = self._load_minute_history(as_of, current)
            # 策略声明 META["daily_history_bars"] 时额外装配日线 enriched 窗口,
            # 供分钟策略叠加日线维度条件 (如 N 日内涨停过)。
            daily_history = None
            if engine is not None:
                daily_bars = engine.minute_daily_history_bars(strategy_ids)
                if daily_bars > 0:
                    daily_history = self._load_enriched_history(as_of, daily_bars)
            return StrategyDataContext(
                asset_type=self.asset_type,
                timeframe=timeframe,
                as_of=as_of,
                current=current,
                history=history,
                daily_history=daily_history,
                market=None,
                cache_key=cache_key,
            )
        history_bars = engine.required_history_bars(
            strategy_ids,
            params_map=params_map,
            overrides_map=overrides_map,
        )
        history = None
        if history_bars > 1:
            history = self._load_enriched_history(as_of, history_bars)
        return StrategyDataContext(
            asset_type=self.asset_type,
            timeframe=timeframe,
            as_of=as_of,
            current=current,
            history=history,
            market=market,
            cache_key=cache_key,
        )

    def _load_minute_history(self, as_of: date, current: pl.DataFrame | None) -> pl.DataFrame:
        """分钟策略数据源: 优先 as_of 当日分钟分区, 缺失时回退全市场最近分区。

        只按日期直读单个分区文件 (get_minute_by_dates), 与全量 glob 扫描解耦,
        内存只随当日分区大小 (~67万行) 走。标的池限定为 enriched 快照 universe;
        分区与快照的日期差是允许的 (分钟分区可能比 enriched 更新, 行自带时间戳)。
        """
        if self.asset_type != "stock":
            raise ValueError("分钟策略当前仅支持 A 股")
        symbols: list[str] = []
        if current is not None and not current.is_empty():
            symbols = current["symbol"].cast(pl.Utf8).unique().to_list()
        if not symbols:
            return pl.DataFrame()
        df = self.repo.get_minute_by_dates(symbols, [as_of])
        if df.is_empty():
            fallback = self.repo.latest_minute_date_global()
            if fallback is None:
                raise ValueError(
                    "无分钟K数据 — 请先在 数据→分钟K 完成同步, 或开启盘中增量刷新"
                )
            if fallback != as_of:
                df = self.repo.get_minute_by_dates(symbols, [fallback])
        return df

    def latest_date(self) -> date | None:
        if self.asset_type != "stock":
            _, d = self.repo.get_enriched_latest_asset(self.asset_type)
            return d
        d = self.repo.enriched_latest_date()
        if d:
            return d
        # 回退 DuckDB
        try:
            res = self.repo.execute_one(
                "SELECT max(date) FROM kline_enriched",
            )
            if res and res[0]:
                d = res[0]
                return d if isinstance(d, date) else date.fromisoformat(str(d))
        except Exception:  # noqa: BLE001
            return None
        return None

    def latest_trading_date(self) -> date | None:
        """latest_date 的交易口径版本: 休市日打戳产生的分区视为无效, 回溯到最近交易日。

        实时落盘用 cn_today() 给每行打戳 (quote_service._build_daily), 一旦在休市日
        跑成就会留下 date=<休市日> 的分区, 内容与上一交易日逐行相同 (2026-10-01 国庆
        实测: 5561 只 OHLC 全等, 全市场 change_pct 归零)。读取侧一律取 max(date),
        于是该假分区成为全应用的 as_of —— 看板把它当交易日渲染, 选股/策略/监控/异动
        全部继承错误日期。

        判据: 候选 == 今天 且 探针确认今天休市 → 回溯到不晚于今天的最近分区。
        探针未知 (None) 时不拦截 —— 探针不可用时"今天"可能就是真实交易日, 回溯会丢
        当日数据; 候选早于今天时也不拦截 (节后首次运行的正常状态)。
        """
        d = self.latest_date()
        if d is None:
            return None
        from app.market_time import cn_today
        from app.services import trading_day

        today = cn_today()
        if d == today and trading_day.is_trading_day() is False:
            return self._latest_date_on_or_before(today) or d
        return d

    def _latest_date_on_or_before(self, cutoff: date) -> date | None:
        """DuckDB 回溯: 不晚于 cutoff 的最大 enriched 日期。"""
        try:
            res = self.repo.execute_one(
                "SELECT max(date) FROM kline_enriched WHERE date <= ?",
                [cutoff],
            )
            if res and res[0]:
                d = res[0]
                return d if isinstance(d, date) else date.fromisoformat(str(d))
        except Exception:  # noqa: BLE001
            return None
        return None

    def coverage_warnings(self, as_of: date, *, required_bars: int | None = None) -> list[str]:
        """数据充足性提示 (#303): enriched 覆盖不足时返回用户可读警告, 充足返回 []。

        空库首跑只拉到 1 个交易日时, 均线/动量/量比等指标暖机不足, 选股会静默
        全 0 — 这里把"数据不够"显式说出来。required_bars 缺省用通用暖机窗口
        (自定义 SQL 选股); 策略运行传 engine.required_history_bars 的精确值。
        available 为 0 时 enriched 为空, 上层 latest_date 已 400, 不重复提示。
        """
        available = enriched_history_days(self.repo.store.data_dir, self.asset_type, as_of)
        if available == 0:
            return []
        need = required_bars if required_bars and required_bars > 0 else MIN_INDICATOR_WARMUP_DAYS
        if available >= need:
            return []
        return [
            f"本地数据仅覆盖 {available} 个交易日, 低于本次计算所需约 {need} 天暖机窗口 — "
            "指标可能失真或全部落空 (选股 0 命中)。建议先全量回填日K并重算指标 "
            "(数据页「日K批量同步」, 符号需带交易所后缀)"
        ]
