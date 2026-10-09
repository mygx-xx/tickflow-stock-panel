#!/usr/bin/env python3
"""集合竞价历史回填 —— 多线程 + eltdx 原生多IP(通达信多行情主站)。

为什么需要它
------------
上游 7709 的集合竞价是**逐标的**接口(无批量端点), 面板平时只在 09:26 / 15:01 各扫一轮,
所以本地 `data/auction/date=*/part.parquet` 只有真正扫过的交易日。本脚本把某个日期区间内
**每个交易日**的全市场竞价逐点序列一次性补齐, 分区格式与 `AuctionService._persist` 完全一致,
面板不需要改任何代码即可读到。

与生产链路的关系(不另起一套口径)
--------------------------------
* 取数与列契约复用 `EltDxProvider.get_auction_batch`(单源: `AUCTION_COLUMNS`/`AUCTION_SCHEMA`,
  price_milli 还原、segment 划分等规则都不在本脚本重写)。
* 落盘复用 `services.fs_utils.atomic_write_parquet`, 与 `AuctionService` 同一原子写语义。
* 传输层复用 `provider._make_transport`: 默认 **HTTP 网关**(`ELTDX_TRANSPORT=http`)——
  与正在运行的面板共用同一个 eltdx-http 进程, 不会另起 7709 运行时把面板连接打死
  (事故记录见 `plugins/eltdx/http_client.py`)。建议给回填单起一个网关
  (`eltdx-http --port 8010 --server-count 8`), 用 `--http-url` 指过去, 避免与面板抢池。

多线程 / 多IP
-------------
* 多线程: `--batch-size` 只一批, 批内由 transport 的线程池并发打点。
* 多IP: eltdx 内置 `DEFAULT_HOSTS`(约 40 个通达信行情主站 IP), `TdxClient` 启动时自动探测
  并选快; `server_count` × `connections_per_server` 决定连接铺到多少个不同主站 IP 上。
  * `--transport http`(默认): 多IP 由网关进程的 TdxClient 提供(网关的 `--server-count`)。
  * `--transport inproc`: 脚本自己起 TdxClient, `--server-count` 直接决定用几个主站 IP。
    注意这会在本进程再起一个 7709 运行时, 面板在跑时优先级低于 http。

窗口边缘的偶发空结果
--------------------
实测上游对最老的 1~5 个交易日会**随机**返回空 points(重试才出), 而空 points 也是"休市/
该标的无竞价"的合法状态, 两者无法区分。因此 `--day-attempts` 会多扫几遍并**按 symbol 合并**
(先到者为准), 达到 `--target-coverage` 就提前停 —— 正常交易日一遍就够(~94%), 只有边缘日
才会多扫。

可续跑
------
默认跳过已存在的 `date=YYYY-MM-DD` 分区, 中断后重跑只补缺失交易日; `--force` 才覆盖重扫。
"""
from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

logger = logging.getLogger("backfill_auction")


def _parse_date(text: str) -> date:
    try:
        return date.fromisoformat(text.strip())
    except ValueError:
        raise argparse.ArgumentTypeError(f"日期格式应为 YYYY-MM-DD, 收到: {text!r}") from None


def _configure_env(args: argparse.Namespace) -> None:
    """构造 provider 前钉住传输层 —— 复用生产同一份 _make_transport 决策。"""
    if args.transport == "inproc":
        os.environ["ELTDX_TRANSPORT"] = "inproc"
        os.environ["ELTDX_SERVER_COUNT"] = str(args.server_count)
        os.environ["ELTDX_CONNECTIONS_PER_SERVER"] = str(args.connections_per_server)
        os.environ["ELTDX_TIMEOUT"] = str(args.timeout)
    else:
        os.environ["ELTDX_TRANSPORT"] = "http"
        os.environ["ELTDX_HTTP_URL"] = args.http_url
        os.environ["ELTDX_HTTP_TIMEOUT"] = str(args.timeout)
        if args.workers > 0:
            # 网关客户端的线程数/连接池 size 都取这个值(默认 8), 是最直接的速度杠杆
            os.environ["ELTDX_HTTP_WORKERS"] = str(args.workers)


def _resolve_data_dir(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    from app.config import settings

    return Path(settings.data_dir)


def _trading_days(client, start: date, end: date, cal_symbol: str) -> list[date]:
    """用一根日K(默认上证指数)当交易日历 —— 与上游同一口径, 不受本地日历缺口影响。"""
    from app.plugins.eltdx.provider import _bar_date

    bars = client.bars(cal_symbol, period="day", count=800)
    days = sorted({d for d in (_bar_date(getattr(b, "time", None)) for b in bars) if d})
    picked = [d for d in days if start <= d <= end]
    if not picked:
        raise SystemExit(
            f"取不到 {start}..{end} 的交易日历(calendar_symbol={cal_symbol} 返回 {len(bars)} 根)"
        )
    return picked


def _universe(client, mode: str, data_dir: Path) -> list[str]:
    """标的池: 默认与 `AuctionService._universe` 同源(instruments 维表); eltdx 为兜底。"""
    if mode == "eltdx":
        syms = sorted(set(client.all_a_shares()))
    else:
        import polars as pl

        from app.tickflow.repository import DataStore, KlineRepository

        repo = KlineRepository(DataStore(data_dir))
        df = repo.get_instruments()
        syms = (
            df["symbol"].cast(pl.Utf8).unique().sort().to_list()
            if not df.is_empty() and "symbol" in df.columns
            else []
        )
    if not syms:
        raise SystemExit(f"标的池为空(universe={mode}); 可改用 --universe eltdx")
    return syms


def _fetch_day(
    provider,
    symbols,
    d: date,
    *,
    batch_size: int,
    attempts: int,
    sleep_s: float,
    target_coverage: float,
):
    """取一个交易日的全市场竞价, 多遍扫描并按 symbol 合并。

    返回 ``(DataFrame | None, 失败批数, 实际尝试次数, coverage)``。
    coverage = 已取到数据的 symbol 数 / 传入 symbol 数; 达到 target_coverage 提前收工。
    正常交易日一遍即达标; 窗口边缘的随机空结果由后续遍次补上。
    """
    import polars as pl

    merged = None
    tried = 0
    failed_total = 0
    for attempt in range(1, attempts + 1):
        tried = attempt
        frames = []
        for i in range(0, len(symbols), batch_size):
            chunk = symbols[i : i + batch_size]
            try:
                df = provider.get_auction_batch(chunk, d)
            except Exception as e:  # noqa: BLE001 — 按批隔离, 单批失败不拖垮整日
                failed_total += 1
                logger.warning(
                    "  %s 第 %d 批失败(%d 只): %s", d, i // batch_size + 1, len(chunk), e
                )
                continue
            if df is not None and not df.is_empty():
                frames.append(df)
        fresh = pl.concat(frames, how="vertical") if frames else None
        if fresh is not None and not fresh.is_empty():
            if merged is None:
                merged = fresh
            else:
                have = merged["symbol"].unique().to_list()
                add = fresh.filter(~pl.col("symbol").is_in(have))
                if not add.is_empty():
                    merged = pl.concat([merged, add], how="vertical")
        coverage = merged["symbol"].n_unique() / len(symbols) if merged is not None and len(symbols) else 0.0
        if coverage >= target_coverage:
            break
        if attempt < attempts:
            logger.info(
                "  %s 覆盖 %.1f%% < 目标 %.1f%%, %.1fs 后补扫(%d/%d)",
                d, coverage * 100, target_coverage * 100, sleep_s, attempt, attempts,
            )
            time.sleep(sleep_s)
    return merged, failed_total, tried, coverage


def _partition_path(data_dir: Path, d: date) -> Path:
    return data_dir / "auction" / f"date={d.isoformat()}" / "part.parquet"


def _coverage_from_disk(path: Path, n_symbols: int) -> float:
    """已落盘分区的 symbol 覆盖率(只投影 symbol 列, 不整表读)。"""
    if n_symbols <= 0 or not path.exists():
        return 0.0
    import polars as pl

    try:
        n = pl.read_parquet(path, columns=["symbol"])["symbol"].n_unique()
    except Exception:  # noqa: BLE001 — 分区损坏按未覆盖处理, 交由重扫覆盖
        return 0.0
    return min(1.0, n / n_symbols)


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="集合竞价全市场历史回填(多线程 + eltdx 多IP)")
    ap.add_argument("--start", type=_parse_date, default=date(2025, 7, 21), help="起始交易日")
    ap.add_argument("--end", type=_parse_date, default=None, help="结束交易日, 默认今天(北京)")
    ap.add_argument("--data-dir", default=None, help="默认 settings.data_dir(开发态=项目根 data/)")
    ap.add_argument("--transport", choices=("http", "inproc"), default="http")
    ap.add_argument(
        "--http-url", default=os.environ.get("ELTDX_HTTP_URL", "http://127.0.0.1:8000")
    )
    ap.add_argument("--server-count", type=int, default=8, help="inproc: 占用多少个通达信主站IP")
    ap.add_argument("--connections-per-server", type=int, default=4, help="inproc: 每主站连接数")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument(
        "--workers", type=int, default=0,
        help="http: 客户端并发线程/连接数(0=沿用 ELTDX_HTTP_WORKERS, 默认 8)",
    )
    ap.add_argument("--batch-size", type=int, default=200, help="每批标的数(批内再并发)")
    ap.add_argument("--universe", choices=("instruments", "eltdx"), default="instruments")
    ap.add_argument("--calendar-symbol", default="000001.SH")
    ap.add_argument("--day-attempts", type=int, default=6, help="每交易日最多扫几遍(按 symbol 合并, 达标提前停)")
    ap.add_argument(
        "--target-coverage", type=float, default=0.92, help="达到该覆盖率提前收工(0~1)"
    )
    ap.add_argument("--retry-sleep", type=float, default=1.5)
    ap.add_argument("--sleep-between-days", type=float, default=0.0)
    ap.add_argument("--symbols-limit", type=int, default=0, help="调试: 只取前 N 只标的")
    ap.add_argument("--max-days", type=int, default=0, help="调试: 只跑前 N 个交易日")
    ap.add_argument("--force", action="store_true", help="重扫已存在分区(与旧行按 symbol 合并, 只增不减)")
    ap.add_argument("--dry-run", action="store_true", help="只取数不落盘")
    return ap


def main(argv: list[str] | None = None) -> int:
    from app.market_time import cn_today

    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    end = args.end or cn_today()
    if args.start > end:
        raise SystemExit(f"--start {args.start} 晚于 --end {end}")

    _configure_env(args)
    from app.plugins.eltdx.provider import EltDxProvider

    data_dir = _resolve_data_dir(args.data_dir)
    provider = EltDxProvider()
    client = provider._client  # noqa: SLF001 — 代码表/交易日历没有公开 provider 方法
    try:
        symbols = _universe(client, args.universe, data_dir)
        if args.symbols_limit > 0:
            symbols = symbols[: args.symbols_limit]
        days = _trading_days(client, args.start, end, args.calendar_symbol)
        if args.max_days > 0:
            days = days[: args.max_days]

        logger.info(
            "回填 %s..%s: %d 个交易日 × %d 只标的 | transport=%s batch=%d attempts=%d target=%.0f%% data_dir=%s",
            days[0], days[-1], len(days), len(symbols), args.transport, args.batch_size,
            max(1, args.day_attempts), args.target_coverage * 100, data_dir,
        )

        import polars as pl

        from app.data_providers.base import AUCTION_COLUMNS

        target = min(1.0, max(0.0, args.target_coverage))
        from app.services.fs_utils import atomic_write_parquet

        records: list[dict] = []
        total_rows = 0
        total_failed_batches = 0
        started = time.perf_counter()
        for idx, d in enumerate(days, 1):
            path = _partition_path(data_dir, d)
            if path.exists() and not args.force:
                have = _coverage_from_disk(path, len(symbols))
                if have >= target:
                    logger.info(
                        "[%d/%d] %s 已存在(覆盖 %.1f%% >= 目标), 跳过",
                        idx, len(days), d, have * 100,
                    )
                    records.append(
                        {"date": d.isoformat(), "skipped": True, "coverage": round(have, 4)}
                    )
                    continue
                logger.info(
                    "[%d/%d] %s 已存在但覆盖 %.1f%% < 目标, 补扫合并",
                    idx, len(days), d, have * 100,
                )

            t0 = time.perf_counter()
            df, failed, tried, coverage = _fetch_day(
                provider,
                symbols,
                d,
                batch_size=args.batch_size,
                attempts=max(1, args.day_attempts),
                sleep_s=args.retry_sleep,
                target_coverage=target,
            )
            rows = df.height if df is not None else 0
            uniq = df["symbol"].n_unique() if rows else 0
            persisted = False
            if rows and not args.dry_run:
                out = df.select(AUCTION_COLUMNS)
                if path.exists():
                    # 与 AuctionService._persist 同语义: 本轮缺失的标的保留旧行,
                    # 反复重扫只会单调补全, 不会因某轮上游抖动而缩短历史覆盖。
                    old = pl.read_parquet(path).select(AUCTION_COLUMNS)
                    kept = old.filter(~pl.col("symbol").is_in(out["symbol"].unique().to_list()))
                    if kept.height:
                        out = pl.concat([out, kept], how="vertical")
                out = out.sort(["symbol", "segment", "datetime"])
                path.parent.mkdir(parents=True, exist_ok=True)
                atomic_write_parquet(out, path)
                persisted = True
            elapsed = time.perf_counter() - t0
            total_rows += rows
            total_failed_batches += failed
            records.append(
                {
                    "date": d.isoformat(),
                    "symbols": uniq,
                    "rows": rows,
                    "coverage": round(coverage, 4),
                    "failed_batches": failed,
                    "attempts": tried,
                    "persisted": persisted,
                    "elapsed_s": round(elapsed, 1),
                }
            )
            logger.info(
                "[%d/%d] %s: %d 只/%d 行(覆盖 %.1f%%), 失败批 %d, 尝试 %d, 落盘=%s, %.1fs",
                idx, len(days), d, uniq, rows, coverage * 100, failed, tried, persisted, elapsed,
            )
            if args.sleep_between_days:
                time.sleep(args.sleep_between_days)

        elapsed = time.perf_counter() - started
        fetched = [r for r in records if not r.get("skipped")]
        empty_days = [r["date"] for r in fetched if not r.get("rows")]
        low_days = [r["date"] for r in fetched if r.get("rows") and r["coverage"] < 0.5]
        report = {
            "start": args.start.isoformat(),
            "end": end.isoformat(),
            "transport": args.transport,
            "universe": args.universe,
            "symbols": len(symbols),
            "trading_days": len(days),
            "rows": total_rows,
            "failed_batches": total_failed_batches,
            "elapsed_s": round(elapsed, 1),
            "empty_days": empty_days,
            "low_coverage_days": low_days,
            "days": records,
        }
        logger.info(
            "完成: %d 个交易日, 新增 %d 行, 用时 %.1fs; 空日 %d 个, 低覆盖日 %d 个",
            len(days), total_rows, elapsed, len(empty_days), len(low_days),
        )
        if empty_days:
            logger.warning("空日(需重跑 --force): %s", ", ".join(empty_days))
        if low_days:
            logger.warning("低覆盖日(建议重跑 --force): %s", ", ".join(low_days))
        if not args.dry_run:
            (data_dir / "auction" / "_backfill_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        return 0
    finally:
        with contextlib.suppress(Exception):
            provider.close()


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    raise SystemExit(main())
