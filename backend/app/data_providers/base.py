"""Provider contracts for external market data sources.

The first implementation wraps TickFlow. Other providers (Tushare/AkShare/etc.)
should return the same normalized Polars schemas so storage, indicators and
backtests stay data-source agnostic.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Protocol

import polars as pl

AssetType = Literal["stock", "index", "etf"]

# 集合竞价长表的规范列与类型(契约层单源: provider 产出、服务落盘、读取校验共用一份)。
# 放在 base 而非插件内, 是为了让服务层不反向依赖某个 provider 的私有常量。
AUCTION_COLUMNS: tuple[str, ...] = (
    "symbol",
    "trade_date",
    "segment",
    "datetime",
    "price",
    "matched_volume",
    "unmatched_volume",
    "unmatched_side",
)
AUCTION_SCHEMA: dict[str, type] = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "segment": pl.Utf8,
    "datetime": pl.Datetime("us"),
    "price": pl.Float64,
    "matched_volume": pl.Float64,
    "unmatched_volume": pl.Float64,
    "unmatched_side": pl.Utf8,
}


@dataclass(frozen=True)
class ProviderCapabilities:
    instruments: bool = False
    daily: bool = False
    adj_factor: bool = False
    minute: bool = False
    realtime: bool = False
    depth5: bool = False
    auction: bool = False
    financial: bool = False


class MarketDataProvider(Protocol):
    name: str
    capabilities: ProviderCapabilities

    def get_instruments(self, asset_type: AssetType) -> pl.DataFrame:
        """Return normalized instruments: symbol/name/code/exchange/asset_type/source."""

    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType,
    ) -> pl.DataFrame:
        """Return normalized daily K rows."""

    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType,
    ) -> pl.DataFrame:
        """Return normalized adjustment factors: symbol/trade_date/ex_factor."""

    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType,
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        """Return normalized minute K rows. Implementations may return empty.

        on_chunk_done 契约: provider 实现内部以 2 参 (cur, total) 调用;
        3 参 seg_label 适配由 kline_sync._try_custom_minute 包装层负责,
        不应泄漏到 provider 契约层。
        """

    def get_realtime(
        self,
        universes: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        """Return normalized realtime quotes. Implementations may return empty."""

    def get_depth_batch(self, symbols: list[str]) -> dict[str, dict]:
        """Return five-level order books keyed by symbol."""

    def get_auction_batch(self, symbols: list[str], trade_date: date) -> pl.DataFrame:
        """Return normalized call-auction points (long format, one row per snapshot).

        列契约: symbol / trade_date(date) / segment("open"|"close") /
        datetime(北京墙钟 naive, 如 09:15:00) / price(元) / matched_volume(手) /
        unmatched_volume(手, 非负) / unmatched_side("buy"=买侧剩余 |"sell"=卖侧剩余)。
        "matched_volume" 是该时点虚拟撮合下的**可匹配量**(非累计), 成交额由消费方按
        ``price × matched_volume × 100`` 换算(手→股), provider 不在契约里塞派生金额。
        无竞价的标的(指数部分品种/北交所多数个股/超回溯窗口)不产行; 全批取不到时
        返回空帧, 传输层失败必须抛异常由服务按批隔离(与 get_depth_batch 一致)。
        """
