"""除权日 (ex-date) 的模拟盘口径: 批次折算边界 / 涨跌停参考价 / 可卖数量 / 绩效配对。

除权在盘前生效, 所以当日三处都必须是「折算后」口径:
  1. 只有事件日之前的批次参与折算 —— 当日成交的份额不该被送转 (否则份额与 NAV 虚增);
  2. 涨跌停参考价 = 上一交易日 raw close / ex_factor —— 直接用未复权昨收会把除权
     缺口判成跌停, 当日正常价卖不出 (与 indicators/pipeline 的除权口径一致);
  3. 卖出的可卖数量按折算后的批次校验 —— 合股后仍按旧数量卖出会凭空多卖。

持仓 / 归因 / 回合统计以前各算一遍, 因子只有前两处看得到; 现在三份派生都走
replay_lots 同一份实现, 以下用例逐条锚定, 防止再次分叉。
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl
import pytest

from app.strategy import paper
from app.tickflow.repository import DataStore, KlineRepository

SYM = "600519.SH"
DAY0 = date(2026, 9, 24)
EX_DAY = DAY0 + timedelta(days=1)


def _cap_account(tmp_path, cash: float = 1_000_000.0) -> dict:
    return paper.create_account(tmp_path, cash)


def _write_daily(tmp_path, rows: list[tuple[date, float, float]]) -> None:
    """写 kline_daily 分区: [(day, open, close)] (不复权 raw 价)。"""
    repo = KlineRepository(DataStore(tmp_path))
    repo.append_daily(pl.DataFrame({
        "symbol": [SYM] * len(rows),
        "date": [r[0] for r in rows],
        "open": [r[1] for r in rows],
        "high": [max(r[1], r[2]) for r in rows],
        "low": [min(r[1], r[2]) for r in rows],
        "close": [r[2] for r in rows],
        "volume": [10000.0] * len(rows),
        "amount": [r[2] * 10000.0 for r in rows],
    }))


def _write_factor(tmp_path, day: date, factor: float) -> None:
    out = tmp_path / "adj_factor" / "all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": [SYM], "trade_date": [day], "ex_factor": [factor]},
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64},
    ).write_parquet(out)


def _buy_at_close(tmp_path, day: date, qty: int, ref: float) -> dict:
    order, err = paper.create_order(
        tmp_path, SYM, "buy", qty=qty, order_type="close", ref_price=ref
    )
    assert err is None
    assert paper.settle_day(tmp_path, day.isoformat())["filled"] == 1
    return order


def test_除权日当日成交批次不被折算(tmp_path, monkeypatch):
    """10送2.5 (factor 1.25): 事件日之前 1000 股 → 1250 股; 当日新买的 1000 股不变。

    旧实现按台账顺序一律乘因子, 当日批次也被送转 → 2500 股 (虚增 250 股,
    按 8.00 计价即 NAV 虚增 2000 元)。
    """
    monkeypatch.setattr(paper, "cn_today", lambda: EX_DAY)
    _write_daily(tmp_path, [(DAY0, 10.0, 10.0), (EX_DAY, 8.0, 8.0)])
    _cap_account(tmp_path)
    first = _buy_at_close(tmp_path, DAY0, 1000, 10.0)

    _write_factor(tmp_path, EX_DAY, 1.25)
    second = _buy_at_close(tmp_path, EX_DAY, 1000, 8.0)

    pos = paper.load_positions(tmp_path)[SYM]
    assert pos["qty"] == pytest.approx(1250 + 1000)
    # 总成本守恒: 折算只动份额, 均价由 cost/qty 导出 → 除权不造出市值
    fills = {f["order_id"]: f for f in paper.load_fills(tmp_path) if f.get("kind") == "fill"}
    total_cost = sum(f["qty"] * f["price"] + f["fee"] for f in fills.values())
    assert pos["avg_cost"] * pos["qty"] == pytest.approx(total_cost, rel=1e-6)
    assert fills[first["id"]]["qty"] == 1000 and fills[second["id"]]["qty"] == 1000


def test_除权日涨跌停基准按因子折算(tmp_path, monkeypatch):
    """昨收 10.0 / 因子 1.25 → 参考价 8.0 → 跌停 7.20 (不是未复权的 9.00)。

    7.50 是除权后 -6.25% 的正常价; 旧实现拿 10.0 判板会把它当成封跌停而拒单。
    """
    monkeypatch.setattr(paper, "cn_today", lambda: EX_DAY)
    _write_daily(tmp_path, [(DAY0, 10.0, 10.0), (EX_DAY, 7.5, 7.5)])
    acc = _cap_account(tmp_path)
    _buy_at_close(tmp_path, DAY0, 1000, 10.0)

    _write_factor(tmp_path, EX_DAY, 1.25)
    order, err = paper.create_order(tmp_path, SYM, "sell", qty=1000, order_type="close")
    assert err is None
    assert paper.settle_day(tmp_path, EX_DAY.isoformat())["filled"] == 1

    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "filled"
    assert got["fill_price"] == pytest.approx(paper.apply_slippage(7.5, "sell", acc["slippage_bps"]))


def test_除权日卖出按折算后的可卖数量校验(tmp_path, monkeypatch):
    """合股 (factor 0.5): 1000 股变 500 股, 前一晚挂的 1000 股卖单撮合时必须被拒。

    拒单发生在撮合侧 (除权先入账), 下单时的可卖校验用的还是折算后重放的持仓 ——
    两处都按同一份 replay_lots, 不会出现「卖出不存在的份额」。
    """
    monkeypatch.setattr(paper, "cn_today", lambda: EX_DAY)
    _write_daily(tmp_path, [(DAY0, 10.0, 10.0), (EX_DAY, 20.0, 20.0)])
    _cap_account(tmp_path)
    _buy_at_close(tmp_path, DAY0, 1000, 10.0)

    order, err = paper.create_order(tmp_path, SYM, "sell", qty=1000, order_type="close")
    assert err is None
    _write_factor(tmp_path, EX_DAY, 0.5)
    summary = paper.settle_day(tmp_path, EX_DAY.isoformat())

    assert summary["filled"] == 0
    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "expired" and "可卖" in got["reason"]
    assert paper.load_positions(tmp_path)[SYM]["qty"] == pytest.approx(500)


def test_除权后卖出按摊薄成本配对(tmp_path, monkeypatch):
    """回合统计要看到除权: 1250 股卖出的成本基础是那 1000 股的买入成本。

    旧实现 round_trips 自己重放且跳过 corp 行 → 送转出的 250 股没有成本基础,
    盈亏被算错 (缺口还会被静默丢弃)。
    """
    monkeypatch.setattr(paper, "cn_today", lambda: EX_DAY)
    _write_daily(tmp_path, [(DAY0, 10.0, 10.0), (EX_DAY, 8.0, 8.0)])
    _cap_account(tmp_path)
    _buy_at_close(tmp_path, DAY0, 1000, 10.0)

    _write_factor(tmp_path, EX_DAY, 1.25)
    paper.settle_day(tmp_path, EX_DAY.isoformat())      # 入账 + 物化: 可卖 1250
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=1250, order_type="close")
    assert err is None
    paper.settle_day(tmp_path, EX_DAY.isoformat())      # 同日重跑不二次折算

    rounds = paper.round_trips(tmp_path)
    assert len(rounds) == 1 and rounds[0]["qty"] == pytest.approx(1250)

    fills = [f for f in paper.load_fills(tmp_path) if f.get("kind") == "fill"]
    buy = next(f for f in fills if f["side"] == "buy")
    sold = next(f for f in fills if f["side"] == "sell")
    proceeds = sold["qty"] * sold["price"] - sold["fee"]
    cost = buy["qty"] * buy["price"] + buy["fee"]
    assert rounds[0]["pnl"] == pytest.approx(proceeds - cost, abs=0.01)
    assert paper.stats(tmp_path)["realized_pnl"] == pytest.approx(proceeds - cost, abs=0.01)
    assert paper.get_order(tmp_path, sell["id"])["status"] == "filled"


def test_除权因子被修订只告警不追改(tmp_path, monkeypatch, caplog):
    """台账已按某因子折算过后, 因子表被修订不自动追改历史 —— 只告警提示追加冲正。

    追改会让历史净值序列在原地变形 (已定版的 NAV 与台账不一致), 修复一律以
    追加冲正行表达 (见模块 docstring 的账务正确性原则)。
    """
    monkeypatch.setattr(paper, "cn_today", lambda: EX_DAY)
    _write_daily(tmp_path, [(DAY0, 10.0, 10.0), (EX_DAY, 8.0, 8.0)])
    _cap_account(tmp_path)
    _buy_at_close(tmp_path, DAY0, 1000, 10.0)

    _write_factor(tmp_path, EX_DAY, 1.25)
    paper.settle_day(tmp_path, EX_DAY.isoformat())

    _write_factor(tmp_path, EX_DAY, 1.5)      # 因子表被修订
    with caplog.at_level(logging.WARNING, logger="app.strategy.paper"):
        assert paper._apply_corp_actions(tmp_path, EX_DAY.isoformat()) == 0

    assert "除权因子已修订" in caplog.text
    corp = [f for f in paper.load_fills(tmp_path) if f.get("kind") == "corp_action"]
    assert len(corp) == 1 and corp[0]["factor"] == pytest.approx(1.25)
    assert paper.load_positions(tmp_path)[SYM]["qty"] == pytest.approx(1250)
