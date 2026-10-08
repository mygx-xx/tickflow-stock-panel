"""虚拟账户(模拟盘)域模块测试 — 撮合口径 / 账务一致性 / 管道幂等。

设计文档: docs/paper-trading-plan.md。价格全程不复权 raw 价;
台账 append-only, 持仓/现金由重放推导, 修复以追加记录表达。
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

import polars as pl
import pytest

from app.market_time import CN_TZ
from app.strategy import paper
from app.tickflow.repository import DataStore, KlineRepository

SYM = "600519.SH"


def _cap_account(tmp_path, cash: float = 1_000_000.0) -> dict:
    return paper.create_account(tmp_path, cash)


def _write_daily(tmp_path, rows: list[tuple[date, float, float]]) -> None:
    """写 kline_daily 分区: [(day, open, close)] (不复权 raw 价)。"""
    repo = KlineRepository(DataStore(tmp_path))
    df = pl.DataFrame(
        {
            "symbol": [SYM] * len(rows),
            "date": [r[0] for r in rows],
            "open": [r[1] for r in rows],
            "high": [max(r[1], r[2]) for r in rows],
            "low": [min(r[1], r[2]) for r in rows],
            "close": [r[2] for r in rows],
            "volume": [10000.0] * len(rows),
            "amount": [r[2] * 10000.0 for r in rows],
        }
    )
    repo.append_daily(df)


def _write_factor(tmp_path, day: date, factor: float, asset_type: str = "stock") -> None:
    sub = "adj_factor_etf" if asset_type == "etf" else "adj_factor"
    out = tmp_path / sub / "all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": [SYM], "trade_date": [day], "ex_factor": [factor]},
        schema={"symbol": pl.String, "trade_date": pl.Date, "ex_factor": pl.Float64},
    ).write_parquet(out)


# ── 纯函数口径 ──────────────────────────────────────────
def test_fees_min_commission_and_stamp_tax_sell_only():
    # 佣金最低 5 元: 100 股 x 10 元 x 万2.5 = 0.25 → 取 5
    assert paper.buy_fee(100, 10.0, 0.00025) == 5.0
    # 大额按比例: 10000 股 x 10 元 = 10万 x 万2.5 = 25
    assert paper.buy_fee(10000, 10.0, 0.00025) == 25.0
    # 印花税仅卖出: 25 + 10万 x 千1 = 125
    assert paper.sell_fee(10000, 10.0, 0.00025, 0.001) == 25.0 + 100.0
    assert paper.buy_fee(10000, 10.0, 0.001) == 100.0


def test_slippage_direction_adverse():
    assert paper.apply_slippage(10.0, "buy", 5) > 10.0
    assert paper.apply_slippage(10.0, "sell", 5) < 10.0
    assert paper.apply_slippage(10.0, "buy", 5) == pytest.approx(10.005)


def test_涨跌停口径不再由paper自维护():
    """paper 不再平行实现第二套涨跌停 —— 幅度与价格都来自 app.price_limits。

    旧实现有两处偏差 (口径用例见 tests/test_price_limits.py):
      - 基金统一 10%, 创业板/科创板 ETF 实际 20%;
      - ST 股完全识别不了, 且用 Python round() (银行家舍入) 而非交易所四舍五入。
    """
    assert not hasattr(paper, "limit_pct")
    assert not hasattr(paper, "limit_prices")


def test_风险警示板现行幅度与主板一致(tmp_path, monkeypatch):
    """2026-07-06 起主板 ST 涨跌幅与主板同为 10% (规则已变更), paper 按当日日期取幅度。

    名称只用来判「是不是风险警示票」与基金的板块归属; 5% 是历史口径, 由
    tests/test_price_limits.py 按日期锚定, 模拟盘不再单独维护一套。
    """
    day = date(2026, 10, 8)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)
    # 10.50 在 10% 板内 → 成交
    paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.5}, name_map={SYM: "ST某某"})) == 1
    # 11.00 触及涨停 → 买不进 (与是否 ST 无关)
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}, name_map={SYM: "ST某某"}) == []
    assert paper.get_order(tmp_path, order["id"])["status"] == "expired"


def test_创业板etf按两成涨跌幅成交(tmp_path, monkeypatch):
    """基金 20% 幅度只有 price_limits + 名称能判出来; 旧实现统一 10% 会误拒。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    acc = _cap_account(tmp_path)
    etf = "159915.SZ"
    repo = KlineRepository(DataStore(tmp_path))

    def _bar(d: date, price: float) -> pl.DataFrame:
        return pl.DataFrame({
            "symbol": [etf], "date": [d],
            "open": [price], "high": [price], "low": [price], "close": [price],
            "volume": [1e6], "amount": [price * 1e6],
        })

    repo.append_etf_daily(pl.concat([_bar(day - timedelta(days=1), 2.0), _bar(day, 2.3)]))
    order, err = paper.create_order(
        tmp_path, etf, "buy", qty=1000, order_type="close",
        asset_type="etf", ref_price=2.0,
    )
    assert err is None
    # 收盘 2.30 = 昨收 +15%: 创业板 ETF 板 20% 内应成交; 按旧的统一 10% 会判成涨停拒单
    summary = paper.settle_day(tmp_path, day.isoformat(), name_map={etf: "创业板ETF易方达"})
    assert summary["filled"] == 1
    filled = paper.get_order(tmp_path, order["id"])
    # 成交价 = 收盘价的不利滑点价, 且被夹在板内 (涨停 2.40)
    assert filled["fill_price"] == pytest.approx(paper.apply_slippage(2.3, "buy", acc["slippage_bps"]))
    assert filled["fill_price"] < 2.40


def test_qty_from_amount_floors_to_lot():
    assert paper.qty_from_amount(9_999.0, 100.0) == 0  # 不足一手
    assert paper.qty_from_amount(10_500.0, 100.0) == 100
    assert paper.qty_from_amount(99_000.0, 100.0) == 900


# ── 下单校验 ────────────────────────────────────────────
def test_create_order_requires_account(tmp_path):
    with pytest.raises(ValueError, match="尚未创建"):
        paper.create_order(tmp_path, SYM, "buy", qty=100)


def test_create_order_rejects_bad_qty_and_insufficient_cash(tmp_path):
    _cap_account(tmp_path, 100_000.0)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=150)  # 非百股整数倍
    assert "100 的整数倍" in err
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=2000, ref_price=100.0)
    assert "资金不足" in err


def test_create_order_etf_market_converts_to_next_open(tmp_path):
    _cap_account(tmp_path)
    order, err = paper.create_order(
        tmp_path, "510300.SH", "buy", qty=1000, order_type="market", asset_type="etf", ref_price=4.0,
    )
    assert err is None
    assert order["order_type"] == "next_open"


def test_sell_requires_holding_and_t1(tmp_path, monkeypatch):
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _cap_account(tmp_path)
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=100)
    assert "不能卖出" in err

    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    buy_order, err = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    assert err is None
    assert buy_order["status"] == "pending"
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0})) == 1
    # T+1: 当日买入当日不可卖
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=100)
    assert "可卖数量不足" in err

    # 次日可卖
    monkeypatch.setattr(paper, "cn_today", lambda: day + timedelta(days=1))
    paper._materialize(tmp_path)
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=100)
    assert err is None and sell["status"] == "pending"


def test_cancel_pending_only(tmp_path, monkeypatch):
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _cap_account(tmp_path)
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    cancelled, err = paper.cancel_order(tmp_path, order["id"])
    assert err is None and cancelled["status"] == "cancelled"
    _, err = paper.cancel_order(tmp_path, order["id"])
    assert "不可撤销" in err


# ── 撮合流程 ────────────────────────────────────────────
def test_immediate_fill_cash_and_positions_consistent(tmp_path, monkeypatch):
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)

    order, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0)
    assert err is None
    events = paper.evaluate_intraday(tmp_path, {SYM: 10.0, "000002.SZ": 5.0})
    assert len(events) == 1
    # V3 成交事件: 字段对齐监控告警 (source=paper), ts 为毫秒, 文案中文可读
    ev = events[0]
    assert ev["source"] == "paper" and ev["type"] == "fill" and ev["severity"] == "info"
    assert ev["symbol"] == SYM and ev["side"] == "buy" and ev["qty"] == 1000
    assert ev["price"] == pytest.approx(10.005)
    assert isinstance(ev["ts"], int) and ev["ts"] > 0
    assert "买入成交" in ev["message"] and "1000股" in ev["message"]
    filled = paper.get_order(tmp_path, order["id"])
    # 滑点向上: 10 * 1.0005
    assert filled["fill_price"] == pytest.approx(10.005)
    fee = paper.buy_fee(1000, 10.005, paper.DEFAULT_COMMISSION_PCT)
    assert filled["fees"] == pytest.approx(fee)

    positions, cash = paper.replay_positions(tmp_path)
    assert cash == pytest.approx(1_000_000 - 1000 * 10.005 - fee)
    assert positions[SYM]["qty"] == 1000
    assert positions[SYM]["avg_cost"] == pytest.approx((1000 * 10.005 + fee) / 1000)
    # 台账重放 == 物化持仓 (账务一致性)
    mat = paper.load_positions(tmp_path)[SYM]
    assert mat["qty"] == positions[SYM]["qty"]
    assert mat["avg_cost"] == pytest.approx(positions[SYM]["avg_cost"])


def test_limit_up_buy_rejected(tmp_path, monkeypatch):
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    # 主板涨停 11.0; 快照 raw 价触板即拒 (滑点不参与判板, 见 test_滑点不改变涨跌停可达性)
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}) == []
    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "expired"
    assert "涨停" in got["reason"]


def test_limit_down_sell_rejected(tmp_path, monkeypatch):
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    assert err is None
    paper.evaluate_intraday(tmp_path, {SYM: 10.0})
    monkeypatch.setattr(paper, "cn_today", lambda: day + timedelta(days=1))
    paper._materialize(tmp_path)
    sell, err = paper.create_order(tmp_path, SYM, "sell", qty=100)
    assert err is None
    # 跌停 9.0; 快照 raw 价触板 → 拒单
    assert paper.evaluate_intraday(tmp_path, {SYM: 9.0}) == []
    got = paper.get_order(tmp_path, sell["id"])
    assert got["status"] == "expired" and "跌停" in got["reason"]


def test_滑点不改变涨跌停可达性(tmp_path, monkeypatch):
    """可达性按 raw 价判, 滑点只决定成交价。

    旧实现拿「滑点后价」判板: 大滑点账户在板内侧的正常报价会被误判成封板拒单。
    新口径下 raw 10.99 < 涨停 11.00 → 可成交, 成交价仍夹在板内。
    """
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    paper.create_account(tmp_path, 100_000.0, slippage_bps=200.0)   # 2% 滑点
    order, err = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.99)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.99})) == 1
    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "filled"
    # 10.99 +200bps = 11.2098, 越过当日涨停 → 夹到 11.00 成交
    assert got["fill_price"] == pytest.approx(11.0)


def test_零股只能一次性清仓(tmp_path, monkeypatch):
    """除权折算会掉出非整百余额 (100 股 ×1.5 = 150)。

    交易所口径: 不足一手的余额只能一次性全额申报 —— 旧实现一律要求整百, 余额
    永远卖不掉 (持仓卡死)。
    """
    day = date(2026, 9, 24)
    ex_day = day + timedelta(days=1)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0})) == 1

    # 次日除权 1.5 → 可卖 150 (盘前入账, 与 settle_day 同序)
    monkeypatch.setattr(paper, "cn_today", lambda: ex_day)
    _write_factor(tmp_path, ex_day, 1.5)
    assert paper._apply_corp_actions(tmp_path, ex_day.isoformat()) == 1
    paper._materialize(tmp_path)

    _, err = paper.create_order(tmp_path, SYM, "sell", qty=120)
    assert err is not None and "零股只能一次性全部卖出" in err
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=150)
    assert err is None


def test_settle_next_open_close_and_postpone_expire(tmp_path):
    day = date(2026, 9, 24)
    _cap_account(tmp_path)
    _write_daily(tmp_path, [
        (day - timedelta(days=1), 10.0, 10.0),
        (day, 10.2, 10.8),  # 涨跌幅内 (±10%): 开 10.2 收 10.8
    ])
    no_open, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, order_type="next_open", ref_price=10.0)
    close_ord, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, order_type="close", ref_price=10.0)
    summary = paper.settle_day(tmp_path, day.isoformat())
    assert summary["filled"] == 2
    assert paper.get_order(tmp_path, no_open["id"])["fill_price"] == pytest.approx(10.2 * 1.0005)
    assert paper.get_order(tmp_path, close_ord["id"])["fill_price"] == pytest.approx(10.8 * 1.0005)

    # 停牌顺延 → 连续无行情 4 日过期 (MAX_POSTPONE_DAYS=3)
    stuck, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, order_type="next_open", ref_price=11.0)
    for i in range(4):
        s = paper.settle_day(tmp_path, (day + timedelta(days=i + 1)).isoformat())
    got = paper.get_order(tmp_path, stuck["id"])
    assert got["status"] == "expired" and "自动过期" in got["reason"]
    assert s["expired"] >= 1


def test_pending_sell_occupies_available_quota(tmp_path, monkeypatch):
    """超卖防护: pending 卖出单占用可卖额度, 第二张超额卖出单在下单时被拒。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=200, ref_price=10.0)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0})) == 1
    # 次日: 可卖 200
    monkeypatch.setattr(paper, "cn_today", lambda: day + timedelta(days=1))
    paper._materialize(tmp_path)
    s1, err = paper.create_order(tmp_path, SYM, "sell", qty=100)
    assert err is None and s1["status"] == "pending"
    # 已 pending 100, 再挂 200 超出可卖 200 → 拒 (基础可卖校验或占用检查, 双闸任一)
    _, err = paper.create_order(tmp_path, SYM, "sell", qty=200)
    assert err is not None and ("可卖" in err or "占用" in err)
    # 撤单后额度释放, 可以再挂
    paper.cancel_order(tmp_path, s1["id"])
    s2, err = paper.create_order(tmp_path, SYM, "sell", qty=100)
    assert err is None and s2["status"] == "pending"


def test_pending_buy_occupies_cash_at_fill(tmp_path, monkeypatch):
    """资金占用兜底: 两张 pending 买入, 第一张成交后现金不够第二张 → 第二张拒单留痕。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [
        (day - timedelta(days=1), 10.0, 10.0),
        (day, 10.0, 10.0),  # 当日有行情, close 单才会在 settle 撮合
    ])
    _cap_account(tmp_path, 10_000.0)  # 只够一张 500 股 x 10 元 + 费用
    a, _ = paper.create_order(tmp_path, SYM, "buy", qty=500, order_type="close", ref_price=10.0)
    b, err = paper.create_order(tmp_path, SYM, "buy", qty=500, order_type="close", ref_price=10.0)
    assert err is None  # 预检按单张各自通过, 由撮合侧 (资金/占用双闸) 兜底
    paper.settle_day(tmp_path, day.isoformat())
    assert paper.get_order(tmp_path, a["id"])["status"] == "filled"
    got_b = paper.get_order(tmp_path, b["id"])
    # b 必须被拒且留痕: 现金已被 a 消耗, 基础资金校验或占用检查任一拦下都算防线生效
    assert got_b["status"] == "expired"
    assert got_b["reason"] is not None
    # 台账只有一张成交, 现金不透支
    fills = [f for f in paper.load_fills(tmp_path) if f.get("kind", "fill") == "fill"]
    assert len(fills) == 1
    _, cash = paper.replay_positions(tmp_path)
    assert cash >= 0


def test_同刻下单仍按先到先排序(tmp_path, monkeypatch):
    """先到先得必须可判定: created_at 撞车时撮合顺序会退化到 id 的随机后缀。

    撮合的 FIFO 完全按 created_at 排 (paper._order_sort_key); 两张抢同一笔现金的
    买单若时间戳相同, 谁先成交就变成掷骰子 —— 上面那条占用用例曾因此偶发红
    (Windows 时钟粒度约 1 ms, 连续下单会拿到同一时刻)。这里把时钟钉死,
    不依赖操作系统碰运气。
    """
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    monkeypatch.setattr(paper, "cn_now", lambda: datetime(2026, 9, 24, 14, 30, tzinfo=CN_TZ))
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0), (day, 10.0, 10.0)])
    _cap_account(tmp_path, 10_000.0)  # 只够一张 500 股 x 10 元 + 费用
    a, _ = paper.create_order(tmp_path, SYM, "buy", qty=500, order_type="close", ref_price=10.0)
    b, _ = paper.create_order(tmp_path, SYM, "buy", qty=500, order_type="close", ref_price=10.0)
    # 时钟钉死 → 两张单 created_at 完全相同, 先后只能由 id 里的单调序号判出来
    assert a["created_at"] == b["created_at"]
    assert paper._order_sort_key(a) < paper._order_sort_key(b)
    paper.settle_day(tmp_path, day.isoformat())
    assert paper.get_order(tmp_path, a["id"])["status"] == "filled"
    assert paper.get_order(tmp_path, b["id"])["status"] == "expired"


def test_position_symbol_cap(tmp_path, monkeypatch):
    """持仓标的数上限: 第 51 只新开仓买入被拒 (加仓已有持仓不受限)。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path, 100_000_000.0)
    # 人为塞满 50 只持仓 (直接写台账, 绕过 100 股 x 价格的资金约束)
    for i in range(50):
        paper._append_fill(tmp_path, {
            "seq": i, "ts": "", "date": (day - timedelta(days=1)).isoformat(),
            "order_id": f"seed{i}", "symbol": f"{600000 + i:06d}.SH",
            "asset_type": "stock", "side": "buy", "qty": 100, "price": 1.0, "fee": 1.0,
            "kind": "fill",
        })
    paper._materialize(tmp_path)
    # 新开仓第 51 只 → 拒
    _, err = paper.create_order(tmp_path, "601999.SH", "buy", qty=100, ref_price=10.0)
    assert "上限" in err
    # 加仓已有持仓 → 放行
    ok, err = paper.create_order(tmp_path, "600000.SH", "buy", qty=100, ref_price=10.0)
    assert err is None and ok["status"] == "pending"


def test_settle_idempotent_no_double_fill(tmp_path):
    day = date(2026, 9, 24)
    _cap_account(tmp_path)
    _write_daily(tmp_path, [(day, 10.0, 10.5)])
    paper.create_order(tmp_path, SYM, "buy", qty=100, order_type="next_open", ref_price=10.0)
    paper.settle_day(tmp_path, day.isoformat())
    paper.settle_day(tmp_path, day.isoformat())  # 重跑同日
    fills = [f for f in paper.load_fills(tmp_path) if f.get("kind", "fill") == "fill"]
    assert len(fills) == 1
    _, cash = paper.replay_positions(tmp_path)
    assert cash == pytest.approx(1_000_000 - fills[0]["qty"] * fills[0]["price"] - fills[0]["fee"])


def test_corporate_action_adjusts_position_and_idempotent(tmp_path):
    day = date(2026, 9, 24)
    _cap_account(tmp_path)
    _write_daily(tmp_path, [
        (day - timedelta(days=1), 10.0, 10.0),
        (day, 8.0, 8.0),  # 除权后价格
    ])
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=1000, order_type="close", ref_price=10.0)
    paper.settle_day(tmp_path, (day - timedelta(days=1)).isoformat())
    assert paper.load_positions(tmp_path)[SYM]["qty"] == 1000

    _write_factor(tmp_path, day, 1.25)
    paper.settle_day(tmp_path, day.isoformat())
    pos = paper.load_positions(tmp_path)[SYM]
    assert pos["qty"] == pytest.approx(1250)
    # 成本被因子摊薄: 总成本不变 (买入费已摊入), 数量乘 1.25 后 avg_cost 除以 1.25
    order_after = paper.get_order(tmp_path, order["id"])
    total_cost = 1000 * order_after["fill_price"] + order_after["fees"]
    replayed = paper.replay_positions(tmp_path)[0][SYM]
    assert replayed["avg_cost"] * 1250 == pytest.approx(total_cost, rel=1e-4)

    # 重跑同日: 不二次乘因子
    paper.settle_day(tmp_path, day.isoformat())
    assert paper.load_positions(tmp_path)[SYM]["qty"] == pytest.approx(1250)
    corp = [f for f in paper.load_fills(tmp_path) if f.get("kind") == "corp_action"]
    assert len(corp) == 1


def test_nav_and_overview_math(tmp_path, monkeypatch):
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _cap_account(tmp_path, 100_000.0)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0)
    assert err is None
    paper.evaluate_intraday(tmp_path, {SYM: 10.0})
    fill = next(f for f in paper.load_fills(tmp_path) if f.get("kind", "fill") == "fill")

    # 盘中估算: 价格 11 → 市值 11000
    ov = paper.overview(tmp_path, {SYM: 11.0})
    assert ov["estimating"] is True
    assert ov["market_value"] == pytest.approx(11000.0)
    assert ov["cash"] == pytest.approx(100_000 - fill["qty"] * fill["price"] - fill["fee"])
    assert ov["total"] == pytest.approx(ov["cash"] + ov["market_value"])

    # 定版净值: 收盘价写 nav/daily.jsonl, 同日重写幂等
    nav = paper.daily_nav(tmp_path, day.isoformat(), {SYM: 10.5})
    paper._write_nav_line(tmp_path, day.isoformat(), nav)
    paper._write_nav_line(tmp_path, day.isoformat(), nav)
    lines = paper.load_nav(tmp_path)
    assert len(lines) == 1 and lines[0]["nav"] == pytest.approx(nav["nav"])


def test_round_trips_fifo_stats(tmp_path):
    day = date(2026, 9, 1)
    _cap_account(tmp_path)
    _write_daily(tmp_path, [(day, 10.0, 10.0), (day + timedelta(days=1), 12.0, 12.0)])
    # 手工构造台账 (纯统计口径测试, 绕过撮合): 两次买入一次卖出
    for f in (
        {"seq": 1, "ts": "", "date": day.isoformat(), "order_id": "o1", "symbol": SYM,
         "asset_type": "stock", "side": "buy", "qty": 100, "price": 10.0, "fee": 5.0, "kind": "fill"},
        {"seq": 2, "ts": "", "date": day.isoformat(), "order_id": "o2", "symbol": SYM,
         "asset_type": "stock", "side": "buy", "qty": 100, "price": 11.0, "fee": 5.0, "kind": "fill"},
        {"seq": 3, "ts": "", "date": (day + timedelta(days=1)).isoformat(), "order_id": "o3", "symbol": SYM,
         "asset_type": "stock", "side": "sell", "qty": 150, "price": 12.0, "fee": 5.0 + 18.0, "kind": "fill"},
    ):
        paper._append_fill(tmp_path, f)
    rounds = paper.round_trips(tmp_path)
    assert len(rounds) == 2
    # FIFO: 先平 10 元批次 100 股, 再平 11 元批次 50 股
    assert rounds[0]["qty"] == 100 and rounds[0]["open_date"] == day.isoformat()
    assert rounds[0]["pnl"] == pytest.approx(12.0 * 100 - (10.0 * 100 + 5.0) - (5.0 + 18.0) * 100 / 150, rel=1e-3)
    assert rounds[1]["qty"] == 50
    st = paper.stats(tmp_path)
    assert st["rounds"] == 2 and st["win_rate"] == 100.0


def test_stats_持平回合不进亏损分母(tmp_path):
    """盈亏恰为 0 的回合既不算胜也不算负 —— 旧口径把它计进亏损, 亏损均值被摊薄。

    构造 +100 / -50 / 0 三个回合: 真实平均亏损 50 → 盈亏比 2.0; 把 0 也算亏损
    会得到平均 25、盈亏比 4.0 (虚高)。
    """
    day = date(2026, 9, 1)
    _cap_account(tmp_path)
    rows = [
        {"seq": 1, "side": "buy", "qty": 100, "price": 10.0},
        {"seq": 2, "side": "sell", "qty": 100, "price": 11.0},   # +100
        {"seq": 3, "side": "buy", "qty": 100, "price": 12.0},
        {"seq": 4, "side": "sell", "qty": 100, "price": 11.5},   # -50
        {"seq": 5, "side": "buy", "qty": 100, "price": 13.0},
        {"seq": 6, "side": "sell", "qty": 100, "price": 13.0},   # 0
    ]
    for r in rows:
        paper._append_fill(tmp_path, {
            "ts": "", "date": day.isoformat(), "order_id": f"o{r['seq']}", "symbol": SYM,
            "asset_type": "stock", "fee": 0.0, "kind": "fill", **r,
        })

    st = paper.stats(tmp_path)
    assert st["rounds"] == 3
    assert st["win_rate"] == pytest.approx(33.33)            # 分母含持平, 分子只算盈利
    assert st["profit_loss_ratio"] == pytest.approx(2.0)
    assert st["realized_pnl"] == pytest.approx(50.0)


# ── 自动跟单 (V2) ───────────────────────────────────────
def _auto_rule(**overrides) -> dict:
    base = {
        "name": "跟单测试",
        "match_kind": "strategy",
        "match_id": "strat_1",
        "side": "buy",
        "size_mode": "fixed_amount",
        "size_value": 5000.0,
        "order_type": "next_open",
        "cooldown_days": 5,
        "enabled": True,
    }
    base.update(overrides)
    return base


def test_auto_rule_crud_and_validation(tmp_path):
    from app.strategy import paper_auto
    with pytest.raises(ValueError, match="不能为空"):
        paper_auto.create_auto_rule(tmp_path, _auto_rule(name=""))
    with pytest.raises(ValueError, match="match_kind"):
        paper_auto.create_auto_rule(tmp_path, _auto_rule(match_kind="xxx"))
    with pytest.raises(ValueError, match="不能超过 100"):
        paper_auto.create_auto_rule(tmp_path, _auto_rule(size_mode="pct_equity", size_value=150))
    rule = paper_auto.create_auto_rule(tmp_path, _auto_rule())
    assert rule["id"].startswith("arule_")
    assert len(paper_auto.load_auto_rules(tmp_path)) == 1
    paper_auto.set_enabled(tmp_path, rule["id"], False)
    assert paper_auto.load_auto_rules(tmp_path, enabled_only=True) == []
    assert paper_auto.delete_auto_rule(tmp_path, rule["id"]) is True
    assert paper_auto.delete_auto_rule(tmp_path, rule["id"]) is False


def test_auto_trigger_matches_strategy_event(tmp_path, monkeypatch):
    from app.strategy import paper_auto
    from app.services import trading_day
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    # 本用例锚定「交易日历不可用 → 退回日历天」这条降级支路
    monkeypatch.setattr(trading_day, "trading_calendar", lambda: None)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)
    paper_auto.create_auto_rule(tmp_path, _auto_rule())

    ev = {"source": "strategy", "strategy_id": "strat_1", "rule_id": "r1",
          "symbol": SYM, "price": 10.0}
    orders = paper_auto.on_rule_events(tmp_path, [ev])
    assert len(orders) == 1
    assert orders[0]["qty"] == 500        # 5000 元 / 10 元 → 500 股
    assert orders[0]["source"].startswith("auto:")
    assert orders[0]["order_type"] == "next_open"

    # 冷却期内同 symbol 不再触发
    assert paper_auto.on_rule_events(tmp_path, [ev]) == []
    # 冷却外 (6 个天前下的单) 恢复触发
    for o in paper.load_orders(tmp_path):
        o["created_at"] = "2026-09-18T10:00:00+08:00"
        paper.save_order(tmp_path, o)
    orders2 = paper_auto.on_rule_events(tmp_path, [ev])
    assert len(orders2) == 1


def test_跟单冷却数交易日(tmp_path, monkeypatch):
    """cooldown_days 声明的是交易日: 连休期间日历天在走, 冷却不该跟着空转到期。

    旧口径 (today - created).days 让「3 个交易日冷却」在国庆前后被 8 个日历天空掉,
    节后第一个交易日立刻又能跟一轮 —— 频率比规则设定的高。
    """
    from app.strategy import paper_auto
    from app.services import trading_day
    day = date(2026, 10, 8)
    created = day - timedelta(days=5)
    monkeypatch.setattr(paper_auto, "cn_now",
                        lambda: datetime.combine(day, time(10, 0), tzinfo=CN_TZ))
    monkeypatch.setattr(trading_day, "trading_calendar", lambda: object())
    elapsed = {"days": 2}            # 5 个日历天里只开了 2 个交易日
    monkeypatch.setattr(trading_day, "trading_days",
                        lambda start, end: [start] * elapsed["days"])

    rule = {"id": "arule_cool"}
    order = {
        "id": "order_cool", "symbol": SYM, "asset_type": "stock", "side": "buy",
        "qty": 500, "order_type": "next_open", "status": "filled", "ref_price": 10.0,
        "postponed": 0, "source": f"auto:{rule['id']}",
        "created_at": f"{created.isoformat()}T09:35:00+08:00",
        "filled_at": None, "fill_price": None, "fees": None, "reason": None,
    }
    paper.save_order(tmp_path, order)

    assert paper_auto._in_cooldown(tmp_path, rule, SYM, 3, paper.DEFAULT_ACCOUNT_ID)
    elapsed["days"] = 3              # 交易日补足 3 天 → 放行
    assert not paper_auto._in_cooldown(tmp_path, rule, SYM, 3, paper.DEFAULT_ACCOUNT_ID)
    # 冷却 0 = 不启用, 不看日历
    assert not paper_auto._in_cooldown(tmp_path, rule, SYM, 0, paper.DEFAULT_ACCOUNT_ID)


def test_跟单按事件的资产类型下单(tmp_path, monkeypatch):
    """ETF 跟单要带 asset_type=etf 下单, 不能靠代码前缀猜 —— 涨跌停幅度与最小价位不同。

    monitor 把规则的 asset_type 放进事件 (见 MonitorRuleEngine), 这里锚定它一路传到
    create_order; 事件缺字段时才由 create_order 兜底。
    """
    from app.strategy import paper_auto
    seen: list[dict] = []

    def _spy(data_dir, symbol, side, **kwargs):
        seen.append({"symbol": symbol, "side": side, **kwargs})
        return None, "测试拦截: 不下真单"

    monkeypatch.setattr(paper, "create_order", _spy)
    _cap_account(tmp_path)
    paper_auto.create_auto_rule(tmp_path, _auto_rule())
    ev = {"source": "strategy", "strategy_id": "strat_1", "symbol": "159915.SZ",
          "price": 2.0, "asset_type": "etf"}

    paper_auto.on_rule_events(tmp_path, [ev])

    assert len(seen) == 1
    assert seen[0]["symbol"] == "159915.SZ" and seen[0]["asset_type"] == "etf"


def test_auto_trigger_non_matching_events_ignored(tmp_path):
    from app.strategy import paper_auto
    _cap_account(tmp_path)
    paper_auto.create_auto_rule(tmp_path, _auto_rule())
    # strategy_id 不匹配
    ev1 = {"source": "strategy", "strategy_id": "other", "symbol": SYM, "price": 10.0}
    # 无 symbol (批量事件)
    ev2 = {"source": "strategy", "strategy_id": "strat_1", "symbol": "", "price": 10.0}
    # 无价格
    ev3 = {"source": "strategy", "strategy_id": "strat_1", "symbol": SYM}
    assert paper_auto.on_rule_events(tmp_path, [ev1, ev2, ev3]) == []


def test_auto_rule_disabled_not_triggered(tmp_path):
    from app.strategy import paper_auto
    _cap_account(tmp_path)
    rule = paper_auto.create_auto_rule(tmp_path, _auto_rule(enabled=False))
    ev = {"source": "strategy", "strategy_id": "strat_1", "symbol": SYM, "price": 10.0}
    assert paper_auto.on_rule_events(tmp_path, [ev]) == []
    paper_auto.set_enabled(tmp_path, rule["id"], True)
    assert len(paper_auto.on_rule_events(tmp_path, [ev])) == 1


def test_auto_frozen_account_rejects(tmp_path, monkeypatch):
    from app.strategy import paper_auto
    acc = _cap_account(tmp_path)
    acc["status"] = "frozen"
    paper.save_account(tmp_path, acc)
    paper_auto.create_auto_rule(tmp_path, _auto_rule())
    ev = {"source": "strategy", "strategy_id": "strat_1", "symbol": SYM, "price": 10.0}
    assert paper_auto.on_rule_events(tmp_path, [ev]) == []


def test_max_drawdown_pure():
    assert paper.max_drawdown([]) is None
    assert paper.max_drawdown([100]) == 0.0
    assert paper.max_drawdown([100, 110, 99, 105]) == pytest.approx(0.1)
    assert paper.max_drawdown([100, 120, 60, 90]) == pytest.approx(0.5)
    assert paper.max_drawdown([100, 100, 100]) == 0.0


# ── 涨跌停排队 (V2: queue_limit_orders) ─────────────────
def test_limit_queue_default_off_rejects_immediately(tmp_path, monkeypatch):
    """默认关闭: 触及涨停直接过期 (与 V1 行为一致, 由 test_limit_up_buy_rejected 锁定)。"""
    acc = _cap_account(tmp_path)
    assert acc["queue_limit_orders"] is False


def test_limit_queue_retries_next_open_then_expires(tmp_path, monkeypatch):
    """排队开启: 涨停买不进 → 转 next_open 次日重试, 连续涨停计顺延, 超限过期。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    # 盘中 10:00 下单并排队: 当日开盘价在排队前已打印, 当日结算不成交, 从次日开盘起重试
    monkeypatch.setattr(paper, "cn_now", lambda: datetime.combine(day, time(10, 0), tzinfo=CN_TZ))
    paper.create_account(tmp_path, 1_000_000, queue_limit_orders=True)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)

    # day1 盘中触及涨停 11.0 → 不再过期, 转 next_open 排队
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}) == []
    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "pending" and got["order_type"] == "next_open"
    assert got["postponed"] == 1 and "排队" in got["reason"]

    # day2..day4 开盘连续一字涨停 (open == 涨停价) → 顺延到 3, 第 4 次超限过期
    _write_daily(tmp_path, [
        (day, 12.10, 12.10),              # day1 收盘 (排队当日, 结算跳过)
        (day + timedelta(days=1), 13.31, 13.31),   # vs 12.10 涨停 13.31
        (day + timedelta(days=2), 14.64, 14.64),   # vs 13.31 涨停 14.64
        (day + timedelta(days=3), 16.10, 16.10),   # vs 14.64 涨停 16.10 → 第 4 次
    ])
    for i in range(4):
        paper.settle_day(tmp_path, (day + timedelta(days=i)).isoformat())
    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "expired" and "排队" in got["reason"]
    assert got["postponed"] == paper.MAX_POSTPONE_DAYS


def test_limit_queue_fills_when_open_below_limit(tmp_path, monkeypatch):
    """排队开启后次日开盘回落 → 按 next_open 正常成交。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    monkeypatch.setattr(paper, "cn_now", lambda: datetime.combine(day, time(10, 0), tzinfo=CN_TZ))
    paper.create_account(tmp_path, 1_000_000, queue_limit_orders=True)
    _write_daily(tmp_path, [
        (day - timedelta(days=1), 10.0, 10.0),
        (day, 10.6, 11.0),                        # 排队当日 (盘中触及涨停 11.0)
        (day + timedelta(days=1), 10.5, 10.9),    # 次日开盘 10.5 未涨停
    ])
    order, _ = paper.create_order(tmp_path, SYM, "buy", qty=100, ref_price=10.0)
    assert paper.evaluate_intraday(tmp_path, {SYM: 11.0}) == []  # 涨停排队
    assert paper.get_order(tmp_path, order["id"])["status"] == "pending"
    assert paper.settle_day(tmp_path, day.isoformat())["filled"] == 0  # 当日开盘价在排队前已打印
    summary = paper.settle_day(tmp_path, (day + timedelta(days=1)).isoformat())
    assert summary["filled"] == 1
    got = paper.get_order(tmp_path, order["id"])
    assert got["status"] == "filled" and got["fill_price"] == pytest.approx(paper.apply_slippage(10.5, "buy", 5.0))


# ── 多账户 (V2) ─────────────────────────────────────────
def test_multi_account_isolation(tmp_path, monkeypatch):
    """两账户完全隔离: 订单/持仓/现金/统计互不可见。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    # 账户遍历按创建时间 (list_account_ids): Windows 时钟粒度约 1 ms, 真实时钟下
    # 连续两次 create_account 会撞在同一微秒, 顺序退化到 id 字典序 → 下面那条
    # ["default", "acc_a"] 断言约 3% 概率翻转 (fb210b1 实测 2/60)。显式推进时钟,
    # 让「按创建时间」这条契约可判定。
    clock = [datetime.combine(day, time(9, 0), tzinfo=CN_TZ)]
    monkeypatch.setattr(paper, "cn_now", lambda: clock[0])
    paper.create_account(tmp_path, 1_000_000, account_id="default", name="主账户")
    clock[0] += timedelta(seconds=1)
    paper.create_account(tmp_path, 500_000, account_id="acc_a", name="策略A")

    # 同一标的分别在两个账户各买一笔
    assert paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id="default") == []  # 无订单
    o1, err = paper.create_order(tmp_path, SYM, "buy", qty=100, account_id="default")
    o2, err = paper.create_order(tmp_path, SYM, "buy", qty=200, account_id="acc_a")
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id="default")) == 1
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0}, account_id="acc_a")) == 1

    # 订单互不可见
    assert [o["id"] for o in paper.load_orders(tmp_path, "default")] == [o1["id"]]
    assert [o["id"] for o in paper.load_orders(tmp_path, "acc_a")] == [o2["id"]]
    # 持仓互不可见
    assert paper.load_positions(tmp_path, "default")[SYM]["qty"] == 100
    assert paper.load_positions(tmp_path, "acc_a")[SYM]["qty"] == 200
    # 现金互不可见 (含费用扣减不同)
    ov1 = paper.overview(tmp_path, account_id="default")
    ov2 = paper.overview(tmp_path, account_id="acc_a")
    assert ov1["cash"] > ov2["cash"]
    assert ov1["account_name"] == "主账户" and ov2["account_name"] == "策略A"

    # 账户列表与遍历
    assert paper.list_account_ids(tmp_path) == ["default", "acc_a"]
    infos = {a["id"]: a for a in paper.list_accounts(tmp_path)}
    assert set(infos) == {"default", "acc_a"} and infos["acc_a"]["name"] == "策略A"

    # settings 只影响目标账户
    paper.update_settings(tmp_path, "acc_a", queue_limit_orders=True)
    assert paper.get_account(tmp_path, "default")["queue_limit_orders"] is False
    assert paper.get_account(tmp_path, "acc_a")["queue_limit_orders"] is True

    # 净值隔离
    paper.settle_day(tmp_path, day.isoformat(), account_id="default")
    paper.settle_day(tmp_path, day.isoformat(), account_id="acc_a")
    nav1 = paper.load_nav(tmp_path, "default")
    nav2 = paper.load_nav(tmp_path, "acc_a")
    assert len(nav1) == 1 and len(nav2) == 1 and nav1[0]["nav"] != nav2[0]["nav"]


def test_account_id_validation_blocks_traversal(tmp_path):
    import pytest as _pytest
    for bad in ("../evil", "a/b", "", "x" * 33, "a b"):
        with _pytest.raises(ValueError):
            paper.get_account(tmp_path, bad)
        with _pytest.raises(ValueError):
            paper.create_account(tmp_path, 100.0, account_id=bad)


def test_legacy_single_account_layout_migrates(tmp_path, monkeypatch):
    """旧版 data/paper/* 单账户布局首次访问自动迁移到 accounts/default/。"""
    monkeypatch.setattr(paper, "_MIGRATION_DONE", False)
    legacy = tmp_path / "paper"
    legacy.mkdir()
    (legacy / "account.json").write_text(
        __import__("json").dumps({
            "id": "default", "initial_cash": 800000.0, "cash": 750000.0,
            "commission_pct": 0.00025, "stamp_tax_pct": 0.001, "slippage_bps": 5.0,
            "status": "active", "created_at": "2026-09-01T10:00:00",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (legacy / "orders").mkdir()
    (legacy / "orders" / "order_x.json").write_text("{}", encoding="utf-8")

    acc = paper.get_account(tmp_path)
    assert acc is not None and acc["cash"] == 750000.0
    # 物理迁移到位
    assert (tmp_path / "paper" / "accounts" / "default" / "account.json").exists()
    assert (tmp_path / "paper" / "accounts" / "default" / "orders" / "order_x.json").exists()
    assert not (legacy / "account.json").exists()
    # 幂等: 重复访问不报错
    assert paper.get_account(tmp_path)["cash"] == 750000.0


# ── V3 补全: 结算成交留痕 / 自动跟单下单事件 ──────────
def test_day_fill_events_returns_todays_fills(tmp_path, monkeypatch):
    """day_fill_events: 只取指定交易日、kind=fill 的台账行, 转 AlertEvent 同构事件。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, [(day - timedelta(days=1), 10.0, 10.0)])
    _cap_account(tmp_path)

    # 当日无成交 → 空列表
    assert paper.day_fill_events(tmp_path, day.isoformat()) == []

    _, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0)
    assert err is None
    assert len(paper.evaluate_intraday(tmp_path, {SYM: 10.0})) == 1

    events = paper.day_fill_events(tmp_path, day.isoformat())
    assert len(events) == 1
    ev = events[0]
    assert ev["source"] == "paper" and ev["type"] == "fill" and ev["severity"] == "info"
    assert ev["symbol"] == SYM and ev["side"] == "buy" and ev["qty"] == 1000
    assert isinstance(ev["ts"], int) and ev["ts"] > 0
    assert "买入成交" in ev["message"]
    # 幂等 (结算重跑同日不重复追加事件)
    assert paper.day_fill_events(tmp_path, day.isoformat()) == events


def test_auto_order_events_shape(tmp_path):
    """auto_order_events: 订单 → AlertEvent 同构事件, rule_id 从 source 还原。"""
    from app.strategy import paper_auto

    orders = [
        {"symbol": "600000.SH", "side": "buy", "qty": 500,
         "order_type": "next_open", "source": "auto:rule_abc"},
        {"symbol": "000001.SZ", "side": "sell", "qty": 200,
         "order_type": "market", "source": "manual"},
    ]
    events = paper_auto.auto_order_events(orders, account_id="acc1")
    assert len(events) == 2
    buy_ev, sell_ev = events
    assert buy_ev["source"] == "paper" and buy_ev["type"] == "auto_order"
    assert buy_ev["rule_id"] == "rule_abc" and buy_ev["account_id"] == "acc1"
    assert "买入" in buy_ev["message"] and "次日开盘" in buy_ev["message"] and "500股" in buy_ev["message"]
    assert isinstance(buy_ev["ts"], int) and buy_ev["ts"] > 0
    assert sell_ev["rule_id"] == "" and "卖出" in sell_ev["message"] and "即时" in sell_ev["message"]
    # 空列表 → 空
    assert paper_auto.auto_order_events([]) == []
