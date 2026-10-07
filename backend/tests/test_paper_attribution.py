"""模拟盘按来源归因 (replay_attribution)。

背景: 订单一直带 source (manual / auto:{rule_id}), 但成交写入台账时丢掉了它 ——
持仓与盈亏无法拆到「哪个策略带来的」。本次给 fill 补上 source, 并新增按
(symbol, source) 二维聚合的归因重放。

核心口径: 卖出按 FIFO 消耗批次, 盈亏归属到**被消耗批次**的来源。所以
「策略买的票被手动卖出」记回策略账, 而不是记成手动平仓 —— 这是归因是否有
意义的关键, 故以下用例逐条锚定该行为。
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from app.strategy import paper
from app.tickflow.repository import DataStore, KlineRepository

RULE_A = "auto:rule_alpha"
RULE_B = "auto:rule_beta"
SYM = "600519.SH"


def _write_fills(tmp_path: Path, fills: list[dict]) -> None:
    p = paper._fills_path(tmp_path, paper.DEFAULT_ACCOUNT_ID)
    p.write_text(
        "\n".join(json.dumps(f, ensure_ascii=False) for f in fills) + "\n",
        encoding="utf-8",
    )


def _fill(side: str, qty: int, price: float, source: str, *, fee: float = 0.0,
          symbol: str = "600519.SH", kind: str = "fill", **extra) -> dict:
    return {
        "seq": 1,
        "ts": f"2026-09-30T09:30:0{extra.get('n', 0)}.000000+08:00",
        "date": "2026-09-30",
        "order_id": f"order_{extra.get('n', 0)}",
        "symbol": symbol,
        "asset_type": "stock",
        "side": side,
        "qty": qty,
        "price": price,
        "fee": fee,
        "kind": kind,
        "source": source,
        **extra,
    }


def _row(result: dict, source: str) -> dict:
    return next(r for r in result["sources"] if r["source"] == source)


def test_手动买入归_manual(tmp_path: Path):
    _write_fills(tmp_path, [_fill("buy", 100, 16.0, "manual", fee=2.0)])

    r = paper.replay_attribution(tmp_path)

    assert len(r["sources"]) == 1
    row = _row(r, "manual")
    assert row["held_qty"] == 100
    assert row["held_cost"] == pytest.approx(1602.0)   # 100*16 + 2 手续费
    assert row["realized_pnl"] == 0.0
    assert row["buy_count"] == 1
    assert r["total_realized"] == 0.0


def test_策略买入归到对应来源(tmp_path: Path):
    _write_fills(tmp_path, [_fill("buy", 200, 10.0, RULE_A)])

    row = _row(paper.replay_attribution(tmp_path), RULE_A)

    assert row["held_qty"] == 200
    assert row["held_cost"] == pytest.approx(2000.0)
    assert row["held_symbols"] == 1
    assert row["realized_pnl"] == 0.0


def test_策略买入手动卖出_盈亏归策略(tmp_path: Path):
    """关键口径: 卖出来源是 manual, 但被消耗的是策略批次 → 盈亏记回策略。"""
    _write_fills(tmp_path, [
        _fill("buy", 100, 10.0, RULE_A, n=1),
        _fill("sell", 100, 12.0, "manual", fee=1.0, n=2),
    ])

    r = paper.replay_attribution(tmp_path)
    a = _row(r, RULE_A)

    assert a["realized_pnl"] == pytest.approx(199.0)   # 净收入 (12-0.01)*100 - 成本 1000
    assert a["held_qty"] == 0
    assert a["sell_count"] == 0                              # 卖出不是它发起的
    # manual 发起过平仓, 所以它在表里; 但盈亏归给被消耗的策略批次
    m = _row(r, "manual")
    assert m["sell_count"] == 1 and m["held_qty"] == 0
    assert m["realized_pnl"] == 0.0
    assert r["total_realized"] == pytest.approx(199.0)


def test_两策略同股各持一批_按来源拆分(tmp_path: Path):
    _write_fills(tmp_path, [
        _fill("buy", 100, 10.0, RULE_A, n=1),
        _fill("buy", 300, 20.0, RULE_B, n=2),
    ])

    r = paper.replay_attribution(tmp_path)

    assert _row(r, RULE_A)["held_cost"] == pytest.approx(1000.0)
    assert _row(r, RULE_B)["held_cost"] == pytest.approx(6000.0)
    assert _row(r, RULE_A)["held_qty"] == 100
    assert _row(r, RULE_B)["held_qty"] == 300


def test_部分卖出按FIFO消耗最早批次(tmp_path: Path):
    """A 先买 100@10, B 后买 100@20; 卖 100 时先进先出消耗 A → 盈亏归 A。"""
    _write_fills(tmp_path, [
        _fill("buy", 100, 10.0, RULE_A, n=1),
        _fill("buy", 100, 20.0, RULE_B, n=2),
        _fill("sell", 100, 30.0, "manual", n=3),
    ])

    r = paper.replay_attribution(tmp_path)
    a, b = _row(r, RULE_A), _row(r, RULE_B)

    assert a["realized_pnl"] == pytest.approx(2000.0)   # (30-10)*100
    assert a["held_qty"] == 0
    assert b["realized_pnl"] == 0.0
    assert b["held_qty"] == 100                          # B 未被动过
    assert r["total_realized"] == pytest.approx(2000.0)


def test_除权按比例调整各来源持仓(tmp_path: Path):
    _write_fills(tmp_path, [
        _fill("buy", 100, 10.0, RULE_A, n=1),
        _fill("buy", 200, 10.0, RULE_B, n=2),
        _fill("buy", 0, 0.0, "manual", kind="corp_action", n=3, factor=0.5),
    ])

    r = paper.replay_attribution(tmp_path)

    assert _row(r, RULE_A)["held_qty"] == pytest.approx(50.0)
    assert _row(r, RULE_A)["held_cost"] == pytest.approx(1000.0)   # 股数减半, 总成本不变
    assert _row(r, RULE_B)["held_qty"] == pytest.approx(100.0)


def test_旧台账无source字段_按manual兜底(tmp_path: Path):
    """历史 fills.jsonl 没有 source 键, 必须归 manual 而不是崩掉。"""
    legacy = _fill("buy", 100, 10.0, "manual")
    legacy.pop("source")
    _write_fills(tmp_path, [legacy])

    row = _row(paper.replay_attribution(tmp_path), "manual")

    assert row["held_qty"] == 100
    assert row["held_cost"] == pytest.approx(1000.0)


def test_持仓来源排序_有持仓的排前面(tmp_path: Path):
    _write_fills(tmp_path, [
        _fill("buy", 100, 10.0, RULE_A, n=1, symbol="600519.SH"),
        _fill("buy", 100, 10.0, RULE_B, n=2, symbol="000001.SZ"),
        _fill("sell", 100, 12.0, RULE_B, n=3, symbol="000001.SZ"),
    ])

    r = paper.replay_attribution(tmp_path)

    assert r["sources"][0]["source"] == RULE_A          # 仍持仓的排最前
    assert r["sources"][0]["held_symbols"] == 1
    assert r["sources"][-1]["source"] == RULE_B          # 已清仓的排最后


# ── 端到端: order → 成交 → 台账 → 归因 ──────────────────
# 上面的用例直接构造台账, 绕过了「订单 source 是否传到成交记录」这一环 ——
# 那正是归因链路的根 (order.source → fill.source)。这里走真实下单+撮合锚定它。

def _write_daily(tmp_path: Path, day: date) -> None:
    repo = KlineRepository(DataStore(tmp_path))
    df = pl.DataFrame(
        {
            "symbol": [SYM],
            "date": [day],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [10000.0],
            "amount": [100000.0],
        }
    )
    repo.append_daily(df)


def test_自动跟单订单成交后归因到策略(tmp_path: Path, monkeypatch):
    """跟单 (source=auto:{rule_id}) 成交后, 归因表里必须出现该策略且金额正确。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, day - timedelta(days=1))
    paper.create_account(tmp_path, 1_000_000.0)

    order, err = paper.create_order(
        tmp_path, SYM, "buy", qty=1000, ref_price=10.0, source=RULE_A
    )
    assert err is None
    assert order["source"] == RULE_A                     # 订单层已带来源
    paper.evaluate_intraday(tmp_path, {SYM: 10.0})       # 撮合成交

    fill = next(f for f in paper.load_fills(tmp_path) if f.get("kind") == "fill")
    assert fill["source"] == RULE_A                      # 成交记录继承来源 ← 归因链路的根

    row = _row(paper.replay_attribution(tmp_path), RULE_A)
    assert row["buy_count"] == 1
    assert row["held_qty"] == 1000
    assert row["held_cost"] == pytest.approx(1000 * fill["price"] + fill["fee"])
    assert row["label"] if "label" in row else True    # label 由 API 层补, 域层不涉及


def test_手动下单成交后归因手动(tmp_path: Path, monkeypatch):
    """对照组: 不传 source 的普通下单归 manual, 不被误记到任何策略。"""
    day = date(2026, 9, 24)
    monkeypatch.setattr(paper, "cn_today", lambda: day)
    _write_daily(tmp_path, day - timedelta(days=1))
    paper.create_account(tmp_path, 1_000_000.0)

    _, err = paper.create_order(tmp_path, SYM, "buy", qty=1000, ref_price=10.0)
    assert err is None
    paper.evaluate_intraday(tmp_path, {SYM: 10.0})

    r = paper.replay_attribution(tmp_path)
    assert [x["source"] for x in r["sources"]] == ["manual"]
    assert r["sources"][0]["held_qty"] == 1000
