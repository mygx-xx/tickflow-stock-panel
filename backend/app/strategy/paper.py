"""虚拟账户(模拟盘)域模块 — 虚拟资金 + 真实行情价格的模拟撮合, 不接券商不做实盘。

设计文档: docs/paper-trading-plan.md。与回测互补: 回测重放历史, 模拟盘实时向前。

账务正确性原则: fills.jsonl 成交台账是唯一事实源 (append-only), 持仓/现金/净值
全部可由台账重放推导; 修复一律以追加记录表达 (除权 corp_action / 冲正), 不改历史行。

价格口径: 全程不复权 raw 价 (与真实交易一致)。费用参数与回测引擎
(app/backtest/engine.py) 同名同默认值: commission_pct 双边佣金 / stamp_tax_pct
卖出印花税 / slippage_bps 滑点。

多账户 (V2): 数据目录 data/paper/accounts/{account_id}/, 账户间完全隔离
(订单/台账/持仓/净值/自动规则均按账户存放)。所有域函数带 account_id 参数
(默认 "default", 即迁移前的单账户)。旧版 data/paper/ 单账户布局首次访问时
自动迁移到 accounts/default/, 一次性且幂等。

撮合规则:
  - 即时单: 盘中由行情轮询钩子按最新快照价成交 (evaluate_intraday);
  - next_open / close 单: 盘后管道 settle_day 按当日 raw_open / raw_close 成交;
  - T+1: 当日买入次一交易日方可卖 (lots 按 buy_date 记账, available = date < today);
    除权折算出的零股只能一次性全部卖出 (与交易所「不足一手的余额一次性申报」一致);
  - 涨跌停: 按 **raw 价**判可达性 (滑点只是成交价偏移, 不代表封板), 幅度与最小价位
    算术复用 app/price_limits (与 indicators/backtest 同一套口径); 触及涨停的买单 /
    跌停的卖单默认拒单 (expired 留痕); 账户开启 queue_limit_orders 后转「排队次日
    重试」(转 next_open, 计顺延, 超限过期);
  - 停牌/缺行情: 顺延, 连续顺延超过 max_postpone 日自动过期;
  - 除权: 按 ex_factor 折算持仓数量 (乘 factor) 与摊薄单位成本, 只作用于事件日之前
    的批次, 且事件在撮合之前入账; 台账记 corp_action 行。

除权口径 (与 docs/paper-trading-plan.md 一致): ex_factor 是除权事件的 pre/post 价格
比值, 无法拆出送转比例与现金分红, 因此整条因子都按「份额折算」入账 —— 现金分红不
产生现金流入, 而是体现在被摊低的单位成本里。缺勤分红当日净值按折算后数量×除权后
价计算, 与真实到账口径有差异。
"""
from __future__ import annotations

import json
import logging
import math
import re
import shutil
import threading
import uuid
from datetime import date as _date
from datetime import datetime
from datetime import time as _time
from pathlib import Path

import polars as pl

from app.market_time import CN_TZ, cn_now, cn_today
from app.price_limits import asset_limit_pct, asset_limit_prices
from app.services.fs_utils import atomic_write_text

logger = logging.getLogger(__name__)

# 单进程内全部写操作互斥 (API 下单 / 行情钩子撮合 / 盘后结算共用一把锁)
PAPER_LOCK = threading.RLock()

# 费用默认值 — 与回测 engine.py 一致
DEFAULT_COMMISSION_PCT = 0.00025   # 双边佣金 万2.5
MIN_COMMISSION = 5.0               # 佣金最低 5 元
DEFAULT_STAMP_TAX_PCT = 0.001      # 印花税, 仅卖出
DEFAULT_SLIPPAGE_BPS = 5.0         # 滑点 (bps, 万分之5)

MAX_POSTPONE_DAYS = 3             # 顺延上限: 连续 N 个交易日无法成交自动过期
# 结算价格的打印时刻 (北京墙钟): 开盘价 09:30 / 收盘价 15:00。下单晚于该时刻的单
# 不能按当日这个价成交 (下单时价格已知), 留到下一交易日结算
_SESSION_OPEN = _time(9, 30)
_SESSION_CLOSE = _time(15, 0)
LOT_SIZE = 100                     # 一手 100 股 (股票/ETF 同)
MAX_POSITION_SYMBOLS = 50          # 持仓标的数上限 (防误操作)
# 仓位约束默认值 — 与回测 MatcherConfig 口径对齐。
# 回测侧 max_exposure_pct (总仓位上限) 默认 1.0; paper 侧此前完全没有这两个约束,
# 导致「回测设了 60% 仓位, 模拟盘却满仓」的口径不一致。默认 None = 不限制,
# 保持既有账户行为不变。
DEFAULT_MAX_EXPOSURE_PCT = None    # 总仓位上限 (占净值比例)
DEFAULT_MAX_POSITION_WEIGHT = None # 单票权重上限
DEFAULT_ACCOUNT_ID = "default"
_ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

_MIGRATION_DONE = False


# ── 路径与账户目录 ──────────────────────────────────────
def validate_account_id(account_id: str) -> str:
    """账户 ID 只允许字母/数字/下划线/短横线 (防路径穿越), 非法抛 ValueError。"""
    if not isinstance(account_id, str) or not _ACCOUNT_ID_RE.match(account_id or ""):
        raise ValueError(f"账户 ID 非法 (仅限字母数字下划线短横线, ≤32 字符): {account_id!r}")
    return account_id


def accounts_base(data_dir: Path) -> Path:
    return Path(data_dir) / "paper" / "accounts"


def _migrate_legacy(data_dir: Path) -> None:
    """旧单账户布局 data/paper/* → data/paper/accounts/default/* (一次性, 幂等)。

    仅当旧 account.json 存在且 accounts/ 尚不存在时迁移; 移动失败只留痕不阻断
    (下次访问重试), 避免迁移异常导致模拟盘整体不可用。
    """
    legacy = Path(data_dir) / "paper"
    base = accounts_base(data_dir)
    if not (legacy / "account.json").exists() or base.exists():
        return
    dest = base / DEFAULT_ACCOUNT_ID
    try:
        dest.mkdir(parents=True, exist_ok=True)
        for name in ("account.json", "orders", "fills.jsonl", "positions.json", "nav", "auto_rules"):
            src = legacy / name
            if src.exists():
                shutil.move(str(src), str(dest / name))
        logger.info("paper: 旧单账户数据已迁移到 %s", dest)
    except Exception as e:
        logger.warning("paper 旧数据迁移失败 (下次重试): %s", e)
        # 迁移半途而废时目标目录已建立, 会跳过后续重试 — 清掉半成品目录保证可重试
        try:
            if dest.exists() and not (dest / "account.json").exists():
                shutil.rmtree(dest, ignore_errors=True)
                if not any(base.iterdir()):
                    base.rmdir()
        except Exception:
            pass


def _root(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> Path:
    global _MIGRATION_DONE
    if not _MIGRATION_DONE:
        _migrate_legacy(data_dir)
        _MIGRATION_DONE = True
    d = accounts_base(data_dir) / validate_account_id(account_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _orders_dir(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> Path:
    d = _root(data_dir, account_id) / "orders"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _nav_dir(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> Path:
    d = _root(data_dir, account_id) / "nav"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _now_iso() -> str:
    return cn_now().isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def _order_sort_key(order: dict) -> tuple:
    """撮合顺序: created_at 先到先得; 同刻则按 id (内含毫秒戳+随机后缀, 稳定)。"""
    return (order.get("created_at", ""), order.get("id", ""))


# ── 账户 ────────────────────────────────────────────────
def get_account(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict | None:
    p = _root(data_dir, account_id) / "account.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("paper account load failed: %s", e)
        return None


def create_account(
    data_dir: Path,
    initial_cash: float,
    *,
    account_id: str = DEFAULT_ACCOUNT_ID,
    name: str | None = None,
    commission_pct: float = DEFAULT_COMMISSION_PCT,
    stamp_tax_pct: float = DEFAULT_STAMP_TAX_PCT,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    queue_limit_orders: bool = False,
    max_exposure_pct: float | None = DEFAULT_MAX_EXPOSURE_PCT,
    max_position_weight: float | None = DEFAULT_MAX_POSITION_WEIGHT,
) -> dict:
    """创建账户 (同 id 已存在则原样返回, 不覆盖 — 幂等)。"""
    validate_account_id(account_id)
    with PAPER_LOCK:
        existing = get_account(data_dir, account_id)
        if existing is not None:
            return existing
        if isinstance(initial_cash, bool) or not isinstance(initial_cash, (int, float)) or initial_cash <= 0:
            raise ValueError("初始资金必须是正数")
        exposure = _validate_ratio("max_exposure_pct", max_exposure_pct)
        pos_weight = _validate_ratio("max_position_weight", max_position_weight)
        acc = {
            "id": account_id,
            "name": (name or "").strip() or account_id,
            "initial_cash": float(initial_cash),
            "cash": float(initial_cash),
            "commission_pct": float(commission_pct),
            "stamp_tax_pct": float(stamp_tax_pct),
            "slippage_bps": float(slippage_bps),
            "queue_limit_orders": bool(queue_limit_orders),
            # 仓位约束 (None = 不限制, 与回测 MatcherConfig 同口径)
            "max_exposure_pct": exposure,
            "max_position_weight": pos_weight,
            "status": "active",  # active / frozen
            "created_at": _now_iso(),
        }
        atomic_write_text(_root(data_dir, account_id) / "account.json", json.dumps(acc, ensure_ascii=False, indent=2))
        return acc


def save_account(data_dir: Path, acc: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    atomic_write_text(
        _root(data_dir, account_id) / "account.json",
        json.dumps(acc, ensure_ascii=False, indent=2),
    )


def list_account_ids(data_dir: Path) -> list[str]:
    """全部账户 id (按创建时间; 供钩子/结算遍历)。"""
    base = accounts_base(data_dir)
    if not base.exists():
        return []
    out: list[tuple[str, str]] = []
    for p in base.glob("*/account.json"):
        try:
            acc = json.loads(p.read_text(encoding="utf-8"))
            out.append((acc.get("created_at", ""), acc.get("id", p.parent.name)))
        except Exception:
            continue
    return [aid for _, aid in sorted(out)]


def list_accounts(data_dir: Path) -> list[dict]:
    """账户列表 (带最新净值摘要, 供切换器展示)。"""
    out: list[dict] = []
    for aid in list_account_ids(data_dir):
        acc = get_account(data_dir, aid)
        if acc is None:
            continue
        nav = load_nav(data_dir, aid)
        out.append({
            "id": acc["id"],
            "name": acc.get("name", acc["id"]),
            "status": acc.get("status", "active"),
            "initial_cash": acc.get("initial_cash"),
            "cash": acc.get("cash"),
            "latest_nav": nav[-1]["nav"] if nav else acc.get("cash"),
            "created_at": acc.get("created_at"),
        })
    return out


def update_settings(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID, **fields) -> dict:
    """更新账户设置 (涨跌停排队 / 费用三参数 / 仓位约束); 未知字段忽略。

    费用只影响之后的新成交 (与回测费用模型同口径), 已有台账不重算。
    仓位约束 (max_exposure_pct / max_position_weight) 只校验不追溯:
    设了之后的下单才受约束, 已持仓不因调小上限而被强平 ——
    与回测「约束只作用于新开仓」一致。
    """
    with PAPER_LOCK:
        acc = get_account(data_dir, account_id)
        if acc is None:
            raise ValueError("尚未创建模拟账户")
        if "queue_limit_orders" in fields and fields["queue_limit_orders"] is not None:
            acc["queue_limit_orders"] = bool(fields["queue_limit_orders"])
        for key, lo, hi in (
            ("commission_pct", 0.0, 0.01),    # 佣金率 ≤1% (100‱)
            ("stamp_tax_pct", 0.0, 0.05),     # 印花税 ≤5% (仅卖出)
            ("slippage_bps", 0.0, 200.0),     # 滑点 ≤200bps
        ):
            v = fields.get(key)
            if v is None:
                continue
            v = float(v)
            if not (lo <= v <= hi):
                raise ValueError(f"{key} 超出合理范围 ({lo}~{hi})")
            acc[key] = v
        # 仓位约束: 显式传 None 表示「不限制」, 传数值则校验后写入。
        for key in ("max_exposure_pct", "max_position_weight"):
            if key in fields:
                acc[key] = _validate_ratio(key, fields[key])
        save_account(data_dir, acc, account_id)
        return acc


# ── 费用 (纯函数) ───────────────────────────────────────
def apply_slippage(price: float, side: str, slippage_bps: float) -> float:
    """买入向上滑、卖出向下滑 (对成交价不利方向)。"""
    adj = price * (1 + slippage_bps / 10000.0) if side == "buy" else price * (1 - slippage_bps / 10000.0)
    return round(max(adj, 0.01), 4)


def buy_fee(qty: int, price: float, commission_pct: float) -> float:
    return round(max(qty * price * commission_pct, MIN_COMMISSION), 2)


def sell_fee(qty: int, price: float, commission_pct: float, stamp_tax_pct: float) -> float:
    return round(max(qty * price * commission_pct, MIN_COMMISSION) + qty * price * stamp_tax_pct, 2)


# ── 数量 (纯函数) ───────────────────────────────────────
def normalize_qty(qty: int) -> int:
    """下取整到百股整数倍。"""
    return int(qty // LOT_SIZE) * LOT_SIZE


def qty_from_amount(amount: float, ref_price: float) -> int:
    """按金额估算数量: 金额 / 参考价 向下取整到百股 (未计费用, 手续费从现金扣)。"""
    if ref_price <= 0:
        return 0
    return normalize_qty(int(amount / ref_price))


# ── 订单 ────────────────────────────────────────────────
def load_orders(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    out: list[dict] = []
    for f in sorted(_orders_dir(data_dir, account_id).glob("order_*.json")):
        try:
            out.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception as e:
            logger.warning("paper order load failed %s: %s", f.name, e)
    return out


def save_order(data_dir: Path, order: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    p = _orders_dir(data_dir, account_id) / f"{order['id']}.json"
    atomic_write_text(p, json.dumps(order, ensure_ascii=False, indent=2))


def get_order(data_dir: Path, order_id: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict | None:
    p = _orders_dir(data_dir, account_id) / f"{order_id}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def create_order(
    data_dir: Path,
    symbol: str,
    side: str,
    *,
    account_id: str = DEFAULT_ACCOUNT_ID,
    qty: int | None = None,
    amount: float | None = None,
    order_type: str = "market",
    asset_type: str | None = None,
    ref_price: float | None = None,
    source: str = "manual",
) -> tuple[dict | None, str | None]:
    """创建订单并校验。qty 与 amount 二选一 (amount 按参考价折算百股)。

    order_type: market(即时, 盘中钩子撮合) / next_open / close。
    ETF 即时单自动转 next_open (盘中钩子只喂股票快照)。
    返回 (order, None) 或 (None, 错误信息)。
    """
    with PAPER_LOCK:
        acc = get_account(data_dir, account_id)
        if acc is None:
            raise ValueError("尚未创建模拟账户")
        if acc.get("status") != "active":
            return None, "账户已冻结, 拒绝新订单"
        if side not in ("buy", "sell"):
            return None, f"side 非法: {side!r}"
        if order_type not in ("market", "next_open", "close"):
            return None, f"order_type 非法: {order_type!r}"
        symbol = (symbol or "").strip()
        if not symbol:
            return None, "symbol 不能为空"
        if asset_type is None:
            asset_type = "etf" if symbol.endswith((".SH", ".SZ")) and symbol.split(".")[0].startswith(("51", "56", "58", "15")) else "stock"

        # ETF 即时单 → 次日开盘 (盘中钩子只喂股票快照)
        if order_type == "market" and asset_type == "etf":
            order_type = "next_open"

        if qty is None and amount is None:
            return None, "qty 与 amount 必须提供一个"
        if qty is None:
            if ref_price is None or ref_price <= 0:
                return None, "amount 模式需要参考价 ref_price"
            qty = qty_from_amount(float(amount), ref_price)
            if qty <= 0:
                return None, f"金额不足以买一手 (参考价 {ref_price})"
        qty = int(qty)
        if qty <= 0:
            return None, "数量必须大于 0"
        if side == "buy" and qty % LOT_SIZE != 0:
            return None, f"数量必须是 {LOT_SIZE} 的整数倍"

        acc_cash = float(acc["cash"])
        if side == "buy":
            # 买入资金预检 (按参考价上界估算, 撮合时二次校验)
            est_price = float(ref_price) if ref_price else 0.0
            if est_price > 0 and qty * est_price * (1 + acc["slippage_bps"] / 10000) + buy_fee(qty, est_price, acc["commission_pct"]) > acc_cash:
                return None, f"可用资金不足 (需约 {qty * est_price:.0f}, 可用 {acc_cash:.0f})"
            # 仓位约束预检 (与回测 MatcherConfig 同口径, 未设则不限制)。
            # 放行时用参考价估算市值; 无参考价时跳过 —— 真实约束在撮合时按成交价二次校验。
            if est_price > 0:
                reason = _check_position_constraints(
                    acc, symbol, qty * est_price, data_dir, account_id
                )
                if reason:
                    return None, reason
        else:
            pos = load_positions(data_dir, account_id).get(symbol)
            if pos is None or pos["qty"] <= 0:
                return None, f"无 {symbol} 持仓, 不能卖出"
            # 可卖数量按当前交易日现算 (与 _fill_order / overview 同口径): 物化文件里的
            # available_qty 是上次重建时的 T+1 口径, 跨日后不会更新, 次日仍会是 0
            available = _available_of(pos["lots"], cn_today().isoformat())
            if qty > available + 1e-9:
                return None, f"可卖数量不足 (T+1): 可卖 {available:g}, 请求数量 {qty}"
            # 零股 (除权折算出的非整百余额) 只能一次性全部卖出 —— 与交易所
            # 「不足一手的余额一次性申报」一致; 整百数量不受限。
            if qty % LOT_SIZE != 0 and qty < math.floor(available):
                return None, (
                    f"卖出数量必须是 {LOT_SIZE} 的整数倍, 零股只能一次性全部卖出"
                    f" (可卖 {available:g})"
                )
            # 超卖防护: pending 卖出单占用可卖额度 — 同 symbol 的 pending 卖出合计
            # 不得超过可卖数量, 否则多张单各自通过校验、成交时逐张扣减会超额
            pending_sell = sum(
                int(o["qty"]) for o in load_orders(data_dir, account_id)
                if o["status"] == "pending" and o["side"] == "sell" and o["symbol"] == symbol
            )
            if pending_sell + qty > available + 1e-9:
                return None, (
                    f"可卖数量不足 (T+1): 可卖 {available:g}, "
                    f"已有待成交卖出 {pending_sell}, 请求数量 {qty}"
                )

        # 持仓标的数上限 (仅新开仓的买入; 加仓已有持仓不受限)
        positions = load_positions(data_dir, account_id)
        if (
            side == "buy"
            and symbol not in positions
            and len([p for p in positions.values() if p["qty"] > 0]) >= MAX_POSITION_SYMBOLS
        ):
            return None, f"持仓标的数已达上限 {MAX_POSITION_SYMBOLS}, 不能再开新仓"

        order = {
            "id": _new_id("order"),
            "symbol": symbol,
            "asset_type": asset_type,
            "side": side,
            "qty": qty,
            "order_type": order_type,
            "status": "pending",   # pending / filled / cancelled / expired
            "ref_price": float(ref_price) if ref_price else None,
            "postponed": 0,        # 顺延交易日计数 (停牌顺延 / 涨跌停排队共用)
            "source": source,      # manual / auto:{rule_id}
            "created_at": _now_iso(),
            "filled_at": None,
            "fill_price": None,
            "fees": None,
            "reason": None,
        }
        save_order(data_dir, order, account_id)
        return order, None


def cancel_order(data_dir: Path, order_id: str, account_id: str = DEFAULT_ACCOUNT_ID) -> tuple[dict | None, str | None]:
    """撤销 pending 订单; 已成交/已过期不可撤。"""
    with PAPER_LOCK:
        order = get_order(data_dir, order_id, account_id)
        if order is None:
            return None, f"订单不存在: {order_id}"
        if order["status"] != "pending":
            return None, f"订单状态为 {order['status']}, 不可撤销"
        order["status"] = "cancelled"
        order["reason"] = "manual cancel"
        save_order(data_dir, order, account_id)
        return order, None


# ── 成交台账与持仓 ──────────────────────────────────────
def _fills_path(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> Path:
    return _root(data_dir, account_id) / "fills.jsonl"


def load_fills(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    p = _fills_path(data_dir, account_id)
    if not p.exists():
        return []
    out: list[dict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception as e:
            logger.warning("paper fill line skipped: %s", e)
    return out


# ── 仓位约束校验 ─────────────────────────────────────────
def _validate_ratio(name: str, value) -> float | None:
    """校验仓位比例类约束。None = 不限制 (返回 None)。

    口径与回测 MatcherConfig 对齐: 合法区间 (0, 1]。传 0 或负数一律拒绝 ——
    「设成 0」几乎总是「以为能全空仓」或「手滑」的误解, 不该静默接受。
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} 必须是数字或 None")
    v = float(value)
    if not (0.0 < v <= 1.0):
        raise ValueError(f"{name} 超出合理范围 (0~1], 收到 {v}")
    return v


def _estimated_nav(acc: dict, data_dir: Path, account_id: str) -> float:
    """估算净值 = 现金 + 持仓市值。

    持仓估值优先用 `avg_cost`（无实时价时的保守口径）, 与 `overview` 一致。
    这里不能用实时价: 下单预检发生在盘中也可能拿不到快照, 用成本价可保证
    校验始终可用且偏保守（成本价通常低于现价, 净值偏低, 约束更严）。
    """
    mv = 0.0
    for pos in load_positions(data_dir, account_id).values():
        qty = pos.get("qty", 0)
        if qty > 0:
            mv += qty * float(pos.get("avg_cost", 0.0))
    return float(acc["cash"]) + mv


def _check_position_constraints(
    acc: dict,
    symbol: str,
    add_value: float,
    data_dir: Path,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> str | None:
    """买入前的仓位约束校验。返回拒绝原因 (None = 放行)。

    两个约束, 口径与回测 `MatcherConfig` 一致:
      max_exposure_pct: 总仓位上限 (现金 + 持仓市值 合计不超过净值 × 上限)
      max_position_weight: 单票权重上限 (该票市值不超过净值 × 上限)

    约束只作用于**新买入**: 调小上限不会强平已有持仓 (与回测一致)。
    已有持仓超限时不阻止加仓以外的操作, 但会阻止进一步买入该票 ——
    超限的票继续加仓会让偏离越走越大。
    """
    exposure_cap = acc.get("max_exposure_pct")
    weight_cap = acc.get("max_position_weight")
    if exposure_cap is None and weight_cap is None:
        return None
    if add_value <= 0:
        return None

    nav = _estimated_nav(acc, data_dir, account_id)
    if nav <= 0:
        return None  # 净值非正 (不应发生) 时不拦, 交给资金预检报错

    positions = load_positions(data_dir, account_id)
    mv_after = sum(
        float(p.get("qty", 0)) * float(p.get("avg_cost", 0.0))
        for p in positions.values()
        if p.get("qty", 0) > 0
    )

    # 总仓位上限: 买入后 (市值 + 本次买入) 不超过净值 × 上限
    if exposure_cap is not None:
        limit_value = nav * float(exposure_cap)
        if mv_after + add_value > limit_value + 1e-6:
            used_pct = mv_after / nav if nav else 0.0
            return (
                f"超出总仓位上限 {float(exposure_cap) * 100:.1f}% "
                f"(当前仓位 {used_pct * 100:.1f}%, "
                f"买入后 {(mv_after + add_value) / nav * 100:.1f}%)"
            )

    # 单票权重上限: 该票买入后市值不超过净值 × 上限
    if weight_cap is not None:
        pos = positions.get(symbol)
        cur_mv = (
            float(pos.get("qty", 0)) * float(pos.get("avg_cost", 0.0))
            if pos else 0.0
        )
        limit_value = nav * float(weight_cap)
        if cur_mv + add_value > limit_value + 1e-6:
            return (
                f"超出单票权重上限 {float(weight_cap) * 100:.1f}% "
                f"(买入后占净值 {(cur_mv + add_value) / nav * 100:.1f}%)"
            )
    return None


def load_positions(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    """读物化持仓 (无文件时从台账重建)。"""
    p = _root(data_dir, account_id) / "positions.json"
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            return data.get("positions", {})
        except Exception as e:
            logger.warning("paper positions load failed, rebuilding: %s", e)
    return replay_positions(data_dir, account_id=account_id)[0]


def replay_lots(
    fills: list[dict],
    initial_cash: float = 0.0,
) -> tuple[dict[str, list[dict]], list[dict], float]:
    """台账 → (批次表, FIFO 配对, 现金)。成交 + 除权的唯一权威派生实现。

    批次表 lots: {symbol: [{date, src, qty, cost}]}, cost 是含买入费的整批成本 ——
    单位成本由 cost/qty 导出, 所以除权只需折算 qty, 总成本天然守恒。除权只作用于
    **事件日之前**的批次: 除权在盘前生效, 当日新买的股票不参与送转。

    配对 matches: 卖出按 FIFO 逐段消耗批次, 每消耗一段记一条
      {symbol, asset_type, open_date, open_src, close_date, close_src, close_seq,
       qty, cost, proceeds}
    proceeds 已按成交量分摊扣掉卖出费。持仓 / 归因 / 回合统计全部从这一份派生 ——
    以前三处各算一遍, 除权折算只有前两处看得到, 绩效因此按除权前的成本配对价格。
    """
    lots: dict[str, list[dict]] = {}
    matches: list[dict] = []
    cash = float(initial_cash)
    for f in fills:
        symbol = f["symbol"]
        day = f.get("date") or ""
        if f.get("kind") == "corp_action":
            factor = float(f.get("factor") or 1.0)
            if factor <= 0 or factor == 1.0:
                continue
            for lot in lots.get(symbol, ()):
                if day and lot["date"] and lot["date"] >= day:
                    continue
                lot["qty"] = round(lot["qty"] * factor, 6)
            continue
        qty, price, fee = int(f["qty"]), float(f["price"]), float(f.get("fee", 0))
        src = f.get("source") or "manual"
        if f["side"] == "buy":
            cash -= qty * price + fee
            lots.setdefault(symbol, []).append({
                "date": day, "src": src, "qty": float(qty),
                "cost": round(qty * price + fee, 6),
                "asset_type": f.get("asset_type") or "stock",
            })
            continue
        cash += qty * price - fee
        fee_unit = fee / qty if qty else 0.0
        remain = float(qty)
        pool = lots.setdefault(symbol, [])
        for lot in pool:
            if remain <= 1e-9:
                break
            if lot["qty"] <= 1e-9:
                continue
            take = min(lot["qty"], remain)
            cost_unit = lot["cost"] / lot["qty"]
            lot["qty"] = round(lot["qty"] - take, 6)
            lot["cost"] = round(lot["cost"] - take * cost_unit, 6)
            remain = round(remain - take, 6)
            matches.append({
                "symbol": symbol,
                "asset_type": lot.get("asset_type") or f.get("asset_type") or "stock",
                "open_date": lot["date"], "open_src": lot["src"],
                "close_date": day, "close_src": src, "close_seq": f.get("seq"),
                "qty": take,
                "cost": round(take * cost_unit, 6),
                "proceeds": round(take * (price - fee_unit), 6),
            })
        if remain > 1e-9:
            # 台账异常 (卖出量超过持仓): 差额没有成本基础可配, 只留痕不虚构盈亏。
            logger.warning("replay: 卖出超出可配对持仓 %s %s: 请求 %s, 缺口 %s (台账异常)",
                           symbol, day, qty, remain)
    return lots, matches, round(cash, 2)


def replay_positions(
    data_dir: Path,
    fills: list[dict] | None = None,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> tuple[dict[str, dict], float]:
    """由台账重放推导 (持仓, 现金)。纯函数 — 重建与校验的唯一权威实现。

    持仓结构: {symbol: {asset_type, qty, avg_cost, available_qty,
                        lots: [{date: 'YYYY-MM-DD', qty, src}]}}
    qty 可为小数 (除权因子是价格比值折算, 不足一股的零股按分数留在批次里)。
    """
    acc = get_account(data_dir, account_id)
    rows = fills if fills is not None else load_fills(data_dir, account_id)
    lots, _matches, cash = replay_lots(rows, float(acc["initial_cash"]) if acc else 0.0)
    # 资产类型取台账首次出现该 symbol 的行 (与历史口径一致)
    asset_types: dict[str, str] = {}
    for f in rows:
        asset_types.setdefault(f["symbol"], f.get("asset_type") or "stock")
    positions: dict[str, dict] = {}
    for symbol, pool in lots.items():
        qty = round(sum(lot["qty"] for lot in pool), 6)
        cost = round(sum(lot["cost"] for lot in pool), 6)
        positions[symbol] = {
            "asset_type": asset_types.get(symbol, "stock"),
            "qty": qty,
            "avg_cost": round(cost / qty, 6) if qty else 0.0,
            "available_qty": _available_of(pool),
            "lots": [{"date": lot["date"], "qty": lot["qty"], "src": lot["src"]} for lot in pool],
        }
    return positions, cash


def replay_attribution(
    data_dir: Path,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> dict:
    """按成交来源重放台账 — 策略归因的唯一权威实现。

    与 replay_positions 的区别: 后者只按 symbol 聚合, 本函数按 (symbol, source)
    二维聚合, 用来回答「哪个策略带来的持仓 / 已实现盈亏」。

    卖出按 FIFO 消耗批次, 盈亏归属到**被消耗批次**的来源 —— 这样「策略买的票
    被手动卖出」也记回策略账, 而不会记成手动平仓。批次与配对都来自 replay_lots,
    所以除权折算后的数量/成本自动进入归因。买入费用计入成本、卖出费用按成交量
    分摊从收入中扣除, 与 replay_positions 同一口径。

    浮动盈亏不在此处计算 (需要现价), 由前端用行情价换算。
    """
    fills = load_fills(data_dir, account_id)
    lots, matches, _cash = replay_lots(fills)
    stats: dict[str, dict] = {}

    def _st(src: str) -> dict:
        return stats.setdefault(src, {
            "source": src,
            "realized_pnl": 0.0,
            "buy_count": 0,
            "sell_count": 0,
            "buy_qty": 0.0,
            "sell_qty": 0.0,
            "buy_amount": 0.0,
            "sell_amount": 0.0,
            "first_trade_at": None,
            "last_trade_at": None,
        })

    for f in fills:
        if f.get("kind") == "corp_action":
            continue          # 除权不是交易: 不计笔数, 也不更新首末成交时刻
        src = f.get("source") or "manual"
        st = _st(src)
        ts = f.get("ts") or ""
        if st["first_trade_at"] is None or (ts and ts < st["first_trade_at"]):
            st["first_trade_at"] = ts or None
        if ts and (st["last_trade_at"] is None or ts > st["last_trade_at"]):
            st["last_trade_at"] = ts
        qty, price, fee = int(f["qty"]), float(f["price"]), float(f.get("fee", 0))
        if f["side"] == "buy":
            st["buy_count"] += 1
            st["buy_qty"] += qty
            st["buy_amount"] = round(st["buy_amount"] + qty * price + fee, 2)
        else:
            st["sell_count"] += 1
            st["sell_qty"] += qty
            st["sell_amount"] = round(st["sell_amount"] + qty * price - fee, 2)

    # 已实现盈亏按「被消耗批次的来源」记账 (而不是发起卖出的来源)
    for m in matches:
        st = _st(m["open_src"])
        st["realized_pnl"] = round(st["realized_pnl"] + m["proceeds"] - m["cost"], 2)

    sources = []
    for src, st in stats.items():
        held_qty = held_cost = 0.0
        held_symbols = 0
        for pool in lots.values():
            open_lots = [lot for lot in pool if lot["src"] == src and lot["qty"] > 1e-9]
            if not open_lots:
                continue
            held_qty += sum(lot["qty"] for lot in open_lots)
            held_cost += sum(lot["cost"] for lot in open_lots)
            held_symbols += 1
        sources.append({
            **st,
            "realized_pnl": round(st["realized_pnl"], 2),
            "held_qty": round(held_qty, 2),
            "held_cost": round(held_cost, 2),
            "held_symbols": held_symbols,
        })
    # 排序: 有持仓的在前, 其次按成交额 — 归因视图关心"现在还拿着多少"
    sources.sort(key=lambda x: (-x["held_symbols"], -x["held_cost"]))
    return {
        "sources": sources,
        "total_realized": round(sum(s["realized_pnl"] for s in sources), 2),
    }


def _available_of(lots: list[dict], today: str | None = None) -> float:
    """T+1 可卖数量 (股): 早于 today 的批次之和; today 为空则全部可卖 (重建口径)。

    除权已在批次上折算 (且只折算事件日之前的批次), 所以除权当日盘前可卖的就是
    折算后的数量; 折算出的零股保留小数, 由「零股一次性卖出」规则处理。
    """
    sel = lots if today is None else [lot for lot in lots if lot["date"] < today]
    return round(sum(lot["qty"] for lot in sel), 6)


def _append_fill(data_dir: Path, fill: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    p = _fills_path(data_dir, account_id)
    with PAPER_LOCK, p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(fill, ensure_ascii=False) + "\n")


def _materialize(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    """由台账重建 positions.json (每次成交后调用, 保持物化缓存与台账一致)。"""
    positions, _cash = replay_positions(data_dir, account_id=account_id)
    acc = get_account(data_dir, account_id)
    if acc is not None:
        today = cn_today().isoformat()
        for pos in positions.values():
            pos["available_qty"] = _available_of(pos["lots"], today)
    atomic_write_text(
        _root(data_dir, account_id) / "positions.json",
        json.dumps({"updated_at": _now_iso(), "positions": positions}, ensure_ascii=False, indent=2),
    )


def rebuild_positions(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    """修复兜底: 强制由台账重建物化持仓。"""
    with PAPER_LOCK:
        positions, cash = replay_positions(data_dir, account_id=account_id)
        acc = get_account(data_dir, account_id)
        if acc is not None:
            acc["cash"] = cash
            save_account(data_dir, acc, account_id)
        today = cn_today().isoformat()
        for pos in positions.values():
            pos["available_qty"] = _available_of(pos["lots"], today)
        atomic_write_text(
            _root(data_dir, account_id) / "positions.json",
            json.dumps({"updated_at": _now_iso(), "positions": positions}, ensure_ascii=False, indent=2),
        )
        return positions


# ── 撮合 ────────────────────────────────────────────────
def _fill_order(
    data_dir: Path,
    order: dict,
    raw_price: float,
    day: str,
    account_id: str = DEFAULT_ACCOUNT_ID,
    name: str | None = None,
) -> dict | None:
    """按指定 raw 价成交一笔订单 (费用/滑点/资金与持仓校验), 返回 fill 或 None。

    调用方持锁。可达性按 **raw 价**判定 —— 滑点只是本账户的成交价偏移, 不代表行情
    封板: raw 价未触板就是能成交。涨跌停按账户 queue_limit_orders 开关: 关 → expired
    留痕; 开 → 转 next_open 排队次日重试 (计顺延, 超限过期)。资金/可卖不足直接过期。

    涨跌停基准价 = 上一交易日 raw close, 除权日按当日因子折算成交易所参考价 (与
    indicators/pipeline 同口径)。幅度与最小价位算术复用 app.price_limits (ST /
    创业板科创板 ETF 均在其中), 本模块不再自己维护第二套。
    """
    acc = get_account(data_dir, account_id)
    symbol, side, qty = order["symbol"], order["side"], int(order["qty"])
    asset_type = order.get("asset_type", "stock")
    price = apply_slippage(raw_price, side, float(acc["slippage_bps"]))

    # 涨跌停检查 (基准价见 _limit_reference_close; 无上一日数据则跳过 — 新股/数据缺失)
    base = _limit_reference_close(data_dir, symbol, asset_type, day)
    if base is not None:
        up, down = asset_limit_prices(
            symbol, asset_type, base, _date.fromisoformat(day), name=name
        )
        if side == "buy" and raw_price >= up:
            _queue_or_expire(data_dir, order, acc, f"触及涨停 {up} 买不进 (模拟)", account_id)
            return None
        if side == "sell" and raw_price <= down:
            _queue_or_expire(data_dir, order, acc, f"触及跌停 {down} 卖不出 (模拟)", account_id)
            return None
        # 板内成交: 滑点不得把成交价推过当日板
        price = min(price, up) if side == "buy" else max(price, down)

    fee = buy_fee(qty, price, acc["commission_pct"]) if side == "buy" else sell_fee(qty, price, acc["commission_pct"], acc["stamp_tax_pct"])
    gross = qty * price
    if side == "buy":
        if gross + fee > float(acc["cash"]) + 1e-6:
            _expire(data_dir, order, f"资金不足 (需 {gross + fee:.2f}, 可用 {acc['cash']:.2f})", account_id)
            return None
        # 资金占用兜底: 只计入 created_at 早于本单的其他 pending 买入 (先到先得,
        # 早单优先成交, 晚单不占早单的额度) — 防止多张 pending 买入各自通过预检、
        # 成交时逐张扣现金而穿透。
        earlier_cash = sum(
            int(o["qty"]) * float(o["ref_price"] or 0)
            for o in load_orders(data_dir, account_id)
            if o["status"] == "pending" and o["side"] == "buy"
            and o["id"] != order["id"]
            and _order_sort_key(o) < _order_sort_key(order)
        )
        if earlier_cash and gross + fee + earlier_cash > float(acc["cash"]) + 1e-6:
            _expire(data_dir, order, f"资金被更早的待成交买入单占用 (约 {earlier_cash:.0f}, 可用 {acc['cash']:.0f})", account_id)
            return None
    if side == "sell":
        pos = load_positions(data_dir, account_id).get(symbol)
        avail = _available_of(pos["lots"], day) if pos else 0.0
        if pos is None or pos["qty"] < qty - 1e-9 or avail < qty - 1e-9:
            _expire(data_dir, order, f"可卖数量不足 (T+1): 可卖 {avail:g}", account_id)
            return None
        # 超卖防护 (撮合侧兜底): 只计入 created_at 早于本单的其他 pending 卖出 —
        # 先到先得, 早单优先成交; 剩余额度不足则本单拒 (而非成交出负持仓)。
        pending_sell = sum(
            int(o["qty"]) for o in load_orders(data_dir, account_id)
            if o["status"] == "pending" and o["side"] == "sell" and o["symbol"] == symbol
            and o["id"] != order["id"]
            and _order_sort_key(o) < _order_sort_key(order)
        )
        if pending_sell + qty > avail + 1e-9:
            _expire(data_dir, order, f"可卖数量被更早的待成交卖出单占用: 可卖 {avail:g}, 先到 {pending_sell}", account_id)
            return None

    fill = {
        "seq": int(datetime.now().timestamp() * 1000),
        "ts": _now_iso(),
        "date": day,
        "order_id": order["id"],
        "symbol": symbol,
        "asset_type": asset_type,
        "side": side,
        "qty": qty,
        "price": price,
        "fee": fee,
        "kind": "fill",
        # 来源继承自订单 (manual / auto:{rule_id})。归因链路的起点:
        # 少了它, 持仓与已实现盈亏都无法拆到「哪个策略带来的」。
        # 旧台账无此字段, 回放时按 manual 兜底。
        "source": order.get("source") or "manual",
    }
    _append_fill(data_dir, fill, account_id)

    order["status"] = "filled"
    order["filled_at"] = fill["ts"]
    order["fill_price"] = price
    order["fees"] = fee
    save_order(data_dir, order, account_id)

    # 现金按台账重放结果定版 (重放是权威, 避免双写不一致)
    _cash = replay_positions(data_dir, account_id=account_id)[1]
    acc["cash"] = _cash
    save_account(data_dir, acc, account_id)
    _materialize(data_dir, account_id)

    # 成交落告警记录 (监控中心触发历史可见; source=paper 走前端通用渲染)
    try:
        label = "模拟盘" if account_id == DEFAULT_ACCOUNT_ID else f"模拟盘[{acc.get('name') or account_id}]"
        from app.services import alert_store
        alert_store.append_many(data_dir, [{
            "ts": int(datetime.now().timestamp() * 1000),
            "source": "paper",
            "type": "paper_fill",
            "rule_id": None,
            "rule_name": "",
            "strategy_id": None,
            "symbol": symbol,
            "name": "",
            "message": f"{label}{'买入' if side == 'buy' else '卖出'} {symbol} {qty}股 @ {price:.3f} (费 {fee:.2f})",
            "price": price,
            "change_pct": 0,
            "signals": [],
            "severity": "info",
            "conditions": [],
            "logic": "and",
        }])
    except Exception as e:
        logger.warning("paper 成交告警落盘失败 (不影响账务): %s", e)

    logger.info("paper filled: %s %s %d x %.3f fee %.2f", side, symbol, qty, price, fee)
    return fill


def _expire(data_dir: Path, order: dict, reason: str, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    """拒单置 expired 并立即落盘 (撮合拒绝必须留痕, 不允许静默丢弃)。"""
    order["status"] = "expired"
    order["reason"] = reason
    save_order(data_dir, order, account_id)


def _queue_or_expire(data_dir: Path, order: dict, acc: dict, reason: str, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    """涨跌停拒单处理: 排队开启 → 转 next_open 次日重试 (计顺延, 超限过期); 关 → 过期。"""
    postponed = int(order.get("postponed", 0))
    if acc.get("queue_limit_orders"):
        if _postpone_allowed(postponed + 1):
            order["order_type"] = "next_open"
            order["postponed"] = postponed + 1
            order["reason"] = f"{reason}; 排队次日重试 ({postponed + 1}/{MAX_POSTPONE_DAYS})"
            # 排队时刻: 盘中排队的单不能按当日 (排队之前已打印的) 开盘价成交
            order["queued_at"] = _now_iso()
            save_order(data_dir, order, account_id)
            return
        reason = f"{reason}; 排队 {MAX_POSTPONE_DAYS} 日未成交, 过期"
    _expire(data_dir, order, reason, account_id)


def day_fill_events(data_dir: Path, day: str, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    """指定交易日的全部成交 → 推送事件。

    盘后结算路径 (daily_pipeline paper_settle) 用它把结算成交留痕到告警中心;
    盘中即时成交走 evaluate_intraday 返回值, 不经过本函数。
    """
    return [
        _fill_event(f, account_id)
        for f in load_fills(data_dir, account_id)
        if f.get("date") == day and f.get("kind") == "fill"
    ]


def _fill_event(fill: dict, account_id: str) -> dict:
    """成交台账行 → 推送事件。字段对齐监控告警 (AlertEvent), 供 SSE toast /
    语音播报 / 系统通知 / alert_store 留痕 / Webhook 复用, 前端无需新管道。
    ts 用 fill.seq (毫秒), 与 alert_store 的 ts 口径一致; 文案用中文可读措辞。
    """
    side_label = "买入" if fill["side"] == "buy" else "卖出"
    return {
        "source": "paper",
        "type": "fill",
        "severity": "info",
        "ts": int(fill["seq"]),
        "symbol": fill["symbol"],
        "side": fill["side"],
        "qty": fill["qty"],
        "price": fill["price"],
        "account_id": account_id,
        "message": f"模拟盘{side_label}成交 {fill['qty']}股 @ {fill['price']:.2f}",
    }


def evaluate_intraday(
    data_dir: Path,
    snapshot: dict[str, float],
    account_id: str = DEFAULT_ACCOUNT_ID,
    *,
    name_map: dict[str, str] | None = None,
) -> list[dict]:
    """盘中钩子: 用最新快照价撮合 pending 即时单。返回本次成交事件列表。

    snapshot: {symbol: raw 最新价} (来自 enriched raw_close)。仅处理 market 单;
    调用方 (quote_service) 已保证交易时段。ETF 不在快照内自然顺延。
    事件由调用方广播 (SSE/语音/系统通知/留痕/Webhook), 域模块只产出不投递。
    除权日先入账再撮合 (与 settle_day 同序) —— 折算完成后当日可卖数量与涨跌停
    基准才是除权后的口径。name_map 供涨跌停幅度判定 (ST / 创业板科创板 ETF)。
    """
    events: list[dict] = []
    with PAPER_LOCK:
        today = cn_today().isoformat()
        pending_orders = sorted(
            (o for o in load_orders(data_dir, account_id) if o["status"] == "pending" and o["order_type"] == "market"),
            key=_order_sort_key,
        )
        # 只在有单要撮合时做除权入账 —— 除权影响的正是可卖数量与涨跌停基准;
        # 无单时不每轮轮询都重放台账。幂等由 _apply_corp_actions 自己保证。
        if pending_orders and _apply_corp_actions(data_dir, today, account_id):
            _materialize(data_dir, account_id)
        for order in pending_orders:
            price = snapshot.get(order["symbol"])
            if price is None or price <= 0:
                continue  # 无快照顺延
            fill = _fill_order(data_dir, order, float(price), today, account_id,
                               name=(name_map or {}).get(order["symbol"]))
            if fill is not None:
                events.append(_fill_event(fill, account_id))
    return events


# ── 行情读取 (结算用, 与账户无关) ───────────────────────
def _daily_path(data_dir: Path, asset_type: str) -> Path:
    sub = "kline_etf_daily" if asset_type == "etf" else "kline_daily"
    return data_dir / sub


def read_daily_bar(data_dir: Path, symbol: str, asset_type: str, day: str) -> dict | None:
    """读某标的某日 raw OHLC (不复权); 无数据返回 None。"""
    base = _daily_path(data_dir, asset_type)
    if not base.exists():
        return None
    try:
        df = (
            pl.scan_parquet((base / "**" / "*.parquet").as_posix())
            .filter((pl.col("symbol") == symbol) & (pl.col("date") == _date.fromisoformat(day)))
            .select(["open", "close"])
            .collect()
        )
    except Exception as e:
        logger.warning("paper read_daily_bar failed %s %s: %s", symbol, day, e)
        return None
    if df.is_empty():
        return None
    return {"open": float(df["open"][0]), "close": float(df["close"][0])}


def _prev_close(data_dir: Path, symbol: str, asset_type: str, day: str) -> float | None:
    """day 之前最近一个有数据的交易日 raw close (严格 < day, 按日期排序取最后)。"""
    base = _daily_path(data_dir, asset_type)
    if not base.exists():
        return None
    try:
        df = (
            pl.scan_parquet((base / "**" / "*.parquet").as_posix())
            .filter((pl.col("symbol") == symbol) & (pl.col("date") < _date.fromisoformat(day)))
            .sort("date")
            .select(pl.col("close").last())
            .collect()
        )
    except Exception as e:
        logger.warning("paper prev_close failed %s: %s", symbol, e)
        return None
    if df.is_empty() or df["close"][0] is None:
        return None
    return float(df["close"][0])


def _index_close(data_dir: Path, day: str) -> float | None:
    """沪深300 当日 raw close (基准对比); 指数日K缺失返回 None。"""
    base = data_dir / "kline_index_daily"
    if not base.exists():
        return None
    try:
        df = (
            pl.scan_parquet((base / "**" / "*.parquet").as_posix())
            .filter((pl.col("symbol") == "000300.SH") & (pl.col("date") == _date.fromisoformat(day)))
            .select("close")
            .collect()
        )
    except Exception as e:
        logger.warning("paper index close read failed: %s", e)
        return None
    if df.is_empty() or df["close"][0] is None:
        return None
    return float(df["close"][0])


def _factors_on(data_dir: Path, asset_type: str, day: str) -> dict[str, float]:
    """day 的全部除权事件 {symbol: ex_factor} (一趟扫描; 无因子文件/无事件返回 {})。

    按资产类型分文件: 股票 adj_factor, 场内基金 adj_factor_etf。
    """
    sub = "adj_factor_etf" if asset_type == "etf" else "adj_factor"
    p = data_dir / sub / "all.parquet"
    if not p.exists():
        return {}
    try:
        df = (
            pl.scan_parquet(p.as_posix())
            .filter(
                (pl.col("trade_date") == _date.fromisoformat(day))
                & pl.col("ex_factor").is_not_null() & (pl.col("ex_factor") > 0)
            )
            .select(["symbol", "ex_factor"])
            .collect()
        )
    except Exception as e:
        logger.warning("paper factor read failed %s %s: %s", asset_type, day, e)
        return {}
    if df.is_empty():
        return {}
    return dict(zip(df["symbol"].to_list(), df["ex_factor"].to_list(), strict=False))


def _limit_reference_close(data_dir: Path, symbol: str, asset_type: str, day: str) -> float | None:
    """day 的涨跌停基准价 (交易所参考价)。

    正常日 = 上一交易日 raw close; 除权日按当日 ex_factor 折算 (prev / factor),
    与 indicators/pipeline 的「除权日用前复权昨收」同口径 —— 直接用未复权昨收会把
    除权缺口当成跌停, 把当日正常价判成触板。无上一日数据返回 None。
    """
    prev = _prev_close(data_dir, symbol, asset_type, day)
    if prev is None:
        return None
    factor = _factors_on(data_dir, asset_type, day).get(symbol)
    if not factor or factor == 1.0:
        return prev
    return prev / factor


# ── 除权折算 ────────────────────────────────────────────
def _apply_corp_actions(data_dir: Path, day: str, account_id: str = DEFAULT_ACCOUNT_ID) -> int:
    """把 day 的除权事件写进台账 (corp_action 行), 返回本次新增条数。

    必须在撮合之前调用: 除权在盘前生效, 当日批次既不参与折算, 当日新单也要按
    折算后的可卖数量与参考价撮合。幂等 —— 同一 (symbol, day) 只记一条, 重跑/盘中
    多次轮询都不会二次折算; 因子表事后被修订时不追改历史行, 只告警 (修复一律
    以追加冲正表达, 见模块 docstring 的账务正确性原则)。
    """
    positions = load_positions(data_dir, account_id)
    held = {symbol: pos for symbol, pos in positions.items() if pos["qty"] > 0}
    if not held:
        return 0
    recorded: dict[tuple[str, str], float] = {}
    for f in load_fills(data_dir, account_id):
        if f.get("kind") == "corp_action":
            recorded[(f["symbol"], f["date"])] = float(f["factor"])
    added = 0
    by_type: dict[str, list[str]] = {}
    for symbol, pos in held.items():
        by_type.setdefault(pos.get("asset_type") or "stock", []).append(symbol)
    for asset_type, symbols in by_type.items():
        factors = _factors_on(data_dir, asset_type, day)
        for symbol in symbols:
            factor = factors.get(symbol)
            if not factor or factor == 1.0:
                continue
            pos = held[symbol]
            prev_factor = recorded.get((symbol, day))
            if prev_factor is not None:
                if abs(prev_factor - factor) > 1e-9:
                    logger.warning(
                        "paper 除权因子已修订 %s %s: 台账按 %.6f 折算, 当前因子表 %.6f "
                        "—— 差额需追加冲正行修正, 不自动追改",
                        symbol, day, prev_factor, factor,
                    )
                continue
            _append_fill(data_dir, {
                "seq": int(datetime.now().timestamp() * 1000),
                "ts": _now_iso(),
                "date": day,
                "order_id": None,
                "symbol": symbol,
                "asset_type": asset_type,
                "side": "corp_action",
                "kind": "corp_action",
                "factor": factor,
                "qty_before": pos["qty"],
                "cost_before": pos["avg_cost"],
            }, account_id)
            added += 1
    return added


def _postpone_allowed(attempt: int) -> bool:
    """第 attempt 次 (1 起) 「顺延/排队而未成交」是否仍在容忍范围内。

    停牌顺延与涨跌停排队共用同一个上限口径, 两处判定都走这里 —— 此前一处自增后
    判 `> MAX`, 另一处自增前判 `< MAX`, 靠巧合才等价。
    """
    return attempt <= MAX_POSTPONE_DAYS


# ── 盘后结算 ────────────────────────────────────────────
def _placed_after_price_time(order: dict, day: str) -> bool:
    """订单 (盘中排队的按排队时刻) 是否晚于 day 结算价格的打印时刻。

    next_open 用当日 09:30 开盘价, close / market 兜底用当日 15:00 收盘价。下单或
    排队发生在同一交易日该时刻之后, 说明这个价格在下单时已经打印过, 按它成交等于
    拿已知价格回填 (次日开盘单的口径是次一交易日 raw_open), 应留到下一交易日结算。
    更早日期的订单不受影响; 时间戳缺失或无法解析时不拦截 (沿用原行为)。
    """
    stamp = order.get("queued_at") or order.get("created_at")
    try:
        ts = datetime.fromisoformat(str(stamp))
    except (TypeError, ValueError):
        return False
    if ts.tzinfo is not None:
        ts = ts.astimezone(CN_TZ)
    if ts.date().isoformat() != day:
        return False
    price_time = _SESSION_OPEN if order.get("order_type") == "next_open" else _SESSION_CLOSE
    return ts.time() >= price_time


def settle_day(
    data_dir: Path,
    day: str,
    account_id: str = DEFAULT_ACCOUNT_ID,
    *,
    name_map: dict[str, str] | None = None,
) -> dict:
    """盘后管道阶段: 除权入账 → 撮合顺延单 → 定版净值。幂等 (重跑同日安全)。

    撮合顺序: close 单用 raw_close; next_open 单用 raw_open; 仍 pending 的
    market 单 (当日无快照) 也按 raw_close 兜底成交 — 避免停牌外无限顺延。
    当日开盘后才下 / 排队的 next_open 单, 以及收盘后才下的 close / market 单,
    当日不成交也不计顺延, 留到下一交易日 (见 _placed_after_price_time)。

    name_map: {symbol: 名称} (涨跌停幅度要用 —— ST 股与创业板/科创板 ETF 的幅度
    与主板不同)。调用方 (盘后管道) 有 instruments 维表就传, 缺名称时按主板 10% 判。
    """
    summary = {"filled": 0, "expired": 0, "corp_actions": 0, "nav": None, "account": account_id}
    with PAPER_LOCK:
        acc = get_account(data_dir, account_id)
        if acc is None:
            return summary
        # 除权先入账 (盘前生效), 再撮合 —— 台账顺序即口径: 当日新批次不被折算,
        # 当日卖出按折算后的可卖数量校验, 涨跌停基准按因子折算。
        summary["corp_actions"] = _apply_corp_actions(data_dir, day, account_id)
        if summary["corp_actions"]:
            _materialize(data_dir, account_id)

        pending_orders = sorted(
            (o for o in load_orders(data_dir, account_id) if o["status"] == "pending"),
            key=_order_sort_key,
        )
        for order in pending_orders:
            bar = read_daily_bar(data_dir, order["symbol"], order.get("asset_type", "stock"), day)
            if bar is None:
                order["postponed"] = int(order.get("postponed", 0)) + 1
                if not _postpone_allowed(order["postponed"]):
                    order["status"] = "expired"
                    order["reason"] = f"连续 {MAX_POSTPONE_DAYS} 个交易日无行情, 自动过期"
                    save_order(data_dir, order, account_id)
                    summary["expired"] += 1
                else:
                    save_order(data_dir, order, account_id)
                continue
            if _placed_after_price_time(order, day):
                continue  # 下单/排队时该价已打印, 留到下一交易日 (不计顺延)
            # next_open 用开盘价; close 与 market 兜底用收盘价
            raw = bar["open"] if order["order_type"] == "next_open" else bar["close"]
            before = order["status"]
            if _fill_order(data_dir, order, raw, day, account_id,
                           name=(name_map or {}).get(order["symbol"])) is not None:
                summary["filled"] += 1
            elif before == "pending" and order["status"] == "expired":
                summary["expired"] += 1

        # 定版净值 (幂等: 重写当日行); 顺带记录沪深300 收盘作基准对比
        nav = daily_nav(data_dir, day, account_id=account_id)
        if nav is not None:
            benchmark = _index_close(data_dir, day)
            if benchmark is not None:
                nav["benchmark_close"] = benchmark
            _write_nav_line(data_dir, day, nav, account_id)
            summary["nav"] = nav
    return summary


# ── 净值与总览 ──────────────────────────────────────────
def latest_prices_from_daily(data_dir: Path, day: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, float]:
    """day 收盘后各持仓标的 raw_close (定版用)。"""
    out: dict[str, float] = {}
    for symbol, pos in load_positions(data_dir, account_id).items():
        if pos["qty"] <= 0:
            continue
        bar = read_daily_bar(data_dir, symbol, pos.get("asset_type", "stock"), day)
        if bar is not None:
            out[symbol] = bar["close"]
    return out


def daily_nav(
    data_dir: Path,
    day: str,
    price_map: dict[str, float] | None = None,
    account_id: str = DEFAULT_ACCOUNT_ID,
) -> dict | None:
    """某日定版净值 {date, cash, mv, nav}; 缺行情的持仓按成本计 (保守, 留痕调用方)。"""
    acc = get_account(data_dir, account_id)
    if acc is None:
        return None
    positions = load_positions(data_dir, account_id)
    prices = price_map if price_map is not None else latest_prices_from_daily(data_dir, day, account_id)
    missing = [s for s, p in positions.items() if p["qty"] > 0 and s not in prices]
    if missing:
        logger.warning("paper nav %s: 缺行情按成本计: %s", day, missing)
    mv = 0.0
    for symbol, pos in positions.items():
        if pos["qty"] <= 0:
            continue
        price = prices.get(symbol, pos["avg_cost"])
        mv += pos["qty"] * price
    return {"date": day, "cash": round(float(acc["cash"]), 2), "mv": round(mv, 2), "nav": round(float(acc["cash"]) + mv, 2)}


def _write_nav_line(data_dir: Path, day: str, nav: dict, account_id: str = DEFAULT_ACCOUNT_ID) -> None:
    p = _nav_dir(data_dir, account_id) / "daily.jsonl"
    lines: list[str] = []
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            try:
                if json.loads(line).get("date") != day:
                    lines.append(line)
            except Exception:
                continue
    lines.append(json.dumps(nav, ensure_ascii=False))
    lines.sort(key=lambda s: json.loads(s)["date"])
    atomic_write_text(p, "\n".join(lines) + "\n")


def load_nav(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    p = _nav_dir(data_dir, account_id) / "daily.jsonl"
    if not p.exists():
        return []
    out: list[dict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def overview(data_dir: Path, price_map: dict[str, float] | None = None, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    """总览 (盘中实时估算): 现金 + 持仓市值 + 估算净值。"""
    acc = get_account(data_dir, account_id)
    if acc is None:
        return {"initialized": False}
    positions = load_positions(data_dir, account_id)
    today = cn_today().isoformat()
    holdings = []
    mv = 0.0
    for symbol, pos in sorted(positions.items()):
        if pos["qty"] <= 0:
            continue
        price = (price_map or {}).get(symbol)
        last = price if price is not None else pos["avg_cost"]
        mv += pos["qty"] * last
        holdings.append({
            "symbol": symbol,
            "asset_type": pos.get("asset_type", "stock"),
            "qty": pos["qty"],
            "avg_cost": pos["avg_cost"],
            "last_price": round(last, 4),
            "market_value": round(pos["qty"] * last, 2),
            "pnl": round(pos["qty"] * (last - pos["avg_cost"]), 2),
            "pnl_pct": round((last / pos["avg_cost"] - 1) * 100, 2) if pos["avg_cost"] else 0.0,
            "available_qty": _available_of(pos["lots"], today),
        })
    return {
        "initialized": True,
        "account_id": account_id,
        "account_name": acc.get("name", account_id),
        "status": acc["status"],
        "queue_limit_orders": bool(acc.get("queue_limit_orders")),
        # 仓位约束: 供设置弹窗回填与展示当前仓位水平。
        # use .get 兼容本次新增前创建的旧账户文件(无这两个键时按不限制处理)。
        "max_exposure_pct": acc.get("max_exposure_pct"),
        "max_position_weight": acc.get("max_position_weight"),
        "position_usage_pct": (
            round(mv / (float(acc["cash"]) + mv) * 100, 2)
            if (float(acc["cash"]) + mv) > 0 else 0.0
        ),
        "cash": round(float(acc["cash"]), 2),
        "market_value": round(mv, 2),
        "total": round(float(acc["cash"]) + mv, 2),
        "total_pnl": round(float(acc["cash"]) + mv - float(acc["initial_cash"]), 2),
        "initial_cash": float(acc["initial_cash"]),
        "estimating": price_map is not None,
        "holdings": holdings,
    }


# ── 回合统计 (FIFO) ─────────────────────────────────────
def round_trips(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    """FIFO 配对的开→平回合: 入场价含买入费, 出场净额扣卖出费。

    配对来自 replay_lots —— 与持仓/归因同一份批次 (含除权折算), 所以除权后的
    卖出按折算数量与摊薄成本配对, 不会把送转出的股数算成亏损。
    """
    fills = load_fills(data_dir, account_id)
    _lots, matches, _cash = replay_lots(fills)
    return [
        {
            "symbol": m["symbol"],
            "open_date": m["open_date"],
            "close_date": m["close_date"],
            "qty": round(m["qty"], 6),
            "pnl": round(m["proceeds"] - m["cost"], 2),
            "pnl_pct": round((m["proceeds"] - m["cost"]) / m["cost"] * 100, 2) if m["cost"] else 0.0,
            "holding_days": _days_between(m["open_date"], m["close_date"]),
        }
        for m in matches
    ]


def max_drawdown(nav_values: list[float]) -> float | None:
    """定版净值序列的最大回撤 (0~1 小数; 空序列返回 None, 单点为 0)。"""
    peak: float | None = None
    mdd: float | None = None
    for v in nav_values:
        if v <= 0:
            continue
        peak = v if peak is None else max(peak, v)
        dd = (peak - v) / peak
        mdd = dd if mdd is None else max(mdd, dd)
    return mdd


def stats(data_dir: Path, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    """回合汇总: 胜率/盈亏比/平均持有天数/已实现盈亏/最大回撤(定版净值)。

    胜率分母是全部回合 (含持平), 分子只算盈利回合 —— 盈亏恰好为 0 的回合既不算
    胜也不算负 (旧口径把 0 计进亏损, 会把打平的票报成输)。
    """
    rounds = round_trips(data_dir, account_id)
    wins = [r for r in rounds if r["pnl"] > 0]
    losses = [r for r in rounds if r["pnl"] < 0]
    avg_win = sum(r["pnl"] for r in wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(r["pnl"] for r in losses) / len(losses)) if losses else 0.0
    mdd = max_drawdown([n["nav"] for n in load_nav(data_dir, account_id)])
    return {
        "rounds": len(rounds),
        "win_rate": round(len(wins) / len(rounds) * 100, 2) if rounds else 0.0,
        "profit_loss_ratio": round(avg_win / avg_loss, 2) if avg_loss else None,
        "avg_holding_days": round(sum(r["holding_days"] for r in rounds) / len(rounds), 1) if rounds else 0.0,
        "realized_pnl": round(sum(r["pnl"] for r in rounds), 2),
        "max_drawdown": round(mdd * 100, 2) if mdd is not None else None,
    }


def _days_between(d1: str, d2: str) -> int:
    try:
        return (_date.fromisoformat(d2) - _date.fromisoformat(d1)).days
    except Exception:
        return 0
