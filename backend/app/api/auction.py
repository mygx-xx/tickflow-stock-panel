"""集合竞价 API — 个股竞价序列 + 全市场竞价榜 + 状态/手动扫描。

数据集 auction 走标准路由, 本层只做参数解析与降级表达, 不含取数逻辑:
  GET  /api/auction/series   单股逐点(个股竞价图)
  GET  /api/auction/board    某日某段末点榜(全市场竞价榜)
  GET  /api/auction/status   能力/路由/已落盘日期/最近扫描
  POST /api/auction/sweep    手动扫一轮(全市场约 15~30s, 与 depth 的手动修正同模式)

state 口径沿用复盘页的既有约定(ok / no_data / source_unavailable), 前端据此
区分「数据源没配」与「今天确实没有竞价」—— 两者在 UI 上必须是不同的提示。
"""
from __future__ import annotations

from datetime import date as date_cls

from fastapi import APIRouter, HTTPException, Query, Request

from app.market_time import cn_today
from app.services.auction_service import AuctionService

router = APIRouter(prefix="/api/auction", tags=["auction"])

_NO_CAP = "无竞价数据源: 请在「数据源配置 → 集合竞价」选择提供该数据集的源"


def _svc(request: Request) -> AuctionService:
    svc = getattr(request.app.state, "auction_service", None)
    if svc is None:  # pragma: no cover — lifespan 必定挂载
        raise HTTPException(503, "竞价服务未就绪")
    return svc


def _parse_date(value: str | None) -> date_cls | None:
    if not value:
        return None
    try:
        return date_cls.fromisoformat(value)
    except ValueError:
        raise HTTPException(400, f"date 格式应为 YYYY-MM-DD, 收到: {value}") from None


def _latest(svc: AuctionService, fallback: date_cls) -> date_cls:
    """缺省日期: 最近一个已落盘日, 没有则今天(可能尚无数据, 由 state 表达)。"""
    dates = svc.available_dates(last_n=1)
    return date_cls.fromisoformat(dates[0]) if dates else fallback


@router.get("/series")
def get_series(
    request: Request,
    symbol: str = Query(..., min_length=1, description="面板 symbol, 如 600519.SH"),
    date: str | None = Query(default=None, description="目标日 YYYY-MM-DD, 缺省取最近有数据日"),
):
    """单股竞价逐点序列(开盘段 + 收盘段, 按 datetime 升序)。

    空 points 是正常状态(休市/该标的无竞价记录/超出约 12 个月回溯窗口),
    state=no_data + message 说明原因, 前端不得画成 0。
    """
    svc = _svc(request)
    target = _parse_date(date) or _latest(svc, cn_today())
    out = svc.get_series(symbol.strip(), target)
    if out["points"]:
        out["state"] = "ok"
    elif not svc.available():
        # 「没配源」与「这天确实没有竞价」是两种提示, 不能都落到无竞价记录那句
        out["state"] = "source_unavailable"
        out["msg"] = _NO_CAP
    else:
        out["state"] = "no_data"
    return out


@router.get("/board")
def get_board(
    request: Request,
    date: str | None = Query(default=None, description="目标日 YYYY-MM-DD, 缺省取最近有数据日"),
    segment: str = Query(default="open", pattern="^(open|close)$"),
    sort_by: str = Query(default="matched_amount", description="matched_amount/unmatched_amount/matched_volume/change_ratio"),
    limit: int = Query(default=100, ge=1, le=2000),
    symbols: str | None = Query(default=None, description="逗号分隔:只看这些标的"),
):
    """全市场竞价榜(每标的该段末点): 虚拟价/匹配量/未匹配量与方向/匹配额。

    量单位=手, 额单位=元(price × volume × 100); auction_change_ratio 为**小数制**。
    """
    svc = _svc(request)
    target = _parse_date(date) or _latest(svc, cn_today())
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()] if symbols else None
    out = svc.board(target, segment=segment, sort_by=sort_by, limit=limit, symbols=sym_list)
    if not svc.available():
        out["state"] = "source_unavailable"
        out["message"] = _NO_CAP
    else:
        out["state"] = "ok" if out["items"] else "no_data"
        out["message"] = "" if out["items"] else f"{out['trade_date']} 无竞价落盘数据(未扫描/休市/超出回溯窗口)"
    return out


@router.get("/status")
def get_status(request: Request):
    """竞价能力与落盘状态(前端能力门控 + 「待采集任务启用」占位判定)。"""
    svc = _svc(request)
    st = svc.status()
    st["state"] = "ok" if st["usable"] else "source_unavailable"
    st["message"] = "" if st["usable"] else _NO_CAP
    return st


@router.post("/sweep")
def run_sweep(
    request: Request,
    date: str | None = Query(default=None, description="扫描目标日, 缺省今天"),
    persist: bool = Query(default=True),
):
    """手动触发一轮全市场竞价扫描(同步执行, 约 15~30s)。

    定时任务在 09:26/15:01; 这里用于补扫历史日(回溯窗口内)或排障。
    """
    svc = _svc(request)
    target = _parse_date(date) or cn_today()
    stats = svc.sweep(target, persist=persist)
    return {"state": "ok" if stats["ok"] else "no_data", **stats}
