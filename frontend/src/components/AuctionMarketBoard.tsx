import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Radar, RefreshCw } from 'lucide-react'
import { api, type AuctionBoardItem, type AuctionSegment } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { fmtBigNum, fmtPct, fmtPrice, priceColorClass } from '@/lib/format'
import { boardTag } from '@/components/stock-table/primitives'
import { toNavItems, type NavItem } from '@/lib/listNav'
import { toast } from '@/components/Toast'

interface Props {
  onOpenStock: (symbol: string, name?: string, navList?: NavItem[]) => void
}

const SORTS: { key: string; label: string; title: string }[] = [
  { key: 'matched_amount', label: '匹配额', title: '竞价撮合成交金额 (元) = 虚拟价 × 匹配量 × 100' },
  { key: 'unmatched_amount', label: '未匹配额', title: '竞价窗口结束时仍挂单的金额 (元), 方向看「未匹配」列' },
  { key: 'matched_volume', label: '匹配量', title: '竞价撮合成交量 (手)' },
  { key: 'change_ratio', label: '竞价涨幅', title: '末点虚拟价相对该竞价日昨收的涨跌幅' },
]

const SEGMENTS: { key: AuctionSegment; label: string }[] = [
  { key: 'open', label: '开盘' },
  { key: 'close', label: '收盘' },
]

const TOP_LIMIT = 50

/**
 * 全市场竞价榜 — 每个标的该竞价段的**末点**读数 (09:25 / 15:00 撮合终态)。
 *
 * 数据来自 auction 数据集的按日落盘分区: 定时任务 09:26 / 15:01 各扫一轮,
 * 没有数据时可以用「立即补扫」手动跑一轮 (全市场约 15~30s)。
 * 只含股票标的池 —— 指数的「手」无意义, ETF 走个股竞价图。
 */
export function AuctionMarketBoard({ onOpenStock }: Props) {
  const qc = useQueryClient()
  const [segment, setSegment] = useState<AuctionSegment>('open')
  const [sortBy, setSortBy] = useState('matched_amount')

  const status = useQuery({ queryKey: QK.auctionStatus, queryFn: api.auctionStatus, staleTime: 60_000 })
  const date = status.data?.dates?.[0] ?? ''
  const board = useQuery({
    queryKey: QK.auctionBoard(date, segment, sortBy, TOP_LIMIT),
    queryFn: () => api.auctionBoard({ date: date || undefined, segment, sortBy, limit: TOP_LIMIT }),
    enabled: !!status.data?.usable,
    staleTime: 60_000,
  })

  const navItems = useMemo(
    () => toNavItems((board.data?.items ?? []).map(i => ({ symbol: i.symbol, name: i.name }))),
    [board.data],
  )

  const sweep = useMutation({
    mutationFn: () => api.auctionSweep(),
    onSuccess: (stats) => {
      void qc.invalidateQueries({ queryKey: ['auction-board'] })
      void qc.invalidateQueries({ queryKey: ['auction-series'] })
      void qc.invalidateQueries({ queryKey: QK.auctionStatus })
      if (stats.state === 'ok') {
        toast(`竞价扫描完成: ${stats.symbols ?? 0} 只 / ${stats.rows ?? 0} 行, ${(stats.elapsed_ms ?? 0) / 1000}s`, 'success')
      } else {
        toast(stats.msg || '本轮未取到竞价数据', 'error')
      }
    },
    onError: (e: Error) => toast(`补扫失败: ${e.message}`, 'error'),
  })

  const items = board.data?.items ?? []
  const sweeping = sweep.isPending

  return (
    <div className="rounded-card border border-border bg-surface/80">
      <div className="flex flex-wrap items-center gap-2 px-4 py-3">
        <span className="grid h-8 w-8 shrink-0 place-items-center rounded bg-violet-500/15 text-violet-400 ring-1 ring-violet-500/20">
          <Radar className="h-4 w-4" />
        </span>
        <span className="leading-tight">
          <span className="flex items-center gap-2 text-[13px] font-semibold text-foreground">全市场竞价榜</span>
          <span className="mt-0.5 block text-[10px] text-muted">
            {status.isFetching ? '正在读取落盘状态…' : (
              date ? `${date} · 共 ${board.data?.total ?? 0} 只入榜口径` : '尚无竞价落盘分区 (定时任务 09:26 / 15:01)'
            )}
          </span>
        </span>
        <div className="ml-auto flex items-center gap-2">
          <div className="inline-flex items-center rounded border border-border/60 bg-base/60 p-0.5" role="tablist" aria-label="竞价段">
            {SEGMENTS.map(s => (
              <button
                key={s.key}
                type="button"
                role="tab"
                aria-selected={segment === s.key}
                onClick={() => setSegment(s.key)}
                className={`h-6 rounded px-2 text-[11px] transition-colors ${
                  segment === s.key ? 'bg-accent/20 text-accent font-medium' : 'text-muted hover:text-secondary'
                }`}
              >
                {s.label}
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={() => sweep.mutate()}
            disabled={sweeping || !status.data?.usable}
            className="inline-flex h-7 items-center gap-1.5 rounded-btn border border-border bg-elevated px-2.5 text-[11px] text-secondary transition-colors hover:text-foreground disabled:opacity-50"
            title="立即扫描一轮全市场竞价 (同步执行, 约 15~30 秒)"
          >
            <RefreshCw className={`h-3 w-3 ${sweeping ? 'animate-spin' : ''}`} />
            {sweeping ? '扫描中…' : '立即补扫'}
          </button>
        </div>
      </div>

      {status.data && !status.data.usable ? (
        <div className="border-t border-border/60 px-4 py-4">
          <p className="text-[11px] leading-relaxed text-muted">{status.data.message}</p>
          <Link to="/settings?tab=data-sources" className="mt-2 inline-block text-[11px] text-accent hover:underline">
            前往配置数据源
          </Link>
        </div>
      ) : board.isLoading ? (
        <div className="flex flex-col gap-2 border-t border-border/60 px-4 py-4">
          {[0, 1, 2].map(i => <span key={i} className="h-3 animate-pulse rounded-full bg-elevated/70" />)}
        </div>
      ) : board.isError ? (
        <p className="border-t border-border/60 px-4 py-4 text-[11px] text-danger">
          {board.error instanceof Error ? board.error.message : '竞价榜读取失败'}
        </p>
      ) : items.length === 0 ? (
        <p className="border-t border-border/60 px-4 py-4 text-[11px] text-muted">
          {board.data?.message || '当日暂无竞价数据'}
          {!sweeping && <span className="ml-1 text-muted/70">— 可点右上角「立即补扫」拉一轮</span>}
        </p>
      ) : (
        <>
          <div className="flex items-center gap-2 border-t border-border/60 bg-elevated/50 px-4 py-1.5">
            {SORTS.map(s => (
              <button
                key={s.key}
                type="button"
                onClick={() => setSortBy(s.key)}
                title={s.title}
                className={`rounded px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wider transition-colors ${
                  sortBy === s.key ? 'bg-accent/15 text-accent' : 'text-muted/70 hover:text-secondary'
                }`}
              >
                {s.label}
              </button>
            ))}
            <span className="ml-auto text-[9px] uppercase tracking-wider text-muted/70">前 {items.length} 只</span>
          </div>
          <BoardRows items={items} navItems={navItems} onOpenStock={onOpenStock} />
        </>
      )}
    </div>
  )
}

function BoardRows({ items, navItems, onOpenStock }: {
  items: AuctionBoardItem[]
  navItems: NavItem[]
  onOpenStock: (symbol: string, name?: string, navList?: NavItem[]) => void
}) {
  return (
    <div>
      <div className="flex items-center gap-2 border-t border-border/60 bg-elevated/50 px-4 py-1.5 text-[9px] font-medium uppercase tracking-wider text-muted/70">
        <span className="min-w-0 flex-1">股票</span>
        <span className="w-16 shrink-0 text-right">虚拟价</span>
        <span className="w-14 shrink-0 text-right">竞价涨幅</span>
        <span className="w-20 shrink-0 text-right">匹配额</span>
        <span className="w-20 shrink-0 text-right">未匹配</span>
      </div>
      {items.map((i, idx) => {
        const board = boardTag(i.symbol)
        const side = (i.unmatched_volume ?? 0) > 0 ? i.unmatched_side : null
        return (
          <button
            key={i.symbol}
            type="button"
            onClick={() => onOpenStock(i.symbol, i.name ?? undefined, navItems)}
            className="flex w-full items-center gap-2 border-t border-border/30 px-4 py-2 text-left text-[11px] transition-colors hover:bg-accent/[0.05]"
            title={`查看 ${i.name ?? i.symbol} 竞价明细`}
          >
            <span className="w-4 shrink-0 text-right font-mono text-[9px] text-muted/60">{idx + 1}</span>
            <span className="flex min-w-0 flex-1 items-center gap-1.5">
              <span className="truncate text-foreground">{i.name ?? i.symbol}</span>
              <span className="shrink-0 font-mono text-[9px] text-muted">{i.symbol}</span>
              {board && (
                <span className={`shrink-0 inline-flex items-center rounded border px-1 text-[8px] font-bold leading-tight ${board.color}`}>
                  {board.label}
                </span>
              )}
            </span>
            <span className="w-16 shrink-0 text-right font-mono tabular-nums text-secondary">{fmtPrice(i.price)}</span>
            <span className={`w-14 shrink-0 text-right font-mono tabular-nums ${priceColorClass(i.auction_change_ratio)}`}>
              {i.auction_change_ratio == null ? '—' : fmtPct(i.auction_change_ratio)}
            </span>
            <span className="w-20 shrink-0 text-right font-mono tabular-nums text-secondary">{fmtBigNum(i.matched_amount)}</span>
            <span className={`w-20 shrink-0 text-right font-mono tabular-nums ${side === 'sell' ? 'text-bear' : side === 'buy' ? 'text-bull' : 'text-muted'}`}>
              {side ? `${fmtBigNum(i.unmatched_amount)} ${side === 'buy' ? '买' : '卖'}` : '—'}
            </span>
          </button>
        )
      })}
    </div>
  )
}
