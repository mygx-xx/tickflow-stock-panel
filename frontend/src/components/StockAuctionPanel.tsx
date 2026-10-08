import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { CalendarClock, Loader2 } from 'lucide-react'
import { api, type AuctionSegment } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { DatePicker } from '@/components/DatePicker'
import { EChartsAuction } from '@/components/EChartsAuction'

interface Props {
  symbol: string
  height?: number
}

const SEGMENTS: { key: AuctionSegment; label: string; window: string }[] = [
  { key: 'open', label: '开盘竞价', window: '09:15-09:25' },
  { key: 'close', label: '收盘竞价', window: '14:57-15:00' },
]

/**
 * 个股集合竞价图 (逐点撮合序列)。
 *
 * 数据来自 auction 数据集的按日落盘分区, 分区缺失时后端单标的回源;
 * 竞价段窗口结束后数据不可变, 所以这里不做轮询, 只随日期/段切换取数。
 */
export function StockAuctionPanel({ symbol, height = 320 }: Props) {
  const [segment, setSegment] = useState<AuctionSegment>('open')
  // '' = 让后端取最近一个已落盘日 (日期不写在 URL 上, 查询键用空串占位)
  const [date, setDate] = useState('')

  const status = useQuery({ queryKey: QK.auctionStatus, queryFn: api.auctionStatus, staleTime: 60_000 })
  const series = useQuery({
    queryKey: QK.auctionSeries(symbol, date),
    queryFn: () => api.auctionSeries(symbol, date || undefined),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  })

  const points = useMemo(
    () => (series.data?.points ?? []).filter(p => p.segment === segment),
    [series.data?.points, segment],
  )

  const head = (
    <div className="flex flex-wrap items-center gap-2 pb-2">
      <div className="inline-flex shrink-0 items-center rounded border border-border/60 bg-base/60 p-0.5" role="tablist" aria-label="竞价段">
        {SEGMENTS.map(s => (
          <button
            key={s.key}
            type="button"
            role="tab"
            aria-selected={segment === s.key}
            onClick={() => setSegment(s.key)}
            className={`h-6 rounded px-2.5 text-[11px] transition-colors ${
              segment === s.key ? 'bg-accent/20 text-accent font-medium' : 'text-muted hover:text-secondary hover:bg-elevated/60'
            }`}
            title={`集合竞价窗口 ${s.window}`}
          >
            {s.label}
          </button>
        ))}
      </div>
      <DatePicker value={date} onChange={setDate} max={status.data?.dates?.[0]} placeholder="最近有数据日" />
      <span className="text-[10px] text-muted">{SEGMENTS.find(s => s.key === segment)?.window}</span>
    </div>
  )

  if (series.isLoading) {
    return (
      <div>
        {head}
        <div className="flex items-center justify-center gap-2 text-xs text-muted" style={{ height }}>
          <Loader2 className="h-4 w-4 animate-spin text-accent" />
          正在加载竞价序列…
        </div>
      </div>
    )
  }

  if (series.isError) {
    return (
      <div>
        {head}
        <div className="flex items-center justify-center text-xs text-danger" style={{ height }}>
          {series.error instanceof Error ? series.error.message : '竞价数据获取失败'}
        </div>
      </div>
    )
  }

  // 三种降级必须分开说: 源没配 ≠ 这天没有竞价
  if (series.data?.state === 'source_unavailable') {
    return (
      <div>
        {head}
        <div className="flex flex-col items-center justify-center gap-3 text-xs" style={{ height }}>
          <span className="max-w-md text-center leading-relaxed text-secondary">{series.data.msg}</span>
          <Link
            to="/settings?tab=data-sources"
            className="inline-flex items-center gap-1.5 rounded-btn bg-accent px-3 py-1.5 font-medium text-white hover:bg-accent/90"
          >
            前往配置数据源
          </Link>
        </div>
      </div>
    )
  }

  const body = points.length === 0 ? (
    <div className="flex items-center justify-center px-4 text-center text-xs text-muted" style={{ height }}>
      {series.data?.msg || `${segment === 'open' ? '开盘' : '收盘'}竞价段暂无逐点记录`}
    </div>
  ) : (
    <EChartsAuction points={points} prevClose={series.data?.prev_close} height={Math.max(180, height - 44)} />
  )

  const src = series.data?.source ?? ''
  const sourceLabel = src === 'parquet' ? '本地落盘'
    : src.startsWith('provider:') ? `实时回源 ${src.slice('provider:'.length)}`
    : (src || '—')

  return (
    <div>
      {head}
      {body}
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 pt-1 text-[10px] text-muted">
        <span className="inline-flex items-center gap-1">
          <CalendarClock className="h-3 w-3" />
          {series.data?.trade_date}
        </span>
        <span>来源 {sourceLabel}</span>
        <span>{points.length} 个撮合点</span>
        <span className="ml-auto">
          竞价源 {status.data?.provider ?? '—'} · 已落盘 {status.data?.dates?.length ?? 0} 个交易日
        </span>
      </div>
    </div>
  )
}
