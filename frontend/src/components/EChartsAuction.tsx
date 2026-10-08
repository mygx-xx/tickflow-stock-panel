import { useEffect, useMemo, useRef, useState } from 'react'
import * as echarts from 'echarts'
import type { ECharts, EChartsOption } from 'echarts'
import type { AuctionPoint } from '@/lib/api'
import { useChartTheme } from '@/lib/theme'
import { fmtBigNum, fmtPrice } from '@/lib/format'

// 序列色 (双主题通用); 轴/网格等主题相关色走 ChartTheme
const C = {
  priceUp: '#C74040',
  priceDown: '#2D9B65',
  priceFlat: '#A1A1AA',
  matched: 'rgba(148,163,184,0.55)',   // 匹配量: 中性灰, 不表达涨跌含义
  unmatchedBuy: 'rgba(240,68,56,0.75)', // 未匹配剩余在买侧 → 红(排队买不进)
  unmatchedSell: 'rgba(18,183,106,0.75)',
  prevClose: '#A1A1AA',
}

interface Props {
  points: AuctionPoint[]
  prevClose?: number | null
  height?: number
}

/** 竞价时间是**北京墙钟 naive**(数据集契约), 不能按本地时区解析成 Date —
 *  非北京时区会把 09:25 显示成 01:25。直接取 ISO 串的时分秒, 零时区换算。 */
function hms(datetime: string): string {
  return datetime.slice(11, 19)
}

function isValidPrice(v: number | null | undefined): v is number {
  return typeof v === 'number' && Number.isFinite(v) && v > 0
}

/** 竞价量读数(手): 十万手以内保留整数精度, 以上才折万/亿。
 *  阈值不能压在 1e4: 大盘股开盘竞价匹配量普遍就在 1万~10万手 (宁德时代 10,996 手),
 *  那样多数行会被 fmtBigNum 折成「1万手」, 只剩一位有效数字。 */
function lotLabel(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return '—'
  return v >= 1e5 ? fmtBigNum(v) : Math.round(v).toLocaleString('en-US')
}

function sideOf(p: AuctionPoint): 'buy' | 'sell' | null {
  if (p.unmatched_volume == null || p.unmatched_volume <= 0) return null
  return p.unmatched_side === 'buy' || p.unmatched_side === 'sell' ? p.unmatched_side : null
}

function buildOption(points: AuctionPoint[], prevClose: number | null | undefined, ct: ReturnType<typeof useChartTheme>): EChartsOption {
  const labels = points.map(p => hms(p.datetime))
  const prices = points.map(p => p.price)
  const matched = points.map(p => p.matched_volume)
  const unmatched = points.map(p => (sideOf(p) ? p.unmatched_volume : null))
  const lastPrice = [...prices].reverse().find(isValidPrice)
  // x 轴标签位: 先按分钟去重(10 分钟窗口能有 60+ 个撮合点), 再从这些分钟里抽约 6 个。
  // 直接按点索引取模会让抽中的槽位反复落进同一分钟, 轴上就成了 14:57 14:57 14:57。
  const minuteMarks = (() => {
    const firstAt = new Map<string, number>()
    labels.forEach((l, i) => {
      const m = l.slice(0, 5)
      if (!firstAt.has(m)) firstAt.set(m, i)
    })
    const idx = [...firstAt.values()]
    if (!idx.length) return new Set<number>()
    const step = Math.max(1, Math.round(idx.length / 6))
    return new Set(idx.filter((_, k) => k % step === 0))
  })()
  const lineColor = !isValidPrice(prevClose) || !isValidPrice(lastPrice)
    ? C.priceFlat
    : lastPrice > prevClose ? C.priceUp : lastPrice < prevClose ? C.priceDown : C.priceFlat

  const markLineData: any[] = []
  if (isValidPrice(prevClose)) {
    markLineData.push({
      yAxis: prevClose,
      symbol: 'none',
      lineStyle: { color: C.prevClose, type: 'dashed', width: 1 },
      label: { show: true, position: 'insideStartTop', color: C.prevClose, fontSize: 10, formatter: `昨收 ${prevClose.toFixed(2)}` },
    })
  }

  // 右轴涨跌幅: 只有拿到昨收才有意义, 拿不到就不画(不臆造基准)
  const pctAxis = isValidPrice(prevClose)
    ? [{
        type: 'value' as const,
        gridIndex: 0,
        position: 'right' as const,
        min: (v: any) => v.min,
        max: (v: any) => v.max,
        splitLine: { show: false },
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          formatter: (v: number) => {
            const pct = ((v - prevClose) / prevClose) * 100
            return Math.abs(pct) < 0.01 ? '0.00%' : `${pct > 0 ? '+' : ''}${pct.toFixed(2)}%`
          },
        },
      }]
    : []

  return {
    animation: false,
    axisPointer: { link: [{ xAxisIndex: 'all' }], lineStyle: { color: ct.crosshair } },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: ct.tooltipBg,
      borderColor: ct.tooltipBorder,
      textStyle: { color: ct.tooltipText, fontSize: 11 },
      formatter: (raw: any) => {
        const arr = Array.isArray(raw) ? raw : [raw]
        const idx = arr[0]?.dataIndex ?? 0
        const p = points[idx]
        if (!p) return ''
        const side = sideOf(p)
        const chg = isValidPrice(prevClose) && isValidPrice(p.price)
          ? `${(((p.price - prevClose) / prevClose) * 100).toFixed(2)}%`
          : '—'
        const rows: [string, string, string][] = [
          ['虚拟参考价', fmtPrice(p.price), chg],
          ['匹配量(手)', lotLabel(p.matched_volume), ''],
          ['匹配额', fmtBigNum(p.price != null && p.matched_volume != null ? p.price * p.matched_volume * 100 : null), ''],
          ['未匹配(手)', side ? `${lotLabel(p.unmatched_volume)} ${side === 'buy' ? '买侧' : '卖侧'}` : '—', ''],
        ]
        return [`<div style="font-weight:600">${hms(p.datetime)}</div>`, ...rows.map(([k, v, x]) =>
          `<div style="display:flex;gap:8px;justify-content:space-between"><span>${k}</span><span>${v}${x ? ` <span style="color:${x === '—' ? ct.text : (x.startsWith('-') ? C.priceDown : C.priceUp)}">${x}</span>` : ''}</span></div>`,
        )].join('')
      },
    },
    grid: [
      { left: 56, right: 56, top: 14, height: '56%' },
      { left: 56, right: 56, top: '70%', bottom: 26 },
    ],
    xAxis: [
      {
        type: 'category',
        gridIndex: 0,
        data: labels,
        boundaryGap: false,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
      },
      {
        type: 'category',
        gridIndex: 1,
        data: labels,
        boundaryGap: true,
        axisLine: { lineStyle: { color: ct.border } },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          // 竞价窗口只有 10 分钟(开盘段)/3 分钟(收盘段), 按分钟去重后再抽稀, 标签不会同刻重复
          interval: (i: number) => minuteMarks.has(i),
          formatter: (v: string) => v.slice(0, 5),
        },
      },
    ],
    yAxis: [
      {
        type: 'value',
        gridIndex: 0,
        scale: true,
        splitNumber: 3,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { lineStyle: { color: ct.grid } },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          formatter: (v: number) => v.toFixed(2),
        },
      },
      ...pctAxis,
      {
        type: 'value',
        gridIndex: 1,
        splitNumber: 2,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: {
          color: ct.text,
          fontSize: 10,
          fontFamily: 'JetBrains Mono, monospace',
          formatter: (v: number) => fmtBigNum(v),
        },
      },
    ],
    series: [
      {
        name: '虚拟价',
        type: 'line',
        xAxisIndex: 0,
        yAxisIndex: 0,
        data: prices,
        symbol: 'none',
        smooth: false,
        connectNulls: true,
        lineStyle: { width: 1.4, color: lineColor },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: lineColor === C.priceUp ? 'rgba(199,64,64,0.35)' : lineColor === C.priceDown ? 'rgba(34,197,94,0.35)' : 'rgba(180,180,190,0.30)' },
              { offset: 1, color: 'rgba(0,0,0,0)' },
            ],
          },
        },
        markLine: markLineData.length ? { symbol: 'none', silent: true, data: markLineData } : undefined,
      },
      {
        name: '匹配量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: pctAxis.length ? 2 : 1,
        stack: 'vol',
        data: matched,
        itemStyle: { color: C.matched },
        barMaxWidth: 8,
      },
      {
        name: '未匹配量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: pctAxis.length ? 2 : 1,
        stack: 'vol',
        data: unmatched.map((v, i) => ({
          value: v,
          // 方向逐点取色: 同一只股竞价过程中买侧/卖侧可能翻转
          itemStyle: { color: sideOf(points[i]) === 'sell' ? C.unmatchedSell : C.unmatchedBuy },
        })),
        barMaxWidth: 8,
      },
    ],
  } as EChartsOption
}

export function EChartsAuction({ points, prevClose, height = 300 }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<ECharts | null>(null)
  const roRef = useRef<ResizeObserver | null>(null)
  const ct = useChartTheme()
  const [hoverIdx, setHoverIdx] = useState(-1)

  const option = useMemo(() => buildOption(points, prevClose, ct), [points, prevClose, ct])

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    let chart = chartRef.current
    if (!chart) {
      chart = echarts.init(el, undefined, { renderer: 'canvas' })
      chartRef.current = chart
      chart.on('updateAxisPointer', (event: any) => {
        const info = event?.axesInfo?.[0]
        setHoverIdx(typeof info?.value === 'number' ? info.value : -1)
      })
      chart.on('globalout', () => setHoverIdx(-1))
      roRef.current = new ResizeObserver(() => chart!.resize())
      roRef.current.observe(el)
    }
    if (points.length > 0) chart.setOption(option, true)
    else chart.clear()
    chart.resize()
  }, [option, points.length])

  useEffect(() => () => {
    roRef.current?.disconnect()
    chartRef.current?.dispose()
    chartRef.current = null
  }, [])

  const shown = hoverIdx >= 0 && hoverIdx < points.length ? points[hoverIdx] : points[points.length - 1]
  const side = shown ? sideOf(shown) : null
  const chg = shown && isValidPrice(prevClose) && isValidPrice(shown.price)
    ? ((shown.price - prevClose) / prevClose) * 100
    : null
  const amount = shown?.price != null && shown?.matched_volume != null
    ? shown.price * shown.matched_volume * 100
    : null

  return (
    <div className="w-full">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-1 py-1 font-mono text-[11px] select-none" style={{ minHeight: 22 }}>
        {shown ? (
          <>
            <span className="text-muted">{hms(shown.datetime)}</span>
            <span className="text-muted">虚拟价</span>
            <span className={chg == null ? 'text-secondary' : chg >= 0 ? 'text-bull' : 'text-bear'}>{fmtPrice(shown.price)}</span>
            {chg != null && <span className={chg >= 0 ? 'text-bull' : 'text-bear'}>{`${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%`}</span>}
            <span className="text-muted">匹配</span>
            <span className="text-secondary">{lotLabel(shown.matched_volume)}手</span>
            <span className="text-muted">额</span>
            <span className="text-secondary">{fmtBigNum(amount)}</span>
            <span className="text-muted">未匹配</span>
            <span className={side === 'sell' ? 'text-bear' : side === 'buy' ? 'text-bull' : 'text-muted'}>
              {side ? `${lotLabel(shown.unmatched_volume)}手 ${side === 'buy' ? '买侧' : '卖侧'}` : '—'}
            </span>
            <span className="ml-auto text-muted">{hoverIdx >= 0 ? '悬停' : '末点'}</span>
          </>
        ) : (
          <span className="text-muted">—</span>
        )}
      </div>
      <div ref={containerRef} className="w-full" style={{ height, cursor: 'crosshair' }} />
    </div>
  )
}
