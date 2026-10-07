import { X, RotateCcw, Filter } from 'lucide-react'
import { BOARDS, getBoardType } from '@/lib/board'

// ===== 筛选类型 =====

export interface ScreenerFilter {
  priceMin: string
  priceMax: string
  changePctMin: string
  changePctMax: string
  momentum5dMin: string
  momentum5dMax: string
  amountMin: string      // 成交额最小(亿)
  marketCapMin: string   // 市值最小(亿)
  marketCapMax: string   // 市值最大(亿)
  floatCapMin: string    // 流通市值最小(亿)
  floatCapMax: string    // 流通市值最大(亿)
  volRatioMin: string    // 量比最小
  rsiMin: string
  rsiMax: string
  turnoverMin: string    // 换手率最小(%)
  turnoverMax: string    // 换手率最大(%)
  amplitudeMin: string   // 振幅最小(%)
  amplitudeMax: string   // 振幅最大(%)
  volumeMin: string      // 成交量最小(万手)
  volumeMax: string      // 成交量最大(万手)
  limitUpMin: string     // 连板数最小值(≥), 0/空=不筛选
  highGapMin: string     // 距 60 日最高价的幅度下限(%): 0=仅创 60 日新高, -5=距高点 5% 以内
  signals: string[]      // 技术信号(与): 行内 signal_* 字段须全部为 true
  boards: string[]       // 板块筛选: 空数组=不筛选, 否则只保留选中的板块
  excludeST: boolean     // 是否排除 ST/*ST/退市股
}

/** 可选的技术信号开关 — key 直接对应结果行里的布尔字段(后端 enriched 输出)。 */
export const SIGNAL_OPTIONS: { key: string; label: string; title: string }[] = [
  { key: 'signal_limit_up', label: '涨停', title: '当日涨停' },
  { key: 'signal_volume_surge', label: '放量', title: '成交量显著放大' },
  { key: 'signal_macd_golden', label: 'MACD金叉', title: 'MACD DIF 上穿 DEA' },
  { key: 'signal_ma_golden_5_20', label: '均线金叉', title: 'MA5 上穿 MA20' },
  { key: 'signal_n_day_high', label: '阶段新高', title: '创 N 日新高' },
  { key: 'signal_boll_breakout_upper', label: '破布林上轨', title: '收盘突破布林上轨' },
]

export const defaultFilter: ScreenerFilter = {
  priceMin: '', priceMax: '',
  changePctMin: '', changePctMax: '',
  momentum5dMin: '', momentum5dMax: '',
  amountMin: '',
  marketCapMin: '', marketCapMax: '',
  floatCapMin: '', floatCapMax: '',
  volRatioMin: '',
  rsiMin: '', rsiMax: '',
  turnoverMin: '', turnoverMax: '',
  amplitudeMin: '', amplitudeMax: '',
  volumeMin: '', volumeMax: '',
  limitUpMin: '',
  highGapMin: '',
  signals: [],
  boards: [],
  excludeST: false,
}

/**
 * 生效判定 / 计数统一走 countActiveFilters —— 旧实现用 Object.entries 遍历,
 * `signals: []` 这类数组字段会因 `[] !== ''` 恒真, 必须避免。
 */
export function filterActive(f: ScreenerFilter): boolean {
  return countActiveFilters(f) > 0
}

export function countActiveFilters(f: ScreenerFilter): number {
  let n = 0
  if (f.priceMin || f.priceMax) n++
  if (f.changePctMin || f.changePctMax) n++
  if (f.momentum5dMin || f.momentum5dMax) n++
  if (f.amountMin) n++
  if (f.marketCapMin || f.marketCapMax) n++
  if (f.floatCapMin || f.floatCapMax) n++
  if (f.volRatioMin) n++
  if (f.rsiMin || f.rsiMax) n++
  if (f.turnoverMin || f.turnoverMax) n++
  if (f.amplitudeMin || f.amplitudeMax) n++
  if (f.volumeMin || f.volumeMax) n++
  if (f.limitUpMin) n++
  if (f.highGapMin) n++
  if (f.signals.length > 0) n++
  if (f.boards.length > 0) n++
  if (f.excludeST) n++
  return n
}

export function applyFilter(rows: any[], f: ScreenerFilter): any[] {
  if (!filterActive(f)) return rows
  const num = (v: string) => v === '' ? null : Number(v)
  return rows.filter((r) => {
    // 板块: 用 symbol 判定板块, 必须在选中列表里
    // 全选 5 个板块 = 不过滤 (等价于 boards:[]), 避免 getBoardType 返回 null 的边缘品种被误删
    if (f.boards.length > 0 && f.boards.length < BOARDS.length) {
      const board = getBoardType(r.symbol)
      if (!board || !f.boards.includes(board)) return false
    }
    // ST: name 含 ST/*ST/退 的排除 (对齐后端口径 (?i)ST|退)
    if (f.excludeST && /ST|退/i.test(String(r.name ?? ''))) return false
    const close = Number(r.close ?? 0)
    const v = (field: string) => num(field)
    // 现价
    if (v(f.priceMin) != null && close < v(f.priceMin)!) return false
    if (v(f.priceMax) != null && close > v(f.priceMax)!) return false
    // 涨跌幅(%)
    const chg = (r.change_pct ?? 0) * 100
    if (v(f.changePctMin) != null && chg < v(f.changePctMin)!) return false
    if (v(f.changePctMax) != null && chg > v(f.changePctMax)!) return false
    // 5日涨幅(%)
    const m5 = (r.momentum_5d ?? 0) * 100
    if (v(f.momentum5dMin) != null && m5 < v(f.momentum5dMin)!) return false
    if (v(f.momentum5dMax) != null && m5 > v(f.momentum5dMax)!) return false
    // 成交额(亿)
    const amount = (r.amount ?? 0) / 1e8
    if (v(f.amountMin) != null && amount < v(f.amountMin)!) return false
    // 市值(亿)
    const cap = close * (r.total_shares ?? 0) / 1e8
    if (v(f.marketCapMin) != null && cap < v(f.marketCapMin)!) return false
    if (v(f.marketCapMax) != null && cap > v(f.marketCapMax)!) return false
    // 流通市值(亿)
    const fcap = close * (r.float_shares ?? 0) / 1e8
    if (v(f.floatCapMin) != null && fcap < v(f.floatCapMin)!) return false
    if (v(f.floatCapMax) != null && fcap > v(f.floatCapMax)!) return false
    // 量比
    if (v(f.volRatioMin) != null && (r.vol_ratio_5d ?? 0) < v(f.volRatioMin)!) return false
    // RSI
    const rsi = r.rsi_14 ?? 0
    if (v(f.rsiMin) != null && rsi < v(f.rsiMin)!) return false
    if (v(f.rsiMax) != null && rsi > v(f.rsiMax)!) return false
    // 换手率(%) — enriched 存储列已是百分比
    const turnover = r.turnover_rate ?? 0
    if (v(f.turnoverMin) != null && turnover < v(f.turnoverMin)!) return false
    if (v(f.turnoverMax) != null && turnover > v(f.turnoverMax)!) return false
    // 振幅(%) — 存储为小数, 与涨跌幅同口径
    const amp = (r.amplitude ?? 0) * 100
    if (v(f.amplitudeMin) != null && amp < v(f.amplitudeMin)!) return false
    if (v(f.amplitudeMax) != null && amp > v(f.amplitudeMax)!) return false
    // 成交量(万手) — 存储单位为手
    const vol = (r.volume ?? 0) / 1e4
    if (v(f.volumeMin) != null && vol < v(f.volumeMin)!) return false
    if (v(f.volumeMax) != null && vol > v(f.volumeMax)!) return false
    // 连板数(≥)
    if (v(f.limitUpMin) != null && (r.consecutive_limit_ups ?? 0) < v(f.limitUpMin)!) return false
    // 距 60 日高点(%): 取幅度下限。0 表示仅保留创 60 日新高的个股; -5 表示
    // 距高点 5% 以内。无 60 日高点数据(次新/停牌)无法判定 → 视为不满足。
    if (v(f.highGapMin) != null) {
      const high60 = Number(r.high_60d ?? 0)
      if (!(high60 > 0)) return false
      const gap = (close / high60 - 1) * 100
      if (gap < v(f.highGapMin)!) return false
    }
    // 技术信号(与): 行内 signal_* 布尔字段须全部为 true
    if (f.signals.length > 0) {
      for (const key of f.signals) {
        if (!r[key]) return false
      }
    }
    return true
  })
}

// ===== 筛选面板 =====

export function FilterPanel({ value, onChange, onClose, onReset }: {
  value: ScreenerFilter
  onChange: (f: ScreenerFilter) => void
  onClose: () => void
  onReset: () => void
}) {
  const set = (key: keyof ScreenerFilter, v: string) => onChange({ ...value, [key]: v })

  const toggleBoard = (board: string) => {
    const next = value.boards.includes(board)
      ? value.boards.filter(b => b !== board)
      : [...value.boards, board]
    onChange({ ...value, boards: next })
  }

  // 数值字段只引用 string 类型的 key (排除 signals/boards/excludeST), 避免类型混乱
  type NumKey = keyof Pick<ScreenerFilter,
    'priceMin' | 'priceMax' | 'changePctMin' | 'changePctMax' |
    'momentum5dMin' | 'momentum5dMax' | 'amountMin' |
    'marketCapMin' | 'marketCapMax' | 'floatCapMin' | 'floatCapMax' |
    'volRatioMin' | 'rsiMin' | 'rsiMax' |
    'turnoverMin' | 'turnoverMax' | 'amplitudeMin' | 'amplitudeMax' |
    'volumeMin' | 'volumeMax' | 'limitUpMin' | 'highGapMin'>
  // single: 'min' | 'max' 表示单向字段(只渲染一个输入框), 决定占位文案
  const fields: { label: string; min: NumKey; max: NumKey; unit: string; step?: string; single?: 'min' | 'max'; title?: string }[] = [
    { label: '现价',      min: 'priceMin',      max: 'priceMax',      unit: '元', step: '0.1' },
    { label: '涨跌幅',    min: 'changePctMin',   max: 'changePctMax',  unit: '%' },
    { label: '5日涨幅',   min: 'momentum5dMin',  max: 'momentum5dMax', unit: '%' },
    { label: '成交额',    min: 'amountMin',      max: 'amountMin',     unit: '亿', step: '0.5', single: 'min' },
    { label: '总市值',    min: 'marketCapMin',   max: 'marketCapMax',  unit: '亿', step: '10' },
    { label: '流通市值',  min: 'floatCapMin',    max: 'floatCapMax',   unit: '亿', step: '10' },
    { label: '量比',      min: 'volRatioMin',    max: 'volRatioMin',   unit: '', step: '0.1', single: 'min' },
    { label: 'RSI14',     min: 'rsiMin',         max: 'rsiMax',        unit: '', step: '1' },
    { label: '换手率',    min: 'turnoverMin',    max: 'turnoverMax',   unit: '%', step: '0.5' },
    { label: '振幅',      min: 'amplitudeMin',   max: 'amplitudeMax',  unit: '%', step: '0.5' },
    { label: '成交量',    min: 'volumeMin',      max: 'volumeMax',     unit: '万手', step: '1' },
    { label: '连板',      min: 'limitUpMin',     max: 'limitUpMin',    unit: '板', step: '1', single: 'min', title: '连板数下限(≥)' },
    { label: '距60日高',  min: 'highGapMin',     max: 'highGapMin',    unit: '%', step: '0.5', single: 'min', title: '距 60 日最高价的幅度下限; 填 0 只看创 60 日新高, 填 -5 看距高点 5% 以内' },
  ]

  const toggleSignal = (key: string) => {
    const next = value.signals.includes(key)
      ? value.signals.filter(k => k !== key)
      : [...value.signals, key]
    onChange({ ...value, signals: next })
  }

  return (
    <div className="rounded-card border border-accent/30 bg-accent/[0.03] p-4 space-y-3">
      {/* 标题栏: 左侧标题 + 激活计数, 右侧重置 + 关闭 */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Filter className="h-3.5 w-3.5 text-accent" />
          <span className="text-xs font-medium text-accent">筛选条件</span>
          {filterActive(value) && (
            <span className="bg-accent/15 text-accent rounded-full px-1.5 h-4 inline-flex items-center text-[10px] font-bold leading-none">
              {countActiveFilters(value)}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1">
          {filterActive(value) && (
            <button
              onClick={onReset}
              title="清空全部筛选"
              className="inline-flex items-center gap-1 px-1.5 h-6 rounded text-[11px]
                text-muted hover:text-danger hover:bg-danger/10 transition-colors"
            >
              <RotateCcw className="h-3 w-3" />
              清空
            </button>
          )}
          <button
            onClick={onClose}
            title="收起筛选"
            className="p-1 rounded text-secondary hover:text-foreground hover:bg-elevated transition-colors"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* 板块 + ST 快速筛选 (按钮组) */}
      <div className="flex flex-wrap items-center gap-1.5">
        <span className="text-[11px] text-secondary shrink-0 w-10">市场</span>
        {BOARDS.map(board => {
          const active = value.boards.includes(board)
          return (
            <button
              key={board}
              onClick={() => toggleBoard(board)}
              className={`px-2 py-0.5 rounded text-[11px] transition-colors ${
                active
                  ? 'bg-accent/15 text-accent'
                  : 'bg-elevated text-secondary hover:text-foreground hover:bg-elevated/80'
              }`}
            >
              {board}
            </button>
          )
        })}
        <span className="w-px h-4 bg-border mx-1" />
        <button
          onClick={() => onChange({ ...value, excludeST: !value.excludeST })}
          className={`px-2 py-0.5 rounded text-[11px] transition-colors ${
            value.excludeST
              ? 'bg-accent/15 text-accent'
              : 'bg-elevated text-secondary hover:text-foreground hover:bg-elevated/80'
          }`}
        >
          排除ST
        </button>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-x-4 gap-y-2.5">
        {fields.map((f) => {
          const isRange = f.min !== f.max
          const singleMax = !isRange && f.single === 'max'
          return (
            <div key={f.label} className="flex items-center gap-1.5" title={f.title}>
              <span className="text-[11px] text-secondary shrink-0 w-14 text-right">{f.label}</span>
              <input
                type="number"
                placeholder={singleMax ? '最大' : '最小'}
                value={value[f.min]}
                onChange={(e) => set(f.min, e.target.value)}
                step={f.step}
                className="w-16 px-1.5 py-1 rounded-btn bg-base border border-border text-[11px] font-mono text-foreground text-center focus:outline-none focus:border-accent/50"
              />
              {isRange && (
                <>
                  <span className="text-[10px] text-muted">~</span>
                  <input
                    type="number"
                    placeholder="最大"
                    value={value[f.max]}
                    onChange={(e) => set(f.max, e.target.value)}
                    step={f.step}
                    className="w-16 px-1.5 py-1 rounded-btn bg-base border border-border text-[11px] font-mono text-foreground text-center focus:outline-none focus:border-accent/50"
                  />
                </>
              )}
              {f.unit && <span className="text-[10px] text-muted shrink-0">{f.unit}</span>}
            </div>
          )
        })}
      </div>

      {/* 技术信号 (与): 全部为 true 才保留 */}
      <div className="flex flex-wrap items-center gap-1.5">
        <span className="text-[11px] text-secondary shrink-0 w-14 text-right">技术信号</span>
        {SIGNAL_OPTIONS.map(sig => {
          const active = value.signals.includes(sig.key)
          return (
            <button
              key={sig.key}
              onClick={() => toggleSignal(sig.key)}
              title={sig.title}
              className={`px-2 py-0.5 rounded text-[11px] transition-colors cursor-pointer ${
                active
                  ? 'bg-accent/15 text-accent'
                  : 'bg-elevated text-secondary hover:text-foreground hover:bg-elevated/80'
              }`}
            >
              {sig.label}
            </button>
          )
        })}
      </div>
      <div className="text-[10px] text-muted/70 pl-1">
        输入即生效 · 点击市场/ST/信号按钮切换 · 信号之间为「与」关系
      </div>
    </div>
  )
}
