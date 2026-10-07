// @vitest-environment jsdom
// 策略结果筛选 —— 维度扩展后的口径回归。
//
// 覆盖三处易错点:
//  1. 单位口径: 振幅/涨跌幅/5日涨幅在 enriched 行里是「小数」, 换手率是「百分比」,
//     成交量是「手」—— 输入框单位分别是 % / % / 万手, 换算错了筛选就静默失效。
//  2. filterActive 的回归: 旧实现用 Object.entries 遍历全部字段判空, 新增数组字段
//     `signals` 后会因 `[] !== ''` 恒真 → 每次渲染都被当成「有筛选」。现统一走
//     countActiveFilters。
//  3. 技术信号为「与」关系, 不是「或」。
import { describe, expect, it } from 'vitest'
import {
  applyFilter, countActiveFilters, defaultFilter, filterActive, SIGNAL_OPTIONS,
  type ScreenerFilter,
} from './ScreenerFilter'

/** 结果行基线 —— 只保留筛选会用到的字段, 默认值均「不触发任何条件」。 */
const row = (over: Record<string, unknown> = {}) => ({
  symbol: '000001.SZ', name: '平安银行', close: 10,
  change_pct: 0, momentum_5d: 0, amount: 0,
  total_shares: 0, float_shares: 0, vol_ratio_5d: 0, rsi_14: 50,
  turnover_rate: 5, amplitude: 0.04, volume: 2e4,
  consecutive_limit_ups: 0, high_60d: 10,
  ...over,
})

const f = (over: Partial<ScreenerFilter> = {}): ScreenerFilter => ({ ...defaultFilter, ...over })
const symbols = (rows: any[], filter: ScreenerFilter) => applyFilter(rows, filter).map(r => r.symbol)

describe('filterActive / countActiveFilters', () => {
  it('默认条件不生效', () => {
    expect(filterActive(defaultFilter)).toBe(false)
    expect(countActiveFilters(defaultFilter)).toBe(0)
  })

  it('数组字段为空时不应被误判为「有筛选」', () => {
    // 回归: signals: [] 曾让 filterActive 恒真
    expect(filterActive(f({ signals: [] }))).toBe(false)
    expect(filterActive(f({ boards: [] }))).toBe(false)
    expect(countActiveFilters(f({ signals: [], boards: [] }))).toBe(0)
  })

  it('技术信号整组算一项', () => {
    expect(filterActive(f({ signals: ['signal_limit_up'] }))).toBe(true)
    expect(countActiveFilters(f({ signals: ['signal_limit_up', 'signal_macd_golden'] }))).toBe(1)
  })

  it('新增维度各自计入计数', () => {
    expect(countActiveFilters(f({ turnoverMin: '3' }))).toBe(1)
    expect(countActiveFilters(f({ amplitudeMax: '8' }))).toBe(1)
    expect(countActiveFilters(f({ volumeMin: '1' }))).toBe(1)
    expect(countActiveFilters(f({ limitUpMin: '2' }))).toBe(1)
    expect(countActiveFilters(f({ highGapMin: '-5' }))).toBe(1)
  })

  it('无筛选时应原样返回同一数组(避免下游 memo 被击穿)', () => {
    const rows = [row()]
    expect(applyFilter(rows, defaultFilter)).toBe(rows)
  })
})

describe('数值维度口径', () => {
  it('换手率按百分比直接比较', () => {
    const rows = [row({ turnover_rate: 1 }), row({ symbol: 'A', turnover_rate: 8 })]
    expect(symbols(rows, f({ turnoverMin: '3' }))).toEqual(['A'])
    expect(symbols(rows, f({ turnoverMax: '3' }))).toEqual(['000001.SZ'])
  })

  it('振幅存储为小数, 单位是 % → 需 ×100 后比较', () => {
    const rows = [row({ amplitude: 0.02 }), row({ symbol: 'A', amplitude: 0.08 })]
    expect(symbols(rows, f({ amplitudeMin: '5' }))).toEqual(['A'])
    expect(symbols(rows, f({ amplitudeMax: '5' }))).toEqual(['000001.SZ'])
  })

  it('成交量存储为手, 单位是万手 → 需 /1e4 后比较', () => {
    const rows = [row({ volume: 5_000 }), row({ symbol: 'A', volume: 50_000 })]
    expect(symbols(rows, f({ volumeMin: '1' }))).toEqual(['A'])
    expect(symbols(rows, f({ volumeMax: '1' }))).toEqual(['000001.SZ'])
  })

  it('连板数取下限', () => {
    const rows = [row({ consecutive_limit_ups: 0 }), row({ symbol: 'A', consecutive_limit_ups: 2 })]
    expect(symbols(rows, f({ limitUpMin: '2' }))).toEqual(['A'])
    // 0 视同未填 → 不过滤
    expect(symbols(rows, f({ limitUpMin: '0' }))).toEqual(['000001.SZ', 'A'])
  })
})

describe('距 60 日高', () => {
  const rows = [
    row({ close: 10, high_60d: 10 }),                 // 正好创 60 日新高 → gap 0
    row({ symbol: 'A', close: 9, high_60d: 10 }),     // gap -10
    row({ symbol: 'B', close: 10, high_60d: 0 }),     // 无 60 日高点数据
  ]

  it('0 = 仅创 60 日新高的个股', () => {
    expect(symbols(rows, f({ highGapMin: '0' }))).toEqual(['000001.SZ'])
  })

  it('-5 = 距高点 5% 以内', () => {
    expect(symbols(rows, f({ highGapMin: '-5' }))).toEqual(['000001.SZ'])
  })

  it('-10 = 允许最多回撤 10%', () => {
    expect(symbols(rows, f({ highGapMin: '-10' }))).toEqual(['000001.SZ', 'A'])
  })

  it('缺 60 日高点数据时视为不满足(而非放行)', () => {
    expect(symbols(rows, f({ highGapMin: '-99' }))).toEqual(['000001.SZ', 'A'])
  })
})

describe('技术信号', () => {
  const rows = [
    row({ signal_limit_up: true, signal_macd_golden: false }),
    row({ symbol: 'A', signal_limit_up: true, signal_macd_golden: true }),
    row({ symbol: 'B', signal_limit_up: false, signal_macd_golden: true }),
  ]

  it('单个信号', () => {
    expect(symbols(rows, f({ signals: ['signal_limit_up'] }))).toEqual(['000001.SZ', 'A'])
  })

  it('多个信号是「与」而非「或」', () => {
    expect(symbols(rows, f({ signals: ['signal_limit_up', 'signal_macd_golden'] }))).toEqual(['A'])
  })

  it('信号的 key 必须是行内布尔字段(全部 signal_ 前缀且不重复)', () => {
    const keys = SIGNAL_OPTIONS.map(o => o.key)
    expect(new Set(keys).size).toBe(keys.length)
    for (const k of keys) expect(k.startsWith('signal_')).toBe(true)
    // 行内确实存在这些字段
    const sample = row({
      signal_limit_up: true, signal_volume_surge: true, signal_macd_golden: true,
      signal_ma_golden_5_20: true, signal_n_day_high: true, signal_boll_breakout_upper: true,
    })
    for (const k of keys) expect(sample).toHaveProperty(k)
  })
})
