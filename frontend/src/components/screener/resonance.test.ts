// @vitest-environment node
// 「全部」模式共振(交集) —— 纯函数回归。
// 并集是原行为; 共振在其之上按「命中策略数」取交集, 阈值 1 必须等价于不过滤
// 且返回同一引用(否则会击穿 displayRows 的 memo)。
import { describe, expect, it } from 'vitest'
import { filterByResonance, sortByResonance, type HitsOf } from './resonance'

const rows = [
  { symbol: 'A', score: 10 },
  { symbol: 'B', score: 90 },
  { symbol: 'C', score: 50 },
  { symbol: 'D', score: null },
]

const hitsTable: Record<string, number> = { A: 3, B: 1, C: 5, D: 0 }
const hitsOf: HitsOf = s => hitsTable[s] ?? 0

describe('filterByResonance', () => {
  it('阈值 1 等价于不过滤, 且返回同一数组引用', () => {
    expect(filterByResonance(rows, hitsOf, 1)).toBe(rows)
  })

  it('阈值 0 / 负数同样视为不过滤', () => {
    expect(filterByResonance(rows, hitsOf, 0)).toBe(rows)
    expect(filterByResonance(rows, hitsOf, -1)).toBe(rows)
  })

  it('阈值 3 只留下被 >=3 个策略命中的个股', () => {
    expect(filterByResonance(rows, hitsOf, 3).map(r => r.symbol)).toEqual(['A', 'C'])
  })

  it('阈值高于最大命中数 → 空(交集为空是合法结果, 不是错误)', () => {
    expect(filterByResonance(rows, hitsOf, 9)).toEqual([])
  })

  it('未知 symbol 命中数为 0, 会被高阈值滤掉', () => {
    const extra = [...rows, { symbol: 'ZZZ', score: 1 }]
    expect(filterByResonance(extra, hitsOf, 2).map(r => r.symbol)).toEqual(['A', 'C'])
  })
})

describe('sortByResonance', () => {
  it('先按命中数降序, 同命中数再按评分降序', () => {
    const out = sortByResonance(rows, hitsOf)
    expect(out.map(r => r.symbol)).toEqual(['C', 'A', 'B', 'D'])
  })

  it('不原地修改入参', () => {
    const input = [...rows]
    sortByResonance(input, hitsOf)
    expect(input.map(r => r.symbol)).toEqual(['A', 'B', 'C', 'D'])
  })

  it('评分缺失(null/undefined)被当作最低, 不污染排序', () => {
    const out = sortByResonance(
      [{ symbol: 'X', score: null }, { symbol: 'Y', score: 5 }, { symbol: 'Z' }],
      () => 1,
    )
    expect(out[0].symbol).toBe('Y')
    expect(out.map(r => r.symbol).slice(1).sort()).toEqual(['X', 'Z'])
  })
})
