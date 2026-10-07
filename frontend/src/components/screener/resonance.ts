/**
 * 「全部」模式的共振(交集)计算 —— 抽成纯函数便于测试, 页面只负责取数。
 *
 * 背景: 「全部」视图的 `allRows` 是所有策略命中的**并集**(按 symbol 去重)。
 * 但多策略共识比单策略命中更有价值 —— 同一只股票被越多策略同时命中, 信号越可信。
 * 于是提供两个纯函数: 按命中数过滤(共振阈值) + 按命中数优先排序。
 */

/** 命中该 symbol 的策略数。 */
export type HitsOf = (symbol: string) => number

/**
 * 共振过滤: 只保留被 >= minHits 个策略同时命中的个股。
 * minHits <= 1 时原样返回(同一数组引用, 不破坏下游 memo)。
 */
export function filterByResonance<T extends { symbol: string }>(
  rows: T[],
  hitsOf: HitsOf,
  minHits: number,
): T[] {
  if (minHits <= 1) return rows
  return rows.filter(r => hitsOf(r.symbol) >= minHits)
}

/**
 * 共振优先排序: 命中策略数降序 → 评分降序。返回新数组(不原地改)。
 * 仅用于「全部」模式的默认排序; 用户点了表头就走按列排序。
 */
export function sortByResonance<T extends { symbol: string; score?: number | null }>(
  rows: T[],
  hitsOf: HitsOf,
): T[] {
  return [...rows].sort((a, b) =>
    (hitsOf(b.symbol) - hitsOf(a.symbol))
    || ((b.score ?? -Infinity) - (a.score ?? -Infinity)),
  )
}
