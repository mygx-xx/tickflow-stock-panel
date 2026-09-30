import { useEffect, useRef, useState } from 'react'

/**
 * 现价闪动: 价格相对上一次快照发生变化时, 返回一次性的方向标记。
 *
 * 自选页由 SSE tick 触发整份 enriched 重取 (`rows` 整体替换), 因此"变动"只能靠
 * 前后两次快照比对得出 —— 后端不推送逐笔增量。
 *
 * 语义要点:
 * - **只在价格真的变了才闪**。若每轮刷新都闪, 闭市或冷门股会持续闪动, 反而掩盖
 *   真正的变动 (实测盘中一轮 ~6s, 全表闪一遍没有信息量)。
 * - 方向按**本次相对上次**判定 (涨=up / 跌=down), 与涨跌色 (`priceColorClass`,
 *   相对昨收) 是两个不同口径, 故只用于背景, 不改文字色。
 * - 首次挂载 (无前值) 不闪: 打开页面时"从无到有"不是行情变动。
 * - 闪动是瞬时的: 定时清回 null, 同一方向连续变动也能重新触发动画。
 *
 * @param price 当前价格; null/undefined 视为无数据, 不参与比对
 */
export type TickDirection = 'up' | 'down'

/** 动画时长须与 index.css 的 tick-flash-* 保持一致 */
const FLASH_MS = 600

export function useTickFlash(price: number | null | undefined): TickDirection | null {
  const prevRef = useRef<number | null>(null)
  const [direction, setDirection] = useState<TickDirection | null>(null)
  const timerRef = useRef<ReturnType<typeof setTimeout>>()

  useEffect(() => {
    if (price == null || Number.isNaN(price)) return

    const prev = prevRef.current
    prevRef.current = price

    // 首次见到该值 (含从 null 恢复): 无前值可比, 不算变动
    if (prev == null || prev === price) return

    setDirection(price > prev ? 'up' : 'down')
    clearTimeout(timerRef.current)
    // 复位为 null, 使同方向的下一次变动也能重新触发 CSS 动画
    // (class 不变时浏览器不会重放 animation)
    timerRef.current = setTimeout(() => setDirection(null), FLASH_MS)
  }, [price])

  // 卸载时清理定时器, 避免对已卸载组件 setState
  useEffect(() => () => clearTimeout(timerRef.current), [])

  return direction
}

/** 闪动 class: 映射到 index.css 的 tick-flash-up / tick-flash-down */
export function tickFlashCls(direction: TickDirection | null): string {
  if (direction === 'up') return 'tick-flash-up'
  if (direction === 'down') return 'tick-flash-down'
  return ''
}
