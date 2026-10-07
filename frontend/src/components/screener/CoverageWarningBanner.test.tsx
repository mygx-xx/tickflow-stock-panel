// @vitest-environment jsdom
// #303 数据充足性提示的收集与展示: 后端只在富化覆盖不足时下发 warnings,
// 而这段提示要能解释「为什么 0 命中」—— 所以它必须独立于结果表渲染。
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, expect, it } from 'vitest'
import { CoverageWarningBanner, collectCoverageWarnings } from './CoverageWarningBanner'

const W1 = '本地数据仅覆盖 3 个交易日, 低于本次计算所需约 60 天暖机窗口'
const W2 = '本地数据仅覆盖 5 个交易日, 低于本次计算所需约 20 天暖机窗口'

let root: Root | null = null
let container: HTMLElement
afterEach(() => {
  if (root) { act(() => { root!.unmount() }); root = null }
  container?.remove()
})

// 每次 render 用新的 container: 同一 container 重复 createRoot 会触发 React 警告,
// 而「关闭后换上下文重新显示」这个用例需要在一个 test 里渲染两次。
function render(warnings: string[], signature?: string) {
  container = document.createElement('div')
  document.body.appendChild(container)
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  root = createRoot(container)
  act(() => { root!.render(<CoverageWarningBanner warnings={warnings} signature={signature} />) })
}

// ── 收集 ──────────────────────────────────────────────────────────────────

it('collects warnings from a single strategy result', () => {
  expect(collectCoverageWarnings(null, [W1])).toEqual([W1])
})

it('collects and de-duplicates warnings across many cached strategies', () => {
  const per = {
    strategy_a: { warnings: [W1] },
    strategy_b: { warnings: [W1, W2] },   // 同一段提示每条策略各算一次
    strategy_c: {},                        // 数据充足, 无 warnings 键
  }

  expect(collectCoverageWarnings(per, null)).toEqual([W1, W2])
})

it('merges the single-strategy and cached sources, single first', () => {
  expect(collectCoverageWarnings({ s: { warnings: [W2] } }, [W1])).toEqual([W1, W2])
})

it('returns an empty list when nothing warns', () => {
  expect(collectCoverageWarnings(null, null)).toEqual([])
  expect(collectCoverageWarnings({ s: {} }, undefined)).toEqual([])
})

// ── 展示 ──────────────────────────────────────────────────────────────────

it('renders the warning text when data is insufficient', () => {
  render([W1])

  expect(container.textContent).toContain('数据不足')
  expect(container.textContent).toContain(W1)
})

it('renders nothing when there are no warnings', () => {
  render([])

  expect(container.textContent).toBe('')
  expect(container.querySelector('button')).toBeNull()
})

it('hides after the user dismisses it', () => {
  render([W1], '2026-09-30:all')

  act(() => { container.querySelector('button')!.click() })

  expect(container.textContent).not.toContain(W1)
})

it('re-appears when the context signature changes after dismissal', () => {
  render([W1], '2026-09-30:all')
  act(() => { container.querySelector('button')!.click() })
  expect(container.textContent).not.toContain(W1)

  // 换日期 → 新的提示不该被上一次的关闭动作静默吞掉
  render([W1], '2026-10-08:all')

  expect(container.textContent).toContain(W1)
})
