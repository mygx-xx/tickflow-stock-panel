// @vitest-environment jsdom
// 筛选面板渲染 —— 锁住「新增维度可见」与「信号开关接线」。
// 纯口径逻辑在 ScreenerFilter.test.tsx 覆盖, 这里只验证 UI 把值正确回传。
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, expect, it, vi } from 'vitest'
import { FilterPanel, SIGNAL_OPTIONS, defaultFilter, type ScreenerFilter } from './ScreenerFilter'

let root: Root | null = null
const container = document.createElement('div')
document.body.appendChild(container)
afterEach(() => {
  if (root) { act(() => { root!.unmount() }); root = null }
  container.innerHTML = ''
})

function render(value: ScreenerFilter, onChange = vi.fn()) {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  root = createRoot(container)
  act(() => {
    root!.render(
      <FilterPanel value={value} onChange={onChange} onClose={() => {}} onReset={() => {}} />,
    )
  })
  return onChange
}

const buttonByText = (text: string) =>
  [...container.querySelectorAll('button')].find(b => b.textContent === text)

it('渲染新增的数值维度', () => {
  render(defaultFilter)
  for (const label of ['换手率', '振幅', '成交量', '连板', '距60日高']) {
    expect(container.textContent).toContain(label)
  }
})

it('渲染全部技术信号开关', () => {
  render(defaultFilter)
  for (const sig of SIGNAL_OPTIONS) {
    expect(container.textContent).toContain(sig.label)
  }
})

it('点击信号按钮把 key 加入 signals', () => {
  const onChange = render(defaultFilter)
  const first = SIGNAL_OPTIONS[0]
  act(() => { buttonByText(first.label)!.click() })
  expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ signals: [first.key] }))
})

it('再次点击同一信号会移除', () => {
  const first = SIGNAL_OPTIONS[0]
  const onChange = render({ ...defaultFilter, signals: [first.key] })
  act(() => { buttonByText(first.label)!.click() })
  expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ signals: [] }))
})

it('激活计数把「信号」与新增维度都算进去', () => {
  render({
    ...defaultFilter,
    signals: ['signal_limit_up'],
    turnoverMin: '3',
  })
  const badge = [...container.querySelectorAll('span')].find(s => s.className.includes('rounded-full'))
  expect(badge?.textContent).toBe('2')
})
