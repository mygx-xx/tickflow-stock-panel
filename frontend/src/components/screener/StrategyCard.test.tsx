// @vitest-environment jsdom
// 分钟策略卡片数字未出时的占位引导: awaitRun → 「待计算」; computing → 脉冲 ···。
// 分钟策略不在 run_all/盘后缓存覆盖内 (Screener.tsx dailyPoolIds 过滤), 首屏
// 数字必为空, 纯空白无引导会让用户以为坏了 — 占位明确引导点击单跑。
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, expect, it, vi } from 'vitest'
import { StrategyCard } from './StrategyCard'

const noop = () => {}
const base = {
  name: '分钟突破', description: '', source: 'custom' as const,
  active: false, loading: false, cardSize: 'normal' as const,
  onRun: noop, disabled: false, onSettings: noop,
  timeframeBadge: '分钟',
}

let root: Root | null = null
const container = document.createElement('div')
document.body.appendChild(container)
afterEach(() => {
  if (root) { act(() => { root!.unmount() }); root = null }
  container.innerHTML = ''
})

function render(props: Record<string, unknown>) {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  root = createRoot(container)
  act(() => { root!.render(<StrategyCard {...base} {...props} />) })
}

it('shows 待计算 hint when awaitRun and count is missing', () => {
  render({ awaitRun: true })

  expect(container.textContent).toContain('待计算')
  expect(container.textContent).not.toContain('···')
})

it('shows pulsing placeholder (not hint) when computing', () => {
  render({ awaitRun: true, computing: true })

  expect(container.textContent).toContain('···')
  expect(container.textContent).not.toContain('待计算')
})

it('shows nothing extra for daily strategies without awaitRun', () => {
  render({})

  expect(container.textContent).not.toContain('待计算')
  expect(container.textContent).not.toContain('···')
})

it('hint yields to the real count once available', () => {
  render({ awaitRun: true, count: 7 })

  expect(container.textContent).not.toContain('待计算')
  expect(container.textContent).toContain('7')
})

it('mini size also renders the hint', () => {
  render({ awaitRun: true, cardSize: 'mini' })

  expect(container.textContent).toContain('待算')
})

// 缺数据态: 策略本轮跑挂 (如引用了面板未提供的数据列) 时后端给的 total 只是占位 0,
// 显示成「0」会被读成「今日无命中」。error 必须顶掉数字、失效数与计算占位, 原因收进 title。
it('error replaces the placeholder count and puts the reason in the tooltip', () => {
  const reason = '策略引用了面板未提供的数据列 "pb_latest"'
  render({ count: 0, expiredCount: 3, error: reason })

  expect(container.textContent).toContain('缺数据')
  expect(container.textContent).not.toContain('0')
  expect(container.textContent).not.toContain('-3')
  const badge = [...container.querySelectorAll('span')].find(el => el.textContent === '缺数据')
  expect(badge?.getAttribute('title')).toBe(reason)
})

it('error wins over computing and awaitRun placeholders in mini size', () => {
  render({ cardSize: 'mini', count: 0, error: '缺少列', computing: true, awaitRun: true })

  expect(container.textContent).toContain('缺数据')
  expect(container.textContent).not.toContain('···')
  expect(container.textContent).not.toContain('待算')
})

it('count renders normally when there is no error', () => {
  render({ count: 12 })

  expect(container.textContent).toContain('12')
  expect(container.textContent).not.toContain('缺数据')
})

it('onRun fires when the card is clicked', () => {
  const onRun = vi.fn()
  render({ awaitRun: true, onRun })

  act(() => { container.querySelector('button')!.click() })

  expect(onRun).toHaveBeenCalledTimes(1)
})

// 生命周期徽标: draft 是默认态(本项目 177 个策略全为 draft), 若也渲染徽标会把
// 整个策略列表刷满「未激活」; 只有非默认态才需要露出。
it.each([
  ['active', '已激活'],
  ['watch', '观察中'],
  ['retired', '已归档'],
])('renders the lifecycle badge for status=%s', (status, label) => {
  render({ status })

  expect(container.textContent).toContain(label)
})

it('hides the badge for draft (default status)', () => {
  render({ status: 'draft' })

  expect(container.textContent).not.toContain('未激活')
})

it('hides the badge when status is absent', () => {
  render({})

  expect(container.textContent).not.toContain('未激活')
  expect(container.textContent).not.toContain('已激活')
})

it('prefers the backend status_label as the badge tooltip', () => {
  render({ status: 'watch', statusLabel: '观察中: 绩效衰退待复核' })

  expect(container.querySelector('[title="观察中: 绩效衰退待复核"]')).not.toBeNull()
})
