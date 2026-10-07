// @vitest-environment jsdom
// 生命周期展示层: 徽标可见性 + 迁移图(后端 TRANSITIONS 的前端镜像)。
//
// 迁移图这份镜像**必须与后端 `app/strategy/lifecycle.TRANSITIONS` 完全一致** ——
// 一旦漂移, 前端要么放出后端必然 409 的按钮(用户点了报错), 要么把合法迁移禁掉
// (功能静默消失)。故这里把完整边集写死做守卫, 改后端时必须同步改这里。
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, expect, it } from 'vitest'
import {
  STRATEGY_STATUS_META,
  STRATEGY_STATUS_ORDER,
  STRATEGY_STATUS_TRANSITIONS,
  StrategyStatusBadge,
  asStrategyStatus,
  canTransitionStatus,
} from './StrategyStatusBadge'

let root: Root | null = null
const container = document.createElement('div')
document.body.appendChild(container)
afterEach(() => {
  if (root) { act(() => { root!.unmount() }); root = null }
  container.innerHTML = ''
})

function render(props: Record<string, unknown>) {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  // 同一 container 上二次 createRoot 会触发 React 警告 —— 先卸载并清空。
  if (root) { act(() => { root!.unmount() }); root = null }
  container.innerHTML = ''
  root = createRoot(container)
  act(() => { root!.render(<StrategyStatusBadge {...props} />) })
}

// ===== 迁移图 =====

it('迁移图与后端 TRANSITIONS 逐边一致', () => {
  // 后端 app/strategy/lifecycle.py::TRANSITIONS
  expect(STRATEGY_STATUS_TRANSITIONS).toEqual({
    draft: ['active', 'watch', 'retired'],
    active: ['draft', 'watch', 'retired'],
    watch: ['active', 'retired'],
    retired: ['draft'],
  })
})

it('active 可一步撤回 draft (2026-10-07 放开的边)', () => {
  expect(canTransitionStatus('active', 'draft')).toBe(true)
  expect(STRATEGY_STATUS_TRANSITIONS.active).toContain('draft')
})

it('迁移图四个状态全覆盖, 且无边指向非法状态或自身', () => {
  expect(Object.keys(STRATEGY_STATUS_TRANSITIONS).sort()).toEqual(
    [...STRATEGY_STATUS_ORDER].sort(),
  )
  for (const [src, targets] of Object.entries(STRATEGY_STATUS_TRANSITIONS)) {
    for (const t of targets) {
      expect(STRATEGY_STATUS_ORDER).toContain(t)
      expect(t, `${src} 不应能迁回自身`).not.toBe(src)
    }
  }
})

it('retired 是终态, 只能回 draft', () => {
  expect(canTransitionStatus('retired', 'draft')).toBe(true)
  expect(canTransitionStatus('retired', 'active')).toBe(false)
  expect(canTransitionStatus('retired', 'watch')).toBe(false)
})

it('原地不动不算合法迁移', () => {
  for (const st of STRATEGY_STATUS_ORDER) {
    expect(canTransitionStatus(st, st)).toBe(false)
  }
})

it('asStrategyStatus 把缺失/非法值按 draft 处理', () => {
  expect(asStrategyStatus(undefined)).toBe('draft')
  expect(asStrategyStatus(null)).toBe('draft')
  expect(asStrategyStatus('bogus')).toBe('draft')
  expect(asStrategyStatus('ACTIVE')).toBe('draft') // 后端 normalize 才是大小写容错层
  expect(asStrategyStatus('watch')).toBe('watch')
})

// ===== 徽标 =====

it('非默认态渲染徽标', () => {
  for (const [st, meta] of Object.entries(STRATEGY_STATUS_META)) {
    if (st === 'draft') continue
    render({ status: st })
    expect(container.textContent).toContain(meta.label)
  }
})

it('draft 默认不出徽标(否则整列表被「未激活」刷屏)', () => {
  render({ status: 'draft' })
  expect(container.textContent).toBe('')
})

it('hideDraft={false} 时 draft 也出徽标', () => {
  render({ status: 'draft', hideDraft: false })
  expect(container.textContent).toContain(STRATEGY_STATUS_META.draft.label)
})

it('无 status 时不渲染', () => {
  render({})
  expect(container.textContent).toBe('')
})

it('tooltip 优先用后端下发的 status_label', () => {
  render({ status: 'watch', statusLabel: '观察中: 绩效衰退待复核' })
  expect(container.querySelector('[title="观察中: 绩效衰退待复核"]')).not.toBeNull()
})

it('四态都有 label/dot/desc 元数据', () => {
  for (const st of STRATEGY_STATUS_ORDER) {
    const meta = STRATEGY_STATUS_META[st]
    expect(meta.label).toBeTruthy()
    expect(meta.dot).toBeTruthy()
    expect(meta.desc).toBeTruthy()
  }
})
