// @vitest-environment jsdom
// 自述验证结论徽标的可见性与文案。
//
// 两条容易做错的语义, 这里各自锚定:
// 1. verdict 是三类互斥值, verified 是**叠加**标记 —— 所以「已验证 + 未标注」是合法组合,
//    不能因为 verified 就断言显示「粗筛通过」(那会谎报类别)。
// 2. 文案必须如实说是**清单里的自述结论**, 不是本系统回测 —— 否则用户会误信。
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, expect, it } from 'vitest'
import { VerdictBadge, verdictTooltip } from './VerdictBadge'
import type { StrategyVerdictItem } from '@/lib/api'

let root: Root | null = null
const container = document.createElement('div')
document.body.appendChild(container)
afterEach(() => {
  if (root) { act(() => { root!.unmount() }); root = null }
  container.innerHTML = ''
})

function render(props: { item: StrategyVerdictItem | undefined; showUntagged?: boolean }) {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true })
  if (root) { act(() => { root!.unmount() }); root = null }
  container.innerHTML = ''
  root = createRoot(container)
  act(() => { root!.render(<VerdictBadge {...props} />) })
}

function item(over: Partial<StrategyVerdictItem> = {}): StrategyVerdictItem {
  return { verdict: 'untagged', verified: false, evidence: '', note: '', ...over }
}

const text = () => container.textContent ?? ''
const hasRing = () => !!container.querySelector('.ring-emerald-400\\/40')

it('无数据时渲染为空', () => {
  render({ item: undefined })
  expect(container.innerHTML).toBe('')
})

it('未标注且未列入已验证表时默认不渲染(152 个里 65 个, 全挂出来是噪音)', () => {
  render({ item: item() })
  expect(container.innerHTML).toBe('')
})

it('showUntagged 强制显示未标注', () => {
  render({ item: item(), showUntagged: true })
  expect(text()).toContain('未标注')
})

it('粗筛通过渲染绿字徽标', () => {
  render({ item: item({ verdict: 'screened' }) })
  expect(text()).toContain('粗筛通过')
  expect(hasRing()).toBe(false)          // 未列入已验证表 → 不该有"重点"描边
})

it('清单判失败渲染低对比样式', () => {
  render({ item: item({ verdict: 'failed' }) })
  expect(text()).toContain('清单判失败')
})

it('已验证叠加在粗筛通过上: 仍显示真实类别, 并加重点描边', () => {
  render({ item: item({ verdict: 'screened', verified: true }) })
  expect(text()).toContain('粗筛通过')
  expect(text()).not.toContain('清单重点')
  expect(hasRing()).toBe(true)
})

it('已验证但类别是未标注: 显示「清单重点」而非矛盾的「未标注」', () => {
  render({ item: item({ verified: true, evidence: '超额 +5.13pp' }) })
  expect(text()).toContain('清单重点')
  expect(text()).not.toContain('未标注')
  expect(hasRing()).toBe(true)
})

it('tooltip 说明来源是清单自述而非本系统回测, 并带出证据', () => {
  const tip = verdictTooltip(item({ verdict: 'screened', verified: true, evidence: '夏普1.4' }))
  expect(tip).toContain('自述结论，非本系统回测')
  expect(tip).toContain('夏普1.4')
  expect(tip).toContain('清单「已验证可用」表列名')
})

it('tooltip 带出未登记措辞原文供人工核对', () => {
  const tip = verdictTooltip(item({ note: '某句没登记' }))
  expect(tip).toContain('原文措辞: 某句没登记')
})

it('title 属性挂上完整说明(悬停可见)', () => {
  render({ item: item({ verdict: 'screened', evidence: '超额 +34.05%' }) })
  const el = container.querySelector('span')
  expect(el?.getAttribute('title')).toContain('超额 +34.05%')
})
