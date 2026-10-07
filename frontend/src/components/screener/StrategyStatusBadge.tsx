import type { StrategyStatus } from '@/lib/api'

/**
 * 策略生命周期状态的展示元数据 + 徽标组件。
 *
 * 状态语义由后端 `app/strategy/lifecycle.py` 定义(四态 draft/active/watch/
 * retired), 这里只做「文案 + 配色 + 迁移图」的前端镜像。**迁移图必须与后端
 * `TRANSITIONS` 保持一致**, 否则前端会放出后端必然 409 的按钮。
 */

export const STRATEGY_STATUS_META: Record<StrategyStatus, {
  /** 徽标短标签 */
  label: string
  /** 徽标配色 */
  cls: string
  /** 状态点配色 */
  dot: string
  /** 状态说明(与后端 describe_status 同义, 用于面板文案) */
  desc: string
}> = {
  draft: {
    label: '未激活',
    cls: 'bg-secondary/10 text-muted border-border',
    dot: 'bg-muted',
    desc: '草稿: 未经检验; 仍可手动跑, 但不算作已验证策略',
  },
  active: {
    label: '已激活',
    cls: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30',
    dot: 'bg-emerald-400',
    desc: '已激活: 检验通过, 视为可信策略',
  },
  watch: {
    label: '观察中',
    cls: 'bg-amber-400/10 text-amber-400 border-amber-400/30',
    dot: 'bg-amber-400',
    desc: '观察中: 绩效衰退待复核',
  },
  retired: {
    label: '已归档',
    cls: 'bg-danger/10 text-danger border-danger/30',
    dot: 'bg-danger',
    desc: '已归档: 不可执行(单跑 409 / 批量跳过), 需先迁回草稿',
  },
}

/** 四态顺序(面板渲染用) */
export const STRATEGY_STATUS_ORDER: StrategyStatus[] = ['draft', 'active', 'watch', 'retired']

/**
 * 合法迁移图 — 后端 `app.strategy.lifecycle.TRANSITIONS` 的前端镜像。
 * 单向衰减、人工可回升: 自动判定只允许 active → watch, 前端不做自动迁移,
 * 只负责把非法按钮禁掉。
 *
 * `active → draft`(人工撤回误激活)在 2026-10-07 放开, 与后端同步。
 */
export const STRATEGY_STATUS_TRANSITIONS: Record<StrategyStatus, StrategyStatus[]> = {
  draft: ['active', 'watch', 'retired'],
  active: ['draft', 'watch', 'retired'],
  watch: ['active', 'retired'],
  retired: ['draft'],
}

/** 后端未下发 status 时按 draft 处理(与后端 normalize_status 的默认一致) */
export function asStrategyStatus(raw?: string | null): StrategyStatus {
  return raw === 'active' || raw === 'watch' || raw === 'retired' ? raw : 'draft'
}

/** 该迁移是否合法(剔除「原地不动」) */
export function canTransitionStatus(from: string | undefined, to: StrategyStatus): boolean {
  const cur = asStrategyStatus(from)
  if (cur === to) return false
  return STRATEGY_STATUS_TRANSITIONS[cur].includes(to)
}

interface BadgeProps {
  status?: string
  /** 后端下发的完整说明, 优先作为 tooltip */
  statusLabel?: string
  /** draft 是默认态, 默认不渲染徽标, 避免整列表被「未激活」刷屏 */
  hideDraft?: boolean
  className?: string
}

/** 生命周期状态徽标。返回 null 表示无需渲染。 */
export function StrategyStatusBadge({ status, statusLabel, hideDraft = true, className = '' }: BadgeProps) {
  if (!status) return null
  if (hideDraft && asStrategyStatus(status) === 'draft') return null
  const meta = STRATEGY_STATUS_META[asStrategyStatus(status)]
  return (
    <span
      title={statusLabel ?? meta.desc}
      className={`inline-flex shrink-0 items-center gap-1 rounded border px-1 py-px text-[9px] font-medium leading-tight ${meta.cls} ${className}`}
    >
      <span className={`h-1 w-1 shrink-0 rounded-full ${meta.dot}`} />
      {meta.label}
    </span>
  )
}
