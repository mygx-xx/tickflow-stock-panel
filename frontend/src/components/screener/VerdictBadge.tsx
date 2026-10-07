import { BadgeCheck, CircleSlash, CircleHelp } from 'lucide-react'
import type { StrategyVerdictItem } from '@/lib/api'
import { cn } from '@/lib/cn'

/**
 * 策略自述验证结论徽标。
 *
 * 重要口径: 这里显示的是 **策略清单文件里人工/脚本汇总的自述结论**,
 * 不是本系统跑的回测。所以文案用「清单结论」而**不用「已验证」** ——
 * verified 标记也只呈现为「清单重点关注」, 避免被误读成系统判定。
 */

type Verdict = StrategyVerdictItem['verdict']

const VERDICT_META: Record<Verdict, { label: string; cls: string; Icon: typeof BadgeCheck }> = {
  screened: {
    label: '粗筛通过',
    cls: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/25',
    Icon: BadgeCheck,
  },
  failed: {
    label: '清单判失败',
    cls: 'bg-secondary/10 text-muted/80 border-border',
    Icon: CircleSlash,
  },
  untagged: {
    label: '未标注',
    cls: 'bg-secondary/10 text-muted/60 border-border',
    Icon: CircleHelp,
  },
}

/** 完整提示文案: 说清来源 + 证据 + 未登记措辞 */
export function verdictTooltip(item: StrategyVerdictItem): string {
  const base = VERDICT_META[item.verdict].label
  const parts = [`清单结论: ${base}`]
  if (item.verified) parts.push('清单「已验证可用」表列名')
  if (item.evidence) parts.push(`证据: ${item.evidence}`)
  if (item.note) parts.push(`原文措辞: ${item.note}`)
  parts.push('来源: data/strategies/custom/README-策略清单.md（自述结论，非本系统回测）')
  return parts.join('\n')
}

/**
 * 结论徽标。untagged 默认不渲染 —— 152 个策略里 65 个未标注,
 * 全挂出来只会制造噪音, 需要时用 prop 强制显示。
 *
 * 文案取舍: verified 且底类为 untagged 时显示「清单重点」而不是「未标注」——
 * 这类策略在清单「全部策略」表里确实标的是未标注, 但被单列进了「已验证可用」
 * (清单用两张表表达两件事)。此时"未标注"配绿色框会让人以为矛盾。
 */
export function VerdictBadge({
  item,
  showUntagged = false,
  className,
}: {
  item: StrategyVerdictItem | undefined
  showUntagged?: boolean
  className?: string
}) {
  if (!item) return null
  if (!item.verified && item.verdict === 'untagged' && !showUntagged) return null

  const promoted = item.verified && item.verdict === 'untagged'
  const { label, cls, Icon } = VERDICT_META[promoted ? 'screened' : item.verdict]
  return (
    <span
      title={verdictTooltip(item)}
      className={cn(
        'inline-flex shrink-0 items-center gap-0.5 rounded border px-1 py-px text-[9px] font-medium leading-tight',
        cls,
        item.verified && 'ring-1 ring-emerald-400/40',
        className,
      )}
    >
      <Icon className="h-2.5 w-2.5" />
      {promoted ? '清单重点' : label}
    </span>
  )
}

export { VERDICT_META }
