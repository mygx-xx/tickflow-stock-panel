import { useState } from 'react'
import { AlertTriangle, X } from 'lucide-react'

/**
 * 数据充足性提示 (#303) 的收集与展示。
 *
 * 后端在 enriched 覆盖低于暖机窗口时给出人话警告
 * (`ScreenerService.coverage_warnings`), 但过去只有 `/run`、`/run_preset` 会
 * 带上它, 而策略页首屏走的是缓存端点 —— 薄库时页面只显示「今日无命中」,
 * 用户得自己跑去数据页排查, 正好是这个提示要解决的场景。
 *
 * 同一段文案会在多个策略里重复 (每条策略各算过一次), 所以按文案去重。
 */

/** 汇总单个策略结果与缓存结果的提示, 按文案去重、保持首次出现顺序。 */
export function collectCoverageWarnings(
  perStrategy: Record<string, { warnings?: string[] }> | null | undefined,
  single: string[] | null | undefined,
): string[] {
  const seen = new Set<string>()
  const lists: (string[] | undefined)[] = [single ?? undefined]
  for (const r of Object.values(perStrategy ?? {})) lists.push(r?.warnings)
  for (const list of lists) {
    for (const w of list ?? []) {
      if (w) seen.add(w)
    }
  }
  return [...seen]
}

interface Props {
  warnings: string[]
  /**
   * 上下文签名 (如 `2026-09-30:all`)。用户关掉提示后, 只要签名或文案变了
   * 就会重新显示 —— 否则换日期/换策略后新的数据不足提示会被静默吞掉。
   */
  signature?: string
}

export function CoverageWarningBanner({ warnings, signature = '' }: Props) {
  const [dismissedKey, setDismissedKey] = useState<string | null>(null)
  if (warnings.length === 0) return null

  const key = `${signature}\u0000${warnings.join('\u0000')}`
  if (dismissedKey === key) return null

  return (
    <div className="flex items-start gap-2 rounded-btn border border-amber-400/25 bg-amber-400/[0.06] px-3 py-2">
      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-400" />
      <div className="min-w-0 flex-1 space-y-0.5">
        <div className="text-[11px] font-medium text-amber-400">数据不足 — 结果可能失真或全部落空</div>
        {warnings.map(w => (
          <div key={w} className="text-[11px] leading-4 text-muted">{w}</div>
        ))}
      </div>
      <button
        onClick={() => setDismissedKey(key)}
        title="关闭提示"
        aria-label="关闭数据不足提示"
        className="shrink-0 cursor-pointer rounded p-0.5 text-muted transition-colors hover:bg-elevated hover:text-foreground"
      >
        <X className="h-3.5 w-3.5" />
      </button>
    </div>
  )
}
