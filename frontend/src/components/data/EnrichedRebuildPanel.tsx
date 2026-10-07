import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Gauge, Loader2 } from 'lucide-react'
import { api, type EnrichedRebuildPlan } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { usePreferences } from '@/lib/useSharedQueries'
import { cn } from '@/lib/cn'

type Props = {
  isRunning: boolean
  onStart: (jobId: string) => void
  purpose?: 'enriched' | 'turnover'
  historicalShareRows?: number
}

const MODE_META: Record<EnrichedRebuildPlan['mode'], { label: string; cls: string }> = {
  noop:    { label: '无需重算', cls: 'text-muted' },
  forward: { label: '增量计算', cls: 'text-emerald-400' },
  local:   { label: '局部重算', cls: 'text-sky-400' },
  full:    { label: '全量重算', cls: 'text-amber-400' },
}

export function EnrichedRebuildPanel({
  isRunning,
  onStart,
  purpose = 'enriched',
  historicalShareRows = 0,
}: Props) {
  const qc = useQueryClient()
  const prefs = usePreferences()
  const batchSize = prefs.data?.enriched_batch_size ?? 1000
  const isTurnoverRebuild = purpose === 'turnover'
  const canRebuild = !isTurnoverRebuild || historicalShareRows > 0
  const [editing, setEditing] = useState(false)
  const [draftSize, setDraftSize] = useState(String(batchSize))
  const [hint, setHint] = useState<string | null>(null)
  // 换手率重算依赖历史股本, 语义上必须覆盖全部分区, 不走智能模式
  const supportsSmart = !isTurnoverRebuild

  // 先探测计划再决定要不要执行 —— 直接点"全量计算"会在已对齐的库上白跑 49 秒
  const planQ = useQuery({
    queryKey: [...QK.enrichedRebuildPlan, isTurnoverRebuild],
    queryFn: () => api.enrichedRebuildPlan('plan'),
    enabled: supportsSmart,
    staleTime: 30_000,
    retry: false,
  })
  const plan = supportsSmart ? planQ.data?.plan : undefined
  const modeMeta = plan ? MODE_META[plan.mode] : null

  function clampAndSave(raw: number) {
    if (isNaN(raw) || 1 > raw) { setHint('已自动设为最小值 1'); saveBatch.mutate(1); return }
    if (raw > 10000) { setHint('已自动设为上限 10000'); saveBatch.mutate(10000); return }
    setHint(null); saveBatch.mutate(raw)
  }

  const saveBatch = useMutation({
    mutationFn: (size: number) => api.updateEnrichedBatchSize(size),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: QK.preferences })
      setEditing(false)
    },
  })
  const rebuild = useMutation({
    // smart = 按探测结果走最省方式; full = 强制全量重写
    mutationFn: (smart: boolean) => api.rebuildEnriched(smart ? 'auto' : 'full'),
    onSuccess: ({ job_id }) => {
      onStart(job_id)
      qc.invalidateQueries({ queryKey: QK.pipelineJobs })
      qc.invalidateQueries({ queryKey: [...QK.enrichedRebuildPlan, isTurnoverRebuild] })
    },
  })

  return (
    <div className="px-4 pb-4 pt-3 border-t border-accent/20 space-y-4">
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-xs font-medium text-foreground">批次大小</div>
            <div className="text-[10px] text-muted">每批计算的标的数量，影响内存占用与进度粒度</div>
          </div>
          {editing ? (
            <div className="flex items-center gap-1.5">
              <input
                type="number"
                value={draftSize}
                onChange={e => setDraftSize(e.target.value)}
                className="w-20 px-2 py-1 text-xs font-mono rounded-btn border border-border bg-surface text-foreground text-right tabular-nums focus:outline-none focus:border-accent"
                min={1}
                max={10000}
                autoFocus
                onKeyDown={e => {
                  if (e.key === 'Enter') clampAndSave(parseInt(draftSize))
                  if (e.key === 'Escape') { setEditing(false); setHint(null) }
                }}
              />
              <button
                onClick={() => clampAndSave(parseInt(draftSize))}
                disabled={saveBatch.isPending}
                className="px-2 py-1 text-[10px] rounded-btn bg-accent/15 text-accent hover:bg-accent/25 disabled:opacity-50 transition-colors"
              >
                {saveBatch.isPending ? '…' : '保存'}
              </button>
              <button
                onClick={() => setEditing(false)}
                className="px-2 py-1 text-[10px] rounded-btn bg-elevated text-muted hover:text-foreground transition-colors"
              >
                取消
              </button>
            </div>
          ) : (
            <button
              onClick={() => { setDraftSize(String(batchSize)); setEditing(true) }}
              className="px-2.5 py-1 rounded-btn border border-border bg-surface text-xs font-mono text-foreground hover:border-accent/50 transition-colors tabular-nums"
            >
              {batchSize} 只/批
            </button>
          )}
        </div>
        <div className="flex items-start gap-1.5 px-3 py-1.5 rounded-btn bg-warning/10 border border-warning/20">
          <span className="text-[10px] text-warning leading-relaxed">
            每批内存占用 = 批次大小 × 日K历史天数。批次越大或日K历史越长，内存占用越高，可能导致程序崩溃。内存不足时请适当降低此值。
          </span>
        </div>
        {hint && (
          <div className="px-3 py-1 rounded-btn bg-accent/10 border border-accent/20 text-[10px] text-accent">
            {hint}
          </div>
        )}
      </div>

      <div>
        {isTurnoverRebuild && (
          <div className={`mb-3 rounded-btn border px-3 py-2 text-[10px] leading-relaxed ${
            canRebuild
              ? 'border-accent/20 bg-accent/5 text-secondary'
              : 'border-warning/20 bg-warning/10 text-warning'
          }`}>
            {canRebuild
              ? `已检测到 ${historicalShareRows.toLocaleString()} 条历史股本记录。重算会按公告可用日匹配历史流通股本，并覆盖全部 Enriched 分区。`
              : '未检测到历史股本数据，请先在财务分析页面同步“股本表”，再执行重算。'}
          </div>
        )}
        <div className="text-[10px] text-muted mb-2">
          {isTurnoverRebuild
            ? '为保证数据一致性，将基于现有日 K、除权因子和历史股本重新生成 Enriched；其他指标也会按当前逻辑同步更新。'
            : '先比对 kline_daily 与 enriched 的实际差异，只补真正缺失的部分（已对齐则不做任何操作）'}
        </div>

        {/* 计划卡: 执行前先说清"要做什么、为什么、代价多大" */}
        {supportsSmart && (
          <div className="mb-2 rounded-btn border border-border bg-elevated/40 px-3 py-2">
            {planQ.isPending && (
              <div className="text-[10px] text-muted">正在比对差异…</div>
            )}
            {planQ.isError && (
              <div className="text-[10px] text-danger">
                差异探测失败：{String((planQ.error as Error)?.message ?? planQ.error)}
              </div>
            )}
            {plan && modeMeta && (
              <>
                <div className="flex items-center justify-between gap-2">
                  <span className={cn('inline-flex items-center gap-1 text-[10px] font-medium', modeMeta.cls)}>
                    <Gauge className="h-3 w-3" />
                    将执行：{modeMeta.label}
                  </span>
                  <span className="font-mono text-[10px] text-muted">{plan.savings}</span>
                </div>
                <div className="mt-1 text-[10px] leading-relaxed text-muted" title={plan.reason}>
                  {plan.reason}
                </div>
                <div className="mt-1 font-mono text-[9px] text-muted/70">
                  daily {plan.daily_days} 天 / enriched {plan.enriched_days} 天
                  {plan.missing_total > 0 && ` · 缺 ${plan.missing_total} 天`}
                  {plan.orphan_total > 0 && ` · 孤儿 ${plan.orphan_total} 天`}
                </div>
              </>
            )}
          </div>
        )}

        <div className="flex gap-1.5">
          <button
            onClick={() => rebuild.mutate(true)}
            disabled={!canRebuild || isRunning || rebuild.isPending}
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-3 py-1.5 rounded-btn bg-accent/90 text-white text-xs font-medium hover:bg-accent disabled:opacity-40 disabled:pointer-events-none transition-colors duration-150"
          >
            {rebuild.isPending ? (
              <><Loader2 className="h-3 w-3 animate-spin" />计算中…</>
            ) : isTurnoverRebuild ? (
              '重新计算并覆盖'
            ) : (
              '按需重算'
            )}
          </button>
          {/* 全量重写是真实存在的运维需求(怀疑历史数据算错时), 但不该是默认路径 */}
          {!isTurnoverRebuild && (
            <button
              onClick={() => rebuild.mutate(false)}
              disabled={!canRebuild || isRunning || rebuild.isPending}
              title="忽略差异检测，强制重写全部分区"
              className="shrink-0 px-2.5 py-1.5 rounded-btn border border-border bg-surface text-xs font-medium text-muted hover:text-amber-400 hover:border-amber-400/40 disabled:opacity-40 disabled:pointer-events-none transition-colors duration-150"
            >
              强制全量
            </button>
          )}
        </div>
        {rebuild.isError && (
          <div className="mt-2 text-[10px] text-danger">
            启动失败：{String((rebuild.error as Error)?.message ?? rebuild.error)}
          </div>
        )}
      </div>
    </div>
  )
}
