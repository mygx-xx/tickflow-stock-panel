/**
 * 系统自检 — 一屏回答「服务在不在 / 数据新不新 / 策略有没有加载错 / 缓存多久没更新」。
 *
 * 排查思路的收敛: 此前要分别看 /health、/api/data/status、/api/strategies 的
 * load_errors、策略缓存文件的 updated_at, 再人工对比时间戳。现在后端一次聚合,
 * 且任一子项失败只标记该项, 不会让整页挂掉。
 */
import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Activity, AlertTriangle, CheckCircle2, Database, Download, Layers, RefreshCw, Save, Timer, Wifi } from 'lucide-react'
import { api, type DiagnosticsReport } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { cn } from '@/lib/cn'
import { downloadJson } from '@/lib/download'
import { PageHeader } from '@/components/PageHeader'

function fmtUptime(sec: number): string {
  if (sec < 60) return `${sec} 秒`
  if (sec < 3600) return `${Math.floor(sec / 60)} 分 ${sec % 60} 秒`
  const h = Math.floor(sec / 3600)
  return `${h} 小时 ${Math.floor((sec % 3600) / 60)} 分`
}

function Card({ title, icon: Icon, children, tone = 'default' }: {
  title: string
  icon: React.ComponentType<{ className?: string }>
  children: React.ReactNode
  tone?: 'default' | 'warn' | 'bad'
}) {
  return (
    <div className={cn(
      'rounded-card border bg-surface p-3',
      tone === 'warn' ? 'border-amber-500/40' : tone === 'bad' ? 'border-danger/50' : 'border-border',
    )}>
      <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-foreground">
        <Icon className={cn('h-3.5 w-3.5', tone === 'bad' ? 'text-danger' : tone === 'warn' ? 'text-amber-400' : 'text-accent')} />
        {title}
      </div>
      <div className="space-y-1 text-[11px] leading-relaxed">{children}</div>
    </div>
  )
}

function Row({ k, v, tone }: { k: string; v: React.ReactNode; tone?: 'bull' | 'bear' | 'muted' }) {
  return (
    <div className="flex items-baseline justify-between gap-3">
      <span className="shrink-0 text-muted">{k}</span>
      <span className={cn(
        'truncate text-right font-mono',
        tone === 'bull' ? 'text-bull' : tone === 'bear' ? 'text-bear' : tone === 'muted' ? 'text-muted' : 'text-foreground',
      )} title={typeof v === 'string' ? v : undefined}>
        {v}
      </span>
    </div>
  )
}

function ErrNote({ text }: { text?: string }) {
  if (!text) return null
  return <div className="rounded bg-danger/10 px-1.5 py-1 text-[10px] text-danger">{text}</div>
}

export function Diagnostics() {
  const qc = useQueryClient()
  const q = useQuery({ queryKey: QK.diagnostics, queryFn: api.diagnostics })
  const [exporting, setExporting] = useState(false)
  const [exportMsg, setExportMsg] = useState<string | null>(null)

  // 策略备份: data/strategies 被 .gitignore 排除, 本机磁盘是唯一副本,
  // 误删或磁盘故障就全没了 —— 这里给一个随手可点的灾备出口。
  const exportAll = async () => {
    setExporting(true)
    setExportMsg(null)
    try {
      const bundle = await api.strategyBundle()
      const stamp = bundle.exported_at.slice(0, 19).replace(/[:T]/g, '-')
      const size = downloadJson(bundle, `tickflow-strategies-${stamp}.json`)
      setExportMsg(
        `已导出 ${bundle.count} 个策略 · ${(size / 1024).toFixed(0)} KB` +
        (bundle.code_missing.length ? ` · ${bundle.code_missing.length} 个缺源码` : ''),
      )
    } catch (e) {
      setExportMsg(`导出失败: ${String((e as Error)?.message ?? e)}`)
    } finally {
      setExporting(false)
    }
  }

  const d: DiagnosticsReport | undefined = q.data
  const lagTone = (lag?: number | null) => (lag == null ? 'muted' : lag > 10 ? 'bear' : lag > 4 ? 'muted' : undefined)

  return (
    <>
      <PageHeader
        title="系统自检"
        subtitle="服务 · 数据 · 策略 · 缓存 · 监控"
        right={
          <div className="flex w-full flex-wrap items-center justify-start gap-2 min-[1800px]:ml-auto min-[1800px]:w-auto">
            <button
              onClick={() => qc.invalidateQueries({ queryKey: QK.diagnostics })}
              className="inline-flex items-center gap-1.5 rounded-btn border border-border bg-surface px-2.5 py-1 text-xs text-muted transition-colors hover:border-accent/50 hover:text-accent"
            >
              <RefreshCw className={cn('h-3.5 w-3.5', q.isFetching && 'animate-spin')} />
              重新检查
            </button>
          </div>
        }
      />

      <div className="h-full min-h-0 overflow-y-auto p-4">
        {q.isLoading && <div className="py-16 text-center text-xs text-muted">检查中…</div>}
        {q.isError && (
          <div className="rounded-card border border-danger/40 bg-danger/5 p-4 text-xs text-danger">
            自检接口不可用 · {String((q.error as Error)?.message ?? q.error)}
            <div className="mt-1 text-muted">后端可能未启动, 或 /api/diagnostics 未注册</div>
          </div>
        )}

        {d && (
          <div className="space-y-3">
            {/* 总状态条 */}
            <div className={cn(
              'flex flex-wrap items-center gap-2 rounded-card border px-3 py-2',
              d.healthy ? 'border-emerald-500/40 bg-emerald-500/5' : 'border-danger/50 bg-danger/5',
            )}>
              {d.healthy
                ? <CheckCircle2 className="h-4 w-4 text-emerald-400" />
                : <AlertTriangle className="h-4 w-4 text-danger" />}
              <span className={cn('text-sm font-medium', d.healthy ? 'text-emerald-400' : 'text-danger')}>
                {d.healthy ? '全部正常' : `${d.failures.length} 项异常`}
              </span>
              <span className="font-mono text-[10px] text-muted">
                v{d.server.app_version} · Python {d.server.python} · 运行 {fmtUptime(d.server.uptime_sec)} · 检查于 {d.server.checked_at.slice(11, 19)}
              </span>
              {d.data.problems.map(p => (
                <span key={p} className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] text-amber-400">{p}</span>
              ))}
            </div>

            <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
              <Card title="服务" icon={Activity}>
                <ErrNote text={d.server.error} />
                <Row k="应用版本" v={d.server.app_version} />
                <Row k="Python" v={d.server.python} />
                <Row k="已运行" v={fmtUptime(d.server.uptime_sec)} />
                <Row k="检查时间" v={d.server.checked_at.slice(11, 19)} />
              </Card>

              <Card title="数据" icon={Database} tone={d.data.problems.length ? 'warn' : 'default'}>
                <ErrNote text={d.data.error} />
                <Row k="最新交易日" v={d.data.latest_date ?? '—'} />
                <Row k="距今" v={d.data.lag_days == null ? '—' : `${d.data.lag_days} 天`} tone={lagTone(d.data.lag_days) as any} />
                <Row k="数据集" v={`${d.data.datasets.length} 个`} />
                <div className="pt-1">
                  {d.data.datasets.slice(0, 6).map(x => (
                    <div key={x.name} className="flex justify-between gap-2 text-muted">
                      <span className="truncate">{x.name}</span>
                      <span className="shrink-0 font-mono text-[10px]">{x.symbols ?? '—'} 只 · {x.latest_date ?? '—'}</span>
                    </div>
                  ))}
                </div>
              </Card>

              <Card title="策略" icon={Layers} tone={d.strategies.load_errors.length ? 'bad' : 'default'}>
                <ErrNote text={d.strategies.error} />
                <Row k="总数" v={d.strategies.total} />
                {Object.entries(d.strategies.by_status).map(([k, n]) => (
                  <Row key={k} k={k} v={n} tone={k === 'draft' ? 'muted' : undefined} />
                ))}
                <Row k="可选" v={d.strategies.selectable} tone={d.strategies.selectable ? undefined : 'muted'} />
                <Row k="内置 / 自定义" v={`${d.strategies.by_source.builtin ?? 0} / ${d.strategies.by_source.custom ?? 0}`} />
                <div className="pt-1">
                  <button
                    onClick={exportAll}
                    disabled={exporting}
                    className="inline-flex w-full items-center justify-center gap-1.5 rounded-btn border border-border bg-surface px-2 py-1 text-[11px] text-muted transition-colors hover:border-accent/50 hover:text-accent disabled:opacity-50"
                  >
                    <Download className="h-3 w-3" />
                    {exporting ? '导出中…' : '导出全部策略(含源码)'}
                  </button>
                  <div className="mt-1 text-[10px] text-muted/70">
                    策略文件不在版本控制内, 定期导出留档
                  </div>
                  {exportMsg && (
                    <div className={cn('mt-1 text-[10px]', exportMsg.startsWith('导出失败') ? 'text-danger' : 'text-emerald-400')}>
                      {exportMsg}
                    </div>
                  )}
                </div>
                {d.strategies.load_errors.length > 0 && (
                  <div className="mt-1 rounded bg-danger/10 px-1.5 py-1 text-[10px] text-danger">
                    加载错误 {d.strategies.load_errors.length} 条: {d.strategies.load_errors.join('; ')}
                  </div>
                )}
              </Card>

              <Card title="策略缓存" icon={Save} tone={d.cache.exists ? 'default' : 'warn'}>
                <ErrNote text={d.cache.error} />
                {d.cache.exists ? (
                  <>
                    <Row k="数据日期" v={d.cache.as_of ?? '—'} />
                    <Row k="落后" v={d.cache.lag_days == null ? '—' : `${d.cache.lag_days} 天`} tone={lagTone(d.cache.lag_days) as any} />
                    <Row k="更新时间" v={d.cache.age_minutes == null ? '—' : `${d.cache.age_minutes} 分钟前`} tone={(d.cache.age_minutes ?? 0) > 120 ? 'bear' : undefined} />
                    <Row k="缓存策略" v={d.cache.strategies ?? '—'} />
                    <Row k="曾命中" v={`${d.cache.ever_matched_symbols ?? 0} 只次`} />
                    <Row k="文件大小" v={`${d.cache.file_mb ?? 0} MB`} />
                  </>
                ) : (
                  <div className="text-muted">{d.cache.note ?? '缓存为空 — 策略页会回退到全量重算'}</div>
                )}
              </Card>

              <Card title="监控规则" icon={Wifi} tone={d.monitor.strategy_rules ? 'default' : 'warn'}>
                <ErrNote text={d.monitor.error} />
                <Row k="规则总数" v={d.monitor.rules} />
                <Row k="已启用" v={d.monitor.enabled} />
                <Row k="策略类" v={d.monitor.strategy_rules} tone={d.monitor.strategy_rules ? undefined : 'muted'} />
                {Object.entries(d.monitor.by_type).map(([k, n]) => <Row key={k} k={`· ${k}`} v={n} />)}
                {d.monitor.strategy_rules === 0 && (
                  <div className="mt-1 text-[10px] text-muted">策略页「全部开启」可批量挂上策略类规则</div>
                )}
              </Card>

              <Card title="自动跟单" icon={Timer} tone={d.auto_follow.rule_files ? 'default' : 'warn'}>
                <ErrNote text={d.auto_follow.error} />
                <Row k="模拟盘账户" v={d.auto_follow.accounts} />
                <Row k="跟单规则" v={d.auto_follow.rule_files} tone={d.auto_follow.rule_files ? undefined : 'muted'} />
                {d.auto_follow.rule_files === 0 && (
                  <div className="mt-1 text-[10px] text-muted">
                    未配置跟单规则 — 策略命中只会告警, 不会自动下单
                  </div>
                )}
              </Card>
            </div>
          </div>
        )}
      </div>
    </>
  )
}
