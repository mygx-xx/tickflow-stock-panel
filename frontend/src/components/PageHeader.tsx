import { cn } from '@/lib/cn'

interface Props {
  title: string
  subtitle?: React.ReactNode
  /** 标题右侧、subtitle 之前的额外节点(如状态徽标) */
  titleExtra?: React.ReactNode
  right?: React.ReactNode
  className?: string
}

export function PageHeader({ title, subtitle, titleExtra, right, className }: Props) {
  return (
    <header
      className={cn(
        // 标题行与控件行默认各占一行 (控件组近 1100px, 与标题挤一行必然换行错乱);
        // 仅超宽屏 (≥1800px) 才合并为单行两端对齐。
        // 标题行左侧留出悬浮汉堡按钮的空间; sm 起恢复 px-5。
        'flex flex-wrap items-center justify-between gap-x-4 gap-y-2 border-b border-border pb-2 pt-3 pl-12 pr-4 sm:px-5',
        className,
      )}
    >
      <div className="flex w-full min-w-0 items-center gap-2 min-[1800px]:w-auto">
        <h1 className="shrink-0 text-lg font-semibold tracking-tight">{title}</h1>
        {titleExtra}
        {subtitle && <span className="hidden min-w-0 truncate text-xs text-muted sm:inline">{subtitle}</span>}
      </div>
      {right}
    </header>
  )
}
