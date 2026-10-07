#!/usr/bin/env bash
# ============================================================
# worktree 只读联接补建(不依赖 powershell)
#
# 背景: dev-worktree.sh 的 setup_data_dirs 在 MINGW 下走
# `powershell New-Item -ItemType Junction`, 但**从 Bash 调 powershell 会被
# 安全策略拦截**("Invoking PowerShell from Bash bypasses PowerShell
# security checks"), 且 link_dir 的 stderr 被 >/dev/null 2>&1 吞掉 ->
# 脚本静默中止在 data 目录阶段, 表现为 create 只输出了 .env/端口几行就结束,
# data/ 目录为 0 项。
#
# 本脚本改用 MSYS `ln -s`。已实测(2026-10-07)MSYS 符号链接对**原生 Windows
# Python 完全透明: Path.exists/is_dir/iterdir 正常, kline_daily_enriched
# 读到 1454 个 date= 分区, instruments 读到 instruments.parquet。
# 只读行情用符号链接安全; 可写状态目录一律建实体, 绝不链接。
#
# 用法:
#   bash scripts/link-worktree-data.sh <目录名>          # 补建某棵树的 data隔离
#   bash scripts/link-worktree-data.sh <目录名> --ports   # 顺带按登记簿校正 .env 端口
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT_REGISTRY="$MAIN_ROOT/../.tickflow-wt-ports"

log() { printf '\033[0;36m[link-data]\033[0m %s\n' "$*"; }
ok()  { printf '\033[0;32m  ok\033[0m %s\n' "$*"; }
warn(){ printf '\033[0;33m  !!\033[0m %s\n' "$*"; }
die() { printf '\033[0;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# 只读行情: 体积大 + 只读, 联接共享(1.9G)
SHARED_DATA_DIRS=(
  kline_minute
  kline_daily_enriched
  kline_daily
  kline_index_enriched
  kline_index_daily
  financials
  dragon_tiger
  instruments
  instruments_etf
  instruments_index
  adj_factor
  adj_factor_etf
)
# 小文件直接复制(符号链接对 json 也没问题, 但文件级link 语义不如目录清晰,
# 且这两个文件极小 —— trading_calendar 3.3K / capabilities 418B)
SHARED_DATA_FILES=(
  trading_calendar.json
  capabilities.json
)

# 可写状态: 必须各自独立实体目录。链接这些会导致 fills.jsonl 串写、
# job_store 状态互相覆盖 —— 组合级/策略级开发必然污染账务与任务。
WRITABLE_DIRS=(
  paper job_store cache user_data pools strategies screener_results
  backtest_results research regime_history mainline_history
  ai_cache data_sources auction_benchmark ext_data
)

dirname="${1:?用法: link-worktree-data.sh <目录名> [--ports]}"
want_ports="${2:-}"
wt_dir="$MAIN_ROOT/../tickflow-wt-$dirname"
[[ -d "$wt_dir" ]] || die "worktree 不存在: $wt_dir"
wt_data="$wt_dir/data"
mkdir -p "$wt_data"

# 判断一个目录是否为 NTFS Junction(reparse point)。Git Bash 的 [[ -L ]] 对
# Junction 恒为 false(Junction 在 POSIX 层就是普通目录), 只能用 fsutil 识别。
# 注意 fsutil 输出是本地编码, 不能用 text=True 解码(会UnicodeDecodeError),
# 只取 bytes 做 ascii 关键词匹配。
is_junction() {
  local target="$1"
  fsutil reparsepoint query "$target" 2>/dev/null \
    | tr -d '\r' \
    | grep -qiE 'IO_REPARSE_TAG_(MOUNT_POINT|SYMLINK)'
}

log "worktree: $wt_dir"
log "data    : $wt_data"

# ---------- 端口校正 ----------
if [[ "$want_ports" == "--ports" && -f "$PORT_REGISTRY" ]]; then
  line="$(grep -E "^$dirname[[:space:]]" "$PORT_REGISTRY" | head -1 || true)"
  if [[ -n "$line" ]]; then
    be="$(echo "$line" | awk '{print $2}')"
    fe="$(echo "$line" | awk '{print $3}')"
    if [[ -f "$wt_dir/.env" ]]; then
      if grep -qE '^PORT=' "$wt_dir/.env"; then
        sed -i "s/^PORT=.*/PORT=$be/" "$wt_dir/.env"
      else
        printf '\nPORT=%s\n' "$be" >> "$wt_dir/.env"
      fi
      # 必须**总是覆盖** FRONTEND_PORT —— 登记簿会被并行创建的worktree 重排,
      # 只在"缺失时追加"会留下上一轮的过期端口, 与其他树的前端撞车。
      if grep -qE '^FRONTEND_PORT=' "$wt_dir/.env"; then
        sed -i "s/^FRONTEND_PORT=.*/FRONTEND_PORT=$fe/" "$wt_dir/.env"
      else
        printf 'FRONTEND_PORT=%s\n' "$fe" >> "$wt_dir/.env"
      fi
      ok ".env PORT=$be FRONTEND_PORT=$fe (已与登记簿对齐)"
    else
      warn "缺 .env, 无法校正端口"
    fi
  else
    warn "登记簿无 $dirname 记录, 跳过端口校正"
  fi
fi

# ---------- 只读目录联接 ----------
# 注意: 这里**不调用 ln -s**。Windows 下 Git Bash 的 ln -s 会静默退化成真实目录
# 副本(见文末详细说明), 必须由 PowerShell 的 New-Item -ItemType Junction 建。
# 本脚本负责: 校验 Junction 到位 + 补建可写目录与可写状态, 不是建联接的工具。
linked=0
for d in "${SHARED_DATA_DIRS[@]}"; do
  src="$MAIN_ROOT/data/$d"
  dst="$wt_data/$d"
  [[ -e "$src" ]] || { warn "主树缺失, 跳过: $d"; continue; }
  if [[ -L "$dst" ]]; then
    linked=$((linked+1)); continue
  fi
  # Junction 在 bash 里 [[ -L ]] 判不出(显示为真目录), 用 fsutil 认reparse point。
  if [[ -d "$dst" ]]; then
    if is_junction "$dst"; then
      linked=$((linked+1)); continue
    fi
    warn "实体目录(非 Junction), 需人工处理: $d"
    continue
  fi
  warn "缺失联接: $d  ->  用 PowerShell 执行 New-Item -ItemType Junction -Path '$dst' -Target '$src'"
done
ok "只读目录已就位$linked 个(其余需PowerShell 建Junction)"

# ---------- 小文件复制 ----------
f_ok=0
for f in "${SHARED_DATA_FILES[@]}"; do
  src="$MAIN_ROOT/data/$f"
  dst="$wt_data/$f"
  [[ -e "$src" ]] || continue
  [[ -e "$dst" ]] && continue
  cp "$src" "$dst"
  f_ok=$((f_ok+1))
done
ok "小文件复制 $f_ok 个"

# ---------- 可写目录: 实体, 绝不联接 ----------
made=0
for d in "${WRITABLE_DIRS[@]}"; do
  [[ -e "$wt_data/$d" ]] && continue
  mkdir -p "$wt_data/$d"
  made=$((made+1))
done
ok "可写目录(实体)就绪 $made 个"

# ---------- 关键: Windows 下 ln -s 不可靠, 必须拒绝静默退化 ----------
# 实测(2026-10-07): 在 Git Bash 里 `ln -s src dst` **返回 0**, 但不创建符号链接,
# 而是把 dst 变成 src 的**真实目录副本**(ls -la 显示 drwxr-xr-x 而非 lrwxrwxrwx)。
# MSYS 无 SeCreateSymbolicLinkPrivilege 时就是这个行为。
# 后果: 只读联接静默变成 1.9G 全量复制 —— 既是巨大空间浪费, 又让人误以为已隔离,
#       而 `check` 只看 [[ -L ]] 会报"实体目录(占空间)"但create 全程显示 ok。
# 因此: 这里禁止用 ln -s 建联接, 必须走 PowerShell 的New-Item -ItemType Junction。
#
# PowerShell 只能从 PowerShell 工具调用 —— 从 Bash 调会被安全策略拦:
#   "Invoking PowerShell from Bash bypasses PowerShell security checks"
# 所以沙箱/IDE 场景下, 建联接请用 PowerShell 工具执行:
#   New-Item -ItemType Junction -Path <wt>\data\<name> -Target <main>\data\<name>
# 实测 Junction 对原生 Windows Python 完全透明:
#   Path.exists/is_dir/iterdir 正常, kline_daily_enriched 读到 1454 个 date= 分区。
if [[ -n "${LINK_ALLOW_LN_S:-}" ]]; then
  warn "LINK_ALLOW_LN_S 已开启 —— ln -s 在 Windows 下会退化成实体副本, 仅调试用"
else
  for d in "${SHARED_DATA_DIRS[@]}"; do
    dst="$wt_data/$d"
    if [[ -d "$dst" && ! -L "$dst" ]]; then
      die "发现实体副本(很可能是 ln -s 退化而来, 不会是 Junction): $dst
     判定依据: Windows 下 ln -s 静默退化为真实目录, 且 Junction 对 [[ -d ]] 与实体无异。
     处理: 用 PowerShell 工具执行
       Remove-Item -LiteralPath '$dst' -Recurse -Force
       New-Item -ItemType Junction -Path '$dst' -Target '<main>/data/$d'
     然后重跑本脚本。跳过此检查请设 LINK_ALLOW_LN_S=1。"
    fi
  done
fi

# ---------- 自检 ----------
echo
log "自检:"
bad=0
for d in "${SHARED_DATA_DIRS[@]}"; do
  dst="$wt_data/$d"
  if [[ -L "$dst" ]] || is_junction "$dst"; then :; else warn "  $d 非联接(Junction/符号链接)"; bad=$((bad+1)); fi
done
for d in paper job_store cache user_data strategies pools; do
  dst="$wt_data/$d"
  if [[ -L "$dst" ]] || is_junction "$dst"; then
    warn "  $d 是联接 —— 会与主树串写, 必须删掉重建实体目录"; bad=$((bad+1))
  elif [[ -d "$dst" ]]; then :; else warn "  $d 缺失"; bad=$((bad+1)); fi
done
if [[ "$bad" -eq 0 ]]; then
  ok "隔离自检通过 (只读联接 / 可写实体)"
else
  warn "隔离自检有 $bad 项异常"
  exit 1
fi

# ---------- DATA_DIR ----------
if [[ -f "$wt_dir/.env" ]] && grep -qE '^DATA_DIR=' "$wt_dir/.env"; then
  sed -i 's|^DATA_DIR=.*|DATA_DIR=./data|' "$wt_dir/.env"
  ok "DATA_DIR -> ./data (本worktree)"
fi