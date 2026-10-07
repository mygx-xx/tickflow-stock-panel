#!/usr/bin/env bash
# ============================================================
#开发 worktree 隔离初始化脚本
#
# 用途: 为 tickflow-stock-panel 创建互相隔离的开发 worktree。
# 关键点: git worktree 只隔离「代码」, 不隔离 .env / data/ / .venv /
#       node_modules —— 这些是 gitignore 的, 每个新 worktree 初始时
#       全都缺失, 必须显式重建, 否则共用主树目录 = 数据串写 + 互相 kill。
#
# 隔离策略(与主树相比):
#   .env              复制并改写 PORT / DATA_DIR     (绝不复用主树端口)
#   data/ 大只读行情   目录联接(JUNCTION) 到主树      (省1.5G, 只读共享安全)
#   data/ 可写状态     独立目录(不链)                (paper账务/cache/job 隔离)
#   backend/.venv     独立 uv sync                   (依赖随分支变化)
#   frontend/node_modules  独立 pnpm install
#
# 用法:
#   scripts/dev-worktree.sh create <分支名> [目录名]
#   scripts/dev-worktree.sh list
#   scripts/dev-worktree.sh check <目录名>
#   scripts/dev-worktree.sh remove <目录名>
# ============================================================

set -euo pipefail

# ---------- 主树定位: 本脚本在主树的 scripts/ 下 ----------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------- 隔离默认值 ----------
DEFAULT_BACKEND_PORT=3028
DEFAULT_FRONTEND_PORT=3021

# 只读行情目录: 体积大 + 只读, 联接共享。这些目录在 worktree 内不会被写。
#   kline_minute 886M / kline_daily_enriched 370M / kline_daily 155M
#   kline_index_enriched 54M / kline_index_daily 40M
# 注意: 新增只读目录时必须先确认不会被写盘, 否则会污染主树数据。
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
  trading_calendar.json
  capabilities.json
)

# 明确不联接的目录(可写状态, 必须各自独立 —— 串写会导致账务/任务错乱):
#   paper/         模拟盘账户、订单、fills 台账、净值
#   job_store/     挖掘/回测任务状态
#   cache/         运行时缓存
#   strategies/    用户策略(已gitignore部分)
#   user_data/     用户配置
#   pools/ screener_results/ backtest_results/ research/ regime_history/
#   mainline_history/ ai_cache/ data_sources/ auction_benchmark/
#   ext_data/ depth5/ kline_ext/ kline_etf_*/

log()  { printf '\033[0;36m[worktree]\033[0m %s\n' "$*"; }
ok()   { printf '\033[0;32m  ok\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m  !!\033[0m  %s\n' "$*"; }
die()  { printf '\033[0;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# ============================================================
# create
# ============================================================
cmd_create() {
  local branch="${1:?用法: create <分支名> [目录名]}"
  local dirname="${2:-${branch//\//-}}"
  local wt_dir="$MAIN_ROOT/../tickflow-wt-$dirname"

  log "主树: $MAIN_ROOT"
  log "目标 worktree: $wt_dir"
  log "分支: $branch"

  # 分支已存在? 复用而不是报错(git worktree add 会拒绝已存在的分支)
  if git show-ref --verify --quiet "refs/heads/$branch"; then
    warn "分支 $branch 已存在, 将直接挂载该分支"
    git worktree add "$wt_dir" "$branch"
  else
    git worktree add -b "$branch" "$wt_dir"
  fi

  # ---------- .env ----------
  # 复制主树 .env 后改写端口与 DATA_DIR。绝不让两个 worktree 监听同一端口,
  # dev.ps1 的 Free-Port 会 taskkill /F /T 占用端口的活进程 —— 会直接杀掉另一个 worktree。
  if [[ -f "$MAIN_ROOT/.env" ]]; then
    cp "$MAIN_ROOT/.env" "$wt_dir/.env"
    # 替换 PORT= 行(而不是追加 —— 后出现的会覆盖前面的, 容易埋雷)
    if grep -qE '^PORT=' "$wt_dir/.env"; then
      sed -i "s/^PORT=.*/PORT=$DEFAULT_BACKEND_PORT/" "$wt_dir/.env"
    else
      printf '\nPORT=%s\n' "$DEFAULT_BACKEND_PORT" >> "$wt_dir/.env"
    fi
    ok ".env 已复制, PORT -> $DEFAULT_BACKEND_PORT"
  else
    warn "主树无 .env, 新 worktree 需自行创建"
  fi

  # ---------- data/ ----------
  setup_data_dirs "$wt_dir"

  # ---------- 依赖 ----------
  # .venv / node_modules 体积大且随分支变化, 各自独立。
  # dev.ps1 首次运行会自动 uv sync / pnpm install, 这里只给出提示。
  log "依赖: 首次执行该 worktree 的 dev.ps1 会自动 uv sync + pnpm install"
  log "启动: cd '$wt_dir' && .\\dev.ps1 -BackendPort $DEFAULT_BACKEND_PORT -FrontendPort $DEFAULT_FRONTEND_PORT"

  cat <<EOF

$(ok "worktree 就绪")

  目录      $wt_dir
  分支      $branch
  后端端口  $DEFAULT_BACKEND_PORT   (主树 3018)
  前端端口  $DEFAULT_FRONTEND_PORT   (主树 3011)

  data/ 策略:
    联接共享(只读):$(printf '%s ' "${SHARED_DATA_DIRS[@]}")
    独立(可写):     paper job_store cache user_data pools strategies ...

  启动命令(必须显式传端口, 不要依赖 .env 默认值):
    cd "$wt_dir"
    .\\dev.ps1 -BackendPort $DEFAULT_BACKEND_PORT -FrontendPort $DEFAULT_FRONTEND_PORT
EOF
}

# ============================================================
# data 目录: 只读联接 + 可写独立
# ============================================================
setup_data_dirs() {
  local wt_dir="$1"
  local wt_data="$wt_dir/data"

  mkdir -p "$wt_data"

  # Windows 下用 NTFS 目录联接(Junction): 不需要管理员权限, 且对所有读盘路径
  # 透明(含 Python Path/glob), 比符号链接兼容性好。
  #
  # 注意: 绝不能用 `cmd //c mklink //J` —— Git Bash 的 MSYS 路径转换会把
  # `/J` 改写成 `D:\J` 之类的路径, cmd 报「无效开关」, 静默失败后脚本会
  # 退化成 cp -r(白拷 1.5G)。必须走 PowerShell 的 New-Item -ItemType Junction。
  if [[ "$(uname -s)" =~ MINGW|MSYS|CYGWIN ]]; then
    link_dir() {
      powershell -NoProfile -NonInteractive -Command \
        "New-Item -ItemType Junction -Path '$1' -Target '$2' -ErrorAction Stop | Out-Null" \
        >/dev/null 2>&1
    }
  else
    link_dir() { ln -s "$2" "$1"; }
  fi

  local linked=0 copied=0
  for d in "${SHARED_DATA_DIRS[@]}"; do
    local src="$MAIN_ROOT/data/$d"
    local dst="$wt_data/$d"
    [[ -e "$src" ]] || continue
    # 已有联接 = 上次跑过, 跳过(联接不可用 -e 判断为假, 所以显式查 -L)
    if [[ -L "$dst" ]]; then
      linked=$((linked+1))
      continue
    fi
    if [[ -e "$dst" ]]; then
      warn "已存在实体, 跳过: $d"
      continue
    fi
    link_dir "$dst" "$src"
    # 不信link_dir 的返回值 —— 事后验证目标状态。
    # Junction 用[[ -L ]] 可判定; 万一退化成实体目录则删掉重来。
    if [[ -L "$dst" ]]; then
      linked=$((linked+1))
    else
      warn "联接失败, 退回复制: $d"
      [[ -e "$dst" ]] && rm -rf "$dst"
      cp -r "$src" "$dst"
      copied=$((copied+1))
    fi
  done
  ok "data/ 只读联接 $linked 个, 复制 $copied 个"

  # 可写状态目录: 建空壳让应用能正常写(首次运行会自行初始化 schema)
  local writable=(
    paper job_store cache user_data pools strategies screener_results
    backtest_results research regime_history mainline_history
    ai_cache data_sources auction_benchmark ext_data
  )
  local made=0
  for d in "${writable[@]}"; do
    [[ -e "$wt_data/$d" ]] && continue
    mkdir -p "$wt_data/$d"
    made=$((made+1))
  done
  ok "data/ 可写目录就绪 $made 个"

  # .env 的 DATA_DIR 指向本worktree 的 data(默认即是, 显式化更清晰)
  if [[ -f "$wt_dir/.env" ]] && grep -qE '^DATA_DIR=' "$wt_dir/.env"; then
    sed -i 's|^DATA_DIR=.*|DATA_DIR=./data|' "$wt_dir/.env"
    ok "DATA_DIR -> ./data (本 worktree)"
  fi
}

# ============================================================
# list / check / remove
# ============================================================
cmd_list() {
  log "worktree 列表:"
  git worktree list
  echo
  log "端口占用:"
  for p in 3018 3011 3028 3021; do
    local_free=$(powershell -NoProfile -Command \
      "if (Get-NetTCPConnection -State Listen -LocalPort $p -EA SilentlyContinue) {'IN USE'} else {'free'}" \
      2>/dev/null | tr -d '\r')
    printf '  %-6s %s\n' "$p" "${local_free:-?}"
  done
}

cmd_check() {
  local dirname="${1:?用法: check <目录名>}"
  local wt_dir="$MAIN_ROOT/../tickflow-wt-$dirname"
  [[ -d "$wt_dir" ]] || die "worktree 不存在: $wt_dir"

  log "检查隔离状态: $wt_dir"

  # .env 端口
  if [[ -f "$wt_dir/.env" ]]; then
    local p; p="$(grep -E '^PORT=' "$wt_dir/.env" | cut -d= -f2)"
    if [[ "$p" == "3018" ]]; then
      warn "PORT=3018 与主树冲突! 必须改"
    else
      ok "PORT=$p"
    fi
  else
    warn "缺 .env"
  fi

  # data 联接状态
  echo
  log "data/ 联接检查:"
  for d in "${SHARED_DATA_DIRS[@]}"; do
    local dst="$wt_dir/data/$d"
    if [[ -L "$dst" ]]; then
      ok "  $d -> 联接"
    elif [[ -d "$dst" ]]; then
      printf '  \033[0;33m  !!\033[0m  %s 实体目录(占空间, 建议联接)\n' "$d"
    fi
  done

  # 可写目录必须是实体, 不能是联接 —— 这是隔离的关键
  echo
  log "data/ 可写目录(必须是实体, 联接=串写风险):"
  for d in paper job_store cache user_data; do
    local dst="$wt_dir/data/$d"
    if [[ -L "$dst" ]]; then
      warn "  $d 是联接 —— 会与主树串写, 必须删掉重建实体目录"
    elif [[ -d "$dst" ]]; then
      ok "  $d 独立"
    else
      warn "  $d 缺失"
    fi
  done
}

cmd_remove() {
  local dirname="${1:?用法: remove <目录名>}"
  local wt_dir="$MAIN_ROOT/../tickflow-wt-$dirname"
  [[ -d "$wt_dir" ]] || die "worktree 不存在: $wt_dir"

  warn "删除 worktree: $wt_dir"
  warn "注意: data/ 里的联接与可写状态目录会一并删除, 不可恢复"
  read -r -p "确认? 输入 yes 继续: " ans
  [[ "$ans" == "yes" ]] || { log "已取消"; return 0; }

  # 删联接要用 rmdir, 否则 git 会跟进真实目录内容
  if [[ -L "$wt_dir/data/kline_daily_enriched" ]]; then
    cmd //c rmdir "$wt_dir\data\kline_daily_enriched" >/dev/null 2>&1 || true
  fi
  git worktree remove --force "$wt_dir"
  ok "已移除"
}

case "${1:-}" in
  create) shift; cmd_create "$@" ;;
  list)   cmd_list ;;
  check)  shift; cmd_check "$@" ;;
  remove) shift; cmd_remove "$@" ;;
  *)      sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
esac
