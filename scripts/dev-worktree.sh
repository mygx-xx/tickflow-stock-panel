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
#   scripts/dev-worktree.sh create <分支名> [目录名] [-b 后端端口] [-f 前端端口]
#   scripts/dev-worktree.sh list
#   scripts/dev-worktree.sh check <目录名>
#   scripts/dev-worktree.sh remove <目录名>
# ============================================================

set -euo pipefail

# ---------- 主树定位 ----------
# 本脚本会被复制到每个 worktree 后运行, 因此不能靠脚本自身位置推导主树 ——
# 在 worktree 里跑时 SCRIPT_DIR 指向的是该 worktree, MAIN_ROOT 会变成它自己,
# 导致 glob 把自己的 .env 匹配两次(list 报假冲突) 且主树端口判定失效。
#
# 权威判据是 `git worktree list` 的**第一个条目**: git 始终把主树列在最前。
# 不用「同级目录里有 .git」来猜 —— 同级可能存在其他 git 项目(如 AxData),
# 会抓错目标。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_SELF_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
_self_base="$(basename "$_SELF_ROOT")"
if [[ "$_self_base" == tickflow-wt-* ]]; then
  # 在 worktree 内: 借 git 查主树 (worktree list 首行即主树)
  MAIN_ROOT="$(git -C "$_SELF_ROOT" rev-parse --path-format=absolute \
    --git-common-dir 2>/dev/null | sed 's/\/\.git$//' || true)"
  if [[ -z "$MAIN_ROOT" || ! -d "$MAIN_ROOT" ]]; then
    MAIN_ROOT="$(git -C "$_SELF_ROOT" worktree list --porcelain 2>/dev/null \
      | head -1 | awk '{print $1}')"
  fi
  MAIN_ROOT="${MAIN_ROOT:-$(cd "$_SELF_ROOT/.." && pwd)}"
else
  MAIN_ROOT="$_SELF_ROOT"
fi

# ---------- 隔离默认值 ----------
MAIN_BACKEND_PORT=3018
MAIN_FRONTEND_PORT=3011

# 端口不再硬编码 —— 硬编码会导致第二个 worktree 与第一个撞端口,
# 而 dev.ps1 的 Free-Port 会 taskkill /F /T 占用端口的活进程
# (不是启动失败, 是把另一个 worktree 的后端直接杀掉)。
# 每次 create 自动向后扫描空闲端口, 并把结果写进 <worktree>/.dev-ports。
PORT_SCAN_STEP=10
PORT_SCAN_TRIES=12

# 自动扫描起点。起点只决定「从哪里开始找」, 真正的占用判定在collect_taken_ports ——
# 它同时查 netstat 监听 + 登记簿 + 各 worktree 的 .env PORT, 所以即使几棵树都没启动,
# 也不会全部拿到同一端口(2026-10-07 三棵树同时拿到 3038 就是只查监听导致的)。
DEFAULT_SCAN_START=3048

# 端口登记簿: 记录每个 worktree 实际占用的端口, 供 list/check 展示。
PORT_REGISTRY="$MAIN_ROOT/../.tickflow-wt-ports"

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
  # regime/phase 的**唯一数据源**(part.parquet, 含 state/score/phase 全序列)。
  # 必须联接共享: 独立成空目录时, 新 worktree 的 regime/phase 全空,
  # 市场环境页与「阶段接入挖掘/回测」这类开发都无从下手(2026-10-07 踩到)。
  # 写盘入口已逐一核实(2026-10-07), 共两个, 都是「全量 upsert」语义:
  #   1) api/regime.py:161 upsert_regime_history   ← recompute 接口
  #   2) api/regime.py:170 upsert_mainline_history ← recompute 同函数内 concept/industry 双写
  # regime_builder.py:549 的实现是「读旧 → 按 date anti-join → concat → 写回同一文件」,
  # 所以**联接状态下重算会改写主树 parquet**, 且新列会以 null 自动补齐
  # (schema 随代码演进而静默迁移, 主树可能突然多出几列)。
  # → 结论: 联接期间严禁在任何树点「重算市场环境」。只读取证无风险。
  # 需要重算时: 先删本树联接(变独立实体目录)再算, 或接受主树被同步更新。
  regime_history
  mainline_history
)

# 文件(非目录)无法做 Junction —— 只能复制。
# 这两个是运行时重写的派生文件, 共享会串写, 必须各自独立:
#   trading_calendar.json  交易日历, 会被日历更新流程重写
#   capabilities.json      能力清单, 启动时按当前配置重算(实测频繁变动)
# 体积都在 KB 级, 复制成本可忽略。
COPIED_DATA_FILES=(
  trading_calendar.json
  capabilities.json
)

# 明确不联接的目录(可写状态, 必须各自独立 —— 串写会导致账务/任务错乱):
#   paper/         模拟盘账户、订单、fills 台账、净值
#   job_store/     挖掘/回测任务状态
#   cache/         运行时缓存
#   strategies/    用户策略(已gitignore部分)
#   user_data/     用户配置
#   pools/ screener_results/ backtest_results/ research/
#   ai_cache/ data_sources/ auction_benchmark/
#   ext_data/ depth5/ kline_ext/ kline_etf_*/
#
# 注意 regime_history / mainline_history 已上移到SHARED_DATA_DIRS:
# 它们是regime/phase 的唯一数据源, 独立成空目录会让新树无法开发。

log()  { printf '\033[0;36m[worktree]\033[0m %s\n' "$*"; }
ok()   { printf '\033[0;32m  ok\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m  !!\033[0m  %s\n' "$*"; }
die()  { printf '\033[0;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# 判断目录是否为 NTFS 联接(Junction / symlink reparse point)。
# 关键: Git Bash 的 `[[ -L ]]` 对 Junction **恒为 false** —— Junction 在 POSIX
# 层就呈现为普通目录。只用 [[ -L ]] 会让 check 把"已建好的联接"误报成
# "实体目录(占空间)", 也让幂等跳过失效(重复跑 create 会重复建)。
# fsutil 输出是本地编码, 不能带 text 解码, 只取 bytes 做 ascii 匹配。
is_link() {
  local t="$1"
  [[ -L "$t" ]] && return 0
  fsutil reparsepoint query "$t" 2>/dev/null \
    | tr -d '\r' \
    | grep -qiE 'IO_REPARSE_TAG_(MOUNT_POINT|SYMLINK)'
}

# ============================================================
# 端口工具
# ============================================================

# 判断端口是否被监听。Git Bash 下不能调powershell(会被安全策略拦),
# 用 netstat 解析, 纯 bash 实现。
port_busy() {
  local p="$1"
  netstat -ano 2>/dev/null \
    | tr -d '\r' \
    | grep -E "[:.]$p[[:space:]]" \
    | grep -q 'LISTENING'
}

# 收集"已被占用"的端口号(每行一个) —— 两个来源:
#   1) 正在监听的(netstat)
#   2) 已分配但尚未启动的(登记簿 + 各 worktree 的 .env PORT)
# 只查(1) 是不够的: 并行/连续建树时各树都没在跑, 会全部拿到同一端口,
# 表现为"第二个启动把第一个的后端静默杀掉"而不是启动失败, 极难排查。
# 这正是 2026-10-07 三棵树同时拿到 3038 的原因。
collect_taken_ports() {
  # (1) 正在监听
  netstat -ano 2>/dev/null | tr -d '\r' \
    | grep 'LISTENING' \
    | sed -E 's/.*[:.]([0-9]+)[[:space:]].*/\1/'
  # (2a) 登记簿
  if [[ -f "$PORT_REGISTRY" ]]; then
    awk 'NF>=3 {print $2; print $3}' "$PORT_REGISTRY" 2>/dev/null
  fi
  # (2b) 各 worktree .env 里实际写着的 PORT(含主树)
  local f
  for f in "$MAIN_ROOT/.env" "$MAIN_ROOT"/../tickflow-wt-*/.env; do
    [[ -f "$f" ]] || continue
    grep -E '^PORT=' "$f" 2>/dev/null | cut -d= -f2
  done
}

# 从start 起按步长扫描, 找第一个同时满足 backend/frontend 都空闲的端口对。
# 返回 "BACKEND FRONTEND"; 找不到则 die。
alloc_ports() {
  local start="$1"
  # 可选第2 参数: 忽略白名单(空格分隔的端口, 通常是本树自己已登记的)
  local ignore="${2:-}"
  local taken; taken="$(collect_taken_ports)"
  local b f
  for ((i = 0; i < PORT_SCAN_TRIES; i++)); do
    b=$((start + i * PORT_SCAN_STEP))
    f=$((b + 3))   # 3028 -> 3031, 3038 -> 3041, 与既有约定一致(+3)
    # 主树端口永远跳过
    [[ "$b" == "$MAIN_BACKEND_PORT" || "$f" == "$MAIN_FRONTEND_PORT" ]] && continue
    # 已被监听/已分配则跳过; 白名单内的除外(重建本树时可复用)
    if grep -qxE "$b" <<<"$taken" && ! grep -qwE "$b" <<<"$ignore"; then continue; fi
    if grep -qxE "$f" <<<"$taken" && ! grep -qwE "$f" <<<"$ignore"; then continue; fi
    printf '%s %s' "$b" "$f"
    return 0
  done
  die "从 $start 起扫描 ${PORT_SCAN_TRIES} 次未找到空闲端口对, 请用 -b/-f 显式指定"
}

# 登记端口到登记簿: "<dirname> <backend> <frontend>"
register_ports() {
  local dirname="$1" b="$2" f="$3"
  mkdir -p "$(dirname "$PORT_REGISTRY")"
  # 先移除同名旧记录(重复 create 时覆盖)
  grep -vE "^$dirname[[:space:]]" "$PORT_REGISTRY" 2>/dev/null > "$PORT_REGISTRY.tmp" || true
  printf '%s %s %s\n' "$dirname" "$b" "$f" >> "$PORT_REGISTRY.tmp"
  mv "$PORT_REGISTRY.tmp" "$PORT_REGISTRY"
}

# 读取登记的端口: "<backend> <frontend>"; 无记录则回落到自动扫描。
registered_ports() {
  local dirname="$1"
  if [[ -f "$PORT_REGISTRY" ]]; then
    local line
    line="$(grep -E "^$dirname[[:space:]]" "$PORT_REGISTRY" 2>/dev/null | head -1 || true)"
    if [[ -n "$line" ]]; then
      printf '%s %s' "$(echo "$line" | awk '{print $2}')" "$(echo "$line" | awk '{print $3}')"
      return 0
    fi
  fi
  alloc_ports "$DEFAULT_SCAN_START" "$("$PORT_REGISTRY" >/dev/null 2>&1; grep -E "^$dirname[[:space:]]" "$PORT_REGISTRY" 2>/dev/null | awk '{print $2, $3}')"
}

# ============================================================
# create
# ============================================================
cmd_create() {
  local branch="" dirname="" want_b="" want_f=""

  # 位置参数 + 可选 -b/-f
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -b|--backend-port) want_b="${2:?-b 需要端口号}"; shift 2 ;;
      -f|--frontend-port) want_f="${2:?-f 需要端口号}"; shift 2 ;;
      -*) die "未知参数: $1" ;;
      *)
        if [[ -z "$branch" ]]; then branch="$1"
        elif [[ -z "$dirname" ]]; then dirname="$1"
        else die "多余的位置参数: $1"
        fi
        shift ;;
    esac
  done

  [[ -n "$branch" ]] || die "用法: create <分支名> [目录名] [-b 后端端口] [-f 前端端口]"
  dirname="${dirname:-${branch//\//-}}"
  local wt_dir="$MAIN_ROOT/../tickflow-wt-$dirname"

  # 端口: 已存在的 worktree **必须沿用它自己的端口**, 否则每重跑一次
  # create 就换一个端口, 登记簿被反复改写, 之前记下的启动命令全部失效。
  local be_port fe_port
  local existing_ports=""
  if [[ -f "$wt_dir/.env" ]]; then
    existing_ports="$(grep -E '^PORT=' "$wt_dir/.env" | head -1 | cut -d= -f2)"
  fi

  if [[ -n "$want_b" || -n "$want_f" ]]; then
    be_port="${want_b:-$((want_f - 3))}"
    fe_port="${want_f:-$((be_port + 3))}"
    if port_busy "$be_port" || port_busy "$fe_port"; then
      warn "指定端口 $be_port/$fe_port 已被占用 —— 继续执行, 但启动时会杀掉占用进程"
    fi
  elif [[ -n "$existing_ports" ]]; then
    be_port="$existing_ports"
    fe_port="$((be_port + 3))"
    log "沿用该worktree 已有端口"
  else
    local scanned; scanned="$(alloc_ports "$DEFAULT_SCAN_START")"
    be_port="${scanned% *}"
    fe_port="${scanned#* }"
  fi

  log "主树: $MAIN_ROOT"
  log "目标 worktree: $wt_dir"
  log "分支: $branch"
  log "端口: backend=$be_port frontend=$fe_port (主树 $MAIN_BACKEND_PORT/$MAIN_FRONTEND_PORT)"

  # 目录已存在 => 该 worktree 已挂载过。
  # 这是常见场景(重跑 create 补建隔离), 不能 fatal 退出,
  # 否则用户无法在不删树的前提下重跑隔离初始化。
  #
  # 本机实测的三处格式差异(必须全部归一):
  #   git --porcelain : "d:/users/.../tickflow-wt-x"
  #   pwd -P          : "/d/users/.../tickflow-wt-x"
  # →差异1: git 无前导斜杠; 差异2: pwd 的盘符不带冒号(要补成 "d:")
  local norm_wt
  norm_wt="$( (cd "$wt_dir" 2>/dev/null && pwd -P) || printf '%s' "$wt_dir" )"
  norm_wt="$(printf '%s' "$norm_wt" | tr 'A-Z' 'a-z' | tr '\\' '/')"
  norm_wt="${norm_wt#/}"          # 去前导斜杠
  # 补盘符冒号: "d/users/..." -> "d:/users/..."
  # 注意不能用 ${var/:\//:} —— 该模式在 bash 里转义有歧义, 实测不生效。
  norm_wt="$(printf '%s' "$norm_wt" | sed -E 's|^([a-z])/|\1:/|')"
  local git_norm
  git_norm="$(git worktree list --porcelain \
    | sed -n 's/^[Ww][Oo][Rr][Kk][Tt][Rr][Ee][Ee] //p' \
    | tr 'A-Z' 'a-z' | tr '\\' '/')"
  if [[ -d "$wt_dir" ]] && printf '%s\n' "$git_norm" | grep -qxF "$norm_wt"; then
    warn "worktree 已存在, 跳过挂载, 仅重跑隔离初始化"
  elif [[ -e "$wt_dir" ]]; then
    die "目录已存在但不是 git worktree: $wt_dir
   请先手动清理该目录, 或换一个目录名(create <分支> <新目录名>)"
  elif git show-ref --verify --quiet "refs/heads/$branch"; then
    # 分支已存在但目录不存在 —— 挂载该分支
    warn "分支 $branch 已存在, 将直接挂载该分支"
    git worktree add "$wt_dir" "$branch"
  else
    git worktree add -b "$branch" "$wt_dir"
  fi

  # ---------- .env ----------
  # 复制主树 .env 后改写端口。绝不让两个 worktree 监听同一端口,
  # dev.ps1 的 Free-Port 会 taskkill /F /T 占用端口的活进程 —— 会直接杀掉另一个 worktree。
  if [[ -f "$MAIN_ROOT/.env" ]]; then
    cp "$MAIN_ROOT/.env" "$wt_dir/.env"
    # 替换 PORT= 行(而不是追加 —— 后出现的会覆盖前面的, 容易埋雷)
    if grep -qE '^PORT=' "$wt_dir/.env"; then
      sed -i "s/^PORT=.*/PORT=$be_port/" "$wt_dir/.env"
    else
      printf '\nPORT=%s\n' "$be_port" >> "$wt_dir/.env"
    fi
    # 注意: 刻意**不**写 FRONTEND_PORT 到 .env ——
    # dev.ps1:57 只读环境变量 $env:FRONTEND_PORT, 不读 .env 里的该键,
    # 写进去只会造成"已隔离"的错觉。vite.config.ts 的 server.port 硬编码 3011,
    # 因此前端端口**只能**靠启动时显式传 -FrontendPort 生效。
    ok ".env 已复制, PORT=$be_port (前端端口须靠启动参数传入)"
  else
    warn "主树无 .env, 新 worktree 需自行创建"
  fi

  register_ports "$dirname" "$be_port" "$fe_port"
  ok "端口已登记: $PORT_REGISTRY"

  # ---------- data/ ----------
  # 重要: 沙箱/IDE 环境下 setup_data_dirs 里的 Junction 创建会失败 ——
  # 从 Bash 调powershell 会被安全策略拦("Invoking PowerShell from Bash
  # bypasses PowerShell security checks"), 且 stderr 被吞, 表现为
  # create 输出了 .env/端口几行就结束、data/ 为 0 项。
  # 补救: 用 PowerShell 工具执行 New-Item -ItemType Junction,
  #或直接跑 scripts/link-worktree-data.sh <目录名> 校验/补建。
  setup_data_dirs "$wt_dir"

  # ---------- 依赖 ----------
  # .venv / node_modules 体积大且随分支变化, 各自独立。
  # dev.ps1 首次运行会自动 uv sync / pnpm install, 这里只给出提示。
  log "依赖: 首次执行该 worktree 的 dev.ps1 会自动 uv sync + pnpm install"

  cat <<EOF

$(ok "worktree 就绪")

  目录      $wt_dir
  分支      $branch
  后端端口  $be_port
  前端端口  $fe_port
  (主树 $MAIN_BACKEND_PORT/$MAIN_FRONTEND_PORT)

  data/ 策略:
    联接共享(只读): kline_minute / kline_daily_enriched / kline_daily /
                kline_index_* / financials / dragon_tiger / instruments* / adj_factor*
    独立(可写):     paper job_store cache user_data pools strategies ...

  启动命令(必须显式传端口, 不要依赖 .env 默认值):
    cd "$wt_dir"
    .\\dev.ps1 -BackendPort $be_port -FrontendPort $fe_port
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
    # 注意: 从 Bash 调 powershell 会被沙箱安全策略拦
    # ("Invoking PowerShell from Bash bypasses PowerShell security checks")。
    # 该失败会**静默发生**, 因此下面必须 `|| true` 兜住 ——
    # 否则 set -e 会把 setup_data_dirs 整个中止, 表现为:
    #   create 输出了 .env/端口几行就结束、data/ 为 0 项,
    #   连可写目录(paper/job_store)都没建, 后端起来直接写不进去。
    link_dir() {
      powershell -NoProfile -NonInteractive -Command \
        "New-Item -ItemType Junction -Path '$1' -Target '$2' -ErrorAction Stop | Out-Null" \
        >/dev/null 2>&1 || true
    }
  else
    link_dir() { ln -s "$2" "$1" || true; }
  fi

  local linked=0 failed=0
  for d in "${SHARED_DATA_DIRS[@]}"; do
    local src="$MAIN_ROOT/data/$d"
    local dst="$wt_data/$d"
    [[ -e "$src" ]] || continue
    # 已有联接 = 上次跑过, 跳过。
    # 必须用 is_link(而非 [[ -e ]] / [[ -L ]]): Junction 在 POSIX 层
    # 就是普通目录, [[ -e ]] 对它为真会被误判成"已存在实体"。
    if is_link "$dst"; then
      linked=$((linked+1))
      continue
    fi
    if [[ -e "$dst" ]]; then
      warn "已存在实体, 跳过: $d"
      continue
    fi
    link_dir "$dst" "$src"
    # 不信link_dir 的返回值 —— 事后验证目标状态。
    if is_link "$dst"; then
      linked=$((linked+1))
    else
      #绝不退化成 cp -r: 只读行情合计 1.9G, 白拷一遍还让人误以为已隔离。
      # 明确报出待执行的 PowerShell 命令, 由调用方用 PowerShell 工具执行。
      failed=$((failed+1))
    fi
  done
  if [[ "$linked" -gt 0 ]]; then
    ok "data/ 只读联接 $linked 个"
  fi
  if [[ "$failed" -gt 0 ]]; then
    warn "data/ 有 $failed 个只读联接未建成(当前环境不允许从 Bash 调 powershell)"
    warn "请用 PowerShell 工具执行以下命令补建, 或直接跑 scripts/link-worktree-data.sh <目录名>"
    printf '     New-Item -ItemType Junction -Path <worktree>\\data\\<name> -Target <main>\\data\\<name>\n'
  fi

  # 派生文件: 无法 Junction, 直接复制(体积KB级)
  local fcopied=0
  for f in "${COPIED_DATA_FILES[@]}"; do
    local fsrc="$MAIN_ROOT/data/$f"
    local fdst="$wt_data/$f"
    [[ -f "$fsrc" ]] || continue
    [[ -e "$fdst" ]] && continue
    cp "$fsrc" "$fdst"
    fcopied=$((fcopied+1))
  done
  ok "data/ 派生文件复制 $fcopied 个(独立副本, 防串写)"

  # 可写状态目录: 建空壳让应用能正常写(首次运行会自行初始化 schema)
  local writable=(
    paper job_store cache user_data pools strategies screener_results
    backtest_results research
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
  log "端口分配(以各树 .env 实际值为准):"
  printf '  %-24s %-9s %-9s %-7s %s\n' "worktree" "backend" "frontend" "前端" "状态"

  local env_f dir p fe
  for env_f in "$MAIN_ROOT/.env" "$MAIN_ROOT"/../tickflow-wt-*/.env; do
    [[ -f "$env_f" ]] || continue
    dir="$(basename "$(dirname "$env_f")")"
    if [[ "$dir" == "tickflow-stock-panel" ]]; then
      dir="main(主树)"
      fe="$MAIN_FRONTEND_PORT"
    else
      dir="${dir#tickflow-wt-}"
      fe="$(( $(grep -E '^PORT=' "$env_f" | cut -d= -f2) + 3 ))"
    fi
    p="$(grep -E '^PORT=' "$env_f" | cut -d= -f2)"
    local st="未启动"
    port_busy "$p" && st="运行中"
    printf '  %-24s %-9s %-9s %-7s %s\n' "$dir" "$p" "$fe" "-" "$st"
  done

  # 交叉检查重复端口
  echo
  local dupes
  dupes="$(for env_f in "$MAIN_ROOT/.env" "$MAIN_ROOT"/../tickflow-wt-*/.env; do
             [[ -f "$env_f" ]] || continue
             grep -E '^PORT=' "$env_f" | cut -d= -f2
           done | sort | uniq -d)"
  if [[ -n "$dupes" ]]; then
    warn "以下端口被多棵树同时占用(会导致相互静默kill):"
    echo "$dupes" | sed 's/^/    /'
  else
    ok "无重复端口"
  fi
}

cmd_check() {
  local dirname="${1:?用法: check <目录名>}"
  local wt_dir="$MAIN_ROOT/../tickflow-wt-$dirname"
  [[ -d "$wt_dir" ]] || die "worktree 不存在: $wt_dir"

  log "检查隔离状态: $wt_dir"

  # .env 端口 —— 与「所有其他树」交叉比对
  # 撞端口的后果不是启动失败, 而是 dev.ps1 的 Free-Port 把对方后端
  # taskkill 掉, 表现为"另一个 worktree 的接口突然 502/连不上"。
  if [[ -f "$wt_dir/.env" ]]; then
    local p; p="$(grep -E '^PORT=' "$wt_dir/.env" | cut -d= -f2)"
    local clashes=""
    local other_dir other_env other_p
    for other_env in "$MAIN_ROOT/.env" "$MAIN_ROOT"/../tickflow-wt-*/.env; do
      [[ -f "$other_env" ]] || continue
      other_dir="$(basename "$(dirname "$other_env")")"
      # 主树目录名是 tickflow-stock-panel, 其余是 tickflow-wt-<name>
      other_dir="${other_dir#tickflow-wt-}"
      [[ "$other_dir" == "$dirname" ]] && continue
      other_p="$(grep -E '^PORT=' "$other_env" 2>/dev/null | cut -d= -f2)"
      [[ -n "$other_p" && "$other_p" == "$p" ]] && clashes="$clashes $other_dir($p)"
    done
    if [[ -n "$clashes" ]]; then
      warn "PORT=$p 与以下 worktree 冲突:$clashes"
      warn "必须改端口, 否则先启动的那个会被后启动的 dev.ps1 静默杀掉"
    else
      ok "PORT=$p (与所有其他 worktree 无冲突)"
    fi
  else
    warn "缺 .env"
  fi

  # data 联接状态
  echo
  log "data/ 联接检查:"
  for d in "${SHARED_DATA_DIRS[@]}"; do
    local dst="$wt_dir/data/$d"
    if is_link "$dst"; then
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
    if is_link "$dst"; then
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

  # ---------- 关键: 先摘掉所有只读联接 ----------
  # Junction 对 git/rsync 是"真目录", 直接 git worktree remove 会跟进删除
  # **主树的真实行情数据**。必须逐个 rmdir(只删链接本身)后再删 worktree。
  local unlinked=0
  for d in "${SHARED_DATA_DIRS[@]}"; do
    local dst="$wt_dir/data/$d"
    if is_link "$dst"; then
      # cmd //c rmdir 只删 junction 本身, 不动目标内容
      cmd //c rmdir "$(cygpath -w "$dst" 2>/dev/null || echo "$dst")" >/dev/null 2>&1 || true
      unlinked=$((unlinked+1))
    fi
  done
  if [[ "$unlinked" -gt 0 ]]; then
    ok "已摘除只读联接 $unlinked 个(主树数据未被触碰)"
  fi

  # 兜底: 若仍有残留联接, 拒绝强删, 避免误伤主树
  local remain=0
  for d in "${SHARED_DATA_DIRS[@]}"; do
    is_link "$wt_dir/data/$d" && remain=$((remain+1))
  done
  if [[ "$remain" -gt 0 ]]; then
    die "仍有 $remain 个联接未摘除, 已中止。请手动检查 $wt_dir/data 后再执行"
  fi

  git worktree remove --force "$wt_dir"

  # 清掉端口登记, 否则该端口会被后续 create 误判为"已分配"而跳过
  if [[ -f "$PORT_REGISTRY" ]]; then
    grep -vE "^$dirname[[:space:]]" "$PORT_REGISTRY" > "$PORT_REGISTRY.tmp" || true
    mv "$PORT_REGISTRY.tmp" "$PORT_REGISTRY"
    ok "已清理端口登记"
  fi
  ok "已移除"
}

case "${1:-}" in
  create) shift; cmd_create "$@" ;;
  list)   cmd_list ;;
  check)  shift; cmd_check "$@" ;;
  remove) shift; cmd_remove "$@" ;;
  *)      sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
esac
