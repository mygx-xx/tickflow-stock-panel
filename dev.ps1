# tickflow-stock-panel - one-shot launcher for backend + frontend (Windows / PowerShell)
#
# Usage:
#   .\dev.ps1
#   .\dev.ps1 -BackendPort 8000 -FrontendPort 5173
#   $env:BACKEND_PORT='8000'; .\dev.ps1
#
# Ctrl-C closes both processes.
#
# If you see "running scripts is disabled":
#   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

[CmdletBinding()]
param(
    [int]$BackendPort  = 0,
    [int]$FrontendPort = 0
)

$ErrorActionPreference = 'Stop'

$Root        = Split-Path -Parent $MyInvocation.MyCommand.Path
$BackendDir  = Join-Path $Root 'backend'
$FrontendDir = Join-Path $Root 'frontend'
$EnvFile     = Join-Path $Root '.env'

# Read only launcher-owned keys. Do not execute .env as PowerShell code.
function Read-DotEnvValue($Path, $Name) {
    if (-not (Test-Path $Path)) { return $null }
    $escaped = [Regex]::Escape($Name)
    foreach ($line in Get-Content $Path) {
        if ($line -match "^\s*$escaped\s*=\s*(.*?)\s*$") {
            $value = $Matches[1].Trim()
            $value = ($value -replace '\s+#.*$', '').Trim()
            if ($value.Length -ge 2 -and
                (($value.StartsWith('"') -and $value.EndsWith('"')) -or
                 ($value.StartsWith("'") -and $value.EndsWith("'")))) {
                return $value.Substring(1, $value.Length - 2)
            }
            return $value
        }
    }
    return $null
}

$DotEnvHost = Read-DotEnvValue $EnvFile 'HOST'
$DotEnvPort = Read-DotEnvValue $EnvFile 'PORT'
$BindAddress = if ($env:HOST) { $env:HOST } elseif ($DotEnvHost) { $DotEnvHost } else { '0.0.0.0' }
$DisplayHost = if ($BindAddress -in @('0.0.0.0', '::')) { 'localhost' } else { $BindAddress }

# Port precedence: CLI arg > BACKEND_PORT env > PORT env > .env PORT > default
if ($BackendPort -le 0) {
    if ($env:BACKEND_PORT) { $BackendPort = [int]$env:BACKEND_PORT }
    elseif ($env:PORT)     { $BackendPort = [int]$env:PORT }
    elseif ($DotEnvPort)   { $BackendPort = [int]$DotEnvPort }
    else                   { $BackendPort = 3018 }
}
if ($FrontendPort -le 0) { $FrontendPort = if ($env:FRONTEND_PORT) { [int]$env:FRONTEND_PORT } else { 3011 } }

# Force UTF-8 console output so child process logs aren't garbled
#
# 根因 (实测 2026-10-07, 本机 Console.InputEncoding 默认 gb2312):
# Start-Job 子进程按控制台代码页解码子进程输出, 而后端是 UTF-8 字节 ->
#   "使用**进程内**传输".encode("utf-8").decode("gb2312")
#     = "浣跨敤**杩涚▼鍐?*浼犺緭"  (与实际日志逐字一致)
# ASCII 不受影响, 故现象是「英文正常、中文乱码」。
#
# 对照实验(先 chcp 936 强制复现乱码, 再逐项验证):
#   仅 chcp 65001                 -> 仍乱码   ✗
#   仅 OutputEncoding             -> 仍乱码   ✗
#   仅 PYTHONIOENCODING=gbk       -> 有效     ✓ 但与 app/__init__.py 的
#                                              UTF-8 reconfigure 冲突
#   chcp65001 + InputEncoding
#              + OutputEncoding   -> 有效     ✓ 采用
#
# 关键: [Console]::InputEncoding 才是「读取子进程原生 stdout」的解码开关;
# OutputEncoding 只管 PowerShell 自身输出流, 单独设它无效。
try {
    $null = & chcp.com 65001 2>&1
    [Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
    [Console]::InputEncoding  = New-Object System.Text.UTF8Encoding $false
    $OutputEncoding           = New-Object System.Text.UTF8Encoding $false
} catch {}

function Log-Info($m) { Write-Host "[dev] $m" -ForegroundColor DarkGray }
function Log-Ok  ($m) { Write-Host "[dev] $m" -ForegroundColor Green }
function Log-Warn($m) { Write-Host "[dev] $m" -ForegroundColor Yellow }
function Log-Err ($m) { Write-Host "[dev] $m" -ForegroundColor Red }

# ===== 1. Dependency check =====
function Require-Cmd($cmd, $hint) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Log-Err "$cmd not found"
        Write-Host "       install via: $hint"
        exit 1
    }
}

Require-Cmd 'uv'   'powershell -c "irm https://astral.sh/uv/install.ps1 | iex"   OR   winget install --id=astral-sh.uv'
Require-Cmd 'pnpm' 'npm i -g pnpm   OR   corepack enable; corepack prepare pnpm@9 --activate'

# ===== 2. Port check - kill anything listening on the target ports =====
function Free-Port($name, $port) {
    $conns = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
    if (-not $conns) { return }
    $pids = @($conns.OwningProcess | Where-Object { $_ -gt 0 } | Sort-Object -Unique)
    if ($pids.Count -eq 0) { return }

    # Filter to PIDs that still exist as running processes.
    # A zombie TCP endpoint can linger after the process is already dead.
    $alive = @($pids | Where-Object {
        try { [System.Diagnostics.Process]::GetProcessById($_) | Out-Null; $true }
        catch { $false }
    })

    if ($alive.Count -eq 0) {
        # All processes are dead but kernel still holds the socket (zombie endpoint).
        # On Windows this can linger for minutes, but uvicorn/vite can still bind
        # via SO_REUSEADDR — no point waiting, just proceed.
        Log-Warn "port ${port} (${name}) - zombie socket (processes gone), starting anyway"
        return
    }

    Log-Warn "port $port ($name) is in use, killing PID: $($alive -join ', ')"
    # Use taskkill /F /T to kill the entire process tree (parent + children),
    # not just the parent. Stop-Process only kills one process, leaving child
    # processes (e.g. uvicorn spawned by uv) as orphans holding the socket.
    foreach ($p in $alive) {
        # Suppress stderr properly for Windows PowerShell (5.x)
        $null = & cmd /c "taskkill /F /T /PID $p 2>nul"
        # Fallback: if taskkill failed, try Stop-Process
        try { Stop-Process -Id $p -Force -ErrorAction SilentlyContinue } catch {}
    }

    # Wait up to 5 seconds for the kernel to release the TCP endpoint
    for ($i = 0; $i -lt 10; $i++) {
        Start-Sleep -Milliseconds 500
        $still = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
        if (-not $still) {
            Log-Ok "port $port freed"
            return
        }
    }

    # Port still stuck — process might be dead with zombie socket
    $anyAlive = $still | Where-Object {
        try { [System.Diagnostics.Process]::GetProcessById($_.OwningProcess) | Out-Null; $true }
        catch { $false }
    }
    if ($anyAlive.Count -eq 0) {
        Log-Warn "port ${port} - processes gone but socket lingers, starting anyway"
    } else {
        Log-Err "port ${port} still in use by live process(es). Inspect: Get-NetTCPConnection -LocalPort ${port}"
        exit 1
    }
}

Free-Port 'backend'  $BackendPort
Free-Port 'frontend' $FrontendPort

# ===== 3. Dependency install =====
# Match Docker's whitespace-separated BACKEND_EXTRAS behavior so old CPUs can
# select Polars' rtcompat runtime before the backend starts.
$BackendExtras = $env:BACKEND_EXTRAS
if (-not (Test-Path Env:BACKEND_EXTRAS)) {
    $BackendExtras = Read-DotEnvValue $EnvFile 'BACKEND_EXTRAS'
}

$BackendExtraArgs = @()
if (-not [string]::IsNullOrWhiteSpace($BackendExtras)) {
    foreach ($extra in ($BackendExtras -split '\s+' | Where-Object { $_ })) {
        $BackendExtraArgs += '--extra', $extra
    }
}

if (-not (Test-Path (Join-Path $BackendDir '.venv')) -or $BackendExtraArgs.Count) {
    if ($BackendExtraArgs.Count) {
        Log-Info "syncing Python deps with extras: $BackendExtras"
    } else {
        Log-Info 'first run - installing Python deps (1-2 min)...'
    }
    Push-Location $BackendDir
    try { & uv sync --frozen @BackendExtraArgs } finally { Pop-Location }
    if ($LASTEXITCODE -ne 0) { Log-Err 'uv sync failed'; exit 1 }
    Log-Ok 'backend deps installed'
}

if (-not (Test-Path (Join-Path $FrontendDir 'node_modules'))) {
    Log-Info 'first run - installing Node deps...'
    Push-Location $FrontendDir
    try { & pnpm install } finally { Pop-Location }
    if ($LASTEXITCODE -ne 0) { Log-Err 'pnpm install failed'; exit 1 }
    Log-Ok 'frontend deps installed'
}

# ===== 4. Banner (ASCII so it renders on any codepage) =====
Write-Host ''
Write-Host '+----------------------------------------------+' -ForegroundColor Blue
Write-Host '|  tickflow-stock-panel                        |' -ForegroundColor Blue
Write-Host '|                                              |' -ForegroundColor Blue
Write-Host "|  backend   http://${DisplayHost}:$BackendPort"  -ForegroundColor Blue
Write-Host "|  frontend  http://${DisplayHost}:$FrontendPort" -ForegroundColor Blue
Write-Host '|                                              |' -ForegroundColor Blue
Write-Host '|  Ctrl-C closes both                          |' -ForegroundColor Blue
Write-Host '+----------------------------------------------+' -ForegroundColor Blue
Write-Host ''

# ===== 5. Launch jobs =====
# Each job writes its $PID to a temp file so the main thread can find the
# child powershell.exe and taskkill /T the whole process tree on exit.
$backendPidFile  = [System.IO.Path]::GetTempFileName()
$frontendPidFile = [System.IO.Path]::GetTempFileName()

# 各子进程的日志落盘文件 (UTF-8)。
# 为什么不直接 Receive-Job 拿对象: Start-Job 读子进程**原生 stdout** 时,
# 解码由子进程的 InputEncoding/控制台代码页 决定, 本机默认 gb2312,
# 而后端与 pnpm 都输出 UTF-8 => 中文乱码 (已在日志中逐字复现:
# "使用**进程内**传输" -> "浣跨敤**杩涚▼鍐?*浼犺緭")。
# 实测对照 (在 gb2312 环境下强制复现后逐项验证):
#   仅 chcp 65001                     -> 仍乱码  ✗
#   仅 [Console]::OutputEncoding      -> 仍乱码  ✗
#   chcp65001 + Input/OutputEncoding   -> 仍乱码  ✗  ← 曾在用, 无效
#   cmd /c "chcp 65001 && cmd > file" -> 正确    ✓ 采用
# 末选方案: 让 cmd 在 UTF-8 代码页下把子进程 stdout **直接重定向到文件**,
# 字节不经过 PowerShell 的原生 stdout 管道, 从根上绕开错误解码;
# 主进程再用 [IO.File]::ReadAllLines(path, UTF8) 按行 tail。
$backendLogFile  = [System.IO.Path]::GetTempFileName()
$frontendLogFile = [System.IO.Path]::GetTempFileName()

$backendJob = Start-Job -Name 'backend' -ScriptBlock {
    param($pidFile, $dir, $envFile, $bindAddress, $port, $logFile)
    # 日志经 cmd 在 UTF-8 代码页下直接重定向到文件, 绕开 PowerShell 对
    # 原生 stdout 的错误解码(本机默认 gb2312 会把UTF-8 读成乱码)。
    # 详见 $backendLogFile 处的对照实验记录。
    $PID | Out-File -FilePath $pidFile -Encoding ascii -Force
    $env:PYTHONUNBUFFERED = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    Set-Location $dir
    $envArgs = if (Test-Path $envFile) { @('--env-file', $envFile) } else { @() }
    $cmd = 'chcp 65001 >nul && .\.venv\Scripts\python.exe -m uvicorn app.main:app'
    if ($envArgs.Count -gt 0) { $cmd += ' ' + ($envArgs -join ' ') }
    $cmd += ' --reload --host ' + $bindAddress + ' --port ' + $port
    $cmd += ' > "' + $logFile + '" 2>&1'
    $null = & cmd.exe /c $cmd
} -ArgumentList $backendPidFile, $BackendDir, $EnvFile, $BindAddress, $BackendPort, $backendLogFile

$frontendJob = Start-Job -Name 'frontend' -ScriptBlock {
    param($pidFile, $dir, $bindAddress, $backendPort, $port, $logFile)
    # 同 backend: 走 cmd + UTF-8 代码页 + 文件重定向。pnpm/vite 输出 UTF-8。
    $PID | Out-File -FilePath $pidFile -Encoding ascii -Force
    Set-Location $dir
    $env:BACKEND_HOST = $bindAddress
    $env:BACKEND_PORT = [string]$backendPort
    $cmd = 'chcp 65001 >nul && pnpm dev --host ' + $bindAddress + ' --port ' + $port
    $cmd += ' > "' + $logFile + '" 2>&1'
    $null = & cmd.exe /c $cmd
} -ArgumentList $frontendPidFile, $FrontendDir, $BindAddress, $BackendPort, $FrontendPort, $frontendLogFile

# Wait up to 5 seconds for the PID files to materialise
function Read-JobPid($file) {
    for ($i = 0; $i -lt 50; $i++) {
        try {
            $c = (Get-Content $file -ErrorAction SilentlyContinue) -as [string]
            if ($c -and $c.Trim()) { return [int]$c.Trim() }
        } catch {}
        Start-Sleep -Milliseconds 100
    }
    return $null
}
$backendChildPid  = Read-JobPid $backendPidFile
$frontendChildPid = Read-JobPid $frontendPidFile

# ===== 6. Cleanup =====
$script:cleaning = $false
function Cleanup-All {
    if ($script:cleaning) { return }
    $script:cleaning = $true
    Write-Host ''
    Log-Info 'shutting down...'

    foreach ($p in @($backendChildPid, $frontendChildPid)) {
        if ($p) {
            # /T kills the whole process tree (the job's powershell + uvicorn/vite)
            $null = & cmd /c "taskkill /F /T /PID $p 2>nul"
        }
    }
    foreach ($j in @($backendJob, $frontendJob)) {
        if ($j) {
            Stop-Job   $j -ErrorAction SilentlyContinue
            Remove-Job $j -Force -ErrorAction SilentlyContinue
        }
    }
    foreach ($f in @($backendPidFile, $frontendPidFile, $backendLogFile, $frontendLogFile)) {
        Remove-Item $f -Force -ErrorAction SilentlyContinue
    }
    Log-Ok 'bye'
}

# ===== 7. Main loop - pump output, handle Ctrl-C =====
# Treat Ctrl-C as input so try/finally is guaranteed to run.
#
# 非交互式会话(CI / IDE 任务面板 / 后台进程 / 输出重定向到管道)下没有真实控制台
# 输入句柄, 此时 [Console]::TreatControlCAsInput / KeyAvailable / ReadKey 会抛
# 「句柄无效」(WinError 6)。这类会话由父进程或 job 终止器负责结束脚本, 因此
# 直接跳过控制台交互, 只泵日志。IsInputRedirected 在无控制台时同样安全返回。
$interactive = -not [Console]::IsInputRedirected
$prevCtrlC = $null
if ($interactive) {
    $prevCtrlC = [Console]::TreatControlCAsInput
}

# 日志 tail 状态: 每个文件记住已读到的行数, 增量打印。
# 不用 Receive-Job 是因为它读子进程原生 stdout 时会按本机代码页(gb2312)
# 解码 UTF-8 输出 -> 中文乱码; 详见 $backendLogFile 处注释。
$logCursor = @{}
function Write-NewLogLines($logPath, $tag, $color) {
    if (-not (Test-Path $logPath)) { return }
    try {
        $all = [System.IO.File]::ReadAllLines($logPath, [System.Text.Encoding]::UTF8)
    } catch { return }
    $from = 0
    if ($logCursor.ContainsKey($logPath)) { $from = $logCursor[$logPath] }
    if ($all.Length -le $from) { return }
    for ($i = $from; $i -lt $all.Length; $i++) {
        Write-Host $tag -NoNewline -ForegroundColor $color
        Write-Host $all[$i]
    }
    $logCursor[$logPath] = $all.Length
}

try {
    if ($interactive) {
        [Console]::TreatControlCAsInput = $true
    }

    while ($true) {
        if ($interactive -and [Console]::KeyAvailable) {
            $key = [Console]::ReadKey($true)
            if (($key.Modifiers -band [ConsoleModifiers]::Control) -and $key.Key -eq 'C') {
                break
            }
        }

        Write-NewLogLines $backendLogFile  '[backend ] ' 'Blue'
        Write-NewLogLines $frontendLogFile '[frontend] ' 'Green'

        if ($backendJob.State -ne 'Running' -or $frontendJob.State -ne 'Running') {
            Log-Warn 'one of the processes exited; closing the other...'
            break
        }

        Start-Sleep -Milliseconds 150
    }
}
finally {
    if ($interactive) {
        try { [Console]::TreatControlCAsInput = $prevCtrlC } catch { }
    }
    Cleanup-All
}
