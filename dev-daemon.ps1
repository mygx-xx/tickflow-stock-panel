<#
  dev-daemon.ps1 - 常驻看门狗 (与 dev.ps1 配套, 不替代它)

  解决的问题:
    1. dev.ps1 日志落在系统临时文件, 退出即删 -> 这里改为 logs/dev/*.log 长期保留
    2. 进程崩了没人拉起 -> 定时健康检查, 异常自动重启 (带限流)
    3. 终端一关服务就没了 -> 看门狗跑在独立控制台, 与调用终端解耦

  用法:
    .\dev-daemon.ps1            # start (默认)
    .\dev-daemon.ps1 status
    .\dev-daemon.ps1 logs      # 跟踪后端日志
    .\dev-daemon.ps1 stop

  日志:
    logs/dev/backend.log    后端 (uvicorn, UTF-8)
    logs/dev/frontend.log   前端 (vite, UTF-8)
    logs/dev/watchdog.log   看门狗自身
#>
[CmdletBinding()]
param(
    [ValidateSet('start','stop','status','restart','logs','watch')]
    [string]$Action = 'start',
    [int]$BackendPort = 3018,
    [int]$FrontendPort = 3011,
    [int]$HealthIntervalSec = 10,
    [int]$MaxRestartsPerHour = 20
)

$ErrorActionPreference = 'Stop'
$Root        = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogDir      = Join-Path $Root 'logs\dev'
$StateDir    = Join-Path $LogDir 'state'
$BackendDir  = Join-Path $Root 'backend'
$FrontendDir = Join-Path $Root 'frontend'
$EnvFile     = Join-Path $Root '.env'

function Say($m)     { Write-Host "[daemon] $m" }
function SayOk($m)   { Write-Host "[daemon] $m" -ForegroundColor Green }
function SayWarn($m) { Write-Host "[daemon] $m" -ForegroundColor Yellow }
function SayErr($m)  { Write-Host "[daemon] $m" -ForegroundColor Red }

function PidFile($name) { Join-Path $StateDir "$name.pid" }
function LogFile($name) { Join-Path $LogDir  "$name.log" }
function WLog($m) {
    "[$(Get-Date -f 'yyyy-MM-dd HH:mm:ss')] $m" | Out-File -FilePath (LogFile 'watchdog') -Append -Encoding utf8
}

# 读 pid 文件并确认进程还活着; 进程已死则返回 $null (pid 文件是陈旧的)
function Read-Pid($name) {
    $f = PidFile $name
    if (-not (Test-Path $f)) { return $null }
    $raw = (Get-Content $f -ErrorAction SilentlyContinue) -as [string]
    if (-not $raw -or -not $raw.Trim()) { return $null }
    $pid0 = 0
    if (-not [int]::TryParse($raw.Trim(), [ref]$pid0)) { return $null }
    try { [System.Diagnostics.Process]::GetProcessById($pid0) | Out-Null; return $pid0 }
    catch { return $null }
}

# /T 杀整棵进程树: 只杀父进程会留下 uvicorn/vite 之类占着端口的孤儿
function Kill-Tree($pid0) {
    if (-not $pid0) { return }
    $null = & cmd /c "taskkill /F /T /PID $pid0 2>nul"
    Start-Sleep -Milliseconds 400
}

function Clear-Port($port) {
    $conns = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
    foreach ($p in @($conns.OwningProcess | Where-Object { $_ -gt 0 } | Sort-Object -Unique)) {
        Kill-Tree $p
    }
}

# 杀光本项目的服务进程, 而不只是监听端口的那些。
#
# 为什么必须多这一步: `uvicorn --reload` 会拉起两个进程 —— reloader 父进程负责
# 监视文件, worker 子进程才真正监听端口。只杀监听端口的 worker, 父进程会当作
# "worker 异常退出" 再拉一个新的, 于是每重启一轮就多一棵树 (实测累积到 6 个
# uvicorn + 2 个 vite, 内存飙升且 watchfiles 互相干扰)。
# vite 同理: 父 node 进程与实际监听端口的子进程分离。
function Stop-ProjectProcesses($kind) {
    $procs = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    foreach ($p in $procs) {
        $cl = $p.CommandLine
        if (-not $cl) { continue }
        if ($cl -like '*dev-daemon.ps1*') { continue }   # 别把看门狗自己杀了
        $hit = if ($kind -eq 'backend') {
            $cl -like '*uvicorn app.main*'
        } else {
            ($cl -like '*vite*') -and ($cl -like '*3011*')
        }
        if ($hit) { Kill-Tree $p.ProcessId }
    }
}

# 启一个服务: cmd 在 UTF-8 代码页下把 stdout/stderr 直接重定向到日志文件。
# 字节不经过 PowerShell 原生管道 -> 不会因本机默认 gb2312 解码而乱码
# (与 dev.ps1 里记录的乱码根因同源, 解法一致)。
function Start-Service($name, $dir, $cmdBody) {
    $log = LogFile $name
    $cmd = 'chcp 65001 >nul && ' + $cmdBody + ' >> "' + $log + '" 2>&1'
    $proc = Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', $cmd `
        -WorkingDirectory $dir -WindowStyle Hidden -PassThru
    $proc.Id | Out-File -FilePath (PidFile $name) -Encoding ascii -Force
    return $proc.Id
}

function Start-Backend() {
    $envArgs = if (Test-Path $EnvFile) { "--env-file `"$EnvFile`"" } else { '' }
    $body = ".\.venv\Scripts\python.exe -m uvicorn app.main:app $envArgs --reload --host 0.0.0.0 --port $BackendPort"
    $env:PYTHONUNBUFFERED = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    return Start-Service 'backend' $BackendDir $body
}

function Start-Frontend() {
    $body = "pnpm dev --host 0.0.0.0 --port $FrontendPort"
    return Start-Service 'frontend' $FrontendDir $body
}

function Test-Health($url) {
    try {
        $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
        return ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500)
    } catch { return $false }
}

# 限流: 一小时内重启超上限就停手, 避免配置错误时无限重启把机器打满
function Rate-Limited {
    $f = Join-Path $StateDir 'restarts.json'
    $now = Get-Date
    $list = @()
    if (Test-Path $f) {
        try { $list = @(Get-Content $f | ConvertFrom-Json) } catch { $list = @() }
    }
    $recent = @($list | Where-Object { $now - [datetime]$_ -lt [timespan]::FromHours(1) })
    $recent += $now.ToString('o')
    Set-Content -Path $f -Value $recent -Encoding ascii
    return ($recent.Count -gt $MaxRestartsPerHour)
}

function Invoke-Watchdog() {
    # 单实例互斥: 多个看门狗会互相抢占"重启权", 每次自愈都多起一套服务。
    # 用命名 Mutex 而不是 pid 文件 —— pid 文件会被后启动的实例覆盖,
    # 先启动的那些就成了杀不掉的孤儿 (本次故障的直接原因之一)。
    $mutex = $null
    try {
        $mutex = New-Object System.Threading.Mutex($false, 'Global\TickFlowDevDaemonWatchdog')
        if (-not $mutex.WaitOne(0)) {
            WLog '已有看门狗实例在运行, 本进程退出 (防多实例重复拉起服务)'
            return
        }
    } catch {
        # 拿不到互斥体不致命: 退化为无锁, 下面的端口/进程清理仍能收敛
        WLog "互斥锁获取失败(继续无锁运行): $($_.Exception.Message)"
    }

    WLog "watchdog up (pid $PID, interval ${HealthIntervalSec}s, ports ${BackendPort}/${FrontendPort})"
    $BE = "http://127.0.0.1:$BackendPort/health"
    $FE = "http://127.0.0.1:$FrontendPort/"

    # 启动宽限: 服务刚拉起时不给它判定机会。
    # 后端冷启动要 10-15s (enriched 预热约 6s), 若间隔 10s 就判 unhealthy,
    # 看门狗会把"正在启动"当成"已挂"反复重启, 服务永远起不来。
    $GraceSec = 60
    $FailThreshold = 3      # 连续 unhealthy 达阈值才重启, 避免抖动误杀
    $graceUntil = @{ backend = (Get-Date).AddSeconds(0); frontend = (Get-Date).AddSeconds(0) }
    $failCount  = @{ backend = 0; frontend = 0 }

    foreach ($n in @('backend','frontend')) {
        $url = if ($n -eq 'backend') { $BE } else { $FE }
        $port = if ($n -eq 'backend') { $BackendPort } else { $FrontendPort }
        # 无论端口健不健康, 都先把这个项目的残留进程清干净 ——
        # 端口健康但存在上一轮没死干净的 reloader 父进程, 是重复实例的来源。
        Stop-ProjectProcesses $n
        Clear-Port $port
        if (-not (Test-Health $url)) {
            $pid0 = if ($n -eq 'backend') { Start-Backend } else { Start-Frontend }
            WLog "start $n pid $pid0 (grace ${GraceSec}s)"
            $graceUntil[$n] = (Get-Date).AddSeconds($GraceSec)
        } else {
            WLog "$n 已在运行, 接管守护"
        }
    }

    while ($true) {
        Start-Sleep -Seconds $HealthIntervalSec
        $now = Get-Date

        foreach ($n in @('backend','frontend')) {
            if ($now -lt $graceUntil[$n]) { continue }      # 宽限期内不判定
            $url = if ($n -eq 'backend') { $BE } else { $FE }
            if (Test-Health $url) { $failCount[$n] = 0; continue }

            $failCount[$n]++
            if ($failCount[$n] -lt $FailThreshold) {
                WLog "$n unhealthy ($($failCount[$n])/$FailThreshold) - 再观察一轮"
                continue
            }

            if (Rate-Limited) {
                WLog "RATE LIMITED ($n down) - 1h 内重启超 $MaxRestartsPerHour 次, 暂停自愈"
                continue
            }

            $failCount[$n] = 0
            $graceUntil[$n] = (Get-Date).AddSeconds($GraceSec)
            WLog "$n 确认不可用 ($FailThreshold 连败) - restarting"
            Kill-Tree (Read-Pid $n)
            # 先杀整棵项目进程树(含不监听端口的 reloader 父进程), 再清端口
            if ($n -eq 'backend') {
                Stop-ProjectProcesses 'backend'; Clear-Port $BackendPort
                WLog "  backend restarted pid $(Start-Backend)"
            } else {
                Stop-ProjectProcesses 'frontend'; Clear-Port $FrontendPort
                WLog "  frontend restarted pid $(Start-Frontend)"
            }
        }
    }
}

function Stop-All {
    # 看门狗: 杀掉所有实例(按命令行匹配), 不只 pid 文件里那一个 ——
    # 后启动的会覆盖 pid 文件, 先前的就成了杀不掉的孤儿。
    $procs = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    $wd = @($procs | Where-Object { $_.CommandLine -and $_.CommandLine -like '*dev-daemon.ps1*' -and $_.CommandLine -like '*-Action*watch*' })
    if ($wd.Count) {
        foreach ($p in $wd) { SayWarn "stopping watchdog pid $($p.ProcessId)"; Kill-Tree $p.ProcessId }
    } else { SayWarn 'watchdog not running' }
    $wdPid = Read-Pid 'watchdog'
    if ($wdPid) { Kill-Tree $wdPid }

    foreach ($n in @('backend','frontend')) {
        $p = Read-Pid $n
        if ($p) { SayWarn "stopping $n pid $p"; Kill-Tree $p }
    }
    # 再按命令行兜底清一遍, 覆盖 pid 文件缺失/失效的情况
    Stop-ProjectProcesses 'backend'
    Stop-ProjectProcesses 'frontend'
    Clear-Port $BackendPort
    Clear-Port $FrontendPort
    Remove-Item (Join-Path $StateDir '*.pid') -Force -ErrorAction SilentlyContinue
    SayOk 'stopped'
}

function Show-Status {
    $wd = Read-Pid 'watchdog'
    $be = Read-Pid 'backend'
    $fe = Read-Pid 'frontend'
    $beOk = Test-Health "http://127.0.0.1:$BackendPort/health"
    $feOk = Test-Health "http://127.0.0.1:$FrontendPort/"
    @(
        [pscustomobject]@{ service='watchdog'; pid=if($wd){$wd}else{'-'};   state=if($wd){'running'}else{'stopped'} }
        [pscustomobject]@{ service='backend';  pid=if($be){$be}else{'-'};   state=if($beOk){'healthy'}elseif($be){'unhealthy'}else{'stopped'} }
        [pscustomobject]@{ service='frontend'; pid=if($fe){$fe}else{'-'};   state=if($feOk){'healthy'}elseif($fe){'unhealthy'}else{'stopped'} }
    ) | Format-Table -AutoSize
    $wlog = LogFile 'watchdog'
    if (Test-Path $wlog) {
        Say 'watchdog 最近日志:'
        Get-Content $wlog -Tail 8 -Encoding utf8 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    }
}

# ===== 分派 =====
switch ($Action) {
    'watch' { Invoke-Watchdog }
    'start' {
        # 守卫: 只要有 watch 进程在跑就拒绝 —— 不能只看 pid 文件, 后启动的实例会
        # 覆盖 pid 文件, 守卫会误判成"没在跑"从而再起一个 (多实例的来源)。
        $running = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { $_.CommandLine -and $_.CommandLine -like '*dev-daemon.ps1*' -and $_.CommandLine -like '*-Action*watch*' })
        if ($running.Count -gt 0) {
            SayWarn "已有 $($running.Count) 个看门狗实例在运行 (pid: $(($running.ProcessId) -join ',')), 用 restart 换新"
            Show-Status
            break
        }
        New-Item -ItemType Directory -Force -Path $StateDir | Out-Null
        # 独立控制台: 调用方终端关闭后看门狗继续存活
        $self = Join-Path $Root 'dev-daemon.ps1'
        $argList = @('-NoProfile','-ExecutionPolicy','Bypass','-File',$self,'-Action','watch',
                     '-BackendPort',$BackendPort,'-FrontendPort',$FrontendPort,
                     '-HealthIntervalSec',$HealthIntervalSec,'-MaxRestartsPerHour',$MaxRestartsPerHour)
        $p = Start-Process -FilePath 'powershell.exe' -ArgumentList $argList -WorkingDirectory $Root -WindowStyle Hidden -PassThru
        $p.Id | Out-File -FilePath (PidFile 'watchdog') -Encoding ascii -Force
        SayOk "watchdog started (pid $($p.Id))"
        Say "日志: $(LogFile 'backend') / $(LogFile 'frontend')"
        for ($i = 0; $i -lt 60; $i++) {
            Start-Sleep -Seconds 1
            if ((Test-Health "http://127.0.0.1:$BackendPort/health") -and (Test-Health "http://127.0.0.1:$FrontendPort/")) {
                SayOk 'backend + frontend 已就绪'
                break
            }
        }
        Show-Status
    }
    'stop'    { Stop-All }
    'restart' {
        Stop-All
        Start-Sleep -Seconds 2
        & (Join-Path $Root 'dev-daemon.ps1') -Action start -BackendPort $BackendPort -FrontendPort $FrontendPort
    }
    'status'  { Show-Status }
    'logs'    {
        $f = LogFile 'backend'
        if (-not (Test-Path $f)) { SayErr "日志不存在: $f"; break }
        Get-Content $f -Tail 40 -Wait -Encoding utf8
    }
}
