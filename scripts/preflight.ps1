# MarketPulse preflight: run before `docker compose up` and before any demo or benchmark.
#
#   powershell -ExecutionPolicy Bypass -File scripts\preflight.ps1            # check only
#   (as Administrator) ... -File scripts\preflight.ps1 -Fix                   # also resync the Windows clock
#   (as Administrator) ... -File scripts\preflight.ps1 -Fix -RestartWsl       # also restart WSL (stops Docker!)
#
# Checks: Windows clock vs NTP, .wslconfig, Docker memory, container clock vs Binance.

param([switch]$Fix, [switch]$RestartWsl)

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
$ok = $true
function Say($status, $msg) {
    $color = @{ OK = "Green"; WARN = "Yellow"; BAD = "Red"; INFO = "Cyan" }[$status]
    Write-Host ("[{0,-4}] {1}" -f $status, $msg) -ForegroundColor $color
}
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)

# ---------- 1. Windows clock ----------
function Get-WinOffset {
    $out = w32tm /stripchart /computer:time.windows.com /samples:3 /dataonly 2>$null
    $vals = @()
    foreach ($line in $out) {
        if ($line -match ',\s*([+-]?\d+\.\d+)s') { $vals += [double]$Matches[1] }
    }
    if ($vals.Count -eq 0) { return $null }
    # stripchart offset = remote - local, so local ahead => negative
    return [math]::Round(-1000 * ($vals | Measure-Object -Average).Average)
}

$winSkew = Get-WinOffset
if ($null -eq $winSkew) {
    Say WARN "Windows clock: NTP unreachable (UDP 123 blocked on this network?)"
} elseif ([math]::Abs($winSkew) -le 200) {
    Say OK "Windows clock skew $winSkew ms"
} else {
    Say BAD "Windows clock skew $winSkew ms"
    if ($Fix -and $isAdmin) {
        Say INFO "configuring w32time and resyncing..."
        Start-Service w32time -ErrorAction SilentlyContinue
        w32tm /config /manualpeerlist:"time.windows.com,0x9 pool.ntp.org,0x9" /syncfromflags:manual /reliable:no /update | Out-Null
        Restart-Service w32time
        Start-Sleep -Seconds 2
        w32tm /resync /force | Out-Null
        Start-Sleep -Seconds 2
        $winSkew = Get-WinOffset
        if ($null -ne $winSkew -and [math]::Abs($winSkew) -le 200) { Say OK "Windows clock fixed: $winSkew ms" }
        else { Say BAD "still $winSkew ms. Try Settings > Time & language > Sync now, or another network"; $ok = $false }
    } elseif ($Fix) {
        Say BAD "-Fix needs an Administrator PowerShell"; $ok = $false
    } else {
        Say INFO "re-run as Administrator with -Fix"; $ok = $false
    }
}

# ---------- 2. .wslconfig ----------
$wslcfg = Join-Path $env:USERPROFILE ".wslconfig"
if (Test-Path $wslcfg) {
    $mem = (Select-String -Path $wslcfg -Pattern '^\s*memory\s*=\s*(\S+)' | Select-Object -First 1)
    if ($mem) { Say OK ".wslconfig memory = $($mem.Matches[0].Groups[1].Value)" }
    else { Say WARN ".wslconfig has no memory= line (WSL default is 50% of RAM)" }
} else {
    Say WARN "no $wslcfg; copy docs\wslconfig.example (or wslconfig.32gb.example) there, then wsl --shutdown"
}

# ---------- 3. optional WSL restart (applies .wslconfig, resets the VM clock) ----------
if ($RestartWsl) {
    if (-not $isAdmin) { Say BAD "-RestartWsl needs Administrator"; $ok = $false }
    else {
        Say INFO "wsl --shutdown (Docker Desktop will restart its engine)..."
        wsl --shutdown
        Start-Sleep -Seconds 5
        Say INFO "start Docker Desktop if it does not come back by itself, then re-run this script"
        exit 0
    }
}

# ---------- 4. Docker memory ----------
$memTotal = docker info --format "{{.MemTotal}}" 2>$null
if (-not $memTotal) {
    Say BAD "Docker is not running"; exit 1
}
$gb = [math]::Round([double]$memTotal / 1GB, 1)
if ($gb -lt 13) { Say BAD "Docker memory $gb GB (need >= 13.5). .wslconfig not applied? run -Fix -RestartWsl"; $ok = $false }
elseif ($gb -lt 20) { Say OK "Docker memory $gb GB -> base profile (docker-compose.yml only)" }
else {
    $override = Join-Path $root "docker-compose.override.yml"
    if (Test-Path $override) { Say OK "Docker memory $gb GB -> large profile active (docker-compose.override.yml)" }
    else { Say INFO "Docker memory $gb GB: you can enable the large profile: copy compose.large.yml docker-compose.override.yml" }
}

# ---------- 5. container clock vs Binance ----------
Push-Location $root
$img = docker images -q marketpulse-producer 2>$null
if ($img) {
    $res = docker compose run --rm --no-deps producer python clock_check.py 2>$null
    $code = $LASTEXITCODE
    $line = ($res | Select-Object -Last 1)
    if ($code -eq 0) { Say OK "container $line" }
    elseif ($code -eq 1) {
        Say BAD "container $line"
        Say INFO "WSL clock drifted (common after sleep). Fix: Administrator + -Fix -RestartWsl"
        $ok = $false
    } else { Say WARN "container clock: $line" }
} else {
    Say INFO "producer image not built yet; container clock check skipped (run after first build)"
}
Pop-Location

if ($ok) { Say OK "preflight passed" } else { Say BAD "preflight found problems (see above)"; exit 1 }
