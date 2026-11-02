$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$healthUrl = 'http://127.0.0.1:8799/api/status'
New-Item -ItemType Directory -Path (Join-Path $projectRoot 'data') -Force | Out-Null

try {
    $health = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 2
    Write-Output ('工作台已运行：' + $healthUrl + '；消息同步：' + $(if ($health.sync_enabled) { '开启' } else { '暂停' }))
    return
} catch {
    # Start the local service when no healthy workbench responds.
}

Start-Process -FilePath 'py' -ArgumentList 'server.py' -WorkingDirectory $projectRoot `
    -WindowStyle Hidden -RedirectStandardOutput (Join-Path $projectRoot 'data\server.stdout.log') `
    -RedirectStandardError (Join-Path $projectRoot 'data\server.stderr.log')

for ($attempt = 0; $attempt -lt 40; $attempt++) {
    Start-Sleep -Milliseconds 250
    try {
        $health = Invoke-RestMethod -Uri $healthUrl -TimeoutSec 2
        Write-Output ('工作台已启动：http://127.0.0.1:8799/；消息同步：' + $(if ($health.sync_enabled) { '开启' } else { '暂停' }))
        return
    } catch {
        # The Python launcher may exit after spawning the interpreter.
    }
}
throw '工作台未能启动；请查看 data/server.stderr.log。'
