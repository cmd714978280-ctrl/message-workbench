param(
    [switch]$Replay
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

Write-Host '1/4 Python syntax'
py -m py_compile processing_core.py workflow_store.py server.py phase3.py feishu_business_targets.py feishu_outbox.py setup_business_targets.py benchmark_workflow_replay.py test_workflow_store.py test_processing_core.py test_business_targets.py test_feishu_outbox.py
if ($LASTEXITCODE -ne 0) { throw 'Python syntax check failed.' }

Write-Host '2/4 Python regression suite'
py -m unittest test_phase2 test_phase3 test_ingestion test_processing16 test_processing17 test_workflow_store test_processing_core test_business_targets test_feishu_outbox -q
if ($LASTEXITCODE -ne 0) { throw 'Python regression suite failed.' }

Write-Host '3/4 Frontend syntax and navigation tests'
node --check app.js
if ($LASTEXITCODE -ne 0) { throw 'Frontend syntax check failed.' }
node --check processing-core.js
if ($LASTEXITCODE -ne 0) { throw 'Processing UI syntax check failed.' }
node --check exports.js
if ($LASTEXITCODE -ne 0) { throw 'Feishu export UI syntax check failed.' }
node --test test_dev_audit_frontend.cjs test_workflow_review_frontend.cjs test_processing_core_frontend.cjs test_feishu_outbox_frontend.cjs
if ($LASTEXITCODE -ne 0) { throw 'Frontend navigation tests failed.' }

if ($Replay) {
    Write-Host '4/4 Synthetic 2,000 message replay'
    py benchmark_workflow_replay.py
    if ($LASTEXITCODE -ne 0) { throw 'Synthetic replay failed.' }
} else {
    Write-Host '4/4 Skipped long replay; use -Replay to run it.'
}

Write-Host 'Acceptance checks completed.'
