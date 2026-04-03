# Data science pipeline: migrate -> optional backfill -> build_features v3 -> data_status -> train_all
# Requires: PostgreSQL reachable (e.g. `docker compose up -d postgres`), DATABASE_URL in .env
# Usage: .\scripts\run-ds-pipeline.ps1
# Optional: -SkipBackfill

param(
    [switch]$SkipBackfill
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not $env:PYTHONPATH) {
    $env:PYTHONPATH = $root
}

$py = "python"
if (Test-Path "$root\.venv\Scripts\python.exe") {
    $py = "$root\.venv\Scripts\python.exe"
}

Write-Host "=== 1) Alembic upgrade head ===" -ForegroundColor Cyan
Push-Location "$root\backend"
& $py -m alembic upgrade head
Pop-Location

if (-not $SkipBackfill) {
    Write-Host "`n=== 2) Backfill player labels (MLB API) ===" -ForegroundColor Cyan
    & $py -m ml.backfill_player_labels
}

Write-Host "`n=== 3) Build engineered features (v3, first-K MiLB seasons) ===" -ForegroundColor Cyan
& $py -m ml.build_features --feature-version v3

Write-Host "`n=== 4) Data status ===" -ForegroundColor Cyan
& $py -m ml.data_status

Write-Host "`n=== 5) Train arrival models (bat + pitch) ===" -ForegroundColor Cyan
& $py -m ml.train_all --feature-version v3

Write-Host "`nDone. Artifacts: data\models\  Manifest: data\models\arrival_manifest.json" -ForegroundColor Green
