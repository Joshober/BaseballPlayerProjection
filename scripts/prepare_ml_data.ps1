# Prepare ML training data: DB status, MLB label backfill, feature build, optional training.
# Requires: repo root .env with DATABASE_URL, PYTHONPATH = repo root (this script sets it).

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

& $py -m ml.prepare_training_data @args
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
