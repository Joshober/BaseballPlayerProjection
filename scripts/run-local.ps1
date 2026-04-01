# Run the FastAPI backend locally (no Docker).
# Terminal 2: cd frontend && npm install && npm run dev
# Then open http://localhost:5173

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

Write-Host "Starting API at http://127.0.0.1:8000/docs (PYTHONPATH=$root)" -ForegroundColor Green
& $py -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
