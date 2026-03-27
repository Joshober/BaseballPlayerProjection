param(
    [Parameter(Mandatory = $true)]
    [string]$DatabaseUrl
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".env.example")) {
    throw ".env.example not found in project root."
}

Copy-Item ".env.example" ".env" -Force

$envContent = Get-Content ".env" -Raw
$envContent = [regex]::Replace(
    $envContent,
    "(?m)^DATABASE_URL=.*$",
    "DATABASE_URL=$DatabaseUrl"
)
Set-Content ".env" $envContent

& ".\.venv\Scripts\python.exe" -m db.init_db --schema schema.sql

Write-Host "Done: .env created, DATABASE_URL set, schema initialized."
