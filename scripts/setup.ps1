param([switch]$Repair)
$ErrorActionPreference = 'Stop'
$dexterRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location $dexterRoot
try {
    if ($Repair) { & python dexter_bootstrap.py --repair }
    else { & python dexter_bootstrap.py }
    if ($LASTEXITCODE) { throw 'Dexter environment setup failed.' }
    & (Join-Path $dexterRoot '.venv\Scripts\python.exe') Dexter.py setup
    if ($LASTEXITCODE) { throw 'Setup needs attention; see the checks above.' }
} finally { Pop-Location }
