param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$DexterArgs
)

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonCommand = Get-Command python -ErrorAction SilentlyContinue

if (-not $pythonCommand) {
    $pythonCommand = Get-Command py -ErrorAction SilentlyContinue
}

if (-not $pythonCommand) {
    Write-Error "Python was not found on PATH. Install Python first, then rerun this script."
    exit 1
}

& $pythonCommand.Source (Join-Path $repoRoot "database_setup.py") @DexterArgs
exit $LASTEXITCODE
