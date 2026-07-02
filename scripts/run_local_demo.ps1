param(
    [int]$TicketCount = 80,
    [switch]$UseCurrentOpenAI
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $ProjectRoot

$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (Test-Path $VenvPython) {
    $Python = $VenvPython
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $Python = "python"
} else {
    throw "Python was not found. Create a .venv or install Python 3.11, then rerun this script."
}

if (-not $UseCurrentOpenAI) {
    Remove-Item Env:OPENAI_API_KEY -ErrorAction SilentlyContinue
}

$Arguments = @("-m", "src.orchestration.local_demo", "--ticket-count", $TicketCount)
if ($UseCurrentOpenAI) {
    $Arguments += "--use-current-openai"
}

& $Python @Arguments
