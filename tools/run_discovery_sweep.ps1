<# Backward-compatible wrapper for the repository-root PowerShell runbook. #>
& (Join-Path (Split-Path $PSScriptRoot -Parent) "run_discovery_sweep.ps1") @args
exit $LASTEXITCODE
