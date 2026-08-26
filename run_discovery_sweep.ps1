<# Thin Windows entry point; all behavior is implemented by the shared Python orchestrator. #>
param(
 [string]$ExportsRoot = ".\Fingerprint_Data",
 [string]$RepoRoot = $PSScriptRoot,
 [string]$SuggestionsCsv = "",
 [string]$Domains = "",
 [switch]$SkipJoin, [switch]$SkipSig, [switch]$Force,
 [switch]$WhatIf, [switch]$Run,
 [string]$Workers = ""
)
$argsList = @("run_discovery_sweep.py", "--exports-root", $ExportsRoot, "--repo-root", $RepoRoot)
if ($SuggestionsCsv) { $argsList += @("--suggestions-csv", $SuggestionsCsv) }
if ($Domains) { $argsList += @("--domains", $Domains) }
if ($SkipJoin) { $argsList += "--skip-join" }; if ($SkipSig) { $argsList += "--skip-sig" }
if ($Force) { $argsList += "--force" }; if ($WhatIf) { $argsList += "--what-if" }; if ($Run) { $argsList += "--run" }
if ($Workers) { $argsList += @("--workers", $Workers) }
Push-Location $RepoRoot
try { & python @argsList; exit $LASTEXITCODE } finally { Pop-Location }
