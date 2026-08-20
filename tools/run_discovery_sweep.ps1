<#
.SYNOPSIS
    Runs the full greedy/pareto join_key + sig_hash discovery sweep across every
    domain, sized per-domain from tools/suggest_discovery_params.py's output CSV.

.DESCRIPTION
    suggest_discovery_params.py's own --emit-commands output uses bash-style
    line continuation ("\" at end of line), which does not work in PowerShell.
    This script sidesteps that entirely by building each invocation as a
    PowerShell argument array and splatting it with `&`, rather than
    reconstructing printed command strings. It reads the suggestions CSV
    directly (already written by suggest_discovery_params.py) instead of
    re-deriving sizes, so it stays in sync with whatever that tool last
    computed for the current corpus.

    For each domain it runs:
      - tools/discover_join_policy.py    (1 or 2 invocations -- see below)
      - tools/discover_hash_policy.py --discovery-target sig   (1 invocation)

    Mirrors suggest_discovery_params.py's own _emit_command() split logic:
    a domain gets ONE discover_join_policy.py command when
    suggested_max_k_harsh_validate == suggested_max_k_discover, otherwise TWO
    (one at --policy-modes discover / smaller max-k, one at
    --policy-modes validate,harsh / larger max-k) -- a single combined command
    at the smaller value would under-size harsh/validate's search space.

    Known gap vs. the Python tool: the CSV does not carry
    harsh_pareto_feasible, so this script cannot replicate the (rare,
    --subset-budget < 1 only) case where harsh/validate gets forced to
    --search-modes greedy. That case does not occur at this repo's default
    --subset-budget 20000, but if you re-ran suggest_discovery_params.py with
    a much smaller --subset-budget, check its own stdout/notes column before
    trusting this script's search-modes choice blindly.

    discover_hash_policy.py has no --policy-modes split by design (it runs
    discover,validate,harsh in one pass) and no --warn-only flag, so only one
    invocation is emitted per domain there, using the LARGER
    (suggested_max_k_harsh_validate) of the two suggested max-k values so
    validate/harsh mode has adequate room; discover mode's own search is
    still bounded by max_k but trying more combinations than the minimum
    needed is not a correctness problem, just extra (still sample-scale, not
    combinatorial-explosion-scale) work.

.PARAMETER ExportsRoot
    Root containing exports/, records/, diagnostics/, segments/ -- same
    convention as corpus_update_runbook.ps1's -ExportsRoot.

.PARAMETER RepoRoot
    Path to the Revit_Fingerprint repo checkout (where tools/ and policies/ live).

.PARAMETER SuggestionsCsv
    Path to discovery_param_suggestions.csv. Defaults to
    <ExportsRoot>\diagnostics\discovery_param_suggestions.csv, matching
    suggest_discovery_params.py's own default --out location.

.PARAMETER Domains
    Optional comma-separated allow-list to scope the sweep to specific domains
    (e.g. for re-running just the heavily-truncated ones after raising
    --subset-budget). Default: every domain in the CSV.

.PARAMETER SkipJoin
    Skip the discover_join_policy.py pass entirely.

.PARAMETER SkipSig
    Skip the discover_hash_policy.py (sig_hash) pass entirely.

.PARAMETER WhatIf
    Print every command that would run, without executing anything.

.EXAMPLE
    .\run_discovery_sweep.ps1 -Run

.EXAMPLE
    .\run_discovery_sweep.ps1 -Domains "fill_patterns_drafting,fill_patterns_model" -Run

.EXAMPLE
    .\run_discovery_sweep.ps1 -WhatIf
#>

param(
    [string]$ExportsRoot = "C:\Users\gmcdowell\Documents\Fingerprint_Data",
    [string]$RepoRoot    = "C:\Users\gmcdowell\Documents\Revit_Fingerprint",
    [string]$SuggestionsCsv = "",
    [string]$Domains = "",
    [switch]$SkipJoin,
    [switch]$SkipSig,
    [switch]$WhatIf,
    [switch]$Run
)

# NOTE: deliberately NOT setting $ErrorActionPreference = "Stop" globally.
# This script runs up to ~78 separate python invocations across 39 domains;
# one domain hitting an unexpected error (bad data, a transient file lock,
# a policy JSON edge case) must not take down the other 38. Invoke-Discovery
# below wraps each individual call in try/catch and records failures instead
# of propagating them, so a single bad domain shows up in the FAILED summary
# at the end rather than aborting the sweep silently partway through.

if (-not $Run -and -not $WhatIf) {
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  .\run_discovery_sweep.ps1 -Run                       # execute the full sweep"
    Write-Host "  .\run_discovery_sweep.ps1 -WhatIf                    # print commands only, run nothing"
    Write-Host "  .\run_discovery_sweep.ps1 -Domains 'a,b' -Run        # scope to specific domains"
    Write-Host "  .\run_discovery_sweep.ps1 -SkipSig -Run              # join_key discovery only"
    Write-Host "  .\run_discovery_sweep.ps1 -SkipJoin -Run             # sig_hash discovery only"
    Write-Host ""
    exit 0
}

$RECORDS      = "$ExportsRoot\records"
$SIG_POL      = "$RepoRoot\policies\domain_sig_hash_policies.json"
$JOIN_POL     = "$RepoRoot\policies\domain_join_key_policies.json"
if ([string]::IsNullOrWhiteSpace($SuggestionsCsv)) {
    $SuggestionsCsv = "$ExportsRoot\diagnostics\discovery_param_suggestions.csv"
}
$LOG_DIR = "$ExportsRoot\diagnostics\discover_logs"

if (-not (Test-Path -LiteralPath $SuggestionsCsv)) {
    Write-Host "ERROR: suggestions CSV not found at $SuggestionsCsv" -ForegroundColor Red
    Write-Host "Run this first:" -ForegroundColor Yellow
    Write-Host "  python tools/suggest_discovery_params.py --phase0-dir `"$RECORDS`" --policy-json `"$JOIN_POL`" --emit-commands" -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path -LiteralPath $RepoRoot)) {
    Write-Host "ERROR: RepoRoot not found: $RepoRoot" -ForegroundColor Red
    exit 1
}

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    Write-Host "ERROR: 'python' not found on PATH. Activate the right environment/venv before running this script." -ForegroundColor Red
    exit 1
}

if (-not $WhatIf) {
    New-Item -ItemType Directory -Force -Path $LOG_DIR -ErrorAction Stop | Out-Null
}

Set-Location $RepoRoot

$rows = Import-Csv -Path $SuggestionsCsv
if ($Domains -and $Domains.Trim() -ne "") {
    $allow = $Domains.Split(",") | ForEach-Object { $_.Trim() }
    $rows = $rows | Where-Object { $allow -contains $_.domain }
    if ($rows.Count -eq 0) {
        Write-Host "ERROR: none of the requested domains were found in $SuggestionsCsv" -ForegroundColor Red
        exit 1
    }
}

Write-Host "=== Discovery sweep: $($rows.Count) domain(s) ===" -ForegroundColor Green
Write-Host "records:   $RECORDS"
Write-Host "join pol:  $JOIN_POL"
Write-Host "sig pol:   $SIG_POL"
Write-Host "logs:      $LOG_DIR"
Write-Host ""

$warnLines = New-Object System.Collections.Generic.List[string]
$failed    = New-Object System.Collections.Generic.List[string]
$ranCount  = 0

function Invoke-Discovery {
    param(
        [string]$Label,
        [string[]]$ArgList,
        [string]$LogPath
    )
    Write-Host "  -> $Label" -ForegroundColor Cyan
    if ($WhatIf) {
        Write-Host "     python $($ArgList -join ' ')" -ForegroundColor DarkGray
        return
    }

    $script:ranCount++

    # Defensive: recreate the log dir before every call, not just once up
    # front. Cheap and idempotent, and guards against the directory having
    # been removed/unavailable partway through a long sweep (e.g. an
    # antivirus scan, a sync client, a stray cleanup script).
    $logParent = Split-Path -Path $LogPath -Parent
    New-Item -ItemType Directory -Force -Path $logParent -ErrorAction SilentlyContinue | Out-Null

    $exitCode = -1
    try {
        # 2>&1 merges stderr into stdout for the external process; python has
        # no separate PS verbose/debug/progress streams to worry about, so
        # this is sufficient (no need for the broader *>&1 merge).
        & python @ArgList 2>&1 | Tee-Object -FilePath $LogPath
        $exitCode = $LASTEXITCODE
    } catch {
        $exitCode = -1
        # Make sure something lands in the log even if the pipeline itself
        # threw before Tee-Object ever opened the file (e.g. python missing,
        # a PowerShell-level parameter-binding error).
        "ERROR: invocation threw before/without producing output: $($_.Exception.Message)" |
            Out-File -FilePath $LogPath -Encoding utf8 -Force
    }

    if ($exitCode -ne 0) {
        Write-Host "     FAILED (exit $exitCode) -- see $LogPath" -ForegroundColor Red
        $script:failed.Add("$Label (exit $exitCode)")
    }

    if (Test-Path -LiteralPath $LogPath) {
        $hits = Select-String -LiteralPath $LogPath -Pattern "\[discover\] WARNING|sample_vs_full_diverges.*true" -ErrorAction SilentlyContinue
        foreach ($h in $hits) {
            $script:warnLines.Add("$Label`: $($h.Line.Trim())")
        }
    } else {
        Write-Host "     (no log file was produced -- see FAILED summary)" -ForegroundColor DarkYellow
        $script:failed.Add("$Label (no log file produced)")
    }
}

foreach ($row in $rows) {
    $domain = $row.domain
    Write-Host "[$domain]" -ForegroundColor Yellow

  try {
    $sampleSize  = $row.suggested_sample_size
    $maxFields   = $row.suggested_max_candidate_fields
    $maxKDiscover = $row.suggested_max_k_discover
    $maxKHarsh    = $row.suggested_max_k_harsh_validate
    $stratify     = $row.stratify_by_recommended

    if ([string]::IsNullOrWhiteSpace($maxKDiscover) -or [string]::IsNullOrWhiteSpace($maxKHarsh)) {
        throw "row for domain '$domain' has a blank suggested_max_k_discover/suggested_max_k_harsh_validate -- check $SuggestionsCsv"
    }

    # ---- join_key discovery (discover_join_policy.py) ----
    if (-not $SkipJoin) {
        $base = @(
            "tools/discover_join_policy.py",
            "--phase0-dir", $RECORDS,
            "--domains", $domain,
            "--sample-size", $sampleSize,
            "--max-candidate-fields", $maxFields,
            "--policy-json", $JOIN_POL,
            "--warn-only"
        )
        if ($stratify -and $stratify.Trim() -ne "") {
            $base += @("--stratify-by", $stratify)
        }

        if ($maxKHarsh -eq $maxKDiscover) {
            $cmd = $base + @("--max-k", $maxKDiscover)
            $log = Join-Path $LOG_DIR "join__${domain}__combined.log"
            Invoke-Discovery -Label "join_key ($domain, combined, max-k=$maxKDiscover)" -ArgList $cmd -LogPath $log
        } else {
            $discoverCmd = $base + @("--max-k", $maxKDiscover, "--policy-modes", "discover")
            $logD = Join-Path $LOG_DIR "join__${domain}__discover.log"
            Invoke-Discovery -Label "join_key ($domain, discover, max-k=$maxKDiscover)" -ArgList $discoverCmd -LogPath $logD

            $harshCmd = $base + @("--max-k", $maxKHarsh, "--policy-modes", "validate,harsh")
            $logH = Join-Path $LOG_DIR "join__${domain}__validate_harsh.log"
            Invoke-Discovery -Label "join_key ($domain, validate+harsh, max-k=$maxKHarsh)" -ArgList $harshCmd -LogPath $logH
        }
    }

    # ---- sig_hash discovery (discover_hash_policy.py) ----
    if (-not $SkipSig) {
        $sigMaxK = [Math]::Max([int]$maxKDiscover, [int]$maxKHarsh)
        $sigCmd = @(
            "tools/discover_hash_policy.py",
            "--phase0-dir", $RECORDS,
            "--domains", $domain,
            "--discovery-target", "sig",
            "--sample-size", $sampleSize,
            "--max-candidate-fields", $maxFields,
            "--max-k", $sigMaxK,
            "--policy-json", $SIG_POL
        )
        if ($stratify -and $stratify.Trim() -ne "") {
            $sigCmd += @("--stratify-by", $stratify)
        }
        $logS = Join-Path $LOG_DIR "sig__${domain}.log"
        Invoke-Discovery -Label "sig_hash ($domain, max-k=$sigMaxK)" -ArgList $sigCmd -LogPath $logS
    }
  } catch {
    Write-Host "  DOMAIN-LEVEL FAILURE ($domain): $($_.Exception.Message)" -ForegroundColor Red
    $failed.Add("$domain (domain-level: $($_.Exception.Message))")
  }

    Write-Host ""
}

if ($WhatIf) {
    Write-Host "=== WHATIF: no commands were executed ===" -ForegroundColor Yellow
    exit 0
}

Write-Host "=== SWEEP COMPLETE: $ranCount invocation(s) ===" -ForegroundColor Green

if ($failed.Count -gt 0) {
    Write-Host ""
    Write-Host "FAILED invocations (nonzero exit):" -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "  $_" -ForegroundColor Red }
}

if ($warnLines.Count -gt 0) {
    Write-Host ""
    Write-Host "WARNINGS / divergence flags found (sample-derived candidate did not hold up on the full population, or full-verify explicitly warned):" -ForegroundColor Yellow
    $warnLines | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
    $warnOutPath = Join-Path $LOG_DIR "_warning_summary.txt"
    $warnLines | Out-File -FilePath $warnOutPath -Encoding utf8
    Write-Host ""
    Write-Host "Full warning summary written to: $warnOutPath" -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "No [discover] WARNING or sample_vs_full_diverges=true lines found across any log." -ForegroundColor Green
}

Write-Host ""
Write-Host "Per-domain logs: $LOG_DIR" -ForegroundColor Cyan
