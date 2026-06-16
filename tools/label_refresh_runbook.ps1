$ErrorActionPreference = "Stop"

$REPO     = "C:\Users\gmcdowell\Documents\Revit_Fingerprint"
$EXPORTS  = "C:\Users\gmcdowell\Documents\Fingerprint_Out\exports"
$RESULTS  = "$EXPORTS\results"
$SEGMENTS = "$EXPORTS\segments"
$RECORDS  = "$RESULTS\records"
$CACHE    = "$RESULTS\label_synthesis\llm_name_cache.json"
$LOOKUP   = "$RESULTS\label_synthesis\identity_items_by_joinhash.csv"
$DP_CSV   = "$RESULTS\analysis\domain_patterns.csv"

Set-Location $REPO

Write-Host "--- L1: identity_items lookup (corpus-level, always rebuild) ---" -ForegroundColor Cyan
python tools\label_synthesis\build_identity_items_lookup.py `
    --records-dir $RECORDS `
    --out-dir "$RESULTS\label_synthesis"

if (-not $env:OPENROUTER_API_KEY) {
    $env:OPENROUTER_API_KEY = Read-Host "Enter OPENROUTER_API_KEY"
}

Write-Host "--- L2: LLM synthesis (bundle-eligible domains, corpus-level cache) ---" -ForegroundColor Cyan
foreach ($dom in @(
    "arrowheads",
    "fill_patterns_drafting",
    "fill_patterns_model",
    "line_patterns",
    "line_styles",
    "view_filter_definitions"
)) {
    Write-Host "  synthesizing: $dom" -ForegroundColor Cyan
    # Requires union mode implementation in synthesize_fragmented_labels.py; see agent_prompt_synthesize_union_mode.md.
    $params = @(
        "--exports-dir",           $EXPORTS,
        "--analysis-dir",          "$RESULTS\label_synthesis",
        "--domain",                $dom,
        "--cache",                 $CACHE,
        "--identity-items-lookup", $LOOKUP,
        "--domain-patterns-csv",   $DP_CSV,
        "--provider",              "openrouter",
        "--filter-mode",           "bundles",
        "--segments-root",         $SEGMENTS,
        "--registry-file",         "$RECORDS\run_registry.csv",
        "--workers",               "8"
    )
    python -m tools.label_synthesis.synthesize_fragmented_labels @params
    # Interim fallback (--filter-mode candidates) until union mode is confirmed:
    # "--filter-mode",  "candidates"
    # Remove --segments-root and --registry-file when using candidates mode.
}

Write-Host "--- L3: patch all domain_patterns (corpus + all segments) ---" -ForegroundColor Cyan
python tools\label_synthesis\patch_all_domain_patterns.py `
    --results-root $RESULTS `
    --segments-root $SEGMENTS

Write-Host "--- L4: export bundle pattern detail (completed bundle segments x all/used) ---" -ForegroundColor Cyan
$registry = Import-Csv "$RECORDS\run_registry.csv"
$active = $registry | Where-Object {
    $_.run_type -eq "bundle" -and $_.status -eq "complete"
}
foreach ($seg in $active) {
    foreach ($view in @("all", "used")) {
        $bundleViewDir = "$SEGMENTS\$($seg.output_folder)\results\bundle_analysis\$view"
        $bundleFiles = @()
        if (Test-Path $bundleViewDir) {
            $bundleFiles = @(Get-ChildItem $bundleViewDir -Recurse -Filter "bundles.csv" -File -ErrorAction SilentlyContinue)
        }
        if ($bundleFiles.Count -eq 0) {
            Write-Host "  skipping segment=$($seg.output_folder)  view=$view (no bundle_analysis/$view/*/bundles.csv)" -ForegroundColor Yellow
            continue
        }

        $outDir = "$SEGMENTS\$($seg.output_folder)\results\bi_export\$view"
        Write-Host "  segment=$($seg.output_folder)  view=$view" -ForegroundColor Cyan
        python tools\export_bundle_pattern_detail.py `
            --output-folder $seg.output_folder `
            --segments-root $SEGMENTS `
            --records-dir   $RECORDS `
            --purge-view    $view `
            --out-dir       $outDir
    }
}

Write-Host ""
Write-Host "=== LABEL REFRESH COMPLETE ===" -ForegroundColor Green
Write-Host "Refresh Power BI: open Fingerprint_Segmented_Bundles.pbix and hit Refresh" -ForegroundColor Green

# NOTES
# When to run:
#   After corpus_update_runbook.ps1 Run C completes and the corpus is stable.
#   After adding new extraction domains that have bundle analysis output.
#   After curator label review (--import-results workflow).
#   Not needed on every corpus update — the LLM cache handles incremental runs.
#
# Incremental behaviour:
#   L1 (lookup)    - always rebuild, fast ~2 min
#   L2 (synthesis) - skips join_hashes already in cache; only new patterns cost tokens
#   L3 (patch)     - skips rows with authoritative sources (curator/synopsis/modal)
#   L4 (export)    - overwrites BI export CSVs for completed bundle runs; fast, no API calls
#
# Adding domains to L2:
#   Add the domain name to the $dom array in the foreach loop.
#   Ensure a domain_prompts/{domain}.py module exists for best results;
#   falls back to generic prompt if absent.
#
# Union mode dependency:
#   L2 uses --filter-mode bundles with --segments-root and --registry-file.
#   This requires the union mode implementation in synthesize_fragmented_labels.py.
#   Until confirmed, use --filter-mode candidates (interim fallback noted inline).
#
# Force-refresh all cached labels (after major prompt module changes):
#   Add --force-refresh to the synthesize invocation in L2.
#   Then re-run L3 and L4.
