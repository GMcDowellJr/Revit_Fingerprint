param(
    [ValidateSet("A","B","C")]
    [string]$Run = ""
)

$ErrorActionPreference = "Stop"

$REPO     = "C:\Users\gmcdowell\Documents\Revit_Fingerprint"
$EXPORTS  = "C:\Users\gmcdowell\Documents\Fingerprint_Out\exports"
$RESULTS  = "$EXPORTS\results"
$SEGMENTS = "$EXPORTS\segments"
$RECORDS  = "$RESULTS\records"
$SIG_POL  = "$REPO\policies\domain_sig_hash_policies.json"
$JOIN_POL = "$REPO\policies\domain_join_key_policies.json"
$CACHE    = "$RESULTS\label_synthesis\llm_name_cache.json"
$LOOKUP   = "$RESULTS\label_synthesis\identity_items_by_joinhash.csv"
$DP_CSV   = "$RESULTS\analysis\domain_patterns.csv"

# $env:OPENROUTER_API_KEY = "sk-or-v1-..."

Set-Location $REPO

if ($Run -eq "") {
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  .\corpus_update_runbook.ps1 -Run A    # flatten + apply + placeholders"
    Write-Host "  .\corpus_update_runbook.ps1 -Run B    # authority + patterns + synthesis + patch"
    Write-Host "  .\corpus_update_runbook.ps1 -Run C    # segments (use compare_cross_segment.py for cross-segment comparison)"
    Write-Host ""
    Write-Host "MANDATORY PAUSE between Run A and Run B:"
    Write-Host "  Edit $RECORDS\file_metadata.csv"
    Write-Host "  Set for each new file:"
    Write-Host "    governance_role  ->  Container | Template | Project | Generic"
    Write-Host "    client_label     ->  client name or internal identifier"
    Write-Host "    unit_system      ->  imperial | metric"
    Write-Host ""
    exit 0
}

if ($Run -eq "A") {
    Write-Host "=== RUN A: Flatten / Apply / Placeholders ===" -ForegroundColor Green

    python tools/run_extract_all.py $EXPORTS `
        --out-root $EXPORTS `
        --stages sig_hash,flatten,apply,placeholders `
        --sig-hash-policy $SIG_POL `
        --join-policy $JOIN_POL

    Write-Host ""
    Write-Host "=== RUN A COMPLETE ===" -ForegroundColor Yellow
    Write-Host "NEXT: Edit file_metadata.csv before running Run B" -ForegroundColor Yellow
    Write-Host "  File: $RECORDS\file_metadata.csv" -ForegroundColor Yellow
    Write-Host "  Set for each new file:" -ForegroundColor Yellow
    Write-Host "    governance_role  ->  Container | Template | Project | Generic" -ForegroundColor Yellow
    Write-Host "    client_label     ->  client name or internal identifier" -ForegroundColor Yellow
    Write-Host "    unit_system      ->  imperial | metric" -ForegroundColor Yellow
    Write-Host "Then run: .\corpus_update_runbook.ps1 -Run B" -ForegroundColor Yellow
}

if ($Run -eq "B") {
    Write-Host "=== RUN B: Authority / Patterns / Synthesis / Patch ===" -ForegroundColor Green

    # Set API key for synthesis — edit this line or set before running
    if (-not $env:OPENROUTER_API_KEY) {
        $env:OPENROUTER_API_KEY = Read-Host "Enter OPENROUTER_API_KEY"
    }

    Write-Host "--- B1: authority + patterns ---" -ForegroundColor Cyan
    python tools/run_extract_all.py $EXPORTS `
        --out-root $EXPORTS `
        --stages authority,patterns

    Write-Host "--- B2: identity_items lookup ---" -ForegroundColor Cyan
    python tools\label_synthesis\build_identity_items_lookup.py `
        --records-dir $RECORDS `
        --out-dir "$RESULTS\label_synthesis"

    Write-Host "--- B3: LLM synthesis ---" -ForegroundColor Cyan
    foreach ($dom in @(
        "fill_patterns_drafting",
        "fill_patterns_model",
        "line_patterns",
        "arrowheads",
        "line_styles"
    )) {
        Write-Host "  synthesizing: $dom" -ForegroundColor Cyan
        $params = @(
            "--exports-dir",           $EXPORTS,
            "--analysis-dir",          "$RESULTS\label_synthesis",
            "--domain",                $dom,
            "--cache",                 $CACHE,
            "--identity-items-lookup", $LOOKUP,
            "--provider",              "openrouter",
            "--filter-mode",           "candidates",
            "--domain-patterns-csv",   $DP_CSV,
            "--workers",               "3"
        )
        python -m tools.label_synthesis.synthesize_fragmented_labels @params
    }

    Write-Host "--- B4: patch corpus domain_patterns ---" -ForegroundColor Cyan
    python tools\label_synthesis\patch_all_domain_patterns.py `
        --results-root $RESULTS `
        --segments-root $SEGMENTS

    Write-Host "=== RUN B COMPLETE - proceed to Run C ===" -ForegroundColor Green
}

if ($Run -eq "C") {
    Write-Host "=== RUN C: Segments ===" -ForegroundColor Green

    Write-Host "--- C1: segment manifest ---" -ForegroundColor Cyan
    python tools\build_segment_manifest.py `
        --metadata-file "$RECORDS\file_metadata.csv" `
        --out-dir $RECORDS `
        --enable-parent-bundle-runs

    Write-Host "--- C2: segment orchestrator ---" -ForegroundColor Cyan
    python tools/run_segment_orchestrator.py `
        --manifest-file "$RECORDS\segment_manifest.csv" `
        --registry-file "$RECORDS\run_registry.csv" `
        --records-dir $RECORDS `
        --exports-dir $EXPORTS `
        --segments-root $SEGMENTS `
        --repo-root $REPO `
        --force `
        --join-policy $JOIN_POL

    Write-Host "--- C3: re-patch all segment domain_patterns ---" -ForegroundColor Cyan
    python tools\label_synthesis\patch_all_domain_patterns.py `
        --results-root $RESULTS `
        --segments-root $SEGMENTS

    Write-Host "=== RUN C COMPLETE ===" -ForegroundColor Green
    Write-Host "Refresh Power BI: open Fingerprint_Segmented_Bundles.pbix and hit Refresh" -ForegroundColor Green
    Write-Host "Cross-segment comparison: run compare_cross_segment.py separately" -ForegroundColor Cyan
}

# NOTES
# Incremental behaviour:
#   B2 (lookup)    - always rebuild, fast ~2 min
#   B3 (synthesis) - skips cached join_hashes, only new patterns cost tokens
#   B4/C3 (patch)  - skips synopsis/modal/curator/llm sources
#   C2 (segments)  - --force re-runs all; use --segment <id> for one segment
#
# After major synthesis additions (new prompt modules or --force-refresh):
#   Replace B4 with full patterns re-emit:
#   python tools/run_extract_all.py $EXPORTS --out-root $EXPORTS --stages patterns
#
# Known deferred items:
#   - build_semantic_groups.py column name fix (item_key/item_value vs k/v/q)
#   - is_cad_import: lp.is_import not flowing into domain_patterns.csv
#   - Bundle-filtered synthesis: --filter-mode bundles --bundle-dir per segment