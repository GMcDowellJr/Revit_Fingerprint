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


Set-Location $REPO

if ($Run -eq "") {
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  .\corpus_update_runbook.ps1 -Run A    # flatten + apply + placeholders"
    Write-Host "  .\corpus_update_runbook.ps1 -Run B    # authority + patterns + patch"
    Write-Host "  .\corpus_update_runbook.ps1 -Run C    # segments + all/used bundle analysis (use compare_cross_segment.py for cross-segment comparison)"
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
    Write-Host "=== RUN B: Authority / Patterns / Patch ===" -ForegroundColor Green

    Write-Host "--- B1: authority + patterns ---" -ForegroundColor Cyan
    python tools/run_extract_all.py $EXPORTS `
        --out-root $EXPORTS `
        --stages authority,patterns

    Write-Host "--- B2: patch corpus domain_patterns ---" -ForegroundColor Cyan
    python tools\label_synthesis\patch_all_domain_patterns.py `
        --results-root $RESULTS `
        --segments-root $SEGMENTS

    Write-Host "=== RUN B COMPLETE - proceed to Run C ===" -ForegroundColor Green
}

if ($Run -eq "C") {
    Write-Host "=== RUN C: Segments + all/used bundle analysis ===" -ForegroundColor Green
    Write-Host "Run C contract:" -ForegroundColor Cyan
    Write-Host "  All view  = full configured vocabulary for each segment." -ForegroundColor Cyan
    Write-Host "  Used view = project vocabulary excluding conclusively purgeable records." -ForegroundColor Cyan
    Write-Host "  Template, Generic, and most Container roles are provided-vocabulary references;" -ForegroundColor Cyan
    Write-Host "  purge/used interpretation is meaningful primarily for Project targets." -ForegroundColor Cyan

    Write-Host "--- C1: segment manifest ---" -ForegroundColor Cyan
    python tools\build_segment_manifest.py `
        --metadata-file "$RECORDS\file_metadata.csv" `
        --out-dir $RECORDS `
        --enable-parent-bundle-runs

    # C1.5: latent_purgeable.csv is created once and cached forever by
    # _ensure_latent_purgeable() in run_bundle_analysis.py — it does NOT
    # refresh on its own when upstream records/identity_items change.
    # Since Run C already fully reprocesses every segment, force a clean
    # rebuild here rather than silently reusing stale purgeability data.
    Write-Host "--- C1.5: clear stale latent_purgeable.csv ---" -ForegroundColor Cyan
    Get-ChildItem -Path $SEGMENTS -Recurse -Filter "latent_purgeable.csv" -ErrorAction SilentlyContinue | Remove-Item -Force
    $corpusLatentPurgeable = "$RECORDS\latent_purgeable.csv"
    if (Test-Path $corpusLatentPurgeable) { Remove-Item $corpusLatentPurgeable -Force }
	
    # The orchestrator invokes run_bundle_analysis.py with --purge-view both,
    # producing results/bundle_analysis/all/... and results/bundle_analysis/used/...
    # for downstream governance comparisons.
    Write-Host "--- C2: segment orchestrator (produces all-view and used-view bundle analysis) ---" -ForegroundColor Cyan
    python tools/run_segment_orchestrator.py `
        --manifest-file "$RECORDS\segment_manifest.csv" `
        --registry-file "$RECORDS\run_registry.csv" `
        --results-registry-file "$RECORDS\results_registry.csv" `
        --records-dir $RECORDS `
        --exports-dir $EXPORTS `
        --segments-root $SEGMENTS `
        --repo-root $REPO `
        --force `
        --join-policy $JOIN_POL

    Write-Host "--- C2.5: rebuild BI results registry ---" -ForegroundColor Cyan
    python tools\build_results_registry.py `
        --manifest-file "$RECORDS\segment_manifest.csv" `
        --registry-file "$RECORDS\run_registry.csv" `
        --output-file "$RECORDS\results_registry.csv"

    Write-Host "--- C3: re-patch all segment domain_patterns ---" -ForegroundColor Cyan
    python tools\label_synthesis\patch_all_domain_patterns.py `
        --results-root $RESULTS `
        --segments-root $SEGMENTS

    Write-Host "=== RUN C COMPLETE ===" -ForegroundColor Green
    Write-Host "Refresh Power BI: open Fingerprint_Segmented_Bundles.pbix and hit Refresh" -ForegroundColor Green
    Write-Host "Cross-segment comparison: run compare_cross_segment.py separately" -ForegroundColor Cyan
    Write-Host "Reminder: used/purge signals are active-delivery signals primarily for Project targets; do not label Template or Generic stock content as unused bloat." -ForegroundColor Cyan
}

# NOTES
# Incremental behaviour:
#   B2/C3 (patch)  - skips synopsis/modal/curator/llm sources
#   Label synthesis is a separate on-demand step.
#   Run: .\tools\label_refresh_runbook.ps1
#   Synthesizes fragmented labels, patches all domain_patterns.csv files,
#   and exports bundle pattern detail CSVs for Power BI.
#   C2 (segments)  - --force re-runs all; use --segment <id> for one segment
#                    emits both all-view (full configured vocabulary) and used-view
#                    (excluding conclusively purgeable records) bundle analysis.
#                    Used/purge interpretation is meaningful primarily for Project
#                    targets, not Template/Generic standards stock.
#
# Known deferred items:
#   - build_semantic_groups.py column name fix (item_key/item_value vs k/v/q)
#   - is_cad_import: lp.is_import not flowing into domain_patterns.csv
