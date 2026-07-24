# collection_label vs. governance_role (manual file_metadata.csv entry)
# ------------------------------------------------------------------------
# collection_label answers "why was this file captured" - the standards/
# resource collection it was pulled for (e.g. "BC_2270 Standards", "Sutter
# Standards"). governance_role answers "what does this file do" - its
# behavioral function (Template / Container / Project / Generic). The two
# are independent and must both be set on their own merits: a
# governance_role=Project file can still carry a collection_label if it is
# kept as a reference exemplar within a client's standards collection
# rather than a live tracked project. Do not infer one from the other.

param(
    [ValidateSet("A","B","C")]
    [string]$Run = "",
    [switch]$ForceAll,
    [switch]$NameKey
)

$ErrorActionPreference = "Stop"

$REPO         = "C:\Users\gmcdowell\Documents\Revit_Fingerprint"
$EXPORTS      = "C:\Users\gmcdowell\Documents\Fingerprint_Out\exports"
$RESULTS      = "$EXPORTS\results"
$SEGMENTS     = "$EXPORTS\segments"
$RECORDS      = "$RESULTS\records"
$SIG_POL      = "$REPO\policies\domain_sig_hash_policies.json"
$JOIN_POL     = "$REPO\policies\domain_join_key_policies.json"
$NAME_KEY_POL = "$REPO\policies\domain_name_key_policies.json"
$NAME_KEY_CSV = "$RESULTS\name_key\name_key_results.csv"


Set-Location $REPO

if ($Run -eq "") {
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  .\corpus_update_runbook.ps1 -Run A    # flatten + apply + placeholders"
    Write-Host "  .\corpus_update_runbook.ps1 -Run B    # authority + patterns + patch"
    Write-Host "  .\corpus_update_runbook.ps1 -Run C    # segments + all/used bundle analysis (use compare_cross_segment.py for cross-segment comparison)"
    Write-Host "  .\corpus_update_runbook.ps1 -Run C -ForceAll   # Run C, but re-run every segment regardless of registry status"
    Write-Host ""
    Write-Host "  -ForceAll (Run C only): registry-driven skip is the default - a segment is"
    Write-Host "    re-run only if its file population changed since the last complete run."
    Write-Host "    Pass -ForceAll after a sig_hash/join_hash policy change (population_hash is"
    Write-Host "    membership-only and cannot detect those) to force a full-corpus rebuild."
    Write-Host ""
    Write-Host "  -NameKey (Run A/B/C, opt-in, additive): also produce the Canonical Name"
    Write-Host "    Identity Projection (join_key_name_identity) alongside the default"
    Write-Host "    join_hash output. Does NOT change any default Run A/B/C output -- see"
    Write-Host "    audit_results/audit_8 and audit_9 for what this does and does not cover."
    Write-Host "      -Run A -NameKey   # parse exports once, corpus-wide -> $NAME_KEY_CSV"
    Write-Host "      -Run B -NameKey   # OPTIONAL whole-corpus (unsegmented) name patterns;"
    Write-Host "                        # not required before Run C, which re-clusters per segment"
    Write-Host "      -Run C -NameKey   # also writes results/bundle_analysis/name/all/ per segment"
    Write-Host "                        # (requires -Run A -NameKey to have been run first)"
    Write-Host ""
    Write-Host "MANDATORY PAUSE between Run A and Run B:"
    Write-Host "  Edit $RECORDS\file_metadata.csv"
    Write-Host "  Set for each new file:"
    Write-Host "    governance_role       ->  Container | Template | Project | Generic"
    Write-Host "    client_label          ->  client name or internal identifier (e.g. 'Stantec' for internal/no-external-client work)"
    Write-Host "    business_center_label ->  bare numeric business center code (e.g. '2014'), or '0000'/'BC_0000' for enterprise-scoped work"
    Write-Host "    collection_label      ->  standards/resource collection this file belongs to (optional; independent of governance_role - see header comment)"
    Write-Host "    unit_system           ->  imperial | metric"
    Write-Host ""
    Write-Host "  Run B hard-fails if client_label or business_center_label is blank or an N/A spelling - see"
    Write-Host "  run_extract_all.py's _check_governance_field_completeness()."
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

    if ($NameKey) {
        Write-Host ""
        Write-Host "--- A-NameKey: parse exports for name-identity projection (join_key_name_identity) ---" -ForegroundColor Cyan
        python tools\apply_name_key_policy.py `
            --export-dir $EXPORTS `
            --name-key-policy $NAME_KEY_POL `
            --out $NAME_KEY_CSV
        Write-Host "  wrote $NAME_KEY_CSV" -ForegroundColor Cyan
    }

    Write-Host ""
    Write-Host "=== RUN A COMPLETE ===" -ForegroundColor Yellow
    Write-Host "NEXT: Edit file_metadata.csv before running Run B" -ForegroundColor Yellow
    Write-Host "  File: $RECORDS\file_metadata.csv" -ForegroundColor Yellow
    Write-Host "  Set for each new file:" -ForegroundColor Yellow
    Write-Host "    governance_role       ->  Container | Template | Project | Generic" -ForegroundColor Yellow
    Write-Host "    client_label          ->  client name or internal identifier (e.g. 'Stantec' for internal/no-external-client work)" -ForegroundColor Yellow
    Write-Host "    business_center_label ->  bare numeric business center code (e.g. '2014'), or '0000'/'BC_0000' for enterprise-scoped work" -ForegroundColor Yellow
    Write-Host "    collection_label      ->  standards/resource collection this file belongs to (optional; independent of governance_role - see header comment)" -ForegroundColor Yellow
    Write-Host "    unit_system           ->  imperial | metric" -ForegroundColor Yellow
    Write-Host "  Run B hard-fails if client_label or business_center_label is blank or an N/A spelling." -ForegroundColor Yellow
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

    if ($NameKey) {
        Write-Host "--- B-NameKey (OPTIONAL; not required before Run C -- Run C re-clusters per segment) ---" -ForegroundColor Cyan
        if (-not (Test-Path $NAME_KEY_CSV)) {
            Write-Host "  SKIPPED: $NAME_KEY_CSV not found -- run '.\corpus_update_runbook.ps1 -Run A -NameKey' first." -ForegroundColor Yellow
        } else {
            python tools\generate_name_key_patterns.py `
                --comparison-target name `
                --name-key-csv $NAME_KEY_CSV `
                --out-root "$RESULTS\name_key\patterns"
        }
    }

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
    # _ensure_latent_purgeable() in run_bundle_analysis.py - it does NOT
    # refresh on its own when upstream records/identity_items change.
    # Only clear it on a -ForceAll full-corpus rebuild: registry-driven skip
    # (default) leaves untouched segments' cached data alone, since orchestrator
    # never revisits a skipped segment to regenerate this file.
    if ($ForceAll) {
        Write-Host "--- C1.5: clear stale latent_purgeable.csv (-ForceAll) ---" -ForegroundColor Cyan
        Get-ChildItem -Path $SEGMENTS -Recurse -Filter "latent_purgeable.csv" -ErrorAction SilentlyContinue | Remove-Item -Force
        $corpusLatentPurgeable = "$RECORDS\latent_purgeable.csv"
        if (Test-Path $corpusLatentPurgeable) { Remove-Item $corpusLatentPurgeable -Force }
    } else {
        Write-Host "--- C1.5: skipped (pass -ForceAll to clear cached latent_purgeable.csv) ---" -ForegroundColor Cyan
    }

    # The orchestrator invokes run_bundle_analysis.py with --purge-view both,
    # producing results/bundle_analysis/all/... and results/bundle_analysis/used/...
    # for downstream governance comparisons.
    Write-Host "--- C2: segment orchestrator (produces all-view and used-view bundle analysis) ---" -ForegroundColor Cyan
    $forceArg = @()
    if ($ForceAll) { $forceArg = @("--force") }

    # --comparison-target both (not name): runs the existing join_hash leg (all/used) AND
    # the name-projection leg (name/all) in the same per-segment pass, so C2 doesn't need
    # to run twice. --comparison-target defaults to "config" (this script's prior,
    # unconditional behaviour) when -NameKey is not passed.
    $nameKeyArgs = @()
    if ($NameKey) {
        if (-not (Test-Path $NAME_KEY_CSV)) {
            Write-Host "ERROR: -NameKey requires $NAME_KEY_CSV to exist -- run '.\corpus_update_runbook.ps1 -Run A -NameKey' first." -ForegroundColor Red
            exit 1
        }
        Write-Host "--- C2-NameKey: also producing results/bundle_analysis/name/all/ per segment ---" -ForegroundColor Cyan
        $nameKeyArgs = @("--comparison-target", "both", "--name-key-results-csv", $NAME_KEY_CSV)
    }

    python tools/run_segment_orchestrator.py `
        --manifest-file "$RECORDS\segment_manifest.csv" `
        --registry-file "$RECORDS\run_registry.csv" `
        --results-registry-file "$RECORDS\results_registry.csv" `
        --records-dir $RECORDS `
        --exports-dir $EXPORTS `
        --segments-root $SEGMENTS `
        --repo-root $REPO `
        --join-policy $JOIN_POL `
        @forceArg `
        @nameKeyArgs

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
    if ($NameKey) {
        Write-Host "Name-projection output: {segment folder}\results\bundle_analysis\name\all\... (join_key_name_identity instead of join_hash; ALL view only -- no used-view/compare/share-profile equivalent yet, see audit_results/audit_8 and audit_9)" -ForegroundColor Cyan
    }
}

# NOTES
# Incremental behaviour:
#   B2/C3 (patch)  - skips synopsis/modal/curator/llm sources
#   Label synthesis is a separate on-demand step.
#   Run: .\tools\label_refresh_runbook.ps1
#   Synthesizes fragmented labels, patches all domain_patterns.csv files,
#   and exports bundle pattern detail CSVs for Power BI.
#   C2 (segments)  - registry-driven skip by default: a segment only re-runs if its
#                    file population changed since the last complete run (see
#                    run_registry.csv population_hash). Pass -ForceAll to this script
#                    to re-run every segment regardless of registry status (needed
#                    after a sig_hash/join_hash policy change, since population_hash
#                    is membership-only and won't detect that). Orchestrator's own
#                    --segment <id> / --force flags still work for targeted manual runs.
#                    Emits both all-view (full configured vocabulary) and used-view
#                    (excluding conclusively purgeable records) bundle analysis.
#                    Used/purge interpretation is meaningful primarily for Project
#                    targets, not Template/Generic standards stock.
#
# Known deferred items:
#   - build_semantic_groups.py column name fix (item_key/item_value vs k/v/q)
#   - is_cad_import: lp.is_import not flowing into domain_patterns.csv
#
# -NameKey (opt-in, additive -- does not change default Run A/B/C output):
#   -Run A -NameKey  - apply_name_key_policy.py parses $EXPORTS once, corpus-wide,
#                      -> $NAME_KEY_CSV. No file_metadata.csv dependency, unlike Run A->B's
#                      mandatory pause; re-run whenever $EXPORTS gets new files.
#   -Run B -NameKey  - OPTIONAL: generate_name_key_patterns.py --comparison-target name,
#                      whole-corpus (unsegmented) pattern set. NOT required before Run C,
#                      which re-clusters name-key rows per segment internally (step 2b).
#   -Run C -NameKey  - run_segment_orchestrator.py --comparison-target both
#                      --name-key-results-csv $NAME_KEY_CSV. Requires -Run A -NameKey to
#                      have been run first (hard-fails otherwise). Adds
#                      results/bundle_analysis/name/all/ per segment alongside the
#                      existing all/used folders -- ALL view only (no used-view/compare/
#                      share-profile equivalent for the name projection yet). See
#                      audit_results/audit_8_bundle_pipeline_name_projection.md and
#                      audit_results/audit_9_segment_orchestrator_name_projection.md.
