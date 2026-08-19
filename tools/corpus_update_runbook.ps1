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
    [switch]$NameKey,
    # Root containing the raw *.json exports plus the results\/segments\ folders nested
    # under it (previously hardcoded as .../Fingerprint_Out/exports; moved to OneDrive as
    # of 2026-07 -- exposed as a param so a future move is a CLI override, not a script edit).
    [string]$ExportsRoot = "C:\Users\gmcdowell\OneDrive - Stantec\Documents\Fingerprint_Data"
)

$ErrorActionPreference = "Stop"

$REPO         = "C:\Users\gmcdowell\Documents\Revit_Fingerprint"
$EXPORTS      = "$ExportsRoot\exports"
# $RESULTS is now an alias for $ExportsRoot itself, not a "results\" subfolder
# -- every $RESULTS-derived path below (name_key\, records\, and whatever
# run_extract_all.py/patch_all_domain_patterns.py write under --out-root/
# --results-root) now lands directly under $ExportsRoot, alongside exports\
# and segments\, instead of nested one level deeper under a "results\" folder.
$RESULTS      = $ExportsRoot
$SEGMENTS     = "$ExportsRoot\segments"
$RECORDS      = "$RESULTS\records"
$SIG_POL      = "$REPO\policies\domain_sig_hash_policies.json"
$JOIN_POL     = "$REPO\policies\domain_join_key_policies.json"
$NAME_KEY_POL = "$REPO\policies\domain_name_key_policies.json"
$NAME_KEY_CSV = "$RESULTS\name_key\name_key_results.csv"


Set-Location $REPO

# $ErrorActionPreference = "Stop" only promotes PowerShell-native terminating
# errors -- it does NOT turn a nonzero exit code from an external process (like
# `python ...`) into a stop. Without this check, a mid-flatten crash in Run A
# silently leaves records.csv/file_metadata.csv untouched at their prior mtimes
# while the script still prints "RUN A COMPLETE" and proceeds -- the corpus then
# looks up to date when it isn't, and downstream Run C segments quietly build
# against stale data (see the imperial_container_2014 step=bundle incident).
function Invoke-Checked {
    param([Parameter(Mandatory)][string]$StepName)
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $StepName failed (exit code $LASTEXITCODE)" -ForegroundColor Red
        exit 1
    }
}

if ($Run -eq "") {
    Write-Host ""
    Write-Host "Usage:"
    Write-Host "  .\corpus_update_runbook.ps1 -Run A    # flatten + apply + placeholders"
    Write-Host "  .\corpus_update_runbook.ps1 -Run B    # authority + patterns + patch"
    Write-Host "  .\corpus_update_runbook.ps1 -Run C    # segments + all/used bundle analysis (use compare_cross_segment.py for cross-segment comparison)"
    Write-Host "  .\corpus_update_runbook.ps1 -Run C -ForceAll   # Run C, but re-run every segment regardless of registry status"
    Write-Host "  .\corpus_update_runbook.ps1 -Run A -ExportsRoot 'D:\Somewhere\Else'   # override the exports/results/segments root"
    Write-Host ""
    Write-Host "  -ExportsRoot (default: $ExportsRoot):"
    Write-Host "    Root containing the raw *.json exports plus the results\ and segments\"
    Write-Host "    folders nested under it. Override if the data has moved without editing"
    Write-Host "    this script."
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
    Write-Host "      -Run C -NameKey   # also writes results/bundle_analysis/name_all/ per segment"
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
        --out-root $RESULTS `
        --out-root-is-results-root `
        --stages sig_hash,flatten,apply,placeholders `
        --sig-hash-policy $SIG_POL `
        --join-policy $JOIN_POL
    Invoke-Checked -StepName "Run A: flatten/apply/placeholders"

    if ($NameKey) {
        Write-Host ""
        Write-Host "--- A-NameKey: parse exports for name-identity projection (join_key_name_identity) ---" -ForegroundColor Cyan
        python tools\apply_name_key_policy.py `
            --export-dir $EXPORTS `
            --name-key-policy $NAME_KEY_POL `
            --out $NAME_KEY_CSV
        Invoke-Checked -StepName "Run A-NameKey: apply_name_key_policy.py"
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
        --out-root $RESULTS `
        --out-root-is-results-root `
        --stages authority,patterns
    Invoke-Checked -StepName "Run B1: authority/patterns"

    Write-Host "--- B2: patch corpus domain_patterns ---" -ForegroundColor Cyan
    python tools\label_synthesis\patch_all_domain_patterns.py `
        --results-root $RESULTS `
        --segments-root $SEGMENTS
    Invoke-Checked -StepName "Run B2: patch_all_domain_patterns.py"

    if ($NameKey) {
        Write-Host "--- B-NameKey (OPTIONAL; not required before Run C -- Run C re-clusters per segment) ---" -ForegroundColor Cyan
        if (-not (Test-Path $NAME_KEY_CSV)) {
            Write-Host "  SKIPPED: $NAME_KEY_CSV not found -- run '.\corpus_update_runbook.ps1 -Run A -NameKey' first." -ForegroundColor Yellow
        } else {
            python tools\generate_name_key_patterns.py `
                --comparison-target name `
                --name-key-csv $NAME_KEY_CSV `
                --out-root "$RESULTS\name_key\patterns"
            Invoke-Checked -StepName "Run B-NameKey: generate_name_key_patterns.py"
        }
    }

    Write-Host "=== RUN B COMPLETE - proceed to Run C ===" -ForegroundColor Green
}

if ($Run -eq "C") {
    Write-Host "=== RUN C: Segments + all/used bundle analysis ===" -ForegroundColor Green
    Write-Host "IF FINGERPRINT_DATA IS UNDER A ONEDRIVE-SYNCED FOLDER: bundle segment runs can" -ForegroundColor Yellow
    Write-Host "  fail near-100% at clear_stale_name_all with [WinError 5] Access is denied on" -ForegroundColor Yellow
    Write-Host "  results\bundle_analysis\name_all\<domain>. Pausing OneDrive sync did NOT fix" -ForegroundColor Yellow
    Write-Host "  this in testing (2026-08-19) -- root cause is unconfirmed, not necessarily" -ForegroundColor Yellow
    Write-Host "  OneDrive itself. Confirmed workaround: relocate Fingerprint_Data outside any" -ForegroundColor Yellow
    Write-Host "  OneDrive-synced folder and pass -ExportsRoot pointing at the new location." -ForegroundColor Yellow
    Write-Host "  This starves comparison_registry.csv -- see CLAUDE.md Warnings for detail." -ForegroundColor Yellow
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
    Invoke-Checked -StepName "Run C1: build_segment_manifest.py"

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
    # the name-projection leg (name_all) in the same per-segment pass, so C2 doesn't need
    # to run twice. --comparison-target defaults to "config" (this script's prior,
    # unconditional behaviour) when -NameKey is not passed.
    $nameKeyArgs = @()
    if ($NameKey) {
        if (-not (Test-Path $NAME_KEY_CSV)) {
            Write-Host "ERROR: -NameKey requires $NAME_KEY_CSV to exist -- run '.\corpus_update_runbook.ps1 -Run A -NameKey' first." -ForegroundColor Red
            exit 1
        }
        # Freshness guard: a stale name_key_results.csv (e.g. Run A ran again for new/
        # changed exports but -Run A -NameKey was forgotten) would silently omit those
        # files from the name projection while the script still reports success. Compare
        # against records.csv, the join_hash leg's own always-rewritten Run A output, as a
        # proxy for "when was Run A last actually run" (see PR #390 review).
        $recordsCsv = "$RECORDS\records.csv"
        if (Test-Path $recordsCsv) {
            $nameKeyAge = (Get-Item $NAME_KEY_CSV).LastWriteTimeUtc
            $recordsAge = (Get-Item $recordsCsv).LastWriteTimeUtc
            if ($nameKeyAge -lt $recordsAge) {
                Write-Host "ERROR: $NAME_KEY_CSV ($nameKeyAge UTC) is older than $recordsCsv ($recordsAge UTC)." -ForegroundColor Red
                Write-Host "  This usually means Run A ran again for new/changed exports without -NameKey, so" -ForegroundColor Red
                Write-Host "  name_key_results.csv is stale and would silently miss those files. Re-run:" -ForegroundColor Red
                Write-Host "    .\corpus_update_runbook.ps1 -Run A -NameKey" -ForegroundColor Red
                exit 1
            }
        }
        Write-Host "--- C2-NameKey: also producing results/bundle_analysis/name_all/ per segment ---" -ForegroundColor Cyan
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
    # Non-fatal by design: the orchestrator processes many independent segments and
    # returns nonzero if ANY segment failed, but individual segment failures are
    # expected/tracked (status=failed + notes in run_registry.csv) and should not
    # block C2.5/C3 from running for the segments that DID succeed.
    if ($LASTEXITCODE -ne 0) {
        Write-Host "WARNING: run_segment_orchestrator.py reported segment failures (exit code $LASTEXITCODE)." -ForegroundColor Yellow
        Write-Host "  See run_registry.csv (status=failed rows) and each failed segment's bundle.log / bundle_name.log / run.log." -ForegroundColor Yellow
    }

    Write-Host "--- C2.5: rebuild BI results registry ---" -ForegroundColor Cyan
    python tools\build_results_registry.py `
        --manifest-file "$RECORDS\segment_manifest.csv" `
        --registry-file "$RECORDS\run_registry.csv" `
        --output-file "$RECORDS\results_registry.csv"
    Invoke-Checked -StepName "Run C2.5: build_results_registry.py"

    Write-Host "--- C3: re-patch all segment domain_patterns ---" -ForegroundColor Cyan
    python tools\label_synthesis\patch_all_domain_patterns.py `
        --results-root $RESULTS `
        --segments-root $SEGMENTS
    Invoke-Checked -StepName "Run C3: patch_all_domain_patterns.py"

    Write-Host "=== RUN C COMPLETE ===" -ForegroundColor Green
    Write-Host "Refresh Power BI: open Fingerprint_Segmented_Bundles.pbix and hit Refresh" -ForegroundColor Green
    Write-Host "Cross-segment comparison: run compare_cross_segment.py separately" -ForegroundColor Cyan
    Write-Host "Reminder: used/purge signals are active-delivery signals primarily for Project targets; do not label Template or Generic stock content as unused bloat." -ForegroundColor Cyan
    if ($NameKey) {
        Write-Host "Name-projection output: {segment folder}\results\bundle_analysis\name_all\... (join_key_name_identity instead of join_hash; ALL view only -- no used-view/compare/share-profile equivalent yet, see audit_results/audit_8, audit_9, and audit_10)" -ForegroundColor Cyan
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
#                      results/bundle_analysis/name_all/ per segment alongside the
#                      existing all/used folders -- ALL view only (no used-view/compare/
#                      share-profile equivalent for the name projection yet). See
#                      audit_results/audit_8_bundle_pipeline_name_projection.md,
#                      audit_results/audit_9_segment_orchestrator_name_projection.md, and
#                      audit_results/audit_10_bundle_bi_output_location_correction.md (the
#                      name_all/ flat-path correction -- name/all/ in earlier revisions of
#                      this script was never reachable via the Power BI model's pPurgeView
#                      parameter).
