# Corpus Update Runbook
# Run when adding new JSON fingerprint files to the exports folder.
# CWD for all commands: C:\Users\gmcdowell\Documents\Revit_Fingerprint

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

# API key (set once per session)
# $env:OPENROUTER_API_KEY = "sk-or-v1-..."

Set-Location $REPO

$ErrorActionPreference = "Stop"

# ============================================================
# STAGE 1 — Flatten, apply, placeholders
# ============================================================
python tools/run_extract_all.py $EXPORTS `
    --out-root $EXPORTS `
    --stages sig_hash,flatten,apply,placeholders `
    --sig-hash-policy $SIG_POL `
    --join-policy     $JOIN_POL

# ============================================================
# STAGE 2 — Authority + patterns (corpus level)
# ============================================================
python tools/run_extract_all.py $EXPORTS `
    --out-root $EXPORTS `
    --stages authority,patterns

# ============================================================
# STAGE 3 — Rebuild identity_items lookup
# Required after any new files are added — new patterns may
# have appeared; lookup must cover all join_hashes in
# domain_patterns.csv for synopsis and LLM prompts to work.
# ============================================================
python tools\label_synthesis\build_identity_items_lookup.py `
    --records-dir $RECORDS `
    --out-dir     "$RESULTS\label_synthesis"

# ============================================================
# STAGE 4 — LLM synthesis for newly fragmented patterns
# Only runs on join_hashes not already in cache (incremental).
# Add/remove domains as needed. Run --dry-run first if unsure.
# ============================================================
foreach ($dom in @(
    "fill_patterns_drafting",
    "fill_patterns_model",
    "line_patterns",
    "arrowheads",
    "line_styles"
)) {
    Write-Host "`n=== Synthesizing: $dom ===" -ForegroundColor Cyan
    $params = @(
        "--exports-dir",         $EXPORTS,
        "--analysis-dir",        "$RESULTS\label_synthesis",
        "--domain",              $dom,
        "--cache",               $CACHE,
        "--identity-items-lookup", $LOOKUP,
        "--provider",            "openrouter",
        "--filter-mode",         "candidates",
        "--domain-patterns-csv", $DP_CSV,
        "--workers",             "3"
    )
    python -m tools.label_synthesis.synthesize_fragmented_labels @params
}

# ============================================================
# STAGE 5 — Patch corpus domain_patterns.csv with LLM cache
# Fast alternative to re-running full patterns stage.
# ============================================================
python tools\label_synthesis\patch_all_domain_patterns.py `
    --results-root  $RESULTS `
    --segments-root $SEGMENTS

# ============================================================
# STAGE 6 — Rebuild segment manifest
# ============================================================
python tools\build_segment_manifest.py `
    --metadata-file "$RECORDS\file_metadata.csv" `
    --out-dir       $RECORDS `
    --enable-parent-bundle-runs

# ============================================================
# STAGE 7 — Segment orchestrator (patterns + bundle per segment)
# NOTE: --force re-runs all segments including bundle analysis.
# This overwrites segment domain_patterns.csv files — run
# patch_all_domain_patterns again after (Stage 8).
# ============================================================
python tools/run_segment_orchestrator.py `
    --manifest-file "$RECORDS\segment_manifest.csv" `
    --registry-file "$RECORDS\run_registry.csv" `
    --records-dir   $RECORDS `
    --exports-dir   $EXPORTS `
    --segments-root $SEGMENTS `
    --repo-root     $REPO `
    --force `
    --join-policy   $JOIN_POL

# ============================================================
# STAGE 8 — Re-patch all segment domain_patterns.csv files
# Segment orchestrator overwrites them; LLM cache must be
# re-applied on top of the fresh emit.
# ============================================================
python tools\label_synthesis\patch_all_domain_patterns.py `
    --results-root  $RESULTS `
    --segments-root $SEGMENTS

# ============================================================
# STAGE 9 — Similarity
# ============================================================
python tools/similarity_compare.py `
    --records    "$RECORDS\records.csv" `
    --metadata   "$RECORDS\file_metadata.csv" `
    --output-dir "$RESULTS\similarity"

# ============================================================
# STAGE 10 — Refresh Power BI
# ============================================================
# Open Fingerprint_Segmented_Bundles.pbix and hit Refresh.

# ============================================================
# NOTES
# ============================================================
# Incremental behaviour:
#   - Stage 3 (lookup): always rebuild — fast, ~2 min
#   - Stage 4 (synthesis): skips already-cached join_hashes
#     automatically. Only new/uncached patterns incur API calls.
#   - Stage 5/8 (patch): skips authoritative sources (synopsis,
#     modal, curator). Only upgrades fallback and llm_unreviewed.
#   - Stage 7 (orchestrator): --force re-runs all segments.
#     If only a subset of segments changed, pass --segment <id>
#     to run one segment at a time and save time.
#
# After major synthesis additions (new domain prompt modules,
# --force-refresh pass), run Stage 2 (patterns) instead of
# Stage 5 (patch) for corpus — the full emit picks up synopsis
# and modal correctly; the patch only applies LLM cache.
#
# Known deferred items (not yet in pipeline):
#   - build_semantic_groups.py: column name fix needed before
#     first run (item_key/item_value vs k/v/q mismatch)
#   - is_cad_import: lp.is_import not flowing into
#     domain_patterns.csv; IMPORT- line patterns show as
#     ungoverned fallbacks rather than filtered noise
#   - Bundle-filtered synthesis pass for differentiating
#     patterns not covered by candidates filter:
#     use --filter-mode bundles --bundle-dir per segment