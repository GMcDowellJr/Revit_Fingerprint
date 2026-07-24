# Audit 9 — Segment-Orchestrator Name-Projection Support (PR4)

Date: 2026-07-24
Scope: follow-up to PR3 (`audit_results/audit_8_bundle_pipeline_name_projection.md`). PR3
covered `tools/bundle_analysis/` in isolation; this PR extends `tools/run_segment_orchestrator.py`
so a name-projection `results/bundle_analysis/name/all/` folder can be produced per segment,
alongside the existing `results/bundle_analysis/{all,used}/` config-target output that
already feeds Power BI. Goal (from conversation): let the operator see how domains would
have clustered under `join_key_name_identity` instead of `join_hash`, for the same segment
population, in the same Power BI model.

## What `run_segment_orchestrator.py` actually does per segment (traced before changing it)

1. **Prepare** — writes `export_run_ids.txt` (the segment's file population) + filtered
   `records.csv`/`file_metadata.csv`/`identity_items_by_domain/*.csv` via `_write_segment_records()`.
2. **Patterns** — `run_extract_all.py --stages patterns --filter-export-run-ids
   export_run_ids.txt` re-derives `domain_patterns.csv`/`pattern_presence_file.csv`
   **scoped to just this segment's files**, into `results/analysis/`.
3. **Bundle** — `run_bundle_analysis.py --analysis-dir results/analysis --out-dir
   results/bundle_analysis --purge-view both --no-discover-populations` mines that
   segment-scoped input into `results/bundle_analysis/{all,used}/...` — what Power BI reads.
4. **BI merge** — `merge_bi_outputs()` combines per-domain CSVs under `bundle_analysis/all/`
   into `*_combined.csv`.

## Design decision: no per-segment JSON re-parse for the name leg

`tools/apply_name_key_policy.py` (PR1) computes `join_key_name_identity` per record with no
cross-file dependency — unlike `join_hash`'s "patterns" step, segment-scoping the name
projection does not need a fresh JSON parse per segment. `apply_name_key_policy.py` runs
**once, corpus-wide** (the name-projection analog of Run A), producing one
`name_key_results.csv`; each segment then just filters those rows down to its own file
population before re-clustering. This is the cheaper, equivalent alternative to adding a
`--filter-export-run-ids` flag to `apply_name_key_policy.py` itself — clustering
(`build_clusters()`) only depends on which rows are included, not on when/how the
per-record `join_key_name_identity` value was computed, so pre-filtering rows is
mathematically identical to re-deriving from a filtered export directory.

## The same split-export normalization bug, one layer up

`name_key_results.csv`'s `export_file` column is the raw `*.details.json`/`*.index.json`
basename PR1 saw on disk; a segment's `export_run_ids.txt` (from `segment_membership.csv`)
carries the canonical id (`tools/extractor.py`'s `_iter_export_files()` primary-file
convention — the `*.index.json` name for a split-export pair, per PR3's own review fix,
`audit_results/audit_8` item and the PR #389 review thread). Filtering
`name_key_results.csv` rows by raw `export_run_ids.txt` membership without normalizing
first would reproduce exactly the bug the PR #389 review caught, one layer earlier in the
pipeline (at segment-membership filtering instead of at bundle-input staging).
`_filter_name_key_csv_to_segment()` reuses `name_projection_adapter.normalize_export_run_id()`
(promoted from a module-private helper to a shared one) for the membership check — the
row's own `export_file` value is left unmodified in the filtered output, since
`stage_name_projection_analysis_dir()` (PR3) normalizes it again downstream when building
bundle-pipeline input; only the *comparison* needed normalizing here.

## What was added, all gated behind `--comparison-target {config,name,both}` (default `config`)

- `--comparison-target config` (the default, and the value every existing caller of this
  script already gets since the flag didn't exist before): **zero new file writes,
  subprocess calls, or log lines** — every new code path in `_run_one_segment()` is gated
  on `comparison_target in ("name", "both")`. No config-target output changes.
- Step 2b (name-patterns): filter `--name-key-results-csv` to this segment
  (`_filter_name_key_csv_to_segment()`) → `generate_name_key_patterns.py
  --comparison-target name` → `results/name_key/patterns/name/`.
- Step 3b (name-bundle): `run_bundle_analysis.py --comparison-target name
  --name-key-patterns-dir results/name_key/patterns/name --no-discover-populations` (no
  `--purge-view` needed — PR3's target-aware default already resolves to `all`, the only
  view name-target supports) → `results/bundle_analysis/name/all/`.
- BI merge (name leg): mirrors the existing merge, reading
  `results/bundle_analysis/name/all/` and `_active_domains_from_name_patterns()` (the
  name-target analog of `_active_domains_from_presence_csv()`, since name-target patterns
  have no `pattern_presence_file.csv`).
- Both the live-run path (`_run_one_segment()`) and the `--dry-run` preview block
  (duplicated command construction in `run_orchestrator()`) were updated together —
  verified by inspecting dry-run output directly (`tests/test_run_segment_orchestrator_name_projection.py`),
  not just by code review, since the two blocks previously only had to stay in sync with
  each other for the config leg.

## What this PR does not attempt

- No `--roles`-equivalent segment-level filtering beyond what the existing membership
  file already provides — unchanged from the config leg's behavior.
- No cross-segment comparison equivalent (`compare_cross_segment.py`) for the name
  projection — out of scope, consistent with PR3's own Do-NOT list.
- No hard-fail diagnostic (`_build_patterns_missing_notes()`'s equivalent) for a segment
  whose name-target pattern set comes back empty (e.g. a segment whose files/domains don't
  intersect the 25 eligible domains) — an empty `domain_patterns.csv` from
  `generate_name_key_patterns.py` produces an empty (not missing) bundle output for that
  segment, which is treated as a legitimate outcome, not a step failure.

## Verification

- `tests/test_run_segment_orchestrator_name_projection.py`: `_filter_name_key_csv_to_segment()`
  (including the split-export normalization case), `_active_domains_from_name_patterns()`,
  and CLI/dry-run behavior (`--name-key-results-csv` required for name/both;
  `--comparison-target config` dry-run output has no name-leg lines; `--comparison-target
  name` dry-run output includes the expected step 2b/3b commands).
- Hand-run end-to-end: `run_bundle_analysis.py --comparison-target name
  --no-discover-populations` (the exact invocation step 3b uses) against a real staged
  name-key fixture produced a real bundle, and `merge_bi_outputs()` against that output
  produced all 10 `*_combined.csv` files with the expected row counts.
- Full suite: 1044 passed, 6 skipped (9 new tests over PR3's post-merge baseline of 1035).

## PR #390 review round — four fixes

1. **Skip-check didn't honor the requested comparison_target.** A segment already marked
   `status=complete` from a prior config-only run was filtered out of `plan_to_run` before
   ever reaching `_run_one_segment()`, so `--comparison-target name` against an
   already-complete corpus silently produced nothing unless the operator also passed
   `--force` (which redoes the config leg for every segment too, not just the ones missing
   the name leg). Fixed via `_segment_has_name_leg_output()` (checks for
   `results/bundle_analysis/name/bundle_provenance.csv`, which
   `emit_name_target_provenance()` always writes on a successful name-leg run, even an
   empty one) — a segment is only treated as skippable when the *requested* target(s) are
   both already satisfied, in both the live-run and `--dry-run` skip-check blocks (which
   must stay in sync with each other, same as the rest of this PR).
2. **Stale per-domain output could leak into the merge for an empty segment.**
   `_active_domains_from_name_patterns()` returned `None` both when `domain_patterns.csv`
   was missing (genuinely no info) and when it was present but had zero domain rows (a
   legitimate outcome — see "What this PR does not attempt" above). `merge_bi_outputs()`
   treats `None` as "unfiltered," so the empty-but-legitimate case would resurrect stale
   per-domain folders left over from a prior, larger population for that segment. Fixed:
   the function now returns `frozenset()` (not `None`) when the file is present but empty,
   reserving `None` for "file missing." `merge_bi_outputs(active_domains=frozenset())`
   correctly matches nothing, so no combined-file entry is produced for that filename.
3. **`-Run C -NameKey` accepted a stale `name_key_results.csv`.** `Test-Path` only checks
   existence, not freshness — if `-Run A -NameKey` was forgotten after a `-Run A` that
   added new exports, Run C would silently produce name-projection output missing those
   files while still reporting success. Fixed with an mtime freshness guard comparing
   `$NAME_KEY_CSV` against `$RECORDS\records.csv` (the join_hash leg's own always-rewritten
   Run A output, used as a proxy for "when was Run A last actually run") — hard-fails with
   a clear re-run instruction if `name_key_results.csv` is older.
4. **`normalize_export_run_id()` silently dropped details-only exports.** For a
   details-only export (no matching `*.index.json`), `tools/extractor.py`'s
   `_iter_export_files()` keeps the `*.details.json` name itself as the canonical
   `export_run_id` — there's no index file to rewrite to. `_filter_name_key_csv_to_segment()`
   was blindly normalizing every `*.details.json` row regardless, so its normalized id
   never matched that segment's real membership, silently dropping every row for that
   export. Fixed: the filter now tries the normalized id against `allowed_ids` first, and
   falls back to the raw (un-normalized) `export_file` value if that doesn't match — safe
   specifically because `allowed_ids` is the segment's own real membership list, not a
   heuristic guess. The same underlying ambiguity exists in PR3's
   `stage_name_projection_analysis_dir()` (merged, `--roles` filtering path) but that
   function has no `allowed_ids`-equivalent to fall back against at that layer — flagged
   as a known follow-up, not fixed here, to avoid a larger, differently-shaped change to
   already-merged code outside this PR's diff surface.

All four covered by new/updated tests in `tests/test_run_segment_orchestrator_name_projection.py`
(`TestCompleteSegmentSkipHonorsNameTarget`, `TestActiveDomainsFromNamePatterns::test_returns_empty_frozenset_when_present_but_empty`,
`TestMergeBiOutputsExcludesStaleDomainsForEmptySegment`, `TestFilterNameKeyCsvToSegment::test_preserves_details_only_export_with_no_index_sibling`).
Fix 3 (PowerShell) has no automated test (no `pwsh` available in this environment) —
verified by careful manual read-through instead. Full suite after all four fixes: 1053
passed, 6 skipped.

## PR #390 review round 2 — one more fix

5. **A previous run's own `*_combined.csv` was left stale on an empty rerun.** Fix 2
   (above) stops *foreign* stale per-domain folders from being merged in, but
   `merge_bi_outputs()` still returned early (`continue`) without touching
   `{stem}_combined.csv` on disk whenever a filename had zero current candidates --
   whether because `active_domains` was `frozenset()` or because every candidate file was
   headerless. So a segment that had real bundles on one run and genuinely produces none
   on a rerun (e.g. its files no longer intersect any eligible domain) kept showing the
   *previous* run's `bundles_combined.csv` content to Power BI as if it were current.
   Fixed: both "no candidates" exit points now delete any pre-existing `{stem}_combined.csv`
   before continuing, so "found nothing this run" always means "no stale file left behind"
   -- for the name leg and (since `merge_bi_outputs()` is shared) the config leg alike.
   This only changes behavior on the already-buggy stale-data path; the "candidates found,
   write fresh combined file" path is untouched. Covered by two new tests in
   `TestMergeBiOutputsExcludesStaleDomainsForEmptySegment` (one per exit point). Full suite
   after this fix: 1055 passed, 6 skipped.
