# Audit 8 — Bundle Pipeline Single-Source Assumptions (PR3 Step 0)

Date: 2026-07-24
Scope: Findings-before-code for PR3 (Name-Projection Bundle Support), per the PR3 brief's
"Deliverable" section — the full read of `tools/bundle_analysis/` required before any code
change, reported here first because it determines the actual diff surface for this PR.

## Files read in full

`common.py`, `utils.py`, `run_bundle_analysis.py`, `step0_discover_populations.py`,
`step1_membership_matrix.py`, `step2_find_bundles.py`, `step2b_bundle_share_profile.py`,
`step3_build_dag.py`, `step4_difference_sets.py`, `step5_classify_patterns.py`,
`step6_classify_files.py`, `step7_overlap_report.py`, `step_compare.py`,
`reference_bundle.py`, `placeholder_exclusions.py`, `placeholder_exclusions_legacy.py`.
Cross-referenced against PR2's `tools/generate_name_key_patterns.py` and
`core/name_key_coverage.py`, and against the production schema writer in
`tools/extractor.py` (`domain_patterns.csv` / `pattern_presence_file.csv`).

## Every point that assumes a single join_hash-based pattern source

1. **`resolve_analysis_run_id()` (`common.py`) requires an `analysis_run_id` column with
   exactly one distinct non-empty value.** Called from `step0_discover_populations.py`,
   `step1_membership_matrix.py`, `step2b_bundle_share_profile.py`, `step_compare.py`, and
   the top-level `run_bundle_analysis()`. PR2's `Results_v21/name_key/patterns/name/
   domain_patterns.csv` / `pattern_membership.csv` have **no `analysis_run_id` column at
   all** — a direct read would raise `ValueError: Expected exactly one analysis_run_id in
   input; found []` immediately.

2. **Hardcoded input filenames.** `step0`/`step1`/`step2b`/`step_compare` all read
   `analysis_dir / "pattern_presence_file.csv"` and/or `analysis_dir / "domain_patterns.csv"`
   by literal name. PR2 emits `domain_patterns.csv` (name matches) but the per-record/file
   table is named `pattern_membership.csv`, not `pattern_presence_file.csv`.

3. **Column-name mismatch on the presence/membership table.** Production
   `pattern_presence_file.csv`: `schema_version, analysis_run_id, export_run_id, domain,
   pattern_id, pattern_share_pct, is_dominant_pattern, deviation_score,
   corpus_classification` — one row per (file, pattern). PR2's `pattern_membership.csv`:
   `domain, coverage_class, export_file, record_id, pattern_id` — one row per **record**
   (not deduplicated to file-level presence), and the file-identity column is named
   `export_file`, not `export_run_id`. Bundle code builds file-pattern sets via a `set()` per
   file (`file_patterns_by_scope[scope][fid].add(pid)`), so record-level duplication
   collapses harmlessly once the column is renamed — but the rename itself has no built-in
   adapter today.

4. **`domain_patterns.csv` column set differs.** Production (`tools/extractor.py`):
   `schema_version, analysis_run_id, domain, pattern_id, pattern_label, source_cluster_id,
   pattern_size_records, pattern_size_files, pattern_rank, is_candidate_standard, notes,
   pattern_label_human, pattern_label_source, pattern_label_fallback, is_cad_import,
   semantic_group`. PR2's name-target: `domain, coverage_class, pattern_id, pattern_label,
   join_key_schema, join_hash, source_cluster_id, pattern_rank, pattern_size_records,
   pattern_size_files`. No `analysis_run_id`, no `is_cad_import`, no `pattern_label_human`.
   `join_key_schema` *is* present in both (bundle's `derive_scope_key()` needs it for
   `SHAPE_GATED_DOMAINS`), which is the one column that lines up by luck rather than design.

5. **CAD-import exclusion silently unavailable for name-target.** `step0`/`step1` build a
   `cad_patterns` exclusion set from `domain_pattern_rows["is_cad_import"] == "true"`. PR2's
   schema carries no such column, so under `comparison_target=name` no pattern is ever
   treated as a CAD import — this is a real behavioral difference from `config`, not just a
   plumbing gap, and needs to be documented rather than quietly absorbed by a default-false
   read.

6. **Row-key/shape-gated scope derivation (`derive_scope_key()`) turns out to be moot for
   name-target, but for a reason worth recording.** `ROW_KEY_DOMAINS =
   {object_styles_model, object_styles_annotation, view_category_overrides}` are all in
   PR1/PR2's `EXCLUDED_DOMAINS` registry (`core/name_key_coverage.py`, reason
   `no_name_like_key`), so they never appear in name-target patterns — the row-key scope
   branch never fires there. `SHAPE_GATED_DOMAINS = {"dimension_types", "arrowheads"}` uses
   the pre-D-015 domain names, which no longer match any live domain string (the current
   names are the `dimension_types_linear` etc. partitions) — this is a **pre-existing
   quirk in `config`-target behavior too**, not something introduced by this PR, and fixing
   it is out of scope (would change `config`-target scope-key output, violating the
   byte-identical requirement).

7. **`--purge-view used`/`both` (latent-purgeable filtering) is config-artifact-specific.**
   `step1`'s "used" filter maps `pattern_id -> sig_hash` via
   `domain_pattern_rows["source_cluster_id"].split("|")[-1]` and cross-references
   `records/latent_purgeable.csv` (itself derived from `analysis_dir/records/records.csv`,
   a `sig_hash`-keyed production artifact). Name-target's `source_cluster_id` follows the
   same `domain|schema|hash` shape but the trailing hash is `join_key_name_identity`'s
   `join_hash`, not a `sig_hash` — cross-referencing it against `latent_purgeable.csv` would
   silently compare the wrong identity space. This is exactly the case the PR3 brief's item
   4 anticipates: USED view must be explicitly blocked for `comparison_target=name`, not
   guessed or silently downgraded to ALL.

8. **Placeholder-exclusion / population discovery depends on `records.csv`'s
   `is_purgeable` column.** `run_bundle_analysis()`'s population-aware path (the default,
   `--no-discover-populations` not passed) unconditionally looks for
   `analysis_dir/records/records.csv` (or `analysis_dir.parent/records/records.csv`) and, if
   found with an `is_purgeable` column, computes `domain_placeholder_exclusions.csv` via
   `placeholder_exclusions.py` → `placeholder_exclusions_legacy.py`
   (`wall_types`/`ceiling_types`/`floor_types`/`roof_types` only — all four are Native/
   eligible domains for name-target, so this *would* matter if wired). Under
   `comparison_target=name`, `analysis_dir` points at `Results_v21/name_key/patterns/name/`,
   which has no `records/` subtree — there is no defined name-projection equivalent of
   `is_purgeable` today. This PR does not attempt to invent one (out of scope, and
   `records.csv`/`is_purgeable` is exactly the "purgeability logic" the brief says not to
   refactor); population discovery for `comparison_target=name` runs with placeholder
   exclusion explicitly disabled (`placeholder_exclusions_path=None`), not silently
   defaulted.

9. **`--compute-share-profile` (step2b) needs `pattern_share_pct`/`is_dominant_pattern`
   from `pattern_presence_file.csv`.** Neither field has a name-target equivalent (see #3).
   Blocked explicitly for `comparison_target=name`, same rationale as USED view.

10. **`--compare` (step_compare.py / reference_bundle.py) reads
    `pattern_presence_file.csv` directly and compares against a config-projection
    `reference_bundle.json` baseline.** No defined name-projection baseline concept exists.
    Blocked explicitly for `comparison_target=name` for the same reason as #7/#9 — this is
    additionally the kind of "resolve the live-corpus verification gap" question the PR3
    brief says is explicitly out of scope for this PR.

11. **`--roles` / `file_metadata.csv` governance-role filtering is file-identity-based, not
    pattern-source-based**, so it composes cleanly with either target once `export_file` is
    normalized to `export_run_id` (see #3) — no change needed to the role-filter logic
    itself.

12. **Output-directory collision risk.** `run_bundle_analysis()` writes directly under the
    caller-supplied `--out-dir` (`all/`, `used/`, `<domain>/...`) with no namespacing by
    input source. Nothing today stops a `name`-target run from being pointed at the same
    `--out-dir` as a `config`-target run and silently interleaving output. The acceptance
    criteria require name-target output to live under its own namespaced path "never mixed
    into or overwriting config-target output directories" — this has to be enforced by the
    tool itself (mirroring PR2's own `patterns/config` / `patterns/name` split), not left to
    caller discipline.

## Design conclusion driving the diff surface

Given #1–#5, the pipeline's own step0/step1/step2b code cannot read PR2's output directly
without either (a) branching comparison-target-aware logic into every step file, or (b)
normalizing PR2's output into the exact shape step0/step1/step2b already expect, once, at
the boundary. (b) is the smaller, safer diff: `step0`–`step7` stay completely unmodified
(zero risk to the `config`-target byte-identical requirement), and a new adapter module
synthesizes a staging `analysis_dir` (constant `analysis_run_id`, `is_cad_import` defaulted
false, `pattern_label_human` populated from `pattern_label`, `export_file` renamed to
`export_run_id`, presence-only columns filled with neutral placeholders) from PR2's `name/`
output before invoking the same `discover_populations` / `build_membership_matrix` /
`find_bundles_for_domain` functions the `config` path already uses. Item #6's CAD-import gap
is carried forward as an explicit provenance-note caveat rather than fixed, since fixing it
would require changing PR2's shipped schema, which this PR treats as a fixed input contract
per the brief's "Inherited from PR2 — do not re-derive."
