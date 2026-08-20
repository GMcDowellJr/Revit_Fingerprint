# Audit 4 — Bundle Analysis, Cross-Segment Pipeline & Structural Debt
Date: 2026-06-17

Note on file locations: there is no dedicated `runbooks/` directory in this
repo — both runbooks (`corpus_update_runbook.ps1`, `label_refresh_runbook.ps1`)
live directly under `tools/`, consistent with prior audits. Domain extractors
live under `domains/` at the repo root, not `extractors/`.

## Summary Table

| Item | Description | Status | Confidence |
|------|-------------|--------|------------|
| D1 | n_pairs threshold removed/raised | IMPLEMENTED (no threshold; all rows emitted) | HIGH |
| D2 | n_unique_patterns_a/b columns | IMPLEMENTED | HIGH |
| D3 | File-grain scores emitted alongside bundle-mode | NOT IMPLEMENTED (by design — single measurement path) | HIGH |
| D4 | Cascade containment data / comparison_mode | PARTIAL | HIGH |
| D5 | score_ambiguity_band / signal_spread | NOT IMPLEMENTED | HIGH |
| D6 | view_templates_renderings_drafting excluded | NOT IMPLEMENTED | HIGH |
| D7 | comparison_mode discriminator column | NOT IMPLEMENTED | HIGH |
| F1 | Used-only bundle analysis two-pass | PARTIAL | HIGH |
| I1 | Flat extraction migration Phase 1 | NOT IMPLEMENTED | HIGH |
| I7 | ProcessPoolExecutor parallelism | IMPLEMENTED | HIGH |
| I9 | Domain family file consolidation | MIXED | HIGH |
| I2-bonus | Results registry (new artifact) | NOT IMPLEMENTED | MEDIUM |

## compare_cross_segment.py — current output columns

**`cross_segment_summary.csv`** (`SUMMARY_FIELDS`, `tools/compare_cross_segment.py:134-164`):
```
comparison_run_id, segment_id_a, segment_id_b, segment_label_a, segment_label_b,
governance_role_a, governance_role_b, client_label_a, client_label_b,
discipline_label_a, discipline_label_b, unit_system, comparison_type, domain,
n_patterns_a, n_patterns_b, n_shared_join_hash,
n_unique_patterns_a, n_unique_patterns_b,
all_containment_a_in_b_mean, all_containment_a_in_b_min,
all_containment_b_in_a_mean, all_containment_b_in_a_min,
all_jaccard_mean, all_jaccard_p10, all_jaccard_p90,
used_jaccard_mean, used_jaccard_p10, used_jaccard_p90,
used_containment_a_in_b_mean, used_containment_a_in_b_min,
used_containment_b_in_a_mean, used_containment_b_in_a_min,
used_n_shared_join_hash,
all_has_bundles_a, all_has_bundles_b,
all_n_shared_bundle_both, all_n_shared_bundle_a_only, all_n_shared_bundle_b_only,
used_has_bundles_a, used_has_bundles_b,
used_n_shared_bundle_both, used_n_shared_bundle_a_only, used_n_shared_bundle_b_only,
n_files_a, n_files_b, n_pairs, data_sufficient,
reference_usage_interpretable, target_usage_interpretable,
recommended_primary_view, comparison_role_semantics, executed_utc
```

**`cross_segment_file_pairs.csv`** (`PAIRS_FIELDS`, `tools/compare_cross_segment.py:166-177`):
```
comparison_run_id, segment_id_a, segment_id_b, domain,
export_run_id_a, export_run_id_b, project_label_a, project_label_b,
n_patterns_a, n_patterns_b, n_shared,
all_jaccard, all_containment_a_in_b, all_containment_b_in_a,
used_n_shared, used_jaccard, used_containment_a_in_b, used_containment_b_in_a,
all_n_shared_bundle_both, all_n_shared_bundle_a_only, all_n_shared_bundle_b_only,
used_n_shared_bundle_both, used_n_shared_bundle_a_only, used_n_shared_bundle_b_only
```

Neither schema contains `comparison_mode`, `score_ambiguity_band`, or `signal_spread`.

## run_bundle_analysis.py — CLI flags summary

`tools/bundle_analysis/run_bundle_analysis.py:888-910`:
- `--analysis-dir` (required, Path)
- `--out-dir` (required, Path)
- `--domain` (str, default `""`)
- `--analysis-run-id` (str, default `""`)
- `--min-support-count` (int, default 3)
- `--min-support-pct` (float, default 0.0)
- `--no-discover-populations` (flag; default discover_populations=True)
- `--min-population-size` (int, default 0)
- `--max-population-overlap` (float, default 0.20)
- `--min-population-jaccard` (float, default 0.30)
- `--discovery-support-pct` (float, default 0.10)
- `--compare` (flag)
- `--compute-share-profile` (flag)
- `--metadata-file` (Path, default None; required when `--roles` is used)
- `--roles` (nargs="+"; choices include Project, Template, Generic, Generic-Host, Container, or alias `template-group`)
- `--purge-view` (choices=`["all", "used", "both"]`, default `"both"`)
- `--latent-purgeable-file` (Path, default None)
- `--workers` (int, default 4 — "Max parallel domains for bundle analysis")

## Domain extractor file structure (for I9)

`domains/` full listing:
```
__init__.py
arrowheads.py
compound_types.py
dimension_types.py
fill_patterns.py
identity.py
line_patterns.py
line_styles.py
loaded_family_types.py
materials.py
object_styles.py
phase_filters.py
phase_graphics.py
phases.py
text_types.py
units.py
view_category_overrides.py
view_category_overrides_annotation.py
view_category_overrides_model.py
view_filter_applications_view_templates.py
view_filter_definitions.py
view_templates.py
```

- **fill_patterns** — fully consolidated: single `fill_patterns.py` with `extract_drafting()` (line 84) and `extract_model()` (line 965), routed via internal constants `_TARGET_DRAFTING_INT`/`_TARGET_MODEL_INT` (lines 40-41).
- **dimension_types** — fully consolidated: single `dimension_types.py` with 7 partition functions (`extract_linear` line 172, `extract_angular` line 460, `extract_radial` line 737, `extract_diameter` line 1036, `extract_spot_elevation` line 1380, `extract_spot_coordinate` line 1754, `extract_spot_slope` line 2100), routed via shape-discriminator frozensets (lines 72-78, e.g. `_LINEAR_HANDLED`, `_ANGULAR_HANDLED`, etc.).
- **view_category_overrides** — partially split (by design): `view_category_overrides.py` (2 KB) is a thin coordinator/legacy-aggregate wrapper (lines 18-43) that imports and calls `view_category_overrides_model.py` and `view_category_overrides_annotation.py` separately, then concatenates their records. Not a single consolidated file — model and annotation partitions remain in distinct files.
- **object_styles** — fully consolidated: single `object_styles.py` with 4 partition functions (`extract_model` line 530, `extract_annotation` line 534, `extract_analytical` line 538, `extract_imported` line 542), routed via internal key-set constants (`_MODEL_SEMANTIC_KEYS`, `_NON_MODEL_SEMANTIC_KEYS`, lines 47-58).

## Detailed Findings

### D1 — n_pairs threshold for cross_segment_file_pairs.csv

**Status: IMPLEMENTED** (interpreted as "removed" — there is no longer any
suppression threshold). `tools/compare_cross_segment.py`:
- Line 1527: comment `# Emit ALL pair rows — no suppression threshold`
- Line 1554: `pair_rows = pair_rows_raw` — no filtering applied
- Line 2456: `atomic_write_csv(out_dir / "cross_segment_file_pairs.csv", PAIRS_FIELDS, pair_detail_rows)` writes every collected row unconditionally.

No `n_pairs`-based gate exists anywhere in the file. This matches the
backlog ask of removing/raising the threshold — it has been fully removed.

### D2 — n_unique_patterns_a / n_unique_patterns_b columns

**Status: IMPLEMENTED.** `tools/compare_cross_segment.py`:
- Lines 134-164 (`SUMMARY_FIELDS`): includes `n_unique_patterns_a`, `n_unique_patterns_b` as distinct entries from `n_patterns_a`/`n_patterns_b`.
- Lines 1698-1699: `"n_unique_patterns_a": str(n_unique_patterns_a)`, `"n_unique_patterns_b": str(n_unique_patterns_b)`.
- Lines 1588-1589: called with `n_unique_patterns_a=n_a, n_unique_patterns_b=n_b`, where `n_a`/`n_b` are the total unique join_hash counts per segment computed at lines 1501-1502 — a genuinely distinct, deduplicated value from the (possibly ambiguous) `n_patterns_a`/`n_patterns_b`.

### D3 — file-grain aggregate scores alongside bundle-mode summary

**Status: NOT IMPLEMENTED**, and the agent's research indicates this is
intentional by design rather than an oversight. The module docstring
(`tools/compare_cross_segment.py:6-13`) states: "Single measurement
path — Comparisons prefer per-file join_hash inventories from
membership_matrix.csv… There is no bundle-mode / file-mode branch. All set
operations (Jaccard, containment) operate on the full join_hash
inventories loaded for the selected view." No `file_grain`, `file_level`,
`pairwise`, or `membership_matrix`-based dual-path logic exists — there is
exactly one Jaccard computation path, not two side-by-side modes.
Consequently there is no `comparison_mode` discriminator needed for this
specific item, since there's only one mode — but this also means the
backlog ask (file-grain scores *alongside* bundle-mode scores, both
present and labeled) has not been delivered.

### D4 — cascade containment data / comparison_mode

**Status: PARTIAL.**

Directed-pair containment data exists: `compare_directed_file()`
(`tools/compare_cross_segment.py:872-904`) computes containment scores for
directed pairs, and a `comparison_type` column (lines 973-982) carries
values such as `generic_to_template`, `template_to_project`,
`container_to_project`, `sibling_projects` — this is the cascade/role
classification mechanism.

However, there is **no separate `comparison_mode` column** anywhere in
`SUMMARY_FIELDS` or `PAIRS_FIELDS` (lines 134-177) distinguishing directed
vs. symmetric pairs as a discriminator independent of `comparison_type`.
The backlog item asks for cascade-gap-sortable data structured for
template→project / template→container / container→project comparisons —
the `comparison_type` values partially cover this taxonomy, but the
explicit `comparison_mode` discriminator column does not exist.

**Verdict: PARTIAL** — the underlying directed-containment computation and
role taxonomy exist via `comparison_type`, but the specific
`comparison_mode` column requested is absent.

### D5 — score_ambiguity_band / signal_spread

**Status: NOT IMPLEMENTED.** No `score_ambiguity_band`, `signal_spread`,
or `score_ambiguity` strings appear anywhere in
`tools/compare_cross_segment.py`. No computation resembling
`(n_shared / min(a,b)) - (n_shared / max(a,b))` was found. This item has
not been started.

### D6 — view_templates_renderings_drafting exclusion

**Status: NOT IMPLEMENTED.** No occurrence of
`"view_templates_renderings_drafting"` or an `EXCLUDED_DOMAINS` constant
(or equivalent exclusion list) exists in
`tools/compare_cross_segment.py`. There is no domain-level filtering logic
in the comparison engine at all — every domain present in the input data
is scored.

### D7 — comparison_mode discriminator column

**Status: NOT IMPLEMENTED.** Confirmed absent from both `SUMMARY_FIELDS`
(lines 134-164) and `PAIRS_FIELDS` (lines 166-177), and from any other
output schema in the file (delta/pooled/governance-state field lists).
Given D3's finding that there is intentionally only one measurement path,
this column currently has no defined purpose in the script as written —
implementing D7 would likely require first implementing D3/D4 properly.

### F1 — used-only bundle analysis two-pass

**Status: PARTIAL.**

`tools/bundle_analysis/run_bundle_analysis.py` supports `--purge-view`
with choices `["all", "used", "both"]` (line 906, default `"both"`). When
set to `"both"` (line 403):
```python
views_to_run = ["all", "used"] if purge_view == "both" else [purge_view]
```
Each view is processed through the full step0-7 bundle-analysis pipeline
and written to a separate subdirectory via `_view_out_dir(out_dir, view)`
(line 416), i.e. `{out_dir}/all/` and `{out_dir}/used/` — matching the
`purge_view/all/` vs `purge_view/used/` directory convention described in
the backlog item.

However, the filtering to `is_purgeable=false` records happens **inside**
`build_membership_matrix()` via a `purgeable_only_set` parameter (line
201), not by pre-filtering `records.csv` at the orchestrator/CLI boundary
into a separate file before invoking analysis. The backlog item's
specific evidence criterion ("a CLI flag or orchestrator parameter exists
to pass a pre-filtered records path, OR the orchestrator explicitly calls
run_bundle_analysis.py twice with different input paths") is not
literally satisfied — there's one invocation with an internal view loop,
not two invocations with distinct input paths.

**Verdict: PARTIAL** — the two-pass *output* structure (separate
`all/`/`used/` directories per segment) is fully realized and functionally
equivalent to the backlog ask, but the *mechanism* (pre-filtering input at
the orchestrator level vs. filtering inside the matrix builder) differs
from how the item was specified, so it doesn't meet the letter of the
IMPLEMENTED criteria.

### I1 — Flat extraction architecture migration

**Status: NOT IMPLEMENTED** (Phase 1 not started in the generic sense).

Found in `tools/migration/`:
- `migrate_materials_identity_items.py` — domain-specific (materials only); confirmed in Audit 3 to inject `material.graphics_sig_hash_v2` into existing exports. This is not a generic flat-items migration.
- `reformat_to_flat_items.py` — exists (~6.2 KB), a smaller utility; its scope was not fully read in this audit pass but its small size suggests a narrow, not corpus-wide, migration tool.

No references to "flat_extraction", "items_migration", or any phase-1
migration marker were found via repo-wide search of `tools/`. No TODO or
comment in `tools/export_to_flat_tables.py` references a planned flat-item
migration.

**Verdict:** narrowly-scoped migration utilities exist (materials-specific,
and a smaller `reformat_to_flat_items.py`), but no general-purpose Phase 1
flat-extraction migration has landed, and the flatten stage itself shows
no sign of being updated to consume a new flat-items format.

### I7 — ProcessPoolExecutor

**Status: IMPLEMENTED.** `tools/compare_cross_segment.py`:
- Line 91: `from concurrent.futures import ProcessPoolExecutor, as_completed`
- Lines 2278+ (main loop):
```python
with ProcessPoolExecutor(max_workers=args.workers) as executor:
    future_to_item = {
        executor.submit(
            _run_pair_domain,
            seg_a, seg_b, ctype, dom,
            manifest, registry, file_metadata,
            segments_root, args.min_patterns,
            executed_utc, args.no_delta,
        ): (seg_a, seg_b, ctype, dom)
        for seg_a, seg_b, ctype, dom in work_items
    }
    for future in as_completed(future_to_item):
        ...
```
Imported and genuinely used to parallelize (segment-pair × domain) work items, not a dead import.

### I9 — Domain family consolidation

**Status: MIXED**, as detailed in the file-structure section above:
- `fill_patterns`, `dimension_types`, `object_styles` — fully consolidated into single files with internal partition-routing logic (verified via dispatch constants/frozensets, not just naming coincidence).
- `view_category_overrides` — intentionally kept split into `_model`/`_annotation` partition files behind a thin coordinator, consistent with the description of this domain's architecture. This is the one domain family that has *not* been collapsed into a single file, by design.

### I2-bonus — Run manifest / results registry

**Status: NOT IMPLEMENTED** (new artifact does not exist).

Existing (already in place, confirmed):
- `segment_manifest.csv` — produced by `tools/build_segment_manifest.py`, with `MANIFEST_FIELDNAMES` = `["segment_id","parent_segment_id","segment_level","unit_system","governance_role","client_label","discipline_label","extra_dimensions","ancestor_segment_ids","run_type","file_count","export_run_ids","has_seed_file","seed_export_run_ids","population_hash","notes","segment_purpose","segment_label"]` (`tools/build_segment_manifest.py:11`).
- `run_registry.csv` — produced/updated by the same script family, with `REGISTRY_FIELDNAMES` = `["segment_id","parent_segment_id","run_type","population_hash","output_folder","status","last_run_utc","notes","segment_purpose","segment_label"]` (`tools/build_segment_manifest.py:12`). Loaded and updated in-place by `run_segment_orchestrator.py` (line 967).

Not found anywhere in the repo:
- `results_registry.csv`
- `run_manifest.json`
- `build_run_manifest.py` / `build_results_registry.py`

The orchestrator does write a `run_summary.txt` (`tools/run_segment_orchestrator.py:1212`), but this is a human-readable text summary, not a queryable CSV/JSON registry suitable for wiring multiple run outputs into BI — so it does not substitute for the I2 deliverable.

## Files Not Found

- A dedicated `runbooks/` directory — does not exist; both runbooks live at `tools/` root.
- `results_registry.csv` — not found anywhere in the repo.
- `run_manifest.json` — not found anywhere in the repo.
- `build_run_manifest.py` — not found.
- `build_results_registry.py` — not found.
