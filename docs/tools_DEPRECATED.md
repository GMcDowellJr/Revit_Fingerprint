# Deprecated / Legacy Tools (tools/)

Status date: 2026-07-16  
Scope: `tools/` only (external entrypoints / CLIs).  
Default assumption: split export surfaces exist (`*.index.json`, `*.details.json`), and legacy is opt-in (`*.legacy.json`).

**2026-07-16 archive cleanup**: `tools/_archive/` was audited file-by-file against the live
codebase. Files with zero remaining references anywhere (superseded duplicates, one-off
diagnostics) were deleted outright rather than kept as dead weight: `compare_manifest.py`,
`compare_view_templates.py` (superseded by `tools/compare_templates_stand-alone/`),
`compute_synthetic_keys.py`, `details_to_csv.py`, `diagnose_phase1_empty.py`,
`emit_element_dominance.py` (a stale duplicate — the live version has always been
`tools/emit_element_dominance.py`, not the `_archive` copy), `example_use_split_export.py`,
`merge_split_exports.py`, `pairwise_drift.py`, `pareto_make_shape_inputs.py`, `score_drift.py`,
`step_template_governance_discovery.py`, `validate_v21_contract.py`, and a personal
hardcoded-path command scratchpad (`split analysis.txt`).

Two files that *looked* archived were not: `join_key_derivation_phase05.py` is still
`import *`-ed live by `tools/join_key_derivation.py`, so it stays in `_archive/`. And
`tools/pareto_joinkey_search.py` had been left behind in `_archive/` during an earlier
`tools/` reorg while `tools/discover_join_policy.py`'s pareto adapter and
`tests/test_pareto_shape_gating.py` still expected it at the `tools/` top level — both call
sites were silently degraded (the adapter always returned
`{"error": "pareto_dependency_missing"}`; the test only ever ran when `pandas` was absent,
which masked the break). It has been moved back to `tools/pareto_joinkey_search.py`, fixing
both call sites. **Known separate issue**: the restored module itself throws
`KeyError: 'max_sigcnt'` under pandas on at least the test's minimal synthetic dataset — a
pre-existing bug in the pareto-search implementation, unrelated to the file's location, not
fixed as part of this pass.

`tools/patterns_analysis/_archive/` is a different situation and was deliberately **not**
touched by this cleanup despite the name: most of it is live, invoked directly by
`tools/run_split_detection_all.py` (`split_detection_file_level`, `build_reference_standards`,
`intradomain_summary`, `emit_intradomain_definition`, `derive_join_keys_by_ids`,
`apply_join_keys_by_ids`, `calibrate_join_key_gates`, `pareto_join_keys_by_ids`,
`split_detection_element_level`) and by `tests/test_fingerprint_export_discovery.py`
(`_archive.io`). The remaining unreferenced files in that directory
(`run_change_type.py`, `run_attribute_stress.py`, `run_attribute_stress_all_joinable.py`,
and the rest of the `run_dimension_types_by_family.py` cluster, plus
`annotate_cluster_labels.py`, `backfill_cluster_label_inputs.py`, `pareto_with_splits.py`)
are intentionally-paused Phase-2 tooling gated on Phase-2 baseline authority (see 
"Two distinct baseline concepts" / "Current operating mode"), not abandoned
code — left in place on purpose.

---

## Deprecation rules used here

A tool is marked **DEPRECATED** if any of these are true:

- It **recursively globs `**/*.json`** (or broadly globs `*.json`) and therefore:
  - unintentionally ingests `*.index.json` and `*.legacy.json`, or
  - double-counts `index+details`, or
  - treats `index` as empty records and poisons analysis.
- It is **superseded** by Phase-1/Phase-2 runners + flat tables.
- It is an **example / one-off probe** and should not be depended on in production workflows.

A tool is marked **KEEP (Docs-only)** if it’s useful as an example but not as an operational entrypoint.

---

## DEPRECATED

### tools/similarity_compare.py

**Superseded by**: `tools/compare_cross_segment.py`

**Deprecation date**: May 2026

**Reason**: Three compounding issues made all historical output from this tool unreliable:

1. **Wrong hash grain** — compared files using `sig_hash` (cosmetically sensitive) rather than `join_hash` (structurally canonical). Cosmetic drift across files caused scores to understate alignment even when governed configuration was consistent.

2. **Multiset weighting was unsound** — weighted domain similarity by record count (`union_mass`), giving high-record-count domains like `object_styles` disproportionate influence over the aggregate score. Governed domains like `dimension_types` and `text_types` could be well-aligned while being swamped by subcategory noise.

3. **`union_mass=1` bug** — when the `sig_hash → join_hash` migration broke the hash lookup, all records fell through to a domain-hash fallback path that hardcoded `union_mass=1`. Every historical similarity score is an artefact of unweighted set Jaccard on domain-level hashes, not the intended multiset Jaccard on record-level hashes. April 2026 baseline scores cannot be compared to any corrected scores.

**Migration path**:

| Use case | Replacement |
|----------|-------------|
| Governance chain comparisons (template→project, container→project) | `compare_cross_segment.py --governance-chain` |
| Sibling comparisons (project vs project, template vs template) | `compare_cross_segment.py --sibling-segments` |
| Within-project file consistency | `compare_cross_segment.py --within-project` |
| Record-level detail (matched, added, removed counts per domain) | Not yet re-implemented; noted as future addition to `compare_cross_segment.py` |

**Historical output**: Similarity CSVs produced before this deprecation should be discarded. Scores are not salvageable.

---

### tools/phase1_semantic_sig_dimension_types.py — REMOVED
This file no longer exists in the repo (already gone before the 2026-07-16 cleanup).
Its replacements, for reference, now live under `tools/patterns_analysis/_archive/`
(the old `tools/phase2_analysis/` package was renamed wholesale into this directory
during the `tools/` reorg — see the note above; despite the `_archive` name most of
this package is still live):
- `python -m tools.patterns_analysis._archive.run_joinhash_label_population ...`
- `python -m tools.patterns_analysis._archive.run_joinhash_parameter_population ...`
- `python -m tools.patterns_analysis._archive.run_candidate_joinkey_simulation ...`
- `python tools/export_to_flat_tables.py ...` (when you need CSV-level analysis)

---

## REMOVED (2026-07-16 archive cleanup)

`tools/details_to_csv.py` and `tools/example_use_split_export.py` were deleted outright
(previously marked CONDITIONAL/KEEP-docs-only below, but had zero remaining references
anywhere in the repo). Use `tools/export_to_flat_tables.py` for standardized CSV surfaces;
see `docs/SPLIT_EXPORT.md` for split-export usage guidance.

---

## Niche probes (keep only if intentionally used)

### tools/probes/probe_arrowheads.py
**Why niche**
- Domain-specific probe; not part of standard Phase-0/1/2 workflow.
- Keep only if you’re still actively probing arrowhead-related identity behavior.

---

## Notes / Warnings

- Any tool that globs `*.json` without preference ordering is considered **unsafe** under split exports unless patched to:
  1) prefer `*.details.json`,
  2) then `*.index.json`,
  3) and ignore `*.legacy.json` unless explicitly requested.

If you want, I can add a short “Deprecation Banner” to the deprecated scripts (single stderr warning + exit code 2 unless `--force`) but that *is* a behavior change, so I did not propose it under current constraints.
