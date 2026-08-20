# Audit 10 — Name-Target Bundle Output Location Correction (PR3 follow-up)

Date: 2026-07-28

Scope: The PR3 brief (Name-Projection Bundle Support) was previously implemented and
merged (PR #389/#390, `--comparison-target {config,name,both}` wired through
`tools/bundle_analysis/` and `tools/run_segment_orchestrator.py`). A corrected brief
narrowed the "BI output compatibility" requirement to a **specific, confirmed** Power BI
convention: `Fingerprint_Segmented_Bundles.vpax` reads bundle output from
`<segment>\results\bundle_analysis\<pPurgeView>\*_combined.csv`, where `pPurgeView` is a
free-text Power Query parameter spliced directly into the folder path as a single path
segment. This audit re-checked the already-merged implementation against that specific,
narrower requirement before making any further changes.

## Gap found

The already-merged implementation wrote the name leg's ALL-view output to
`out_dir/name/all/` — **two** path segments (`name_out_dir = out_dir / "name"`, then
`run_bundle_analysis()`'s own `_view_out_dir()` mechanism appends `/all` for the ALL view,
identically to how the `config` leg's own `all/`/`used/` folders are built). Config-target
output is a single segment (`out_dir/all/`, `out_dir/used/`) because `pPurgeView` splices
in directly at that level; a report author setting `pPurgeView` to any single string could
never reach `out_dir/name/all/` — it does not match the one-path-segment convention the
`.vpax`'s shared expression (`pPurgeView = "all" meta [...]`) actually requires.

`bundle_provenance.csv` / `domain_coverage.csv` / `README.md` (written by
`emit_name_target_provenance()`) landed one level up, at `out_dir/name/` — a *third*,
inconsistent location, sibling to (not inside) the `all/` per-domain+combined-file tree.

Separately: none of the ten combined/per-domain files carried `comparison_target` /
`coverage_class` / `provenance_note` as row-level columns. The existing implementation's
provenance mechanism was a **sibling file** (`bundle_provenance.csv`), not columns on the
same combined CSVs the Power BI model's `Table.TransformColumnTypes` steps actually read
by name — invisible to a report pointed at `pPurgeView=name_all`, even once the location
itself was corrected.

## Fix

- `run_bundle_analysis_for_target()` (`tools/bundle_analysis/run_bundle_analysis.py`) now
  relocates the name leg's completed `out_dir/name/all/` tree — plus the three
  provenance-adjacent files — to a flat `out_dir/name_all/` as its last step. Implemented
  as a directory move (self-clearing any stale `name_all/` from a previous run first), not
  a parallel write path, so `step0`-`step7` and `emit_name_target_provenance()` remain
  completely unmodified — this is a boundary-only relocation, matching the same "normalize
  once at the boundary" design `name_projection_adapter.py` already established for PR3's
  input side (see audit_8). One useful side effect: because the move empties the source
  directory rather than leaving it in place, a stale per-domain folder from a prior run no
  longer survives into the next run's `name_all/` output even without an external
  pre-clean step (previously guarded only by `run_segment_orchestrator.py`'s own step-3b
  pre-clean of `bundle_analysis/name/`, which still runs unchanged and is now
  belt-and-suspenders for this specific concern).
- `tools/run_segment_orchestrator.py`'s name-leg BI merge (`merge_bi_outputs()` call) now
  reads/writes `bundle_analysis/name_all/` directly instead of `bundle_analysis/name/all/`.
  `_segment_has_name_leg_output()`'s marker-file check was updated to match.
- New `annotate_name_target_combined_files()` (`tools/bundle_analysis/
  name_projection_adapter.py`), called once per segment right after `merge_bi_outputs()`
  for the name leg only: appends `comparison_target` / `coverage_class` /
  `provenance_note` to the header and every row of each `*_combined.csv` under
  `name_all/`, strictly additive (existing typed columns keep their name, order, and
  values) so the model's `Table.TransformColumnTypes` steps parse unmodified once pointed
  at the new folder. `coverage_class` is looked up per row from that row's own `domain`
  column (present natively in all ten files) via `core/name_key_coverage.py` — no second
  eligibility list. Idempotent (a file already carrying `comparison_target` is left
  alone).

## What did not change

- `comparison_target=config` output (`out_dir/all/`, `out_dir/used/` and, under `both`,
  `out_dir/config/...`) — untouched by this fix; the relocation/annotation code paths are
  gated entirely inside the name-leg branch. Verified via
  `TestNameAllOutputLocation::test_config_target_output_untouched_by_relocation`.
- The nine merged filenames and their existing typed columns (unchanged schema, from the
  same shared `step1`-`step6` code the `config` leg also uses).
- `bundle_analysis_thresholds.csv` (feeds `Meta_BundleThresholds`) is **not** in
  `run_segment_orchestrator.BI_MERGE_FILES` for either target today — it is not
  pre-combined by this pipeline at all, only written per-domain. It moves along
  unchanged as part of the `name/all` → `name_all` directory relocation (still reachable
  per-domain under `name_all/<domain>/bundle_analysis_thresholds.csv`, exactly mirroring
  how `bundle_analysis/all/<domain>/bundle_analysis_thresholds.csv` already works for
  `config`); this fix does not add provenance columns to it, since it is a scope/threshold
  metadata artifact, not a "bundle" artifact in the item-3 sense the Column-shape
  constraint describes. Flagged explicitly here rather than left as a silent scope
  decision.
