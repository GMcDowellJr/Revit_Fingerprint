# Fill Pattern Mapping -- Manual Verification Procedure

This is the documented Revit-side verification procedure for
`mapping/create_fill_pattern_mappings.py` (see `docs/fill_pattern_mapping.md`
for the utility's design). It demonstrates the same round trip the utility's
own per-pattern post-creation check performs automatically, so it can be run
once by hand to build confidence in the pipeline end to end, independent of
the automated check. Mirrors `docs/line_pattern_mapping_verification.md`'s
procedure (D-038) -- read that first if you haven't.

The automated check (inside
`mapping/fill_pattern_revit_apply.py::create_and_verify_fill_pattern`) already
re-fingerprints and compares every single pattern it creates or reuses,
inside the same transaction, before ever committing. This procedure is for
validating that automation itself, not a substitute for it.

## Prerequisites

- A source RVT with at least one non-trivial, named `FillPatternElement` per
  target you want to verify (`fill_patterns_drafting` needs a Drafting-target
  pattern; `fill_patterns_model` needs a Model-target pattern) -- not a
  `<Solid fill>` pattern (excluded from both domains entirely) -- whose grid
  definition you can read directly in Revit's Fill Patterns UI (Manage tab ->
  Additional Settings -> Fill Patterns) for a sanity cross-check.
- A clean/controlled target RVT (empty or near-empty project) to create the
  mapping element(s) in. Do not run this against a production/authored
  model -- this utility writes to the currently open document.
- A checked-out copy of this repository accessible from the Dynamo/Revit
  host running the extraction and mapping scripts.

## Steps

1. **Fingerprint the source model.** Open the source RVT and run the normal
   extraction path (`runner/run_dynamo.py`) to produce a `*.details.json` /
   `*.index.json` export. Note the `fill_patterns_drafting` and/or
   `fill_patterns_model` record(s) for the pattern(s) you intend to verify --
   `sig_hash` is *not* what you'll compare later; what matters is the
   pattern's `join_key.join_hash` once policy-applied join keys exist for it
   (see step 2).

2. **Run the analysis pipeline through `apply` and `patterns`.**
   `tools/run_extract_all.py --stages flatten sig_hash discover apply patterns
   --domains-in-scope fill_patterns_drafting fill_patterns_model ...` (adjust
   domain scope / other flags to your run configuration) so that
   `phase0_records.csv` carries a policy-applied join key for these patterns,
   and `domain_patterns.csv` clusters them into patterns/bundles.

3. **Run `tools/bundle_analysis/` and then
   `tools/export_bundle_pattern_detail.py`** for the segment containing the
   source model, once per domain (or omit `--domain` to export every domain
   with bundle-analysis output, which will include both partitions plus any
   other in-scope domains):
   ```
   python tools/export_bundle_pattern_detail.py \
       --output-folder <segment_output_folder> \
       --segments-root <segments_root> \
       --records-dir <records_dir> \
       --out-dir <export_out_dir> \
       --domain fill_patterns_drafting
   python tools/export_bundle_pattern_detail.py \
       --output-folder <segment_output_folder> \
       --segments-root <segments_root> \
       --records-dir <records_dir> \
       --out-dir <export_out_dir> \
       --domain fill_patterns_model
   ```
   (Running the exporter twice into the SAME `--out-dir` is safe and expected
   here: each run only appends rows for its own `--domain`, and the mapping
   utility filters by domain internally -- see
   `mapping/fill_pattern_reconstruction.py::group_requested_join_hashes`.)

   Confirm each pattern's `join_hash` appears in the emitted
   `<export_out_dir>/bundle_pattern_inventory.csv`, and that
   `pattern_settings.csv` has a full, `q="ok"` set of `fill_pattern.target` /
   `fill_pattern.grid_count` / `fill_pattern.grid[NNN].*` / `grids_def_hash`
   rows for it (if it doesn't, the bundle/support filtering upstream excluded
   it from scope -- adjust `--top-bundles` / `--top-bundles-auto` or pick a
   different pattern for this dry run).

4. **Open the clean/controlled target RVT** and, in a Dynamo Python Script
   node (or an equivalent CPython3 host), run
   `mapping/create_fill_pattern_mappings.py` with:
   - `IN[0]` = `<export_out_dir>` from step 3
   - `IN[1]` = a report path of your choosing
   - `IN[2]` = the absolute path to your Revit_Fingerprint checkout (required
     when pasting the script directly into the node, since `__file__` isn't
     available in that context -- omit only if
     `REVIT_FINGERPRINT_REPO_ROOT_SELECTED`/`REVIT_FINGERPRINT_REPO_DIR` is
     already set in the environment, or the node loads the script from a
     file on disk rather than pasted text)

   This single run processes BOTH `fill_patterns_drafting` and
   `fill_patterns_model` requests found in `IN[0]` -- confirm each pattern's
   row in the report has `status="ok"` (or `"degraded"` with an expected,
   non-blocking reason such as `grids_def_hash_evidence_unavailable`) and
   `action` in `{"created", "existing"}`, with `verified_join_hash` equal to
   `requested_join_hash`. Any other `status`/`action` combination means the
   utility itself already detected and reported the failure -- do not
   proceed to step 5 with a `blocked` row.

5. **Re-fingerprint the target RVT** (same `runner/run_dynamo.py` extraction
   path, `fill_patterns_drafting`/`fill_patterns_model` domains) and confirm
   each newly created `MAP__<observed_name>` element's `join_key.join_hash`
   in the new export equals the `join_hash` from step 2/3, for the matching
   partition (a Drafting-target mapping element's re-fingerprinted join_hash
   must match against the `fill_patterns_drafting` record, not
   `fill_patterns_model`). This is the same identity the in-transaction check
   in step 4 already verified -- this step confirms it survives a full
   commit + a completely independent extraction pass, not just the read-back
   inside the still-open transaction.

## What "pass" means

Steps 4 and 5 both converging on the same `join_hash` value as the original
source pattern (step 2), for each partition tested, confirms the full chain:
source pattern -> fingerprint -> bundle-pattern-detail evidence -> mapping
utility reconstruction -> Revit API creation -> re-fingerprint, all agree on
the same governance identity. A mismatch anywhere in that chain is a bug in
this utility (or, if steps 1-3 already disagree with each other, upstream of
it) and should block use of the utility until resolved -- do not manually
"fix up" a mismatched element's grids to force agreement.

## Known, out-of-scope presentational difference

A created mapping element's grid lines are always continuous (no per-grid
dash/dot styling), and its `FillPattern.HostOrientation` is always `ToHost`,
regardless of what the source pattern actually used -- neither is captured
as identity by `domains/fill_patterns.py` (see
`docs/fill_pattern_mapping.md`'s "Known evidence gaps" section), so there is
no evidence to reconstruct either from and no `join_hash` impact from
defaulting them. This is expected, not a verification failure.
