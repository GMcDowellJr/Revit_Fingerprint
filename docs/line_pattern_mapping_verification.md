# Line Pattern Mapping -- Manual Verification Procedure

This is the documented Revit-side verification procedure for
`mapping/create_line_pattern_mappings.py` (see `docs/line_pattern_mapping.md`
for the utility's design). It demonstrates the same round trip the utility's
own per-pattern post-creation check performs automatically, so it can be run
once by hand to build confidence in the pipeline end to end, independent of
the automated check.

The automated check (inside `mapping/line_pattern_revit_apply.py::create_and_verify_line_pattern`)
already re-fingerprints and compares every single pattern it creates or
reuses, inside the same transaction, before ever committing. This procedure
is for validating that automation itself, not a substitute for it.

## Prerequisites

- A source RVT with at least one non-trivial, named `LinePatternElement`
  (i.e. not `Solid`) whose segment definition you can read directly in
  Revit's Line Patterns UI (Manage tab -> Additional Settings -> Line
  Patterns) for a sanity cross-check.
- A clean/controlled target RVT (empty or near-empty project) to create the
  mapping element in. Do not run this against a production/authored model --
  this utility writes to the currently open document.
- A checked-out copy of this repository accessible from the Dynamo/Revit
  host running the extraction and mapping scripts.

## Steps

1. **Fingerprint the source model.** Open the source RVT and run the normal
   extraction path (`runner/run_dynamo.py`, per the project's existing
   Dynamo workflow) to produce a `*.details.json` / `*.index.json` export.
   Note the `line_patterns` record for the pattern you intend to verify --
   record its `sig_hash` is *not* what you'll compare later; what matters is
   the pattern's `join_key.join_hash` once policy-applied join keys exist
   for it (see step 2).

2. **Run the analysis pipeline through `apply` and `patterns`.**
   `tools/run_extract_all.py --stages flatten sig_hash discover apply patterns
   --domains-in-scope line_patterns ...` (adjust domain scope / other flags
   to your run configuration) so that `phase0_records.csv` carries a
   policy-applied `line_patterns.join_key.v3` `join_hash` for this pattern,
   and `domain_patterns.csv` clusters it into a pattern/bundle.

3. **Run `tools/bundle_analysis/` and then
   `tools/export_bundle_pattern_detail.py`** for the segment containing the
   source model, e.g.:
   ```
   python tools/export_bundle_pattern_detail.py \
       --output-folder <segment_output_folder> \
       --segments-root <segments_root> \
       --records-dir <records_dir> \
       --out-dir <export_out_dir> \
       --domain line_patterns
   ```
   Confirm the pattern's `join_hash` appears in the emitted
   `<export_out_dir>/bundle_pattern_inventory.csv`, and that
   `pattern_settings.csv` has a full, `q="ok"` set of
   `line_pattern.segment_count` / `seg[NNN].kind` / `seg[NNN].length` /
   `segments_def_hash` rows for it (if it doesn't, the bundle/support
   filtering upstream excluded it from scope -- adjust `--top-bundles` /
   `--top-bundles-auto` or pick a different pattern for this dry run).

4. **Open the clean/controlled target RVT** and, in a Dynamo Python Script
   node (or an equivalent CPython3 host), run
   `mapping/create_line_pattern_mappings.py` with:
   - `IN[0]` = `<export_out_dir>` from step 3
   - `IN[1]` = a report path of your choosing

   Confirm the pattern's row in the report has `status="ok"` (or
   `"degraded"` with an expected, non-blocking reason such as
   `segments_def_hash_evidence_unavailable`) and `action` in
   `{"created", "existing"}`, with `verified_join_hash` equal to
   `requested_join_hash`. Any other `status`/`action` combination means the
   utility itself already detected and reported the failure -- do not
   proceed to step 5 with a `blocked` row.

5. **Re-fingerprint the target RVT** (same `runner/run_dynamo.py` extraction
   path, `line_patterns` domain) and confirm the newly created
   `MAP__<observed_name>` element's `join_key.join_hash` in the new export
   equals the `join_hash` from step 2/3. This is the same identity the
   in-transaction check in step 4 already verified -- this step confirms it
   survives a full commit + a completely independent extraction pass, not
   just the read-back inside the still-open transaction.

## What "pass" means

Steps 4 and 5 both converging on the same `join_hash` value as the original
source pattern (step 2) confirms the full chain: source pattern ->
fingerprint -> bundle-pattern-detail evidence -> mapping utility
reconstruction -> Revit API creation -> re-fingerprint, all agree on the same
governance identity. A mismatch anywhere in that chain is a bug in this
utility (or, if steps 1-3 already disagree with each other, upstream of it)
and should block use of the utility until resolved -- do not manually
"fix up" a mismatched element's name or segments to force agreement.
