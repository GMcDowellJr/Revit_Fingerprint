# Reference Comparison Tool (`tools/compare_reference.py`)

A standalone, deliberately-invoked workflow for comparing one reference
fingerprint export against a single target export, or an entire
already-materialized segment, producing a stable package of consumable
outputs.

This tool implements **no comparison mathematics of its own**. It is a thin
orchestration/output layer, called **in-process** (no subprocess), over the
existing, authoritative implementation:

- `tools/bundle_analysis/step_compare.py::run_compare_for_domain` computes
  the symmetric shared/reference_only/target_only comparison and its
  ok/degraded/blocked reliability semantics
  (`tools/bundle_analysis/comparison_status.py`, documented in full in
  `tools/bundle_analysis/README.md`).

## What this tool does NOT do

Unlike an earlier version of this tool, `compare_reference.py` does **not**:

- parse or open any fingerprint export JSON file
  (`*.details.json`/`*.index.json`/`*__fingerprint.json`);
- stage export files anywhere;
- invoke `tools/run_extract_all.py`, or regenerate records, sig_hashes,
  join keys, or patterns;
- fall back to any JSON-driven path when a filename can't be resolved or a
  segment's materialization is incomplete.

Instead, it consumes the **already-materialized, segment-local** outputs
`tools/run_segment_orchestrator.py` produced for one segment:
`results/records/{records.csv,file_metadata.csv}`,
`results/analysis/pattern_presence_file.csv`, and
`results/bundle_analysis/{all,used}/<domain>/membership_matrix.csv`. If that
materialization is missing, incomplete, or internally inconsistent, the
comparison blocks explicitly rather than regenerating anything.

## What a "reference" is (and is not)

A reference file is a **comparison anchor**. It is not automatically a
standard, approved content, authoritative, required, or a remediation
target. `reference_only` / `target_only` differences reported by this tool
are **descriptive only** — the tool does not determine whether either
configuration is approved, required, or compliant.

## Inputs

- `--segments-root DIR` (required) — root directory containing every
  segment's output folder (`tools/run_segment_orchestrator.py`'s
  `--segments-root`).
- `--registry-file PATH` (required) — the corpus-level `run_registry.csv`
  mapping `segment_id` to its `output_folder` and completion `status`.
- `--reference-segment SEGMENT_ID` (required) — the segment the `--reference`
  selector and the reference pattern set are resolved against.
- `--target-segment SEGMENT_ID` (optional) — the segment the `--target`
  selector (or, if `--target` is omitted, the whole-segment comparison) is
  resolved against. **Defaults to the same segment as `--reference-segment`**
  — omitting it (or passing the same value) reproduces the tool's original
  single-segment behavior exactly, byte-for-byte. Pass a different segment
  to compare a reference from one segment against a target (file or entire
  segment) from a different segment.
- `--reference SELECTOR` (required) — a filename selector for the reference
  export (e.g. `template_v3.details.json`). Resolved against the reference
  segment's own `results/records/file_metadata.csv`, never against raw JSON.
- `--target SELECTOR` (optional) — a filename selector for a single target
  export, resolved against the target segment. **Omit it to compare the
  reference against the entire target segment** (every other materialized
  file in that segment).
- `--out-dir DIR` (required) — output location, owned exclusively by this
  tool (see **Overwrite behavior** below). Must not be, or contain,
  `--segments-root` or `--registry-file` — this tool refuses to run rather
  than risk destroying materialization it only ever reads.
- `--domains d1,d2` (optional) — restrict comparison to these domains.
  Default: every domain present in the **target segment's** own
  `pattern_presence_file.csv` (that's the population actually being
  scored).
- `--purge-view {all,used,both}` (default `both`) — which segment-local
  bundle-analysis view(s) to compare against. `all`/`used` populations are
  never collapsed into one another; `both` runs and reports each
  separately, distinguished by the `purge_view` column.

## Filename resolution

A selector is matched against its own segment's `file_metadata.csv`
`export_run_id` column, first for an exact match, then (falling back) by
stripping known export-file suffixes (`.details.json`, `.index.json`,
`__fingerprint.json`, `.json`) from both sides and matching on the
resulting stem — so `template_v3.details.json` resolves correctly even when
the segment's canonical `export_run_id` for that file is
`template_v3.index.json` (index is always canonical for a split-export
pair). **Zero matches or more than one match blocks the comparison** with an
explicit reason code; the tool never guesses or picks a first match.

## Examples

### Compare one file within a segment against a reference

```bash
python tools/compare_reference.py \
    --segments-root /data/Fingerprint_Data/segments \
    --registry-file /data/Fingerprint_Data/records/run_registry.csv \
    --reference-segment enterprise_all \
    --reference template_v3.details.json \
    --target project_42.details.json \
    --out-dir /data/comparisons/project_42_vs_template_v3
```

### Compare an entire segment against a reference

```bash
python tools/compare_reference.py \
    --segments-root /data/Fingerprint_Data/segments \
    --registry-file /data/Fingerprint_Data/records/run_registry.csv \
    --reference-segment client_acme \
    --reference template_v3.details.json \
    --out-dir /data/comparisons/client_acme_vs_template_v3
```

### Compare a target segment against a reference sourced from a *different* segment

```bash
python tools/compare_reference.py \
    --segments-root /data/Fingerprint_Data/segments \
    --registry-file /data/Fingerprint_Data/records/run_registry.csv \
    --reference-segment enterprise_all \
    --target-segment client_acme \
    --reference template_v3.details.json \
    --out-dir /data/comparisons/client_acme_vs_enterprise_template_v3
```

Domains whose `(join_key_schema, join_key_policy_id, join_key_policy_version)`
tuple doesn't agree between `enterprise_all` and `client_acme` block with
`CROSS_SEGMENT_JOIN_POLICY_MISMATCH` rather than comparing pattern_id sets
produced under different join policies; other, agreeing domains still
compare normally in the same run. If the two segments' own
`extractor_schema_version` don't agree (or either is absent), the entire run
blocks with `CROSS_SEGMENT_SCHEMA_MISMATCH` before any domain is evaluated —
join-key agreement alone doesn't prove the underlying pattern evidence is
comparable across extractor versions.

## Outputs

Every run writes exactly these files directly under `--out-dir`:

| File | Role |
|---|---|
| `reference_comparison_summary.csv` | One row per purge_view × target × domain |
| `reference_comparison_detail.csv` | One row per classified pattern (`shared` / `reference_only` / `target_only`) |
| `reference_comparison_diagnostics.json` | Machine-readable status/failure/degradation information |
| `reference_comparison_report.json` | Run manifest: resolved reference/target identities, target segment (`segment_id`), reference segment (`reference_segment_id`), analysis run id, aggregate status |

`--out-dir` also retains the raw intermediate `compare_all/`/`compare_used/`
directories (`file_gap_report.csv`/`file_gap_detail.csv`, written directly
by the authoritative comparator) for drill-down or debugging.

### `reference_comparison_summary.csv` columns

`segment_id, purge_view, reference_bundle_id, analysis_run_id,
target_export_run_id, domain, population_id, comparison_status,
comparison_reason_codes, reference_pattern_count, target_pattern_count,
shared_count, reference_only_count, target_only_count, union_count,
reference_coverage_pct, jaccard`

On a `blocked` row, every target-derived field (`target_pattern_count`
onward) is blank rather than a fabricated zero.

### `reference_comparison_detail.csv` columns

`segment_id, purge_view, reference_bundle_id, analysis_run_id,
target_export_run_id, domain, population_id, pattern_id, comparison_class`
where `comparison_class` is one of `shared` / `reference_only` /
`target_only`.

## Statuses: `ok` / `degraded` / `blocked`

Same semantics as the underlying comparator
(`tools/bundle_analysis/README.md`, "Comparison reliability semantics"):
**ok** — fully trustworthy target evidence; **degraded** — part of the
target's evidence was unreliable but a real partial result is still
reported; **blocked** — not enough trustworthy evidence to compute the
comparison at all for that target/domain. A `blocked` row is never zero
similarity — every target-derived numeric field is left blank specifically
so it can never be misread as a real `0`.

## Pre-flight blocking reason codes (this tool's own, distinct from the
comparator's)

| Reason code | Meaning |
|---|---|
| `SEGMENT_NOT_FOUND` | `--reference-segment`/`--target-segment` has no matching row in `--registry-file` |
| `SEGMENT_MATERIALIZATION_INCOMPLETE` | that segment's `run_registry.csv` status is not `complete` |
| `REQUIRED_ANALYSIS_ARTIFACT_MISSING` | `records.csv`/`file_metadata.csv`/`pattern_presence_file.csv`, or a requested `--purge-view` directory, is missing despite `status=complete` |
| `REFERENCE_NOT_MATERIALIZED` / `TARGET_NOT_MATERIALIZED` | the selector resolves to zero export_run_ids in its segment |
| `REFERENCE_AMBIGUOUS` / `TARGET_AMBIGUOUS` | the selector resolves to more than one export_run_id |
| `NO_COMPARISON_TARGETS` | after excluding the reference itself, no comparison target remains (an explicit `--target` resolved to the same export_run_id as `--reference`, or the target segment has no other materialized file at all) |
| `REFERENCE_HAS_NO_PATTERNS` | the resolved reference has zero `pattern_id` evidence across every domain in its segment's `pattern_presence_file.csv` — mirrors `reference_bundle.py::write_sidecar`'s own rejection of a globally empty reference |
| `CROSS_SEGMENT_SCHEMA_MISMATCH` | only possible when `--reference-segment` != `--target-segment`: the reference segment's and target segment's `extractor_schema_version` (from each segment's own `corpus_manifest.csv`) don't agree, or either is absent — mirrors `reference_bundle.py::load_and_validate`'s own sidecar-vs-current `extractor_schema_version` rejection; without it, two segments materialized under different extractor schema versions that happen to share the same join-key tuple would pass the join-policy gate and report `ok` metrics across pattern evidence that isn't actually comparable |
| `MATERIALIZATION_VERSION_INCOMPATIBLE` | the target segment's `records.csv` shows more than one distinct **complete** (all three fields populated) `(join_key_schema, join_key_policy_id, join_key_policy_version)` tuple for a requested domain -- an internal-consistency check within one segment's own materialization |
| `MATERIALIZATION_COMPATIBILITY_UNPROVEN` | no complete tuple is populated for that domain at all in the target segment's `records.csv`, or at least one record has an incomplete tuple (any of the three fields blank) even alongside an otherwise-consistent complete one — a partially-populated tuple is never treated as proof of compatibility, and an incomplete record is never simply discarded |
| `CROSS_SEGMENT_JOIN_POLICY_MISMATCH` | only possible when `--reference-segment` != `--target-segment`: the reference segment's and target segment's `(join_key_schema, join_key_policy_id, join_key_policy_version)` tuples for a requested domain don't agree, or either segment's own tuple for that domain isn't independently `ok` (see `MATERIALIZATION_VERSION_INCOMPATIBLE`/`_UNPROVEN` above) — distinct from those two codes, which check consistency *within* one segment; this checks agreement *between* two segments, since comparing pattern_id sets produced under different join policies is meaningless |
| `STALE_MEMBERSHIP_MATRIX` | a requested domain/view's `membership_matrix.csv` has rows, but none for the target segment's current `analysis_run_id` — comparing against it would silently look like "target has none of the reference's patterns" |

A `SEGMENT_NOT_FOUND`/`SEGMENT_MATERIALIZATION_INCOMPLETE`/
`REQUIRED_ANALYSIS_ARTIFACT_MISSING`/`*_NOT_MATERIALIZED`/`*_AMBIGUOUS`/
`NO_COMPARISON_TARGETS`/`REFERENCE_HAS_NO_PATTERNS`/`CROSS_SEGMENT_SCHEMA_MISMATCH`
condition blocks the **entire run** (nonzero exit; still writes the 4-file
output contract, header-only summary/detail, so the failure is never
console-only). A
`MATERIALIZATION_VERSION_INCOMPATIBLE`/`MATERIALIZATION_COMPATIBILITY_UNPROVEN`/
`CROSS_SEGMENT_JOIN_POLICY_MISMATCH`/`STALE_MEMBERSHIP_MATRIX` condition
blocks only the affected **domain** (exit `0`; check `comparison_status` per
row) — matching the existing convention that a row/domain-level blocked
outcome is not a process failure.

A domain the caller explicitly requested via `--domains` that this segment
never observed at all is not a new reason code: it surfaces as the
comparator's own existing `COMPARISON_INPUT_INVALID`
(missing `membership_matrix.csv`), reused rather than duplicated.

## Overwrite / resume behavior

Each invocation of this tool **cleanly replaces** the contents of
`--out-dir`; it never merges into a prior run's output. `--out-dir` is
treated as owned exclusively by this tool: if it already exists, is
non-empty, and was not produced by a prior run of this tool (no
`reference_comparison_report.json` present), the tool refuses to touch it
unless you pass `--overwrite`.

## Scope / limitations

- One reference vs. one target, and one reference vs. the entire target
  segment's population, are supported, whether the reference and target
  come from the same segment or two different segments. Reference-vs-many-
  references and multi-reference ranking are explicitly out of scope for
  this tool.
- No governance, compliance, or "ideal file" interpretation is added.
  `reference_only`/`target_only` are descriptive set differences only.
- `--roles` filtering and `--discover-populations` population-aware mode
  are not exposed by this tool — it always runs each segment's own
  already-computed default (non-population-aware) membership.
- Reference and target need not share an `analysis_run_id`, even when they
  come from the same segment; a future artifact-SHA-based compatibility
  check can be added to `check_domain_compatibility()`/`(join_key_schema,
  join_key_policy_id, join_key_policy_version)` without changing comparison
  semantics.
- Run selection is implicit in segment choice today because a segment's
  materialization enforces exactly one `analysis_run_id`
  (`resolve_analysis_run_id`). If segments ever retain multiple historical
  runs without being re-materialized, `--reference-run-id`/
  `--target-run-id` will need to become explicit, independent selectors
  rather than assuming segment choice is sufficient.
