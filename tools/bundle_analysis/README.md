# Bundle Analysis Pipeline

> For a deliberately-invoked, single-command reference-vs-target comparison
> workflow that orchestrates this pipeline together with
> `tools/run_extract_all.py` end to end, see `tools/compare_reference.py`
> and `docs/reference_comparison_tool.md`. This README remains the
> authoritative reference for the underlying `--compare` contract itself.

This directory contains the multi-step bundle analysis pipeline:

0. `step0_discover_populations.py` *(optional pre-pass)*
1. `step1_membership_matrix.py`
2. `step2_find_bundles.py`
3. `step3_build_dag.py`
4. `step4_difference_sets.py`
5. `step5_classify_patterns.py`
6. `step6_classify_files.py`
7. `step7_overlap_report.py`

Use `run_bundle_analysis.py` to orchestrate end-to-end execution.

---

## Population-aware mode (Step 0 + per-population runs)

`run_bundle_analysis.py` supports two modes:

- **Single-pass mode (default)**: runs steps 1–7 once per domain.
- **Population-aware mode** (`--discover-populations`): runs:
  1. Step 0 discovery pre-pass
  2. Steps 1–7 once per discovered primary population

### Discovery flags

`run_bundle_analysis.py` and `step0_discover_populations.py` expose:

- `--discover-populations` (orchestrator only)
- `--min-population-size`
- `--max-population-overlap`
- `--min-population-jaccard`
- `--discovery-support-pct` (default `0.50`, minimum `0.05`)

### Step 0 output files

Step 0 writes corpus-level outputs to `bundle_analysis/`:

- `corpus_populations.csv`
- `corpus_population_summary.csv`
- `corpus_population_root_patterns.csv`
- `corpus_population_parameters.csv`

And debug outputs to:

- `bundle_analysis/_population_discovery/`

### Scope-aware discovery

Step 0 uses the same scope derivation logic as step 1 (`derive_scope_key`):

- Normal domains: scope is `""`
- Row-key domains: scope per element label
- Shape-gated domains: scope per schema key

As a result, corpus-level outputs include `scope_key` and are keyed by
`analysis_run_id × domain × scope_key × ...`.

### Per-population output directories

In population-aware mode, each population run is staged and then written to:

- `bundle_analysis/{domain}/{population_id}/`

`population_id` already includes the `pop_` prefix.

---

## Step 2 thresholding (`step2_find_bundles.py`)

Step 2 discovers closed frequent itemsets (bundles) from the file × pattern
membership matrix.

### Effective threshold

For each `domain × scope_key`, Step 2 derives an **auto-threshold** and then
applies the CLI value as a floor:

- `chosen_auto_threshold` comes from data-derived computation
- `effective_threshold = max(cli_min_support_count, chosen_auto_threshold)`

`--min-support-count` remains available and defaults to `3`. Auto-thresholding
is always active and can only raise the threshold above the CLI floor.

### Auto-threshold derivation

`compute_auto_threshold(file_sets, files_total)` computes:

- `expected_floor`:
  - build pairwise expected co-occurrence under independence
  - take p90 of expected values
  - multiply by `EXPECTED_MULTIPLIER=2.0`
  - ceil and clamp to at least `2`
- `natural_breaks_floor`:
  - build actual pairwise co-occurrence counts (for pairs with support ≥ 2)
  - run pure-Python Fisher-Jenks (`jenks_natural_breaks`, `n_classes=3`)
  - take the noise→transition break (`breaks[0]`), ceil, clamp to at least `2`
  - if co-occurrence values are too sparse (<4 distinct values), fall back to
    `expected_floor`

Primary threshold choice is `natural_breaks_floor`.

### Diagnostics output

Step 2 writes:

- `bundle_analysis/{domain}/bundle_analysis_thresholds.csv`

One row is emitted per `domain × scope_key`, including:

- expected / natural-break thresholds
- chosen auto-threshold
- CLI floor and effective threshold
- method details
- co-occurrence histogram (JSON-encoded)

This diagnostics CSV is written even when zero bundles are found.

### Logging

Per scope, Step 2 logs:

- `[step2_threshold] ... expected_floor=... natural_breaks_floor=... chosen=... cli_floor=... effective=...`
- `[step2_threshold_fallback] ...` when auto-threshold computation errors
- `[step2] ... effective_threshold=...` on step summary

---

## Reference Bundle Comparison (`--compare` / `step_compare.py`)

`run_bundle_analysis.py --compare` compares each target file's discovered
bundle-analysis patterns against a `reference_bundle.json` sidecar (written
by `tools/run_extract_all.py --seed`, see `reference_bundle.write_sidecar`)
for the same domain. Pattern identity is the existing bundle-analysis
`pattern_id` from `membership_matrix.csv` — no separate identity scheme is
introduced.

The comparison is symmetric: for every `reference_bundle_id × export_run_id
× domain × population_id` it derives

- `shared = reference ∩ target`
- `reference_only = reference - target`
- `target_only = target - reference`
- `union = reference ∪ target`

`export_run_id` never includes the seed file itself
(`reference["seed_export_run_id"]` is excluded from the comparable set).

### `compare_*/file_gap_report.csv`

One row per `reference_bundle_id × export_run_id × domain × population_id`.

Original one-way reference-coverage fields (semantics unchanged):

| Field | Meaning |
|---|---|
| `patterns_required` | `len(reference)` |
| `patterns_present` | `len(shared)` |
| `patterns_missing` | `len(reference_only)` |
| `gap_pattern_ids` | sorted `reference_only`, `\|`-joined |
| `coverage_pct` | `patterns_present / patterns_required`, `%.6f` |
| `coverage_status` | `full` / `partial` / `none` / `NO_REFERENCE_DEFINED` |

Symmetric set-comparison fields (added):

| Field | Meaning |
|---|---|
| `reference_pattern_count` | `len(reference)` |
| `target_pattern_count` | `len(target)` |
| `shared_count` | `len(shared)` |
| `reference_only_count` | `len(reference_only)` |
| `target_only_count` | `len(target_only)` |
| `union_count` | `len(union)` |
| `shared_pattern_ids` | sorted `shared`, `\|`-joined |
| `reference_only_pattern_ids` | sorted `reference_only`, `\|`-joined (same set as `gap_pattern_ids`) |
| `target_only_pattern_ids` | sorted `target_only`, `\|`-joined |
| `reference_coverage_pct` | same value as `coverage_pct`, under the new field name |
| `jaccard` | `shared_count / union_count`, `%.6f` |

A target that contains every reference pattern plus additional ones
(`coverage_status == "full"`, `coverage_pct == "1.000000"`) still reports
those additional patterns via `target_only_count`/`target_only_pattern_ids`
— full reference coverage no longer causes target-only patterns to be
dropped from the output.

**`NO_REFERENCE_DEFINED`** (the domain has no entry in the reference
sidecar's `domains` map) is distinct from a defined reference containing
zero patterns (the sidecar schema forbids an empty pattern list per
domain). In this case the new count fields are `"0"`, the new
pattern-id/ratio fields are `""`, and no detail rows are emitted for that
`export_run_id`/domain — there is no reference set to classify patterns
against.

**Zero-denominator ratios** (`coverage_pct`, `reference_coverage_pct`,
`jaccard`) never emit `NaN`/`Infinity`; the only zero-denominator case is
`NO_REFERENCE_DEFINED`, where they serialize as `""`.

### `compare_*/file_gap_detail.csv`

One row per `reference_bundle_id × export_run_id × domain × population_id ×
pattern_id`, for machine-readable per-pattern drill-down:

| Field | Meaning |
|---|---|
| `reference_bundle_id` | from the reference sidecar |
| `analysis_run_id` | current analysis run |
| `domain` | domain name |
| `population_id` | population id, or `""` outside population-aware mode |
| `export_run_id` | target file |
| `pattern_id` | bundle-analysis pattern identity |
| `comparison_class` | one of `shared` / `reference_only` / `target_only` |

Rows are sorted by `(analysis_run_id, domain, population_id, export_run_id,
pattern_id)`; pattern IDs within each class are always emitted in sorted
order for deterministic output.

No governance, compliance, or "correctness" meaning is assigned to
`reference_only`/`target_only` — these are descriptive set differences
only. Whether a difference matters is left to a downstream consumer.

### Comparison reliability semantics (`comparison_status`, PR2)

The fields above describe *what* differs. They say nothing about whether
there was enough trustworthy evidence to compute that difference at all —
absence of a `membership_matrix.csv` row for a pattern can mean either
"the target genuinely doesn't have this configuration" or "the target
domain couldn't be observed reliably" (extraction failure, missing/invalid
join identity). `file_gap_report.csv` and `compare_run_summary.csv` add
three fields to make that distinction explicit and machine-readable:

| Field | Meaning |
|---|---|
| `comparison_status` | `ok` / `degraded` / `blocked` (see `tools/bundle_analysis/comparison_status.py`; reuses `core/contracts.py`'s domain-status vocabulary verbatim) |
| `comparison_reason_codes` | `\|`-joined, sorted, stable reason codes; `""` when `ok` with no notable condition |
| `comparison_detail` | optional human-readable elaboration (e.g. `identity_unknown_share=0.500000`); never authoritative, `comparison_reason_codes` is |

Classification per `(export_run_id, domain)`, using `pattern_presence_file.csv`'s
existing `pattern_id=""` "UNKNOWN" bucket row (already emitted by
`tools/extractor.py::_process_one_domain` whenever a record has no
assignable `join_hash` — e.g. missing/invalid join identity) plus presence-row
existence itself:

- **No presence evidence at all** for an `export_run_id` that was explicitly
  proposed as an eligible comparison target (via `--roles` or
  population-membership filtering) → `blocked` / `TARGET_DOMAIN_UNAVAILABLE`.
  Not reachable via the default (non-eligibility-widened) derivation path,
  which only ever proposes `export_run_id`s that already have ≥1 presence row.
- **100% of a file's presence rows are the UNKNOWN bucket** (every record
  present had unassignable identity) → `blocked` / `TARGET_IDENTITY_INVALID`.
- **Some but not all of a file's presence rows are the UNKNOWN bucket** →
  `degraded` / `TARGET_DOMAIN_DEGRADED`, `comparison_detail` carries
  `identity_unknown_share=<pct>`. The ordinary `shared`/`reference_only`/
  `target_only` fields are still computed and populated from the reliable
  (assignable) subset — a degraded row is a real partial result, not blanked.
- **A presence schema without the `pattern_id` column at all** (older/minimal
  exports) carries no per-pattern granularity to classify from; presence
  alone is treated as sufficient evidence of a genuine observation (`ok`) —
  this is the pre-PR2 behavior, preserved exactly.
- Otherwise → `ok`.

`NO_REFERENCE_DEFINED` (see above) is a distinct condition from a
target-evidence failure — nothing about the target's evidence is unreliable
there, there is simply no reference to compare against — so it reports
`comparison_status=ok`, `comparison_reason_codes=REFERENCE_DOMAIN_UNDEFINED`.

**On a `blocked` row**, every target-derived field is blanked to `""` rather
than a plausible-looking `"0"`/count/ratio: `coverage_status`, `coverage_pct`,
`reference_coverage_pct`, `jaccard`, `target_pattern_count`, `shared_count`,
`reference_only_count`, `target_only_count`, `union_count`, and all three
`*_pattern_ids` list fields (and no `file_gap_detail.csv` rows are emitted for
that `export_run_id`/domain). `patterns_required`/`reference_pattern_count`
stay populated — they describe the reference side only, which target-evidence
reliability doesn't affect.

**Reference sidecar failures** (missing file, malformed/non-UTF-8 JSON, or
`extractor_schema_version` mismatch) raise `ReferenceBundleMissingError` /
`ReferenceBundleInvalidError` / `ReferenceBundleSchemaMismatchError`
(`tools/bundle_analysis/reference_bundle.py`, all `ValueError` subclasses)
before any per-domain comparison begins. `run_bundle_analysis.py` writes a
`blocked` row (reason `REFERENCE_INVALID` or `SCHEMA_INCOMPATIBLE`) for
every requested domain into each requested view's own
`compare_<view>/compare_run_summary.csv` / `compare_run_status.csv` — the
same paths a successful run writes, so a consumer watching the normal
per-view outputs sees the failure, and any stale `ok` status left there by
an earlier successful run into a reused `out_dir` is overwritten — then
re-raises: comparison is never silently skipped, but the failure is
machine-readable rather than console-only. Separately, if
`run_compare_for_domain`'s own inputs (`pattern_presence_file.csv` /
`membership_matrix.csv`) are missing or internally inconsistent, it returns
a `blocked` / `COMPARISON_INPUT_INVALID` domain summary instead of raising
into the caller's bare `except Exception: print(...)` domain-pipeline
handler, and replaces any stale per-file rows a prior successful run left in
`file_gap_report.csv`/`file_gap_detail.csv` for that same
`(analysis_run_id, domain, population_id)` with a single blocked placeholder
row (`export_run_id=""`) rather than leaving them standing. And within
`run_bundle_analysis.py`'s own per-domain/per-population loops, a pipeline
stage failing *before* `run_compare_for_domain` is ever called (step0/step1/
discovery raising) appends a precisely-scoped blocked/
`COMPARISON_INPUT_INVALID` row for that exact `(domain, population_id)`
right at the failure site, rather than letting it vanish into the
surrounding `except Exception: print(...)` handler with no comparison
record at all.

**Run-level aggregation**: `compare_run_summary.csv` gains
`comparison_status`/`comparison_reason_codes`/`comparison_ok_count`/
`comparison_degraded_count`/`comparison_blocked_count` per domain/population
row. A separate `compare_run_status.csv` gives one row per analysis run with
the overall rollup — any `blocked` domain → `blocked`, else any `degraded` →
`degraded`, else `ok` — so a degraded or blocked comparison run is visible
without reading console output. In population-aware mode, a domain's
several `(domain, population_id)` rows are first rolled up to one status per
domain before counting `domains_total`/`domains_ok`/`domains_degraded`/
`domains_blocked`, so a domain with three population rows is counted once,
not three times.

**Known gap, not addressed by this contract:** a domain that is entirely
blocked at extraction time (e.g. a hard dependency failure in
`runner/run_dynamo.py`) can produce zero individual item records for a file,
which — using only `pattern_presence_file.csv`/`membership_matrix.csv`/
`records.csv` — is indistinguishable from a domain that was genuinely
observed and legitimately has zero elements. The authoritative signal for
that distinction (`_contract.domains[domain].status` in the raw per-file
export JSON) is never propagated into any analysis-side CSV by
`tools/extractor.py::emit_records`. Closing this gap would require extending
the flatten stage; it is out of scope here (extraction/flatten logic is not
touched by this contract).
