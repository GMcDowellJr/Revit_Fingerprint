# Bundle Analysis Pipeline

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
