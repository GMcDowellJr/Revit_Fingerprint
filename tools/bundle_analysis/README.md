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
  - take the noise→transition break (`breaks[1]`), ceil, clamp to at least `2`
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

## Compare Mode

Use `run_compare_mode.py` when you already have a known standard bundle and want to score each file against that standard. This differs from discovery mode (`run_bundle_analysis.py`), which mines bundles inductively from corpus convergence.

### When to use compare mode vs discovery mode

- **Discovery mode**: corpus mining, threshold-driven, useful for large populations and emergent standards.
- **Compare mode**: direct compliance scoring against a pre-authored (or derived) reference bundle, useful for small corpora and standards validation.

### Reference input paths (`--reference`)

`run_compare_mode.py` accepts two reference path types:

1. **JSON reference bundle** (`.json`)
   - Loaded and validated directly.
   - Hard-stops on schema mismatch (`extractor_schema_version` must equal current extractor schema version).

2. **Revit seed file** (`.rvt`)
   - The script runs `tools/run_extract_all.py` into `--out-dir/_rvt_extract_<rvt_stem>/`.
   - It reads the generated `domain_patterns.csv` + `pattern_presence_file.csv` and derives a bundle.
   - It writes a sidecar JSON next to the `.rvt`:
     - `<rvt_parent>/<rvt_stem>_reference_bundle.json`

Path selection is automatic based on extension:
- `.rvt` -> extraction + derive sidecar
- everything else -> JSON load/validate

### Sidecar JSON behavior for `.rvt` references

For `.rvt` references, compare mode writes the sidecar bundle JSON before scoring so standards managers can inspect and reuse it without re-running extraction.

Auto-populated in sidecar:
- `seed_export_run_id` (from extraction output)
- `extractor_schema_version` (from extraction output)
- `effective_date` (today, placeholder)
- `reference_bundle_id` (`<rvt_stem>-derived`, placeholder)

After writing, compare mode emits this reminder to stderr:

`[compare] Sidecar reference bundle written to <path>. Review and set reference_bundle_id and effective_date before using as a standalone reference.`

### Reference bundle format (required fields)

Required top-level fields:
- `reference_bundle_id`
- `effective_date` (ISO 8601 date string)
- `extractor_schema_version` (must match current schema exactly)
- `seed_export_run_id`
- `domains` (non-empty object: `domain -> non-empty list of non-empty pattern_id strings`)

Example shape:

```json
{
  "reference_bundle_id": "stantec-v1",
  "effective_date": "2026-04-06",
  "extractor_schema_version": "2.1",
  "seed_export_run_id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "domains": {
    "dimension_types": ["pid_abc123", "pid_def456"],
    "text_types": ["pid_ghi789"]
  }
}
```

A template is provided at:
- `tools/bundle_analysis/reference_bundle_template.json`

### Deriving pattern IDs from `domain_patterns.csv`

Pattern IDs used in reference bundles come directly from `pattern_id` values in `domain_patterns.csv` for each target domain. Compare mode performs exact string matching only (no normalization/fuzzy matching).

### Seed exclusion behavior

Compare mode excludes `seed_export_run_id` from scoring population before coverage calculations. This applies regardless of whether the reference was loaded from JSON or derived from `.rvt`.

### `NO_REFERENCE_DEFINED` sentinel behavior

If a domain exists in membership matrices but is missing from `reference.domains`, compare mode emits a sentinel row with:
- `coverage_status = NO_REFERENCE_DEFINED`
- empty pattern count / gap fields

This is a visibility signal for downstream BI, not a pipeline failure.

### Outputs

Compare mode writes:
- `file_gap_report.csv` with columns:
  - `reference_bundle_id,effective_date,analysis_run_id,domain,export_run_id,patterns_required,patterns_present,patterns_missing,gap_pattern_ids,coverage_pct,coverage_status`
- `compare_run_summary.csv` with per-domain rollups:
  - `reference_bundle_id,effective_date,analysis_run_id,domain,files_scored,full_count,partial_count,none_count,no_reference_count`

### Entrypoint integration

You can run compare mode either directly (`run_compare_mode.py`) or via the main orchestrator entrypoint:

```bash
python tools/bundle_analysis/run_bundle_analysis.py \
  --analysis-dir results/bundle_analysis \
  --out-dir results/bundle_compare \
  --compare-reference /path/to/reference_bundle.json
```

When `--compare-reference` is provided, `run_bundle_analysis.py` dispatches to compare mode instead of running discovery steps 1-7.

### Example invocations

JSON reference path:

```bash
python tools/bundle_analysis/run_compare_mode.py \
  --analysis-dir results/bundle_analysis \
  --out-dir results/bundle_compare \
  --reference tools/bundle_analysis/reference_bundle_template.json
```

RVT reference path:

```bash
python tools/bundle_analysis/run_compare_mode.py \
  --analysis-dir results/bundle_analysis \
  --out-dir results/bundle_compare \
  --reference /path/to/seed_model.rvt
```

Optional single-domain run:

```bash
python tools/bundle_analysis/run_compare_mode.py \
  --analysis-dir results/bundle_analysis \
  --out-dir results/bundle_compare \
  --reference /path/to/reference_bundle.json \
  --domain dimension_types
```
