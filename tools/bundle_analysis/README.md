# Bundle Analysis Pipeline

This directory contains the multi-step bundle analysis pipeline:

1. `step1_membership_matrix.py`
2. `step2_find_bundles.py`
3. `step3_build_dag.py`
4. `step4_difference_sets.py`
5. `step5_classify_patterns.py`
6. `step6_classify_files.py`
7. `step7_overlap_report.py`

Use `run_bundle_analysis.py` to orchestrate end-to-end execution.

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
