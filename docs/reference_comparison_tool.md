# Reference Comparison Tool (`tools/compare_reference.py`)

PR3. A standalone, deliberately-invoked workflow for comparing one reference
fingerprint export against one target export or a target corpus, producing a
stable package of consumable outputs.

This tool implements **no comparison mathematics of its own**. It is an
orchestration/output layer over the existing, authoritative implementation:

- `tools/run_extract_all.py --seed` builds the `reference_bundle.json`
  sidecar and the analysis-side pattern outputs.
- `tools/bundle_analysis/run_bundle_analysis.py --compare` computes the
  symmetric shared/reference_only/target_only comparison
  (`tools/bundle_analysis/step_compare.py`) and its ok/degraded/blocked
  reliability semantics (`tools/bundle_analysis/comparison_status.py`,
  documented in full in `tools/bundle_analysis/README.md`).

Both are invoked as subprocesses. There remains exactly one implementation of
comparison semantics; this tool only stages inputs and reshapes outputs.

## What a "reference" is (and is not)

A reference file is a **comparison anchor**. It is not automatically a
standard, approved content, authoritative, required, or a remediation
target. `reference_only` / `target_only` differences reported by this tool
are **descriptive only** — the tool does not determine whether either
configuration is approved, required, or compliant. That judgment is left
entirely to whoever reads the output.

## Inputs

This tool operates on **already-extracted fingerprint exports**
(`*.details.json` / `*.index.json` / `*__fingerprint.json`), not raw RVT
files. If you only have RVT files, extract them first with the existing
Dynamo runner (`runner/run_dynamo.py`) — that step is unchanged by this PR.

- `--reference PATH` (required) — the comparison anchor's export file.
- `--target PATH [PATH ...]` — one or more target export files (mutually
  exclusive with `--target-dir`).
- `--target-dir DIR` — a directory containing a target corpus of export
  files. Every primary export file discovered there (same file-format
  priority as the rest of the pipeline: `*__fingerprint.json` >
  `*.details.json` > a bare `*.json`; `*.legacy.json` is never picked up
  implicitly) becomes a target. A file that happens to share the
  reference's exact filename is silently excluded from the target set (it
  is the reference, not a target).
- `--out-dir DIR` (required) — output location, owned exclusively by this
  tool (see **Overwrite behavior** below).

Split-export pairs (`foo.details.json` + `foo.index.json`) are staged
together automatically, whichever half you point `--reference`/`--target`
at.

## Recommended: pass an existing join-key policy

`--join-policy PATH` lets you point at an already-discovered,
corpus-representative `domain_join_key_policies.v21.json` (from a prior full
corpus run of `tools/run_extract_all.py`). This is the recommended way to
run a reference-vs-target comparison: discovering a *new* join-key policy
from just a reference plus a handful of targets is not statistically
meaningful, so `--join-policy` skips the `discover` stage entirely and
reuses your corpus's established policy. Without `--join-policy`, this tool
runs `discover` fresh every time, which is only appropriate for small
exploratory comparisons.

## Examples

### Compare one extracted file against a reference

```bash
python tools/compare_reference.py \
    --reference /data/exports/template_v3.details.json \
    --target /data/exports/project_42.details.json \
    --join-policy /data/corpus_results/policies/domain_join_key_policies.v21.json \
    --out-dir /data/comparisons/project_42_vs_template_v3
```

### Compare a corpus against a reference

```bash
python tools/compare_reference.py \
    --reference /data/exports/template_v3.details.json \
    --target-dir /data/exports/regional_office_corpus/ \
    --join-policy /data/corpus_results/policies/domain_join_key_policies.v21.json \
    --out-dir /data/comparisons/regional_office_vs_template_v3
```

## Outputs

Every run writes exactly these files directly under `--out-dir`:

| File | Role |
|---|---|
| `reference_comparison_summary.csv` | One row per reference × target × domain (× population, if `--discover-populations` is used) |
| `reference_comparison_detail.csv` | One row per classified pattern (`shared` / `reference_only` / `target_only`) |
| `reference_comparison_diagnostics.json` | Machine-readable status/failure/degradation information, scoped to the affected domain/target |
| `reference_comparison_report.json` | Run manifest: reference identity, target scope, analysis run id, output files, aggregate status, the exact sub-tool commands run |

`--out-dir` also retains the raw intermediate output of the two sub-tools
(`extraction/`, `bundle_analysis/`, `staged_exports/`) for drill-down or
debugging — the four files above are the intended consumable package.

### `reference_comparison_summary.csv` columns

`reference_bundle_id, analysis_run_id, target_export_run_id, domain,
population_id, comparison_status, comparison_reason_codes,
reference_pattern_count, target_pattern_count, shared_count,
reference_only_count, target_only_count, union_count,
reference_coverage_pct, jaccard`

On a `blocked` row, every target-derived field (`target_pattern_count`
onward) is blank rather than a fabricated zero — see **Statuses** below.

### `reference_comparison_detail.csv` columns

`reference_bundle_id, analysis_run_id, target_export_run_id, domain,
population_id, pattern_id, comparison_class` where `comparison_class` is one
of `shared` / `reference_only` / `target_only`.

### `reference_comparison_diagnostics.json` shape

```json
{
  "reference_bundle_id": "...",
  "analysis_run_id": "...",
  "run_comparison_status": "ok|degraded|blocked",
  "run_comparison_reason_codes": ["..."],
  "domains_total": "1", "domains_ok": "1", "domains_degraded": "0", "domains_blocked": "0",
  "domain_summaries": [ { "domain": "...", "comparison_status": "...", "files_scored": "...", ... } ],
  "target_diagnostics": [ { "target_export_run_id": "...", "domain": "...", "comparison_status": "degraded|blocked", "comparison_reason_codes": ["..."], "comparison_detail": "..." } ]
}
```

`target_diagnostics` lists only non-`ok` targets/domains — you should not
need to scrape stdout to learn that a comparison failed or degraded.

## What the fields mean

- **shared** — a configuration (pattern) appears in both the reference and
  the target.
- **reference_only** — a configuration appears in the reference but not the
  target.
- **target_only** — a configuration appears in the target but not the
  reference.
- **reference_coverage** — the proportion of reference configurations found
  in the target.
- **jaccard** — shared configurations divided by all distinct configurations
  across both files (`shared / union`).

Reference-only and target-only differences are **descriptive**. This tool
does not determine whether either configuration is approved, required, or
compliant.

## Statuses: `ok` / `degraded` / `blocked`

- **ok** — the comparison ran with fully trustworthy target evidence.
- **degraded** — part of the target's evidence for a domain was unreliable
  (e.g. some records had unassignable identity); the comparison still
  reports a real, partial result over the reliable subset.
- **blocked** — there was not enough trustworthy evidence to compute the
  comparison at all for that target/domain (e.g. the domain was never
  observed, or every record's identity was unassignable), or the reference
  itself was missing/invalid/schema-incompatible.

**Do not interpret a `blocked` comparison as zero similarity or complete
absence.** A blocked row means the comparison could not be trusted, not that
the target has none of the reference's configurations — every
target-derived numeric field on a blocked row is left blank specifically so
it can never be misread as a real `0`.

Full semantics (including exact classification rules) live in
`tools/bundle_analysis/README.md`, "Comparison reliability semantics".

## Overwrite / resume behavior

Each invocation of this tool **cleanly replaces** the contents of
`--out-dir`; it never merges into a prior run's output. `--out-dir` is
treated as owned exclusively by this tool: if it already exists, is
non-empty, and was not produced by a prior run of this tool (no
`reference_comparison_report.json` present), the tool refuses to touch it
unless you pass `--overwrite`. This guarantees a stale prior comparison can
never be confused with the current one, and that reference/target
provenance is always explicit in every persisted result.

## Command exit behavior

- Missing or malformed `--reference`/`--target` input → nonzero exit before
  any sub-tool is invoked.
- A totally invalid or schema-incompatible reference (caught by
  `run_bundle_analysis.py --compare`) → nonzero exit, but
  `reference_comparison_diagnostics.json` is still written with the blocked
  status and reason code recorded — the failure is never console-only.
- A comparison that completes with some `degraded` or row/domain-level
  `blocked` results (but no total sidecar failure) → exit `0`; check
  `reference_comparison_report.json`'s `aggregate_comparison_status` and
  `reference_comparison_diagnostics.json`'s `target_diagnostics` for the
  real status. This matches the existing convention in
  `tools/bundle_analysis/run_bundle_analysis.py`.

## Scope / limitations

- One reference vs. one target, and one reference vs. many targets (via
  `--target-dir`, reusing the existing corpus/bundle-analysis pipeline in a
  single pass) are supported. Reference-vs-many-references and
  multi-reference ranking are explicitly out of scope for this PR.
- No governance, compliance, or "ideal file" interpretation is added.
  `reference_only`/`target_only` are descriptive set differences only.
- `--target-dir` stages (hardlinks or copies) the discovered export files
  into `--out-dir/staged_exports/`; for a very large corpus this is a
  one-time, bounded-memory file-copy operation, not a re-extraction.
- Fine-grained population scoping (`--discover-populations` and its
  thresholds) and governance-role filtering (`--roles` /
  `--metadata-file`) are passed straight through to
  `run_bundle_analysis.py`; see that script's own `--help` for the full
  semantics.
