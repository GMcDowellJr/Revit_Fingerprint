# Governance Generator Coverage of Cross-Segment Outputs

`tools/compare_cross_segment.py` now emits more governance-adjacent CSVs than
`tools/generate_governance_narrative.py` consumes directly. This note documents
which files are already covered, which files are only partially covered, and
where the remaining outputs should be incorporated so the governance summary does
not over-weight pair means while ignoring corpus-wide reuse, project matrix, and
run completeness signals.

## Current direct inputs

The narrative generator currently requires only:

- `cross_segment_summary.csv` via `--summary` for cascade, discipline, client,
  domain-tier, and domain-summary CSV metrics.
- `cross_segment_pooled.csv` via `--pooled` for client discovery/file counts and
  client summary support.

It optionally consumes:

- `cross_segment_governance_state_summary.csv` / `cross_segment_governance_states.csv`
  for explicit provided/used/passive/missing/local-active state sections.
- `cross_segment_delta.csv` as a legacy fallback only when governance-state
  outputs are absent.
- `cross_segment_union_inventory.csv`, `pattern_reuse_distribution.csv`, and
  `matrix_output_manifest.csv` for the current `Union Inventory Reuse Summary`.
- `pattern_reuse_summary_by_client.csv` via `--reuse-by-client`, for an
  adoption-breadth cut (how many of a domain's clients reach a
  corpus-wide-reused pattern) inside the same `Union Inventory Reuse Summary`
  section -- additive to, and independent of, the distinct-pattern table.
- `project_union_jaccard_matrix.csv`, `project_density_similarity_matrix.csv`,
  `project_pool_containment_similarity_matrix.csv`, and
  `project_fragmentation_diagnostic.csv` (via `--project-union-jaccard-matrix`,
  `--project-density-similarity-matrix`, `--project-pool-containment-matrix`,
  `--project-fragmentation-diagnostic`) feed the new **Project Portfolio**
  section, kept outside `assign_tier()`/`governance_domain_summary.csv` per
  the reporting guardrails below.

## Cross-segment outputs still absent or under-used

| Cross-segment output | Current generator coverage | Recommended inclusion point | Why it belongs there |
|---|---|---|---|
| `comparison_registry.csv` | Not consumed. | Add optional `--comparison-registry`; render an **Input Completeness / Staleness** note near Analytical Notes and block or warn when expected segment/domain pairs are missing or stale. | The narrative currently treats missing rows as weak evidence, but the registry can distinguish actual weak evidence from not-run/stale comparisons. |
| `cross_segment_file_pairs.csv` | Not consumed. | Usually keep out of the top-level narrative; use only for drill-through appendix links or a generated review pack for domains flagged by anomalies. | It is too large for leadership summary, but it is the best evidence trail when a tier or anomaly needs file-level audit. |
| `cross_segment_union_inventory.csv` | Partially consumed only to count blocked project domains when manifest metadata exists. | Extend the domain summary CSV with corpus-wide/project/client/file breadth fields and use it to annotate tier confidence. | It reports pattern prevalence across files/projects/clients, which pairwise Jaccard cannot express. |
| `cross_segment_delta.csv` | Consumed only as fallback if governance-state summaries are not supplied. | Keep as fallback; do not blend with governance-state outputs unless rendered as a separate legacy-comparison appendix. | Governance-state rows supersede delta for provided/local/missing interpretation; mixing both can double-count the same drift signal. |
| `pattern_reuse_distribution.csv` | Consumed for a top-20 bucket table by domain. | Add domain-level reuse-breadth metrics to `governance_domain_summary.csv` and call out domains with high `corpus_wide`/`client_wide` reuse but weak formal cascade. | This identifies natural standards candidates that are broadly reused even when template propagation is weak. |
| `pattern_reuse_summary_by_domain.csv` | Not consumed -- deliberate scoping decision, not a gap. | Do not wire in: its `n_patterns` duplicates the corpus-wide reuse signal the distinct-pattern table (sourced from `pattern_reuse_distribution.csv`) already reports. | Evaluated and confirmed to add no new signal beyond what the existing distinct-pattern dedup table already provides. |
| `pattern_reuse_summary_by_client.csv` | Consumed via `--reuse-by-client`, as an adoption-breadth cut inside `Union Inventory Reuse Summary`. | Done. | Supports "how many clients reach this pattern" claims that the distinct-pattern table (which never groups by client) cannot answer. |
| `project_union_jaccard_matrix.csv` | Consumed via `--project-union-jaccard-matrix`, in the Project Portfolio section's footprint-identity paragraph (ALL_DOMAINS, all-view, top/bottom-N project pairs). | Done. | It measures exact system-level project footprint overlap, which differs from mean file-pair similarity. |
| `project_density_similarity_matrix.csv` | Consumed via `--project-density-similarity-matrix`, in the Project Portfolio section's density-similarity paragraph, cross-referenced against union Jaccard for the "same shape, different content" caveat when both are supplied. | Done. | Density similarity can show similar domain population even when exact pattern identity diverges. |
| `project_mean_file_pair_jaccard_matrix.csv` | Not consumed standalone -- deliberate scoping decision. Its signal is already present in `project_fragmentation_diagnostic.csv`'s own `exact_identity_overlap` column, folded into the fragmentation-diagnostic paragraph. | Do not add a separate flag for this file. | Rendering it standalone would duplicate a signal already visible via the fragmentation diagnostic and the existing `sibling_projects`/`cross_client` rows elsewhere in the narrative. |
| `project_pool_containment_similarity_matrix.csv` | Consumed via `--project-pool-containment-matrix`, in the Project Portfolio section's peer-pool-containment paragraph, rendered as a per-project outlier list (mean across available domains per project/pool-scope grain, since this matrix carries no ALL_DOMAINS aggregate row). | Done. | This is the matrix counterpart to pooled comparisons and is a direct review aid for outlier projects. |
| `project_fragmentation_diagnostic.csv` | Consumed via `--project-fragmentation-diagnostic`, as the Project Portfolio section's fragmentation-diagnostic paragraph. | Done. | It explicitly captures divergence between project footprint overlap and file-pair identity overlap. |
| `matrix_output_manifest.csv` | Consumed only as descriptive bullets. | Keep as the availability/metadata source for any matrix section; use it to disclose blocked/unavailable matrices and metric limitations. | It documents source grain, interpretation, and limitations for matrix-derived claims. |

## Suggested implementation sequence

1. **Completeness first:** add `--comparison-registry` and an input completeness
   section before making additional governance claims from missing rows.
2. **Cheap compact reuse:** ~~add `--reuse-summary-by-domain` and~~
   `--reuse-by-client` -- done, as an additive adoption-breadth cut alongside
   the existing distinct-pattern table. `--reuse-summary-by-domain` was
   evaluated and deliberately not added (see the table above).
3. **Domain confidence enrichment:** add union-inventory-derived breadth columns to
   `governance_domain_summary.csv`, then render only the strongest narrative
   exceptions: broad natural reuse but weak formal cascade, or narrow reuse but
   strong cascade. (Still open.)
4. **Project/portfolio matrix section:** done -- the **Project Portfolio**
   section consumes `project_union_jaccard_matrix.csv`,
   `project_density_similarity_matrix.csv`,
   `project_pool_containment_similarity_matrix.csv`, and
   `project_fragmentation_diagnostic.csv` behind optional flags (folding in
   `project_mean_file_pair_jaccard_matrix.csv`'s signal via the diagnostic's
   own `exact_identity_overlap` column rather than a fifth flag). Kept
   separate from the existing domain tiers and `governance_domain_summary.csv`
   because these matrices answer portfolio-shape questions, not
   domain-standard approval questions.
5. **Drill-through only:** reserve `cross_segment_file_pairs.csv` for appendices or
   generated audit packs after a domain/project has already been flagged.

## Reporting guardrails

- Do not use matrix values to override domain governance tiers directly; they are
  project/portfolio diagnostics and should remain separate from domain-level
  cascade/state classifications.
- Prefer producer summaries (`pattern_reuse_summary_by_*`) when present, and use
  full-detail files (`pattern_reuse_distribution.csv`, `cross_segment_file_pairs.csv`)
  only for fallback aggregation or drill-through.
- Every optional section should state whether its source file was supplied and
  whether `matrix_output_manifest.csv` reports a limitation or unavailable status
  for that metric.
