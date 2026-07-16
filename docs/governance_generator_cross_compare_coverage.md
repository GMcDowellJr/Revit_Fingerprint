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

## Cross-segment outputs still absent or under-used

| Cross-segment output | Current generator coverage | Recommended inclusion point | Why it belongs there |
|---|---|---|---|
| `comparison_registry.csv` | Not consumed. | Add optional `--comparison-registry`; render an **Input Completeness / Staleness** note near Analytical Notes and block or warn when expected segment/domain pairs are missing or stale. | The narrative currently treats missing rows as weak evidence, but the registry can distinguish actual weak evidence from not-run/stale comparisons. |
| `cross_segment_file_pairs.csv` | Not consumed. | Usually keep out of the top-level narrative; use only for drill-through appendix links or a generated review pack for domains flagged by anomalies. | It is too large for leadership summary, but it is the best evidence trail when a tier or anomaly needs file-level audit. |
| `cross_segment_union_inventory.csv` | Partially consumed only to count blocked project domains when manifest metadata exists. | Extend the domain summary CSV with corpus-wide/project/client/file breadth fields and use it to annotate tier confidence. | It reports pattern prevalence across files/projects/clients, which pairwise Jaccard cannot express. |
| `cross_segment_delta.csv` | Consumed only as fallback if governance-state summaries are not supplied. | Keep as fallback; do not blend with governance-state outputs unless rendered as a separate legacy-comparison appendix. | Governance-state rows supersede delta for provided/local/missing interpretation; mixing both can double-count the same drift signal. |
| `pattern_reuse_distribution.csv` | Consumed for a top-20 bucket table by domain. | Add domain-level reuse-breadth metrics to `governance_domain_summary.csv` and call out domains with high `corpus_wide`/`client_wide` reuse but weak formal cascade. | This identifies natural standards candidates that are broadly reused even when template propagation is weak. |
| `pattern_reuse_summary_by_domain.csv` | Not consumed, despite being a compact version of distribution. | Prefer this file over re-aggregating the full distribution when present; fall back to distribution only when summary is absent. | It is smaller and already producer-normalized by bucket/domain/view/role. |
| `pattern_reuse_summary_by_client.csv` | Not consumed. | Add optional client-level table or enrich `governance_client_summary.csv` with each client's dominant reuse buckets. | It supports onboarding and client playbook claims better than cross-client similarity alone. |
| `project_union_jaccard_matrix.csv` | Not consumed. | Add optional `--project-union-jaccard-matrix`; summarize strongest/weakest project clusters and feed client/portfolio variation notes. | It measures exact system-level project footprint overlap, which differs from mean file-pair similarity. |
| `project_density_similarity_matrix.csv` | Not consumed. | Add optional `--project-density-similarity-matrix`; use beside union Jaccard to separate same-domain-footprint projects from same-pattern projects. | Density similarity can show similar domain population even when exact pattern identity diverges. |
| `project_mean_file_pair_jaccard_matrix.csv` | Not consumed. | Add optional `--project-mean-file-pair-jaccard-matrix`; compare against union Jaccard in the project variation section. | It captures typical file-to-file identity overlap, not just project-level union overlap. |
| `project_pool_containment_similarity_matrix.csv` | Not consumed. | Add optional `--project-pool-containment-matrix`; summarize each project group's fit against parent-sibling, business-center, and client pools. | This is the matrix counterpart to pooled comparisons and is a direct review aid for outlier projects. |
| `project_fragmentation_diagnostic.csv` | Not consumed. | Add optional `--project-fragmentation-diagnostic`; add a **Project Fragmentation / Portfolio Shape** section before client analysis and enrich high-fragmentation tier notes. | It explicitly captures divergence between project footprint overlap and file-pair identity overlap. |
| `matrix_output_manifest.csv` | Consumed only as descriptive bullets. | Keep as the availability/metadata source for any matrix section; use it to disclose blocked/unavailable matrices and metric limitations. | It documents source grain, interpretation, and limitations for matrix-derived claims. |

## Suggested implementation sequence

1. **Completeness first:** add `--comparison-registry` and an input completeness
   section before making additional governance claims from missing rows.
2. **Cheap compact reuse:** add `--reuse-summary-by-domain` and
   `--reuse-summary-by-client`; prefer these compact summaries over scanning the
   full distribution when supplied.
3. **Domain confidence enrichment:** add union-inventory-derived breadth columns to
   `governance_domain_summary.csv`, then render only the strongest narrative
   exceptions: broad natural reuse but weak formal cascade, or narrow reuse but
   strong cascade.
4. **Project/portfolio matrix section:** consume the four project matrix CSVs and
   `project_fragmentation_diagnostic.csv` behind optional flags. Keep this section
   separate from the existing domain tiers because these matrices answer
   portfolio-shape questions, not domain-standard approval questions.
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
