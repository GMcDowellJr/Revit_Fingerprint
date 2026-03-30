# V2.1 Analysis Schema

Outputs are additive and written under `Results_v21/analysis_v21/`.

## Files

- `analysis_manifest.csv`
- `analysis_export_membership.csv`
- `phase1_domain_metrics.csv`
- `domain_patterns.csv`
- `record_pattern_membership.csv`
- `phase2_authority_pattern.csv`
- `pattern_presence_file.csv`
- `domain_pattern_diagnostics.csv`
- `element_dominance.csv`

## Notes

- Scope is `CORPUS` only in v2.1.
- `schema_version` in every v2.1 CSV is the CSV schema version string: `2.1`.
- Pattern construction source-of-truth is Phase0 record-level `join_hash` + `join_key_schema` from fingerprint export JSON.
- Missing `join_hash` records are preserved with blank `pattern_id` in membership and unknown-rate diagnostics (no synthetic patterns).
- Domains with records but 100% missing `join_hash` still emit `pattern_presence_file.csv` UNKNOWN rows and `domain_pattern_diagnostics.csv` rows.
- `analysis_scope_hash` is SHA-1 of the sorted `export_run_id` list joined by `|`.
- Threshold outputs are placeholders/defaults; DAX alignment is deferred and defaults are logged in `analysis_manifest.notes`.
- `MIN_FILES_FOR_DOMAIN` evidence checks use files where the specific domain is present (not corpus-wide export count).
- `element_dominance.csv` is a post-emit artifact generated after `domain_patterns.csv` and `phase2_authority_pattern.csv`.
- `element_dominance.csv` scope is row_key domains only: `object_styles_model`, `object_styles_annotation`, `view_category_overrides`.
- `element_dominance.csv` grain is `(domain, element_label, sub_label)` where each row corresponds to a distinct `pattern_label_human` within row_key domains.
- Deterministic sorting is explicit per output key:
  - `analysis_export_membership.csv`: `(analysis_run_id, export_run_id)`
  - `phase1_domain_metrics.csv`: `(domain, join_key_schema, join_hash)`
  - `domain_patterns.csv`: `(analysis_run_id, domain, pattern_id)`
  - `record_pattern_membership.csv`: `(analysis_run_id, export_run_id, domain, record_pk)`
  - `phase2_authority_pattern.csv`: `(analysis_run_id, domain, pattern_id)`
  - `pattern_presence_file.csv`: `(analysis_run_id, export_run_id, domain, pattern_id)`
  - `domain_pattern_diagnostics.csv`: `(analysis_run_id, domain)`
  - `element_dominance.csv`: `(domain, element_label, sub_label)`
