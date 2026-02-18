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

## Notes

- Scope is `CORPUS` only in v2.1.
- Missing `join_hash` records are preserved with blank `pattern_id` in membership and unknown-rate diagnostics.
- Threshold defaults are emitted as notes in `analysis_manifest.csv`.
- Rows are deterministically sorted before write.
