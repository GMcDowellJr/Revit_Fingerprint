# V2.1 Phase 0 Export Schema

Outputs are additive and written under `Results_v21/phase0_v21/`.

## Files

- `file_metadata.csv`
- `phase0_records.csv`
- `phase0_identity_items.csv`
- `phase0_label_components.csv`
- `phase0_status_reasons.csv`

## Notes

- `export_run_id` currently uses file-id basename mode.
- `schema_version` in every v2.1 CSV is the CSV schema version string: `2.1`.
- `exported_utc` is the conversion execution timestamp (UTC ISO-8601).
- `tool_version` resolution order:
  1. `FINGERPRINT_TOOL_VERSION`
  2. `0.0.0+<gitsha>`
  3. `0.0.0+nogit`
- Rows are deterministically sorted before write:
  - `file_metadata.csv`: `(export_run_id)`
  - `phase0_records.csv`: `(export_run_id, domain, record_pk)`
  - `phase0_identity_items.csv`: `(export_run_id, domain, record_pk, item_key, item_value)`
  - `phase0_label_components.csv`: `(export_run_id, domain, record_pk, component_order, component_key)`
  - `phase0_status_reasons.csv`: `(export_run_id, domain, record_pk, reason_code)`

## `phase0_records.csv` columns

`schema_version, export_run_id, file_id, domain, record_pk, record_id, record_ordinal, status, identity_quality, sig_hash, join_hash, join_key_schema, join_key_status, join_key_policy_id, join_key_policy_version, label_display, label_quality, label_provenance, is_purgeable, instance_count, is_sole_type_in_category`
