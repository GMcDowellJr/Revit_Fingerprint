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
- `exported_utc` is the conversion execution timestamp (UTC ISO-8601).
- `tool_version` resolution order:
  1. `FINGERPRINT_TOOL_VERSION`
  2. `0.0.0+<gitsha>`
  3. `0.0.0+nogit`
- Rows are deterministically sorted before write.
