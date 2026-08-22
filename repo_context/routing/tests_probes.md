# Routing catalog: `tests/probes`

- Generated (UTC): 2026-08-22T04:24:28Z
- Tool version: 0.1.0
- Files covered: 1
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `f6fbdaf3c6533dd6a106bb8b7bcdd056c8430ef0abeb21b6176012d175a42eed`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tools/probes/test_probe_inventory_builder.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test probe inventory builder
- Important symbols (10 total):
  - `_run` (function) — line 10
  - `_read_csv_rows` (function) — line 32
  - `test_merges_and_dedupes_across_dated_runs` (function) — line 37
  - `test_empty_probes_dir_refuses_to_overwrite_by_default` (function) — line 143
  - `test_all_inputs_invalid_refuses_to_overwrite_by_default` (function) — line 162
  - `test_empty_probes_dir_with_force_writes_empty_inventory` (function) — line 186
  - `_run_shaped_payload` (function) — line 197
  - `test_merges_run_shaped_files_and_tracks_revit_version` (function) — line 212
  - `test_merges_across_legacy_and_run_shapes_for_same_domain` (function) — line 275
  - `test_crosswalk_column_profile_across_runs` (function) — line 328
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `test_all_inputs_invalid_refuses_to_overwrite_by_default (tools/probes/test_probe_inventory_builder.py:177)`
  - `test_all_inputs_invalid_refuses_to_overwrite_by_default (tools/probes/test_probe_inventory_builder.py:182)`
  - `test_crosswalk_column_profile_across_runs (tools/probes/test_probe_inventory_builder.py:349)`
  - `test_crosswalk_column_profile_across_runs (tools/probes/test_probe_inventory_builder.py:367)`
  - `test_crosswalk_column_profile_across_runs (tools/probes/test_probe_inventory_builder.py:388)`
  - `test_crosswalk_column_profile_across_runs (tools/probes/test_probe_inventory_builder.py:393)`
  - `test_crosswalk_column_profile_across_runs (tools/probes/test_probe_inventory_builder.py:401)`
  - `test_empty_probes_dir_refuses_to_overwrite_by_default (tools/probes/test_probe_inventory_builder.py:153)`
  - `test_empty_probes_dir_refuses_to_overwrite_by_default (tools/probes/test_probe_inventory_builder.py:158)`
  - `test_empty_probes_dir_with_force_writes_empty_inventory (tools/probes/test_probe_inventory_builder.py:191)`
  - `test_empty_probes_dir_with_force_writes_empty_inventory (tools/probes/test_probe_inventory_builder.py:192)`
  - `test_merges_across_legacy_and_run_shapes_for_same_domain (tools/probes/test_probe_inventory_builder.py:295)`
  - `test_merges_across_legacy_and_run_shapes_for_same_domain (tools/probes/test_probe_inventory_builder.py:318)`
  - `test_merges_across_legacy_and_run_shapes_for_same_domain (tools/probes/test_probe_inventory_builder.py:322)`
  - `test_merges_and_dedupes_across_dated_runs (tools/probes/test_probe_inventory_builder.py:114)`
  - ... and 5 more (see python_calls.csv)
- Related tests:
  - `tools/probes/test_probe_inventory_builder.py`
- Retrieval identity: sha256=`39e601e7681e1011…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/probes/test_probe_inventory_builder.py`)

