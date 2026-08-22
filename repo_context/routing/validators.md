# Routing catalog: `validators`

- Generated (UTC): 2026-08-22T04:24:28Z
- Tool version: 0.1.0
- Files covered: 1
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `5faa239f02106094aff7b8eef555881dd76944d19f7617181e782602ac80b4d8`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `validators/record_v2.py`
- Role: `library_module` (evidence: no `__main__` guard; located under 'validators/')
- Purpose clues:
  - filename/path terms: record v2
- Important symbols (8 total):
  - `load_json_file` (function) — line 27
  - `validate_record_v2` (function) — line 35
  - `validate_records_v2` (function) — line 265
  - `serialize_identity_items` (function) — line 293
  - `_compute_identity_quality` (function) — line 312
  - `_is_allowed_indexed_key` (function) — line 325
  - `_normalize_indexed_key` (function) — line 339
  - `_hash_preimage` (function) — line 344
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_is_allowed_indexed_key (validators/record_v2.py:335)`
  - `test_all_exported_records_conform_to_record_contract_v2 (tests/test_record_contract_v2.py:49)`
  - `test_extract_units_doc_emits_exactly_one_populated_record (tests/test_units_canonical_selectors.py:121)`
  - `test_object_styles_model_area9_fields_pass_contract_validation_for_subcategory (tests/test_object_styles_canonical_selectors.py:96)`
  - `test_object_styles_model_parent_name_missing_and_none_for_top_level_category (tests/test_object_styles_canonical_selectors.py:118)`
  - `test_validate_records_duplicate_within_file_and_domain (tests/test_record_id_determinism.py:108)`
  - `validate_record_v2 (validators/record_v2.py:213)`
  - `validate_record_v2 (validators/record_v2.py:237)`
  - `validate_record_v2 (validators/record_v2.py:252)`
  - `validate_record_v2 (validators/record_v2.py:254)`
  - `validate_record_v2 (validators/record_v2.py:256)`
  - `validate_records_v2 (validators/record_v2.py:284)`
- Related tests:
  - `tests/test_object_styles_canonical_selectors.py`
  - `tests/test_record_contract_v2.py`
  - `tests/test_record_id_determinism.py`
  - `tests/test_units_canonical_selectors.py`
- Retrieval identity: sha256=`0a3285d32d34de84…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `validators/record_v2.py`)

