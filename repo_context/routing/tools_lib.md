# Routing catalog: `tools/lib`

- Generated (UTC): 2026-08-22T17:32:12Z
- Tool version: 0.1.0
- Files covered (this page): 4
- Catalog source hash (sha256 of sorted `path:sha256` pairs for the full `tools/lib` partition): `63bcd849dab91a65e3fc7ce2ed15ef8fcfec13bfc567b2c92850e61c18d04338`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tools/lib/diff_engine.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: diff engine
- Important symbols (16 total):
  - `normalize_name` (function) — line 72
  - `ensure_str` (function) — line 76
  - `load_json` (function) — line 82
  - `get_domain_payload` (function) — line 87
  - `get_label_and_quality` (function) — line 98
  - `get_items` (function) — line 112
  - `extract_records` (function) — line 132
  - `build_index` (function) — line 157
  - `parse_name_map` (function) — line 187
  - `index_items_by_key` (function) — line 203
  - `compare_entries` (function) — line 210
  - `write_csv` (function) — line 277
  - `_pair_name` (function) — line 286
  - `rebuild_unmatched` (function) — line 298
  - `_validate_paths` (function) — line 328
  - `run_comparison` (function) — line 345
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `tools/lib/domain_profile.py`
- Called by (high/medium-confidence static callers):
  - `build_index (tools/lib/diff_engine.py:161)`
  - `build_index (tools/lib/diff_engine.py:162)`
  - `build_index (tools/lib/diff_engine.py:168)`
  - `build_index (tools/lib/diff_engine.py:170)`
  - `build_index (tools/lib/diff_engine.py:171)`
  - `build_index (tools/lib/diff_engine.py:172)`
  - `build_index (tools/lib/diff_engine.py:173)`
  - `compare_entries (tools/lib/diff_engine.py:211)`
  - `compare_entries (tools/lib/diff_engine.py:212)`
  - `extract_records (tools/lib/diff_engine.py:138)`
  - `extract_records (tools/lib/diff_engine.py:151)`
  - `get_items (tools/lib/diff_engine.py:123)`
  - `get_items (tools/lib/diff_engine.py:128)`
  - `get_label_and_quality (tools/lib/diff_engine.py:103)`
  - `get_label_and_quality (tools/lib/diff_engine.py:104)`
  - ... and 24 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`fd27fd846511bfc8…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/lib/diff_engine.py`)

### `tools/lib/domain_profile.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: domain profile
- Important symbols (2 total):
  - `ResolutionSpec` (class) — line 6
  - `DomainProfile` (class) — line 17
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `<module> (tools/lib/vt_profile.py:21)`
  - `<module> (tools/lib/vt_profile.py:26)`
  - `DomainProfile._build_maps_for_file (tools/lib/domain_profile.py:41)`
  - `DomainProfile._build_maps_for_file (tools/lib/domain_profile.py:53)`
  - `DomainProfile._classify_sig_basis (tools/lib/domain_profile.py:125)`
  - `DomainProfile.build_resolution_maps (tools/lib/domain_profile.py:33)`
  - `DomainProfile.classify_bucket (tools/lib/domain_profile.py:111)`
  - `DomainProfile.classify_bucket (tools/lib/domain_profile.py:113)`
  - `DomainProfile.resolve_value (tools/lib/domain_profile.py:88)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`fead578df1b29bdd…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/lib/domain_profile.py`)

### `tools/lib/vt_profile.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: vt profile
- Important symbols (19 total):
  - `_get_domain_payload` (function) — line 73
  - `_load_domain_records` (function) — line 85
  - `_load_vco_records` (function) — line 96
  - `_get_phase2_cosmetic_value` (function) — line 103
  - `_get_identity_item_value` (function) — line 111
  - `_index_vco_by_template` (function) — line 119
  - `_normalize_template_name` (function) — line 146
  - `_build_template_lookup` (function) — line 150
  - `_get_template_vco` (function) — line 160
  - `_index_object_styles_by_row_key` (function) — line 167
  - `_extract_graphic_fields` (function) — line 184
  - `_extract_object_style_baseline_fields` (function) — line 214
  - `_is_non_ok_quality` (function) — line 230
  - `_is_default_vco_value` (function) — line 242
  - `_extract_active_vco_fields` (function) — line 265
  - `_reconstruct_effective` (function) — line 275
  - `_build_synthetic_items_for_pair` (function) — line 296
  - `ViewTemplateDomainProfile` (class) — line 347
  - `make_vt_profile` (function) — line 460
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `tools/lib/domain_profile.py`
- Called by (high/medium-confidence static callers):
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:399)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:400)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:405)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:406)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:411)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:412)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:413)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:414)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:420)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:421)`
  - `ViewTemplateDomainProfile.reconstruct (tools/lib/vt_profile.py:426)`
  - `_build_synthetic_items_for_pair (tools/lib/vt_profile.py:322)`
  - `_build_synthetic_items_for_pair (tools/lib/vt_profile.py:323)`
  - `_build_template_lookup (tools/lib/vt_profile.py:154)`
  - `_extract_active_vco_fields (tools/lib/vt_profile.py:267)`
  - ... and 13 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`a23692499ea27eff…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/lib/vt_profile.py`)

## Other files (non-Python / boilerplate)

| Path | Title/summary | Role |
|---|---|---|
| `tools/lib/__init__.py` | init | `developer_utility` |

