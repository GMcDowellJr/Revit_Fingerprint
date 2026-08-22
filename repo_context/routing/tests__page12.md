# Routing catalog: `tests (page 12)`

- Generated (UTC): 2026-08-22T11:28:23Z
- Tool version: 0.1.0
- Files covered (this page): 7
- Catalog source hash (sha256 of sorted `path:sha256` pairs for the full `tests` partition): `ddcd11e2891dd4b8bd511ebecb705b7c31329d4844340bd6d528c0b1373a5252`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tests/test_v21_join_policy_compat.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test v21 join policy compat
- Important symbols (7 total):
  - `_write_csv` (function) — line 10
  - `test_flat_required_fields_backward_compatible` (function) — line 19
  - `test_required_items_alias_and_shape_gating` (function) — line 36
  - `test_apply_diagnostics_include_discriminator_context` (function) — line 73
  - `test_optional_items_not_required_or_selected_by_default` (function) — line 139
  - `test_discover_emits_legacy_compat_shape_and_lists` (function) — line 150
  - `test_validate_pareto_auto_bumps_max_k_for_required_items` (function) — line 221
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `tools/join_key_discovery/eval.py`
- Called by (high/medium-confidence static callers):
  - `test_apply_diagnostics_include_discriminator_context (tests/test_v21_join_policy_compat.py:79)`
  - `test_apply_diagnostics_include_discriminator_context (tests/test_v21_join_policy_compat.py:87)`
  - `test_discover_emits_legacy_compat_shape_and_lists (tests/test_v21_join_policy_compat.py:157)`
  - `test_discover_emits_legacy_compat_shape_and_lists (tests/test_v21_join_policy_compat.py:162)`
  - `test_validate_pareto_auto_bumps_max_k_for_required_items (tests/test_v21_join_policy_compat.py:227)`
  - `test_validate_pareto_auto_bumps_max_k_for_required_items (tests/test_v21_join_policy_compat.py:232)`
- Related tests:
  - `tests/test_v21_join_policy_compat.py`
- Retrieval identity: sha256=`e6099462f9dd6cef…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_v21_join_policy_compat.py`)

### `tests/test_view_category_overrides_canonical_selectors.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test view category overrides canonical selectors
- Important symbols (1 total):
  - `test_view_category_overrides_join_and_sig_selectors_are_distinct` (function) — line 9
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `core/hashing.py`
  - imports `core/join_key_builder.py`
  - imports `core/join_key_policy.py`
  - imports `core/record_v2.py`
- Called by (high/medium-confidence static callers):
  - (none resolved statically; see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`b40ce9d78db2a890…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_view_category_overrides_canonical_selectors.py`)

### `tests/test_view_filter_applications_view_templates_canonical_selectors.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test view filter applications view templates canonical selectors
- Important symbols (2 total):
  - `_policy` (function) — line 28
  - `test_view_filter_applications_view_templates_uses_canonical_selectors_for_join_and_sig` (function) — line 33
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `core/hashing.py`
  - imports `core/join_key_builder.py`
  - imports `core/join_key_policy.py`
  - imports `core/record_v2.py`
  - imports `domains/view_filter_applications_view_templates.py`
- Called by (high/medium-confidence static callers):
  - `test_view_filter_applications_view_templates_uses_canonical_selectors_for_join_and_sig (tests/test_view_filter_applications_view_templates_canonical_selectors.py:44)`
- Related tests:
  - `tests/test_view_filter_applications_view_templates_canonical_selectors.py`
- Retrieval identity: sha256=`0ec6e5a5074734e1…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_view_filter_applications_view_templates_canonical_selectors.py`)

### `tests/test_view_filter_definitions_canonical_selectors.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test view filter definitions canonical selectors
- Important symbols (4 total):
  - `_install_fake_revit_db` (function) — line 13
  - `test_view_filter_definitions_join_hash_uses_policy_required_keys_only` (function) — line 36
  - `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges` (function) — line 73
  - `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list` (function) — line 135
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `core/hashing.py`
  - imports `core/join_key_builder.py`
  - imports `core/join_key_policy.py`
  - imports `core/record_v2.py`
- Called by (high/medium-confidence static callers):
  - `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges (tests/test_view_filter_definitions_canonical_selectors.py:74)`
  - `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list (tests/test_view_filter_definitions_canonical_selectors.py:136)`
- Related tests:
  - `tests/test_view_filter_definitions_canonical_selectors.py`
- Retrieval identity: sha256=`abd395de50d47ff5…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_view_filter_definitions_canonical_selectors.py`)

### `tests/test_view_filter_definitions_empty_domain.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test view filter definitions empty domain
- Important symbols (2 total):
  - `_install_fake_revit_db` (function) — line 8
  - `test_view_filter_definitions_empty_collection_is_not_blocked` (function) — line 31
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `core/hashing.py`
- Called by (high/medium-confidence static callers):
  - `test_view_filter_definitions_empty_collection_is_not_blocked (tests/test_view_filter_definitions_empty_domain.py:32)`
- Related tests:
  - `tests/test_view_filter_definitions_empty_domain.py`
- Retrieval identity: sha256=`3947f092bdf5b7b5…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_view_filter_definitions_empty_domain.py`)

### `tests/test_view_instances_cache_key_consistency.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test view instances cache key consistency
- Important symbols (1 total):
  - `test_view_instances_cache_key_consistency` (function) — line 6
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `domains/view_templates.py`
- Called by (high/medium-confidence static callers):
  - (none resolved statically; see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`62fe03c764676437…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_view_instances_cache_key_consistency.py`)

### `tests/test_view_templates_canonical_selectors.py`
- Role: `test_harness` (evidence: file classified as 'test' (test-path/filename convention, see classify_file))
- Purpose clues:
  - filename/path terms: test view templates canonical selectors
- Important symbols (11 total):
  - `_load_policy` (function) — line 8
  - `test_view_templates_all_split_domains_have_schemas` (function) — line 22
  - `test_view_templates_all_split_domains_require_def_hash` (function) — line 36
  - `test_view_templates_floor_policy` (function) — line 45
  - `test_view_templates_ceiling_policy` (function) — line 51
  - `test_view_templates_elevations_policy` (function) — line 57
  - `test_view_templates_renderings_policy` (function) — line 63
  - `test_view_templates_schedules_policy` (function) — line 69
  - `test_view_templates_name_uid_excluded` (function) — line 75
  - `test_view_templates_join_key_build_with_def_hash` (function) — line 86
  - `test_view_templates_join_key_missing_def_hash` (function) — line 108
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `core/join_key_builder.py`
  - imports `core/join_key_policy.py`
  - imports `core/record_v2.py`
- Called by (high/medium-confidence static callers):
  - `test_view_templates_all_split_domains_have_schemas (tests/test_view_templates_canonical_selectors.py:25)`
  - `test_view_templates_all_split_domains_require_def_hash (tests/test_view_templates_canonical_selectors.py:39)`
  - `test_view_templates_ceiling_policy (tests/test_view_templates_canonical_selectors.py:53)`
  - `test_view_templates_elevations_policy (tests/test_view_templates_canonical_selectors.py:59)`
  - `test_view_templates_floor_policy (tests/test_view_templates_canonical_selectors.py:47)`
  - `test_view_templates_join_key_build_with_def_hash (tests/test_view_templates_canonical_selectors.py:88)`
  - `test_view_templates_join_key_missing_def_hash (tests/test_view_templates_canonical_selectors.py:110)`
  - `test_view_templates_name_uid_excluded (tests/test_view_templates_canonical_selectors.py:79)`
  - `test_view_templates_renderings_policy (tests/test_view_templates_canonical_selectors.py:65)`
  - `test_view_templates_schedules_policy (tests/test_view_templates_canonical_selectors.py:71)`
- Related tests:
  - `tests/test_view_templates_canonical_selectors.py`
- Retrieval identity: sha256=`e4c97c9d83f953b9…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tests/test_view_templates_canonical_selectors.py`)

