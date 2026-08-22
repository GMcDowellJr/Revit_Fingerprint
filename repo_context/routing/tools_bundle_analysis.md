# Routing catalog: `tools/bundle_analysis`

- Generated (UTC): 2026-08-22T09:43:57Z
- Tool version: 0.1.0
- Files covered: 19
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `f736b277d9e1f6fd82b4fe97f9be754014de1e4e507b028c1e996c87ecd81442`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `tools/bundle_analysis/__init__.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - module docstring: Bundle analysis post-process pipeline.
  - filename/path terms: init
- Important symbols (0 total):
  - (none)
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - (none resolved statically; see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`1101fa2b0d208f18…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/__init__.py`)

### `tools/bundle_analysis/common.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: common
- Important symbols (7 total):
  - `retry_fs_op` (function) — line 17
  - `read_csv_rows` (function) — line 42
  - `atomic_write_csv` (function) — line 47
  - `resolve_analysis_run_id` (function) — line 58
  - `derive_scope_key` (function) — line 67
  - `compute_effective_support` (function) — line 76
  - `make_bundle_id` (function) — line 81
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `TestAnnotateNameTargetCombinedFiles.test_adds_three_columns_after_existing_header_and_looks_up_coverage_class (tests/test_bundle_analysis_name_projection.py:658)`
  - `TestAnnotateNameTargetCombinedFiles.test_excluded_domain_row_still_annotated_with_its_own_coverage_class (tests/test_bundle_analysis_name_projection.py:707)`
  - `TestBundleProvenance.test_every_bundle_declares_comparison_target_and_coverage_class (tests/test_bundle_analysis_name_projection.py:397)`
  - `TestBundleProvenance.test_excluded_domains_stated_explicitly_in_readme_and_coverage_csv (tests/test_bundle_analysis_name_projection.py:410)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle (tests/test_bundle_analysis_name_projection.py:358)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle (tests/test_bundle_analysis_name_projection.py:366)`
  - `TestRetryFsOp.test_non_os_error_is_not_retried (tests/test_bundle_analysis_name_projection.py:765)`
  - `TestRetryFsOp.test_passes_through_positional_args (tests/test_bundle_analysis_name_projection.py:754)`
  - `TestRetryFsOp.test_recovers_after_transient_failures (tests/test_bundle_analysis_name_projection.py:741)`
  - `TestRetryFsOp.test_reraises_after_exhausting_attempts (tests/test_bundle_analysis_name_projection.py:749)`
  - `TestRetryFsOp.test_succeeds_on_first_try_without_retry (tests/test_bundle_analysis_name_projection.py:729)`
  - `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile.test_metadata_file_resolves_details_only_export_correctly (tests/test_bundle_analysis_name_projection.py:460)`
  - `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile.test_without_metadata_file_still_falls_back_to_blind_normalize (tests/test_bundle_analysis_name_projection.py:483)`
  - `TestSplitExportFileIdNormalization.test_staged_presence_rows_use_index_export_run_id_for_split_export (tests/test_bundle_analysis_name_projection.py:272)`
  - `TestStageWithKnownExportRunIds.test_details_only_export_stages_with_raw_id_when_known (tests/test_bundle_analysis_name_projection.py:324)`
  - ... and 104 more (see python_calls.csv)
- Related tests:
  - `tests/test_bundle_analysis_name_projection.py`
- Retrieval identity: sha256=`bb58452921525204…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/common.py`)

### `tools/bundle_analysis/name_projection_adapter.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: name projection adapter
- Important symbols (4 total):
  - `normalize_export_run_id` (function) — line 55
  - `stage_name_projection_analysis_dir` (function) — line 103
  - `emit_name_target_provenance` (function) — line 220
  - `annotate_name_target_combined_files` (function) — line 317
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - imports `core/name_key_coverage.py`
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `TestAnnotateNameTargetCombinedFiles.test_adds_three_columns_after_existing_header_and_looks_up_coverage_class (tests/test_bundle_analysis_name_projection.py:655)`
  - `TestAnnotateNameTargetCombinedFiles.test_excluded_domain_row_still_annotated_with_its_own_coverage_class (tests/test_bundle_analysis_name_projection.py:706)`
  - `TestAnnotateNameTargetCombinedFiles.test_idempotent_second_call_leaves_already_annotated_file_unchanged (tests/test_bundle_analysis_name_projection.py:690)`
  - `TestAnnotateNameTargetCombinedFiles.test_idempotent_second_call_leaves_already_annotated_file_unchanged (tests/test_bundle_analysis_name_projection.py:694)`
  - `TestAnnotateNameTargetCombinedFiles.test_missing_files_are_skipped_without_error (tests/test_bundle_analysis_name_projection.py:680)`
  - `TestBundleProvenance._run_pipeline (tests/test_bundle_analysis_name_projection.py:386)`
  - `TestBundleProvenance.test_determinism_of_provenance_output (tests/test_bundle_analysis_name_projection.py:421)`
  - `TestBundleProvenance.test_determinism_of_provenance_output (tests/test_bundle_analysis_name_projection.py:423)`
  - `TestBundleProvenance.test_every_bundle_declares_comparison_target_and_coverage_class (tests/test_bundle_analysis_name_projection.py:394)`
  - `TestBundleProvenance.test_excluded_domains_stated_explicitly_in_readme_and_coverage_csv (tests/test_bundle_analysis_name_projection.py:408)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle (tests/test_bundle_analysis_name_projection.py:351)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staging_is_deterministic (tests/test_bundle_analysis_name_projection.py:375)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staging_is_deterministic (tests/test_bundle_analysis_name_projection.py:376)`
  - `TestNormalizeExportRunIdWithKnownIds.test_details_only_export_resolves_to_raw_form (tests/test_bundle_analysis_name_projection.py:290)`
  - `TestNormalizeExportRunIdWithKnownIds.test_index_and_plain_names_unaffected_by_known_ids (tests/test_bundle_analysis_name_projection.py:302)`
  - ... and 14 more (see python_calls.csv)
- Related tests:
  - `tests/test_bundle_analysis_name_projection.py`
- Retrieval identity: sha256=`89416e984be6d676…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/name_projection_adapter.py`)

### `tools/bundle_analysis/placeholder_exclusions.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: placeholder exclusions
- Important symbols (8 total):
  - `t` (function) — line 19
  - `lg` (function) — line 21
  - `_to_int` (function) — line 31
  - `_load_governance_roles` (function) — line 39
  - `_load_existing_overrides` (function) — line 50
  - `_choose_threshold` (function) — line 64
  - `compute_placeholder_exclusions` (function) — line 80
  - `main` (function) — line 92
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
  - imports `tools/bundle_analysis/placeholder_exclusions_legacy.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/placeholder_exclusions.py:229)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:107)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:108)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:127)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:128)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:136)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:145)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:146)`
  - `main (tools/bundle_analysis/placeholder_exclusions.py:199)`
  - `run_bundle_analysis (tools/bundle_analysis/run_bundle_analysis.py:657)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`1a0dabb09d025ec5…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/placeholder_exclusions.py`)

### `tools/bundle_analysis/placeholder_exclusions_legacy.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: placeholder exclusions legacy
- Important symbols (5 total):
  - `_is_truthy` (function) — line 24
  - `_largest_gap_threshold` (function) — line 28
  - `compute_placeholder_exclusions` (function) — line 46
  - `_parse_args` (function) — line 110
  - `main` (function) — line 117
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/placeholder_exclusions_legacy.py:124)`
  - `compute_placeholder_exclusions (tools/bundle_analysis/placeholder_exclusions.py:90)`
  - `compute_placeholder_exclusions (tools/bundle_analysis/placeholder_exclusions_legacy.py:60)`
  - `compute_placeholder_exclusions (tools/bundle_analysis/placeholder_exclusions_legacy.py:77)`
  - `main (tools/bundle_analysis/placeholder_exclusions_legacy.py:118)`
  - `main (tools/bundle_analysis/placeholder_exclusions_legacy.py:119)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`d0d72a81804d060e…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/placeholder_exclusions_legacy.py`)

### `tools/bundle_analysis/reference_bundle.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'tools/')
- Purpose clues:
  - filename/path terms: reference bundle
- Important symbols (3 total):
  - `_escape_control_chars_in_json_strings` (function) — line 10
  - `write_sidecar` (function) — line 60
  - `load_and_validate` (function) — line 103
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `load_and_validate (tools/bundle_analysis/reference_bundle.py:118)`
  - `run_bundle_analysis (tools/bundle_analysis/run_bundle_analysis.py:424)`
  - `test_load_and_validate_allows_legacy_control_characters (tests/test_reference_bundle.py:21)`
  - `test_load_and_validate_allows_raw_newline_in_string (tests/test_reference_bundle.py:42)`
- Related tests:
  - `tests/test_reference_bundle.py`
- Retrieval identity: sha256=`821f94ad73e1fadd…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/reference_bundle.py`)

### `tools/bundle_analysis/run_bundle_analysis.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: run bundle analysis
- Important symbols (11 total):
  - `_view_out_dir` (function) — line 67
  - `_ensure_latent_purgeable` (function) — line 72
  - `_emit_meta_scatter_thresholds` (function) — line 94
  - `_load_purgeable_only_set` (function) — line 153
  - `_run_pipeline_once` (function) — line 180
  - `_run_step2_to_step7` (function) — line 288
  - `run_bundle_analysis` (function) — line 354
  - `_validate_name_target_constraints` (function) — line 900
  - `run_bundle_analysis_for_target` (function) — line 936
  - `_parse_args` (function) — line 1129
  - `main` (function) — line 1174
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
  - imports `tools/bundle_analysis/name_projection_adapter.py`
  - imports `tools/bundle_analysis/placeholder_exclusions.py`
  - imports `tools/bundle_analysis/reference_bundle.py`
  - imports `tools/bundle_analysis/step0_discover_populations.py`
  - imports `tools/bundle_analysis/step1_membership_matrix.py`
  - imports `tools/bundle_analysis/step2_find_bundles.py`
  - imports `tools/bundle_analysis/step2b_bundle_share_profile.py`
  - imports `tools/bundle_analysis/step3_build_dag.py`
  - imports `tools/bundle_analysis/step4_difference_sets.py`
  - imports `tools/bundle_analysis/step5_classify_patterns.py`
  - imports `tools/bundle_analysis/step6_classify_files.py`
  - imports `tools/bundle_analysis/step7_overlap_report.py`
  - imports `tools/bundle_analysis/step_compare.py`
  - imports `tools/jenks_utils.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/run_bundle_analysis.py:1202)`
  - `TestConfigPassthroughUnchanged.test_both_target_nests_config_output_under_config_subdir (tests/test_bundle_analysis_name_projection.py:156)`
  - `TestConfigPassthroughUnchanged.test_config_target_calls_run_bundle_analysis_with_unchanged_out_dir (tests/test_bundle_analysis_name_projection.py:127)`
  - `TestNameAllOutputLocation._run (tests/test_bundle_analysis_name_projection.py:497)`
  - `TestNameAllOutputLocation.test_config_target_output_untouched_by_relocation (tests/test_bundle_analysis_name_projection.py:557)`
  - `TestNameAllOutputLocation.test_rerun_against_same_out_dir_self_clears_stale_name_all (tests/test_bundle_analysis_name_projection.py:531)`
  - `TestNameAllOutputLocation.test_rerun_against_same_out_dir_self_clears_stale_name_all (tests/test_bundle_analysis_name_projection.py:542)`
  - `TestPurgeViewDefaultIsTargetAware.test_both_target_without_explicit_purge_view_does_not_raise (tests/test_bundle_analysis_name_projection.py:204)`
  - `TestPurgeViewDefaultIsTargetAware.test_cli_purge_view_default_is_none (tests/test_bundle_analysis_name_projection.py:230)`
  - `TestPurgeViewDefaultIsTargetAware.test_config_target_without_explicit_purge_view_still_defaults_to_both (tests/test_bundle_analysis_name_projection.py:221)`
  - `TestPurgeViewDefaultIsTargetAware.test_name_target_with_explicit_used_still_raises (tests/test_bundle_analysis_name_projection.py:214)`
  - `TestPurgeViewDefaultIsTargetAware.test_name_target_without_explicit_purge_view_does_not_raise (tests/test_bundle_analysis_name_projection.py:196)`
  - `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile.test_metadata_file_resolves_details_only_export_correctly (tests/test_bundle_analysis_name_projection.py:451)`
  - `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile.test_without_metadata_file_still_falls_back_to_blind_normalize (tests/test_bundle_analysis_name_projection.py:475)`
  - `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure.test_stale_name_all_removed_even_when_mining_raises (tests/test_bundle_analysis_name_projection.py:589)`
  - ... and 21 more (see python_calls.csv)
- Related tests:
  - `tests/test_bundle_analysis_name_projection.py`
- Retrieval identity: sha256=`f78aca08e1415706…`, chunked=yes (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/run_bundle_analysis.py`)

### `tools/bundle_analysis/step0_discover_populations.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step0 discover populations
- Important symbols (7 total):
  - `_pattern_summary` (function) — line 40
  - `_population_id` (function) — line 44
  - `_select_populations` (function) — line 53
  - `_collapse_subset_related_roots` (function) — line 79
  - `discover_populations` (function) — line 121
  - `_parse_args` (function) — line 460
  - `main` (function) — line 474
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
  - imports `tools/bundle_analysis/step2_find_bundles.py`
  - imports `tools/bundle_analysis/utils.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step0_discover_populations.py:491)`
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:213)`
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:246)`
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:248)`
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:276)`
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:297)`
  - `main (tools/bundle_analysis/step0_discover_populations.py:475)`
  - `main (tools/bundle_analysis/step0_discover_populations.py:476)`
  - `run_bundle_analysis (tools/bundle_analysis/run_bundle_analysis.py:679)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`36606c3524fed85c…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step0_discover_populations.py`)

### `tools/bundle_analysis/step1_membership_matrix.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step1 membership matrix
- Important symbols (4 total):
  - `_load_population_file_ids` (function) — line 18
  - `build_membership_matrix` (function) — line 48
  - `_parse_args` (function) — line 236
  - `main` (function) — line 249
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step1_membership_matrix.py:267)`
  - `TestBundleProvenance._run_pipeline (tests/test_bundle_analysis_name_projection.py:388)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle (tests/test_bundle_analysis_name_projection.py:363)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:202)`
  - `build_membership_matrix (tools/bundle_analysis/step1_membership_matrix.py:73)`
  - `main (tools/bundle_analysis/step1_membership_matrix.py:250)`
  - `main (tools/bundle_analysis/step1_membership_matrix.py:251)`
  - `run_bundle_analysis (tools/bundle_analysis/run_bundle_analysis.py:518)`
- Related tests:
  - `tests/test_bundle_analysis_name_projection.py`
- Retrieval identity: sha256=`947ab4a6fc33d1e3…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step1_membership_matrix.py`)

### `tools/bundle_analysis/step2_find_bundles.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step2 find bundles
- Important symbols (5 total):
  - `_percentile` (function) — line 33
  - `compute_auto_threshold` (function) — line 50
  - `find_bundles_for_domain` (function) — line 118
  - `_parse_args` (function) — line 378
  - `main` (function) — line 387
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
  - imports `tools/bundle_analysis/utils.py`
  - imports `tools/jenks_utils.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step2_find_bundles.py:394)`
  - `TestBundleProvenance._run_pipeline (tests/test_bundle_analysis_name_projection.py:389)`
  - `TestNameProjectionAdapterProducesConsumableInput.test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle (tests/test_bundle_analysis_name_projection.py:364)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:219)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:302)`
  - `compute_auto_threshold (tools/bundle_analysis/step2_find_bundles.py:77)`
  - `discover_populations (tools/bundle_analysis/step0_discover_populations.py:260)`
  - `find_bundles_for_domain (tools/bundle_analysis/step2_find_bundles.py:151)`
  - `main (tools/bundle_analysis/step2_find_bundles.py:388)`
  - `main (tools/bundle_analysis/step2_find_bundles.py:389)`
- Related tests:
  - `tests/test_bundle_analysis_name_projection.py`
- Retrieval identity: sha256=`4ada0e721a8d51d0…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step2_find_bundles.py`)

### `tools/bundle_analysis/step3_build_dag.py`
- Role: `active_pipeline` (evidence: contains `if __name__ == "__main__":` guard; no operator-facing directory or docstring hint matched, conservatively treated as an active pipeline stage rather than an operator entrypoint)
- Purpose clues:
  - filename/path terms: step3 build dag
- Important symbols (3 total):
  - `build_dag_for_domain` (function) — line 18
  - `_parse_args` (function) — line 228
  - `main` (function) — line 235
- Entrypoint evidence: contains `if __name__ == "__main__":` guard
- Internal dependencies (resolved imports within this repository):
  - imports `tools/bundle_analysis/common.py`
- Called by (high/medium-confidence static callers):
  - `<module> (tools/bundle_analysis/step3_build_dag.py:242)`
  - `_run_pipeline_once (tools/bundle_analysis/run_bundle_analysis.py:238)`
  - `_run_step2_to_step7 (tools/bundle_analysis/run_bundle_analysis.py:320)`
  - `main (tools/bundle_analysis/step3_build_dag.py:236)`
  - `main (tools/bundle_analysis/step3_build_dag.py:237)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`b0e54a7ae5de0558…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `tools/bundle_analysis/step3_build_dag.py`)

## Other files (non-Python)

| Path | Title/summary | Role |
|---|---|---|
| `tools/bundle_analysis/README.md` | Bundle Analysis Pipeline | `unknown` |

## Omitted from this catalog (size limit reached)

7 file(s) omitted; see `file_inventory.csv` / `routing/routing_manifest.json` for the complete list.

- `tools/bundle_analysis/step2b_bundle_share_profile.py`
- `tools/bundle_analysis/step4_difference_sets.py`

