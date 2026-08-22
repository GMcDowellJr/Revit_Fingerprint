# Revit_Fingerprint — approximate Python trace map

Name-based static call relationships; this is not a runtime call graph.

## Trace: `dev_tools/repo_context/repo_context.py:main`

- No calls to indexed symbols found.

## Trace: `mapping/create_line_pattern_mappings.py:run`

- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:MappingOutcome`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:build_report_rows`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:get_line_patterns_join_key_policy`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:group_names_by_join_hash`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:group_requested_join_hashes`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:group_settings_by_join_hash`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:load_bundle_pattern_detail_export`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:reconstruct_pattern`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_reconstruction.py:write_report_csv`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_revit_apply.py:build_name_index`
- `mapping/create_line_pattern_mappings.py:run` → `mapping/line_pattern_revit_apply.py:resolve_mapping`
  - `mapping/line_pattern_reconstruction.py:build_report_rows` → `mapping/line_pattern_reconstruction.py:outcome_to_report_row`
  - `mapping/line_pattern_reconstruction.py:get_line_patterns_join_key_policy` → `core/join_key_policy.py:get_domain_join_key_policy`
  - `mapping/line_pattern_reconstruction.py:group_requested_join_hashes` → `mapping/line_pattern_reconstruction.py:SkippedRequest`
  - `mapping/line_pattern_reconstruction.py:reconstruct_pattern` → `mapping/line_pattern_reconstruction.py:ReconstructedPattern`
  - `mapping/line_pattern_reconstruction.py:reconstruct_pattern` → `mapping/line_pattern_reconstruction.py:_blocked`
  - `mapping/line_pattern_reconstruction.py:reconstruct_pattern` → `mapping/line_pattern_reconstruction.py:compute_join_hash_for_segments`
  - `mapping/line_pattern_reconstruction.py:reconstruct_pattern` → `mapping/line_pattern_reconstruction.py:compute_segments_def_hash`
  - `mapping/line_pattern_reconstruction.py:reconstruct_pattern` → `mapping/line_pattern_reconstruction.py:compute_segments_norm_hash`
  - `mapping/line_pattern_revit_apply.py:build_name_index` → `core/collect.py:collect_instances`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_reconstruction.py:MappingOutcome`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_reconstruction.py:build_mapping_name_candidates`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_reconstruction.py:dominant_status`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_reconstruction.py:get_line_patterns_join_key_policy`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_reconstruction.py:resolve_observed_name`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_revit_apply.py:create_and_verify_line_pattern`
  - `mapping/line_pattern_revit_apply.py:resolve_mapping` → `mapping/line_pattern_revit_apply.py:verify_element_join_hash`
    - `mapping/line_pattern_reconstruction.py:_blocked` → `mapping/line_pattern_reconstruction.py:ReconstructedPattern`
    - `mapping/line_pattern_reconstruction.py:compute_join_hash_for_segments` → `core/join_key_builder.py:build_join_key_from_policy`
    - `mapping/line_pattern_reconstruction.py:compute_join_hash_for_segments` → `core/record_v2.py:make_identity_item`
    - `mapping/line_pattern_reconstruction.py:compute_join_hash_for_segments` → `mapping/line_pattern_reconstruction.py:compute_segments_norm_hash`
    - `mapping/line_pattern_reconstruction.py:compute_join_hash_for_segments` → `mapping/line_pattern_reconstruction.py:get_line_patterns_join_key_policy`
    - `mapping/line_pattern_reconstruction.py:compute_segments_def_hash` → `core/record_v2.py:canonicalize_float`
    - `core/collect.py:collect_instances` → `core/collect.py:collect_elements`
    - `mapping/line_pattern_reconstruction.py:build_mapping_name_candidates` → `mapping/line_pattern_reconstruction.py:sanitize_revit_name`
    - `mapping/line_pattern_reconstruction.py:build_mapping_name_candidates` → `mapping/line_pattern_reconstruction.py:short_join_hash`
    - `mapping/line_pattern_reconstruction.py:resolve_observed_name` → `mapping/line_pattern_reconstruction.py:select_observed_name`
    - `mapping/line_pattern_reconstruction.py:resolve_observed_name` → `mapping/line_pattern_reconstruction.py:short_join_hash`
    - `mapping/line_pattern_revit_apply.py:create_and_verify_line_pattern` → `mapping/line_pattern_revit_apply.py:CreationResult`
    - `mapping/line_pattern_revit_apply.py:create_and_verify_line_pattern` → `mapping/line_pattern_revit_apply.py:_build_api_segments`
    - `mapping/line_pattern_revit_apply.py:create_and_verify_line_pattern` → `mapping/line_pattern_revit_apply.py:verify_element_join_hash`
    - `mapping/line_pattern_revit_apply.py:verify_element_join_hash` → `mapping/line_pattern_reconstruction.py:compute_join_hash_for_segments`
    - `mapping/line_pattern_revit_apply.py:verify_element_join_hash` → `mapping/line_pattern_revit_apply.py:VerificationResult`
    - `mapping/line_pattern_revit_apply.py:verify_element_join_hash` → `mapping/line_pattern_revit_apply.py:read_segments_from_element`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:_expand_sequence_key`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:_get_shape_specific_requirements`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:_items_to_kqv_map`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:build_join_key_from_policy.emit_key`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/phase2.py:phase2_join_hash`
      - `core/collect.py:collect_elements` → `core/collect.py:CollectCtx.inc`
      - `core/collect.py:collect_elements` → `core/collect.py:_get_element`
      - `core/collect.py:collect_elements` → `core/collect.py:collect_id_ints`
      - `mapping/line_pattern_reconstruction.py:select_observed_name` → `mapping/line_pattern_reconstruction.py:select_observed_name._files_count`

## Trace: `scripts/check_audit_references.py:main`

- `scripts/check_audit_references.py:main` → `scripts/check_audit_references.py:tracked_files`

## Trace: `sync_revitlookup_reference.py:main`

- `sync_revitlookup_reference.py:main` → `sync_revitlookup_reference.py:sync`
  - `sync_revitlookup_reference.py:sync` → `sync_revitlookup_reference.py:fetch_raw`
  - `sync_revitlookup_reference.py:sync` → `sync_revitlookup_reference.py:get_current_commit_sha`
  - `sync_revitlookup_reference.py:sync` → `sync_revitlookup_reference.py:list_all_cs_files`
    - `sync_revitlookup_reference.py:get_current_commit_sha` → `sync_revitlookup_reference.py:github_get`
    - `sync_revitlookup_reference.py:list_all_cs_files` → `sync_revitlookup_reference.py:github_get`

## Trace: `tests/revit/revit_test_runner_pyrevit.py:main`

- `tests/revit/revit_test_runner_pyrevit.py:main` → `core/manifest.py:build_manifest`
- `tests/revit/revit_test_runner_pyrevit.py:main` → `runner/run_dynamo.py:run_fingerprint`
- `tests/revit/revit_test_runner_pyrevit.py:main` → `tests/revit/_json_diff.py:compare_json`
- `tests/revit/revit_test_runner_pyrevit.py:main` → `tests/revit/revit_test_runner_pyrevit.py:_now_stamp`
  - `core/manifest.py:build_manifest` → `core/manifest.py:_safe_dict`
  - `runner/run_dynamo.py:run_fingerprint` → `core/collect.py:CollectCtx`
  - `runner/run_dynamo.py:run_fingerprint` → `core/collect.py:build_purgeable_id_set`
  - `runner/run_dynamo.py:run_fingerprint` → `core/context.py:DocViewContext`
  - `runner/run_dynamo.py:run_fingerprint` → `core/contracts.py:add_bounded_error`
  - `runner/run_dynamo.py:run_fingerprint` → `core/contracts.py:new_domain_envelope`
  - `runner/run_dynamo.py:run_fingerprint` → `core/contracts.py:new_run_diag`
  - `runner/run_dynamo.py:run_fingerprint` → `core/contracts.py:new_run_envelope`
  - `runner/run_dynamo.py:run_fingerprint` → `core/deps.py:require_domain`
  - `runner/run_dynamo.py:run_fingerprint` → `core/features.py:build_features`
  - `runner/run_dynamo.py:run_fingerprint` → `core/manifest.py:build_manifest`
  - `runner/run_dynamo.py:run_fingerprint` → `core/timing_collector.py:TimingCollector`
  - `runner/run_dynamo.py:run_fingerprint` → `core/timing_collector.py:TimingCollector.end_timer`
  - `runner/run_dynamo.py:run_fingerprint` → `core/timing_collector.py:TimingCollector.get_report`
  - `runner/run_dynamo.py:run_fingerprint` → `core/timing_collector.py:TimingCollector.start_timer`
  - `runner/run_dynamo.py:run_fingerprint` → `runner/extraction_context.py:build_extraction_context`
  - `runner/run_dynamo.py:run_fingerprint` → `runner/extraction_context.py:operator_deployment_config_path`
  - `runner/run_dynamo.py:run_fingerprint` → `runner/run_dynamo.py:_build_workset_name_to_unique_id_ctx`
  - `runner/run_dynamo.py:run_fingerprint` → `runner/run_dynamo.py:_canonicalize_all_domain_records`
  - `runner/run_dynamo.py:run_fingerprint` → `runner/run_dynamo.py:_domain_run`
  - `runner/run_dynamo.py:run_fingerprint` → `runner/run_dynamo.py:_enabled`
  - `tests/revit/_json_diff.py:compare_json` → `tests/revit/_json_diff.py:diff_paths`
  - `tests/revit/_json_diff.py:compare_json` → `tests/revit/_json_diff.py:sha256_of_json`
    - `core/contracts.py:new_domain_envelope` → `core/contracts.py:_ensure_list`
    - `core/features.py:build_features` → `core/features.py:_extract_counts_from_legacy`
    - `core/timing_collector.py:TimingCollector.end_timer` → `core/timing_collector.py:TimingCollector._record_elapsed_locked`
    - `core/timing_collector.py:TimingCollector.get_report` → `core/timing_collector.py:TimingCollector._build_report`
    - `runner/extraction_context.py:build_extraction_context` → `core/deployment_config.py:load_deployment_config`
    - `runner/run_dynamo.py:_canonicalize_all_domain_records` → `core/canonical_items.py:canonicalize_record`
    - `runner/run_dynamo.py:_domain_run` → `core/contracts.py:add_bounded_error`
    - `runner/run_dynamo.py:_domain_run` → `core/contracts.py:new_domain_envelope`
    - `runner/run_dynamo.py:_domain_run` → `core/timing_collector.py:TimingCollector.end_timer`
    - `runner/run_dynamo.py:_domain_run` → `core/timing_collector.py:TimingCollector.set_active_domain`
    - `runner/run_dynamo.py:_domain_run` → `core/timing_collector.py:TimingCollector.start_timer`
    - `runner/run_dynamo.py:_domain_run` → `runner/run_dynamo.py:_extract_legacy_quality`
    - `runner/run_dynamo.py:_domain_run` → `runner/run_dynamo.py:_extract_v2_block_reasons`
    - `runner/run_dynamo.py:_domain_run` → `runner/run_dynamo.py:_extract_v2_hash`
    - `runner/run_dynamo.py:_domain_run` → `runner/run_dynamo.py:_has_v2_surface`
    - `runner/run_dynamo.py:_domain_run` → `runner/run_dynamo.py:_looks_like_revit_unique_id`
    - `tests/revit/_json_diff.py:sha256_of_json` → `tests/revit/_json_diff.py:canonical_json_bytes`
      - `core/deployment_config.py:load_deployment_config` → `core/deployment_config.py:_identity_allowed_keys`
      - `core/deployment_config.py:load_deployment_config` → `core/deployment_config.py:validate_project_info_shared_parameters`
      - `core/canonical_items.py:canonicalize_record` → `core/canonical_items.py:build_flat_items`
      - `tests/revit/_json_diff.py:canonical_json_bytes` → `tests/revit/_json_diff.py:_canon_obj`

## Trace: `tools/_archive/join_key_derivation_phase05.py:main`

- `tools/_archive/join_key_derivation_phase05.py:main` → `tools/_archive/join_key_derivation_phase05.py:derive_join_keys`
- `tools/_archive/join_key_derivation_phase05.py:main` → `tools/_archive/join_key_derivation_phase05.py:expand_globs`
  - `tools/_archive/join_key_derivation_phase05.py:derive_join_keys` → `tools/_archive/join_key_derivation_phase05.py:choose_record_handle`
  - `tools/_archive/join_key_derivation_phase05.py:derive_join_keys` → `tools/_archive/join_key_derivation_phase05.py:extract_file_id`
  - `tools/_archive/join_key_derivation_phase05.py:derive_join_keys` → `tools/_archive/join_key_derivation_phase05.py:index_items_by_k`
  - `tools/_archive/join_key_derivation_phase05.py:derive_join_keys` → `tools/_archive/join_key_derivation_phase05.py:select_items_for_policy`
  - `tools/_archive/join_key_derivation_phase05.py:derive_join_keys` → `tools/_archive/join_key_derivation_phase05.py:stable_serialize_value`
    - `tools/_archive/join_key_derivation_phase05.py:select_items_for_policy` → `tools/_archive/join_key_derivation_phase05.py:_as_str_list`
    - `tools/_archive/join_key_derivation_phase05.py:select_items_for_policy` → `tools/_archive/join_key_derivation_phase05.py:choose_candidate_deterministically`
    - `tools/_archive/join_key_derivation_phase05.py:select_items_for_policy` → `tools/_archive/join_key_derivation_phase05.py:is_usable_q`
      - `tools/_archive/join_key_derivation_phase05.py:choose_candidate_deterministically` → `tools/_archive/join_key_derivation_phase05.py:stable_serialize_value`

## Trace: `tools/acc_scan_dc.py:main`

- `tools/acc_scan_dc.py:main` → `dev_tools/repo_context/rc_validate.py:ValidationResult.error`
- `tools/acc_scan_dc.py:main` → `tools/acc_scan_dc.py:load_existing_includes`
- `tools/acc_scan_dc.py:main` → `tools/acc_scan_dc.py:parse_types`
- `tools/acc_scan_dc.py:main` → `tools/acc_scan_dc.py:scan`
  - `tools/acc_scan_dc.py:parse_types` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/acc_scan_dc.py:scan` → `tools/_archive/join_key_derivation_phase05.py:extract_records.walk`
  - `tools/acc_scan_dc.py:scan` → `tools/acc_scan_dc.py:read_rvt_version`
    - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`

## Trace: `tools/acc_sync_dc.py:main`

- `tools/acc_sync_dc.py:main` → `tools/acc_sync_dc.py:hydrate`
- `tools/acc_sync_dc.py:main` → `tools/acc_sync_dc.py:is_stub`
- `tools/acc_sync_dc.py:main` → `tools/acc_sync_dc.py:load_included_entries`
- `tools/acc_sync_dc.py:main` → `tools/acc_sync_dc.py:write_log`
  - `tools/acc_sync_dc.py:hydrate` → `tools/acc_sync_dc.py:is_stub`

## Trace: `tools/analyze_promotion_candidates.py:main`

- `tools/analyze_promotion_candidates.py:main` → `tools/analyze_promotion_candidates.py:apply_export_cap`
- `tools/analyze_promotion_candidates.py:main` → `tools/analyze_promotion_candidates.py:compute_reuse_scope`
- `tools/analyze_promotion_candidates.py:main` → `tools/analyze_promotion_candidates.py:compute_seeded_scope`
- `tools/analyze_promotion_candidates.py:main` → `tools/analyze_promotion_candidates.py:require_columns`
- `tools/analyze_promotion_candidates.py:main` → `tools/analyze_promotion_candidates.py:safe_bool_series`
- `tools/analyze_promotion_candidates.py:main` → `tools/enterprise_policy.py:write_enterprise_policy_provenance`
  - `tools/analyze_promotion_candidates.py:compute_reuse_scope` → `tools/enterprise_policy.py:load_enterprise_policy`
  - `tools/enterprise_policy.py:write_enterprise_policy_provenance` → `tools/enterprise_policy.py:EnterprisePolicy.provenance_bytes`
    - `tools/enterprise_policy.py:load_enterprise_policy` → `tools/enterprise_policy.py:EnterprisePolicy`
    - `tools/enterprise_policy.py:load_enterprise_policy` → `tools/enterprise_policy.py:_label`
    - `tools/enterprise_policy.py:EnterprisePolicy.provenance_bytes` → `tools/enterprise_policy.py:EnterprisePolicy.provenance`

## Trace: `tools/apply_join_policy.py:main`

- `tools/apply_join_policy.py:main` → `tools/join_key_discovery/eval.py:build_candidate_join_key_with_details`
- `tools/apply_join_policy.py:main` → `tools/join_key_discovery/eval.py:build_identity_index`
- `tools/apply_join_policy.py:main` → `tools/join_key_discovery/eval.py:normalize_policy_block`
  - `tools/join_key_discovery/eval.py:build_candidate_join_key_with_details` → `tools/join_key_discovery/eval.py:_lookup_shape_cfg`
  - `tools/join_key_discovery/eval.py:normalize_policy_block` → `tools/join_key_discovery/eval.py:_listish`

## Trace: `tools/apply_name_key_policy.py:main`

- `tools/apply_name_key_policy.py:main` → `tools/apply_name_key_policy.py:_iter_export_paths`
- `tools/apply_name_key_policy.py:main` → `tools/apply_name_key_policy.py:_rows_for_export`
  - `tools/apply_name_key_policy.py:_rows_for_export` → `core/name_key_builder.py:build_name_key_for_record`
  - `tools/apply_name_key_policy.py:_rows_for_export` → `tools/apply_name_key_policy.py:_iter_domain_payloads`
  - `tools/apply_name_key_policy.py:_rows_for_export` → `tools/patterns_analysis/_archive/domain_identity_contract.py:DomainIdentityContract.load`
    - `core/name_key_builder.py:build_name_key_for_record` → `core/join_key_builder.py:build_join_key_from_policy`
    - `core/name_key_builder.py:build_name_key_for_record` → `core/join_key_builder.py:compute_projection_status`
    - `core/name_key_builder.py:build_name_key_for_record` → `core/join_key_policy.py:get_domain_join_key_policy`
    - `core/name_key_builder.py:build_name_key_for_record` → `core/name_key_builder.py:_has_detail_data`
    - `core/name_key_builder.py:build_name_key_for_record` → `core/name_key_builder.py:flat_items_for_record`
    - `core/name_key_builder.py:build_name_key_for_record` → `core/record_v2.py:canonicalize_str`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:_expand_sequence_key`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:_get_shape_specific_requirements`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:_items_to_kqv_map`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/join_key_builder.py:build_join_key_from_policy.emit_key`
      - `core/join_key_builder.py:build_join_key_from_policy` → `core/phase2.py:phase2_join_hash`
      - `core/name_key_builder.py:flat_items_for_record` → `core/canonical_items.py:build_flat_items`

## Trace: `tools/archetype/assign_archetype_classifications.py:main`

- `tools/archetype/assign_archetype_classifications.py:main` → `tools/archetype/_common.py:atomic_write_json`
- `tools/archetype/assign_archetype_classifications.py:main` → `tools/archetype/_common.py:build_edge_aliases`
- `tools/archetype/assign_archetype_classifications.py:main` → `tools/archetype/assign_archetype_classifications.py:DomainPatternLabelCache`
- `tools/archetype/assign_archetype_classifications.py:main` → `tools/archetype/assign_archetype_classifications.py:_evaluate_signal`
- `tools/archetype/assign_archetype_classifications.py:main` → `tools/archetype/assign_archetype_classifications.py:_signal_fired_source`
- `tools/archetype/assign_archetype_classifications.py:main` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/_common.py:build_edge_aliases` → `tools/archetype/_common.py:strip_partition_suffix`
  - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`

## Trace: `tools/archetype/build_cross_domain_items.py:main`

- `tools/archetype/build_cross_domain_items.py:main` → `tools/archetype/build_cross_domain_items.py:_build_dynamic_rows`
- `tools/archetype/build_cross_domain_items.py:main` → `tools/archetype/build_cross_domain_items.py:_build_structural_rows`
  - `tools/archetype/build_cross_domain_items.py:_build_dynamic_rows` → `tools/archetype/_common.py:field_matches`
  - `tools/archetype/build_cross_domain_items.py:_build_dynamic_rows` → `tools/archetype/_common.py:is_valid_item`
  - `tools/archetype/build_cross_domain_items.py:_build_dynamic_rows` → `tools/archetype/build_cross_domain_items.py:_parse_vf_categories`
  - `tools/archetype/build_cross_domain_items.py:_build_structural_rows` → `tools/archetype/_common.py:field_matches`
  - `tools/archetype/build_cross_domain_items.py:_build_structural_rows` → `tools/archetype/_common.py:is_valid_item`
    - `tools/archetype/build_cross_domain_items.py:_parse_vf_categories` → `tools/extract_segment_subtree.py:NumericStats.add`
      - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`

## Trace: `tools/archetype/cluster_archetype_signals.py:main`

- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/_common.py:atomic_write_json`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_apply_threshold`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_build_clusters`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_build_coverage_summary`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_build_detail_files_lookup`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_build_n_files_classified_lookup`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_build_signal_cluster_map`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_build_signal_graph`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_compute_file_universe`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_derive_coupling_threshold`
- `tools/archetype/cluster_archetype_signals.py:main` → `tools/archetype/cluster_archetype_signals.py:_rollup_classifications`
  - `tools/archetype/cluster_archetype_signals.py:_build_clusters` → `tools/archetype/cluster_archetype_signals.py:_cluster_label_stub`
  - `tools/archetype/cluster_archetype_signals.py:_build_clusters` → `tools/archetype/cluster_archetype_signals.py:_complete_linkage_clusters`
  - `tools/archetype/cluster_archetype_signals.py:_compute_file_universe` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/cluster_archetype_signals.py:_derive_coupling_threshold` → `tools/archetype/cluster_archetype_signals.py:_jenks_threshold_for_values`
    - `tools/archetype/cluster_archetype_signals.py:_cluster_label_stub` → `tools/archetype/cluster_archetype_signals.py:_bare_signal_name`
    - `tools/archetype/cluster_archetype_signals.py:_complete_linkage_clusters` → `tools/archetype/cluster_archetype_signals.py:_complete_linkage_clusters.pair_value`
    - `tools/archetype/cluster_archetype_signals.py:_complete_linkage_clusters` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`
    - `tools/archetype/cluster_archetype_signals.py:_jenks_threshold_for_values` → `tools/jenks_utils.py:jenks_breaks`

## Trace: `tools/archetype/compute_cross_domain_cooccurrence.py:main`

- `tools/archetype/compute_cross_domain_cooccurrence.py:main` → `tools/archetype/_common.py:build_edge_aliases`
- `tools/archetype/compute_cross_domain_cooccurrence.py:main` → `tools/archetype/compute_cross_domain_cooccurrence.py:_eligibility_reason`
- `tools/archetype/compute_cross_domain_cooccurrence.py:main` → `tools/archetype/compute_cross_domain_cooccurrence.py:_pattern_id`
- `tools/archetype/compute_cross_domain_cooccurrence.py:main` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/_common.py:build_edge_aliases` → `tools/archetype/_common.py:strip_partition_suffix`
  - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`

## Trace: `tools/archetype/discover_vfd_edges.py:main`

- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:build_domain_gap_rows`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:build_edge_rows`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:build_inventory_rows`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:build_unresolved_file_rows`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:find_identity_items_path`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:load_bip_hints`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:print_summary`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:print_unresolved_summary`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:read_json_optional`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:read_json_required`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:resolve_params`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:stream_observations`
- `tools/archetype/discover_vfd_edges.py:main` → `tools/archetype/discover_vfd_edges.py:verify_outputs`
  - `tools/archetype/discover_vfd_edges.py:build_domain_gap_rows` → `tools/archetype/discover_vfd_edges.py:_candidate_category_details`
  - `tools/archetype/discover_vfd_edges.py:build_domain_gap_rows` → `tools/archetype/discover_vfd_edges.py:_category_map_domain_extracted`
  - `tools/archetype/discover_vfd_edges.py:build_edge_rows` → `tools/archetype/discover_vfd_edges.py:bool_s`
  - `tools/archetype/discover_vfd_edges.py:build_edge_rows` → `tools/archetype/discover_vfd_edges.py:normalize_param_name`
  - `tools/archetype/discover_vfd_edges.py:build_edge_rows` → `tools/archetype/discover_vfd_edges.py:parse_category_set`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:_decompose_conflict_to_domains`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:_find_verify_blocked_candidate`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:_resolve_target_domain_from_categories`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:_validate_domain_has_identity_items`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:build_inventory_rows.append_inventory_row`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:infer_domain`
  - `tools/archetype/discover_vfd_edges.py:build_inventory_rows` → `tools/archetype/discover_vfd_edges.py:parse_categories`
  - `tools/archetype/discover_vfd_edges.py:build_unresolved_file_rows` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/discover_vfd_edges.py:load_bip_hints` → `tools/archetype/discover_vfd_edges.py:read_json_required`
  - `tools/archetype/discover_vfd_edges.py:read_json_optional` → `tools/patterns_analysis/_archive/domain_identity_contract.py:DomainIdentityContract.load`
  - `tools/archetype/discover_vfd_edges.py:read_json_required` → `tools/patterns_analysis/_archive/domain_identity_contract.py:DomainIdentityContract.load`
  - `tools/archetype/discover_vfd_edges.py:resolve_params` → `tools/archetype/discover_vfd_edges.py:ResolvedParam`
  - `tools/archetype/discover_vfd_edges.py:resolve_params` → `tools/archetype/discover_vfd_edges.py:canonical_param_kind`
  - `tools/archetype/discover_vfd_edges.py:stream_observations` → `tools/archetype/discover_vfd_edges.py:flush_record`
  - `tools/archetype/discover_vfd_edges.py:stream_observations` → `tools/archetype/discover_vfd_edges.py:is_usable_identity_item_value`
  - `tools/archetype/discover_vfd_edges.py:stream_observations` → `tools/archetype/discover_vfd_edges.py:row_quality`
  - `tools/archetype/discover_vfd_edges.py:stream_observations` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/archetype/discover_vfd_edges.py:_candidate_category_details` → `tools/archetype/discover_vfd_edges.py:category_entry_name`
    - `tools/archetype/discover_vfd_edges.py:_candidate_category_details` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/archetype/discover_vfd_edges.py:_category_map_domain_extracted` → `tools/archetype/discover_vfd_edges.py:bool_s`
    - `tools/archetype/discover_vfd_edges.py:_category_map_domain_extracted` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/archetype/discover_vfd_edges.py:_find_verify_blocked_candidate` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/archetype/discover_vfd_edges.py:_resolve_target_domain_from_categories` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/archetype/discover_vfd_edges.py:build_inventory_rows.append_inventory_row` → `tools/archetype/discover_vfd_edges.py:_category_flags_for_ids`
    - `tools/archetype/discover_vfd_edges.py:build_inventory_rows.append_inventory_row` → `tools/archetype/discover_vfd_edges.py:_category_names_for_ids`
    - `tools/archetype/discover_vfd_edges.py:build_inventory_rows.append_inventory_row` → `tools/archetype/discover_vfd_edges.py:bool_s`
    - `tools/archetype/discover_vfd_edges.py:infer_domain` → `tools/archetype/discover_vfd_edges.py:DomainHint`
    - `tools/archetype/discover_vfd_edges.py:infer_domain` → `tools/archetype/discover_vfd_edges.py:hint_target_and_verify`
    - `tools/archetype/discover_vfd_edges.py:infer_domain` → `tools/archetype/discover_vfd_edges.py:iter_name_contains_rules`
    - `tools/archetype/discover_vfd_edges.py:parse_categories` → `tools/archetype/discover_vfd_edges.py:ParsedCategories`
    - `tools/archetype/discover_vfd_edges.py:parse_categories` → `tools/archetype/discover_vfd_edges.py:category_entry_name`
    - `tools/archetype/discover_vfd_edges.py:parse_categories` → `tools/archetype/discover_vfd_edges.py:parse_category_tokens`
    - `tools/archetype/discover_vfd_edges.py:parse_categories` → `tools/archetype/discover_vfd_edges.py:sort_category_tokens`
    - `tools/archetype/discover_vfd_edges.py:parse_categories` → `tools/extract_segment_subtree.py:NumericStats.add`
    - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`
    - `tools/archetype/discover_vfd_edges.py:flush_record` → `tools/archetype/discover_vfd_edges.py:RawObservation`
    - `tools/archetype/discover_vfd_edges.py:flush_record` → `tools/archetype/discover_vfd_edges.py:canonical_param_kind`
    - `tools/archetype/discover_vfd_edges.py:flush_record` → `tools/archetype/discover_vfd_edges.py:is_bad_param_id`
      - `tools/archetype/discover_vfd_edges.py:_category_names_for_ids` → `tools/archetype/discover_vfd_edges.py:category_entry_name`

## Trace: `tools/archetype/generate_archetype_candidates.py:main`

- `tools/archetype/generate_archetype_candidates.py:main` → `tools/archetype/_common.py:atomic_write_json`
- `tools/archetype/generate_archetype_candidates.py:main` → `tools/archetype/_common.py:build_edge_aliases`
- `tools/archetype/generate_archetype_candidates.py:main` → `tools/archetype/_common.py:slugify`
- `tools/archetype/generate_archetype_candidates.py:main` → `tools/archetype/generate_archetype_candidates.py:_collapsed_from_for_edge`
- `tools/archetype/generate_archetype_candidates.py:main` → `tools/archetype/generate_archetype_candidates.py:_governance_question_hint`
- `tools/archetype/generate_archetype_candidates.py:main` → `tools/archetype/generate_archetype_candidates.py:_signal_coverage_pct`
- `tools/archetype/generate_archetype_candidates.py:main` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/_common.py:build_edge_aliases` → `tools/archetype/_common.py:strip_partition_suffix`
  - `tools/archetype/generate_archetype_candidates.py:_governance_question_hint` → `tools/archetype/generate_archetype_candidates.py:_is_vfd_related`
  - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`

## Trace: `tools/archetype/generate_reference_graph.py:main`

- `tools/archetype/generate_reference_graph.py:main` → `tools/archetype/_common.py:atomic_write_json`
- `tools/archetype/generate_reference_graph.py:main` → `tools/archetype/generate_reference_graph.py:_build_dynamic_edges`
- `tools/archetype/generate_reference_graph.py:main` → `tools/archetype/generate_reference_graph.py:_check_static_edge_availability`
  - `tools/archetype/generate_reference_graph.py:_build_dynamic_edges` → `tools/archetype/generate_reference_graph.py:_normalize_param_name`
  - `tools/archetype/generate_reference_graph.py:_build_dynamic_edges` → `tools/archetype/generate_reference_graph.py:_param_id_slug`
  - `tools/archetype/generate_reference_graph.py:_build_dynamic_edges` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/generate_reference_graph.py:_check_static_edge_availability` → `tools/archetype/_common.py:field_matches`
  - `tools/archetype/generate_reference_graph.py:_check_static_edge_availability` → `tools/archetype/_common.py:is_valid_item`
    - `tools/archetype/generate_reference_graph.py:_param_id_slug` → `tools/archetype/generate_reference_graph.py:_normalize_param_name`
    - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`

## Trace: `tools/archetype/prepare_archetype_review.py:main`

- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_all_cluster_ids`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_all_clusters`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_build_cluster_context`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_find_cluster`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_load_file_path_lookup`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_load_label_lookup`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_load_vfd_resolution`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_process_cluster`
- `tools/archetype/prepare_archetype_review.py:main` → `tools/archetype/prepare_archetype_review.py:_write_review_schedule_outputs`
  - `tools/archetype/prepare_archetype_review.py:_all_cluster_ids` → `tools/archetype/prepare_archetype_review.py:_all_clusters`
  - `tools/archetype/prepare_archetype_review.py:_build_cluster_context` → `tools/archetype/prepare_archetype_review.py:ClusterContext`
  - `tools/archetype/prepare_archetype_review.py:_build_cluster_context` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/prepare_archetype_review.py:_find_cluster` → `tools/archetype/prepare_archetype_review.py:_all_clusters`
  - `tools/archetype/prepare_archetype_review.py:_load_vfd_resolution` → `tools/archetype/_common.py:field_matches`
  - `tools/archetype/prepare_archetype_review.py:_load_vfd_resolution` → `tools/archetype/_common.py:is_valid_item`
  - `tools/archetype/prepare_archetype_review.py:_load_vfd_resolution` → `tools/archetype/prepare_archetype_review.py:_parse_category_ids`
  - `tools/archetype/prepare_archetype_review.py:_process_cluster` → `tools/extract_segment_subtree.py:NumericStats.add`
  - `tools/archetype/prepare_archetype_review.py:_write_review_schedule_outputs` → `tools/archetype/prepare_archetype_review.py:_select_schedule_rows_for_cluster`
    - `tools/extract_segment_subtree.py:NumericStats.add` → `tools/extract_segment_subtree.py:norm`
    - `tools/archetype/prepare_archetype_review.py:_select_schedule_rows_for_cluster` → `tools/archetype/prepare_archetype_review.py:_is_named_element`
    - `tools/archetype/prepare_archetype_review.py:_select_schedule_rows_for_cluster` → `tools/archetype/prepare_archetype_review.py:_schedule_file_sort_key`
    - `tools/archetype/prepare_archetype_review.py:_select_schedule_rows_for_cluster` → `tools/archetype/prepare_archetype_review.py:_selected_file_name_status`
      - `tools/archetype/prepare_archetype_review.py:_selected_file_name_status` → `tools/archetype/prepare_archetype_review.py:_is_named_element`

## Trace: `tools/archetype/review/select_archetype_review_files.py:main`

- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_all_cluster_pairs`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_build_approach_label_map`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_build_file_cluster_index`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_build_output_rows`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_cluster_signal_ids`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_greedy_cover`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_identify_gaps`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_load_file_paths`
- `tools/archetype/review/select_archetype_review_files.py:main` → `tools/archetype/review/select_archetype_review_files.py:_load_review_csvs`
