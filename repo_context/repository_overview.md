# Repository Overview: Revit_Fingerprint

- Scan time (UTC): 2026-08-21T23:22:38Z
- Generator: repo_context.py v0.1.0
- Total files considered: 648
- Included: 645
- Excluded: 3

## Files by extension

- `.py`: 378
- `.cs`: 118
- `.md`: 99
- `.json`: 27
- `(none)`: 6
- `.csv`: 6
- `.ps1`: 3
- `.sh`: 3
- `.dyn`: 2
- `.yml`: 2
- `.bak`: 1

## Files by classification

- python_source: 257
- unsupported_text: 127
- test: 121
- documentation: 99
- data: 32
- script_powershell: 3
- script_shell: 3
- configuration: 2
- json_schema: 1

## Largest text files

- `tools/generate_governance_narrative.py` — 374542 bytes, 6890 lines
- `tools/compare_cross_segment.py` — 254650 bytes, 5383 lines
- `tools/archetype/bip_lookup.json` — 179066 bytes, 3630 lines
- `CHANGELOG.md` — 173732 bytes, 2477 lines
- `domains/dimension_types.py` — 146708 bytes, 3103 lines
- `DECISIONS.md` — 109006 bytes, 1674 lines
- `tests/test_compare_cross_segment_governance.py` — 96239 bytes, 2213 lines
- `domains/view_templates.py` — 91960 bytes, 2557 lines
- `tests/test_build_segment_manifest.py` — 91342 bytes, 1886 lines
- `audit_results/audit_11_domain_extractor_delta_step0_findings.md` — 91153 bytes, 517 lines
- `tools/governance_evidence_package.py` — 90748 bytes, 1621 lines
- `tools/run_segment_orchestrator.py` — 89715 bytes, 1867 lines
- `domains/fill_patterns.py` — 77455 bytes, 1945 lines
- `docs/research/semantic_label_review.csv` — 70375 bytes, 106 lines
- `tools/extractor.py` — 68307 bytes, 1515 lines

## Largest Python files by line count

- `tools/generate_governance_narrative.py` — 6890 lines
- `tools/compare_cross_segment.py` — 5383 lines
- `domains/dimension_types.py` — 3103 lines
- `domains/view_templates.py` — 2557 lines
- `tests/test_compare_cross_segment_governance.py` — 2213 lines
- `domains/fill_patterns.py` — 1945 lines
- `tests/test_build_segment_manifest.py` — 1886 lines
- `tools/run_segment_orchestrator.py` — 1867 lines
- `tools/governance_evidence_package.py` — 1621 lines
- `runner/run_dynamo.py` — 1581 lines
- `tools/extractor.py` — 1515 lines
- `legacy/fingerprint_mvp.py` — 1402 lines
- `tools/archetype/discover_vfd_edges.py` — 1353 lines
- `tools/run_extract_all.py` — 1234 lines
- `tools/bundle_analysis/run_bundle_analysis.py` — 1202 lines

## Files that required chunking (37)

- `CHANGELOG.md`
- `DECISIONS.md`
- `audit_results/audit_11_domain_extractor_delta_step0_findings.md`
- `contracts/domain_identity_keys_v2.json`
- `core/dimension_type_helpers.py`
- `domains/dimension_types.py`
- `domains/fill_patterns.py`
- `domains/view_templates.py`
- `legacy/fingerprint_mvp.py`
- `policies/domain_join_key_policies.json`
- `policies/domain_sig_hash_policies.json`
- `runner/run_dynamo.py`
- `tests/test_build_segment_manifest.py`
- `tests/test_compare_cross_segment_governance.py`
- `tests/test_generate_governance_narrative_evidence_package.py`
- ... and 22 more (see chunk_manifest.csv)

## Python parse failures (0)

- (none)

## Likely entry points (103)

- `dev_tools/repo_context/repo_context.py` — contains `if __name__ == "__main__":` guard
- `scripts/check_audit_references.py` — contains `if __name__ == "__main__":` guard
- `sync_revitlookup_reference.py` — contains `if __name__ == "__main__":` guard
- `tests/revit/revit_test_runner_pyrevit.py` — contains `if __name__ == "__main__":` guard
- `tools/_archive/join_key_derivation_phase05.py` — contains `if __name__ == "__main__":` guard
- `tools/acc_scan_dc.py` — contains `if __name__ == "__main__":` guard
- `tools/acc_sync_dc.py` — contains `if __name__ == "__main__":` guard
- `tools/analyze_promotion_candidates.py` — contains `if __name__ == "__main__":` guard
- `tools/apply_join_policy.py` — contains `if __name__ == "__main__":` guard
- `tools/apply_name_key_policy.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/assign_archetype_classifications.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/build_cross_domain_items.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/cluster_archetype_signals.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/compute_cross_domain_cooccurrence.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/discover_vfd_edges.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/generate_archetype_candidates.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/generate_reference_graph.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/prepare_archetype_review.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/review/select_archetype_review_files.py` — contains `if __name__ == "__main__":` guard
- `tools/archetype/validate_archetype_signals.py` — contains `if __name__ == "__main__":` guard
- `tools/build_results_registry.py` — contains `if __name__ == "__main__":` guard
- `tools/build_segment_manifest.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/placeholder_exclusions.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/placeholder_exclusions_legacy.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/run_bundle_analysis.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step0_discover_populations.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step1_membership_matrix.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step2_find_bundles.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step2b_bundle_share_profile.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step3_build_dag.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step4_difference_sets.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step5_classify_patterns.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step6_classify_files.py` — contains `if __name__ == "__main__":` guard
- `tools/bundle_analysis/step7_overlap_report.py` — contains `if __name__ == "__main__":` guard
- `tools/compare_cross_segment.py` — contains `if __name__ == "__main__":` guard
- `tools/compare_governance_populations.py` — contains `if __name__ == "__main__":` guard
- `tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py` — contains `if __name__ == "__main__":` guard
- `tools/compute_governance_thresholds.py` — contains `if __name__ == "__main__":` guard
- `tools/compute_latent_purgeable.py` — contains `if __name__ == "__main__":` guard
- `tools/discover_hash_policy.py` — contains `if __name__ == "__main__":` guard
- `tools/discover_join_policy.py` — contains `if __name__ == "__main__":` guard
- `tools/domain_authority.py` — contains `if __name__ == "__main__":` guard
- `tools/emit_element_dominance.py` — contains `if __name__ == "__main__":` guard
- `tools/export_bundle_pattern_detail.py` — contains `if __name__ == "__main__":` guard
- `tools/export_to_flat_tables.py` — contains `if __name__ == "__main__":` guard
- `tools/extract_segment_subtree.py` — contains `if __name__ == "__main__":` guard
- `tools/generate_governance_narrative.py` — contains `if __name__ == "__main__":` guard
- `tools/generate_name_key_patterns.py` — contains `if __name__ == "__main__":` guard
- `tools/generate_sig_hash_policy.py` — contains `if __name__ == "__main__":` guard
- `tools/governance/standards_governance_report.py` — contains `if __name__ == "__main__":` guard
- `tools/governance_manifest.py` — contains `if __name__ == "__main__":` guard
- `tools/governance_relationships.py` — contains `if __name__ == "__main__":` guard
- `tools/inspect_lft_similarity.py` — contains `if __name__ == "__main__":` guard
- `tools/join_key_discovery/materials_joinkey_discover.py` — contains `if __name__ == "__main__":` guard
- `tools/label_synthesis/build_identity_items_lookup.py` — contains `if __name__ == "__main__":` guard
- `tools/label_synthesis/build_label_population.py` — contains `if __name__ == "__main__":` guard
- `tools/label_synthesis/build_semantic_groups.py` — contains `if __name__ == "__main__":` guard
- `tools/label_synthesis/patch_all_domain_patterns.py` — contains `if __name__ == "__main__":` guard
- `tools/label_synthesis/patch_domain_patterns_labels.py` — contains `if __name__ == "__main__":` guard
- `tools/label_synthesis/synthesize_fragmented_labels.py` — contains `if __name__ == "__main__":` guard
- `tools/migration/compress_fingerprint_json.py` — contains `if __name__ == "__main__":` guard
- `tools/migration/extract_first_record.py` — contains `if __name__ == "__main__":` guard
- `tools/migration/migrate_materials_identity_items.py` — contains `if __name__ == "__main__":` guard
- `tools/migration/reformat_to_flat_items.py` — contains `if __name__ == "__main__":` guard
- `tools/pairwise_analysis.py` — contains `if __name__ == "__main__":` guard
- `tools/pareto_joinkey_search.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/annotate_cluster_labels.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/apply_join_keys_by_ids.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/backfill_cluster_label_inputs.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/build_reference_standards.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/calibrate_join_key_gates.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/derive_join_keys_by_ids.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/emit_intradomain_definition.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/intradomain_summary.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/pareto_join_keys_by_ids.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/pareto_with_splits.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_attribute_stress.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_attribute_stress_all_joinable.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_candidate_joinkey_simulation.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_change_type.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_collision_differencing.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_dimension_types_by_family.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_identity_collision_diagnostics.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_joinhash_label_population.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_joinhash_parameter_population.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_population_stability.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_text_types_candidate_joinkey_simulation.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_view_category_overrides_joinkey_analysis.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/run_view_templates_joinkey_analysis.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/split_detection_element_level.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/_archive/split_detection_file_level.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/split_detection_element_level.py` — contains `if __name__ == "__main__":` guard
- `tools/patterns_analysis/split_detection_file_level.py` — contains `if __name__ == "__main__":` guard
- `tools/population_framing.py` — contains `if __name__ == "__main__":` guard
- `tools/probes/build_probe_inventory.py` — contains `if __name__ == "__main__":` guard
- `tools/probes/find_crosswalk_candidates.py` — contains `if __name__ == "__main__":` guard
- `tools/probes/sweep_line_pattern_normhash_precision.py` — contains `if __name__ == "__main__":` guard
- `tools/reset_wall_types_for_reapply.py` — contains `if __name__ == "__main__":` guard
- `tools/run_extract_all.py` — contains `if __name__ == "__main__":` guard
- `tools/run_segment_orchestrator.py` — contains `if __name__ == "__main__":` guard
- `tools/run_split_detection_all.py` — contains `if __name__ == "__main__":` guard
- `tools/similarity_compare.py` — contains `if __name__ == "__main__":` guard
- `tools/suggest_discovery_params.py` — contains `if __name__ == "__main__":` guard

## Directories by role (heuristic, top-level only)

_Derived only from file classification counts in each top-level directory; not a claim about architecture._

- test: dev_tools, tests, tools
- documentation: (repository root), .agents, .codex, .copilot, .github, audit_results, contracts, dev_tools, docs, policies, reference, tests, tools
- configuration: .github
- json_schema: contracts
- script_shell: .claude, .codex
- script_powershell: tools
- python_source: (repository root), core, dev_tools, domains, legacy, mapping, runner, scripts, tools, validators

## Unusually large or structurally complex Python modules (177)

_Heuristic only: line count > 800 or any symbol with approximate cyclomatic complexity > 15._

- `core/canonical_items.py` — 151 lines, max symbol complexity ~20
- `core/collect.py` — 595 lines, max symbol complexity ~36
- `core/context.py` — 186 lines, max symbol complexity ~25
- `core/deployment_config.py` — 92 lines, max symbol complexity ~17
- `core/dimension_type_helpers.py` — 1091 lines, max symbol complexity ~42
- `core/join_key_builder.py` — 333 lines, max symbol complexity ~38
- `core/join_key_policy.py` — 296 lines, max symbol complexity ~21
- `core/manifest.py` — 100 lines, max symbol complexity ~16
- `core/record_v2.py` — 639 lines, max symbol complexity ~18
- `core/rows.py` — 333 lines, max symbol complexity ~16
- `core/sig_hash_builder.py` — 123 lines, max symbol complexity ~25
- `core/vg_sig.py` — 462 lines, max symbol complexity ~18
- `dev_tools/repo_context/rc_chunking.py` — 199 lines, max symbol complexity ~24
- `dev_tools/repo_context/rc_classify.py` — 106 lines, max symbol complexity ~21
- `dev_tools/repo_context/rc_overview.py` — 245 lines, max symbol complexity ~42
- ... and 162 more

