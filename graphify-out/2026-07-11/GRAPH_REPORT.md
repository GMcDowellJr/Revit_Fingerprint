# Graph Report - .  (2026-07-11)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 4006 nodes · 9670 edges · 242 communities (214 shown, 28 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 218 edges (avg confidence: 0.74)
- Token cost: 13,911 input · 2,257 output

## Graph Freshness
- Built from commit: `59193c53`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Line Style Extraction
- Wall Object Management
- Hash Key Generation
- Pareto Key Ranking
- Canonical Item Processing
- Cache Key Management
- Value Canonicalization
- Schema Definition
- Record Structuring
- Domain Identity Management
- Governance Testing
- Cluster Analysis
- Revit Value Canonicalization
- Timing Data Collection
- Join Key Schema
- Domain Edge Analysis
- Dimension Type Processing
- Cluster Similarity Detection
- Domain Contract Management
- Cluster Threshold Testing
- Join Key Evaluation
- Record Assembly
- Semantic Group Building
- Segment Orchestration
- Domain Record Management
- Dimension Label Formatting
- Analysis Metrics Computation
- Cluster Label Annotation
- Join Key Migration Testing
- View Template Comparison
- Record ID Generation
- Segment Manifest Testing
- Attribute Stability Analysis
- Number Canonicalization
- Profile Building
- Domain Payload Management
- Layer Stack Testing
- Discipline Container Management
- Cluster Review Preparation
- Signal Clustering
- Graphic Override Management
- Fragmented Label Synthesis
- Governance Narrative Generation
- Domain Index Building
- Governance State Comparison
- View Category Probing
- Reference Standards Building
- Dimension Type Probing
- Unit Probing
- Comparison Registry Testing
- Graphic Attribute Extraction
- LFT Similarity Analysis
- Governance Report Generation
- Role Policy Management
- Bundle Analysis
- Domain Prompt Building
- Dimension Formatting
- Text Type Probing
- Signature Hash Management
- View Template Testing
- Placeholder Exclusion Management
- Material Key Discovery
- Line Pattern Probing
- Object Style Probing
- Record Contract Testing
- Fill Pattern Probing
- Manifest Row Testing
- Split Domain Policy Testing
- Export File Management
- Join Key Derivation
- Browser Organization Probing
- VFD Edge Discovery
- View Category Overrides
- Line Style Probing
- Cluster Label Backfill
- Phase 1 Diagnosis
- Export Merging
- Bundle Analysis Helpers
- Comparison Pair Management
- JSON Comparison
- Shared IO Helpers
- Comparison Engine
- Domain Profile Management
- View Filter Definition Probing
- Export File Loading
- Phase Filter Probing
- Phase Graphics Probing
- Corpus Normalization Testing
- Bundle Schema Detection
- Config Probing
- Run Configuration
- View Template Comparison
- Element Classification
- Dependency Management
- Domain Pattern Patching
- Fingerprint Compression
- Material Migration
- NA Token Testing
- Bundle Overlap Comparison
- Collision Differencing
- Join Key Analysis
- Domain Hints and Guidance
- Domain Label Lookup
- Arrowhead Type Probing
- Parameter Definition Classification
- Architecture Overview
- Feature Extraction
- Policy Validation
- Join-Key Discovery
- Domain Patterns Patching
- Family Types Probing
- Phase Probing
- View Template Probing
- Repository Path Probing
- Family Mapping Tests
- Policy Discovery Tests
- Desktop Connector Scanner
- Desktop Connector Sync Tool
- Population Framing
- Domain Similarity Comparison
- Manifest Comparison
- Element Dominance Emission
- Intradomain Summary
- Join Hash Parameter Extraction
- Step Template Governance
- CSV Contract Analysis
- Dimension Types
- Results Registry Management
- Example Workflows
- Reference Bundle Management
- View Context Management
- Filename Generation
- Membership Row Tests
- Shape Constant Testing
- Hashing Tests
- Governance State Summary
- Pareto Analysis
- Typography Surface Extraction
- View Category Overrides Analysis
- Drift Scoring
- Color Conversion
- RevitLookup Sync
- Record Extraction
- Pairwise Drift Analysis
- View Filter Definitions
- Identity Items Lookup
- Object Styles Management
- Unit System Testing
- Element ID Validation
- Wall Type Reset
- Join Key Application
- Synthetic Key Computation
- Intradomain Definition Emission
- Candidate Join Key Simulation
- Frequent Itemset Finding
- Graphify Reference Management
- RevitLookup Audit Tests
- Schedule Row Selection
- Pairwise Analysis
- Identity Item Matching
- Shape Input Calibration
- Core Principles Overview
- Legacy Tools Overview
- Refactor Strategy
- Norm Hash Precision Computation
- Configuration Example
- Module Purging
- Text Types Export Testing
- Cross-Domain Analysis
- Client Onboarding Profile
- Contract Validation Tests
- Identity Management
- Governance Role Patterns
- View Probing
- Bundle Pattern Classification Tests
- Integration Test Patterns
- Filtered Element Collector Tests
- Sentinel Policy Tests
- Signature Hash Policy Generation
- Join Key Calibration
- Details to CSV Conversion
- Documentation Overview
- Settings Management
- Hook Configuration
- Label Population Building
- First Record Extraction
- Pareto Shape Gating Tests
- Bundle Analysis Post-Processing
- Operational Review
- Hook Check Script
- Setup Script
- Similarity Comparison
- Session Start Script
- Post-Export Analysis Helpers
- Graph Generation
- Commit Practices
- Completion Criteria
- Execution Stages
- Join Key Phase 2
- Identity & Semantics Refactor
- Legacy Tools
- Tools Mapping Phases
- Code Refactoring
- Descriptor to Domain Mapping
- Join Key Dimension Types
- Verification Strategy

## God Nodes (most connected - your core abstractions)
1. `make_identity_item()` - 110 edges
2. `make_hash()` - 105 edges
3. `safe_str()` - 98 edges
4. `serialize_identity_items()` - 88 edges
5. `build_join_key_from_policy()` - 79 edges
6. `_build_segments()` - 75 edges
7. `get_domain_join_key_policy()` - 73 edges
8. `canonicalize_str()` - 65 edges
9. `canonicalize_int()` - 51 edges
10. `build_record_v2()` - 51 edges

## Surprising Connections (you probably didn't know these)
- `test_blank_unit_system_excluded()` --calls--> `_build_segments()`  [INFERRED]
  tests/test_build_segment_manifest.py → tools/build_segment_manifest.py
- `test_make_hash_accepts_generator_large_input_sanity()` --calls--> `make_hash()`  [EXTRACTED]
  tests/test_hashing_incremental.py → core/hashing.py
- `test_make_hash_deterministic_repeated_calls()` --calls--> `make_hash()`  [EXTRACTED]
  tests/test_hashing_incremental.py → core/hashing.py
- `test_make_hash_is_order_sensitive_contract()` --calls--> `make_hash()`  [EXTRACTED]
  tests/test_hashing_incremental.py → core/hashing.py
- `load_join_key_policies()` --references--> `Path`  [EXTRACTED]
  core/join_key_policy.py → tools/_archive/pareto_joinkey_search.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Architecture Layers** — layer_0_core, layer_1_domain_extractors, layer_2_context_builder, layer_3_runner [EXTRACTED 0.75]
- **Changelog and Decisions** — changelog, decisions [EXTRACTED 0.75]
- **Governance Documentation** — invariants, decisions, repo_operational_review [EXTRACTED 0.75]
- **Cross-Segment Analysis** — docs_cross_segment_comparison, tools_compare_cross_segment, docs_phase_2_join_keys, docs_phase_2_join-key_discovery [EXTRACTED 0.75]
- **Archetype Discovery and Analysis Pipeline** — tools_archetype_readme, tools_bundle_analysis_readme, tools_compare_templates_stand_alone_compare_view_templates_stand_alone_report [EXTRACTED 0.75]

## Communities (242 total, 28 thin omitted)

### Community 0 - "Line Style Extraction"
Cohesion: 0.06
Nodes (136): extract(), _line_pattern_segment_kind_id(), _line_pattern_synopsis_from_element(), _line_pattern_synopsis_from_segments(), Extract Line Styles fingerprint from document.      record.v2 surfaces:       -, _binding_scope(), _build_param_key(), extract() (+128 more)

### Community 1 - "Wall Object Management"
Cohesion: 0.07
Nodes (63): object, _basic_wall(), _CS, _CSWrapError, _default_ctx(), _Doc, _Id, _Layer (+55 more)

### Community 2 - "Hash Key Generation"
Cohesion: 0.08
Nodes (68): Return (required_items, optional_items, explicitly_excluded_items) for a domain., make_hash(), _make_hash_impl(), Deterministic hash based on a sequence of strings.      Streaming/incremental im, Inner hash implementation (separated for timing wrapper clarity)., build_join_key_from_policy(), _dedupe_preserve_order(), _expand_sequence_key() (+60 more)

### Community 3 - "Pareto Key Ranking"
Cohesion: 0.06
Nodes (71): build_wide_kv_table(), compute_v_norm(), _dedupe_preserve_order(), dominates(), eval_subset(), EvalConfig, main(), pareto_front() (+63 more)

### Community 4 - "Canonical Item Processing"
Cohesion: 0.07
Nodes (53): build_flat_items(), canonicalize_record(), compile_role_policy(), merge_legacy_buckets(), _normalize_item(), Any, Resolve roles from item.k via runtime lookup.      Returns grouped items without, Canonicalize a record to flat `items` shape and remove legacy/derived keys. (+45 more)

### Community 5 - "Cache Key Management"
Cohesion: 0.06
Nodes (30): CacheKey, collect_elements(), collect_id_ints(), _collect_id_ints_uncached(), CollectCtx, _get_element(), _is_invalid_element_id(), is_type_purgeable() (+22 more)

### Community 6 - "Value Canonicalization"
Cohesion: 0.06
Nodes (55): fnum(), Legacy alias for canon_num., canonicalize_bool(), canonicalize_enum(), Canonicalize boolean values for IdentityItem.v.      Returns:         ("true"|"f, Canonicalize enum-like values for IdentityItem.v.      This is intentionally con, _as_bool_from_param(), _as_int() (+47 more)

### Community 7 - "Schema Definition"
Cohesion: 0.06
Nodes (55): additionalProperties, allOf, $id, const, items, minItems, minLength, pattern (+47 more)

### Community 8 - "Record Structuring"
Cohesion: 0.06
Nodes (51): sig_hash_keys, sig_hash_schema, _block_record_for_unstable_id(), block_record_v2(), canonical_structural_fields(), _canonical_structural_value(), compute_identity_quality(), _default_record_id_secondary_key() (+43 more)

### Community 9 - "Domain Identity Management"
Cohesion: 0.11
Nodes (53): banned_identity_value_substrings, domains, ceiling_types, dimension_types_angular, dimension_types_diameter, dimension_types_linear, dimension_types_spot_coordinate, dimension_types_spot_elevation (+45 more)

### Community 10 - "Governance Testing"
Cohesion: 0.08
Nodes (53): Path, Tests for governance semantics in tools/compare_cross_segment.py., _seg(), test_build_governance_state_rows_include_inherited_unused_and_local_active(), test_density_similarity_uses_domain_density_vectors_not_containment(), test_discover_governance_chain_falls_back_to_collection_label_for_na_client(), test_discover_governance_chain_includes_generic_upstream_roles(), test_explicit_matrices_union_jaccard_differs_from_mean_file_pair() (+45 more)

### Community 11 - "Cluster Analysis"
Cohesion: 0.07
Nodes (51): build_distance_matrix_from_similarity(), Cluster, cluster_assignments_to_labels(), compute_silhouette_score(), extract_dates_from_paths(), extract_metadata_patterns(), build_element_profiles(), compute_avg_between_cluster_similarity() (+43 more)

### Community 12 - "Revit Value Canonicalization"
Cohesion: 0.06
Nodes (46): sig_hash_key_prefixes, canon_bool(), canon_id(), is_sentinel(), Any, Canonicalize Revit ElementId-like values to a decimal string.      Accepts:, Create RGB signature string from a Revit Color object., Create RGB dict from a Revit Color object.      Returns None on missing/unreadab (+38 more)

### Community 13 - "Timing Data Collection"
Cohesion: 0.06
Nodes (21): Any, Set the currently executing domain for sub-timing scoping., Return structured timing report.          Returns a dict with:           - ``tot, Build the structured report (must hold lock)., Collects hierarchical timing data for extraction runs.      Labels follow the co, Begin timing a labeled operation., End timing and record duration for a labeled operation., Record a pre-computed elapsed duration directly.          Used for hot-loop accu (+13 more)

### Community 14 - "Join Key Schema"
Cohesion: 0.10
Nodes (51): category, evidence, importance, domain, dim_attr.tick_mark_uid, dim_type.name, dim_type.tick_mark_uid, explicitly_excluded_items (+43 more)

### Community 15 - "Domain Edge Analysis"
Cohesion: 0.11
Nodes (50): atomic_write_csv(), bool_s(), build_domain_gap_rows(), build_edge_rows(), build_inventory_rows(), build_unresolved_file_rows(), _candidate_category_details(), canonical_param_kind() (+42 more)

### Community 16 - "Dimension Type Processing"
Cohesion: 0.15
Nodes (49): purge_lookup(), _build_text_appearance_items(), _fmt_float(), _fmt_in_from_ft(), _format_options_to_kv(), _get_dimension_shape(), get_type_display_name(), Detect dimension shape from a Revit DimensionType object.      Revit exposes sha (+41 more)

### Community 17 - "Cluster Similarity Detection"
Cohesion: 0.07
Nodes (49): build_distance_matrix_from_similarity(), Cluster, cluster_assignments_to_labels(), compute_silhouette_score(), extract_dates_from_paths(), extract_metadata_patterns(), compute_avg_between_cluster_similarity(), compute_avg_internal_similarity() (+41 more)

### Community 18 - "Domain Contract Management"
Cohesion: 0.09
Nodes (44): add_bounded_error(), compute_run_status(), DiagError, _ensure_list(), new_domain_envelope(), new_run_diag(), new_run_envelope(), Any (+36 more)

### Community 19 - "Cluster Threshold Testing"
Cohesion: 0.11
Nodes (41): Path, test_compute_alignment_rates_and_contract_header_preserves_is_named_cluster(), test_compute_alignment_rates_falls_back_to_percentage_when_size_absent(), test_compute_alignment_rates_falls_back_to_percentage_when_size_values_are_invalid(), test_compute_alignment_rates_uses_raw_share_or_size_for_unrounded_result(), test_thresholds_breaks_and_ordering(), test_thresholds_reject_non_three_classes(), _write_csv() (+33 more)

### Community 20 - "Join Key Evaluation"
Cohesion: 0.08
Nodes (32): compute_coverage(), compute_join_hash_for_record(), evaluate_gates(), evaluate_keyset(), extract_identity_map(), greedy_select_keys(), md5_utf8_join_pipe(), Bootstrap by sampling files (not individual records) to preserve lineage structu (+24 more)

### Community 21 - "Record Assembly"
Cohesion: 0.13
Nodes (41): collect_types(), Convert any value to a string representation safely.      Handles both str and u, safe_str(), build_record_v2(), canonicalize_float(), Canonicalize a float-like value for IdentityItem.v.      Returns:         (value, Assemble a record.v2 structure.      This helper does not compute sig_hash; call, _attach_placeholder_metadata() (+33 more)

### Community 22 - "Semantic Group Building"
Cohesion: 0.11
Nodes (41): build_grouping_prompt(), build_semantic_groups(), _call_grouping_llm(), _derive_element_label(), _extract_behavioral_props(), _infer_fill_geometry_description(), _is_fill_angle_close(), _is_nullish() (+33 more)

### Community 23 - "Segment Orchestration"
Cohesion: 0.09
Nodes (41): CompletedProcess, Lock, Namespace, _active_domains_from_presence_csv(), _build_patterns_missing_notes(), build_run_plan(), load_manifest(), load_membership() (+33 more)

### Community 24 - "Domain Record Management"
Cohesion: 0.06
Nodes (37): build_purgeable_id_set(), Builds a frozenset of ElementId.IntegerValue (int) for all elements     currentl, _canonicalize_all_domain_records(), _domain_run(), _enabled(), _ensure_parent_dir(), _extract_legacy_quality(), _extract_v2_block_reasons() (+29 more)

### Community 25 - "Dimension Label Formatting"
Cohesion: 0.06
Nodes (34): format_synopsis(), _inches_to_fraction(), tools/label_synthesis/synopsis_formatters/arrowheads.py  Behavioral synopsis for, _accuracy_label(), _center_marks_label(), _decoration_label(), _extract_kv(), format_synopsis() (+26 more)

### Community 26 - "Analysis Metrics Computation"
Cohesion: 0.12
Nodes (38): Any, compute_attribute_concentration_metrics(), compute_effective_clusters(), compute_hhi_from_shares(), emit_analysis(), emit_records(), _extract_acc_project_label(), _file_id() (+30 more)

### Community 27 - "Cluster Label Annotation"
Cohesion: 0.09
Nodes (37): _clean_text(), _extract_cluster_id(), _first_non_noise(), _identity_items_from_representatives(), _is_unknown(), _iter_domains(), _key_suffix(), main() (+29 more)

### Community 28 - "Join Key Migration Testing"
Cohesion: 0.05
Nodes (25): Tests for _phase2_partition_items function., Semantic items must include baseline refs and override_properties_hash., Cosmetic items must include individual delta properties., Tests for view_templates join_key structure., join_hash must be a 32-char hex string (MD5)., For v1 policy, join_hash must equal def_hash., Tests that join_key policies are properly defined., view_category_overrides policy must exist in policies file. (+17 more)

### Community 29 - "View Template Comparison"
Cohesion: 0.09
Nodes (38): _best_match_index(), _build_html(), _diff_dicts(), _diff_vco(), _esc(), _extract_records(), _get_label_component(), _get_label_display() (+30 more)

### Community 30 - "Record ID Generation"
Cohesion: 0.15
Nodes (37): canon_str(), Canonicalize string-like values.      Rules:     - None -> <MISSING>     - str(., collect_instances(), make_record_id_from_element(), Create a stable record_id from a Revit element.      Priority:       1) UniqueId, emit_builtin_params(), emit_shared_params_stub(), Emit include-flag + value items for built-in params for a domain. (+29 more)

### Community 31 - "Segment Manifest Testing"
Cohesion: 0.09
Nodes (28): _membership_ids(), _meta_row(), Path, Tests for tools/build_segment_manifest.py., Read segment_membership.csv and return the export_run_id set for one segment_id., _read_csv(), test_blank_client_label_level2_id_distinct_from_level1(), test_blank_unit_system_excluded() (+20 more)

### Community 32 - "Attribute Stability Analysis"
Cohesion: 0.11
Nodes (31): AttrStabilityRow, compute_attr_stability(), compute_stress_rank(), StressRow, ChangeCounts, classify_pair(), _phase2_items_map(), Return k -> (q,v) across phase2 buckets, and duplicate_k_count.      Used for eq (+23 more)

### Community 33 - "Number Canonicalization"
Cohesion: 0.09
Nodes (31): canon_num(), Canonicalize numbers to a fixed decimal string.      - None -> <MISSING>     - C, phase2_qv_from_legacy_sentinel_str(), phase2_sorted_items(), Return IdentityItem-like dicts sorted by key 'k'., Map legacy sentinel strings to record.v2-safe (v,q) without emitting sentinel li, _canonical_identity_items_from_signature(), _compute_delta_items() (+23 more)

### Community 34 - "Profile Building"
Cohesion: 0.09
Nodes (33): _build_synthetic_items_for_pair(), _build_template_lookup(), _extract_active_vco_fields(), _extract_graphic_fields(), _extract_object_style_baseline_fields(), _get_domain_payload(), _get_identity_item_value(), _get_phase2_cosmetic_value() (+25 more)

### Community 35 - "Domain Payload Management"
Cohesion: 0.12
Nodes (28): get_domain_payload(), get_domain_records(), load_exports(), Return the domain payload (legacy surface) if present., Extract record.v2 records list from the domain payload.      Notes:     - Contra, Load all monolithic exports in a directory (each file = one authority sample)., _discover_families_from_exports(), _families_present_in_baseline() (+20 more)

### Community 36 - "Layer Stack Testing"
Cohesion: 0.17
Nodes (31): _make_layer_row(), _make_wall_record(), Any, Path, Single type with simple layers emits one stack row and correct layer rows., Two types sharing the same stack_hash_loose collapse to one stack row with type_, wall_types and floor_types each emit separate rows distinguished by domain., layer_stacks is NOT written when --emit uses the default set. (+23 more)

### Community 37 - "Discipline Container Management"
Cohesion: 0.09
Nodes (32): _disc_rows(), Multi-client, multi-discipline Container corpus for discipline tests., test_blank_discipline_does_not_generate_discipline_cut(), test_client_discipline_leaf_label_container(), test_client_discipline_leaf_no_empty_purpose(), test_client_discipline_leaf_purpose_container(), test_discipline_cut_extra_dimensions_populated(), test_discipline_cut_level3_bundle_not_demoted_by_children() (+24 more)

### Community 38 - "Cluster Review Preparation"
Cohesion: 0.13
Nodes (29): _all_cluster_ids(), _all_clusters(), _build_cluster_context(), _build_curated_gq_map(), ClusterContext, _find_cluster(), _governance_question_from_archetype_id(), _governance_question_from_cluster_id() (+21 more)

### Community 39 - "Signal Clustering"
Cohesion: 0.11
Nodes (29): _apply_threshold(), _bare_signal_name(), _build_clusters(), _build_coverage_summary(), _build_curated_gq_map(), _build_detail_files_lookup(), _build_n_files_classified_lookup(), _build_signal_cluster_map() (+21 more)

### Community 40 - "Graphic Override Management"
Cohesion: 0.15
Nodes (29): _append_color_item(), _append_pattern_items(), _append_value_item(), extract_cut_graphics(), extract_projection_graphics(), _is_category(), _is_invalid_element_id(), _is_ogs() (+21 more)

### Community 41 - "Fragmented Label Synthesis"
Cohesion: 0.11
Nodes (28): _call_llm(), _collect_union_bundle_join_hashes(), _generic_build_prompt(), _generic_system_prompt(), _get_domain_records(), _groups_vocab_path(), _load_domain_prompt_module(), _load_governance_join_hashes() (+20 more)

### Community 42 - "Governance Narrative Generation"
Cohesion: 0.15
Nodes (27): assign_tier(), detect_anomalies(), fmt(), _has_material_state_exception(), main(), normalise_summary_schema(), pct(), generate_governance_narrative.py  Deterministic governance narrative renderer fo (+19 more)

### Community 43 - "Domain Index Building"
Cohesion: 0.12
Nodes (21): build_domain_index(), _get_join_hash(), Build a per-file join_hash index for one domain., Return a map k -> (q, v) across concatenated phase2 item buckets.      Also retu, _phase2_items_map_no_dups(), Return k -> (q, v) across all phase2 buckets.      If duplicate k is detected wi, extract_phase2_items(), Return k -> (q, v) map across all phase2 buckets.     Assumes caller has already (+13 more)

### Community 44 - "Governance State Comparison"
Cohesion: 0.16
Nodes (27): Phase 0–6: Questions Each Phase Can Answer, Cross-Segment Comparison, test_project_target_governance_state_uses_target_used(), test_standards_carrier_target_avoids_passive_bloat_label(), _bool_str(), build_governance_state_outputs(), _build_summary_row(), build_union_inventory_rows() (+19 more)

### Community 45 - "View Category Probing"
Cohesion: 0.16
Nodes (25): _baseline_for_cat(), _bool_int(), _bucket_for_view(), _category_path(), _contract_eid(), _contract_int(), _contract_missing(), _contract_string() (+17 more)

### Community 46 - "Reference Standards Building"
Cohesion: 0.12
Nodes (25): build_reference_standards_from_clusters(), main(), Build reference standards from cluster representatives., get_contract(), get_domain_envelope(), get_domains_map(), get_run_provenance(), iter_json_paths() (+17 more)

### Community 47 - "Dimension Type Probing"
Cohesion: 0.13
Nodes (23): _example_score(), _find_tick_param(), _fmt_display(), _format_param_contract(), _format_synth_contract(), _get_dim_shape_info(), _get_family_name_param(), _is_angle_datatype() (+15 more)

### Community 48 - "Unit Probing"
Cohesion: 0.21
Nodes (26): _discover_specs(), _forge_id_string(), _is_forge_type_id(), _label_for_discipline_id(), _label_for_spec_id(), _maybe_set_example(), _pv_from_bool(), _pv_from_double() (+18 more)

### Community 49 - "Comparison Registry Testing"
Cohesion: 0.16
Nodes (25): ComparisonRegistryKey, Tests for comparison_registry.csv staleness tracking in tools/compare_cross_segm, _reg_row(), test_build_comparison_registry_rows_domain_scoped_run_omits_other_domains(), test_build_comparison_registry_rows_is_a_full_snapshot_no_carryover(), test_build_comparison_registry_rows_omits_pair_when_reference_segment_is_pending(), test_build_comparison_registry_rows_omits_pair_when_target_segment_is_failed(), test_build_comparison_registry_rows_omits_work_items_with_no_output() (+17 more)

### Community 50 - "Graphic Attribute Extraction"
Cohesion: 0.11
Nodes (12): extract_halftone(), extract_transparency(), Safely read an attribute, returning (value, q)., Try multiple attribute names and return the first one found., Extract halftone override from a Category or OverrideGraphicSettings., Extract transparency override from a Category or OverrideGraphicSettings., _read_attr(), _read_first_attr() (+4 more)

### Community 51 - "LFT Similarity Analysis"
Cohesion: 0.14
Nodes (25): build_class_profiles(), build_dim_summaries(), build_exact_match_table(), build_family_file_detail(), build_name_cluster_table(), build_subgroups(), extract_category(), extract_family_name() (+17 more)

### Community 52 - "Governance Report Generation"
Cohesion: 0.14
Nodes (17): _build_html_report(), build_pattern_table(), build_table(), _get_identity_value(), main(), _pattern_row_template(), ProjectExport, Standards Governance Report Generator  Analyzes fingerprint exports to: 1. Detec (+9 more)

### Community 53 - "Role Policy Management"
Cohesion: 0.16
Nodes (24): ceiling_types, fill_patterns_drafting, fill_patterns_model, fill_pattern.source_element_id, fill_pattern.source_unique_id, floor_types, _notes, phases (+16 more)

### Community 54 - "Bundle Analysis"
Cohesion: 0.20
Nodes (25): bundle_analysis_dir(), discover_domains_for_segment(), discover_within_project(), domain_patterns_path(), load_bundle_join_hash_set(), _load_export_run_ids_for_segment(), load_file_join_hashes(), load_file_metadata() (+17 more)

### Community 55 - "Domain Prompt Building"
Cohesion: 0.16
Nodes (20): build_prompt(), _detect_record_class(), _fmt_size(), _format_identity_items(), tools/label_synthesis/domain_prompts/arrowheads.py  LLM system prompt and prompt, build_prompt(), _extract_grid_geometry(), _get_identity_value() (+12 more)

### Community 56 - "Dimension Formatting"
Cohesion: 0.15
Nodes (20): build_prompt(), _fmt_accuracy(), _fmt_witness(), _format_identity_items(), _get_shape(), Return a brief shape-specific note to insert before the parameters., _shape_context_note(), build_prompt() (+12 more)

### Community 57 - "Text Type Probing"
Cohesion: 0.13
Nodes (14): _find_leader_arrow_param(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _looks_like_text_type(), Contract:       {         "q": "ok|missing|unreadable|unsupported",         "sto, Best-effort parse for Revit integer color surfaces.     Assumes a 24-bit packed (+6 more)

### Community 58 - "Signature Hash Management"
Cohesion: 0.19
Nodes (19): apply_sig_hash_policy_to_record(), build_sig_hash_from_policy(), _items_to_map(), _key_allowed(), Any, Return (sig_hash, status, status_reasons, hash_items).      The builder hashes e, Mutate and return a canonical record dict with policy-generated sig_hash/status., get_domain_sig_hash_policy() (+11 more)

### Community 59 - "View Template Testing"
Cohesion: 0.13
Nodes (21): _load_policy(), Join key build must report missing when view_template.def_hash is absent., All view_templates split domains must have a valid join_key_schema., All split view_template domains must require view_template.def_hash in join key., Floor/structural/area plans policy must use view_template.def_hash., Ceiling plans policy must use view_template.def_hash., Elevations/sections/detail policy must use view_template.def_hash., Renderings/drafting policy must use view_template.def_hash. (+13 more)

### Community 60 - "Placeholder Exclusion Management"
Cohesion: 0.19
Nodes (20): atomic_write_csv(), read_csv_rows(), _choose_threshold(), compute_placeholder_exclusions(), compute_placeholder_exclusions(), _is_truthy(), _largest_gap_threshold(), main() (+12 more)

### Community 61 - "Material Key Discovery"
Cohesion: 0.20
Nodes (18): _build_key(), _build_key_files(), _compute_metrics(), discover(), _extract_sig(), _is_system_material(), _load_class_map(), _load_materials() (+10 more)

### Community 62 - "Line Pattern Probing"
Cohesion: 0.15
Nodes (16): _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _iter_line_style_categories(), _linepattern_signature(), _lp_seg_type_id_and_name(), _maybe_set_example() (+8 more)

### Community 63 - "Object Style Probing"
Cohesion: 0.14
Nodes (9): _category_type_label(), _contract_eid(), _eid_name(), _get_name(), _infer_object_styles_tab(), _iter_categories(), Return a human-readable CategoryType label.     Handles environments where str(C, Best-effort classification into the Object Styles UI tabs.     Heuristic only. U (+1 more)

### Community 64 - "Record Contract Testing"
Cohesion: 0.15
Nodes (16): exported_fingerprint_json(), Provide exporter output JSON for validation.      Options:       1) Set env var, test_all_exported_records_conform_to_record_contract_v2(), _compute_identity_quality(), _hash_preimage(), _is_allowed_indexed_key(), _normalize_indexed_key(), Any (+8 more)

### Community 65 - "Fill Pattern Probing"
Cohesion: 0.19
Nodes (18): _add_computed_surface(), _bucket_key_for_fill_pattern(), _contract_from_value(), _ensure_entry(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype() (+10 more)

### Community 66 - "Manifest Row Testing"
Cohesion: 0.10
Nodes (20): _manifest_row(), Hand-craft a manifest-row-shaped dict for testing _build_registry() in     isola, test_conformance_reference_mode_carried_over_across_runs(), test_conformance_reference_mode_defaults_to_latest_for_new_segment(), test_conformance_reference_mode_defaults_to_latest_for_old_registry_missing_field(), test_registry_drops_removed_segment_ids_with_warning(), test_registry_excludes_skip_segments(), test_registry_first_run_no_existing_file_unaffected() (+12 more)

### Community 67 - "Split Domain Policy Testing"
Cohesion: 0.14
Nodes (11): Test join key policies for each split dimension_types domain., Load join key policy for a specific split domain., Linear domain must require witness_line_control., Radial domain must require center_marks and center_mark_size., Angular domain must require unit_format_id., Diameter domain must have a valid policy., Spot elevation domain must have a valid policy., Spot coordinate domain must have a valid policy. (+3 more)

### Community 68 - "Export File Management"
Cohesion: 0.23
Nodes (19): _atomic_write_csv(), _discover_domains(), _int_safe(), _iter_identity_csv(), _load_identity_items(), _load_label_population(), _load_pattern_map(), _load_representative_map() (+11 more)

### Community 69 - "Join Key Derivation"
Cohesion: 0.30
Nodes (18): _as_str_list(), choose_candidate_deterministically(), choose_record_handle(), derive_join_keys(), expand_globs(), extract_file_id(), is_usable_q(), load_join_key_policies() (+10 more)

### Community 70 - "Browser Organization Probing"
Cohesion: 0.19
Nodes (17): _append_unique(), _best_name(), _builtin_label(), _clean_name(), _increment_count(), Resolve a built-in parameter label when Revit exposes one., Pick display name, preserving real folder names before descriptor fallbacks., Classify and resolve a FolderItemInfo.ElementId. (+9 more)

### Community 71 - "VFD Edge Discovery"
Cohesion: 0.20
Nodes (17): read_csv(), test_discover_vfd_edges_applies_threshold_after_category_aggregation(), test_discover_vfd_edges_category_file_count_controls_generator_threshold(), test_discover_vfd_edges_emits_multi_domain_conflict_rows(), test_discover_vfd_edges_filters_hint_comments_and_exact_bip_lookup(), test_discover_vfd_edges_gaps_multi_domain_identity_items_missing(), test_discover_vfd_edges_ignores_unusable_category_rows(), test_discover_vfd_edges_ignores_unusable_param_ref_rows_with_item_quality() (+9 more)

### Community 72 - "View Category Overrides"
Cohesion: 0.24
Nodes (15): notes, _safe_bool(), _safe_bool(), view_category_overrides_model, vco.category_path, vco.cut.line_color.rgb, vco.cut.line_pattern_ref.sig_hash, vco.cut.line_weight (+7 more)

### Community 73 - "Line Style Probing"
Cohesion: 0.20
Nodes (16): _bucket_key(), _contract_value(), _format_param_contract(), _get_lines_category_id(), _hex_rgb_from_triplet(), _index_param(), _is_angle_datatype(), _is_length_datatype() (+8 more)

### Community 74 - "Cluster Label Backfill"
Cohesion: 0.27
Nodes (15): _build_cluster_representative_items(), _build_discriminator_lookup(), _extract_cluster_common_path_parts(), _file_map(), _iter_domains(), _load_json(), main(), _normalize_parts() (+7 more)

### Community 75 - "Phase 1 Diagnosis"
Cohesion: 0.24
Nodes (16): check_domain_records(), check_phase1_compatibility(), diagnose_exports(), extract_domains_from_fp(), load_json(), main(), predict_phase1_output(), Load JSON file safely. (+8 more)

### Community 76 - "Export Merging"
Cohesion: 0.18
Nodes (16): find_split_pairs(), load_json(), main(), merge_fingerprints(), print_summary(), Verify merged fingerprint has expected structure.          Returns: List of issu, Merge all split exports in input_dir into monolithic format in output_dir., Print summary of merge operation. (+8 more)

### Community 77 - "Bundle Analysis Helpers"
Cohesion: 0.20
Nodes (11): resolve_analysis_run_id(), build_bundle_share_profile(), _fmt_float(), _is_true(), main(), _parse_args(), _compute_gap_rows(), run_compare_for_domain() (+3 more)

### Community 78 - "Comparison Pair Management"
Cohesion: 0.18
Nodes (17): ComparisonPair, test_output_row_sort_helpers_are_stable_by_content(), test_pair_domain_work_items_use_pair_domain_union(), build_pair_domain_work_items(), build_pattern_reuse_summary_rows(), _classify_delta(), deduplicate_pairs(), discover_governance_chain() (+9 more)

### Community 79 - "JSON Comparison"
Cohesion: 0.24
Nodes (15): _canon_obj(), canonical_json_bytes(), compare_json(), diff_paths(), pretty_json(), Returns (equal, summary)     summary includes stable hashes and bounded diffs., Return an object that is stable under json.dumps(sort_keys=True)., Canonical JSON encoding used for hashing and deterministic file writes. (+7 more)

### Community 80 - "Shared IO Helpers"
Cohesion: 0.18
Nodes (13): atomic_write_csv(), atomic_write_json(), build_edge_aliases(), is_valid_item(), Shared IO/logging helpers for the cross-domain archetype discovery pipeline.  Co, Strip a trailing "_drafting"/"_model" suffix; None if neither present., Build edge_id -> canonical_edge_id and canonical -> [collapsed edge_ids].      T, True if an identity_items row carries usable evidence of a value. (+5 more)

### Community 81 - "Comparison Engine"
Cohesion: 0.28
Nodes (15): build_index(), compare_entries(), ensure_str(), get_items(), get_label_and_quality(), load_json(), normalize_name(), _pair_name() (+7 more)

### Community 82 - "Domain Profile Management"
Cohesion: 0.23
Nodes (3): DomainProfile, Declarative profile for comparing one domain family., Any

### Community 83 - "View Filter Definition Probing"
Cohesion: 0.24
Nodes (12): _element_filter_kind(), _flatten_element_filter(), _get_rules_from_element_parameter_filter(), _get_subfilters(), _maybe_set_example(), _observe(), Returns:       logic: "and" | "or" | "single" | "unknown"       rules: list of d, _resolve_category_name() (+4 more)

### Community 84 - "Export File Loading"
Cohesion: 0.19
Nodes (14): load_export_file(), Load one export JSON file., classify_file_elements(), compute_element_statistics(), extract_label_display(), generate_remediation_plan(), main(), Aggregate element-level classifications. (+6 more)

### Community 85 - "Phase Filter Probing"
Cohesion: 0.25
Nodes (13): _add_inventory_obs(), _fmt_display(), _format_param_contract(), _get_view_phase_filter_param(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _phase_status_bucket() (+5 more)

### Community 86 - "Phase Graphics Probing"
Cohesion: 0.26
Nodes (13): _fmt_display(), _format_param_contract(), _get_phasefilter_param_from_view(), _index_params_from_elem(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), Contract:       {         "q": "ok|missing|unreadable|unsupported",         "sto (+5 more)

### Community 87 - "Corpus Normalization Testing"
Cohesion: 0.20
Nodes (14): test_clean_corpus_unaffected_by_normalization(), test_population_hash_deterministic(), test_population_hash_in_manifest(), test_sanitize_folder_strips_path_separators(), _append_note(), _atomic_write_csv(), main(), _normalize_rows() (+6 more)

### Community 88 - "Bundle Schema Detection"
Cohesion: 0.15
Nodes (15): build_cascade(), build_client_summary(), _col(), detect_bundle_schema(), get_client(), get_disc(), is_generic(), load_corpus_counts() (+7 more)

### Community 89 - "Config Probing"
Cohesion: 0.13
Nodes (14): analysis_run_id, cluster_method, convergence_thresholds, high, medium, domains_in_scope, ignored_thresholds, hhi_max (+6 more)

### Community 90 - "Run Configuration"
Cohesion: 0.13
Nodes (14): analysis_run_id, cluster_method, convergence_thresholds, high, medium, domains_in_scope, ignored_thresholds, hhi_max (+6 more)

### Community 91 - "View Template Comparison"
Cohesion: 0.18
Nodes (9): main(), _parse_args(), DomainProfile, Declares that item keys contain sig_hashes resolvable via sibling domains., ResolutionSpec, make_vt_profile(), Domain profile for view_templates_* partitions., For VCO synthetic items (item_key contains " > "), classify by property (+1 more)

### Community 92 - "Element Classification"
Cohesion: 0.21
Nodes (13): Stream rows from a CSV file as dicts (UTF-8)., classify_file_elements(), compute_element_statistics(), extract_label_display(), generate_remediation_plan(), main(), Aggregate element-level classifications., Create actionable remediation plan for contaminated file. (+5 more)

### Community 93 - "Dependency Management"
Cohesion: 0.22
Nodes (11): Blocked, Any, core/deps.py  Centralized dependency enforcement for domain execution.  Non-nego, Typed exception used to signal a hard dependency block.      Attributes:, Require an upstream domain envelope to exist and be acceptable.      Args:, require_domain(), Exception, test_require_domain_allows_degraded_by_default() (+3 more)

### Community 94 - "Domain Pattern Patching"
Cohesion: 0.32
Nodes (13): _find_targets(), _load_cache(), _load_label_population(), main(), _patch_one(), tools/label_synthesis/patch_all_domain_patterns.py  Recursively patches pattern_, Patch one domain_patterns.csv.     Returns (n_updated, n_skipped_source, n_skipp, Return list of (domain_patterns_csv, label_synth_dir, label) tuples.     label i (+5 more)

### Community 95 - "Fingerprint Compression"
Cohesion: 0.29
Nodes (13): _compress_file(), _find_json_files(), _fmt_kb(), _is_already_compact(), _load_json(), main(), Find fingerprint JSON files, preferring *__fingerprint.json., Write compact production JSON.     Returns bytes written. (+5 more)

### Community 96 - "Material Migration"
Cohesion: 0.24
Nodes (12): _find_json_files(), _get_identity_items(), _iter_materials_records(), _load_json(), main(), _migrate_file(), _migrate_record(), Inject material.graphics_sig_hash_v2, material.class, material.keynote,     mate (+4 more)

### Community 97 - "NA Token Testing"
Cohesion: 0.22
Nodes (6): TestIsBlankOrNa, TestIsNaToken, is_blank_or_na(), is_na_token(), True for any spelling of "not applicable" (na, n/a, N/A, not applicable,     not, True if value is blank (not yet filled in) or an explicit "not     applicable" s

### Community 98 - "Bundle Overlap Comparison"
Cohesion: 0.23
Nodes (14): annotate_bundle_overlap(), compare_directed_file(), compare_symmetric_file(), _fmt(), make_comparison_run_id(), _mean(), _min(), _pct() (+6 more)

### Community 99 - "Collision Differencing"
Cohesion: 0.26
Nodes (11): CollisionGroup, _get_join_hash(), _is_scalar(), _phase2_bucket_items(), _phase2_items_map(), For each top-level key, collect variants across records.     Returns key -> {"di, JSON-ish string for CSV cells; truncates long values explicitly., k -> (q,v) map for a single bucket. Returns duplicate_k_count explicitly. (+3 more)

### Community 100 - "Join Key Analysis"
Cohesion: 0.26
Nodes (12): analyze_view_templates(), _detect_demo_plan(), _join_key_from_record(), main(), _pareto_cover(), _print_option_summary(), _print_sample_interpretation(), _project_identifier() (+4 more)

### Community 101 - "Domain Hints and Guidance"
Cohesion: 0.15
Nodes (13): vfd_bip_target_domain_hints exact_bip_id, fill_patterns domain_prompts module, wt.cfpsh in wall_types extractor, n_pairs threshold removed/raised, Join Key, Record schema version, incremental update and cluster-only, Use this repo guidance (+5 more)

### Community 102 - "Domain Label Lookup"
Cohesion: 0.28
Nodes (9): FiredEdgeRow, DomainPatternLabelCache, _evaluate_signal(), main(), Any, Path, Lazy `(domain, join_hash) -> human_label` lookup for domain patterns., Return the best `(source_join_hash, source_domain)` for a fired signal.      The (+1 more)

### Community 103 - "Arrowhead Type Probing"
Cohesion: 0.27
Nodes (9): _arrow_style_key(), _collect_dimension_types_with_tick_param(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _safe(), _safe_get_datatype(), _safe_param_def_name() (+1 more)

### Community 104 - "Parameter Definition Classification"
Cohesion: 0.24
Nodes (10): _add_inventory_record(), _as_str(), _definition_origin(), _format_param_contract(), _param_group_legacy_str(), Robust classifier that works even when Definition.BuiltInParameter is unavailabl, inv: dict param_key -> accumulator     Dedup rule (probe-local): group observati, _safe() (+2 more)

### Community 105 - "Architecture Overview"
Cohesion: 0.17
Nodes (12): Architecture Overview, Context Dictionary Schema, Dependency Contract, Dependency Direction, Design Intent, Layer 0 - Core (Pure Python), Layer 1 - Domain Extractors (Revit-aware), Layer 2 - Context Builder (+4 more)

### Community 106 - "Feature Extraction"
Cohesion: 0.35
Nodes (10): _as_dict(), _as_int(), build_features(), _extract_counts_from_legacy(), Any, Extract stable count signals from legacy domain payloads when present.      Conv, Build deterministic features from payload.      Features include:       - schema, _sample_monolithic() (+2 more)

### Community 107 - "Policy Validation"
Cohesion: 0.26
Nodes (11): _is_list_of_str(), Validate shape-gating semantics and return structured issues.      Args:, Validate shape_gating section of a policy.      Args:         domain_name: Name, validate_domain_join_key_policy(), _validate_shape_gating(), test_rule_a1_discriminator_first_required(), test_rule_a2_no_overlap_common_required(), test_rule_a3_additional_required_in_optional_items() (+3 more)

### Community 108 - "Join-Key Discovery"
Cohesion: 0.39
Nodes (10): Phase 2 — Join-Key Discovery (Summary), Refreshed Definition of Done — Revit Standards Governance Narrative Outputs, _fingerprint_payload(), Path, test_detect_surfaces_counts_fingerprint_separately(), test_domain_discovery_prefers_fingerprint_candidates(), test_iter_export_files_prioritizes_fingerprint_and_uses_none_secondary(), test_load_exports_prefers_fingerprint_files_before_plain_fallback() (+2 more)

### Community 109 - "Domain Patterns Patching"
Cohesion: 0.36
Nodes (11): _load_cache(), _load_label_population(), main(), patch(), tools/label_synthesis/patch_domain_patterns_labels.py  Targeted label patcher: u, Load joinhash_label_population.csv for a domain, keyed by join_hash., _read_csv(), _try_modal() (+3 more)

### Community 110 - "Family Types Probing"
Cohesion: 0.45
Nodes (11): _cat_info(), _element_name(), _family_record(), _family_symbols(), _format_double(), _id_int(), _normalize_double(), _param_definition_identity() (+3 more)

### Community 111 - "Phase Probing"
Cohesion: 0.30
Nodes (9): _fmt_display(), _format_param_contract(), _inv_add(), _inv_init(), _is_angle_datatype(), _is_length_datatype(), _phase_key(), _safe() (+1 more)

### Community 112 - "View Template Probing"
Cohesion: 0.30
Nodes (9): _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), Contract:       {         "q": "ok|missing|unreadable|unsupported",         "sto, _safe(), _safe_get_datatype(), _safe_view_name() (+1 more)

### Community 113 - "Repository Path Probing"
Cohesion: 0.21
Nodes (8): _candidate_repo_dirs(), _is_probably_sync_path(), _is_repo_root(), _iter_dyn_path_candidates(), _nearest_repo_root_from_path(), # NOTE: IN[3] is reserved for .dyn graph-path probing (see _iter_dyn_path_candid, Heuristic, Windows-centric: previously used to hard-block sync paths.     Retain, # NOTE: Documents is sometimes redirected into OneDrive/SharePoint.

### Community 114 - "Family Mapping Tests"
Cohesion: 0.17
Nodes (7): Test family mapping correctness via SHAPE_TO_FAMILY., Linear and LinearFixed must map to linear family., Radial, Diameter, DiameterLinked must map to radial family., Angular and ArcLength must map to angular family., Spot elevation/coordinate/slope must map to spot family., Unknown shape must map to unknown family., TestFamilyMappings

### Community 115 - "Policy Discovery Tests"
Cohesion: 0.42
Nodes (11): Path, Stratified sampling gives each family equal weight regardless of type count., test_loaded_family_types_skips_orphan_gate_buckets(), test_loaded_family_types_surfaces_missing_shape_gate_records(), test_out_policy_creates_parent_directories(), test_phase0_dir_auto_resolves_results_records(), test_phase0_dir_can_be_results_root(), test_stratify_by_limits_overrepresentation() (+3 more)

### Community 116 - "Desktop Connector Scanner"
Cohesion: 0.24
Nodes (11): load_existing_includes(), main(), parse_types(), acc_scan_dc.py — Desktop Connector / network folder scanner  Walks a root folder, Walk root, yield one dict per matching file.     Skips names starting with '~$', Return {relative_path: include_value} from an existing manifest., Expand --types argument to a set of lowercase extensions., Return the Revit version year as a string (e.g. "2025"),     "stub" if the file (+3 more)

### Community 117 - "Desktop Connector Sync Tool"
Cohesion: 0.24
Nodes (11): hydrate(), is_stub(), load_included_entries(), main(), acc_sync_dc.py — Desktop Connector pre-sync tool  Reads acc_manifest.csv, identi, Trigger hydration of a stub by opening the file for read.     Polls until cloud-, Write a persistent timestamped sync log.      Each result dict must have:, Return True if the file is an online-only stub (not fully downloaded).      Uses (+3 more)

### Community 118 - "Population Framing"
Cohesion: 0.35
Nodes (11): classify_population_shape(), effective_cluster_count(), hhi(), load_csv(), load_json(), main(), pick_population_baselines(), Any (+3 more)

### Community 119 - "Domain Similarity Comparison"
Cohesion: 0.33
Nodes (11): _build_file_universe(), DomainSimilarityRow, _load_metadata(), _load_records_grouped(), main(), _multiset_jaccard(), _pair_type(), _passes_filters() (+3 more)

### Community 120 - "Manifest Comparison"
Cohesion: 0.35
Nodes (9): _diff_manifests(), _load_json(), main(), _to_manifest(), build_manifest(), Any, Build a deterministic manifest derived from payload["_contract"].      Args:, _safe_dict() (+1 more)

### Community 121 - "Element Dominance Emission"
Cohesion: 0.29
Nodes (8): main(), _read_csv_rows(), _write_csv_atomic(), Path, main(), Path, _read_csv_rows(), _write_csv_atomic()

### Community 122 - "Intradomain Summary"
Cohesion: 0.36
Nodes (10): build_intradomain_summary(), _extract_identity_items(), _load_export_by_file_id(), main(), _pick_representative(), _profile_records(), Aggregate identity evidence across records in a representative file., _safe_str() (+2 more)

### Community 123 - "Join Hash Parameter Extraction"
Cohesion: 0.33
Nodes (9): _extract_qv_from_value(), _get_join_hash(), _is_scalar(), _iter_record_parameters(), _phase2_bucket_items(), Yield (param_key, q, v) observations for a single record.     Returns (observati, Normalize a value into (q, v_str) while preserving explicit states when present., _stable_json() (+1 more)

### Community 124 - "Step Template Governance"
Cohesion: 0.31
Nodes (10): classify(), containment_score(), jaccard_multiset(), main(), _parse_args(), step_template_governance_discovery.py  Reads records.csv and computes per-domain, Share of template's sig_hashes present in the corpus file.      Iterates templat, run() (+2 more)

### Community 125 - "CSV Contract Analysis"
Cohesion: 0.18
Nodes (11): CSV Contract v2.1, Pattern ID and Label Rules (v2.1), Split export removed, V2.1 Analysis Schema, V2.1 Determinism & Identity, V2.1 Phase 0 Export Schema, Central Path Norm Rule, Fingerprint Hashing Rules (+3 more)

### Community 126 - "Dimension Types"
Cohesion: 0.55
Nodes (11): dimension_types_angular, dimension_types_diameter, dimension_types_linear, dim_attr.tick_mark_uid, dim_type.name, dim_type.source_element_id, dim_type.source_unique_id, dim_type.tick_mark_uid (+3 more)

### Community 127 - "Results Registry Management"
Cohesion: 0.26
Nodes (12): Path, atomic_write_csv(), build_results_registry_rows(), main(), Build and atomically write results_registry.csv. Returns rows written., Read a CSV file into string-normalized dictionaries., Write CSV rows atomically using a temp file in the destination directory., Return one results-registry row for every segment in the manifest. (+4 more)

### Community 128 - "Example Workflows"
Cohesion: 0.33
Nodes (9): example_combined_workflow(), example_details_workflow(), example_index_only_workflow(), main(), Example: Using both index and details together.      Use case: Full analysis pip, Run example workflows demonstrating split export usage., Example: Fast contract validation using only index.json.      Use case: CI/CD pi, Example: Record-level analysis using details.json.      Use case: Similarity com (+1 more)

### Community 129 - "Reference Bundle Management"
Cohesion: 0.31
Nodes (8): _escape_control_chars_in_json_strings(), load_and_validate(), Return JSON text with raw control characters escaped only inside strings., write_sidecar(), Path, test_load_and_validate_allows_legacy_control_characters(), test_load_and_validate_allows_raw_newline_in_string(), Path

### Community 130 - "View Context Management"
Cohesion: 0.24
Nodes (7): DocViewContext, Any, # NOTE: Current repo domains are mostly non-geometry; link/transform helpers are, Deterministic, explainable view context snapshot.      Fields:     - view_id: in, Shared context object for domains that need consistent view-scoped reads.      T, Return a cached ViewInfo for `view`, with explicit reasons for missing/unreadabl, ViewInfo

### Community 131 - "Filename Generation"
Cohesion: 0.44
Nodes (9): build_output_filename(), derive_doc_key(), _file_stem_from_doc(), _project_information(), Any, Returns identifiers suitable for filenames and indexing.      Keyed ONLY to the, Build a filename tied to RVT identity.      Args:         doc: Revit document, safe_slug() (+1 more)

### Community 132 - "Membership Row Tests"
Cohesion: 0.27
Nodes (10): test_export_run_ids_sorted_pipe_delimited(), test_membership_rows_no_pipe_delimited_values(), test_registry_both_new_and_removed_files_reasons_when_combined_change(), test_registry_new_files_reason_does_not_cause_false_removal_warnings(), test_registry_new_files_reason_when_file_added(), test_registry_removed_files_reason_when_file_removed(), _build_membership_rows(), _membership_by_segment() (+2 more)

### Community 133 - "Shape Constant Testing"
Cohesion: 0.20
Nodes (6): SHAPE_INT_TO_NAME must map DimensionStyleType enum values correctly., Test shape constant definitions and mappings., All expected shape constants must be defined., All expected family constants must be defined., SHAPE_TO_FAMILY must map all shapes to families., TestShapeConstants

### Community 134 - "Hashing Tests"
Cohesion: 0.29
Nodes (9): Reference implementation: MD5("|".join(safe_str(v) for v in values)) over UTF-8, _reference_hash(), test_make_hash_accepts_generator_large_input_sanity(), test_make_hash_deterministic_repeated_calls(), test_make_hash_handles_unrepr_values(), test_make_hash_is_order_sensitive_contract(), test_make_hash_matches_reference_empty(), test_make_hash_matches_reference_multiple_and_unicode_and_pipes() (+1 more)

### Community 136 - "Governance State Summary"
Cohesion: 0.24
Nodes (10): _add_float(), build_governance_state_summary(), load_delta_summary(), _mean(), pf(), _pick(), Summarise legacy delta patterns by attribution category per comparison type., Return the first non-empty value from row for the provided column names. (+2 more)

### Community 137 - "Pareto Analysis"
Cohesion: 0.31
Nodes (8): assess_split_likelihood(), detect_pareto_cliffs(), main(), Run Pareto analysis with automatic split detection., Detect cliffs in Pareto front that indicate splits., Assess likelihood of organizational split based on Pareto cliffs., run_pareto_with_split_detection(), DataFrame

### Community 138 - "Typography Surface Extraction"
Cohesion: 0.50
Nodes (8): _extract_features(), _get_p2_value(), _get_top(), main(), _norm_scalar(), Extract the typography surfaces we care about, preferring top-level where presen, run(), Any

### Community 139 - "View Category Overrides Analysis"
Cohesion: 0.31
Nodes (8): analyze_override_patterns(), _extract_override_record(), main(), View Category Overrides Join Key Discovery  Hypothesis: Override identity = base, Compute a stable hash for delta items (k/v pairs) to model delta_sig_hash., Return (baseline_sig, delta_sig, delta_items, record_id, label)., Analyze view_category_overrides for join key discovery.      Metrics:     - Base, _stable_delta_hash()

### Community 140 - "Drift Scoring"
Cohesion: 0.44
Nodes (7): _as_dict(), _load_json(), main(), _status_penalty(), _to_features(), _to_manifest(), Any

### Community 141 - "Color Conversion"
Cohesion: 0.33
Nodes (4): Convert a Revit Color object into an "R-G-B" string., _rgb_from_color(), Color that raises on attribute access should return unreadable., TestRgbFromColor

### Community 142 - "RevitLookup Sync"
Cohesion: 0.42
Nodes (8): fetch_raw(), get_current_commit_sha(), github_get(), list_all_cs_files(), main(), Path, sync_revitlookup_reference.py  Copies RevitLookup descriptor source files into t, sync()

### Community 144 - "Record Extraction"
Cohesion: 0.36
Nodes (6): Return all record.v2 dicts found anywhere in a fingerprint.details.json payload., extract_records(), get_domain_payload(), _Id, test_converted_old_and_new_records_converge(), _Type

### Community 145 - "Pairwise Drift Analysis"
Cohesion: 0.43
Nodes (7): _as_dict(), main(), Returns:       summary: dict with meaning columns       domain_scores: dict {dom, _repo_root(), _resolve_runs_dir(), _safe_float(), _summarize_drift()

### Community 146 - "View Filter Definitions"
Cohesion: 0.50
Nodes (7): build_prompt(), _collect_rules(), _format_rule_summary(), _get_value(), _is_opaque_name(), _op_short(), Any

### Community 147 - "Identity Items Lookup"
Cohesion: 0.43
Nodes (7): build_lookup(), _find_file(), main(), tools/label_synthesis/build_identity_items_lookup.py  Pre-processing step for sy, Return (key_col, value_col, quality_col) from a header row.     Supports both sc, _sniff_item_columns(), Path

### Community 148 - "Object Styles Management"
Cohesion: 0.50
Nodes (8): object_styles_analytical, object_styles_annotation, object_styles_imported, obj_style.material_sig_hash, obj_style.weight.cut, object_styles_model, obj_style.source_element_id, obj_style.source_unique_id

### Community 149 - "Unit System Testing"
Cohesion: 0.68
Nodes (7): _length_record(), _payload(), test_accepts_degraded_records(), test_accepts_plural_meters(), test_broader_length_unit_matching(), test_continues_after_unrecognized_or_missing_unit_type_id(), _derive_unit_system()

### Community 150 - "Element ID Validation"
Cohesion: 0.25
Nodes (3): In OGS context, 0 means 'no override' and should be invalid., Object without IntegerValue should not be considered invalid., TestIsInvalidElementId

### Community 151 - "Wall Type Reset"
Cohesion: 0.36
Nodes (7): _is_function_only_block(), main(), Path, Reset wall_type records that are blocked solely because wt.function=unsupported., Return {record_pk: {key: q}} for all wall_types items., True if wt.function is the only non-ok required item and compound structure item, _read_wall_items()

### Community 152 - "Join Key Application"
Cohesion: 0.53
Nodes (4): compute_join_hash(), extract_identity_map(), md5_utf8_join_pipe(), Any

### Community 153 - "Synthetic Key Computation"
Cohesion: 0.47
Nodes (5): main(), _parse_args(), _synthetic_line_patterns(), DataFrame, Namespace

### Community 154 - "Intradomain Definition Emission"
Cohesion: 0.47
Nodes (5): emit_ids_artifacts(), IDS, main(), _make_ids_ids(), Stable mapping from standard_name -> IDS_### (sorted by name).

### Community 155 - "Candidate Join Key Simulation"
Cohesion: 0.60
Nodes (4): _extract_features(), _get(), _qv(), Any

### Community 156 - "Frequent Itemset Finding"
Cohesion: 0.47
Nodes (5): find_closed_itemsets(), find_root_bundles(), Find closed frequent itemsets via pairwise-intersection candidate generation., Lightweight closed frequent itemset finder returning only root bundles     (item, _supporting_files_by_superset()

### Community 157 - "Graphify Reference Management"
Cohesion: 0.33
Nodes (6): graphify reference: extra exports and benchmark, graphify reference: extraction subagent prompt, graphify reference: GitHub clone and cross-repo merge, graphify reference: commit hook and native CLAUDE.md integration, graphify reference: query, path, explain, graphify reference: transcribe video and audio

### Community 158 - "RevitLookup Audit Tests"
Cohesion: 0.47
Nodes (3): _install_revit_stubs(), test_object_styles_model_semantic_keys_include_material_sig_hash(), test_vco_category_hidden_is_semantic_for_model_and_annotation()

### Community 159 - "Schedule Row Selection"
Cohesion: 0.40
Nodes (6): _is_named_element(), Select manual file-open schedule rows.      ``review_rows`` are the final review, _schedule_file_sort_key(), _schedule_row_sort_key(), _select_schedule_rows_for_cluster(), _selected_file_name_status()

### Community 160 - "Pairwise Analysis"
Cohesion: 0.60
Nodes (5): load_csv(), main(), Any, Path, write_csv()

### Community 161 - "Identity Item Matching"
Cohesion: 0.50
Nodes (4): field_matches(), Match an identity_items item_key against an edge's source_field.      field_matc, _coherence_tier(), main()

### Community 162 - "Shape Input Calibration"
Cohesion: 0.50
Nodes (4): add_record_key(), main(), DataFrame, Series

### Community 163 - "Core Principles Overview"
Cohesion: 0.40
Nodes (5): Core Principles, Execution Environment, README Overview, Scope (Current), Status

### Community 164 - "Legacy Tools Overview"
Cohesion: 0.40
Nodes (5): Deprecated / Legacy Tools (tools/), Phase 0 / Phase 1 / Phase 2 Tools Map (tools/), revit_test_runner_pyrevit.py, Revit integration tests (golden baselines), Powershell Commands

### Community 165 - "Refactor Strategy"
Cohesion: 0.40
Nodes (5): Milestones, Non-Negotiables, Refactor Approach, Refactor Strategy, Target Structure

### Community 166 - "Norm Hash Precision Computation"
Cohesion: 0.70
Nodes (4): compute_norm_hash_for_group(), detect_cols(), main(), md5s()

### Community 167 - "Configuration Example"
Cohesion: 0.40
Nodes (4): cases, golden_dir, max_diffs, out_dir

### Community 169 - "Text Types Export Testing"
Cohesion: 0.60
Nodes (3): _Id, test_text_types_extract_emits_flat_items_only(), _Type

### Community 170 - "Cross-Domain Analysis"
Cohesion: 0.50
Nodes (5): Cross-Domain Archetype Discovery Pipeline, Bundle Analysis Pipeline, View Template Comparison, Domain Probe Inventory, Refreshed Definition of Done — Revit Standards Governance Narrative Outputs

### Community 171 - "Client Onboarding Profile"
Cohesion: 0.40
Nodes (5): _client_onboarding_profile(), _format_domain_items(), Return deterministic onboarding implications from client-level metrics., Render client-specific onboarding and operating implications., render_onboarding_section()

### Community 172 - "Contract Validation Tests"
Cohesion: 0.83
Nodes (3): main(), read_csv(), Path

### Community 173 - "Identity Management"
Cohesion: 0.50
Nodes (4): identity, identity.central_path, identity.filename, identity.project_title

### Community 174 - "Governance Role Patterns"
Cohesion: 0.50
Nodes (3): notes, rules, schema_version

### Community 176 - "Bundle Pattern Classification Tests"
Cohesion: 0.83
Nodes (3): Path, test_emit_stub_classifies_root_to_leaf_patterns_as_differentiating(), _write_csv()

### Community 177 - "Integration Test Patterns"
Cohesion: 0.50
Nodes (3): Documented integration test patterns for full Revit validation., Policy load integration pattern placeholder., TestPolicyLoadPattern

### Community 178 - "Filtered Element Collector Tests"
Cohesion: 0.67
Nodes (3): PR5 policy:     - Domains must not directly import or reference FilteredElementC, _repo_root(), test_domains_do_not_reference_filtered_element_collector()

### Community 179 - "Sentinel Policy Tests"
Cohesion: 0.67
Nodes (3): Enforces PR3 sentinel policy:      - Domains may not contain any "<Token>" liter, _repo_root(), test_domains_do_not_emit_extra_angle_bracket_tokens()

### Community 181 - "Signature Hash Policy Generation"
Cohesion: 0.67
Nodes (3): build_policy(), main(), Any

### Community 184 - "Documentation Overview"
Cohesion: 0.67
Nodes (3): CHANGELOG, DECISIONS, INVARIANTS

## Knowledge Gaps
- **217 isolated node(s):** `PreToolUse`, `PreToolUse`, `version`, `record_schema_version`, `identity_item_schema` (+212 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **28 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `_phase2_item()` connect `Domain Index Building` to `Number Canonicalization`, `Value Canonicalization`?**
  _High betweenness centrality (0.179) - this node is a cross-community bridge._
- **Why does `load_exports()` connect `Domain Payload Management` to `Attribute Stability Analysis`, `Collision Differencing`, `Typography Surface Extraction`, `Domain Index Building`, `Join Hash Parameter Extraction`, `Cluster Analysis`, `Reference Standards Building`, `Join-Key Discovery`, `Join Key Evaluation`, `Export File Loading`, `Join Key Application`, `Intradomain Summary`, `Candidate Join Key Simulation`?**
  _High betweenness centrality (0.144) - this node is a cross-community bridge._
- **Why does `_load_governance_role_rules()` connect `Placeholder Exclusion Management` to `Analysis Metrics Computation`?**
  _High betweenness centrality (0.091) - this node is a cross-community bridge._
- **What connects `PreToolUse`, `PreToolUse`, `version` to the rest of the system?**
  _933 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Line Style Extraction` be split into smaller, more focused modules?**
  _Cohesion score 0.055239236839476576 - nodes in this community are weakly interconnected._
- **Should `Wall Object Management` be split into smaller, more focused modules?**
  _Cohesion score 0.07472527472527472 - nodes in this community are weakly interconnected._
- **Should `Hash Key Generation` be split into smaller, more focused modules?**
  _Cohesion score 0.082896379525593 - nodes in this community are weakly interconnected._