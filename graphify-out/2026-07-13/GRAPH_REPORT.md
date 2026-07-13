# Graph Report - .  (2026-07-13)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 3987 nodes · 9611 edges · 241 communities (211 shown, 30 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 231 edges (avg confidence: 0.75)
- Token cost: 13,718 input · 2,278 output

## Graph Freshness
- Built from commit: `cd5dbca8`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Hashing and Key Generation
- Dimension Shape Detection
- Wall Properties and Types
- Pareto Key Ranking
- Graphic Overrides Management
- Element Collection and Caching
- Canonical Item Processing
- Dependency Management
- Record Schema Definition
- Domain Identity Management
- Timing Data Collection
- Cluster Analysis and Metrics
- Join Key Schema Definition
- Domain Edge Discovery
- Cluster Analysis and Metrics
- Structural Hashing
- Governance Testing
- Domain Envelope Management
- Dimension Type Management
- Subcategory ID Management
- Cluster Threshold Testing
- Join Key Evaluation
- Value Canonicalization
- Semantic Group Building
- Segment Orchestration
- Dimension Label Formatting
- Attribute Analysis
- Cluster Label Annotation
- Join Key Migration Testing
- View Template Comparison
- Cross-Segment Comparison
- Phase 2 Join Key Management
- Cluster Review Preparation
- Domain Indexing
- Domain Record Extraction
- Profile Building
- Dimension Type Management
- Discipline Management
- Join Key Policy Management
- Comparison Registry Management
- Layer Stack Management
- Analysis Run Management
- Safe String Conversion
- Fragmented Label Synthesis
- Segment Manifest Testing
- Governance Narrative Generation
- View Category Overrides
- Signal Clustering
- Change Classification
- Dimension Type Probing
- Forge Type Probing
- Similarity Analysis
- Reference Standards Building
- Governance Report Generation
- View Category Overrides Management
- Material Management
- Comparison Registry Testing
- Label Synthesis
- Dimension Formatting
- Text Type Probing
- Line Style Extraction
- Record Contract Testing
- View Template Policy Testing
- Material Join Key Discovery
- Line Pattern Probing
- Object Style Probing
- Fill Pattern Probing
- Manifest Row Testing
- Split Domain Policy Testing
- Export File Management
- Join Key Derivation
- Signature Hash Building
- Browser Organization Probing
- VFD Edge Discovery Testing
- Phase Graphics Probing
- Cluster Label Backfill
- Phase 1 Diagnosis
- Split Export Merging
- Domain Classification
- Shared IO Helpers
- Domain Profile Management
- View Filter Definitions
- Element-Level Classification
- Phase Filter Probing
- Phase Graphics Probing
- Corpus Normalization Testing
- Client Summary Building
- Configuration Probing
- Run Configuration Management
- Architecture Overview
- View Template Comparison
- Element-Level Classification
- Domain Pattern Patching
- Comparison Engine
- Fingerprint Compression
- Material Migration
- NA Token Testing
- Collision Differencing
- View Template Analysis
- Knowledge Graph Rebuild
- Fingerprint API Mapping
- Parameter Metadata Extraction
- Arrowhead Type Probing
- Identity Record Probing
- Reuse Classification Testing
- Feature Extraction
- Domain Patterns Labeling
- Family Types Probing
- Phase Parameter Probing
- View Template Probing
- JSON Comparison Utilities
- Repository Path Analysis
- Family Mapping Testing
- Hash Policy Testing
- Desktop Connector Scanner
- Desktop Connector Sync Tool
- Population Framing Analysis
- Domain Similarity Comparison
- Manifest Comparison
- Element Dominance Emission
- Intradomain Identity Summary
- Join Hash Parameter Extraction
- Template Governance Discovery
- CSV Contract Analysis
- Results Registry Builder
- Bundle Overlap Analysis
- Split Export Examples
- Reference Bundle Management
- Filename Generation
- Membership Rows Testing
- Shape Constant Testing
- Hashing Incremental Testing
- Governance State Summary
- Pareto Analysis with Splits
- Typography Features Extraction
- View Category Overrides Analysis
- Drift Scoring
- Shape-Gating Validation
- Shape Gating Definitions
- RevitLookup Sync
- Segment Membership Analysis
- Record Extraction
- Pairwise Drift Analysis
- Bundle Share Profiling
- View Filter Definitions
- Identity Items Lookup
- Unit System Testing
- Wall Type Reset
- JSON IO Helper
- Join Keys Application
- Synthetic Key Computation
- Intradomain Definition Emission
- Candidate Join Key Simulation
- Frequent Itemset Finder
- Graphify Reference Tools
- Pairwise Analysis
- Identity Item Matching
- Shape Input Preparation
- Core Principles Overview
- Refactor Strategy
- Domain Signature Policies
- Pattern Normhash Computation
- Configuration Example
- Module Purging Script
- Text Types Export Testing
- Governance Pipeline Overview
- Client Onboarding Profile
- Contract Validation Testing
- Governance Role Patterns
- View Probing
- Client Collection Data
- Collision Row Testing
- Bundle Pattern Classification
- Integration Test Documentation
- Filtered Element Collector Testing
- Sentinel Policy Testing
- Signature Hash Policy Generation
- Join Key Calibration
- Details to CSV Conversion
- Documentation Overview
- Settings Configuration
- Hooks Configuration
- Identity & Semantics Refactor
- Label Population Building
- First Record Extraction
- Pareto Shape Gating Testing
- Jenks Breaks Utility
- Bundle Analysis Post-Processing
- Operational Review
- Hook Check Script
- Setup Script
- Session Start Script
- Post-Export Analysis Helpers
- Revit Integration Tests
- Graph Generation
- Commit Guidelines
- Done Definition
- Execution Staging
- Join Key Phase 2
- Identity & Semantics Refactor
- Governance Standards Definition
- Code Refactoring
- Revit Test Runner
- Descriptor to Fingerprint Mapping
- Dimension Types Join Key
- Verification Planning

## God Nodes (most connected - your core abstractions)
1. `make_identity_item()` - 110 edges
2. `make_hash()` - 105 edges
3. `safe_str()` - 98 edges
4. `serialize_identity_items()` - 88 edges
5. `_build_segments()` - 84 edges
6. `build_join_key_from_policy()` - 79 edges
7. `get_domain_join_key_policy()` - 73 edges
8. `canonicalize_str()` - 65 edges
9. `canonicalize_int()` - 51 edges
10. `build_record_v2()` - 51 edges

## Surprising Connections (you probably didn't know these)
- `test_load_comparison_registry_missing_file_returns_empty()` --calls--> `load_comparison_registry()`  [INFERRED]
  tests/test_compare_cross_segment_comparison_registry.py → tools/compare_cross_segment.py
- `test_seed_detection_level2()` --calls--> `_build_segments()`  [INFERRED]
  tests/test_build_segment_manifest.py → tools/build_segment_manifest.py
- `test_sort_order_within_level_alphabetical()` --calls--> `_build_segments()`  [INFERRED]
  tests/test_build_segment_manifest.py → tools/build_segment_manifest.py
- `test_pair_domain_work_items_use_pair_domain_union()` --calls--> `build_pair_domain_work_items()`  [INFERRED]
  tests/test_compare_cross_segment_governance.py → tools/compare_cross_segment.py
- `test_make_hash_accepts_generator_large_input_sanity()` --calls--> `make_hash()`  [EXTRACTED]
  tests/test_hashing_incremental.py → core/hashing.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Architecture Layers** — layer_0_core, layer_1_domain_extractors, layer_2_context_builder, layer_3_runner [EXTRACTED 0.75]
- **Cross-Segment Analysis** — docs_cross_segment_comparison, tools_compare_cross_segment, docs_phase_2_join-key_discovery, tools_run_extract_all [EXTRACTED 0.75]
- **Refreshed Definition of Done Concepts** — tools_refreshed_revit_governance_dod, tools_archetype_readme, tools_bundle_analysis_readme, tools_compare_templates_stand-alone_compare_view_templates_stand-alone_report, tools_probes_domain_probe_inventory_2024-02-05 [EXTRACTED 0.75]
- **Changelog and Decisions** — changelog, decisions [EXTRACTED 0.75]
- **Governance Documentation** — invariants, decisions, repo_operational_review [EXTRACTED 0.75]

## Communities (241 total, 30 thin omitted)

### Community 0 - "Hashing and Key Generation"
Cohesion: 0.08
Nodes (73): Return (required_items, optional_items, explicitly_excluded_items) for a domain., make_hash(), _make_hash_impl(), Deterministic hash based on a sequence of strings.      Streaming/incremental im, Inner hash implementation (separated for timing wrapper clarity)., build_join_key_from_policy(), _dedupe_preserve_order(), _expand_sequence_key() (+65 more)

### Community 1 - "Dimension Shape Detection"
Cohesion: 0.07
Nodes (87): purge_lookup(), _build_text_appearance_items(), _fmt_float(), _fmt_in_from_ft(), _format_options_to_kv(), _get_dimension_shape(), get_type_display_name(), Detect dimension shape from a Revit DimensionType object.      Revit exposes sha (+79 more)

### Community 2 - "Wall Properties and Types"
Cohesion: 0.07
Nodes (63): object, _basic_wall(), _CS, _CSWrapError, _default_ctx(), _Doc, _Id, _Layer (+55 more)

### Community 3 - "Pareto Key Ranking"
Cohesion: 0.06
Nodes (71): build_wide_kv_table(), compute_v_norm(), _dedupe_preserve_order(), dominates(), eval_subset(), EvalConfig, main(), pareto_front() (+63 more)

### Community 4 - "Graphic Overrides Management"
Cohesion: 0.06
Nodes (48): _append_color_item(), _append_pattern_items(), _append_value_item(), extract_cut_graphics(), extract_halftone(), extract_projection_graphics(), extract_transparency(), _is_category() (+40 more)

### Community 5 - "Element Collection and Caching"
Cohesion: 0.06
Nodes (32): CacheKey, build_purgeable_id_set(), collect_elements(), collect_id_ints(), _collect_id_ints_uncached(), CollectCtx, _get_element(), _is_invalid_element_id() (+24 more)

### Community 6 - "Canonical Item Processing"
Cohesion: 0.07
Nodes (53): build_flat_items(), canonicalize_record(), compile_role_policy(), merge_legacy_buckets(), _normalize_item(), Any, Resolve roles from item.k via runtime lookup.      Returns grouped items without, Canonicalize a record to flat `items` shape and remove legacy/derived keys. (+45 more)

### Community 7 - "Dependency Management"
Cohesion: 0.09
Nodes (56): canon_str(), Canonicalize string-like values.      Rules:     - None -> <MISSING>     - str(., collect_instances(), Blocked, Any, core/deps.py  Centralized dependency enforcement for domain execution.  Non-nego, Typed exception used to signal a hard dependency block.      Attributes:, Require an upstream domain envelope to exist and be acceptable.      Args: (+48 more)

### Community 8 - "Record Schema Definition"
Cohesion: 0.06
Nodes (55): additionalProperties, allOf, $id, const, items, minItems, minLength, pattern (+47 more)

### Community 9 - "Domain Identity Management"
Cohesion: 0.11
Nodes (53): banned_identity_value_substrings, domains, ceiling_types, dimension_types_angular, dimension_types_diameter, dimension_types_linear, dimension_types_spot_coordinate, dimension_types_spot_elevation (+45 more)

### Community 10 - "Timing Data Collection"
Cohesion: 0.06
Nodes (21): Any, Set the currently executing domain for sub-timing scoping., Return structured timing report.          Returns a dict with:           - ``tot, Build the structured report (must hold lock)., Collects hierarchical timing data for extraction runs.      Labels follow the co, Begin timing a labeled operation., End timing and record duration for a labeled operation., Record a pre-computed elapsed duration directly.          Used for hot-loop accu (+13 more)

### Community 11 - "Cluster Analysis and Metrics"
Cohesion: 0.07
Nodes (51): build_distance_matrix_from_similarity(), Cluster, cluster_assignments_to_labels(), compute_silhouette_score(), extract_dates_from_paths(), extract_metadata_patterns(), build_element_profiles(), compute_avg_between_cluster_similarity() (+43 more)

### Community 12 - "Join Key Schema Definition"
Cohesion: 0.10
Nodes (51): category, evidence, importance, domain, dim_attr.tick_mark_uid, dim_type.name, dim_type.tick_mark_uid, explicitly_excluded_items (+43 more)

### Community 13 - "Domain Edge Discovery"
Cohesion: 0.11
Nodes (50): atomic_write_csv(), bool_s(), build_domain_gap_rows(), build_edge_rows(), build_inventory_rows(), build_unresolved_file_rows(), _candidate_category_details(), canonical_param_kind() (+42 more)

### Community 14 - "Cluster Analysis and Metrics"
Cohesion: 0.07
Nodes (51): build_distance_matrix_from_similarity(), Cluster, cluster_assignments_to_labels(), compute_silhouette_score(), extract_dates_from_paths(), extract_metadata_patterns(), build_element_profiles(), compute_avg_between_cluster_similarity() (+43 more)

### Community 15 - "Structural Hashing"
Cohesion: 0.07
Nodes (46): sig_hash_keys, sig_hash_schema, _block_record_for_unstable_id(), block_record_v2(), canonical_structural_fields(), _canonical_structural_value(), _default_record_id_secondary_key(), finalize_record_ids_for_domain() (+38 more)

### Community 16 - "Governance Testing"
Cohesion: 0.10
Nodes (48): Path, Tests for governance semantics in tools/compare_cross_segment.py., _seg(), test_build_governance_state_rows_include_inherited_unused_and_local_active(), test_density_similarity_uses_domain_density_vectors_not_containment(), test_discover_governance_chain_collection_match_is_soft_for_client_scope(), test_discover_governance_chain_falls_back_to_collection_label_for_na_client(), test_discover_governance_chain_final_fallback_normalizes_na_spelling() (+40 more)

### Community 17 - "Domain Envelope Management"
Cohesion: 0.09
Nodes (44): add_bounded_error(), compute_run_status(), DiagError, _ensure_list(), new_domain_envelope(), new_run_diag(), new_run_envelope(), Any (+36 more)

### Community 18 - "Dimension Type Management"
Cohesion: 0.09
Nodes (47): ceiling_types, dimension_types_angular, dimension_types_diameter, dimension_types_linear, dim_attr.tick_mark_uid, dim_type.name, dim_type.source_element_id, dim_type.source_unique_id (+39 more)

### Community 19 - "Subcategory ID Management"
Cohesion: 0.07
Nodes (40): sig_hash_key_prefixes, build_subcategory_used_id_set(), Build/cache used subcategory ids for a given parent category., _collect_fill_patterns(), _export_fill_pattern_ctx(), extract_drafting(), extract_model(), _phase2_fill_pattern_is_import() (+32 more)

### Community 20 - "Cluster Threshold Testing"
Cohesion: 0.11
Nodes (41): Path, test_compute_alignment_rates_and_contract_header_preserves_is_named_cluster(), test_compute_alignment_rates_falls_back_to_percentage_when_size_absent(), test_compute_alignment_rates_falls_back_to_percentage_when_size_values_are_invalid(), test_compute_alignment_rates_uses_raw_share_or_size_for_unrounded_result(), test_thresholds_breaks_and_ordering(), test_thresholds_reject_non_three_classes(), _write_csv() (+33 more)

### Community 21 - "Join Key Evaluation"
Cohesion: 0.08
Nodes (32): compute_coverage(), compute_join_hash_for_record(), evaluate_gates(), evaluate_keyset(), extract_identity_map(), greedy_select_keys(), md5_utf8_join_pipe(), Bootstrap by sampling files (not individual records) to preserve lineage structu (+24 more)

### Community 22 - "Value Canonicalization"
Cohesion: 0.07
Nodes (36): canon_bool(), canon_id(), canon_num(), fnum(), is_sentinel(), Any, Canonicalize Revit ElementId-like values to a decimal string.      Accepts:, Legacy alias for canon_num. (+28 more)

### Community 23 - "Semantic Group Building"
Cohesion: 0.11
Nodes (41): build_grouping_prompt(), build_semantic_groups(), _call_grouping_llm(), _derive_element_label(), _extract_behavioral_props(), _infer_fill_geometry_description(), _is_fill_angle_close(), _is_nullish() (+33 more)

### Community 24 - "Segment Orchestration"
Cohesion: 0.09
Nodes (41): CompletedProcess, Lock, Namespace, _active_domains_from_presence_csv(), _build_patterns_missing_notes(), build_run_plan(), load_manifest(), load_membership() (+33 more)

### Community 25 - "Dimension Label Formatting"
Cohesion: 0.06
Nodes (34): format_synopsis(), _inches_to_fraction(), tools/label_synthesis/synopsis_formatters/arrowheads.py  Behavioral synopsis for, _accuracy_label(), _center_marks_label(), _decoration_label(), _extract_kv(), format_synopsis() (+26 more)

### Community 26 - "Attribute Analysis"
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

### Community 30 - "Cross-Segment Comparison"
Cohesion: 0.11
Nodes (38): ComparisonPair, Phase 0–6: Questions Each Phase Can Answer, Cross-Segment Comparison, Deprecated / Legacy Tools, test_output_row_sort_helpers_are_stable_by_content(), test_project_target_governance_state_uses_target_used(), test_standards_carrier_target_avoids_passive_bloat_label(), _bool_str() (+30 more)

### Community 31 - "Phase 2 Join Key Management"
Cohesion: 0.10
Nodes (34): phase2_join_hash(), phase2_qv_from_legacy_sentinel_str(), phase2_sorted_items(), Return IdentityItem-like dicts sorted by key 'k'., Map legacy sentinel strings to record.v2-safe (v,q) without emitting sentinel li, Deterministic join-hash for Phase-2 joins.     Expects caller to have already so, canonicalize_bool(), Canonicalize boolean values for IdentityItem.v.      Returns:         ("true"|"f (+26 more)

### Community 32 - "Cluster Review Preparation"
Cohesion: 0.11
Nodes (35): _all_cluster_ids(), _all_clusters(), _build_cluster_context(), _build_curated_gq_map(), ClusterContext, _find_cluster(), _governance_question_from_archetype_id(), _governance_question_from_cluster_id() (+27 more)

### Community 33 - "Domain Indexing"
Cohesion: 0.10
Nodes (28): AttrStabilityRow, compute_attr_stability(), compute_stress_rank(), StressRow, build_domain_index(), DomainIndex, _get_join_hash(), Build a per-file join_hash index for one domain. (+20 more)

### Community 34 - "Domain Record Extraction"
Cohesion: 0.12
Nodes (26): get_domain_payload(), get_domain_records(), Return the domain payload (legacy surface) if present., Extract record.v2 records list from the domain payload.      Notes:     - Contra, _discover_families_from_exports(), _families_present_in_baseline(), _family_shape(), FamilyRun (+18 more)

### Community 35 - "Profile Building"
Cohesion: 0.09
Nodes (33): _build_synthetic_items_for_pair(), _build_template_lookup(), _extract_active_vco_fields(), _extract_graphic_fields(), _extract_object_style_baseline_fields(), _get_domain_payload(), _get_identity_item_value(), _get_phase2_cosmetic_value() (+25 more)

### Community 36 - "Dimension Type Management"
Cohesion: 0.35
Nodes (35): domains, ceiling_types, dimension_types_angular, dimension_types_diameter, dimension_types_linear, dimension_types_radial, dimension_types_spot_coordinate, dimension_types_spot_elevation (+27 more)

### Community 37 - "Discipline Management"
Cohesion: 0.08
Nodes (35): _disc_rows(), Multi-client, multi-discipline Container corpus for discipline tests., test_blank_discipline_does_not_generate_discipline_cut(), test_blank_unit_system_excluded(), test_client_discipline_leaf_label_container(), test_client_discipline_leaf_no_empty_purpose(), test_client_discipline_leaf_purpose_container(), test_collection_label_own_leading_colon_does_not_collide_with_forged_escape() (+27 more)

### Community 38 - "Join Key Policy Management"
Cohesion: 0.33
Nodes (33): domains, ceiling_types, dimension_types_angular, dimension_types_diameter, dimension_types_linear, dimension_types_radial, dimension_types_spot_coordinate, dimension_types_spot_elevation (+25 more)

### Community 39 - "Comparison Registry Management"
Cohesion: 0.14
Nodes (36): test_load_comparison_registry_roundtrip(), atomic_write_csv(), build_pair_domain_work_items(), bundle_analysis_dir(), _classify_delta(), discover_domains_for_segment(), discover_within_project(), domain_patterns_path() (+28 more)

### Community 40 - "Layer Stack Management"
Cohesion: 0.17
Nodes (31): _make_layer_row(), _make_wall_record(), Any, Path, Single type with simple layers emits one stack row and correct layer rows., Two types sharing the same stack_hash_loose collapse to one stack row with type_, wall_types and floor_types each emit separate rows distinguished by domain., layer_stacks is NOT written when --emit uses the default set. (+23 more)

### Community 41 - "Analysis Run Management"
Cohesion: 0.14
Nodes (24): atomic_write_csv(), read_csv_rows(), resolve_analysis_run_id(), _choose_threshold(), compute_placeholder_exclusions(), compute_placeholder_exclusions(), _is_truthy(), _largest_gap_threshold() (+16 more)

### Community 42 - "Safe String Conversion"
Cohesion: 0.19
Nodes (29): collect_types(), Convert any value to a string representation safely.      Handles both str and u, safe_str(), _attach_placeholder_metadata(), _blocked_required_items(), _build_instance_count_map(), _canon_non_sentinel_str(), _coarse_fill_reads() (+21 more)

### Community 43 - "Fragmented Label Synthesis"
Cohesion: 0.11
Nodes (28): _call_llm(), _collect_union_bundle_join_hashes(), _generic_build_prompt(), _generic_system_prompt(), _get_domain_records(), _groups_vocab_path(), _load_domain_prompt_module(), _load_governance_join_hashes() (+20 more)

### Community 44 - "Segment Manifest Testing"
Cohesion: 0.10
Nodes (20): _meta_row(), Tests for tools/build_segment_manifest.py., test_blank_client_label_level2_id_distinct_from_level1(), test_client_discipline_leaf_purpose_project(), test_client_discipline_leaf_purpose_template(), test_client_label_first_seen_casing_is_canonical(), test_level1_run_type_bundle_at_min_files(), test_level1_run_type_skip_when_below_min_files() (+12 more)

### Community 45 - "Governance Narrative Generation"
Cohesion: 0.14
Nodes (28): assign_tier(), detect_anomalies(), fmt(), _has_material_state_exception(), main(), normalise_summary_schema(), pct(), generate_governance_narrative.py  Deterministic governance narrative renderer fo (+20 more)

### Community 46 - "View Category Overrides"
Cohesion: 0.16
Nodes (25): _baseline_for_cat(), _bool_int(), _bucket_for_view(), _category_path(), _contract_eid(), _contract_int(), _contract_missing(), _contract_string() (+17 more)

### Community 47 - "Signal Clustering"
Cohesion: 0.13
Nodes (27): _apply_threshold(), _bare_signal_name(), _build_clusters(), _build_coverage_summary(), _build_curated_gq_map(), _build_detail_files_lookup(), _build_n_files_classified_lookup(), _build_signal_cluster_map() (+19 more)

### Community 48 - "Change Classification"
Cohesion: 0.13
Nodes (24): ChangeCounts, classify_pair(), _phase2_items_map(), Return k -> (q,v) across phase2 buckets, and duplicate_k_count.      Used for eq, Classify changes for one domain between baseline and other.      Definitions:, ExportFile, One exported fingerprint JSON treated as one authority sample., ensure_dir() (+16 more)

### Community 49 - "Dimension Type Probing"
Cohesion: 0.13
Nodes (23): _example_score(), _find_tick_param(), _fmt_display(), _format_param_contract(), _format_synth_contract(), _get_dim_shape_info(), _get_family_name_param(), _is_angle_datatype() (+15 more)

### Community 50 - "Forge Type Probing"
Cohesion: 0.21
Nodes (26): _discover_specs(), _forge_id_string(), _is_forge_type_id(), _label_for_discipline_id(), _label_for_spec_id(), _maybe_set_example(), _pv_from_bool(), _pv_from_double() (+18 more)

### Community 51 - "Similarity Analysis"
Cohesion: 0.14
Nodes (25): build_class_profiles(), build_dim_summaries(), build_exact_match_table(), build_family_file_detail(), build_name_cluster_table(), build_subgroups(), extract_category(), extract_family_name() (+17 more)

### Community 52 - "Reference Standards Building"
Cohesion: 0.13
Nodes (23): build_reference_standards_from_clusters(), main(), Build reference standards from cluster representatives., get_contract(), get_domain_envelope(), get_domains_map(), get_run_provenance(), iter_json_paths() (+15 more)

### Community 53 - "Governance Report Generation"
Cohesion: 0.14
Nodes (17): _build_html_report(), build_pattern_table(), build_table(), _get_identity_value(), main(), _pattern_row_template(), ProjectExport, Standards Governance Report Generator  Analyzes fingerprint exports to: 1. Detec (+9 more)

### Community 54 - "View Category Overrides Management"
Cohesion: 0.16
Nodes (18): notes, _safe_bool(), _safe_bool(), view_category_overrides_model, vco.category_path, vco.cut.line_color.rgb, vco.cut.line_pattern_ref.sig_hash, vco.cut.line_weight (+10 more)

### Community 55 - "Material Management"
Cohesion: 0.11
Nodes (23): _canon_id_local(), _export_ctx(), extract(), _mk_item(), Return (value, q) safe for make_identity_item without sentinel literals., Return (v, q) for optional identity metadata fields., _read_param_as_string(), _read_prop() (+15 more)

### Community 56 - "Comparison Registry Testing"
Cohesion: 0.19
Nodes (22): ComparisonRegistryKey, Tests for comparison_registry.csv staleness tracking in tools/compare_cross_segm, _reg_row(), test_build_comparison_registry_rows_domain_scoped_run_omits_other_domains(), test_build_comparison_registry_rows_is_a_full_snapshot_no_carryover(), test_build_comparison_registry_rows_omits_pair_when_reference_segment_is_pending(), test_build_comparison_registry_rows_omits_pair_when_target_segment_is_failed(), test_build_comparison_registry_rows_omits_work_items_with_no_output() (+14 more)

### Community 57 - "Label Synthesis"
Cohesion: 0.16
Nodes (20): build_prompt(), _detect_record_class(), _fmt_size(), _format_identity_items(), tools/label_synthesis/domain_prompts/arrowheads.py  LLM system prompt and prompt, build_prompt(), _extract_grid_geometry(), _get_identity_value() (+12 more)

### Community 58 - "Dimension Formatting"
Cohesion: 0.15
Nodes (20): build_prompt(), _fmt_accuracy(), _fmt_witness(), _format_identity_items(), _get_shape(), Return a brief shape-specific note to insert before the parameters., _shape_context_note(), build_prompt() (+12 more)

### Community 59 - "Text Type Probing"
Cohesion: 0.13
Nodes (14): _find_leader_arrow_param(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _looks_like_text_type(), Contract:       {         "q": "ok|missing|unreadable|unsupported",         "sto, Best-effort parse for Revit integer color surfaces.     Assumes a 24-bit packed (+6 more)

### Community 60 - "Line Style Extraction"
Cohesion: 0.17
Nodes (20): extract(), _line_pattern_segment_kind_id(), _line_pattern_synopsis_from_element(), _line_pattern_synopsis_from_segments(), Extract Line Styles fingerprint from document.      record.v2 surfaces:       -, line_style.path, line_style.pattern_ref.kind, line_style.source_element_id (+12 more)

### Community 61 - "Record Contract Testing"
Cohesion: 0.14
Nodes (17): exported_fingerprint_json(), Provide exporter output JSON for validation.      Options:       1) Set env var, test_all_exported_records_conform_to_record_contract_v2(), test_validate_records_duplicate_within_file_and_domain(), _compute_identity_quality(), _hash_preimage(), _is_allowed_indexed_key(), _normalize_indexed_key() (+9 more)

### Community 62 - "View Template Policy Testing"
Cohesion: 0.13
Nodes (21): _load_policy(), Join key build must report missing when view_template.def_hash is absent., All view_templates split domains must have a valid join_key_schema., All split view_template domains must require view_template.def_hash in join key., Floor/structural/area plans policy must use view_template.def_hash., Ceiling plans policy must use view_template.def_hash., Elevations/sections/detail policy must use view_template.def_hash., Renderings/drafting policy must use view_template.def_hash. (+13 more)

### Community 63 - "Material Join Key Discovery"
Cohesion: 0.20
Nodes (18): _build_key(), _build_key_files(), _compute_metrics(), discover(), _extract_sig(), _is_system_material(), _load_class_map(), _load_materials() (+10 more)

### Community 64 - "Line Pattern Probing"
Cohesion: 0.15
Nodes (16): _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _iter_line_style_categories(), _linepattern_signature(), _lp_seg_type_id_and_name(), _maybe_set_example() (+8 more)

### Community 65 - "Object Style Probing"
Cohesion: 0.14
Nodes (9): _category_type_label(), _contract_eid(), _eid_name(), _get_name(), _infer_object_styles_tab(), _iter_categories(), Return a human-readable CategoryType label.     Handles environments where str(C, Best-effort classification into the Object Styles UI tabs.     Heuristic only. U (+1 more)

### Community 66 - "Fill Pattern Probing"
Cohesion: 0.19
Nodes (18): _add_computed_surface(), _bucket_key_for_fill_pattern(), _contract_from_value(), _ensure_entry(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype() (+10 more)

### Community 67 - "Manifest Row Testing"
Cohesion: 0.10
Nodes (20): _manifest_row(), Hand-craft a manifest-row-shaped dict for testing _build_registry() in     isola, test_conformance_reference_mode_carried_over_across_runs(), test_conformance_reference_mode_defaults_to_latest_for_new_segment(), test_conformance_reference_mode_defaults_to_latest_for_old_registry_missing_field(), test_registry_drops_removed_segment_ids_with_warning(), test_registry_excludes_skip_segments(), test_registry_first_run_no_existing_file_unaffected() (+12 more)

### Community 68 - "Split Domain Policy Testing"
Cohesion: 0.14
Nodes (11): Test join key policies for each split dimension_types domain., Load join key policy for a specific split domain., Linear domain must require witness_line_control., Radial domain must require center_marks and center_mark_size., Angular domain must require unit_format_id., Diameter domain must have a valid policy., Spot elevation domain must have a valid policy., Spot coordinate domain must have a valid policy. (+3 more)

### Community 69 - "Export File Management"
Cohesion: 0.23
Nodes (19): _atomic_write_csv(), _discover_domains(), _int_safe(), _iter_identity_csv(), _load_identity_items(), _load_label_population(), _load_pattern_map(), _load_representative_map() (+11 more)

### Community 70 - "Join Key Derivation"
Cohesion: 0.30
Nodes (18): _as_str_list(), choose_candidate_deterministically(), choose_record_handle(), derive_join_keys(), expand_globs(), extract_file_id(), is_usable_q(), load_join_key_policies() (+10 more)

### Community 71 - "Signature Hash Building"
Cohesion: 0.23
Nodes (16): apply_sig_hash_policy_to_record(), build_sig_hash_from_policy(), _items_to_map(), _key_allowed(), Any, Return (sig_hash, status, status_reasons, hash_items).      The builder hashes e, Mutate and return a canonical record dict with policy-generated sig_hash/status., get_domain_sig_hash_policy() (+8 more)

### Community 72 - "Browser Organization Probing"
Cohesion: 0.19
Nodes (17): _append_unique(), _best_name(), _builtin_label(), _clean_name(), _increment_count(), Resolve a built-in parameter label when Revit exposes one., Pick display name, preserving real folder names before descriptor fallbacks., Classify and resolve a FolderItemInfo.ElementId. (+9 more)

### Community 73 - "VFD Edge Discovery Testing"
Cohesion: 0.20
Nodes (17): read_csv(), test_discover_vfd_edges_applies_threshold_after_category_aggregation(), test_discover_vfd_edges_category_file_count_controls_generator_threshold(), test_discover_vfd_edges_emits_multi_domain_conflict_rows(), test_discover_vfd_edges_filters_hint_comments_and_exact_bip_lookup(), test_discover_vfd_edges_gaps_multi_domain_identity_items_missing(), test_discover_vfd_edges_ignores_unusable_category_rows(), test_discover_vfd_edges_ignores_unusable_param_ref_rows_with_item_quality() (+9 more)

### Community 74 - "Phase Graphics Probing"
Cohesion: 0.20
Nodes (16): _bucket_key(), _contract_value(), _format_param_contract(), _get_lines_category_id(), _hex_rgb_from_triplet(), _index_param(), _is_angle_datatype(), _is_length_datatype() (+8 more)

### Community 75 - "Cluster Label Backfill"
Cohesion: 0.27
Nodes (15): _build_cluster_representative_items(), _build_discriminator_lookup(), _extract_cluster_common_path_parts(), _file_map(), _iter_domains(), _load_json(), main(), _normalize_parts() (+7 more)

### Community 76 - "Phase 1 Diagnosis"
Cohesion: 0.24
Nodes (16): check_domain_records(), check_phase1_compatibility(), diagnose_exports(), extract_domains_from_fp(), load_json(), main(), predict_phase1_output(), Load JSON file safely. (+8 more)

### Community 77 - "Split Export Merging"
Cohesion: 0.18
Nodes (16): find_split_pairs(), load_json(), main(), merge_fingerprints(), print_summary(), Verify merged fingerprint has expected structure.          Returns: List of issu, Merge all split exports in input_dir into monolithic format in output_dir., Print summary of merge operation. (+8 more)

### Community 78 - "Domain Classification"
Cohesion: 0.19
Nodes (9): FiredEdgeRow, DomainPatternLabelCache, _evaluate_signal(), main(), Any, Path, Lazy `(domain, join_hash) -> human_label` lookup for domain patterns., Return the best `(source_join_hash, source_domain)` for a fired signal.      The (+1 more)

### Community 79 - "Shared IO Helpers"
Cohesion: 0.18
Nodes (13): atomic_write_csv(), atomic_write_json(), build_edge_aliases(), is_valid_item(), Shared IO/logging helpers for the cross-domain archetype discovery pipeline.  Co, Strip a trailing "_drafting"/"_model" suffix; None if neither present., Build edge_id -> canonical_edge_id and canonical -> [collapsed edge_ids].      T, True if an identity_items row carries usable evidence of a value. (+5 more)

### Community 80 - "Domain Profile Management"
Cohesion: 0.23
Nodes (3): DomainProfile, Declarative profile for comparing one domain family., Any

### Community 81 - "View Filter Definitions"
Cohesion: 0.24
Nodes (12): _element_filter_kind(), _flatten_element_filter(), _get_rules_from_element_parameter_filter(), _get_subfilters(), _maybe_set_example(), _observe(), Returns:       logic: "and" | "or" | "single" | "unknown"       rules: list of d, _resolve_category_name() (+4 more)

### Community 82 - "Element-Level Classification"
Cohesion: 0.19
Nodes (14): load_export_file(), Load one export JSON file., classify_file_elements(), compute_element_statistics(), extract_label_display(), generate_remediation_plan(), main(), Aggregate element-level classifications. (+6 more)

### Community 83 - "Phase Filter Probing"
Cohesion: 0.25
Nodes (13): _add_inventory_obs(), _fmt_display(), _format_param_contract(), _get_view_phase_filter_param(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _phase_status_bucket() (+5 more)

### Community 84 - "Phase Graphics Probing"
Cohesion: 0.26
Nodes (13): _fmt_display(), _format_param_contract(), _get_phasefilter_param_from_view(), _index_params_from_elem(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), Contract:       {         "q": "ok|missing|unreadable|unsupported",         "sto (+5 more)

### Community 85 - "Corpus Normalization Testing"
Cohesion: 0.20
Nodes (14): test_clean_corpus_unaffected_by_normalization(), test_population_hash_deterministic(), test_population_hash_in_manifest(), test_sanitize_folder_strips_path_separators(), _append_note(), _atomic_write_csv(), main(), _normalize_rows() (+6 more)

### Community 86 - "Client Summary Building"
Cohesion: 0.15
Nodes (15): build_cascade(), build_client_summary(), _col(), detect_bundle_schema(), get_client(), get_disc(), is_generic(), load_corpus_counts() (+7 more)

### Community 87 - "Configuration Probing"
Cohesion: 0.13
Nodes (14): analysis_run_id, cluster_method, convergence_thresholds, high, medium, domains_in_scope, ignored_thresholds, hhi_max (+6 more)

### Community 88 - "Run Configuration Management"
Cohesion: 0.13
Nodes (14): analysis_run_id, cluster_method, convergence_thresholds, high, medium, domains_in_scope, ignored_thresholds, hhi_max (+6 more)

### Community 89 - "Architecture Overview"
Cohesion: 0.14
Nodes (14): Architecture Overview, Context Builder, Core, Context Dictionary Schema, Dependency Contract, Design Intent, Domain Extractors, Layer 0 - Core (Pure Python) (+6 more)

### Community 90 - "View Template Comparison"
Cohesion: 0.18
Nodes (9): main(), _parse_args(), DomainProfile, Declares that item keys contain sig_hashes resolvable via sibling domains., ResolutionSpec, make_vt_profile(), Domain profile for view_templates_* partitions., For VCO synthetic items (item_key contains " > "), classify by property (+1 more)

### Community 91 - "Element-Level Classification"
Cohesion: 0.21
Nodes (13): Stream rows from a CSV file as dicts (UTF-8)., classify_file_elements(), compute_element_statistics(), extract_label_display(), generate_remediation_plan(), main(), Aggregate element-level classifications., Create actionable remediation plan for contaminated file. (+5 more)

### Community 92 - "Domain Pattern Patching"
Cohesion: 0.32
Nodes (13): _find_targets(), _load_cache(), _load_label_population(), main(), _patch_one(), tools/label_synthesis/patch_all_domain_patterns.py  Recursively patches pattern_, Patch one domain_patterns.csv.     Returns (n_updated, n_skipped_source, n_skipp, Return list of (domain_patterns_csv, label_synth_dir, label) tuples.     label i (+5 more)

### Community 93 - "Comparison Engine"
Cohesion: 0.33
Nodes (13): build_index(), compare_entries(), ensure_str(), get_items(), get_label_and_quality(), normalize_name(), _pair_name(), parse_name_map() (+5 more)

### Community 94 - "Fingerprint Compression"
Cohesion: 0.29
Nodes (13): _compress_file(), _find_json_files(), _fmt_kb(), _is_already_compact(), _load_json(), main(), Find fingerprint JSON files, preferring *__fingerprint.json., Write compact production JSON.     Returns bytes written. (+5 more)

### Community 95 - "Material Migration"
Cohesion: 0.26
Nodes (13): _find_json_files(), _get_identity_items(), _iter_materials_records(), _load_json(), main(), _migrate_file(), _migrate_record(), Inject material.graphics_sig_hash_v2, material.class, material.keynote,     mate (+5 more)

### Community 96 - "NA Token Testing"
Cohesion: 0.22
Nodes (6): TestIsBlankOrNa, TestIsNaToken, is_blank_or_na(), is_na_token(), True for any spelling of "not applicable" (na, n/a, N/A, not applicable,     not, True if value is blank (not yet filled in) or an explicit "not     applicable" s

### Community 97 - "Collision Differencing"
Cohesion: 0.26
Nodes (11): CollisionGroup, _get_join_hash(), _is_scalar(), _phase2_bucket_items(), _phase2_items_map(), For each top-level key, collect variants across records.     Returns key -> {"di, JSON-ish string for CSV cells; truncates long values explicitly., k -> (q,v) map for a single bucket. Returns duplicate_k_count explicitly. (+3 more)

### Community 98 - "View Template Analysis"
Cohesion: 0.26
Nodes (12): analyze_view_templates(), _detect_demo_plan(), _join_key_from_record(), main(), _pareto_cover(), _print_option_summary(), _print_sample_interpretation(), _project_identifier() (+4 more)

### Community 99 - "Knowledge Graph Rebuild"
Cohesion: 0.15
Nodes (13): vfd_bip_target_domain_hints exact_bip_id, fill_patterns domain_prompts module, wt.cfpsh in wall_types extractor, n_pairs threshold removed/raised, Join Key, Record schema version, incremental update and cluster-only, Use this repo guidance (+5 more)

### Community 100 - "Fingerprint API Mapping"
Cohesion: 0.35
Nodes (11): Fingerprint API Semantic Mapping Research, Phase 2 — Join-Key Discovery, Phase 0 / Phase 1 / Phase 2 Tools Map, _fingerprint_payload(), Path, test_detect_surfaces_counts_fingerprint_separately(), test_domain_discovery_prefers_fingerprint_candidates(), test_iter_export_files_prioritizes_fingerprint_and_uses_none_secondary() (+3 more)

### Community 101 - "Parameter Metadata Extraction"
Cohesion: 0.32
Nodes (12): _binding_scope(), _build_param_key(), extract(), _extract_param_meta(), _param_id_int(), Return GUID string when parameter is shared; empty string otherwise., Extract stable metadata for a parameter definition (schema-level, not value-leve, Return (storage_type, has_value, value_display, value_raw) for a parameter. (+4 more)

### Community 102 - "Arrowhead Type Probing"
Cohesion: 0.27
Nodes (9): _arrow_style_key(), _collect_dimension_types_with_tick_param(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _safe(), _safe_get_datatype(), _safe_param_def_name() (+1 more)

### Community 103 - "Identity Record Probing"
Cohesion: 0.24
Nodes (10): _add_inventory_record(), _as_str(), _definition_origin(), _format_param_contract(), _param_group_legacy_str(), Robust classifier that works even when Definition.BuiltInParameter is unavailabl, inv: dict param_key -> accumulator     Dedup rule (probe-local): group observati, _safe() (+2 more)

### Community 104 - "Reuse Classification Testing"
Cohesion: 0.15
Nodes (13): test_missing_source_identity_degrades_reuse_classification(), test_pattern_reuse_many_files_gets_broad_classification(), test_pattern_reuse_one_file_gets_single_file_classification(), test_project_used_view_uses_project_and_file_denominators_for_emerging_bucket(), test_reuse_distribution_order_is_deterministic(), test_reuse_thresholds_are_centralized_and_used(), test_reuse_zero_denominator_is_degraded_unclassified(), test_single_project_reuse_takes_precedence_over_emerging() (+5 more)

### Community 105 - "Feature Extraction"
Cohesion: 0.35
Nodes (10): _as_dict(), _as_int(), build_features(), _extract_counts_from_legacy(), Any, Extract stable count signals from legacy domain payloads when present.      Conv, Build deterministic features from payload.      Features include:       - schema, _sample_monolithic() (+2 more)

### Community 106 - "Domain Patterns Labeling"
Cohesion: 0.36
Nodes (11): _load_cache(), _load_label_population(), main(), patch(), tools/label_synthesis/patch_domain_patterns_labels.py  Targeted label patcher: u, Load joinhash_label_population.csv for a domain, keyed by join_hash., _read_csv(), _try_modal() (+3 more)

### Community 107 - "Family Types Probing"
Cohesion: 0.45
Nodes (11): _cat_info(), _element_name(), _family_record(), _family_symbols(), _format_double(), _id_int(), _normalize_double(), _param_definition_identity() (+3 more)

### Community 108 - "Phase Parameter Probing"
Cohesion: 0.30
Nodes (9): _fmt_display(), _format_param_contract(), _inv_add(), _inv_init(), _is_angle_datatype(), _is_length_datatype(), _phase_key(), _safe() (+1 more)

### Community 109 - "View Template Probing"
Cohesion: 0.30
Nodes (9): _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), Contract:       {         "q": "ok|missing|unreadable|unsupported",         "sto, _safe(), _safe_get_datatype(), _safe_view_name() (+1 more)

### Community 110 - "JSON Comparison Utilities"
Cohesion: 0.32
Nodes (11): _canon_obj(), canonical_json_bytes(), compare_json(), diff_paths(), pretty_json(), Returns (equal, summary)     summary includes stable hashes and bounded diffs., Return an object that is stable under json.dumps(sort_keys=True)., Canonical JSON encoding used for hashing and deterministic file writes. (+3 more)

### Community 111 - "Repository Path Analysis"
Cohesion: 0.21
Nodes (8): _candidate_repo_dirs(), _is_probably_sync_path(), _is_repo_root(), _iter_dyn_path_candidates(), _nearest_repo_root_from_path(), # NOTE: IN[3] is reserved for .dyn graph-path probing (see _iter_dyn_path_candid, Heuristic, Windows-centric: previously used to hard-block sync paths.     Retain, # NOTE: Documents is sometimes redirected into OneDrive/SharePoint.

### Community 112 - "Family Mapping Testing"
Cohesion: 0.17
Nodes (7): Test family mapping correctness via SHAPE_TO_FAMILY., Linear and LinearFixed must map to linear family., Radial, Diameter, DiameterLinked must map to radial family., Angular and ArcLength must map to angular family., Spot elevation/coordinate/slope must map to spot family., Unknown shape must map to unknown family., TestFamilyMappings

### Community 113 - "Hash Policy Testing"
Cohesion: 0.42
Nodes (11): Path, Stratified sampling gives each family equal weight regardless of type count., test_loaded_family_types_skips_orphan_gate_buckets(), test_loaded_family_types_surfaces_missing_shape_gate_records(), test_out_policy_creates_parent_directories(), test_phase0_dir_auto_resolves_results_records(), test_phase0_dir_can_be_results_root(), test_stratify_by_limits_overrepresentation() (+3 more)

### Community 114 - "Desktop Connector Scanner"
Cohesion: 0.24
Nodes (11): load_existing_includes(), main(), parse_types(), acc_scan_dc.py — Desktop Connector / network folder scanner  Walks a root folder, Walk root, yield one dict per matching file.     Skips names starting with '~$', Return {relative_path: include_value} from an existing manifest., Expand --types argument to a set of lowercase extensions., Return the Revit version year as a string (e.g. "2025"),     "stub" if the file (+3 more)

### Community 115 - "Desktop Connector Sync Tool"
Cohesion: 0.24
Nodes (11): hydrate(), is_stub(), load_included_entries(), main(), acc_sync_dc.py — Desktop Connector pre-sync tool  Reads acc_manifest.csv, identi, Trigger hydration of a stub by opening the file for read.     Polls until cloud-, Write a persistent timestamped sync log.      Each result dict must have:, Return True if the file is an online-only stub (not fully downloaded).      Uses (+3 more)

### Community 116 - "Population Framing Analysis"
Cohesion: 0.35
Nodes (11): classify_population_shape(), effective_cluster_count(), hhi(), load_csv(), load_json(), main(), pick_population_baselines(), Any (+3 more)

### Community 117 - "Domain Similarity Comparison"
Cohesion: 0.33
Nodes (11): _build_file_universe(), DomainSimilarityRow, _load_metadata(), _load_records_grouped(), main(), _multiset_jaccard(), _pair_type(), _passes_filters() (+3 more)

### Community 118 - "Manifest Comparison"
Cohesion: 0.35
Nodes (9): _diff_manifests(), _load_json(), main(), _to_manifest(), build_manifest(), Any, Build a deterministic manifest derived from payload["_contract"].      Args:, _safe_dict() (+1 more)

### Community 119 - "Element Dominance Emission"
Cohesion: 0.29
Nodes (8): main(), _read_csv_rows(), _write_csv_atomic(), Path, main(), Path, _read_csv_rows(), _write_csv_atomic()

### Community 120 - "Intradomain Identity Summary"
Cohesion: 0.29
Nodes (12): build_intradomain_summary(), _extract_identity_items(), _load_export_by_file_id(), main(), _pick_representative(), _profile_records(), Aggregate identity evidence across records in a representative file., _safe_str() (+4 more)

### Community 121 - "Join Hash Parameter Extraction"
Cohesion: 0.33
Nodes (9): _extract_qv_from_value(), _get_join_hash(), _is_scalar(), _iter_record_parameters(), _phase2_bucket_items(), Yield (param_key, q, v) observations for a single record.     Returns (observati, Normalize a value into (q, v_str) while preserving explicit states when present., _stable_json() (+1 more)

### Community 122 - "Template Governance Discovery"
Cohesion: 0.31
Nodes (10): classify(), containment_score(), jaccard_multiset(), main(), _parse_args(), step_template_governance_discovery.py  Reads records.csv and computes per-domain, Share of template's sig_hashes present in the corpus file.      Iterates templat, run() (+2 more)

### Community 123 - "CSV Contract Analysis"
Cohesion: 0.18
Nodes (11): CSV Contract v2.1, Pattern ID and Label Rules (v2.1), Split export removed, V2.1 Analysis Schema, V2.1 Determinism & Identity, V2.1 Phase 0 Export Schema, Central Path Norm Rule, Fingerprint Hashing Rules (+3 more)

### Community 124 - "Results Registry Builder"
Cohesion: 0.27
Nodes (10): atomic_write_csv(), build_results_registry_rows(), main(), Build and atomically write results_registry.csv. Returns rows written., Read a CSV file into string-normalized dictionaries., Write CSV rows atomically using a temp file in the destination directory., Return one results-registry row for every segment in the manifest., read_csv_rows() (+2 more)

### Community 125 - "Bundle Overlap Analysis"
Cohesion: 0.33
Nodes (11): annotate_bundle_overlap(), compare_directed_file(), compare_symmetric_file(), _fmt(), _mean(), _min(), _pct(), Return (n_both, n_a_only, n_b_only) for shared join_hashes. (+3 more)

### Community 126 - "Split Export Examples"
Cohesion: 0.33
Nodes (9): example_combined_workflow(), example_details_workflow(), example_index_only_workflow(), main(), Example: Using both index and details together.      Use case: Full analysis pip, Run example workflows demonstrating split export usage., Example: Fast contract validation using only index.json.      Use case: CI/CD pi, Example: Record-level analysis using details.json.      Use case: Similarity com (+1 more)

### Community 127 - "Reference Bundle Management"
Cohesion: 0.31
Nodes (8): _escape_control_chars_in_json_strings(), load_and_validate(), Return JSON text with raw control characters escaped only inside strings., write_sidecar(), Path, test_load_and_validate_allows_legacy_control_characters(), test_load_and_validate_allows_raw_newline_in_string(), Path

### Community 128 - "Filename Generation"
Cohesion: 0.44
Nodes (9): build_output_filename(), derive_doc_key(), _file_stem_from_doc(), _project_information(), Any, Returns identifiers suitable for filenames and indexing.      Keyed ONLY to the, Build a filename tied to RVT identity.      Args:         doc: Revit document, safe_slug() (+1 more)

### Community 129 - "Membership Rows Testing"
Cohesion: 0.27
Nodes (10): test_export_run_ids_sorted_pipe_delimited(), test_membership_rows_no_pipe_delimited_values(), test_registry_both_new_and_removed_files_reasons_when_combined_change(), test_registry_new_files_reason_does_not_cause_false_removal_warnings(), test_registry_new_files_reason_when_file_added(), test_registry_removed_files_reason_when_file_removed(), _build_membership_rows(), _membership_by_segment() (+2 more)

### Community 130 - "Shape Constant Testing"
Cohesion: 0.20
Nodes (6): SHAPE_INT_TO_NAME must map DimensionStyleType enum values correctly., Test shape constant definitions and mappings., All expected shape constants must be defined., All expected family constants must be defined., SHAPE_TO_FAMILY must map all shapes to families., TestShapeConstants

### Community 131 - "Hashing Incremental Testing"
Cohesion: 0.29
Nodes (9): Reference implementation: MD5("|".join(safe_str(v) for v in values)) over UTF-8, _reference_hash(), test_make_hash_accepts_generator_large_input_sanity(), test_make_hash_deterministic_repeated_calls(), test_make_hash_handles_unrepr_values(), test_make_hash_is_order_sensitive_contract(), test_make_hash_matches_reference_empty(), test_make_hash_matches_reference_multiple_and_unicode_and_pipes() (+1 more)

### Community 133 - "Governance State Summary"
Cohesion: 0.24
Nodes (10): _add_float(), build_governance_state_summary(), load_delta_summary(), _mean(), pf(), _pick(), Summarise legacy delta patterns by attribution category per comparison type., Return the first non-empty value from row for the provided column names. (+2 more)

### Community 134 - "Pareto Analysis with Splits"
Cohesion: 0.31
Nodes (8): assess_split_likelihood(), detect_pareto_cliffs(), main(), Run Pareto analysis with automatic split detection., Detect cliffs in Pareto front that indicate splits., Assess likelihood of organizational split based on Pareto cliffs., run_pareto_with_split_detection(), DataFrame

### Community 135 - "Typography Features Extraction"
Cohesion: 0.50
Nodes (8): _extract_features(), _get_p2_value(), _get_top(), main(), _norm_scalar(), Extract the typography surfaces we care about, preferring top-level where presen, run(), Any

### Community 136 - "View Category Overrides Analysis"
Cohesion: 0.31
Nodes (8): analyze_override_patterns(), _extract_override_record(), main(), View Category Overrides Join Key Discovery  Hypothesis: Override identity = base, Compute a stable hash for delta items (k/v pairs) to model delta_sig_hash., Return (baseline_sig, delta_sig, delta_items, record_id, label)., Analyze view_category_overrides for join key discovery.      Metrics:     - Base, _stable_delta_hash()

### Community 137 - "Drift Scoring"
Cohesion: 0.44
Nodes (7): _as_dict(), _load_json(), main(), _status_penalty(), _to_features(), _to_manifest(), Any

### Community 138 - "Shape-Gating Validation"
Cohesion: 0.39
Nodes (8): Validate shape-gating semantics and return structured issues.      Args:, validate_domain_join_key_policy(), test_rule_a1_discriminator_first_required(), test_rule_a2_no_overlap_common_required(), test_rule_a3_additional_required_in_optional_items(), test_rule_a4_requires_non_empty_additional_required(), test_rule_a5_orphaned_keys_warning_only(), test_valid_shape_gated_policy_has_no_errors()

### Community 139 - "Shape Gating Definitions"
Cohesion: 0.33
Nodes (9): shape_gating, default_shape_behavior, discriminator_key, shape_requirements, Arrow, Heavy end tick mark, true, additional_optional (+1 more)

### Community 140 - "RevitLookup Sync"
Cohesion: 0.42
Nodes (8): fetch_raw(), get_current_commit_sha(), github_get(), list_all_cs_files(), main(), Path, sync_revitlookup_reference.py  Copies RevitLookup descriptor source files into t, sync()

### Community 141 - "Segment Membership Analysis"
Cohesion: 0.25
Nodes (9): _membership_ids(), Path, Read segment_membership.csv and return the export_run_id set for one segment_id., _read_csv(), test_governance_role_case_variants_merge_and_no_false_warning(), test_main_writes_files(), test_manifest_and_registry_fields_stay_under_size_threshold(), test_segment_membership_join_keys_present_in_manifest_and_metadata() (+1 more)

### Community 143 - "Record Extraction"
Cohesion: 0.36
Nodes (6): Return all record.v2 dicts found anywhere in a fingerprint.details.json payload., extract_records(), get_domain_payload(), _Id, test_converted_old_and_new_records_converge(), _Type

### Community 144 - "Pairwise Drift Analysis"
Cohesion: 0.43
Nodes (7): _as_dict(), main(), Returns:       summary: dict with meaning columns       domain_scores: dict {dom, _repo_root(), _resolve_runs_dir(), _safe_float(), _summarize_drift()

### Community 145 - "Bundle Share Profiling"
Cohesion: 0.39
Nodes (7): build_bundle_share_profile(), _fmt_float(), _is_true(), main(), _parse_args(), Namespace, Path

### Community 146 - "View Filter Definitions"
Cohesion: 0.50
Nodes (7): build_prompt(), _collect_rules(), _format_rule_summary(), _get_value(), _is_opaque_name(), _op_short(), Any

### Community 147 - "Identity Items Lookup"
Cohesion: 0.43
Nodes (7): build_lookup(), _find_file(), main(), tools/label_synthesis/build_identity_items_lookup.py  Pre-processing step for sy, Return (key_col, value_col, quality_col) from a header row.     Supports both sc, _sniff_item_columns(), Path

### Community 148 - "Unit System Testing"
Cohesion: 0.68
Nodes (7): _length_record(), _payload(), test_accepts_degraded_records(), test_accepts_plural_meters(), test_broader_length_unit_matching(), test_continues_after_unrecognized_or_missing_unit_type_id(), _derive_unit_system()

### Community 149 - "Wall Type Reset"
Cohesion: 0.36
Nodes (7): _is_function_only_block(), main(), Path, Reset wall_type records that are blocked solely because wt.function=unsupported., Return {record_pk: {key: q}} for all wall_types items., True if wt.function is the only non-ok required item and compound structure item, _read_wall_items()

### Community 150 - "JSON IO Helper"
Cohesion: 0.48
Nodes (6): load_json(), main(), _now_stamp(), _write_json(), _write_text(), Small helper for tests; keep IO out of core exporter if you want.

### Community 151 - "Join Keys Application"
Cohesion: 0.53
Nodes (4): compute_join_hash(), extract_identity_map(), md5_utf8_join_pipe(), Any

### Community 152 - "Synthetic Key Computation"
Cohesion: 0.47
Nodes (5): main(), _parse_args(), _synthetic_line_patterns(), DataFrame, Namespace

### Community 153 - "Intradomain Definition Emission"
Cohesion: 0.47
Nodes (5): emit_ids_artifacts(), IDS, main(), _make_ids_ids(), Stable mapping from standard_name -> IDS_### (sorted by name).

### Community 154 - "Candidate Join Key Simulation"
Cohesion: 0.60
Nodes (4): _extract_features(), _get(), _qv(), Any

### Community 155 - "Frequent Itemset Finder"
Cohesion: 0.47
Nodes (5): find_closed_itemsets(), find_root_bundles(), Find closed frequent itemsets via pairwise-intersection candidate generation., Lightweight closed frequent itemset finder returning only root bundles     (item, _supporting_files_by_superset()

### Community 156 - "Graphify Reference Tools"
Cohesion: 0.33
Nodes (6): graphify reference: extra exports and benchmark, graphify reference: extraction subagent prompt, graphify reference: GitHub clone and cross-repo merge, graphify reference: commit hook and native CLAUDE.md integration, graphify reference: query, path, explain, graphify reference: transcribe video and audio

### Community 157 - "Pairwise Analysis"
Cohesion: 0.60
Nodes (5): load_csv(), main(), Any, Path, write_csv()

### Community 158 - "Identity Item Matching"
Cohesion: 0.50
Nodes (4): field_matches(), Match an identity_items item_key against an edge's source_field.      field_matc, _coherence_tier(), main()

### Community 159 - "Shape Input Preparation"
Cohesion: 0.50
Nodes (4): add_record_key(), main(), DataFrame, Series

### Community 160 - "Core Principles Overview"
Cohesion: 0.40
Nodes (5): Core Principles, Execution Environment, README Overview, Scope (Current), Status

### Community 161 - "Refactor Strategy"
Cohesion: 0.40
Nodes (5): Milestones, Non-Negotiables, Refactor Approach, Refactor Strategy, Target Structure

### Community 162 - "Domain Signature Policies"
Cohesion: 0.40
Nodes (4): identity_item_schema, record_schema_version, source_registry_version, version

### Community 163 - "Pattern Normhash Computation"
Cohesion: 0.70
Nodes (4): compute_norm_hash_for_group(), detect_cols(), main(), md5s()

### Community 164 - "Configuration Example"
Cohesion: 0.40
Nodes (4): cases, golden_dir, max_diffs, out_dir

### Community 166 - "Text Types Export Testing"
Cohesion: 0.60
Nodes (3): _Id, test_text_types_extract_emits_flat_items_only(), _Type

### Community 167 - "Governance Pipeline Overview"
Cohesion: 0.40
Nodes (5): Cross-Domain Archetype Discovery Pipeline, Bundle Analysis Pipeline, View Template Comparison, Domain Probe Inventory, Refreshed Definition of Done — Revit Standards Governance Narrative Outputs

### Community 168 - "Client Onboarding Profile"
Cohesion: 0.40
Nodes (5): _client_onboarding_profile(), _format_domain_items(), Return deterministic onboarding implications from client-level metrics., Render client-specific onboarding and operating implications., render_onboarding_section()

### Community 169 - "Contract Validation Testing"
Cohesion: 0.83
Nodes (3): main(), read_csv(), Path

### Community 170 - "Governance Role Patterns"
Cohesion: 0.50
Nodes (3): notes, rules, schema_version

### Community 172 - "Client Collection Data"
Cohesion: 0.50
Nodes (4): _client_collection_rows(), Mirrors real Sutter-shaped data: a client's Container/Template rows     are all, test_client_collection_discipline_leaf_gets_purpose_and_label(), test_client_collection_leaf_gets_purpose_and_label()

### Community 173 - "Collision Row Testing"
Cohesion: 0.50
Nodes (4): _collision_rows(), test_collection_label_segment_id_namespaced_in_output(), test_collection_label_value_does_not_collide_with_other_dimension_value(), test_non_collection_segment_ids_unaffected_by_namespacing()

### Community 174 - "Bundle Pattern Classification"
Cohesion: 0.83
Nodes (3): Path, test_emit_stub_classifies_root_to_leaf_patterns_as_differentiating(), _write_csv()

### Community 175 - "Integration Test Documentation"
Cohesion: 0.50
Nodes (3): Documented integration test patterns for full Revit validation., Policy load integration pattern placeholder., TestPolicyLoadPattern

### Community 176 - "Filtered Element Collector Testing"
Cohesion: 0.67
Nodes (3): PR5 policy:     - Domains must not directly import or reference FilteredElementC, _repo_root(), test_domains_do_not_reference_filtered_element_collector()

### Community 177 - "Sentinel Policy Testing"
Cohesion: 0.67
Nodes (3): Enforces PR3 sentinel policy:      - Domains may not contain any "<Token>" liter, _repo_root(), test_domains_do_not_emit_extra_angle_bracket_tokens()

### Community 178 - "Signature Hash Policy Generation"
Cohesion: 0.67
Nodes (3): build_policy(), main(), Any

### Community 181 - "Documentation Overview"
Cohesion: 0.67
Nodes (3): CHANGELOG, DECISIONS, INVARIANTS

## Knowledge Gaps
- **220 isolated node(s):** `PreToolUse`, `PreToolUse`, `version`, `record_schema_version`, `identity_item_schema` (+215 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **30 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `_phase2_item()` connect `Domain Indexing` to `Value Canonicalization`, `Phase 2 Join Key Management`?**
  _High betweenness centrality (0.203) - this node is a cross-community bridge._
- **Why does `load_exports()` connect `Intradomain Identity Summary` to `Domain Indexing`, `Collision Differencing`, `Domain Record Extraction`, `Fingerprint API Mapping`, `Typography Features Extraction`, `Cluster Analysis and Metrics`, `Change Classification`, `Element-Level Classification`, `Reference Standards Building`, `Join Key Evaluation`, `Join Keys Application`, `Join Hash Parameter Extraction`, `Candidate Join Key Simulation`?**
  _High betweenness centrality (0.165) - this node is a cross-community bridge._
- **Why does `_load_governance_role_rules()` connect `Analysis Run Management` to `Attribute Analysis`?**
  _High betweenness centrality (0.110) - this node is a cross-community bridge._
- **Are the 79 inferred relationships involving `_build_segments()` (e.g. with `test_blank_client_label_level2_id_distinct_from_level1()` and `test_blank_discipline_does_not_generate_discipline_cut()`) actually correct?**
  _`_build_segments()` has 79 INFERRED edges - model-reasoned connections that need verification._
- **What connects `PreToolUse`, `PreToolUse`, `version` to the rest of the system?**
  _220 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Hashing and Key Generation` be split into smaller, more focused modules?**
  _Cohesion score 0.07960526315789473 - nodes in this community are weakly interconnected._
- **Should `Dimension Shape Detection` be split into smaller, more focused modules?**
  _Cohesion score 0.07472527472527472 - nodes in this community are weakly interconnected._