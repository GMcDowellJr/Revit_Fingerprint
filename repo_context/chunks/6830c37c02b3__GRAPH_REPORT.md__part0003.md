# Chunk of graphify-out/GRAPH_REPORT.md

- Source relative path: `graphify-out/GRAPH_REPORT.md`
- Chunk: 3 of 4
- Original line range: 802-1201
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 50653c6b8a269da31349c8c44444133fa7ab40b96b82ad274a5df417fda7f704
- Starts inside symbol: no
- Ends inside symbol: no

```
   802| ### Community 102 - "Document Identity Probing"
   803| Cohesion: 0.13
   804| Nodes (20): _add_inventory_record(), _as_str(), _definition_origin(), _format_param_contract(), _param_group_legacy_str(), _probe_document_identity(), _probe_revit_version(), _probe_wrap() (+12 more)
   805| 
   806| ### Community 103 - "Family Type Probing"
   807| Cohesion: 0.18
   808| Nodes (22): _cat_info(), _element_name(), _family_record(), _family_symbols(), _format_double(), _id_int(), _normalize_double(), _param_definition_identity() (+14 more)
   809| 
   810| ### Community 104 - "Phase Probing"
   811| Cohesion: 0.14
   812| Nodes (20): _fmt_display(), _format_param_contract(), _get_view_phase_param(), _inv_add(), _inv_init(), _is_angle_datatype(), _is_length_datatype(), _phase_key() (+12 more)
   813| 
   814| ### Community 105 - "Comparison Registry Testing"
   815| Cohesion: 0.19
   816| Nodes (22): ComparisonRegistryKey, Tests for comparison_registry.csv staleness tracking in…, _reg_row(), test_build_comparison_registry_rows_domain_scoped_run_omits_other_domains(), test_build_comparison_registry_rows_is_a_full_snapshot_no_carryover(), test_build_comparison_registry_rows_omits_pair_when_reference_segment_is_pending(), test_build_comparison_registry_rows_omits_pair_when_target_segment_is_failed(), test_build_comparison_registry_rows_omits_work_items_with_no_output() (+14 more)
   817| 
   818| ### Community 106 - "Canonical Item Management"
   819| Cohesion: 0.17
   820| Nodes (20): build_flat_items(), canonicalize_record(), compile_role_policy(), merge_legacy_buckets(), _normalize_item(), Any, Resolve roles from item.k via runtime lookup.      Returns grouped items without, Canonicalize a record to flat `items` shape and remove legacy/derived keys. (+12 more)
   821| 
   822| ### Community 107 - "DataFrame Operations"
   823| Cohesion: 0.17
   824| Nodes (22): DataFrame, Series, build_wide_kv_table(), compute_v_norm(), _dedupe_preserve_order(), dominates(), eval_subset(), EvalConfig (+14 more)
   825| 
   826| ### Community 108 - "Record Contract Validation"
   827| Cohesion: 0.14
   828| Nodes (18): exported_fingerprint_json(), Provide exporter output JSON for validation.      Options:       1) Set env var, test_all_exported_records_conform_to_record_contract_v2(), _compute_identity_quality(), _hash_preimage(), _is_allowed_indexed_key(), load_json_file(), _normalize_indexed_key() (+10 more)
   829| 
   830| ### Community 109 - "View Template Probing"
   831| Cohesion: 0.15
   832| Nodes (19): _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _probe_document_identity(), _probe_revit_version(), _probe_wrap(), Contract: { "q": "ok|missing|unreadable|unsupported", "storage":… (+11 more)
   833| 
   834| ### Community 110 - "Governance Manifest Testing"
   835| Cohesion: 0.27
   836| Nodes (21): _find(), _members(), _row(), test_blank_client_label_raises_defense_in_depth(), test_business_center_label_case_variants_merge_after_prefix_strip(), test_case_variant_role_merges_into_canonical_population(), test_client_label_case_variants_merge_to_first_seen_casing(), test_discipline_label_case_variants_merge() (+13 more)
   837| 
   838| ### Community 111 - "View Template Selector Testing"
   839| Cohesion: 0.13
   840| Nodes (21): _load_policy(), Join key build must report missing when view_template.def_hash is absent., All view_templates split domains must have a valid join_key_schema., All split view_template domains must require view_template.def_hash in join key., Floor/structural/area plans policy must use view_template.def_hash., Ceiling plans policy must use view_template.def_hash., Elevations/sections/detail policy must use view_template.def_hash., Renderings/drafting policy must use view_template.def_hash. (+13 more)
   841| 
   842| ### Community 112 - "Join Key Derivation"
   843| Cohesion: 0.25
   844| Nodes (21): _as_str_list(), choose_candidate_deterministically(), choose_record_handle(), derive_join_keys(), expand_globs(), extract_file_id(), extract_records(), index_items_by_k() (+13 more)
   845| 
   846| ### Community 113 - "Bundle Analysis Execution"
   847| Cohesion: 0.16
   848| Nodes (20): _emit_meta_scatter_thresholds(), _ensure_latent_purgeable(), _load_purgeable_only_set(), main(), _parse_args(), Namespace, Path, Read latent_purgeable.csv once and return the purgeable_only set.… (+12 more)
   849| 
   850| ### Community 114 - "Thin Runner Probing"
   851| Cohesion: 0.13
   852| Nodes (14): _candidate_repo_dirs(), _document_identity(), _flush_domain(), _is_probably_sync_path(), _is_probes_root(), _json_block(), _looks_like_unc_path(), _probe_in_for() (+6 more)
   853| 
   854| ### Community 115 - "Governance Evidence Packaging"
   855| Cohesion: 0.15
   856| Nodes (20): test_build_file_inventory_document_wraps_files_and_counts(), test_write_json_round_trips(), _artifact(), build_evidence_map(), build_file_inventory_document(), build_package_manifest(), _classify_scalar(), _column_dtype() (+12 more)
   857| 
   858| ### Community 116 - "Material Join Key Discovery"
   859| Cohesion: 0.20
   860| Nodes (18): _build_key(), _build_key_files(), _compute_metrics(), discover(), _extract_sig(), _is_system_material(), _load_class_map(), _load_materials() (+10 more)
   861| 
   862| ### Community 117 - "Join Key Derivation by IDs"
   863| Cohesion: 0.18
   864| Nodes (19): compute_coverage(), compute_join_hash_for_record(), derive_join_keys_by_ids(), evaluate_gates(), evaluate_keyset(), extract_identity_map(), greedy_select_keys(), jaccard_similarity() (+11 more)
   865| 
   866| ### Community 118 - "Arrowhead Identity Management"
   867| Cohesion: 0.19
   868| Nodes (18): _as_value_string(), _build_arrow_identity_items(), _build_common_identity_items(), _build_tick_identity_items(), _canon_yesno_bool(), extract(), _fmt_deg_from_rad(), _get_arrowhead_style() (+10 more)
   869| 
   870| ### Community 119 - "Subcategory ID Management"
   871| Cohesion: 0.15
   872| Nodes (18): build_subcategory_used_id_set(), Build/cache used subcategory ids for a given parent category., _build_info(), _collect_categories(), extract_analytical(), extract_annotation(), extract_imported(), extract_model() (+10 more)
   873| 
   874| ### Community 120 - "Join Key Policy Testing"
   875| Cohesion: 0.14
   876| Nodes (11): Test join key policies for each split dimension_types domain., Load join key policy for a specific split domain., Linear domain must require witness_line_control., Radial domain must require center_marks and center_mark_size., Angular domain must require unit_format_id., Diameter domain must have a valid policy., Spot elevation domain must have a valid policy., Spot coordinate domain must have a valid policy. (+3 more)
   877| 
   878| ### Community 121 - "Scope Key Governance Testing"
   879| Cohesion: 0.12
   880| Nodes (19): test_compute_scope_key_accepts_case_insensitive_deployment_override(), test_cross_segment_and_governance_manifest_share_policy_classification(), test_normalize_recognizes_enterprise_tokens_before_prefix_strip(), test_normalize_strips_bc_prefix_case_insensitive(), test_normalize_zero_pad_does_not_affect_bc_prefixed_values(), test_normalize_zero_pad_recognizes_collapsed_enterprise_token(), test_normalize_zero_pads_short_numeric_values(), test_scope_key_requires_both_conditions_for_enterprise() (+11 more)
   881| 
   882| ### Community 122 - "Common IO Helpers"
   883| Cohesion: 0.16
   884| Nodes (16): atomic_write_csv(), atomic_write_json(), build_edge_aliases(), Any, Path, Shared IO/logging helpers for the cross-domain archetype discovery pipeline.  Co, Strip a trailing "_drafting"/"_model" suffix; None if neither present., Build edge_id -> canonical_edge_id and canonical -> [collapsed edge_ids].      T (+8 more)
   885| 
   886| ### Community 123 - "VCO Profile Extraction"
   887| Cohesion: 0.14
   888| Nodes (19): _build_synthetic_items_for_pair(), _extract_active_vco_fields(), _extract_graphic_fields(), _extract_object_style_baseline_fields(), _get_domain_payload(), _is_default_vco_value(), _is_non_ok_quality(), _load_domain_records() (+11 more)
   889| 
   890| ### Community 124 - "VFD Edge Discovery Testing"
   891| Cohesion: 0.20
   892| Nodes (17): read_csv(), test_discover_vfd_edges_applies_threshold_after_category_aggregation(), test_discover_vfd_edges_category_file_count_controls_generator_threshold(), test_discover_vfd_edges_emits_multi_domain_conflict_rows(), test_discover_vfd_edges_filters_hint_comments_and_exact_bip_lookup(), test_discover_vfd_edges_gaps_multi_domain_identity_items_missing(), test_discover_vfd_edges_ignores_unusable_category_rows(), test_discover_vfd_edges_ignores_unusable_param_ref_rows_with_item_quality() (+9 more)
   893| 
   894| ### Community 125 - "Segment Orchestrator Testing"
   895| Cohesion: 0.21
   896| Nodes (8): _read_csv(), TestFilterNameKeyCsvToSegment, TestMergeBiOutputsExcludesStaleDomainsForEmptySegment, _write_csv(), _filter_name_key_csv_to_segment(), merge_bi_outputs(), Filter a corpus-wide name_key_results.csv (tools/apply_name_key_policy.py…, Pre-merge per-domain bundle analysis CSVs into single combined files for Power…
   897| 
   898| ### Community 126 - "Dimension Type Formatting"
   899| Cohesion: 0.15
   900| Nodes (18): _accuracy_label(), _center_marks_label(), _decoration_label(), _extract_kv(), format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/dimension_types.py  Behavioral synopsi, Map Revit dimension shape string to short label. (+10 more)
   901| 
   902| ### Community 127 - "Crosswalk Candidate Finding"
   903| Cohesion: 0.18
   904| Nodes (18): _already_resolved(), _already_resolved_via_param_display(), build_resolved_index(), find_candidates(), group_by_member(), _is_elementid_typed(), _is_int_like(), main() (+10 more)
   905| 
   906| ### Community 128 - "Material Probing"
   907| Cohesion: 0.19
   908| Nodes (15): _fmt_display(), _format_param_contract(), _is_length_datatype(), _probe_document_identity(), _probe_revit_version(), _probe_wrap(), Resolve an Element.WorksetId value to (name, resolved_bool) via…, _reflect_contract() (+7 more)
   909| 
   910| ### Community 129 - "Record Join Key Management"
   911| Cohesion: 0.19
   912| Nodes (8): build_name_key_for_record(), flat_items_for_record(), _has_detail_data(), Any, Reconstruct the join_key_name_identity dict for one already-exported record.…, Merge identity_basis.items + phase2 buckets into one flat items list, non-…, True if record carries record-level detail (identity_basis, phase2, or…, TestAnalysisSideReconstruction
   913| 
   914| ### Community 130 - "Record ID Finalization"
   915| Cohesion: 0.38
   916| Nodes (9): finalize_record_ids_for_domain(), make_record_id_structural(), Create a structural-hash record_id base and canonical preimage., Assign dup_index for structural record_id groups deterministically.      Mutates, _make_record(), test_structural_record_id_dup_index_deterministic(), test_structural_record_id_duplicate_keys_blocked(), test_structural_record_id_stable_across_runs() (+1 more)
   917| 
   918| ### Community 131 - "Fill Pattern Testing"
   919| Cohesion: 0.26
   920| Nodes (13): _coordination_item(), _FillPatternDef, _FillPatternElem, _Id, _module(), object, Minimal FillPattern double (the object GetFillPattern() returns)., Minimal FillPatternElement double -- no Revit API dependency. is_imported=None… (+5 more)
   921| 
   922| ### Community 132 - "Diff Engine Operations"
   923| Cohesion: 0.26
   924| Nodes (17): build_index(), compare_entries(), ensure_str(), extract_records(), get_domain_payload(), get_items(), get_label_and_quality(), index_items_by_key() (+9 more)
   925| 
   926| ### Community 133 - "Export File Management"
   927| Cohesion: 0.15
   928| Nodes (17): ExportFile, load_export_file(), One exported fingerprint JSON treated as one authority sample., Load one export JSON file., classify_file_elements(), compute_element_statistics(), extract_label_display(), generate_remediation_plan() (+9 more)
   929| 
   930| ### Community 134 - "Record Schema Definition"
   931| Cohesion: 0.12
   932| Nodes (17): type, enum, type, properties, debug, identity_quality, is_purgeable, record_id (+9 more)
   933| 
   934| ### Community 135 - "Graphify Command Management"
   935| Cohesion: 0.12
   936| Nodes (17): Add Watch Command, For /graphify add, For --watch, graphify reference: add a URL and watch a folder, Export Commands, graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag) (+9 more)
   937| 
   938| ### Community 136 - "Feature Extraction"
   939| Cohesion: 0.24
   940| Nodes (14): _as_dict(), _as_int(), build_features(), _extract_counts_from_legacy(), Any, Extract stable count signals from legacy domain payloads when present.      Conv, Build deterministic features from payload.      Features include:       - schema, build_manifest() (+6 more)
   941| 
   942| ### Community 137 - "Policy Health Monitoring"
   943| Cohesion: 0.12
   944| Nodes (17): _health(), A caller that hasn't adopted policy loading (policy_load_status omitted) must…, A caller that hasn't threaded this through (interpretation_guide_present…, PR review finding: governance_interpretation_guide carries…, test_health_all_policy_profiles_from_file_adds_no_warning(), test_health_complete_when_all_required_present_and_no_warnings(), test_health_degraded_not_invalid_when_only_optional_signal_is_a_warning(), test_health_degrades_when_interpretation_guide_absent() (+9 more)
   945| 
   946| ### Community 138 - "Cluster Label Input Management"
   947| Cohesion: 0.27
   948| Nodes (15): _build_cluster_representative_items(), _build_discriminator_lookup(), _extract_cluster_common_path_parts(), _file_map(), _iter_domains(), _load_json(), main(), _normalize_parts() (+7 more)
   949| 
   950| ### Community 139 - "Workset Management"
   951| Cohesion: 0.22
   952| Nodes (14): _collect_kind_counts(), _collect_user_worksets(), _discover_workset_kind_names(), extract_worksets(), extract_worksets_doc(), Map WorksetKind int value -> member name (e.g. 0 -> "UserWorkset").…, Returns (is_workshared, is_workshared_for_gating). is_workshared: True/False…, Returns (active_workset_id, lookup_ok). lookup_ok is False only when… (+6 more)
   953| 
   954| ### Community 140 - "Governance Narrative Reliability Testing"
   955| Cohesion: 0.20
   956| Nodes (15): Tests for the within_project score_reliability p10/p90 capture's segment_manifes, When the row's own segment already passes _is_unscoped_segment (the     pre-exis, segment_manifest resolves the root to a segment that simply has no     within_pr, Without segment_manifest, a bc-scoped-only row (not _is_unscoped_segment)     mu, The real-corpus scenario: the true root ("imperial|Project") is     registration, A registration segment with no redundant_single_child pointer (a     genuine dea, _row(), test_dead_end_redundant_chain_stays_unknown() (+7 more)
   957| 
   958| ### Community 141 - "Manifest Management"
   959| Cohesion: 0.15
   960| Nodes (16): _manifest(), Path, A caller that hasn't adopted policy loading (policy_profiles omitted) must get…, PR review finding: adding anomaly_thresholds (D-029) means every generated…, PR review finding: an automated consumer following reasoning_ prerequisites…, test_evidence_map_output_local_path_present_when_sibling_present_and_out_dir_supplied(), test_manifest_does_not_claim_missing_source_identifiers(), test_manifest_marks_input_present_based_on_real_filesystem_state() (+8 more)
   961| 
   962| ### Community 142 - "Domain Profile Management"
   963| Cohesion: 0.23
   964| Nodes (3): DomainProfile, Any, Declarative profile for comparing one domain family.
   965| 
   966| ### Community 143 - "JSON Diffing"
   967| Cohesion: 0.27
   968| Nodes (13): _canon_obj(), canonical_json_bytes(), compare_json(), diff_paths(), pretty_json(), Any, Returns (equal, summary)     summary includes stable hashes and bounded diffs., Return an object that is stable under json.dumps(sort_keys=True). (+5 more)
   969| 
   970| ### Community 144 - "Unscoped Segment Testing"
   971| Cohesion: 0.26
   972| Nodes (14): Tests for _is_unscoped_segment() in tools/generate_governance_narrative.py.…, build_segment_manifest.py's _subset_to_id() emits a literal empty token for a…, Blank client (empty token) followed by a REAL bc/collection value must still be…, See docs/governance_narrative_scope_gap_audit.md B5 -- a blank-role scope…, _row(), test_bc_scoped_segment_is_rejected(), test_blank_client_token_with_real_hidden_scope_value_is_rejected(), test_blank_role_rollup_is_rejected() (+6 more)
   973| 
   974| ### Community 145 - "Template Lookup Management"
   975| Cohesion: 0.15
   976| Nodes (14): _build_template_lookup(), _get_identity_item_value(), _get_phase2_cosmetic_value(), _get_template_vco(), _index_object_styles_by_row_key(), _index_vco_by_template(), _normalize_template_name(), Return the value of a named item from record phase2.cosmetic_items, or None. (+6 more)
   977| 
   978| ### Community 146 - "Material Identity Migration"
   979| Cohesion: 0.28
   980| Nodes (14): _find_json_files(), _get_identity_items(), _iter_materials_records(), _load_json(), main(), _migrate_file(), _migrate_record(), Any (+6 more)
   981| 
   982| ### Community 147 - "Governance Audits"
   983| Cohesion: 0.14
   984| Nodes (14): A.4 Verdict, Audit 8 — Bundle Pipeline Single-Source Assumptions (PR3 Step 0), Audit 9 — Segment-Orchestrator Name-Projection Support (PR4), Deployment configuration, Extract Orchestrator Stage Matrix, Governance Evidence Package, Governance Generator Coverage of Cross-Segment Outputs, Investigation: Group 1 (`tc`/`cp`/`tp`) scope-gating gap in `build_cascade()` (+6 more)
   985| 
   986| ### Community 148 - "Dependency Management"
   987| Cohesion: 0.25
   988| Nodes (11): Blocked, Any, core/deps.py  Centralized dependency enforcement for domain execution.  Non-nego, Typed exception used to signal a hard dependency block.      Attributes:, Require an upstream domain envelope to exist and be acceptable.      Args:, require_domain(), Exception, test_require_domain_allows_degraded_by_default() (+3 more)
   989| 
   990| ### Community 149 - "View Filter Definitions"
   991| Cohesion: 0.21
   992| Nodes (13): extract(), _logic_root_token(), _op_token_from_rule(), _param_ref_from_param_id(), Any, Return (value_v, value_q, kind_v) where kind_v is a stable rule kind token., Return (op_v, op_q) for vf.rule[i].op., Depth-first traversal accumulating parameter rules.      Returns:       (ok, rea (+5 more)
   993| 
   994| ### Community 150 - "Hash Policy Discovery Testing"
   995| Cohesion: 0.36
   996| Nodes (13): Path, Stratified sampling gives each family equal weight regardless of type count., test_discover_hash_policy_join_and_sig(), test_loaded_family_types_skips_orphan_gate_buckets(), test_loaded_family_types_surfaces_missing_shape_gate_records(), test_out_policy_creates_parent_directories(), test_phase0_dir_auto_resolves_records_subfolder(), test_phase0_dir_auto_resolves_results_records() (+5 more)
   997| 
   998| ### Community 151 - "Governance Narrative Client Testing"
   999| Cohesion: 0.24
  1000| Nodes (13): _bc_client_row(), _client_bc_row(), Tests for render_bc_composition_section() /…, test_bc_composition_section_absent_when_no_rows(), test_bc_composition_section_lists_clients_by_descending_share(), test_client_bc_distribution_falls_back_to_business_centers_list_when_bc_matrix_missing(), test_client_bc_distribution_no_fallback_bullets_when_business_centers_blank(), test_client_bc_distribution_section_absent_when_no_rows() (+5 more)
  1001| 
  1002| ### Community 152 - "Cascade Scope Management"
  1003| Cohesion: 0.14
  1004| Nodes (14): _minimal_cascade_dict(), PR review finding: the note must name which scope(s) qualified rather than…, Backward compatible with a hand-built union_breadth dict lacking the newer…, A domain whose primary containment sits between the weak and strong cascade…, primary (tp else cp) is None -- no basis to judge 'weak cascade'., Mirrors D-021/D-029's threshold-override test pattern: a --policy-dir override…, test_broad_reuse_note_names_qualifying_scope_when_present(), test_broad_reuse_note_omits_scope_clause_when_broad_scopes_missing() (+6 more)
  1005| 
  1006| ### Community 153 - "Inventory Scan Testing"
  1007| Cohesion: 0.24
  1008| Nodes (14): test_inventory_scan_all_blank_column_is_empty_dtype(), test_inventory_scan_blank_cells_do_not_break_integer_inference(), test_inventory_scan_dedupes_same_file_seen_via_two_scan_dirs(), test_inventory_scan_excludes_known_paths(), test_inventory_scan_header_only_file_is_empty_file(), test_inventory_scan_infers_column_dtypes(), test_inventory_scan_mixed_numeric_and_text_column_is_string(), test_inventory_scan_never_retains_sample_values() (+6 more)
  1009| 
  1010| ### Community 154 - "NA Token Testing"
  1011| Cohesion: 0.22
  1012| Nodes (6): TestIsBlankOrNa, TestIsNaToken, is_blank_or_na(), is_na_token(), True for any spelling of "not applicable" (na, n/a, N/A, not applicable,     not, True if value is blank (not yet filled in) or an explicit "not     applicable" s
  1013| 
  1014| ### Community 155 - "Domain Pattern Patching"
  1015| Cohesion: 0.32
  1016| Nodes (13): _find_targets(), _load_cache(), _load_label_population(), main(), _patch_one(), Any, Path, tools/label_synthesis/patch_all_domain_patterns.py  Recursively patches pattern_ (+5 more)
  1017| 
  1018| ### Community 156 - "Fingerprint JSON Compression"
  1019| Cohesion: 0.29
  1020| Nodes (13): _compress_file(), _find_json_files(), _fmt_kb(), _is_already_compact(), _load_json(), main(), Any, Path (+5 more)
  1021| 
  1022| ### Community 157 - "Collision Differencing"
  1023| Cohesion: 0.30
  1024| Nodes (13): CollisionGroup, _get_join_hash(), _is_scalar(), main(), _phase2_bucket_items(), _phase2_items_map(), Any, For each top-level key, collect variants across records.     Returns key -> {"di (+5 more)
  1025| 
  1026| ### Community 158 - "Element Level Classification"
  1027| Cohesion: 0.21
  1028| Nodes (13): classify_file_elements(), compute_element_statistics(), extract_label_display(), generate_remediation_plan(), main(), Aggregate element-level classifications., Stream rows from a CSV file as dicts (UTF-8 with BOM support)., Create actionable remediation plan for contaminated file. (+5 more)
  1029| 
  1030| ### Community 159 - "Workset Probing"
  1031| Cohesion: 0.21
  1032| Nodes (10): _discover_enum_members(), _probe_document_identity(), _probe_revit_version(), _probe_wrap(), Returns [(name, int_value), ...] for a CLR enum type, or [] if unavailable., _reflect_contract(), _reflect_member_names(), _reflect_try_get() (+2 more)
  1033| 
  1034| ### Community 160 - "Schema Definition"
  1035| Cohesion: 0.15
  1036| Nodes (13): type, type, additionalProperties, properties, required, type, components, display (+5 more)
  1037| 
  1038| ### Community 161 - "Filename Management"
  1039| Cohesion: 0.29
  1040| Nodes (12): build_output_filename(), derive_doc_key(), _file_stem_from_doc(), _project_information(), Any, Returns identifiers suitable for filenames and indexing.      Keyed ONLY to the, Build a filename tied to RVT identity.      Args:         doc: Revit document, safe_slug() (+4 more)
  1041| 
  1042| ### Community 162 - "Family Type Extraction"
  1043| Cohesion: 0.32
  1044| Nodes (12): _binding_scope(), _build_param_key(), extract(), _extract_param_meta(), _param_id_int(), Return GUID string when parameter is shared; empty string otherwise., Extract stable metadata for a parameter definition (schema-level, not value-…, Return (storage_type, has_value, value_display, value_raw) for a parameter.… (+4 more)
  1045| 
  1046| ### Community 163 - "Domain Classification Management"
  1047| Cohesion: 0.28
  1048| Nodes (9): FiredEdgeRow, DomainPatternLabelCache, _evaluate_signal(), main(), Any, Path, Lazy `(domain, join_hash) -> human_label` lookup for domain patterns., Return the best `(source_join_hash, source_domain)` for a fired signal.      The (+1 more)
  1049| 
  1050| ### Community 164 - "Feature Documentation"
  1051| Cohesion: 0.15
  1052| Nodes (12): Change set, Classification, Compatibility & migration, Design, Feature PR — New or Extended Capability, Motivation / Problem statement, Reviewer guidance, Risks / failure modes (+4 more)
  1053| 
  1054| ### Community 165 - "Population Verification"
  1055| Cohesion: 0.29
  1056| Nodes (13): _items_row(), test_discover_greedy_required_seed_still_scores_challengers(), test_discover_greedy_seeds_selected_with_required_fields(), test_discover_greedy_without_required_fields_behaves_as_before(), test_full_population_verify_detects_fragmentation_sample_missed(), test_full_population_verify_flags_collision_rate_delta_above_threshold(), test_full_population_verify_flags_coverage_collapse_even_with_zero_collision_and_fragmentation(), test_full_population_verify_no_divergence_for_coverage_drop_within_threshold() (+5 more)
  1057| 
  1058| ### Community 166 - "Cross Domain Item Building"
  1059| Cohesion: 0.29
  1060| Nodes (12): _build_dynamic_rows(), _build_structural_rows(), _load_identity_items(), main(), _parse_vf_categories(), Any, Path, Parse a vf.categories value into category-id strings.      Discovery accepts bot (+4 more)
  1061| 
  1062| ### Community 167 - "View Template Join Key Analysis"
  1063| Cohesion: 0.26
  1064| Nodes (12): analyze_view_templates(), _detect_demo_plan(), _join_key_from_record(), main(), _pareto_cover(), _print_option_summary(), _print_sample_interpretation(), _project_identifier() (+4 more)
  1065| 
  1066| ### Community 168 - "Material Management"
  1067| Cohesion: 0.29
  1068| Nodes (11): _canon_id_local(), _export_ctx(), extract(), _mk_item(), Return (value, q) safe for make_identity_item without sentinel literals., Return (v, q) for optional identity metadata fields., _read_param_as_string(), _read_prop() (+3 more)
  1069| 
  1070| ### Community 169 - "Thin Runner Operations"
  1071| Cohesion: 0.21
  1072| Nodes (8): _candidate_repo_dirs(), _is_probably_sync_path(), _is_repo_root(), _iter_dyn_path_candidates(), _nearest_repo_root_from_path(), # NOTE: IN[3] is reserved for .dyn graph-path probing (see…, Heuristic, Windows-centric: previously used to hard-block sync paths. Retained…, # NOTE: Documents is sometimes redirected into OneDrive/SharePoint.
  1073| 
  1074| ### Community 170 - "Worker Count Testing"
  1075| Cohesion: 0.27
  1076| Nodes (11): Tests for resolve_worker_count() in tools/compare_cross_segment.py., test_auto_caps_at_61_on_windows(), test_auto_derives_from_cpu_count(), test_auto_is_case_insensitive_and_trims_whitespace(), test_auto_never_returns_zero_on_low_core_count(), test_auto_uncapped_on_non_windows(), test_auto_with_no_cpu_count_falls_back_to_four(), test_explicit_int_passthrough() (+3 more)
  1077| 
  1078| ### Community 171 - "Family Mapping Testing"
  1079| Cohesion: 0.17
  1080| Nodes (7): Test family mapping correctness via SHAPE_TO_FAMILY., Linear and LinearFixed must map to linear family., Radial, Diameter, DiameterLinked must map to radial family., Angular and ArcLength must map to angular family., Spot elevation/coordinate/slope must map to spot family., Unknown shape must map to unknown family., TestFamilyMappings
  1081| 
  1082| ### Community 172 - "Unit System Testing"
  1083| Cohesion: 0.39
  1084| Nodes (11): _legacy_length_record(), _length_record(), _payload(), Pre-canonicalization identity_basis shape, kept only for the fallback that…, Canonical flat-`items` shape -- what runner/run_dynamo.py's…, test_accepts_degraded_records(), test_accepts_plural_meters(), test_broader_length_unit_matching() (+3 more)
  1085| 
  1086| ### Community 173 - "Governance Narrative Testing"
  1087| Cohesion: 0.14
  1088| Nodes (20): _minimal_fixture(), _pooled_row(), Path, Tests for policy externalization (PR3) in…, generate_governance_narrative.py's own default finding_rules profile is…, Mirrors D-021's threshold-override test pattern (D-029): a --policy-dir…, No --policy-dir passed -- must resolve to policies/governance/ and report…, Passing --policy-dir pointing explicitly at the shipped policies/governance/… (+12 more)
  1089| 
  1090| ### Community 174 - "Governance State Testing"
  1091| Cohesion: 0.21
  1092| Nodes (10): _gs_row(), Tests for governance-state comparison_type handling in tools/generate_governance, A domain whose ENTIRE governance-state signal is Group 3 (scope-level     fan-ou, A future producer-side addition/removal to GOVERNANCE_STATE_DIRECTED_TYPES     m, bc_to_project (scope-level) and template_to_project (cascade-stage) rows for, _state_row(), test_compact_summary_loop_does_not_blend_distinct_comparison_types(), test_detailed_loop_no_longer_drops_new_scope_types() (+2 more)
  1093| 
  1094| ### Community 175 - "Name Key Policy Testing"
  1095| Cohesion: 0.17
  1096| Nodes (3): name_key_policies(), fixture, TestDimensionConfigNonInclusion
  1097| 
  1098| ### Community 176 - "Segment Completion Testing"
  1099| Cohesion: 0.36
  1100| Nodes (3): PR #390 review: a segment already marked complete under a prior config-only run…, TestCLIComparisonTarget, TestCompleteSegmentSkipHonorsNameTarget
  1101| 
  1102| ### Community 177 - "Worker Split Testing"
  1103| Cohesion: 0.27
  1104| Nodes (11): Tests for compute_worker_split() in tools/run_segment_orchestrator.py., test_auto_with_no_cpu_count_falls_back_to_hardcoded_four_four(), test_budget_of_one_never_returns_zero(), test_explicit_segment_workers_coordinates_domain_workers(), test_explicit_segment_workers_never_returns_zero_domain_workers(), test_explicit_segment_workers_small_n_gets_larger_domain_share(), test_explicit_segment_workers_with_no_cpu_count_falls_back_to_four_budget(), test_large_budget_gives_expected_split() (+3 more)
  1105| 
  1106| ### Community 178 - "Desktop Connector Scanning"
  1107| Cohesion: 0.24
  1108| Nodes (11): load_existing_includes(), main(), parse_types(), acc_scan_dc.py — Desktop Connector / network folder scanner Walks a root…, Walk root, yield one dict per matching file. Skips names starting with '~$'…, Return {relative_path: include_value} from an existing manifest., Expand --types argument to a set of lowercase extensions., Return the Revit version year as a string (e.g. "2025"), "stub" if the file is… (+3 more)
  1109| 
  1110| ### Community 179 - "Desktop Connector Syncing"
  1111| Cohesion: 0.26
  1112| Nodes (11): hydrate(), is_stub(), load_included_entries(), main(), acc_sync_dc.py — Desktop Connector pre-sync tool Reads acc_manifest.csv,…, Trigger hydration of a stub by opening the file for read. Polls until cloud-…, Write a persistent timestamped sync log. Each result dict must have:…, Return True if the file is an online-only stub (not fully downloaded). Uses… (+3 more)
  1113| 
  1114| ### Community 180 - "Reference Graph Generation"
  1115| Cohesion: 0.36
  1116| Nodes (11): read_csv_rows(), _build_dynamic_edges(), _check_static_edge_availability(), main(), _normalize_param_name(), _param_id_slug(), Any, Path (+3 more)
  1117| 
  1118| ### Community 181 - "Results Registry Management"
  1119| Cohesion: 0.29
  1120| Nodes (11): atomic_write_csv(), build_results_registry_rows(), main(), Path, Build and atomically write results_registry.csv. Returns rows written., Read a CSV file into string-normalized dictionaries., Write CSV rows atomically using a temp file in the destination directory., Return one results-registry row for every segment in the manifest. (+3 more)
  1121| 
  1122| ### Community 182 - "Domain Pattern Label Patching"
  1123| Cohesion: 0.36
  1124| Nodes (11): _load_cache(), _load_label_population(), main(), patch(), Any, Path, tools/label_synthesis/patch_domain_patterns_labels.py  Targeted label patcher: u, Load joinhash_label_population.csv for a domain, keyed by join_hash. (+3 more)
  1125| 
  1126| ### Community 183 - "Join Hash Parameter Population"
  1127| Cohesion: 0.33
  1128| Nodes (11): _extract_qv_from_value(), _get_join_hash(), _is_scalar(), _iter_record_parameters(), main(), _phase2_bucket_items(), Any, Yield (param_key, q, v) observations for a single record.     Returns (observati (+3 more)
  1129| 
  1130| ### Community 184 - "Population Framing"
  1131| Cohesion: 0.35
  1132| Nodes (11): classify_population_shape(), effective_cluster_count(), hhi(), load_csv(), load_json(), main(), pick_population_baselines(), Any (+3 more)
  1133| 
  1134| ### Community 185 - "Probe Inventory Testing"
  1135| Cohesion: 0.42
  1136| Nodes (11): Concrete, hand-traceable acceptance numbers for the crosswalk report, across…, _read_csv_rows(), _run(), _run_shaped_payload(), test_all_inputs_invalid_refuses_to_overwrite_by_default(), test_crosswalk_column_profile_across_runs(), test_empty_probes_dir_refuses_to_overwrite_by_default(), test_empty_probes_dir_with_force_writes_empty_inventory() (+3 more)
  1137| 
  1138| ### Community 186 - "Domain Similarity Comparison"
  1139| Cohesion: 0.33
  1140| Nodes (11): _build_file_universe(), DomainSimilarityRow, _load_metadata(), _load_records_grouped(), main(), _multiset_jaccard(), _pair_type(), _passes_filters() (+3 more)
  1141| 
  1142| ### Community 187 - "Domain Extraction"
  1143| Cohesion: 0.18
  1144| Nodes (11): _domain_run(), _extract_legacy_quality(), _extract_v2_block_reasons(), _extract_v2_hash(), _has_v2_surface(), _looks_like_revit_unique_id(), Best-effort extraction of the contract semantic hash (v2) without changing…, Best-effort extraction of v2 block reasons from a domain payload. Domains are… (+3 more)
  1145| 
  1146| ### Community 188 - "Manifest Row Testing"
  1147| Cohesion: 0.16
  1148| Nodes (11): _full_row(), _manifest_row(), Like _meta_row, but every required field must be passed explicitly -- for…, Hand-craft a manifest-row-shaped dict for testing _build_registry() in…, test_business_center_0000_main_succeeds(), test_business_center_case_variants_of_0000_still_fold_by_casing_not_bookkeeping(), test_collection_label_column_absence_produces_identical_manifest(), test_enterprise_bc_0000_preserved_literally_not_folded_to_blank() (+3 more)
  1149| 
  1150| ### Community 189 - "Membership Row Testing"
  1151| Cohesion: 0.24
  1152| Nodes (11): test_export_run_ids_sorted_pipe_delimited(), test_former_collection_specific_rows_collapse_with_union_membership(), test_membership_rows_no_pipe_delimited_values(), test_registry_both_new_and_removed_files_reasons_when_combined_change(), test_registry_new_files_reason_does_not_cause_false_removal_warnings(), test_registry_new_files_reason_when_file_added(), test_registry_removed_files_reason_when_file_removed(), _build_membership_rows() (+3 more)
  1153| 
  1154| ### Community 190 - "Identity Lineage Testing"
  1155| Cohesion: 0.36
  1156| Nodes (9): test_identity_lineage_items_are_preserved_in_canonical_conversion(), iter_input_files(), main(), parse_domains(), process_payload(), Any, Counter, Path (+1 more)
  1157| 
  1158| ### Community 191 - "Bundle Analysis Testing"
  1159| Cohesion: 0.35
  1160| Nodes (4): CompletedProcess, Path, PR #390 review, third round: run_bundle_analysis.py only ever writes per-domain…, TestStaleNameBundleOutputClearedBeforeRerun
  1161| 
  1162| ### Community 192 - "Domain Profile Definition"
  1163| Cohesion: 0.18
  1164| Nodes (6): Declares that item keys contain sig_hashes resolvable via sibling domains., ResolutionSpec, make_vt_profile(), Domain profile for view_templates_* partitions., For VCO synthetic items (item_key contains " > "), classify by property, ViewTemplateDomainProfile
  1165| 
  1166| ### Community 193 - "Intradomain Summary Building"
  1167| Cohesion: 0.36
  1168| Nodes (10): build_intradomain_summary(), _extract_identity_items(), _load_export_by_file_id(), main(), _pick_representative(), _profile_records(), Any, DataFrame (+2 more)
  1169| 
  1170| ### Community 194 - "Schema Definition"
  1171| Cohesion: 0.20
  1172| Nodes (10): minLength, type, additionalProperties, properties, required, type, const, hash_alg (+2 more)
  1173| 
  1174| ### Community 195 - "Schema Properties"
  1175| Cohesion: 0.22
  1176| Nodes (10): additionalProperties, items, minItems, properties, required, type, items, k (+2 more)
  1177| 
  1178| ### Community 196 - "Governance Pipeline Management"
  1179| Cohesion: 0.22
  1180| Nodes (10): Governance Question Routes, Governance Reading Order, Deployment Policy Data, Cross-Domain Archetype Discovery Pipeline, Archetype Decision Point 1 - Candidate Promotion, Archetype Decision Point 2 - Approach Label Ratification, Bundle Analysis Pipeline, Probe Export Data (+2 more)
  1181| 
  1182| ### Community 197 - "Documentation Instructions"
  1183| Cohesion: 0.20
  1184| Nodes (9): CI context, Helpful quick links, Important conventions, Knowledge graph (graphify), Project structure, Revit Fingerprint workspace instructions, Test and development commands, Use this repo guidance (+1 more)
  1185| 
  1186| ### Community 198 - "Pull Request Template"
  1187| Cohesion: 0.20
  1188| Nodes (9): Changes, Notes for reviewers, Risks / Failure modes, Rollback plan, Scope, Summary, Type, Verification (+1 more)
  1189| 
  1190| ### Community 199 - "Test Validation Features"
  1191| Cohesion: 0.38
  1192| Nodes (4): parametrize, TestValidationBlocksUnsupportedFeatures, Fail loudly (never guess, never silently fall back) when a caller asks the…, _validate_name_target_constraints()
  1193| 
  1194| ### Community 200 - "Pattern Classification Tests"
  1195| Cohesion: 0.33
  1196| Nodes (8): Path, test_emit_stub_classifies_root_to_leaf_patterns_as_differentiating(), _write_csv(), emit_stub(), main(), _parse_args(), Namespace, Path
  1197| 
  1198| ### Community 201 - "Shape Constants Validation"
  1199| Cohesion: 0.20
  1200| Nodes (6): SHAPE_INT_TO_NAME must map DimensionStyleType enum values correctly., Test shape constant definitions and mappings., All expected shape constants must be defined., All expected family constants must be defined., SHAPE_TO_FAMILY must map all shapes to families., TestShapeConstants
  1201| 
```
