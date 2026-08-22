# Chunk of graphify-out/GRAPH_REPORT.md

- Source relative path: `graphify-out/GRAPH_REPORT.md`
- Chunk: 2 of 4
- Original line range: 402-801
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 50653c6b8a269da31349c8c44444133fa7ab40b96b82ad274a5df417fda7f704
- Starts inside symbol: no
- Ends inside symbol: no

```
   402| ### Community 2 - "Comparison Testing Framework"
   403| Cohesion: 0.06
   404| Nodes (93): ComparisonPair, test_load_comparison_registry_roundtrip(), test_make_comparison_run_id_differs_by_comparison_type_for_same_pair_and_timestamp(), test_output_row_sort_helpers_are_stable_by_content(), test_pair_domain_work_items_use_pair_domain_union(), test_project_target_governance_state_uses_target_used(), test_standards_carrier_target_avoids_passive_bloat_label(), annotate_bundle_overlap() (+85 more)
   405| 
   406| ### Community 3 - "Mapping and Segment Processing"
   407| Cohesion: 0.05
   408| Nodes (86): _load_bootstrap_module_from(), _load_dynamo_bootstrap(), Locate and load mapping/_dynamo_bootstrap.py directly from disk, without…, run(), _blocked(), build_mapping_name_candidates(), build_report_rows(), compute_join_hash_for_segments() (+78 more)
   409| 
   410| ### Community 4 - "Client Sector Governance"
   411| Cohesion: 0.04
   412| Nodes (87): Existing invocations that don't pass --client-sector must still pick up the…, is_healthcare=False alone can't distinguish 'known different sector' from 'we…, test_default_client_sector_path_exists_and_loads(), test_disc_label_humanizes_unknown_discipline(), test_disc_label_uses_override_for_known_discipline(), test_load_client_sectors_builds_map(), test_load_client_sectors_empty_when_absent(), test_unclassified_client_not_treated_as_confirmed_non_healthcare() (+79 more)
   413| 
   414| ### Community 5 - "Cross-Segment Governance Testing"
   415| Cohesion: 0.06
   416| Nodes (85): Path, Tests for governance semantics in tools/compare_cross_segment.py., _seg(), test_build_governance_state_rows_include_inherited_unused_and_local_active(), test_density_similarity_uses_domain_density_vectors_not_containment(), test_discover_client_cross_bc_and_bc_to_bc_do_not_reference_collection_label(), test_discover_client_cross_bc_multi_bc_enumeration(), test_discover_client_cross_bc_single_bc_produces_no_pairs() (+77 more)
   417| 
   418| ### Community 6 - "Segment Manifest Testing"
   419| Cohesion: 0.06
   420| Nodes (75): _disc_rows(), _meta_row(), Tests for tools/build_segment_manifest.py., Multi-client, multi-discipline Container corpus for discipline tests., test_ancestor_segment_ids_semicolon_joined_not_pipe(), test_ancestor_segment_ids_two_element_roundtrip(), test_blank_client_label_no_longer_participates_in_subset(), test_blank_discipline_does_not_generate_discipline_cut() (+67 more)
   421| 
   422| ### Community 7 - "Cross-Client Comparison Testing"
   423| Cohesion: 0.05
   424| Nodes (76): Tests for discover_cross_client() / _is_client_only_project_segment() in…, A client's discipline-scoped Project child is its own distinct population -- it…, Two clients each with the SAME discipline-scoped Project population pair on…, Matching unit_system alone is not sufficient -- differing discipline_label…, Regression for a Codex review finding on PR #370: when…, A sibling_projects pair with no matching cross_client entry (e.g. two…, Only sibling_projects is special-cased against cross_client -- other…, Regression for a fifth Codex review finding on PR #370: main() must apply… (+68 more)
   425| 
   426| ### Community 8 - "Governance Narrative Evidence"
   427| Cohesion: 0.06
   428| Nodes (73): _delta_row(), _gov_state_summary_row(), _minimal_fixture(), _pooled_row(), Path, Tests for the PR1 governance evidence-package layer wired into…, Regression test for a PR review finding: --package-schema-version was reflected…, Regression for a PR #373 review finding: compare_cross_segment.py's new… (+65 more)
   429| 
   430| ### Community 9 - "Graphic Extraction Utilities"
   431| Cohesion: 0.06
   432| Nodes (48): _append_color_item(), _append_pattern_items(), _append_value_item(), extract_cut_graphics(), extract_halftone(), extract_projection_graphics(), extract_transparency(), _is_category() (+40 more)
   433| 
   434| ### Community 10 - "Domain Prompt Labeling"
   435| Cohesion: 0.05
   436| Nodes (67): test_domain_prompt_loader_falls_back_to_base_for_multi_segment_domains(), test_domain_prompt_loader_supports_single_word_domains(), _are_near_duplicates(), _extract_kv_typed(), find_near_duplicate_merges(), _get_synopsis_formatter(), is_fragmented(), load_annotations() (+59 more)
   437| 
   438| ### Community 11 - "Wall Object Management"
   439| Cohesion: 0.09
   440| Nodes (42): object, _basic_wall(), _CS, _CSWrapError, _default_ctx(), _Doc, _FillPatternDef, _FillPatternElem (+34 more)
   441| 
   442| ### Community 12 - "Governance Narrative Testing"
   443| Cohesion: 0.05
   444| Nodes (63): _bc_pooled_dict(), Tests for the Group 1 (tc/cp/tp) bc-pooled fallback in…, Unlike Group 2 (only the target/b side is classified, since the reference/a…, _target_scope_label() already joins multi-dimension side labels with "_" (e.g.…, _target_scope_label() only records SHAPE (which dimensions are populated), not…, Same shape (both "bc") AND same value must still land in "bc::bc" -- the value-…, The value-match guard applies to every multi-dimension shape, not just bare…, A domain whose ONLY signal is scoped Group 1 evidence (no enterprise tc/cp/tp,… (+55 more)
   445| 
   446| ### Community 13 - "Cache Key Management"
   447| Cohesion: 0.06
   448| Nodes (32): CacheKey, build_purgeable_id_set(), collect_elements(), collect_id_ints(), _collect_id_ints_uncached(), CollectCtx, _get_element(), _is_invalid_element_id() (+24 more)
   449| 
   450| ### Community 14 - "Discipline Classification Testing"
   451| Cohesion: 0.07
   452| Nodes (60): _client_fixture(), _pooled_row(), Tests for discipline-vocabulary and client-sector classification in…, A client absent from sector_map (or sector_map entirely absent) must NOT get…, discover_sibling_segments() groups purely by (parent_segment_id,…, cross_client's contribution to xc is gated to both-healthcare pairs, the same…, governance_client_summary.csv's cross_client_similarity_mean must be populated…, Regression for a Codex review finding on PR #370: xc_by_client/… (+52 more)
   453| 
   454| ### Community 15 - "Attribute Stability Analysis"
   455| Cohesion: 0.08
   456| Nodes (52): AttrStabilityRow, compute_attr_stability(), compute_stress_rank(), StressRow, ChangeCounts, classify_pair(), _phase2_items_map(), Any (+44 more)
   457| 
   458| ### Community 16 - "Signature Profile Analysis"
   459| Cohesion: 0.06
   460| Nodes (55): test_compute_named_cluster_flags_largest_gap_and_equal_shares(), test_compute_named_cluster_flags_uses_raw_share_not_rounded_percentage(), load_records_sig_profiles(), Build sig_hash presence profiles per export_run_id from v2.1 Phase0 CSVs.      R, build_distance_matrix_from_similarity(), Cluster, cluster_assignments_to_labels(), compute_silhouette_score() (+47 more)
   461| 
   462| ### Community 17 - "Name Key Coverage"
   463| Cohesion: 0.09
   464| Nodes (33): coverage_class(), exclusion_reason(), Reason string for an excluded (or untraced) domain; "" for an eligible domain., Classify a domain for the name-identity projection. Returns one of…, Path, _read_csv(), TestBothModeNonCollision, TestConfigPathRegression (+25 more)
   465| 
   466| ### Community 18 - "Timing Data Collection"
   467| Cohesion: 0.06
   468| Nodes (22): Any, Set the currently executing domain for sub-timing scoping., Return structured timing report.          Returns a dict with:           - ``tot, Build the structured report (must hold lock)., Collects hierarchical timing data for extraction runs.      Labels follow the co, Begin timing a labeled operation., End timing and record duration for a labeled operation., Record a pre-computed elapsed duration directly.          Used for hot-loop accu (+14 more)
   469| 
   470| ### Community 19 - "Governance Findings Testing"
   471| Cohesion: 0.08
   472| Nodes (52): _client_row(), _min_domain_dict(), Tests for structured governance findings (PR2): build_structured_findings(), bui, Regression test for a PR review finding: when primary >= 0.90 and     local_acti, Regression test for a PR review finding: a domain can land in     Baseline Candi, Regression test for a PR review finding: TIER_INVESTIGATE (primary     containme, Consolidation regression lock: after five separate PR review findings     each f, DoD requirement: no baseline finding is emitted when required     supporting met (+44 more)
   473| 
   474| ### Community 20 - "Discovery Parameter Testing"
   475| Cohesion: 0.08
   476| Nodes (51): Path, test_cli_emit_commands_prints_ready_to_run_invocations(), test_cli_emit_commands_uses_resolved_phase0_dir_not_unresolved_argument(), test_cli_writes_suggestions_csv_and_reads_required_counts_from_policy(), test_compute_domain_stats_counts_n_g_f_and_candidates(), test_compute_domain_stats_file_hhi_fully_concentrated_in_one_file(), test_compute_domain_stats_file_hhi_perfectly_even_distribution(), test_compute_domain_stats_file_hhi_treats_blank_file_id_as_unknown_bucket() (+43 more)
   477| 
   478| ### Community 21 - "Domain Edge Discovery"
   479| Cohesion: 0.11
   480| Nodes (51): atomic_write_csv(), bool_s(), build_domain_gap_rows(), build_edge_rows(), build_inventory_rows(), build_unresolved_file_rows(), _candidate_category_details(), canonical_param_kind() (+43 more)
   481| 
   482| ### Community 22 - "Cluster Similarity Analysis"
   483| Cohesion: 0.07
   484| Nodes (49): build_distance_matrix_from_similarity(), Cluster, cluster_assignments_to_labels(), compute_silhouette_score(), extract_dates_from_paths(), extract_metadata_patterns(), compute_avg_between_cluster_similarity(), compute_avg_internal_similarity() (+41 more)
   485| 
   486| ### Community 23 - "Governance Narrative Briefing"
   487| Cohesion: 0.08
   488| Nodes (46): _finding(), _minimal_fixture(), _pooled_row(), Path, Tests for PR4 (interpretation & routing split): render_governance_brief(), the -, The brief must reflect exactly the findings list it was given -- no     hidden r, Interpretation guide / question routes are static repo docs, not     per-run out, The interpretation guide / question routes docs ship with the repo --     their (+38 more)
   489| 
   490| ### Community 24 - "Lineage Testing Framework"
   491| Cohesion: 0.08
   492| Nodes (44): _lattice_manifest(), _pop(), _pop_hash(), Tests for the structural_ancestor / population_containment lineage model…, Reproduces the exact shape of a real, corpus-verified…, Synthetic 3-non-root-field lattice (governance x client x business_ center,…, _real_corpus_shaped_manifest(), _sibling_row() (+36 more)
   493| 
   494| ### Community 25 - "String Identity Management"
   495| Cohesion: 0.05
   496| Nodes (102): collect_instances(), Convert any value to a string representation safely.      Handles both str and u, safe_str(), phase2_join_hash(), phase2_qv_from_legacy_sentinel_str(), phase2_sorted_items(), Return IdentityItem-like dicts sorted by key 'k'., Map legacy sentinel strings to record.v2-safe (v,q) without emitting sentinel li (+94 more)
   497| 
   498| ### Community 26 - "Governance Thresholds Computation"
   499| Cohesion: 0.11
   500| Nodes (41): Path, test_compute_alignment_rates_and_contract_header_preserves_is_named_cluster(), test_compute_alignment_rates_falls_back_to_percentage_when_size_absent(), test_compute_alignment_rates_falls_back_to_percentage_when_size_values_are_invalid(), test_compute_alignment_rates_uses_raw_share_or_size_for_unrounded_result(), test_thresholds_breaks_and_ordering(), test_thresholds_reject_non_three_classes(), _write_csv() (+33 more)
   501| 
   502| ### Community 27 - "Domain Payload Management"
   503| Cohesion: 0.10
   504| Nodes (40): get_domain_payload(), get_domain_records(), load_exports(), Return the domain payload (legacy surface) if present., Extract record.v2 records list from the domain payload.      Notes:     - Contra, Load all monolithic exports in a directory (each file = one authority sample)., _extract_features(), _get() (+32 more)
   505| 
   506| ### Community 28 - "Export Run ID Testing"
   507| Cohesion: 0.20
   508| Nodes (6): PR #389 review: tools/apply_name_key_policy.py records `export_file` as the…, PR #390 review, fourth round: a details-only export (no sibling *.index.json)…, TestNormalizeExportRunIdWithKnownIds, TestSplitExportFileIdNormalization, normalize_export_run_id(), Normalize PR2's `export_file` (`tools/apply_name_key_policy.py`, which prefers…
   509| 
   510| ### Community 29 - "Comparison Registry Testing"
   511| Cohesion: 0.11
   512| Nodes (42): _gov_state_summary_row(), _minimal_fixture(), _pooled_row(), Path, Tests for D-032's comparison-registry input-completeness note:…, PR review finding: a registry stamp with no matching summary row (the current…, PR review finding: compare_cross_segment.py legitimately stamps…, PR review finding: an independently supplied registry and state CSV can come… (+34 more)
   513| 
   514| ### Community 30 - "Semantic Group Building"
   515| Cohesion: 0.11
   516| Nodes (41): build_grouping_prompt(), build_semantic_groups(), _call_grouping_llm(), _derive_element_label(), _extract_behavioral_props(), _infer_fill_geometry_description(), _is_fill_angle_close(), _is_nullish() (+33 more)
   517| 
   518| ### Community 31 - "Join Key Testing"
   519| Cohesion: 0.05
   520| Nodes (26): _compute_override_properties_hash(), Tests for _phase2_partition_items function., Semantic items must include baseline refs and override_properties_hash., Cosmetic items must include individual delta properties., Tests for view_templates join_key structure., join_hash must be a 32-char hex string (MD5)., For v1 policy, join_hash must equal def_hash., Tests that join_key policies are properly defined. (+18 more)
   521| 
   522| ### Community 32 - "Element ID Canonicalization"
   523| Cohesion: 0.08
   524| Nodes (30): canon_bool(), canon_id(), canon_num(), fnum(), is_sentinel(), Any, Canonicalize Revit ElementId-like values to a decimal string.      Accepts:, Legacy alias for canon_num. (+22 more)
   525| 
   526| ### Community 33 - "Key Decisions Reference"
   527| Cohesion: 0.05
   528| Nodes (40): Key Decisions Reference, Behavior-First Fingerprinting, Deterministic, Auditable Hashes, record_rows as Canonical Explainability, UniqueId Usage Is Restricted, Fail-Soft Is Mandatory, Ordering Rules Are Explicit Per Domain, Global vs Contextual Domain Split (+32 more)
   529| 
   530| ### Community 34 - "Attribute Concentration Metrics"
   531| Cohesion: 0.12
   532| Nodes (38): compute_attribute_concentration_metrics(), compute_effective_clusters(), compute_hhi_from_shares(), emit_analysis(), emit_records(), _extract_acc_project_label(), _file_id(), _fmt_metric() (+30 more)
   533| 
   534| ### Community 35 - "Governance Fixture Management"
   535| Cohesion: 0.13
   536| Nodes (34): Shared neutral identities for governance tests; never imported by production., _as_dict(), _DocForPI, _extract_items(), FakeProjectInformation, _project_info_with_configured_business_center(), _project_info_without_configured_business_center(), parametrize (+26 more)
   537| 
   538| ### Community 36 - "Material Properties Testing"
   539| Cohesion: 0.16
   540| Nodes (27): _Color, _Doc, _FillPatternElem, _Id, _make_ctx_with_fill_patterns(), _Mat, _material_payload(), _Param (+19 more)
   541| 
   542| ### Community 37 - "View Template Comparison"
   543| Cohesion: 0.09
   544| Nodes (38): _best_match_index(), _build_html(), _diff_dicts(), _diff_vco(), _esc(), _extract_records(), _get_label_component(), _get_label_display() (+30 more)
   545| 
   546| ### Community 38 - "Projection Status Classification"
   547| Cohesion: 0.12
   548| Nodes (30): compute_projection_status(), Classify a join-key computation using join_key_status's closed vocabulary., _append_assigned_view_count_cosmetic_item(), _append_filter_stack_signature(), _append_phase_filter_value(), _append_workset_visibility(), _build_ceiling_plan_viewtype_set(), _build_elevation_section_detail_viewtype_set() (+22 more)
   549| 
   550| ### Community 39 - "Archetype Review Preparation"
   551| Cohesion: 0.11
   552| Nodes (36): _all_cluster_ids(), _all_clusters(), _build_cluster_context(), _build_curated_gq_map(), ClusterContext, _find_cluster(), _governance_question_from_archetype_id(), _governance_question_from_cluster_id() (+28 more)
   553| 
   554| ### Community 40 - "Segment Orchestration"
   555| Cohesion: 0.10
   556| Nodes (36): Lock, _active_domains_from_presence_csv(), _build_patterns_missing_notes(), build_run_plan(), load_manifest(), load_membership(), load_registry(), main() (+28 more)
   557| 
   558| ### Community 41 - "Membership Matrix Building"
   559| Cohesion: 0.11
   560| Nodes (30): atomic_write_csv(), derive_scope_key(), Path, resolve_analysis_run_id(), build_membership_matrix(), _load_population_file_ids(), main(), _parse_args() (+22 more)
   561| 
   562| ### Community 42 - "View Category Probing"
   563| Cohesion: 0.12
   564| Nodes (32): _baseline_for_cat(), _bool_int(), _bucket_for_view(), _category_path(), _contract_eid(), _contract_int(), _contract_missing(), _contract_string() (+24 more)
   565| 
   566| ### Community 43 - "Bundle Analysis Testing"
   567| Cohesion: 0.07
   568| Nodes (28): _build_pr2_name_patterns_dir(), _materials_name_key_rows(), Path, PR #389 review: the flat "both" default (inherited from config target) made…, PR #390 review, fourth round: run_bundle_analysis_for_target() must supply…, PR3 BI-output-compatibility follow-up: the name leg's BI-facing output must…, Regression scenario: a failure during staging/mining/ provenance generation…, 3 files, domain=materials (Native), 2 co-occurring records (Concrete/Steel)… (+20 more)
   569| 
   570| ### Community 44 - "Governance Evidence Package Testing"
   571| Cohesion: 0.10
   572| Nodes (34): _evidence_map(), Unit tests for tools/governance_evidence_package.py: build_package_manifest,…, A caller that hasn't adopted out_dir (omitted, the default) must get identical…, Absent sibling files must not carry columns/row_count -- scanning a path that…, D-023: the artifact facts (header/dtype/row count) are directly observed, not…, The invariant D-030 exists to guarantee: build_evidence_map()'s top-level…, Same real Path.exists() treatment as the two existing static docs…, Same real Path.exists() treatment as the other static docs -- see D-029. (+26 more)
   573| 
   574| ### Community 45 - "Text Type Probing"
   575| Cohesion: 0.09
   576| Nodes (25): _find_leader_arrow_param(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _looks_like_text_type(), _probe_document_identity(), _probe_revit_version() (+17 more)
   577| 
   578| ### Community 46 - "Unit Type Probing"
   579| Cohesion: 0.15
   580| Nodes (33): _discover_specs(), _forge_id_string(), _is_forge_type_id(), _label_for_discipline_id(), _label_for_spec_id(), _maybe_set_example(), _probe_document_identity(), _probe_revit_version() (+25 more)
   581| 
   582| ### Community 47 - "Group 1 Testing"
   583| Cohesion: 0.12
   584| Nodes (33): _group1_rows(), A single bc pair has no spread to compare (len(v) > 1 gate on…, test_detect_anomalies_flags_material_bc_divergence(), test_detect_anomalies_silent_when_bc_pairs_agree(), test_detect_anomalies_silent_when_only_one_bc_pair(), test_render_group1_scope_section_includes_bc_bc_row(), _generic_to_template_rows(), Tests for gt/gc/gp per-target-scope-level breakdown (Option C) in… (+25 more)
   585| 
   586| ### Community 48 - "View Filter Application Probing"
   587| Cohesion: 0.11
   588| Nodes (32): _as_bool_int_contract(), _as_elementid_contract(), _as_int_contract(), _as_string_contract(), _collect_applied_filters_in_order(), _color_rgb_hex(), _contract(), _eid_int() (+24 more)
   589| 
   590| ### Community 49 - "Type Collection Utilities"
   591| Cohesion: 0.11
   592| Nodes (39): collect_types(), extract_ceiling_types(), _attach_placeholder_metadata(), _build_instance_count_map(), _build_name_key(), _coarse_fill_reads(), _enum_name(), _label_for_type() (+31 more)
   593| 
   594| ### Community 50 - "Governance Population Comparison"
   595| Cohesion: 0.13
   596| Nodes (30): A manifest shaped to exercise every comparison_type in one shot:…, _records_rows(), _row(), _synthetic_manifest(), test_comparison_type_never_mixes_symmetric_and_directed_metric_shape(), test_comparison_type_still_unambiguous_with_project_scoped_template(), test_directed_bc_to_project_matches_by_business_center_label_alone(), test_directed_bc_to_project_matches_regardless_of_differing_client() (+22 more)
   597| 
   598| ### Community 51 - "Layer Stack Testing"
   599| Cohesion: 0.17
   600| Nodes (31): _make_layer_row(), _make_wall_record(), Any, Path, Single type with simple layers emits one stack row and correct layer rows., Two types sharing the same stack_hash_loose collapse to one stack row with type_, wall_types and floor_types each emit separate rows distinguished by domain., layer_stacks is NOT written when --emit uses the default set. (+23 more)
   601| 
   602| ### Community 52 - "JSON Loading Utilities"
   603| Cohesion: 0.10
   604| Nodes (31): _load_json(), main(), _now_stamp(), _membership_ids(), parametrize, Path, Read segment_membership.csv and return the export_run_id set for one segment_id., _read_csv() (+23 more)
   605| 
   606| ### Community 53 - "Join Policy Verification Testing"
   607| Cohesion: 0.13
   608| Nodes (30): Path, test_diagnostics_domain_suffix_empty_when_unscoped(), test_diagnostics_domain_suffix_falls_back_to_hash_for_long_lists(), test_diagnostics_domain_suffix_includes_policy_modes_to_avoid_split_run_collisions(), test_diagnostics_domain_suffix_short_domain_list(), test_discover_join_policy_scoped_run_does_not_clobber_unscoped_filenames(), test_full_population_verify_stays_consistent_with_fixed_pareto_search(), test_full_population_verify_uses_same_effective_gates_as_greedy_search() (+22 more)
   609| 
   610| ### Community 54 - "Join Key Discovery Testing"
   611| Cohesion: 0.13
   612| Nodes (26): test_identity_index_keeps_q_only_rows_for_required_presence(), test_shape_gating_does_not_require_phase_filter_for_false(), test_shape_gating_matches_bool_case_variants(), Path, test_apply_diagnostics_include_discriminator_context(), test_discover_emits_legacy_compat_shape_and_lists(), test_flat_required_fields_backward_compatible(), test_optional_items_not_required_or_selected_by_default() (+18 more)
   613| 
   614| ### Community 55 - "Population Discovery Management"
   615| Cohesion: 0.12
   616| Nodes (27): compute_effective_support(), make_bundle_id(), _collapse_subset_related_roots(), discover_populations(), main(), _parse_args(), _pattern_summary(), _population_id() (+19 more)
   617| 
   618| ### Community 56 - "Domain Authority Analysis"
   619| Cohesion: 0.14
   620| Nodes (26): analyze(), _canonical_item_str(), classify_authority_outcome(), classify_convergence(), _domain_payload_from_fp(), extract_domains_summary(), _extract_record_sig_hashes_v2(), _jaccard_multiset() (+18 more)
   621| 
   622| ### Community 57 - "Join Key Application"
   623| Cohesion: 0.11
   624| Nodes (28): apply_join_keys_by_ids(), compute_join_hash(), extract_identity_map(), main(), md5_utf8_join_pipe(), Any, build_reference_standards_from_clusters(), main() (+20 more)
   625| 
   626| ### Community 58 - "Dimension Type Probing"
   627| Cohesion: 0.11
   628| Nodes (26): _example_score(), _find_tick_param(), _fmt_display(), _format_param_contract(), _format_synth_contract(), _get_dim_shape_info(), _get_family_name_param(), _is_angle_datatype() (+18 more)
   629| 
   630| ### Community 59 - "Line Pattern Probing"
   631| Cohesion: 0.11
   632| Nodes (25): _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _iter_line_style_categories(), _linepattern_signature(), _lp_seg_type_id_and_name(), _maybe_set_example() (+17 more)
   633| 
   634| ### Community 60 - "Export Bundle Quality Testing"
   635| Cohesion: 0.15
   636| Nodes (28): Path, test_legacy_kvq_schema_unaffected(), test_v21_schema_ignores_item_role_even_when_populated(), test_v21_schema_reads_quality_from_item_value_type_when_role_blank(), _write_csv(), _apply_bundle_threshold(), _atomic_write_csv(), _compute_domain_bundle_threshold() (+20 more)
   637| 
   638| ### Community 61 - "Segment Subtree Extraction"
   639| Cohesion: 0.17
   640| Nodes (25): RuntimeError, atomic_write_csv(), Blocked, expand_ancestors(), FileSpec, find_seeds_by_id(), find_seeds_by_search(), load_manifest() (+17 more)
   641| 
   642| ### Community 62 - "Promotion Candidate Analysis"
   643| Cohesion: 0.12
   644| Nodes (27): corpus_root(), _gov_row(), fixture, Synthetic-fixture tests for tools/analyze_promotion_candidates.py. No real…, _read(), _reuse_row(), test_all_view_fallback_dropped_when_used_data_exists_elsewhere(), test_baseline_equal_scope_excluded() (+19 more)
   645| 
   646| ### Community 63 - "Enterprise Policy Testing"
   647| Cohesion: 0.12
   648| Nodes (27): test_blank_override_rejected(), test_default_enterprise_label_is_synthetic(), test_malformed_schema_and_invalid_bookkeeping_token_are_rejected(), test_policy_file_and_cli_precedence(), test_policy_file_path_is_memory_only_provenance_is_safe(), test_policy_instances_are_immutable_serializable_and_do_not_leak_state(), test_policy_provenance_records_effective_configuration(), apply_export_cap() (+19 more)
   649| 
   650| ### Community 64 - "Archetype Candidate Generation"
   651| Cohesion: 0.13
   652| Nodes (28): _collapsed_from_for_edge(), _governance_question_hint(), _is_vfd_related(), main(), True for a dynamic VFD edge (source == view_filter_definitions) or the     stati, _signal_coverage_pct(), _utc_now_iso(), _all_cluster_pairs() (+20 more)
   653| 
   654| ### Community 65 - "Ceiling Type Probing"
   655| Cohesion: 0.11
   656| Nodes (25): _ensure_entry(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _multi_repr(), _observe() (+17 more)
   657| 
   658| ### Community 66 - "Fill Pattern Probing"
   659| Cohesion: 0.12
   660| Nodes (27): _add_computed_surface(), _bucket_key_for_fill_pattern(), _contract_from_value(), _ensure_entry(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype() (+19 more)
   661| 
   662| ### Community 67 - "Floor Type Probing"
   663| Cohesion: 0.11
   664| Nodes (25): _ensure_entry(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _multi_repr(), _observe() (+17 more)
   665| 
   666| ### Community 68 - "Roof Type Probing"
   667| Cohesion: 0.11
   668| Nodes (25): _ensure_entry(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _multi_repr(), _observe() (+17 more)
   669| 
   670| ### Community 69 - "Union Breadth Testing"
   671| Cohesion: 0.11
   672| Nodes (27): Tests for D-033's union-inventory-derived domain confidence enrichment:…, PR review finding: by_domain sums tier counts across every discipline/…, PR review finding: when one client row for a (domain, discipline, unit_system,…, PR review finding: build_pattern_reuse_distribution_rows() sends a row with…, A blank source_status/inventory_status (e.g. an older export missing the…, PR review finding: cross_segment_union_inventory.csv emits one row per…, Only aggregate integer counts per domain -- never join_hash/pattern_label…, PR review finding: with only one client in the grain, pct_clients_present is… (+19 more)
   673| 
   674| ### Community 70 - "Governance Relationship Testing"
   675| Cohesion: 0.17
   676| Nodes (27): _find(), _rel_row(), _row(), test_bc_prefix_variant_folds_to_same_bc_identity(), test_blank_project_label_falls_back_to_export_run_id_per_file(), test_client_bc_matrix_never_recomputes_percentage_it_only_sums_counts(), test_client_label_casing_variants_fold_to_one_project(), test_enterprise_bookkeeping_bc_token_blanked_not_carried_as_fake_bc() (+19 more)
   677| 
   678| ### Community 71 - "Signal Clustering"
   679| Cohesion: 0.14
   680| Nodes (28): _apply_threshold(), _bare_signal_name(), _build_clusters(), _build_coverage_summary(), _build_curated_gq_map(), _build_detail_files_lookup(), _build_n_files_classified_lookup(), _build_signal_cluster_map() (+20 more)
   681| 
   682| ### Community 72 - "Line Style Probing"
   683| Cohesion: 0.13
   684| Nodes (26): _bucket_key(), _contract_value(), _fmt_display_param(), _format_param_contract(), _get_lines_category_id(), _hex_rgb_from_triplet(), _index_param(), _is_angle_datatype() (+18 more)
   685| 
   686| ### Community 73 - "Object Style Probing"
   687| Cohesion: 0.11
   688| Nodes (16): _category_type_label(), _contract_eid(), _eid_name(), _get_name(), _infer_object_styles_tab(), _iter_categories(), _probe_document_identity(), _probe_revit_version() (+8 more)
   689| 
   690| ### Community 74 - "Governance Policy Testing"
   691| Cohesion: 0.11
   692| Nodes (21): apply_governance_policy() with the shipped default policy dir must leave every…, Regression test for a PR review finding: render_limitations()'s 'Excluded…, test_loading_default_policy_dir_reproduces_module_level_defaults(), test_overriding_domain_guidance_changes_detect_anomalies_text(), test_overriding_excluded_from_scoring_changes_build_cascade(), test_overriding_excluded_from_scoring_changes_render_limitations_note(), test_overriding_static_findings_guidance_changes_rendered_prose(), test_render_limitations_handles_empty_excluded_set() (+13 more)
   693| 
   694| ### Community 75 - "Browser Organization Probing"
   695| Cohesion: 0.12
   696| Nodes (23): _best_name(), _builtin_label(), _clean_name(), _probe_document_identity(), _probe_revit_version(), _probe_wrap(), Resolve an Element.WorksetId value to (name, resolved_bool) via…, Resolve a GetSimilarTypes() id via a document-wide doc.GetElement() lookup --… (+15 more)
   697| 
   698| ### Community 76 - "Phase Graphics Probing"
   699| Cohesion: 0.13
   700| Nodes (25): _fmt_display(), _format_param_contract(), _get_phasefilter_param_from_view(), _index_params_from_elem(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _probe_document_identity() (+17 more)
   701| 
   702| ### Community 77 - "Extraction Runner"
   703| Cohesion: 0.17
   704| Nodes (26): _append_line_pattern_synthetic_norm_hash(), _apply_sig_hash_to_phase0(), _detect_surfaces(), _discover_domains_from_exports(), _emit_join_policy_diagnostics(), _enforce_policy_gate(), _ensure_dir(), _ensure_domain_scoped_identity_items() (+18 more)
   705| 
   706| ### Community 78 - "Domain Contract Management"
   707| Cohesion: 0.18
   708| Nodes (24): add_bounded_error(), compute_run_status(), DiagError, _ensure_list(), new_domain_envelope(), new_run_diag(), new_run_envelope(), Any (+16 more)
   709| 
   710| ### Community 79 - "Name Key Analysis Testing"
   711| Cohesion: 0.18
   712| Nodes (9): _inline_equivalent(), name_key_policies(), Any, fixture, parametrize, Reports the concrete sample size / match rate this file's parametrized cases…, Mirrors the exact build_join_key_from_policy call every domains/*.py inline…, test_agreement_sample_size_and_match_rate() (+1 more)
   713| 
   714| ### Community 80 - "Dynamo Bootstrap Utilities"
   715| Cohesion: 0.13
   716| Nodes (25): add_revit_api_references(), bootstrap(), looks_like_repo_root(), promote_on_sys_path(), purge_repo_modules(), Unconditionally purge cached repo modules from sys.modules. A persistent Dynamo…, Ensure `repo_root` is first on sys.path, removing any existing entry first. A…, Reference RevitServices/RevitAPI so pythonnet exposes them. A fresh Dynamo… (+17 more)
   717| 
   718| ### Community 81 - "Business Center Normalization"
   719| Cohesion: 0.13
   720| Nodes (24): test_0000_flows_through_as_literal_enterprise_value(), test_bc_0000_spelling_variants_canonicalize_to_0000(), test_na_spelled_business_center_labels_normalize_to_blank(), _bc_of(), _build_pooled_row(), _build_summary_row(), _client_of(), discover_client_cross_bc() (+16 more)
   721| 
   722| ### Community 82 - "Static Docs Copy Testing"
   723| Cohesion: 0.13
   724| Nodes (24): _minimal_fixture(), _pooled_row(), Path, Tests for D-034's static-doc copy-into-out behavior: main() copies the four…, PR review finding: render_evidence_authority_header() (the second renderer with…, PR review finding: health had no signal for a missing…, A prior run (default flags) copies the four docs in; a later run over the same…, P1 PR review finding: if --out resolves to the same directory the four static… (+16 more)
   725| 
   726| ### Community 83 - "LFT Similarity Inspection"
   727| Cohesion: 0.13
   728| Nodes (26): build_class_profiles(), build_dim_summaries(), build_exact_match_table(), build_family_file_detail(), build_name_cluster_table(), build_subgroups(), extract_category(), extract_family_name() (+18 more)
   729| 
   730| ### Community 84 - "Arrowhead Probing"
   731| Cohesion: 0.13
   732| Nodes (22): _arrow_style_key(), _collect_dimension_types_with_tick_param(), _fmt_display(), _format_param_contract(), _is_angle_datatype(), _is_length_datatype(), _probe_document_identity(), _probe_revit_version() (+14 more)
   733| 
   734| ### Community 85 - "Wall Type Probing"
   735| Cohesion: 0.13
   736| Nodes (23): _fmt_display(), _format_param_contract(), _is_length_datatype(), _maybe_set_example(), _observe_synth(), _probe_document_identity(), _probe_revit_version(), _probe_wrap() (+15 more)
   737| 
   738| ### Community 86 - "Extraction Context Building"
   739| Cohesion: 0.10
   740| Nodes (19): build_extraction_context(), operator_deployment_config_path(), Importable runner context construction; deliberately free of Revit/Dynamo…, Read the deployment path at the single operator/environment boundary., Build and validate context portions required before any domain executes., _build_workset_name_to_unique_id_ctx(), _enabled(), Build a workset name -> unique_id crosswalk for browser_organization.… (+11 more)
   741| 
   742| ### Community 87 - "Business Center Validation"
   743| Cohesion: 0.11
   744| Nodes (25): test_business_center_0000_is_a_valid_value_not_a_validation_failure(), test_business_center_label_already_four_digits_unaffected(), test_business_center_label_non_numeric_unaffected(), test_business_center_label_zero_padded_short_digit_values(), test_sanitize_folder_preserves_selected_blank_vs_unselected_dimension(), test_sanitize_folder_renders_selected_blank_as_neutral_token(), test_sanitize_folder_strips_path_separators(), test_validate_required_metadata_empty_for_fully_valid_rows() (+17 more)
   745| 
   746| ### Community 88 - "Placeholder Exclusion Management"
   747| Cohesion: 0.14
   748| Nodes (23): read_csv_rows(), _choose_threshold(), compute_placeholder_exclusions(), compute_placeholder_exclusions(), _is_truthy(), _largest_gap_threshold(), main(), _parse_args() (+15 more)
   749| 
   750| ### Community 89 - "Governance Report Generation"
   751| Cohesion: 0.14
   752| Nodes (17): _build_html_report(), build_pattern_table(), build_table(), _get_identity_value(), main(), _pattern_row_template(), ProjectExport, Any (+9 more)
   753| 
   754| ### Community 90 - "Probe Inventory Building"
   755| Cohesion: 0.14
   756| Nodes (25): build(), _crosswalk_value_sig(), discover_probe_files(), _example_score(), _fmt_example(), _fmt_q_counts(), _fmt_rate(), load_payload() (+17 more)
   757| 
   758| ### Community 91 - "View Filter Definition Probing"
   759| Cohesion: 0.14
   760| Nodes (21): _element_filter_kind(), _flatten_element_filter(), _get_rules_from_element_parameter_filter(), _get_subfilters(), _maybe_set_example(), _observe(), _probe_document_identity(), _probe_revit_version() (+13 more)
   761| 
   762| ### Community 92 - "Deployment Configuration Management"
   763| Cohesion: 0.18
   764| Nodes (23): _identity_allowed_keys(), load_deployment_config(), Any, Path, Dependency-neutral validation for deployment-owned extraction configuration., Validate and canonically normalize deployment mapping entries., Load the closed v1 schema and validate mappings against the identity contract., validate_project_info_shared_parameters() (+15 more)
   765| 
   766| ### Community 93 - "Hash Policy Discovery"
   767| Cohesion: 0.21
   768| Nodes (22): _domain_rows(), _load_items(), main(), Path, Accept either: - direct phase0 folder (contains records.csv), or - Results_v21…, _resolve_phase0_dir(), _run_target(), _dedupe() (+14 more)
   769| 
   770| ### Community 94 - "Domain Identity Contract"
   771| Cohesion: 0.17
   772| Nodes (17): DomainIdentityContract, beam_search_candidates(), build_candidate_pool(), Candidate, choose_from_front(), compute_join_hash_for_record(), dominates(), evaluate_keyset() (+9 more)
   773| 
   774| ### Community 95 - "Phase Filter Probing"
   775| Cohesion: 0.14
   776| Nodes (22): _add_inventory_obs(), _fmt_display(), _format_param_contract(), _get_view_phase_filter_param(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _phase_status_bucket() (+14 more)
   777| 
   778| ### Community 96 - "View Probing"
   779| Cohesion: 0.14
   780| Nodes (21): _add_inventory_obs(), _fmt_display(), _format_param_contract(), _int_enum(), _is_angle_datatype(), _is_length_datatype(), _maybe_set_example(), _probe_document_identity() (+13 more)
   781| 
   782| ### Community 97 - "Graphify Skill Documentation"
   783| Cohesion: 0.08
   784| Nodes (23): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+15 more)
   785| 
   786| ### Community 98 - "Graphify Skill Documentation"
   787| Cohesion: 0.08
   788| Nodes (23): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Part A - Structural extraction for code files (+15 more)
   789| 
   790| ### Community 99 - "Signature Hash Management"
   791| Cohesion: 0.18
   792| Nodes (21): apply_sig_hash_policy_to_record(), build_sig_hash_from_policy(), _items_to_map(), _key_allowed(), Any, Mutate and return a canonical record dict with policy-generated sig_hash/status., Return (sig_hash, status, status_reasons, hash_items). The builder hashes every…, get_domain_sig_hash_policy() (+13 more)
   793| 
   794| ### Community 100 - "Cardinality Comparison Testing"
   795| Cohesion: 0.29
   796| Nodes (23): _clear_caches(), _manifest_entry(), Path, Tests for explicit cardinality/status and aggregation-method semantics in…, A client population dominated by one business center (imbalanced file counts…, files: {export_run_id: [join_hash, ...]} -- writes domain_patterns.csv and an…, _registry_entry(), test_blocked_row_preserves_populated_side_bundle_availability() (+15 more)
   797| 
   798| ### Community 101 - "Latent Purgeable Computation"
   799| Cohesion: 0.16
   800| Nodes (23): _accumulate_item_rows(), _build_matcher(), _classify(), _domains_of_interest(), _fmt_consumers(), _is_purgeable_false(), _is_purgeable_true(), _is_vfa_filter_ref_key() (+15 more)
   801| 
```
