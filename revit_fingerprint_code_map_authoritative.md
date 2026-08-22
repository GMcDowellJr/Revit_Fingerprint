# Revit_Fingerprint — authoritative Python code map

## Scope

Deterministic AST inventory of Python imports and definitions.

## Files

### `core/__init__.py`

- No imports or definitions.

### `core/canon.py`

**Imports**
- `.hashing:safe_str`
- `__future__:annotations`
- `typing:Any,Optional`

**Definitions**
- `is_sentinel` (function, L33)
- `canon_str` (function, L41)
- `canon_bool` (function, L73)
- `canon_num` (function, L93)
- `canon_id` (function, L108)
- `fnum` (function, L146)
- `rgb_sig_from_color` (function, L151)
- `rgb_dict_from_color` (function, L161)

### `core/canonical_items.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,Iterable,List,Mapping,Optional,Sequence,Tuple`

**Definitions**
- `_normalize_item` (function, L26)
- `build_flat_items` (function, L35)
- `merge_legacy_buckets` (function, L58)
- `compile_role_policy` (function, L72)
- `resolve_item_roles` (function, L107)
- `canonicalize_record` (function, L130)

### `core/collect.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,BuiltInParameter`
- `System.Collections.Generic`
- `__future__:annotations`
- `dataclasses:dataclass,field`
- `typing:Any,Callable,Dict,Iterable,List,Optional,Tuple,Union`

**Definitions**
- `CollectCtx` (class, L34)
- `CollectCtx.inc` (method, L46)
- `_is_invalid_element_id` (function, L54)
- `_safe_unique_id` (function, L86)
- `_make_query_key` (function, L97)
- `_require_revit_api` (function, L126)
- `_collect_id_ints_uncached` (function, L131)
- `collect_id_ints` (function, L238)
- `_get_element` (function, L312)
- `collect_elements` (function, L324)
- `collect_types` (function, L362)
- `collect_instances` (function, L386)
- `build_purgeable_id_set` (function, L412)
- `purge_lookup` (function, L465)
- `build_subcategory_used_id_set` (function, L478)
- `is_type_purgeable` (function, L552)

### `core/context.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInParameter`
- `__future__:annotations`
- `core.canon:canon_id,canon_str,S_MISSING,S_NOT_APPLICABLE,S_UNREADABLE`
- `dataclasses:dataclass`
- `typing:Any,Dict,Optional,Tuple`

**Definitions**
- `ViewInfo` (class, L28)
- `DocViewContext` (class, L50)
- `DocViewContext.__init__` (method, L59)
- `DocViewContext.view_info` (method, L63)

### `core/contracts.py`

**Imports**
- `__future__:annotations`
- `dataclasses:dataclass`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `DiagError` (class, L55)
- `_ensure_list` (function, L62)
- `new_run_diag` (function, L66)
- `add_bounded_error` (function, L85)
- `new_domain_envelope` (function, L134)
- `new_run_envelope` (function, L170)
- `compute_run_status` (function, L205)

### `core/deployment_config.py`

**Imports**
- `__future__:annotations`
- `json`
- `pathlib:Path`
- `typing:Any,Dict,Iterable,Optional,Union`
- `uuid`

**Definitions**
- `validate_project_info_shared_parameters` (function, L22)
- `_identity_allowed_keys` (function, L64)
- `load_deployment_config` (function, L75)

### `core/deps.py`

**Imports**
- `__future__:annotations`
- `core.contracts:DOMAIN_STATUS_BLOCKED,DOMAIN_STATUS_DEGRADED,DOMAIN_STATUS_FAILED,DOMAIN_STATUS_OK,DOMAIN_STATUS_UNSUPPORTED,VALID_DOMAIN_STATUSES`
- `dataclasses:dataclass`
- `typing:Any,Dict,Iterable,Optional,Set`

**Definitions**
- `Blocked` (class, L29)
- `Blocked.__str__` (method, L42)
- `require_domain` (function, L55)

### `core/dimension_type_helpers.py`

**Imports**
- `core.canon:canon_str,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.hashing:make_hash,safe_str`
- `core.record_v2:canonicalize_str,canonicalize_str_allow_empty,canonicalize_int,canonicalize_float,canonicalize_bool,canonicalize_enum,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,make_identity_item`
- `core.rows:first_param,_as_string,_as_value_string,_as_double,_as_int,format_len_inches,try_get_color_rgb_from_elem,get_element_display_name,_canon_rgb`
- `os`
- `sys`

**Definitions**
- `_get_dimension_shape` (function, L130)
- `_fmt_in_from_ft` (function, L254)
- `_fmt_float` (function, L265)
- `_format_options_to_kv` (function, L279)
- `get_type_display_name` (function, L327)
- `_build_text_appearance_items` (function, L402)
- `_read_tick_mark_sig_hash` (function, L528)
- `_read_unit_format_info` (function, L577)
- `_read_unit_format_info._units_fo_not_applicable` (method, L605)
- `_read_prefix_suffix` (function, L685)
- `_read_leader_arrowhead` (function, L720)
- `_read_arrowhead_ref_sig_hash` (function, L799)
- `_read_element_ref_name` (function, L859)
- `_read_line_pattern_ref_sig_hash` (function, L931)
- `_build_alternate_units_items` (function, L1040)

### `core/features.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,Optional,Tuple`

**Definitions**
- `_as_dict` (function, L19)
- `_as_int` (function, L23)
- `_extract_counts_from_legacy` (function, L41)
- `build_features` (function, L55)

### `core/graphic_overrides.py`

**Imports**
- `Autodesk.Revit.DB:GraphicsStyleType,Category,OverrideGraphicSettings`
- `__future__:annotations`
- `core.canon:canon_str`
- `core.record_v2:make_identity_item,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED,canonicalize_int,canonicalize_str,canonicalize_bool`
- `importlib.util`
- `importlib.util`
- `sys`
- `typing:Any,Dict,List,Optional,Sequence,Tuple`

**Definitions**
- `_is_category` (function, L62)
- `_is_ogs` (function, L67)
- `_is_invalid_element_id` (function, L72)
- `_rgb_from_color` (function, L89)
- `_read_attr` (function, L100)
- `_read_first_attr` (function, L110)
- `_read_category_line_weight` (function, L122)
- `_read_category_line_pattern_id` (function, L133)
- `_read_category_line_color` (function, L143)
- `_read_category_fill_pattern_id` (function, L155)
- `_read_category_fill_color` (function, L170)
- `_resolve_pattern_sig_hash` (function, L179)
- `_append_pattern_items` (function, L204)
- `_append_color_item` (function, L237)
- `_append_value_item` (function, L243)
- `extract_projection_graphics` (function, L260)
- `extract_cut_graphics` (function, L393)
- `extract_halftone` (function, L524)
- `extract_transparency` (function, L545)

### `core/hashing.py`

**Imports**
- `System.Security.Cryptography:MD5`
- `System.Text:Encoding`
- `hashlib`

**Definitions**
- `safe_str` (function, L14)
- `make_hash` (function, L29)
- `_make_hash_impl` (function, L62)

### `core/join_key_builder.py`

**Imports**
- `core.phase2:phase2_join_hash`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_OK`
- `re`

**Definitions**
- `_dedupe_preserve_order` (function, L56)
- `_items_to_kqv_map` (function, L67)
- `_infer_indexed_count` (function, L81)
- `_expand_sequence_key` (function, L100)
- `_get_shape_specific_requirements` (function, L147)
- `build_join_key_from_policy` (function, L194)
- `build_join_key_from_policy.emit_key` (method, L238)
- `compute_projection_status` (function, L316)

### `core/join_key_policy.py`

**Imports**
- `json`

**Definitions**
- `_is_list_of_str` (function, L34)
- `_validate_shape_gating` (function, L38)
- `validate_domain_join_key_policy` (function, L108)
- `validate_domain_join_key_policy.add_issue` (method, L121)
- `load_join_key_policies` (function, L233)
- `get_domain_join_key_policy` (function, L288)

### `core/manifest.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,Optional`

**Definitions**
- `_safe_dict` (function, L19)
- `build_manifest` (function, L23)

### `core/name_key_builder.py`

**Imports**
- `__future__:annotations`
- `core.canonical_items:build_flat_items`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.record_v2:canonicalize_str`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `flat_items_for_record` (function, L60)
- `_has_detail_data` (function, L84)
- `build_name_key_for_record` (function, L101)

### `core/name_key_coverage.py`

**Imports**
- `__future__:annotations`
- `typing:Dict,FrozenSet`

**Definitions**
- `coverage_class` (function, L91)
- `exclusion_reason` (function, L108)
- `is_eligible` (function, L115)

### `core/naming.py`

**Imports**
- `__future__:annotations`
- `os`
- `re`
- `typing:Any,Dict,Optional`

**Definitions**
- `safe_slug` (function, L24)
- `_file_stem_from_doc` (function, L45)
- `_project_information` (function, L65)
- `_short_uid` (function, L88)
- `derive_doc_key` (function, L100)
- `build_output_filename` (function, L120)

### `core/phase2.py`

**Imports**
- `core.canon:S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.hashing:make_hash,safe_str`
- `core.record_v2:canonicalize_bool,canonicalize_int,ITEM_Q_UNREADABLE,ITEM_Q_OK,ITEM_Q_MISSING`
- `core.record_v2:canonicalize_str,canonicalize_str_allow_empty,serialize_identity_items,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE`

**Definitions**
- `phase2_sorted_items` (function, L31)
- `phase2_qv_from_legacy_sentinel_str` (function, L36)
- `phase2_join_hash` (function, L65)

### `core/record_v2.py`

**Imports**
- `__future__:annotations`
- `core.hashing:safe_str`
- `enum`
- `hashlib`
- `json`
- `math`
- `typing:Any,Dict,Iterable,List,Optional,Sequence,Tuple`

**Definitions**
- `canonicalize_str` (function, L83)
- `canonicalize_str_allow_empty` (function, L109)
- `canonicalize_int` (function, L135)
- `canonicalize_float` (function, L178)
- `canonicalize_bool` (function, L205)
- `canonicalize_enum` (function, L237)
- `make_record_id_from_element` (function, L268)
- `_canonical_structural_value` (function, L302)
- `canonical_structural_fields` (function, L327)
- `make_record_id_structural` (function, L335)
- `_default_record_id_secondary_key` (function, L342)
- `finalize_record_ids_for_domain` (function, L357)
- `_block_record_for_unstable_id` (function, L403)
- `make_identity_item` (function, L415)
- `serialize_identity_items` (function, L455)
- `serialize_identity_items._k` (method, L464)
- `compute_identity_quality` (function, L480)
- `build_record_v2` (function, L530)
- `block_record_v2` (function, L606)

### `core/rows.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInParameter,UnitUtils,UnitTypeId`
- `core.canon:canon_str,safe_str`

**Definitions**
- `_param` (function, L21)
- `_as_string` (function, L38)
- `_as_value_string` (function, L58)
- `_as_double` (function, L82)
- `_as_int` (function, L100)
- `_as_bool_from_param` (function, L118)
- `first_param` (function, L134)
- `format_len_inches` (function, L174)
- `_canon_rgb` (function, L196)
- `try_get_color_rgb_from_elem` (function, L230)
- `get_element_display_name` (function, L259)
- `get_type_display_name` (function, L301)

### `core/sig_hash_builder.py`

**Imports**
- `__future__:annotations`
- `core.hashing:make_hash`
- `core.record_v2:ITEM_Q_OK,STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK,serialize_identity_items`
- `typing:Any,Dict,Iterable,List,Optional,Sequence,Tuple`

**Definitions**
- `_items_to_map` (function, L18)
- `_key_allowed` (function, L29)
- `build_sig_hash_from_policy` (function, L38)
- `apply_sig_hash_policy_to_record` (function, L105)

### `core/sig_hash_policy.py`

**Imports**
- `__future__:annotations`
- `json`
- `typing:Any,Dict,Optional`

**Definitions**
- `_is_list_of_str` (function, L15)
- `validate_domain_sig_hash_policy` (function, L19)
- `load_sig_hash_policies` (function, L38)
- `get_domain_sig_hash_policy` (function, L53)

### `core/timing_collector.py`

**Imports**
- `__future__:annotations`
- `threading`
- `time`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `TimingCollector` (class, L21)
- `TimingCollector.__init__` (method, L35)
- `TimingCollector.start_timer` (method, L46)
- `TimingCollector.end_timer` (method, L54)
- `TimingCollector.record_elapsed` (method, L67)
- `TimingCollector._record_elapsed_locked` (method, L81)
- `TimingCollector.set_active_domain` (method, L106)
- `TimingCollector.get_report` (method, L114)
- `TimingCollector._build_report` (method, L130)

### `core/vg_sig.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInParameter`
- `core.hashing:make_hash,safe_str`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:ITEM_Q_OK,ITEM_Q_UNREADABLE,canonicalize_int,canonicalize_str,make_identity_item,serialize_identity_items`
- `os`
- `sys`

**Definitions**
- `_read_bip_int` (function, L168)
- `emit_builtin_params` (function, L211)
- `emit_shared_params_stub` (function, L274)
- `_phase2_items_from_def_signature` (function, L285)
- `_canonical_identity_items_from_signature` (function, L335)
- `_semantic_keys_from_identity_items` (function, L358)
- `_traceability_unknown_items` (function, L407)
- `_compute_delta_items` (function, L432)

### `dev_tools/repo_context/rc_chunking.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `rc_common:ChunkRecord,stable_path_id,sanitize_stem,sha256_text,estimate_tokens,redact_secrets,atomic_write_text`

**Definitions**
- `_build_python_units` (function, L20)
- `_pack_units` (function, L40)
- `_pack_units.flush` (method, L48)
- `_find_logical_boundary` (function, L82)
- `_pack_generic_lines` (function, L98)
- `chunk_file` (function, L119)

### `dev_tools/repo_context/rc_classify.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`

**Definitions**
- `is_test_path` (function, L30)
- `classify_file` (function, L42)
- `detect_generated_or_vendor` (function, L84)
- `detect_entrypoint_reason` (function, L100)

### `dev_tools/repo_context/rc_common.py`

**Imports**
- `__future__:annotations`
- `dataclasses:dataclass,field`
- `fnmatch`
- `hashlib`
- `pathlib:Path`
- `re`
- `typing:Optional`

**Definitions**
- `redact_secrets` (function, L66)
- `to_posix_rel` (function, L79)
- `sha256_file` (function, L83)
- `sha256_text` (function, L94)
- `count_lines_streaming` (function, L98)
- `stable_path_id` (function, L114)
- `sanitize_stem` (function, L118)
- `match_any_glob` (function, L123)
- `sniff_binary` (function, L130)
- `FileRecord` (class, L141)
- `SymbolRecord` (class, L159)
- `ImportRecord` (class, L178)
- `CallRecord` (class, L192)
- `ChunkRecord` (class, L205)
- `atomic_write_text` (function, L252)
- `atomic_write_bytes` (function, L260)
- `estimate_tokens` (function, L268)

### `dev_tools/repo_context/rc_manifest.py`

**Imports**
- `__future__:annotations`
- `csv`
- `datetime:datetime,timezone`
- `hashlib`
- `json`
- `pathlib:Path`
- `rc_common:ChunkRecord,TOOL_VERSION,sha256_file,atomic_write_text`

**Definitions**
- `chunking_signature` (function, L18)
- `load_previous_state` (function, L28)
- `make_chunk_reuse_provider` (function, L81)
- `make_chunk_reuse_provider.provider` (method, L90)
- `write_manifest` (function, L112)
- `utc_now_iso` (function, L152)

### `dev_tools/repo_context/rc_overview.py`

**Imports**
- `__future__:annotations`
- `collections:Counter,defaultdict`
- `rc_common:TOOL_VERSION,atomic_write_text`

**Definitions**
- `_top_dir` (function, L13)
- `generate_overview_md` (function, L18)
- `generate_readme_md` (function, L137)

### `dev_tools/repo_context/rc_packet.py`

**Imports**
- `__future__:annotations`
- `csv`
- `dataclasses:dataclass,field`
- `pathlib:Path`
- `rc_common:atomic_write_text,sanitize_stem,redact_secrets,sha256_file`
- `typing:Optional`

**Definitions**
- `_load_csv` (function, L17)
- `_norm_rel` (function, L24)
- `_file_is_fresh` (function, L28)
- `_safe_excerpt` (function, L53)
- `_symbol_matches` (function, L75)
- `_find_symbol_candidates` (function, L80)
- `PacketOptions` (class, L88)
- `Budget` (class, L105)
- `Budget.__init__` (method, L106)
- `Budget.allow` (method, L113)
- `Budget.spend` (method, L116)
- `_callers_of` (function, L121)
- `_callees_of` (function, L125)
- `_bfs_callers` (function, L129)
- `_bfs_callees` (function, L148)
- `_enclosing_class_or_func` (function, L168)
- `_candidate_tests_for_file` (function, L178)
- `_render_symbol_block` (function, L193)
- `generate_packet` (function, L230)
- `generate_packet.add_file_section` (method, L271)
- `generate_packet.add_symbol_section` (method, L342)

### `dev_tools/repo_context/rc_pyanalysis.py`

**Imports**
- `__future__:annotations`
- `ast`
- `dataclasses:dataclass,field`
- `rc_common:SymbolRecord,ImportRecord,CallRecord`
- `typing:Optional`

**Definitions**
- `dotted_module_path` (function, L22)
- `_complexity_count` (function, L30)
- `complexity_approx` (function, L47)
- `_is_main_guard` (function, L51)
- `_is_static_false_test` (function, L62)
- `_is_static_true_test` (function, L76)
- `_unparse_safe` (function, L82)
- `format_params` (function, L91)
- `format_params.fmt` (method, L94)
- `RawCall` (class, L126)
- `ClassInfo` (class, L137)
- `PyFileAnalysis` (class, L144)
- `_lambda_param_names` (function, L157)
- `_collect_local_bound_names` (function, L172)
- `_collect_local_bound_names.walk_stmts` (method, L187)
- `_collect_module_reassigned_names` (function, L223)
- `_collect_module_reassigned_names.walk_stmts` (method, L232)
- `analyze_python_source` (function, L246)
- `analyze_python_source.make_call` (method, L284)
- `analyze_python_source.walk_expr_for_calls` (method, L297)
- `analyze_python_source.record_import` (method, L311)
- `analyze_python_source.walk_body` (method, L350)
- `analyze_python_source.walk_child` (method, L355)
- `analyze_python_source.handle_class` (method, L409)
- `analyze_python_source.handle_func` (method, L449)
- `resolve_import_record` (function, L519)
- `build_import_bindings` (function, L569)
- `build_bindings_by_scope` (function, L597)
- `_lookup_in_scope_chain` (function, L606)
- `resolve_calls` (function, L657)
- `name_in_index` (function, L813)

### `dev_tools/repo_context/rc_scan.py`

**Imports**
- `__future__:annotations`
- `ast`
- `dataclasses:dataclass,field`
- `hashlib`
- `os`
- `pathlib:Path`
- `rc_chunking`
- `rc_classify`
- `rc_common:FileRecord,ChunkRecord,DEFAULT_EXCLUDE_DIRS,DEFAULT_EXCLUDE_FILE_GLOBS,BINARY_EXTENSIONS,match_any_glob,sniff_binary,sha256_file,count_lines_streaming`
- `rc_pyanalysis`
- `typing:Optional`

**Definitions**
- `ScanOptions` (class, L28)
- `ScanResult` (class, L48)
- `_sort_key` (function, L62)
- `_is_ancestor` (function, L66)
- `_should_exclude_file` (function, L74)
- `_walk` (function, L88)
- `_walk.recurse` (method, L96)
- `scan_repository` (function, L175)
- `_cleanup_stale_chunks` (function, L340)
- `_resolve_python_relationships` (function, L359)

### `dev_tools/repo_context/rc_tree.py`

**Imports**
- `__future__:annotations`
- `rc_common:atomic_write_text`

**Definitions**
- `_sort_key` (function, L7)
- `_human_size` (function, L11)
- `_Node` (class, L19)
- `_Node.__init__` (method, L22)
- `_build_tree` (function, L29)
- `_render` (function, L52)
- `generate_tree_text` (function, L75)
- `write_tree` (function, L82)

### `dev_tools/repo_context/rc_validate.py`

**Imports**
- `__future__:annotations`
- `csv`
- `hashlib`
- `json`
- `pathlib:Path`
- `rc_common:CSV_SCHEMAS`
- `re`

**Definitions**
- `_looks_absolute_or_backslashed` (function, L54)
- `ValidationResult` (class, L60)
- `ValidationResult.__init__` (method, L61)
- `ValidationResult.error` (method, L65)
- `ValidationResult.warn` (method, L68)
- `ValidationResult.ok` (method, L72)
- `_read_csv_rows` (function, L76)
- `validate_output_dir` (function, L83)
- `_symbol_name_matches` (function, L266)
- `format_report` (function, L270)

### `dev_tools/repo_context/rc_writers.py`

**Imports**
- `__future__:annotations`
- `csv`
- `io`
- `json`
- `pathlib:Path`
- `rc_common:CSV_SCHEMAS,atomic_write_text`

**Definitions**
- `_bool_str` (function, L12)
- `_rows_to_csv_text` (function, L16)
- `file_record_to_row` (function, L25)
- `file_record_to_dict` (function, L34)
- `symbol_record_to_row` (function, L46)
- `symbol_record_to_dict` (function, L55)
- `import_record_to_row` (function, L67)
- `call_record_to_row` (function, L74)
- `chunk_record_to_row` (function, L81)
- `write_all_tables` (function, L89)

### `dev_tools/repo_context/repo_context.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `pathlib:Path`
- `rc_common:TOOL_VERSION`
- `rc_common:atomic_write_text`
- `rc_manifest`
- `rc_overview`
- `rc_packet`
- `rc_scan`
- `rc_tree`
- `rc_validate`
- `rc_writers`
- `sys`

**Definitions**
- `_positive_int` (function, L34)
- `_non_negative_int` (function, L41)
- `_resolve_output_dir` (function, L48)
- `cmd_scan` (function, L74)
- `cmd_packet` (function, L137)
- `cmd_validate` (function, L166)
- `build_parser` (function, L173)
- `main` (function, L231)

### `dev_tools/repo_context/tests/conftest.py`

**Imports**
- `pathlib:Path`
- `pytest`
- `subprocess`
- `sys`

**Definitions**
- `run_tool` (function, L16)
- `write_files` (function, L23)
- `repo` (function, L35)
- `out` (function, L40)

### `dev_tools/repo_context/tests/test_chunking.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`
- `hashlib`

**Definitions**
- `_chunk_rows` (function, L6)
- `_assert_full_contiguous_coverage` (function, L12)
- `_make_big_python_file` (function, L20)
- `test_large_python_file_gets_chunked` (function, L30)
- `test_single_oversized_function_is_split_by_line_range` (function, L53)
- `test_chunk_target_lines_one_does_not_hang` (function, L71)
- `test_generic_text_chunking_has_overlap_and_full_coverage` (function, L95)

### `dev_tools/repo_context/tests/test_determinism_incremental.py`

**Imports**
- `conftest:run_tool,write_files`
- `json`
- `json`
- `json`
- `time`

**Definitions**
- `_make_repo` (function, L6)
- `test_deterministic_output_across_identical_runs` (function, L15)
- `test_incremental_regeneration_reuses_unchanged_chunk_output` (function, L28)
- `test_stale_chunks_removed_when_source_deleted` (function, L61)
- `test_force_bypasses_incremental_reuse` (function, L84)

### `dev_tools/repo_context/tests/test_imports_and_calls.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`

**Definitions**
- `_read` (function, L6)
- `test_absolute_and_relative_imports` (function, L11)
- `test_same_module_and_imported_function_calls` (function, L47)
- `test_unresolved_call_is_preserved_not_guessed` (function, L77)
- `test_ambiguous_import_resolution` (function, L98)
- `test_imports_in_nested_scopes_are_recorded` (function, L114)
- `test_call_shadowed_by_parameter_is_not_resolved_to_module_function` (function, L140)
- `test_function_local_import_does_not_leak_into_unrelated_function` (function, L159)
- `test_nested_def_name_shadows_module_level_symbol_throughout_function` (function, L182)
- `test_aliased_base_class_import_resolves_inherited_method` (function, L204)
- `test_module_level_rebinding_is_not_confidently_resolved` (function, L224)
- `test_module_qualified_base_class_resolves_inherited_method` (function, L246)
- `test_duplicate_import_resolved_by_call_site_order` (function, L266)
- `test_definition_time_calls_in_decorators_defaults_and_annotations` (function, L281)
- `test_lambda_parameter_shadowing_is_not_confidently_resolved` (function, L300)
- `test_import_inside_dead_if_false_branch_does_not_activate_call_resolution` (function, L313)
- `test_import_inside_type_checking_branch_does_not_activate_call_resolution` (function, L336)
- `test_import_in_live_else_of_dead_if_false_branch_still_resolves` (function, L355)
- `test_comprehension_target_does_not_shadow_module_level_symbol` (function, L375)
- `test_self_method_call_within_known_class` (function, L393)

### `dev_tools/repo_context/tests/test_inventory_and_exclusions.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`

**Definitions**
- `_read_csv` (function, L6)
- `test_basic_repository_inventory` (function, L11)
- `test_default_exclusions` (function, L35)
- `test_binary_file_detection` (function, L56)
- `test_duplicate_filenames_in_different_directories` (function, L71)
- `test_dyn_files_are_parsed_as_text_not_binary` (function, L88)
- `test_secret_files_excluded_by_default` (function, L98)

### `dev_tools/repo_context/tests/test_large_file_cap.py`

**Imports**
- `conftest:write_files`
- `rc_scan`

**Definitions**
- `test_oversized_text_file_is_surfaced_not_silently_skipped` (function, L12)

### `dev_tools/repo_context/tests/test_packets.py`

**Imports**
- `conftest:run_tool,write_files`

**Definitions**
- `_scan` (function, L4)
- `test_packet_by_file` (function, L9)
- `test_search_packet_respects_size_budget_on_repetitive_file` (function, L21)
- `test_packet_withholds_excerpt_when_source_changed_since_scan` (function, L37)
- `test_file_packet_respects_tiny_size_budget` (function, L51)
- `test_line_packet_raw_fallback_withholds_stale_excerpt` (function, L70)
- `test_search_packet_skips_files_changed_since_scan` (function, L83)
- `test_file_packet_imports_respect_size_budget` (function, L96)
- `test_packet_by_symbol` (function, L113)
- `test_packet_by_search` (function, L123)
- `test_packet_by_line` (function, L134)
- `test_ambiguous_symbol_requires_qualifier_or_all_matches` (function, L144)
- `test_packet_accepts_windows_style_relative_path` (function, L173)
- `test_symbol_packet_callers_respect_size_budget` (function, L184)
- `test_packet_without_prior_scan_fails_cleanly` (function, L205)

### `dev_tools/repo_context/tests/test_python_symbols.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`

**Definitions**
- `_symbols` (function, L30)
- `test_functions_classes_methods_nested_async` (function, L35)
- `test_utf8_bom_prefixed_python_file_still_parses` (function, L66)
- `test_syntax_error_handling_does_not_abort_scan` (function, L80)

### `dev_tools/repo_context/tests/test_safety.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`
- `shutil`

**Definitions**
- `test_refuses_output_dir_equal_to_root` (function, L6)
- `test_output_dir_inside_root_is_never_scanned_into_itself` (function, L12)
- `test_custom_named_output_dir_inside_root_is_auto_excluded` (function, L24)
- `test_nested_output_dir_excludes_only_itself_not_its_whole_parent` (function, L36)
- `test_invalid_root_returns_nonzero` (function, L55)
- `test_missing_subcommand_returns_nonzero` (function, L62)
- `test_non_positive_chunk_target_lines_rejected` (function, L67)
- `test_validate_does_not_require_packets_dir` (function, L75)

### `dev_tools/repo_context/tests/test_symlinks.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`
- `os`
- `pytest`

**Definitions**
- `test_symlink_escaping_root_is_excluded` (function, L13)
- `test_file_symlink_escaping_root_is_excluded_without_being_read` (function, L36)
- `test_symlink_cycle_inside_root_does_not_hang` (function, L53)
- `test_symlink_pointing_at_output_dir_is_excluded` (function, L62)
- `test_symlink_into_output_subdirectory_is_excluded` (function, L84)
- `test_fifo_directly_under_root_does_not_hang` (function, L103)

### `dev_tools/repo_context/tests/test_validate.py`

**Imports**
- `conftest:run_tool,write_files`
- `csv`

**Definitions**
- `_big_text` (function, L6)
- `test_validate_passes_on_freshly_generated_output` (function, L10)
- `test_validate_detects_modified_chunk` (function, L22)
- `test_validate_detects_missing_chunk` (function, L38)
- `test_validate_catches_entirely_deleted_chunk_rows` (function, L54)
- `test_scan_does_not_crash_on_malformed_prior_chunk_manifest` (function, L69)
- `test_validate_reports_bad_chunk_manifest_header_instead_of_crashing` (function, L89)
- `test_validate_reports_malformed_chunk_manifest_instead_of_crashing` (function, L110)
- `test_validate_reports_invalid_utf8_instead_of_crashing` (function, L131)
- `test_validate_fails_on_missing_required_file` (function, L147)

### `domains/__init__.py`

- No imports or definitions.

### `domains/arrowheads.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInCategory,ElementType`
- `core.canon:canon_str,fnum,S_MISSING,S_UNREADABLE`
- `core.collect:collect_types,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,canonicalize_str,canonicalize_str_allow_empty,canonicalize_int,canonicalize_float,canonicalize_bool,canonicalize_enum,make_identity_item,serialize_identity_items,build_record_v2`
- `core.rows:first_param,_as_string,_as_double,_as_int,format_len_inches,get_type_display_name`
- `os`
- `sys`

**Definitions**
- `_fmt_deg_from_rad` (function, L64)
- `_canon_yesno_bool` (function, L73)
- `_as_value_string` (function, L91)
- `_get_arrowhead_style` (function, L107)
- `_build_common_identity_items` (function, L154)
- `_build_arrow_identity_items` (function, L167)
- `_build_tick_identity_items` (function, L183)
- `_is_arrowhead_type` (function, L212)
- `_iter_arrowhead_ids_from_element` (function, L230)
- `extract` (function, L281)
- `extract._v2_block` (method, L330)

### `domains/browser_organization.py`

**Imports**
- `Autodesk.Revit.DB:BrowserOrganization,BuiltInParameter,ElementId`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_str_allow_empty,canonicalize_int,canonicalize_bool,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_discover_bip_reverse_lookup` (function, L172)
- `_resolve_sorting_parameter_name` (function, L203)
- `_resolve_workset_crosswalk` (function, L230)
- `_build_record` (function, L312)
- `extract_browser_organization` (function, L471)

### `domains/ceiling_types.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInCategory`
- `Autodesk.Revit.DB:CeilingType`
- `core.collect:collect_types,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_int,canonicalize_float,make_identity_item,serialize_identity_items,build_record_v2`
- `domains.compound_layers:_build_name_key,_build_instance_count_map,_attach_placeholder_metadata,_read_compound_structure,_read_type_name,_label_for_type,_require_compound_dependencies,_coarse_fill_reads`
- `os`
- `sys`

**Definitions**
- `extract_ceiling_types` (function, L55)

### `domains/compound_layers.py`

**Imports**
- `Autodesk.Revit.DB:CompoundStructure,CompoundStructureLayer,MaterialFunctionAssignment,BuiltInParameter,ShellLayerType`
- `Autodesk.Revit.DB:DeckEmbeddingType`
- `core.canon:S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:collect_instances`
- `core.deps:require_domain,Blocked`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_UNREADABLE,canonicalize_str`
- `domains.materials:CTX_MATERIAL_UID_TO_NAME,CTX_MATERIAL_UID_TO_CLASS`
- `os`
- `sys`

**Definitions**
- `_enum_name` (function, L70)
- `_build_name_key` (function, L78)
- `_build_instance_count_map` (function, L99)
- `_attach_placeholder_metadata` (function, L120)
- `_na_or` (function, L141)
- `_material_identity_from_layer` (function, L147)
- `_layer_function_str` (function, L168)
- `_stack_hash_field` (function, L175)
- `_read_compound_structure` (function, L181)
- `_read_type_name` (function, L385)
- `_label_for_type` (function, L403)
- `_require_compound_dependencies` (function, L412)
- `_coarse_fill_reads` (function, L430)

### `domains/dimension_types.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInCategory,DimensionType`
- `core.canon:canon_str,S_MISSING,S_UNREADABLE`
- `core.collect:collect_types,collect_instances,purge_lookup`
- `core.dimension_type_helpers:_fmt_in_from_ft,_get_dimension_shape,_build_text_appearance_items,_read_tick_mark_sig_hash,_read_unit_format_info,_read_prefix_suffix,_read_leader_arrowhead,_read_arrowhead_ref_sig_hash,_read_element_ref_name,_read_line_pattern_ref_sig_hash,_build_alternate_units_items,get_type_display_name,SHAPE_LINEAR,SHAPE_LINEAR_FIXED,SHAPE_ARC_LENGTH,SHAPE_ANGULAR,SHAPE_RADIAL,SHAPE_DIAMETER,SHAPE_SPOT_ELEVATION,SHAPE_SPOT_ELEVATION_FIXED,SHAPE_SPOT_COORDINATE,SHAPE_ALIGNMENT_STATION_LABEL,SHAPE_SPOT_SLOPE,FAMILY_LINEAR,FAMILY_ANGULAR,FAMILY_RADIAL,FAMILY_SPOT`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:canonicalize_str,canonicalize_str_allow_empty,canonicalize_int,canonicalize_float,canonicalize_bool,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,build_record_v2,make_identity_item,serialize_identity_items,STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED`
- `core.rows:first_param,_as_string,_as_value_string,_as_double,_as_int,format_len_inches`
- `os`
- `sys`

**Definitions**
- `_collect_dim_types` (function, L106)
- `_build_dimension_instance_count_map` (function, L123)
- `_attach_placeholder_metadata` (function, L144)
- `_apply_family_name_override` (function, L156)
- `extract_linear` (function, L178)
- `_apply_family_name_override` (function, L631)
- `extract_angular` (function, L653)
- `_apply_family_name_override` (function, L1058)
- `extract_radial` (function, L1080)
- `_apply_family_name_override` (function, L1431)
- `extract_diameter` (function, L1453)
- `_apply_family_name_override` (function, L1804)
- `_read_symbol_name` (function, L1825)
- `extract_spot_elevation` (function, L1871)
- `_apply_family_name_override` (function, L2263)
- `_read_symbol_name` (function, L2284)
- `extract_spot_coordinate` (function, L2330)
- `_apply_family_name_override` (function, L2740)
- `extract_spot_slope` (function, L2761)

### `domains/fill_patterns.py`

**Imports**
- `Autodesk.Revit.DB:FillPatternElement`
- `core.canon:canon_str,fnum,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:purge_lookup,collect_instances`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,canonicalize_str,canonicalize_int,canonicalize_bool,canonicalize_float,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `re`
- `sys`

**Definitions**
- `_phase2_fill_pattern_is_import` (function, L54)
- `_export_fill_pattern_ctx` (function, L98)
- `_collect_fill_patterns` (function, L117)
- `extract_drafting` (function, L133)
- `extract_drafting.f` (method, L175)
- `extract_drafting.grid_sig` (method, L183)
- `extract_drafting.grid_sig.add_float` (method, L205)
- `extract_drafting.grid_sig.add_origin_2d` (method, L213)
- `extract_drafting._bump_v2_reason` (method, L256)
- `extract_drafting._grid_sig_v2` (method, L263)
- `extract_drafting._grid_sig_v2.req_float` (method, L286)
- `extract_drafting._grid_sig_v2.req_origin` (method, L301)
- `extract_drafting._phase2_try_get_grid` (method, L363)
- `extract_drafting._phase2_add_float` (method, L378)
- `extract_drafting._phase2_add_int` (method, L385)
- `extract_drafting._phase2_add_bool` (method, L392)
- `extract_drafting._phase2_add_str` (method, L399)
- `extract_drafting._phase2_build_phase2` (method, L406)
- `extract_model` (function, L1040)
- `extract_model.f` (method, L1082)
- `extract_model.grid_sig` (method, L1090)
- `extract_model.grid_sig.add_float` (method, L1112)
- `extract_model.grid_sig.add_origin_2d` (method, L1120)
- `extract_model._bump_v2_reason` (method, L1163)
- `extract_model._grid_sig_v2` (method, L1170)
- `extract_model._grid_sig_v2.req_float` (method, L1193)
- `extract_model._grid_sig_v2.req_origin` (method, L1208)
- `extract_model._phase2_try_get_grid` (method, L1270)
- `extract_model._phase2_add_float` (method, L1285)
- `extract_model._phase2_add_int` (method, L1292)
- `extract_model._phase2_add_bool` (method, L1299)
- `extract_model._phase2_add_str` (method, L1306)
- `extract_model._phase2_build_phase2` (method, L1313)

### `domains/floor_types.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInCategory`
- `Autodesk.Revit.DB:FloorFunction`
- `Autodesk.Revit.DB:FloorType`
- `core.canon:S_UNREADABLE`
- `core.collect:collect_types,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_int,canonicalize_float,canonicalize_bool,make_identity_item,serialize_identity_items,build_record_v2`
- `domains.compound_layers:_enum_name,_build_name_key,_build_instance_count_map,_attach_placeholder_metadata,_read_compound_structure,_read_type_name,_label_for_type,_require_compound_dependencies,_coarse_fill_reads`
- `os`
- `sys`

**Definitions**
- `extract_floor_types` (function, L64)

### `domains/identity.py`

**Imports**
- `Autodesk.Revit.DB:WorksharingUtils,BuiltInParameter`
- `System:Guid`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.deployment_config:validate_project_info_shared_parameters`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str,phase2_join_hash`
- `core.record_v2:STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,build_record_v2,canonicalize_bool,canonicalize_str,make_identity_item,serialize_identity_items`
- `os`
- `sys`

**Definitions**
- `_param_raw_str` (function, L110)
- `_read_project_info_builtin_item` (function, L118)
- `_read_project_info_named_item` (function, L150)
- `_configured_project_info_fields` (function, L191)
- `_extract_project_info_items` (function, L196)
- `_phase2_build_lineage_items` (function, L236)
- `extract` (function, L272)

### `domains/line_patterns.py`

**Imports**
- `Autodesk.Revit.DB:LinePatternElement`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:purge_lookup,collect_instances`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,canonicalize_str,canonicalize_int,canonicalize_float,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_lp_seg_type_id_and_name` (function, L76)
- `_line_pattern_segments_def_hash` (function, L111)
- `extract` (function, L142)

### `domains/line_styles.py`

**Imports**
- `Autodesk.Revit.DB:Category,BuiltInCategory,GraphicsStyleType,ElementId`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED,canonicalize_str,canonicalize_int,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_line_pattern_segment_kind_id` (function, L70)
- `_line_pattern_synopsis_from_segments` (function, L84)
- `_line_pattern_synopsis_from_element` (function, L104)
- `extract` (function, L128)

### `domains/loaded_family_types.py`

**Imports**
- `Autodesk.Revit.DB:FamilySymbol,ParameterElement,SharedParameterElement`
- `collections:defaultdict`
- `core.collect:collect_types`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,canonicalize_str,canonicalize_int,canonicalize_bool,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_safe_attr` (function, L77)
- `_param_id_int` (function, L84)
- `_read_param_value` (function, L91)
- `_binding_scope` (function, L141)
- `_semantic_role` (function, L162)
- `_safe_guid_str` (function, L173)
- `_build_param_key` (function, L182)
- `_extract_param_meta` (function, L190)
- `extract` (function, L217)

### `domains/materials.py`

**Imports**
- `Autodesk.Revit.DB:Material,BuiltInParameter`
- `core.canon:S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE,S_NONE,S_UNRESOLVED,canon_str`
- `core.collect:collect_instances,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,canonicalize_str,canonicalize_int,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_read_prop` (function, L59)
- `_canon_id_local` (function, L66)
- `_rgb_sig` (function, L79)
- `_read_param_as_string` (function, L88)
- `_resolve_pattern_slot` (function, L142)
- `_export_ctx` (function, L204)
- `_safe_item_value` (function, L213)
- `_mk_item` (function, L224)
- `extract` (function, L253)

### `domains/object_styles.py`

**Imports**
- `Autodesk.Revit.DB:GraphicsStyleType,CategoryType`
- `core.canon:canon_str`
- `core.collect:build_subcategory_used_id_set`
- `core.deps:require_domain,Blocked`
- `core.graphic_overrides:extract_projection_graphics,extract_cut_graphics`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_int,canonicalize_bool,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_is_model_category_int0` (function, L62)
- `_collect_categories` (function, L72)
- `_is_analytical_category_type` (function, L120)
- `_matches_category_type` (function, L127)
- `_subcategory_purge_lookup` (function, L143)
- `_rgb_sig` (function, L152)
- `_material_ref_item` (function, L159)
- `_build_info` (function, L202)
- `_extract_object_styles` (function, L218)
- `extract_model` (function, L558)
- `extract_annotation` (function, L562)
- `extract_analytical` (function, L566)
- `extract_imported` (function, L570)

### `domains/phase_filters.py`

**Imports**
- `Autodesk.Revit.DB:PhaseFilter,ElementOnPhaseStatus`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:collect_instances`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,canonicalize_int,canonicalize_str,canonicalize_str_allow_empty,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `extract` (function, L64)

### `domains/phase_graphics.py`

**Imports**
- `Autodesk.Revit.DB:ElementOnPhaseStatus`
- `core.hashing:make_hash,safe_str`
- `core.phase2:phase2_sorted_items,phase2_join_hash`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_IMPLEMENTED,canonicalize_str,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_phase2_build_join_key_items` (function, L56)
- `extract` (function, L68)

### `domains/phases.py`

**Imports**
- `Autodesk.Revit.DB:Phase`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:collect_types`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,canonicalize_int,canonicalize_str,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_phase2_build_phase2_payload` (function, L65)
- `extract` (function, L111)
- `extract._v2_block` (method, L170)

### `domains/roof_types.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInCategory`
- `Autodesk.Revit.DB:RoofType`
- `core.collect:collect_types,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_int,canonicalize_float,make_identity_item,serialize_identity_items,build_record_v2`
- `domains.compound_layers:_build_name_key,_build_instance_count_map,_attach_placeholder_metadata,_read_compound_structure,_read_type_name,_label_for_type,_require_compound_dependencies,_coarse_fill_reads`
- `os`
- `sys`

**Definitions**
- `extract_roof_types` (function, L55)

### `domains/text_types.py`

**Imports**
- `Autodesk.Revit.DB:BuiltInCategory,TextNoteType,TextNote`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,fnum,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.canonical_items:build_flat_items`
- `core.collect:collect_types,collect_instances,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,SCHEMA_VERSION_RECORD_V2,canonicalize_str,canonicalize_int,canonicalize_float,canonicalize_bool,make_identity_item,serialize_identity_items,build_record_v2`
- `core.rows:first_param,_as_string,_as_double,_as_int,_as_bool_from_param,format_len_inches,try_get_color_rgb_from_elem,get_type_display_name,get_element_display_name`
- `os`
- `sys`

**Definitions**
- `_phase2_item` (function, L76)
- `_phase2_build_payload` (function, L81)
- `_build_textnote_instance_count_map` (function, L154)
- `_read_instance_and_sole_flags` (function, L175)
- `extract` (function, L195)
- `extract._v2_block` (method, L248)
- `extract._canon_rgb` (method, L285)

### `domains/units.py`

**Imports**
- `Autodesk.Revit.DB:SpecTypeId`
- `core.canon:canon_str,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:canonicalize_str,canonicalize_enum,canonicalize_float,canonicalize_bool,canonicalize_int,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED,build_record_v2,make_identity_item,serialize_identity_items,STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED`
- `os`
- `sys`

**Definitions**
- `extract` (function, L119)
- `extract._resolve_spec` (method, L169)
- `extract_units_doc` (function, L491)

### `domains/view_category_overrides.py`

**Imports**
- `core.hashing:make_hash,safe_str`
- `domains.view_templates:_VIEW_INSTANCES_CACHE_KEY`
- `domains:view_category_overrides_annotation`
- `domains:view_category_overrides_model`
- `os`
- `sys`

**Definitions**
- `extract` (function, L18)

### `domains/view_category_overrides_annotation.py`

**Imports**
- `Autodesk.Revit.DB:View,OverrideGraphicSettings`
- `core.collect:collect_instances`
- `core.graphic_overrides:extract_projection_graphics,extract_cut_graphics,extract_halftone,extract_transparency`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,make_identity_item,serialize_identity_items,build_record_v2,canonicalize_str,canonicalize_int,canonicalize_bool,ITEM_Q_UNSUPPORTED`
- `domains.view_templates:_VIEW_INSTANCES_CACHE_KEY`
- `os`
- `sys`

**Definitions**
- `_phase2_partition_items` (function, L49)
- `_safe_bool` (function, L66)
- `_category_hidden_item` (function, L73)
- `extract` (function, L83)

### `domains/view_category_overrides_model.py`

**Imports**
- `Autodesk.Revit.DB:View,OverrideGraphicSettings`
- `core.collect:collect_instances`
- `core.deps:require_domain,Blocked`
- `core.graphic_overrides:extract_projection_graphics,extract_cut_graphics,extract_halftone,extract_transparency`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,make_identity_item,serialize_identity_items,build_record_v2,canonicalize_str,canonicalize_int,canonicalize_bool,ITEM_Q_UNSUPPORTED`
- `domains.view_templates:_VIEW_INSTANCES_CACHE_KEY`
- `hashlib`
- `os`
- `sys`
- `time`

**Definitions**
- `_compute_override_properties_hash` (function, L51)
- `_phase2_partition_items` (function, L73)
- `_safe_bool` (function, L90)
- `_category_hidden_item` (function, L97)
- `extract` (function, L107)

### `domains/view_filter_applications_view_templates.py`

**Imports**
- `Autodesk.Revit.DB:ElementId,View,ViewSchedule`
- `__future__:annotations`
- `core.collect:collect_instances`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_join_hash,phase2_sorted_items`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_OK,ITEM_Q_UNSUPPORTED,ITEM_Q_UNREADABLE,STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK,build_record_v2,canonical_structural_fields,canonicalize_bool,canonicalize_int,canonicalize_str,finalize_record_ids_for_domain,make_identity_item,make_record_id_from_element,make_record_id_structural,serialize_identity_items`
- `domains.view_templates:_VIEW_INSTANCES_CACHE_KEY`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `_is_schedule_view` (function, L54)
- `_semantic_keys_from_identity_items` (function, L71)
- `extract` (function, L85)

### `domains/view_filter_definitions.py`

**Imports**
- `Autodesk.Revit:DB`
- `__future__:annotations`
- `core.collect:purge_lookup,collect_instances`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_OK,ITEM_Q_UNREADABLE,STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK,build_record_v2,canonical_structural_fields,canonicalize_int,canonicalize_str,finalize_record_ids_for_domain,make_identity_item,make_record_id_from_element,make_record_id_structural,serialize_identity_items`
- `json`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_logic_root_token` (function, L55)
- `_param_ref_from_param_id` (function, L69)
- `_value_token_from_rule` (function, L117)
- `_op_token_from_rule` (function, L162)
- `_walk_rules` (function, L175)
- `_walk_rules._append_rule` (method, L184)
- `extract` (function, L235)

### `domains/view_templates.py`

**Imports**
- `Autodesk.Revit.DB:FilteredWorksetCollector,WorksetKind`
- `Autodesk.Revit.DB:View,ViewSchedule,BuiltInParameter`
- `Autodesk.Revit.DB:ViewType`
- `core.canon:canon_str,fnum,canon_num,canon_bool,canon_id,S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:purge_lookup,collect_instances`
- `core.deps:require_domain,Blocked`
- `core.graphic_overrides:extract_projection_graphics,extract_cut_graphics,extract_halftone,extract_transparency`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items,phase2_qv_from_legacy_sentinel_str,phase2_join_hash`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_MISSING,ITEM_Q_OK,ITEM_Q_UNREADABLE,canonicalize_int,canonicalize_str,build_record_v2,make_identity_item,make_record_id_from_element,serialize_identity_items`
- `core.vg_sig:_traceability_unknown_items,emit_builtin_params,emit_shared_params_stub`
- `os`
- `sys`

**Definitions**
- `_collect_templates` (function, L57)
- `_non_ctrl_bips_from_view` (function, L74)
- `_is_template_param_included` (function, L85)
- `_append_assigned_view_count_cosmetic_item` (function, L94)
- `_append_phase_filter_value` (function, L122)
- `_append_filter_stack_signature` (function, L176)
- `_append_workset_visibility` (function, L274)
- `_phase2_items_from_def_signature` (function, L307)
- `_canonical_identity_items_from_signature` (function, L343)
- `_semantic_keys_from_identity_items` (function, L351)
- `_build_floor_structural_area_viewtype_set` (function, L395)
- `_build_ceiling_plan_viewtype_set` (function, L408)
- `extract_floor_structural_area_plans` (function, L422)
- `extract_floor_structural_area_plans._v2_block` (method, L508)
- `extract_ceiling_plans` (function, L847)
- `extract_ceiling_plans._v2_block` (method, L933)
- `_build_elevation_section_detail_viewtype_set` (function, L1264)
- `extract_elevations_sections_detail` (function, L1297)
- `extract_elevations_sections_detail._v2_block` (method, L1383)
- `_build_renderings_drafting_viewtype_set` (function, L1707)
- `extract_renderings_drafting` (function, L1723)
- `extract_renderings_drafting._v2_block` (method, L1809)
- `_is_schedule_view` (function, L2127)
- `extract_schedules` (function, L2145)
- `extract_schedules._v2_block` (method, L2230)

### `domains/wall_types.py`

**Imports**
- `Autodesk.Revit.DB:WallType,WallKind,WallFunction,BuiltInCategory`
- `core.canon:S_MISSING,S_UNREADABLE,S_NOT_APPLICABLE`
- `core.collect:collect_types,purge_lookup`
- `core.hashing:make_hash,safe_str`
- `core.record_v2:STATUS_OK,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_int,canonicalize_float,canonicalize_bool,make_identity_item,serialize_identity_items,build_record_v2`
- `domains.compound_layers:_enum_name,_build_name_key,_build_instance_count_map,_attach_placeholder_metadata,_read_compound_structure,_read_type_name,_label_for_type,_require_compound_dependencies,_coarse_fill_reads`
- `domains.materials:CTX_MATERIAL_UID_TO_NAME,CTX_MATERIAL_UID_TO_CLASS`
- `os`
- `sys`

**Definitions**
- `_canon_non_sentinel_str` (function, L76)
- `_read_wall_kind` (function, L85)
- `_blocked_required_items` (function, L99)
- `extract_wall_types` (function, L108)

### `domains/worksets.py`

**Imports**
- `Autodesk.Revit.DB:FilteredWorksetCollector,WorksetKind`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.phase2:phase2_sorted_items`
- `core.record_v2:STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE,canonicalize_str,canonicalize_str_allow_empty,canonicalize_bool,canonicalize_int,make_identity_item,serialize_identity_items,build_record_v2`
- `os`
- `sys`

**Definitions**
- `_discover_workset_kind_names` (function, L141)
- `_build_per_workset_record` (function, L174)
- `_build_doc_level_record` (function, L328)
- `_read_is_workshared` (function, L429)
- `_resolve_active_workset_id` (function, L448)
- `_collect_user_worksets` (function, L466)
- `_collect_kind_counts` (function, L484)
- `_resolve_active_workset_name` (function, L500)
- `extract_worksets` (function, L512)
- `extract_worksets_doc` (function, L611)

### `legacy/fingerprint_mvp.py`

**Imports**
- `Autodesk.Revit.DB:CategoryType`
- `Autodesk.Revit.DB:FilteredElementCollector,LinePatternElement,TextNoteType,DimensionType,View,GraphicsStyleType,WorksharingUtils,BuiltInCategory,SpecTypeId,CategoryType,ElementType,BuiltInParameter,UnitUtils,UnitTypeId,ElementId,FillPatternElement,Category`
- `RevitServices.Persistence:DocumentManager`
- `System.Security.Cryptography:MD5`
- `System.Text:Encoding`
- `clr`
- `json`

**Definitions**
- `add_origin` (function, L35)
- `rgb_sig_from_color` (function, L64)
- `canon_str` (function, L70)
- `sig_val` (function, L79)
- `get_element_display_name` (function, L85)
- `_param` (function, L115)
- `_as_string` (function, L121)
- `_as_double` (function, L131)
- `_as_int` (function, L139)
- `_as_bool_from_param` (function, L147)
- `first_param` (function, L153)
- `fnum` (function, L180)
- `format_len_inches` (function, L183)
- `rgb_dict_from_color` (function, L194)
- `try_get_color_rgb_from_elem` (function, L200)
- `get_type_display_name` (function, L219)
- `safe_str` (function, L246)
- `make_hash` (function, L255)
- `get_doc` (function, L269)
- `get_linestyles_fingerprint` (function, L274)
- `get_fillpattern_fingerprint` (function, L379)
- `get_fillpattern_fingerprint.f` (method, L402)
- `get_fillpattern_fingerprint.read_is_model` (method, L410)
- `get_fillpattern_fingerprint.grid_sig` (method, L428)
- `get_fillpattern_fingerprint.grid_sig.add_float` (method, L450)
- `get_fillpattern_fingerprint.grid_sig.add_origin_2d` (method, L458)
- `get_identity_fingerprint` (function, L601)
- `get_units_fingerprint` (function, L631)
- `get_objectstyles_fingerprint` (function, L695)
- `get_objectstyles_fingerprint.row_sig` (method, L739)
- `get_linepattern_fingerprint` (function, L884)
- `get_linepattern_fingerprint.fnum` (method, L916)
- `get_texttype_fingerprint` (function, L1075)
- `get_dimtype_fingerprint` (function, L1230)
- `get_viewtemplate_fingerprint` (function, L1362)

### `mapping/__init__.py`

- No imports or definitions.

### `mapping/_dynamo_bootstrap.py`

**Imports**
- `__future__:annotations`
- `clr`
- `os`
- `sys`
- `typing:Iterable,Optional,Sequence`

**Definitions**
- `looks_like_repo_root` (function, L60)
- `resolve_repo_root` (function, L70)
- `purge_repo_modules` (function, L124)
- `promote_on_sys_path` (function, L141)
- `add_revit_api_references` (function, L152)
- `bootstrap` (function, L168)

### `mapping/create_line_pattern_mappings.py`

**Imports**
- `RevitServices.Persistence:DocumentManager`
- `importlib.util`
- `mapping.line_pattern_reconstruction:ACTION_SKIPPED,STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK,MappingOutcome,build_report_rows,compute_run_status,get_line_patterns_join_key_policy,group_names_by_join_hash,group_requested_join_hashes,group_settings_by_join_hash,load_bundle_pattern_detail_export,reconstruct_pattern,write_report_csv`
- `mapping.line_pattern_revit_apply:build_name_index,resolve_mapping`
- `os`

**Definitions**
- `_load_bootstrap_module_from` (function, L57)
- `_load_dynamo_bootstrap` (function, L67)
- `run` (function, L155)

### `mapping/line_pattern_reconstruction.py`

**Imports**
- `__future__:annotations`
- `core.hashing:make_hash,safe_str`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:load_join_key_policies,get_domain_join_key_policy`
- `core.record_v2:ITEM_Q_OK,STATUS_OK,STATUS_DEGRADED,STATUS_BLOCKED,canonicalize_float,make_identity_item`
- `csv`
- `dataclasses:dataclass,field`
- `domains.line_patterns:_LP_SEG_TYPE_NAME`
- `math`
- `os`
- `re`
- `sys`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `dominant_status` (function, L124)
- `get_line_patterns_join_key_policy` (function, L133)
- `read_csv_rows` (function, L153)
- `load_bundle_pattern_detail_export` (function, L158)
- `SkippedRequest` (class, L183)
- `group_requested_join_hashes` (function, L190)
- `group_settings_by_join_hash` (function, L248)
- `group_names_by_join_hash` (function, L260)
- `compute_segments_def_hash` (function, L276)
- `compute_segments_norm_hash` (function, L290)
- `compute_join_hash_for_segments` (function, L317)
- `ReconstructedPattern` (class, L348)
- `ReconstructedPattern.blocked` (method, L358)
- `_blocked` (function, L362)
- `reconstruct_pattern` (function, L366)
- `short_join_hash` (function, L511)
- `sanitize_revit_name` (function, L515)
- `select_observed_name` (function, L532)
- `select_observed_name._files_count` (method, L547)
- `resolve_observed_name` (function, L560)
- `build_mapping_name_candidates` (function, L569)
- `MappingOutcome` (class, L588)
- `outcome_to_report_row` (function, L625)
- `build_report_rows` (function, L643)
- `write_report_csv` (function, L653)
- `compute_run_status` (function, L668)

### `mapping/line_pattern_revit_apply.py`

**Imports**
- `Autodesk.Revit.DB:LinePattern,LinePatternElement,LinePatternSegment,LinePatternSegmentType,Transaction,TransactionStatus`
- `System.Collections.Generic:List`
- `__future__:annotations`
- `clr`
- `core.collect:collect_instances`
- `core.hashing:safe_str`
- `dataclasses:dataclass`
- `domains.line_patterns:_LP_SEG_TYPE_NAME,_lp_seg_type_id_and_name`
- `mapping.line_pattern_reconstruction:ACTION_BLOCKED,ACTION_CREATED,ACTION_EXISTING,MappingOutcome,ReconstructedPattern,STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK,build_mapping_name_candidates,compute_join_hash_for_segments,dominant_status,get_line_patterns_join_key_policy,resolve_observed_name`
- `os`
- `sys`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `read_segments_from_element` (function, L83)
- `build_name_index` (function, L137)
- `VerificationResult` (class, L166)
- `verify_element_join_hash` (function, L172)
- `CreationResult` (class, L193)
- `_build_api_segments` (function, L201)
- `create_and_verify_line_pattern` (function, L212)
- `resolve_mapping` (function, L291)

### `runner/__init__.py`

- No imports or definitions.

### `runner/extraction_context.py`

**Imports**
- `__future__:annotations`
- `core.deployment_config:load_deployment_config`
- `core.join_key_policy:load_join_key_policies`
- `os`
- `pathlib:Path`

**Definitions**
- `operator_deployment_config_path` (function, L13)
- `build_extraction_context` (function, L19)

### `runner/probe_thin_runner.py`

**Imports**
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `fnmatch`
- `gc`
- `json`
- `os`
- `sys`
- `traceback`
- `uuid`

**Definitions**
- `_looks_like_unc_path` (function, L97)
- `_is_probably_sync_path` (function, L104)
- `_is_probes_root` (function, L117)
- `_candidate_repo_dirs` (function, L125)
- `_resolve_repo_dir` (function, L160)
- `_read_tool_version` (function, L177)
- `_get_in` (function, L193)
- `_safe` (function, L246)
- `_revit_version` (function, L253)
- `_document_identity` (function, L263)
- `_discover_probe_files` (function, L290)
- `_matches_filter` (function, L307)
- `_probe_in_for` (function, L320)
- `_default_output_dir` (function, L328)
- `_run_one_probe` (function, L339)
- `_domains_declared_in_out` (function, L366)
- `_json_block` (function, L427)
- `_flush_domain` (function, L461)

### `runner/purge_sys_modules_standalone.py`

**Imports**
- `sys`

**Definitions**
- `_to_list` (function, L36)
- `_to_bool` (function, L47)
- `_is_protected` (function, L65)
- `purge_modules` (function, L70)

### `runner/run_dynamo.py`

**Imports**
- `Autodesk.Revit.DB:View`
- `RevitServices.Persistence:DocumentManager`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `core.canonical_items:canonicalize_record`
- `core.collect:CollectCtx,build_purgeable_id_set`
- `core.collect:collect_instances`
- `core.context:DocViewContext`
- `core.deps:Blocked,require_domain`
- `core.features:build_features`
- `core.manifest:build_manifest`
- `core.timing_collector:TimingCollector`
- `core:contracts`
- `core:hashing`
- `core:hashing`
- `core:naming`
- `datetime:datetime`
- `datetime:datetime`
- `domains.view_templates:_VIEW_INSTANCES_CACHE_KEY`
- `domains:arrowheads,text_types`
- `domains:browser_organization`
- `domains:dimension_types`
- `domains:fill_patterns`
- `domains:identity,units,line_patterns,line_styles`
- `domains:loaded_family_types`
- `domains:materials`
- `domains:object_styles`
- `domains:phases,phase_filters,phase_graphics`
- `domains:view_category_overrides_annotation`
- `domains:view_category_overrides_model`
- `domains:view_filter_definitions,view_filter_applications_view_templates`
- `domains:view_templates`
- `domains:wall_types,floor_types,roof_types,ceiling_types`
- `domains:worksets`
- `hashlib`
- `json`
- `os`
- `re`
- `runner.extraction_context:build_extraction_context,operator_deployment_config_path`
- `sys`
- `tempfile`
- `time`
- `time`
- `traceback`
- `traceback`

**Definitions**
- `_looks_like_unc_path` (function, L23)
- `_is_probably_sync_path` (function, L30)
- `_is_repo_root` (function, L47)
- `_read_tool_version` (function, L91)
- `_use_filename_stamp` (function, L171)
- `_extract_v2_hash` (function, L202)
- `_extract_legacy_quality` (function, L221)
- `_extract_v2_block_reasons` (function, L232)
- `_looks_like_revit_unique_id` (function, L274)
- `_has_v2_surface` (function, L285)
- `_domain_run` (function, L302)
- `_build_workset_name_to_unique_id_ctx` (function, L472)
- `_enabled` (function, L506)
- `get_doc` (function, L520)
- `run_fingerprint` (function, L524)
- `_resolve_output_mode` (function, L1175)
- `_strip_detail_surfaces` (function, L1184)
- `_canonicalize_all_domain_records` (function, L1247)
- `_get_output_path_from_dynamo` (function, L1330)
- `_ensure_parent_dir` (function, L1379)
- `_write_json_to_disk` (function, L1387)
- `_write_fingerprint` (function, L1416)
- `_write_fingerprint._try_write` (method, L1431)
- `_sha256_of_file` (function, L1450)

### `runner/thin_runner.py`

**Imports**
- `Dynamo.Events:ExecutionEvents`
- `System`
- `clr`
- `importlib`
- `json`
- `os`
- `sys`
- `traceback`

**Definitions**
- `_parse_boolish` (function, L52)
- `_looks_like_unc_path` (function, L146)
- `_is_probably_sync_path` (function, L153)
- `_is_repo_root` (function, L176)
- `_iter_dyn_path_candidates` (function, L185)
- `_iter_dyn_path_candidates._normalize_host_path` (method, L190)
- `_iter_dyn_path_candidates._add` (method, L214)
- `_iter_dyn_path_candidates._probe_dynamo_workspace_path` (method, L252)
- `_nearest_repo_root_from_path` (function, L359)
- `_candidate_repo_dirs` (function, L383)
- `_purge_repo_modules` (function, L485)

### `scripts/check_audit_references.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `re`
- `subprocess`

**Definitions**
- `tracked_files` (function, L14)
- `main` (function, L21)

### `sync_revitlookup_reference.py`

**Imports**
- `argparse`
- `datetime:datetime,timezone`
- `json`
- `os`
- `pathlib:Path`
- `sys`
- `time`
- `urllib.error`
- `urllib.request`

**Definitions**
- `github_get` (function, L106)
- `fetch_raw` (function, L123)
- `get_current_commit_sha` (function, L137)
- `list_all_cs_files` (function, L142)
- `sync` (function, L153)
- `main` (function, L264)

### `tests/conftest.py`

**Imports**
- `os`
- `sys`

**Definitions**
- `pytest_configure` (function, L6)

### `tests/revit/_json_diff.py`

**Imports**
- `__future__:annotations`
- `hashlib`
- `json`
- `typing:Any,Dict,List,Tuple`

**Definitions**
- `_canon_obj` (function, L19)
- `canonical_json_bytes` (function, L29)
- `sha256_of_json` (function, L36)
- `pretty_json` (function, L40)
- `diff_paths` (function, L45)
- `diff_paths._preview` (method, L54)
- `diff_paths._walk` (method, L67)
- `compare_json` (function, L138)

### `tests/revit/revit_test_runner_pyrevit.py`

**Imports**
- `__future__:annotations`
- `core.manifest:build_manifest`
- `datetime:datetime`
- `json`
- `os`
- `runner.run_dynamo:run_fingerprint`
- `sys`
- `tests.revit._json_diff:compare_json,pretty_json`
- `traceback`

**Definitions**
- `_load_json` (function, L39)
- `_write_text` (function, L44)
- `_write_json` (function, L52)
- `_now_stamp` (function, L56)
- `main` (function, L60)

### `tests/run_join_key_tests.py`

**Imports**
- `hashlib`
- `json`
- `os`
- `sys`

**Definitions**
- `_compute_override_properties_hash` (function, L21)

### `tests/synthetic_governance_fixtures.py`

- No imports or definitions.

### `tests/test_analyze_promotion_candidates.py`

**Imports**
- `analyze_promotion_candidates`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `_gov_row` (function, L28)
- `_reuse_row` (function, L50)
- `corpus_root` (function, L79)
- `_read` (function, L249)
- `test_enterprise_pool_flag_is_policy_driven_and_has_no_legacy_alias` (function, L253)
- `test_invalid_source_schema_creates_no_output_directory` (function, L265)
- `test_scope_gap_candidate_routing` (function, L277)
- `test_baseline_equal_scope_excluded` (function, L287)
- `test_underused_routed_separately` (function, L295)
- `test_below_reuse_floor_not_classified` (function, L305)
- `test_files_used_not_inflated_by_repeated_target_across_references` (function, L314)
- `test_tied_client_rows_are_aggregated_not_dropped` (function, L325)
- `test_pattern_label_variation_does_not_split_identity` (function, L340)
- `test_unit_system_partitions_scope_evidence` (function, L353)
- `test_used_view_preferred_over_all_view` (function, L371)
- `test_all_view_fallback_dropped_when_used_data_exists_elsewhere` (function, L383)
- `test_discipline_tied_rows_not_summed_within_client` (function, L398)
- `test_domain_rollup_total_matches_bucket_sum` (function, L410)
- `test_unclassified_reuse_routed_separately` (function, L426)
- `test_min_enterprise_clients_downgrade` (function, L435)
- `test_generic_reference_does_not_seed` (function, L446)
- `test_baseline_threshold_gate_overrides_scope_gap` (function, L453)
- `test_semantic_noise_filter_routes_separately` (function, L464)
- `test_semantic_noise_filter_disabled_by_default` (function, L475)
- `test_rank_is_ordinal_per_domain` (function, L482)
- `test_no_bare_numeric_score_in_any_output` (function, L489)
- `test_routing_buckets_are_mutually_exclusive` (function, L501)

### `tests/test_arrowheads_shape_gating.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:load_join_key_policies,get_domain_join_key_policy`
- `core.record_v2:ITEM_Q_OK,ITEM_Q_MISSING,make_identity_item,serialize_identity_items`
- `domains.arrowheads:_build_common_identity_items,_build_arrow_identity_items,_build_tick_identity_items,_get_arrowhead_style`
- `pytest`

**Definitions**
- `test_style_discriminator_first` (function, L20)
- `test_style_specific_keys_are_omitted_when_not_applicable` (function, L31)
- `test_no_missing_for_unrelated_style_properties` (function, L50)
- `test_join_key_builder_additional_required_only_for_shape` (function, L69)
- `test_get_arrowhead_style_fallback` (function, L87)
- `test_join_key_builder_other_style` (function, L95)
- `test_join_key_keys_used_and_hash_for_arrow_style` (function, L112)

### `tests/test_build_segment_manifest.py`

**Imports**
- `__future__:annotations`
- `build_segment_manifest:_build_segments,_build_registry,_population_hash,_normalize_rows,_validate_required_metadata,_build_membership_rows,_membership_by_segment,DIMENSION_CONFIG,REQUIRED_ROW_FIELDS,MANIFEST_FIELDNAMES,REGISTRY_FIELDNAMES,main`
- `build_segment_manifest:_sanitize_folder`
- `build_segment_manifest:_sanitize_folder`
- `build_segment_manifest:_sanitize_folder`
- `csv`
- `hashlib`
- `pathlib:Path`
- `pytest`
- `random`
- `sys`

**Definitions**
- `_meta_row` (function, L33)
- `_full_row` (function, L44)
- `_read_csv` (function, L70)
- `_membership_ids` (function, L75)
- `test_population_hash_deterministic` (function, L99)
- `test_blank_unit_system_excluded` (function, L108)
- `test_level1_segments_present` (function, L114)
- `test_level1_run_type_skip_when_below_min_files` (function, L121)
- `test_level1_run_type_bundle_at_min_files` (function, L129)
- `test_level1_file_counts` (function, L136)
- `test_level2_segments_present` (function, L143)
- `test_level2_run_type_below_min` (function, L152)
- `test_level2_run_type_at_min` (function, L158)
- `test_seed_detection_level2` (function, L164)
- `test_seed_detection_renown_no_seed` (function, L171)
- `test_seed_detection_container_role` (function, L178)
- `test_level1_parent_is_empty` (function, L185)
- `test_level2_parent_is_unit_system` (function, L192)
- `test_sort_order_level1_before_level2` (function, L199)
- `test_sort_order_within_level_alphabetical` (function, L205)
- `test_export_run_ids_sorted_pipe_delimited` (function, L213)
- `test_membership_rows_no_pipe_delimited_values` (function, L224)
- `test_manifest_and_registry_have_no_list_columns` (function, L235)
- `test_population_hash_in_manifest` (function, L245)
- `test_registry_excludes_skip_segments` (function, L252)
- `test_registry_output_folder_sanitized` (function, L259)
- `test_sanitize_folder_strips_path_separators` (function, L266)
- `test_sanitize_folder_preserves_selected_blank_vs_unselected_dimension` (function, L276)
- `test_sanitize_folder_renders_selected_blank_as_neutral_token` (function, L296)
- `test_registry_output_folders_globally_unique_with_suffix_collision` (function, L313)
- `test_registry_distinguishes_selected_blank_client_from_unselected_client_pool` (function, L334)
- `test_registry_initial_status_pending` (function, L357)
- `test_main_writes_files` (function, L369)
- `test_seed_only_note_not_set_for_generic_only_segment` (function, L397)
- `test_seed_only_note_not_suppressed_by_blank_eid_project_row` (function, L407)
- `test_seed_only_note_set_when_segment_has_seeds_no_project` (function, L427)
- `test_registry_folder_merges_for_client_label_case_variants` (function, L440)
- `test_blank_client_label_no_longer_participates_in_subset` (function, L459)
- `test_main_missing_metadata_file` (function, L473)
- `test_main_fails_on_missing_required_columns` (function, L478)
- `test_main_fails_when_export_run_id_column_absent` (function, L493)
- `test_main_blocks_on_blank_export_run_id` (function, L505)
- `test_main_fails_on_missing_columns_even_with_no_data_rows` (function, L528)
- `test_level2_project_bundle_with_parent_bundle_runs_enabled` (function, L541)
- `test_level2_project_registration_without_flag` (function, L551)
- `test_mixed_role_client_segment_stays_reference` (function, L561)
- `test_single_child_suppression_still_fires` (function, L576)
- `_disc_rows` (function, L588)
- `test_discipline_cut_level3_segment_generated` (function, L599)
- `test_discipline_cut_level4_segment_generated` (function, L606)
- `test_discipline_cut_extra_dimensions_populated` (function, L613)
- `test_discipline_label_top_level_field_blank_for_non_discipline_segments` (function, L621)
- `test_discipline_label_top_level_field_populated_in_mixed_cut` (function, L631)
- `test_discipline_cut_level3_purpose` (function, L638)
- `test_discipline_cut_level3_label` (function, L646)
- `test_blank_discipline_does_not_generate_discipline_cut` (function, L652)
- `test_no_discipline_column_rows_not_broken` (function, L663)
- `test_discipline_cut_not_required_column_now_blocks` (function, L675)
- `test_discipline_cut_level3_bundle_not_demoted_by_children` (function, L699)
- `test_discipline_cut_level4_bundle_not_affected` (function, L710)
- `test_multi_child_parent_not_demoted_redundant_single_child` (function, L721)
- `test_single_child_same_hash_still_demoted` (function, L732)
- `test_matching_child_demotes_parent_even_with_other_nonmatching_children` (function, L742)
- `test_client_discipline_leaf_purpose_container` (function, L780)
- `test_client_discipline_leaf_label_container` (function, L786)
- `test_client_discipline_leaf_purpose_template` (function, L792)
- `test_client_discipline_leaf_purpose_project` (function, L803)
- `test_registry_first_run_no_existing_file_unaffected` (function, L818)
- `test_registry_preserves_output_folder_across_runs_when_unchanged` (function, L833)
- `test_registry_preserves_status_when_population_hash_unchanged` (function, L842)
- `test_registry_resets_status_when_population_hash_changes` (function, L857)
- `test_registry_new_segment_gets_unique_folder_not_colliding_with_carryover` (function, L879)
- `_manifest_row` (function, L912)
- `test_registry_resets_status_when_run_type_changes` (function, L922)
- `test_registry_reserves_dropped_segment_folder_from_new_reuse` (function, L954)
- `test_registry_drops_removed_segment_ids_with_warning` (function, L975)
- `test_client_discipline_leaf_no_empty_purpose` (function, L993)
- `test_unit_system_case_variants_merge_into_single_segment` (function, L1020)
- `test_governance_role_case_variants_merge_and_no_false_warning` (function, L1035)
- `test_unknown_governance_role_still_warns_after_normalization_added` (function, L1058)
- `test_client_label_first_seen_casing_is_canonical` (function, L1076)
- `test_business_center_label_zero_padded_short_digit_values` (function, L1094)
- `test_business_center_label_already_four_digits_unaffected` (function, L1110)
- `test_business_center_label_non_numeric_unaffected` (function, L1118)
- `test_business_center_label_zero_pad_merges_with_correctly_formatted_rows` (function, L1128)
- `test_business_center_label_zero_pad_warning_emitted` (function, L1144)
- `test_normalization_warning_emitted_with_aggregate_count` (function, L1165)
- `test_clean_corpus_unaffected_by_normalization` (function, L1186)
- `test_conformance_reference_mode_defaults_to_latest_for_new_segment` (function, L1207)
- `test_conformance_reference_mode_carried_over_across_runs` (function, L1214)
- `test_conformance_reference_mode_defaults_to_latest_for_old_registry_missing_field` (function, L1222)
- `test_registry_no_longer_carries_export_run_ids` (function, L1234)
- `test_registry_new_files_reason_when_file_added` (function, L1245)
- `test_registry_removed_files_reason_when_file_removed` (function, L1260)
- `test_registry_both_new_and_removed_files_reasons_when_combined_change` (function, L1279)
- `test_registry_new_files_reason_does_not_cause_false_removal_warnings` (function, L1298)
- `test_registry_no_reason_notes_for_brand_new_segment` (function, L1320)
- `test_segment_membership_round_trip_reconstructs_in_memory_sets` (function, L1341)
- `test_segment_membership_join_keys_present_in_manifest_and_metadata` (function, L1375)
- `test_population_hash_unchanged_by_membership_storage_migration` (function, L1398)
- `test_manifest_and_registry_fields_stay_under_size_threshold` (function, L1418)
- `test_enterprise_bc_0000_preserved_literally_not_folded_to_blank` (function, L1459)
- `test_enterprise_identity_not_inferred_from_blank_business_center` (function, L1478)
- `test_business_center_case_variants_of_0000_still_fold_by_casing_not_bookkeeping` (function, L1491)
- `test_collection_label_ignored_same_segments_same_membership` (function, L1510)
- `test_collection_label_column_absence_produces_identical_manifest` (function, L1533)
- `test_collection_label_column_absence_produces_identical_manifest._write_and_build` (method, L1542)
- `_write_metadata_csv` (function, L1572)
- `test_required_field_blank_blocks_entire_build` (function, L1581)
- `test_required_field_na_sentinel_blocks_entire_build` (function, L1603)
- `test_required_field_semicolon_blocks_entire_build` (function, L1621)
- `test_export_run_id_semicolon_does_not_block_build` (function, L1644)
- `test_validate_required_metadata_reports_row_and_field_directly` (function, L1662)
- `test_validate_required_metadata_empty_for_fully_valid_rows` (function, L1674)
- `test_duplicate_export_run_id_blocks_as_distinct_conflict_reason` (function, L1678)
- `test_unreadable_input_reported_distinctly_not_bare_except` (function, L1695)
- `test_business_center_0000_is_a_valid_value_not_a_validation_failure` (function, L1718)
- `test_business_center_0000_main_succeeds` (function, L1723)
- `test_project_label_not_a_required_field` (function, L1741)
- `test_project_label_sentinel_does_not_affect_segmentation` (function, L1746)
- `test_project_label_sentinel_does_not_affect_segmentation._build_with_project_label` (method, L1754)
- `test_running_builder_twice_on_identical_input_is_byte_identical` (function, L1778)
- `test_reordering_input_rows_does_not_change_segment_ids_or_parents` (function, L1791)
- `test_former_collection_specific_rows_collapse_with_union_membership` (function, L1821)
- `test_ancestor_segment_ids_semicolon_joined_not_pipe` (function, L1850)
- `test_ancestor_segment_ids_two_element_roundtrip` (function, L1880)

### `tests/test_bundle_analysis_name_projection.py`

**Imports**
- `__future__:annotations`
- `core.name_key_coverage:COVERAGE_NATIVE`
- `csv`
- `filecmp`
- `pathlib:Path`
- `pytest`
- `tools.bundle_analysis.common:read_csv_rows,retry_fs_op`
- `tools.bundle_analysis.name_projection_adapter:DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,NAME_TARGET_COMBINED_FILES,PROVENANCE_NOTE_NAME_TARGET,annotate_name_target_combined_files,normalize_export_run_id,emit_name_target_provenance,stage_name_projection_analysis_dir`
- `tools.bundle_analysis.run_bundle_analysis`
- `tools.bundle_analysis.run_bundle_analysis`
- `tools.bundle_analysis.run_bundle_analysis`
- `tools.bundle_analysis.run_bundle_analysis`
- `tools.bundle_analysis.run_bundle_analysis`
- `tools.bundle_analysis.run_bundle_analysis`
- `tools.bundle_analysis.run_bundle_analysis:VALID_COMPARISON_TARGETS,_validate_name_target_constraints,run_bundle_analysis_for_target`
- `tools.bundle_analysis.step1_membership_matrix:build_membership_matrix`
- `tools.bundle_analysis.step2_find_bundles:find_bundles_for_domain`
- `tools.generate_name_key_patterns:emit_name_patterns`
- `tools.run_segment_orchestrator`

**Definitions**
- `_write_csv` (function, L49)
- `_materials_name_key_rows` (function, L58)
- `_build_pr2_name_patterns_dir` (function, L76)
- `TestValidationBlocksUnsupportedFeatures` (class, L84)
- `TestValidationBlocksUnsupportedFeatures.test_used_view_blocked` (method, L86)
- `TestValidationBlocksUnsupportedFeatures.test_both_purge_view_blocked` (method, L91)
- `TestValidationBlocksUnsupportedFeatures.test_share_profile_blocked` (method, L96)
- `TestValidationBlocksUnsupportedFeatures.test_compare_blocked` (method, L101)
- `TestValidationBlocksUnsupportedFeatures.test_config_target_never_blocked` (method, L105)
- `TestValidationBlocksUnsupportedFeatures.test_name_target_all_view_no_extras_passes` (method, L109)
- `TestConfigPassthroughUnchanged` (class, L113)
- `TestConfigPassthroughUnchanged.test_config_target_calls_run_bundle_analysis_with_unchanged_out_dir` (method, L114)
- `TestConfigPassthroughUnchanged.test_config_target_calls_run_bundle_analysis_with_unchanged_out_dir._fake_run_bundle_analysis` (method, L119)
- `TestConfigPassthroughUnchanged.test_both_target_nests_config_output_under_config_subdir` (method, L136)
- `TestConfigPassthroughUnchanged.test_both_target_nests_config_output_under_config_subdir._fake_run_bundle_analysis` (method, L141)
- `TestPurgeViewDefaultIsTargetAware` (class, L167)
- `TestPurgeViewDefaultIsTargetAware._capture` (method, L172)
- `TestPurgeViewDefaultIsTargetAware._capture._fake_run_bundle_analysis` (method, L177)
- `TestPurgeViewDefaultIsTargetAware.test_name_target_without_explicit_purge_view_does_not_raise` (method, L192)
- `TestPurgeViewDefaultIsTargetAware.test_both_target_without_explicit_purge_view_does_not_raise` (method, L202)
- `TestPurgeViewDefaultIsTargetAware.test_name_target_with_explicit_used_still_raises` (method, L211)
- `TestPurgeViewDefaultIsTargetAware.test_config_target_without_explicit_purge_view_still_defaults_to_both` (method, L219)
- `TestPurgeViewDefaultIsTargetAware.test_cli_purge_view_default_is_none` (method, L227)
- `TestSplitExportFileIdNormalization` (class, L236)
- `TestSplitExportFileIdNormalization.test_details_filename_normalized_to_index_filename` (method, L244)
- `TestSplitExportFileIdNormalization.test_details_filename_normalization_is_case_insensitive_on_suffix` (method, L247)
- `TestSplitExportFileIdNormalization.test_index_filename_left_unchanged` (method, L250)
- `TestSplitExportFileIdNormalization.test_plain_filename_left_unchanged` (method, L253)
- `TestSplitExportFileIdNormalization.test_staged_presence_rows_use_index_export_run_id_for_split_export` (method, L256)
- `TestNormalizeExportRunIdWithKnownIds` (class, L278)
- `TestNormalizeExportRunIdWithKnownIds.test_split_export_resolves_to_normalized_form` (method, L284)
- `TestNormalizeExportRunIdWithKnownIds.test_details_only_export_resolves_to_raw_form` (method, L288)
- `TestNormalizeExportRunIdWithKnownIds.test_neither_form_known_falls_back_to_normalized_guess` (method, L292)
- `TestNormalizeExportRunIdWithKnownIds.test_no_known_ids_is_unchanged_blind_rewrite` (method, L296)
- `TestNormalizeExportRunIdWithKnownIds.test_index_and_plain_names_unaffected_by_known_ids` (method, L301)
- `TestStageWithKnownExportRunIds` (class, L306)
- `TestStageWithKnownExportRunIds.test_details_only_export_stages_with_raw_id_when_known` (method, L307)
- `TestStageWithKnownExportRunIds.test_without_known_export_run_ids_details_only_export_is_wrongly_normalized` (method, L327)
- `TestNameProjectionAdapterProducesConsumableInput` (class, L347)
- `TestNameProjectionAdapterProducesConsumableInput.test_staged_input_is_consumed_by_unmodified_step1_step2_and_forms_a_bundle` (method, L348)
- `TestNameProjectionAdapterProducesConsumableInput.test_staging_is_deterministic` (method, L371)
- `TestBundleProvenance` (class, L382)
- `TestBundleProvenance._run_pipeline` (method, L383)
- `TestBundleProvenance.test_every_bundle_declares_comparison_target_and_coverage_class` (method, L392)
- `TestBundleProvenance.test_excluded_domains_stated_explicitly_in_readme_and_coverage_csv` (method, L406)
- `TestBundleProvenance.test_determinism_of_provenance_output` (method, L419)
- `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile` (class, L428)
- `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile.test_metadata_file_resolves_details_only_export_correctly` (method, L433)
- `TestRunBundleAnalysisForTargetResolvesDetailsOnlyIdsFromMetadataFile.test_without_metadata_file_still_falls_back_to_blind_normalize` (method, L463)
- `TestNameAllOutputLocation` (class, L487)
- `TestNameAllOutputLocation._run` (method, L494)
- `TestNameAllOutputLocation.test_name_all_is_flat_single_segment_under_out_dir` (method, L507)
- `TestNameAllOutputLocation.test_provenance_and_coverage_and_readme_relocated_alongside_bundle_output` (method, L514)
- `TestNameAllOutputLocation.test_staging_input_remains_under_internal_name_dir_not_relocated` (method, L521)
- `TestNameAllOutputLocation.test_rerun_against_same_out_dir_self_clears_stale_name_all` (method, L526)
- `TestNameAllOutputLocation.test_config_target_output_untouched_by_relocation` (method, L549)
- `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure` (class, L565)
- `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure.test_stale_name_all_removed_even_when_mining_raises` (method, L573)
- `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure.test_stale_name_all_removed_even_when_mining_raises._raises` (method, L582)
- `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure.test_stale_name_all_removed_even_when_staging_raises` (method, L599)
- `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure.test_stale_name_all_removed_even_when_staging_raises._raises` (method, L607)
- `TestStaleNameAllClearedBeforeRegenerationEvenOnFailure.test_successful_run_still_repopulates_name_all_normally` (method, L624)
- `TestAnnotateNameTargetCombinedFiles` (class, L641)
- `TestAnnotateNameTargetCombinedFiles.test_adds_three_columns_after_existing_header_and_looks_up_coverage_class` (method, L647)
- `TestAnnotateNameTargetCombinedFiles.test_missing_files_are_skipped_without_error` (method, L677)
- `TestAnnotateNameTargetCombinedFiles.test_idempotent_second_call_leaves_already_annotated_file_unchanged` (method, L683)
- `TestAnnotateNameTargetCombinedFiles.test_excluded_domain_row_still_annotated_with_its_own_coverage_class` (method, L699)
- `TestAnnotateNameTargetCombinedFiles.test_covers_all_ten_bi_merge_filenames` (method, L710)
- `TestRetryFsOp` (class, L719)
- `TestRetryFsOp.test_succeeds_on_first_try_without_retry` (method, L727)
- `TestRetryFsOp.test_recovers_after_transient_failures` (method, L732)
- `TestRetryFsOp.test_recovers_after_transient_failures.flaky` (method, L735)
- `TestRetryFsOp.test_reraises_after_exhausting_attempts` (method, L744)
- `TestRetryFsOp.test_reraises_after_exhausting_attempts.always_fails` (method, L745)
- `TestRetryFsOp.test_passes_through_positional_args` (method, L751)
- `TestRetryFsOp.test_non_os_error_is_not_retried` (method, L757)
- `TestRetryFsOp.test_non_os_error_is_not_retried.raises_value_error` (method, L760)

### `tests/test_bundle_pattern_classification_roles.py`

**Imports**
- `__future__:annotations`
- `csv`
- `pathlib:Path`
- `tools.bundle_analysis.step5_classify_patterns:emit_stub`

**Definitions**
- `_write_csv` (function, L9)
- `test_emit_stub_classifies_root_to_leaf_patterns_as_differentiating` (function, L17)

### `tests/test_canonical_items_migration.py`

**Imports**
- `core.canonical_items:build_flat_items,compile_role_policy,merge_legacy_buckets,resolve_item_roles`

**Definitions**
- `test_merge_legacy_buckets_to_flat_items_equivalence_and_dedupe` (function, L9)
- `test_build_flat_items_preserves_counts_for_unique_keys` (function, L22)
- `test_merge_legacy_buckets_preserves_existing_canonical_items` (function, L29)
- `test_compile_and_resolve_roles_runtime_from_key_only` (function, L37)
- `test_compile_role_policy_skips_scalar_string_for_role_keys` (function, L60)
- `test_compile_role_policy_accepts_top_level_domains_wrapper` (function, L73)

### `tests/test_collect.py`

**Imports**
- `core.collect:CollectCtx,_is_invalid_element_id,_safe_unique_id,_make_query_key,collect_id_ints`
- `importlib`
- `importlib`
- `importlib`
- `pytest`

**Definitions**
- `TestCollectCtx` (class, L18)
- `TestCollectCtx.test_inc_initializes_counter` (method, L19)
- `TestCollectCtx.test_inc_accumulates` (method, L24)
- `TestCollectCtx.test_inc_multiple_keys` (method, L30)
- `TestCollectCtx.test_inc_coerces_to_int` (method, L36)
- `TestCollectCtx.test_default_fields` (method, L42)
- `TestIsInvalidElementId` (class, L53)
- `TestIsInvalidElementId.test_none_is_invalid` (method, L54)
- `TestIsInvalidElementId.test_object_without_integer_value_is_invalid` (method, L57)
- `TestIsInvalidElementId.test_negative_integer_value_is_invalid` (method, L61)
- `TestIsInvalidElementId.test_negative_integer_value_is_invalid.FakeId` (class, L62)
- `TestIsInvalidElementId.test_zero_is_invalid` (method, L66)
- `TestIsInvalidElementId.test_zero_is_invalid.FakeId` (class, L68)
- `TestIsInvalidElementId.test_positive_integer_value_is_valid` (method, L77)
- `TestIsInvalidElementId.test_positive_integer_value_is_valid.FakeId` (class, L78)
- `TestIsInvalidElementId.test_large_positive_is_valid` (method, L82)
- `TestIsInvalidElementId.test_large_positive_is_valid.FakeId` (class, L83)
- `TestIsInvalidElementId.test_integer_value_none_is_invalid` (method, L87)
- `TestIsInvalidElementId.test_integer_value_none_is_invalid.FakeId` (class, L88)
- `TestSafeUniqueId` (class, L97)
- `TestSafeUniqueId.test_none_object` (method, L98)
- `TestSafeUniqueId.test_no_unique_id_attr` (method, L101)
- `TestSafeUniqueId.test_unique_id_none` (method, L104)
- `TestSafeUniqueId.test_unique_id_none.Fake` (class, L105)
- `TestSafeUniqueId.test_unique_id_empty` (method, L109)
- `TestSafeUniqueId.test_unique_id_empty.Fake` (class, L110)
- `TestSafeUniqueId.test_unique_id_whitespace` (method, L114)
- `TestSafeUniqueId.test_unique_id_whitespace.Fake` (class, L115)
- `TestSafeUniqueId.test_unique_id_valid` (method, L119)
- `TestSafeUniqueId.test_unique_id_valid.Fake` (class, L120)
- `TestSafeUniqueId.test_unique_id_coerced_to_str` (method, L124)
- `TestSafeUniqueId.test_unique_id_coerced_to_str.Fake` (class, L125)
- `TestMakeQueryKey` (class, L134)
- `TestMakeQueryKey.test_basic_key` (method, L135)
- `TestMakeQueryKey.test_class_name_extracted` (method, L145)
- `TestMakeQueryKey.test_class_name_extracted.FakeClass` (class, L146)
- `TestMakeQueryKey.test_category_as_int` (method, L157)
- `TestMakeQueryKey.test_where_key_included` (method, L167)
- `TestMakeQueryKey.test_same_inputs_same_key` (method, L177)
- `TestCollectIdIntsNoRevit` (class, L187)
- `TestCollectIdIntsNoRevit.test_no_revit_raises` (method, L188)
- `TestCollectIdIntsNoRevit.test_none_doc_raises` (method, L193)
- `TestCollectIdIntsNoRevit.test_cache_bypass_for_unkeyed_predicate` (method, L199)
- `test_build_purgeable_id_set_ok` (function, L211)
- `test_build_purgeable_id_set_ok.FakeId` (class, L215)
- `test_build_purgeable_id_set_ok.FakeId.__init__` (method, L216)
- `test_build_purgeable_id_set_ok.FakeDoc` (class, L218)
- `test_build_purgeable_id_set_ok.FakeDoc.GetUnusedElements` (method, L219)
- `test_build_purgeable_id_set_failure` (function, L230)
- `test_build_purgeable_id_set_failure.BadDoc` (class, L234)
- `test_build_purgeable_id_set_failure.BadDoc.GetUnusedElements` (method, L235)
- `test_build_purgeable_id_set_uses_cache` (function, L246)
- `test_build_purgeable_id_set_uses_cache.TrackingDoc` (class, L251)
- `test_build_purgeable_id_set_uses_cache.TrackingDoc.GetUnusedElements` (method, L252)

### `tests/test_compare_cross_segment_cardinality.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment`
- `compare_cross_segment:compare_directed_file,compare_symmetric_file,run_pair,run_pooled_comparison,_segment_domain_source_status`
- `csv`
- `enterprise_policy:load_enterprise_policy`
- `pathlib:Path`
- `sys`

**Definitions**
- `_write_csv` (function, L34)
- `_write_segment` (function, L43)
- `_manifest_entry` (function, L63)
- `_registry_entry` (function, L70)
- `_clear_caches` (function, L74)
- `test_one_by_one_comparison_is_ok_and_populates_union_and_pairwise` (function, L84)
- `test_one_by_twenty_comparison_is_degraded_single_a` (function, L112)
- `test_three_by_twenty_comparison_n_pairs_not_used_for_status` (function, L141)
- `test_union_metrics_stable_pairwise_mean_shifts_under_duplication` (function, L168)
- `test_directed_reference_heterogeneity_core_share_below_one` (function, L216)
- `test_directed_single_file_reference_produces_normal_output` (function, L237)
- `test_zero_files_on_either_side_is_blocked_not_zero_valued` (function, L265)
- `test_empty_domain_and_unreadable_segment_get_different_inventory_status` (function, L309)
- `test_union_containment_does_not_track_file_count_ratio_like_pairwise_mean` (function, L359)
- `test_single_file_side_is_never_blocked` (function, L399)
- `test_pooled_comparison_schedules_pool_only_domain_for_empty_focal` (function, L426)
- `test_pooled_comparison_skips_when_lineage_filtering_empties_the_pool` (function, L464)
- `test_blocked_row_preserves_populated_side_bundle_availability` (function, L503)
- `test_pooled_blocked_row_preserves_pool_bundle_availability` (function, L543)

### `tests/test_compare_cross_segment_comparison_registry.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:COMPARISON_REGISTRY_FIELDS,atomic_write_csv,build_comparison_registry_rows,comparison_is_stale,load_comparison_registry`
- `enterprise_policy:load_enterprise_policy`
- `pathlib:Path`
- `sys`

**Definitions**
- `_reg_row` (function, L21)
- `test_comparison_is_stale_when_never_computed` (function, L30)
- `test_comparison_not_stale_when_both_sides_unchanged` (function, L35)
- `test_comparison_stale_when_reference_side_population_changed` (function, L46)
- `test_comparison_stale_when_target_side_population_changed` (function, L61)
- `test_comparison_stale_when_forced_rerun_changes_last_run_utc_without_population_change` (function, L72)
- `test_comparison_staleness_is_isolated_per_domain` (function, L85)
- `test_build_comparison_registry_rows_stamps_completed_work_items` (function, L99)
- `test_build_comparison_registry_rows_is_a_full_snapshot_no_carryover` (function, L117)
- `test_build_comparison_registry_rows_domain_scoped_run_omits_other_domains` (function, L132)
- `test_build_comparison_registry_rows_omits_work_items_with_no_output` (function, L161)
- `test_build_comparison_registry_rows_omits_pair_when_reference_segment_is_pending` (function, L174)
- `test_build_comparison_registry_rows_omits_pair_when_target_segment_is_failed` (function, L193)
- `test_build_comparison_registry_rows_stamps_when_both_sides_complete` (function, L203)
- `test_load_comparison_registry_roundtrip` (function, L213)
- `test_load_comparison_registry_missing_file_returns_empty` (function, L228)

### `tests/test_compare_cross_segment_cross_client.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:_build_summary_row,_is_client_only_project_segment,_redundant_child_segment_id,_resolve_runnable_segment,_scope_override_key,discover_cross_client,discover_parent_siblings,discover_sibling_segments,drop_legacy_siblings_covered_by_peer_comparisons`
- `enterprise_policy:load_enterprise_policy`
- `pathlib:Path`
- `sys`

**Definitions**
- `_summary_row` (function, L37)
- `_seg` (function, L52)
- `test_is_client_only_project_segment_true_for_bare_client_scope` (function, L82)
- `test_is_client_only_project_segment_false_for_non_project_role` (function, L86)
- `test_is_client_only_project_segment_false_when_client_blank` (function, L90)
- `test_is_client_only_project_segment_true_when_further_scoped_by_discipline` (function, L95)
- `test_is_client_only_project_segment_false_when_further_scoped_by_bc` (function, L104)
- `test_is_client_only_project_segment_false_when_further_scoped_by_collection` (function, L110)
- `test_discover_cross_client_pairs_distinct_clients_same_unit` (function, L120)
- `test_discover_cross_client_no_pair_across_different_unit_systems` (function, L130)
- `test_discover_cross_client_discipline_scoped_segment_does_not_mix_with_broader_grain` (function, L138)
- `test_discover_cross_client_matching_discipline_peers_do_pair` (function, L153)
- `test_discover_cross_client_discipline_mismatch_produces_no_pair` (function, L166)
- `test_discover_cross_client_excludes_non_project_roles` (function, L176)
- `test_discover_cross_client_excludes_registration_only_segments` (function, L184)
- `test_discover_cross_client_three_clients_produces_all_pairs` (function, L192)
- `test_discover_cross_client_no_self_pair_or_reverse_duplicate` (function, L202)
- `test_drops_sibling_projects_when_same_pair_covered_by_cross_client` (function, L216)
- `test_drop_legacy_sibling_projects_is_order_independent` (function, L231)
- `test_drop_legacy_sibling_projects_leaves_uncovered_pairs_untouched` (function, L240)
- `test_drop_legacy_sibling_projects_leaves_other_types_untouched` (function, L254)
- `test_drop_legacy_sibling_projects_noop_when_no_cross_client_rows` (function, L266)
- `test_segment_filter_before_drop_preserves_reversed_orientation_pair` (function, L271)
- `test_drops_sibling_templates_when_same_pair_covered_by_bc_to_bc` (function, L310)
- `test_drops_sibling_containers_when_same_pair_covered_by_bc_to_bc` (function, L325)
- `test_drops_sibling_projects_when_same_pair_covered_by_client_cross_bc` (function, L334)
- `test_drop_leaves_sibling_generic_and_sibling_segments_untouched_when_uncovered` (function, L348)
- `test_redundant_child_segment_id_extracts_pointer` (function, L371)
- `test_redundant_child_segment_id_survives_pipe_in_segment_id_and_prior_notes` (function, L377)
- `test_redundant_child_segment_id_none_when_no_marker` (function, L386)
- `test_resolve_runnable_segment_returns_self_when_already_eligible` (function, L390)
- `test_resolve_runnable_segment_follows_single_hop` (function, L395)
- `test_resolve_runnable_segment_follows_multi_hop_chain` (function, L404)
- `test_resolve_runnable_segment_none_when_chain_dead_ends` (function, L416)
- `test_resolve_runnable_segment_none_when_no_pointer_and_ineligible` (function, L423)
- `test_resolve_runnable_segment_guards_against_cycle` (function, L428)
- `test_discover_cross_client_rescues_single_bc_client_via_redundant_pointer` (function, L440)
- `test_discover_cross_client_no_rescue_when_pointed_to_child_also_ineligible` (function, L455)
- `test_discover_sibling_segments_rescues_single_bc_client_under_shared_parent` (function, L469)
- `test_discover_parent_siblings_rescues_single_bc_template_rollup` (function, L488)
- `test_discover_parent_siblings_does_not_misclassify_blank_role_rollup` (function, L501)
- `test_discover_cross_client_rescued_pair_reports_original_blank_scope` (function, L537)
- `test_discover_cross_client_override_does_not_leak_into_other_comparison_types` (function, L554)
- `test_discover_cross_client_no_override_when_no_resolution_needed` (function, L571)
- `test_discover_sibling_segments_rescued_pair_reports_original_scope` (function, L583)
- `test_discover_parent_siblings_rescued_pair_reports_resolved_descendants_true_scope` (function, L602)

### `tests/test_compare_cross_segment_governance.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment`
- `compare_cross_segment:_bc_of,_classify_governance_state,_comparison_role_semantics,_normalize_bc_label,_recommended_primary_view,_scope_level,_usage_interpretable_for_role,REUSE_BUCKET_THRESHOLDS,_reuse_bucket_for,build_pair_domain_work_items,build_pattern_reuse_distribution_rows,build_union_inventory_rows,deduplicate_pairs,discover_client_cross_bc,discover_domains_for_segment,discover_governance_chain,discover_sibling_segments,discover_within_project,drop_legacy_siblings_covered_by_peer_comparisons,load_file_join_hashes,main,make_comparison_run_id,run_pooled_comparison,sort_pair_detail_rows,sort_summary_rows`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_explicit_matrix_outputs`
- `compare_cross_segment:build_governance_state_outputs`
- `compare_cross_segment:build_governance_state_outputs`
- `csv`
- `csv`
- `csv`
- `enterprise_policy:load_enterprise_policy`
- `pathlib:Path`
- `sys`

**Definitions**
- `_seg` (function, L41)
- `test_discover_governance_chain_includes_generic_upstream_roles` (function, L51)
- `test_discover_governance_chain_falls_back_to_collection_label_for_na_client` (function, L71)
- `test_discover_governance_chain_prefers_business_center_label_over_collection_label` (function, L90)
- `test_discover_governance_chain_namespaces_business_center_fallback_from_real_client` (function, L123)
- `test_discover_governance_chain_preserves_collection_scope_within_business_center` (function, L153)
- `test_discover_governance_chain_final_fallback_normalizes_na_spelling` (function, L207)
- `test_discover_governance_chain_collection_match_is_soft_for_client_scope` (function, L226)
- `test_discover_governance_chain_rollup_does_not_wildcard_match_specific_collection` (function, L261)
- `test_scope_level_derivation` (function, L312)
- `test_0000_flows_through_as_literal_enterprise_value` (function, L327)
- `test_bc_0000_spelling_variants_canonicalize_to_0000` (function, L337)
- `test_na_spelled_business_center_labels_normalize_to_blank` (function, L350)
- `test_discover_governance_chain_enterprise_to_project_reaches_every_scope` (function, L363)
- `test_discover_governance_chain_bc_to_project_scoped_to_matching_bc_only` (function, L380)
- `test_discover_governance_chain_enterprise_to_bc_and_client_are_same_role_only` (function, L397)
- `test_enterprise_to_bc_and_sibling_template_survive_with_distinct_run_ids` (function, L420)
- `test_make_comparison_run_id_differs_by_comparison_type_for_same_pair_and_timestamp` (function, L465)
- `test_discover_governance_chain_excludes_generic_from_scope_fanout` (function, L474)
- `test_discover_governance_chain_excludes_ancestor_descendant_from_scope_fanout` (function, L489)
- `test_discover_governance_chain_enterprise_to_bc_reaches_every_real_bc` (function, L530)
- `test_discover_governance_chain_bc_to_bc_pairs_every_peer_business_center` (function, L546)
- `test_discover_governance_chain_bc_to_bc_excludes_same_bc_and_enterprise` (function, L560)
- `test_discover_governance_chain_disc_match_has_no_blank_wildcard` (function, L575)
- `test_discover_client_cross_bc_multi_bc_enumeration` (function, L594)
- `test_discover_client_cross_bc_single_bc_produces_no_pairs` (function, L608)
- `test_discover_client_cross_bc_and_bc_to_bc_do_not_reference_collection_label` (function, L617)
- `test_pooled_comparison_bc_scope_pools_across_clients_ignoring_client` (function, L646)
- `test_pooled_comparison_bc_scope_pools_enterprise_0000_segments` (function, L682)
- `test_pooled_comparison_client_scope_pools_across_bcs_ignoring_bc` (function, L729)
- `test_pooled_comparison_excludes_rollup_ancestor_from_bc_pool` (function, L765)
- `test_project_target_governance_state_uses_target_used` (function, L824)
- `test_standards_carrier_target_avoids_passive_bloat_label` (function, L837)
- `_write_csv` (function, L849)
- `_write_segment` (function, L861)
- `_write_reference_analysis_segment` (function, L884)
- `test_reference_analysis_segment_discovers_domains_without_bundle_outputs` (function, L930)
- `test_reference_analysis_segment_loads_all_view_from_domain_patterns` (function, L946)
- `test_reference_analysis_segment_groups_fallback_by_export_run_id_column` (function, L971)
- `test_reference_analysis_segment_uses_presence_for_multi_file_fallback` (function, L993)
- `test_build_governance_state_rows_include_inherited_unused_and_local_active` (function, L1019)
- `test_pair_domain_work_items_use_pair_domain_union` (function, L1090)
- `test_output_row_sort_helpers_are_stable_by_content` (function, L1117)
- `test_non_project_target_blanks_used_summary_shares` (function, L1147)
- `test_main_emits_governance_states_when_pair_skipped_by_min_patterns` (function, L1219)
- `test_main_skips_delta_generation_for_blocked_reference` (function, L1329)
- `_union_rows_for` (function, L1434)
- `test_union_inventory_project_all_view_normalized_union` (function, L1449)
- `test_union_inventory_project_used_view_normalized_union` (function, L1474)
- `test_union_inventory_non_project_used_view_not_active_usage` (function, L1500)
- `test_union_inventory_duplicate_join_hash_collapses_counts` (function, L1521)
- `test_union_inventory_pattern_id_not_cross_segment_identity` (function, L1562)
- `test_union_inventory_missing_source_cluster_status_no_synthetic_pattern` (function, L1597)
- `test_union_inventory_output_order_is_deterministic` (function, L1618)
- `test_union_inventory_used_view_unavailable_keeps_source_status_ok` (function, L1651)
- `test_union_inventory_client_denominator_includes_status_rows_used_by_reuse` (function, L1696)
- `test_union_inventory_missing_domain_patterns_keeps_source_status_ok` (function, L1746)
- `test_pattern_reuse_many_files_gets_broad_classification` (function, L1758)
- `test_pattern_reuse_one_file_gets_single_file_classification` (function, L1777)
- `test_project_used_view_uses_project_and_file_denominators_for_emerging_bucket` (function, L1795)
- `test_single_project_reuse_takes_precedence_over_emerging` (function, L1815)
- `test_missing_source_identity_degrades_reuse_classification` (function, L1834)
- `test_template_all_view_is_not_interpreted_as_active_usage` (function, L1855)
- `test_reuse_zero_denominator_is_degraded_unclassified` (function, L1873)
- `test_reuse_distribution_order_is_deterministic` (function, L1883)
- `test_reuse_thresholds_are_centralized_and_used` (function, L1903)
- `test_explicit_matrices_union_jaccard_differs_from_mean_file_pair` (function, L1914)
- `test_fragmentation_diagnostic_uses_all_domains_file_pair_aggregate` (function, L1944)
- `test_density_similarity_uses_domain_density_vectors_not_containment` (function, L1966)
- `test_pool_matrix_keeps_pool_scopes_distinct_for_same_project` (function, L1985)
- `test_fragmentation_diagnostic_unavailable_without_required_inputs` (function, L2027)
- `test_non_project_union_inventory_blocks_project_union_matrices` (function, L2047)
- `test_mean_file_pair_matrix_adds_synthetic_diagonal_cells` (function, L2074)
- `test_mean_file_pair_diagonals_limited_to_project_observed_domains` (function, L2095)
- `test_mean_file_pair_matrix_emits_symmetric_cells` (function, L2112)
- `test_missing_union_inventory_blocks_union_matrix_with_explicit_status` (function, L2132)
- `test_matrix_manifest_and_diagonal_are_deterministic` (function, L2142)
- `_write_within_project_segment` (function, L2158)
- `test_discover_within_project_na_spellings_do_not_group` (function, L2166)
- `test_discover_within_project_real_shared_label_still_groups` (function, L2196)

### `tests/test_compare_cross_segment_lineage.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:_build_ancestor_map,_compute_containment_thresholds,_is_lineage_related,_is_population_contained,_population_containment_map,detect_stale_ancestor_encoding,discover_sibling_segments,main,validate_membership_against_manifest`
- `csv`
- `enterprise_policy:load_enterprise_policy`
- `hashlib`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `_lattice_manifest` (function, L46)
- `_lattice_manifest.row` (method, L59)
- `test_build_ancestor_map_full_lattice_closure` (function, L77)
- `test_build_ancestor_map_superset_of_single_parent_chain` (function, L93)
- `test_is_lineage_related_symmetric_across_full_closure` (function, L105)
- `test_build_ancestor_map_cycle_detection_still_fires` (function, L116)
- `_pop` (function, L132)
- `_pop_hash` (function, L136)
- `test_population_containment_above_and_below_materiality_bar` (function, L140)
- `test_population_containment_excludes_structural_pairs_from_threshold_fit_but_still_flags_them` (function, L182)
- `test_population_containment_identical_populations_always_contained` (function, L217)
- `test_population_containment_boundary_value_included_not_excluded` (function, L237)
- `test_compute_containment_thresholds_deterministic` (function, L272)
- `test_compute_containment_thresholds_no_non_structural_pairs` (function, L286)
- `test_compute_containment_thresholds_empty_membership` (function, L299)
- `_sibling_row` (function, L311)
- `_real_corpus_shaped_manifest` (function, L325)
- `test_discover_sibling_segments_pre_fix_reproduces_violation` (function, L356)
- `test_discover_sibling_segments_post_fix_excludes_violation` (function, L364)
- `test_discover_sibling_segments_unrelated_siblings_still_pair` (function, L377)
- `test_discover_sibling_segments_backward_compatible_without_ancestor_data` (function, L392)
- `test_validate_membership_against_manifest_agreement_no_errors` (function, L414)
- `test_validate_membership_against_manifest_file_count_mismatch` (function, L420)
- `test_validate_membership_against_manifest_population_hash_mismatch` (function, L428)
- `test_validate_membership_against_manifest_unknown_segment_ignored` (function, L436)
- `test_validate_membership_against_manifest_entirely_missing_segment` (function, L444)
- `test_validate_membership_against_manifest_zero_file_count_segment_not_flagged` (function, L460)
- `test_detect_stale_ancestor_encoding_flags_pipe_joined_blob` (function, L474)
- `test_detect_stale_ancestor_encoding_does_not_flag_wellformed_semicolon_data` (function, L487)
- `test_detect_stale_ancestor_encoding_does_not_flag_genuine_single_ancestor` (function, L492)
- `test_validate_membership_against_manifest_completely_empty_sidecar` (function, L502)
- `_write_csv` (function, L526)
- `test_main_removes_stale_thresholds_when_containment_disabled` (function, L534)

### `tests/test_compare_cross_segment_streamed_pairs.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment`
- `compare_cross_segment:main,PAIRS_FIELDS`
- `csv`
- `enterprise_policy:load_enterprise_policy`
- `pathlib:Path`
- `sys`

**Definitions**
- `_write_csv` (function, L21)
- `_write_segment` (function, L29)
- `test_main_streams_real_file_pair_rows_to_disk` (function, L52)
- `_build_sibling_fixture` (function, L152)
- `test_failure_after_streaming_leaves_previous_pairs_file_untouched` (function, L208)
- `test_failure_after_streaming_leaves_previous_pairs_file_untouched._boom` (method, L237)

### `tests/test_compare_cross_segment_worker_count.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment`
- `compare_cross_segment`
- `compare_cross_segment`
- `compare_cross_segment`
- `compare_cross_segment`
- `compare_cross_segment:resolve_worker_count`
- `pathlib:Path`
- `sys`

**Definitions**
- `test_auto_derives_from_cpu_count` (function, L11)
- `test_auto_never_returns_zero_on_low_core_count` (function, L17)
- `test_auto_with_no_cpu_count_falls_back_to_four` (function, L23)
- `test_auto_is_case_insensitive_and_trims_whitespace` (function, L29)
- `test_explicit_string_int_is_parsed` (function, L33)
- `test_explicit_int_passthrough` (function, L37)
- `test_auto_caps_at_61_on_windows` (function, L41)
- `test_auto_uncapped_on_non_windows` (function, L48)

### `tests/test_compare_governance_populations.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `sys`
- `tools.compare_governance_populations:discover_same_role_peer_pairs,discover_directed_tc_to_project_pairs,discover_generic_pairs,run_comparisons`
- `tools.enterprise_policy:load_enterprise_policy`
- `tools.governance_manifest:build_governance_populations`

**Definitions**
- `_row` (function, L24)
- `_synthetic_manifest` (function, L35)
- `_records_rows` (function, L55)
- `test_same_role_peer_produces_expected_comparison_type_set` (function, L75)
- `test_same_role_peer_excludes_project_and_generic` (function, L82)
- `test_same_role_peer_excludes_project_scoped_template_or_container` (function, L92)
- `test_comparison_type_still_unambiguous_with_project_scoped_template` (function, L110)
- `test_generic_pairs_unconditionally_against_every_tc_project_population` (function, L131)
- `test_directed_enterprise_to_project_is_unconditional_on_scope` (function, L140)
- `test_directed_bc_to_project_matches_by_business_center_label_alone` (function, L152)
- `test_directed_bc_to_project_matches_regardless_of_differing_client` (function, L165)
- `test_directed_client_to_project_matches_by_client_label_alone` (function, L187)
- `test_files_with_no_inventory_for_domain_are_excluded_not_zero_padded` (function, L206)
- `test_zero_inventory_domain_produces_no_row` (function, L233)
- `test_comparison_type_never_mixes_symmetric_and_directed_metric_shape` (function, L251)
- `test_run_comparisons_end_to_end_type_coverage` (function, L268)

### `tests/test_compound_types_wall.py`

**Imports**
- `domains:compound_layers`
- `importlib`

**Definitions**
- `_Id` (class, L8)
- `_Id.__init__` (method, L9)
- `_MatElem` (class, L13)
- `_MatElem.__init__` (method, L14)
- `_FillPatternDef` (class, L18)
- `_FillPatternDef.__init__` (method, L19)
- `_FillPatternElem` (class, L23)
- `_FillPatternElem.__init__` (method, L24)
- `_FillPatternElem.GetFillPattern` (method, L28)
- `_Param` (class, L32)
- `_Param.__init__` (method, L33)
- `_Param.AsElementId` (method, L37)
- `_Param.AsInteger` (method, L40)
- `_Layer` (class, L44)
- `_Layer.__init__` (method, L45)
- `_LayerWidthError` (class, L63)
- `_LayerWidthError.__init__` (method, L64)
- `_LayerWidthError.Width` (method, L81)
- `_CS` (class, L85)
- `_CS.__init__` (method, L86)
- `_CS.GetLayers` (method, L94)
- `_CS.GetCoreBoundaryLayerIndex` (method, L97)
- `_CS.ParticipatesInWrapping` (method, L102)
- `_CS.GetWallSweepsInfo` (method, L105)
- `_CSWrapError` (class, L109)
- `_CSWrapError.__init__` (method, L110)
- `_CSWrapError.WrapAtInserts` (method, L117)
- `_CSWrapError.WrapAtEnds` (method, L121)
- `_WallType` (class, L125)
- `_WallType.__init__` (method, L126)
- `_WallType.Function` (method, L142)
- `_WallType.GetCompoundStructure` (method, L147)
- `_WallType.get_Parameter` (method, L150)
- `_ParamString` (class, L161)
- `_ParamString.__init__` (method, L162)
- `_ParamString.AsString` (method, L165)
- `_Doc` (class, L169)
- `_Doc.__init__` (method, L170)
- `_Doc.GetElement` (method, L174)
- `_setup_module` (function, L183)
- `_default_ctx` (function, L202)
- `_basic_wall` (function, L212)
- `test_basic_wall_produces_record` (function, L222)
- `test_instance_count_present_on_wall_record` (function, L237)
- `test_is_sole_type_in_category_true_when_single_wall` (function, L249)
- `test_is_sole_type_in_category_false_when_multiple_walls` (function, L260)
- `test_instance_count_not_in_identity_basis` (function, L272)
- `test_is_sole_type_not_in_identity_basis` (function, L283)
- `test_non_basic_wall_produces_blocked_record` (function, L294)
- `test_core_boundary_in_layer_rows` (function, L313)
- `test_layer_count_excludes_core_boundaries` (function, L324)
- `test_stack_hash_loose_excludes_material_name` (function, L334)
- `test_stack_hash_loose_excludes_material_name.get` (method, L349)
- `test_stack_hash_strict_includes_material_name` (function, L355)
- `test_stack_hash_order_sensitive` (function, L372)
- `test_material_ctx_miss_emits_missing_sentinel` (function, L385)
- `test_coarse_fill_invalid_id_uses_producer_no_pattern_symbol` (function, L396)
- `test_coarse_fill_uses_producer_mapping_for_solid_and_uid_hash` (function, L406)
- `test_type_name_not_in_sig_hash` (function, L433)
- `test_layer_rows_attached_to_record` (function, L441)
- `test_identity_items_sorted_and_sig_basis_declared` (function, L451)
- `test_label_has_quality_provenance_and_components` (function, L469)
- `test_stack_hash_preserves_zero_vs_unreadable_thickness` (function, L481)
- `test_unreadable_function_does_not_block_record` (function, L500)
- `test_unreadable_layer_width_blocks_required_total_thickness` (function, L519)
- `test_no_compound_structure_blocked_record_includes_required_keys` (function, L536)
- `test_type_name_fallback_to_all_model_type_name` (function, L550)
- `test_unreadable_wrap_fields_do_not_block_required_identity` (function, L559)
- `test_mixed_ok_and_blocked_records_keep_domain_hash_from_ok_records` (function, L576)

### `tests/test_contracts_bounded_errors.py`

**Imports**
- `core.contracts:add_bounded_error,new_run_diag`

**Definitions**
- `test_bounded_errors_caps_and_counts_dropped` (function, L6)
- `test_bounded_errors_defensive_cap_nonpositive` (function, L25)

### `tests/test_contracts_run_status.py`

**Imports**
- `core.contracts:SCHEMA_VERSION,compute_run_status,new_domain_envelope,new_run_diag,new_run_envelope,RUN_STATUS_OK,RUN_STATUS_DEGRADED,RUN_STATUS_FAILED,DOMAIN_STATUS_OK,DOMAIN_STATUS_DEGRADED,DOMAIN_STATUS_BLOCKED,DOMAIN_STATUS_FAILED,DOMAIN_STATUS_UNSUPPORTED`
- `pytest`

**Definitions**
- `_env` (function, L22)
- `test_run_status_all_ok` (function, L34)
- `test_run_status_degraded_if_any_degraded` (function, L45)
- `test_run_status_degraded_if_any_blocked` (function, L54)
- `test_run_status_degraded_if_any_unsupported` (function, L63)
- `test_run_status_failed_if_any_failed` (function, L72)
- `test_invalid_domain_status_counts_as_failed_and_records_error` (function, L82)
- `test_new_run_envelope_rejects_mismatched_version` (function, L94)
- `test_new_run_envelope_accepts_current_version` (function, L106)

### `tests/test_deployment_config.py`

**Imports**
- `core.deployment_config:load_deployment_config`
- `json`
- `pytest`

**Definitions**
- `_write` (function, L14)
- `test_no_configuration_is_empty` (function, L25)
- `test_valid_name_only_configuration` (function, L29)
- `test_valid_guid_is_canonicalized` (function, L36)
- `test_rejects_non_object_top_level` (function, L45)
- `test_rejects_missing_or_invalid_schema` (function, L56)
- `test_rejects_non_list_mapping` (function, L64)
- `test_rejects_invalid_mapping_entries` (function, L79)
- `test_rejects_duplicate_guid_mapped_to_conflicting_keys` (function, L84)
- `test_rejects_unknown_top_level_field` (function, L97)
- `test_rejects_missing_mapping_field` (function, L102)
- `test_missing_contract_fails` (function, L109)
- `test_malformed_contract_fails` (function, L115)

### `tests/test_deps_require_domain.py`

**Imports**
- `core.deps:Blocked,require_domain`
- `core:contracts`
- `pytest`

**Definitions**
- `test_require_domain_missing_upstream_blocks` (function, L9)
- `test_require_domain_invalid_envelope_blocks` (function, L18)
- `test_require_domain_upstream_not_acceptable_blocks` (function, L26)
- `test_require_domain_allows_degraded_by_default` (function, L45)

### `tests/test_dimension_types_placeholder_fields.py`

**Imports**
- `domains.dimension_types`

**Definitions**
- `test_attach_placeholder_metadata_ok` (function, L4)
- `test_attach_placeholder_metadata_unreadable` (function, L11)

### `tests/test_dimension_types_shape_gating.py`

**Imports**
- `core.dimension_type_helpers:SHAPE_LINEAR,SHAPE_ANGULAR,SHAPE_RADIAL,SHAPE_DIAMETER,SHAPE_ARC_LENGTH,SHAPE_SPOT_ELEVATION,SHAPE_SPOT_COORDINATE,SHAPE_SPOT_SLOPE,SHAPE_LINEAR_FIXED,SHAPE_SPOT_ELEVATION_FIXED,SHAPE_DIAMETER_LINKED,SHAPE_UNKNOWN,FAMILY_LINEAR,FAMILY_RADIAL,FAMILY_ANGULAR,FAMILY_SPOT,FAMILY_UNKNOWN,SHAPE_TO_FAMILY,SHAPE_INT_TO_NAME`
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:load_join_key_policies,get_domain_join_key_policy`
- `core.record_v2:ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,make_identity_item,serialize_identity_items`
- `pytest`

**Definitions**
- `TestShapeConstants` (class, L62)
- `TestShapeConstants.test_all_shape_constants_defined` (method, L65)
- `TestShapeConstants.test_all_family_constants_defined` (method, L80)
- `TestShapeConstants.test_shape_to_family_mapping_complete` (method, L88)
- `TestShapeConstants.test_shape_int_to_name_mapping` (method, L112)
- `TestFamilyMappings` (class, L131)
- `TestFamilyMappings.test_linear_shapes_map_to_linear_family` (method, L134)
- `TestFamilyMappings.test_radial_shapes_map_to_radial_family` (method, L139)
- `TestFamilyMappings.test_angular_shapes_map_to_angular_family` (method, L145)
- `TestFamilyMappings.test_spot_shapes_map_to_spot_family` (method, L150)
- `TestFamilyMappings.test_unknown_maps_to_unknown_family` (method, L157)
- `TestSplitDomainPolicies` (class, L166)
- `TestSplitDomainPolicies._load_policy` (method, L169)
- `TestSplitDomainPolicies.test_linear_policy_has_witness_line_control` (method, L174)
- `TestSplitDomainPolicies.test_radial_policy_has_center_marks` (method, L182)
- `TestSplitDomainPolicies.test_angular_policy_has_unit_format_id` (method, L189)
- `TestSplitDomainPolicies.test_diameter_policy_exists` (method, L195)
- `TestSplitDomainPolicies.test_spot_elevation_policy_exists` (method, L201)
- `TestSplitDomainPolicies.test_spot_coordinate_policy_exists` (method, L207)
- `TestSplitDomainPolicies.test_spot_slope_policy_exists` (method, L213)
- `TestSplitDomainPolicies.test_all_split_domains_have_schemas` (method, L219)
- `TestPolicyLoadPattern` (class, L241)
- `TestPolicyLoadPattern.test_policy_load_integration_pattern` (method, L244)
- `TestCanonicalEvidenceSelectors` (class, L249)
- `TestCanonicalEvidenceSelectors.test_linear_join_key_uses_required_keys_only` (method, L252)

### `tests/test_discover_hash_policy.py`

**Imports**
- `__future__:annotations`
- `csv`
- `pathlib:Path`
- `subprocess`
- `sys`

**Definitions**
- `_write_csv` (function, L6)
- `test_discover_hash_policy_join_and_sig` (function, L12)
- `test_validate_marks_blocked_when_required_fields_missing_from_selected` (function, L35)
- `test_validate_pareto_auto_bumps_max_k_to_required_count` (function, L58)
- `test_phase0_dir_can_be_results_root` (function, L85)
- `test_phase0_dir_auto_resolves_results_records` (function, L106)
- `test_phase0_dir_auto_resolves_records_subfolder` (function, L122)
- `test_out_policy_creates_parent_directories` (function, L138)
- `test_loaded_family_types_skips_orphan_gate_buckets` (function, L154)
- `test_loaded_family_types_surfaces_missing_shape_gate_records` (function, L175)
- `test_stratify_by_limits_overrepresentation` (function, L196)

### `tests/test_discover_join_policy_verification.py`

**Imports**
- `__future__:annotations`
- `csv`
- `json`
- `pathlib:Path`
- `pytest`
- `subprocess`
- `sys`
- `tools.discover_join_policy:_diagnostics_domain_suffix,_full_population_verify,_rank_all,_stratified_sample`
- `tools.join_key_discovery.eval:build_identity_index,score_candidate`
- `tools.join_key_discovery.greedy:discover_greedy`

**Definitions**
- `_write_csv` (function, L13)
- `_items_row` (function, L25)
- `test_full_population_verify_detects_fragmentation_sample_missed` (function, L29)
- `test_full_population_verify_no_divergence_when_full_matches_sample` (function, L49)
- `test_full_population_verify_flags_collision_rate_delta_above_threshold` (function, L65)
- `test_full_population_verify_flags_coverage_collapse_even_with_zero_collision_and_fragmentation` (function, L85)
- `test_full_population_verify_no_divergence_for_coverage_drop_within_threshold` (function, L108)
- `test_stratified_sample_by_file_id_balances_across_files` (function, L130)
- `test_stratified_sample_by_file_id_is_deterministic` (function, L141)
- `test_stratified_sample_falls_back_to_flat_when_key_uncovered` (function, L148)
- `test_stratified_sample_survivors_are_not_lexicographically_first_group_when_groups_exceed_cap` (function, L155)
- `test_stratified_sample_group_selection_varies_by_seed_when_groups_exceed_cap` (function, L168)
- `test_stratified_sample_does_not_starve_records_missing_the_stratifier` (function, L177)
- `test_stratified_sample_ungrouped_stratum_is_deterministic` (function, L191)
- `test_full_verify_columns_present_by_default` (function, L203)
- `test_no_full_verify_flag_skips_verification` (function, L229)
- `test_stratify_by_file_id_end_to_end` (function, L249)
- `test_discover_greedy_seeds_selected_with_required_fields` (function, L278)
- `test_discover_greedy_without_required_fields_behaves_as_before` (function, L293)
- `test_discover_greedy_required_seed_still_scores_challengers` (function, L306)
- `test_full_population_verify_uses_same_effective_gates_as_greedy_search` (function, L334)
- `test_full_population_verify_stays_consistent_with_fixed_pareto_search` (function, L377)
- `test_out_policy_excludes_candidate_that_diverges_on_full_population` (function, L422)
- `test_validate_mode_blocked_when_required_field_absent_from_data` (function, L469)
- `test_validate_mode_not_blocked_when_required_field_ranked_below_candidate_cap` (function, L497)
- `test_stratified_sample_tops_up_from_ungrouped_remainder_when_groups_are_small` (function, L545)
- `test_rank_all_is_deterministic_full_sort` (function, L552)
- `test_diagnostics_domain_suffix_empty_when_unscoped` (function, L567)
- `test_diagnostics_domain_suffix_short_domain_list` (function, L571)
- `test_diagnostics_domain_suffix_falls_back_to_hash_for_long_lists` (function, L576)
- `test_diagnostics_domain_suffix_includes_policy_modes_to_avoid_split_run_collisions` (function, L583)
- `test_discover_join_policy_scoped_run_does_not_clobber_unscoped_filenames` (function, L596)

### `tests/test_discover_vfd_edges.py`

**Imports**
- `csv`
- `importlib.util`
- `importlib.util`
- `json`
- `pathlib:Path`
- `subprocess`
- `sys`

**Definitions**
- `read_csv` (function, L11)
- `test_discover_vfd_edges_resolves_builtin_and_groups_edge` (function, L16)
- `test_discover_vfd_edges_without_shared_names_keeps_guid_out_of_edges` (function, L71)
- `test_discover_vfd_edges_filters_hint_comments_and_exact_bip_lookup` (function, L111)
- `test_generated_dynamic_edges_include_category_id_for_reference_graph` (function, L172)
- `test_discover_vfd_edges_keeps_same_name_param_categories_separate` (function, L249)
- `test_discover_vfd_edges_applies_threshold_after_category_aggregation` (function, L300)
- `test_discover_vfd_edges_emits_multi_domain_conflict_rows` (function, L345)
- `test_discover_vfd_edges_gaps_multi_domain_identity_items_missing` (function, L399)
- `test_discover_vfd_edges_skips_edges_without_category_scope` (function, L451)
- `test_discover_vfd_edges_ignores_unusable_category_rows` (function, L492)
- `test_discover_vfd_edges_ignores_unusable_param_ref_rows_with_item_quality` (function, L535)
- `test_discover_vfd_edges_category_file_count_controls_generator_threshold` (function, L577)
- `_write_unresolved_guid_inputs` (function, L639)
- `test_dump_unresolved_files_writes_csv_and_summary` (function, L664)
- `test_dump_unresolved_files_sort_order` (function, L740)
- `test_dump_unresolved_files_requires_file_metadata` (function, L792)
- `test_without_dump_unresolved_files_behavior_unchanged` (function, L824)

### `tests/test_dynamo_bootstrap.py`

**Imports**
- `mapping:_dynamo_bootstrap`
- `os`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `test_looks_like_repo_root_true_for_real_checkout` (function, L29)
- `test_looks_like_repo_root_false_for_bogus_path` (function, L33)
- `test_resolve_repo_root_explicit_valid_wins_over_env` (function, L37)
- `test_resolve_repo_root_explicit_bad_raises_not_silently_falls_back` (function, L46)
- `test_resolve_repo_root_env_var_priority` (function, L57)
- `test_resolve_repo_root_falls_back_to_module_file` (function, L69)
- `test_resolve_repo_root_raises_when_nothing_resolves` (function, L77)
- `test_purge_repo_modules_removes_matching_prefixes_only` (function, L90)
- `test_purge_repo_modules_custom_prefixes` (function, L111)
- `test_promote_on_sys_path_inserts_at_front` (function, L128)
- `test_promote_on_sys_path_moves_existing_entry_to_front` (function, L141)
- `test_bootstrap_without_revit_references_returns_repo_root` (function, L159)
- `test_bootstrap_purges_stale_modules_before_promoting` (function, L168)

### `tests/test_enterprise_policy.py`

**Imports**
- `json`
- `pickle`
- `pytest`
- `tools.enterprise_policy:DEFAULT_ENTERPRISE_LABEL,load_enterprise_policy,write_enterprise_policy_provenance`

**Definitions**
- `test_default_enterprise_label_is_synthetic` (function, L7)
- `test_policy_file_and_cli_precedence` (function, L11)
- `test_blank_override_rejected` (function, L19)
- `test_policy_provenance_records_effective_configuration` (function, L23)
- `test_policy_instances_are_immutable_serializable_and_do_not_leak_state` (function, L36)
- `test_malformed_schema_and_invalid_bookkeeping_token_are_rejected` (function, L47)
- `test_policy_file_path_is_memory_only_provenance_is_safe` (function, L57)

### `tests/test_export_bundle_pattern_detail_quality_resolution.py`

**Imports**
- `csv`
- `export_bundle_pattern_detail:_iter_identity_csv`
- `pathlib:Path`
- `sys`

**Definitions**
- `_write_csv` (function, L27)
- `test_v21_schema_reads_quality_from_item_value_type_when_role_blank` (function, L34)
- `test_v21_schema_ignores_item_role_even_when_populated` (function, L52)
- `test_legacy_kvq_schema_unaffected` (function, L72)

### `tests/test_export_layer_stacks.py`

**Imports**
- `__future__:annotations`
- `csv`
- `json`
- `pathlib:Path`
- `pytest`
- `sys`
- `sys`
- `tools.export_to_flat_tables:main`
- `typing:Any,Dict,List`

**Definitions**
- `_write_fingerprint` (function, L19)
- `_make_layer_row` (function, L23)
- `_make_wall_record` (function, L47)
- `_run_export` (function, L75)
- `_read_csv` (function, L93)
- `test_layer_stacks_basic` (function, L100)
- `test_layer_stacks_type_count_deduplication` (function, L146)
- `test_layer_stacks_multiple_domains` (function, L175)
- `test_layer_stacks_multiple_domains._make_floor_record` (method, L181)
- `test_layer_stacks_not_in_default_emit` (function, L214)
- `test_layer_stacks_split_by_domain` (function, L235)
- `test_layer_stacks_total_thickness_excludes_core_boundary` (function, L256)
- `test_layer_stacks_records_without_layer_rows_ignored` (function, L283)

### `tests/test_extractor_unit_system.py`

**Imports**
- `tools.extractor:_derive_unit_system`

**Definitions**
- `_length_record` (function, L4)
- `_legacy_length_record` (function, L16)
- `_payload` (function, L25)
- `test_accepts_plural_meters` (function, L29)
- `test_continues_after_unrecognized_or_missing_unit_type_id` (function, L34)
- `test_accepts_degraded_records` (function, L43)
- `test_broader_length_unit_matching` (function, L48)
- `test_falls_back_to_legacy_identity_basis_shape` (function, L55)

### `tests/test_fill_patterns_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `domains.fill_patterns:_export_fill_pattern_ctx`

**Definitions**
- `test_fill_patterns_drafting_join_key_uses_policy_required_keys_only` (function, L9)
- `test_fill_patterns_model_join_key_uses_policy_required_keys_only` (function, L46)
- `test_fill_pattern_ctx_contract_exports_specials_and_preserves_uid_map` (function, L74)

### `tests/test_fill_patterns_is_import_coercion.py`

**Imports**
- `importlib`

**Definitions**
- `_Id` (class, L16)
- `_Id.__init__` (method, L17)
- `_FillPatternDef` (class, L21)
- `_FillPatternDef.__init__` (method, L24)
- `_FillPatternElem` (class, L30)
- `_FillPatternElem.__init__` (method, L37)
- `_FillPatternElem.GetFillPattern` (method, L45)
- `_module` (function, L49)
- `_coordination_item` (function, L53)
- `test_extract_drafting_is_import_direct_attribute_true_is_string_not_bool` (function, L58)
- `test_extract_drafting_is_import_direct_attribute_false_is_string_not_bool` (function, L76)
- `test_extract_drafting_is_import_name_regex_match_is_string_not_bool` (function, L93)
- `test_extract_model_is_import_direct_attribute_is_string_not_bool` (function, L109)
- `test_extract_model_is_import_name_regex_match_is_string_not_bool` (function, L126)

### `tests/test_fingerprint_export_discovery.py`

**Imports**
- `__future__:annotations`
- `json`
- `pathlib:Path`
- `sys`
- `tools.extractor:_iter_export_files`
- `tools.patterns_analysis._archive.io:load_exports`
- `tools.run_extract_all:_detect_surfaces,_discover_domains_from_exports,_infer_domains,_pick_sample_file`

**Definitions**
- `_write_json` (function, L23)
- `_fingerprint_payload` (function, L27)
- `test_iter_export_files_prioritizes_fingerprint_and_uses_none_secondary` (function, L38)
- `test_pick_sample_file_prefers_fingerprint_and_falls_back_to_split` (function, L58)
- `test_detect_surfaces_counts_fingerprint_separately` (function, L82)
- `test_domain_discovery_prefers_fingerprint_candidates` (function, L93)
- `test_load_exports_prefers_fingerprint_files_before_plain_fallback` (function, L107)

### `tests/test_gen_map.py`

**Imports**
- `pathlib:Path`
- `tools:gen_map`

**Definitions**
- `test_generator_supports_arbitrary_layout` (function, L6)
- `test_trace_does_not_merge_duplicate_function_names` (function, L24)
- `test_parse_error_is_reported_without_stopping_other_files` (function, L36)

### `tests/test_generate_governance_narrative_bc_client_sections.py`

**Imports**
- `__future__:annotations`
- `generate_governance_narrative:render_bc_composition_section,render_client_bc_distribution_section`
- `pathlib:Path`
- `sys`

**Definitions**
- `_bc_client_row` (function, L17)
- `_client_bc_row` (function, L28)
- `test_bc_composition_section_absent_when_no_rows` (function, L38)
- `test_bc_composition_section_lists_clients_by_descending_share` (function, L42)
- `test_client_bc_distribution_section_absent_when_no_rows` (function, L55)
- `test_client_bc_distribution_section_renders_per_bc_rows_when_both_matrices_supplied` (function, L59)
- `test_client_bc_distribution_falls_back_to_business_centers_list_when_bc_matrix_missing` (function, L71)
- `test_client_bc_distribution_no_fallback_bullets_when_business_centers_blank` (function, L87)

### `tests/test_generate_governance_narrative_brief.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS,POOLED_FIELDS`
- `csv`
- `generate_governance_narrative:INTERPRETATION_GUIDE_PATH,QUESTION_ROUTES_PATH,INTERPRETATION_GUIDE_VERSION,QUESTION_ROUTES_VERSION,main,render_evidence_authority_header,render_file_inventory_brief_section,render_governance_brief`
- `json`
- `pathlib:Path`
- `sys`

**Definitions**
- `_finding` (function, L29)
- `test_brief_states_its_own_convenience_summary_role` (function, L53)
- `test_brief_points_at_interpretation_guide_and_question_routes` (function, L59)
- `test_brief_reports_package_health_and_corpus_counts` (function, L67)
- `test_brief_groups_findings_by_type_with_domain_label` (function, L73)
- `test_brief_omits_empty_sections` (function, L80)
- `test_brief_caps_long_lists_and_points_to_findings_json` (function, L86)
- `test_brief_lists_leadership_questions_as_numbered_list_not_findings` (function, L95)
- `test_brief_includes_low_client_coherence_section` (function, L106)
- `test_brief_does_not_recompute_only_consumes_passed_findings` (function, L113)
- `test_file_inventory_section_omitted_when_no_files` (function, L127)
- `test_file_inventory_section_lists_each_file_with_narrative` (function, L132)
- `test_brief_omits_file_inventory_section_when_absent` (function, L144)
- `test_brief_omits_file_inventory_section_when_files_list_empty` (function, L149)
- `test_brief_includes_file_inventory_section_when_files_present` (function, L154)
- `test_authority_header_points_to_brief_when_interpretation_layer_on` (function, L167)
- `test_authority_header_omits_brief_pointer_when_interpretation_layer_off` (function, L173)
- `test_authority_header_always_points_to_static_docs` (function, L179)
- `test_interpretation_guide_and_question_routes_docs_exist` (function, L195)
- `_summary_row` (function, L206)
- `_pooled_row` (function, L212)
- `_write_csv` (function, L218)
- `_minimal_fixture` (function, L225)
- `_run_main` (function, L252)
- `test_default_invocation_writes_governance_brief` (function, L257)
- `test_no_emit_interpretation_layer_suppresses_brief_but_not_findings` (function, L263)
- `test_no_emit_evidence_package_also_suppresses_brief` (function, L272)
- `test_stale_brief_removed_when_interpretation_layer_turned_off_between_runs` (function, L279)
- `test_stale_brief_removed_when_evidence_package_turned_off_between_runs` (function, L288)
- `test_manifest_records_governance_brief_output` (function, L297)
- `test_manifest_omits_governance_brief_output_when_layer_off` (function, L307)
- `test_evidence_map_governance_brief_present_true_by_default` (function, L316)
- `test_evidence_map_governance_brief_absent_when_layer_off` (function, L325)
- `test_evidence_map_static_docs_present_regardless_of_interpretation_layer_flag` (function, L335)
- `test_evidence_map_related_artifacts_reference_valid_ids_including_pr4` (function, L350)

### `tests/test_generate_governance_narrative_classification.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS,POOLED_FIELDS`
- `generate_governance_narrative:_DEFAULT_CLIENT_SECTOR_PATH,_client_onboarding_profile,_disc_label,build_cascade,build_client_summary,EXCLUDED_FROM_SCORING,load_client_sectors,normalise_summary_schema,read_csv,render_client_section,render_discipline_section`
- `pathlib:Path`
- `sys`

**Definitions**
- `_summary_row` (function, L29)
- `_pooled_row` (function, L35)
- `test_disc_label_uses_override_for_known_discipline` (function, L45)
- `test_disc_label_humanizes_unknown_discipline` (function, L49)
- `test_render_discipline_section_includes_disciplines_beyond_disc_labels` (function, L55)
- `test_load_client_sectors_empty_when_absent` (function, L81)
- `test_load_client_sectors_builds_map` (function, L86)
- `_client_fixture` (function, L94)
- `test_known_healthcare_client_is_flagged_healthcare` (function, L114)
- `test_known_non_healthcare_sector_gets_non_comparable_tier` (function, L123)
- `test_unclassified_client_falls_through_to_normal_tiering` (function, L132)
- `test_cascade_cross_client_jaccard_uses_sector_map` (function, L145)
- `test_default_client_sector_path_exists_and_loads` (function, L173)
- `test_unclassified_client_not_treated_as_confirmed_non_healthcare` (function, L183)
- `test_within_client_sibling_projects_excluded_from_cross_client_xc` (function, L215)
- `test_cascade_cross_client_requires_both_healthcare_like_sibling_projects` (function, L247)
- `test_cascade_cross_client_feeds_xc_when_both_sides_healthcare` (function, L271)
- `test_cascade_cross_client_and_sibling_projects_both_feed_xc` (function, L287)
- `test_cascade_cross_client_excludes_pair_with_one_non_healthcare_side` (function, L312)
- `test_build_client_summary_xc_mean_uses_cross_client_rows` (function, L336)
- `test_build_client_summary_xc_mean_uses_client_label_not_segment_id_shape` (function, L357)
- `test_build_client_summary_backfills_n_files_for_cross_client_only_clients` (function, L381)
- `test_within_client_cross_client_like_pair_excluded_from_xc_mean` (function, L405)
- `test_build_client_summary_excludes_confirmed_non_healthcare_partner_from_xc_mean` (function, L434)
- `test_build_client_summary_unclassified_partner_still_feeds_xc_mean` (function, L461)
- `test_build_client_summary_excludes_policy_excluded_domain_from_xc_mean` (function, L482)
- `test_non_project_within_project_rows_excluded_from_client_summary` (function, L517)
- `test_xc_mean_reads_used_union_jaccard_not_pairwise_mean` (function, L550)
- `test_wp_mean_reads_used_union_jaccard_and_exposes_all_view_companion` (function, L574)
- `test_cascade_xc_reads_used_union_jaccard_with_distinct_all_view_companion` (function, L594)
- `test_cascade_wp_all_and_wp_used_stay_a_true_all_used_pair_not_flipped` (function, L615)
- `test_cascade_wp_falls_back_to_pairwise_when_union_blank_real_producer_shape` (function, L646)
- `test_wp_by_client_falls_back_to_pairwise_when_union_blank_real_producer_shape` (function, L665)
- `test_disc_domain_wp_falls_back_to_pairwise_when_union_blank_real_producer_shape` (function, L683)
- `test_xc_does_not_fall_back_to_pairwise_when_union_blank` (function, L700)
- `test_disc_domain_wp_keeps_all_view_primary_for_non_project_within_project_rows` (function, L724)
- `test_disc_domain_wp_labels_non_project_discipline_as_all_view_not_active_practice` (function, L752)
- `test_disc_domain_wp_labels_mixed_project_and_non_project_discipline` (function, L773)

### `tests/test_generate_governance_narrative_comparison_registry.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:GOVERNANCE_STATE_FIELDS`
- `compare_cross_segment:GOVERNANCE_STATE_FIELDS`
- `compare_cross_segment:SUMMARY_FIELDS,POOLED_FIELDS,COMPARISON_REGISTRY_FIELDS,GOVERNANCE_STATE_SUMMARY_FIELDS`
- `csv`
- `generate_governance_narrative`
- `json`
- `pathlib:Path`
- `sys`

**Definitions**
- `_summary_row` (function, L22)
- `_pooled_row` (function, L28)
- `_registry_row` (function, L34)
- `_gov_state_summary_row` (function, L40)
- `_write_csv` (function, L46)
- `_minimal_fixture` (function, L53)
- `_run_main` (function, L80)
- `test_completeness_present_when_registry_row_matches` (function, L89)
- `test_completeness_missing_when_no_matching_registry_row` (function, L98)
- `test_completeness_stale_when_registry_computed_utc_predates_summary_executed_utc` (function, L105)
- `test_completeness_not_stale_when_registry_computed_utc_is_current` (function, L114)
- `test_completeness_counts_registry_only_entry_as_present_and_stale` (function, L123)
- `test_completeness_registry_only_entry_uses_registry_own_domain_for_grouping` (function, L135)
- `test_completeness_registry_only_entry_with_matching_state_evidence_is_not_stale` (function, L143)
- `test_completeness_registry_only_entry_without_state_evidence_is_still_stale` (function, L157)
- `test_completeness_registry_only_entry_with_state_evidence_is_stale_when_registry_predates_state` (function, L164)
- `test_completeness_registry_only_entry_with_state_evidence_not_stale_when_registry_is_current` (function, L179)
- `test_completeness_state_evidence_uses_newest_timestamp_across_both_state_sources` (function, L189)
- `test_completeness_state_evidence_uses_newest_timestamp_across_both_state_sources._gov_state_row` (method, L197)
- `test_completeness_state_evidence_from_detailed_rows_also_prevents_stale` (function, L216)
- `test_completeness_state_evidence_from_detailed_rows_also_prevents_stale._gov_state_row` (method, L219)
- `test_completeness_state_only_key_with_no_registry_or_summary_row_is_counted_missing` (function, L232)
- `test_completeness_summary_row_stale_check_also_considers_newer_state_evidence` (function, L246)
- `test_completeness_ignores_rows_with_no_domain` (function, L262)
- `test_completeness_registry_content_never_appears_in_result` (function, L268)
- `test_health_reports_comparison_registry_absent_when_not_supplied` (function, L284)
- `test_health_reports_comparison_registry_present_and_completeness_when_supplied` (function, L292)
- `test_health_overall_status_degrades_when_comparison_completeness_has_gaps` (function, L308)
- `test_health_overall_status_stays_complete_when_comparison_completeness_has_no_gaps` (function, L324)
- `test_evidence_map_comparison_registry_path_matches_explicit_flag` (function, L339)
- `test_narrative_omits_completeness_note_when_not_supplied` (function, L359)
- `test_narrative_includes_completeness_note_when_supplied_with_gap` (function, L366)
- `test_registry_content_never_reproduced_in_output_package` (function, L377)

### `tests/test_generate_governance_narrative_evidence_package.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS,POOLED_FIELDS,DELTA_FIELDS,GOVERNANCE_STATE_SUMMARY_FIELDS,COMPARISON_REGISTRY_FIELDS,REUSE_SUMMARY_FIELDS,MATRIX_OUTPUT_FIELDS,UNION_INVENTORY_FIELDS,MATRIX_MANIFEST_FIELDS`
- `csv`
- `generate_governance_narrative:CASCADE_GROUP1_TYPES,CASCADE_GROUP2_TYPES,CASCADE_GROUP3_TYPES,CASCADE_GROUP3B_TYPES,CASCADE_GROUP4_EXCLUDED_TYPES,EVIDENCE_MAP_SCHEMA_VERSION,_comparison_type_coverage,_DIRECTED_GOVERNANCE_TYPES,main,render_evidence_authority_header,render_limitations`
- `governance_evidence_package:GENERATOR_IDENTITY`
- `json`
- `pathlib:Path`
- `sys`

**Definitions**
- `_summary_row` (function, L40)
- `_pooled_row` (function, L46)
- `_delta_row` (function, L52)
- `_gov_state_summary_row` (function, L58)
- `_write_csv` (function, L64)
- `_minimal_fixture` (function, L71)
- `_run_main` (function, L98)
- `test_footer_references_real_generator_identity_not_stale_filename` (function, L107)
- `test_authority_header_states_controlled_interpretation_and_no_llm` (function, L118)
- `test_authority_header_inserted_between_header_and_state_model` (function, L126)
- `test_comparison_type_coverage_matches_known_cascade_groups` (function, L141)
- `test_bc_to_bc_and_client_cross_bc_are_registered_not_unrecognized` (function, L152)
- `test_comparison_type_coverage_governance_state_uses_directed_types` (function, L175)
- `test_unrecognized_comparison_type_still_warns_to_stderr` (function, L180)
- `test_domain_csv_column_set_unchanged` (function, L252)
- `test_client_csv_column_set_unchanged` (function, L260)
- `test_emit_evidence_package_default_writes_three_json_files` (function, L272)
- `test_no_emit_evidence_package_suppresses_json_but_not_existing_outputs` (function, L280)
- `test_no_emit_removes_stale_evidence_package_files_from_prior_run` (function, L292)
- `test_emit_and_no_emit_produce_identical_csvs` (function, L313)
- `test_no_emit_narrative_does_not_point_at_missing_package_files` (function, L329)
- `test_emit_narrative_points_at_package_files` (function, L347)
- `test_package_manifest_records_inputs_and_outputs` (function, L356)
- `test_package_manifest_reports_sibling_json_outputs_as_present_with_real_sizes` (function, L369)
- `test_package_manifest_records_comparison_run_ids` (function, L392)
- `test_package_manifest_comparison_run_ids_include_pooled_only_values` (function, L400)
- `test_package_manifest_comparison_run_ids_include_optional_evidence_values` (function, L434)
- `test_package_health_schema_detection_dual_for_dual_view_rows` (function, L469)
- `test_package_health_optional_inputs_present_reflects_cli_flags` (function, L478)
- `test_segment_manifest_recorded_in_evidence_package_when_supplied` (function, L489)
- `test_segment_manifest_absent_from_evidence_package_when_not_supplied` (function, L518)
- `test_evidence_map_lists_thirty_seven_artifacts_with_required_fields` (function, L531)
- `test_governance_relationships_resolved_beside_supplied_matrix_not_summary_dir` (function, L562)
- `test_pattern_reuse_summary_by_domain_resolved_beside_supplied_reuse_by_client_not_summary_dir` (function, L604)
- `test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_fragmentation_diagnostic_not_summary_dir` (function, L637)
- `test_pattern_reuse_summary_by_domain_resolved_beside_supplied_union_inventory_when_no_reuse_flag` (function, L673)
- `test_project_mean_file_pair_jaccard_matrix_resolved_beside_supplied_matrix_manifest_when_no_project_flag` (function, L703)
- `test_evidence_map_findings_entry_has_a_real_path` (function, L734)
- `test_manifest_output_artifact_ids_match_evidence_map_artifact_ids` (function, L750)
- `test_manifest_input_artifact_ids_match_evidence_map_artifact_ids` (function, L766)
- `test_evidence_map_related_artifacts_use_artifact_ids_not_filenames` (function, L784)
- `test_file_inventory_written_and_registered_in_manifest_and_evidence_map` (function, L806)
- `test_file_inventory_is_empty_when_no_undiscovered_files_present` (function, L818)
- `test_file_inventory_surfaces_an_undiscovered_sibling_csv` (function, L826)
- `test_file_inventory_never_flags_this_runs_own_outputs_as_undiscovered` (function, L852)
- `test_file_inventory_borrows_interpretation_from_matrix_output_manifest` (function, L866)
- `test_no_emit_evidence_package_suppresses_file_inventory` (function, L914)
- `test_stale_file_inventory_removed_when_evidence_package_turned_off_between_runs` (function, L921)
- `test_file_inventory_surfaces_regardless_of_interpretation_layer_flag` (function, L930)
- `test_escalation_target_files_get_real_shape_in_evidence_map_not_generic_inventory` (function, L957)
- `test_cli_accepts_policy_dir_and_package_schema_version_as_inert` (function, L1023)
- `test_package_schema_version_override_is_consistent_across_manifest_health_and_evidence_map` (function, L1036)

### `tests/test_generate_governance_narrative_findings.py`

**Imports**
- `__future__:annotations`
- `generate_governance_narrative:PASSIVE_INHERITANCE_RISK_DOMAINS,TIER_ACTIVE_LOCAL,TIER_BASELINE_CONTAINER_GAP,TIER_BASELINE_LOCAL_REVIEW,TIER_HIGH_FRAGMENTATION,TIER_STRONG_BASELINE,_TIER_DRIVER_SUPPORT_FIELDS,build_structured_findings,render_findings_and_recommendations`
- `governance_evidence_package:AUTHORITY_CONTROLLED_INTERPRETATION,AUTHORITY_CONVENIENCE_SUMMARY,FINDING_FIDELITY_EXACT,FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,FINDING_STATUS_QUESTION_NOT_CLAIM,FINDING_STATUS_SUPPORTED,FINDING_TYPES,build_findings_document`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `_min_domain_dict` (function, L46)
- `_client_row` (function, L57)
- `test_strong_baseline_candidate_finding` (function, L69)
- `test_baseline_candidate_without_strong_for_container_gap` (function, L90)
- `test_active_local_practice_and_local_review_required` (function, L107)
- `test_active_local_practice_finding_at_high_primary_containment` (function, L116)
- `test_local_review_required_via_passive_or_missing_share_lists_all_triggering_fields` (function, L140)
- `test_local_review_required_via_investigate_tier_lists_primary_containment_fields` (function, L162)
- `test_high_fragmentation_finding` (function, L180)
- `test_tier_based_findings_use_the_shared_driver_field_list` (function, L188)
- `test_missing_or_degraded_evidence_when_primary_metric_absent` (function, L220)
- `test_missing_or_degraded_evidence_for_sparse_tier_lists_primary_fields` (function, L246)
- `test_missing_or_degraded_evidence_lists_container_to_project_scoped_fields` (function, L265)
- `test_missing_or_degraded_evidence_not_emitted_for_non_renderable_domain` (function, L282)
- `test_cross_client_convergence_finding_independent_of_tier` (function, L293)
- `test_passive_inheritance_risk_finding_for_risk_domain_dual_schema` (function, L303)
- `test_passive_inheritance_risk_not_flagged_for_non_risk_domain` (function, L310)
- `test_passive_inheritance_risk_bundle_fallback_skipped_when_state_present_but_not_material` (function, L319)
- `test_passive_inheritance_risk_not_flagged_below_threshold` (function, L334)
- `test_passive_inheritance_risk_defers_to_clean_explicit_state_over_bundle_signal` (function, L342)
- `test_passive_inheritance_risk_flagged_from_explicit_state_when_material` (function, L358)
- `test_passive_inheritance_risk_finding_from_state_signal_without_bundle_data` (function, L373)
- `test_low_client_coherence_finding` (function, L391)
- `test_leadership_questions_are_questions_not_claims` (function, L400)
- `test_every_finding_has_provenance_and_limits` (function, L414)
- `test_findings_do_not_reference_nonexistent_artifact_ids` (function, L435)
- `test_finding_ids_are_unique_and_stable_order` (function, L445)
- `test_build_findings_document_wraps_with_schema_version` (function, L462)
- `test_build_findings_document_rejects_unknown_finding_type` (function, L469)
- `test_render_findings_uses_passed_in_findings_without_recomputing` (function, L479)
- `test_render_findings_defaults_to_computing_findings_when_none_passed` (function, L500)

### `tests/test_generate_governance_narrative_group1_bc_pooled.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS`
- `generate_governance_narrative:TIER_INSUFFICIENT,TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE,TIER_ORDER,_group1_scope_pair,_has_group1_bc_pooled_evidence,_has_renderable_cascade_signal,assign_tier,build_cascade,detect_anomalies,normalise_summary_schema,render_group1_scope_section`
- `pathlib:Path`
- `sys`

**Definitions**
- `_row` (function, L41)
- `test_tc_enterprise_slice_unchanged_by_bc_scoped_rows` (function, L51)
- `test_tp_by_scope_absent_when_no_group1_rows` (function, L83)
- `test_tp_bc_pooled_when_both_sides_same_bc_no_enterprise_pair` (function, L96)
- `test_group1_scope_pair_uses_both_sides_unlike_group2` (function, L120)
- `test_scope_pair_separator_does_not_collide_across_multi_dimension_labels` (function, L138)
- `test_group1_scope_pair_rejects_mismatched_bc_values` (function, L171)
- `test_group1_scope_pair_accepts_matching_bc_values` (function, L194)
- `test_group1_scope_pair_mismatched_client_bc_combo` (function, L206)
- `test_has_renderable_cascade_signal_true_for_scope_only_evidence` (function, L224)
- `test_has_renderable_cascade_signal_false_when_by_scope_dicts_all_empty` (function, L238)
- `test_scope_only_domain_reaches_bc_evidence_tier_in_full_pipeline` (function, L247)
- `test_cp_scoped_fallback_populated_when_n_files_sufficient_and_no_enterprise_pair` (function, L265)
- `test_cp_scoped_fallback_ignores_n_files_insufficient_rows` (function, L285)
- `test_cp_scoped_fallback_does_not_change_cp_by_scope_population` (function, L302)
- `test_cp_scoped_fallback_prefers_enterprise_pair_when_present` (function, L321)
- `test_cp_scoped_fallback_picks_bucket_with_most_rows` (function, L341)
- `test_domain_with_no_group1_rows_at_all_absent` (function, L367)
- `_bc_pooled_dict` (function, L389)
- `test_has_group1_bc_pooled_evidence_true_for_tp_bc_bc` (function, L401)
- `test_has_group1_bc_pooled_evidence_true_for_cp_bc_bc` (function, L406)
- `test_has_group1_bc_pooled_evidence_false_when_only_other_scope_pairs` (function, L411)
- `test_has_group1_bc_pooled_evidence_false_when_empty` (function, L418)
- `test_assign_tier_returns_bc_evidence_tier_when_primary_none_and_bc_pooled_present` (function, L423)
- `test_assign_tier_still_returns_insufficient_when_no_bc_pooled_evidence` (function, L428)
- `test_assign_tier_enterprise_primary_path_unaffected_by_bc_data` (function, L435)
- `test_tier_order_places_new_tier_between_high_fragmentation_and_insufficient` (function, L446)
- `_group1_rows` (function, L454)
- `test_detect_anomalies_flags_material_bc_divergence` (function, L469)
- `test_detect_anomalies_silent_when_bc_pairs_agree` (function, L479)
- `test_detect_anomalies_silent_when_only_one_bc_pair` (function, L487)
- `test_render_group1_scope_section_includes_bc_bc_row` (function, L508)
- `test_render_group1_scope_section_empty_when_only_enterprise_pair` (function, L517)
- `test_render_group1_scope_section_empty_when_no_group1_data` (function, L530)

### `tests/test_generate_governance_narrative_policy.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS,POOLED_FIELDS`
- `csv`
- `generate_governance_narrative`
- `governance_policy:DEFAULT_POLICY_DIR,load_governance_policy`
- `json`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `_reset_governance_policy` (function, L33)
- `_summary_row` (function, L41)
- `_pooled_row` (function, L47)
- `_write_csv` (function, L53)
- `_minimal_fixture` (function, L60)
- `_run_main` (function, L87)
- `test_default_policy_dir_is_policies_governance_in_repo` (function, L96)
- `test_shipped_policy_file_matches_python_default_profile` (function, L109)
- `test_finding_rules_json_documents_every_rule_id_generator_emits` (function, L131)
- `test_loading_default_policy_dir_reproduces_module_level_defaults` (function, L146)
- `test_overriding_tier_threshold_changes_assign_tier_output` (function, L173)
- `test_overriding_excluded_from_scoring_changes_build_cascade` (function, L188)
- `test_overriding_excluded_from_scoring_changes_render_limitations_note` (function, L209)
- `test_render_limitations_excluded_note_pluralizes_for_multiple_domains` (function, L226)
- `test_render_limitations_handles_empty_excluded_set` (function, L238)
- `test_overriding_domain_guidance_changes_detect_anomalies_text` (function, L248)
- `test_overriding_static_findings_guidance_changes_rendered_prose` (function, L266)
- `test_overriding_client_onboarding_threshold_changes_profile_text` (function, L277)
- `test_overriding_anomaly_threshold_changes_detect_anomalies_text` (function, L288)
- `test_main_default_invocation_uses_shipped_policy_dir_and_stays_complete` (function, L319)
- `test_main_with_policy_dir_missing_some_files_reports_defaulted_and_degraded` (function, L334)
- `test_main_with_nonexistent_policy_dir_does_not_crash_and_uses_all_defaults` (function, L357)
- `test_main_output_identical_with_default_and_explicit_shipped_policy_dir` (function, L367)

### `tests/test_generate_governance_narrative_scope_breakdown.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS`
- `generate_governance_narrative:_target_scope_label,build_cascade,detect_anomalies,normalise_summary_schema,render_generic_baseline_scope_section`
- `pathlib:Path`
- `sys`

**Definitions**
- `_row` (function, L29)
- `test_target_scope_label_enterprise` (function, L35)
- `test_target_scope_label_single_dimensions` (function, L40)
- `test_target_scope_label_combined_dimensions` (function, L54)
- `test_target_scope_label_collection_only_is_other_scoped` (function, L61)
- `test_gt_enterprise_slice_unchanged_by_scoped_rows` (function, L69)
- `test_gt_by_scope_absent_when_no_generic_to_template_rows` (function, L106)
- `test_generic_side_still_gated_to_unscoped_reference` (function, L119)
- `_generic_to_template_rows` (function, L140)
- `test_detect_anomalies_flags_material_scope_divergence` (function, L158)
- `test_detect_anomalies_silent_when_no_material_divergence` (function, L168)
- `test_detect_anomalies_silent_when_only_enterprise_scope_exists` (function, L177)
- `test_render_generic_baseline_scope_section_includes_all_scopes` (function, L187)
- `test_render_generic_baseline_scope_section_empty_when_no_data` (function, L197)

### `tests/test_generate_governance_narrative_state_types.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:GOVERNANCE_STATE_DIRECTED_TYPES`
- `generate_governance_narrative:_DIRECTED_GOVERNANCE_TYPES,_GOVERNANCE_STATE_RENDERED_TYPES,build_governance_state_summary`
- `pathlib:Path`
- `sys`

**Definitions**
- `_gs_row` (function, L26)
- `_state_row` (function, L41)
- `test_directed_governance_types_match_producer_modulo_flagged_legacy_entries` (function, L48)
- `test_rendered_types_excludes_scope_level_fan_out` (function, L65)
- `test_compact_summary_loop_does_not_blend_distinct_comparison_types` (function, L70)
- `test_detailed_loop_no_longer_drops_new_scope_types` (function, L92)
- `test_domain_with_only_group3_state_is_omitted_from_result` (function, L120)

### `tests/test_generate_governance_narrative_static_docs_copy.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS,POOLED_FIELDS`
- `csv`
- `generate_governance_narrative`
- `json`
- `json`
- `pathlib:Path`
- `sys`

**Definitions**
- `_summary_row` (function, L18)
- `_pooled_row` (function, L24)
- `_write_csv` (function, L30)
- `_minimal_fixture` (function, L37)
- `_run_main` (function, L64)
- `test_render_header_points_at_out_directory_copy_when_guide_will_be_copied` (function, L81)
- `test_render_header_points_at_repo_path_when_guide_will_not_be_copied` (function, L94)
- `test_authority_header_points_at_out_directory_copy_when_guides_present` (function, L101)
- `test_authority_header_points_at_repo_path_when_guides_absent` (function, L116)
- `test_authority_header_points_at_repo_path_when_evidence_package_disabled` (function, L128)
- `test_default_run_copies_all_four_static_docs_into_out` (function, L133)
- `test_narrative_pointer_matches_actual_guide_presence_end_to_end` (function, L141)
- `test_health_degrades_end_to_end_when_interpretation_guide_source_absent` (function, L155)
- `test_no_emit_evidence_package_removes_previously_copied_docs` (function, L169)
- `test_no_emit_evidence_package_does_not_delete_source_docs_when_out_is_docs_governance` (function, L184)
- `test_copy_removed_when_source_doc_absent_but_stale_copy_exists` (function, L206)
- `test_evidence_map_output_local_path_matches_actual_copy_end_to_end` (function, L221)

### `tests/test_generate_governance_narrative_union_breadth.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:UNION_INVENTORY_FIELDS`
- `generate_governance_narrative`
- `governance_policy:load_governance_policy`
- `json`
- `json`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `_reset_governance_policy` (function, L21)
- `_union_row` (function, L26)
- `_minimal_cascade_dict` (function, L36)
- `test_union_breadth_classifies_corpus_wide_pattern` (function, L52)
- `test_union_breadth_classifies_client_wide_pattern` (function, L60)
- `test_union_breadth_single_client_grain_never_classifies_corpus_or_client_wide` (function, L68)
- `test_union_breadth_classifies_project_wide_pattern` (function, L81)
- `test_union_breadth_classifies_file_level_pattern` (function, L87)
- `test_union_breadth_does_not_merge_across_discipline_grains` (function, L94)
- `test_union_breadth_records_broad_scopes_for_corpus_and_client_wide_patterns` (function, L121)
- `test_union_breadth_broad_scopes_empty_when_no_broad_pattern` (function, L139)
- `test_union_breadth_degraded_row_vetoes_healthy_classification_for_same_pattern` (function, L146)
- `test_union_breadth_degraded_source_status_classifies_unclassified` (function, L168)
- `test_union_breadth_degraded_inventory_status_classifies_unclassified` (function, L181)
- `test_union_breadth_blank_status_fields_are_not_treated_as_degraded` (function, L189)
- `test_union_breadth_ignores_non_project_role` (function, L201)
- `test_union_breadth_ignores_non_all_view_scope` (function, L207)
- `test_union_breadth_empty_input_returns_empty_dict` (function, L213)
- `test_union_breadth_preserves_highest_tier_across_repeated_client_rows` (function, L217)
- `test_union_breadth_preserves_highest_tier_regardless_of_row_order` (function, L238)
- `test_union_breadth_never_returns_raw_pattern_content` (function, L250)
- `test_broad_reuse_weak_cascade_fires` (function, L265)
- `test_broad_reuse_note_names_qualifying_scope_when_present` (function, L273)
- `test_broad_reuse_note_omits_scope_clause_when_broad_scopes_missing` (function, L287)
- `test_narrow_reuse_strong_cascade_fires` (function, L297)
- `test_unremarkable_breadth_and_cascade_does_not_fire` (function, L305)
- `test_no_union_breadth_supplied_never_fires` (function, L318)
- `test_broad_reuse_with_no_primary_containment_does_not_fire` (function, L324)
- `test_overriding_union_breadth_threshold_changes_detect_anomalies_text` (function, L332)

### `tests/test_generate_governance_narrative_unscoped_segment.py`

**Imports**
- `__future__:annotations`
- `generate_governance_narrative:_is_unscoped_segment`
- `pathlib:Path`
- `sys`

**Definitions**
- `_row` (function, L24)
- `test_genuinely_broadest_segment_is_unscoped` (function, L33)
- `test_trailing_blank_client_token_is_still_unscoped` (function, L37)
- `test_bc_scoped_segment_is_rejected` (function, L45)
- `test_collection_scoped_segment_is_rejected` (function, L49)
- `test_blank_client_token_with_real_hidden_scope_value_is_rejected` (function, L53)
- `test_client_scoped_segment_is_rejected` (function, L59)
- `test_blank_role_rollup_is_rejected` (function, L63)

### `tests/test_generate_governance_narrative_wp_reliability_resolved.py`

**Imports**
- `__future__:annotations`
- `compare_cross_segment:SUMMARY_FIELDS`
- `generate_governance_narrative:RELIABILITY_UNKNOWN,build_cascade,normalise_summary_schema,score_reliability`
- `pathlib:Path`
- `sys`

**Definitions**
- `_row` (function, L37)
- `_wp_row` (function, L43)
- `test_no_manifest_keeps_today_behavior_unknown` (function, L55)
- `test_redundant_single_child_root_resolves_to_bc_scoped_row` (function, L68)
- `test_dead_end_redundant_chain_stays_unknown` (function, L98)
- `test_directly_unscoped_row_still_tagged_enterprise_not_resolved` (function, L126)
- `test_manifest_provided_but_no_matching_row_leaves_unknown` (function, L146)

### `tests/test_generate_name_key_patterns.py`

**Imports**
- `__future__:annotations`
- `core.name_key_coverage:COVERAGE_EXCLUDED,COVERAGE_NATIVE,COVERAGE_PHASES_REDUNDANT,COVERAGE_WIDENED,ELIGIBLE_DOMAINS,EXCLUDED_DOMAINS`
- `csv`
- `filecmp`
- `pathlib:Path`
- `tools.generate_name_key_patterns:_assert_no_pattern_id_collision`
- `tools.generate_name_key_patterns:build_domain_coverage,build_name_membership,build_name_patterns,emit_config_patterns,emit_name_patterns`
- `tools.pattern_id_utils:stable_pattern_id`

**Definitions**
- `_write_csv` (function, L34)
- `_read_csv` (function, L43)
- `TestConfigPathRegression` (class, L54)
- `TestConfigPathRegression.test_config_output_is_byte_identical_to_production_source` (method, L55)
- `TestConfigPathRegression.test_config_target_never_writes_to_the_source_path` (method, L68)
- `TestNamePathCoverageClassTagging` (class, L76)
- `TestNamePathCoverageClassTagging._sample_rows` (method, L77)
- `TestNamePathCoverageClassTagging.test_pattern_rows_tagged_with_coverage_class` (method, L90)
- `TestNamePathCoverageClassTagging.test_materials_cluster_spans_both_files` (method, L97)
- `TestNamePathCoverageClassTagging.test_source_cluster_id_matches_production_convention` (method, L103)
- `TestNamePathCoverageClassTagging.test_non_ok_status_rows_excluded_from_patterns` (method, L111)
- `TestNamePathCoverageClassTagging.test_membership_rows_link_records_to_pattern_ids` (method, L118)
- `TestNamePathCoverageClassTagging.test_end_to_end_emit_name_patterns` (method, L127)
- `TestExcludedDomainExplicitAbsence` (class, L146)
- `TestExcludedDomainExplicitAbsence.test_domain_coverage_lists_all_37_traced_domains` (method, L147)
- `TestExcludedDomainExplicitAbsence.test_excluded_domains_marked_not_included_with_reason` (method, L153)
- `TestExcludedDomainExplicitAbsence.test_eligible_domains_marked_included` (method, L161)
- `TestExcludedDomainExplicitAbsence.test_excluded_domain_rows_in_name_key_csv_produce_no_pattern` (method, L167)
- `TestExcludedDomainExplicitAbsence.test_untraced_input_domain_reported_not_traced_not_silently_dropped` (method, L177)
- `TestBothModeNonCollision` (class, L191)
- `TestBothModeNonCollision.test_pattern_id_formula_differs_by_schema_even_for_same_domain_and_hash` (method, L192)
- `TestBothModeNonCollision.test_end_to_end_both_target_no_collision` (method, L202)

### `tests/test_governance_evidence_package.py`

**Imports**
- `__future__:annotations`
- `governance_evidence_package:AUTHORITY_LEVELS,EVIDENCE_MAP_SCHEMA_VERSION,FILE_INVENTORY_SCHEMA_VERSION,GENERATOR_IDENTITY,GENERATOR_ROLE,PACKAGE_SCHEMA_VERSION,PACKAGE_TYPE,build_evidence_map,build_file_inventory_document,build_package_health,build_package_manifest,comparison_type_coverage,inventory_export_directory_files,write_json`
- `json`
- `pathlib:Path`
- `sys`

**Definitions**
- `test_write_json_round_trips` (function, L37)
- `test_comparison_type_coverage_recognized_only` (function, L49)
- `test_comparison_type_coverage_flags_unrecognized` (function, L58)
- `test_comparison_type_coverage_intentionally_excluded_is_distinct_from_unrecognized` (function, L64)
- `test_comparison_type_coverage_ignores_blank_values` (function, L75)
- `_manifest` (function, L84)
- `test_manifest_records_generator_identity_and_schema_version` (function, L100)
- `test_manifest_marks_input_present_based_on_real_filesystem_state` (function, L107)
- `test_manifest_does_not_claim_missing_source_identifiers` (function, L117)
- `test_manifest_package_status_incomplete_when_required_input_missing` (function, L123)
- `test_manifest_package_status_complete_when_required_inputs_present` (function, L129)
- `test_manifest_records_output_sizes` (function, L138)
- `test_manifest_records_policy_dir_as_inert_field` (function, L153)
- `test_manifest_without_policy_profiles_kwarg_keeps_pr1_not_yet_implemented_note` (function, L158)
- `test_manifest_records_policy_profiles_when_supplied` (function, L168)
- `test_manifest_policy_profiles_note_says_five_profiles_not_four` (function, L178)
- `_health` (function, L198)
- `test_health_complete_when_all_required_present_and_no_warnings` (function, L218)
- `test_health_invalid_when_required_input_missing` (function, L225)
- `test_health_degraded_not_invalid_when_only_optional_signal_is_a_warning` (function, L232)
- `test_health_reports_unrecognized_comparison_type_as_warning` (function, L239)
- `test_health_reports_client_sector_default_missing` (function, L247)
- `test_health_reports_client_sector_explicit_missing` (function, L253)
- `test_health_omitted_policy_load_status_adds_no_warning_and_stays_complete` (function, L259)
- `test_health_reports_policy_profile_defaulted_as_warning_and_degraded` (function, L268)
- `test_health_all_policy_profiles_from_file_adds_no_warning` (function, L279)
- `test_health_omitted_interpretation_guide_present_adds_no_warning` (function, L288)
- `test_health_degrades_when_interpretation_guide_absent` (function, L297)
- `test_health_no_warning_when_interpretation_guide_present` (function, L307)
- `test_health_does_not_warn_when_client_sector_explicitly_provided` (function, L313)
- `test_health_matrix_manifest_reports_row_count_and_names` (function, L320)
- `test_health_warning_and_limitation_text_has_no_severity_language` (function, L327)
- `_evidence_map` (function, L346)
- `test_evidence_map_has_thirty_seven_unique_artifacts` (function, L362)
- `test_evidence_map_required_fields_populated_for_every_artifact` (function, L383)
- `test_evidence_map_omits_output_local_path_when_out_dir_not_supplied` (function, L400)
- `test_evidence_map_output_local_path_present_when_sibling_present_and_out_dir_supplied` (function, L409)
- `test_evidence_map_omits_output_local_path_when_sibling_absent_even_with_out_dir` (function, L436)
- `test_evidence_map_authority_levels_use_only_defined_vocabulary` (function, L442)
- `test_evidence_map_narrative_is_controlled_interpretation_not_authoritative` (function, L448)
- `test_evidence_map_source_csvs_are_authoritative` (function, L454)
- `test_evidence_map_client_sector_is_user_provided_note` (function, L462)
- `test_evidence_map_related_artifacts_are_valid_artifact_ids` (function, L468)
- `test_evidence_map_self_lists_all_other_artifacts` (function, L477)
- `test_evidence_map_sibling_artifacts_present_flag_reflects_filesystem` (function, L484)
- `test_evidence_map_excluded_siblings_get_scanned_columns_and_row_count_when_present` (function, L515)
- `test_evidence_map_excluded_siblings_have_no_scan_fields_when_absent` (function, L533)
- `test_evidence_map_excluded_sibling_scan_never_retains_sample_values` (function, L544)
- `test_evidence_map_uses_overridden_package_schema_version_for_manifest_and_health_entries` (function, L554)
- `test_evidence_map_defaults_manifest_and_health_schema_version_to_package_default` (function, L563)
- `test_evidence_map_known_limitations_text_has_no_severity_language` (function, L570)
- `test_evidence_map_governance_file_inventory_is_authoritative_and_has_no_fixed_related_artifacts` (function, L580)
- `test_evidence_map_governance_file_inventory_honors_overridden_schema_version` (function, L592)
- `test_evidence_map_reasoning_prerequisites_matches_required_before_conclusions_flags` (function, L602)
- `test_evidence_map_reasoning_prerequisites_includes_primary_rollups_and_health_and_findings` (function, L616)
- `test_evidence_map_reasoning_prerequisites_excludes_purely_descriptive_artifacts` (function, L627)
- `test_evidence_map_governance_reading_order_present_flag_reflects_filesystem` (function, L634)
- `test_evidence_map_governance_classification_rules_present_flag_reflects_filesystem` (function, L643)
- `_write` (function, L657)
- `test_inventory_scan_infers_column_dtypes` (function, L662)
- `test_inventory_scan_blank_cells_do_not_break_integer_inference` (function, L671)
- `test_inventory_scan_all_blank_column_is_empty_dtype` (function, L677)
- `test_inventory_scan_mixed_numeric_and_text_column_is_string` (function, L683)
- `test_inventory_scan_header_only_file_is_empty_file` (function, L689)
- `test_inventory_scan_zero_byte_file_is_flagged_empty_file` (function, L696)
- `test_inventory_scan_excludes_known_paths` (function, L703)
- `test_inventory_scan_never_retains_sample_values` (function, L711)
- `test_inventory_scan_dedupes_same_file_seen_via_two_scan_dirs` (function, L717)
- `test_inventory_scan_skips_nonexistent_directory` (function, L723)
- `test_inventory_scan_only_matches_csv_files` (function, L728)
- `test_build_file_inventory_document_wraps_files_and_counts` (function, L736)
- `test_domain_and_client_summary_null_semantics_cite_the_actual_em_dash` (function, L748)

### `tests/test_governance_field_completeness_gate.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `pytest`
- `sys`
- `tools.run_extract_all:_check_governance_field_completeness`

**Definitions**
- `_row` (function, L17)
- `test_blank_client_label_fails_with_export_run_id` (function, L25)
- `test_blank_business_center_label_fails_with_export_run_id` (function, L33)
- `test_na_spelling_fails_same_as_blank` (function, L42)
- `test_fully_populated_row_passes` (function, L51)
- `test_multiple_offenders_all_reported` (function, L59)

### `tests/test_governance_manifest.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `pytest`
- `sys`
- `tools.compare_cross_segment:_scope_level`
- `tools.enterprise_policy:load_enterprise_policy`
- `tools.enterprise_policy:load_enterprise_policy`
- `tools.governance_manifest:build_governance_populations,compute_scope_key,normalize_business_center_label`

**Definitions**
- `_row` (function, L25)
- `_find` (function, L36)
- `_members` (function, L45)
- `test_normalize_strips_bc_prefix_case_insensitive` (function, L53)
- `test_normalize_recognizes_enterprise_tokens_before_prefix_strip` (function, L59)
- `test_normalize_zero_pads_short_numeric_values` (function, L65)
- `test_normalize_zero_pad_recognizes_collapsed_enterprise_token` (function, L77)
- `test_normalize_zero_pad_does_not_affect_bc_prefixed_values` (function, L83)
- `test_scope_key_requires_both_conditions_for_enterprise` (function, L89)
- `test_internalenterprise_and_0000_collapse_to_one_enterprise_population` (function, L108)
- `test_legacy_bc_prefixed_and_bare_numeric_collapse_to_one_population` (function, L120)
- `test_excel_collapsed_and_correctly_formatted_bc_collapse_to_one_population` (function, L134)
- `test_generic_role_gets_no_scope_key` (function, L152)
- `test_generic_host_role_treated_same_as_generic` (function, L165)
- `test_case_variant_role_merges_into_canonical_population` (function, L172)
- `test_generic_host_case_variant_folds_to_generic_role_label` (function, L189)
- `test_unit_system_case_variants_merge_to_lowercase` (function, L201)
- `test_client_label_case_variants_merge_to_first_seen_casing` (function, L214)
- `test_discipline_label_case_variants_merge` (function, L228)
- `test_business_center_label_case_variants_merge_after_prefix_strip` (function, L239)
- `test_enterprise_bookkeeping_casing_still_recognized_after_normalization` (function, L254)
- `test_unrecognized_role_excluded_with_loud_report` (function, L266)
- `test_blank_client_label_raises_defense_in_depth` (function, L281)
- `test_na_business_center_label_raises_defense_in_depth` (function, L287)
- `test_fully_populated_rows_build_without_raising` (function, L293)
- `test_compute_scope_key_accepts_case_insensitive_deployment_override` (function, L301)
- `test_cross_segment_and_governance_manifest_share_policy_classification` (function, L308)

### `tests/test_governance_policy.py`

**Imports**
- `__future__:annotations`
- `governance_policy:ANOMALY_THRESHOLDS_FILENAME,CLIENT_ONBOARDING_FILENAME,DEFAULT_POLICY_DIR,DOMAIN_POLICY_FILENAME,FINDING_RULES_FILENAME,THRESHOLDS_FILENAME,load_governance_policy`
- `json`
- `pathlib:Path`
- `pytest`
- `sys`

**Definitions**
- `test_default_policy_dir_is_repo_policies_governance` (function, L38)
- `test_none_policy_dir_uses_default_for_every_profile` (function, L43)
- `test_missing_files_in_existing_dir_use_default_per_file` (function, L52)
- `test_present_file_overrides_default_for_that_profile_only` (function, L60)
- `test_all_five_profile_files_can_be_overridden_independently` (function, L74)
- `test_present_but_malformed_json_raises_not_silently_falls_back` (function, L89)
- `test_policy_dir_string_input_normalised_to_str_in_result` (function, L95)

### `tests/test_governance_relationships.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `sys`
- `tools.governance_relationships:build_relationships_rows,build_bc_client_matrix_rows,build_client_bc_matrix_rows`

**Definitions**
- `_row` (function, L19)
- `_find` (function, L31)
- `test_non_project_roles_excluded` (function, L41)
- `test_lowercase_governance_role_still_counted_as_project` (function, L53)
- `test_client_label_casing_variants_fold_to_one_project` (function, L63)
- `test_bc_prefix_variant_folds_to_same_bc_identity` (function, L74)
- `test_excel_collapsed_bc_folds_to_same_bc_identity` (function, L87)
- `test_same_project_label_different_client_stays_distinct` (function, L102)
- `test_blank_project_label_falls_back_to_export_run_id_per_file` (function, L122)
- `test_multi_discipline_project_collects_sorted_discipline_list` (function, L139)
- `test_enterprise_bookkeeping_bc_token_blanked_not_carried_as_fake_bc` (function, L151)
- `test_enterprise_bookkeeping_project_excluded_from_bc_client_matrix` (function, L166)
- `test_inconsistent_unit_system_within_one_project_warns_not_raises` (function, L181)
- `_rel_row` (function, L197)
- `test_percentage_of_bc_and_client_single_bc_per_client` (function, L206)
- `test_percentage_of_client_sums_to_one_across_multiple_bcs` (function, L217)
- `test_client_bc_matrix_never_recomputes_percentage_it_only_sums_counts` (function, L240)

### `tests/test_graphic_overrides.py`

**Imports**
- `core.graphic_overrides:_is_invalid_element_id,_rgb_from_color,_read_attr,_read_first_attr,extract_projection_graphics,extract_cut_graphics,extract_halftone,extract_transparency`
- `core.record_v2:ITEM_Q_OK,ITEM_Q_MISSING,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED`
- `pytest`

**Definitions**
- `TestIsInvalidElementId` (class, L27)
- `TestIsInvalidElementId.test_none_is_invalid` (method, L28)
- `TestIsInvalidElementId.test_positive_integer_value_is_valid` (method, L31)
- `TestIsInvalidElementId.test_positive_integer_value_is_valid.FakeId` (class, L32)
- `TestIsInvalidElementId.test_zero_is_invalid` (method, L36)
- `TestIsInvalidElementId.test_zero_is_invalid.FakeId` (class, L38)
- `TestIsInvalidElementId.test_negative_is_invalid` (method, L42)
- `TestIsInvalidElementId.test_negative_is_invalid.FakeId` (class, L43)
- `TestIsInvalidElementId.test_no_integer_value_attr_is_valid` (method, L47)
- `TestRgbFromColor` (class, L56)
- `TestRgbFromColor.test_none_returns_missing` (method, L57)
- `TestRgbFromColor.test_valid_color` (method, L62)
- `TestRgbFromColor.test_valid_color.FakeColor` (class, L63)
- `TestRgbFromColor.test_black` (method, L71)
- `TestRgbFromColor.test_black.FakeColor` (class, L72)
- `TestRgbFromColor.test_white` (method, L80)
- `TestRgbFromColor.test_white.FakeColor` (class, L81)
- `TestRgbFromColor.test_unreadable_color` (method, L89)
- `TestRgbFromColor.test_unreadable_color.BadColor` (class, L91)
- `TestRgbFromColor.test_unreadable_color.BadColor.Red` (method, L93)
- `TestReadAttr` (class, L104)
- `TestReadAttr.test_existing_attr` (method, L105)
- `TestReadAttr.test_existing_attr.Obj` (class, L106)
- `TestReadAttr.test_missing_attr` (method, L112)
- `TestReadAttr.test_raising_attr` (method, L117)
- `TestReadAttr.test_raising_attr.Obj` (class, L118)
- `TestReadAttr.test_raising_attr.Obj.broken` (method, L120)
- `TestReadFirstAttr` (class, L131)
- `TestReadFirstAttr.test_first_found` (method, L132)
- `TestReadFirstAttr.test_first_found.Obj` (class, L133)
- `TestReadFirstAttr.test_none_found` (method, L140)
- `TestReadFirstAttr.test_first_raises_returns_unreadable` (method, L146)
- `TestReadFirstAttr.test_first_raises_returns_unreadable.Obj` (class, L147)
- `TestReadFirstAttr.test_first_raises_returns_unreadable.Obj.a` (method, L149)
- `TestExtractUnknownSource` (class, L161)
- `TestExtractUnknownSource.test_projection_graphics_unknown_source` (method, L165)
- `TestExtractUnknownSource.test_cut_graphics_unknown_source` (method, L177)
- `TestExtractUnknownSource.test_halftone_unknown_source` (method, L189)
- `TestExtractUnknownSource.test_transparency_unknown_source` (method, L195)
- `TestExtractUnknownSource.test_custom_key_prefix` (method, L201)
- `TestExtractUnknownSource.test_halftone_custom_prefix` (method, L208)
- `TestExtractUnknownSource.test_transparency_custom_prefix` (method, L212)

### `tests/test_hashing_incremental.py`

**Imports**
- `core.hashing:make_hash,safe_str`
- `hashlib`
- `pytest`

**Definitions**
- `_reference_hash` (function, L9)
- `test_make_hash_matches_reference_empty` (function, L19)
- `test_make_hash_matches_reference_single` (function, L23)
- `test_make_hash_matches_reference_multiple_and_unicode_and_pipes` (function, L28)
- `test_make_hash_deterministic_repeated_calls` (function, L33)
- `test_make_hash_is_order_sensitive_contract` (function, L40)
- `test_make_hash_separator_off_by_one_cases` (function, L46)
- `test_make_hash_handles_unrepr_values` (function, L60)
- `test_make_hash_handles_unrepr_values.BadStr` (class, L61)
- `test_make_hash_handles_unrepr_values.BadStr.__str__` (method, L62)
- `test_make_hash_accepts_generator_large_input_sanity` (function, L70)

### `tests/test_identity_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`

**Definitions**
- `test_identity_join_key_uses_required_plus_gated_required_only` (function, L9)

### `tests/test_identity_project_info.py`

**Imports**
- `core.join_key_policy:load_join_key_policies`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_OK,ITEM_Q_UNREADABLE,ITEM_Q_UNSUPPORTED_NOT_APPLICABLE`
- `domains.identity`
- `json`
- `pytest`
- `pytest`
- `tests.synthetic_governance_fixtures:BUSINESS_CENTER_01,ENTERPRISE,PROJECT_ALPHA`

**Definitions**
- `FakeParameter` (class, L14)
- `FakeParameter.__init__` (method, L15)
- `FakeParameter.AsString` (method, L18)
- `FakeParameter.AsValueString` (method, L21)
- `FakeBuiltInParameter` (class, L25)
- `FakeGuid` (class, L40)
- `FakeGuid.__init__` (method, L43)
- `FakeGuid.__eq__` (method, L46)
- `FakeGuid.__hash__` (method, L49)
- `FakeGuid.__repr__` (method, L52)
- `_system_guid_available` (function, L57)
- `FakeProjectInformation` (class, L62)
- `FakeProjectInformation.__init__` (method, L77)
- `FakeProjectInformation.get_Parameter` (method, L84)
- `FakeProjectInformation.LookupParameter` (method, L93)
- `_project_info_with_configured_business_center` (function, L117)
- `_project_info_without_configured_business_center` (function, L130)
- `_extract_items` (function, L153)
- `_as_dict` (function, L173)
- `_DocForPI` (class, L177)
- `_DocForPI.__init__` (method, L182)
- `FakeDoc` (class, L186)
- `FakeDoc.__init__` (method, L187)
- `FakeDoc.__init__._App` (class, L193)
- `test_extract_project_info_items_covers_exact_expected_keys` (function, L201)
- `test_no_deployment_configuration_emits_only_builtin_fields` (function, L209)
- `test_malformed_configured_guid_is_rejected` (function, L216)
- `test_extract_project_info_items_keys_are_registered_in_contract` (function, L226)
- `test_builtin_fields_resolve_regardless_of_configured_shared_parameter` (function, L233)
- `test_business_center_and_ifc_guids_resolve_ok_when_present` (function, L252)
- `test_business_center_is_read_via_shared_parameter_guid_when_available` (function, L263)
- `test_guid_configuration_fails_closed_when_guid_type_unavailable` (function, L283)
- `test_business_center_is_not_applicable_when_shared_param_absent_not_unreadable` (function, L292)
- `test_ifc_guid_builtins_follow_same_semantics_as_other_builtins` (function, L311)
- `test_named_field_present_but_blank_is_missing_not_not_applicable` (function, L335)
- `test_named_field_read_exception_is_unreadable` (function, L349)
- `test_named_field_read_exception_is_unreadable.ThrowingParameter` (class, L352)
- `test_named_field_read_exception_is_unreadable.ThrowingParameter.AsString` (method, L353)
- `test_configured_field_quality_states_participate_in_signature` (function, L368)
- `test_builtin_field_unreadable_when_no_builtinparameter_enum` (function, L384)
- `test_project_information_missing_marks_every_field_unreadable` (function, L392)
- `test_extract_end_to_end_includes_project_info_in_sig_hash_and_leaves_status_ok` (function, L399)
- `test_phase2_semantic_keys_excludes_project_info_and_stays_the_pre_d025_core` (function, L421)
- `test_extract_end_to_end_without_configured_shared_parameter_stays_ok` (function, L447)
- `test_join_key_and_name_key_are_unaffected_by_project_info_fields` (function, L463)

### `tests/test_join_key_builder_shape_gating_dedupe.py`

**Imports**
- `core.join_key_builder:build_join_key_from_policy`
- `core.record_v2:ITEM_Q_OK,make_identity_item`
- `pytest`

**Definitions**
- `_policy_with_overlap` (function, L12)
- `test_join_key_builder_dedupes_required_and_optional` (function, L32)

### `tests/test_join_key_discovery_shape_matching.py`

**Imports**
- `tools.join_key_discovery.eval:build_candidate_join_key_with_details`
- `tools.join_key_discovery.eval:build_identity_index`

**Definitions**
- `test_shape_gating_matches_bool_case_variants` (function, L6)
- `test_shape_gating_does_not_require_phase_filter_for_false` (function, L39)
- `test_identity_index_keeps_q_only_rows_for_required_presence` (function, L71)

### `tests/test_join_key_migration.py`

**Imports**
- `core.record_v2:ITEM_Q_OK,ITEM_Q_MISSING,make_identity_item`
- `domains.view_category_overrides:_compute_override_properties_hash`
- `domains.view_category_overrides:_compute_override_properties_hash`
- `domains.view_category_overrides:_compute_override_properties_hash`
- `domains.view_category_overrides:_compute_override_properties_hash`
- `domains.view_category_overrides:_phase2_partition_items`
- `domains.view_category_overrides:_phase2_partition_items`
- `hashlib`
- `json`
- `json`
- `os`
- `pytest`
- `sys`

**Definitions**
- `TestViewCategoryOverridesOverrideHash` (class, L29)
- `TestViewCategoryOverridesOverrideHash.test_override_hash_deterministic` (method, L32)
- `TestViewCategoryOverridesOverrideHash.test_override_hash_excludes_baseline_items` (method, L55)
- `TestViewCategoryOverridesOverrideHash.test_override_hash_handles_none_values` (method, L76)
- `TestViewCategoryOverridesOverrideHash.test_override_hash_different_values_produce_different_hash` (method, L89)
- `TestViewCategoryOverridesPhase2Partition` (class, L107)
- `TestViewCategoryOverridesPhase2Partition.test_partition_semantic_items` (method, L110)
- `TestViewCategoryOverridesPhase2Partition.test_partition_cosmetic_items` (method, L130)
- `TestViewTemplatesJoinKey` (class, L156)
- `TestViewTemplatesJoinKey.test_join_hash_format` (method, L159)
- `TestViewTemplatesJoinKey.test_join_hash_equals_def_hash` (method, L181)
- `TestJoinKeyPolicyStructure` (class, L195)
- `TestJoinKeyPolicyStructure.test_vco_policy_exists` (method, L198)
- `TestJoinKeyPolicyStructure.test_vt_policy_exists` (method, L214)
- `TestJoinKeyStructureValidation` (class, L249)
- `TestJoinKeyStructureValidation.test_vco_join_key_has_required_fields` (method, L252)
- `TestJoinKeyStructureValidation.test_vt_join_key_has_required_fields` (method, L272)
- `TestGroupingBasis` (class, L292)
- `TestGroupingBasis.test_vco_grouping_basis_is_join_key` (method, L295)
- `TestGroupingBasis.test_vt_grouping_basis_is_join_key` (method, L307)

### `tests/test_join_key_policy_validation.py`

**Imports**
- `core.join_key_policy:validate_domain_join_key_policy`
- `pytest`

**Definitions**
- `test_valid_shape_gated_policy_has_no_errors` (function, L28)
- `test_rule_a1_discriminator_first_required` (function, L33)
- `test_rule_a2_no_overlap_common_required` (function, L43)
- `test_rule_a3_additional_required_in_optional_items` (function, L56)
- `test_rule_a4_requires_non_empty_additional_required` (function, L66)
- `test_rule_a5_orphaned_keys_warning_only` (function, L79)

### `tests/test_label_synthesis_domain_prompt_loader.py`

**Imports**
- `tools.label_synthesis.synthesize_fragmented_labels:_load_domain_prompt_module`

**Definitions**
- `test_domain_prompt_loader_supports_single_word_domains` (function, L4)
- `test_domain_prompt_loader_falls_back_to_base_for_multi_segment_domains` (function, L10)

### `tests/test_line_pattern_mapping_reconstruction.py`

**Imports**
- `core.record_v2:STATUS_BLOCKED,STATUS_DEGRADED,STATUS_OK`
- `csv`
- `mapping.line_pattern_reconstruction:MappingOutcome,ReconstructedPattern,build_mapping_name_candidates,build_report_rows,compute_join_hash_for_segments,compute_run_status,compute_segments_def_hash,compute_segments_norm_hash,dominant_status,group_requested_join_hashes,resolve_observed_name,sanitize_revit_name,select_observed_name,reconstruct_pattern,short_join_hash`
- `pathlib:Path`
- `run_extract_all`
- `sys`

**Definitions**
- `_seg_rows` (function, L42)
- `_requested_join_hash_for` (function, L62)
- `test_reconstruct_ok_with_full_evidence` (function, L74)
- `test_reconstruct_degraded_when_forensic_evidence_absent` (function, L92)
- `test_dot_length_normalization_forces_zero_and_degrades` (function, L104)
- `test_block_settings_absent` (function, L126)
- `test_block_no_items_marker` (function, L133)
- `test_block_duplicate_segment_key` (function, L140)
- `test_block_segment_count_mismatch` (function, L151)
- `test_block_segment_indices_non_contiguous` (function, L158)
- `test_block_quality_not_ok` (function, L172)
- `test_block_segment_kind_unmapped` (function, L184)
- `test_block_non_positive_length_for_non_dot_segment` (function, L191)
- `test_block_segments_def_hash_mismatch` (function, L198)
- `test_block_reconstructed_join_hash_mismatch` (function, L209)
- `_reference_norm_hash_via_run_extract_all` (function, L222)
- `test_segments_norm_hash_matches_run_extract_all_reference` (function, L262)
- `test_select_observed_name_highest_files_count_wins` (function, L282)
- `test_select_observed_name_ignores_non_ok_and_empty` (function, L294)
- `test_select_observed_name_none_when_no_acceptable_rows` (function, L305)
- `test_resolve_observed_name_synthetic_fallback_is_deterministic` (function, L312)
- `test_build_mapping_name_candidates_deterministic_collision_name` (function, L321)
- `test_sanitize_revit_name_replaces_illegal_characters` (function, L331)
- `test_group_requested_join_hashes_dedupes_and_preserves_bundle_associations` (function, L341)
- `test_dominant_status_ordering` (function, L366)
- `test_build_report_rows_deterministic_ordering_and_dedup_of_reasons` (function, L373)
- `test_compute_run_status_dominance_over_outcomes` (function, L383)

### `tests/test_line_patterns_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:load_join_key_policies,get_domain_join_key_policy`
- `core.record_v2:make_identity_item,serialize_identity_items`
- `domains.line_patterns:_line_pattern_segments_def_hash`
- `hashlib`

**Definitions**
- `_Seg` (class, L12)
- `_Seg.__init__` (method, L13)
- `_line_patterns_policy` (function, L18)
- `test_line_patterns_canonical_evidence_selectors_and_hashing` (function, L23)

### `tests/test_line_styles_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:load_join_key_policies,get_domain_join_key_policy`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `domains.line_styles:LINE_STYLE_SEMANTIC_KEYS`

**Definitions**
- `_line_styles_policy` (function, L10)
- `test_line_styles_canonical_evidence_selectors_and_hashing` (function, L15)

### `tests/test_materials.py`

**Imports**
- `core.hashing:make_hash`
- `core.record_v2:serialize_identity_items`
- `importlib`

**Definitions**
- `_Id` (class, L8)
- `_Id.__init__` (method, L9)
- `_Color` (class, L13)
- `_Color.__init__` (method, L14)
- `_Param` (class, L20)
- `_Param.__init__` (method, L21)
- `_Param.AsString` (method, L25)
- `_Param.AsValueString` (method, L28)
- `_FillPatternElem` (class, L32)
- `_FillPatternElem.__init__` (method, L33)
- `_Mat` (class, L38)
- `_Mat.__init__` (method, L39)
- `_Mat.LookupParameter` (method, L70)
- `_Doc` (class, L74)
- `_Doc.__init__` (method, L75)
- `_Doc.GetElement` (method, L78)
- `_identity_map` (function, L83)
- `_material_payload` (function, L88)
- `_make_ctx_with_fill_patterns` (function, L92)
- `test_materials_emits_records_and_hash` (function, L103)
- `test_identity_fields_captured_but_excluded_from_graphics_hash` (function, L119)
- `test_use_render_appearance_captured_but_not_hashed` (function, L141)
- `test_color_and_transparency_are_displayed_values` (function, L160)
- `test_no_pattern_element_id_minus_one_maps_to_none` (function, L173)
- `test_missing_fill_pattern_ctx_degrades_not_blocks` (function, L187)
- `test_fill_pattern_ctx_resolution_populates_sig_hash` (function, L200)
- `test_material_ctx_maps_populated` (function, L210)
- `test_optional_identity_fields_do_not_emit_canonical_sentinel_literals` (function, L228)
- `test_identity_basis_contains_uid_and_sig_basis_items` (function, L242)
- `test_sig_basis_keys_used_reproduces_sig_hash` (function, L254)
- `test_graphics_sig_basis_keys_used_reproduces_graphics_sig_hash_v2` (function, L270)
- `test_label_uses_contract_provenance_token` (function, L283)
- `test_fill_pattern_ctx_missing_is_evaluated_per_record` (function, L291)
- `test_keynote_populated_emits_ok_item` (function, L308)
- `test_keynote_blank_emits_missing_item_not_omitted` (function, L323)
- `test_keynote_unset_param_emits_missing_item_not_omitted` (function, L338)
- `test_blocked_when_api_unavailable` (function, L353)

### `tests/test_na_token.py`

**Imports**
- `na_token:is_na_token,is_blank_or_na`
- `pathlib:Path`
- `sys`

**Definitions**
- `TestIsNaToken` (class, L9)
- `TestIsNaToken.test_recognizes_common_spellings` (method, L10)
- `TestIsNaToken.test_real_values_are_not_na` (method, L20)
- `TestIsNaToken.test_blank_is_not_na` (method, L24)
- `TestIsBlankOrNa` (class, L30)
- `TestIsBlankOrNa.test_blank_variants_are_ignore` (method, L31)
- `TestIsBlankOrNa.test_na_variants_are_ignore` (method, L35)
- `TestIsBlankOrNa.test_real_values_are_not_ignore` (method, L40)

### `tests/test_name_key_inline_analysis_agreement.py`

**Imports**
- `__future__:annotations`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.name_key_builder:build_name_key_for_record`
- `core.name_key_coverage:ELIGIBLE_DOMAINS`
- `core.record_v2:canonicalize_str,make_identity_item`
- `pytest`
- `typing:Any,Dict,List,Tuple`

**Definitions**
- `_inline_equivalent` (function, L48)
- `_native_case` (function, L70)
- `_bucket_widened_case` (function, L80)
- `_label_only_case` (function, L91)
- `name_key_policies` (function, L142)
- `test_inline_equivalent_matches_analysis_side` (function, L147)
- `test_agreement_sample_size_and_match_rate` (function, L167)

### `tests/test_name_key_policy.py`

**Imports**
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_builder:build_join_key_from_policy,compute_projection_status`
- `core.join_key_policy:get_domain_join_key_policy`
- `core.join_key_policy:load_join_key_policies,get_domain_join_key_policy`
- `core.name_key_builder:build_name_key_for_record,flat_items_for_record,_has_detail_data`
- `core.record_v2:canonicalize_str`
- `importlib`
- `json`
- `pathlib:Path`
- `pytest`
- `re`

**Definitions**
- `name_key_policies` (function, L71)
- `test_policy_file_loads_and_validates` (function, L75)
- `test_eligibility_allow_list_matches_exactly` (function, L80)
- `test_excluded_domains_have_no_policy_entry` (function, L85)
- `test_every_eligible_entry_has_exactly_one_required_item` (function, L95)
- `test_phases_carries_explicit_redundancy_marker` (function, L102)
- `test_non_phases_entries_use_bare_schema` (function, L109)
- `TestStatusVocabulary` (class, L116)
- `TestStatusVocabulary.test_missing_policy` (method, L117)
- `TestStatusVocabulary.test_blocked_when_no_required_items_configured` (method, L120)
- `TestStatusVocabulary.test_missing_required_when_required_item_absent` (method, L123)
- `TestStatusVocabulary.test_ok_when_required_items_present` (method, L127)
- `TestDimensionConfigNonInclusion` (class, L132)
- `TestDimensionConfigNonInclusion.test_dimension_config_has_no_name_key_field` (method, L133)
- `TestDimensionConfigNonInclusion.test_dimension_config_fields_unchanged` (method, L142)
- `TestAnalysisSideReconstruction` (class, L156)
- `TestAnalysisSideReconstruction.test_native_domain_materials` (method, L157)
- `TestAnalysisSideReconstruction.test_widened_domain_phase_filters_reads_coordination_bucket` (method, L168)
- `TestAnalysisSideReconstruction.test_label_only_domain_arrowheads_reads_raw_component` (method, L179)
- `TestAnalysisSideReconstruction.test_loaded_family_types_reads_raw_family_name_not_decorated_display` (method, L192)
- `TestAnalysisSideReconstruction.test_view_filter_definitions_reads_raw_name_not_decorated_display` (method, L212)
- `TestAnalysisSideReconstruction.test_ineligible_domain_returns_none` (method, L240)
- `TestAnalysisSideReconstruction.test_missing_name_yields_missing_required_status` (method, L248)
- `TestAnalysisSideReconstruction.test_summary_only_record_does_not_synthesize_label_only_name_key` (method, L258)
- `TestAnalysisSideReconstruction.test_has_detail_data_true_for_identity_basis_phase2_or_items` (method, L269)
- `TestAnalysisSideReconstruction.test_flat_items_for_record_merges_all_buckets` (method, L275)

### `tests/test_no_direct_filtered_element_collector_in_domains.py`

**Imports**
- `glob`
- `os`

**Definitions**
- `_repo_root` (function, L6)
- `test_domains_do_not_reference_filtered_element_collector` (function, L10)

### `tests/test_object_styles_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_OK,build_record_v2,make_identity_item,serialize_identity_items`
- `json`
- `validators.record_v2:validate_record_v2`

**Definitions**
- `_object_styles_model_policy` (function, L12)
- `test_object_styles_model_canonical_evidence_selectors_and_hashing` (function, L17)
- `_domain_identity_registry_v2` (function, L58)
- `_area9_identity_items` (function, L63)
- `test_object_styles_model_area9_fields_pass_contract_validation_for_subcategory` (function, L83)
- `test_object_styles_model_parent_name_missing_and_none_for_top_level_category` (function, L99)

### `tests/test_pareto_shape_gating.py`

**Imports**
- `csv`
- `pathlib:Path`
- `pytest`
- `tools:pareto_joinkey_search`

**Definitions**
- `_write_csv` (function, L16)
- `test_pareto_shape_gating_per_shape` (function, L35)

### `tests/test_phase_filters_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:serialize_identity_items`

**Definitions**
- `test_phase_filters_selectors_and_hashing_use_policy_required_presentation_ids` (function, L9)

### `tests/test_phases_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `domains.phases:_phase2_build_phase2_payload`

**Definitions**
- `_phases_policy` (function, L10)
- `test_phases_join_hash_uses_policy_required_only_and_semantic_selector_is_separate` (function, L15)

### `tests/test_placeholder_exclusions.py`

**Imports**
- `csv`
- `pathlib:Path`
- `subprocess`
- `sys`

**Definitions**
- `_wcsv` (function, L4)
- `test_placeholder_exclusions_smoke` (function, L9)

### `tests/test_probe_inventory_builder.py`

**Imports**
- `csv`
- `json`
- `pathlib:Path`
- `subprocess`
- `sys`

**Definitions**
- `_run` (function, L10)
- `_read_csv_rows` (function, L27)
- `test_merges_and_dedupes_across_dated_runs` (function, L32)
- `test_empty_probes_dir_refuses_to_overwrite_by_default` (function, L138)
- `test_all_inputs_invalid_refuses_to_overwrite_by_default` (function, L157)
- `test_empty_probes_dir_with_force_writes_empty_inventory` (function, L181)
- `_run_shaped_payload` (function, L192)
- `test_merges_run_shaped_files_and_tracks_revit_version` (function, L207)
- `test_merges_across_legacy_and_run_shapes_for_same_domain` (function, L270)

### `tests/test_record_contract_v2.py`

**Imports**
- `json`
- `os`
- `pytest`
- `validators.record_v2:validate_record_v2`

**Definitions**
- `domain_identity_registry_v2` (function, L11)
- `exported_fingerprint_json` (function, L17)
- `test_all_exported_records_conform_to_record_contract_v2` (function, L32)
- `test_blocked_records_have_no_sig_hash` (function, L59)
- `test_exported_records_have_unique_record_id_per_file_and_domain` (function, L66)

### `tests/test_record_id_determinism.py`

**Imports**
- `core.record_v2:STATUS_BLOCKED,STATUS_OK,finalize_record_ids_for_domain,make_record_id_structural`
- `validators.record_v2:validate_records_v2`

**Definitions**
- `_make_record` (function, L11)
- `test_structural_record_id_dup_index_deterministic` (function, L25)
- `test_structural_record_id_duplicate_keys_blocked` (function, L39)
- `test_structural_record_id_stable_across_runs` (function, L54)
- `test_structural_record_id_stable_across_runs._build_records_in_order` (method, L55)
- `test_validate_records_duplicate_within_file_and_domain` (function, L73)

### `tests/test_record_v2_utils.py`

**Imports**
- `core:record_v2`
- `pytest`

**Definitions**
- `test_canonicalize_str_rules` (function, L8)
- `test_canonicalize_int_rules` (function, L14)
- `test_canonicalize_float_rules` (function, L23)
- `test_canonicalize_bool_rules` (function, L30)
- `test_make_identity_item_banned_substring_guard` (function, L41)
- `test_make_identity_item_empty_string_becomes_null_missing` (function, L46)
- `test_serialize_identity_items_is_sorted_and_deterministic` (function, L52)
- `test_compute_identity_quality_dominance` (function, L78)
- `test_compute_identity_quality_blocked_short_circuit` (function, L83)

### `tests/test_reference_bundle.py`

**Imports**
- `pathlib:Path`
- `tools.bundle_analysis.reference_bundle:load_and_validate`

**Definitions**
- `test_load_and_validate_allows_legacy_control_characters` (function, L6)
- `test_load_and_validate_allows_raw_newline_in_string` (function, L26)

### `tests/test_reformat_to_flat_items_identity_lineage.py`

**Imports**
- `tools.migration.reformat_to_flat_items:transform_record`

**Definitions**
- `test_identity_lineage_items_are_preserved_in_canonical_conversion` (function, L4)

### `tests/test_repository_data_remediation.py`

**Imports**
- `json`
- `pathlib:Path`

**Definitions**
- `_read` (function, L10)
- `test_runner_install_discovery_has_only_generic_defaults` (function, L14)
- `test_dynamo_graphs_embed_current_runners_without_workstation_paths` (function, L24)
- `test_default_client_sector_policy_uses_synthetic_labels` (function, L38)

### `tests/test_revitlookup_audit_regressions.py`

**Imports**
- `core.record_v2:ITEM_Q_OK,make_identity_item`
- `domains:object_styles`
- `domains:view_category_overrides_annotation`
- `domains:view_category_overrides_model`
- `pathlib:Path`
- `sys`
- `types`

**Definitions**
- `_install_revit_stubs` (function, L10)
- `test_vfa_phase2_payload_carries_semantic_keys` (function, L25)
- `test_view_templates_signatures_capture_filter_enabled_and_workset_visibility` (function, L30)
- `test_vco_category_hidden_is_semantic_for_model_and_annotation` (function, L38)
- `test_object_styles_model_semantic_keys_include_material_sig_hash` (function, L57)

### `tests/test_run_segment_orchestrator_name_projection.py`

**Imports**
- `__future__:annotations`
- `csv`
- `pathlib:Path`
- `pytest`
- `run_segment_orchestrator`
- `run_segment_orchestrator`
- `run_segment_orchestrator:_active_domains_from_name_patterns,_clear_stale_name_all_before_run,_filter_name_key_csv_to_segment,_run_one_segment,_segment_has_name_leg_output,merge_bi_outputs`
- `shutil`
- `subprocess`
- `sys`
- `threading`
- `threading`
- `tools.generate_name_key_patterns:emit_name_patterns`
- `tools.generate_name_key_patterns:emit_name_patterns`

**Definitions**
- `_write_csv` (function, L41)
- `_read_csv` (function, L50)
- `TestFilterNameKeyCsvToSegment` (class, L55)
- `TestFilterNameKeyCsvToSegment.test_filters_by_export_run_id_membership` (method, L56)
- `TestFilterNameKeyCsvToSegment.test_matches_split_export_details_rows_against_canonical_index_id` (method, L70)
- `TestFilterNameKeyCsvToSegment.test_raises_on_missing_corpus_csv` (method, L89)
- `TestFilterNameKeyCsvToSegment.test_preserves_details_only_export_with_no_index_sibling` (method, L93)
- `TestFilterNameKeyCsvToSegment.test_split_export_and_details_only_export_coexist_correctly` (method, L112)
- `TestActiveDomainsFromNamePatterns` (class, L127)
- `TestActiveDomainsFromNamePatterns.test_reads_domain_column` (method, L128)
- `TestActiveDomainsFromNamePatterns.test_returns_none_when_missing` (method, L140)
- `TestActiveDomainsFromNamePatterns.test_returns_empty_frozenset_when_present_but_empty` (method, L143)
- `TestMergeBiOutputsExcludesStaleDomainsForEmptySegment` (class, L156)
- `TestMergeBiOutputsExcludesStaleDomainsForEmptySegment.test_empty_active_domains_merges_nothing_even_with_stale_folder_present` (method, L157)
- `TestMergeBiOutputsExcludesStaleDomainsForEmptySegment.test_none_active_domains_merges_everything_found_unfiltered` (method, L170)
- `TestMergeBiOutputsExcludesStaleDomainsForEmptySegment.test_stale_combined_csv_from_previous_run_is_deleted_on_empty_rerun` (method, L185)
- `TestMergeBiOutputsExcludesStaleDomainsForEmptySegment.test_stale_combined_csv_deleted_when_all_candidates_are_headerless` (method, L205)
- `TestSegmentHasNameLegOutput` (class, L224)
- `TestSegmentHasNameLegOutput.test_false_when_no_provenance_file` (method, L225)
- `TestSegmentHasNameLegOutput.test_true_when_provenance_file_present` (method, L228)
- `TestCLIComparisonTarget` (class, L235)
- `TestCLIComparisonTarget._build_fixture` (method, L236)
- `TestCLIComparisonTarget._base_args` (method, L263)
- `TestCLIComparisonTarget.test_name_target_requires_name_key_results_csv` (method, L277)
- `TestCLIComparisonTarget.test_config_target_dry_run_has_no_name_leg_lines` (method, L286)
- `TestCLIComparisonTarget.test_name_target_dry_run_includes_name_leg_commands` (method, L294)
- `TestCompleteSegmentSkipHonorsNameTarget` (class, L313)
- `TestCompleteSegmentSkipHonorsNameTarget._build_fixture` (method, L319)
- `TestCompleteSegmentSkipHonorsNameTarget._base_args` (method, L353)
- `TestCompleteSegmentSkipHonorsNameTarget.test_config_target_still_skips_complete_segment` (method, L367)
- `TestCompleteSegmentSkipHonorsNameTarget.test_name_target_does_not_skip_complete_segment_missing_name_leg` (method, L373)
- `TestCompleteSegmentSkipHonorsNameTarget.test_name_target_still_skips_complete_segment_with_existing_name_leg_output` (method, L389)
- `TestCompleteSegmentSkipHonorsNameTarget.test_name_target_still_skips_complete_reference_row_missing_name_leg` (method, L403)
- `TestStaleNameBundleOutputClearedBeforeRerun` (class, L423)
- `TestStaleNameBundleOutputClearedBeforeRerun._materials_name_key_rows` (method, L440)
- `TestStaleNameBundleOutputClearedBeforeRerun._build_populated_name_patterns_dir` (method, L455)
- `TestStaleNameBundleOutputClearedBeforeRerun._build_empty_name_patterns_dir` (method, L464)
- `TestStaleNameBundleOutputClearedBeforeRerun._run_name_bundle_analysis` (method, L473)
- `TestStaleNameBundleOutputClearedBeforeRerun.test_reusing_the_same_out_dir_without_clearing_no_longer_leaves_stale_provenance` (method, L487)
- `TestStaleNameBundleOutputClearedBeforeRerun.test_clearing_out_dir_before_rerun_removes_stale_provenance` (method, L516)
- `TestClearStaleNameAllBeforeRun` (class, L543)
- `TestClearStaleNameAllBeforeRun.test_clears_existing_name_all_for_bundle_and_name_target` (method, L551)
- `TestClearStaleNameAllBeforeRun.test_clears_for_both_target_too` (method, L563)
- `TestClearStaleNameAllBeforeRun.test_noop_when_name_all_does_not_exist` (method, L572)
- `TestClearStaleNameAllBeforeRun.test_noop_for_config_target` (method, L578)
- `TestClearStaleNameAllBeforeRun.test_noop_for_reference_run_type` (method, L587)
- `TestAnnotationFailureFailsTheSegment` (class, L599)
- `TestAnnotationFailureFailsTheSegment._run` (method, L609)
- `TestAnnotationFailureFailsTheSegment._run._fake_run_step_log` (method, L620)
- `TestAnnotationFailureFailsTheSegment._run._raises` (method, L632)
- `TestAnnotationFailureFailsTheSegment.test_annotation_failure_marks_segment_failed_not_complete` (method, L670)
- `TestAnnotationFailureFailsTheSegment.test_no_annotation_failure_still_marks_segment_complete` (method, L675)
- `TestClearStaleNameAllFailureFailsTheSegment` (class, L683)
- `TestClearStaleNameAllFailureFailsTheSegment._run` (method, L694)
- `TestClearStaleNameAllFailureFailsTheSegment._run._fake_run_step_log` (method, L700)
- `TestClearStaleNameAllFailureFailsTheSegment._run._raises` (method, L713)
- `TestClearStaleNameAllFailureFailsTheSegment.test_persistent_clear_failure_does_not_escape_and_marks_segment_failed` (method, L755)
- `TestClearStaleNameAllFailureFailsTheSegment.test_no_clear_failure_still_marks_segment_complete` (method, L763)

### `tests/test_run_segment_orchestrator_worker_split.py`

**Imports**
- `__future__:annotations`
- `pathlib:Path`
- `run_segment_orchestrator`
- `run_segment_orchestrator`
- `run_segment_orchestrator:compute_worker_split`
- `sys`

**Definitions**
- `test_small_budget_gives_low_single_digits` (function, L11)
- `test_large_budget_gives_expected_split` (function, L17)
- `test_budget_of_one_never_returns_zero` (function, L22)
- `test_explicit_segment_workers_coordinates_domain_workers` (function, L28)
- `test_explicit_segment_workers_never_returns_zero_domain_workers` (function, L36)
- `test_explicit_segment_workers_small_n_gets_larger_domain_share` (function, L42)
- `test_explicit_segment_workers_with_no_cpu_count_falls_back_to_four_budget` (function, L48)
- `test_auto_with_no_cpu_count_falls_back_to_hardcoded_four_four` (function, L56)

### `tests/test_runner_canonicalization.py`

**Imports**
- `core.canonical_items:canonicalize_record`

**Definitions**
- `test_canonicalize_record_merges_all_sources_and_strips_legacy_keys` (function, L4)

### `tests/test_runner_extraction_context.py`

**Imports**
- `domains.identity`
- `json`
- `pathlib:Path`
- `runner.extraction_context:build_extraction_context,operator_deployment_config_path`
- `tests.test_identity_project_info:FakeBuiltInParameter,FakeDoc,FakeGuid,FakeProjectInformation,_ALL_BUILTIN_VALUES`

**Definitions**
- `test_operator_environment_boundary` (function, L11)
- `test_runner_loaded_mapping_reaches_identity_and_signature` (function, L16)

### `tests/test_sentinel_policy.py`

**Imports**
- `glob`
- `os`
- `re`

**Definitions**
- `_repo_root` (function, L10)
- `test_domains_do_not_emit_extra_angle_bracket_tokens` (function, L14)

### `tests/test_sig_hash_policy_builder.py`

**Imports**
- `core.hashing:make_hash`
- `core.record_v2:ITEM_Q_MISSING,ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `core.sig_hash_builder:build_sig_hash_from_policy,apply_sig_hash_policy_to_record`
- `core.sig_hash_policy:load_sig_hash_policies,get_domain_sig_hash_policy`
- `os`

**Definitions**
- `test_generated_sig_hash_policy_loads` (function, L9)
- `test_sig_hash_builder_hashes_allowed_items_from_items_list_order_independent` (function, L15)
- `test_sig_hash_builder_blocks_when_required_not_ok` (function, L32)
- `test_sig_hash_builder_degrades_when_optional_hash_item_not_ok` (function, L52)
- `test_sig_hash_builder_degrades_when_required_not_ok_and_block_disabled` (function, L75)
- `test_sig_hash_builder_prefix_and_first_writer_wins` (function, L88)
- `test_apply_sig_hash_policy_to_record_uses_items_and_writes_sig_basis` (function, L106)
- `test_text_types_sig_hash_excludes_name_includes_behavioral_items` (function, L128)
- `test_object_styles_model_sig_hash_excludes_area9_additions` (function, L159)

### `tests/test_split_export.py`

**Imports**
- `core.features:build_features`
- `core.manifest:build_manifest`
- `pathlib:Path`
- `sys`

**Definitions**
- `_sample_monolithic` (function, L15)
- `test_monolithic_manifest_surface` (function, L50)
- `test_monolithic_features_surface` (function, L60)

### `tests/test_split_named_clusters_and_thresholds.py`

**Imports**
- `__future__:annotations`
- `csv`
- `pathlib:Path`
- `pytest`
- `tools.compute_governance_thresholds:compute_alignment_rates,compute_thresholds,jenks_natural_breaks`
- `tools.patterns_analysis._archive.split_detection_file_level:compute_named_cluster_flags`
- `tools.run_split_detection_all:_inject_split_contract_headers`

**Definitions**
- `_write_csv` (function, L15)
- `test_compute_named_cluster_flags_largest_gap_and_equal_shares` (function, L23)
- `test_compute_named_cluster_flags_uses_raw_share_not_rounded_percentage` (function, L45)
- `test_thresholds_breaks_and_ordering` (function, L56)
- `test_thresholds_reject_non_three_classes` (function, L65)
- `test_compute_alignment_rates_and_contract_header_preserves_is_named_cluster` (function, L71)
- `test_compute_alignment_rates_uses_raw_share_or_size_for_unrounded_result` (function, L114)
- `test_compute_alignment_rates_falls_back_to_percentage_when_size_absent` (function, L130)
- `test_compute_alignment_rates_falls_back_to_percentage_when_size_values_are_invalid` (function, L146)

### `tests/test_suggest_discovery_params.py`

**Imports**
- `__future__:annotations`
- `csv`
- `pathlib:Path`
- `pytest`
- `subprocess`
- `sys`
- `tools.suggest_discovery_params:compute_domain_stats,suggest_sample_size,_cumulative_subset_count,solve_candidate_fields_and_k,suggest_params_for_domain,_emit_command,_load_policy_fields`

**Definitions**
- `_write_csv` (function, L19)
- `test_compute_domain_stats_counts_n_g_f_and_candidates` (function, L27)
- `test_compute_domain_stats_file_hhi_treats_blank_file_id_as_unknown_bucket` (function, L51)
- `test_compute_domain_stats_file_hhi_perfectly_even_distribution` (function, L64)
- `test_compute_domain_stats_file_hhi_fully_concentrated_in_one_file` (function, L73)
- `test_suggest_sample_size_scales_with_diversity_not_just_population` (function, L80)
- `test_suggest_sample_size_never_exceeds_population` (function, L87)
- `test_suggest_sample_size_zero_population` (function, L91)
- `test_cumulative_subset_count_matches_manual_sum` (function, L95)
- `test_solve_candidate_fields_and_k_keeps_all_fields_when_budget_allows` (function, L100)
- `test_solve_candidate_fields_and_k_trims_fields_when_budget_too_small_for_min_k` (function, L106)
- `test_suggest_params_for_domain_required_baseline_never_makes_harsh_infeasible` (function, L115)
- `test_suggest_params_for_domain_harsh_max_k_grows_independently_of_discover_max_k` (function, L132)
- `test_suggest_params_for_domain_harsh_infeasible_only_when_subset_budget_below_one` (function, L145)
- `test_suggest_params_for_domain_optional_items_inflate_extra_pool_not_the_required_floor` (function, L156)
- `test_suggest_params_for_domain_required_items_alone_bump_harsh_max_k_independent_of_optional` (function, L174)
- `test_suggest_params_for_domain_dedupes_extra_pool_when_required_fields_overlap_candidates` (function, L187)
- `test_suggest_params_for_domain_recommends_stratify_by_file_id_on_real_concentration` (function, L224)
- `test_suggest_params_for_domain_no_stratify_recommendation_when_records_evenly_spread` (function, L237)
- `test_suggest_params_for_domain_no_stratify_recommendation_when_no_sampling_needed` (function, L247)
- `test_cli_writes_suggestions_csv_and_reads_required_counts_from_policy` (function, L258)
- `test_load_policy_fields_falls_back_to_selected_fields_like_normalize_policy_block` (function, L285)
- `test_load_policy_fields_prefers_required_fields_over_required_items_like_normalize_policy_block` (function, L302)
- `test_cli_emit_commands_prints_ready_to_run_invocations` (function, L314)
- `test_cli_emit_commands_uses_resolved_phase0_dir_not_unresolved_argument` (function, L330)
- `test_emit_command_single_command_when_discover_and_harsh_k_match` (function, L355)
- `test_emit_command_splits_into_discover_and_harsh_commands_when_k_differs` (function, L367)
- `test_emit_command_forces_greedy_on_harsh_command_when_pareto_infeasible` (function, L382)
- `test_emit_command_single_command_when_harsh_feasible_and_k_matches` (function, L404)
- `test_emit_command_quotes_paths_containing_spaces` (function, L420)

### `tests/test_text_types_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `domains.text_types:TEXT_TYPE_SEMANTIC_KEYS`

**Definitions**
- `_text_types_policy` (function, L10)
- `test_text_types_canonical_evidence_selectors_and_hashing` (function, L15)

### `tests/test_text_types_conversion_convergence.py`

**Imports**
- `domains:text_types`
- `json`
- `tools.migration.reformat_to_flat_items:transform_record`

**Definitions**
- `_Id` (class, L6)
- `_Id.__init__` (method, L7)
- `_Type` (class, L11)
- `_Type.__init__` (method, L12)
- `_extract_record` (function, L17)
- `test_converted_old_and_new_records_converge` (function, L33)

### `tests/test_text_types_flat_items_export.py`

**Imports**
- `domains:text_types`
- `json`

**Definitions**
- `_Id` (class, L5)
- `_Id.__init__` (method, L6)
- `_Type` (class, L10)
- `_Type.__init__` (method, L11)
- `test_text_types_extract_emits_flat_items_only` (function, L16)

### `tests/test_timing_collector.py`

**Imports**
- `core.hashing:make_hash`
- `core.timing_collector:TimingCollector`
- `core:hashing`
- `core:hashing`
- `pytest`
- `threading`
- `time`

**Definitions**
- `TestTimingCollectorBasic` (class, L22)
- `TestTimingCollectorBasic.test_single_timer` (method, L25)
- `TestTimingCollectorBasic.test_multiple_calls_same_label` (method, L37)
- `TestTimingCollectorBasic.test_record_elapsed_single_call` (method, L47)
- `TestTimingCollectorBasic.test_record_elapsed_multiple_calls` (method, L56)
- `TestTimingCollectorBasic.test_unmatched_end_timer_silently_ignored` (method, L65)
- `TestTimingCollectorBasic.test_overlapping_timers` (method, L72)
- `TestTimingCollectorDomainScoping` (class, L84)
- `TestTimingCollectorDomainScoping.test_sub_timings_attributed_to_active_domain` (method, L87)
- `TestTimingCollectorDomainScoping.test_record_elapsed_domain_scoped` (method, L106)
- `TestTimingCollectorDomainScoping.test_record_elapsed_no_active_domain` (method, L119)
- `TestTimingCollectorDomainScoping.test_sub_timings_without_active_domain_not_scoped` (method, L127)
- `TestTimingCollectorDomainScoping.test_multiple_domains_scoped_independently` (method, L138)
- `TestTimingCollectorReport` (class, L164)
- `TestTimingCollectorReport.test_report_has_required_keys` (method, L167)
- `TestTimingCollectorReport.test_summary_totals` (method, L174)
- `TestTimingCollectorReport.test_domain_other_seconds_non_negative` (method, L195)
- `TestTimingCollectorReport.test_empty_report` (method, L207)
- `TestTimingCollectorThreadSafety` (class, L218)
- `TestTimingCollectorThreadSafety.test_concurrent_timers` (method, L221)
- `TestTimingCollectorThreadSafety.test_concurrent_timers.worker` (method, L225)
- `TestTimingCollectorDefensive` (class, L252)
- `TestTimingCollectorDefensive.test_start_timer_with_none_label` (method, L255)
- `TestTimingCollectorDefensive.test_end_timer_with_none_label` (method, L260)
- `TestTimingCollectorDefensive.test_record_elapsed_defensive` (method, L265)
- `TestTimingCollectorDefensive.test_set_active_domain_none` (method, L275)
- `TestHashingTimingIntegration` (class, L282)
- `TestHashingTimingIntegration.test_make_hash_determinism_with_timing` (method, L285)
- `TestHashingTimingIntegration.test_make_hash_timing_cleaned_up` (method, L309)

### `tests/test_units_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `domains.units:UNITS_SEMANTIC_KEYS,UNITS_DOC_SEMANTIC_KEYS,extract_units_doc`
- `enum`
- `json`
- `validators.record_v2:validate_record_v2`

**Definitions**
- `_units_policy` (function, L14)
- `_domain_identity_registry_v2` (function, L19)
- `test_units_join_selectors_and_sig_basis_are_distinct` (function, L24)
- `test_units_boolean_formatting_flags_are_semantic` (function, L61)
- `_FakeDecimalSymbol` (class, L73)
- `_FakeDigitGroupingSymbol` (class, L78)
- `_FakeUnits` (class, L83)
- `_FakeDoc` (class, L89)
- `_FakeDoc.GetUnits` (method, L90)
- `_FakeDocUnitsUnreadable` (class, L94)
- `_FakeDocUnitsUnreadable.GetUnits` (method, L95)
- `test_extract_units_doc_emits_exactly_one_populated_record` (function, L99)
- `test_extract_units_doc_never_blocks_on_read_failure` (function, L125)

### `tests/test_v21_join_policy_compat.py`

**Imports**
- `csv`
- `json`
- `pathlib:Path`
- `subprocess`
- `sys`
- `tools.join_key_discovery.eval:build_candidate_join_key_with_details,normalize_policy_block`

**Definitions**
- `_write_csv` (function, L10)
- `test_flat_required_fields_backward_compatible` (function, L19)
- `test_required_items_alias_and_shape_gating` (function, L36)
- `test_apply_diagnostics_include_discriminator_context` (function, L73)
- `test_optional_items_not_required_or_selected_by_default` (function, L139)
- `test_discover_emits_legacy_compat_shape_and_lists` (function, L150)
- `test_validate_pareto_auto_bumps_max_k_for_required_items` (function, L221)

### `tests/test_view_category_overrides_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`

**Definitions**
- `test_view_category_overrides_join_and_sig_selectors_are_distinct` (function, L9)

### `tests/test_view_filter_applications_view_templates_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,make_identity_item,serialize_identity_items`
- `domains.view_filter_applications_view_templates:_semantic_keys_from_identity_items`
- `sys`
- `types`

**Definitions**
- `_policy` (function, L28)
- `test_view_filter_applications_view_templates_uses_canonical_selectors_for_join_and_sig` (function, L33)

### `tests/test_view_filter_definitions_canonical_selectors.py`

**Imports**
- `core.hashing:make_hash`
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:serialize_identity_items`
- `importlib`
- `sys`
- `types`

**Definitions**
- `_install_fake_revit_db` (function, L13)
- `_install_fake_revit_db._T` (class, L18)
- `test_view_filter_definitions_join_hash_uses_policy_required_keys_only` (function, L36)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges` (function, L73)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeLeafRule` (class, L77)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeLeafRule.__init__` (method, L78)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeElementParameterFilter` (class, L81)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeElementParameterFilter.__init__` (method, L82)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeElementParameterFilter.GetRules` (method, L85)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeFilterInverseRule` (class, L88)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeFilterInverseRule.__init__` (method, L89)
- `test_view_filter_definitions_inverse_rule_not_prefix_and_sig_diverges.FakeFilterInverseRule.GetInnerRule` (method, L92)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list` (function, L135)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeLeafRule` (class, L139)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeElementParameterFilter` (class, L142)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeElementParameterFilter.__init__` (method, L143)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeElementParameterFilter.GetRules` (method, L146)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeFilterInverseRule` (class, L149)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeFilterInverseRule.__init__` (method, L150)
- `test_view_filter_definitions_inverse_rule_unwrapped_from_leaf_rule_list.FakeFilterInverseRule.GetInnerRule` (method, L153)

### `tests/test_view_filter_definitions_empty_domain.py`

**Imports**
- `core.hashing:make_hash`
- `importlib`
- `sys`
- `types`

**Definitions**
- `_install_fake_revit_db` (function, L8)
- `_install_fake_revit_db._T` (class, L13)
- `test_view_filter_definitions_empty_collection_is_not_blocked` (function, L31)

### `tests/test_view_instances_cache_key_consistency.py`

**Imports**
- `domains:view_templates`
- `pathlib:Path`

**Definitions**
- `test_view_instances_cache_key_consistency` (function, L6)

### `tests/test_view_templates_canonical_selectors.py`

**Imports**
- `core.join_key_builder:build_join_key_from_policy`
- `core.join_key_policy:get_domain_join_key_policy,load_join_key_policies`
- `core.record_v2:ITEM_Q_OK,ITEM_Q_MISSING,make_identity_item`

**Definitions**
- `_load_policy` (function, L8)
- `test_view_templates_all_split_domains_have_schemas` (function, L22)
- `test_view_templates_all_split_domains_require_def_hash` (function, L36)
- `test_view_templates_floor_policy` (function, L45)
- `test_view_templates_ceiling_policy` (function, L51)
- `test_view_templates_elevations_policy` (function, L57)
- `test_view_templates_renderings_policy` (function, L63)
- `test_view_templates_schedules_policy` (function, L69)
- `test_view_templates_name_uid_excluded` (function, L75)
- `test_view_templates_join_key_build_with_def_hash` (function, L86)
- `test_view_templates_join_key_missing_def_hash` (function, L108)

### `tools/_archive/join_key_derivation_phase05.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `dataclasses:dataclass`
- `datetime:datetime`
- `datetime:datetime,timezone`
- `glob`
- `hashlib`
- `json`
- `os`
- `typing:Any,Dict,Iterable,List,Optional,Sequence,Tuple`

**Definitions**
- `safe_str` (function, L39)
- `stable_serialize_value` (function, L42)
- `serialize_identity_items` (function, L51)
- `serialize_identity_items._k` (method, L59)
- `md5_utf8_join_pipe` (function, L74)
- `JoinKeyPolicy` (class, L84)
- `_as_str_list` (function, L94)
- `load_join_key_policies` (function, L104)
- `index_items_by_k` (function, L186)
- `choose_candidate_deterministically` (function, L198)
- `is_usable_q` (function, L205)
- `choose_record_handle` (function, L208)
- `select_items_for_policy` (function, L215)
- `read_json` (function, L369)
- `extract_file_id` (function, L373)
- `extract_records` (function, L388)
- `extract_records.walk` (method, L395)
- `derive_join_keys` (function, L416)
- `write_csv` (function, L497)
- `expand_globs` (function, L509)
- `main` (function, L523)

### `tools/acc_scan_dc.py`

**Imports**
- `argparse`
- `csv`
- `datetime:datetime`
- `os`
- `re`

**Definitions**
- `read_rvt_version` (function, L83)
- `scan` (function, L112)
- `load_existing_includes` (function, L175)
- `write_manifest` (function, L188)
- `parse_types` (function, L210)
- `main` (function, L227)

### `tools/acc_sync_dc.py`

**Imports**
- `argparse`
- `csv`
- `ctypes`
- `datetime:datetime`
- `os`
- `sys`
- `threading`
- `time`

**Definitions**
- `is_stub` (function, L59)
- `_trigger_read` (function, L91)
- `hydrate` (function, L101)
- `load_included_entries` (function, L170)
- `write_log` (function, L193)
- `main` (function, L271)

### `tools/analyze_promotion_candidates.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `enterprise_policy:EnterprisePolicy,load_enterprise_policy,write_enterprise_policy_provenance`
- `numpy`
- `pandas`
- `pathlib:Path`

**Definitions**
- `parse_args` (function, L172)
- `require_columns` (function, L318)
- `safe_bool_series` (function, L324)
- `apply_export_cap` (function, L336)
- `compute_seeded_scope` (function, L346)
- `compute_reuse_scope` (function, L399)
- `compute_reuse_scope._union_tokens` (method, L501)
- `main` (function, L574)
- `main._join_labels` (method, L704)
- `main._row_is_unclassified` (method, L741)
- `main._route` (method, L784)

### `tools/apply_join_policy.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `join_key_derivation:md5_utf8_join_pipe,serialize_identity_items`
- `join_key_discovery.eval:build_candidate_join_key_with_details,build_identity_index,normalize_policy_block`
- `json`
- `pathlib:Path`
- `sys`
- `tools.join_key_derivation:md5_utf8_join_pipe,serialize_identity_items`
- `tools.join_key_discovery.eval:build_candidate_join_key_with_details,build_identity_index,normalize_policy_block`
- `typing:Dict,List`

**Definitions**
- `_read_csv` (function, L19)
- `_write_csv` (function, L24)
- `main` (function, L33)
- `main._get_domain_items` (method, L85)

### `tools/apply_name_key_policy.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `core.join_key_policy:load_join_key_policies`
- `core.join_key_policy:load_join_key_policies`
- `core.name_key_builder:build_name_key_for_record`
- `core.name_key_builder:build_name_key_for_record`
- `csv`
- `json`
- `os`
- `pathlib:Path`
- `sys`
- `typing:Any,Dict,List`

**Definitions**
- `_iter_export_paths` (function, L52)
- `_iter_domain_payloads` (function, L76)
- `_rows_for_export` (function, L84)
- `main` (function, L113)

### `tools/archetype/_common.py`

**Imports**
- `__future__:annotations`
- `collections:defaultdict`
- `csv`
- `json`
- `pathlib:Path`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Any,Dict,Iterable,List,Optional,Sequence,Tuple`

**Definitions**
- `log` (function, L36)
- `is_valid_item` (function, L40)
- `read_csv_rows` (function, L49)
- `read_json` (function, L59)
- `atomic_write_csv` (function, L66)
- `atomic_write_json` (function, L80)
- `field_matches` (function, L92)
- `strip_partition_suffix` (function, L108)
- `build_edge_aliases` (function, L116)
- `slugify` (function, L178)

### `tools/archetype/assign_archetype_classifications.py`

**Imports**
- `__future__:annotations`
- `_common:log,atomic_write_csv,atomic_write_json,build_edge_aliases,read_csv_rows,read_json`
- `argparse`
- `collections:defaultdict`
- `pathlib:Path`
- `sys`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `DomainPatternLabelCache` (class, L113)
- `DomainPatternLabelCache.__init__` (method, L116)
- `DomainPatternLabelCache.get` (method, L121)
- `DomainPatternLabelCache._load_domain` (method, L128)
- `_evaluate_signal` (function, L157)
- `_signal_fired_source` (function, L180)
- `main` (function, L206)

### `tools/archetype/build_cross_domain_items.py`

**Imports**
- `__future__:annotations`
- `_common:field_matches,is_valid_item,log,atomic_write_csv,read_csv_rows,read_json`
- `argparse`
- `collections:defaultdict`
- `json`
- `pathlib:Path`
- `typing:Any,Dict,List,Set,Tuple`

**Definitions**
- `_load_identity_items` (function, L74)
- `_parse_vf_categories` (function, L82)
- `_build_structural_rows` (function, L113)
- `_build_dynamic_rows` (function, L153)
- `main` (function, L217)

### `tools/archetype/cluster_archetype_signals.py`

**Imports**
- `__future__:annotations`
- `_common:log,atomic_write_csv,atomic_write_json,read_csv_rows,SCHEMA_VERSION`
- `argparse`
- `collections:defaultdict`
- `datetime:datetime,timezone`
- `jenks_utils:jenks_breaks`
- `pathlib:Path`
- `re`
- `sys`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `_utc_now_iso` (function, L162)
- `_governance_question_from_archetype_id` (function, L166)
- `_build_curated_gq_map` (function, L173)
- `_resolve_governance_question` (function, L189)
- `_bare_signal_name` (function, L193)
- `_cluster_label_stub` (function, L199)
- `_complete_linkage_clusters` (function, L209)
- `_complete_linkage_clusters.pair_value` (method, L225)
- `_build_n_files_classified_lookup` (function, L262)
- `_build_detail_files_lookup` (function, L296)
- `_build_signal_graph` (function, L325)
- `_jenks_threshold_for_values` (function, L440)
- `_derive_coupling_threshold` (function, L460)
- `_apply_threshold` (function, L486)
- `_build_clusters` (function, L499)
- `_build_signal_cluster_map` (function, L541)
- `_rollup_classifications` (function, L554)
- `_compute_file_universe` (function, L636)
- `_build_coverage_summary` (function, L658)
- `main` (function, L686)

### `tools/archetype/compute_cross_domain_cooccurrence.py`

**Imports**
- `__future__:annotations`
- `_common:log,atomic_write_csv,build_edge_aliases,read_csv_rows,read_json`
- `argparse`
- `collections:defaultdict`
- `hashlib`
- `itertools:combinations`
- `pathlib:Path`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `_pattern_id` (function, L104)
- `_eligibility_reason` (function, L110)
- `main` (function, L131)

### `tools/archetype/discover_vfd_edges.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `dataclasses:dataclass`
- `json`
- `pathlib:Path`
- `re`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Any,Dict,Iterable,Iterator,List,Optional,Sequence,Set,Tuple`

**Definitions**
- `RawObservation` (class, L120)
- `ResolvedParam` (class, L129)
- `DomainHint` (class, L137)
- `ParsedCategories` (class, L144)
- `warn` (function, L153)
- `read_json_required` (function, L157)
- `read_json_optional` (function, L167)
- `atomic_write_csv` (function, L180)
- `load_file_metadata` (function, L191)
- `bool_s` (function, L214)
- `find_identity_items_path` (function, L218)
- `is_bad_param_id` (function, L232)
- `row_quality` (function, L237)
- `is_usable_identity_item_value` (function, L241)
- `canonical_param_kind` (function, L250)
- `flush_record` (function, L261)
- `stream_observations` (function, L279)
- `resolve_params` (function, L331)
- `load_bip_hints` (function, L364)
- `hint_target_and_verify` (function, L393)
- `iter_name_contains_rules` (function, L402)
- `infer_domain` (function, L419)
- `parse_category_tokens` (function, L437)
- `sort_category_tokens` (function, L468)
- `parse_category_ints` (function, L472)
- `category_entry_name` (function, L478)
- `parse_categories` (function, L487)
- `normalize_param_name` (function, L536)
- `parse_category_set` (function, L551)
- `_resolve_target_domain_from_categories` (function, L559)
- `_decompose_conflict_to_domains` (function, L591)
- `_category_names_for_ids` (function, L623)
- `_category_flags_for_ids` (function, L634)
- `_find_verify_blocked_candidate` (function, L649)
- `_validate_domain_has_identity_items` (function, L686)
- `build_inventory_rows` (function, L701)
- `build_inventory_rows.append_inventory_row` (method, L771)
- `_category_map_domain_extracted` (function, L909)
- `_candidate_category_details` (function, L926)
- `build_domain_gap_rows` (function, L948)
- `build_edge_rows` (function, L1010)
- `verify_outputs` (function, L1126)
- `print_summary` (function, L1146)
- `build_unresolved_file_rows` (function, L1201)
- `print_unresolved_summary` (function, L1247)
- `parse_args` (function, L1285)
- `main` (function, L1306)

### `tools/archetype/generate_archetype_candidates.py`

**Imports**
- `__future__:annotations`
- `_common:log,atomic_write_json,build_edge_aliases,read_csv_rows,read_json,slugify,SCHEMA_VERSION`
- `argparse`
- `collections:defaultdict`
- `datetime:datetime,timezone`
- `pathlib:Path`
- `sys`
- `typing:Any,Dict,List,Set,Tuple`

**Definitions**
- `_utc_now_iso` (function, L109)
- `_is_vfd_related` (function, L113)
- `_governance_question_hint` (function, L120)
- `_signal_coverage_pct` (function, L136)
- `_collapsed_from_for_edge` (function, L149)
- `main` (function, L158)

### `tools/archetype/generate_reference_graph.py`

**Imports**
- `__future__:annotations`
- `_common:field_matches,is_valid_item,log,atomic_write_json,read_csv_rows,read_json,SCHEMA_VERSION`
- `argparse`
- `datetime:datetime,timezone`
- `hashlib`
- `json`
- `pathlib:Path`
- `typing:Any,Dict,List,Optional,Set`

**Definitions**
- `_utc_now_iso` (function, L69)
- `_check_static_edge_availability` (function, L73)
- `_normalize_param_name` (function, L115)
- `_resolve_param_name` (function, L128)
- `_param_id_slug` (function, L136)
- `_build_dynamic_edges` (function, L147)
- `main` (function, L221)

### `tools/archetype/prepare_archetype_review.py`

**Imports**
- `__future__:annotations`
- `_common:log,atomic_write_csv,field_matches,is_valid_item,read_csv_rows,read_json`
- `argparse`
- `collections:defaultdict`
- `csv`
- `json`
- `pathlib:Path`
- `sys`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `_find_cluster` (function, L181)
- `_all_clusters` (function, L188)
- `_all_cluster_ids` (function, L199)
- `_resolve_param_name` (function, L203)
- `_parse_category_ids` (function, L211)
- `_resolve_category_name` (function, L241)
- `_governance_question_from_cluster_id` (function, L250)
- `_governance_question_from_archetype_id` (function, L261)
- `_build_curated_gq_map` (function, L272)
- `_resolve_governance_question` (function, L289)
- `ClusterContext` (class, L293)
- `ClusterContext.__init__` (method, L296)
- `_build_cluster_context` (function, L307)
- `_load_label_lookup` (function, L367)
- `_load_vfd_resolution` (function, L415)
- `_load_file_path_lookup` (function, L469)
- `_is_named_element` (function, L493)
- `_schedule_file_sort_key` (function, L500)
- `_schedule_row_sort_key` (function, L516)
- `_selected_file_name_status` (function, L521)
- `_select_schedule_rows_for_cluster` (function, L540)
- `_write_review_schedule_outputs` (function, L635)
- `_sort_key` (function, L702)
- `_process_cluster` (function, L716)
- `main` (function, L901)

### `tools/archetype/review/select_archetype_review_files.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `json`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `log` (function, L105)
- `read_csv_rows` (function, L109)
- `read_json` (function, L116)
- `atomic_write_csv` (function, L123)
- `_load_file_paths` (function, L135)
- `_all_cluster_pairs` (function, L152)
- `_cluster_signal_ids` (function, L163)
- `_build_approach_label_map` (function, L174)
- `_build_file_cluster_index` (function, L208)
- `_load_review_csvs` (function, L269)
- `_greedy_cover` (function, L294)
- `_build_output_rows` (function, L355)
- `_identify_gaps` (function, L444)
- `main` (function, L470)

### `tools/archetype/validate_archetype_signals.py`

**Imports**
- `__future__:annotations`
- `_common:log,atomic_write_csv,build_edge_aliases,read_csv_rows,read_json`
- `argparse`
- `collections:Counter,defaultdict`
- `pathlib:Path`
- `typing:Any,Dict,List,Set,Tuple`

**Definitions**
- `_coherence_tier` (function, L153)
- `main` (function, L161)

### `tools/build_results_registry.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `pathlib:Path`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Dict,Iterable,List,Sequence`

**Definitions**
- `read_csv_rows` (function, L30)
- `atomic_write_csv` (function, L40)
- `build_results_registry_rows` (function, L59)
- `_safe_int` (function, L92)
- `write_results_registry` (function, L99)
- `main` (function, L108)

### `tools/build_segment_manifest.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `hashlib`
- `itertools:combinations`
- `na_token:is_na_token`
- `pathlib:Path`
- `re`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Dict,Iterable,List,Sequence`

**Definitions**
- `_read_csv` (function, L43)
- `_atomic_write_csv` (function, L50)
- `_population_hash` (function, L58)
- `_sanitize_folder` (function, L68)
- `_build_membership_rows` (function, L89)
- `_membership_by_segment` (function, L108)
- `_append_note` (function, L120)
- `_invalid_required_value_reason` (function, L130)
- `_invalid_dimension_value_reason` (function, L154)
- `_validate_required_metadata` (function, L169)
- `_normalize_rows` (function, L225)
- `_build_segments` (function, L307)
- `_build_segments._subset_to_id` (method, L322)
- `_build_segments.child_span` (method, L474)
- `_build_registry` (function, L571)
- `_print_summary` (function, L711)
- `main` (function, L746)

### `tools/bundle_analysis/__init__.py`

- No imports or definitions.

### `tools/bundle_analysis/common.py`

**Imports**
- `__future__:annotations`
- `base64`
- `csv`
- `hashlib`
- `math`
- `pathlib:Path`
- `tempfile:NamedTemporaryFile`
- `time`
- `typing:Dict,Iterable,List,Optional,Sequence`

**Definitions**
- `retry_fs_op` (function, L17)
- `read_csv_rows` (function, L42)
- `atomic_write_csv` (function, L47)
- `resolve_analysis_run_id` (function, L58)
- `derive_scope_key` (function, L67)
- `compute_effective_support` (function, L76)
- `make_bundle_id` (function, L81)

### `tools/bundle_analysis/name_projection_adapter.py`

**Imports**
- `.common:atomic_write_csv,read_csv_rows`
- `__future__:annotations`
- `common:atomic_write_csv,read_csv_rows`
- `core.name_key_coverage:coverage_class`
- `csv`
- `pathlib:Path`
- `sys`
- `sys`
- `typing:Dict,List,Optional,Set`

**Definitions**
- `normalize_export_run_id` (function, L55)
- `stage_name_projection_analysis_dir` (function, L103)
- `emit_name_target_provenance` (function, L220)
- `annotate_name_target_combined_files` (function, L317)

### `tools/bundle_analysis/placeholder_exclusions.py`

**Imports**
- `.common:atomic_write_csv,read_csv_rows`
- `.placeholder_exclusions_legacy:compute_placeholder_exclusions`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `common:atomic_write_csv,read_csv_rows`
- `json`
- `pathlib:Path`
- `placeholder_exclusions_legacy:compute_placeholder_exclusions`
- `re`
- `sys`

**Definitions**
- `t` (function, L19)
- `lg` (function, L21)
- `_to_int` (function, L31)
- `_load_governance_roles` (function, L39)
- `_load_existing_overrides` (function, L50)
- `_choose_threshold` (function, L64)
- `compute_placeholder_exclusions` (function, L80)
- `main` (function, L92)

### `tools/bundle_analysis/placeholder_exclusions_legacy.py`

**Imports**
- `.common:atomic_write_csv,read_csv_rows`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `common:atomic_write_csv,read_csv_rows`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `_is_truthy` (function, L24)
- `_largest_gap_threshold` (function, L28)
- `compute_placeholder_exclusions` (function, L46)
- `_parse_args` (function, L110)
- `main` (function, L117)

### `tools/bundle_analysis/reference_bundle.py`

**Imports**
- `__future__:annotations`
- `datetime:date`
- `json`
- `pathlib:Path`
- `tempfile:NamedTemporaryFile`
- `typing:Dict,List`

**Definitions**
- `_escape_control_chars_in_json_strings` (function, L10)
- `write_sidecar` (function, L60)
- `load_and_validate` (function, L103)

### `tools/bundle_analysis/run_bundle_analysis.py`

**Imports**
- `..jenks_utils:jenks_breaks`
- `.common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows,resolve_analysis_run_id,retry_fs_op`
- `.name_projection_adapter:DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,emit_name_target_provenance,stage_name_projection_analysis_dir`
- `.placeholder_exclusions:compute_placeholder_exclusions`
- `.reference_bundle:load_and_validate`
- `.step0_discover_populations:discover_populations`
- `.step1_membership_matrix:build_membership_matrix`
- `.step2_find_bundles:find_bundles_for_domain`
- `.step2b_bundle_share_profile:build_bundle_share_profile`
- `.step3_build_dag:build_dag_for_domain`
- `.step4_difference_sets:emit_stub`
- `.step5_classify_patterns:emit_stub`
- `.step6_classify_files:emit_stub`
- `.step7_overlap_report:emit_stub`
- `.step_compare:run_compare_for_domain`
- `__future__:annotations`
- `argparse`
- `common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows,resolve_analysis_run_id,retry_fs_op`
- `concurrent.futures:ProcessPoolExecutor,ThreadPoolExecutor,as_completed`
- `csv`
- `jenks_utils:jenks_breaks`
- `name_projection_adapter:DEFAULT_NAME_PROJECTION_ANALYSIS_RUN_ID,emit_name_target_provenance,stage_name_projection_analysis_dir`
- `pathlib:Path`
- `placeholder_exclusions:compute_placeholder_exclusions`
- `reference_bundle:load_and_validate`
- `shutil`
- `step0_discover_populations:discover_populations`
- `step1_membership_matrix:build_membership_matrix`
- `step2_find_bundles:find_bundles_for_domain`
- `step2b_bundle_share_profile:build_bundle_share_profile`
- `step3_build_dag:build_dag_for_domain`
- `step4_difference_sets:emit_stub`
- `step5_classify_patterns:emit_stub`
- `step6_classify_files:emit_stub`
- `step7_overlap_report:emit_stub`
- `step_compare:run_compare_for_domain`
- `subprocess`
- `sys`
- `time`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `_view_out_dir` (function, L67)
- `_ensure_latent_purgeable` (function, L72)
- `_emit_meta_scatter_thresholds` (function, L94)
- `_load_purgeable_only_set` (function, L153)
- `_run_pipeline_once` (function, L180)
- `_run_step2_to_step7` (function, L288)
- `run_bundle_analysis` (function, L354)
- `_validate_name_target_constraints` (function, L900)
- `run_bundle_analysis_for_target` (function, L936)
- `_parse_args` (function, L1129)
- `main` (function, L1174)

### `tools/bundle_analysis/step0_discover_populations.py`

**Imports**
- `.common:ROW_KEY_DOMAINS,SCHEMA_VERSION,atomic_write_csv,derive_scope_key,make_bundle_id,read_csv_rows,resolve_analysis_run_id`
- `.step2_find_bundles:compute_auto_threshold`
- `.utils:find_root_bundles`
- `__future__:annotations`
- `argparse`
- `common:ROW_KEY_DOMAINS,SCHEMA_VERSION,atomic_write_csv,derive_scope_key,make_bundle_id,read_csv_rows,resolve_analysis_run_id`
- `math`
- `pathlib:Path`
- `step2_find_bundles:compute_auto_threshold`
- `sys`
- `typing:Dict,List,Optional,Set`
- `utils:find_root_bundles`

**Definitions**
- `_pattern_summary` (function, L40)
- `_population_id` (function, L44)
- `_select_populations` (function, L53)
- `_collapse_subset_related_roots` (function, L79)
- `discover_populations` (function, L121)
- `discover_populations._merge` (method, L440)
- `_parse_args` (function, L460)
- `main` (function, L474)

### `tools/bundle_analysis/step1_membership_matrix.py`

**Imports**
- `.common:SCHEMA_VERSION,atomic_write_csv,derive_scope_key,read_csv_rows,resolve_analysis_run_id`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `common:SCHEMA_VERSION,atomic_write_csv,derive_scope_key,read_csv_rows,resolve_analysis_run_id`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `_load_population_file_ids` (function, L18)
- `build_membership_matrix` (function, L48)
- `_parse_args` (function, L236)
- `main` (function, L249)

### `tools/bundle_analysis/step2_find_bundles.py`

**Imports**
- `..jenks_utils:jenks_breaks`
- `.common:SCHEMA_VERSION,atomic_write_csv,compute_effective_support,make_bundle_id,read_csv_rows`
- `.utils:find_closed_itemsets`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `common:SCHEMA_VERSION,atomic_write_csv,compute_effective_support,make_bundle_id,read_csv_rows`
- `jenks_utils:jenks_breaks`
- `json`
- `math`
- `pathlib:Path`
- `sys`
- `time`
- `typing:Any,Dict,List,Optional`
- `utils:find_closed_itemsets`

**Definitions**
- `_percentile` (function, L33)
- `compute_auto_threshold` (function, L50)
- `find_bundles_for_domain` (function, L118)
- `_parse_args` (function, L378)
- `main` (function, L387)

### `tools/bundle_analysis/step2b_bundle_share_profile.py`

**Imports**
- `.common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows,resolve_analysis_run_id`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows,resolve_analysis_run_id`
- `pathlib:Path`
- `statistics`
- `sys`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `_is_true` (function, L19)
- `_fmt_float` (function, L23)
- `build_bundle_share_profile` (function, L27)
- `_parse_args` (function, L233)
- `main` (function, L243)

### `tools/bundle_analysis/step3_build_dag.py`

**Imports**
- `.common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict,deque`
- `common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `build_dag_for_domain` (function, L18)
- `build_dag_for_domain._depth` (method, L136)
- `_parse_args` (function, L228)
- `main` (function, L235)

### `tools/bundle_analysis/step4_difference_sets.py`

**Imports**
- `.common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows`
- `__future__:annotations`
- `argparse`
- `common:SCHEMA_VERSION,atomic_write_csv,read_csv_rows`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional`

**Definitions**
- `emit_stub` (function, L17)
- `_parse_args` (function, L140)
- `main` (function, L147)

### `tools/bundle_analysis/step5_classify_patterns.py`

**Imports**
- `.common:atomic_write_csv`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict,deque`
- `common:atomic_write_csv`
- `csv`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional`

**Definitions**
- `emit_stub` (function, L17)
- `emit_stub._norm` (method, L41)
- `emit_stub._truthy` (method, L44)
- `_parse_args` (function, L223)
- `main` (function, L230)

### `tools/bundle_analysis/step6_classify_files.py`

**Imports**
- `.common:SCHEMA_VERSION`
- `.common:atomic_write_csv`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `common:SCHEMA_VERSION`
- `common:atomic_write_csv`
- `csv`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional`

**Definitions**
- `emit_stub` (function, L17)
- `emit_stub._read_csv_rows` (method, L27)
- `emit_stub._safe_int` (method, L36)
- `emit_stub._ancestors` (method, L109)
- `emit_stub._select_primary` (method, L124)
- `_parse_args` (function, L216)
- `main` (function, L223)

### `tools/bundle_analysis/step7_overlap_report.py`

**Imports**
- `.common:atomic_write_csv`
- `__future__:annotations`
- `argparse`
- `common:atomic_write_csv`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional`

**Definitions**
- `emit_stub` (function, L17)
- `_parse_args` (function, L52)
- `main` (function, L59)

### `tools/bundle_analysis/step_compare.py`

**Imports**
- `.common:atomic_write_csv,read_csv_rows,resolve_analysis_run_id`
- `__future__:annotations`
- `collections:Counter`
- `common:atomic_write_csv,read_csv_rows,resolve_analysis_run_id`
- `pathlib:Path`
- `threading`
- `typing:Dict,List,Optional,Set`

**Definitions**
- `_compute_gap_rows` (function, L30)
- `run_compare_for_domain` (function, L133)

### `tools/bundle_analysis/utils.py`

**Imports**
- `__future__:annotations`
- `itertools:combinations`
- `time`
- `typing:Dict,FrozenSet,List,Set,Tuple`

**Definitions**
- `_supporting_files_by_superset` (function, L8)
- `find_closed_itemsets` (function, L15)
- `find_root_bundles` (function, L83)

### `tools/compare_cross_segment.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:Counter`
- `collections:defaultdict`
- `concurrent.futures:ProcessPoolExecutor,as_completed`
- `csv`
- `datetime:datetime,timezone`
- `enterprise_policy:EnterprisePolicy,load_enterprise_policy,write_enterprise_policy_provenance`
- `hashlib`
- `itertools:combinations`
- `jenks_utils:jenks_breaks`
- `na_token:is_blank_or_na,ENTERPRISE_BC_BOOKKEEPING_TOKENS`
- `os`
- `pathlib:Path`
- `sys`
- `tempfile:NamedTemporaryFile`
- `time`
- `typing:Dict,Iterable,List,Optional,Sequence,Set,Tuple`

**Definitions**
- `read_csv_rows` (function, L172)
- `atomic_write_csv` (function, L180)
- `_classify_delta` (function, L499)
- `_bool_str` (function, L535)
- `_role_key` (function, L539)
- `_is_generic_role` (function, L543)
- `_role_matches` (function, L547)
- `_usage_interpretable_for_role` (function, L553)
- `_recommended_primary_view` (function, L559)
- `_comparison_role_semantics` (function, L565)
- `_classify_governance_state` (function, L583)
- `load_manifest` (function, L618)
- `load_registry` (function, L636)
- `load_file_metadata` (function, L643)
- `load_membership` (function, L651)
- `validate_membership_against_manifest` (function, L676)
- `load_comparison_registry` (function, L773)
- `_segment_status_complete` (function, L787)
- `build_comparison_registry_rows` (function, L791)
- `comparison_is_stale` (function, L849)
- `segment_output_dir` (function, L881)
- `bundle_analysis_dir` (function, L895)
- `domain_patterns_path` (function, L899)
- `pattern_presence_file_path` (function, L903)
- `_load_export_run_ids_for_segment` (function, L907)
- `discover_domains_for_segment` (function, L922)
- `resolve_join_hashes` (function, L983)
- `load_pattern_labels` (function, L1024)
- `get_role_jh_set` (function, L1066)
- `load_file_join_hashes` (function, L1113)
- `_segment_domain_source_status` (function, L1207)
- `_load_segment_file_join_hashes_with_status` (function, L1241)
- `_project_label_for_file` (function, L1272)
- `build_union_inventory_rows` (function, L1277)
- `_safe_pct` (function, L1447)
- `_reuse_bucket_for` (function, L1451)
- `build_pattern_reuse_distribution_rows` (function, L1485)
- `build_pattern_reuse_summary_rows` (function, L1583)
- `load_segment_join_hash_union` (function, L1619)
- `load_bundle_join_hash_set` (function, L1632)
- `annotate_bundle_overlap` (function, L1676)
- `_pct` (function, L1692)
- `_fmt` (function, L1703)
- `_mean` (function, L1707)
- `_min` (function, L1711)
- `_comparison_status` (function, L1726)
- `_cardinality_shape` (function, L1734)
- `_file_count_ratio` (function, L1744)
- `_cardinality_fields` (function, L1750)
- `_union_similarity` (function, L1758)
- `compare_directed_file` (function, L1773)
- `compare_symmetric_file` (function, L1828)
- `_normalize_bc_label` (function, L1947)
- `_bc_of` (function, L1963)
- `_client_of` (function, L1967)
- `_is_enterprise_client` (function, L1973)
- `_is_enterprise_bc` (function, L1977)
- `_scope_level` (function, L1981)
- `_is_client_wide_rollup` (function, L2005)
- `_is_standard_role` (function, L2018)
- `detect_stale_ancestor_encoding` (function, L2034)
- `_build_ancestor_map` (function, L2082)
- `_build_ancestor_map._immediate_parents` (method, L2122)
- `_build_ancestor_map._walk` (method, L2131)
- `_is_lineage_related` (function, L2153)
- `_compute_containment_thresholds` (function, L2207)
- `write_population_containment_thresholds` (function, L2274)
- `_population_containment_map` (function, L2310)
- `_is_population_contained` (function, L2358)
- `_same_unit` (function, L2387)
- `discover_within_segment` (function, L2399)
- `_redundant_child_segment_id` (function, L2448)
- `_resolve_runnable_segment` (function, L2480)
- `_scope_override_key` (function, L2514)
- `_stash_scope_override` (function, L2518)
- `discover_sibling_segments` (function, L2567)
- `_is_client_only_project_segment` (function, L2644)
- `discover_cross_client` (function, L2674)
- `discover_client_cross_bc` (function, L2742)
- `discover_parent_siblings` (function, L2778)
- `discover_governance_chain` (function, L2840)
- `discover_governance_chain._key` (method, L2849)
- `discover_governance_chain._disc` (method, L2917)
- `discover_governance_chain._disc_match` (method, L2920)
- `discover_governance_chain._collection` (method, L2929)
- `discover_governance_chain._is_collection_rollup` (method, L2956)
- `discover_governance_chain._collection_match` (method, L2959)
- `discover_within_project` (function, L3160)
- `deduplicate_pairs` (function, L3206)
- `drop_legacy_siblings_covered_by_peer_comparisons` (function, L3236)
- `make_comparison_run_id` (function, L3283)
- `run_pair` (function, L3311)
- `_run_pair_domain` (function, L3729)
- `_build_summary_row` (function, L3760)
- `build_governance_state_outputs` (function, L3884)
- `_build_pooled_row` (function, L4041)
- `run_pooled_comparison` (function, L4257)
- `run_pooled_comparison._domains_for` (method, L4333)
- `run_pooled_comparison._emit_for_groups` (method, L4338)
- `_matrix_group_id_from_values` (function, L4403)
- `_matrix_group_id` (function, L4412)
- `_label_by_project_group` (function, L4421)
- `_jaccard_sets` (function, L4439)
- `_cosine_similarity` (function, L4446)
- `build_explicit_matrix_outputs` (function, L4458)
- `build_explicit_matrix_outputs.add_manifest` (method, L4474)
- `build_explicit_matrix_outputs.add_matrix` (method, L4484)
- `segment_is_runnable` (function, L4648)
- `build_pair_domain_work_items` (function, L4665)
- `sort_summary_rows` (function, L4699)
- `sort_pair_detail_rows` (function, L4708)
- `resolve_worker_count` (function, L4731)
- `main` (function, L4748)

### `tools/compare_governance_populations.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `bundle_analysis.common:atomic_write_csv,read_csv_rows`
- `collections:defaultdict`
- `compare_cross_segment:compare_symmetric_file,compare_directed_file,make_comparison_run_id`
- `datetime:datetime,timezone`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Set,Tuple`

**Definitions**
- `load_join_hashes_by_domain` (function, L96)
- `_files_for_population` (function, L111)
- `_disc_match` (function, L128)
- `discover_same_role_peer_pairs` (function, L134)
- `discover_directed_tc_to_project_pairs` (function, L180)
- `discover_generic_pairs` (function, L224)
- `_pop_export_run_ids` (function, L251)
- `run_comparisons` (function, L255)
- `run_comparisons._base_row` (method, L276)
- `main` (function, L329)

### `tools/compare_templates_stand-alone/compare_view_templates_stand-alone.py`

**Imports**
- `argparse`
- `collections:defaultdict`
- `json`
- `pathlib:Path`
- `sys`

**Definitions**
- `_extract_records` (function, L52)
- `_get_label_display` (function, L71)
- `_get_label_component` (function, L78)
- `_get_view_type_family` (function, L85)
- `_get_template_uid` (function, L97)
- `_get_vco_template_uid` (function, L105)
- `_get_vco_category_path` (function, L113)
- `_parse_vt_signature` (function, L121)
- `_parse_vco_items` (function, L133)
- `_index_vco_by_template` (function, L155)
- `_jaccard` (function, L169)
- `_best_match_index` (function, L178)
- `_make_pair` (function, L212)
- `match_templates` (function, L233)
- `match_templates._sort_key` (method, L300)
- `_diff_dicts` (function, L316)
- `_diff_vco` (function, L337)
- `_print_pair` (function, L388)
- `_print_report` (function, L439)
- `_esc` (function, L554)
- `_vt_diff_rows` (function, L558)
- `_vco_rows` (function, L588)
- `_pair_html` (function, L628)
- `_build_html` (function, L676)
- `main` (function, L715)

### `tools/compute_governance_thresholds.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `pathlib:Path`
- `typing:Dict,List`

**Definitions**
- `_read_csv` (function, L25)
- `_write_csv` (function, L30)
- `compute_alignment_rates` (function, L38)
- `compute_alignment_rates._parse_size` (method, L46)
- `jenks_natural_breaks` (function, L92)
- `compute_thresholds` (function, L156)
- `main` (function, L188)

### `tools/compute_latent_purgeable.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `pathlib:Path`
- `sys`
- `typing:Callable,Dict,List,Optional,Set,Tuple`

**Definitions**
- `_is_vfd_ref_key` (function, L139)
- `_is_vfa_filter_ref_key` (function, L146)
- `_build_matcher` (function, L153)
- `_is_purgeable_true` (function, L169)
- `_is_purgeable_false` (function, L173)
- `_make_zero_counts` (function, L177)
- `_select_chains` (function, L181)
- `_domains_of_interest` (function, L195)
- `_load_records` (function, L207)
- `_accumulate_item_rows` (function, L268)
- `_load_chain_ref_data` (function, L307)
- `_classify` (function, L423)
- `_write_output` (function, L487)
- `_fmt_consumers` (function, L499)
- `_print_summary` (function, L524)
- `main` (function, L600)

### `tools/discover_hash_policy.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `discover_join_policy:_read_csv,_write_csv,_sample_domain_records,_stratified_sample,_pick_candidate_fields,_without_excluded,_pareto_search_adapter`
- `join_key_discovery.eval:build_identity_index,normalize_policy_block,score_candidate`
- `join_key_discovery.greedy:discover_greedy`
- `json`
- `pathlib:Path`
- `tools.discover_join_policy:_read_csv,_write_csv,_sample_domain_records,_stratified_sample,_pick_candidate_fields,_without_excluded,_pareto_search_adapter`
- `tools.join_key_discovery.eval:build_identity_index,normalize_policy_block,score_candidate`
- `tools.join_key_discovery.greedy:discover_greedy`
- `typing:Dict,List`

**Definitions**
- `_resolve_phase0_dir` (function, L19)
- `_load_items` (function, L39)
- `_domain_rows` (function, L45)
- `_run_target` (function, L72)
- `main` (function, L157)

### `tools/discover_join_policy.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `hashlib`
- `join_key_discovery.eval:build_identity_index,normalize_policy_block,score_candidate`
- `join_key_discovery.greedy:discover_greedy`
- `json`
- `math`
- `pareto_joinkey_search:pareto_search`
- `pathlib:Path`
- `sys`
- `tools.join_key_discovery.eval:build_identity_index,normalize_policy_block,score_candidate`
- `tools.join_key_discovery.greedy:discover_greedy`
- `tools.pareto_joinkey_search:pareto_search`
- `typing:Dict,List,Sequence`

**Definitions**
- `_read_csv` (function, L21)
- `_pareto_search_adapter` (function, L26)
- `_diagnostics_domain_suffix` (function, L37)
- `_write_csv` (function, L67)
- `_rank_all` (function, L76)
- `_rank_all._rank` (method, L84)
- `_sample_domain_records` (function, L91)
- `_stratified_sample` (function, L97)
- `_stratified_sample._group_rank` (method, L204)
- `_full_population_verify` (function, L247)
- `_pick_candidate_fields` (function, L295)
- `_dedupe` (function, L308)
- `_without_excluded` (function, L320)
- `_to_legacy_shape_gating` (function, L327)
- `main` (function, L337)
- `main._get_domain_items` (method, L438)

### `tools/domain_authority.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:Counter`
- `csv`
- `dataclasses:dataclass`
- `hashlib`
- `json`
- `math`
- `os`
- `pathlib:Path`
- `statistics`
- `sys`
- `typing:Any,Dict,Iterable,List,Optional,Tuple`

**Definitions**
- `RunConfig` (class, L37)
- `RunConfig.from_json` (method, L53)
- `load_json` (function, L75)
- `project_id_from_fp` (function, L80)
- `_extract_record_sig_hashes_v2` (function, L92)
- `_jaccard_set` (function, L142)
- `_jaccard_multiset` (function, L158)
- `_md5_utf8` (function, L187)
- `_strip_revit_uid_tail` (function, L191)
- `_canonical_item_str` (function, L204)
- `_semantic_record_sig_hash` (function, L214)
- `_domain_payload_from_fp` (function, L250)
- `_semantic_domain_multiset_hash` (function, L274)
- `extract_domains_summary` (function, L305)
- `extract_domains_summary.ingest` (method, L316)
- `hhi` (function, L356)
- `classify_convergence` (function, L360)
- `authority_confidence` (function, L368)
- `authority_scope_recommendation` (function, L377)
- `classify_authority_outcome` (function, L387)
- `write_csv` (function, L427)
- `analyze` (function, L440)
- `main` (function, L951)

### `tools/emit_element_dominance.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `jenks_utils:jenks_breaks`
- `pathlib:Path`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Dict,List,Optional,Tuple`

**Definitions**
- `_read_csv_rows` (function, L23)
- `_split_label` (function, L28)
- `_write_csv_atomic` (function, L35)
- `emit_element_dominance` (function, L46)
- `emit_element_dominance._compute_breaks` (method, L131)
- `emit_element_dominance._bucket` (method, L137)
- `main` (function, L296)

### `tools/enterprise_policy.py`

**Imports**
- `__future__:annotations`
- `dataclasses:dataclass`
- `hashlib`
- `json`
- `os`
- `pathlib:Path`
- `typing:Any,Dict,Optional,Union`

**Definitions**
- `EnterprisePolicy` (class, L17)
- `EnterprisePolicy.is_enterprise` (method, L28)
- `EnterprisePolicy.provenance` (method, L31)
- `EnterprisePolicy.provenance_bytes` (method, L41)
- `_label` (function, L46)
- `load_enterprise_policy` (function, L52)
- `normalize_enterprise_label` (function, L86)
- `write_enterprise_policy_provenance` (function, L91)

### `tools/export_bundle_pattern_detail.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `jenks_utils:jenks_breaks`
- `pathlib:Path`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Any,Dict,Iterable,Iterator,List,Optional,Sequence,Set,Tuple`

**Definitions**
- `_read_csv` (function, L35)
- `_atomic_write_csv` (function, L43)
- `_int_safe` (function, L70)
- `_float_safe` (function, L77)
- `_compute_domain_bundle_threshold` (function, L97)
- `_apply_bundle_threshold` (function, L144)
- `_resolve_segment_paths` (function, L157)
- `_discover_domains` (function, L230)
- `_load_pattern_map` (function, L244)
- `_load_representative_map` (function, L273)
- `_iter_identity_csv` (function, L292)
- `_load_identity_items` (function, L317)
- `_load_label_population` (function, L339)
- `_process_domain` (function, L359)
- `main` (function, L570)

### `tools/export_to_flat_tables.py`

**Imports**
- `argparse`
- `csv`
- `hashlib`
- `json`
- `os`
- `pathlib:Path`
- `re`
- `typing:Any,Dict,Iterable,List,Optional,Tuple`

**Definitions**
- `_is_scalar` (function, L16)
- `_safe_str` (function, L20)
- `_iter_json_paths` (function, L28)
- `_get_contract` (function, L54)
- `_get_domain_payload` (function, L59)
- `_get_domain_records` (function, L64)
- `_iter_domains` (function, L78)
- `_read_json` (function, L99)
- `main` (function, L107)
- `main._write_csv` (method, L388)
- `main._safe_name` (method, L397)

### `tools/extract_segment_subtree.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `dataclasses:dataclass`
- `math`
- `pathlib:Path`
- `re`
- `sys`
- `tempfile:NamedTemporaryFile`
- `typing:Dict,Iterable,List,Optional,Sequence,Set,Tuple`

**Definitions**
- `norm` (function, L44)
- `norm_fold` (function, L50)
- `sanitize_label` (function, L54)
- `atomic_write_csv` (function, L59)
- `Blocked` (class, L73)
- `load_manifest` (function, L90)
- `find_seeds_by_search` (function, L115)
- `find_seeds_by_id` (function, L127)
- `expand_ancestors` (function, L139)
- `resolve_endpoint_columns` (function, L198)
- `row_matches` (function, L217)
- `NumericStats` (class, L249)
- `NumericStats.__init__` (method, L252)
- `NumericStats.add` (method, L258)
- `NumericStats.emit` (method, L273)
- `FileSpec` (class, L291)
- `FileSpec.supports_summary` (method, L298)
- `ProcessResult` (class, L365)
- `_update_aggregate` (function, L376)
- `_write_summary` (function, L387)
- `process_file` (function, L408)
- `parse_args` (function, L521)
- `main` (function, L554)

### `tools/extractor.py`

**Imports**
- `__future__:annotations`
- `base64`
- `collections:defaultdict`
- `concurrent.futures:ProcessPoolExecutor,as_completed`
- `csv`
- `datetime:datetime,timezone`
- `hashlib`
- `json`
- `label_synthesis.label_resolver:find_near_duplicate_merges,load_annotations,load_label_population,load_llm_cache,resolve_pattern_label`
- `os`
- `pathlib:Path`
- `re`
- `subprocess`
- `sys`
- `time`
- `time`
- `typing:Any,Dict,Iterable,List,Optional,Set,Tuple`

**Definitions**
- `_safe_str` (function, L55)
- `_utc_now_iso` (function, L63)
- `_iter_export_files` (function, L67)
- `_read_json` (function, L107)
- `_merge_index_details` (function, L115)
- `_iter_domains` (function, L123)
- `_file_id` (function, L136)
- `_get_tool_version` (function, L144)
- `_identity_metadata` (function, L158)
- `_extract_acc_project_label` (function, L190)
- `_model_label_from_path` (function, L202)
- `_norm_central_path` (function, L212)
- `_b32_sha1_16` (function, L231)
- `_load_governance_role_rules` (function, L237)
- `_infer_governance_role` (function, L272)
- `_stable_pattern_id` (function, L289)
- `_write_csv` (function, L308)
- `_read_existing_csv` (function, L317)
- `_sort_rows` (function, L326)
- `compute_hhi_from_shares` (function, L330)
- `compute_effective_clusters` (function, L362)
- `_fmt_metric` (function, L373)
- `compute_attribute_concentration_metrics` (function, L377)
- `_iter_object_style_name_candidates` (function, L386)
- `_remap_object_style_domain` (function, L408)
- `_remap_vco_domain` (function, L424)
- `_load_identity_items_by_record` (function, L435)
- `_load_label_resolution_inputs` (function, L474)
- `_load_semantic_groups` (function, L515)
- `_derive_unit_system` (function, L549)
- `_process_one_domain` (function, L617)
- `emit_records` (function, L983)
- `emit_analysis` (function, L1274)
- `emit_analysis._merge_result` (method, L1358)

### `tools/gen_map.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `ast`
- `collections:defaultdict,deque`
- `dataclasses:dataclass`
- `os`
- `pathlib:Path`
- `re`
- `sys`
- `typing:Iterable,Sequence`

**Definitions**
- `DefInfo` (class, L26)
- `DefInfo.name` (method, L33)
- `CallSite` (class, L38)
- `FileInfo` (class, L46)
- `_slug` (function, L52)
- `_excluded_dir` (function, L57)
- `iter_py_files` (function, L61)
- `_dotted_name` (function, L74)
- `_Analyzer` (class, L85)
- `_Analyzer.__init__` (method, L86)
- `_Analyzer._visit_definition` (method, L92)
- `_Analyzer.visit_FunctionDef` (method, L98)
- `_Analyzer.visit_AsyncFunctionDef` (method, L101)
- `_Analyzer.visit_ClassDef` (method, L104)
- `_Analyzer.visit_Call` (method, L110)
- `_imports` (function, L120)
- `build_index` (function, L131)
- `_definitions` (function, L147)
- `_calls` (function, L155)
- `_write` (function, L163)
- `write_code_map` (function, L168)
- `write_symbol_index` (function, L186)
- `_definition_id` (function, L202)
- `_display_definition` (function, L206)
- `_trace_roots` (function, L210)
- `write_trace_map` (function, L220)
- `build_parser` (function, L262)
- `main` (function, L272)

### `tools/generate_governance_narrative.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `compare_cross_segment:GOVERNANCE_STATE_DIRECTED_TYPES,_resolve_runnable_segment`
- `csv`
- `datetime:date`
- `enterprise_policy:load_enterprise_policy,write_enterprise_policy_provenance`
- `governance_evidence_package:GENERATOR_IDENTITY,GENERATOR_ROLE,PACKAGE_SCHEMA_VERSION,EVIDENCE_MAP_SCHEMA_VERSION,FINDINGS_SCHEMA_VERSION,FILE_INVENTORY_SCHEMA_VERSION,AUTHORITY_AUTHORITATIVE_DETERMINISTIC_EVIDENCE,AUTHORITY_CONTROLLED_INTERPRETATION,AUTHORITY_CONVENIENCE_SUMMARY,FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,FINDING_FIDELITY_EXACT,FINDING_STATUS_SUPPORTED,FINDING_STATUS_QUESTION_NOT_CLAIM,build_evidence_map,build_file_inventory_document,build_findings_document,build_package_health,build_package_manifest,comparison_type_coverage,inventory_export_directory_files,write_json`
- `governance_policy:DEFAULT_POLICY_DIR,load_governance_policy`
- `pathlib:Path`
- `shutil`
- `statistics`
- `sys`
- `typing:Optional`

**Definitions**
- `pf` (function, L135)
- `pct` (function, L142)
- `fmt` (function, L148)
- `_warn_unrecognized_comparison_types` (function, L154)
- `read_csv` (function, L170)
- `_disc_label` (function, L233)
- `detect_bundle_schema` (function, L283)
- `normalise_summary_schema` (function, L308)
- `normalise_summary_schema.alias` (method, L315)
- `_col` (function, L359)
- `_resolved_col_name` (function, L365)
- `_col_union_or_pairwise` (function, L372)
- `used_view_falls_back_to_legacy` (function, L390)
- `_is_unscoped_segment` (function, L406)
- `_target_scope_label` (function, L450)
- `_group1_scope_pair` (function, L495)
- `load_client_sectors` (function, L566)
- `load_corpus_counts` (function, L589)
- `_has_renderable_cascade_signal` (function, L773)
- `build_cascade` (function, L788)
- `build_cascade.mean_or_none` (method, L1372)
- `build_cascade._largest_scope_bucket` (method, L1375)
- `score_reliability` (function, L1547)
- `apply_governance_policy` (function, L1929)
- `apply_governance_policy.th` (method, L1973)
- `apply_governance_policy.ct` (method, L2017)
- `apply_governance_policy.at_` (method, L2031)
- `_state_value` (function, L2077)
- `_has_material_state_exception` (function, L2083)
- `_has_group1_bc_pooled_evidence` (function, L2099)
- `assign_tier` (function, L2117)
- `detect_anomalies` (function, L2187)
- `build_client_summary` (function, L2433)
- `build_client_summary._confirmed_non_healthcare` (method, L2508)
- `build_bc_summary` (function, L2680)
- `build_bc_summary._note_bc_file` (method, L2754)
- `_pick` (function, L2924)
- `_truthy` (function, L2933)
- `_add_float` (function, L2937)
- `_state_bucket` (function, L2943)
- `_mean` (function, L3005)
- `_merge_state_buckets` (function, L3009)
- `_finalize_state_bucket` (function, L3021)
- `build_governance_state_summary` (function, L3077)
- `load_delta_summary` (function, L3200)
- `render_header` (function, L3232)
- `render_evidence_authority_header` (function, L3343)
- `render_evidence_authority_header._static_doc_pointer` (method, L3369)
- `render_governance_state_model` (function, L3417)
- `render_domain_tiers` (function, L3444)
- `render_generic_baseline_scope_section` (function, L3600)
- `render_group1_scope_section` (function, L3647)
- `render_discipline_section` (function, L3711)
- `_format_domain_items` (function, L3828)
- `_client_onboarding_profile` (function, L3837)
- `render_onboarding_section` (function, L3907)
- `render_client_section` (function, L3943)
- `render_enterprise_section` (function, L4001)
- `render_bc_section` (function, L4048)
- `render_governance_state_section` (function, L4108)
- `render_governance_state_section.top_by` (method, L4124)
- `render_delta_section` (function, L4200)
- `build_union_breadth_by_domain` (function, L4250)
- `render_union_reuse_summary` (function, L4392)
- `_matrix_value_status_blocked` (function, L4580)
- `_manifest_bullets_for_matrix` (function, L4590)
- `_unordered_project_pairs` (function, L4606)
- `_render_portfolio_footprint_identity` (function, L4634)
- `_render_portfolio_density_similarity` (function, L4669)
- `_render_portfolio_density_similarity._shape_note` (method, L4703)
- `_render_portfolio_pool_containment` (function, L4722)
- `_render_portfolio_fragmentation` (function, L4783)
- `render_project_portfolio_section` (function, L4834)
- `render_bc_composition_section` (function, L4901)
- `render_client_bc_distribution_section` (function, L4949)
- `_classify_domains_for_findings` (function, L5062)
- `_passive_inheritance_risk_domains` (function, L5109)
- `_low_coherence_clients` (function, L5164)
- `build_structured_findings` (function, L5191)
- `build_structured_findings.next_id` (method, L5214)
- `build_structured_findings.domain_support` (method, L5218)
- `build_structured_findings.add_domain_finding` (method, L5225)
- `render_findings_and_recommendations` (function, L5372)
- `render_findings_and_recommendations._domain_ids` (method, L5381)
- `build_comparison_completeness` (function, L5458)
- `build_comparison_completeness._key` (method, L5510)
- `build_comparison_completeness._state_key` (method, L5516)
- `render_limitations` (function, L5597)
- `_narrative_for_inventory_entry` (function, L5697)
- `render_file_inventory_brief_section` (function, L5733)
- `render_governance_brief` (function, L5757)
- `main` (function, L5848)

### `tools/generate_name_key_patterns.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `core.name_key_coverage:COVERAGE_EXCLUDED,COVERAGE_NATIVE,COVERAGE_PHASES_REDUNDANT,COVERAGE_WIDENED,ELIGIBLE_DOMAINS,EXCLUDED_DOMAINS,coverage_class,exclusion_reason`
- `csv`
- `pathlib:Path`
- `pattern_id_utils:build_clusters,pattern_label,rank_clusters`
- `shutil`
- `sys`
- `typing:Any,Dict,Iterable,List`

**Definitions**
- `_read_csv` (function, L104)
- `_write_csv` (function, L110)
- `emit_config_patterns` (function, L119)
- `build_name_patterns` (function, L130)
- `build_name_membership` (function, L171)
- `build_domain_coverage` (function, L195)
- `emit_name_patterns` (function, L231)
- `_assert_no_pattern_id_collision` (function, L248)
- `main` (function, L258)

### `tools/generate_sig_hash_policy.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `json`
- `pathlib:Path`
- `typing:Any,Dict`

**Definitions**
- `build_policy` (function, L10)
- `main` (function, L50)

### `tools/governance/standards_governance_report.py`

**Imports**
- `__future__:annotations`
- `collections:Counter,defaultdict`
- `dataclasses:dataclass`
- `datetime:datetime`
- `json`
- `pathlib:Path`
- `sys`
- `typing:Any,Dict,Iterable,List,Optional`

**Definitions**
- `ProjectExport` (class, L26)
- `StandardsGovernanceAnalyzer` (class, L32)
- `StandardsGovernanceAnalyzer.__init__` (method, L33)
- `StandardsGovernanceAnalyzer.load_exports` (method, L38)
- `StandardsGovernanceAnalyzer.analyze_baseline_drift` (method, L51)
- `StandardsGovernanceAnalyzer.analyze_template_overrides` (method, L96)
- `StandardsGovernanceAnalyzer.identify_common_patterns` (method, L146)
- `StandardsGovernanceAnalyzer.generate_report` (method, L191)
- `StandardsGovernanceAnalyzer._get_canonical_baselines` (method, L200)
- `StandardsGovernanceAnalyzer._build_summary` (method, L222)
- `_get_identity_value` (function, L235)
- `_build_html_report` (function, L243)
- `_row_template` (function, L416)
- `_pattern_row_template` (function, L427)
- `build_table` (function, L442)
- `build_pattern_table` (function, L456)
- `main` (function, L470)

### `tools/governance_evidence_package.py`

**Imports**
- `__future__:annotations`
- `csv`
- `datetime:datetime,timezone`
- `json`
- `pathlib:Path`
- `typing:Optional`

**Definitions**
- `_utc_now_iso` (function, L92)
- `write_json` (function, L98)
- `build_findings_document` (function, L102)
- `build_package_manifest` (function, L118)
- `comparison_type_coverage` (function, L225)
- `build_package_health` (function, L250)
- `_artifact` (function, L458)
- `_sibling_scan_fields` (function, L516)
- `build_evidence_map` (function, L539)
- `build_evidence_map.p` (method, L563)
- `build_evidence_map._output_local_path` (method, L567)
- `_classify_scalar` (function, L1494)
- `_column_dtype` (function, L1519)
- `_scan_csv_file` (function, L1538)
- `inventory_export_directory_files` (function, L1573)
- `build_file_inventory_document` (function, L1605)

### `tools/governance_manifest.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `bundle_analysis.common:atomic_write_csv,read_csv_rows`
- `collections:defaultdict`
- `enterprise_policy:EnterprisePolicy,load_enterprise_policy,write_enterprise_policy_provenance`
- `hashlib`
- `na_token:ENTERPRISE_BC_BOOKKEEPING_TOKENS`
- `pathlib:Path`
- `run_extract_all:_check_governance_field_completeness`
- `sys`
- `typing:Dict,List,Tuple`

**Definitions**
- `normalize_business_center_label` (function, L111)
- `_is_enterprise_client` (function, L123)
- `_governance_role_key` (function, L130)
- `_is_generic_role` (function, L134)
- `compute_scope_key` (function, L142)
- `_governance_id` (function, L164)
- `_population_hash` (function, L169)
- `_normalize_manual_metadata` (function, L174)
- `build_governance_populations` (function, L226)
- `main` (function, L334)

### `tools/governance_policy.py`

**Imports**
- `__future__:annotations`
- `json`
- `pathlib:Path`
- `typing:Optional`

**Definitions**
- `_load_profile` (function, L50)
- `load_governance_policy` (function, L67)

### `tools/governance_relationships.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `bundle_analysis.common:atomic_write_csv,read_csv_rows`
- `collections:defaultdict`
- `enterprise_policy:load_enterprise_policy,write_enterprise_policy_provenance`
- `governance_manifest:_normalize_manual_metadata,_governance_role_key,normalize_business_center_label`
- `hashlib`
- `na_token:is_blank_or_na`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Tuple`

**Definitions**
- `_project_key` (function, L99)
- `_project_id` (function, L111)
- `build_relationships_rows` (function, L116)
- `build_bc_client_matrix_rows` (function, L227)
- `build_client_bc_matrix_rows` (function, L269)
- `main` (function, L294)

### `tools/inspect_lft_similarity.py`

**Imports**
- `argparse`
- `collections:defaultdict,Counter`
- `csv`
- `os`
- `os`
- `re`
- `re`
- `sys`

**Definitions**
- `extract_category` (function, L48)
- `extract_family_name` (function, L51)
- `normalise_name` (function, L54)
- `token_overlap` (function, L59)
- `match_dim_bucket` (function, L88)
- `parse_raw_values` (function, L98)
- `format_range` (function, L111)
- `is_classification_param` (function, L139)
- `load_unit_systems` (function, L150)
- `load_lft_records` (function, L165)
- `stream_parameter_rows` (function, L208)
- `build_dim_summaries` (function, L269)
- `build_class_profiles` (function, L289)
- `build_subgroups` (function, L347)
- `build_exact_match_table` (function, L389)
- `build_name_cluster_table` (function, L458)
- `write_detail_file` (function, L537)
- `write_detail_file_kv` (function, L563)
- `write_csv` (function, L605)
- `build_family_file_detail` (function, L615)
- `main` (function, L676)

### `tools/jenks_utils.py`

**Imports**
- `__future__:annotations`
- `typing:List`

**Definitions**
- `jenks_breaks` (function, L6)

### `tools/join_key_derivation.py`

**Imports**
- `_archive.join_key_derivation_phase05:*`
- `pathlib:Path`
- `sys`

### `tools/join_key_discovery/eval.py`

**Imports**
- `__future__:annotations`
- `collections:defaultdict`
- `typing:Any,Dict,Iterable,List,Sequence,Tuple`

**Definitions**
- `_norm` (function, L7)
- `build_identity_index` (function, L11)
- `_listish` (function, L36)
- `_lookup_shape_cfg` (function, L44)
- `normalize_policy_block` (function, L68)
- `build_candidate_join_key_with_details` (function, L95)
- `build_candidate_join_key` (function, L143)
- `score_candidate` (function, L154)

### `tools/join_key_discovery/greedy.py`

**Imports**
- `.eval:score_candidate`
- `__future__:annotations`
- `typing:Any,Dict,List,Sequence`

**Definitions**
- `_score` (function, L8)
- `discover_greedy` (function, L18)

### `tools/join_key_discovery/materials_joinkey_discover.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `hashlib`
- `pathlib:Path`
- `typing:Dict,List,Optional,Set,Tuple`

**Definitions**
- `_read_csv` (function, L39)
- `_write_csv` (function, L45)
- `_md5` (function, L53)
- `_load_materials` (function, L61)
- `_is_system_material` (function, L98)
- `_partition_rows` (function, L110)
- `_extract_name` (function, L120)
- `_extract_sig` (function, L125)
- `_extract_uid` (function, L130)
- `_build_key` (function, L135)
- `_load_class_map` (function, L143)
- `_compute_metrics` (function, L178)
- `_top_patterns` (function, L229)
- `_build_key_files` (function, L259)
- `_print_tiered` (function, L272)
- `_tiered_csv_rows` (function, L294)
- `_print_summary_table` (function, L321)
- `discover` (function, L346)
- `discover.key_uid` (method, L365)
- `discover.key_name` (method, L366)
- `discover.key_sig` (method, L367)
- `discover.key_name_sig` (method, L369)
- `discover.key_class` (method, L373)
- `discover.key_name_class` (method, L376)
- `main` (function, L454)

### `tools/label_synthesis/__init__.py`

- No imports or definitions.

### `tools/label_synthesis/build_identity_items_lookup.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Optional,Tuple`

**Definitions**
- `_find_file` (function, L55)
- `_sniff_item_columns` (function, L63)
- `build_lookup` (function, L92)
- `build_lookup._process_item_rows` (method, L198)
- `main` (function, L316)

### `tools/label_synthesis/build_label_population.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `pathlib:Path`
- `sys`
- `typing:Dict,Set,Tuple`

**Definitions**
- `build_label_population` (function, L37)
- `main` (function, L143)

### `tools/label_synthesis/build_semantic_groups.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `datetime:datetime,timezone`
- `json`
- `pathlib:Path`
- `re`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `build_grouping_prompt` (function, L48)
- `_peer_block` (function, L71)
- `_normalize_text_size` (function, L83)
- `_parse_text_type_label_fields` (function, L125)
- `_prompt_text_types` (function, L150)
- `_prompt_arrowheads` (function, L230)
- `_prompt_line_patterns` (function, L289)
- `_prompt_line_styles` (function, L322)
- `_normalise_fill_angle` (function, L384)
- `_is_fill_angle_close` (function, L388)
- `_infer_fill_geometry_description` (function, L396)
- `_prompt_fill_patterns` (function, L432)
- `_utc_now_iso` (function, L557)
- `_read_csv_rows` (function, L561)
- `_load_analysis_run_id` (function, L566)
- `_load_cache` (function, L576)
- `_save_cache` (function, L596)
- `_write_json` (function, L602)
- `_resolve_export_target` (function, L608)
- `_write_export_batches` (function, L613)
- `_load_export_progress` (function, L634)
- `_save_export_progress` (function, L652)
- `_derive_element_label` (function, L661)
- `_load_pattern_rows` (function, L677)
- `_load_pattern_to_record_pk` (function, L716)
- `_resolve_identity_items_source` (function, L732)
- `_load_identity_items_by_record` (function, L751)
- `_line_pattern_segment_keys` (function, L772)
- `_is_nullish` (function, L777)
- `_extract_behavioral_props` (function, L782)
- `_parse_grouping_response` (function, L847)
- `_normalize_import_payload` (function, L879)
- `_call_grouping_llm` (function, L896)
- `build_semantic_groups` (function, L900)
- `main` (function, L1137)

### `tools/label_synthesis/domain_prompts/__init__.py`

- No imports or definitions.

### `tools/label_synthesis/domain_prompts/arrowheads.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `build_prompt` (function, L206)
- `_detect_record_class` (function, L318)
- `_fmt_size` (function, L334)
- `_format_identity_items` (function, L345)

### `tools/label_synthesis/domain_prompts/dimension_types.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `_get_shape` (function, L276)
- `_shape_context_note` (function, L283)
- `build_prompt` (function, L325)
- `_fmt_accuracy` (function, L522)
- `_fmt_witness` (function, L533)
- `_format_identity_items` (function, L537)

### `tools/label_synthesis/domain_prompts/fill_patterns.py`

**Imports**
- `__future__:annotations`
- `re`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `build_prompt` (function, L178)
- `_is_opaque_fallback` (function, L286)
- `_normalise_angle` (function, L294)
- `_is_angle_close` (function, L298)
- `_infer_geometry_description` (function, L306)
- `_extract_grid_geometry` (function, L353)
- `_get_identity_value` (function, L378)

### `tools/label_synthesis/domain_prompts/line_patterns.py`

**Imports**
- `__future__:annotations`
- `re`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `_strip_import_prefix` (function, L125)
- `_is_opaque_name` (function, L133)
- `build_prompt` (function, L146)
- `_format_identity_items` (function, L269)

### `tools/label_synthesis/domain_prompts/line_styles.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `_strip_lines_prefix` (function, L204)
- `_fmt_color` (function, L218)
- `build_prompt` (function, L233)
- `_format_identity_items` (function, L330)

### `tools/label_synthesis/domain_prompts/text_types.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `build_prompt` (function, L190)
- `_fmt_size` (function, L307)
- `_fmt_color` (function, L318)
- `_format_identity_items` (function, L322)

### `tools/label_synthesis/domain_prompts/view_filter_definitions.py`

**Imports**
- `__future__:annotations`
- `re`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `_is_opaque_name` (function, L135)
- `_get_value` (function, L149)
- `_collect_rules` (function, L162)
- `_op_short` (function, L182)
- `_format_rule_summary` (function, L189)
- `build_prompt` (function, L211)

### `tools/label_synthesis/label_resolver.py`

**Imports**
- `__future__:annotations`
- `csv`
- `csv`
- `importlib`
- `json`
- `math`
- `os`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `resolve_pattern_label` (function, L60)
- `find_near_duplicate_merges` (function, L135)
- `_extract_kv_typed` (function, L195)
- `_are_near_duplicates` (function, L222)
- `_within_tolerance` (function, L243)
- `_try_synopsis` (function, L254)
- `_get_synopsis_formatter` (function, L264)
- `_try_modal` (function, L279)
- `is_fragmented` (function, L324)
- `load_llm_cache` (function, L357)
- `save_llm_cache` (function, L367)
- `load_annotations` (function, L373)
- `load_label_population` (function, L390)

### `tools/label_synthesis/patch_all_domain_patterns.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `json`
- `math`
- `pathlib:Path`
- `shutil`
- `sys`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_read_csv` (function, L64)
- `_write_csv` (function, L72)
- `_load_cache` (function, L83)
- `_load_label_population` (function, L91)
- `_try_modal` (function, L119)
- `_patch_one` (function, L147)
- `_find_targets` (function, L259)
- `main` (function, L297)

### `tools/label_synthesis/patch_domain_patterns_labels.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `json`
- `math`
- `os`
- `pathlib:Path`
- `shutil`
- `sys`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_read_csv` (function, L56)
- `_write_csv` (function, L64)
- `_load_cache` (function, L71)
- `_load_label_population` (function, L78)
- `_try_modal` (function, L114)
- `patch` (function, L142)
- `main` (function, L302)

### `tools/label_synthesis/synopsis_formatters/__init__.py`

- No imports or definitions.

### `tools/label_synthesis/synopsis_formatters/arrowheads.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L17)
- `_inches_to_fraction` (function, L68)

### `tools/label_synthesis/synopsis_formatters/dimension_types.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L41)
- `_shape_label` (function, L104)
- `_shape_family` (function, L123)
- `_accuracy_label` (function, L138)
- `_witness_label` (function, L188)
- `_center_marks_label` (function, L204)
- `_decoration_label` (function, L215)
- `_extract_kv` (function, L236)

### `tools/label_synthesis/synopsis_formatters/fill_patterns.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L30)

### `tools/label_synthesis/synopsis_formatters/line_patterns.py`

**Imports**
- `__future__:annotations`
- `re`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `format_synopsis` (function, L30)

### `tools/label_synthesis/synopsis_formatters/line_styles.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L40)
- `_format_rgb` (function, L77)

### `tools/label_synthesis/synopsis_formatters/object_styles_annotation.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L54)
- `_format_rgb` (function, L86)

### `tools/label_synthesis/synopsis_formatters/object_styles_model.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L60)
- `_format_rgb` (function, L108)

### `tools/label_synthesis/synopsis_formatters/phase_filters.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L44)

### `tools/label_synthesis/synopsis_formatters/text_types.py`

**Imports**
- `__future__:annotations`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `format_synopsis` (function, L16)
- `_inches_to_fraction` (function, L66)

### `tools/label_synthesis/synthesize_fragmented_labels.py`

**Imports**
- `.label_resolver:is_fragmented,load_label_population,load_llm_cache,save_llm_cache,MODAL_THRESHOLD`
- `__future__:annotations`
- `anthropic`
- `argparse`
- `concurrent.futures:ThreadPoolExecutor,as_completed`
- `csv`
- `csv`
- `datetime:date`
- `importlib`
- `json`
- `os`
- `pathlib:Path`
- `re`
- `sys`
- `threading`
- `time`
- `typing:Any,Dict,List,Optional,Tuple`
- `urllib.error`
- `urllib.request`

**Definitions**
- `_collect_union_bundle_join_hashes` (function, L53)
- `_load_governance_join_hashes` (function, L136)
- `_strip_json_fences` (function, L258)
- `_call_llm` (function, L266)
- `_groups_vocab_path` (function, L359)
- `load_groups_vocab` (function, L364)
- `save_groups_vocab` (function, L378)
- `_load_identity_items_from_csv` (function, L392)
- `_load_representative_identity_items` (function, L432)
- `_get_domain_records` (function, L484)
- `_load_domain_prompt_module` (function, L505)
- `_write_review_csv` (function, L535)
- `synthesize` (function, L570)
- `synthesize._process_join_hash` (method, L740)
- `_generic_system_prompt` (function, L848)
- `_generic_build_prompt` (function, L856)
- `main` (function, L879)

### `tools/lib/__init__.py`

- No imports or definitions.

### `tools/lib/diff_engine.py`

**Imports**
- `argparse`
- `collections:defaultdict`
- `csv`
- `datetime:datetime`
- `json`
- `pathlib:Path`
- `tools.lib.domain_profile:DomainProfile`
- `typing:Any,Dict,List,Tuple`

**Definitions**
- `normalize_name` (function, L72)
- `ensure_str` (function, L76)
- `load_json` (function, L82)
- `get_domain_payload` (function, L87)
- `get_label_and_quality` (function, L98)
- `get_items` (function, L112)
- `extract_records` (function, L132)
- `build_index` (function, L157)
- `parse_name_map` (function, L187)
- `index_items_by_key` (function, L203)
- `compare_entries` (function, L210)
- `write_csv` (function, L277)
- `_pair_name` (function, L286)
- `rebuild_unmatched` (function, L298)
- `_validate_paths` (function, L328)
- `run_comparison` (function, L345)

### `tools/lib/domain_profile.py`

**Imports**
- `dataclasses:dataclass,field`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `ResolutionSpec` (class, L6)
- `DomainProfile` (class, L17)
- `DomainProfile.build_resolution_maps` (method, L28)
- `DomainProfile._build_maps_for_file` (method, L35)
- `DomainProfile._get_domain_payload` (method, L59)
- `DomainProfile._extract_name` (method, L72)
- `DomainProfile.resolve_value` (method, L84)
- `DomainProfile._key_matches_spec` (method, L97)
- `DomainProfile.classify_bucket` (method, L104)
- `DomainProfile._classify_sig_basis` (method, L116)
- `DomainProfile._classify_phase2` (method, L127)
- `DomainProfile.is_key_valid_for_domain` (method, L148)
- `DomainProfile.reconstruct` (method, L154)
- `DomainProfile.get_deferred_domains` (method, L162)
- `DomainProfile.get_hash_resolution_meta` (method, L165)

### `tools/lib/vt_profile.py`

**Imports**
- `dataclasses:dataclass`
- `tools.lib.domain_profile:DomainProfile,ResolutionSpec`

**Definitions**
- `_get_domain_payload` (function, L73)
- `_load_domain_records` (function, L85)
- `_load_vco_records` (function, L96)
- `_get_phase2_cosmetic_value` (function, L103)
- `_get_identity_item_value` (function, L111)
- `_index_vco_by_template` (function, L119)
- `_normalize_template_name` (function, L146)
- `_build_template_lookup` (function, L150)
- `_get_template_vco` (function, L160)
- `_index_object_styles_by_row_key` (function, L167)
- `_extract_graphic_fields` (function, L184)
- `_extract_object_style_baseline_fields` (function, L214)
- `_is_non_ok_quality` (function, L230)
- `_is_default_vco_value` (function, L242)
- `_extract_active_vco_fields` (function, L265)
- `_reconstruct_effective` (function, L275)
- `_build_synthetic_items_for_pair` (function, L296)
- `ViewTemplateDomainProfile` (class, L347)
- `ViewTemplateDomainProfile.__post_init__` (method, L350)
- `ViewTemplateDomainProfile.get_deferred_domains` (method, L362)
- `ViewTemplateDomainProfile.get_hash_resolution_meta` (method, L365)
- `ViewTemplateDomainProfile.classify_bucket` (method, L373)
- `ViewTemplateDomainProfile.reconstruct` (method, L389)
- `make_vt_profile` (function, L460)

### `tools/migration/compress_fingerprint_json.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `json`
- `pathlib:Path`
- `shutil`
- `sys`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_load_json` (function, L44)
- `_write_compact` (function, L49)
- `_is_already_compact` (function, L61)
- `_fmt_kb` (function, L80)
- `_compress_file` (function, L88)
- `_find_json_files` (function, L142)
- `main` (function, L158)

### `tools/migration/extract_first_record.py`

**Imports**
- `__future__:annotations`
- `json`
- `pathlib:Path`
- `sys`
- `typing:Any`

**Definitions**
- `extract_first_records` (function, L34)
- `main` (function, L61)

### `tools/migration/migrate_materials_identity_items.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `hashlib`
- `json`
- `pathlib:Path`
- `shutil`
- `sys`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_load_json` (function, L60)
- `_write_json` (function, L65)
- `_iter_materials_records` (function, L77)
- `_get_identity_items` (function, L106)
- `_migrate_record` (function, L121)
- `_migrate_file` (function, L171)
- `_find_json_files` (function, L230)
- `main` (function, L241)

### `tools/migration/reformat_to_flat_items.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:Counter`
- `core.canonical_items:build_flat_items`
- `json`
- `os`
- `pathlib:Path`
- `sys`
- `typing:Any`

**Definitions**
- `parse_domains` (function, L22)
- `transform_record` (function, L29)
- `process_payload` (function, L59)
- `iter_input_files` (function, L103)
- `main` (function, L112)

### `tools/na_token.py`

**Imports**
- `__future__:annotations`
- `re`

**Definitions**
- `is_na_token` (function, L17)
- `is_blank_or_na` (function, L26)

### `tools/pairwise_analysis.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `itertools:combinations`
- `pathlib:Path`
- `typing:Dict,List,Tuple,Any`

**Definitions**
- `load_csv` (function, L15)
- `write_csv` (function, L20)
- `main` (function, L33)

### `tools/pareto_joinkey_search.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `dataclasses:dataclass`
- `itertools`
- `join_key_discovery.eval:score_candidate`
- `json`
- `math`
- `pandas`
- `pathlib:Path`
- `tools.join_key_discovery.eval:score_candidate`
- `typing:Dict,Iterable,List,Sequence,Tuple`

**Definitions**
- `pareto_search` (function, L37)
- `_dedupe_preserve_order` (function, L103)
- `_load_join_key_policy` (function, L114)
- `_rank_challengers_from_wide` (function, L144)
- `make_record_key` (function, L174)
- `compute_v_norm` (function, L184)
- `pareto_front` (function, L207)
- `dominates` (function, L230)
- `EvalConfig` (class, L249)
- `build_wide_kv_table` (function, L255)
- `sample_records` (function, L283)
- `eval_subset` (function, L306)
- `eval_subset.row_join` (method, L317)
- `main` (function, L358)
- `main.iter_subsets` (method, L605)
- `main.iter_subsets_policy_respecting` (method, L609)
- `main._run_search` (method, L639)
- `_shape_label` (function, L846)

### `tools/pattern_id_utils.py`

**Imports**
- `__future__:annotations`
- `base64`
- `hashlib`
- `typing:Any,Dict,List,Set,Tuple`

**Definitions**
- `stable_pattern_id` (function, L23)
- `pattern_label` (function, L38)
- `rank_clusters` (function, L42)
- `build_clusters` (function, L51)

### `tools/patterns_analysis/__init__.py`

- No imports or definitions.

### `tools/patterns_analysis/_archive/annotate_cluster_labels.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `label_synthesis.label_resolver:_try_synopsis`
- `os`
- `pandas`
- `pathlib:Path`
- `sys`
- `tempfile`
- `typing:Any`

**Definitions**
- `_is_unknown` (function, L38)
- `_split_common_path_parts` (function, L45)
- `_first_non_noise` (function, L54)
- `_parse_bool` (function, L61)
- `_clean_text` (function, L69)
- `resolve_provenance_label` (function, L75)
- `_identity_items_from_representatives` (function, L102)
- `_key_suffix` (function, L118)
- `_extract_cluster_id` (function, L124)
- `resolve_content_label` (function, L134)
- `_iter_domains` (function, L191)
- `annotate_cluster_labels` (function, L199)
- `main` (function, L290)

### `tools/patterns_analysis/_archive/apply_join_keys_by_ids.py`

**Imports**
- `.io:load_exports,get_domain_records,load_records_records_with_identity`
- `__future__:annotations`
- `argparse`
- `csv`
- `hashlib`
- `json`
- `os`
- `pandas`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `md5_utf8_join_pipe` (function, L21)
- `extract_identity_map` (function, L26)
- `compute_join_hash` (function, L44)
- `apply_join_keys_by_ids` (function, L59)
- `main` (function, L165)

### `tools/patterns_analysis/_archive/attributes.py`

**Imports**
- `.index:DomainIndex`
- `__future__:annotations`
- `dataclasses:dataclass`
- `typing:Dict,List,Sequence`

**Definitions**
- `AttrStabilityRow` (class, L27)
- `StressRow` (class, L40)
- `compute_attr_stability` (function, L55)
- `compute_stress_rank` (function, L64)

### `tools/patterns_analysis/_archive/backfill_cluster_label_inputs.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:Counter`
- `json`
- `os`
- `pandas`
- `pathlib:Path`
- `tempfile`
- `typing:Any,Dict,List,Tuple`

**Definitions**
- `_file_map` (function, L25)
- `_load_json` (function, L37)
- `_normalize_parts` (function, L42)
- `_common_components_from_paths` (function, L55)
- `_extract_cluster_common_path_parts` (function, L70)
- `_extract_cluster_common_path_parts._extract_parts_from_entry` (method, L79)
- `_extract_cluster_common_path_parts._add_cluster_entry` (method, L103)
- `_update_cluster_summary` (function, L153)
- `_build_discriminator_lookup` (function, L201)
- `_build_cluster_representative_items` (function, L219)
- `_validate_domain_inputs` (function, L290)
- `_iter_domains` (function, L301)
- `run_backfill` (function, L311)
- `main` (function, L347)

### `tools/patterns_analysis/_archive/build_reference_standards.py`

**Imports**
- `.io:load_exports,get_domain_records,_read_csv_rows`
- `__future__:annotations`
- `argparse`
- `json`
- `os`
- `pandas`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Set`

**Definitions**
- `build_reference_standards_from_clusters` (function, L18)
- `main` (function, L125)

### `tools/patterns_analysis/_archive/calibrate_join_key_gates.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `json`
- `numpy`
- `os`
- `pandas`

**Definitions**
- `calibrate` (function, L7)
- `main` (function, L63)

### `tools/patterns_analysis/_archive/compare.py`

**Imports**
- `.index:DomainIndex`
- `__future__:annotations`
- `dataclasses:dataclass`
- `typing:Any,Dict,Optional,Set,Tuple`

**Definitions**
- `ChangeCounts` (class, L10)
- `_phase2_items_map` (function, L27)
- `classify_pair` (function, L82)

### `tools/patterns_analysis/_archive/derive_join_keys_by_ids.py`

**Imports**
- `.io:load_exports,get_domain_records,load_records_records_with_identity`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `hashlib`
- `json`
- `os`
- `pandas`
- `random`
- `re`
- `statistics:median`
- `tools.patterns_analysis.domain_identity_contract:DomainIdentityContract`
- `typing:Any,Dict,Iterable,List,Optional,Set,Tuple`

**Definitions**
- `md5_utf8_join_pipe` (function, L35)
- `is_eligible_join_key_item` (function, L53)
- `extract_identity_map` (function, L56)
- `compute_join_hash_for_record` (function, L75)
- `evaluate_keyset` (function, L89)
- `compute_coverage` (function, L138)
- `jaccard_similarity` (function, L145)
- `sample_records_by_file` (function, L155)
- `evaluate_gates` (function, L176)
- `greedy_select_keys` (function, L204)
- `greedy_select_keys.usable` (method, L220)
- `derive_join_keys_by_ids` (function, L275)
- `main` (function, L537)

### `tools/patterns_analysis/_archive/domain_identity_contract.py`

**Imports**
- `json`
- `pathlib:Path`

**Definitions**
- `DomainIdentityContract` (class, L7)
- `DomainIdentityContract.__init__` (method, L8)
- `DomainIdentityContract.load` (method, L12)
- `DomainIdentityContract.allowed_keys_for_domain` (method, L18)
- `DomainIdentityContract.required_keys_for_domain` (method, L29)
- `DomainIdentityContract.is_key_allowed` (method, L35)

### `tools/patterns_analysis/_archive/emit_intradomain_definition.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `dataclasses:dataclass`
- `json`
- `os`
- `pandas`
- `typing:Dict,List`

**Definitions**
- `IDS` (class, L23)
- `_make_ids_ids` (function, L28)
- `emit_ids_artifacts` (function, L36)
- `main` (function, L126)

### `tools/patterns_analysis/_archive/index.py`

**Imports**
- `__future__:annotations`
- `dataclasses:dataclass`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `DomainIndex` (class, L8)
- `_get_join_hash` (function, L34)
- `_phase2_items_by_k` (function, L48)
- `build_domain_index` (function, L103)

### `tools/patterns_analysis/_archive/intradomain_summary.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `__future__:annotations`
- `argparse`
- `collections:Counter,defaultdict`
- `json`
- `os`
- `pandas`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_safe_str` (function, L27)
- `_extract_identity_items` (function, L33)
- `_profile_records` (function, L57)
- `_pick_representative` (function, L85)
- `_load_export_by_file_id` (function, L93)
- `build_intradomain_summary` (function, L100)
- `main` (function, L218)

### `tools/patterns_analysis/_archive/io.py`

**Imports**
- `__future__:annotations`
- `csv`
- `dataclasses:dataclass`
- `json`
- `os`
- `pathlib:Path`
- `typing:Any,Dict,Iterator,List,Optional`
- `typing:Set,Tuple`

**Definitions**
- `ExportFile` (class, L13)
- `_ordered_export_names` (function, L21)
- `iter_json_paths` (function, L35)
- `load_export_file` (function, L49)
- `load_exports` (function, L63)
- `get_contract` (function, L82)
- `get_domains_map` (function, L88)
- `get_domain_envelope` (function, L94)
- `get_domain_payload` (function, L115)
- `get_domain_records` (function, L121)
- `get_run_provenance` (function, L141)
- `_read_csv_rows` (function, L165)
- `load_records_file_paths` (function, L174)
- `load_records_sig_profiles` (function, L198)
- `load_records_records_with_identity` (function, L244)

### `tools/patterns_analysis/_archive/pareto_join_keys_by_ids.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `dataclasses:dataclass`
- `hashlib`
- `json`
- `os`
- `pandas`
- `re`
- `tools.patterns_analysis.domain_identity_contract:DomainIdentityContract`
- `typing:Any,Dict,Iterable,List,Optional,Tuple`

**Definitions**
- `is_eligible_join_key_item` (function, L58)
- `md5_utf8_join_pipe` (function, L66)
- `extract_identity_map` (function, L71)
- `compute_join_hash_for_record` (function, L89)
- `Candidate` (class, L103)
- `Candidate.key_str` (method, L114)
- `evaluate_keyset` (function, L118)
- `dominates` (function, L180)
- `pareto_front` (function, L187)
- `choose_from_front` (function, L208)
- `build_candidate_pool` (function, L220)
- `beam_search_candidates` (function, L240)
- `beam_search_candidates.get_eval` (method, L257)
- `run_pareto_by_ids` (function, L300)
- `main` (function, L513)

### `tools/patterns_analysis/_archive/pareto_with_splits.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `json`
- `os`
- `pandas`
- `pareto_joinkey_search:run_pareto_analysis`
- `pathlib:Path`
- `sys`
- `typing:Dict,List,Tuple`

**Definitions**
- `detect_pareto_cliffs` (function, L20)
- `assess_split_likelihood` (function, L57)
- `run_pareto_with_split_detection` (function, L117)
- `main` (function, L193)

### `tools/patterns_analysis/_archive/report.py`

**Imports**
- `.compare:ChangeCounts`
- `__future__:annotations`
- `csv`
- `dataclasses:asdict`
- `datetime:datetime,timezone`
- `json`
- `os`
- `typing:Any,Dict,Iterable,List`

**Definitions**
- `ensure_dir` (function, L13)
- `utc_timestamp` (function, L18)
- `write_change_type_csv` (function, L22)
- `write_json_report` (function, L55)
- `format_console_summary` (function, L73)

### `tools/patterns_analysis/_archive/run_attribute_stress.py`

**Imports**
- `.index:build_domain_index`
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `.stability:stable_join_hashes`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `os`
- `typing:Dict,List,Tuple`

**Definitions**
- `extract_phase2_items` (function, L15)
- `run_attribute_stress` (function, L32)
- `main` (function, L125)

### `tools/patterns_analysis/_archive/run_attribute_stress_all_joinable.py`

**Imports**
- `.index:build_domain_index`
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `.stability:presence_counts`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `os`
- `typing:Dict,List,Tuple`

**Definitions**
- `_phase2_items_map_no_dups` (function, L15)
- `run_attribute_stress_all_joinable` (function, L52)
- `main` (function, L162)

### `tools/patterns_analysis/_archive/run_candidate_joinkey_simulation.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `os`
- `typing:Dict,Any,List,Tuple`

**Definitions**
- `_get` (function, L13)
- `_qv` (function, L22)
- `_extract_features` (function, L30)
- `run_candidate_joinkey_simulation` (function, L39)
- `main` (function, L132)

### `tools/patterns_analysis/_archive/run_change_type.py`

**Imports**
- `.compare:classify_pair,ChangeCounts`
- `.index:build_domain_index`
- `.io:ExportFile,load_exports,get_domain_records,get_run_provenance`
- `.report:write_change_type_csv,write_json_report,format_console_summary`
- `__future__:annotations`
- `argparse`
- `os`
- `typing:Any,Dict,List`

**Definitions**
- `run_change_type` (function, L13)
- `_parse_args` (function, L92)
- `main` (function, L108)

### `tools/patterns_analysis/_archive/run_collision_differencing.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `dataclasses:dataclass`
- `json`
- `os`
- `typing:Any,Dict,Iterable,List,Optional,Tuple`

**Definitions**
- `_get_join_hash` (function, L19)
- `_is_scalar` (function, L33)
- `_stable_json` (function, L37)
- `_phase2_bucket_items` (function, L51)
- `_phase2_items_map` (function, L65)
- `_top_level_field_variants` (function, L97)
- `CollisionGroup` (class, L147)
- `run_collision_differencing` (function, L157)
- `main` (function, L325)

### `tools/patterns_analysis/_archive/run_dimension_types_by_family.py`

**Imports**
- `.io:load_exports,get_domain_records,get_domain_payload`
- `.run_candidate_joinkey_simulation:run_candidate_joinkey_simulation`
- `.run_change_type:run_change_type`
- `.run_collision_differencing:run_collision_differencing`
- `.run_identity_collision_diagnostics:run_identity_collision_diagnostics`
- `.run_joinhash_label_population:run_joinhash_label_population`
- `.run_joinhash_parameter_population:run_joinhash_parameter_population`
- `.run_population_stability:run_population_stability`
- `__future__:annotations`
- `argparse`
- `dataclasses:dataclass`
- `json`
- `os`
- `re`
- `shutil`
- `time`
- `time`
- `typing:Any,Dict,Iterable,List,Optional,Set,Tuple`

**Definitions**
- `_get` (function, L24)
- `_qv_to_v` (function, L33)
- `_family_shape` (function, L46)
- `_slug` (function, L104)
- `_write_json` (function, L111)
- `_filter_export_domain_records` (function, L116)
- `_discover_families_from_exports` (function, L150)
- `_families_present_in_baseline` (function, L159)
- `FamilyRun` (class, L171)
- `_prepare_filtered_dirs` (function, L178)
- `_prepare_filtered_dirs._on_rm_error` (method, L194)
- `_run_all_phase2` (function, L246)
- `main` (function, L309)
- `main._on_rm_error` (method, L389)

### `tools/patterns_analysis/_archive/run_identity_collision_diagnostics.py`

**Imports**
- `.index:build_domain_index`
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `os`
- `typing:Dict,List,Tuple`

**Definitions**
- `_multiplicity_map` (function, L14)
- `run_identity_collision_diagnostics` (function, L32)
- `main` (function, L163)

### `tools/patterns_analysis/_archive/run_joinhash_label_population.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `json`
- `os`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_get_join_hash` (function, L14)
- `_extract_label_qv` (function, L28)
- `run_joinhash_label_population` (function, L78)
- `main` (function, L216)

### `tools/patterns_analysis/_archive/run_joinhash_parameter_population.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `json`
- `os`
- `typing:Any,Dict,Iterable,List,Optional,Set,Tuple`

**Definitions**
- `_get_join_hash` (function, L14)
- `_stable_json` (function, L28)
- `_is_scalar` (function, L41)
- `_extract_qv_from_value` (function, L45)
- `_phase2_bucket_items` (function, L87)
- `_iter_record_parameters` (function, L101)
- `_iter_record_parameters.emit_bucket` (method, L130)
- `run_joinhash_parameter_population` (function, L173)
- `main` (function, L411)

### `tools/patterns_analysis/_archive/run_population_stability.py`

**Imports**
- `.index:build_domain_index`
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `.stability:presence_counts,stability_distribution`
- `__future__:annotations`
- `argparse`
- `csv`
- `os`
- `typing:List`

**Definitions**
- `run_population_stability` (function, L14)
- `_parse_args` (function, L89)
- `main` (function, L102)

### `tools/patterns_analysis/_archive/run_text_types_candidate_joinkey_simulation.py`

**Imports**
- `.io:load_exports,get_domain_records`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `os`
- `typing:Any,Dict,Optional,Set,Tuple`

**Definitions**
- `_norm_scalar` (function, L13)
- `_get_top` (function, L23)
- `_get_p2_value` (function, L27)
- `_extract_features` (function, L43)
- `run` (function, L61)
- `main` (function, L165)

### `tools/patterns_analysis/_archive/run_view_category_overrides_joinkey_analysis.py`

**Imports**
- `argparse`
- `collections:Counter,defaultdict`
- `hashlib`
- `json`
- `statistics:mean,median`
- `typing:Dict,Iterable,List,Tuple`

**Definitions**
- `_stable_delta_hash` (function, L21)
- `_extract_override_record` (function, L28)
- `analyze_override_patterns` (function, L55)
- `main` (function, L188)

### `tools/patterns_analysis/_archive/run_view_templates_joinkey_analysis.py`

**Imports**
- `argparse`
- `collections:Counter,defaultdict`
- `json`
- `statistics:mean,median`
- `typing:Dict,List,Optional`

**Definitions**
- `_join_key_from_record` (function, L20)
- `_detect_demo_plan` (function, L30)
- `_project_identifier` (function, L42)
- `_pareto_cover` (function, L55)
- `test_join_key` (function, L68)
- `_print_option_summary` (function, L127)
- `_print_sample_interpretation` (function, L145)
- `analyze_view_templates` (function, L155)
- `main` (function, L190)

### `tools/patterns_analysis/_archive/split_detection.py`

**Imports**
- `__future__:annotations`
- `collections:Counter,defaultdict`
- `dataclasses:dataclass`
- `json`
- `numpy`
- `pandas`
- `pathlib:Path`
- `re`
- `sklearn.metrics:silhouette_score`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `Cluster` (class, L20)
- `SplitSignal` (class, L33)
- `extract_metadata_patterns` (function, L42)
- `find_common_path_components` (function, L85)
- `infer_region_from_paths` (function, L105)
- `infer_office_from_paths` (function, L132)
- `extract_dates_from_paths` (function, L161)
- `compute_silhouette_score` (function, L180)
- `interpret_silhouette_score` (function, L200)
- `cluster_assignments_to_labels` (function, L213)
- `build_distance_matrix_from_similarity` (function, L227)

### `tools/patterns_analysis/_archive/split_detection_element_level.py`

**Imports**
- `.io:load_exports,get_domain_records,load_export_file`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:Counter`
- `csv`
- `json`
- `os`
- `pandas`
- `pathlib:Path`
- `sys`
- `typing:Dict,List`
- `typing:Iterator`

**Definitions**
- `_read_csv_rows` (function, L21)
- `extract_label_display` (function, L30)
- `classify_file_elements` (function, L76)
- `compute_element_statistics` (function, L187)
- `generate_remediation_plan` (function, L227)
- `run_element_level_classification` (function, L280)
- `main` (function, L433)

### `tools/patterns_analysis/_archive/split_detection_file_level.py`

**Imports**
- `.io:load_exports,get_domain_records,load_records_sig_profiles`
- `.report:write_json_report`
- `.split_detection:Cluster,extract_metadata_patterns,compute_silhouette_score,interpret_silhouette_score,cluster_assignments_to_labels,build_distance_matrix_from_similarity`
- `__future__:annotations`
- `argparse`
- `json`
- `numpy`
- `os`
- `pandas`
- `pathlib:Path`
- `scipy.cluster.hierarchy:linkage,fcluster`
- `scipy.spatial.distance:squareform`
- `sys`
- `typing:Dict,List,Set,Tuple`

**Definitions**
- `compute_named_cluster_flags` (function, L33)
- `build_element_profiles` (function, L76)
- `compute_pairwise_similarity_candidates` (function, L111)
- `_files_with_any_candidate` (function, L179)
- `compute_pairwise_similarity` (function, L187)
- `hierarchical_cluster_files` (function, L213)
- `threshold_graph_cluster_files` (function, L280)
- `threshold_graph_cluster_files.find` (method, L310)
- `threshold_graph_cluster_files.union` (method, L316)
- `compute_avg_internal_similarity` (function, L362)
- `compute_avg_between_cluster_similarity` (function, L381)
- `select_cluster_representative` (function, L403)
- `infer_standard_name` (function, L428)
- `run_file_level_clustering` (function, L445)
- `main` (function, L616)

### `tools/patterns_analysis/_archive/stability.py`

**Imports**
- `.index:DomainIndex`
- `__future__:annotations`
- `dataclasses:dataclass`
- `typing:Dict,List,Sequence,Set`

**Definitions**
- `PresenceStability` (class, L21)
- `presence_counts` (function, L28)
- `stable_join_hashes` (function, L37)
- `stability_distribution` (function, L58)

### `tools/patterns_analysis/split_detection.py`

**Imports**
- `__future__:annotations`
- `collections:Counter,defaultdict`
- `dataclasses:dataclass`
- `json`
- `numpy`
- `pandas`
- `pathlib:Path`
- `re`
- `sklearn.metrics:silhouette_score`
- `typing:Any,Dict,List,Optional,Set,Tuple`

**Definitions**
- `Cluster` (class, L20)
- `SplitSignal` (class, L33)
- `extract_metadata_patterns` (function, L42)
- `find_common_path_components` (function, L85)
- `infer_region_from_paths` (function, L105)
- `infer_office_from_paths` (function, L132)
- `extract_dates_from_paths` (function, L161)
- `compute_silhouette_score` (function, L180)
- `interpret_silhouette_score` (function, L200)
- `cluster_assignments_to_labels` (function, L213)
- `build_distance_matrix_from_similarity` (function, L227)

### `tools/patterns_analysis/split_detection_element_level.py`

**Imports**
- `.io:load_exports,get_domain_records,load_export_file`
- `.report:write_json_report`
- `__future__:annotations`
- `argparse`
- `collections:Counter`
- `csv`
- `json`
- `os`
- `pandas`
- `pathlib:Path`
- `sys`
- `typing:Dict,List`
- `typing:Iterator`

**Definitions**
- `_read_csv_rows` (function, L21)
- `extract_label_display` (function, L30)
- `classify_file_elements` (function, L76)
- `compute_element_statistics` (function, L187)
- `generate_remediation_plan` (function, L227)
- `run_element_level_classification` (function, L280)
- `main` (function, L433)

### `tools/patterns_analysis/split_detection_file_level.py`

**Imports**
- `.io:load_exports,get_domain_records,load_records_sig_profiles`
- `.report:write_json_report`
- `.split_detection:Cluster,extract_metadata_patterns,compute_silhouette_score,interpret_silhouette_score,cluster_assignments_to_labels,build_distance_matrix_from_similarity`
- `__future__:annotations`
- `argparse`
- `json`
- `numpy`
- `os`
- `pandas`
- `pathlib:Path`
- `scipy.cluster.hierarchy:linkage,fcluster`
- `scipy.spatial.distance:squareform`
- `sys`
- `typing:Dict,List,Set,Tuple`

**Definitions**
- `compute_named_cluster_flags` (function, L33)
- `build_element_profiles` (function, L76)
- `compute_pairwise_similarity_candidates` (function, L111)
- `_files_with_any_candidate` (function, L179)
- `compute_pairwise_similarity` (function, L187)
- `hierarchical_cluster_files` (function, L213)
- `threshold_graph_cluster_files` (function, L280)
- `threshold_graph_cluster_files.find` (method, L310)
- `threshold_graph_cluster_files.union` (method, L316)
- `compute_avg_internal_similarity` (function, L362)
- `compute_avg_between_cluster_similarity` (function, L381)
- `select_cluster_representative` (function, L403)
- `infer_standard_name` (function, L428)
- `run_file_level_clustering` (function, L445)
- `main` (function, L616)

### `tools/population_framing.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:defaultdict`
- `csv`
- `json`
- `math`
- `pathlib:Path`
- `typing:Dict,List,Tuple,Any`

**Definitions**
- `load_json` (function, L17)
- `load_csv` (function, L22)
- `write_csv` (function, L27)
- `hhi` (function, L36)
- `effective_cluster_count` (function, L40)
- `pick_population_baselines` (function, L49)
- `classify_population_shape` (function, L69)
- `main` (function, L89)

### `tools/probes/build_probe_inventory.py`

**Imports**
- `argparse`
- `collections:OrderedDict`
- `csv`
- `json`
- `os`
- `re`
- `sys`

**Definitions**
- `_example_score` (function, L70)
- `discover_probe_files` (function, L87)
- `load_payload` (function, L115)
- `_merge_observed` (function, L120)
- `_new_agg` (function, L139)
- `_new_crosswalk_col_agg` (function, L181)
- `_crosswalk_value_sig` (function, L197)
- `_merge_crosswalk_records` (function, L201)
- `_merge_entries_for_domain` (function, L235)
- `merge_probe_files` (function, L308)
- `_fmt_q_counts` (function, L396)
- `_fmt_example` (function, L402)
- `write_csv` (function, L408)
- `_fmt_rate` (function, L466)
- `write_crosswalk_csv` (function, L472)
- `write_crosswalk_markdown` (function, L517)
- `scan_domain_coverage` (function, L599)
- `write_markdown` (function, L611)
- `build` (function, L730)
- `main` (function, L780)

### `tools/probes/check_line_patterns_normhash.py`

**Imports**
- `argparse`
- `collections:Counter,defaultdict`
- `csv`
- `pathlib:Path`

**Definitions**
- `sample_problem_records` (function, L89)

### `tools/probes/find_crosswalk_candidates.py`

**Imports**
- `argparse`
- `collections:OrderedDict`
- `csv`
- `os`
- `re`
- `sys`

**Definitions**
- `_normalize` (function, L67)
- `_member_name` (function, L73)
- `_read_csv_rows` (function, L79)
- `_is_elementid_typed` (function, L86)
- `build_resolved_index` (function, L95)
- `_already_resolved` (function, L107)
- `_is_int_like` (function, L119)
- `_already_resolved_via_param_display` (function, L130)
- `find_candidates` (function, L155)
- `group_by_member` (function, L185)
- `write_csv` (function, L199)
- `write_markdown` (function, L226)
- `main` (function, L274)

### `tools/probes/probe_arrowheads.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,ElementType,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `hashlib`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L75)
- `_safe_type_name` (function, L81)
- `_safe_param_def_name` (function, L96)
- `_safe_get_datatype` (function, L103)
- `_is_length_datatype` (function, L112)
- `_is_angle_datatype` (function, L120)
- `_fmt_display` (function, L128)
- `_format_param_contract` (function, L139)
- `_looks_like_arrowhead_type` (function, L217)
- `_arrow_style_key` (function, L232)
- `_maybe_set_example` (function, L317)
- `_push_example` (function, L341)
- `_collect_dimension_types_with_tick_param` (function, L427)
- `_resolve_workset` (function, L444)
- `_resolve_similar_type` (function, L476)
- `_reflect_member_names` (function, L593)
- `_reflect_try_get` (function, L741)
- `_reflect_contract` (function, L763)
- `_run_reflection_sweep` (function, L834)
- `_probe_revit_version` (function, L908)
- `_probe_document_identity` (function, L917)
- `_probe_run_id` (function, L924)
- `_probe_wrap` (function, L933)

### `tools/probes/probe_browser_organization.py`

**Imports**
- `Autodesk.Revit.DB:BrowserOrganization`
- `Autodesk.Revit.DB:ElementId,BuiltInParameter`
- `Autodesk.Revit.DB:LabelUtils`
- `RevitServices.Persistence:DocumentManager`
- `System`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L115)
- `_safe_str` (function, L122)
- `_int_enum` (function, L129)
- `_clean_name` (function, L136)
- `_pv` (function, L143)
- `_pv._coerce` (method, L154)
- `_resolve_workset` (function, L167)
- `_resolve_similar_type` (function, L186)
- `_try_get_definition_record` (function, L201)
- `_builtin_label` (function, L231)
- `_best_name` (function, L241)
- `_resolve_folder_item` (function, L264)
- `_walk_tree` (function, L315)
- `_add_inventory_obs` (function, L380)
- `_reflect_member_names` (function, L573)
- `_reflect_try_get` (function, L722)
- `_reflect_contract` (function, L741)
- `_run_reflection_sweep` (function, L813)
- `_probe_revit_version` (function, L886)
- `_probe_document_identity` (function, L896)
- `_probe_run_id` (function, L904)
- `_probe_wrap` (function, L915)

### `tools/probes/probe_ceiling_types.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,BuiltInCategory,CeilingType`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L97)
- `_safe_capture` (function, L103)
- `_multi_repr` (function, L109)
- `_safe_type_name` (function, L129)
- `_safe_param_def_name` (function, L144)
- `_safe_get_datatype` (function, L151)
- `_is_length_datatype` (function, L160)
- `_is_angle_datatype` (function, L168)
- `_fmt_display` (function, L176)
- `_format_param_contract` (function, L187)
- `_contract_from_value` (function, L239)
- `_to_inches` (function, L242)
- `_resolve_material` (function, L285)
- `_resolve_similar_type` (function, L298)
- `_resolve_workset` (function, L312)
- `_ensure_entry` (function, L334)
- `_maybe_set_example` (function, L340)
- `_observe` (function, L351)
- `_reflect_member_names` (function, L505)
- `_reflect_try_get` (function, L653)
- `_reflect_contract` (function, L675)
- `_run_reflection_sweep` (function, L746)
- `_probe_revit_version` (function, L793)
- `_probe_document_identity` (function, L802)
- `_probe_run_id` (function, L809)
- `_probe_wrap` (function, L818)

### `tools/probes/probe_dimension_types.py`

**Imports**
- `Autodesk.Revit.DB:DimensionType`
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,ElementType,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `System`
- `clr`
- `datetime:datetime`
- `hashlib`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L79)
- `_safe_type_name` (function, L85)
- `_get_family_name_param` (function, L100)
- `_get_family_name_param._normalize_param_string` (method, L106)
- `_safe_param_def_name` (function, L141)
- `_safe_get_datatype` (function, L148)
- `_is_length_datatype` (function, L157)
- `_is_angle_datatype` (function, L165)
- `_fmt_display` (function, L173)
- `_format_param_contract` (function, L184)
- `_shape_family_from_label` (function, L282)
- `_get_dim_shape_info` (function, L307)
- `_get_dim_shape_info._pack` (method, L321)
- `_looks_like_dimension_type` (function, L457)
- `_example_score` (function, L546)
- `_maybe_set_example` (function, L572)
- `_md5` (function, L605)
- `_try_call` (function, L613)
- `_kv_norm` (function, L638)
- `_format_synth_contract` (function, L657)
- `_reflect_members` (function, L677)
- `_try_extract_format_surface` (function, L717)
- `_try_extract_format_surface._sig` (method, L768)
- `_upsert_synth_inventory` (function, L783)
- `_find_tick_param` (function, L986)
- `_probe_revit_version` (function, L1084)
- `_probe_document_identity` (function, L1093)
- `_probe_run_id` (function, L1100)
- `_probe_wrap` (function, L1109)

### `tools/probes/probe_fill_patterns.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,FillPatternElement`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L82)
- `_safe_type_name` (function, L88)
- `_safe_param_def_name` (function, L104)
- `_safe_get_datatype` (function, L111)
- `_is_length_datatype` (function, L120)
- `_is_angle_datatype` (function, L128)
- `_fmt_display` (function, L136)
- `_format_param_contract` (function, L147)
- `_contract_from_value` (function, L221)
- `_to_inches` (function, L231)
- `_to_degrees` (function, L236)
- `_bucket_key_for_fill_pattern` (function, L241)
- `_ensure_entry` (function, L329)
- `_maybe_set_example` (function, L341)
- `_observe` (function, L365)
- `_add_computed_surface` (function, L383)
- `_resolve_workset` (function, L563)
- `_reflect_member_names` (function, L685)
- `_reflect_try_get` (function, L833)
- `_reflect_contract` (function, L855)
- `_run_reflection_sweep` (function, L926)
- `_probe_revit_version` (function, L1001)
- `_probe_document_identity` (function, L1010)
- `_probe_run_id` (function, L1017)
- `_probe_wrap` (function, L1026)

### `tools/probes/probe_floor_types.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,BuiltInCategory,FloorType`
- `Autodesk.Revit.DB:FloorFunction`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L89)
- `_safe_capture` (function, L95)
- `_multi_repr` (function, L101)
- `_safe_type_name` (function, L121)
- `_safe_param_def_name` (function, L136)
- `_safe_get_datatype` (function, L143)
- `_is_length_datatype` (function, L152)
- `_is_angle_datatype` (function, L160)
- `_fmt_display` (function, L168)
- `_format_param_contract` (function, L179)
- `_contract_from_value` (function, L231)
- `_to_inches` (function, L234)
- `_resolve_material` (function, L277)
- `_resolve_similar_type` (function, L288)
- `_resolve_workset` (function, L302)
- `_ensure_entry` (function, L324)
- `_maybe_set_example` (function, L330)
- `_observe` (function, L341)
- `_reflect_member_names` (function, L490)
- `_reflect_try_get` (function, L638)
- `_reflect_contract` (function, L660)
- `_run_reflection_sweep` (function, L731)
- `_probe_revit_version` (function, L778)
- `_probe_document_identity` (function, L787)
- `_probe_run_id` (function, L794)
- `_probe_wrap` (function, L803)

### `tools/probes/probe_identity.py`

**Imports**
- `Autodesk.Revit.DB:ElementId,StorageType,FilteredElementCollector,ProjectInfo,ModelPathUtils,BuiltInParameter`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L75)
- `_as_str` (function, L81)
- `_param_contract_from_value` (function, L89)
- `_safe_defn_builtin` (function, L98)
- `_definition_origin` (function, L101)
- `_shared_guid_if_any` (function, L124)
- `_param_group_legacy_str` (function, L152)
- `_format_param_contract` (function, L157)
- `_add_inventory_record` (function, L222)
- `_provenance_layer` (function, L271)
- `_resolve_workset` (function, L465)
- `_reflect_member_names` (function, L511)
- `_reflect_try_get` (function, L659)
- `_reflect_contract` (function, L681)
- `_run_reflection_sweep` (function, L752)
- `_probe_revit_version` (function, L831)
- `_probe_document_identity` (function, L840)
- `_probe_run_id` (function, L847)
- `_probe_wrap` (function, L856)

### `tools/probes/probe_line_patterns.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInCategory,GraphicsStyleType,LinePatternElement`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `hashlib`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L74)
- `_safe_elem_name` (function, L80)
- `_safe_param_def_name` (function, L87)
- `_safe_get_datatype` (function, L94)
- `_is_length_datatype` (function, L103)
- `_is_angle_datatype` (function, L111)
- `_fmt_display` (function, L119)
- `_format_param_contract` (function, L130)
- `_contract_from_raw` (function, L204)
- `_to_inches` (function, L207)
- `_lp_seg_type_id_and_name` (function, L216)
- `_linepattern_signature` (function, L249)
- `_maybe_set_example` (function, L439)
- `_touch_param` (function, L462)
- `_iter_line_style_categories` (function, L581)
- `_category_line_pattern_id` (function, L603)
- `_resolve_workset` (function, L610)
- `_reflect_member_names` (function, L712)
- `_reflect_try_get` (function, L860)
- `_reflect_contract` (function, L882)
- `_run_reflection_sweep` (function, L953)
- `_probe_revit_version` (function, L1027)
- `_probe_document_identity` (function, L1036)
- `_probe_run_id` (function, L1043)
- `_probe_wrap` (function, L1052)

### `tools/probes/probe_line_styles.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInCategory`
- `Autodesk.Revit.DB:GraphicsStyle,GraphicsStyleType,LinePatternElement`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L116)
- `_safe_param_def_name` (function, L122)
- `_safe_get_datatype` (function, L129)
- `_is_length_datatype` (function, L138)
- `_is_angle_datatype` (function, L146)
- `_fmt_display_param` (function, L154)
- `_format_param_contract` (function, L165)
- `_contract_value` (function, L235)
- `_rgb_triplet` (function, L239)
- `_hex_rgb_from_triplet` (function, L250)
- `_get_lines_category_id` (function, L262)
- `_is_line_style_graphicsstyle` (function, L266)
- `_bucket_key` (function, L279)
- `_maybe_set_example` (function, L355)
- `_index_param` (function, L378)
- `_virtual_surface` (function, L400)
- `_resolve_workset` (function, L536)
- `_reflect_member_names` (function, L620)
- `_reflect_try_get` (function, L768)
- `_reflect_contract` (function, L790)
- `_run_reflection_sweep` (function, L861)
- `_probe_revit_version` (function, L935)
- `_probe_document_identity` (function, L944)
- `_probe_run_id` (function, L951)
- `_probe_wrap` (function, L960)

### `tools/probes/probe_loaded_family_types.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,Family,FamilySymbol,ElementId,StorageType,UnitUtils,UnitFormatUtils,BuiltInParameter`
- `Autodesk.Revit.DB:SpecTypeId,UnitTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L78)
- `_id_int` (function, L84)
- `_element_name` (function, L90)
- `_cat_info` (function, L107)
- `_param_definition_identity` (function, L117)
- `_format_double` (function, L148)
- `_normalize_double` (function, L156)
- `_param_value_contract` (function, L186)
- `_parameters_for_element` (function, L235)
- `_family_symbols` (function, L266)
- `_family_record` (function, L276)
- `_reflect_member_names` (function, L411)
- `_reflect_try_get` (function, L559)
- `_reflect_contract` (function, L581)
- `_run_reflection_sweep` (function, L652)
- `_resolve_workset` (function, L701)
- `_resolve_similar_type` (function, L720)
- `_probe_revit_version` (function, L802)
- `_probe_document_identity` (function, L811)
- `_probe_run_id` (function, L818)
- `_probe_wrap` (function, L827)

### `tools/probes/probe_materials.py`

**Imports**
- `Autodesk.Revit.DB:AppearanceAssetElement`
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,Material,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter`
- `Autodesk.Revit.DB:PropertySetElement`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L82)
- `_safe_name` (function, L88)
- `_safe_param_def_name` (function, L91)
- `_safe_get_datatype` (function, L98)
- `_is_length_datatype` (function, L107)
- `_fmt_display` (function, L115)
- `_format_param_contract` (function, L126)
- `_maybe_set_example` (function, L224)
- `_resolve_workset` (function, L300)
- `_reflect_member_names` (function, L405)
- `_reflect_try_get` (function, L553)
- `_reflect_contract` (function, L575)
- `_run_reflection_sweep` (function, L652)
- `_probe_revit_version` (function, L733)
- `_probe_document_identity` (function, L742)
- `_probe_run_id` (function, L749)
- `_probe_wrap` (function, L758)

### `tools/probes/probe_object_styles.py`

**Imports**
- `Autodesk.Revit.DB:ElementId,GraphicsStyleType,UnitFormatUtils,CategoryType`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L61)
- `_color_hex` (function, L67)
- `_rgb_triplet` (function, L75)
- `_hex_from_rgb_triplet` (function, L87)
- `_get_name` (function, L99)
- `_eid_name` (function, L102)
- `_contract_missing` (function, L108)
- `_contract_unreadable` (function, L111)
- `_contract_unsupported` (function, L114)
- `_contract_string` (function, L117)
- `_contract_int` (function, L120)
- `_contract_eid` (function, L124)
- `_maybe_set_example` (function, L131)
- `_obs_sig` (function, L139)
- `_category_type_label` (function, L152)
- `_infer_object_styles_tab` (function, L186)
- `_iter_categories` (function, L209)
- `_bool_int` (function, L279)
- `_reflect_member_names` (function, L423)
- `_reflect_try_get` (function, L571)
- `_reflect_contract` (function, L593)
- `_run_reflection_sweep` (function, L664)
- `_probe_revit_version` (function, L724)
- `_probe_document_identity` (function, L733)
- `_probe_run_id` (function, L740)
- `_probe_wrap` (function, L749)

### `tools/probes/probe_phase_filters.py`

**Imports**
- `Autodesk.Revit.DB:ElementOnPhaseStatus`
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,View`
- `Autodesk.Revit.DB:PhaseFilter`
- `Autodesk.Revit.DB:PhaseStatus`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L88)
- `_safe_elem_name` (function, L94)
- `_safe_param_def_name` (function, L114)
- `_safe_get_datatype` (function, L121)
- `_is_length_datatype` (function, L130)
- `_is_angle_datatype` (function, L138)
- `_fmt_display` (function, L146)
- `_format_param_contract` (function, L157)
- `_phase_status_bucket` (function, L236)
- `_status_enum` (function, L324)
- `_maybe_set_example` (function, L334)
- `_add_inventory_obs` (function, L357)
- `_get_view_phase_filter_param` (function, L501)
- `_resolve_workset` (function, L520)
- `_reflect_member_names` (function, L633)
- `_reflect_try_get` (function, L781)
- `_reflect_contract` (function, L803)
- `_run_reflection_sweep` (function, L874)
- `_probe_revit_version` (function, L947)
- `_probe_document_identity` (function, L956)
- `_probe_run_id` (function, L963)
- `_probe_wrap` (function, L972)

### `tools/probes/probe_phase_graphics.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,ElementType,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,View`
- `Autodesk.Revit.DB:PhaseFilter`
- `Autodesk.Revit.DB:SpecTypeId`
- `Autodesk.Revit.DB:ViewSchedule`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L98)
- `_safe_elem_name` (function, L104)
- `_safe_param_def_name` (function, L112)
- `_safe_get_datatype` (function, L119)
- `_is_length_datatype` (function, L128)
- `_is_angle_datatype` (function, L136)
- `_fmt_display` (function, L144)
- `_format_param_contract` (function, L155)
- `_cap` (function, L286)
- `_maybe_set_example` (function, L344)
- `_index_params_from_elem` (function, L367)
- `_resolve_workset_for_view_crosswalk` (function, L446)
- `_resolve_filter_name` (function, L463)
- `_get_phasefilter_param_from_view` (function, L537)
- `_resolve_workset` (function, L556)
- `_reflect_member_names` (function, L653)
- `_reflect_try_get` (function, L801)
- `_reflect_contract` (function, L823)
- `_run_reflection_sweep` (function, L894)
- `_probe_revit_version` (function, L966)
- `_probe_document_identity` (function, L975)
- `_probe_run_id` (function, L982)
- `_probe_wrap` (function, L991)

### `tools/probes/probe_phases.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,Phase,View,BuiltInParameter`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L96)
- `_safe_param_def_name` (function, L102)
- `_safe_get_datatype` (function, L109)
- `_is_length_datatype` (function, L118)
- `_is_angle_datatype` (function, L126)
- `_fmt_display` (function, L134)
- `_format_param_contract` (function, L145)
- `_phase_key` (function, L219)
- `_synthetic_value_contract` (function, L232)
- `_inv_init` (function, L235)
- `_inv_add` (function, L244)
- `_reflect_member_names` (function, L386)
- `_reflect_try_get` (function, L534)
- `_reflect_contract` (function, L556)
- `_run_reflection_sweep` (function, L627)
- `_get_view_phase_param` (function, L673)
- `_resolve_workset` (function, L714)
- `_probe_revit_version` (function, L778)
- `_probe_document_identity` (function, L787)
- `_probe_run_id` (function, L794)
- `_probe_wrap` (function, L803)

### `tools/probes/probe_roof_type_import.py`

**Imports**
- `Autodesk.Revit.DB:RoofType`

### `tools/probes/probe_roof_types.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,BuiltInCategory,RoofType`
- `Autodesk.Revit.DB:SpecTypeId`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L79)
- `_safe_capture` (function, L85)
- `_multi_repr` (function, L91)
- `_safe_type_name` (function, L111)
- `_safe_param_def_name` (function, L126)
- `_safe_get_datatype` (function, L133)
- `_is_length_datatype` (function, L142)
- `_is_angle_datatype` (function, L150)
- `_fmt_display` (function, L158)
- `_format_param_contract` (function, L169)
- `_contract_from_value` (function, L221)
- `_to_inches` (function, L224)
- `_resolve_material` (function, L267)
- `_resolve_similar_type` (function, L278)
- `_resolve_workset` (function, L292)
- `_ensure_entry` (function, L314)
- `_maybe_set_example` (function, L320)
- `_observe` (function, L331)
- `_reflect_member_names` (function, L477)
- `_reflect_try_get` (function, L625)
- `_reflect_contract` (function, L647)
- `_run_reflection_sweep` (function, L718)
- `_probe_revit_version` (function, L765)
- `_probe_document_identity` (function, L774)
- `_probe_run_id` (function, L781)
- `_probe_wrap` (function, L790)

### `tools/probes/probe_text_types.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,ElementType,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter`
- `Autodesk.Revit.DB:SpecTypeId`
- `Autodesk.Revit.DB:TextNoteType`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L87)
- `_safe_type_name` (function, L93)
- `_safe_param_def_name` (function, L108)
- `_safe_get_datatype` (function, L115)
- `_is_length_datatype` (function, L124)
- `_is_angle_datatype` (function, L132)
- `_fmt_display` (function, L140)
- `_format_param_contract` (function, L151)
- `_contract_value` (function, L227)
- `_rgb_triplet_from_int` (function, L231)
- `_hex32_from_int` (function, L249)
- `_rgb_rrggbb_from_int` (function, L258)
- `_rgb_bbgrr_from_int` (function, L271)
- `_hex_rgb_from_triplet` (function, L284)
- `_slug` (function, L296)
- `_looks_like_text_type` (function, L302)
- `_text_font_key` (function, L334)
- `_maybe_set_example` (function, L432)
- `_find_leader_arrow_param` (function, L594)
- `_resolve_workset` (function, L602)
- `_resolve_similar_type` (function, L621)
- `_reflect_member_names` (function, L736)
- `_reflect_try_get` (function, L884)
- `_reflect_contract` (function, L906)
- `_run_reflection_sweep` (function, L977)
- `_probe_revit_version` (function, L1050)
- `_probe_document_identity` (function, L1059)
- `_probe_run_id` (function, L1066)
- `_probe_wrap` (function, L1075)

### `tools/probes/probe_units.py`

**Imports**
- `Autodesk.Revit.DB:SpecTypeId`
- `Autodesk.Revit.DB:UnitUtils,LabelUtils`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L73)
- `_try` (function, L79)
- `_safe_str` (function, L85)
- `_is_forge_type_id` (function, L91)
- `_forge_id_string` (function, L102)
- `_pv_missing` (function, L111)
- `_pv_unreadable` (function, L114)
- `_pv_unsupported` (function, L117)
- `_pv_from_string` (function, L120)
- `_pv_from_int` (function, L123)
- `_pv_from_double` (function, L127)
- `_pv_from_forge_type_id` (function, L131)
- `_pv_from_bool` (function, L137)
- `_pv_from_enum` (function, L144)
- `_unitutils_get_discipline_id` (function, L152)
- `_label_for_spec_id` (function, L168)
- `_label_for_discipline_id` (function, L188)
- `_units_get_format_options` (function, L205)
- `_pv_from_prop_any` (function, L250)
- `_pv_from_format_surface` (function, L269)
- `_discover_specs` (function, L323)
- `_maybe_set_example` (function, L386)
- `_touch_param` (function, L399)
- `_reflect_member_names` (function, L564)
- `_reflect_try_get` (function, L712)
- `_reflect_contract` (function, L734)
- `_run_reflection_sweep` (function, L805)
- `_probe_revit_version` (function, L879)
- `_probe_document_identity` (function, L888)
- `_probe_run_id` (function, L895)
- `_probe_wrap` (function, L904)

### `tools/probes/probe_view_category_overrides.py`

**Imports**
- `Autodesk.Revit.DB:ElementId,FilteredElementCollector,View,Category,OverrideGraphicSettings,GraphicsStyleType,GraphicsStyle`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L80)
- `_get_name` (function, L86)
- `_eid_int` (function, L89)
- `_eid_name` (function, L101)
- `_rgb_triplet` (function, L107)
- `_hex_from_rgb_triplet` (function, L116)
- `_contract_missing` (function, L128)
- `_contract_unreadable` (function, L131)
- `_contract_unsupported` (function, L134)
- `_contract_string` (function, L137)
- `_contract_int` (function, L140)
- `_contract_eid` (function, L144)
- `_maybe_set_example` (function, L151)
- `_bool_int` (function, L159)
- `_ogs_default` (function, L162)
- `_ogs_has_override_for_field` (function, L165)
- `_iter_categories_from_object_styles` (function, L181)
- `_category_path` (function, L230)
- `_baseline_for_cat` (function, L242)
- `_bucket_for_view` (function, L314)
- `_select_views` (function, L326)
- `_touch_param` (function, L376)
- `_pv_from_field` (function, L396)
- `_pv_bool_flag` (function, L428)
- `_value_norm_for_compare` (function, L431)
- `_touch_diag` (function, L442)
- `_reflect_member_names` (function, L617)
- `_reflect_try_get` (function, L765)
- `_reflect_contract` (function, L787)
- `_run_reflection_sweep` (function, L858)
- `_probe_revit_version` (function, L923)
- `_probe_document_identity` (function, L932)
- `_probe_run_id` (function, L939)
- `_probe_wrap` (function, L948)

### `tools/probes/probe_view_filter_applications.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,BuiltInParameter,View,OverrideGraphicSettings,ParameterFilterElement,Color,Category`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `hashlib`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L75)
- `_safe_type_name` (function, L81)
- `_eid_int` (function, L96)
- `_color_rgb_hex` (function, L106)
- `_contract` (function, L119)
- `_as_int_contract` (function, L129)
- `_as_bool_int_contract` (function, L138)
- `_as_string_contract` (function, L147)
- `_as_elementid_contract` (function, L156)
- `_ogs_get` (function, L169)
- `_hash_sig` (function, L190)
- `_ensure_param` (function, L208)
- `_q_rank` (function, L220)
- `_maybe_set_example` (function, L226)
- `_observe` (function, L248)
- `_view_bucket_key` (function, L302)
- `_view_has_filters` (function, L307)
- `_pv_from_ogs_field` (function, L390)
- `_is_defaultish_ogs_value` (function, L426)
- `_collect_applied_filters_in_order` (function, L446)
- `_resolve_workset` (function, L553)
- `_reflect_member_names` (function, L711)
- `_reflect_try_get` (function, L859)
- `_reflect_contract` (function, L881)
- `_run_reflection_sweep` (function, L952)
- `_probe_revit_version` (function, L1025)
- `_probe_document_identity` (function, L1034)
- `_probe_run_id` (function, L1041)
- `_probe_wrap` (function, L1050)

### `tools/probes/probe_view_filter_definitions.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,BuiltInParameter,Category,ParameterFilterElement,LogicalAndFilter,LogicalOrFilter,ElementParameterFilter,View`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `hashlib`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L101)
- `_sha1` (function, L107)
- `_as_param_payload` (function, L114)
- `_maybe_set_example` (function, L123)
- `_observe` (function, L146)
- `_bucket_label_from_categories` (function, L170)
- `_resolve_category_name` (function, L179)
- `_element_filter_kind` (function, L192)
- `_get_subfilters` (function, L200)
- `_get_rules_from_element_parameter_filter` (function, L212)
- `_rule_parameter_id` (function, L231)
- `_rule_evaluator_name` (function, L249)
- `_rule_value_best_effort` (function, L262)
- `_flatten_element_filter` (function, L275)
- `_reflect_member_names` (function, L574)
- `_reflect_try_get` (function, L722)
- `_reflect_contract` (function, L744)
- `_run_reflection_sweep` (function, L815)
- `_resolve_workset` (function, L902)
- `_probe_revit_version` (function, L983)
- `_probe_document_identity` (function, L992)
- `_probe_run_id` (function, L999)
- `_probe_wrap` (function, L1008)

### `tools/probes/probe_view_templates.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,View`
- `Autodesk.Revit.DB:SpecTypeId`
- `Autodesk.Revit.DB:ViewSchedule`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L86)
- `_safe_view_name` (function, L92)
- `_safe_param_def_name` (function, L104)
- `_safe_get_datatype` (function, L111)
- `_is_length_datatype` (function, L120)
- `_is_angle_datatype` (function, L128)
- `_fmt_display` (function, L136)
- `_format_param_contract` (function, L147)
- `_viewtype_bucket` (function, L238)
- `_maybe_set_example` (function, L321)
- `_resolve_workset` (function, L436)
- `_resolve_filter_name` (function, L472)
- `_reflect_member_names` (function, L606)
- `_reflect_try_get` (function, L754)
- `_reflect_contract` (function, L776)
- `_run_reflection_sweep` (function, L847)
- `_probe_revit_version` (function, L920)
- `_probe_document_identity` (function, L929)
- `_probe_run_id` (function, L936)
- `_probe_wrap` (function, L945)

### `tools/probes/probe_views.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter,View`
- `Autodesk.Revit.DB:SpecTypeId`
- `Autodesk.Revit.DB:ViewSchedule`
- `Autodesk.Revit.DB:ViewSheet`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L124)
- `_safe_elem_name` (function, L131)
- `_safe_param_def_name` (function, L150)
- `_safe_get_datatype` (function, L158)
- `_is_length_datatype` (function, L168)
- `_is_angle_datatype` (function, L177)
- `_fmt_display` (function, L186)
- `_format_param_contract` (function, L198)
- `_pv` (function, L253)
- `_pv._coerce` (method, L264)
- `_int_enum` (function, L277)
- `_view_kind_classification` (function, L284)
- `_maybe_set_example` (function, L352)
- `_add_inventory_obs` (function, L363)
- `_resolve_workset` (function, L466)
- `_reflect_member_names` (function, L565)
- `_reflect_try_get` (function, L714)
- `_reflect_contract` (function, L733)
- `_run_reflection_sweep` (function, L805)
- `_probe_revit_version` (function, L875)
- `_probe_document_identity` (function, L885)
- `_probe_run_id` (function, L893)
- `_probe_wrap` (function, L904)

### `tools/probes/probe_wall_types.py`

**Imports**
- `Autodesk.Revit.DB:FilteredElementCollector,ElementId,WallType,StorageType,UnitUtils,UnitTypeId,UnitFormatUtils,BuiltInParameter`
- `Autodesk.Revit.DB:SpecTypeId`
- `Autodesk.Revit.DB:WallKind`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L86)
- `_safe_name` (function, L92)
- `_safe_type_name` (function, L95)
- `_wall_kind_label` (function, L107)
- `_safe_param_def_name` (function, L118)
- `_safe_get_datatype` (function, L125)
- `_is_length_datatype` (function, L134)
- `_fmt_display` (function, L142)
- `_format_param_contract` (function, L153)
- `_maybe_set_example` (function, L252)
- `_observe_synth` (function, L305)
- `_pv` (function, L339)
- `_resolve_material` (function, L408)
- `_resolve_similar_type` (function, L418)
- `_resolve_workset` (function, L435)
- `_reflect_member_names` (function, L525)
- `_reflect_try_get` (function, L673)
- `_reflect_contract` (function, L695)
- `_run_reflection_sweep` (function, L766)
- `_probe_revit_version` (function, L828)
- `_probe_document_identity` (function, L837)
- `_probe_run_id` (function, L844)
- `_probe_wrap` (function, L853)

### `tools/probes/probe_worksets.py`

**Imports**
- `Autodesk.Revit.DB:ElementWorksetFilter`
- `Autodesk.Revit.DB:FilteredElementCollector,FilteredWorksetCollector`
- `Autodesk.Revit.DB:WorksetKind`
- `RevitServices.Persistence:DocumentManager`
- `clr`
- `datetime:datetime`
- `json`
- `os`
- `uuid`

**Definitions**
- `_safe` (function, L97)
- `_pv` (function, L104)
- `_pv._coerce` (method, L115)
- `_discover_enum_members` (function, L152)
- `_add_inventory_obs` (function, L225)
- `_reflect_member_names` (function, L363)
- `_reflect_try_get` (function, L512)
- `_reflect_contract` (function, L535)
- `_run_reflection_sweep` (function, L607)
- `_probe_revit_version` (function, L677)
- `_probe_document_identity` (function, L687)
- `_probe_run_id` (function, L695)
- `_probe_wrap` (function, L706)

### `tools/probes/sweep_line_pattern_normhash_precision.py`

**Imports**
- `argparse`
- `collections:defaultdict,Counter`
- `csv`
- `hashlib`
- `pathlib:Path`
- `re`

**Definitions**
- `md5s` (function, L19)
- `detect_cols` (function, L22)
- `compute_norm_hash_for_group` (function, L29)
- `main` (function, L97)

### `tools/probes/test_probe_inventory_builder.py`

**Imports**
- `csv`
- `json`
- `pathlib:Path`
- `subprocess`
- `sys`

**Definitions**
- `_run` (function, L10)
- `_read_csv_rows` (function, L32)
- `test_merges_and_dedupes_across_dated_runs` (function, L37)
- `test_empty_probes_dir_refuses_to_overwrite_by_default` (function, L143)
- `test_all_inputs_invalid_refuses_to_overwrite_by_default` (function, L162)
- `test_empty_probes_dir_with_force_writes_empty_inventory` (function, L186)
- `_run_shaped_payload` (function, L197)
- `test_merges_run_shaped_files_and_tracks_revit_version` (function, L212)
- `test_merges_across_legacy_and_run_shapes_for_same_domain` (function, L275)
- `test_crosswalk_column_profile_across_runs` (function, L328)

### `tools/reset_wall_types_for_reapply.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `os`
- `pathlib:Path`
- `sys`

**Definitions**
- `_read_wall_items` (function, L28)
- `_is_function_only_block` (function, L50)
- `main` (function, L58)

### `tools/run_extract_all.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `bundle_analysis.common:atomic_write_csv,read_csv_rows`
- `bundle_analysis.reference_bundle:write_sidecar`
- `core.sig_hash_builder:build_sig_hash_from_policy`
- `core.sig_hash_policy:load_sig_hash_policies,get_domain_sig_hash_policy`
- `csv`
- `csv`
- `emit_element_dominance:emit_element_dominance`
- `extractor:emit_analysis,emit_records`
- `hashlib`
- `json`
- `na_token:is_na_token`
- `os`
- `pathlib:Path`
- `re`
- `subprocess`
- `sys`
- `time`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `_append_line_pattern_synthetic_norm_hash` (function, L41)
- `_discover_domains_from_exports` (function, L190)
- `_ensure_dir` (function, L230)
- `_resolve_sig_hash_policy_path` (function, L234)
- `_apply_sig_hash_to_phase0` (function, L252)
- `_apply_sig_hash_to_phase0._load_items_for_domain` (method, L293)
- `_run` (function, L400)
- `_read_csv_rows` (function, L407)
- `_iter_csv_rows` (function, L412)
- `_check_governance_field_completeness` (function, L421)
- `_ensure_domain_scoped_identity_items` (function, L453)
- `_validate_line_pattern_synthetic_norm_hash` (function, L500)
- `_emit_join_policy_diagnostics` (function, L541)
- `_detect_surfaces` (function, L574)
- `_merge_index_details` (function, L591)
- `_pick_sample_file` (function, L601)
- `_read_json` (function, L648)
- `_infer_domains` (function, L656)
- `_parse_stage_csv` (function, L701)
- `_warn_deprecated_alias` (function, L707)
- `_enforce_policy_gate` (function, L711)
- `main` (function, L727)

### `tools/run_segment_orchestrator.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `build_results_registry:write_results_registry`
- `bundle_analysis.common:atomic_write_csv,retry_fs_op`
- `bundle_analysis.name_projection_adapter:annotate_name_target_combined_files,normalize_export_run_id`
- `concurrent.futures:ThreadPoolExecutor,as_completed`
- `csv`
- `datetime:datetime,timezone`
- `hashlib`
- `math`
- `os`
- `pathlib:Path`
- `shutil`
- `subprocess`
- `sys`
- `tempfile`
- `threading`
- `time`
- `typing:Any,Dict,List,Optional`

**Definitions**
- `load_manifest` (function, L69)
- `load_registry` (function, L80)
- `load_membership` (function, L86)
- `write_registry_atomic` (function, L105)
- `utc_now_iso` (function, L118)
- `compute_worker_split` (function, L122)
- `_write_run_summary` (function, L161)
- `run_step` (function, L295)
- `run_step_capture` (function, L300)
- `run_step_log` (function, L308)
- `_preshard_one_shard` (function, L331)
- `_preshard_corpus_records` (function, L411)
- `_write_segment_records` (function, L553)
- `_filter_name_key_csv_to_segment` (function, L626)
- `_filter_name_key_csv_to_segment._in_segment` (method, L663)
- `_build_patterns_missing_notes` (function, L684)
- `_active_domains_from_presence_csv` (function, L743)
- `_active_domains_from_name_patterns` (function, L763)
- `_segment_has_name_leg_output` (function, L788)
- `merge_bi_outputs` (function, L804)
- `build_run_plan` (function, L876)
- `build_run_plan.sort_key` (method, L889)
- `validate_membership_against_manifest` (function, L908)
- `_clear_stale_name_all_before_run` (function, L947)
- `_run_one_segment` (function, L968)
- `_run_one_segment.log` (method, L1042)
- `run_orchestrator` (function, L1397)
- `main` (function, L1773)

### `tools/run_split_detection_all.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `hashlib`
- `json`
- `os`
- `pathlib:Path`
- `subprocess`
- `sys`
- `typing:Any,Dict,List`

**Definitions**
- `_read_csv` (function, L20)
- `_write_csv` (function, L26)
- `_domain_record_count` (function, L35)
- `_domain_has_records` (function, L42)
- `_write_no_data_stub_reports` (function, L47)
- `_validate_join_policy_ready` (function, L71)
- `_load_export_mapping` (function, L112)
- `_load_analysis_run_id` (function, L128)
- `_derive_analysis_run_id` (function, L138)
- `_finalize_split_outputs` (function, L154)
- `_inject_split_contract_headers` (function, L167)
- `_emit_file_to_export_bridge` (function, L204)
- `_emit_cluster_to_pattern_map` (function, L211)
- `run_command` (function, L289)
- `run_split_detection_workflow` (function, L304)
- `main` (function, L530)

### `tools/similarity_compare.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `collections:Counter,defaultdict`
- `csv`
- `dataclasses:dataclass`
- `itertools`
- `math`
- `pathlib:Path`
- `typing:Dict,Iterable,List,Optional,Tuple`

**Definitions**
- `DomainSimilarityRow` (class, L16)
- `_set_jaccard` (function, L32)
- `_multiset_jaccard` (function, L40)
- `_load_metadata` (function, L61)
- `_load_records_grouped` (function, L72)
- `_pair_type` (function, L90)
- `_passes_filters` (function, L97)
- `_build_file_universe` (function, L107)
- `main` (function, L133)

### `tools/suggest_discovery_params.py`

**Imports**
- `__future__:annotations`
- `argparse`
- `csv`
- `discover_join_policy:_pick_candidate_fields,_read_csv,_write_csv`
- `join_key_discovery.eval:normalize_policy_block`
- `json`
- `math`
- `pathlib:Path`
- `shlex`
- `sys`
- `tools.discover_join_policy:_pick_candidate_fields,_read_csv,_write_csv`
- `tools.join_key_discovery.eval:normalize_policy_block`
- `typing:Dict,List,Optional,Sequence`

**Definitions**
- `compute_domain_stats` (function, L99)
- `suggest_sample_size` (function, L155)
- `_cumulative_subset_count` (function, L170)
- `_cumulative_subset_count_from_zero` (function, L177)
- `solve_candidate_fields_and_k` (function, L193)
- `suggest_params_for_domain` (function, L230)
- `_resolve_phase0_dir` (function, L381)
- `_load_policy_fields` (function, L397)
- `_emit_command` (function, L445)
- `_emit_command._base_parts` (method, L463)
- `main` (function, L511)
- `main._get_domain_items` (method, L570)

### `validators/record_v2.py`

**Imports**
- `__future__:annotations`
- `hashlib`
- `json`
- `re`
- `typing:Any,Dict,List,Optional,Tuple`

**Definitions**
- `load_json_file` (function, L27)
- `validate_record_v2` (function, L35)
- `validate_records_v2` (function, L265)
- `serialize_identity_items` (function, L293)
- `_compute_identity_quality` (function, L312)
- `_is_allowed_indexed_key` (function, L325)
- `_normalize_indexed_key` (function, L339)
- `_hash_preimage` (function, L344)
