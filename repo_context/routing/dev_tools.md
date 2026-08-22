# Routing catalog: `dev_tools`

- Generated (UTC): 2026-08-22T10:53:20Z
- Tool version: 0.1.0
- Files covered: 25
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `564e00d7f04bed4aaa145ccfcbe24838779ac22abe2d0ddf0e771b20abd57fa9`
- If this hash differs from a previous copy of this file, the underlying source changed and this catalog should be regenerated via `scan`.

### `dev_tools/repo_context/rc_chunking.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Line-numbered chunk generation for oversized text files.
  - filename/path terms: rc chunking
- Important symbols (5 total):
  - `_build_python_units` (function) — line 20
  - `_pack_units` (function) — line 40
  - `_find_logical_boundary` (function) — line 82
  - `_pack_generic_lines` (function) — line 98
  - `chunk_file` (function) — line 119
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_pack_generic_lines (dev_tools/repo_context/rc_chunking.py:107)`
  - `chunk_file (dev_tools/repo_context/rc_chunking.py:129)`
  - `chunk_file (dev_tools/repo_context/rc_chunking.py:130)`
  - `chunk_file (dev_tools/repo_context/rc_chunking.py:133)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`77b42edf8213c712…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_chunking.py`)

### `dev_tools/repo_context/rc_classify.py`
- Role: `unknown` (evidence: file marked 'generated' by generated/vendor heuristics; operational role not inferred for generated/vendor files)
- Purpose clues:
  - module docstring: Explainable file-classification heuristics.
  - filename/path terms: rc classify
- Important symbols (5 total):
  - `is_test_path` (function) — line 30
  - `classify_file` (function) — line 42
  - `detect_generated_or_vendor` (function) — line 84
  - `detect_entrypoint_reason` (function) — line 100
  - `classify_operational_role` (function) — line 130
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `classify_file (dev_tools/repo_context/rc_classify.py:50)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`3dd9c774ba95531f…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_classify.py`)

### `dev_tools/repo_context/rc_common.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Shared constants, dataclasses, and small utilities for repo_context.
  - filename/path terms: rc common
- Important symbols (20 total):
  - `_looks_secret_shaped` (function) — line 86
  - `redact_secrets` (function) — line 121
  - `to_posix_rel` (function) — line 143
  - `sha256_file` (function) — line 147
  - `sha256_text` (function) — line 158
  - `count_lines_streaming` (function) — line 162
  - `stable_path_id` (function) — line 178
  - `sanitize_stem` (function) — line 182
  - `match_any_glob` (function) — line 187
  - `sniff_binary` (function) — line 194
  - `FileRecord` (class) — line 205
  - `SymbolRecord` (class) — line 223
  - `ImportRecord` (class) — line 242
  - `CallRecord` (class) — line 256
  - `ChunkRecord` (class) — line 269
  - `atomic_write_text` (function) — line 316
  - `atomic_write_bytes` (function) — line 324
  - `estimate_tokens` (function) — line 332
  - `get_git_info` (function) — line 336
  - `generated_output_exclude_paths` (function) — line 375
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `redact_secrets._sub (dev_tools/repo_context/rc_common.py:129)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`21a9f2c285350588…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_common.py`)

### `dev_tools/repo_context/rc_discover.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Lightweight, deterministic discovery: natural-language question ->
  - filename/path terms: rc discover
- Important symbols (2 total):
  - `_terms` (function) — line 29
  - `run_discover` (function) — line 51
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `run_discover (dev_tools/repo_context/rc_discover.py:65)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`8b8728dd73310e93…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_discover.py`)

### `dev_tools/repo_context/rc_graphify.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Optional Graphify adapter.
  - filename/path terms: rc graphify
- Important symbols (2 total):
  - `load_graphify_communities` (function) — line 20
  - `format_communities` (function) — line 143
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - (none resolved statically; see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`fb3d297b2f82e173…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_graphify.py`)

### `dev_tools/repo_context/rc_manifest.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Incremental reuse support + generation_manifest.json.
  - filename/path terms: rc manifest
- Important symbols (5 total):
  - `chunking_signature` (function) — line 18
  - `load_previous_state` (function) — line 28
  - `make_chunk_reuse_provider` (function) — line 81
  - `write_manifest` (function) — line 112
  - `utc_now_iso` (function) — line 152
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `make_chunk_reuse_provider (dev_tools/repo_context/rc_manifest.py:84)`
  - `make_chunk_reuse_provider (dev_tools/repo_context/rc_manifest.py:87)`
  - `write_manifest (dev_tools/repo_context/rc_manifest.py:131)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`a8ec68be54057149…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_manifest.py`)

### `dev_tools/repo_context/rc_overview.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: repository_overview.md and README.md generation.
  - filename/path terms: rc overview
- Important symbols (3 total):
  - `_top_dir` (function) — line 13
  - `generate_overview_md` (function) — line 18
  - `generate_readme_md` (function) — line 137
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `generate_overview_md (dev_tools/repo_context/rc_overview.py:36)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`32060399c7e77ec5…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_overview.py`)

### `dev_tools/repo_context/rc_packet.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Targeted context packet generation.
  - filename/path terms: rc packet
- Important symbols (17 total):
  - `_load_csv` (function) — line 17
  - `_norm_rel` (function) — line 24
  - `_file_is_fresh` (function) — line 28
  - `_iter_safe_lines` (function) — line 53
  - `_safe_excerpt` (function) — line 101
  - `_symbol_matches` (function) — line 108
  - `_find_symbol_candidates` (function) — line 113
  - `PacketOptions` (class) — line 121
  - `Budget` (class) — line 138
  - `_callers_of` (function) — line 154
  - `_callees_of` (function) — line 158
  - `_bfs_callers` (function) — line 162
  - `_bfs_callees` (function) — line 181
  - `_enclosing_class_or_func` (function) — line 201
  - `_candidate_tests_for_file` (function) — line 211
  - `_render_symbol_block` (function) — line 226
  - `generate_packet` (function) — line 263
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_bfs_callees (dev_tools/repo_context/rc_packet.py:188)`
  - `_bfs_callers (dev_tools/repo_context/rc_packet.py:169)`
  - `_find_symbol_candidates (dev_tools/repo_context/rc_packet.py:114)`
  - `_render_symbol_block (dev_tools/repo_context/rc_packet.py:238)`
  - `_render_symbol_block (dev_tools/repo_context/rc_packet.py:246)`
  - `_safe_excerpt (dev_tools/repo_context/rc_packet.py:102)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:264)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:265)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:266)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:267)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:275)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:286)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:294)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:299)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:479)`
  - ... and 14 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`690a0ff8a10663cf…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_packet.py`)

### `dev_tools/repo_context/rc_pyanalysis.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: AST-based Python symbol / import / call extraction.
  - filename/path terms: rc pyanalysis
- Important symbols (21 total):
  - `dotted_module_path` (function) — line 22
  - `_complexity_count` (function) — line 30
  - `complexity_approx` (function) — line 47
  - `_is_main_guard` (function) — line 51
  - `_is_static_false_test` (function) — line 62
  - `_is_static_true_test` (function) — line 76
  - `_unparse_safe` (function) — line 82
  - `format_params` (function) — line 91
  - `RawCall` (class) — line 126
  - `ClassInfo` (class) — line 137
  - `PyFileAnalysis` (class) — line 144
  - `_lambda_param_names` (function) — line 157
  - `_collect_local_bound_names` (function) — line 172
  - `_collect_module_reassigned_names` (function) — line 223
  - `analyze_python_source` (function) — line 246
  - `resolve_import_record` (function) — line 519
  - `build_import_bindings` (function) — line 569
  - `build_bindings_by_scope` (function) — line 597
  - `_lookup_in_scope_chain` (function) — line 606
  - `resolve_calls` (function) — line 657
  - `name_in_index` (function) — line 813
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_complexity_count (dev_tools/repo_context/rc_pyanalysis.py:43)`
  - `analyze_python_source (dev_tools/repo_context/rc_pyanalysis.py:258)`
  - `analyze_python_source (dev_tools/repo_context/rc_pyanalysis.py:276)`
  - `analyze_python_source (dev_tools/repo_context/rc_pyanalysis.py:510)`
  - `analyze_python_source (dev_tools/repo_context/rc_pyanalysis.py:515)`
  - `analyze_python_source.handle_class (dev_tools/repo_context/rc_pyanalysis.py:411)`
  - `analyze_python_source.handle_class (dev_tools/repo_context/rc_pyanalysis.py:420)`
  - `analyze_python_source.handle_class (dev_tools/repo_context/rc_pyanalysis.py:427)`
  - `analyze_python_source.handle_class (dev_tools/repo_context/rc_pyanalysis.py:442)`
  - `analyze_python_source.handle_func (dev_tools/repo_context/rc_pyanalysis.py:469)`
  - `analyze_python_source.handle_func (dev_tools/repo_context/rc_pyanalysis.py:470)`
  - `analyze_python_source.handle_func (dev_tools/repo_context/rc_pyanalysis.py:472)`
  - `analyze_python_source.handle_func (dev_tools/repo_context/rc_pyanalysis.py:476)`
  - `analyze_python_source.handle_func (dev_tools/repo_context/rc_pyanalysis.py:485)`
  - `analyze_python_source.make_call (dev_tools/repo_context/rc_pyanalysis.py:287)`
  - ... and 17 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`6250793f1eacbfb1…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_pyanalysis.py`)

### `dev_tools/repo_context/rc_request.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: packet_request.json: schema validation, deterministic selector
  - filename/path terms: rc request
- Important symbols (20 total):
  - `RequestError` (class) — line 42
  - `ResolvedRequest` (class) — line 49
  - `_is_safe_repo_relative_path` (function) — line 68
  - `validate_request_dict` (function) — line 82
  - `parse_and_validate_request` (function) — line 230
  - `SelectorResolution` (class) — line 272
  - `resolve_files` (function) — line 281
  - `resolve_symbols` (function) — line 295
  - `resolve_lines` (function) — line 320
  - `_scan_term_matches` (function) — line 346
  - `_RegexSearchTimeout` (class) — line 387
  - `_raise_regex_timeout` (function) — line 391
  - `_scan_term_matches_bounded` (function) — line 398
  - `resolve_search_terms` (function) — line 442
  - `_render_origin_header` (function) — line 525
  - `_render_excerpt_block` (function) — line 529
  - `_symbol_expansion` (function) — line 594
  - `_file_expansion` (function) — line 663
  - `generate_packet_from_request` (function) — line 758
  - `_res_to_dict` (function) — line 1279
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_raise_regex_timeout (dev_tools/repo_context/rc_request.py:392)`
  - `_scan_term_matches_bounded (dev_tools/repo_context/rc_request.py:431)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1009)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1015)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1047)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1066)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1070)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1078)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1128)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1140)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1261)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:773)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:786)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:787)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:788)`
  - ... and 29 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`da6b351bdc8071f0…`, chunked=yes (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_request.py`)

### `dev_tools/repo_context/rc_routing.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Hierarchical routing-catalog generation.
  - filename/path terms: rc routing
- Important symbols (13 total):
  - `RoutingOptions` (class) — line 37
  - `_sort_key` (function) — line 46
  - `_partition_files` (function) — line 50
  - `_byte_len` (function) — line 108
  - `_truncate_to_byte_limit` (function) — line 112
  - `_catalog_filenames` (function) — line 126
  - `_top_level_symbols` (function) — line 162
  - `_module_docstring` (function) — line 168
  - `_filename_terms` (function) — line 175
  - `_markdown_title` (function) — line 185
  - `_render_file_entry` (function) — line 228
  - `generate_routing` (function) — line 291
  - `_render_index` (function) — line 572
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_catalog_filenames (dev_tools/repo_context/rc_routing.py:151)`
  - `_catalog_filenames (dev_tools/repo_context/rc_routing.py:157)`
  - `_catalog_filenames (dev_tools/repo_context/rc_routing.py:158)`
  - `_render_file_entry (dev_tools/repo_context/rc_routing.py:234)`
  - `_render_file_entry (dev_tools/repo_context/rc_routing.py:235)`
  - `_render_file_entry (dev_tools/repo_context/rc_routing.py:246)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:364)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:372)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:376)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:379)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:383)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:420)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:422)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:437)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:531)`
  - ... and 1 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`c5d518e7937a1645…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_routing.py`)

### `dev_tools/repo_context/rc_scan.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Filesystem walk + inventory + Python analysis orchestration.
  - filename/path terms: rc scan
- Important symbols (9 total):
  - `ScanOptions` (class) — line 28
  - `ScanResult` (class) — line 48
  - `_sort_key` (function) — line 62
  - `_is_ancestor` (function) — line 66
  - `_should_exclude_file` (function) — line 74
  - `_walk` (function) — line 88
  - `scan_repository` (function) — line 175
  - `_cleanup_stale_chunks` (function) — line 340
  - `_resolve_python_relationships` (function) — line 359
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_walk.recurse (dev_tools/repo_context/rc_scan.py:137)`
  - `_walk.recurse (dev_tools/repo_context/rc_scan.py:138)`
  - `_walk.recurse (dev_tools/repo_context/rc_scan.py:98)`
  - `scan_repository (dev_tools/repo_context/rc_scan.py:176)`
  - `scan_repository (dev_tools/repo_context/rc_scan.py:186)`
  - `scan_repository (dev_tools/repo_context/rc_scan.py:206)`
  - `scan_repository (dev_tools/repo_context/rc_scan.py:335)`
  - `scan_repository (dev_tools/repo_context/rc_scan.py:336)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`6ebef0788146efcb…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_scan.py`)

### `dev_tools/repo_context/rc_tree.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Deterministic repository_tree.txt generation.
  - filename/path terms: rc tree
- Important symbols (7 total):
  - `_sort_key` (function) — line 7
  - `_human_size` (function) — line 11
  - `_Node` (class) — line 19
  - `_build_tree` (function) — line 29
  - `_render` (function) — line 52
  - `generate_tree_text` (function) — line 75
  - `write_tree` (function) — line 82
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_build_tree (dev_tools/repo_context/rc_tree.py:30)`
  - `_build_tree (dev_tools/repo_context/rc_tree.py:37)`
  - `_build_tree (dev_tools/repo_context/rc_tree.py:38)`
  - `_build_tree (dev_tools/repo_context/rc_tree.py:45)`
  - `_build_tree (dev_tools/repo_context/rc_tree.py:46)`
  - `_render (dev_tools/repo_context/rc_tree.py:62)`
  - `_render (dev_tools/repo_context/rc_tree.py:64)`
  - `_render (dev_tools/repo_context/rc_tree.py:72)`
  - `generate_tree_text (dev_tools/repo_context/rc_tree.py:76)`
  - `generate_tree_text (dev_tools/repo_context/rc_tree.py:78)`
  - `write_tree (dev_tools/repo_context/rc_tree.py:83)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`fcff5535df129fa5…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_tree.py`)

## Other files (non-Python)

| Path | Title/summary | Role |
|---|---|---|
| `dev_tools/repo_context/E2E_VERIFICATION.md` | Discovery-to-packet workflow: end-to-end verification | `unknown` |
| `dev_tools/repo_context/examples/packet_request.invalid_ambiguous_line_range.json` | packet request.invalid ambiguous line range | `unknown` |
| `dev_tools/repo_context/examples/packet_request.invalid_empty_selectors.json` | packet request.invalid empty selectors | `unknown` |
| `dev_tools/repo_context/examples/packet_request.invalid_path_traversal.json` | packet request.invalid path traversal | `unknown` |
| `dev_tools/repo_context/examples/packet_request.invalid_schema_version.json` | packet request.invalid schema version | `unknown` |
| `dev_tools/repo_context/examples/packet_request.invalid_unknown_field.json` | packet request.invalid unknown field | `unknown` |
| `dev_tools/repo_context/examples/packet_request.valid.json` | packet request.valid | `unknown` |
| `dev_tools/repo_context/README.md` | repo_context | `unknown` |
| `dev_tools/repo_context/schema/packet_request.schema.json` | packet request.schema | `unknown` |

## Omitted from this catalog (size limit reached)

3 file(s) omitted; see `file_inventory.csv` / `routing/routing_manifest.json` for the complete list.

- `dev_tools/repo_context/rc_validate.py`
- `dev_tools/repo_context/rc_writers.py`
- `dev_tools/repo_context/repo_context.py`

