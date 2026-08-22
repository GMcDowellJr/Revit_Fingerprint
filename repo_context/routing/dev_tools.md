# Routing catalog: `dev_tools`

- Generated (UTC): 2026-08-22T09:43:57Z
- Tool version: 0.1.0
- Files covered: 25
- Catalog source hash (sha256 of sorted `path:sha256` pairs): `94310d041f90ec48ba6ef544a42a845b19867e612e4ad83f7340fc6a16c10e12`
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
  - `_looks_secret_shaped` (function) — line 71
  - `redact_secrets` (function) — line 100
  - `to_posix_rel` (function) — line 122
  - `sha256_file` (function) — line 126
  - `sha256_text` (function) — line 137
  - `count_lines_streaming` (function) — line 141
  - `stable_path_id` (function) — line 157
  - `sanitize_stem` (function) — line 161
  - `match_any_glob` (function) — line 166
  - `sniff_binary` (function) — line 173
  - `FileRecord` (class) — line 184
  - `SymbolRecord` (class) — line 202
  - `ImportRecord` (class) — line 221
  - `CallRecord` (class) — line 235
  - `ChunkRecord` (class) — line 248
  - `atomic_write_text` (function) — line 295
  - `atomic_write_bytes` (function) — line 303
  - `estimate_tokens` (function) — line 311
  - `get_git_info` (function) — line 315
  - `generated_output_exclude_paths` (function) — line 354
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `redact_secrets._sub (dev_tools/repo_context/rc_common.py:108)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`d59f767e4e1eb828…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_common.py`)

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
- Retrieval identity: sha256=`bd8a84e48c5b7790…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_discover.py`)

### `dev_tools/repo_context/rc_graphify.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Optional Graphify adapter.
  - filename/path terms: rc graphify
- Important symbols (2 total):
  - `load_graphify_communities` (function) — line 20
  - `format_communities` (function) — line 126
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - (none resolved statically; see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`0220d396b2b75d19…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_graphify.py`)

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
- Important symbols (16 total):
  - `_load_csv` (function) — line 17
  - `_norm_rel` (function) — line 24
  - `_file_is_fresh` (function) — line 28
  - `_safe_excerpt` (function) — line 53
  - `_symbol_matches` (function) — line 75
  - `_find_symbol_candidates` (function) — line 80
  - `PacketOptions` (class) — line 88
  - `Budget` (class) — line 105
  - `_callers_of` (function) — line 121
  - `_callees_of` (function) — line 125
  - `_bfs_callers` (function) — line 129
  - `_bfs_callees` (function) — line 148
  - `_enclosing_class_or_func` (function) — line 168
  - `_candidate_tests_for_file` (function) — line 178
  - `_render_symbol_block` (function) — line 193
  - `generate_packet` (function) — line 230
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_bfs_callees (dev_tools/repo_context/rc_packet.py:155)`
  - `_bfs_callers (dev_tools/repo_context/rc_packet.py:136)`
  - `_find_symbol_candidates (dev_tools/repo_context/rc_packet.py:81)`
  - `_render_symbol_block (dev_tools/repo_context/rc_packet.py:205)`
  - `_render_symbol_block (dev_tools/repo_context/rc_packet.py:213)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:231)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:232)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:233)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:234)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:242)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:253)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:261)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:266)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:446)`
  - `generate_packet (dev_tools/repo_context/rc_packet.py:450)`
  - ... and 13 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`51c343731f80f2f0…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_packet.py`)

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
- Important symbols (19 total):
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
  - `_RegexSearchTimeout` (class) — line 375
  - `_raise_regex_timeout` (function) — line 379
  - `_scan_term_matches_bounded` (function) — line 386
  - `resolve_search_terms` (function) — line 430
  - `_render_origin_header` (function) — line 511
  - `_render_excerpt_block` (function) — line 515
  - `_symbol_expansion` (function) — line 550
  - `generate_packet_from_request` (function) — line 699
  - `_res_to_dict` (function) — line 1157
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_raise_regex_timeout (dev_tools/repo_context/rc_request.py:380)`
  - `_scan_term_matches_bounded (dev_tools/repo_context/rc_request.py:419)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:1139)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:714)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:727)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:728)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:729)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:730)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:738)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:815)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:828)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:859)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:865)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:897)`
  - `generate_packet_from_request (dev_tools/repo_context/rc_request.py:916)`
  - ... and 27 more (see python_calls.csv)
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`50a7a8ece86c108e…`, chunked=yes (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_request.py`)

### `dev_tools/repo_context/rc_routing.py`
- Role: `developer_utility` (evidence: no `__main__` guard; located under developer-tooling directory 'dev_tools/')
- Purpose clues:
  - module docstring: Hierarchical routing-catalog generation.
  - filename/path terms: rc routing
- Important symbols (11 total):
  - `RoutingOptions` (class) — line 37
  - `_sort_key` (function) — line 46
  - `_partition_files` (function) — line 50
  - `_catalog_filenames` (function) — line 107
  - `_top_level_symbols` (function) — line 138
  - `_module_docstring` (function) — line 144
  - `_filename_terms` (function) — line 151
  - `_markdown_title` (function) — line 161
  - `_render_file_entry` (function) — line 196
  - `generate_routing` (function) — line 259
  - `_render_index` (function) — line 540
- Entrypoint evidence: none
- Internal dependencies (resolved imports within this repository):
  - (none resolved; see python_imports.csv for unresolved/external imports)
- Called by (high/medium-confidence static callers):
  - `_render_file_entry (dev_tools/repo_context/rc_routing.py:202)`
  - `_render_file_entry (dev_tools/repo_context/rc_routing.py:203)`
  - `_render_file_entry (dev_tools/repo_context/rc_routing.py:214)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:332)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:340)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:344)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:347)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:351)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:388)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:390)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:405)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:499)`
  - `generate_routing (dev_tools/repo_context/rc_routing.py:515)`
- Related tests:
  - (none found via resolved imports/calls)
- Retrieval identity: sha256=`1ea94b1923cbc2d9…`, chunked=no (see chunk_manifest.csv / file_inventory.csv for `dev_tools/repo_context/rc_routing.py`)

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

