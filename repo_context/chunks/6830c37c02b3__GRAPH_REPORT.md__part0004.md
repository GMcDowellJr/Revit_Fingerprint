# Chunk of graphify-out/GRAPH_REPORT.md

- Source relative path: `graphify-out/GRAPH_REPORT.md`
- Chunk: 4 of 4
- Original line range: 1202-1511
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 50653c6b8a269da31349c8c44444133fa7ab40b96b82ad274a5df417fda7f704
- Starts inside symbol: no
- Ends inside symbol: no

```
  1202| ### Community 202 - "Fingerprint Export Discovery"
  1203| Cohesion: 0.56
  1204| Nodes (9): _fingerprint_payload(), Path, test_detect_surfaces_counts_fingerprint_separately(), test_domain_discovery_prefers_fingerprint_candidates(), test_iter_export_files_prioritizes_fingerprint_and_uses_none_secondary(), test_load_exports_prefers_fingerprint_files_before_plain_fallback(), test_pick_sample_file_prefers_fingerprint_and_falls_back_to_split(), _write_json() (+1 more)
  1205| 
  1206| ### Community 203 - "Probe Inventory Testing"
  1207| Cohesion: 0.51
  1208| Nodes (9): _read_csv_rows(), _run(), _run_shaped_payload(), test_all_inputs_invalid_refuses_to_overwrite_by_default(), test_empty_probes_dir_refuses_to_overwrite_by_default(), test_empty_probes_dir_with_force_writes_empty_inventory(), test_merges_across_legacy_and_run_shapes_for_same_domain(), test_merges_and_dedupes_across_dated_runs() (+1 more)
  1209| 
  1210| ### Community 205 - "Reference Bundle Validation"
  1211| Cohesion: 0.33
  1212| Nodes (8): Path, test_load_and_validate_allows_legacy_control_characters(), test_load_and_validate_allows_raw_newline_in_string(), _escape_control_chars_in_json_strings(), load_and_validate(), Path, Return JSON text with raw control characters escaped only inside strings., write_sidecar()
  1213| 
  1214| ### Community 206 - "Annotation Failure Testing"
  1215| Cohesion: 0.27
  1216| Nodes (4): Regression scenario: if annotate_name_target_combined_files() (or…, Regression scenario: _clear_stale_name_all_ before_run() is called before…, TestAnnotationFailureFailsTheSegment, TestClearStaleNameAllFailureFailsTheSegment
  1217| 
  1218| ### Community 207 - "Dimension Types Prompts"
  1219| Cohesion: 0.36
  1220| Nodes (9): build_prompt(), _fmt_accuracy(), _fmt_witness(), _format_identity_items(), _get_shape(), Any, tools/label_synthesis/domain_prompts/dimension_types.py LLM system prompt and…, Return a brief shape-specific note to insert before the parameters. (+1 more)
  1221| 
  1222| ### Community 208 - "Fill Patterns Prompts"
  1223| Cohesion: 0.40
  1224| Nodes (9): build_prompt(), _extract_grid_geometry(), _get_identity_value(), _infer_geometry_description(), _is_angle_close(), _is_opaque_fallback(), _normalise_angle(), Any (+1 more)
  1225| 
  1226| ### Community 209 - "Shape-Gating Validation"
  1227| Cohesion: 0.39
  1228| Nodes (8): Validate shape-gating semantics and return structured issues.      Args:, validate_domain_join_key_policy(), test_rule_a1_discriminator_first_required(), test_rule_a2_no_overlap_common_required(), test_rule_a3_additional_required_in_optional_items(), test_rule_a4_requires_non_empty_additional_required(), test_rule_a5_orphaned_keys_warning_only(), test_valid_shape_gated_policy_has_no_errors()
  1229| 
  1230| ### Community 210 - "RevitLookup Sync"
  1231| Cohesion: 0.42
  1232| Nodes (8): fetch_raw(), get_current_commit_sha(), github_get(), list_all_cs_files(), main(), Path, sync_revitlookup_reference.py  Copies RevitLookup descriptor source files into t, sync()
  1233| 
  1234| ### Community 211 - "Filesystem Operation Retry"
  1235| Cohesion: 0.33
  1236| Nodes (4): A cloud-synced segments root (OneDrive, etc.) can transiently lock a…, TestRetryFsOp, Run a filesystem-mutating callable (shutil.move / shutil.rmtree), retrying with…, retry_fs_op()
  1237| 
  1238| ### Community 212 - "Comparison Type Coverage"
  1239| Cohesion: 0.22
  1240| Nodes (9): Task-spec decision: known_limitations/warnings text must be mechanical/ factual…, test_comparison_type_coverage_flags_unrecognized(), test_comparison_type_coverage_ignores_blank_values(), test_comparison_type_coverage_intentionally_excluded_is_distinct_from_unrecognized(), test_comparison_type_coverage_recognized_only(), test_health_reports_unrecognized_comparison_type_as_warning(), test_health_warning_and_limitation_text_has_no_severity_language(), comparison_type_coverage() (+1 more)
  1241| 
  1242| ### Community 213 - "Governance Field Completeness"
  1243| Cohesion: 0.50
  1244| Nodes (8): _row(), test_blank_business_center_label_fails_with_export_run_id(), test_blank_client_label_fails_with_export_run_id(), test_fully_populated_row_passes(), test_multiple_offenders_all_reported(), test_na_spelling_fails_same_as_blank(), _check_governance_field_completeness(), Hard-fail if any file_metadata.csv row has a blank or N/A-spelled client_label…
  1245| 
  1246| ### Community 214 - "Unit Testing"
  1247| Cohesion: 0.23
  1248| Nodes (9): _domain_identity_registry_v2(), _FakeDecimalSymbol, _FakeDigitGroupingSymbol, _FakeDoc, _FakeDocUnitsUnreadable, _FakeUnits, object, test_extract_units_doc_emits_exactly_one_populated_record() (+1 more)
  1249| 
  1250| ### Community 215 - "Name Target Cleanup"
  1251| Cohesion: 0.33
  1252| Nodes (4): Regression scenario: a failure in step 2b (name-pattern generation) or step 3…, TestClearStaleNameAllBeforeRun, _clear_stale_name_all_before_run(), Clear this segment's stale name-leg BI-facing output before any step of this…
  1253| 
  1254| ### Community 216 - "Pareto Analysis with Splits"
  1255| Cohesion: 0.31
  1256| Nodes (8): assess_split_likelihood(), detect_pareto_cliffs(), main(), DataFrame, Run Pareto analysis with automatic split detection., Detect cliffs in Pareto front that indicate splits., Assess likelihood of organizational split based on Pareto cliffs., run_pareto_with_split_detection()
  1257| 
  1258| ### Community 217 - "Typography Surface Extraction"
  1259| Cohesion: 0.50
  1260| Nodes (8): _extract_features(), _get_p2_value(), _get_top(), main(), _norm_scalar(), Any, Extract the typography surfaces we care about, preferring top-level where presen, run()
  1261| 
  1262| ### Community 218 - "View Category Overrides Analysis"
  1263| Cohesion: 0.31
  1264| Nodes (8): analyze_override_patterns(), _extract_override_record(), main(), View Category Overrides Join Key Discovery  Hypothesis: Override identity = base, Compute a stable hash for delta items (k/v pairs) to model delta_sig_hash., Return (baseline_sig, delta_sig, delta_items, record_id, label)., Analyze view_category_overrides for join key discovery.      Metrics:     - Base, _stable_delta_hash()
  1265| 
  1266| ### Community 219 - "Graphify Reference Exports"
  1267| Cohesion: 0.25
  1268| Nodes (7): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)
  1269| 
  1270| ### Community 220 - "Audit Pipeline Integration"
  1271| Cohesion: 0.36
  1272| Nodes (8): Audit 1 — Archetype Pipeline + Pipeline Integration, Audit 2 — Label Synthesis Layer, Audit 3 — Extraction Layer & Contracts, Audit 4 — Bundle Analysis, Cross-Segment Pipeline & Structural Debt, Audit 5 — Step 0 Inventory: Existing Identity, Hashing, and Parameterization Machinery, Audit 6 — Step 0-within-PR1: Per-Domain `label.display` Item Mapping and Final Eligibility List, Audit 7 — PR2 Item 0 (inline/analysis-side agreement) and Item 3 (CLI flag naming), Repository Operational Review
  1273| 
  1274| ### Community 221 - "Record Contract Schema"
  1275| Cohesion: 0.25
  1276| Nodes (7): additionalProperties, allOf, $id, required, $schema, title, type
  1277| 
  1278| ### Community 222 - "Git Commit Hook Integration"
  1279| Cohesion: 0.25
  1280| Nodes (8): Git Commit Hook, For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration, Incremental Update Process, For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only
  1281| 
  1282| ### Community 223 - "Patch Documentation"
  1283| Cohesion: 0.25
  1284| Nodes (7): Change set, Evidence, Intent, Invariants, PATCH PR — Non-breaking only, Post-conditions, Reviewer checklist
  1285| 
  1286| ### Community 224 - "RevitLookup Domain Mapping"
  1287| Cohesion: 0.25
  1288| Nodes (7): Descriptor files that don't map to current domains, Direct descriptor coverage, Future domains — descriptor availability, How to read a descriptor against an extractor, Indirect / compound coverage, (potential future domain signals), RevitLookup Descriptor → Fingerprint Domain Map
  1289| 
  1290| ### Community 225 - "Cross Segment Streaming Test"
  1291| Cohesion: 0.50
  1292| Nodes (7): _build_sibling_fixture(), Path, Regression test for streamed (incremental) cross_segment_file_pairs.csv writes.…, test_failure_after_streaming_leaves_previous_pairs_file_untouched(), test_main_streams_real_file_pair_rows_to_disk(), _write_csv(), _write_segment()
  1293| 
  1294| ### Community 226 - "Identity Items Lookup Builder"
  1295| Cohesion: 0.39
  1296| Nodes (7): build_lookup(), _find_file(), main(), Path, tools/label_synthesis/build_identity_items_lookup.py Pre-processing step for…, Return (key_col, value_col, quality_col) from a header row. Supports both…, _sniff_item_columns()
  1297| 
  1298| ### Community 227 - "Line Patterns Prompts"
  1299| Cohesion: 0.43
  1300| Nodes (7): build_prompt(), _format_identity_items(), _is_opaque_name(), Any, tools/label_synthesis/domain_prompts/line_patterns.py  LLM system prompt and pro, Heuristic: name is opaque if it's all-caps/numeric with no spaces and     doesn', _strip_import_prefix()
  1301| 
  1302| ### Community 228 - "Line Styles Prompts"
  1303| Cohesion: 0.39
  1304| Nodes (7): build_prompt(), _fmt_color(), _format_identity_items(), Any, tools/label_synthesis/domain_prompts/line_styles.py  LLM system prompt and promp, Strip 'Lines|' parent prefix; return None for self-referential paths., _strip_lines_prefix()
  1305| 
  1306| ### Community 229 - "View Filter Definitions Prompts"
  1307| Cohesion: 0.50
  1308| Nodes (7): build_prompt(), _collect_rules(), _format_rule_summary(), _get_value(), _is_opaque_name(), _op_short(), Any
  1309| 
  1310| ### Community 230 - "Wall Type Reset"
  1311| Cohesion: 0.36
  1312| Nodes (7): _is_function_only_block(), main(), Path, Reset wall_type records that are blocked solely because wt.function=unsupported., Return {record_pk: {key: q}} for all wall_types items., True if wt.function is the only non-ok required item and compound structure item, _read_wall_items()
  1313| 
  1314| ### Community 231 - "Graphify Query Commands"
  1315| Cohesion: 0.29
  1316| Nodes (7): Query Commands, For /graphify explain, For /graphify path, graphify reference: query, path, explain, Transcription Process, graphify reference: transcribe video and audio, Step 2.5 - Transcribe video / audio files (only if video files detected)
  1317| 
  1318| ### Community 232 - "Context Dictionary Schema"
  1319| Cohesion: 0.29
  1320| Nodes (7): Context Dictionary Schema, Dependency Contract, Design Intent, Layer 0 — Core (Pure Python), Layer 1 — Domain Extractors (Revit-aware), Layer 2 — Context Builder, Layer 3 — Runner (Host-specific)
  1321| 
  1322| ### Community 233 - "Text Types Conversion Test"
  1323| Cohesion: 0.43
  1324| Nodes (4): _extract_record(), _Id, test_converted_old_and_new_records_converge(), _Type
  1325| 
  1326| ### Community 234 - "Name Key Policy Application"
  1327| Cohesion: 0.57
  1328| Nodes (6): _iter_domain_payloads(), _iter_export_paths(), main(), Any, Path, _rows_for_export()
  1329| 
  1330| ### Community 235 - "Element Dominance Emission"
  1331| Cohesion: 0.62
  1332| Nodes (6): emit_element_dominance(), main(), Path, _read_csv_rows(), _split_label(), _write_csv_atomic()
  1333| 
  1334| ### Community 236 - "Arrowheads Prompts"
  1335| Cohesion: 0.52
  1336| Nodes (6): build_prompt(), _detect_record_class(), _fmt_size(), _format_identity_items(), Any, tools/label_synthesis/domain_prompts/arrowheads.py  LLM system prompt and prompt
  1337| 
  1338| ### Community 237 - "Text Types Prompts"
  1339| Cohesion: 0.48
  1340| Nodes (6): build_prompt(), _fmt_color(), _fmt_size(), _format_identity_items(), Any, tools/label_synthesis/domain_prompts/text_types.py  LLM system prompt and prompt
  1341| 
  1342| ### Community 238 - "Governance Analysis Utility"
  1343| Cohesion: 0.33
  1344| Nodes (6): Mapping Line Patterns Revit Mapping Utility, Compare Cross Segment, Compare Governance Populations, Generate Governance Narrative, Governance Findings JSON, Governance Manifest
  1345| 
  1346| ### Community 240 - "Repository Data Remediation"
  1347| Cohesion: 0.53
  1348| Nodes (5): Regression checks for repository-neutral runtime and sample configuration., _read(), test_default_client_sector_policy_uses_synthetic_labels(), test_dynamo_graphs_embed_current_runners_without_workstation_paths(), test_runner_install_discovery_has_only_generic_defaults()
  1349| 
  1350| ### Community 241 - "Active Domains Testing"
  1351| Cohesion: 0.47
  1352| Nodes (3): TestActiveDomainsFromNamePatterns, _active_domains_from_name_patterns(), Same purpose as _active_domains_from_presence_csv(), but for the name-…
  1353| 
  1354| ### Community 242 - "Text Types Export Test"
  1355| Cohesion: 0.47
  1356| Nodes (3): _Id, test_text_types_extract_emits_flat_items_only(), _Type
  1357| 
  1358| ### Community 243 - "Line Styles Synopsis Formatter"
  1359| Cohesion: 0.40
  1360| Nodes (5): _format_rgb(), format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/line_styles.py  Behavioral synopsis fo, Convert 'R-G-B' to '#RRGGBB' hex.
  1361| 
  1362| ### Community 244 - "Object Styles Annotation Formatter"
  1363| Cohesion: 0.40
  1364| Nodes (5): _format_rgb(), format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/object_styles_annotation.py…, Convert 'R-G-B' to '#RRGGBB' hex.
  1365| 
  1366| ### Community 245 - "Object Styles Model Formatter"
  1367| Cohesion: 0.40
  1368| Nodes (5): _format_rgb(), format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/object_styles_model.py Behavioral…, Convert 'R-G-B' to '#RRGGBB' hex.
  1369| 
  1370| ### Community 246 - "Pairwise Analysis"
  1371| Cohesion: 0.60
  1372| Nodes (5): load_csv(), main(), Any, Path, write_csv()
  1373| 
  1374| ### Community 247 - "Intradomain Definition Emission"
  1375| Cohesion: 0.47
  1376| Nodes (5): emit_ids_artifacts(), IDS, main(), _make_ids_ids(), Stable mapping from standard_name -> IDS_### (sorted by name).
  1377| 
  1378| ### Community 248 - "Validation Constraints"
  1379| Cohesion: 0.40
  1380| Nodes (5): minLength, pattern, status_reasons, items, type
  1381| 
  1382| ### Community 250 - "Pareto Shape Gating Test"
  1383| Cohesion: 0.60
  1384| Nodes (4): Path, test_pareto_shape_gating_per_shape(), _write_csv(), xfail
  1385| 
  1386| ### Community 251 - "Segment Name Leg Output Test"
  1387| Cohesion: 0.50
  1388| Nodes (3): TestSegmentHasNameLegOutput, Whether this segment's name-projection leg (step 2b/3b/BI-merge-name) has…, _segment_has_name_leg_output()
  1389| 
  1390| ### Community 252 - "Arrowheads Synopsis Formatter"
  1391| Cohesion: 0.50
  1392| Nodes (4): format_synopsis(), _inches_to_fraction(), Any, tools/label_synthesis/synopsis_formatters/arrowheads.py  Behavioral synopsis for
  1393| 
  1394| ### Community 253 - "Text Types Synopsis Formatter"
  1395| Cohesion: 0.50
  1396| Nodes (4): format_synopsis(), _inches_to_fraction(), Any, tools/label_synthesis/synopsis_formatters/text_types.py  Behavioral synopsis for
  1397| 
  1398| ### Community 254 - "Sweep Line Pattern Analysis"
  1399| Cohesion: 0.70
  1400| Nodes (4): compute_norm_hash_for_group(), detect_cols(), main(), md5s()
  1401| 
  1402| ### Community 255 - "Graphify Add Watch Reference"
  1403| Cohesion: 0.50
  1404| Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder
  1405| 
  1406| ### Community 256 - "Graphify Hooks Reference"
  1407| Cohesion: 0.50
  1408| Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration
  1409| 
  1410| ### Community 257 - "Graphify Query Reference"
  1411| Cohesion: 0.50
  1412| Nodes (3): For /graphify explain, For /graphify path, graphify reference: query, path, explain
  1413| 
  1414| ### Community 258 - "Graphify Update Reference"
  1415| Cohesion: 0.50
  1416| Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only
  1417| 
  1418| ### Community 259 - "Validation Constraints"
  1419| Cohesion: 0.50
  1420| Nodes (4): minLength, pattern, type, domain
  1421| 
  1422| ### Community 260 - "Policy Load Integration Test"
  1423| Cohesion: 0.50
  1424| Nodes (3): Documented integration test patterns for full Revit validation., Policy load integration pattern placeholder., TestPolicyLoadPattern
  1425| 
  1426| ### Community 261 - "Filtered Element Collector Policy"
  1427| Cohesion: 0.67
  1428| Nodes (3): PR5 policy:     - Domains must not directly import or reference FilteredElementC, _repo_root(), test_domains_do_not_reference_filtered_element_collector()
  1429| 
  1430| ### Community 262 - "Sentinel Policy Enforcement"
  1431| Cohesion: 0.67
  1432| Nodes (3): Enforces PR3 sentinel policy:      - Domains may not contain any "<Token>" liter, _repo_root(), test_domains_do_not_emit_extra_angle_bracket_tokens()
  1433| 
  1434| ### Community 263 - "Signature Hash Policy Generation"
  1435| Cohesion: 0.67
  1436| Nodes (3): build_policy(), main(), Any
  1437| 
  1438| ### Community 264 - "Label Population Builder"
  1439| Cohesion: 0.83
  1440| Nodes (3): build_label_population(), main(), Path
  1441| 
  1442| ### Community 265 - "Fill Patterns Synopsis Formatter"
  1443| Cohesion: 0.50
  1444| Nodes (3): format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/fill_patterns.py  Behavioral synopsis
  1445| 
  1446| ### Community 266 - "Line Patterns Synopsis Formatter"
  1447| Cohesion: 0.50
  1448| Nodes (3): format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/line_patterns.py  Behavioral synopsis
  1449| 
  1450| ### Community 267 - "Phase Filters Synopsis Formatter"
  1451| Cohesion: 0.50
  1452| Nodes (3): format_synopsis(), Any, tools/label_synthesis/synopsis_formatters/phase_filters.py  Behavioral synopsis
  1453| 
  1454| ### Community 268 - "First Record Extraction"
  1455| Cohesion: 0.67
  1456| Nodes (3): extract_first_records(), main(), Any
  1457| 
  1458| ### Community 271 - "Identity Items Audit Findings"
  1459| Cohesion: 0.67
  1460| Nodes (3): Audit 13 Identity Items Monolithic vs Shard Step 0 Findings, Audit 14 PR 2: Port Discover Join Policy and Suggest Discovery Params to Shard-Preferred Loading, Audit 15 PR 4: Stop Writing the Monolithic Identity Items CSV
  1461| 
  1462| ### Community 272 - "Purgeable Type Definition"
  1463| Cohesion: 0.67
  1464| Nodes (3): enum, type, is_purgeable_q
  1465| 
  1466| ### Community 273 - "Join Key Shape-Gating Schema"
  1467| Cohesion: 0.67
  1468| Nodes (3): Join Key Shape-Gating Schema Extension, Phase-2 Identity & Semantics Refactor Plan, Phase 2 — Join-Key Discovery
  1469| 
  1470| ### Community 275 - "Fill Patterns Extraction"
  1471| Cohesion: 0.39
  1472| Nodes (7): _collect_fill_patterns(), _export_fill_pattern_ctx(), extract_drafting(), extract_model(), # IMPORTANT: grid order is identity-significant; do NOT sort the preimage., # NOTE: name/uid/elem_id are labels/metadata and MUST NOT participate in…, test_fill_pattern_ctx_contract_exports_specials_and_preserves_uid_map()
  1473| 
  1474| ### Community 276 - "Governance Narrative Testing"
  1475| Cohesion: 0.67
  1476| Nodes (3): parametrize, The shipped JSON and generate_governance_narrative.py's own _POLICY_DEFAULTS…, test_shipped_policy_file_matches_python_default_profile()
  1477| 
  1478| ### Community 277 - "System Extraction Tests"
  1479| Cohesion: 0.67
  1480| Nodes (3): fixture, Normal extraction tests model production's required System.Guid surface., _system_guid_available()
  1481| 
  1482| ### Community 280 - "Subprocess Step Execution"
  1483| Cohesion: 0.67
  1484| Nodes (3): CompletedProcess, Run a subprocess step, capturing stderr, raising on non-zero exit., run_step()
  1485| 
  1486| ### Community 356 - "Governance Policy Reset"
  1487| Cohesion: 0.67
  1488| Nodes (3): fixture, Undo any apply_governance_policy() call a test makes, so overridden…, _reset_governance_policy()
  1489| 
  1490| ## Knowledge Gaps
  1491| - **255 isolated node(s):** `EvalConfig`, `_FakeDecimalSymbol`, `_FakeDigitGroupingSymbol`, `type`, `enum` (+250 more)
  1492|   These have ≤1 connection - possible missing edges or undocumented components.
  1493| - **68 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.
  1494| 
  1495| ## Suggested Questions
  1496| _Questions this graph is uniquely positioned to answer:_
  1497| 
  1498| - **Why does `load_exports()` connect `Domain Payload Management` to `Intradomain Summary Building`, `Typography Surface Extraction`, `Export File Management`, `Fingerprint Export Discovery`, `Attribute Stability Analysis`, `Signature Profile Analysis`, `Join Key Derivation by IDs`, `Join Hash Parameter Population`, `Join Key Application`, `Element Level Classification`, `Collision Differencing`, `Domain Identity Contract`?**
  1499|   _High betweenness centrality (0.053) - this node is a cross-community bridge._
  1500| - **Why does `run_fingerprint()` connect `Extraction Context Building` to `Element ID Canonicalization`, `Feature Extraction`, `Canonical Item Management`, `Cache Key Management`, `Domain Contract Management`, `JSON Diffing`, `Type Collection Utilities`, `Timing Data Collection`, `Dependency Management`, `JSON Loading Utilities`, `Domain Extraction`?**
  1501|   _High betweenness centrality (0.047) - this node is a cross-community bridge._
  1502| - **Why does `main()` connect `JSON Loading Utilities` to `Feature Extraction`, `Governance Narrative Evidence`, `JSON Diffing`, `Extraction Context Building`, `Manifest Row Testing`?**
  1503|   _High betweenness centrality (0.045) - this node is a cross-community bridge._
  1504| - **Are the 3 inferred relationships involving `safe_str()` (e.g. with `_as_string()` and `_as_value_string()`) actually correct?**
  1505|   _`safe_str()` has 3 INFERRED edges - model-reasoned connections that need verification._
  1506| - **Are the 79 inferred relationships involving `_build_segments()` (e.g. with `test_ancestor_segment_ids_semicolon_joined_not_pipe()` and `test_ancestor_segment_ids_two_element_roundtrip()`) actually correct?**
  1507|   _`_build_segments()` has 79 INFERRED edges - model-reasoned connections that need verification._
  1508| - **What connects `EvalConfig`, `_FakeDecimalSymbol`, `_FakeDigitGroupingSymbol` to the rest of the system?**
  1509|   _255 weakly-connected nodes found - possible documentation gaps or missing edges._
  1510| - **Should `String Canonicalization` be split into smaller, more focused modules?**
  1511|   _Cohesion score 0.08860759493670886 - nodes in this community are weakly interconnected._
```
