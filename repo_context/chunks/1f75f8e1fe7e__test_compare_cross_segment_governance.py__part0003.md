# Chunk of tests/test_compare_cross_segment_governance.py

- Source relative path: `tests/test_compare_cross_segment_governance.py`
- Chunk: 3 of 5
- Original line range: 993-1499
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: test_reference_analysis_segment_uses_presence_for_multi_file_fallback, test_build_governance_state_rows_include_inherited_unused_and_local_active, test_pair_domain_work_items_use_pair_domain_union, test_output_row_sort_helpers_are_stable_by_content, test_non_project_target_blanks_used_summary_shares, test_main_emits_governance_states_when_pair_skipped_by_min_patterns, test_main_skips_delta_generation_for_blocked_reference, _union_rows_for, test_union_inventory_project_all_view_normalized_union, test_union_inventory_project_used_view_normalized_union
- Source SHA-256: 41a98d942cef2b25dee2bd74f79b3ba9f6e871cbbff68d9ef81011f7e3336043
- Starts inside symbol: no
- Ends inside symbol: no

```
   993| def test_reference_analysis_segment_uses_presence_for_multi_file_fallback(tmp_path):
   994|     segments_root = tmp_path / "segments"
   995|     registry = {"generic": {"output_folder": "generic"}}
   996|     _write_reference_analysis_segment(
   997|         segments_root,
   998|         "generic",
   999|         "line_patterns",
  1000|         [
  1001|             {"pattern_id": "g1", "join_hash": "provided_a"},
  1002|             {"pattern_id": "g2", "join_hash": "provided_b"},
  1003|         ],
  1004|         export_run_ids=["file_a", "file_b"],
  1005|         presence_rows=[
  1006|             {"export_run_id": "file_a", "pattern_id": "g1"},
  1007|             {"export_run_id": "file_b", "pattern_id": "g2"},
  1008|         ],
  1009|     )
  1010| 
  1011|     assert load_file_join_hashes(
  1012|         segments_root, registry, "generic", "line_patterns", "all"
  1013|     ) == {
  1014|         "file_a": {"provided_a"},
  1015|         "file_b": {"provided_b"},
  1016|     }
  1017| 
  1018| 
  1019| def test_build_governance_state_rows_include_inherited_unused_and_local_active(tmp_path):
  1020|     from compare_cross_segment import build_governance_state_outputs  # noqa: E402
  1021| 
  1022|     domain = "line_patterns"
  1023|     segments_root = tmp_path / "segments"
  1024|     _write_segment(
  1025|         segments_root,
  1026|         "ref",
  1027|         domain,
  1028|         [("r1", "provided_used", "Provided Used"), ("r2", "provided_passive", "Provided Passive")],
  1029|         [
  1030|             {"export_run_id": "ref_file", "pattern_id": "r1"},
  1031|             {"export_run_id": "ref_file", "pattern_id": "r2"},
  1032|         ],
  1033|         [{"export_run_id": "ref_file", "pattern_id": "r1"}],
  1034|         ["r1", "r2"],
  1035|     )
  1036|     _write_segment(
  1037|         segments_root,
  1038|         "tgt",
  1039|         domain,
  1040|         [
  1041|             ("t1", "provided_used", "Provided Used"),
  1042|             ("t2", "provided_passive", "Provided Passive"),
  1043|             ("t3", "local_active", "Local Active"),
  1044|         ],
  1045|         [
  1046|             {"export_run_id": "target_file", "pattern_id": "t1"},
  1047|             {"export_run_id": "target_file", "pattern_id": "t2"},
  1048|             {"export_run_id": "target_file", "pattern_id": "t3"},
  1049|         ],
  1050|         [
  1051|             {"export_run_id": "target_file", "pattern_id": "t1"},
  1052|             {"export_run_id": "target_file", "pattern_id": "t3"},
  1053|         ],
  1054|         ["t1", "t2", "t3"],
  1055|     )
  1056|     manifest = {
  1057|         "ref": {**_seg("Template"), "segment_label": "Template"},
  1058|         "tgt": {**_seg("Project"), "segment_label": "Project"},
  1059|     }
  1060|     registry = {
  1061|         "ref": {"output_folder": "ref", "run_type": "bundle"},
  1062|         "tgt": {"output_folder": "tgt", "run_type": "bundle"},
  1063|     }
  1064| 
  1065|     rows, summary = build_governance_state_outputs(
  1066|         POLICY,
  1067|         "cmp_test",
  1068|         "ref",
  1069|         "tgt",
  1070|         "template_to_project",
  1071|         domain,
  1072|         manifest,
  1073|         registry,
  1074|         segments_root,
  1075|         "2026-05-29T00:00:00Z",
  1076|     )
  1077| 
  1078|     states = {row["join_hash"]: row["state"] for row in rows}
  1079|     assert states == {
  1080|         "provided_used": "provided_and_used",
  1081|         "provided_passive": "provided_but_passive",
  1082|         "local_active": "local_active",
  1083|     }
  1084|     assert summary["provided_to_configured_containment"] == "1.000000"
  1085|     assert summary["provided_to_used_containment"] == "0.500000"
  1086|     assert summary["provided_passive_share"] == "0.500000"
  1087|     assert summary["local_active_share"] == "0.500000"
  1088| 
  1089| 
  1090| def test_pair_domain_work_items_use_pair_domain_union(tmp_path):
  1091|     segments_root = tmp_path / "segments"
  1092|     registry = {
  1093|         "a": {"output_folder": "a"},
  1094|         "b": {"output_folder": "b"},
  1095|         "c": {"output_folder": "c"},
  1096|     }
  1097|     for folder, domain in [("a", "domain_a"), ("b", "domain_b"), ("c", "domain_c")]:
  1098|         (segments_root / folder / "results" / "bundle_analysis" / "all" / domain).mkdir(
  1099|             parents=True
  1100|         )
  1101| 
  1102|     work_items, _domains_by_segment, active_domains = build_pair_domain_work_items(
  1103|         [("a", "b", "sibling_projects"), ("a", "c", "sibling_projects")],
  1104|         segments_root,
  1105|         registry,
  1106|     )
  1107| 
  1108|     assert work_items == [
  1109|         ("a", "b", "sibling_projects", "domain_a"),
  1110|         ("a", "b", "sibling_projects", "domain_b"),
  1111|         ("a", "c", "sibling_projects", "domain_a"),
  1112|         ("a", "c", "sibling_projects", "domain_c"),
  1113|     ]
  1114|     assert active_domains == ["domain_a", "domain_b", "domain_c"]
  1115| 
  1116| 
  1117| def test_output_row_sort_helpers_are_stable_by_content():
  1118|     summary_rows = [
  1119|         {"comparison_type": "z", "segment_id_a": "b", "segment_id_b": "a", "domain": "d2"},
  1120|         {"comparison_type": "a", "segment_id_a": "b", "segment_id_b": "a", "domain": "d1"},
  1121|     ]
  1122|     sort_summary_rows(summary_rows)
  1123|     assert [row["comparison_type"] for row in summary_rows] == ["a", "z"]
  1124| 
  1125|     pair_rows = [
  1126|         {
  1127|             "_comparison_type": "sibling_projects",
  1128|             "segment_id_a": "b",
  1129|             "segment_id_b": "c",
  1130|             "domain": "d",
  1131|             "export_run_id_a": "2",
  1132|             "export_run_id_b": "1",
  1133|         },
  1134|         {
  1135|             "_comparison_type": "sibling_projects",
  1136|             "segment_id_a": "a",
  1137|             "segment_id_b": "c",
  1138|             "domain": "d",
  1139|             "export_run_id_a": "1",
  1140|             "export_run_id_b": "1",
  1141|         },
  1142|     ]
  1143|     sort_pair_detail_rows(pair_rows)
  1144|     assert [row["segment_id_a"] for row in pair_rows] == ["a", "b"]
  1145| 
  1146| 
  1147| def test_non_project_target_blanks_used_summary_shares(tmp_path):
  1148|     from compare_cross_segment import build_governance_state_outputs  # noqa: E402
  1149| 
  1150|     domain = "line_patterns"
  1151|     segments_root = tmp_path / "segments"
  1152|     _write_segment(
  1153|         segments_root,
  1154|         "generic",
  1155|         domain,
  1156|         [("g1", "provided_a", "Provided A"), ("g2", "provided_b", "Provided B")],
  1157|         [
  1158|             {"export_run_id": "generic_file", "pattern_id": "g1"},
  1159|             {"export_run_id": "generic_file", "pattern_id": "g2"},
  1160|         ],
  1161|         [{"export_run_id": "generic_file", "pattern_id": "g1"}],
  1162|         ["g1", "g2"],
  1163|     )
  1164|     _write_segment(
  1165|         segments_root,
  1166|         "template",
  1167|         domain,
  1168|         [
  1169|             ("t1", "provided_a", "Provided A"),
  1170|             ("t2", "provided_b", "Provided B"),
  1171|             ("t3", "template_local", "Template Local"),
  1172|         ],
  1173|         [
  1174|             {"export_run_id": "template_file", "pattern_id": "t1"},
  1175|             {"export_run_id": "template_file", "pattern_id": "t2"},
  1176|             {"export_run_id": "template_file", "pattern_id": "t3"},
  1177|         ],
  1178|         [{"export_run_id": "template_file", "pattern_id": "t3"}],
  1179|         ["t1", "t2", "t3"],
  1180|     )
  1181|     manifest = {
  1182|         "generic": {**_seg("Generic"), "segment_label": "Generic"},
  1183|         "template": {**_seg("Template"), "segment_label": "Template"},
  1184|     }
  1185|     registry = {
  1186|         "generic": {"output_folder": "generic", "run_type": "bundle"},
  1187|         "template": {"output_folder": "template", "run_type": "bundle"},
  1188|     }
  1189| 
  1190|     rows, summary = build_governance_state_outputs(
  1191|         POLICY,
  1192|         "cmp_test",
  1193|         "generic",
  1194|         "template",
  1195|         "generic_to_template",
  1196|         domain,
  1197|         manifest,
  1198|         registry,
  1199|         segments_root,
  1200|         "2026-05-29T00:00:00Z",
  1201|     )
  1202| 
  1203|     states = {row["join_hash"]: row["state"] for row in rows}
  1204|     assert states == {
  1205|         "provided_a": "provided_configured",
  1206|         "provided_b": "provided_configured",
  1207|         "template_local": "local_configured",
  1208|     }
  1209|     assert summary["target_usage_interpretable"] == "false"
  1210|     assert summary["provided_to_configured_containment"] == "1.000000"
  1211|     assert summary["provided_to_used_containment"] == ""
  1212|     assert summary["provided_passive_share"] == ""
  1213|     assert summary["local_active_share"] == ""
  1214|     assert summary["provided_and_used_pct_of_reference_all"] == ""
  1215|     assert summary["provided_but_passive_pct_of_reference_all"] == ""
  1216|     assert summary["local_active_pct_of_target_used"] == ""
  1217| 
  1218| 
  1219| def test_main_emits_governance_states_when_pair_skipped_by_min_patterns(tmp_path, monkeypatch):
  1220|     import csv
  1221| 
  1222|     domain = "sparse_line_patterns"
  1223|     records_dir = tmp_path / "records"
  1224|     segments_root = tmp_path / "segments"
  1225|     out_dir = tmp_path / "out"
  1226|     records_dir.mkdir()
  1227| 
  1228|     _write_csv(
  1229|         records_dir / "segment_manifest.csv",
  1230|         [
  1231|             {
  1232|                 "segment_id": "generic_sparse",
  1233|                 "segment_label": "Generic",
  1234|                 "governance_role": "Generic",
  1235|                 "client_label": "Global",
  1236|                 "discipline_label": "Arch",
  1237|                 "unit_system": "imperial",
  1238|                 "run_type": "bundle",
  1239|                 "segment_level": "2",
  1240|                 "parent_segment_id": "imperial",
  1241|             },
  1242|             {
  1243|                 "segment_id": "project_sparse",
  1244|                 "segment_label": "Project",
  1245|                 "governance_role": "Project",
  1246|                 "client_label": "Acme",
  1247|                 "discipline_label": "Arch",
  1248|                 "unit_system": "imperial",
  1249|                 "run_type": "bundle",
  1250|                 "segment_level": "2",
  1251|                 "parent_segment_id": "imperial",
  1252|             },
  1253|         ],
  1254|     )
  1255|     _write_csv(
  1256|         records_dir / "run_registry.csv",
  1257|         [
  1258|             {"segment_id": "generic_sparse", "output_folder": "generic_sparse", "run_type": "bundle"},
  1259|             {"segment_id": "project_sparse", "output_folder": "project_sparse", "run_type": "bundle"},
  1260|         ],
  1261|     )
  1262|     _write_csv(records_dir / "file_metadata.csv", [{"export_run_id": "generic_file", "project_label": ""}])
  1263|     _write_segment(
  1264|         segments_root,
  1265|         "generic_sparse",
  1266|         domain,
  1267|         [("g1", "provided_missing_a", "Provided Missing A"), ("g2", "provided_missing_b", "Provided Missing B")],
  1268|         [
  1269|             {"export_run_id": "generic_file", "pattern_id": "g1"},
  1270|             {"export_run_id": "generic_file", "pattern_id": "g2"},
  1271|         ],
  1272|         [{"export_run_id": "generic_file", "pattern_id": "g1"}],
  1273|         ["g1", "g2"],
  1274|     )
  1275|     (segments_root / "project_sparse").mkdir(parents=True)
  1276| 
  1277|     monkeypatch.setattr(
  1278|         sys,
  1279|         "argv",
  1280|         [
  1281|             "compare_cross_segment.py",
  1282|             "--segments-root",
  1283|             str(segments_root),
  1284|             "--records-dir",
  1285|             str(records_dir),
  1286|             "--out-dir",
  1287|             str(out_dir),
  1288|             "--governance-chain",
  1289|             "--domain",
  1290|             domain,
  1291|             "--min-patterns",
  1292|             "3",
  1293|             "--workers",
  1294|             "1",
  1295|             "--no-delta",
  1296|         ],
  1297|     )
  1298| 
  1299|     assert compare_main() == 0
  1300| 
  1301|     summary_path = out_dir / "cross_segment_summary.csv"
  1302|     states_path = out_dir / "cross_segment_governance_states.csv"
  1303|     state_summary_path = out_dir / "cross_segment_governance_state_summary.csv"
  1304|     # project_sparse has zero readable files (not merely below min_patterns) --
  1305|     # this is now the explicit blocked case: a real, schema-complete summary
  1306|     # row is emitted with comparison_status="blocked" rather than the pair
  1307|     # being suppressed outright. Governance-state outputs are unaffected --
  1308|     # they run through a separate code path from cross_segment_summary.csv.
  1309|     assert summary_path.exists()
  1310|     with summary_path.open("r", encoding="utf-8", newline="") as f:
  1311|         summary_rows = list(csv.DictReader(f))
  1312|     assert len(summary_rows) == 1
  1313|     assert summary_rows[0]["comparison_status"] == "blocked"
  1314|     assert summary_rows[0]["all_pairwise_jaccard_mean"] == ""
  1315|     assert states_path.exists()
  1316|     assert state_summary_path.exists()
  1317| 
  1318|     with states_path.open("r", encoding="utf-8", newline="") as f:
  1319|         rows = list(csv.DictReader(f))
  1320|     assert {row["state"] for row in rows} == {"provided_but_missing"}
  1321|     assert {row["join_hash"] for row in rows} == {"provided_missing_a", "provided_missing_b"}
  1322| 
  1323|     with state_summary_path.open("r", encoding="utf-8", newline="") as f:
  1324|         summary_rows = list(csv.DictReader(f))
  1325|     assert summary_rows[0]["provided_but_missing_count"] == "2"
  1326|     assert summary_rows[0]["provided_missing_share"] == "1.000000"
  1327| 
  1328| 
  1329| def test_main_skips_delta_generation_for_blocked_reference(tmp_path, monkeypatch):
  1330|     import csv
  1331| 
  1332|     domain = "delta_blocked_domain"
  1333|     records_dir = tmp_path / "records"
  1334|     segments_root = tmp_path / "segments"
  1335|     out_dir = tmp_path / "out"
  1336|     records_dir.mkdir()
  1337| 
  1338|     _write_csv(
  1339|         records_dir / "segment_manifest.csv",
  1340|         [
  1341|             {
  1342|                 "segment_id": "template_ref",
  1343|                 "segment_label": "Template",
  1344|                 "governance_role": "Template",
  1345|                 "client_label": "Acme",
  1346|                 "discipline_label": "",
  1347|                 "unit_system": "imperial",
  1348|                 "run_type": "bundle",
  1349|                 "segment_level": "2",
  1350|                 "parent_segment_id": "imperial",
  1351|             },
  1352|             {
  1353|                 "segment_id": "project_tgt",
  1354|                 "segment_label": "Project",
  1355|                 "governance_role": "Project",
  1356|                 "client_label": "Acme",
  1357|                 "discipline_label": "",
  1358|                 "unit_system": "imperial",
  1359|                 "run_type": "bundle",
  1360|                 "segment_level": "2",
  1361|                 "parent_segment_id": "imperial",
  1362|             },
  1363|         ],
  1364|     )
  1365|     _write_csv(
  1366|         records_dir / "run_registry.csv",
  1367|         [
  1368|             {"segment_id": "template_ref", "output_folder": "template_ref", "run_type": "bundle"},
  1369|             {"segment_id": "project_tgt", "output_folder": "project_tgt", "run_type": "bundle"},
  1370|         ],
  1371|     )
  1372|     _write_csv(records_dir / "file_metadata.csv", [{"export_run_id": "tgt_file", "project_label": ""}])
  1373|     # template_ref: zero readable files -- the reference side is blocked.
  1374|     (segments_root / "template_ref").mkdir(parents=True)
  1375|     # project_tgt: real patterns the (blocked) reference has no knowledge of.
  1376|     _write_segment(
  1377|         segments_root,
  1378|         "project_tgt",
  1379|         domain,
  1380|         [("p1", "tgt_a", "Target A"), ("p2", "tgt_b", "Target B")],
  1381|         [
  1382|             {"export_run_id": "tgt_file", "pattern_id": "p1"},
  1383|             {"export_run_id": "tgt_file", "pattern_id": "p2"},
  1384|         ],
  1385|         [
  1386|             {"export_run_id": "tgt_file", "pattern_id": "p1"},
  1387|             {"export_run_id": "tgt_file", "pattern_id": "p2"},
  1388|         ],
  1389|         ["p1"],
  1390|     )
  1391| 
  1392|     monkeypatch.setattr(
  1393|         sys,
  1394|         "argv",
  1395|         [
  1396|             "compare_cross_segment.py",
  1397|             "--segments-root", str(segments_root),
  1398|             "--records-dir", str(records_dir),
  1399|             "--out-dir", str(out_dir),
  1400|             "--governance-chain",
  1401|             "--domain", domain,
  1402|             "--min-patterns", "1",
  1403|             "--workers", "1",
  1404|             # deliberately no --no-delta: delta generation must be active
  1405|             # for this comparison_type so the fix is actually exercised.
  1406|         ],
  1407|     )
  1408| 
  1409|     assert compare_main() == 0
  1410| 
  1411|     summary_path = out_dir / "cross_segment_summary.csv"
  1412|     delta_path = out_dir / "cross_segment_delta.csv"
  1413| 
  1414|     with summary_path.open("r", encoding="utf-8", newline="") as f:
  1415|         summary_rows = [r for r in csv.DictReader(f) if r["comparison_type"] == "template_to_project"]
  1416|     assert len(summary_rows) == 1
  1417|     assert summary_rows[0]["comparison_status"] == "blocked"
  1418|     assert summary_rows[0]["n_files_a"] == "0"
  1419|     assert summary_rows[0]["n_files_b"] == "1"
  1420| 
  1421|     # The blocked reference must not produce delta rows -- with an empty
  1422|     # ref_union, tgt_a/tgt_b would otherwise both be misreported as locally
  1423|     # drifted patterns instead of "reference unknown."
  1424|     if delta_path.exists():
  1425|         with delta_path.open("r", encoding="utf-8", newline="") as f:
  1426|             delta_rows = [
  1427|                 r for r in csv.DictReader(f)
  1428|                 if r["segment_id_reference"] == "template_ref" and r["segment_id_target"] == "project_tgt"
  1429|             ]
  1430|         assert delta_rows == []
  1431| 
  1432| 
  1433| 
  1434| def _union_rows_for(tmp_path, manifest, registry, domain="line_patterns"):
  1435|     import compare_cross_segment as ccs
  1436| 
  1437|     ccs._jh_cache.clear()
  1438|     ccs._pattern_label_cache.clear()
  1439|     return build_union_inventory_rows(
  1440|         manifest,
  1441|         registry,
  1442|         {},
  1443|         tmp_path / "segments",
  1444|         "2026-06-22T00:00:00Z",
  1445|         domain_filter=domain,
  1446|     )
  1447| 
  1448| 
  1449| def test_union_inventory_project_all_view_normalized_union(tmp_path):
  1450|     domain = "line_patterns"
  1451|     segments_root = tmp_path / "segments"
  1452|     _write_segment(
  1453|         segments_root,
  1454|         "project",
  1455|         domain,
  1456|         [("p1", "join_a", "Join A"), ("p2", "join_b", "Join B")],
  1457|         [
  1458|             {"export_run_id": "file_1", "pattern_id": "p1"},
  1459|             {"export_run_id": "file_2", "pattern_id": "p2"},
  1460|         ],
  1461|         [{"export_run_id": "file_1", "pattern_id": "p1"}],
  1462|         ["p1", "p2"],
  1463|     )
  1464|     manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
  1465|     registry = {"project": {"output_folder": "project", "run_type": "bundle"}}
  1466| 
  1467|     rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "all"]
  1468| 
  1469|     assert [r["join_hash"] for r in rows] == ["join_a", "join_b"]
  1470|     assert {r["inventory_status"] for r in rows} == {"ok"}
  1471|     assert rows[0]["usage_interpretable"] == "true"
  1472| 
  1473| 
  1474| def test_union_inventory_project_used_view_normalized_union(tmp_path):
  1475|     domain = "line_patterns"
  1476|     segments_root = tmp_path / "segments"
  1477|     _write_segment(
  1478|         segments_root,
  1479|         "project",
  1480|         domain,
  1481|         [("p1", "join_a", "Join A"), ("p2", "join_b", "Join B")],
  1482|         [
  1483|             {"export_run_id": "file_1", "pattern_id": "p1"},
  1484|             {"export_run_id": "file_2", "pattern_id": "p2"},
  1485|         ],
  1486|         [{"export_run_id": "file_1", "pattern_id": "p1"}],
  1487|         ["p1", "p2"],
  1488|     )
  1489|     manifest = {"project": {**_seg("Project"), "segment_label": "Project"}}
  1490|     registry = {"project": {"output_folder": "project", "run_type": "bundle"}}
  1491| 
  1492|     rows = [r for r in _union_rows_for(tmp_path, manifest, registry, domain) if r["view_scope"] == "used"]
  1493| 
  1494|     assert len(rows) == 1
  1495|     assert rows[0]["join_hash"] == "join_a"
  1496|     assert rows[0]["n_files_present"] == "1"
  1497|     assert rows[0]["pct_files_present"] == "1.000000"
  1498| 
  1499| 
```
