# Chunk of contracts/domain_identity_keys_v2.json

- Source relative path: `contracts/domain_identity_keys_v2.json`
- Chunk: 4 of 4
- Original line range: 1171-1441
- Overlap lines with previous chunk: 10
- Symbols fully or partially present: none
- Source SHA-256: 97b5e6834d04d3f21f92697078fa5f994471002da74d6487c4bc46e4cab7ad10
- Starts inside symbol: no
- Ends inside symbol: no

```
  1171|       "domain_family": "view_filter_applications_view_templates",
  1172|       "display_label": "View Filter Applications",
  1173|       "allowed_keys": [
  1174|         "vfa.stack_def_hash"
  1175|       ],
  1176|       "required_keys": [
  1177|         "vfa.stack_def_hash"
  1178|       ],
  1179|       "minima": {
  1180|         "block_if_any_required_not_ok": true
  1181|       },
  1182|       "indexed_key_rules": {
  1183|         "vfa.stack[i].filter_sig_hash": true,
  1184|         "vfa.stack[i].visibility": true,
  1185|         "vfa.stack[i].overrides": true
  1186|       }
  1187|     },
  1188|     "view_filter_definitions": {
  1189|       "domain_family": "view_filter_definitions",
  1190|       "display_label": "View Filter Definitions",
  1191|       "allowed_keys": [
  1192|         "vf.categories",
  1193|         "vf.def_hash",
  1194|         "vf.logic_root",
  1195|         "vf.rule_count"
  1196|       ],
  1197|       "allowed_key_prefixes": [
  1198|         "vf.rule["
  1199|       ],
  1200|       "required_keys": [
  1201|         "vf.logic_root",
  1202|         "vf.rule_count"
  1203|       ],
  1204|       "sig_hash_schema": "view_filter_definitions.sig_hash.v2",
  1205|       "sig_hash_keys": [
  1206|         "vf.categories",
  1207|         "vf.logic_root",
  1208|         "vf.rule_count"
  1209|       ],
  1210|       "minima": {
  1211|         "block_if_any_required_not_ok": true
  1212|       },
  1213|       "indexed_key_rules": {
  1214|         "vf.rule[i].kind": true,
  1215|         "vf.rule[i].op": true,
  1216|         "vf.rule[i].param_ref.kind": true,
  1217|         "vf.rule[i].param_ref.id": true,
  1218|         "vf.rule[i].prefix": true,
  1219|         "vf.rule[i].sig": true,
  1220|         "vf.rule[i].value": true
  1221|       }
  1222|     },
  1223|     "view_templates_ceiling_plans": {
  1224|       "domain_family": "view_templates",
  1225|       "display_label": "View Templates — Ceiling Plans",
  1226|       "allowed_keys": [
  1227|         "view_template.def_hash"
  1228|       ],
  1229|       "allowed_key_prefixes": [
  1230|         "view_template.sig."
  1231|       ],
  1232|       "required_keys": [
  1233|         "view_template.def_hash"
  1234|       ],
  1235|       "minima": {
  1236|         "block_if_any_required_not_ok": true
  1237|       }
  1238|     },
  1239|     "view_templates_elevations_sections_detail": {
  1240|       "domain_family": "view_templates",
  1241|       "display_label": "View Templates — Elevations/Sections",
  1242|       "allowed_keys": [
  1243|         "view_template.def_hash"
  1244|       ],
  1245|       "allowed_key_prefixes": [
  1246|         "view_template.sig."
  1247|       ],
  1248|       "required_keys": [
  1249|         "view_template.def_hash"
  1250|       ],
  1251|       "minima": {
  1252|         "block_if_any_required_not_ok": true
  1253|       }
  1254|     },
  1255|     "view_templates_floor_structural_area_plans": {
  1256|       "domain_family": "view_templates",
  1257|       "display_label": "View Templates — Floor/Structural Plans",
  1258|       "allowed_keys": [
  1259|         "view_template.def_hash"
  1260|       ],
  1261|       "allowed_key_prefixes": [
  1262|         "view_template.sig."
  1263|       ],
  1264|       "required_keys": [
  1265|         "view_template.def_hash"
  1266|       ],
  1267|       "minima": {
  1268|         "block_if_any_required_not_ok": true
  1269|       }
  1270|     },
  1271|     "view_templates_renderings_drafting": {
  1272|       "domain_family": "view_templates",
  1273|       "display_label": "View Templates — Renderings/Drafting",
  1274|       "allowed_keys": [
  1275|         "view_template.def_hash"
  1276|       ],
  1277|       "allowed_key_prefixes": [
  1278|         "view_template.sig."
  1279|       ],
  1280|       "required_keys": [
  1281|         "view_template.def_hash"
  1282|       ],
  1283|       "minima": {
  1284|         "block_if_any_required_not_ok": true
  1285|       }
  1286|     },
  1287|     "view_templates_schedules": {
  1288|       "domain_family": "view_templates",
  1289|       "display_label": "View Templates — Schedules",
  1290|       "allowed_keys": [
  1291|         "view_template.def_hash"
  1292|       ],
  1293|       "allowed_key_prefixes": [
  1294|         "view_template.sig."
  1295|       ],
  1296|       "required_keys": [
  1297|         "view_template.def_hash"
  1298|       ],
  1299|       "minima": {
  1300|         "block_if_any_required_not_ok": true
  1301|       }
  1302|     },
  1303|     "wall_types": {
  1304|       "domain_family": "compound_types",
  1305|       "allowed_keys": [
  1306|         "wt.function",
  1307|         "wt.layer_count",
  1308|         "wt.total_thickness_in",
  1309|         "wt.stack_hash_loose",
  1310|         "wt.wraps_at_inserts",
  1311|         "wt.wraps_at_ends",
  1312|         "wt.kind",
  1313|         "wt.total_layer_rows",
  1314|         "wt.stack_hash_strict",
  1315|         "wt.stack_hash_function_only",
  1316|         "wt.coarse_fill_pattern_sig_hash",
  1317|         "wt.has_embedded_sweeps",
  1318|         "wt.type_name",
  1319|         "wt.coarse_fill_color_rgb"
  1320|       ],
  1321|       "required_keys": [
  1322|         "wt.layer_count",
  1323|         "wt.total_thickness_in",
  1324|         "wt.stack_hash_loose"
  1325|       ],
  1326|       "required": [
  1327|         "wt.layer_count",
  1328|         "wt.total_thickness_in",
  1329|         "wt.stack_hash_loose"
  1330|       ],
  1331|       "optional": [
  1332|         "wt.wraps_at_inserts",
  1333|         "wt.wraps_at_ends",
  1334|         "wt.kind",
  1335|         "wt.total_layer_rows",
  1336|         "wt.stack_hash_strict",
  1337|         "wt.stack_hash_function_only",
  1338|         "wt.coarse_fill_pattern_sig_hash",
  1339|         "wt.has_embedded_sweeps",
  1340|         "wt.type_name",
  1341|         "wt.coarse_fill_color_rgb"
  1342|       ],
  1343|       "minima": {
  1344|         "block_if_any_required_not_ok": true
  1345|       }
  1346|     },
  1347|     "worksets": {
  1348|       "domain_family": "worksets",
  1349|       "display_label": "Worksets",
  1350|       "allowed_keys": [
  1351|         "workset.name",
  1352|         "workset.kind",
  1353|         "workset.is_editable",
  1354|         "workset.is_default_workset",
  1355|         "workset.owner",
  1356|         "workset.is_active_workset",
  1357|         "workset.unique_id"
  1358|       ],
  1359|       "allowed_key_prefixes": [],
  1360|       "required_keys": [
  1361|         "workset.name",
  1362|         "workset.kind",
  1363|         "workset.is_default_workset"
  1364|       ],
  1365|       "sig_hash_schema": "worksets.sig_hash.v1",
  1366|       "sig_hash_keys": [
  1367|         "workset.is_default_workset",
  1368|         "workset.kind",
  1369|         "workset.name"
  1370|       ],
  1371|       "sig_hash_key_prefixes": [],
  1372|       "minima": {
  1373|         "block_if_any_required_not_ok": true
  1374|       },
  1375|       "indexed_key_rules": {}
  1376|     },
  1377|     "worksets_doc": {
  1378|       "domain_family": "worksets",
  1379|       "display_label": "Worksets (Document Summary)",
  1380|       "allowed_keys": [
  1381|         "worksets_doc.is_workshared",
  1382|         "worksets_doc.active_workset_name",
  1383|         "worksets_doc.count_user_workset",
  1384|         "worksets_doc.count_standard_workset",
  1385|         "worksets_doc.count_view_workset",
  1386|         "worksets_doc.count_family_workset",
  1387|         "worksets_doc.count_other_workset"
  1388|       ],
  1389|       "allowed_key_prefixes": [],
  1390|       "required_keys": [],
  1391|       "sig_hash_schema": "worksets_doc.sig_hash.v1",
  1392|       "sig_hash_keys": [
  1393|         "worksets_doc.count_family_workset",
  1394|         "worksets_doc.count_other_workset",
  1395|         "worksets_doc.count_standard_workset",
  1396|         "worksets_doc.count_user_workset",
  1397|         "worksets_doc.count_view_workset",
  1398|         "worksets_doc.is_workshared"
  1399|       ],
  1400|       "sig_hash_key_prefixes": [],
  1401|       "minima": {
  1402|         "block_if_any_required_not_ok": false
  1403|       },
  1404|       "indexed_key_rules": {}
  1405|     },
  1406|     "browser_organization": {
  1407|       "domain_family": "browser_organization",
  1408|       "display_label": "Browser Organization",
  1409|       "allowed_keys": [
  1410|         "bo.category",
  1411|         "bo.sorting_order",
  1412|         "bo.sorting_parameter_id",
  1413|         "bo.filter_has_value",
  1414|         "bo.family_name",
  1415|         "bo.org_id",
  1416|         "bo.unique_id",
  1417|         "bo.workset_id",
  1418|         "bo.workset_name",
  1419|         "bo.workset_unique_id"
  1420|       ],
  1421|       "allowed_key_prefixes": [],
  1422|       "required_keys": [
  1423|         "bo.category",
  1424|         "bo.sorting_order",
  1425|         "bo.sorting_parameter_id"
  1426|       ],
  1427|       "sig_hash_schema": "browser_organization.sig_hash.v1",
  1428|       "sig_hash_keys": [
  1429|         "bo.category",
  1430|         "bo.filter_has_value",
  1431|         "bo.sorting_order",
  1432|         "bo.sorting_parameter_id"
  1433|       ],
  1434|       "sig_hash_key_prefixes": [],
  1435|       "minima": {
  1436|         "block_if_any_required_not_ok": true
  1437|       },
  1438|       "indexed_key_rules": {}
  1439|     }
  1440|   }
  1441| }
```
