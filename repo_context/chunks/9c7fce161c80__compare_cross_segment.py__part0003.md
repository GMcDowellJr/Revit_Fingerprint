# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 3 of 13
- Original line range: 983-1484
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: resolve_join_hashes, load_pattern_labels, get_role_jh_set, load_file_join_hashes, _segment_domain_source_status, _load_segment_file_join_hashes_with_status, _project_label_for_file, build_union_inventory_rows, _safe_pct, _reuse_bucket_for
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
   983| def resolve_join_hashes(
   984|     segments_root: Path,
   985|     registry: Dict[str, Dict[str, str]],
   986|     segment_id: str,
   987|     domain: str,
   988| ) -> Dict[str, str]:
   989|     key = (segment_id, domain)
   990|     if key in _jh_cache:
   991|         return _jh_cache[key]
   992| 
   993|     seg_out = segment_output_dir(segments_root, registry, segment_id)
   994|     if seg_out is None:
   995|         _jh_cache[key] = {}
   996|         return {}
   997| 
   998|     dp_path = domain_patterns_path(seg_out)
   999|     if not dp_path.exists():
  1000|         _jh_cache[key] = {}
  1001|         return {}
  1002| 
  1003|     result: Dict[str, str] = {}
  1004|     for row in read_csv_rows(dp_path):
  1005|         if row.get("domain", "") != domain:
  1006|             continue
  1007|         pid = row.get("pattern_id", "").strip()
  1008|         scid = row.get("source_cluster_id", "").strip()
  1009|         if not pid:
  1010|             continue
  1011|         if not scid:
  1012|             print(
  1013|                 f"[warn] segment={segment_id} domain={domain} pattern_id={pid} "
  1014|                 "has blank source_cluster_id — skipped",
  1015|                 file=sys.stderr,
  1016|             )
  1017|             continue
  1018|         result[pid] = scid.split("|")[-1]
  1019| 
  1020|     _jh_cache[key] = result
  1021|     return result
  1022| 
  1023| 
  1024| def load_pattern_labels(
  1025|     segments_root: Path,
  1026|     registry: Dict[str, Dict[str, str]],
  1027|     segment_id: str,
  1028|     domain: str,
  1029| ) -> Dict[str, str]:
  1030|     """Return {join_hash: label} from the segment's domain_patterns.csv.
  1031| 
  1032|     Prefers pattern_label_human; falls back to pattern_label; else empty string.
  1033|     """
  1034|     key = (segment_id, domain)
  1035|     if key in _pattern_label_cache:
  1036|         return _pattern_label_cache[key]
  1037| 
  1038|     seg_out = segment_output_dir(segments_root, registry, segment_id)
  1039|     if seg_out is None:
  1040|         _pattern_label_cache[key] = {}
  1041|         return {}
  1042| 
  1043|     dp_path = domain_patterns_path(seg_out)
  1044|     if not dp_path.exists():
  1045|         _pattern_label_cache[key] = {}
  1046|         return {}
  1047| 
  1048|     result: Dict[str, str] = {}
  1049|     for row in read_csv_rows(dp_path):
  1050|         if row.get("domain", "") != domain:
  1051|             continue
  1052|         scid = row.get("source_cluster_id", "").strip()
  1053|         if not scid:
  1054|             continue
  1055|         jh = scid.split("|")[-1]
  1056|         label = (
  1057|             row.get("pattern_label_human", "").strip()
  1058|             or row.get("pattern_label", "").strip()
  1059|         )
  1060|         result[jh] = label
  1061| 
  1062|     _pattern_label_cache[key] = result
  1063|     return result
  1064| 
  1065| 
  1066| def get_role_jh_set(
  1067|     role: str,
  1068|     domain: str,
  1069|     unit_system: str,
  1070|     manifest: Dict[str, Dict[str, str]],
  1071|     registry: Dict[str, Dict[str, str]],
  1072|     segments_root: Path,
  1073|     exclude_segment_id: str = "",
  1074| ) -> Set[str]:
  1075|     """Return the union of all join_hashes present in segments with the given role.
  1076| 
  1077|     Built once per (role, domain, unit_system, exclude_segment_id) and cached
  1078|     for the run lifetime. Segments with run_type skip/registration are silently
  1079|     excluded. Pass exclude_segment_id to omit a specific segment from the union
  1080|     (used when the target segment is itself the role being looked up).
  1081|     """
  1082|     cache_key = (role, domain, unit_system, exclude_segment_id)
  1083|     if cache_key in _role_jh_cache:
  1084|         return _role_jh_cache[cache_key]
  1085| 
  1086|     result: Set[str] = set()
  1087|     for sid, mrow in manifest.items():
  1088|         if sid == exclude_segment_id:
  1089|             continue
  1090|         if not _role_matches(mrow.get("governance_role", ""), role):
  1091|             continue
  1092|         if mrow.get("unit_system", "").strip() != unit_system:
  1093|             continue
  1094|         rt = registry.get(sid, {}).get("run_type", "").strip().lower()
  1095|         if rt in ("skip", "registration"):
  1096|             continue
  1097|         # Use all view — scores are view-invariant. load_segment_join_hash_union
  1098|         # preserves membership_matrix behavior for bundle segments and also allows
  1099|         # Generic/reference provided-vocabulary segments to contribute their
  1100|         # domain_patterns.csv fallback inventory when bundle outputs are absent.
  1101|         result |= load_segment_join_hash_union(
  1102|             segments_root, registry, sid, domain, "all"
  1103|         )
  1104| 
  1105|     _role_jh_cache[cache_key] = result
  1106|     return result
  1107| 
  1108| 
  1109| # ---------------------------------------------------------------------------
  1110| # Membership loading
  1111| # ---------------------------------------------------------------------------
  1112| 
  1113| def load_file_join_hashes(
  1114|     segments_root: Path,
  1115|     registry: Dict[str, Dict[str, str]],
  1116|     segment_id: str,
  1117|     domain: str,
  1118|     purge_view: str = "all",
  1119| ) -> Dict[str, Set[str]]:
  1120|     """Return {export_run_id: set_of_join_hashes} for a segment/domain/view."""
  1121|     seg_out = segment_output_dir(segments_root, registry, segment_id)
  1122|     if seg_out is None:
  1123|         return {}
  1124| 
  1125|     mm_path = bundle_analysis_dir(seg_out, domain, purge_view) / "membership_matrix.csv"
  1126|     if mm_path.exists():
  1127|         jh_map = resolve_join_hashes(segments_root, registry, segment_id, domain)
  1128|         result: Dict[str, Set[str]] = defaultdict(set)
  1129|         for row in read_csv_rows(mm_path):
  1130|             eid = row.get("export_run_id", "").strip()
  1131|             pid = row.get("pattern_id", "").strip()
  1132|             if not eid or not pid:
  1133|                 continue
  1134|             jh = jh_map.get(pid)
  1135|             if jh:
  1136|                 result[eid].add(jh)
  1137|         return dict(result)
  1138| 
  1139|     # Generic/reference segments are provided-vocabulary sources. They may not
  1140|     # produce bundle_analysis or membership matrices, but their analysis
  1141|     # inventory is valid for all-view containment/provision comparisons. File
  1142|     # membership comes from pattern_presence_file.csv when available. Used-view
  1143|     # is intentionally not inferred because analysis rows do not distinguish
  1144|     # active project use from configured/provided vocabulary.
  1145|     if purge_view != "all":
  1146|         return {}
  1147| 
  1148|     dp_path = domain_patterns_path(seg_out)
  1149|     if not dp_path.exists():
  1150|         return {}
  1151| 
  1152|     pattern_join_hashes: Dict[str, str] = {}
  1153|     pattern_export_run_ids: Dict[str, str] = {}
  1154|     for row in read_csv_rows(dp_path):
  1155|         if row.get("domain", "").strip() != domain:
  1156|             continue
  1157|         pid = row.get("pattern_id", "").strip()
  1158|         scid = row.get("source_cluster_id", "").strip()
  1159|         if not pid or not scid:
  1160|             continue
  1161|         join_hash = scid.split("|")[-1]
  1162|         if join_hash:
  1163|             pattern_join_hashes[pid] = join_hash
  1164|             eid = row.get("export_run_id", "").strip()
  1165|             if eid:
  1166|                 pattern_export_run_ids[pid] = eid
  1167| 
  1168|     if not pattern_join_hashes:
  1169|         return {}
  1170| 
  1171|     result: Dict[str, Set[str]] = defaultdict(set)
  1172| 
  1173|     # Standard v2.1 analysis writes file membership to pattern_presence_file.csv,
  1174|     # not domain_patterns.csv. Use it when present so multi-file Generic/reference
  1175|     # inventories preserve per-export containment inputs instead of collapsing or
  1176|     # dropping rows that have no export_run_id in domain_patterns.csv.
  1177|     presence_path = pattern_presence_file_path(seg_out)
  1178|     if presence_path.exists():
  1179|         for row in read_csv_rows(presence_path):
  1180|             if row.get("domain", "").strip() != domain:
  1181|                 continue
  1182|             eid = row.get("export_run_id", "").strip()
  1183|             pid = row.get("pattern_id", "").strip()
  1184|             if not eid or not pid:
  1185|                 continue
  1186|             join_hash = pattern_join_hashes.get(pid)
  1187|             if join_hash:
  1188|                 result[eid].add(join_hash)
  1189|         if result:
  1190|             return dict(result)
  1191| 
  1192|     for pid, join_hash in pattern_join_hashes.items():
  1193|         eid = pattern_export_run_ids.get(pid, "")
  1194|         if eid:
  1195|             result[eid].add(join_hash)
  1196|     if result:
  1197|         return dict(result)
  1198| 
  1199|     export_run_ids = _load_export_run_ids_for_segment(seg_out)
  1200|     single_export_run_id = export_run_ids[0] if len(export_run_ids) == 1 else ""
  1201|     if single_export_run_id:
  1202|         result[single_export_run_id] = set(pattern_join_hashes.values())
  1203|     return dict(result)
  1204| 
  1205| 
  1206| 
  1207| def _segment_domain_source_status(
  1208|     segments_root: Path,
  1209|     registry: Dict[str, Dict[str, str]],
  1210|     segment_id: str,
  1211|     domain: str,
  1212| ) -> Tuple[str, int]:
  1213|     """Return (source_status, missing_source_cluster_count) for a segment/domain."""
  1214|     seg_out = segment_output_dir(segments_root, registry, segment_id)
  1215|     if seg_out is None:
  1216|         return "missing_domain_patterns", 0
  1217|     dp_path = domain_patterns_path(seg_out)
  1218|     if not dp_path.exists():
  1219|         return "missing_domain_patterns", 0
  1220|     domain_rows = [
  1221|         row for row in read_csv_rows(dp_path)
  1222|         if row.get("domain", "").strip() == domain
  1223|     ]
  1224|     if not domain_rows:
  1225|         return "no_patterns", 0
  1226|     missing = sum(
  1227|         1 for row in domain_rows
  1228|         if row.get("pattern_id", "").strip()
  1229|         and not row.get("source_cluster_id", "").strip()
  1230|     )
  1231|     valid = any(
  1232|         row.get("pattern_id", "").strip()
  1233|         and row.get("source_cluster_id", "").strip()
  1234|         for row in domain_rows
  1235|     )
  1236|     if not valid:
  1237|         return "no_patterns", missing
  1238|     return "ok", missing
  1239| 
  1240| 
  1241| def _load_segment_file_join_hashes_with_status(
  1242|     segments_root: Path,
  1243|     registry: Dict[str, Dict[str, str]],
  1244|     segment_id: str,
  1245|     domain: str,
  1246|     view_scope: str,
  1247| ) -> Tuple[Dict[str, Set[str]], str, int]:
  1248|     """Load file join_hashes plus explicit status for union inventory output."""
  1249|     source_status, missing_scid = _segment_domain_source_status(
  1250|         segments_root, registry, segment_id, domain
  1251|     )
  1252|     if source_status == "missing_domain_patterns":
  1253|         return {}, "missing_domain_patterns", missing_scid
  1254|     if source_status == "no_patterns":
  1255|         return {}, "no_patterns", missing_scid
  1256| 
  1257|     seg_out = segment_output_dir(segments_root, registry, segment_id)
  1258|     if seg_out is None:
  1259|         return {}, "missing_domain_patterns", missing_scid
  1260|     mm_path = bundle_analysis_dir(seg_out, domain, view_scope) / "membership_matrix.csv"
  1261|     if view_scope == "used" and not mm_path.exists():
  1262|         return {}, "used_view_unavailable", missing_scid
  1263| 
  1264|     files = load_file_join_hashes(segments_root, registry, segment_id, domain, view_scope)
  1265|     if files:
  1266|         return files, "ok", missing_scid
  1267|     if view_scope == "all":
  1268|         return {}, "no_patterns", missing_scid
  1269|     return {}, "used_view_unavailable", missing_scid
  1270| 
  1271| 
  1272| def _project_label_for_file(file_metadata: Dict[str, Dict[str, str]], export_run_id: str) -> str:
  1273|     label = file_metadata.get(export_run_id, {}).get("project_label", "").strip()
  1274|     return export_run_id if is_blank_or_na(label) else label
  1275| 
  1276| 
  1277| def build_union_inventory_rows(
  1278|     manifest: Dict[str, Dict[str, str]],
  1279|     registry: Dict[str, Dict[str, str]],
  1280|     file_metadata: Dict[str, Dict[str, str]],
  1281|     segments_root: Path,
  1282|     executed_utc: str,
  1283|     domain_filter: Optional[str] = None,
  1284| ) -> List[Dict[str, str]]:
  1285|     """Build normalized union inventory rows at governance/client/discipline/unit/domain/view/join_hash grain."""
  1286|     groups: Dict[Tuple[str, str, str, str, str], List[str]] = defaultdict(list)
  1287|     for segment_id, mrow in manifest.items():
  1288|         if not segment_is_runnable(registry, segment_id):
  1289|             continue
  1290|         domains = {domain_filter} if domain_filter else discover_domains_for_segment(
  1291|             segments_root, registry, segment_id
  1292|         )
  1293|         for domain in sorted(d for d in domains if d):
  1294|             groups[(
  1295|                 mrow.get("governance_role", "").strip(),
  1296|                 mrow.get("client_label", "").strip(),
  1297|                 mrow.get("discipline_label", "").strip(),
  1298|                 mrow.get("unit_system", "").strip(),
  1299|                 domain,
  1300|             )].append(segment_id)
  1301| 
  1302|     rows: List[Dict[str, str]] = []
  1303|     for (role, client, discipline, unit_system, domain), segment_ids in sorted(groups.items()):
  1304|         for view_scope in ("all", "used"):
  1305|             usage_ok = _usage_interpretable_for_role(role)
  1306|             by_jh: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: {
  1307|                 "segments": set(), "files": set(), "projects": set()
  1308|             })
  1309|             labels: Dict[str, str] = {}
  1310|             statuses: Set[str] = set()
  1311|             missing_scid_total = 0
  1312|             denominator_files: Set[str] = set()
  1313|             denominator_projects: Set[str] = set()
  1314|             client_has_inventory = False
  1315| 
  1316|             for segment_id in sorted(segment_ids):
  1317|                 files, status, missing_scid = _load_segment_file_join_hashes_with_status(
  1318|                     segments_root, registry, segment_id, domain, view_scope
  1319|                 )
  1320|                 statuses.add(status)
  1321|                 missing_scid_total += missing_scid
  1322|                 for jh, label in load_pattern_labels(segments_root, registry, segment_id, domain).items():
  1323|                     labels.setdefault(jh, label)
  1324|                 for export_run_id, join_hashes in files.items():
  1325|                     if join_hashes:
  1326|                         client_has_inventory = True
  1327|                         denominator_files.add(export_run_id)
  1328|                         denominator_projects.add(_project_label_for_file(file_metadata, export_run_id))
  1329|                     for join_hash in join_hashes:
  1330|                         entry = by_jh[join_hash]
  1331|                         entry["segments"].add(segment_id)
  1332|                         entry["files"].add(export_run_id)
  1333|                         entry["projects"].add(_project_label_for_file(file_metadata, export_run_id))
  1334| 
  1335|             if view_scope == "used" and not usage_ok and by_jh:
  1336|                 inventory_status = "not_interpretable"
  1337|             elif by_jh:
  1338|                 inventory_status = "ok"
  1339|             elif "missing_domain_patterns" in statuses:
  1340|                 inventory_status = "missing_domain_patterns"
  1341|             elif view_scope == "used" and "used_view_unavailable" in statuses:
  1342|                 inventory_status = "used_view_unavailable"
  1343|             elif statuses == {"no_patterns"} or "no_patterns" in statuses:
  1344|                 inventory_status = "no_patterns"
  1345|             else:
  1346|                 inventory_status = "ok"
  1347| 
  1348|             source_status = "ok" if missing_scid_total == 0 else "missing_source_cluster_id"
  1349|             if not by_jh:
  1350|                 if inventory_status in {"ok", "not_interpretable"}:
  1351|                     continue
  1352|                 rows.append({
  1353|                     "governance_role": role,
  1354|                     "client_label": client,
  1355|                     "discipline_label": discipline,
  1356|                     "unit_system": unit_system,
  1357|                     "domain": domain,
  1358|                     "view_scope": view_scope,
  1359|                     "join_hash": "",
  1360|                     "pattern_label": "",
  1361|                     "n_segments_present": "0",
  1362|                     "n_files_present": "0",
  1363|                     "n_files_denominator": "0",
  1364|                     "pct_files_present": "0.000000",
  1365|                     "n_projects_present": "0",
  1366|                     "n_projects_denominator": "0",
  1367|                     "n_clients_present": "0",
  1368|                     "n_clients_denominator": "1" if client_has_inventory else "0",
  1369|                     "pct_clients_present": "0.000000",
  1370|                     "pct_projects_present": "0.000000",
  1371|                     "usage_interpretable": _bool_str(usage_ok),
  1372|                     "inventory_status": inventory_status,
  1373|                     "source_status": source_status,
  1374|                     "executed_utc": executed_utc,
  1375|                 })
  1376|                 continue
  1377| 
  1378|             file_den = len(denominator_files)
  1379|             project_den = len(denominator_projects)
  1380|             for join_hash in sorted(by_jh):
  1381|                 entry = by_jh[join_hash]
  1382|                 n_files = len(entry["files"])
  1383|                 n_projects = len(entry["projects"])
  1384|                 rows.append({
  1385|                     "governance_role": role,
  1386|                     "client_label": client,
  1387|                     "discipline_label": discipline,
  1388|                     "unit_system": unit_system,
  1389|                     "domain": domain,
  1390|                     "view_scope": view_scope,
  1391|                     "join_hash": join_hash,
  1392|                     "pattern_label": labels.get(join_hash, ""),
  1393|                     "n_segments_present": str(len(entry["segments"])),
  1394|                     "n_files_present": str(n_files),
  1395|                     "n_files_denominator": str(file_den),
  1396|                     "pct_files_present": _safe_pct(n_files, file_den) or "0.000000",
  1397|                     "n_projects_present": str(n_projects),
  1398|                     "n_projects_denominator": str(project_den),
  1399|                     "n_clients_present": "1",
  1400|                     "n_clients_denominator": "1",
  1401|                     "pct_clients_present": "1.000000",
  1402|                     "pct_projects_present": _safe_pct(n_projects, project_den) or "0.000000",
  1403|                     "usage_interpretable": _bool_str(usage_ok),
  1404|                     "inventory_status": inventory_status,
  1405|                     "source_status": source_status,
  1406|                     "executed_utc": executed_utc,
  1407|                 })
  1408| 
  1409|     clients_by_group: Dict[Tuple[str, str, str, str, str], Set[str]] = defaultdict(set)
  1410|     clients_by_pattern: Dict[Tuple[str, str, str, str, str, str], Set[str]] = defaultdict(set)
  1411|     for row in rows:
  1412|         if not row.get("join_hash", "").strip() and row.get("inventory_status", "") == "ok":
  1413|             continue
  1414|         group_key = (
  1415|             row.get("view_scope", ""),
  1416|             row.get("governance_role", ""),
  1417|             row.get("discipline_label", ""),
  1418|             row.get("unit_system", ""),
  1419|             row.get("domain", ""),
  1420|         )
  1421|         clients_by_group[group_key].add(row.get("client_label", ""))
  1422|         clients_by_pattern[(*group_key, row.get("join_hash", ""))].add(row.get("client_label", ""))
  1423| 
  1424|     for row in rows:
  1425|         if not row.get("join_hash", "").strip() and row.get("inventory_status", "") == "ok":
  1426|             continue
  1427|         group_key = (
  1428|             row.get("view_scope", ""),
  1429|             row.get("governance_role", ""),
  1430|             row.get("discipline_label", ""),
  1431|             row.get("unit_system", ""),
  1432|             row.get("domain", ""),
  1433|         )
  1434|         n_clients_present = len(clients_by_pattern.get((*group_key, row.get("join_hash", "")), set()))
  1435|         n_clients_denominator = len(clients_by_group.get(group_key, set()))
  1436|         row["n_clients_present"] = str(n_clients_present)
  1437|         row["n_clients_denominator"] = str(n_clients_denominator)
  1438|         row["pct_clients_present"] = _safe_pct(n_clients_present, n_clients_denominator) or "0.000000"
  1439| 
  1440|     rows.sort(key=lambda r: (
  1441|         r["governance_role"], r["client_label"], r["discipline_label"],
  1442|         r["unit_system"], r["domain"], r["view_scope"], r["join_hash"],
  1443|     ))
  1444|     return rows
  1445| 
  1446| 
  1447| def _safe_pct(numerator: int, denominator: int) -> str:
  1448|     return _fmt(numerator / denominator) if denominator else ""
  1449| 
  1450| 
  1451| def _reuse_bucket_for(
  1452|     *,
  1453|     n_files: int,
  1454|     n_files_den: int,
  1455|     n_projects: int,
  1456|     n_projects_den: int,
  1457|     n_clients: int,
  1458|     n_clients_den: int,
  1459| ) -> Tuple[str, str, str]:
  1460|     """Classify reuse breadth with explicit denominator basis.
  1461| 
  1462|     Buckets are neutral reporting classes, not approval or correctness claims.
  1463|     Returns (reuse_bucket, bucket_basis, classification_status).
  1464|     """
  1465|     if n_files_den <= 0 or n_projects_den <= 0 or n_clients_den <= 0:
  1466|         return "unclassified", "denominator_unavailable", "degraded_zero_denominator"
  1467| 
  1468|     pct_clients = n_clients / n_clients_den
  1469|     pct_files = n_files / n_files_den
  1470|     if pct_clients >= REUSE_BUCKET_THRESHOLDS["corpus_wide_min_pct_clients"] and n_clients_den > 1:
  1471|         return "corpus_wide", "clients_in_corpus_domain", "ok"
  1472|     if pct_files >= REUSE_BUCKET_THRESHOLDS["client_wide_min_pct_files"]:
  1473|         return "client_wide", "files_in_role_client_domain", "ok"
  1474|     if n_projects >= REUSE_BUCKET_THRESHOLDS["multi_project_min_projects"] and n_projects_den > 1:
  1475|         return "multi_project", "projects_in_client_domain", "ok"
  1476|     if n_files == 1:
  1477|         return "single_file", "files_in_role_client_domain", "ok"
  1478|     if n_projects == 1:
  1479|         return "single_project", "projects_in_client_domain", "ok"
  1480|     if n_files >= REUSE_BUCKET_THRESHOLDS["emerging_min_files"]:
  1481|         return "emerging", "files_in_role_client_domain", "ok"
  1482|     return "unclassified", "files_in_role_client_domain", "ok"
  1483| 
  1484| 
```
