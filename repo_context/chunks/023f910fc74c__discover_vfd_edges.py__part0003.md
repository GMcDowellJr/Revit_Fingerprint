# Chunk of tools/archetype/discover_vfd_edges.py

- Source relative path: `tools/archetype/discover_vfd_edges.py`
- Chunk: 3 of 3
- Original line range: 948-1353
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_domain_gap_rows, build_edge_rows, verify_outputs, print_summary, build_unresolved_file_rows, print_unresolved_summary, parse_args, main
- Source SHA-256: 95fe05c8009121c853de6753dc3020bdb0607b4cb260aeb1ac07496433793634
- Starts inside symbol: no
- Ends inside symbol: no

```
   948| def build_domain_gap_rows(
   949|     inventory_rows: Sequence[Dict[str, Any]],
   950|     category_map: Dict[str, Any],
   951| ) -> List[Dict[str, Any]]:
   952|     groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
   953|     for row in inventory_rows:
   954|         candidate_domains = [
   955|             candidate_domain
   956|             for candidate_domain in str(row.get("candidate_domain") or "").split("|")
   957|             if candidate_domain
   958|         ]
   959|         blocked_reason = str(row.get("candidate_domain_blocked_reason") or "")
   960|         if not candidate_domains:
   961|             if blocked_reason != "category_map_conflict_all_blocked":
   962|                 continue
   963|             candidate_domains = ["unknown"]
   964|         for candidate_domain in candidate_domains:
   965|             key = (candidate_domain, blocked_reason)
   966|             group = groups.setdefault(
   967|                 key,
   968|                 {
   969|                     "candidate_domain": candidate_domain,
   970|                     "domain_extracted": _category_map_domain_extracted(candidate_domain, category_map),
   971|                     "identity_items_present": "false" if blocked_reason == "identity_items_missing" else "N/A",
   972|                     "blocked_reason": blocked_reason,
   973|                     "export_run_ids": set(),
   974|                     "file_count_fallback": 0,
   975|                     "param_ids": set(),
   976|                     "category_ids": set(),
   977|                     "category_names": set(),
   978|                 },
   979|             )
   980|             category_ids, category_names = _candidate_category_details(
   981|                 candidate_domain,
   982|                 str(row.get("category_set") or ""),
   983|                 category_map,
   984|             )
   985|             export_run_ids = row.get("_export_run_ids")
   986|             if export_run_ids is None:
   987|                 group["file_count_fallback"] += int(row.get("file_count") or 0)
   988|             else:
   989|                 group["export_run_ids"].update(str(export_run_id) for export_run_id in export_run_ids)
   990|             group["param_ids"].add(str(row.get("param_id") or ""))
   991|             group["category_ids"].update(category_ids)
   992|             group["category_names"].update(category_names)
   993| 
   994|     rows: List[Dict[str, Any]] = []
   995|     for group in groups.values():
   996|         rows.append({
   997|             "candidate_domain": group["candidate_domain"],
   998|             "domain_extracted": group["domain_extracted"],
   999|             "identity_items_present": group["identity_items_present"],
  1000|             "blocked_reason": group["blocked_reason"],
  1001|             "file_count_demand": len(group["export_run_ids"]) + int(group["file_count_fallback"]),
  1002|             "param_count": len(group["param_ids"]),
  1003|             "category_ids": "|".join(sorted(group["category_ids"], key=lambda value: int(value))),
  1004|             "category_names": "|".join(sorted(group["category_names"])),
  1005|         })
  1006|     rows.sort(key=lambda r: (-int(r["file_count_demand"]), str(r["candidate_domain"]), str(r["blocked_reason"])))
  1007|     return rows
  1008| 
  1009| 
  1010| def build_edge_rows(
  1011|     inventory_rows: Sequence[Dict[str, Any]],
  1012|     include_unresolved: bool,
  1013|     support_min_files: int,
  1014| ) -> List[Dict[str, Any]]:
  1015|     edge_groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
  1016|     name_domain_param_ids: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
  1017| 
  1018|     for row in inventory_rows:
  1019|         name_resolved = row["name_resolved"] == "true"
  1020|         target_domain = str(row["target_domain"])
  1021|         if not name_resolved:
  1022|             continue
  1023|         if not target_domain and not include_unresolved:
  1024|             continue
  1025| 
  1026|         normalized = normalize_param_name(str(row["param_name"]))
  1027|         if not normalized:
  1028|             continue
  1029| 
  1030|         param_id = str(row["param_id"])
  1031|         edge_domain_component = target_domain or "unresolved"
  1032|         key = (normalized, target_domain, param_id)
  1033|         name_domain_param_ids[(normalized, target_domain)].add(param_id)
  1034|         group = edge_groups.setdefault(
  1035|             key,
  1036|             {
  1037|                 "param_id": param_id,
  1038|                 "param_kind": row["param_kind"],
  1039|                 "param_name": row["param_name"],
  1040|                 "normalized": normalized,
  1041|                 "target_domain": target_domain,
  1042|                 "edge_domain_component": edge_domain_component,
  1043|                 "category_files": defaultdict(set),
  1044|                 "category_rule_counts": defaultdict(int),
  1045|                 "all_export_run_ids": set(),
  1046|                 "rule_count": 0,
  1047|                 "target_domain_sources": [],
  1048|                 "target_domain_verified": True,
  1049|                 "requires_human_review": False,
  1050|             },
  1051|         )
  1052| 
  1053|         export_run_ids = set(row.get("_export_run_ids", set()))
  1054|         group["all_export_run_ids"].update(export_run_ids)
  1055|         group["rule_count"] += int(row["rule_count"])
  1056| 
  1057|         category_ids = parse_category_set(str(row["category_set"]))
  1058|         for category_id in category_ids:
  1059|             group["category_files"][category_id].update(export_run_ids)
  1060|             group["category_rule_counts"][category_id] += int(row["rule_count"])
  1061| 
  1062|         source = str(row["target_domain_source"])
  1063|         if source not in group["target_domain_sources"]:
  1064|             group["target_domain_sources"].append(source)
  1065|         if row["target_domain_verified"] != "true":
  1066|             group["target_domain_verified"] = False
  1067|         if row["requires_human_review"] == "true":
  1068|             group["requires_human_review"] = True
  1069| 
  1070|     for (normalized, target_domain), param_ids in sorted(name_domain_param_ids.items()):
  1071|         guid_ids = [param_id for param_id in sorted(param_ids) if GUID_RE.match(param_id)]
  1072|         if len(guid_ids) > 1:
  1073|             warn(
  1074|                 f'Multiple param_ids resolve to same normalized name "{normalized}" for target_domain '
  1075|                 f'"{target_domain or "null"}": {sorted(param_ids)}. Emitting separate param/category scopes; '
  1076|                 "verify these are the same parameter before manually grouping them."
  1077|             )
  1078| 
  1079|     rows: List[Dict[str, Any]] = []
  1080|     for (normalized, target_domain, param_id), group in sorted(edge_groups.items()):
  1081|         supported_category_ids = sorted(
  1082|             (
  1083|                 category_id
  1084|                 for category_id, files in group["category_files"].items()
  1085|                 if category_id != "" and len(files) >= support_min_files
  1086|             ),
  1087|             key=lambda category_id: int(category_id),
  1088|         )
  1089|         if not supported_category_ids:
  1090|             continue
  1091| 
  1092|         edge_id = f"vfd.{normalized}__{group['edge_domain_component']}"
  1093|         category_file_counts = {
  1094|             str(category_id): len(group["category_files"][category_id])
  1095|             for category_id in supported_category_ids
  1096|         }
  1097|         supported_files: Set[str] = set()
  1098|         total_rule_count = 0
  1099|         for category_id in supported_category_ids:
  1100|             supported_files.update(group["category_files"][category_id])
  1101|             total_rule_count += int(group["category_rule_counts"][category_id])
  1102| 
  1103|         scope_conditions = json.dumps(
  1104|             {"param_ids": [param_id], "category_ids": supported_category_ids},
  1105|             separators=(",", ":"),
  1106|         )
  1107|         rows.append({
  1108|             "edge_id": edge_id,
  1109|             "param_id": param_id,
  1110|             "param_kind": group["param_kind"],
  1111|             "param_name": group["param_name"],
  1112|             "param_name_normalized": normalized,
  1113|             "target_domain": target_domain,
  1114|             "scope_conditions": scope_conditions,
  1115|             "category_file_counts": json.dumps(category_file_counts, separators=(",", ":")),
  1116|             "file_count": len(supported_files),
  1117|             "rule_count": total_rule_count,
  1118|             "name_resolved": "true",
  1119|             "target_domain_source": "|".join(group["target_domain_sources"]),
  1120|             "target_domain_verified": bool_s(bool(group["target_domain_verified"])),
  1121|             "requires_human_review": bool_s(bool(group["requires_human_review"])),
  1122|         })
  1123|     return rows
  1124| 
  1125| 
  1126| def verify_outputs(edge_rows: Sequence[Dict[str, Any]], inventory_rows: Sequence[Dict[str, Any]], total_files: int) -> None:
  1127|     for row in edge_rows:
  1128|         edge_id = str(row["edge_id"])
  1129|         if not EDGE_ID_RE.match(edge_id) or " " in edge_id or "null" in edge_id.lower():
  1130|             raise SystemExit(f"ERROR [{STAGE}] invalid edge_id generated: {edge_id}")
  1131|         try:
  1132|             scope = json.loads(str(row["scope_conditions"]))
  1133|         except json.JSONDecodeError as exc:
  1134|             raise SystemExit(f"ERROR [{STAGE}] invalid scope_conditions JSON for {edge_id}: {exc}") from exc
  1135|         if not isinstance(scope, dict) or not isinstance(scope.get("param_ids"), list) or not isinstance(scope.get("category_ids"), list):
  1136|             raise SystemExit(f"ERROR [{STAGE}] scope_conditions missing param_ids/category_ids lists for {edge_id}")
  1137|         if int(row["file_count"]) > total_files:
  1138|             raise SystemExit(f"ERROR [{STAGE}] edge {edge_id} file_count exceeds total unique export_run_ids")
  1139|         if row["name_resolved"] != "true":
  1140|             raise SystemExit(f"ERROR [{STAGE}] edge {edge_id} has name_resolved=false")
  1141| 
  1142|     if edge_rows and not any(row["param_kind"] == "builtin" and row["name_resolved"] == "true" for row in inventory_rows):
  1143|         warn("inventory contains no resolved builtin rows; expected BIP rows will only appear if present in the corpus.")
  1144| 
  1145| 
  1146| def print_summary(
  1147|     rows_read: int,
  1148|     export_run_ids: Set[str],
  1149|     observations: Sequence[RawObservation],
  1150|     resolved: Dict[str, ResolvedParam],
  1151|     inventory_rows: Sequence[Dict[str, Any]],
  1152|     edge_rows: Sequence[Dict[str, Any]],
  1153|     category_stats: Dict[str, Any],
  1154|     support_min_files: int,
  1155|     out_dir: Path,
  1156| ) -> None:
  1157|     builtin_obs = [o for o in observations if o.param_kind == "builtin"]
  1158|     shared_obs = [o for o in observations if o.param_kind == "shared"]
  1159|     unresolved_obs = [o for o in observations if o.param_kind == "unresolved"]
  1160|     builtin_params = [p for p in resolved.values() if p.param_kind == "builtin"]
  1161|     shared_params = [p for p in resolved.values() if p.param_kind == "shared"]
  1162|     bip_resolved = sum(1 for p in builtin_params if p.name_resolved)
  1163|     guid_resolved = sum(1 for p in shared_params if p.name_resolved)
  1164|     groups_with_domain = sum(1 for r in inventory_rows if r["target_domain"])
  1165|     groups_without_domain = len(inventory_rows) - groups_with_domain
  1166|     unextracted_edge_candidates = sum(1 for r in edge_rows if any(
  1167|         inv["has_unextracted_domain"] == "true"
  1168|         and inv["meets_threshold"] == "true"
  1169|         and inv["name_resolved"] == "true"
  1170|         and (inv["target_domain"] == r["target_domain"] or (not inv["target_domain"] and r["requires_human_review"] == "true"))
  1171|         for inv in inventory_rows
  1172|     ))
  1173| 
  1174|     print("VFD Edge Discovery Summary")
  1175|     print("--------------------------")
  1176|     print(f"Identity items read:       {rows_read} rows")
  1177|     print(f"Unique export_run_ids:     {len(export_run_ids)}")
  1178|     print(f"Param refs extracted:      {len(observations)}")
  1179|     print(f"  builtin:                 {len(builtin_obs)} ({len({o.param_id for o in builtin_obs})} unique bip_ids)")
  1180|     print(f"  shared:                  {len(shared_obs)} ({len({o.param_id for o in shared_obs})} unique GUIDs)")
  1181|     print(f"  unresolved:              {len(unresolved_obs)}")
  1182|     print("Name resolution:")
  1183|     print(f"  BIP resolved:            {bip_resolved}/{len(builtin_params)}  ({len(builtin_params) - bip_resolved} unresolved BIP ints)")
  1184|     print(f"  GUID resolved:           {guid_resolved}/{len(shared_params)}  ({len(shared_params) - guid_resolved} unresolved GUIDs)")
  1185|     print("Target domain:")
  1186|     print(f"  With domain:             {groups_with_domain} groups")
  1187|     print(f"  null (classification):   {groups_without_domain} groups")
  1188|     print(f"Threshold (>= {support_min_files} files):")
  1189|     print(f"  Inventory total:         {len(inventory_rows)} rows")
  1190|     print(f"  Edge candidates:         {len(edge_rows)} rows")
  1191|     print("Categories:")
  1192|     print(f"  Recognized integers:     {len(category_stats['recognized_distinct'])} distinct")
  1193|     print(f"  Unrecognized integers:   {len(category_stats['unrecognized_distinct'])} distinct")
  1194|     print(f"  Unextracted domains:     {unextracted_edge_candidates} edge candidates")
  1195|     print("Output:")
  1196|     print(f"  {out_dir / 'vfd_param_inventory.csv'}")
  1197|     print(f"  {out_dir / 'vfd_dynamic_edges.csv'}")
  1198|     print(f"  {out_dir / 'vfd_domain_gaps.csv'}")
  1199| 
  1200| 
  1201| def build_unresolved_file_rows(
  1202|     observations: Sequence[RawObservation],
  1203|     resolved: Dict[str, ResolvedParam],
  1204|     file_metadata: Dict[str, Dict[str, str]],
  1205| ) -> List[Dict[str, Any]]:
  1206|     rule_counts: Dict[Tuple[str, str], int] = defaultdict(int)
  1207|     files_by_param: Dict[str, Set[str]] = defaultdict(set)
  1208| 
  1209|     for obs in observations:
  1210|         if not GUID_RE.match(obs.param_id):
  1211|             continue
  1212|         param = resolved.get(obs.param_id)
  1213|         if param is None or param.name_resolved:
  1214|             continue
  1215|         rule_counts[(obs.param_id, obs.export_run_id)] += 1
  1216|         files_by_param[obs.param_id].add(obs.export_run_id)
  1217| 
  1218|     warned_missing: Set[str] = set()
  1219|     rows: List[Dict[str, Any]] = []
  1220|     for (param_id, export_run_id), rule_count in rule_counts.items():
  1221|         meta = file_metadata.get(export_run_id)
  1222|         if meta is None:
  1223|             if export_run_id not in warned_missing:
  1224|                 warn(f"export_run_id {export_run_id} not found in file_metadata.csv")
  1225|                 warned_missing.add(export_run_id)
  1226|             client_label = "unknown"
  1227|             governance_role = "unknown"
  1228|             unit_system = "unknown"
  1229|         else:
  1230|             client_label = meta["client_label"] or "unknown"
  1231|             governance_role = meta["governance_role"] or "unknown"
  1232|             unit_system = meta["unit_system"] or "unknown"
  1233| 
  1234|         rows.append({
  1235|             "param_id": param_id,
  1236|             "export_run_id": export_run_id,
  1237|             "client_label": client_label,
  1238|             "governance_role": governance_role,
  1239|             "unit_system": unit_system,
  1240|             "rule_count": rule_count,
  1241|         })
  1242| 
  1243|     rows.sort(key=lambda r: (r["param_id"], -len(files_by_param[r["param_id"]]), r["export_run_id"]))
  1244|     return rows
  1245| 
  1246| 
  1247| def print_unresolved_summary(rows: Sequence[Dict[str, Any]]) -> None:
  1248|     distinct_guids: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
  1249|     for row in rows:
  1250|         distinct_guids[str(row["param_id"])].append(row)
  1251| 
  1252|     print("Unresolved GUID file mapping")
  1253|     print("----------------------------")
  1254|     print(f"Distinct unresolved GUIDs:  {len(distinct_guids)}")
  1255|     print(f"Total file×GUID rows:       {len(rows)}")
  1256|     print()
  1257| 
  1258|     print("Top GUIDs by file count:")
  1259|     top_guids = sorted(distinct_guids.items(), key=lambda kv: (-len(kv[1]), kv[0]))[:10]
  1260|     for param_id, guid_rows in top_guids:
  1261|         clients = sorted({str(r["client_label"]) for r in guid_rows})
  1262|         print(f"  {param_id}  {len(guid_rows)} files  clients: {', '.join(clients)}")
  1263|     print()
  1264| 
  1265|     print("Recommended source files (Template role, highest GUID coverage):")
  1266|     rows_by_client: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
  1267|     for row in rows:
  1268|         rows_by_client[str(row["client_label"])].append(row)
  1269| 
  1270|     for client_label in sorted(rows_by_client):
  1271|         client_rows = rows_by_client[client_label]
  1272|         guids_by_file: Dict[str, Set[str]] = defaultdict(set)
  1273|         role_by_file: Dict[str, str] = {}
  1274|         for row in client_rows:
  1275|             export_run_id = str(row["export_run_id"])
  1276|             guids_by_file[export_run_id].add(str(row["param_id"]))
  1277|             role_by_file[export_run_id] = str(row["governance_role"])
  1278| 
  1279|         template_files = [f for f in guids_by_file if role_by_file[f].lower() == "template"]
  1280|         candidates = template_files or list(guids_by_file)
  1281|         best_file = max(candidates, key=lambda f: (len(guids_by_file[f]), f))
  1282|         print(f"  client={client_label}  file={best_file}  resolves {len(guids_by_file[best_file])} distinct GUIDs")
  1283| 
  1284| 
  1285| def parse_args() -> argparse.Namespace:
  1286|     script_dir = Path(__file__).resolve().parent
  1287|     ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  1288|     ap.add_argument("--identity-items-dir", required=True, help="Parent directory containing identity_items shards")
  1289|     ap.add_argument("--bip-lookup", required=True, help="Path to bip_lookup.json")
  1290|     ap.add_argument("--out-dir", required=True, help="Output directory")
  1291|     ap.add_argument("--category-domain-map", default=str(script_dir / "vfd_category_domain_map.json"), help="Path to vfd_category_domain_map.json")
  1292|     ap.add_argument("--bip-hints", default=str(script_dir / "vfd_bip_target_domain_hints.json"), help="Path to vfd_bip_target_domain_hints.json")
  1293|     ap.add_argument("--shared-param-names", default=None, help="Optional path to shared_param_names.json")
  1294|     ap.add_argument("--support-min-files", type=int, default=10, help="Minimum distinct export_run_ids for edge candidates")
  1295|     ap.add_argument("--include-unresolved", action="store_true", help="Include target_domain=null rows in vfd_dynamic_edges.csv for review")
  1296|     ap.add_argument("--dump-unresolved-files", default=None, help="Optional path for unresolved-GUID file-mapping CSV (vfd_unresolved_files.csv)")
  1297|     ap.add_argument("--file-metadata", default=None, help="Path to file_metadata.csv (required when --dump-unresolved-files is set)")
  1298|     args = ap.parse_args()
  1299|     if args.support_min_files < 1:
  1300|         raise SystemExit("ERROR [vfd_discover] --support-min-files must be >= 1")
  1301|     if args.dump_unresolved_files and not args.file_metadata:
  1302|         raise SystemExit("ERROR [vfd_discover] --file-metadata is required when --dump-unresolved-files is set")
  1303|     return args
  1304| 
  1305| 
  1306| def main() -> int:
  1307|     args = parse_args()
  1308|     identity_items_dir = Path(args.identity_items_dir)
  1309|     identity_path = find_identity_items_path(identity_items_dir)
  1310|     bip_lookup_path = Path(args.bip_lookup)
  1311|     out_dir = Path(args.out_dir)
  1312|     category_map_path = Path(args.category_domain_map)
  1313|     bip_hints_path = Path(args.bip_hints)
  1314|     shared_param_names_path = Path(args.shared_param_names) if args.shared_param_names else None
  1315| 
  1316|     bip_lookup = read_json_required(bip_lookup_path, "bip_lookup.json")
  1317|     category_map = read_json_required(category_map_path, "vfd_category_domain_map.json")
  1318|     bip_hints = load_bip_hints(bip_hints_path)
  1319|     shared_param_names = read_json_optional(shared_param_names_path, "shared_param_names.json")
  1320|     bip_name_to_id = {str(name): str(param_id) for param_id, name in bip_lookup.items()}
  1321|     if len(bip_name_to_id) != len(bip_lookup):
  1322|         warn("bip_lookup.json contains duplicate BIP names; reverse lookup is non-unique.")
  1323| 
  1324|     observations, rows_read, export_run_ids = stream_observations(identity_path)
  1325|     resolved = resolve_params(observations, bip_lookup, shared_param_names)
  1326|     inventory_rows, category_stats = build_inventory_rows(
  1327|         observations, resolved, bip_hints, category_map, args.support_min_files,
  1328|         identity_items_dir=identity_items_dir,
  1329|     )
  1330|     edge_rows = build_edge_rows(inventory_rows, args.include_unresolved, args.support_min_files)
  1331|     domain_gap_rows = build_domain_gap_rows(inventory_rows, category_map)
  1332|     verify_outputs(edge_rows, inventory_rows, len(export_run_ids))
  1333| 
  1334|     atomic_write_csv(out_dir / "vfd_param_inventory.csv", INVENTORY_FIELDS, inventory_rows)
  1335|     atomic_write_csv(out_dir / "vfd_dynamic_edges.csv", EDGE_FIELDS, edge_rows)
  1336|     atomic_write_csv(out_dir / "vfd_domain_gaps.csv", DOMAIN_GAP_FIELDS, domain_gap_rows)
  1337|     print_summary(
  1338|         rows_read, export_run_ids, observations, resolved, inventory_rows, edge_rows,
  1339|         category_stats, args.support_min_files, out_dir,
  1340|     )
  1341| 
  1342|     if args.dump_unresolved_files:
  1343|         file_metadata = load_file_metadata(Path(args.file_metadata))
  1344|         unresolved_rows = build_unresolved_file_rows(observations, resolved, file_metadata)
  1345|         atomic_write_csv(Path(args.dump_unresolved_files), UNRESOLVED_FILE_FIELDS, unresolved_rows)
  1346|         print()
  1347|         print_unresolved_summary(unresolved_rows)
  1348| 
  1349|     return 0
  1350| 
  1351| 
  1352| if __name__ == "__main__":
  1353|     raise SystemExit(main())
```
