# Chunk of tools/extractor.py

- Source relative path: `tools/extractor.py`
- Chunk: 3 of 4
- Original line range: 983-1273
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: emit_records
- Source SHA-256: d75cdfbab8fb9d4bbc3c46c3611b1bcf54844b8f5421954d15f78ef298ab109e
- Starts inside symbol: no
- Ends inside symbol: no

```
   983| def emit_records(exports_dir: Path, out_dir: Path, file_id_mode: str = "basename") -> Tuple[int, int]:
   984|     """Stream flatten outputs directly to disk as each export file is processed.
   985| 
   986|     Returns (file_count, record_count). High-volume CSVs (records, labels, reasons,
   987|     parameter_rows) are written row-by-row so memory is bounded by a single export
   988|     file's content, not the entire corpus. file_metadata.csv is written after the
   989|     loop since it is small (one row per file) and needs annotation preservation.
   990|     """
   991|     exported_utc = _utc_now_iso()
   992|     tool_version = _get_tool_version()
   993|     governance_rules = _load_governance_role_rules()
   994| 
   995|     _RECORD_FIELDS = [
   996|         "schema_version", "export_run_id", "file_id", "domain", "record_pk", "record_id", "record_ordinal",
   997|         "status", "identity_quality", "sig_hash", "join_hash", "join_key_schema",
   998|         "join_key_status", "join_key_policy_id", "join_key_policy_version",
   999|         "label_display", "label_quality", "label_provenance", "is_purgeable",
  1000|         "instance_count", "is_sole_type_in_category",
  1001|     ]
  1002|     _LABEL_FIELDS = [
  1003|         "schema_version", "export_run_id", "domain", "record_pk", "component_key", "component_value", "component_order",
  1004|     ]
  1005|     _REASON_FIELDS = [
  1006|         "schema_version", "export_run_id", "domain", "record_pk", "reason_code", "reason_detail",
  1007|     ]
  1008|     _PARAM_FIELDS = [
  1009|         "schema_version", "export_run_id", "domain", "record_pk", "param_index",
  1010|         "lftp.key", "lftp.name", "lftp.guid", "lftp.id", "lftp.id_sign",
  1011|         "lftp.storage_type", "lftp.has_value", "lftp.data_type",
  1012|         "lftp.binding_scope", "lftp.semantic_role", "lftp.source",
  1013|         "lftp.value_uniform", "lftp.value_distinct_count", "lftp.value_set", "lftp.value_raw_set",
  1014|     ]
  1015|     _ITEM_FIELDS = [
  1016|         "schema_version", "export_run_id", "domain", "record_pk", "item_key", "item_value",
  1017|         "item_value_type", "item_role",
  1018|     ]
  1019| 
  1020|     # Read existing annotations before opening output files so we can apply them inline.
  1021|     annotation_columns = [
  1022|         "client_label", "governance_role", "discipline_label", "project_label",
  1023|         "business_center_label", "collection_label",
  1024|     ]
  1025|     existing_annotations: Dict[str, Dict[str, str]] = {}
  1026|     existing_meta_path = out_dir / "file_metadata.csv"
  1027|     if existing_meta_path.exists():
  1028|         for _ar in _read_existing_csv(existing_meta_path):
  1029|             eid = _ar.get("export_run_id", "").strip()
  1030|             if eid:
  1031|                 preserved = {col: _ar.get(col, "").strip() for col in annotation_columns}
  1032|                 preserved["unit_system"] = _ar.get("unit_system", "").strip()
  1033|                 if any(v for v in preserved.values()):
  1034|                     existing_annotations[eid] = preserved
  1035| 
  1036|     # Set up identity-item shard dir (already streams per-domain).
  1037|     shard_dir = out_dir / "identity_items_by_domain"
  1038|     shard_dir.mkdir(parents=True, exist_ok=True)
  1039|     for _stale in shard_dir.glob("*.csv"):
  1040|         _stale.unlink(missing_ok=True)
  1041|     (shard_dir / ".complete").unlink(missing_ok=True)
  1042|     _item_shard_handles: Dict[str, Any] = {}
  1043|     _item_shard_writers: Dict[str, Any] = {}
  1044| 
  1045|     meta_rows: List[Dict[str, str]] = []  # one row per file — stays in memory
  1046|     record_count = 0
  1047|     governance_counts: Dict[str, int] = defaultdict(int)
  1048| 
  1049|     out_dir.mkdir(parents=True, exist_ok=True)
  1050|     # Write to .tmp files during the loop. Files are promoted to their final names only
  1051|     # after the loop AND file_metadata.csv both succeed, so a mid-loop failure (corrupt
  1052|     # JSON, disk error, etc.) leaves the previous complete output intact.
  1053|     _streaming_stems = ["records", "label_components", "status_reasons", "parameter_rows"]
  1054|     _tmp: Dict[str, Path] = {s: out_dir / f"{s}.csv.tmp" for s in _streaming_stems}
  1055|     with (
  1056|         _tmp["records"].open("w", newline="", encoding="utf-8") as _rec_f,
  1057|         _tmp["label_components"].open("w", newline="", encoding="utf-8") as _lbl_f,
  1058|         _tmp["status_reasons"].open("w", newline="", encoding="utf-8") as _rsn_f,
  1059|         _tmp["parameter_rows"].open("w", newline="", encoding="utf-8") as _par_f,
  1060|     ):
  1061|         _rec_w = csv.DictWriter(_rec_f, fieldnames=_RECORD_FIELDS)
  1062|         _lbl_w = csv.DictWriter(_lbl_f, fieldnames=_LABEL_FIELDS)
  1063|         _rsn_w = csv.DictWriter(_rsn_f, fieldnames=_REASON_FIELDS)
  1064|         _par_w = csv.DictWriter(_par_f, fieldnames=_PARAM_FIELDS)
  1065|         for _w in (_rec_w, _lbl_w, _rsn_w, _par_w):
  1066|             _w.writeheader()
  1067| 
  1068|         for _, primary, secondary in _iter_export_files(exports_dir):
  1069|             data = _read_json(primary)
  1070|             if secondary is not None:
  1071|                 data = _merge_index_details(data, _read_json(secondary))
  1072|             export_run_id = _file_id(primary, file_id_mode)
  1073|             file_id = export_run_id
  1074| 
  1075|             contract = data.get("_contract") if isinstance(data.get("_contract"), dict) else {}
  1076|             ident = contract.get("identity") if isinstance(contract.get("identity"), dict) else {}
  1077|             identity_meta = _identity_metadata(data)
  1078|             # A file that already has unit_system set in file_metadata.csv keeps that
  1079|             # value verbatim -- only new files (no prior annotation) pay the cost of
  1080|             # deriving unit_system from the export JSON.
  1081|             ann = existing_annotations.get(export_run_id, {})
  1082|             existing_unit_system = ann.get("unit_system", "").strip()
  1083|             meta_row: Dict[str, str] = {
  1084|                 "schema_version": SCHEMA_VERSION,
  1085|                 "export_run_id": export_run_id,
  1086|                 "file_id": file_id,
  1087|                 "project_id": _safe_str(ident.get("project_id") or ident.get("project_title")),
  1088|                 "model_id": _safe_str(ident.get("model_id") or ident.get("model_title")),
  1089|                 "project_label": identity_meta["project_label"],
  1090|                 "model_label": identity_meta["model_label"],
  1091|                 "central_path": identity_meta["central_path"],
  1092|                 "central_path_norm": identity_meta["central_path_norm"],
  1093|                 "lineage_hash": identity_meta["lineage_hash"],
  1094|                 "revit_version_number": identity_meta["revit_version_number"],
  1095|                 "revit_version_name": identity_meta["revit_version_name"],
  1096|                 "revit_build": identity_meta["revit_build"],
  1097|                 "is_workshared": identity_meta["is_workshared"],
  1098|                 "tool_version": tool_version,
  1099|                 "exported_utc": exported_utc,
  1100|                 "client_label": "",
  1101|                 "governance_role": "",
  1102|                 "unit_system": existing_unit_system or _derive_unit_system(data, export_run_id),
  1103|             }
  1104|             # Apply annotation preservation and governance inference inline per file.
  1105|             if ann:
  1106|                 for col in annotation_columns:
  1107|                     if not meta_row.get(col, "").strip():
  1108|                         meta_row[col] = ann.get(col, "")
  1109|             if not meta_row.get("governance_role", "").strip():
  1110|                 meta_row["governance_role"] = _infer_governance_role(meta_row.get("central_path_norm", ""), governance_rules)
  1111|             governance_counts[meta_row.get("governance_role", "").strip()] += 1
  1112|             meta_rows.append(meta_row)
  1113| 
  1114|             for source_domain in _iter_domains(data):
  1115|                 payload = data.get(source_domain)
  1116|                 recs = payload.get("records") if isinstance(payload, dict) else None
  1117|                 if not isinstance(recs, list):
  1118|                     continue
  1119|                 for i, rec in enumerate(recs):
  1120|                     if not isinstance(rec, dict):
  1121|                         continue
  1122|                     domain = _remap_object_style_domain(source_domain, rec)
  1123|                     if not domain:
  1124|                         continue
  1125|                     domain = _remap_vco_domain(domain, rec)
  1126|                     if not domain:
  1127|                         continue
  1128|                     record_ordinal = f"{i:06d}"
  1129|                     record_pk = f"{file_id}|{domain}|{record_ordinal}"
  1130|                     record_id = _safe_str(rec.get("record_id") or rec.get("id") or rec.get("name"))
  1131|                     # Day-1 identity-mode flatten join regime:
  1132|                     # - keep sig_hash as-is
  1133|                     # - set join_hash = sig_hash
  1134|                     # - set join_key_schema = sig_hash_as_join_key.v1
  1135|                     sig_hash_v = _safe_str(rec.get("sig_hash"))
  1136|                     _rec_w.writerow({
  1137|                         "schema_version": SCHEMA_VERSION,
  1138|                         "export_run_id": export_run_id,
  1139|                         "file_id": file_id,
  1140|                         "domain": domain,
  1141|                         "record_pk": record_pk,
  1142|                         "record_id": record_id,
  1143|                         "record_ordinal": record_ordinal,
  1144|                         "status": _safe_str(rec.get("status")),
  1145|                         "identity_quality": _safe_str(rec.get("identity_quality")),
  1146|                         "sig_hash": sig_hash_v,
  1147|                         "join_hash": sig_hash_v,
  1148|                         "join_key_schema": "sig_hash_as_join_key.v1",
  1149|                         "join_key_status": "bootstrap",
  1150|                         "join_key_policy_id": "",
  1151|                         "join_key_policy_version": "",
  1152|                         "label_display": _safe_str((rec.get("label") or {}).get("display")),
  1153|                         "label_quality": _safe_str((rec.get("label") or {}).get("quality")),
  1154|                         "label_provenance": _safe_str((rec.get("label") or {}).get("provenance")),
  1155|                         "is_purgeable": _safe_str(rec.get("is_purgeable")),
  1156|                         "instance_count": _safe_str(rec.get("instance_count")),
  1157|                         "is_sole_type_in_category": _safe_str(rec.get("is_sole_type_in_category")),
  1158|                     })
  1159|                     record_count += 1
  1160| 
  1161|                     for reason in rec.get("status_reasons") if isinstance(rec.get("status_reasons"), list) else []:
  1162|                         if isinstance(reason, str) and reason:
  1163|                             _rsn_w.writerow({
  1164|                                 "schema_version": SCHEMA_VERSION,
  1165|                                 "export_run_id": export_run_id,
  1166|                                 "domain": domain,
  1167|                                 "record_pk": record_pk,
  1168|                                 "reason_code": reason,
  1169|                                 "reason_detail": "",
  1170|                             })
  1171| 
  1172|                     items = rec.get("items") if isinstance(rec.get("items"), list) else None
  1173|                     if not isinstance(items, list):
  1174|                         items = (rec.get("identity_basis") or {}).get("items") if isinstance(rec.get("identity_basis"), dict) else None
  1175|                     if isinstance(items, list):
  1176|                         for it in items:
  1177|                             if not isinstance(it, dict):
  1178|                                 continue
  1179|                             if domain not in _item_shard_writers:
  1180|                                 _fp = (shard_dir / f"{domain}.csv").open("w", newline="", encoding="utf-8")
  1181|                                 _item_shard_handles[domain] = _fp
  1182|                                 _w = csv.DictWriter(_fp, fieldnames=_ITEM_FIELDS)
  1183|                                 _w.writeheader()
  1184|                                 _item_shard_writers[domain] = _w
  1185|                             _item_shard_writers[domain].writerow({
  1186|                                 "schema_version": SCHEMA_VERSION,
  1187|                                 "export_run_id": export_run_id,
  1188|                                 "domain": domain,
  1189|                                 "record_pk": record_pk,
  1190|                                 "item_key": _safe_str(it.get("k")),
  1191|                                 "item_value": _safe_str(it.get("v")),
  1192|                                 "item_value_type": _safe_str(it.get("q")),
  1193|                                 "item_role": "",
  1194|                             })
  1195| 
  1196|                     for pr in rec.get("parameter_rows") if isinstance(rec.get("parameter_rows"), list) else []:
  1197|                         if not isinstance(pr, dict):
  1198|                             continue
  1199|                         _par_w.writerow({
  1200|                             "schema_version": SCHEMA_VERSION,
  1201|                             "export_run_id": export_run_id,
  1202|                             "domain": domain,
  1203|                             "record_pk": record_pk,
  1204|                             "param_index": _safe_str(pr.get("param_index")),
  1205|                             "lftp.key": _safe_str(pr.get("lftp.key")),
  1206|                             "lftp.name": _safe_str(pr.get("lftp.name")),
  1207|                             "lftp.guid": _safe_str(pr.get("lftp.guid")),
  1208|                             "lftp.id": _safe_str(pr.get("lftp.id")),
  1209|                             "lftp.id_sign": _safe_str(pr.get("lftp.id_sign")),
  1210|                             "lftp.storage_type": _safe_str(pr.get("lftp.storage_type")),
  1211|                             "lftp.has_value": _safe_str(pr.get("lftp.has_value")),
  1212|                             "lftp.data_type": _safe_str(pr.get("lftp.data_type")),
  1213|                             "lftp.binding_scope": _safe_str(pr.get("lftp.binding_scope")),
  1214|                             "lftp.semantic_role": _safe_str(pr.get("lftp.semantic_role")),
  1215|                             "lftp.source": _safe_str(pr.get("lftp.source")),
  1216|                             "lftp.value_uniform": _safe_str(pr.get("lftp.value_uniform")),
  1217|                             "lftp.value_distinct_count": _safe_str(pr.get("lftp.value_distinct_count")),
  1218|                             "lftp.value_set": _safe_str(pr.get("lftp.value_set")),
  1219|                             "lftp.value_raw_set": _safe_str(pr.get("lftp.value_raw_set")),
  1220|                         })
  1221| 
  1222|                     comps = (rec.get("label") or {}).get("components") if isinstance(rec.get("label"), dict) else None
  1223|                     if isinstance(comps, dict):
  1224|                         for order, key in enumerate(sorted(comps.keys(), key=str)):
  1225|                             val = comps.get(key)
  1226|                             if not isinstance(val, (str, int, float, bool)) and val is not None:
  1227|                                 val = json.dumps(val, ensure_ascii=False, sort_keys=True)
  1228|                             _lbl_w.writerow({
  1229|                                 "schema_version": SCHEMA_VERSION,
  1230|                                 "export_run_id": export_run_id,
  1231|                                 "domain": domain,
  1232|                                 "record_pk": record_pk,
  1233|                                 "component_key": _safe_str(key),
  1234|                                 "component_value": _safe_str(val),
  1235|                                 "component_order": str(order),
  1236|                             })
  1237| 
  1238|     for _fp in _item_shard_handles.values():
  1239|         _fp.close()
  1240|     _item_shard_handles.clear()
  1241|     _item_shard_writers.clear()
  1242|     # Shards are the sole identity_items output -- no monolithic identity_items.csv
  1243|     # rebuild (see DECISIONS.md D-037).
  1244|     # Remove any stale monolithic file left behind by an older extractor.py version
  1245|     # rerun into this same out_dir, so legacy-fallback readers can't silently read
  1246|     # rows from a previous corpus alongside the freshly-written shards. Only done
  1247|     # after the new shards are confirmed fully written and closed above, matching
  1248|     # this function's existing promote-only-after-success convention.
  1249|     (out_dir / "identity_items.csv").unlink(missing_ok=True)
  1250|     # Sentinel content is never parsed by any reader (existence-only gate), so a
  1251|     # wall-clock timestamp is sufficient.
  1252|     (shard_dir / ".complete").write_text(str(time.time()), encoding="utf-8")
  1253| 
  1254|     sys.stderr.write(
  1255|         "[INFO extractor] governance_role inference summary: "
  1256|         + ", ".join(f"{(role or '<empty>')}={count}" for role, count in sorted(governance_counts.items()))
  1257|         + "\n"
  1258|     )
  1259|     _write_csv(out_dir / "file_metadata.csv", [
  1260|         "schema_version", "export_run_id", "file_id", "project_id", "model_id",
  1261|         "project_label", "model_label", "central_path", "central_path_norm",
  1262|         "lineage_hash", "revit_version_number", "revit_version_name", "revit_build",
  1263|         "is_workshared", "tool_version", "exported_utc",
  1264|         "client_label", "governance_role", "unit_system", "discipline_label",
  1265|         "business_center_label", "collection_label",
  1266|     ], _sort_rows(meta_rows, ["export_run_id"]))
  1267| 
  1268|     for stem in _streaming_stems:
  1269|         _tmp[stem].replace(out_dir / f"{stem}.csv")
  1270| 
  1271|     return len(meta_rows), record_count
  1272| 
  1273| 
```
