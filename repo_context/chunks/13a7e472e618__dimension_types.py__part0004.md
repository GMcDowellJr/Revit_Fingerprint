# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 4 of 8
- Original line range: 1080-1452
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_radial, _apply_family_name_override
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
  1080| def extract_radial(doc, ctx=None):
  1081|     _HANDLED_SHAPES = _RADIAL_HANDLED
  1082|     EXPECTED_FAMILY = _RADIAL_EXPECTED_FAMILY
  1083|     DOMAIN_NAME = "dimension_types_radial"
  1084|     """
  1085|     Extract Radial dimension types fingerprint.
  1086| 
  1087|     Args:
  1088|         doc: Revit Document
  1089|         ctx: Context dictionary
  1090| 
  1091|     Returns:
  1092|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
  1093|     """
  1094|     info = {
  1095|         "count": 0,
  1096|         "raw_count": 0,
  1097|         "records": [],
  1098|         "signature_hashes_v2": [],
  1099|         "hash_v2": None,
  1100|         "debug_v2_blocked": False,
  1101|         "debug_v2_block_reasons": {},
  1102|     }
  1103| 
  1104|     if ctx is None:
  1105|         ctx = {}
  1106| 
  1107|     if DimensionType is None:
  1108|         info["debug_v2_blocked"] = True
  1109|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
  1110|         return info
  1111| 
  1112|     try:
  1113|         all_types = _collect_dim_types(doc, ctx)
  1114|     except Exception:
  1115|         all_types = []
  1116| 
  1117|     info["raw_count"] = len(all_types)
  1118|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
  1119| 
  1120|     v2_records = []
  1121|     v2_sig_hashes = []
  1122|     _eligible_type_count = 0
  1123| 
  1124|     for d in all_types:
  1125|         try:
  1126|             type_name = get_type_display_name(d)
  1127| 
  1128|             # Exclude system built-in types with id-based labels (not user-accessible)
  1129|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
  1130|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
  1131|                 continue
  1132| 
  1133|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
  1134| 
  1135|             # Apply family-name heuristic override to detect Spot types
  1136|             shape_v, shape_family, shape_q = _apply_family_name_override(
  1137|                 d, shape_v, shape_family, shape_q, type_name
  1138|             )
  1139| 
  1140|             # Filter: skip shapes not handled by this domain
  1141|             if shape_v not in _HANDLED_SHAPES:
  1142|                 continue
  1143| 
  1144|             # Exclude confirmed wrong-family types (system/infrastructure types)
  1145|             family_name = None
  1146|             try:
  1147|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
  1148|                 if p_fam:
  1149|                     family_name = _as_string(p_fam)
  1150|                     if family_name:
  1151|                         family_name = canon_str(family_name)
  1152|             except Exception:
  1153|                 pass
  1154|             if family_name and family_name != EXPECTED_FAMILY:
  1155|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
  1156|                 continue
  1157| 
  1158|             _eligible_type_count += 1
  1159| 
  1160|             # --- Read core identity fields ---
  1161| 
  1162|             # Unit format info
  1163|             (unit_format_id_v, unit_format_id_q,
  1164|              rounding_v, rounding_q,
  1165|              accuracy_v, accuracy_q,
  1166|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
  1167| 
  1168|             # Tick mark sig hash
  1169|             tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)
  1170| 
  1171|             # Center marks (radial-specific)
  1172|             center_marks_v, center_marks_q = (None, ITEM_Q_MISSING)
  1173|             try:
  1174|                 p_cm = first_param(d, ui_names=["Center Marks"])
  1175|                 cm_int = _as_int(p_cm) if p_cm is not None else None
  1176|                 if cm_int is not None:
  1177|                     center_marks_v, center_marks_q = canonicalize_str(safe_str(cm_int))
  1178|                     if center_marks_v is None:
  1179|                         center_marks_q = ITEM_Q_UNREADABLE
  1180|             except Exception:
  1181|                 center_marks_v, center_marks_q = (None, ITEM_Q_UNREADABLE)
  1182| 
  1183|             # Center mark size (radial-specific), stored in feet, convert to inches
  1184|             center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
  1185|             try:
  1186|                 p_cms = first_param(d, ui_names=["Center Mark Size"])
  1187|                 cms_ft = _as_double(p_cms) if p_cms is not None else None
  1188|                 if cms_ft is not None:
  1189|                     center_mark_size_v, center_mark_size_q = canonicalize_float(_fmt_in_from_ft(cms_ft))
  1190|                 else:
  1191|                     center_mark_size_v, center_mark_size_q = (None, ITEM_Q_MISSING)
  1192|             except Exception:
  1193|                 center_mark_size_v, center_mark_size_q = (None, ITEM_Q_UNREADABLE)
  1194| 
  1195|             # Radius symbol location
  1196|             radius_symbol_location_v, radius_symbol_location_q = (None, ITEM_Q_MISSING)
  1197|             try:
  1198|                 p_rsl = first_param(d, ui_names=["Radius Symbol Location", "Symbol Location"])
  1199|                 rsl_raw = _as_string(p_rsl) if p_rsl is not None else None
  1200|                 radius_symbol_location_v, radius_symbol_location_q = canonicalize_str_allow_empty(rsl_raw)
  1201|             except Exception:
  1202|                 radius_symbol_location_v, radius_symbol_location_q = (None, ITEM_Q_UNREADABLE)
  1203| 
  1204|             # Radius symbol text
  1205|             radius_symbol_text_v, radius_symbol_text_q = (None, ITEM_Q_MISSING)
  1206|             try:
  1207|                 p_rst = first_param(d, ui_names=["Radius Symbol Text"])
  1208|                 rst_raw = _as_string(p_rst) if p_rst is not None else None
  1209|                 radius_symbol_text_v, radius_symbol_text_q = canonicalize_str_allow_empty(rst_raw)
  1210|             except Exception:
  1211|                 radius_symbol_text_v, radius_symbol_text_q = (None, ITEM_Q_UNREADABLE)
  1212| 
  1213|             # --- Area 7 §2/§4c: leader config + tick weight (angular/diameter/linear/radial) ---
  1214|             leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
  1215|                 d, ctx, ui_names=["Leader Tick Mark"]
  1216|             )
  1217|             leader_type_v, leader_type_q = (None, ITEM_Q_MISSING)
  1218|             try:
  1219|                 p_lt = first_param(d, ui_names=["Leader Type"])
  1220|                 lt_raw = _as_value_string(p_lt) if p_lt is not None else None
  1221|                 leader_type_v, leader_type_q = canonicalize_str(lt_raw)
  1222|             except Exception:
  1223|                 leader_type_v, leader_type_q = (None, ITEM_Q_UNREADABLE)
  1224|             show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_MISSING)
  1225|             try:
  1226|                 p_slwtm = first_param(d, ui_names=["Show Leader When Text Moves"])
  1227|                 slwtm_raw = _as_value_string(p_slwtm) if p_slwtm is not None else None
  1228|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = canonicalize_str(slwtm_raw)
  1229|             except Exception:
  1230|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_UNREADABLE)
  1231|             tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_MISSING)
  1232|             try:
  1233|                 p_tmlw = first_param(d, ui_names=["Tick Mark Line Weight"])
  1234|                 tmlw_int = _as_int(p_tmlw) if p_tmlw is not None else None
  1235|                 tick_mark_line_weight_v, tick_mark_line_weight_q = canonicalize_int(tmlw_int)
  1236|             except Exception:
  1237|                 tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_UNREADABLE)
  1238| 
  1239|             # --- Area 7 §7: Text Offset (Angular/Diameter/Linear/Radial per probe) ---
  1240|             text_offset_v, text_offset_q = (None, ITEM_Q_MISSING)
  1241|             try:
  1242|                 p_toff = first_param(d, ui_names=["Text Offset"])
  1243|                 toff_ft = _as_double(p_toff) if p_toff is not None else None
  1244|                 text_offset_v, text_offset_q = canonicalize_float(_fmt_in_from_ft(toff_ft))
  1245|             except Exception:
  1246|                 text_offset_v, text_offset_q = (None, ITEM_Q_UNREADABLE)
  1247| 
  1248|             # --- Build identity items ---
  1249|             core_items = [
  1250|                 make_identity_item("dim_type.shape", shape_v, shape_q),
  1251|                 make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
  1252|                 make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
  1253|                 make_identity_item("dim_type.center_marks", center_marks_v, center_marks_q),
  1254|                 make_identity_item("dim_type.center_mark_size", center_mark_size_v, center_mark_size_q),
  1255|                 make_identity_item("dim_type.radius_symbol_location", radius_symbol_location_v, radius_symbol_location_q),
  1256|                 make_identity_item("dim_type.radius_symbol_text", radius_symbol_text_v, radius_symbol_text_q),
  1257|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
  1258|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
  1259|                 make_identity_item("dim_type.leader_tick_mark_sig_hash", leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q),
  1260|                 make_identity_item("dim_type.leader_type", leader_type_v, leader_type_q),
  1261|                 make_identity_item("dim_type.show_leader_when_text_moves", show_leader_when_text_moves_v, show_leader_when_text_moves_q),
  1262|                 make_identity_item("dim_type.tick_mark_line_weight", tick_mark_line_weight_v, tick_mark_line_weight_q),
  1263|                 make_identity_item("dim_type.text_offset_in", text_offset_v, text_offset_q),
  1264|             ]
  1265| 
  1266|             text_items = _build_text_appearance_items(d)
  1267|             alt_units_items = _build_alternate_units_items(d)
  1268|             all_items = core_items + text_items + alt_units_items
  1269| 
  1270|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
  1271| 
  1272|             # Required qualities for blocking
  1273|             # radius_symbol_location, radius_symbol_text are optional enrichment — not blocking
  1274|             required_qs = [
  1275|                 shape_q,
  1276|                 accuracy_q,
  1277|                 center_marks_q,
  1278|                 center_mark_size_q,
  1279|                 unit_format_id_q,
  1280|             ]
  1281|             # text/appearance fields, and all Area 7 additions, are cross-family alignment /
  1282|             # non-blocking enrichment — not blocking
  1283| 
  1284|             blocked = any(q != ITEM_Q_OK for q in required_qs)
  1285| 
  1286|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
  1287|                 "dim_type.tick_mark_sig_hash",
  1288|                 "dim_type.leader_tick_mark_sig_hash",
  1289|             })
  1290| 
  1291|             status_reasons = []
  1292|             for it in identity_items:
  1293|                 q = it.get("q")
  1294|                 k = it.get("k", "")
  1295|                 if q == ITEM_Q_OK:
  1296|                     continue
  1297|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
  1298|                     continue
  1299|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
  1300| 
  1301|             if blocked:
  1302|                 status = STATUS_BLOCKED
  1303|             elif status_reasons:
  1304|                 status = STATUS_DEGRADED
  1305|             else:
  1306|                 status = STATUS_OK
  1307| 
  1308|             preimage = serialize_identity_items(identity_items)
  1309|             sig_hash = None if blocked else make_hash(preimage)
  1310| 
  1311|             try:
  1312|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
  1313|             except Exception:
  1314|                 type_id_int = None
  1315| 
  1316|             try:
  1317|                 uid_raw = getattr(d, "UniqueId", None)
  1318|             except Exception:
  1319|                 uid_raw = None
  1320| 
  1321|             label_str = type_name
  1322|             rec_v2 = build_record_v2(
  1323|                 domain=DOMAIN_NAME,
  1324|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
  1325|                 status=status,
  1326|                 status_reasons=sorted(set(status_reasons)),
  1327|                 sig_hash=sig_hash,
  1328|                 identity_items=identity_items,
  1329|                 required_qs=tuple(required_qs),
  1330|                 label={
  1331|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
  1332|                     "quality": "human" if label_str else "placeholder_missing",
  1333|                     "provenance": "revit.DimensionType.params",
  1334|                 },
  1335|             )
  1336|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
  1337|             rec_v2["is_purgeable"] = _ip
  1338|             rec_v2["is_purgeable_q"] = _ip_q
  1339|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
  1340| 
  1341|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  1342|             rec_v2["join_key"], _missing = build_join_key_from_policy(
  1343|                 domain_policy=pol,
  1344|                 identity_items=identity_items,
  1345|                 include_optional_items=False,
  1346|                 emit_keys_used=True,
  1347|                 hash_optional_items=False,
  1348|                 emit_items=False,
  1349|                 emit_selectors=True,
  1350|             )
  1351| 
  1352|             # Canonical Name Identity Projection (PR1): second, independent join_hash
  1353|             # variant keyed off this record's own label.display-backing item
  1354|             # (dim_type.name). dim_type.name does not exist anywhere in this file --
  1355|             # type_name/label_str feeds label.display only. Widened items list used
  1356|             # only for this call; identity_basis.items/sig_hash/join_key above are
  1357|             # unaffected. (dimension_types_spot_coordinate/spot_elevation are excluded
  1358|             # from the name-key policy entirely -- their only other name-shaped item,
  1359|             # dim_type.symbol_name, names a different, referenced tick-mark/leader
  1360|             # symbol element, not this record's own label.)
  1361|             dt_name_v, dt_name_q = canonicalize_str(type_name)
  1362|             name_key_items = identity_items + [
  1363|                 make_identity_item("dim_type.name", dt_name_v, dt_name_q)
  1364|             ]
  1365|             name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  1366|             rec_v2["join_key_name_identity"], _name_key_missing = build_join_key_from_policy(
  1367|                 domain_policy=name_key_pol,
  1368|                 identity_items=name_key_items,
  1369|                 include_optional_items=False,
  1370|                 emit_keys_used=True,
  1371|                 hash_optional_items=False,
  1372|                 emit_items=False,
  1373|                 emit_selectors=True,
  1374|             )
  1375|             rec_v2["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _name_key_missing)
  1376| 
  1377|             coordination_items = [
  1378|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
  1379|             ]
  1380| 
  1381|             unknown_items = []
  1382|             try:
  1383|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
  1384|             except Exception:
  1385|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  1386|             try:
  1387|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
  1388|             except Exception:
  1389|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  1390|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
  1391|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
  1392| 
  1393|             rec_v2["phase2"] = {
  1394|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  1395|                 "grouping_basis": "phase2.hypothesis",
  1396|                 "cosmetic_items": phase2_sorted_items([]),
  1397|                 "coordination_items": phase2_sorted_items(coordination_items),
  1398|                 "unknown_items": phase2_sorted_items(unknown_items),
  1399|             }
  1400| 
  1401|             if sig_hash:
  1402|                 v2_sig_hashes.append(sig_hash)
  1403|             v2_records.append(rec_v2)
  1404| 
  1405|         except Exception:
  1406|             continue  # fail-soft per record
  1407| 
  1408|     _total_type_count = _eligible_type_count
  1409|     for rec in v2_records:
  1410|         try:
  1411|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
  1412|             rec["is_sole_type_in_category_q"] = "ok"
  1413|         except Exception:
  1414|             rec["is_sole_type_in_category"] = None
  1415|             rec["is_sole_type_in_category_q"] = "unreadable"
  1416| 
  1417|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
  1418|     info["count"] = len(v2_records)
  1419|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  1420| 
  1421|     if v2_sig_hashes:
  1422|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1423|         info["debug_v2_blocked"] = False
  1424|     else:
  1425|         info["hash_v2"] = None
  1426|         info["debug_v2_blocked"] = True
  1427|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
  1428| 
  1429|     return info
  1430| 
  1431| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
  1432|     """
  1433|     Heuristic override: if the FamilyName prefix indicates a Spot family,
  1434|     force Spot classification so we skip this record (spot shapes have their own domain).
  1435|     Returns updated (shape_v, shape_family, shape_q).
  1436|     """
  1437|     try:
  1438|         family_name = getattr(d, "FamilyName", None)
  1439|         basis = family_name if family_name else type_name
  1440|         bn_l = safe_str(basis).strip().lower()
  1441| 
  1442|         if bn_l.startswith("spot slopes"):
  1443|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
  1444|         elif bn_l.startswith("spot elevations"):
  1445|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
  1446|         elif bn_l.startswith("spot coordinates"):
  1447|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
  1448|     except Exception:
  1449|         pass
  1450|     return (shape_v, shape_family, shape_q)
  1451| 
  1452| 
```
