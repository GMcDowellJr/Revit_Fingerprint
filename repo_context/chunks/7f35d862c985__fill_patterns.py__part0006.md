# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 6 of 8
- Original line range: 1040-1439
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_model, extract_model.f, extract_model.grid_sig, extract_model.grid_sig.add_float, extract_model.grid_sig.add_origin_2d, extract_model._bump_v2_reason, extract_model._grid_sig_v2, extract_model._grid_sig_v2.req_float, extract_model._grid_sig_v2.req_origin, extract_model._phase2_try_get_grid, extract_model._phase2_add_float, extract_model._phase2_add_int, extract_model._phase2_add_bool, extract_model._phase2_add_str, extract_model._phase2_build_phase2
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: no
- Ends inside symbol: extract_model

```
  1040| def extract_model(doc, ctx=None):
  1041|     _TARGET_INT = _TARGET_MODEL_INT
  1042|     _TARGET_NAME = "Model"
  1043|     DOMAIN_NAME = "fill_patterns_model"
  1044|     """
  1045|     Extract Fill Patterns fingerprint from document.
  1046| 
  1047|     Args:
  1048|         doc: Revit Document
  1049|         ctx: Context dictionary (unused for this domain)
  1050| 
  1051|     Returns:
  1052|         Dictionary with count, hash, signature_hashes, records,
  1053|         record_rows, and debug counters
  1054|     """
  1055|     info = {
  1056|         "count": 0,
  1057|         "raw_count": 0,
  1058|         "names": [],
  1059|                 "records": [],
  1060| 
  1061|         # debug counters so you can see why things disappear
  1062|         "debug_total_elements": 0,
  1063|         "debug_kept": 0,
  1064|         "debug_skipped_no_name": 0,
  1065|         "debug_skipped_wrong_target": 0,
  1066|         "debug_fail_getfillpattern": 0,
  1067|         "debug_fail_grid_read": 0,
  1068| 
  1069|         # v2 (contract semantic) surfaces - additive only
  1070|         "hash_v2": None,
  1071|         "signature_hashes_v2": [],
  1072|         "debug_v2_blocked": 0,
  1073|         "debug_v2_block_reasons": {},
  1074|     }
  1075| 
  1076|     try:
  1077|         col = _collect_fill_patterns(doc, ctx)
  1078|     except Exception as e:
  1079|         return info
  1080|     info["raw_count"] = len(col)
  1081| 
  1082|     def f(v, nd=9):
  1083|         if v is None:
  1084|             return S_MISSING
  1085|         try:
  1086|             return format(float(v), ".{}f".format(nd))
  1087|         except Exception as e:
  1088|             return canon_str(v)
  1089| 
  1090|     def grid_sig(fp, i):
  1091|         # Return a stable list; never raise
  1092|         idx = "{:03d}".format(int(i))
  1093|         g = None
  1094|         try:
  1095|             if hasattr(fp, "GetFillPatternGrid"):
  1096|                 g = fp.GetFillPatternGrid(i)
  1097|         except Exception as e:
  1098|             g = None
  1099|         if g is None:
  1100|             try:
  1101|                 if hasattr(fp, "GetFillGrid"):
  1102|                     g = fp.GetFillGrid(i)
  1103|             except Exception as e:
  1104|                 g = None
  1105| 
  1106|         if g is None:
  1107|             info["debug_fail_grid_read"] += 1
  1108|             return ["grid[{}].unreadable={}".format(idx, S_MISSING)]
  1109| 
  1110|         parts = []
  1111| 
  1112|         def add_float(prop_name, key):
  1113|             try:
  1114|                 v = getattr(g, prop_name)
  1115|                 parts.append("grid[{}].{}={}".format(idx, key, f(v)))
  1116|             except Exception as e:
  1117|                 parts.append("grid[{}].{}={}".format(idx, key, S_MISSING))
  1118| 
  1119|         # origin can vary across versions; try a couple shapes
  1120|         def add_origin_2d():
  1121|             # Try UV-style origin (U,V)
  1122|             try:
  1123|                 o = g.Origin
  1124|                 u = getattr(o, "U", None)
  1125|                 v = getattr(o, "V", None)
  1126|                 if u is not None and v is not None:
  1127|                     parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
  1128|                     return
  1129|             except Exception as e:
  1130|                 pass
  1131| 
  1132|             # Try XYZ-style origin but store only X,Y
  1133|             try:
  1134|                 o = g.Origin
  1135|                 x = getattr(o, "X", None)
  1136|                 y = getattr(o, "Y", None)
  1137|                 if x is not None and y is not None:
  1138|                     parts.append("grid[{}].origin_xy={},{}".format(idx, f(x), f(y)))
  1139|                     return
  1140|             except Exception as e:
  1141|                 pass
  1142| 
  1143|             # Try separate scalars
  1144|             for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
  1145|                 try:
  1146|                     u = getattr(g, u_name)
  1147|                     v = getattr(g, v_name)
  1148|                     parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
  1149|                     return
  1150|                 except Exception as e:
  1151|                     pass
  1152| 
  1153|             parts.append("grid[{}].origin={}".format(idx, S_MISSING))
  1154| 
  1155|         add_float("Angle", "angle")
  1156|         add_origin_2d()
  1157|         add_float("Offset", "offset")
  1158|         add_float("Shift", "shift")
  1159| 
  1160|         return parts
  1161| 
  1162|     # v2 helpers (strict: block on unreadables / missing)
  1163|     def _bump_v2_reason(reason):
  1164|         info["debug_v2_blocked"] += 1
  1165|         try:
  1166|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
  1167|         except Exception as e:
  1168|             pass
  1169| 
  1170|     def _grid_sig_v2(fp, i):
  1171|         """
  1172|         Return (ok, parts, reason). parts contain only numeric primitives.
  1173|         """
  1174|         idx = "{:03d}".format(int(i))
  1175|         g = None
  1176|         try:
  1177|             if hasattr(fp, "GetFillPatternGrid"):
  1178|                 g = fp.GetFillPatternGrid(i)
  1179|         except Exception as e:
  1180|             g = None
  1181|         if g is None:
  1182|             try:
  1183|                 if hasattr(fp, "GetFillGrid"):
  1184|                     g = fp.GetFillGrid(i)
  1185|             except Exception as e:
  1186|                 g = None
  1187| 
  1188|         if g is None:
  1189|             return False, [], "grid_unreadable"
  1190| 
  1191|         parts = []
  1192| 
  1193|         def req_float(prop_name, key):
  1194|             try:
  1195|                 v = getattr(g, prop_name)
  1196|             except Exception as e:
  1197|                 return False, "grid_{}_unreadable".format(key)
  1198|             if v is None:
  1199|                 return False, "grid_{}_none".format(key)
  1200|             try:
  1201|                 fv = float(v)
  1202|             except Exception as e:
  1203|                 return False, "grid_{}_not_float".format(key)
  1204|             parts.append("grid[{}].{}={}".format(idx, key, canon_str(f(v, 9))))
  1205|             return True, None
  1206| 
  1207|         # origin: require 2 floats, pick first supported shape
  1208|         def req_origin():
  1209|             # UV origin
  1210|             try:
  1211|                 o = g.Origin
  1212|                 u = getattr(o, "U", None)
  1213|                 v = getattr(o, "V", None)
  1214|                 if u is not None and v is not None:
  1215|                     fu = float(u)
  1216|                     fv = float(v)
  1217|                     parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
  1218|                     return True, None
  1219|             except Exception as e:
  1220|                 pass
  1221| 
  1222|             # XY origin
  1223|             try:
  1224|                 o = g.Origin
  1225|                 x = getattr(o, "X", None)
  1226|                 y = getattr(o, "Y", None)
  1227|                 if x is not None and y is not None:
  1228|                     fx = float(x)
  1229|                     fy = float(y)
  1230|                     parts.append("grid[{}].origin_xy={},{}".format(idx, canon_str(f(fx, 9)), canon_str(f(fy, 9))))
  1231|                     return True, None
  1232|             except Exception as e:
  1233|                 pass
  1234| 
  1235|             # scalar origin props
  1236|             for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
  1237|                 try:
  1238|                     u = getattr(g, u_name)
  1239|                     v = getattr(g, v_name)
  1240|                     if u is None or v is None:
  1241|                         continue
  1242|                     fu = float(u)
  1243|                     fv = float(v)
  1244|                     parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
  1245|                     return True, None
  1246|                 except Exception as e:
  1247|                     continue
  1248| 
  1249|             return False, "grid_origin_unreadable"
  1250| 
  1251|         ok, reason = req_float("Angle", "angle")
  1252|         if not ok:
  1253|             return False, [], reason
  1254|         ok, reason = req_origin()
  1255|         if not ok:
  1256|             return False, [], reason
  1257|         ok, reason = req_float("Offset", "offset")
  1258|         if not ok:
  1259|             return False, [], reason
  1260|         ok, reason = req_float("Shift", "shift")
  1261|         if not ok:
  1262|             return False, [], reason
  1263| 
  1264|         return True, parts, None
  1265| 
  1266|     # -------------------------
  1267|     # Phase 2 (additive-only) builders
  1268|     # -------------------------
  1269| 
  1270|     def _phase2_try_get_grid(fp, i):
  1271|         g = None
  1272|         try:
  1273|             if hasattr(fp, "GetFillPatternGrid"):
  1274|                 g = fp.GetFillPatternGrid(i)
  1275|         except Exception:
  1276|             g = None
  1277|         if g is None:
  1278|             try:
  1279|                 if hasattr(fp, "GetFillGrid"):
  1280|                     g = fp.GetFillGrid(i)
  1281|             except Exception:
  1282|                 g = None
  1283|         return g
  1284| 
  1285|     def _phase2_add_float(items, k, v, *, unreadable=False):
  1286|         if unreadable:
  1287|             items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
  1288|             return
  1289|         v2, q2 = canonicalize_float(v)
  1290|         items.append({"k": k, "v": v2, "q": q2})
  1291| 
  1292|     def _phase2_add_int(items, k, v, *, unreadable=False):
  1293|         if unreadable:
  1294|             items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
  1295|             return
  1296|         v2, q2 = canonicalize_int(v)
  1297|         items.append({"k": k, "v": v2, "q": q2})
  1298| 
  1299|     def _phase2_add_bool(items, k, v, *, unreadable=False):
  1300|         if unreadable:
  1301|             items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
  1302|             return
  1303|         v2, q2 = canonicalize_bool(v)
  1304|         items.append({"k": k, "v": v2, "q": q2})
  1305| 
  1306|     def _phase2_add_str(items, k, v, *, allow_empty=False):
  1307|         if allow_empty:
  1308|             v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=True)
  1309|         else:
  1310|             v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=False)
  1311|         items.append({"k": k, "v": v2, "q": q2})
  1312| 
  1313|     def _phase2_build_phase2(name, uid, elem_id_str, fp, elem):
  1314|         semantic = []
  1315|         cosmetic = []
  1316|         coordination = []
  1317|         unknown = []
  1318| 
  1319|         # cosmetic
  1320|         v_name, q_name = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
  1321|         cosmetic.append({"k": "fill_pattern.name", "v": v_name, "q": q_name})
  1322| 
  1323|         # unknown identifiers (do not affect semantic hypotheses)
  1324|         v_uid, q_uid = canonicalize_str(uid)
  1325|         unknown.append({"k": "fill_pattern.uid", "v": v_uid, "q": q_uid})
  1326|         v_eid, q_eid = canonicalize_str(elem_id_str)
  1327|         unknown.append({"k": "fill_pattern.elem_id", "v": v_eid, "q": q_eid})
  1328| 
  1329|         # Traceability fields (metadata only — never in hash/sig/join)
  1330|         try:
  1331|             _eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
  1332|             _eid_v, _eid_q = canonicalize_int(_eid_raw)
  1333|         except Exception:
  1334|             _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  1335|         try:
  1336|             _uid_raw = getattr(elem, "UniqueId", None)
  1337|             _uid_v, _uid_q = canonicalize_str(_uid_raw)
  1338|         except Exception:
  1339|             _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  1340|         unknown.append({"k": "fill_pattern.source_element_id", "v": _eid_v, "q": _eid_q})
  1341|         unknown.append({"k": "fill_pattern.source_unique_id", "v": _uid_v, "q": _uid_q})
  1342| 
  1343|         # target is always _TARGET_NAME for this domain
  1344|         semantic.append({"k": "fill_pattern.target", "v": _TARGET_NAME, "q": ITEM_Q_OK})
  1345| 
  1346|         if fp is None:
  1347|             # Explicit unreadable (GetFillPattern failed)
  1348|             _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
  1349|             # is_solid in coordination only (filter criterion, not identity)
  1350|             _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
  1351|             coordination.append(make_identity_item("fill_pattern.is_import", None, ITEM_Q_UNREADABLE))
  1352|         else:
  1353|             # is_solid goes to coordination_items only — it is a filter criterion, not identity
  1354|             try:
  1355|                 is_solid = fp.IsSolidFill
  1356|             except Exception:
  1357|                 _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
  1358|             else:
  1359|                 _phase2_add_bool(coordination, "fill_pattern.is_solid", bool(is_solid))
  1360| 
  1361|             is_import_v, is_import_q = _phase2_fill_pattern_is_import(elem, name)
  1362|             coordination.append(make_identity_item("fill_pattern.is_import", is_import_v, is_import_q))
  1363| 
  1364|             # grid_count
  1365|             try:
  1366|                 gc = fp.GridCount
  1367|             except Exception:
  1368|                 _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
  1369|                 gc_i = None
  1370|             else:
  1371|                 if gc is None:
  1372|                     _phase2_add_int(semantic, "fill_pattern.grid_count", None)
  1373|                     gc_i = None
  1374|                 else:
  1375|                     try:
  1376|                         gc_i = int(gc)
  1377|                     except Exception:
  1378|                         _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
  1379|                         gc_i = None
  1380|                     else:
  1381|                         _phase2_add_int(semantic, "fill_pattern.grid_count", gc_i)
  1382| 
  1383|             # grids (no inference; explicit kind for origin)
  1384|             if gc_i:
  1385|                 for i in range(int(gc_i)):
  1386|                     idx = "{:03d}".format(int(i))
  1387|                     g = _phase2_try_get_grid(fp, i)
  1388|                     if g is None:
  1389|                         semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1390|                         semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1391|                         semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1392|                         semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1393|                         continue
  1394| 
  1395|                     # Angle / Offset / Shift
  1396|                     try:
  1397|                         _phase2_add_float(semantic, "fill_pattern.grid[{}].angle".format(idx), float(getattr(g, "Angle")))
  1398|                     except Exception:
  1399|                         semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
  1400| 
  1401|                     # Origin (explicit kind)
  1402|                     origin_kind = None
  1403|                     ox = oy = None
  1404| 
  1405|                     # UV origin
  1406|                     try:
  1407|                         o = g.Origin
  1408|                         u = getattr(o, "U", None)
  1409|                         v = getattr(o, "V", None)
  1410|                         if u is not None and v is not None:
  1411|                             origin_kind = "uv"
  1412|                             ox = float(u)
  1413|                             oy = float(v)
  1414|                     except Exception:
  1415|                         pass
  1416| 
  1417|                     # XY origin
  1418|                     if origin_kind is None:
  1419|                         try:
  1420|                             o = g.Origin
  1421|                             x = getattr(o, "X", None)
  1422|                             y = getattr(o, "Y", None)
  1423|                             if x is not None and y is not None:
  1424|                                 origin_kind = "xy"
  1425|                                 ox = float(x)
  1426|                                 oy = float(y)
  1427|                         except Exception:
  1428|                             pass
  1429| 
  1430|                     # Scalar origin props
  1431|                     if origin_kind is None:
  1432|                         for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
  1433|                             try:
  1434|                                 u2 = getattr(g, u_name)
  1435|                                 v2 = getattr(g, v_name)
  1436|                                 if u2 is None or v2 is None:
  1437|                                     continue
  1438|                                 origin_kind = "uv"
  1439|                                 ox = float(u2)
```
