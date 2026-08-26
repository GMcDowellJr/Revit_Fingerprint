# Chunk of legacy/fingerprint_mvp.py

- Source relative path: `legacy/fingerprint_mvp.py`
- Chunk: 3 of 3
- Original line range: 884-1402
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: get_linepattern_fingerprint, get_linepattern_fingerprint.fnum, get_texttype_fingerprint, get_dimtype_fingerprint, get_viewtemplate_fingerprint
- Source SHA-256: 2b3c4e30443f4500e886e1f968d5ea1da344bf6c9fb01c0de3bb148cfc1b7332
- Starts inside symbol: no
- Ends inside symbol: no

```
   884| def get_linepattern_fingerprint(doc):
   885|     info = {
   886|         "count": 0,
   887|         "raw_count": 0,
   888|         "names": [],
   889|         "records": [],
   890|         "signature_hashes": [],
   891|         "hash": None,
   892| 
   893|         # debug counters
   894|         "debug_missing_name": 0,
   895|         "debug_fail_getpattern": 0,
   896|         "debug_fail_segment_read": 0,
   897|         "debug_kept": 0,
   898|         
   899|         "debug_getpattern_ex_types": {},
   900|         "debug_getpattern_ex_samples": [],
   901|         "debug_segment_ex_types": {},
   902|         "debug_segment_ex_samples": [],
   903|     }
   904| 
   905|     try:
   906|         col = list(FilteredElementCollector(doc).OfClass(LinePatternElement))
   907|     except:
   908|         return info
   909| 
   910|     info["raw_count"] = len(col)
   911| 
   912|     names = []
   913|     records = []
   914|     per_hashes = []
   915| 
   916|     def fnum(v, nd=9):
   917|         if v is None:
   918|             return "<None>"
   919|         try:
   920|             return format(float(v), ".{}f".format(nd))
   921|         except:
   922|             return sig_val(v)
   923| 
   924|     for e in col:
   925|         # name is metadata only
   926|         name = canon_str(getattr(e, "Name", None))
   927|         if not name:
   928|             info["debug_missing_name"] += 1
   929|             name = "<unnamed>"
   930|         names.append(name)
   931| 
   932|         uid = None
   933|         try:
   934|             uid = canon_str(getattr(e, "UniqueId", None))
   935|         except:
   936|             uid = None
   937| 
   938|         lp = None
   939|         try:
   940|             # Use static overload to avoid pythonnet/IronPython method-binding issues
   941|             lp = LinePatternElement.GetLinePattern(doc, e.Id)
   942|         except Exception as ex:
   943|             info["debug_fail_getpattern"] += 1
   944| 
   945|             t = ex.__class__.__name__
   946|             info["debug_getpattern_ex_types"][t] = info["debug_getpattern_ex_types"].get(t, 0) + 1
   947| 
   948|             if len(info["debug_getpattern_ex_samples"]) < 5:
   949|                 info["debug_getpattern_ex_samples"].append({
   950|                     "name": name,
   951|                     "id": safe_str(e.Id.IntegerValue),
   952|                     "uid": uid,
   953|                     "ex_type": t,
   954|                     "ex_msg": safe_str(str(ex)),
   955|                 })
   956|             lp = None
   957| 
   958|         sig = []
   959| 
   960|         if lp is None:
   961|             # Fail-soft: keep element, but signature will collapse unless we add distinguishing info.
   962|             # We add uid as metadata marker ONLY for the failure case to avoid "all same hash".
   963|             sig.append("error=GetLinePatternFailed")
   964|             sig.append("uid={}".format(sig_val(uid)))
   965|         else:
   966|             segs = None
   967|             try:
   968|                 # Prefer method (often binds better in pythonnet) if present
   969|                 get_segs = getattr(lp, "GetSegments", None)
   970|                 if get_segs:
   971|                     segs = list(get_segs())
   972|                 else:
   973|                     segs = list(getattr(lp, "Segments"))
   974|             except Exception as ex:
   975|                 segs = None
   976|                 info["debug_fail_segment_read"] += 1
   977| 
   978|                 # Optional: capture why segments are unreadable (bounded)
   979|                 t = ex.__class__.__name__
   980|                 info.setdefault("debug_segment_ex_types", {})
   981|                 info["debug_segment_ex_types"][t] = info["debug_segment_ex_types"].get(t, 0) + 1
   982|                 if len(info.setdefault("debug_segment_ex_samples", [])) < 5:
   983|                     info["debug_segment_ex_samples"].append({
   984|                         "name": name,
   985|                         "id": safe_str(e.Id.IntegerValue),
   986|                         "uid": uid,
   987|                         "ex_type": t,
   988|                         "ex_msg": safe_str(str(ex)),
   989|                     })
   990| 
   991|             if segs is None:
   992|                 sig.append("error=SegmentsUnreadable")
   993|                 sig.append("uid={}".format(sig_val(uid)))
   994|             else:
   995|                 # IMPORTANT: do NOT sort; segment order is part of the definition.
   996|                 sig.append("segment_count={}".format(sig_val(len(segs))))
   997|                 for i, s in enumerate(segs):
   998|                     idx = "{:03d}".format(int(i))
   999|                     try:
  1000|                         # Segment type (pythonnet sometimes fails to bind enum properties cleanly)
  1001|                         stype = None
  1002|                         try:
  1003|                             # 1) property
  1004|                             stype = getattr(s, "SegmentType", None)
  1005|                         except:
  1006|                             stype = None
  1007| 
  1008|                         if stype is None:
  1009|                             try:
  1010|                                 # 2) method form (if present)
  1011|                                 m = getattr(s, "GetSegmentType", None)
  1012|                                 if m:
  1013|                                     stype = m()
  1014|                             except:
  1015|                                 stype = None
  1016| 
  1017|                         # Segment type (Revit API: LinePatternSegment.Type)
  1018|                         stype_out = "<None>"
  1019|                         try:
  1020|                             st = s.Type
  1021|                             try:
  1022|                                 stype_out = canon_str(st.ToString()) or "<None>"
  1023|                             except:
  1024|                                 stype_out = safe_str(int(st))
  1025|                         except:
  1026|                             stype_out = "<None>"
  1027|                         
  1028|                         try:
  1029|                             slen = getattr(s, "Length", None)
  1030|                         except:
  1031|                             slen = None
  1032|                         sig.append("seg[{}].type={}".format(idx, sig_val(stype_out)))
  1033|                         sig.append("seg[{}].len={}".format(idx, sig_val(fnum(slen, 9))))
  1034|                     except:
  1035|                         info["debug_fail_segment_read"] += 1
  1036|                         sig.append("seg[{}].error=SegmentReadFailed".format(idx))
  1037| 
  1038|         # Deterministic: keep order (don’t sort), hash the definition signature
  1039|         def_hash = make_hash(sig)
  1040| 
  1041|         rec = {
  1042|             "id": safe_str(e.Id.IntegerValue),
  1043|             "name": name,          # metadata only
  1044|             "uid": uid,            # metadata only
  1045|             "def_hash": def_hash,  # hashed definition (or failure-signature)
  1046|         }
  1047|         if DEBUG_INCLUDE_LINEPATTERN_SIGNATURES:
  1048|             rec["def_signature"] = sig
  1049| 
  1050|         records.append(rec)
  1051|         per_hashes.append(def_hash)
  1052|         info["debug_kept"] += 1
  1053| 
  1054|     info["names"] = sorted(set(names))
  1055|     info["count"] = len(info["names"])
  1056|     info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
  1057|     info["signature_hashes"] = sorted(per_hashes)
  1058|     info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None
  1059| 
  1060|     info["record_rows"] = []
  1061|     try:
  1062|         recs = info.get("records") or []
  1063|         info["record_rows"] = [{
  1064|             "record_key": safe_str(r.get("uid", "")),        # <-- UniqueId
  1065|             "sig_hash":   safe_str(r.get("def_hash", "")),
  1066|             "name":       safe_str(r.get("name", "")),       # optional metadata
  1067|         } for r in recs]
  1068|     except:
  1069|         info["record_rows"] = []
  1070|     
  1071|     return info
  1072| 
  1073| # ------------- text types fingerprint -----------------
  1074| 
  1075| def get_texttype_fingerprint(doc):
  1076|     info = {
  1077|         "count": 0,
  1078|         "names": [],
  1079|         "hash": None,
  1080| 
  1081|         # new
  1082|         "records": [],
  1083|         "signature_hashes": [],
  1084|         "raw_count": 0,
  1085|         "debug_missing_name": 0
  1086|     }
  1087| 
  1088|     types = list(FilteredElementCollector(doc).OfClass(TextNoteType))
  1089|     info["raw_count"] = len(types)
  1090| 
  1091|     names = []
  1092|     missing = 0
  1093|     records = []
  1094|     sig_hashes = []
  1095| 
  1096|     for t in types:
  1097|         type_name = get_type_display_name(t)
  1098|         if type_name:
  1099|             type_name = canon_str(type_name)
  1100|             names.append(type_name)
  1101|         else:
  1102|             missing += 1
  1103|             type_name = "<unnamed>"
  1104| 
  1105|         # --- core fields (same pattern you validated in the TextStyles exercise) ---
  1106|         font = _as_string(first_param(t, bip_names=["TEXT_FONT"], ui_names=["Text Font"]))
  1107|         size_ft = _as_double(first_param(t, bip_names=["TEXT_SIZE"], ui_names=["Text Size"]))
  1108|         size_in = fnum(format_len_inches(size_ft), 6)
  1109|         
  1110|         font = canon_str(font)
  1111| 
  1112|         width_factor = _as_double(first_param(t, bip_names=["TEXT_WIDTH_SCALE"], ui_names=["Width Factor"]))
  1113|         width_factor_n = fnum(width_factor, 6)
  1114| 
  1115|         background_i = _as_int(first_param(t, bip_names=["TEXT_BACKGROUND"], ui_names=["Background"]))
  1116| 
  1117|         # Graphics
  1118|         p_lw = first_param(t, bip_names=["TEXT_LINE_WEIGHT", "LINE_PEN"], ui_names=["Line Weight"])
  1119|         line_weight = _as_int(p_lw)
  1120| 
  1121|         color_int, color_rgb = try_get_color_rgb_from_elem(t)
  1122| 
  1123|         # Border / tabs / styles
  1124|         show_border = _as_bool_from_param(first_param(t, ui_names=["Show Border", "Show border"]))
  1125|         leader_border_offset_ft = _as_double(first_param(t, ui_names=["Leader/Border Offset", "Leader / Border Offset"]))
  1126|         leader_border_offset_in = fnum(format_len_inches(leader_border_offset_ft), 6)
  1127| 
  1128|         tab_size_ft = _as_double(first_param(t, ui_names=["Tab Size", "Tab size"]))
  1129|         tab_size_in = fnum(format_len_inches(tab_size_ft), 6)
  1130| 
  1131|         bold = _as_bool_from_param(first_param(t, ui_names=["Bold"]))
  1132|         italic = _as_bool_from_param(first_param(t, ui_names=["Italic"]))
  1133|         underline = _as_bool_from_param(first_param(t, ui_names=["Underline"]))
  1134| 
  1135|         # Leader Arrowhead (metadata only; do NOT put in core signature)
  1136|         leader_arrow_uid = None
  1137|         leader_arrow_name = None
  1138|         try:
  1139|             p_arrow = first_param(t, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])
  1140|             if p_arrow and p_arrow.HasValue:
  1141|                 ah_eid = p_arrow.AsElementId()
  1142|                 if ah_eid and ah_eid.IntegerValue > 0:
  1143|                     ah = doc.GetElement(ah_eid)
  1144|                     if ah:
  1145|                         leader_arrow_uid = ah.UniqueId
  1146|                         # robust display name
  1147|                         try:
  1148|                             leader_arrow_name = get_element_display_name(ah)
  1149|                         except:
  1150|                             leader_arrow_name = None
  1151|         except:
  1152|             pass
  1153| 
  1154|         # --- signature tuple (core) ---
  1155|         signature_tuple = [
  1156|             "font={}".format(sig_val(font)),
  1157|             "size_in={}".format(sig_val(size_in)),
  1158|             "width_factor={}".format(sig_val(width_factor_n)),
  1159|             "background={}".format(sig_val(background_i)),
  1160|             "line_weight={}".format(sig_val(line_weight)),
  1161|             "color_int={}".format(sig_val(color_int)),
  1162| 
  1163|             "show_border={}".format(sig_val(show_border)),
  1164|             "leader_border_offset_in={}".format(sig_val(leader_border_offset_in)),
  1165|             "tab_size_in={}".format(sig_val(tab_size_in)),
  1166|             "bold={}".format(sig_val(bold)),
  1167|             "italic={}".format(sig_val(italic)),
  1168|             "underline={}".format(sig_val(underline)),
  1169|         ]
  1170|         sig_hash = make_hash(signature_tuple)
  1171| 
  1172|         rec = {
  1173|             "type_id": safe_str(t.Id.IntegerValue),
  1174|             "type_uid": getattr(t, "UniqueId", "") or "",
  1175|             "type_name": type_name,
  1176| 
  1177|             "font": font,
  1178|             "text_size_ft": size_ft,
  1179|             "text_size_in": size_in,
  1180|             "width_factor": width_factor_n,
  1181|             "background_raw": background_i,
  1182|             "line_weight": line_weight,
  1183| 
  1184|             "color_int": color_int,
  1185|             "color_rgb": color_rgb,
  1186| 
  1187|             "show_border": show_border,
  1188|             "leader_border_offset_in": leader_border_offset_in,
  1189|             "tab_size_in": tab_size_in,
  1190|             "bold": bold,
  1191|             "italic": italic,
  1192|             "underline": underline,
  1193| 
  1194|             "leader_arrowhead_uid": leader_arrow_uid,
  1195|             "leader_arrowhead_name": leader_arrow_name,
  1196| 
  1197|             "signature_tuple": signature_tuple,
  1198|             "signature_hash": sig_hash
  1199|         }
  1200| 
  1201|         records.append(rec)
  1202|         sig_hashes.append(sig_hash)
  1203| 
  1204|     info["debug_missing_name"] = missing
  1205| 
  1206|     names_sorted = sorted(set(names))
  1207|     info["count"] = len(names_sorted)
  1208|     info["names"] = names_sorted
  1209| 
  1210|     # new: records + signature-based hash
  1211|     info["records"] = sorted(records, key=lambda r: (r.get("type_name",""), r.get("type_id","")))
  1212|     info["signature_hashes"] = sorted(sig_hashes)
  1213|     info["hash"] = make_hash(sorted(sig_hashes)) if sig_hashes else None
  1214|     
  1215|     info["record_rows"] = []
  1216|     try:
  1217|         recs = info.get("records") or []
  1218|         info["record_rows"] = [{
  1219|             "record_key": safe_str(r.get("type_uid", "")) or safe_str(r.get("uid", "")),
  1220|             "sig_hash":  safe_str(r.get("signature_hash", "")),
  1221|             "name":      safe_str(r.get("type_name", "")),   # optional metadata
  1222|         } for r in recs]
  1223|     except:
  1224|         info["record_rows"] = []
  1225|     
  1226|     return info
  1227| 
  1228| # ------------- dimension types fingerprint -----------------
  1229| 
  1230| def get_dimtype_fingerprint(doc):
  1231|     info = {
  1232|         "count": 0,
  1233|         "names": [],
  1234|         "hash": None,
  1235| 
  1236|         # new
  1237|         "records": [],
  1238|         "signature_hashes": [],
  1239|         "raw_count": 0,
  1240|         "debug_missing_name": 0
  1241|     }
  1242| 
  1243|     types = list(FilteredElementCollector(doc).OfClass(DimensionType))
  1244|     info["raw_count"] = len(types)
  1245| 
  1246|     names = []
  1247|     missing = 0
  1248|     records = []
  1249|     sig_hashes = []
  1250| 
  1251|     for d in types:
  1252|         type_name = get_type_display_name(d)
  1253|         if type_name:
  1254|             type_name = canon_str(type_name)
  1255|             if type_name:
  1256|                 names.append(type_name)
  1257|             else:
  1258|                 missing += 1
  1259|                 continue
  1260|         else:
  1261|             missing += 1
  1262|             continue
  1263| 
  1264|         # --- minimal dim-style signature (text + graphics + ticks) ---
  1265|         text_font = _as_string(first_param(d, ui_names=["Text Font"]))
  1266|         text_font = canon_str(text_font)
  1267| 
  1268|         text_size_ft = _as_double(first_param(d, ui_names=["Text Size"]))
  1269|         text_size_in = fnum(format_len_inches(text_size_ft), 6)
  1270| 
  1271|         lw = _as_int(first_param(d, ui_names=["Line Weight"]))
  1272|         color_int, color_rgb = try_get_color_rgb_from_elem(d)
  1273| 
  1274|         # Tick Mark (arrowhead) – store UniqueId metadata + include NAME in signature (more stable than ids)
  1275|        
  1276|         tick_name = _as_string(first_param(d, ui_names=["Tick Mark"]))
  1277|         tick_uid = None
  1278|         try:
  1279|             p_tick = first_param(d, ui_names=["Tick Mark"])
  1280|             if p_tick and p_tick.HasValue:
  1281|                 tid = p_tick.AsElementId()
  1282|                 if tid and tid.IntegerValue > 0:
  1283|                     te = doc.GetElement(tid)
  1284|                     if te:
  1285|                         tick_uid = te.UniqueId
  1286|                         # prefer element.Name where available
  1287|                         try:
  1288|                             tick_name = tick_name or get_element_display_name(te)
  1289|                             if tick_name is not None:
  1290|                                 tick_name = canon_str(tick_name)
  1291|                         except:
  1292|                             pass
  1293|         except:
  1294|             pass
  1295| 
  1296|         # Witness line control is common; keep as metadata + optional signature
  1297|         witness = _as_string(first_param(d, ui_names=["Witness Line Control"]))
  1298|         witness = canon_str(witness)
  1299| 
  1300|         tick_name = canon_str(tick_name)
  1301| 
  1302|         signature_tuple = [
  1303|             "text_font={}".format(sig_val(text_font)),
  1304|             "text_size_in={}".format(sig_val(text_size_in)),
  1305|             "line_weight={}".format(sig_val(lw)),
  1306|             "color_int={}".format(sig_val(color_int)),
  1307|             "tick_mark={}".format(sig_val(tick_name)),
  1308|             "witness_ctrl={}".format(sig_val(witness)),
  1309|         ]
  1310| 
  1311|         sig_hash = make_hash(signature_tuple)
  1312| 
  1313|         rec = {
  1314|             "type_id": safe_str(d.Id.IntegerValue),
  1315|             "type_uid": getattr(d, "UniqueId", "") or "",
  1316|             "type_name": type_name,
  1317| 
  1318|             "text_font": text_font,
  1319|             "text_size_ft": text_size_ft,
  1320|             "text_size_in": text_size_in,
  1321| 
  1322|             "line_weight": lw,
  1323|             "color_int": color_int,
  1324|             "color_rgb": color_rgb,
  1325| 
  1326|             "tick_mark_name": tick_name,
  1327|             "tick_mark_uid": tick_uid,
  1328|             "witness_line_control": witness,
  1329| 
  1330|             "signature_tuple": signature_tuple,
  1331|             "signature_hash": sig_hash
  1332|         }
  1333| 
  1334|         records.append(rec)
  1335|         sig_hashes.append(sig_hash)
  1336| 
  1337|     info["debug_missing_name"] = missing
  1338| 
  1339|     names_sorted = sorted(set(names))
  1340|     info["count"] = len(names_sorted)
  1341|     info["names"] = names_sorted
  1342| 
  1343|     info["records"] = sorted(records, key=lambda r: (r.get("type_name",""), r.get("type_id","")))
  1344|     info["signature_hashes"] = sorted(sig_hashes)
  1345|     info["hash"] = make_hash(sorted(sig_hashes)) if sig_hashes else None
  1346| 
  1347|     info["record_rows"] = []
  1348|     try:
  1349|         recs = info.get("records") or []
  1350|         info["record_rows"] = [{
  1351|             "record_key": safe_str(r.get("type_uid", "")),
  1352|             "sig_hash":  safe_str(r.get("signature_hash", "")),
  1353|             "name":      safe_str(r.get("type_name", "")),   # optional metadata
  1354|         } for r in recs]
  1355|     except:
  1356|         info["record_rows"] = []
  1357|         
  1358|     return info
  1359| 
  1360| # ------------- view templates fingerprint -----------------
  1361| 
  1362| def get_viewtemplate_fingerprint(doc):
  1363|     info = {
  1364|         "count": 0,
  1365|         "names": [],
  1366|         "hash": None
  1367|     }
  1368| 
  1369|     try:
  1370|         col = FilteredElementCollector(doc).OfClass(View)
  1371|         names = []
  1372|         for v in col:
  1373|             try:
  1374|                 if v.IsTemplate:
  1375|                     names.append(canon_str(v.Name))
  1376|             except:
  1377|                 continue
  1378|         names_sorted = sorted(set(names))
  1379|         info["count"] = len(names_sorted)
  1380|         info["names"] = names_sorted
  1381|         info["hash"]  = make_hash(names_sorted)
  1382|     except:
  1383|         pass
  1384| 
  1385|     return info
  1386| 
  1387| # ------------- main -----------------
  1388| 
  1389| doc = get_doc()
  1390| 
  1391| fingerprint = {}
  1392| fingerprint["identity"]        = get_identity_fingerprint(doc)
  1393| fingerprint["units"]           = get_units_fingerprint(doc)
  1394| fingerprint["objectstyles"] = get_objectstyles_fingerprint(doc)
  1395| fingerprint["line_patterns"]   = get_linepattern_fingerprint(doc)
  1396| fingerprint["text_types"]      = get_texttype_fingerprint(doc)
  1397| fingerprint["dimension_types"] = get_dimtype_fingerprint(doc)
  1398| fingerprint["view_templates"]  = get_viewtemplate_fingerprint(doc)
  1399| fingerprint["fill_patterns"] = get_fillpattern_fingerprint(doc)
  1400| fingerprint["line_styles"] = get_linestyles_fingerprint(doc)
  1401| 
  1402| OUT = json.dumps(fingerprint, indent=2, sort_keys=True)
```
