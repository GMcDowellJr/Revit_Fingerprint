# Chunk of domains/view_templates.py

- Source relative path: `domains/view_templates.py`
- Chunk: 3 of 6
- Original line range: 847-1296
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_ceiling_plans, extract_ceiling_plans._v2_block, _build_elevation_section_detail_viewtype_set
- Source SHA-256: ca478c676990e318341a80d987cc318a4531ef7d17b52cb5fd1b41c67678296d
- Starts inside symbol: no
- Ends inside symbol: no

```
   847| def extract_ceiling_plans(doc, ctx=None):
   848|     DOMAIN_NAME = "view_templates_ceiling_plans"
   849|     DOMAIN_VIEWTYPE_SET = _CEILING_PLAN_VIEWTYPE_SET
   850|     """
   851|     Extract view templates fingerprint - Ceiling Plans only.
   852| 
   853|     Per-template signature: include flags + phase filter hash + filter stack.
   854|     No category-override iteration (VCO domain handles that separately).
   855| 
   856|     Args:
   857|         doc: Revit document
   858|         ctx: context dict with mappings from other domains
   859| 
   860|     Returns:
   861|         Dictionary with count, hash_v2, records, record_rows, and debug counters
   862|     """
   863|     info = {
   864|         "count": 0,
   865|         "raw_count": 0,
   866|         "names": [],
   867|         "records": [],
   868| 
   869|         # debug counters
   870|         "debug_not_template": 0,
   871|         "debug_missing_name": 0,
   872|         "debug_missing_uid": 0,
   873|         "debug_fail_read": 0,
   874|         "debug_kept": 0,
   875|         "debug_view_type_filtered": 0,
   876| 
   877|         # v2 surfaces
   878|         "hash_v2": None,
   879|         "signature_hashes_v2": [],
   880|         "debug_v2_blocked": False,
   881|         "debug_v2_block_reasons": {},
   882|         # PR6: deterministic degraded signaling
   883|         "debug_view_context_problem": 0,
   884|         "debug_view_context_reasons": {},
   885|         "debug_collect_types_failed": 0,
   886|     }
   887| 
   888|     ctx_map = ctx or {}
   889| 
   890|     try:
   891|         require_domain(ctx_map.get("_domains", {}), "phase_filters")
   892|         require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
   893|     except Blocked as b:
   894|         info["debug_v2_blocked"] = True
   895|         info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
   896|         info["count"] = 0
   897|         info["records"] = []
   898|         info["hash_v2"] = None
   899|         return info
   900| 
   901|     phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
   902|     phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
   903|     view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})
   904| 
   905|     try:
   906|         col = list(
   907|             collect_instances(
   908|                 doc,
   909|                 of_class=View,
   910|                 require_unique_id=True,
   911|                 cctx=(ctx or {}).get("_collect") if ctx is not None else None,
   912|                 cache_key=_VIEW_INSTANCES_CACHE_KEY,
   913|             )
   914|         )
   915|     except Exception as e:
   916|         info["debug_collect_types_failed"] += 1
   917|         info["_domain_status"] = "degraded"
   918|         info["_domain_diag"] = {
   919|             "degraded_reasons": ["collect_types_failed"],
   920|             "degraded_reason_counts": {"collect_types_failed": 1},
   921|             "error": str(e),
   922|         }
   923|         return info
   924| 
   925|     info["raw_count"] = len(col)
   926| 
   927|     names = []
   928|     records = []
   929|     per_hashes = []
   930|     per_hashes_v2 = []
   931|     v2_any_blocked = False
   932| 
   933|     def _v2_block(reason):
   934|         nonlocal v2_any_blocked
   935|         v2_any_blocked = True
   936|         info["debug_v2_blocked"] += 1
   937|         try:
   938|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
   939|         except Exception:
   940|             pass
   941| 
   942|     for v in col:
   943|         try:
   944|             is_template = v.IsTemplate
   945|         except Exception:
   946|             is_template = False
   947| 
   948|         if not is_template:
   949|             info["debug_not_template"] += 1
   950|             continue
   951| 
   952|         # Integer ViewType filter (CPython3 returns int string from enum)
   953|         try:
   954|             vt_int = int(v.ViewType)
   955|         except Exception:
   956|             vt_int = None
   957|         if vt_int not in DOMAIN_VIEWTYPE_SET:
   958|             info["debug_view_type_filtered"] += 1
   959|             continue
   960| 
   961|         name = canon_str(getattr(v, "Name", None))
   962|         if not name:
   963|             info["debug_missing_name"] += 1
   964|             name = S_MISSING
   965|         names.append(name)
   966| 
   967|         uid = None
   968|         try:
   969|             uid = canon_str(getattr(v, "UniqueId", None))
   970|         except Exception:
   971|             uid = None
   972| 
   973|         if not uid:
   974|             info["debug_missing_uid"] += 1
   975| 
   976|         # PR6: view-scoped context snapshot
   977|         try:
   978|             dv = (ctx or {}).get("_doc_view") if ctx is not None else None
   979|             if dv is not None:
   980|                 vi = dv.view_info(v, source="HOST")
   981|                 if vi.reasons:
   982|                     info["debug_view_context_problem"] += 1
   983|                     for r in vi.reasons:
   984|                         info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
   985|         except Exception:
   986|             info["debug_view_context_problem"] += 1
   987|             info["debug_view_context_reasons"]["view_context_unreadable"] = (
   988|                 info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
   989|             )
   990| 
   991|         v2_ok = True
   992|         sig_v2 = []
   993|         sig = []
   994| 
   995|         # Template-controlled parameters ("Include" surface)
   996|         try:
   997|             tpl_ids = v.GetTemplateParameterIds() or []
   998|             tpl_bips = set(
   999|                 pid.IntegerValue for pid in tpl_ids
  1000|                 if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
  1001|             )
  1002|         except Exception:
  1003|             tpl_ids = []
  1004|             tpl_bips = set()
  1005| 
  1006|         non_ctrl_bips = _non_ctrl_bips_from_view(v)
  1007|         info["debug_non_ctrl_bips_count"] = len(non_ctrl_bips)
  1008| 
  1009|         # Common include flags
  1010|         try:
  1011|             sig.append("include_phase_filter={}".format(_is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")))
  1012|         except Exception:
  1013|             sig.append("include_phase_filter=False")
  1014| 
  1015|         try:
  1016|             sig.append("include_filters={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_FILTERS")))
  1017|         except Exception:
  1018|             sig.append("include_filters=False")
  1019| 
  1020|         try:
  1021|             sig.append("include_appearance={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_APPEARANCE")))
  1022|         except Exception:
  1023|             sig.append("include_appearance=False")
  1024| 
  1025|         # Domain-specific: view range (ceiling plans support view depth)
  1026|         try:
  1027|             include_view_range = (
  1028|                 int(BuiltInParameter.VIEWER_VOLUME_OF_INTEREST_CROP) not in non_ctrl_bips
  1029|             )
  1030|             sig.append("include_view_range={}".format(include_view_range))
  1031|             if v2_ok:
  1032|                 sig_v2.append("include_view_range={}".format(include_view_range))
  1033|         except Exception:
  1034|             sig.append("include_view_range=False")
  1035|             if v2_ok:
  1036|                 sig_v2.append("include_view_range=False")
  1037| 
  1038|         # Phase Filter (resolved via phase_filters domain)
  1039|         try:
  1040|             include_pf = _is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")
  1041|         except Exception:
  1042|             include_pf = False
  1043| 
  1044|         v2_ok = _append_phase_filter_value(
  1045|             v=v,
  1046|             doc=doc,
  1047|             include_pf=include_pf,
  1048|             phase_filter_map=phase_filter_map,
  1049|             phase_filter_map_v2=phase_filter_map_v2,
  1050|             sig=sig,
  1051|             sig_v2=sig_v2,
  1052|             v2_ok=v2_ok,
  1053|             v2_block_fn=_v2_block,
  1054|             debug_counters=info,
  1055|         )
  1056| 
  1057|         # Filter stack (order-sensitive)
  1058|         v2_ok = _append_filter_stack_signature(v, doc, view_filter_map, sig, sig_v2, v2_ok, _v2_block)
  1059|         v2_ok = _append_workset_visibility(v, doc, sig, sig_v2, v2_ok, _v2_block)
  1060| 
  1061|         # Built-in visual/behavioural parameters
  1062|         emit_builtin_params(v, DOMAIN_NAME, tpl_bips, non_ctrl_bips, sig, sig_v2,
  1063|                             debug_counters=info)
  1064| 
  1065|         # Shared/project parameters (stub — no-op until GUIDs confirmed)
  1066|         emit_shared_params_stub(v, DOMAIN_NAME, tpl_ids, sig, sig_v2,
  1067|                                 debug_counters=info)
  1068| 
  1069|         # Finalize signature (deterministic)
  1070|         sig_final = sorted(sig)
  1071|         def_hash = make_hash(sig_final)
  1072| 
  1073|         # v2 finalize
  1074|         if v2_ok:
  1075|             try:
  1076|                 sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
  1077|                 sig_v2_final = sorted(set(sig_v2))
  1078|                 def_hash_v2 = make_hash(sig_v2_final)
  1079|                 per_hashes_v2.append(def_hash_v2)
  1080|             except Exception:
  1081|                 _v2_block("template_finalize_failed")
  1082|                 v2_ok = False
  1083| 
  1084|         # record.v2 + Phase-2
  1085|         identity_items = _canonical_identity_items_from_signature(def_hash, sig_final)
  1086|         semantic_keys = _semantic_keys_from_identity_items(identity_items)
  1087|         semantic_items = [it for it in identity_items if it.get("k") in set(semantic_keys)]
  1088|         sig_hash = make_hash(serialize_identity_items(semantic_items))
  1089| 
  1090|         rid_info = make_record_id_from_element(v)
  1091|         if rid_info:
  1092|             record_id, record_id_alg = rid_info
  1093|         else:
  1094|             record_id = "eid:{}".format(safe_str(getattr(getattr(v, "Id", None), "IntegerValue", "")))
  1095|             record_id_alg = "revit_elementid_v1"
  1096| 
  1097|         status = STATUS_OK
  1098|         status_reasons = []
  1099|         for it in identity_items:
  1100|             if it.get("q") != ITEM_Q_OK:
  1101|                 status = STATUS_DEGRADED
  1102|                 status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))
  1103|         if not v2_ok:
  1104|             status = STATUS_BLOCKED
  1105|             status_reasons.append("semantic_v2_unresolved_dependency")
  1106|             sig_hash = None
  1107| 
  1108|         vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING
  1109| 
  1110|         rec = build_record_v2(
  1111|             domain=DOMAIN_NAME,
  1112|             record_id=record_id,
  1113|             record_id_alg=record_id_alg,
  1114|             status=status,
  1115|             status_reasons=sorted(set(status_reasons)),
  1116|             sig_hash=sig_hash,
  1117|             identity_items=identity_items,
  1118|             required_qs=tuple(it.get("q") for it in identity_items),
  1119|             label={
  1120|                 "display": safe_str(name),
  1121|                 "quality": "human" if safe_str(name) and safe_str(name) != S_MISSING else "placeholder_missing",
  1122|                 "provenance": "revit.ViewName",
  1123|                 "components": {
  1124|                     "view_type": vt_raw_str,
  1125|                 },
  1126|             },
  1127|         )
  1128|         _ip, _ip_q = purge_lookup(getattr(getattr(v, "Id", None), "IntegerValue", None), ctx)
  1129|         rec["is_purgeable"] = _ip
  1130|         rec["is_purgeable_q"] = _ip_q
  1131| 
  1132|         rec["phase2"] = {
  1133|             "schema": "phase2.{}.v2".format(DOMAIN_NAME),
  1134|             "grouping_basis": "join_key.join_hash",
  1135|             "cosmetic_items": [],
  1136|             "coordination_items": [
  1137|                 make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
  1138|                 make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
  1139|             ],
  1140|             "unknown_items": _traceability_unknown_items(v),
  1141|         }
  1142|         _append_assigned_view_count_cosmetic_item(rec, doc, v, ctx)
  1143| 
  1144|         rec["sig_basis"] = {
  1145|             "hash_alg": "md5_utf8_join_pipe",
  1146|             "keys_used": semantic_keys,
  1147|         }
  1148| 
  1149|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  1150|         vt_join_key, _vt_missing = build_join_key_from_policy(
  1151|             domain_policy=pol,
  1152|             identity_items=identity_items,
  1153|             include_optional_items=False,
  1154|             emit_keys_used=True,
  1155|             hash_optional_items=False,
  1156|             emit_items=False,
  1157|             emit_selectors=True,
  1158|         )
  1159|         rec["join_key"] = vt_join_key
  1160| 
  1161|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
  1162|         # keyed off this record's own label.display-backing item (view_template.name).
  1163|         # view_template.name does not exist in identity_items for any partition --
  1164|         # identity_items are built from _canonical_identity_items_from_signature(def_hash,
  1165|         # sig_final), a structured signature that explicitly strips "name="-prefixed
  1166|         # entries before hashing. Widened items list used only for this call;
  1167|         # identity_basis.items/sig_hash/join_key above are unaffected.
  1168|         vt_name_v, vt_name_q = canonicalize_str(name)
  1169|         name_key_items = identity_items + [
  1170|             make_identity_item("view_template.name", vt_name_v, vt_name_q)
  1171|         ]
  1172|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  1173|         rec["join_key_name_identity"], _vt_name_key_missing = build_join_key_from_policy(
  1174|             domain_policy=name_key_pol,
  1175|             identity_items=name_key_items,
  1176|             include_optional_items=False,
  1177|             emit_keys_used=True,
  1178|             hash_optional_items=False,
  1179|             emit_items=False,
  1180|             emit_selectors=True,
  1181|         )
  1182|         rec["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _vt_name_key_missing)
  1183| 
  1184|         rec["def_hash"] = def_hash
  1185|         rec["def_signature"] = sig_final
  1186| 
  1187|         records.append(rec)
  1188|         per_hashes.append(def_hash)
  1189|         info["debug_kept"] += 1
  1190| 
  1191|     # Finalize
  1192|     info["names"] = sorted(set(names))
  1193|     info["count"] = len(records)
  1194| 
  1195|     info["records"] = sorted(
  1196|         records,
  1197|         key=lambda r: (
  1198|             safe_str(((r.get("label", {}) or {}).get("display", ""))),
  1199|             safe_str(r.get("record_id", "")),
  1200|         ),
  1201|     )
  1202| 
  1203|     info["signature_hashes_v2"] = sorted(per_hashes_v2)
  1204|     if v2_any_blocked:
  1205|         info["hash_v2"] = None
  1206|     else:
  1207|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1208| 
  1209|     info["record_rows"] = []
  1210|     try:
  1211|         recs = info.get("records") or []
  1212|         info["record_rows"] = [{
  1213|             "record_key": safe_str(r.get("record_id", "")),
  1214|             "sig_hash":   safe_str(r.get("sig_hash", "")),
  1215|             "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
  1216|             "view_type":  safe_str(((r.get("label", {}) or {}).get("components", {}) or {}).get("view_type", "")),
  1217|         } for r in recs]
  1218|     except Exception:
  1219|         info["record_rows"] = []
  1220| 
  1221|     # PR6: deterministic degraded signaling
  1222|     degraded_reason_counts = {}
  1223| 
  1224|     try:
  1225|         if int(info.get("debug_missing_uid", 0)) > 0:
  1226|             degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
  1227|     except Exception:
  1228|         pass
  1229| 
  1230|     try:
  1231|         if int(info.get("debug_fail_read", 0)) > 0:
  1232|             degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
  1233|     except Exception:
  1234|         pass
  1235| 
  1236|     try:
  1237|         if int(info.get("debug_view_context_problem", 0)) > 0:
  1238|             for k, vv in dict(info.get("debug_view_context_reasons", {})).items():
  1239|                 key = str(k)
  1240|                 if key.endswith("_not_applicable"):
  1241|                     continue
  1242|                 degraded_reason_counts[key] = int(vv)
  1243|     except Exception:
  1244|         pass
  1245| 
  1246|     try:
  1247|         if int(info.get("debug_v2_blocked", 0)) > 0:
  1248|             degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
  1249|     except Exception:
  1250|         pass
  1251| 
  1252|     if degraded_reason_counts:
  1253|         info["_domain_status"] = "degraded"
  1254|         info["_domain_diag"] = {
  1255|             "degraded_reasons": sorted(degraded_reason_counts.keys()),
  1256|             "degraded_reason_counts": degraded_reason_counts,
  1257|         }
  1258|     else:
  1259|         info["_domain_status"] = "ok"
  1260|         info["_domain_diag"] = {}
  1261| 
  1262|     return info
  1263| 
  1264| def _build_elevation_section_detail_viewtype_set():
  1265|     """
  1266|     Build the ViewType integer set for elevations/sections/detail.
  1267| 
  1268|     Probe-confirmed integers:
  1269|       3 = Elevation (stable across Revit versions)
  1270|       117 = Section in this Revit version (confirmed from corpus templates:
  1271|             Building Sections, Wall Sections, Exterior Details, Interior Details)
  1272| 
  1273|     Note: int(ViewType.Section) resolves to 117 at runtime in this environment.
  1274|     117 was intentionally removed from floor_structural_area_plans (where it was
  1275|     incorrectly routing Section templates). It belongs here in elevations.
  1276| 
  1277|     The runtime resolution path is kept for forward compatibility with Revit
  1278|     versions where Section may have a different integer.
  1279|     """
  1280|     vt_set = {3, 117}  # Elevation=3, Section=117 (probe-confirmed)
  1281|     try:
  1282|         from Autodesk.Revit.DB import ViewType
  1283|         sec = getattr(ViewType, "Section", None)
  1284|         if sec is not None:
  1285|             vt_set.add(int(sec))
  1286|         det = getattr(ViewType, "Detail", None)
  1287|         if det is not None:
  1288|             vt_set.add(int(det))
  1289|     except Exception:
  1290|         pass
  1291|     return frozenset(vt_set)
  1292| 
  1293| 
  1294| _ELEVATION_SECTION_DETAIL_VIEWTYPE_SET = _build_elevation_section_detail_viewtype_set()
  1295| 
  1296| 
```
