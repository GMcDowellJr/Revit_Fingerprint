# Chunk of domains/view_templates.py

- Source relative path: `domains/view_templates.py`
- Chunk: 2 of 6
- Original line range: 422-846
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_floor_structural_area_plans, extract_floor_structural_area_plans._v2_block
- Source SHA-256: ca478c676990e318341a80d987cc318a4531ef7d17b52cb5fd1b41c67678296d
- Starts inside symbol: no
- Ends inside symbol: no

```
   422| def extract_floor_structural_area_plans(doc, ctx=None):
   423|     DOMAIN_NAME = "view_templates_floor_structural_area_plans"
   424|     DOMAIN_VIEWTYPE_SET = _FLOOR_STRUCTURAL_AREA_VIEWTYPE_SET
   425|     """
   426|     Extract view templates fingerprint - Floor Plans and Area Plans only.
   427| 
   428|     Per-template signature: include flags + phase filter hash + filter stack.
   429|     No category-override iteration (VCO domain handles that separately).
   430| 
   431|     Args:
   432|         doc: Revit document
   433|         ctx: context dict with mappings from other domains
   434| 
   435|     Returns:
   436|         Dictionary with count, hash_v2, records, record_rows, and debug counters
   437|     """
   438|     info = {
   439|         "count": 0,
   440|         "raw_count": 0,
   441|         "names": [],
   442|         "records": [],
   443| 
   444|         # debug counters
   445|         "debug_not_template": 0,
   446|         "debug_missing_name": 0,
   447|         "debug_missing_uid": 0,
   448|         "debug_fail_read": 0,
   449|         "debug_kept": 0,
   450|         "debug_view_type_filtered": 0,
   451| 
   452|         # v2 surfaces
   453|         "hash_v2": None,
   454|         "signature_hashes_v2": [],
   455|         "debug_v2_blocked": False,
   456|         "debug_v2_block_reasons": {},
   457|         # PR6: deterministic degraded signaling
   458|         "debug_view_context_problem": 0,
   459|         "debug_view_context_reasons": {},
   460|         "debug_collect_types_failed": 0,
   461|     }
   462| 
   463|     ctx_map = ctx or {}
   464| 
   465|     try:
   466|         require_domain(ctx_map.get("_domains", {}), "phase_filters")
   467|         require_domain(ctx_map.get("_domains", {}), "view_filter_definitions")
   468|     except Blocked as b:
   469|         info["debug_v2_blocked"] = True
   470|         info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
   471|         info["count"] = 0
   472|         info["records"] = []
   473|         info["hash_v2"] = None
   474|         return info
   475| 
   476|     phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
   477|     phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
   478|     view_filter_map = ctx_map.get("view_filter_uid_to_sig_hash_v2", {})
   479| 
   480|     try:
   481|         col = list(
   482|             collect_instances(
   483|                 doc,
   484|                 of_class=View,
   485|                 require_unique_id=True,
   486|                 cctx=(ctx or {}).get("_collect") if ctx is not None else None,
   487|                 cache_key=_VIEW_INSTANCES_CACHE_KEY,
   488|             )
   489|         )
   490|     except Exception as e:
   491|         info["debug_collect_types_failed"] += 1
   492|         info["_domain_status"] = "degraded"
   493|         info["_domain_diag"] = {
   494|             "degraded_reasons": ["collect_types_failed"],
   495|             "degraded_reason_counts": {"collect_types_failed": 1},
   496|             "error": str(e),
   497|         }
   498|         return info
   499| 
   500|     info["raw_count"] = len(col)
   501| 
   502|     names = []
   503|     records = []
   504|     per_hashes = []
   505|     per_hashes_v2 = []
   506|     v2_any_blocked = False
   507| 
   508|     def _v2_block(reason):
   509|         nonlocal v2_any_blocked
   510|         v2_any_blocked = True
   511|         info["debug_v2_blocked"] += 1
   512|         try:
   513|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
   514|         except Exception:
   515|             pass
   516| 
   517|     for v in col:
   518|         try:
   519|             is_template = v.IsTemplate
   520|         except Exception:
   521|             is_template = False
   522| 
   523|         if not is_template:
   524|             info["debug_not_template"] += 1
   525|             continue
   526| 
   527|         # Integer ViewType filter (CPython3 returns int string from enum)
   528|         try:
   529|             vt_int = int(v.ViewType)
   530|         except Exception:
   531|             vt_int = None
   532|         if vt_int not in DOMAIN_VIEWTYPE_SET:
   533|             info["debug_view_type_filtered"] += 1
   534|             continue
   535| 
   536|         name = canon_str(getattr(v, "Name", None))
   537|         if not name:
   538|             info["debug_missing_name"] += 1
   539|             name = S_MISSING
   540|         names.append(name)
   541| 
   542|         uid = None
   543|         try:
   544|             uid = canon_str(getattr(v, "UniqueId", None))
   545|         except Exception:
   546|             uid = None
   547| 
   548|         if not uid:
   549|             info["debug_missing_uid"] += 1
   550| 
   551|         # PR6: view-scoped context snapshot
   552|         try:
   553|             dv = (ctx or {}).get("_doc_view") if ctx is not None else None
   554|             if dv is not None:
   555|                 vi = dv.view_info(v, source="HOST")
   556|                 if vi.reasons:
   557|                     info["debug_view_context_problem"] += 1
   558|                     for r in vi.reasons:
   559|                         info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
   560|         except Exception:
   561|             info["debug_view_context_problem"] += 1
   562|             info["debug_view_context_reasons"]["view_context_unreadable"] = (
   563|                 info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
   564|             )
   565| 
   566|         v2_ok = True
   567|         sig_v2 = []
   568|         sig = []
   569| 
   570|         # Template-controlled parameters ("Include" surface)
   571|         try:
   572|             tpl_ids = v.GetTemplateParameterIds() or []
   573|             tpl_bips = set(
   574|                 pid.IntegerValue for pid in tpl_ids
   575|                 if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
   576|             )
   577|         except Exception:
   578|             tpl_ids = []
   579|             tpl_bips = set()
   580| 
   581|         non_ctrl_bips = _non_ctrl_bips_from_view(v)
   582|         info["debug_non_ctrl_bips_count"] = len(non_ctrl_bips)
   583|         info["debug_view_range_bip_in_non_ctrl"] = (
   584|             int(BuiltInParameter.VIEWER_VOLUME_OF_INTEREST_CROP) in non_ctrl_bips
   585|             if BuiltInParameter is not None else "bip_none"
   586|         )
   587|         info["debug_plan_view_range_bip_in_non_ctrl"] = (
   588|             int(BuiltInParameter.PLAN_VIEW_RANGE) in non_ctrl_bips
   589|             if BuiltInParameter is not None else "bip_none"
   590|         )
   591| 
   592|         # Common include flags
   593|         try:
   594|             sig.append("include_phase_filter={}".format(_is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")))
   595|         except Exception:
   596|             sig.append("include_phase_filter=False")
   597| 
   598|         try:
   599|             sig.append("include_filters={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_FILTERS")))
   600|         except Exception:
   601|             sig.append("include_filters=False")
   602| 
   603|         try:
   604|             sig.append("include_appearance={}".format(_is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_APPEARANCE")))
   605|         except Exception:
   606|             sig.append("include_appearance=False")
   607| 
   608|         # Domain-specific: view range (floor/area plans support view depth)
   609|         try:
   610|             include_view_range = (
   611|                 int(BuiltInParameter.VIEWER_VOLUME_OF_INTEREST_CROP) not in non_ctrl_bips
   612|             )
   613|             sig.append("include_view_range={}".format(include_view_range))
   614|             if v2_ok:
   615|                 sig_v2.append("include_view_range={}".format(include_view_range))
   616|         except Exception:
   617|             sig.append("include_view_range=False")
   618|             if v2_ok:
   619|                 sig_v2.append("include_view_range=False")
   620| 
   621|         # Phase Filter (resolved via phase_filters domain)
   622|         try:
   623|             include_pf = _is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")
   624|         except Exception:
   625|             include_pf = False
   626| 
   627|         v2_ok = _append_phase_filter_value(
   628|             v=v,
   629|             doc=doc,
   630|             include_pf=include_pf,
   631|             phase_filter_map=phase_filter_map,
   632|             phase_filter_map_v2=phase_filter_map_v2,
   633|             sig=sig,
   634|             sig_v2=sig_v2,
   635|             v2_ok=v2_ok,
   636|             v2_block_fn=_v2_block,
   637|             debug_counters=info,
   638|         )
   639| 
   640|         # Filter stack (order-sensitive)
   641|         v2_ok = _append_filter_stack_signature(v, doc, view_filter_map, sig, sig_v2, v2_ok, _v2_block)
   642|         v2_ok = _append_workset_visibility(v, doc, sig, sig_v2, v2_ok, _v2_block)
   643| 
   644|         # Built-in visual/behavioural parameters
   645|         emit_builtin_params(v, DOMAIN_NAME, tpl_bips, non_ctrl_bips, sig, sig_v2,
   646|                             debug_counters=info)
   647| 
   648|         # Shared/project parameters (stub — no-op until GUIDs confirmed)
   649|         emit_shared_params_stub(v, DOMAIN_NAME, tpl_ids, sig, sig_v2,
   650|                                 debug_counters=info)
   651| 
   652|         # Finalize signature (deterministic)
   653|         sig_final = sorted(sig)
   654|         def_hash = make_hash(sig_final)
   655| 
   656|         # v2 finalize
   657|         if v2_ok:
   658|             try:
   659|                 sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
   660|                 sig_v2_final = sorted(set(sig_v2))
   661|                 def_hash_v2 = make_hash(sig_v2_final)
   662|                 per_hashes_v2.append(def_hash_v2)
   663|             except Exception:
   664|                 _v2_block("template_finalize_failed")
   665|                 v2_ok = False
   666| 
   667|         # record.v2 + Phase-2
   668|         identity_items = _canonical_identity_items_from_signature(def_hash, sig_final)
   669|         semantic_keys = _semantic_keys_from_identity_items(identity_items)
   670|         semantic_items = [it for it in identity_items if it.get("k") in set(semantic_keys)]
   671|         sig_hash = make_hash(serialize_identity_items(semantic_items))
   672| 
   673|         rid_info = make_record_id_from_element(v)
   674|         if rid_info:
   675|             record_id, record_id_alg = rid_info
   676|         else:
   677|             record_id = "eid:{}".format(safe_str(getattr(getattr(v, "Id", None), "IntegerValue", "")))
   678|             record_id_alg = "revit_elementid_v1"
   679| 
   680|         status = STATUS_OK
   681|         status_reasons = []
   682|         for it in identity_items:
   683|             if it.get("q") != ITEM_Q_OK:
   684|                 status = STATUS_DEGRADED
   685|                 status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))
   686|         if not v2_ok:
   687|             status = STATUS_BLOCKED
   688|             status_reasons.append("semantic_v2_unresolved_dependency")
   689|             sig_hash = None
   690| 
   691|         vt_raw_str = safe_str(vt_int) if vt_int is not None else S_MISSING
   692| 
   693|         rec = build_record_v2(
   694|             domain=DOMAIN_NAME,
   695|             record_id=record_id,
   696|             record_id_alg=record_id_alg,
   697|             status=status,
   698|             status_reasons=sorted(set(status_reasons)),
   699|             sig_hash=sig_hash,
   700|             identity_items=identity_items,
   701|             required_qs=tuple(it.get("q") for it in identity_items),
   702|             label={
   703|                 "display": safe_str(name),
   704|                 "quality": "human" if safe_str(name) and safe_str(name) != S_MISSING else "placeholder_missing",
   705|                 "provenance": "revit.ViewName",
   706|                 "components": {
   707|                     "view_type": vt_raw_str,
   708|                 },
   709|             },
   710|         )
   711|         _ip, _ip_q = purge_lookup(getattr(getattr(v, "Id", None), "IntegerValue", None), ctx)
   712|         rec["is_purgeable"] = _ip
   713|         rec["is_purgeable_q"] = _ip_q
   714| 
   715|         rec["phase2"] = {
   716|             "schema": "phase2.{}.v2".format(DOMAIN_NAME),
   717|             "grouping_basis": "join_key.join_hash",
   718|             "cosmetic_items": [],
   719|             "coordination_items": [
   720|                 make_identity_item("vt.view_type_family", DOMAIN_NAME, ITEM_Q_OK),
   721|                 make_identity_item("vt.view_type_raw", vt_raw_str, ITEM_Q_OK),
   722|             ],
   723|             "unknown_items": _traceability_unknown_items(v),
   724|         }
   725|         _append_assigned_view_count_cosmetic_item(rec, doc, v, ctx)
   726| 
   727|         rec["sig_basis"] = {
   728|             "hash_alg": "md5_utf8_join_pipe",
   729|             "keys_used": semantic_keys,
   730|         }
   731| 
   732|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
   733|         vt_join_key, _vt_missing = build_join_key_from_policy(
   734|             domain_policy=pol,
   735|             identity_items=identity_items,
   736|             include_optional_items=False,
   737|             emit_keys_used=True,
   738|             hash_optional_items=False,
   739|             emit_items=False,
   740|             emit_selectors=True,
   741|         )
   742|         rec["join_key"] = vt_join_key
   743| 
   744|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
   745|         # keyed off this record's own label.display-backing item (view_template.name).
   746|         # view_template.name does not exist in identity_items for any partition --
   747|         # identity_items are built from _canonical_identity_items_from_signature(def_hash,
   748|         # sig_final), a structured signature that explicitly strips "name="-prefixed
   749|         # entries before hashing. Widened items list used only for this call;
   750|         # identity_basis.items/sig_hash/join_key above are unaffected.
   751|         vt_name_v, vt_name_q = canonicalize_str(name)
   752|         name_key_items = identity_items + [
   753|             make_identity_item("view_template.name", vt_name_v, vt_name_q)
   754|         ]
   755|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
   756|         rec["join_key_name_identity"], _vt_name_key_missing = build_join_key_from_policy(
   757|             domain_policy=name_key_pol,
   758|             identity_items=name_key_items,
   759|             include_optional_items=False,
   760|             emit_keys_used=True,
   761|             hash_optional_items=False,
   762|             emit_items=False,
   763|             emit_selectors=True,
   764|         )
   765|         rec["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _vt_name_key_missing)
   766| 
   767|         rec["def_hash"] = def_hash
   768|         rec["def_signature"] = sig_final
   769| 
   770|         records.append(rec)
   771|         per_hashes.append(def_hash)
   772|         info["debug_kept"] += 1
   773| 
   774|     # Finalize
   775|     info["names"] = sorted(set(names))
   776|     info["count"] = len(records)
   777| 
   778|     info["records"] = sorted(
   779|         records,
   780|         key=lambda r: (
   781|             safe_str(((r.get("label", {}) or {}).get("display", ""))),
   782|             safe_str(r.get("record_id", "")),
   783|         ),
   784|     )
   785| 
   786|     info["signature_hashes_v2"] = sorted(per_hashes_v2)
   787|     if v2_any_blocked:
   788|         info["hash_v2"] = None
   789|     else:
   790|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
   791| 
   792|     info["record_rows"] = []
   793|     try:
   794|         recs = info.get("records") or []
   795|         info["record_rows"] = [{
   796|             "record_key": safe_str(r.get("record_id", "")),
   797|             "sig_hash":   safe_str(r.get("sig_hash", "")),
   798|             "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
   799|             "view_type":  safe_str(((r.get("label", {}) or {}).get("components", {}) or {}).get("view_type", "")),
   800|         } for r in recs]
   801|     except Exception:
   802|         info["record_rows"] = []
   803| 
   804|     # PR6: deterministic degraded signaling
   805|     degraded_reason_counts = {}
   806| 
   807|     try:
   808|         if int(info.get("debug_missing_uid", 0)) > 0:
   809|             degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
   810|     except Exception:
   811|         pass
   812| 
   813|     try:
   814|         if int(info.get("debug_fail_read", 0)) > 0:
   815|             degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
   816|     except Exception:
   817|         pass
   818| 
   819|     try:
   820|         if int(info.get("debug_view_context_problem", 0)) > 0:
   821|             for k, vv in dict(info.get("debug_view_context_reasons", {})).items():
   822|                 key = str(k)
   823|                 if key.endswith("_not_applicable"):
   824|                     continue
   825|                 degraded_reason_counts[key] = int(vv)
   826|     except Exception:
   827|         pass
   828| 
   829|     try:
   830|         if int(info.get("debug_v2_blocked", 0)) > 0:
   831|             degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
   832|     except Exception:
   833|         pass
   834| 
   835|     if degraded_reason_counts:
   836|         info["_domain_status"] = "degraded"
   837|         info["_domain_diag"] = {
   838|             "degraded_reasons": sorted(degraded_reason_counts.keys()),
   839|             "degraded_reason_counts": degraded_reason_counts,
   840|         }
   841|     else:
   842|         info["_domain_status"] = "ok"
   843|         info["_domain_diag"] = {}
   844| 
   845|     return info
   846| 
```
