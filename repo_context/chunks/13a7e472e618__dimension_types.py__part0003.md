# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 3 of 8
- Original line range: 653-1079
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_angular, _apply_family_name_override
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
   653| def extract_angular(doc, ctx=None):
   654|     _HANDLED_SHAPES = _ANGULAR_HANDLED
   655|     EXPECTED_FAMILY = _ANGULAR_EXPECTED_FAMILY
   656|     DOMAIN_NAME = "dimension_types_angular"
   657|     """
   658|     Extract Angular dimension types fingerprint.
   659| 
   660|     Args:
   661|         doc: Revit Document
   662|         ctx: Context dictionary
   663| 
   664|     Returns:
   665|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
   666|     """
   667|     info = {
   668|         "count": 0,
   669|         "raw_count": 0,
   670|         "records": [],
   671|         "signature_hashes_v2": [],
   672|         "hash_v2": None,
   673|         "debug_v2_blocked": False,
   674|         "debug_v2_block_reasons": {},
   675|     }
   676| 
   677|     if ctx is None:
   678|         ctx = {}
   679| 
   680|     if DimensionType is None:
   681|         info["debug_v2_blocked"] = True
   682|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
   683|         return info
   684| 
   685|     try:
   686|         all_types = _collect_dim_types(doc, ctx)
   687|     except Exception:
   688|         all_types = []
   689| 
   690|     info["raw_count"] = len(all_types)
   691|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
   692| 
   693|     v2_records = []
   694|     v2_sig_hashes = []
   695|     _eligible_type_count = 0
   696| 
   697|     for d in all_types:
   698|         try:
   699|             type_name = get_type_display_name(d)
   700| 
   701|             # Exclude system built-in types with id-based labels (not user-accessible)
   702|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
   703|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
   704|                 continue
   705| 
   706|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
   707| 
   708|             # Apply family-name heuristic override to detect Spot types
   709|             shape_v, shape_family, shape_q = _apply_family_name_override(
   710|                 d, shape_v, shape_family, shape_q, type_name
   711|             )
   712| 
   713|             # Filter: skip shapes not handled by this domain
   714|             if shape_v not in _HANDLED_SHAPES:
   715|                 continue
   716| 
   717|             # Exclude confirmed wrong-family types (system/infrastructure types)
   718|             family_name = None
   719|             try:
   720|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
   721|                 if p_fam:
   722|                     family_name = _as_string(p_fam)
   723|                     if family_name:
   724|                         family_name = canon_str(family_name)
   725|             except Exception:
   726|                 pass
   727|             if family_name and family_name != EXPECTED_FAMILY:
   728|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
   729|                 continue
   730| 
   731|             _eligible_type_count += 1
   732| 
   733|             # --- Read core identity fields ---
   734| 
   735|             # Unit format info
   736|             (unit_format_id_v, unit_format_id_q,
   737|              rounding_v, rounding_q,
   738|              accuracy_v, accuracy_q,
   739|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
   740| 
   741|             # Prefix/Suffix
   742|             prefix_v, prefix_q, suffix_v, suffix_q = _read_prefix_suffix(d)
   743| 
   744|             # Tick mark sig hash
   745|             tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)
   746| 
   747|             # Witness line control (Angular dimensions expose witness_line_control per spec)
   748|             witness_v, witness_q = (None, ITEM_Q_MISSING)
   749|             try:
   750|                 p_wit = first_param(d, ui_names=["Witness Line Control", "Witness line control"])
   751|                 if p_wit is None:
   752|                     witness_v, witness_q = (None, ITEM_Q_MISSING)
   753|                 else:
   754|                     # Witness Line Control is Integer/enum — must use AsValueString(), not AsString()
   755|                     witness_raw = _as_value_string(p_wit)
   756|                     if witness_raw is not None and witness_raw.strip() == "":
   757|                         witness_v, witness_q = (None, ITEM_Q_MISSING)
   758|                     else:
   759|                         witness_v, witness_q = canonicalize_str(witness_raw)
   760|             except Exception:
   761|                 witness_v, witness_q = (None, ITEM_Q_UNREADABLE)
   762| 
   763|             # --- Area 7 §2: linear/angular/radial/diameter leader config ---
   764|             leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   765|                 d, ctx, ui_names=["Leader Tick Mark"]
   766|             )
   767|             leader_type_v, leader_type_q = (None, ITEM_Q_MISSING)
   768|             try:
   769|                 p_lt = first_param(d, ui_names=["Leader Type"])
   770|                 lt_raw = _as_value_string(p_lt) if p_lt is not None else None
   771|                 leader_type_v, leader_type_q = canonicalize_str(lt_raw)
   772|             except Exception:
   773|                 leader_type_v, leader_type_q = (None, ITEM_Q_UNREADABLE)
   774|             show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_MISSING)
   775|             try:
   776|                 p_slwtm = first_param(d, ui_names=["Show Leader When Text Moves"])
   777|                 slwtm_raw = _as_value_string(p_slwtm) if p_slwtm is not None else None
   778|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = canonicalize_str(slwtm_raw)
   779|             except Exception:
   780|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_UNREADABLE)
   781|             tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_MISSING)
   782|             try:
   783|                 p_tmlw = first_param(d, ui_names=["Tick Mark Line Weight"])
   784|                 tmlw_int = _as_int(p_tmlw) if p_tmlw is not None else None
   785|                 tick_mark_line_weight_v, tick_mark_line_weight_q = canonicalize_int(tmlw_int)
   786|             except Exception:
   787|                 tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_UNREADABLE)
   788| 
   789|             # --- Area 7 §3: witness lines (linear/angular only) ---
   790|             witness_line_extension_v, witness_line_extension_q = (None, ITEM_Q_MISSING)
   791|             try:
   792|                 p_wle = first_param(d, ui_names=["Witness Line Extension"])
   793|                 wle_ft = _as_double(p_wle) if p_wle is not None else None
   794|                 witness_line_extension_v, witness_line_extension_q = canonicalize_float(_fmt_in_from_ft(wle_ft))
   795|             except Exception:
   796|                 witness_line_extension_v, witness_line_extension_q = (None, ITEM_Q_UNREADABLE)
   797|             witness_line_gap_v, witness_line_gap_q = (None, ITEM_Q_MISSING)
   798|             try:
   799|                 p_wlg = first_param(d, ui_names=["Witness Line Gap to Element"])
   800|                 wlg_ft = _as_double(p_wlg) if p_wlg is not None else None
   801|                 witness_line_gap_v, witness_line_gap_q = canonicalize_float(_fmt_in_from_ft(wlg_ft))
   802|             except Exception:
   803|                 witness_line_gap_v, witness_line_gap_q = (None, ITEM_Q_UNREADABLE)
   804|             witness_line_length_v, witness_line_length_q = (None, ITEM_Q_MISSING)
   805|             try:
   806|                 p_wll = first_param(d, ui_names=["Witness Line Length"])
   807|                 wll_ft = _as_double(p_wll) if p_wll is not None else None
   808|                 witness_line_length_v, witness_line_length_q = canonicalize_float(_fmt_in_from_ft(wll_ft))
   809|             except Exception:
   810|                 witness_line_length_v, witness_line_length_q = (None, ITEM_Q_UNREADABLE)
   811|             # Witness Line Tick Mark intentionally NOT read here: probe data shows it is
   812|             # Linear-only (Angular never observed), consistent across all 3 probe runs.
   813| 
   814|             # --- Area 7 §4a: equality dimensions (linear/angular only) ---
   815|             equality_text_v, equality_text_q = (None, ITEM_Q_MISSING)
   816|             try:
   817|                 p_eqt = first_param(d, ui_names=["Equality Text"])
   818|                 eqt_raw = _as_string(p_eqt) if p_eqt is not None else None
   819|                 equality_text_v, equality_text_q = canonicalize_str_allow_empty(eqt_raw)
   820|             except Exception:
   821|                 equality_text_v, equality_text_q = (None, ITEM_Q_UNREADABLE)
   822|             equality_witness_display_v, equality_witness_display_q = (None, ITEM_Q_MISSING)
   823|             try:
   824|                 p_ewd = first_param(d, ui_names=["Equality Witness Display"])
   825|                 ewd_raw = _as_value_string(p_ewd) if p_ewd is not None else None
   826|                 equality_witness_display_v, equality_witness_display_q = canonicalize_str(ewd_raw)
   827|             except Exception:
   828|                 equality_witness_display_v, equality_witness_display_q = (None, ITEM_Q_UNREADABLE)
   829| 
   830|             # --- Area 7 §4b: centerline / interior tick marks (linear/angular only) ---
   831|             centerline_pattern_sig_hash_v, centerline_pattern_sig_hash_q = _read_line_pattern_ref_sig_hash(
   832|                 d, ctx, doc, ui_names=["Centerline Pattern"]
   833|             )
   834|             centerline_symbol_name_v, centerline_symbol_name_q = _read_element_ref_name(
   835|                 d, doc, ui_names=["Centerline Symbol"]
   836|             )
   837|             centerline_tick_mark_sig_hash_v, centerline_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   838|                 d, ctx, ui_names=["Centerline Tick Mark"]
   839|             )
   840|             interior_tick_mark_sig_hash_v, interior_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   841|                 d, ctx, ui_names=["Interior Tick Mark"]
   842|             )
   843|             interior_tick_mark_display_v, interior_tick_mark_display_q = (None, ITEM_Q_MISSING)
   844|             try:
   845|                 p_itmd = first_param(d, ui_names=["Interior Tick Mark Display"])
   846|                 itmd_raw = _as_value_string(p_itmd) if p_itmd is not None else None
   847|                 interior_tick_mark_display_v, interior_tick_mark_display_q = canonicalize_str(itmd_raw)
   848|             except Exception:
   849|                 interior_tick_mark_display_v, interior_tick_mark_display_q = (None, ITEM_Q_UNREADABLE)
   850| 
   851|             # --- Area 7 §7: Text Offset (Angular/Diameter/Linear/Radial per probe) ---
   852|             text_offset_v, text_offset_q = (None, ITEM_Q_MISSING)
   853|             try:
   854|                 p_toff = first_param(d, ui_names=["Text Offset"])
   855|                 toff_ft = _as_double(p_toff) if p_toff is not None else None
   856|                 text_offset_v, text_offset_q = canonicalize_float(_fmt_in_from_ft(toff_ft))
   857|             except Exception:
   858|                 text_offset_v, text_offset_q = (None, ITEM_Q_UNREADABLE)
   859| 
   860|             # --- Build identity items ---
   861|             core_items = [
   862|                 make_identity_item("dim_type.shape", shape_v, shape_q),
   863|                 make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
   864|                 make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
   865|                 make_identity_item("dim_type.witness_line_control", witness_v, witness_q),
   866|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
   867|                 make_identity_item("dim_type.rounding", rounding_v, rounding_q),
   868|                 make_identity_item("dim_type.prefix", prefix_v, prefix_q),
   869|                 make_identity_item("dim_type.suffix", suffix_v, suffix_q),
   870|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
   871|                 make_identity_item("dim_type.leader_tick_mark_sig_hash", leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q),
   872|                 make_identity_item("dim_type.leader_type", leader_type_v, leader_type_q),
   873|                 make_identity_item("dim_type.show_leader_when_text_moves", show_leader_when_text_moves_v, show_leader_when_text_moves_q),
   874|                 make_identity_item("dim_type.tick_mark_line_weight", tick_mark_line_weight_v, tick_mark_line_weight_q),
   875|                 make_identity_item("dim_type.witness_line_extension_in", witness_line_extension_v, witness_line_extension_q),
   876|                 make_identity_item("dim_type.witness_line_gap_to_element_in", witness_line_gap_v, witness_line_gap_q),
   877|                 make_identity_item("dim_type.witness_line_length_in", witness_line_length_v, witness_line_length_q),
   878|                 make_identity_item("dim_type.equality_text", equality_text_v, equality_text_q),
   879|                 make_identity_item("dim_type.equality_witness_display", equality_witness_display_v, equality_witness_display_q),
   880|                 make_identity_item("dim_type.centerline_pattern_sig_hash", centerline_pattern_sig_hash_v, centerline_pattern_sig_hash_q),
   881|                 make_identity_item("dim_type.centerline_symbol_name", centerline_symbol_name_v, centerline_symbol_name_q),
   882|                 make_identity_item("dim_type.centerline_tick_mark_sig_hash", centerline_tick_mark_sig_hash_v, centerline_tick_mark_sig_hash_q),
   883|                 make_identity_item("dim_type.interior_tick_mark_sig_hash", interior_tick_mark_sig_hash_v, interior_tick_mark_sig_hash_q),
   884|                 make_identity_item("dim_type.interior_tick_mark_display", interior_tick_mark_display_v, interior_tick_mark_display_q),
   885|                 make_identity_item("dim_type.text_offset_in", text_offset_v, text_offset_q),
   886|             ]
   887| 
   888|             text_items = _build_text_appearance_items(d)
   889|             alt_units_items = _build_alternate_units_items(d)
   890|             all_items = core_items + text_items + alt_units_items
   891| 
   892|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
   893| 
   894|             # Required qualities for blocking
   895|             # rounding, prefix, suffix are optional enrichment — not blocking for Angular
   896|             required_qs = [
   897|                 shape_q,
   898|                 accuracy_q,
   899|                 tick_sig_hash_q,
   900|                 unit_format_id_q,
   901|             ]
   902|             # witness_line_control: soft-required — only contributes when successfully read
   903|             if witness_q == ITEM_Q_OK:
   904|                 required_qs.append(witness_q)
   905|             # text/appearance fields, and all Area 7 additions, are cross-family alignment /
   906|             # non-blocking enrichment — not blocking
   907|             blocked = any(q != ITEM_Q_OK for q in required_qs)
   908| 
   909|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
   910|                 "dim_type.tick_mark_sig_hash",
   911|                 "dim_type.leader_tick_mark_sig_hash",
   912|                 "dim_type.centerline_tick_mark_sig_hash",
   913|                 "dim_type.interior_tick_mark_sig_hash",
   914|                 "dim_type.centerline_pattern_sig_hash",
   915|                 "dim_type.centerline_symbol_name",
   916|             })
   917| 
   918|             status_reasons = []
   919|             for it in identity_items:
   920|                 q = it.get("q")
   921|                 k = it.get("k", "")
   922|                 if q == ITEM_Q_OK:
   923|                     continue
   924|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
   925|                     continue
   926|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
   927| 
   928|             if blocked:
   929|                 status = STATUS_BLOCKED
   930|             elif status_reasons:
   931|                 status = STATUS_DEGRADED
   932|             else:
   933|                 status = STATUS_OK
   934| 
   935|             preimage = serialize_identity_items(identity_items)
   936|             sig_hash = None if blocked else make_hash(preimage)
   937| 
   938|             try:
   939|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
   940|             except Exception:
   941|                 type_id_int = None
   942| 
   943|             try:
   944|                 uid_raw = getattr(d, "UniqueId", None)
   945|             except Exception:
   946|                 uid_raw = None
   947| 
   948|             label_str = type_name
   949|             rec_v2 = build_record_v2(
   950|                 domain=DOMAIN_NAME,
   951|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
   952|                 status=status,
   953|                 status_reasons=sorted(set(status_reasons)),
   954|                 sig_hash=sig_hash,
   955|                 identity_items=identity_items,
   956|                 required_qs=tuple(required_qs),
   957|                 label={
   958|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
   959|                     "quality": "human" if label_str else "placeholder_missing",
   960|                     "provenance": "revit.DimensionType.params",
   961|                 },
   962|             )
   963|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
   964|             rec_v2["is_purgeable"] = _ip
   965|             rec_v2["is_purgeable_q"] = _ip_q
   966|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
   967| 
   968|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
   969|             rec_v2["join_key"], _missing = build_join_key_from_policy(
   970|                 domain_policy=pol,
   971|                 identity_items=identity_items,
   972|                 include_optional_items=False,
   973|                 emit_keys_used=True,
   974|                 hash_optional_items=False,
   975|                 emit_items=False,
   976|                 emit_selectors=True,
   977|             )
   978| 
   979|             # Canonical Name Identity Projection (PR1): second, independent join_hash
   980|             # variant keyed off this record's own label.display-backing item
   981|             # (dim_type.name). dim_type.name does not exist anywhere in this file --
   982|             # type_name/label_str feeds label.display only. Widened items list used
   983|             # only for this call; identity_basis.items/sig_hash/join_key above are
   984|             # unaffected. (dimension_types_spot_coordinate/spot_elevation are excluded
   985|             # from the name-key policy entirely -- their only other name-shaped item,
   986|             # dim_type.symbol_name, names a different, referenced tick-mark/leader
   987|             # symbol element, not this record's own label.)
   988|             dt_name_v, dt_name_q = canonicalize_str(type_name)
   989|             name_key_items = identity_items + [
   990|                 make_identity_item("dim_type.name", dt_name_v, dt_name_q)
   991|             ]
   992|             name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
   993|             rec_v2["join_key_name_identity"], _name_key_missing = build_join_key_from_policy(
   994|                 domain_policy=name_key_pol,
   995|                 identity_items=name_key_items,
   996|                 include_optional_items=False,
   997|                 emit_keys_used=True,
   998|                 hash_optional_items=False,
   999|                 emit_items=False,
  1000|                 emit_selectors=True,
  1001|             )
  1002|             rec_v2["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _name_key_missing)
  1003| 
  1004|             coordination_items = [
  1005|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
  1006|             ]
  1007| 
  1008|             unknown_items = []
  1009|             try:
  1010|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
  1011|             except Exception:
  1012|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  1013|             try:
  1014|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
  1015|             except Exception:
  1016|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  1017|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
  1018|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
  1019| 
  1020|             rec_v2["phase2"] = {
  1021|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  1022|                 "grouping_basis": "phase2.hypothesis",
  1023|                 "cosmetic_items": phase2_sorted_items([]),
  1024|                 "coordination_items": phase2_sorted_items(coordination_items),
  1025|                 "unknown_items": phase2_sorted_items(unknown_items),
  1026|             }
  1027| 
  1028|             if sig_hash:
  1029|                 v2_sig_hashes.append(sig_hash)
  1030|             v2_records.append(rec_v2)
  1031| 
  1032|         except Exception:
  1033|             continue  # fail-soft per record
  1034| 
  1035|     _total_type_count = _eligible_type_count
  1036|     for rec in v2_records:
  1037|         try:
  1038|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
  1039|             rec["is_sole_type_in_category_q"] = "ok"
  1040|         except Exception:
  1041|             rec["is_sole_type_in_category"] = None
  1042|             rec["is_sole_type_in_category_q"] = "unreadable"
  1043| 
  1044|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
  1045|     info["count"] = len(v2_records)
  1046|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  1047| 
  1048|     if v2_sig_hashes:
  1049|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  1050|         info["debug_v2_blocked"] = False
  1051|     else:
  1052|         info["hash_v2"] = None
  1053|         info["debug_v2_blocked"] = True
  1054|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
  1055| 
  1056|     return info
  1057| 
  1058| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
  1059|     """
  1060|     Heuristic override: if the FamilyName prefix indicates a Spot family,
  1061|     force Spot classification so we skip this record (spot shapes have their own domain).
  1062|     Returns updated (shape_v, shape_family, shape_q).
  1063|     """
  1064|     try:
  1065|         family_name = getattr(d, "FamilyName", None)
  1066|         basis = family_name if family_name else type_name
  1067|         bn_l = safe_str(basis).strip().lower()
  1068| 
  1069|         if bn_l.startswith("spot slopes"):
  1070|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
  1071|         elif bn_l.startswith("spot elevations"):
  1072|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
  1073|         elif bn_l.startswith("spot coordinates"):
  1074|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
  1075|     except Exception:
  1076|         pass
  1077|     return (shape_v, shape_family, shape_q)
  1078| 
  1079| 
```
