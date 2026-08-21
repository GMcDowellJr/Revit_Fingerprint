# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 2 of 8
- Original line range: 178-652
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_linear, _apply_family_name_override
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
   178| def extract_linear(doc, ctx=None):
   179|     _HANDLED_SHAPES = _LINEAR_HANDLED
   180|     EXPECTED_FAMILY = _LINEAR_EXPECTED_FAMILY
   181|     DOMAIN_NAME = "dimension_types_linear"
   182|     """
   183|     Extract Linear/LinearFixed/ArcLength dimension types fingerprint.
   184| 
   185|     Args:
   186|         doc: Revit Document
   187|         ctx: Context dictionary
   188| 
   189|     Returns:
   190|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
   191|     """
   192|     info = {
   193|         "count": 0,
   194|         "raw_count": 0,
   195|         "records": [],
   196|         "signature_hashes_v2": [],
   197|         "hash_v2": None,
   198|         "debug_v2_blocked": False,
   199|         "debug_v2_block_reasons": {},
   200|     }
   201| 
   202|     if ctx is None:
   203|         ctx = {}
   204| 
   205|     if DimensionType is None:
   206|         info["debug_v2_blocked"] = True
   207|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
   208|         return info
   209| 
   210|     try:
   211|         all_types = _collect_dim_types(doc, ctx)
   212|     except Exception:
   213|         all_types = []
   214| 
   215|     info["raw_count"] = len(all_types)
   216|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
   217| 
   218|     v2_records = []
   219|     v2_sig_hashes = []
   220|     _eligible_type_count = 0
   221| 
   222|     for d in all_types:
   223|         try:
   224|             # Get display name for heuristic override
   225|             type_name = get_type_display_name(d)
   226| 
   227|             # Exclude system built-in types with id-based labels (not user-accessible)
   228|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
   229|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
   230|                 continue
   231| 
   232|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
   233| 
   234|             # Apply family-name heuristic override to detect Spot types
   235|             shape_v, shape_family, shape_q = _apply_family_name_override(
   236|                 d, shape_v, shape_family, shape_q, type_name
   237|             )
   238| 
   239|             # Filter: skip shapes not handled by this domain
   240|             if shape_v not in _HANDLED_SHAPES:
   241|                 continue
   242| 
   243|             # Exclude confirmed wrong-family types (system/infrastructure types)
   244|             # family_name=None means unreadable — do not exclude on absence
   245|             family_name = None
   246|             try:
   247|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
   248|                 if p_fam:
   249|                     family_name = _as_string(p_fam)
   250|                     if family_name:
   251|                         family_name = canon_str(family_name)
   252|             except Exception:
   253|                 pass
   254|             if family_name and family_name != EXPECTED_FAMILY:
   255|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
   256|                 continue
   257| 
   258|             _eligible_type_count += 1
   259| 
   260|             # --- Read core identity fields ---
   261| 
   262|             # Unit format info
   263|             (unit_format_id_v, unit_format_id_q,
   264|              rounding_v, rounding_q,
   265|              accuracy_v, accuracy_q,
   266|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
   267| 
   268|             # Prefix/Suffix
   269|             prefix_v, prefix_q, suffix_v, suffix_q = _read_prefix_suffix(d)
   270| 
   271|             # Tick mark sig hash
   272|             tick_sig_hash_v, tick_sig_hash_q = _read_tick_mark_sig_hash(d, ctx, doc)
   273| 
   274|             # Witness line control (required for all shapes in this domain)
   275|             witness_v, witness_q = (None, ITEM_Q_MISSING)
   276|             try:
   277|                 p_wit = first_param(d, ui_names=["Witness Line Control", "Witness line control"])
   278|                 if p_wit is None:
   279|                     witness_v, witness_q = (None, ITEM_Q_MISSING)
   280|                 else:
   281|                     # Witness Line Control is Integer/enum — must use AsValueString(), not AsString()
   282|                     witness_raw = _as_value_string(p_wit)
   283|                     if witness_raw is not None and witness_raw.strip() == "":
   284|                         witness_v, witness_q = (None, ITEM_Q_MISSING)
   285|                     else:
   286|                         witness_v, witness_q = canonicalize_str(witness_raw)
   287|             except Exception:
   288|                 witness_v, witness_q = (None, ITEM_Q_UNREADABLE)
   289| 
   290|             # --- Area 7 §2: linear/angular/radial/diameter leader config ---
   291|             # (dimension-line leader, distinct from the spot-family Leader Arrowhead in §1)
   292|             leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   293|                 d, ctx, ui_names=["Leader Tick Mark"]
   294|             )
   295|             leader_type_v, leader_type_q = (None, ITEM_Q_MISSING)
   296|             try:
   297|                 p_lt = first_param(d, ui_names=["Leader Type"])
   298|                 lt_raw = _as_value_string(p_lt) if p_lt is not None else None
   299|                 leader_type_v, leader_type_q = canonicalize_str(lt_raw)
   300|             except Exception:
   301|                 leader_type_v, leader_type_q = (None, ITEM_Q_UNREADABLE)
   302|             show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_MISSING)
   303|             try:
   304|                 p_slwtm = first_param(d, ui_names=["Show Leader When Text Moves"])
   305|                 slwtm_raw = _as_value_string(p_slwtm) if p_slwtm is not None else None
   306|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = canonicalize_str(slwtm_raw)
   307|             except Exception:
   308|                 show_leader_when_text_moves_v, show_leader_when_text_moves_q = (None, ITEM_Q_UNREADABLE)
   309|             # Tick Mark Line Weight (§4): the tick-mark glyph's own weight, distinct from
   310|             # dim_type.line_weight (the overall dimension line weight)
   311|             tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_MISSING)
   312|             try:
   313|                 p_tmlw = first_param(d, ui_names=["Tick Mark Line Weight"])
   314|                 tmlw_int = _as_int(p_tmlw) if p_tmlw is not None else None
   315|                 tick_mark_line_weight_v, tick_mark_line_weight_q = canonicalize_int(tmlw_int)
   316|             except Exception:
   317|                 tick_mark_line_weight_v, tick_mark_line_weight_q = (None, ITEM_Q_UNREADABLE)
   318| 
   319|             # --- Area 7 §3: witness lines (linear/angular only) ---
   320|             witness_line_extension_v, witness_line_extension_q = (None, ITEM_Q_MISSING)
   321|             try:
   322|                 p_wle = first_param(d, ui_names=["Witness Line Extension"])
   323|                 wle_ft = _as_double(p_wle) if p_wle is not None else None
   324|                 witness_line_extension_v, witness_line_extension_q = canonicalize_float(_fmt_in_from_ft(wle_ft))
   325|             except Exception:
   326|                 witness_line_extension_v, witness_line_extension_q = (None, ITEM_Q_UNREADABLE)
   327|             witness_line_gap_v, witness_line_gap_q = (None, ITEM_Q_MISSING)
   328|             try:
   329|                 p_wlg = first_param(d, ui_names=["Witness Line Gap to Element"])
   330|                 wlg_ft = _as_double(p_wlg) if p_wlg is not None else None
   331|                 witness_line_gap_v, witness_line_gap_q = canonicalize_float(_fmt_in_from_ft(wlg_ft))
   332|             except Exception:
   333|                 witness_line_gap_v, witness_line_gap_q = (None, ITEM_Q_UNREADABLE)
   334|             witness_line_length_v, witness_line_length_q = (None, ITEM_Q_MISSING)
   335|             try:
   336|                 p_wll = first_param(d, ui_names=["Witness Line Length"])
   337|                 wll_ft = _as_double(p_wll) if p_wll is not None else None
   338|                 witness_line_length_v, witness_line_length_q = canonicalize_float(_fmt_in_from_ft(wll_ft))
   339|             except Exception:
   340|                 witness_line_length_v, witness_line_length_q = (None, ITEM_Q_UNREADABLE)
   341|             # Witness Line Tick Mark: Linear only per probe data (not Angular, an asymmetry
   342|             # confirmed consistent across all 3 probe runs, not a sampling artifact)
   343|             witness_line_tick_mark_sig_hash_v, witness_line_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   344|                 d, ctx, ui_names=["Witness Line Tick Mark"]
   345|             )
   346| 
   347|             # --- Area 7 §4a: equality dimensions (linear/angular only) ---
   348|             equality_text_v, equality_text_q = (None, ITEM_Q_MISSING)
   349|             try:
   350|                 p_eqt = first_param(d, ui_names=["Equality Text"])
   351|                 eqt_raw = _as_string(p_eqt) if p_eqt is not None else None
   352|                 equality_text_v, equality_text_q = canonicalize_str_allow_empty(eqt_raw)
   353|             except Exception:
   354|                 equality_text_v, equality_text_q = (None, ITEM_Q_UNREADABLE)
   355|             equality_witness_display_v, equality_witness_display_q = (None, ITEM_Q_MISSING)
   356|             try:
   357|                 p_ewd = first_param(d, ui_names=["Equality Witness Display"])
   358|                 ewd_raw = _as_value_string(p_ewd) if p_ewd is not None else None
   359|                 equality_witness_display_v, equality_witness_display_q = canonicalize_str(ewd_raw)
   360|             except Exception:
   361|                 equality_witness_display_v, equality_witness_display_q = (None, ITEM_Q_UNREADABLE)
   362|             # Equality Formula intentionally dropped: probe storage=None/unsupported, not a
   363|             # plain-parameter read like Equality Text/Equality Witness Display (Area 7 §4a).
   364| 
   365|             # --- Area 7 §4b: centerline / interior tick marks (linear/angular only) ---
   366|             # Note: this is the Linear/Angular Dimension Style "Centerline" tab, a distinct
   367|             # Revit feature from Radial/Diameter's Center Mark (dim_type.center_marks) --
   368|             # confirmed by probe observed_on_shapes (Linear/Angular only, never Radial/Diameter).
   369|             centerline_pattern_sig_hash_v, centerline_pattern_sig_hash_q = _read_line_pattern_ref_sig_hash(
   370|                 d, ctx, doc, ui_names=["Centerline Pattern"]
   371|             )
   372|             centerline_symbol_name_v, centerline_symbol_name_q = _read_element_ref_name(
   373|                 d, doc, ui_names=["Centerline Symbol"]
   374|             )
   375|             centerline_tick_mark_sig_hash_v, centerline_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   376|                 d, ctx, ui_names=["Centerline Tick Mark"]
   377|             )
   378|             interior_tick_mark_sig_hash_v, interior_tick_mark_sig_hash_q = _read_arrowhead_ref_sig_hash(
   379|                 d, ctx, ui_names=["Interior Tick Mark"]
   380|             )
   381|             interior_tick_mark_display_v, interior_tick_mark_display_q = (None, ITEM_Q_MISSING)
   382|             try:
   383|                 p_itmd = first_param(d, ui_names=["Interior Tick Mark Display"])
   384|                 itmd_raw = _as_value_string(p_itmd) if p_itmd is not None else None
   385|                 interior_tick_mark_display_v, interior_tick_mark_display_q = canonicalize_str(itmd_raw)
   386|             except Exception:
   387|                 interior_tick_mark_display_v, interior_tick_mark_display_q = (None, ITEM_Q_UNREADABLE)
   388| 
   389|             # --- Area 7 §7: Text Offset (Angular/Diameter/Linear/Radial per probe) ---
   390|             text_offset_v, text_offset_q = (None, ITEM_Q_MISSING)
   391|             try:
   392|                 p_toff = first_param(d, ui_names=["Text Offset"])
   393|                 toff_ft = _as_double(p_toff) if p_toff is not None else None
   394|                 text_offset_v, text_offset_q = canonicalize_float(_fmt_in_from_ft(toff_ft))
   395|             except Exception:
   396|                 text_offset_v, text_offset_q = (None, ITEM_Q_UNREADABLE)
   397| 
   398|             # --- Area 7 §7: Dimension String Type / Show Opening Height (Linear only per probe) ---
   399|             dimension_string_type_v, dimension_string_type_q = (None, ITEM_Q_MISSING)
   400|             try:
   401|                 p_dst2 = first_param(d, ui_names=["Dimension String Type"])
   402|                 dst2_raw = _as_value_string(p_dst2) if p_dst2 is not None else None
   403|                 dimension_string_type_v, dimension_string_type_q = canonicalize_str(dst2_raw)
   404|             except Exception:
   405|                 dimension_string_type_v, dimension_string_type_q = (None, ITEM_Q_UNREADABLE)
   406|             show_opening_height_v, show_opening_height_q = (None, ITEM_Q_MISSING)
   407|             try:
   408|                 p_soh = first_param(d, ui_names=["Show Opening Height"])
   409|                 soh_int = _as_int(p_soh) if p_soh is not None else None
   410|                 show_opening_height_v, show_opening_height_q = canonicalize_bool(soh_int)
   411|             except Exception:
   412|                 show_opening_height_v, show_opening_height_q = (None, ITEM_Q_UNREADABLE)
   413| 
   414|             # --- Build identity items ---
   415|             core_items = [
   416|                 make_identity_item("dim_type.shape", shape_v, shape_q),
   417|                 make_identity_item("dim_type.accuracy", accuracy_v, accuracy_q),
   418|                 make_identity_item("dim_type.tick_mark_sig_hash", tick_sig_hash_v, tick_sig_hash_q),
   419|                 make_identity_item("dim_type.witness_line_control", witness_v, witness_q),
   420|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
   421|                 make_identity_item("dim_type.rounding", rounding_v, rounding_q),
   422|                 make_identity_item("dim_type.prefix", prefix_v, prefix_q),
   423|                 make_identity_item("dim_type.suffix", suffix_v, suffix_q),
   424|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
   425|                 make_identity_item("dim_type.leader_tick_mark_sig_hash", leader_tick_mark_sig_hash_v, leader_tick_mark_sig_hash_q),
   426|                 make_identity_item("dim_type.leader_type", leader_type_v, leader_type_q),
   427|                 make_identity_item("dim_type.show_leader_when_text_moves", show_leader_when_text_moves_v, show_leader_when_text_moves_q),
   428|                 make_identity_item("dim_type.tick_mark_line_weight", tick_mark_line_weight_v, tick_mark_line_weight_q),
   429|                 make_identity_item("dim_type.witness_line_extension_in", witness_line_extension_v, witness_line_extension_q),
   430|                 make_identity_item("dim_type.witness_line_gap_to_element_in", witness_line_gap_v, witness_line_gap_q),
   431|                 make_identity_item("dim_type.witness_line_length_in", witness_line_length_v, witness_line_length_q),
   432|                 make_identity_item("dim_type.witness_line_tick_mark_sig_hash", witness_line_tick_mark_sig_hash_v, witness_line_tick_mark_sig_hash_q),
   433|                 make_identity_item("dim_type.equality_text", equality_text_v, equality_text_q),
   434|                 make_identity_item("dim_type.equality_witness_display", equality_witness_display_v, equality_witness_display_q),
   435|                 make_identity_item("dim_type.centerline_pattern_sig_hash", centerline_pattern_sig_hash_v, centerline_pattern_sig_hash_q),
   436|                 make_identity_item("dim_type.centerline_symbol_name", centerline_symbol_name_v, centerline_symbol_name_q),
   437|                 make_identity_item("dim_type.centerline_tick_mark_sig_hash", centerline_tick_mark_sig_hash_v, centerline_tick_mark_sig_hash_q),
   438|                 make_identity_item("dim_type.interior_tick_mark_sig_hash", interior_tick_mark_sig_hash_v, interior_tick_mark_sig_hash_q),
   439|                 make_identity_item("dim_type.interior_tick_mark_display", interior_tick_mark_display_v, interior_tick_mark_display_q),
   440|                 make_identity_item("dim_type.text_offset_in", text_offset_v, text_offset_q),
   441|                 make_identity_item("dim_type.dimension_string_type", dimension_string_type_v, dimension_string_type_q),
   442|                 make_identity_item("dim_type.show_opening_height", show_opening_height_v, show_opening_height_q),
   443|             ]
   444| 
   445|             text_items = _build_text_appearance_items(d)
   446|             alt_units_items = _build_alternate_units_items(d)
   447|             all_items = core_items + text_items + alt_units_items
   448| 
   449|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
   450| 
   451|             # Required qualities for blocking
   452|             required_qs = [
   453|                 shape_q,
   454|                 accuracy_q,
   455|                 tick_sig_hash_q,
   456|                 unit_format_id_q,
   457|                 rounding_q,
   458|                 prefix_q,
   459|                 suffix_q,
   460|             ]
   461|             # Area 7 additions (suppress_spaces, alternate units, leader config, witness-line
   462|             # detail, equality, centerline/interior tick marks, text offset, dimension string
   463|             # type, show opening height) are non-blocking enrichment -- not added to
   464|             # required_qs, matching the existing treatment of text/appearance and other
   465|             # optional-enrichment fields in this domain.
   466|             # witness_line_control: soft-required — only contributes to blocking if
   467|             # successfully read (q=OK appended to list has no blocking effect; this
   468|             # pattern ensures the field never blocks on lookup failure)
   469|             if witness_q == ITEM_Q_OK:
   470|                 required_qs.append(witness_q)
   471|             # text/appearance fields are cross-family alignment, not primary identity — not blocking
   472| 
   473|             blocked = any(q != ITEM_Q_OK for q in required_qs)
   474| 
   475|             # ElementId-referenced sig_hash items where MISSING legitimately means
   476|             # "no reference selected" (e.g. tick mark/leader arrowhead set to None),
   477|             # not a data gap -- same treatment as the pre-existing tick_mark_sig_hash.
   478|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
   479|                 "dim_type.tick_mark_sig_hash",
   480|                 "dim_type.leader_tick_mark_sig_hash",
   481|                 "dim_type.witness_line_tick_mark_sig_hash",
   482|                 "dim_type.centerline_tick_mark_sig_hash",
   483|                 "dim_type.interior_tick_mark_sig_hash",
   484|                 "dim_type.centerline_pattern_sig_hash",
   485|                 "dim_type.centerline_symbol_name",
   486|             })
   487| 
   488|             status_reasons = []
   489|             for it in identity_items:
   490|                 q = it.get("q")
   491|                 k = it.get("k", "")
   492|                 if q == ITEM_Q_OK:
   493|                     continue
   494|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
   495|                     continue
   496|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
   497| 
   498|             if blocked:
   499|                 status = STATUS_BLOCKED
   500|             elif status_reasons:
   501|                 status = STATUS_DEGRADED
   502|             else:
   503|                 status = STATUS_OK
   504| 
   505|             preimage = serialize_identity_items(identity_items)
   506|             sig_hash = None if blocked else make_hash(preimage)
   507| 
   508|             # Record ID from element ID integer
   509|             try:
   510|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
   511|             except Exception:
   512|                 type_id_int = None
   513| 
   514|             try:
   515|                 uid_raw = getattr(d, "UniqueId", None)
   516|             except Exception:
   517|                 uid_raw = None
   518| 
   519|             label_str = type_name
   520|             rec_v2 = build_record_v2(
   521|                 domain=DOMAIN_NAME,
   522|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
   523|                 status=status,
   524|                 status_reasons=sorted(set(status_reasons)),
   525|                 sig_hash=sig_hash,
   526|                 identity_items=identity_items,
   527|                 required_qs=tuple(required_qs),
   528|                 label={
   529|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
   530|                     "quality": "human" if label_str else "placeholder_missing",
   531|                     "provenance": "revit.DimensionType.params",
   532|                 },
   533|             )
   534|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
   535|             rec_v2["is_purgeable"] = _ip
   536|             rec_v2["is_purgeable_q"] = _ip_q
   537|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
   538| 
   539|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
   540|             rec_v2["join_key"], _missing = build_join_key_from_policy(
   541|                 domain_policy=pol,
   542|                 identity_items=identity_items,
   543|                 include_optional_items=False,
   544|                 emit_keys_used=True,
   545|                 hash_optional_items=False,
   546|                 emit_items=False,
   547|                 emit_selectors=True,
   548|             )
   549| 
   550|             # Canonical Name Identity Projection (PR1): second, independent join_hash
   551|             # variant keyed off this record's own label.display-backing item
   552|             # (dim_type.name). dim_type.name does not exist anywhere in this file --
   553|             # type_name/label_str feeds label.display only. Widened items list used
   554|             # only for this call; identity_basis.items/sig_hash/join_key above are
   555|             # unaffected. (dimension_types_spot_coordinate/spot_elevation are excluded
   556|             # from the name-key policy entirely -- their only other name-shaped item,
   557|             # dim_type.symbol_name, names a different, referenced tick-mark/leader
   558|             # symbol element, not this record's own label.)
   559|             dt_name_v, dt_name_q = canonicalize_str(type_name)
   560|             name_key_items = identity_items + [
   561|                 make_identity_item("dim_type.name", dt_name_v, dt_name_q)
   562|             ]
   563|             name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
   564|             rec_v2["join_key_name_identity"], _name_key_missing = build_join_key_from_policy(
   565|                 domain_policy=name_key_pol,
   566|                 identity_items=name_key_items,
   567|                 include_optional_items=False,
   568|                 emit_keys_used=True,
   569|                 hash_optional_items=False,
   570|                 emit_items=False,
   571|                 emit_selectors=True,
   572|             )
   573|             rec_v2["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _name_key_missing)
   574| 
   575|             # coordination_items
   576|             coordination_items = [
   577|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
   578|             ]
   579| 
   580|             # unknown_items (traceability only)
   581|             unknown_items = []
   582|             try:
   583|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
   584|             except Exception:
   585|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
   586|             try:
   587|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
   588|             except Exception:
   589|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
   590|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
   591|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
   592| 
   593|             rec_v2["phase2"] = {
   594|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
   595|                 "grouping_basis": "phase2.hypothesis",
   596|                 "cosmetic_items": phase2_sorted_items([]),
   597|                 "coordination_items": phase2_sorted_items(coordination_items),
   598|                 "unknown_items": phase2_sorted_items(unknown_items),
   599|             }
   600| 
   601|             if sig_hash:
   602|                 v2_sig_hashes.append(sig_hash)
   603|             v2_records.append(rec_v2)
   604| 
   605|         except Exception:
   606|             continue  # fail-soft per record
   607| 
   608|     _total_type_count = _eligible_type_count
   609|     for rec in v2_records:
   610|         try:
   611|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
   612|             rec["is_sole_type_in_category_q"] = "ok"
   613|         except Exception:
   614|             rec["is_sole_type_in_category"] = None
   615|             rec["is_sole_type_in_category_q"] = "unreadable"
   616| 
   617|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
   618|     info["count"] = len(v2_records)
   619|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
   620| 
   621|     if v2_sig_hashes:
   622|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
   623|         info["debug_v2_blocked"] = False
   624|     else:
   625|         info["hash_v2"] = None
   626|         info["debug_v2_blocked"] = True
   627|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
   628| 
   629|     return info
   630| 
   631| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
   632|     """
   633|     Heuristic override: if the FamilyName prefix indicates a Spot family,
   634|     force Spot classification so we skip this record (spot shapes have their own domain).
   635|     Returns updated (shape_v, shape_family, shape_q).
   636|     """
   637|     try:
   638|         family_name = getattr(d, "FamilyName", None)
   639|         basis = family_name if family_name else type_name
   640|         bn_l = safe_str(basis).strip().lower()
   641| 
   642|         if bn_l.startswith("spot slopes"):
   643|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
   644|         elif bn_l.startswith("spot elevations"):
   645|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
   646|         elif bn_l.startswith("spot coordinates"):
   647|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
   648|     except Exception:
   649|         pass
   650|     return (shape_v, shape_family, shape_q)
   651| 
   652| 
```
