# Chunk of core/dimension_type_helpers.py

- Source relative path: `core/dimension_type_helpers.py`
- Chunk: 2 of 3
- Original line range: 402-858
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _build_text_appearance_items, _read_tick_mark_sig_hash, _read_unit_format_info, _read_unit_format_info._units_fo_not_applicable, _read_prefix_suffix, _read_leader_arrowhead, _read_arrowhead_ref_sig_hash
- Source SHA-256: dc024129e8ca371f3567208f529d49c1000eb622f9944da778190d69805bdfd6
- Starts inside symbol: no
- Ends inside symbol: no

```
   402| def _build_text_appearance_items(d):
   403|     """
   404|     Extract text/appearance identity items common to all dimension type shapes.
   405| 
   406|     Returns a list of identity item dicts for:
   407|       - dim_type.text_font
   408|       - dim_type.text_size_in
   409|       - dim_type.text_bold
   410|       - dim_type.text_italic
   411|       - dim_type.text_underline
   412|       - dim_type.text_width_factor
   413|       - dim_type.text_background
   414|       - dim_type.color_rgb
   415|       - dim_type.line_weight
   416| 
   417|     These items are always included regardless of shape.
   418|     """
   419|     items = []
   420| 
   421|     # text_font
   422|     try:
   423|         p_font = first_param(
   424|             d,
   425|             bip_names=["TEXT_FONT", "DIM_TEXT_FONT", "SPOT_ELEV_TEXT_FONT", "SPOT_COORDINATE_TEXT_FONT"],
   426|             ui_names=["Text Font"],
   427|         )
   428|         font_raw = _as_string(p_font) if p_font is not None else None
   429|         font_v, font_q = canonicalize_str(font_raw)
   430|     except Exception:
   431|         font_v, font_q = (None, ITEM_Q_UNREADABLE)
   432|     items.append(make_identity_item("dim_type.text_font", font_v, font_q))
   433| 
   434|     # text_size_in (stored as feet, converted to inches)
   435|     try:
   436|         p_size = first_param(
   437|             d,
   438|             bip_names=["TEXT_SIZE", "DIM_TEXT_SIZE", "SPOT_ELEV_TEXT_SIZE", "SPOT_COORDINATE_TEXT_SIZE"],
   439|             ui_names=["Text Size"],
   440|         )
   441|         size_ft = _as_double(p_size) if p_size is not None else None
   442|         if size_ft is not None:
   443|             size_in_str = _fmt_in_from_ft(size_ft)
   444|             size_v, size_q = canonicalize_float(size_in_str)
   445|         else:
   446|             size_v, size_q = (None, ITEM_Q_MISSING)
   447|     except Exception:
   448|         size_v, size_q = (None, ITEM_Q_UNREADABLE)
   449|     items.append(make_identity_item("dim_type.text_size_in", size_v, size_q))
   450| 
   451|     # text_bold
   452|     try:
   453|         p_bold = first_param(d, ui_names=["Bold"])
   454|         bold_int = _as_int(p_bold) if p_bold is not None else None
   455|         bold_v, bold_q = canonicalize_bool(bold_int)
   456|     except Exception:
   457|         bold_v, bold_q = (None, ITEM_Q_UNREADABLE)
   458|     items.append(make_identity_item("dim_type.text_bold", bold_v, bold_q))
   459| 
   460|     # text_italic
   461|     try:
   462|         p_italic = first_param(d, ui_names=["Italic"])
   463|         italic_int = _as_int(p_italic) if p_italic is not None else None
   464|         italic_v, italic_q = canonicalize_bool(italic_int)
   465|     except Exception:
   466|         italic_v, italic_q = (None, ITEM_Q_UNREADABLE)
   467|     items.append(make_identity_item("dim_type.text_italic", italic_v, italic_q))
   468| 
   469|     # text_underline
   470|     try:
   471|         p_underline = first_param(d, ui_names=["Underline"])
   472|         underline_int = _as_int(p_underline) if p_underline is not None else None
   473|         underline_v, underline_q = canonicalize_bool(underline_int)
   474|     except Exception:
   475|         underline_v, underline_q = (None, ITEM_Q_UNREADABLE)
   476|     items.append(make_identity_item("dim_type.text_underline", underline_v, underline_q))
   477| 
   478|     # text_width_factor
   479|     try:
   480|         p_wf = first_param(d, ui_names=["Width Factor"])
   481|         wf_raw = _as_double(p_wf) if p_wf is not None else None
   482|         wf_v, wf_q = canonicalize_float(wf_raw)
   483|     except Exception:
   484|         wf_v, wf_q = (None, ITEM_Q_UNREADABLE)
   485|     items.append(make_identity_item("dim_type.text_width_factor", wf_v, wf_q))
   486| 
   487|     # text_background (storage=Integer/enum — use AsValueString; probe shows display='Opaque')
   488|     try:
   489|         p_bg = first_param(d, ui_names=["Text Background"])
   490|         bg_raw = _as_value_string(p_bg) if p_bg is not None else None
   491|         bg_v, bg_q = canonicalize_str(bg_raw)
   492|     except Exception:
   493|         bg_v, bg_q = (None, ITEM_Q_UNREADABLE)
   494|     items.append(make_identity_item("dim_type.text_background", bg_v, bg_q))
   495| 
   496|     # color_rgb — canonicalize dict to "r-g-b" string before storing
   497|     try:
   498|         _color_int, color_rgb_raw = try_get_color_rgb_from_elem(d)
   499|         color_rgb_str = _canon_rgb(color_rgb_raw)
   500|         if color_rgb_str is not None:
   501|             color_v, color_q = canonicalize_str(color_rgb_str)
   502|         else:
   503|             color_v, color_q = (None, ITEM_Q_MISSING)
   504|     except Exception:
   505|         color_v, color_q = (None, ITEM_Q_UNREADABLE)
   506|     items.append(make_identity_item("dim_type.color_rgb", color_v, color_q))
   507| 
   508|     # line_weight
   509|     try:
   510|         p_lw = first_param(
   511|             d,
   512|             bip_names=["LINE_WEIGHT", "DIM_LINE_WEIGHT"],
   513|             ui_names=["Line Weight"],
   514|         )
   515|         lw_raw = _as_int(p_lw) if p_lw is not None else None
   516|         lw_v, lw_q = canonicalize_int(lw_raw)
   517|     except Exception:
   518|         lw_v, lw_q = (None, ITEM_Q_UNREADABLE)
   519|     items.append(make_identity_item("dim_type.line_weight", lw_v, lw_q))
   520| 
   521|     return items
   522| 
   523| 
   524| # ---------------------------------------------------------------------------
   525| # Tick Mark Sig Hash Reader
   526| # ---------------------------------------------------------------------------
   527| 
   528| def _read_tick_mark_sig_hash(d, ctx, doc=None):
   529|     """
   530|     Read the tick mark parameter and return (sig_hash_v, sig_hash_q) using
   531|     the ctx arrowheads_by_type_id map.
   532| 
   533|     Returns:
   534|         (sig_hash_v, sig_hash_q) where:
   535|           - sig_hash_v: str hash or None
   536|           - sig_hash_q: ITEM_Q_OK if found, ITEM_Q_MISSING if not found/none
   537|     """
   538|     tick_sig_hash = None
   539| 
   540|     try:
   541|         p_tick = first_param(
   542|             d,
   543|             bip_names=["DIM_LEADER_ARROWHEAD", "TICK_MARK", "DIM_TICK_MARK"],
   544|             ui_names=["Tick Mark"],
   545|         )
   546| 
   547|         if p_tick is not None and getattr(p_tick, "HasValue", False):
   548|             tid = None
   549|             try:
   550|                 tid = p_tick.AsElementId()
   551|             except Exception:
   552|                 tid = None
   553| 
   554|             if tid is not None and getattr(tid, "IntegerValue", 0) > 0:
   555|                 # Try ctx lookup first (preferred - UID-free)
   556|                 try:
   557|                     ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
   558|                     k = safe_str(getattr(tid, "IntegerValue", None))
   559|                     if k and isinstance(ah_map, dict) and k in ah_map:
   560|                         tick_sig_hash = ah_map.get(k, {}).get("sig_hash", None)
   561|                 except Exception:
   562|                     tick_sig_hash = None
   563| 
   564|     except Exception:
   565|         tick_sig_hash = None
   566| 
   567|     if tick_sig_hash:
   568|         return (safe_str(tick_sig_hash), ITEM_Q_OK)
   569|     else:
   570|         return (None, ITEM_Q_MISSING)
   571| 
   572| 
   573| # ---------------------------------------------------------------------------
   574| # Unit Format Info Reader
   575| # ---------------------------------------------------------------------------
   576| 
   577| def _read_unit_format_info(d):
   578|     """
   579|     Read UnitsFormatOptions from a DimensionType and return a tuple of
   580|     (unit_format_id_v, unit_format_id_q, rounding_v, rounding_q, accuracy_v, accuracy_q,
   581|      suppress_spaces_v, suppress_spaces_q).
   582| 
   583|     Handles UseDefault by returning ("use_default", ITEM_Q_OK) for all four.
   584|     Handles unsupported (e.g., SpotSlope) by returning
   585|     (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE).
   586| 
   587|     suppress_spaces (Area 7 §6) is read off the same FormatOptions object as
   588|     rounding/accuracy -- same FormatOptions-boolean-flag gap Area 8 documented
   589|     for units.py, just on DimensionType.GetUnitsFormatOptions() instead of the
   590|     doc-level Units object.
   591| 
   592|     NOTE: an earlier revision of this function accepted an alternate= flag
   593|     selecting DimensionType.GetAlternateUnitsFormatOptions() for Area 7 §5's
   594|     dim_type.alternate_units_format_id field. That accessor does not exist on
   595|     the Revit surface this repo's committed probe data represents (raises
   596|     AttributeError there), so every call fell through to
   597|     ITEM_Q_UNSUPPORTED_NOT_APPLICABLE -- making dim_type.alternate_units_format_id
   598|     degrade every dimension-type record without ever capturing real data
   599|     (PR #412 review). The field and the alternate= parameter were removed
   600|     rather than shipped against an unverified accessor; dim_type.alternate_units
   601|     (the master toggle) and dim_type.alternate_units_prefix/_suffix (plain
   602|     String parameters, unaffected by this issue) are retained.
   603|     """
   604| 
   605|     def _units_fo_not_applicable(ex):
   606|         msg = safe_str(getattr(ex, "Message", None) or ex)
   607|         tname = safe_str(getattr(type(ex), "__name__", "")).lower()
   608|         msg_l = msg.lower()
   609|         return (
   610|             "notsupported" in tname
   611|             or "invalidoperation" in tname
   612|             or "attributeerror" in tname
   613|             or "not supported" in msg_l
   614|             or "not applicable" in msg_l
   615|             or "unsupported" in msg_l
   616|         )
   617| 
   618|     unit_format_id_v = None
   619|     unit_format_id_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
   620|     rounding_v = None
   621|     rounding_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
   622|     accuracy_v = None
   623|     accuracy_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
   624|     suppress_spaces_v = None
   625|     suppress_spaces_q = ITEM_Q_UNSUPPORTED_NOT_APPLICABLE
   626| 
   627|     fo = None
   628|     fo_exc = None
   629|     try:
   630|         fo = d.GetUnitsFormatOptions()
   631|     except Exception as ex:
   632|         fo_exc = ex
   633| 
   634|     if fo is None:
   635|         if fo_exc is not None and (not _units_fo_not_applicable(fo_exc)):
   636|             unit_format_id_q = ITEM_Q_UNREADABLE
   637|             rounding_q = ITEM_Q_UNREADABLE
   638|             accuracy_q = ITEM_Q_UNREADABLE
   639|             suppress_spaces_q = ITEM_Q_UNREADABLE
   640|         # else: leave as UNSUPPORTED_NOT_APPLICABLE
   641|     else:
   642|         use_default = getattr(fo, "UseDefault", None)
   643|         if use_default is True:
   644|             unit_format_id_v, unit_format_id_q = ("use_default", ITEM_Q_OK)
   645|             rounding_v, rounding_q = ("use_default", ITEM_Q_OK)
   646|             accuracy_v, accuracy_q = ("use_default", ITEM_Q_OK)
   647|             suppress_spaces_v, suppress_spaces_q = ("use_default", ITEM_Q_OK)
   648|         else:
   649|             try:
   650|                 forge_type_id_obj = fo.GetUnitTypeId()
   651|                 uid_str = getattr(forge_type_id_obj, "TypeId", None)
   652|                 if uid_str is None:
   653|                     uid_str = forge_type_id_obj.ToString()
   654|                 unit_format_id_v, unit_format_id_q = canonicalize_str(str(uid_str))
   655|             except Exception:
   656|                 unit_format_id_v, unit_format_id_q = (None, ITEM_Q_UNREADABLE)
   657| 
   658|             try:
   659|                 rounding_v, rounding_q = canonicalize_enum(getattr(fo, "RoundingMethod", None))
   660|             except Exception:
   661|                 rounding_v, rounding_q = (None, ITEM_Q_UNREADABLE)
   662| 
   663|             try:
   664|                 accuracy_v, accuracy_q = canonicalize_float(_fmt_in_from_ft(getattr(fo, "Accuracy", None)))
   665|             except Exception:
   666|                 accuracy_v, accuracy_q = (None, ITEM_Q_UNREADABLE)
   667| 
   668|             try:
   669|                 suppress_spaces_v, suppress_spaces_q = canonicalize_bool(getattr(fo, "SuppressSpaces", None))
   670|             except Exception:
   671|                 suppress_spaces_v, suppress_spaces_q = (None, ITEM_Q_UNREADABLE)
   672| 
   673|     return (
   674|         unit_format_id_v, unit_format_id_q,
   675|         rounding_v, rounding_q,
   676|         accuracy_v, accuracy_q,
   677|         suppress_spaces_v, suppress_spaces_q,
   678|     )
   679| 
   680| 
   681| # ---------------------------------------------------------------------------
   682| # Prefix/Suffix Reader
   683| # ---------------------------------------------------------------------------
   684| 
   685| def _read_prefix_suffix(d):
   686|     """
   687|     Read Prefix and Suffix properties from a DimensionType.
   688| 
   689|     Returns:
   690|         (prefix_v, prefix_q, suffix_v, suffix_q)
   691|     """
   692|     prefix_v, prefix_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
   693|     suffix_v, suffix_q = (None, ITEM_Q_UNSUPPORTED_NOT_APPLICABLE)
   694| 
   695|     if hasattr(d, "Prefix"):
   696|         try:
   697|             raw = getattr(d, "Prefix", "")
   698|             if raw is None:
   699|                 raw = ""
   700|             prefix_v, prefix_q = (safe_str(raw), ITEM_Q_OK)
   701|         except Exception:
   702|             prefix_v, prefix_q = (None, ITEM_Q_UNREADABLE)
   703| 
   704|     if hasattr(d, "Suffix"):
   705|         try:
   706|             raw = getattr(d, "Suffix", "")
   707|             if raw is None:
   708|                 raw = ""
   709|             suffix_v, suffix_q = (safe_str(raw), ITEM_Q_OK)
   710|         except Exception:
   711|             suffix_v, suffix_q = (None, ITEM_Q_UNREADABLE)
   712| 
   713|     return (prefix_v, prefix_q, suffix_v, suffix_q)
   714| 
   715| 
   716| # ---------------------------------------------------------------------------
   717| # Leader Arrowhead Reader (Area 7 §1 -- spot-family leader/arrowhead cluster)
   718| # ---------------------------------------------------------------------------
   719| 
   720| def _read_leader_arrowhead(d, ctx, doc):
   721|     """
   722|     Read the Leader Arrowhead parameter (BuiltInParameter.LEADER_ARROWHEAD)
   723|     and resolve it to uid/name/sig_hash, mirroring the working pattern
   724|     already shipped in domains/text_types.py (same field, read for the
   725|     text_types domain). Shared here so the 3 spot dimension_types partitions
   726|     (extract_spot_elevation/_spot_coordinate/_spot_slope) don't each
   727|     duplicate the read-and-resolve logic.
   728| 
   729|     A positive Leader Arrowhead reference that resolves to a real element but
   730|     is absent from ctx["arrowheads_by_type_id"] (e.g. the arrowheads domain
   731|     excluded from this run's domain allowlist, or its record blocked) yields
   732|     sig_hash_q=ITEM_Q_UNREADABLE, not ITEM_Q_MISSING: it is an unresolved
   733|     dependency, not "no arrowhead selected" -- spot types using different
   734|     arrowhead definitions must not silently collapse to the same hash just
   735|     because they're both unresolved (PR #412 review).
   736| 
   737|     Returns:
   738|         (uid_v, uid_q, name_v, name_q, sig_hash_v, sig_hash_q)
   739|     """
   740|     uid_v, uid_q = (None, ITEM_Q_MISSING)
   741|     name_v, name_q = (None, ITEM_Q_MISSING)
   742|     sig_hash_v, sig_hash_q = (None, ITEM_Q_MISSING)
   743| 
   744|     try:
   745|         p_arrow = first_param(d, bip_names=["LEADER_ARROWHEAD"], ui_names=["Leader Arrowhead"])
   746|         if p_arrow is not None and getattr(p_arrow, "HasValue", False):
   747|             arrow_id = None
   748|             try:
   749|                 arrow_id = p_arrow.AsElementId()
   750|             except Exception:
   751|                 arrow_id = None
   752| 
   753|             if arrow_id is not None and getattr(arrow_id, "IntegerValue", 0) > 0:
   754|                 arrow = None
   755|                 try:
   756|                     arrow = doc.GetElement(arrow_id) if doc is not None else None
   757|                 except Exception:
   758|                     arrow = None
   759| 
   760|                 if arrow is not None:
   761|                     try:
   762|                         arrow_uid = getattr(arrow, "UniqueId", None)
   763|                         uid_v, uid_q = canonicalize_str(arrow_uid) if arrow_uid else (None, ITEM_Q_MISSING)
   764|                     except Exception:
   765|                         uid_v, uid_q = (None, ITEM_Q_UNREADABLE)
   766| 
   767|                     try:
   768|                         arrow_name = get_type_display_name(arrow)
   769|                         if arrow_name in (None, S_MISSING, S_UNREADABLE):
   770|                             arrow_name = getattr(arrow, "Name", None)
   771|                         name_v, name_q = canonicalize_str(arrow_name)
   772|                     except Exception:
   773|                         name_v, name_q = (None, ITEM_Q_UNREADABLE)
   774| 
   775|                     try:
   776|                         ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
   777|                         k = safe_str(getattr(arrow_id, "IntegerValue", None))
   778|                         sh = None
   779|                         if k and isinstance(ah_map, dict) and k in ah_map:
   780|                             sh = ah_map.get(k, {}).get("sig_hash", None)
   781|                         if sh:
   782|                             sig_hash_v, sig_hash_q = (safe_str(sh), ITEM_Q_OK)
   783|                         else:
   784|                             # Positive reference, real element resolved, but no
   785|                             # sig_hash available -- unresolved dependency.
   786|                             sig_hash_v, sig_hash_q = (None, ITEM_Q_UNREADABLE)
   787|                     except Exception:
   788|                         sig_hash_v, sig_hash_q = (None, ITEM_Q_UNREADABLE)
   789|     except Exception:
   790|         pass
   791| 
   792|     return (uid_v, uid_q, name_v, name_q, sig_hash_v, sig_hash_q)
   793| 
   794| 
   795| # ---------------------------------------------------------------------------
   796| # Generic tick-mark-family ElementId -> arrowheads sig_hash resolver
   797| # ---------------------------------------------------------------------------
   798| 
   799| def _read_arrowhead_ref_sig_hash(d, ctx, bip_names=None, ui_names=None):
   800|     """
   801|     Generic ElementId-parameter -> ctx["arrowheads_by_type_id"] sig_hash
   802|     resolver, generalizing the pattern already used by _read_tick_mark_sig_hash
   803|     (kept separate/unchanged to avoid touching its established Tick Mark
   804|     call sites). Shared by the other tick-mark-style fields added in
   805|     Area 7: Leader Tick Mark (§2), Centerline Tick Mark, Interior Tick Mark
   806|     (§4), and Witness Line Tick Mark (§3) -- all reference an
   807|     arrowhead/tick-mark-style element the same way the existing Tick Mark
   808|     field does.
   809| 
   810|     Returns:
   811|         (sig_hash_v, sig_hash_q) where sig_hash_q is:
   812|           - ITEM_Q_OK if resolved
   813|           - ITEM_Q_MISSING for a genuine "no reference" state: no
   814|             parameter/value, or a negative/built-in id (e.g. a "None"
   815|             tick-mark selection)
   816|           - ITEM_Q_UNREADABLE for a positive reference that could not be
   817|             resolved (ctx["arrowheads_by_type_id"] absent entirely -- e.g.
   818|             arrowheads excluded from this run's domain allowlist -- or
   819|             present but missing this id) -- an unresolved dependency, not
   820|             an absence; distinct custom tick marks must not collapse to the
   821|             same hash just because they're both unresolved (PR #412 review)
   822|     """
   823|     sig_hash = None
   824|     positive_unresolved = False
   825| 
   826|     try:
   827|         p = first_param(d, bip_names=bip_names, ui_names=ui_names)
   828|         if p is not None and getattr(p, "HasValue", False):
   829|             eid = None
   830|             try:
   831|                 eid = p.AsElementId()
   832|             except Exception:
   833|                 return (None, ITEM_Q_UNREADABLE)
   834| 
   835|             if eid is not None and getattr(eid, "IntegerValue", 0) > 0:
   836|                 try:
   837|                     ah_map = (ctx or {}).get("arrowheads_by_type_id", {}) if ctx is not None else {}
   838|                     k = safe_str(getattr(eid, "IntegerValue", None))
   839|                     if k and isinstance(ah_map, dict) and k in ah_map:
   840|                         sig_hash = ah_map.get(k, {}).get("sig_hash", None)
   841|                     if not sig_hash:
   842|                         positive_unresolved = True
   843|                 except Exception:
   844|                     positive_unresolved = True
   845|     except Exception:
   846|         return (None, ITEM_Q_UNREADABLE)
   847| 
   848|     if sig_hash:
   849|         return (safe_str(sig_hash), ITEM_Q_OK)
   850|     if positive_unresolved:
   851|         return (None, ITEM_Q_UNREADABLE)
   852|     return (None, ITEM_Q_MISSING)
   853| 
   854| 
   855| # ---------------------------------------------------------------------------
   856| # Generic named-element-reference resolver (no ctx sig_hash map coverage)
   857| # ---------------------------------------------------------------------------
   858| 
```
