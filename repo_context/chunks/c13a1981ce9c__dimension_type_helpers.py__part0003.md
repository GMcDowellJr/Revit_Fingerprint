# Chunk of core/dimension_type_helpers.py

- Source relative path: `core/dimension_type_helpers.py`
- Chunk: 3 of 3
- Original line range: 859-1091
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _read_element_ref_name, _read_line_pattern_ref_sig_hash, _build_alternate_units_items
- Source SHA-256: dc024129e8ca371f3567208f529d49c1000eb622f9944da778190d69805bdfd6
- Starts inside symbol: no
- Ends inside symbol: no

```
   859| def _read_element_ref_name(d, doc, ui_names):
   860|     """
   861|     Generic ElementId-parameter -> referenced element's display name
   862|     resolver, for fields that reference a named element but are NOT known
   863|     to be covered by ctx["arrowheads_by_type_id"] (e.g. Centerline Symbol,
   864|     which the probe's sample value ("ANG-Centerline") suggests is a
   865|     line-style/annotation symbol name rather than an arrowhead -- see
   866|     Area 7 §4 open question). Resolves by name only, not sig_hash, to
   867|     avoid asserting an unconfirmed ctx map lookup.
   868| 
   869|     NOT used for Centerline Pattern: that field is a genuine LinePatternId
   870|     reference (confirmed by the probe's -3000010 example, the same built-in
   871|     "Solid" pattern id domains/object_styles.py already special-cases) and
   872|     is resolved via _read_line_pattern_ref_sig_hash() instead, which handles
   873|     negative/built-in pattern ids through ctx["line_pattern_special_values"]
   874|     the same way object_styles.py/line_styles.py already do (PR #412 review:
   875|     this function's doc.GetElement() lookup returns None/missing for a
   876|     built-in pattern id, collapsing "Solid" and "no pattern" to the same
   877|     identity value).
   878| 
   879|     Unlike _read_arrowhead_ref_sig_hash (which treats a negative ElementId
   880|     as "no reference" -- built-in tick-mark constants like -2 mean "None"),
   881|     this treats only IntegerValue == 0/None as "no reference": negative
   882|     built-in ids in general are potentially real, resolvable references,
   883|     not necessarily a "none selected" state -- see _read_line_pattern_ref_sig_hash
   884|     for the field where that distinction actually matters.
   885|     """
   886|     try:
   887|         p = first_param(d, ui_names=ui_names)
   888|         if p is None or not getattr(p, "HasValue", False):
   889|             return (None, ITEM_Q_MISSING)
   890| 
   891|         eid = None
   892|         try:
   893|             eid = p.AsElementId()
   894|         except Exception:
   895|             return (None, ITEM_Q_UNREADABLE)
   896| 
   897|         if eid is None or getattr(eid, "IntegerValue", 0) == 0:
   898|             return (None, ITEM_Q_MISSING)
   899| 
   900|         elem = None
   901|         try:
   902|             elem = doc.GetElement(eid) if doc is not None else None
   903|         except Exception:
   904|             return (None, ITEM_Q_UNREADABLE)
   905| 
   906|         if elem is None:
   907|             return (None, ITEM_Q_MISSING)
   908| 
   909|         name = None
   910|         try:
   911|             name = get_type_display_name(elem)
   912|             if name in (None, S_MISSING, S_UNREADABLE):
   913|                 name = getattr(elem, "Name", None)
   914|         except Exception:
   915|             try:
   916|                 name = getattr(elem, "Name", None)
   917|             except Exception:
   918|                 name = None
   919| 
   920|         if name:
   921|             return canonicalize_str(name)
   922|         return (None, ITEM_Q_MISSING)
   923|     except Exception:
   924|         return (None, ITEM_Q_UNREADABLE)
   925| 
   926| 
   927| # ---------------------------------------------------------------------------
   928| # Line-Pattern-Reference Resolver (Centerline Pattern)
   929| # ---------------------------------------------------------------------------
   930| 
   931| def _read_line_pattern_ref_sig_hash(d, ctx, doc, ui_names):
   932|     """
   933|     ElementId-parameter -> line-pattern sig_hash resolver for fields that
   934|     reference a LinePatternElement (e.g. Centerline Pattern), mirroring the
   935|     established 3-tier resolution domains/object_styles.py and
   936|     domains/line_styles.py already use for their own line-pattern references
   937|     (Category.GetLinePatternId()) rather than a plain doc.GetElement() name
   938|     lookup, which returns None/missing for a negative/built-in pattern id
   939|     (PR #412 review) -- collapsing "Solid" and "no pattern selected" to the
   940|     same identity value and hash.
   941| 
   942|     Resolution order, matching object_styles.py/line_styles.py exactly:
   943|       1. IntegerValue > 0 and present in ctx["line_pattern_id_to_value"]
   944|          (pre-canonicalized value for well-known pattern ids).
   945|       2. IntegerValue > 0, not in (1): resolve doc.GetElement(id).UniqueId
   946|          through ctx["line_pattern_uid_to_hash"] (populated by
   947|          domains/line_patterns.py, which runs before dimension_types in
   948|          runner/run_dynamo.py -- same soft cross-domain dependency already
   949|          used by _read_arrowhead_ref_sig_hash/_read_tick_mark_sig_hash, no
   950|          hard require_domain() call).
   951|       3. IntegerValue <= 0 (a negative built-in pattern id, e.g. -3000010
   952|          for "Solid"): ctx["line_pattern_special_values"]["solid"].
   953|       4. No parameter/value at all: ITEM_Q_MISSING (a genuine "no centerline
   954|          pattern selected" state).
   955| 
   956|     Both (2) and (3) return ITEM_Q_UNREADABLE, not ITEM_Q_MISSING, when their
   957|     ctx map coverage is unavailable (line_patterns excluded from this run's
   958|     domain allowlist, or present but missing this specific id/sentinel): a
   959|     real reference (positive custom pattern, or negative built-in pattern
   960|     such as "Solid") that could not be resolved is an unresolved dependency,
   961|     not an absence. It must not be silently treated as "no pattern" or fall
   962|     into the caller's missing-is-acceptable exemption -- different custom or
   963|     built-in centerline patterns must not collapse to the same identity value
   964|     just because they're all unresolved (PR #412 review).
   965| 
   966|     Returns:
   967|         (sig_hash_v, sig_hash_q)
   968|     """
   969|     try:
   970|         p = first_param(d, ui_names=ui_names)
   971|         if p is None or not getattr(p, "HasValue", False):
   972|             return (None, ITEM_Q_MISSING)
   973| 
   974|         eid = None
   975|         try:
   976|             eid = p.AsElementId()
   977|         except Exception:
   978|             return (None, ITEM_Q_UNREADABLE)
   979| 
   980|         if eid is None:
   981|             return (None, ITEM_Q_MISSING)
   982| 
   983|         id_int = getattr(eid, "IntegerValue", 0)
   984| 
   985|         lp_id_to_value = (ctx or {}).get("line_pattern_id_to_value", {}) if ctx is not None else {}
   986|         if not isinstance(lp_id_to_value, dict):
   987|             lp_id_to_value = {}
   988|         lp_uid_to_sig_hash = (ctx or {}).get("line_pattern_uid_to_hash", None) if ctx is not None else None
   989|         lp_special_values = (ctx or {}).get("line_pattern_special_values", {}) if ctx is not None else {}
   990|         if not isinstance(lp_special_values, dict):
   991|             lp_special_values = {}
   992| 
   993|         if id_int > 0:
   994|             pid_key = safe_str(id_int)
   995|             if pid_key in lp_id_to_value:
   996|                 return canonicalize_str(lp_id_to_value.get(pid_key))
   997| 
   998|             if not isinstance(lp_uid_to_sig_hash, dict):
   999|                 # line_patterns didn't run / ctx map never populated for this
  1000|                 # extraction -- an unresolved dependency, not "no pattern selected".
  1001|                 return (None, ITEM_Q_UNREADABLE)
  1002| 
  1003|             try:
  1004|                 lp_elem = doc.GetElement(eid) if doc is not None else None
  1005|                 lp_uid = canon_str(getattr(lp_elem, "UniqueId", None)) if lp_elem else None
  1006|             except Exception:
  1007|                 return (None, ITEM_Q_UNREADABLE)
  1008| 
  1009|             if lp_uid and lp_uid in lp_uid_to_sig_hash:
  1010|                 # ctx["line_pattern_uid_to_hash"] maps uid -> sig_hash string directly
  1011|                 # (domains/line_patterns.py:500), unlike arrowheads_by_type_id's
  1012|                 # {type_id: {"sig_hash": ...}} shape.
  1013|                 sig_hash = lp_uid_to_sig_hash.get(lp_uid, None)
  1014|                 if sig_hash:
  1015|                     return (safe_str(sig_hash), ITEM_Q_OK)
  1016|             # Positive reference we could not resolve (uid missing, or not found
  1017|             # in the map) -- unresolved dependency, not "no pattern selected".
  1018|             return (None, ITEM_Q_UNREADABLE)
  1019|         else:
  1020|             solid_v = lp_special_values.get("solid", None)
  1021|             if solid_v:
  1022|                 return canonicalize_str(solid_v)
  1023|             # A real negative/built-in pattern reference (e.g. -3000010 for
  1024|             # "Solid") that we could not resolve to a known sentinel because
  1025|             # line_patterns didn't run / ctx["line_pattern_special_values"]
  1026|             # was never populated for this extraction -- an unresolved
  1027|             # dependency, not "no pattern selected" (PR #412 review). This
  1028|             # branch cannot import domains/line_patterns.py's
  1029|             # LINE_PATTERN_SYMBOLIC_SOLID constant directly to resolve it
  1030|             # independently -- Core must not depend on Domains.
  1031|             return (None, ITEM_Q_UNREADABLE)
  1032|     except Exception:
  1033|         return (None, ITEM_Q_UNREADABLE)
  1034| 
  1035| 
  1036| # ---------------------------------------------------------------------------
  1037| # Alternate Units Cluster (Area 7 §5)
  1038| # ---------------------------------------------------------------------------
  1039| 
  1040| def _build_alternate_units_items(d):
  1041|     """
  1042|     Read the Alternate Units cluster: master toggle, prefix, suffix.
  1043| 
  1044|     Per Greg's correction (Area 7 §5), Revit's UI repeats the Alternate Units
  1045|     parameter set across all dimension-type families -- observed on every
  1046|     shape in probe data (linear/angular/radial/diameter/all 3 spot families),
  1047|     not just Linear. Presence/absence is captured as real per-type signal,
  1048|     not gated to a subset of shapes the way Witness Lines/Centerline/Equality
  1049|     are.
  1050| 
  1051|     dim_type.alternate_units_format_id (the unit-type-id counterpart to
  1052|     dim_type.unit_format_id) was dropped: it required
  1053|     DimensionType.GetAlternateUnitsFormatOptions(), an accessor not
  1054|     confirmed to exist on the Revit surface this repo's probe data
  1055|     represents -- see _read_unit_format_info()'s docstring (PR #412 review).
  1056| 
  1057|     Returns a list of identity item dicts:
  1058|       - dim_type.alternate_units
  1059|       - dim_type.alternate_units_prefix
  1060|       - dim_type.alternate_units_suffix
  1061|     """
  1062|     items = []
  1063| 
  1064|     # alternate_units (master toggle)
  1065|     try:
  1066|         p_au = first_param(d, ui_names=["Alternate Units"])
  1067|         au_int = _as_int(p_au) if p_au is not None else None
  1068|         au_v, au_q = canonicalize_bool(au_int)
  1069|     except Exception:
  1070|         au_v, au_q = (None, ITEM_Q_UNREADABLE)
  1071|     items.append(make_identity_item("dim_type.alternate_units", au_v, au_q))
  1072| 
  1073|     # alternate_units_prefix
  1074|     try:
  1075|         p_pfx = first_param(d, ui_names=["Alternate Units Prefix"])
  1076|         pfx_raw = _as_string(p_pfx) if p_pfx is not None else None
  1077|         pfx_v, pfx_q = canonicalize_str_allow_empty(pfx_raw)
  1078|     except Exception:
  1079|         pfx_v, pfx_q = (None, ITEM_Q_UNREADABLE)
  1080|     items.append(make_identity_item("dim_type.alternate_units_prefix", pfx_v, pfx_q))
  1081| 
  1082|     # alternate_units_suffix
  1083|     try:
  1084|         p_sfx = first_param(d, ui_names=["Alternate Units Suffix"])
  1085|         sfx_raw = _as_string(p_sfx) if p_sfx is not None else None
  1086|         sfx_v, sfx_q = canonicalize_str_allow_empty(sfx_raw)
  1087|     except Exception:
  1088|         sfx_v, sfx_q = (None, ITEM_Q_UNREADABLE)
  1089|     items.append(make_identity_item("dim_type.alternate_units_suffix", sfx_v, sfx_q))
  1090| 
  1091|     return items
```
