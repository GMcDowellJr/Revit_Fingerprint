# Chunk of domains/view_templates.py

- Source relative path: `domains/view_templates.py`
- Chunk: 1 of 6
- Original line range: 1-421
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _collect_templates, _non_ctrl_bips_from_view, _is_template_param_included, _append_assigned_view_count_cosmetic_item, _append_phase_filter_value, _append_filter_stack_signature, _append_workset_visibility, _phase2_items_from_def_signature, _canonical_identity_items_from_signature, _semantic_keys_from_identity_items, _build_floor_structural_area_viewtype_set, _build_ceiling_plan_viewtype_set
- Source SHA-256: ca478c676990e318341a80d987cc318a4531ef7d17b52cb5fd1b41c67678296d
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| 
     2| # -*- coding: utf-8 -*-
     3| """View Templates domain family extractor."""
     4| 
     5| import os
     6| import sys
     7| 
     8| current_dir = os.path.dirname(os.path.abspath(__file__))
     9| repo_root = os.path.dirname(current_dir)
    10| if repo_root not in sys.path:
    11|     sys.path.insert(0, repo_root)
    12| 
    13| from core.hashing import make_hash, safe_str
    14| from core.deps import require_domain, Blocked
    15| from core.collect import purge_lookup, collect_instances
    16| from core.canon import canon_str, fnum, canon_num, canon_bool, canon_id, S_MISSING, S_UNREADABLE, S_NOT_APPLICABLE
    17| from core.record_v2 import (
    18|     STATUS_OK,
    19|     STATUS_DEGRADED,
    20|     STATUS_BLOCKED,
    21|     ITEM_Q_MISSING,
    22|     ITEM_Q_OK,
    23|     ITEM_Q_UNREADABLE,
    24|     canonicalize_int,
    25|     canonicalize_str,
    26|     build_record_v2,
    27|     make_identity_item,
    28|     make_record_id_from_element,
    29|     serialize_identity_items,
    30| )
    31| from core.phase2 import phase2_sorted_items, phase2_qv_from_legacy_sentinel_str, phase2_join_hash
    32| from core.join_key_policy import get_domain_join_key_policy
    33| from core.join_key_builder import build_join_key_from_policy, compute_projection_status
    34| from core.graphic_overrides import extract_projection_graphics, extract_cut_graphics, extract_halftone, extract_transparency
    35| from core.vg_sig import _traceability_unknown_items, emit_builtin_params, emit_shared_params_stub
    36| 
    37| try:
    38|     from Autodesk.Revit.DB import View, ViewSchedule, BuiltInParameter
    39| except Exception as e:
    40|     View = None
    41|     ViewSchedule = None
    42|     BuiltInParameter = None
    43| 
    44| try:
    45|     from Autodesk.Revit.DB import FilteredWorksetCollector, WorksetKind
    46| except Exception:
    47|     FilteredWorksetCollector = None
    48|     WorksetKind = None
    49| 
    50| _CTX_TEMPLATES_CACHE_KEY = "_view_templates_cache"
    51| 
    52| # Canonical cache key for all-View-instances collection.
    53| # All view_templates_* domains use this key so FEC runs once per extraction run.
    54| _VIEW_INSTANCES_CACHE_KEY = "view_instances:View:all"
    55| 
    56| 
    57| def _collect_templates(doc, ctx):
    58|     if ctx is not None and _CTX_TEMPLATES_CACHE_KEY in ctx:
    59|         return ctx[_CTX_TEMPLATES_CACHE_KEY]
    60|     col = list(
    61|         collect_instances(
    62|             doc,
    63|             of_class=View,
    64|             require_unique_id=True,
    65|             cctx=(ctx or {}).get("_collect") if ctx is not None else None,
    66|             cache_key=_VIEW_INSTANCES_CACHE_KEY,
    67|         )
    68|     )
    69|     if ctx is not None:
    70|         ctx[_CTX_TEMPLATES_CACHE_KEY] = col
    71|     return col
    72| 
    73| 
    74| def _non_ctrl_bips_from_view(v):
    75|     try:
    76|         non_ctrl_ids = v.GetNonControlledTemplateParameterIds() or []
    77|         return set(
    78|             pid.IntegerValue for pid in non_ctrl_ids
    79|             if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
    80|         )
    81|     except Exception:
    82|         return set()
    83| 
    84| 
    85| def _is_template_param_included(non_ctrl_bips, bip_name):
    86|     if BuiltInParameter is None or not non_ctrl_bips:
    87|         return False
    88|     try:
    89|         return int(getattr(BuiltInParameter, bip_name)) not in non_ctrl_bips
    90|     except Exception:
    91|         return False
    92| 
    93| 
    94| def _append_assigned_view_count_cosmetic_item(rec, doc, v, ctx):
    95|     assigned_count = 0
    96|     try:
    97|         col = collect_instances(
    98|             doc,
    99|             of_class=View,
   100|             require_unique_id=False,
   101|             cctx=(ctx or {}).get("_collect") if ctx is not None else None,
   102|             cache_key="view_templates:all_view_instances",
   103|         )
   104|         template_id = getattr(v, "Id", None)
   105|         assigned_count = sum(
   106|             1 for view in (col or [])
   107|             if not getattr(view, "IsTemplate", False)
   108|             and getattr(view, "ViewTemplateId", None) == template_id
   109|         )
   110|     except Exception:
   111|         assigned_count = None
   112| 
   113|     if assigned_count is not None:
   114|         ac_v, ac_q = canonicalize_int(assigned_count)
   115|     else:
   116|         ac_v, ac_q = (None, ITEM_Q_UNREADABLE)
   117| 
   118|     assigned_item = make_identity_item("vt.assigned_view_count", ac_v, ac_q)
   119|     rec["phase2"]["cosmetic_items"] = list(rec["phase2"].get("cosmetic_items") or []) + [assigned_item]
   120| 
   121| 
   122| def _append_phase_filter_value(
   123|     v,
   124|     doc,
   125|     include_pf,
   126|     phase_filter_map,
   127|     phase_filter_map_v2,
   128|     sig,
   129|     sig_v2,
   130|     v2_ok,
   131|     v2_block_fn,
   132|     debug_counters=None,
   133| ):
   134|     sentinel = None
   135|     try:
   136|         p = v.get_Parameter(BuiltInParameter.VIEW_PHASE_FILTER)
   137|         has_value = bool(getattr(p, "HasValue", False)) if p is not None else False
   138|         if p is None or not has_value:
   139|             sentinel = S_MISSING if include_pf else S_NOT_APPLICABLE
   140|         else:
   141|             pf_id = p.AsElementId()
   142|             if not pf_id or canon_id(pf_id) == S_MISSING:
   143|                 sentinel = S_MISSING if include_pf else S_NOT_APPLICABLE
   144|             else:
   145|                 pf_elem = doc.GetElement(pf_id)
   146|                 if pf_elem:
   147|                     pf_uid = canon_str(getattr(pf_elem, "UniqueId", None))
   148|                     pf_hash = phase_filter_map.get(pf_uid) if pf_uid else None
   149|                     if pf_hash:
   150|                         sig.append("phase_filter={}".format(canon_str(pf_hash)))
   151|                         if v2_ok:
   152|                             pf_hash_v2 = None
   153|                             try:
   154|                                 pf_hash_v2 = phase_filter_map_v2.get(pf_uid) if pf_uid else None
   155|                             except Exception:
   156|                                 pf_hash_v2 = None
   157|                             if pf_hash_v2:
   158|                                 sig_v2.append("phase_filter_hash={}".format(canon_str(pf_hash_v2)))
   159|                             elif include_pf:
   160|                                 v2_block_fn("phase_filter_unresolved")
   161|                                 v2_ok = False
   162|                         return v2_ok
   163|                 sentinel = S_UNREADABLE if include_pf else S_NOT_APPLICABLE
   164|     except Exception:
   165|         if debug_counters is not None:
   166|             debug_counters["debug_fail_read"] = debug_counters.get("debug_fail_read", 0) + 1
   167|         sentinel = S_UNREADABLE if include_pf else S_NOT_APPLICABLE
   168| 
   169|     sig.append("phase_filter={}".format(sentinel))
   170|     if include_pf and sentinel in (S_UNREADABLE, S_MISSING) and v2_ok:
   171|         v2_block_fn("phase_filter_unresolved")
   172|         v2_ok = False
   173|     return v2_ok
   174| 
   175| 
   176| def _append_filter_stack_signature(v, doc, view_filter_map, sig, sig_v2, v2_ok, v2_block_fn):
   177|     try:
   178|         filter_ids = list(v.GetFilters() or []) if hasattr(v, "GetFilters") else []
   179|         sig.append("filter_stack_count={}".format(len(filter_ids)))
   180|         if v2_ok:
   181|             sig_v2.append("vts.filter_stack_count={}".format(canon_str(len(filter_ids))))
   182|     except Exception:
   183|         filter_ids = None
   184|         sig.append("filter_stack_count=<UNREADABLE>")
   185|         if v2_ok:
   186|             v2_block_fn("filter_stack_unreadable")
   187|             v2_ok = False
   188| 
   189|     if filter_ids is None:
   190|         return v2_ok
   191| 
   192|     for i, fid in enumerate(filter_ids):
   193|         idx3 = "{:03d}".format(i)
   194| 
   195|         f_uid = None
   196|         try:
   197|             fe = doc.GetElement(fid)
   198|             f_uid = canon_str(getattr(fe, "UniqueId", None)) if fe is not None else None
   199|         except Exception:
   200|             f_uid = None
   201| 
   202|         def_sig = view_filter_map.get(f_uid) if f_uid else None
   203| 
   204|         if def_sig:
   205|             sig.append("filter[{}].def_sig={}".format(idx3, canon_str(def_sig)))
   206|             if v2_ok:
   207|                 sig_v2.append("vts.filter[{}].def_sig_hash={}".format(idx3, canon_str(def_sig)))
   208|         else:
   209|             sig.append("filter[{}].def_sig=<UNREADABLE>".format(idx3))
   210|             if v2_ok:
   211|                 v2_block_fn("view_filter_unresolved")
   212|                 v2_ok = False
   213| 
   214|         try:
   215|             enabled = bool(v.GetIsFilterEnabled(fid)) if hasattr(v, "GetIsFilterEnabled") else None
   216|         except Exception:
   217|             enabled = None
   218| 
   219|         if enabled is None:
   220|             sig.append("filter[{}].enabled=<UNREADABLE>".format(idx3))
   221|             if v2_ok:
   222|                 v2_block_fn("filter_enabled_unreadable")
   223|                 v2_ok = False
   224|         else:
   225|             sig.append("filter[{}].enabled={}".format(idx3, int(enabled)))
   226|             if v2_ok:
   227|                 sig_v2.append("vts.filter[{}].enabled={}".format(idx3, int(enabled)))
   228| 
   229|         try:
   230|             vis = bool(v.GetFilterVisibility(fid)) if hasattr(v, "GetFilterVisibility") else None
   231|         except Exception:
   232|             vis = None
   233| 
   234|         if vis is None:
   235|             sig.append("filter[{}].vis=<UNREADABLE>".format(idx3))
   236|             if v2_ok:
   237|                 v2_block_fn("filter_visibility_unreadable")
   238|                 v2_ok = False
   239|         else:
   240|             sig.append("filter[{}].vis={}".format(idx3, int(vis)))
   241|             if v2_ok:
   242|                 sig_v2.append("vts.filter[{}].visibility={}".format(idx3, int(vis)))
   243| 
   244|         try:
   245|             ogs = v.GetFilterOverrides(fid) if hasattr(v, "GetFilterOverrides") else None
   246|         except Exception:
   247|             ogs = None
   248| 
   249|         try:
   250|             has_ovr = False
   251|             if ogs is not None:
   252|                 if getattr(ogs, "Halftone", False):
   253|                     has_ovr = True
   254|                 for attr in ("ProjectionLineWeight", "CutLineWeight", "SurfaceTransparency"):
   255|                     vattr = getattr(ogs, attr, None)
   256|                     if vattr is not None and int(vattr) > 0:
   257|                         has_ovr = True
   258|                 for attr in ("ProjectionLinePatternId", "CutLinePatternId"):
   259|                     eid = getattr(ogs, attr, None)
   260|                     if eid is not None and int(getattr(eid, "IntegerValue", 0)) not in (0, -1):
   261|                         has_ovr = True
   262|             sig.append("filter[{}].ovr={}".format(idx3, int(has_ovr)))
   263|             if v2_ok:
   264|                 sig_v2.append("vts.filter[{}].overrides={}".format(idx3, int(has_ovr)))
   265|         except Exception:
   266|             sig.append("filter[{}].ovr=<UNREADABLE>".format(idx3))
   267|             if v2_ok:
   268|                 v2_block_fn("filter_overrides_unreadable")
   269|                 v2_ok = False
   270| 
   271|     return v2_ok
   272| 
   273| 
   274| def _append_workset_visibility(v, doc, sig, sig_v2, v2_ok, v2_block_fn):
   275|     if FilteredWorksetCollector is None or WorksetKind is None:
   276|         return v2_ok
   277|     if not hasattr(v, "GetWorksetVisibility"):
   278|         return v2_ok
   279| 
   280|     try:
   281|         worksets = list(FilteredWorksetCollector(doc).OfKind(WorksetKind.UserWorkset).ToWorksets())
   282|     except Exception:
   283|         return v2_ok
   284| 
   285|     decorated = []
   286|     for ws in (worksets or []):
   287|         name = canon_str(getattr(ws, "Name", None))
   288|         try:
   289|             vis = v.GetWorksetVisibility(getattr(ws, "Id", None))
   290|             decorated.append((safe_str(name), safe_str(vis), False))
   291|         except Exception:
   292|             decorated.append((safe_str(name), "<UNREADABLE>", True))
   293|             if v2_ok:
   294|                 v2_block_fn("workset_visibility_unreadable")
   295|                 v2_ok = False
   296| 
   297|     for idx, (name, vis, is_unreadable) in enumerate(sorted(decorated, key=lambda x: (x[0], x[1]))):
   298|         sig.append("workset[{}].name={}".format(idx, name))
   299|         sig.append("workset[{}].visibility={}".format(idx, vis))
   300|         if v2_ok and (not is_unreadable):
   301|             sig_v2.append("vts.workset[{}].name={}".format(idx, name))
   302|             sig_v2.append("vts.workset[{}].visibility={}".format(idx, vis))
   303| 
   304|     return v2_ok
   305| 
   306| 
   307| def _phase2_items_from_def_signature(def_signature):
   308|     """Convert legacy def_signature entries ('k=v') into IdentityItems safely."""
   309|     out = []
   310|     for s in (def_signature or []):
   311|         try:
   312|             ss = safe_str(s)
   313|         except Exception:
   314|             continue
   315|         if "=" not in ss:
   316|             k = "view_template.sig.{}".format(ss)
   317|             out.append(make_identity_item(k, None, "missing"))
   318|             continue
   319|         left, right = ss.split("=", 1)
   320|         k = "view_template.sig.{}".format(safe_str(left).strip())
   321|         rr = safe_str(right).strip()
   322|         if len(rr) >= 2 and ((rr[0] == rr[-1] == "'") or (rr[0] == rr[-1] == '"')):
   323|             rr = rr[1:-1].strip()
   324|         if ("|" in rr) and ("=" in rr):
   325|             parts = [p.strip() for p in rr.split("|") if p.strip()]
   326|             for part in parts:
   327|                 if "=" not in part:
   328|                     out.append(make_identity_item("{}.part".format(k), None, "missing"))
   329|                     continue
   330|                 subk_raw, subv_raw = part.split("=", 1)
   331|                 subk = safe_str(subk_raw).strip()
   332|                 subv = safe_str(subv_raw).strip()
   333|                 if len(subv) >= 2 and ((subv[0] == subv[-1] == "'") or (subv[0] == subv[-1] == '"')):
   334|                     subv = subv[1:-1].strip()
   335|                 sv, sq = phase2_qv_from_legacy_sentinel_str(subv, allow_empty=True)
   336|                 out.append(make_identity_item("{}.{}".format(k, subk), sv, sq))
   337|         else:
   338|             v, q = phase2_qv_from_legacy_sentinel_str(rr, allow_empty=True)
   339|             out.append(make_identity_item(k, v, q))
   340|     return phase2_sorted_items(out)
   341| 
   342| 
   343| def _canonical_identity_items_from_signature(def_hash, def_signature, override_stack_hash=None):
   344|     items = [make_identity_item("view_template.def_hash", def_hash, ITEM_Q_OK)]
   345|     if override_stack_hash:
   346|         items.append(make_identity_item("view_template.category_overrides_def_hash", override_stack_hash, ITEM_Q_OK))
   347|     items.extend(_phase2_items_from_def_signature(def_signature))
   348|     return phase2_sorted_items(items)
   349| 
   350| 
   351| def _semantic_keys_from_identity_items(identity_items):
   352|     """Semantic selector list over canonical evidence.
   353| 
   354|     Value keys with companion include flags set to False are omitted from the
   355|     semantic key basis (sig_hash input), while still remaining in identity
   356|     output for diagnostics/traceability.
   357|     """
   358|     include_map = {}
   359|     for it in (identity_items or []):
   360|         try:
   361|             k = safe_str(it.get("k", ""))
   362|         except Exception:
   363|             continue
   364|         if not k.startswith("view_template.sig.include_"):
   365|             continue
   366|         base = k.replace("view_template.sig.include_", "", 1)
   367|         try:
   368|             raw_v = safe_str(it.get("v", "")).strip().lower()
   369|         except Exception:
   370|             raw_v = ""
   371|         include_map[base] = raw_v == "true"
   372| 
   373|     keys = set()
   374|     for it in (identity_items or []):
   375|         if not isinstance(it.get("k"), str):
   376|             continue
   377|         key = safe_str(it.get("k", ""))
   378|         if (not key) or key == "view_template.def_hash":
   379|             continue
   380| 
   381|         if key.startswith("view_template.sig.include_"):
   382|             keys.add(key)
   383|             continue
   384| 
   385|         if key.startswith("view_template.sig."):
   386|             sig_key = key.replace("view_template.sig.", "", 1)
   387|             if sig_key in include_map and not include_map.get(sig_key, False):
   388|                 continue
   389| 
   390|         keys.add(key)
   391| 
   392|     return sorted(keys)
   393| 
   394| 
   395| def _build_floor_structural_area_viewtype_set():
   396|     """
   397|     Build the ViewType integer set for floor/structural/area plans.
   398| 
   399|     Probe-confirmed integers only:
   400|       1 = FloorPlan
   401| 
   402|     AreaPlan and StructuralPlan are intentionally excluded here because
   403|     117 collides with Section in this Revit version.
   404|     """
   405|     return frozenset({1})
   406| 
   407| 
   408| def _build_ceiling_plan_viewtype_set():
   409|     """
   410|     Build the ViewType integer set for ceiling plans.
   411| 
   412|     Probe-confirmed integers only:
   413|       2 = CeilingPlan
   414|     """
   415|     return frozenset({2})
   416| 
   417| 
   418| _FLOOR_STRUCTURAL_AREA_VIEWTYPE_SET = _build_floor_structural_area_viewtype_set()
   419| _CEILING_PLAN_VIEWTYPE_SET = _build_ceiling_plan_viewtype_set()
   420| 
   421| 
```
