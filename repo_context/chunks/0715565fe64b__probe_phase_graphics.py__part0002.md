# Chunk of tools/probes/probe_phase_graphics.py

- Source relative path: `tools/probes/probe_phase_graphics.py`
- Chunk: 2 of 3
- Original line range: 468-986
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _get_phasefilter_param_from_view, _resolve_workset, _reflect_member_names, _reflect_try_get, _reflect_contract, _run_reflection_sweep, _probe_revit_version, _probe_document_identity, _probe_run_id
- Source SHA-256: 7c3277cf7d241a99aedfa2014cd4c5b4c1c314e89283d9945dd9c402f059fbe9
- Starts inside symbol: no
- Ends inside symbol: no

```
   468| 
   469| 
   470| # View -> BodyTextTypeId/HeaderTextTypeId/TitleTextTypeId (ViewSchedule-only,
   471| # same as probe_views.py) and GetFilters()/GetOrderedFilters() (any View).
   472| # Unconditional -- not gated behind enable_crosswalk, which only governs the
   473| # heavier ViewTemplate -> PhaseFilter join below.
   474| for v in selected_views:
   475|     v_id = _safe(lambda: v.Id.IntegerValue, None)
   476|     if v_id is None:
   477|         continue
   478|     v_name = _safe(lambda: _safe_elem_name(v), None)
   479|     v_ws_id_obj = _safe(lambda: v.WorksetId, None)
   480|     v_ws_name, _v_ws_resolved = _resolve_workset_for_view_crosswalk(doc, v_ws_id_obj)
   481|     v_ws_id_int = _safe(lambda: v_ws_id_obj.IntegerValue, None) if v_ws_id_obj is not None else None
   482| 
   483|     body_tt_id = header_tt_id = title_tt_id = None
   484|     body_tt_name = header_tt_name = title_tt_name = None
   485|     if ViewSchedule is not None and _safe(lambda: isinstance(v, ViewSchedule), False):
   486|         _btt = _safe(lambda: v.BodyTextTypeId, None)
   487|         body_tt_id = _safe(lambda: _btt.IntegerValue, None) if _btt is not None else None
   488|         if body_tt_id is not None and body_tt_id >= 0:
   489|             _btt_elem = _safe(lambda: doc.GetElement(_btt), None)
   490|             body_tt_name = _safe(lambda: _btt_elem.Name, None) if _btt_elem is not None else None
   491| 
   492|         _htt = _safe(lambda: v.HeaderTextTypeId, None)
   493|         header_tt_id = _safe(lambda: _htt.IntegerValue, None) if _htt is not None else None
   494|         if header_tt_id is not None and header_tt_id >= 0:
   495|             _htt_elem = _safe(lambda: doc.GetElement(_htt), None)
   496|             header_tt_name = _safe(lambda: _htt_elem.Name, None) if _htt_elem is not None else None
   497| 
   498|         _ttt = _safe(lambda: v.TitleTextTypeId, None)
   499|         title_tt_id = _safe(lambda: _ttt.IntegerValue, None) if _ttt is not None else None
   500|         if title_tt_id is not None and title_tt_id >= 0:
   501|             _ttt_elem = _safe(lambda: doc.GetElement(_ttt), None)
   502|             title_tt_name = _safe(lambda: _ttt_elem.Name, None) if _ttt_elem is not None else None
   503| 
   504|     get_filters_ids = _safe(lambda: list(v.GetFilters() or []), default=[])
   505|     get_filters_names = [_resolve_filter_name(_safe(lambda fid=fid: fid.IntegerValue, None)) for fid in get_filters_ids]
   506|     get_ordered_filters_ids = _safe(lambda: list(v.GetOrderedFilters() or []), default=[])
   507|     get_ordered_filters_names = [_resolve_filter_name(_safe(lambda fid=fid: fid.IntegerValue, None)) for fid in get_ordered_filters_ids]
   508| 
   509|     optional_crosswalk.append({
   510|         "view.id": v_id,
   511|         "view.name": v_name,
   512|         "view.workset_id": v_ws_id_int,
   513|         "view.workset_name": v_ws_name,
   514|         "body_text_type.id": body_tt_id,
   515|         "body_text_type.name": body_tt_name,
   516|         "header_text_type.id": header_tt_id,
   517|         "header_text_type.name": header_tt_name,
   518|         "title_text_type.id": title_tt_id,
   519|         "title_text_type.name": title_tt_name,
   520|         "get_filters.ids": [_safe(lambda fid=fid: fid.IntegerValue, None) for fid in get_filters_ids],
   521|         "get_filters.names": get_filters_names,
   522|         "get_ordered_filters.ids": [_safe(lambda fid=fid: fid.IntegerValue, None) for fid in get_ordered_filters_ids],
   523|         "get_ordered_filters.names": get_ordered_filters_names,
   524|     })
   525| 
   526| # Candidate parameter names (varies by localization/templates; keep flexible)
   527| VIEW_PHASE_FILTER_PARAM_CANDIDATES = [
   528|     "Phase Filter",
   529|     "Phase filter",
   530|     "PhaseFilter",
   531|     "View Phase Filter"
   532| ]
   533| 
   534| # Built-in parameter is preferred if present (more stable than name strings)
   535| BIP_PHASE_FILTER = _safe(lambda: BuiltInParameter.VIEW_PHASE_FILTER, None)
   536| 
   537| def _get_phasefilter_param_from_view(v):
   538|     # Try BIP first
   539|     if BIP_PHASE_FILTER is not None:
   540|         p = _safe(lambda: v.get_Parameter(BIP_PHASE_FILTER), None)
   541|         if p is not None:
   542|             return ("BuiltInParameter.VIEW_PHASE_FILTER", p)
   543| 
   544|     # Then try name candidates
   545|     for nm in VIEW_PHASE_FILTER_PARAM_CANDIDATES:
   546|         p = _safe(lambda: v.LookupParameter(nm), None)
   547|         if p is not None:
   548|             return (nm, p)
   549| 
   550|     return (None, None)
   551| 
   552| if enable_crosswalk:
   553|     # Keep compact: one row per distinct phasefilter id
   554|     seen_pf_ids = set()
   555| 
   556|     def _resolve_workset(doc, ws_id_obj):
   557|         """Resolve an Element.WorksetId value to (name, resolved_bool) via
   558|         WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   559|         distinct .NET type from ElementId (both happen to expose
   560|         .IntegerValue, which is why reflection reports this member as
   561|         ElementId-storage), and Workset is not derived from Element, so
   562|         doc.GetElement() would never resolve it even with the right type
   563|         assumed."""
   564|         if ws_id_obj is None:
   565|             return (None, False)
   566|         wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   567|         if wt_table is None:
   568|             return (None, False)
   569|         ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   570|         if ws is None:
   571|             return (None, False)
   572|         name = _safe(lambda: ws.Name, None)
   573|         return (name, name is not None)
   574| 
   575|     # Prefer templates for crosswalk signal
   576|     crosswalk_views = templates if len(templates) > 0 else selected_views
   577| 
   578|     # crosswalk_limit caps rows THIS loop adds, not the list's total length --
   579|     # optional_crosswalk already holds one unconditional row per selected_views
   580|     # entry (body/header/title text type + get_filters/get_ordered_filters,
   581|     # added above) by the time this loop starts, so comparing against the raw
   582|     # list length would let that pre-existing content silently starve this
   583|     # loop's phase-filter rows out of the cap entirely.
   584|     _phasefilter_rows_start = len(optional_crosswalk)
   585| 
   586|     for v in crosswalk_views:
   587|         if (len(optional_crosswalk) - _phasefilter_rows_start) >= int(crosswalk_limit):
   588|             break
   589| 
   590|         is_t = _safe(lambda: v.IsTemplate, False)
   591|         if not is_t:
   592|             # Crosswalk is primarily meaningful on templates; skip non-templates unless no templates exist
   593|             if len(templates) > 0:
   594|                 continue
   595| 
   596|         matched_name, p = _get_phasefilter_param_from_view(v)
   597|         pv = _format_param_contract(p)
   598| 
   599|         # Only keep ElementId mappings
   600|         if pv.get("storage") != "ElementId" or pv.get("raw") is None:
   601|             continue
   602| 
   603|         pf_id = int(pv.get("raw"))
   604|         if pf_id in seen_pf_ids:
   605|             continue
   606| 
   607|         pf_elem = _safe(lambda: doc.GetElement(ElementId(pf_id)), None)
   608|         pf_name = _safe(lambda: _safe_elem_name(pf_elem), None) if pf_elem is not None else None
   609| 
   610|         # Keep only resolved if possible (signal > noise)
   611|         resolved = True if pf_name is not None else False
   612|         if not resolved:
   613|             continue
   614| 
   615|         v_ws_id_obj = _safe(lambda: v.WorksetId, None)
   616|         v_ws_name, _v_ws_resolved = _resolve_workset(doc, v_ws_id_obj)
   617|         v_ws_id_int = _safe(lambda: v_ws_id_obj.IntegerValue, None) if v_ws_id_obj is not None else None
   618| 
   619|         row = {
   620|             "view_template.id": _safe(lambda: v.Id.IntegerValue, None),
   621|             "view_template.name": _safe(lambda: _safe_elem_name(v), None),
   622|             "view_template.workset_id": v_ws_id_int,
   623|             "view_template.workset_name": v_ws_name,
   624|             "phase_filter_param.matched_name": matched_name,
   625|             "phase_filter_param": pv,
   626|             "phasefilter.resolved": resolved,
   627|             "phasefilter.id": pf_id,
   628|             "phasefilter.name": pf_name
   629|         }
   630| 
   631|         seen_pf_ids.add(pf_id)
   632|         optional_crosswalk.append(row)
   633| 
   634| 
   635| # -------------------------
   636| # Assemble OUT + optional write
   637| # -------------------------
   638| 
   639| 
   640| # -------------------------
   641| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   642| # -------------------------
   643| # Complements the curated/dynamic capture above with a breadth-only sweep of
   644| # the sampled objects' .NET properties and zero-arg methods. This is
   645| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   646| # key list or a Parameters-only walk could otherwise miss.
   647| 
   648| _REFLECTION_SKIP = set([
   649|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   650|     "Dispose", "GetEnumerator", "Clone",
   651| ])
   652| 
   653| def _reflect_member_names(obj):
   654|     out = []
   655|     if obj is None:
   656|         return out
   657|     try:
   658|         t = obj.GetType()
   659|     except:
   660|         return out
   661|     try:
   662|         for p in t.GetProperties():
   663|             try:
   664|                 n = p.Name
   665|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   666|                     continue
   667|                 if p.GetIndexParameters():
   668|                     continue
   669|                 out.append(("property", n))
   670|             except:
   671|                 pass
   672|     except:
   673|         pass
   674|     try:
   675|         for m in t.GetMethods():
   676|             try:
   677|                 n = m.Name
   678|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   679|                     continue
   680|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   681|                     continue
   682|                 if m.GetParameters().Length != 0:
   683|                     continue
   684|                 if m.IsSpecialName:
   685|                     continue
   686|                 out.append(("method", n))
   687|             except:
   688|                 pass
   689|     except:
   690|         pass
   691|     seen = set()
   692|     uniq = []
   693|     for kind, n in out:
   694|         if n in seen:
   695|             continue
   696|         seen.add(n)
   697|         uniq.append((kind, n))
   698|     return sorted(uniq, key=lambda x: x[1])
   699| 
   700| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   701| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   702| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   703| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   704| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   705| # see the notes below the dict) are ground-truth confirmed, against the live
   706| # RevitAPI 2025 documentation (not name/return-type inference), to be
   707| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   708| # from the branching logic in _reflect_try_get below, so it can be reviewed
   709| # as one block and extended later without touching control flow.
   710| #
   711| # Keyed by method NAME only, not (declaring_class, name): this reflection
   712| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   713| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   714| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   715| # is reached in this codebase via more than a dozen different concrete
   716| # type_labels across the probe domains, never via type_label=="Element"
   717| # itself, since no probe in this file reflects a bare Element instance.
   718| # Scoping the allowlist by declaring-class name would silently fail to match
   719| # nearly every real call site and defeat the point of this allowlist.
   720| # _reflect_member_names() below already restricts candidate methods to
   721| # public, non-special-name, zero-parameter methods before this allowlist is
   722| # ever consulted, so a name-only match here does not weaken the zero-arg/
   723| # no-side-effect intent the allowlist exists to enforce -- it only widens
   724| # which already-zero-arg, already-name-matched members get invoked. The
   725| # dict value (declaring class) is kept for traceability back to the Step 0
   726| # CSV only; it is not used in the match.
   727| _ALLOWLISTED_REFLECTION_METHODS = {
   728|     "GetTypeId": "Element",
   729|     "GetLayers": "CompoundStructure",
   730|     "GetEntitySchemaGuids": "Element",
   731|     "GetSubelements": "Element",
   732|     "GetFamilyPointLocations": "FamilySymbol",
   733|     "GetModelToProjectionTransforms": "View",
   734|     "GetRenderingAsset": "AppearanceAssetElement",
   735|     "GetExternalFileReference": "Element",
   736|     "GetMonitoredLinkElementIds": "Element",
   737|     "GetMonitoredLocalElementIds": "Element",
   738|     "GetSimilarTypes": "ElementType",
   739|     "GetStructuralSection": "FamilySymbol",
   740|     "GetThermalProperties": "FamilySymbol",
   741|     "GetFillPattern": "FillPatternElement",
   742|     "GetCategories": "ParameterFilterElement",
   743|     "GetElementFilter": "ParameterFilterElement",
   744|     "GetReference": "Subelement",
   745|     "GetBackground": "View",
   746|     "GetCalloutParentId": "View",
   747|     "GetCropRegionShapeManager": "View",
   748|     "GetDepthCueing": "View",
   749|     "GetDirectContext3DHandleOverrides": "View",
   750|     "GetFilters": "View",
   751|     "GetOrderedFilters": "View",
   752|     "GetPointCloudOverrides": "View",
   753|     "GetPrimaryViewId": "View",
   754|     "GetReferenceCallouts": "View",
   755|     "GetReferenceElevations": "View",
   756|     "GetReferenceSections": "View",
   757|     "GetSketchyLines": "View",
   758|     "GetTemporaryViewPropertiesId": "View",
   759|     "GetViewDisplayModel": "View",
   760| }
   761| 
   762| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   763| # allowlist above after a live re-run (PR #395 discussion) showed
   764| # Element.GetValidTypes fails 100% of the time -- not with a documented
   765| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   766| # GetModelToProjectionTransforms above, which each match a real
   767| # InvalidOperationException precondition stated on their own RevitAPI doc
   768| # pages), but with a CLR/pythonnet interop binding failure:
   769| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   770| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   771| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   772| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   773| # overload-ambiguity problem either -- the call is rejected by the binder
   774| # before it ever reaches Revit's implementation. This will never succeed
   775| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   776| # allowlisted only adds permanent error noise with zero chance of a real
   777| # value. Subelement.GetValidTypes was never independently tested (no probe
   778| # in this codebase reflects a raw Subelement object as its own type_label),
   779| # but shares the same allowlist name and the same removal; re-evaluate
   780| # independently against a live Subelement instance before re-adding either.
   781| 
   782| # LinePatternElement.GetLinePattern was removed from the allowlist above
   783| # after a live re-run (PR #398's exception-capture work, which surfaced the
   784| # error text for the first time) showed it fails 100% of the time -- not
   785| # with a documented Revit API exception (unlike GetCalloutParentId/
   786| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   787| # match a real InvalidOperationException precondition stated on their own
   788| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   789| # failure family as Element.GetValidTypes above: `TypeError: No method
   790| # matches given arguments for GetLinePattern: (<class
   791| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   792| # binder before it ever reaches Revit's implementation, so it cannot
   793| # succeed through `getattr(obj, name)()` regardless of model, Revit
   794| # version, or which element is sampled -- keeping it allowlisted only adds
   795| # permanent error noise for zero chance of real data.
   796| 
   797| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   798| # see the identity check in _reflect_contract below for why this must never be
   799| # comparable-by-value to a real Revit return.
   800| 
   801| def _reflect_try_get(obj, member_kind, name):
   802|     if member_kind == "method":
   803|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   804|             # SAFETY: never invoke a reflection-discovered method that is not
   805|             # on the allowlist above. Revit API methods can have side effects
   806|             # (printing, export, regenerate, delete, transaction commits,
   807|             # ...) and there is no reliable way to tell a safe zero-arg
   808|             # query method from a side-effecting one by name alone for
   809|             # anything outside the allowlist's ground-truth-verified set.
   810|             # Record that the method exists without calling it.
   811|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   812|         try:
   813|             v = getattr(obj, name)()
   814|         except Exception as ex:
   815|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   816|         return (True, v, None)
   817|     try:
   818|         v = getattr(obj, name)
   819|     except Exception as ex:
   820|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   821|     return (True, v, None)
   822| 
   823| def _reflect_contract(raw_v):
   824|     if raw_v is None:
   825|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   826|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   827|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   828|         # unique object(), never a string, specifically so a genuine reflected
   829|         # property or allowlisted-method return whose real value happens to be
   830|         # the literal text "<method not invoked>" cannot collide with this
   831|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   832|         # an earlier version of this check compared by value against a string
   833|         # constant, which had exactly that collision risk). Checked before
   834|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   835|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   836|     if isinstance(raw_v, bool):
   837|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   838|     if isinstance(raw_v, int):
   839|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   840|     if isinstance(raw_v, float):
   841|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   842|     if isinstance(raw_v, str):
   843|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   844|     try:
   845|         if hasattr(raw_v, "IntegerValue"):
   846|             iv = int(raw_v.IntegerValue)
   847|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   848|     except:
   849|         pass
   850|     try:
   851|         if hasattr(raw_v, "ToString"):
   852|             s = raw_v.ToString()
   853|             if s and "Autodesk.Revit" not in s and "System." not in s:
   854|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   855|     except:
   856|         pass
   857|     try:
   858|         ids = []
   859|         saw_item = False
   860|         for item in raw_v:
   861|             saw_item = True
   862|             if not hasattr(item, "IntegerValue"):
   863|                 raise TypeError("non-ElementId item in collection")
   864|             ids.append(int(item.IntegerValue))
   865|         if not saw_item:
   866|             # An empty collection is vacuously "every item has .IntegerValue"
   867|             # -- there's nothing to fail the check against, so item-by-item
   868|             # duck-typing alone can never tell an empty ElementId collection
   869|             # (GetMonitoredLinkElementIds returning [] because a type has no
   870|             # monitored links) apart from an empty collection of anything
   871|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   872|             # IList<Subelement>, both returning [] because that instance
   873|             # happens to have zero). A CLR generic-type reflection check
   874|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   875|             # found not to reliably discriminate types against a live
   876|             # Revit/pythonnet session (still produced the same false
   877|             # positives), so it was dropped rather than kept as an
   878|             # unreliable safety net. Per this project's fail-soft principle
   879|             # (never silently collapse distinct states), an empty collection
   880|             # of unconfirmed item type gets its own explicit q value instead
   881|             # of defaulting to "ok" (would reintroduce this exact bug) or
   882|             # bare "unsupported" (would make it indistinguishable from a
   883|             # totally opaque complex-object failure). storage stays "None"
   884|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   885|             # _is_elementid_typed() correctly does not treat this as a
   886|             # reference candidate.
   887|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   888|         disp = ",".join(str(i) for i in ids)
   889|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   890|     except:
   891|         pass
   892|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   893| 
   894| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   895|     idx = {}
   896|     for obj in sample_objs:
   897|         if obj is None:
   898|             continue
   899|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   900|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   901|             key = "refl.{}.{}".format(type_label, name)
   902|             if key not in idx:
   903|                 idx[key] = {
   904|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   905|                     "type_label": type_label, "example": None, "example_error": None,
   906|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   907|                 }
   908|             e = idx[key]
   909|             if not ok:
   910|                 e["error_count"] += 1
   911|                 if e["example_error"] is None and err:
   912|                     e["example_error"] = err
   913|                 continue
   914|             contract = _reflect_contract(raw_v)
   915|             e["ok_count"] += 1
   916|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   917|             if sig not in e["_seen"]:
   918|                 e["_seen"].add(sig)
   919|                 e["unique_value_count"] += 1
   920|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   921|                 e["example"] = contract
   922|     records = []
   923|     for key in sorted(idx.keys()):
   924|         e = idx[key]
   925|         records.append({
   926|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   927|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   928|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   929|         })
   930|     return records
   931| 
   932| _reflection_records_0 = _run_reflection_sweep(selected_views, "View", "phase_graphics")
   933| _reflection_records = _reflection_records_0
   934| 
   935| OUT_payload = [
   936|     {
   937|         "kind": "reflection",
   938|         "domain": "phase_graphics",
   939|         "records": _reflection_records
   940|     },
   941|     {
   942|         "kind": "inventory",
   943|         "domain": "phase_graphics",
   944|         "records": param_inventory
   945|     },
   946|     {
   947|         "kind": "crosswalk",
   948|         "domain": "phase_graphics",
   949|         "records": optional_crosswalk
   950|     }
   951| ]
   952| 
   953| file_written = None
   954| write_error = None
   955| 
   956| # -------------------------
   957| # Unified run metadata (release-separated, not date-filename-separated)
   958| # -------------------------
   959| # extraction_date lives as JSON metadata, not as a filename token; the
   960| # filename groups by Revit release (revit_version) plus an opaque run_id so
   961| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
   962| # which consumes this shape directly.
   963| 
   964| import uuid as _uuid_mod
   965| 
   966| def _probe_revit_version():
   967|     try:
   968|         _uiapp = DocumentManager.Instance.CurrentUIApplication
   969|         _app = _uiapp.Application if _uiapp is not None else None
   970|         v = _safe(lambda: _app.VersionNumber, None)
   971|         return str(v) if v else None
   972|     except:
   973|         return None
   974| 
   975| def _probe_document_identity():
   976|     return {
   977|         "title": _safe(lambda: doc.Title, None),
   978|         "path_name": _safe(lambda: doc.PathName, None),
   979|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
   980|     }
   981| 
   982| def _probe_run_id():
   983|     try:
   984|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
   985|     except:
   986|         return _uuid_mod.uuid4().hex[:12]
```
