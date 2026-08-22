# Chunk of tools/probes/probe_view_filter_applications.py

- Source relative path: `tools/probes/probe_view_filter_applications.py`
- Chunk: 2 of 3
- Original line range: 462-951
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _resolve_workset, _reflect_member_names, _reflect_try_get, _reflect_contract
- Source SHA-256: 54662f061e5f0ad2fd398cf9882aff71bf5cf2f3312ded3f562bef5b2eabfb1b
- Starts inside symbol: no
- Ends inside symbol: no

```
   462| 
   463| _reflect_ogs_samples = []
   464| _reflect_filter_samples = []
   465| 
   466| for v in selected:
   467|     bk = _view_bucket_key(v)
   468| 
   469|     # view-level signals
   470|     _observe("vfa.view.is_template", _as_bool_int_contract(_safe(lambda: v.IsTemplate, False)), bk)
   471|     _observe("vfa.view.view_type", _as_string_contract(_safe(lambda: v.ViewType, None)), bk)
   472| 
   473|     vname = _safe(lambda: v.Name, None)
   474|     _observe("vfa.view.name", _as_string_contract(vname), bk)
   475| 
   476|     # filter stack
   477|     fids = _collect_applied_filters_in_order(v)
   478|     _observe("vfa.filter_stack.count", _as_int_contract(len(fids)), bk)
   479| 
   480|     for idx, fid in enumerate(fids):
   481|         # application-level synthetic keys (each produces inventory evidence)
   482|         _observe("vfa.filter.order_index", _as_int_contract(idx), bk)
   483| 
   484|         _observe("vfa.filter.id", _as_elementid_contract(fid), bk)
   485| 
   486|         f = _safe(lambda: doc.GetElement(fid), None)
   487|         if f is not None and len(_reflect_filter_samples) < 60:
   488|             _reflect_filter_samples.append(f)
   489|         fname = _safe(lambda: f.Name, None) if f is not None else None
   490|         _observe("vfa.filter.name", _as_string_contract(fname), bk)
   491| 
   492|         vis = _safe(lambda: v.GetFilterVisibility(fid), default=None)
   493|         if vis is None:
   494|             _observe("vfa.filter.visibility", _contract("unreadable", "Integer", None, None, None), bk)
   495|         else:
   496|             _observe("vfa.filter.visibility", _as_bool_int_contract(vis), bk)
   497| 
   498|         ogs = _safe(lambda: v.GetFilterOverrides(fid), default=None)
   499|         if ogs is not None and len(_reflect_ogs_samples) < 60:
   500|             _reflect_ogs_samples.append(ogs)
   501|         if ogs is None:
   502|             _observe("vfa.ogs.present", _contract("unreadable", "Integer", None, None, None), bk)
   503|             continue
   504| 
   505|         _observe("vfa.ogs.present", _as_bool_int_contract(True), bk)
   506| 
   507|         # capture the override surface (field-by-field)
   508|         sig_pairs = []
   509|         for (pk, attr, kind) in OGS_FIELDS:
   510|             pv = _pv_from_ogs_field(ogs, attr, kind)
   511|             _observe(pk, pv, bk)
   512|             if not _is_defaultish_ogs_value(pk, pv):
   513|                 sig_pairs.append((pk, str(pv.get("norm"))))
   514| 
   515|         sig_hash = _hash_sig(sig_pairs) if len(sig_pairs) > 0 else None
   516|         _observe("vfa.ogs.sig_hash", _as_string_contract(sig_hash), bk)
   517| 
   518|         has_any_override = True if sig_hash is not None else False
   519|         _observe("vfa.ogs.has_any_override", _as_bool_int_contract(has_any_override), bk)
   520| 
   521| 
   522| # -------------------------
   523| # Emit inventory records (stable order)
   524| # -------------------------
   525| 
   526| param_inventory = []
   527| for pk in sorted(param_index.keys()):
   528|     e = param_index[pk]
   529|     param_inventory.append({
   530|         "domain": "view_filter_applications",
   531|         "param_key": pk,
   532|         "selected_view_sample_count": len(selected),
   533|         "example": e["example"],
   534|         "observed": {
   535|             "storage_types": sorted(list(e["storage_types"])),
   536|             "q_counts": e["q_counts"],
   537|             "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
   538|         }
   539|     })
   540| 
   541| 
   542| # -------------------------
   543| # Optional Crosswalk: View/ViewTemplate → ParameterFilterElement
   544| # -------------------------
   545| 
   546| optional_crosswalk = []
   547| 
   548| if enable_crosswalk:
   549|     # Emit one row per (view, filter) occurrence so overrides are verifiable per template/view.
   550|     # Dedup key: (view.id, filter.id)
   551|     seen_occ = set()
   552| 
   553|     def _resolve_workset(doc, ws_id_obj):
   554|         """Resolve an Element.WorksetId value to (name, resolved_bool) via
   555|         WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   556|         distinct .NET type from ElementId (both happen to expose
   557|         .IntegerValue, which is why reflection reports this member as
   558|         ElementId-storage), and Workset is not derived from Element, so
   559|         doc.GetElement() would never resolve it even with the right type
   560|         assumed."""
   561|         if ws_id_obj is None:
   562|             return (None, False)
   563|         wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   564|         if wt_table is None:
   565|             return (None, False)
   566|         ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   567|         if ws is None:
   568|             return (None, False)
   569|         name = _safe(lambda: ws.Name, None)
   570|         return (name, name is not None)
   571| 
   572|     for v in selected:
   573|         v_is_template = _safe(lambda: v.IsTemplate, False)
   574|         v_viewtype = _safe(lambda: v.ViewType, None)
   575|         v_id = _safe(lambda: v.Id.IntegerValue, None)
   576|         v_name = _safe(lambda: v.Name, None)
   577| 
   578|         fids = _collect_applied_filters_in_order(v)
   579| 
   580|         for idx, fid in enumerate(fids):
   581|             fid_int = _eid_int(fid)
   582|             if fid_int is None or v_id is None:
   583|                 continue
   584| 
   585|             occ_key = "{}|{}".format(str(v_id), str(fid_int))
   586|             if occ_key in seen_occ:
   587|                 continue
   588| 
   589|             fe = _safe(lambda: doc.GetElement(fid), None)
   590|             if fe is None:
   591|                 continue
   592| 
   593|             fname = _safe(lambda: fe.Name, None)
   594|             fe_ws_id_obj = _safe(lambda: fe.WorksetId, None)
   595|             fe_ws_name, _fe_ws_resolved = _resolve_workset(doc, fe_ws_id_obj)
   596|             fe_ws_id_int = _safe(lambda: fe_ws_id_obj.IntegerValue, None) if fe_ws_id_obj is not None else None
   597| 
   598|             # Pull per-occurrence visibility + overrides (guarded)
   599|             vis = _safe(lambda: v.GetFilterVisibility(fid), default=None)
   600|             ogs = _safe(lambda: v.GetFilterOverrides(fid), default=None)
   601| 
   602|             # Colors (proof fields)
   603|             pv_pl_rgb = _pv_from_ogs_field(ogs, "ProjectionLineColor", "color_rgb") if ogs is not None else _contract("missing", "String", None, None, None)
   604|             pv_pl_hex = _pv_from_ogs_field(ogs, "ProjectionLineColor", "color_hex") if ogs is not None else _contract("missing", "String", None, None, None)
   605|             pv_cl_rgb = _pv_from_ogs_field(ogs, "CutLineColor", "color_rgb") if ogs is not None else _contract("missing", "String", None, None, None)
   606|             pv_cl_hex = _pv_from_ogs_field(ogs, "CutLineColor", "color_hex") if ogs is not None else _contract("missing", "String", None, None, None)
   607| 
   608|             row = {
   609|                 "view.id": v_id,
   610|                 "view.name": v_name,
   611|                 "view.is_template": True if v_is_template else False,
   612|                 "view.view_type": str(v_viewtype),
   613|                 "filter.order_index": idx,
   614|                 "filter.id": fid_int,
   615|                 "filter.name": fname,
   616|                 "filter.workset_id": fe_ws_id_int,
   617|                 "filter.workset_name": fe_ws_name,
   618|                 "filter.class": _safe(lambda: fe.GetType().FullName, None),
   619|                 "filter.visibility": vis,
   620|                 "ogs.present": True if ogs is not None else False
   621|             }
   622| 
   623|             # Emit full OGS surface as parameter-like payloads
   624|             ogs_payload = {}
   625|             sig_pairs = []
   626| 
   627|             if ogs is not None:
   628|                 for (pk, attr, kind) in OGS_FIELDS:
   629|                     pv = _pv_from_ogs_field(ogs, attr, kind)
   630|                     ogs_payload[pk] = pv
   631| 
   632|                     # keep sig_hash aligned with inventory behavior
   633|                     if not _is_defaultish_ogs_value(pk, pv):
   634|                         sig_pairs.append((pk, str(pv.get("norm"))))
   635| 
   636|             sig_hash = _hash_sig(sig_pairs) if len(sig_pairs) > 0 else None
   637| 
   638|             row["ogs.overrides"] = ogs_payload
   639|             row["ogs.sig_hash"] = sig_hash
   640|             row["ogs.has_any_override"] = True if sig_hash is not None else False
   641| 
   642|             for src_attr, pfx in (
   643|                 ("CutBackgroundPatternId", "cut_background_pattern"),
   644|                 ("CutForegroundPatternId", "cut_foreground_pattern"),
   645|                 ("SurfaceBackgroundPatternId", "surface_background_pattern"),
   646|                 ("SurfaceForegroundPatternId", "surface_foreground_pattern"),
   647|             ):
   648|                 pid = _safe(lambda src_attr=src_attr: getattr(ogs, src_attr), None) if ogs is not None else None
   649|                 pid_int = _eid_int(pid) if pid is not None else None
   650|                 pe = _safe(lambda pid=pid: doc.GetElement(pid), None) if pid_int is not None else None
   651|                 row[pfx + ".id"] = pid_int
   652|                 row[pfx + ".name"] = _safe(lambda pe=pe: pe.Name, None) if pe is not None else None
   653|                 row[pfx + ".resolved"] = row[pfx + ".name"] is not None
   654| 
   655|             # Category sampling if it is a ParameterFilterElement
   656|             if isinstance(fe, ParameterFilterElement):
   657|                 row["pfe.is_parameter_filter_element"] = True
   658|                 cats = _safe(lambda: list(fe.GetCategories()), default=[])
   659|                 try:
   660|                     cats = list(cats)
   661|                 except:
   662|                     pass
   663|                 row["pfe.category_count"] = len(cats)
   664| 
   665|                 # GetCategories() returns Category ids (BuiltInCategory-backed,
   666|                 # e.g. -2001100), NOT ordinary element ids -- doc.GetElement()
   667|                 # does not reliably resolve those (returns None). Category.
   668|                 # GetCategory(doc, id) is the correct resolution, same helper
   669|                 # already used correctly in probe_view_filter_definitions.py's
   670|                 # _resolve_category_name().
   671|                 cat_ids_int = []
   672|                 names = []
   673|                 for cid in cats:
   674|                     cid_int = _safe(lambda cid=cid: cid.IntegerValue, None)
   675|                     cat_ids_int.append(cid_int)
   676|                     ce = _safe(lambda cid=cid: Category.GetCategory(doc, cid), None)
   677|                     cn = _safe(lambda ce=ce: ce.Name, None) if ce is not None else None
   678|                     if cn:
   679|                         names.append(cn)
   680|                 row["pfe.category_names_sample"] = sorted(list(set(names)))[:25]
   681|                 row["get_categories.ids"] = cat_ids_int
   682|                 row["get_categories.names"] = names
   683|             else:
   684|                 row["pfe.is_parameter_filter_element"] = False
   685|                 row["pfe.category_count"] = None
   686|                 row["pfe.category_names_sample"] = []
   687|                 row["get_categories.ids"] = []
   688|                 row["get_categories.names"] = []
   689| 
   690|             seen_occ.add(occ_key)
   691|             optional_crosswalk.append(row)
   692| 
   693| # -------------------------
   694| # Assemble OUT + optional JSON write
   695| # -------------------------
   696| 
   697| 
   698| # -------------------------
   699| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   700| # -------------------------
   701| # Complements the curated/dynamic capture above with a breadth-only sweep of
   702| # the sampled objects' .NET properties and zero-arg methods. This is
   703| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   704| # key list or a Parameters-only walk could otherwise miss.
   705| 
   706| _REFLECTION_SKIP = set([
   707|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   708|     "Dispose", "GetEnumerator", "Clone",
   709| ])
   710| 
   711| def _reflect_member_names(obj):
   712|     out = []
   713|     if obj is None:
   714|         return out
   715|     try:
   716|         t = obj.GetType()
   717|     except:
   718|         return out
   719|     try:
   720|         for p in t.GetProperties():
   721|             try:
   722|                 n = p.Name
   723|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   724|                     continue
   725|                 if p.GetIndexParameters():
   726|                     continue
   727|                 out.append(("property", n))
   728|             except:
   729|                 pass
   730|     except:
   731|         pass
   732|     try:
   733|         for m in t.GetMethods():
   734|             try:
   735|                 n = m.Name
   736|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   737|                     continue
   738|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   739|                     continue
   740|                 if m.GetParameters().Length != 0:
   741|                     continue
   742|                 if m.IsSpecialName:
   743|                     continue
   744|                 out.append(("method", n))
   745|             except:
   746|                 pass
   747|     except:
   748|         pass
   749|     seen = set()
   750|     uniq = []
   751|     for kind, n in out:
   752|         if n in seen:
   753|             continue
   754|         seen.add(n)
   755|         uniq.append((kind, n))
   756|     return sorted(uniq, key=lambda x: x[1])
   757| 
   758| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   759| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   760| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   761| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   762| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   763| # see the notes below the dict) are ground-truth confirmed, against the live
   764| # RevitAPI 2025 documentation (not name/return-type inference), to be
   765| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   766| # from the branching logic in _reflect_try_get below, so it can be reviewed
   767| # as one block and extended later without touching control flow.
   768| #
   769| # Keyed by method NAME only, not (declaring_class, name): this reflection
   770| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   771| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   772| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   773| # is reached in this codebase via more than a dozen different concrete
   774| # type_labels across the probe domains, never via type_label=="Element"
   775| # itself, since no probe in this file reflects a bare Element instance.
   776| # Scoping the allowlist by declaring-class name would silently fail to match
   777| # nearly every real call site and defeat the point of this allowlist.
   778| # _reflect_member_names() below already restricts candidate methods to
   779| # public, non-special-name, zero-parameter methods before this allowlist is
   780| # ever consulted, so a name-only match here does not weaken the zero-arg/
   781| # no-side-effect intent the allowlist exists to enforce -- it only widens
   782| # which already-zero-arg, already-name-matched members get invoked. The
   783| # dict value (declaring class) is kept for traceability back to the Step 0
   784| # CSV only; it is not used in the match.
   785| _ALLOWLISTED_REFLECTION_METHODS = {
   786|     "GetTypeId": "Element",
   787|     "GetLayers": "CompoundStructure",
   788|     "GetEntitySchemaGuids": "Element",
   789|     "GetSubelements": "Element",
   790|     "GetFamilyPointLocations": "FamilySymbol",
   791|     "GetModelToProjectionTransforms": "View",
   792|     "GetRenderingAsset": "AppearanceAssetElement",
   793|     "GetExternalFileReference": "Element",
   794|     "GetMonitoredLinkElementIds": "Element",
   795|     "GetMonitoredLocalElementIds": "Element",
   796|     "GetSimilarTypes": "ElementType",
   797|     "GetStructuralSection": "FamilySymbol",
   798|     "GetThermalProperties": "FamilySymbol",
   799|     "GetFillPattern": "FillPatternElement",
   800|     "GetCategories": "ParameterFilterElement",
   801|     "GetElementFilter": "ParameterFilterElement",
   802|     "GetReference": "Subelement",
   803|     "GetBackground": "View",
   804|     "GetCalloutParentId": "View",
   805|     "GetCropRegionShapeManager": "View",
   806|     "GetDepthCueing": "View",
   807|     "GetDirectContext3DHandleOverrides": "View",
   808|     "GetFilters": "View",
   809|     "GetOrderedFilters": "View",
   810|     "GetPointCloudOverrides": "View",
   811|     "GetPrimaryViewId": "View",
   812|     "GetReferenceCallouts": "View",
   813|     "GetReferenceElevations": "View",
   814|     "GetReferenceSections": "View",
   815|     "GetSketchyLines": "View",
   816|     "GetTemporaryViewPropertiesId": "View",
   817|     "GetViewDisplayModel": "View",
   818| }
   819| 
   820| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   821| # allowlist above after a live re-run (PR #395 discussion) showed
   822| # Element.GetValidTypes fails 100% of the time -- not with a documented
   823| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   824| # GetModelToProjectionTransforms above, which each match a real
   825| # InvalidOperationException precondition stated on their own RevitAPI doc
   826| # pages), but with a CLR/pythonnet interop binding failure:
   827| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   828| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   829| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   830| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   831| # overload-ambiguity problem either -- the call is rejected by the binder
   832| # before it ever reaches Revit's implementation. This will never succeed
   833| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   834| # allowlisted only adds permanent error noise with zero chance of a real
   835| # value. Subelement.GetValidTypes was never independently tested (no probe
   836| # in this codebase reflects a raw Subelement object as its own type_label),
   837| # but shares the same allowlist name and the same removal; re-evaluate
   838| # independently against a live Subelement instance before re-adding either.
   839| 
   840| # LinePatternElement.GetLinePattern was removed from the allowlist above
   841| # after a live re-run (PR #398's exception-capture work, which surfaced the
   842| # error text for the first time) showed it fails 100% of the time -- not
   843| # with a documented Revit API exception (unlike GetCalloutParentId/
   844| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   845| # match a real InvalidOperationException precondition stated on their own
   846| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   847| # failure family as Element.GetValidTypes above: `TypeError: No method
   848| # matches given arguments for GetLinePattern: (<class
   849| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   850| # binder before it ever reaches Revit's implementation, so it cannot
   851| # succeed through `getattr(obj, name)()` regardless of model, Revit
   852| # version, or which element is sampled -- keeping it allowlisted only adds
   853| # permanent error noise for zero chance of real data.
   854| 
   855| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   856| # see the identity check in _reflect_contract below for why this must never be
   857| # comparable-by-value to a real Revit return.
   858| 
   859| def _reflect_try_get(obj, member_kind, name):
   860|     if member_kind == "method":
   861|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   862|             # SAFETY: never invoke a reflection-discovered method that is not
   863|             # on the allowlist above. Revit API methods can have side effects
   864|             # (printing, export, regenerate, delete, transaction commits,
   865|             # ...) and there is no reliable way to tell a safe zero-arg
   866|             # query method from a side-effecting one by name alone for
   867|             # anything outside the allowlist's ground-truth-verified set.
   868|             # Record that the method exists without calling it.
   869|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   870|         try:
   871|             v = getattr(obj, name)()
   872|         except Exception as ex:
   873|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   874|         return (True, v, None)
   875|     try:
   876|         v = getattr(obj, name)
   877|     except Exception as ex:
   878|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   879|     return (True, v, None)
   880| 
   881| def _reflect_contract(raw_v):
   882|     if raw_v is None:
   883|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   884|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   885|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   886|         # unique object(), never a string, specifically so a genuine reflected
   887|         # property or allowlisted-method return whose real value happens to be
   888|         # the literal text "<method not invoked>" cannot collide with this
   889|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   890|         # an earlier version of this check compared by value against a string
   891|         # constant, which had exactly that collision risk). Checked before
   892|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   893|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   894|     if isinstance(raw_v, bool):
   895|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   896|     if isinstance(raw_v, int):
   897|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   898|     if isinstance(raw_v, float):
   899|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   900|     if isinstance(raw_v, str):
   901|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   902|     try:
   903|         if hasattr(raw_v, "IntegerValue"):
   904|             iv = int(raw_v.IntegerValue)
   905|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   906|     except:
   907|         pass
   908|     try:
   909|         if hasattr(raw_v, "ToString"):
   910|             s = raw_v.ToString()
   911|             if s and "Autodesk.Revit" not in s and "System." not in s:
   912|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   913|     except:
   914|         pass
   915|     try:
   916|         ids = []
   917|         saw_item = False
   918|         for item in raw_v:
   919|             saw_item = True
   920|             if not hasattr(item, "IntegerValue"):
   921|                 raise TypeError("non-ElementId item in collection")
   922|             ids.append(int(item.IntegerValue))
   923|         if not saw_item:
   924|             # An empty collection is vacuously "every item has .IntegerValue"
   925|             # -- there's nothing to fail the check against, so item-by-item
   926|             # duck-typing alone can never tell an empty ElementId collection
   927|             # (GetMonitoredLinkElementIds returning [] because a type has no
   928|             # monitored links) apart from an empty collection of anything
   929|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   930|             # IList<Subelement>, both returning [] because that instance
   931|             # happens to have zero). A CLR generic-type reflection check
   932|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   933|             # found not to reliably discriminate types against a live
   934|             # Revit/pythonnet session (still produced the same false
   935|             # positives), so it was dropped rather than kept as an
   936|             # unreliable safety net. Per this project's fail-soft principle
   937|             # (never silently collapse distinct states), an empty collection
   938|             # of unconfirmed item type gets its own explicit q value instead
   939|             # of defaulting to "ok" (would reintroduce this exact bug) or
   940|             # bare "unsupported" (would make it indistinguishable from a
   941|             # totally opaque complex-object failure). storage stays "None"
   942|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   943|             # _is_elementid_typed() correctly does not treat this as a
   944|             # reference candidate.
   945|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   946|         disp = ",".join(str(i) for i in ids)
   947|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   948|     except:
   949|         pass
   950|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   951| 
```
