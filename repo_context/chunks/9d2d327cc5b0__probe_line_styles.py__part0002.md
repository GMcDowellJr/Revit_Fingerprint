# Chunk of tools/probes/probe_line_styles.py

- Source relative path: `tools/probes/probe_line_styles.py`
- Chunk: 2 of 3
- Original line range: 473-972
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _resolve_workset, _reflect_member_names, _reflect_try_get, _reflect_contract, _run_reflection_sweep, _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: 37339fee28db23b0f48664b771d0c3bb9d107f1271f8c7ea137315329b786ee7
- Starts inside symbol: no
- Ends inside symbol: no

```
   473| 
   474| for gs in selected:
   475|     bk = _bucket_key(gs)
   476| 
   477|     # 1) real parameters on GraphicsStyle (if any)
   478|     params = _safe(lambda: list(gs.GetOrderedParameters()), default=None)
   479|     if params is None:
   480|         params = _safe(lambda: list(gs.Parameters), default=[])
   481| 
   482|     for p in params:
   483|         dn = _safe(lambda: _safe_param_def_name(p), None)
   484|         if not dn:
   485|             continue
   486|         pk = "p.{}".format(dn)
   487|         pv = _format_param_contract(p)
   488|         _index_param(pk, pv, bk)
   489| 
   490|     # 2) virtual properties (Category/GraphicsStyle surface)
   491|     v = _virtual_surface(gs)
   492|     for vk in v.keys():
   493|         _index_param(vk, v[vk], bk)
   494| 
   495| # Emit inventory records (stable order)
   496| param_inventory = []
   497| for pk in sorted(param_index.keys()):
   498|     e = param_index[pk]
   499|     param_inventory.append({
   500|         "domain": "line_styles",
   501|         "param_key": pk,
   502|         "selected_style_sample_count": len(selected),
   503|         "example": e["example"],
   504|         "observed": {
   505|             "storage_types": sorted(list(e["storage_types"])),
   506|             "q_counts": e["q_counts"],
   507|             "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
   508|         }
   509|     })
   510| 
   511| 
   512| # -------------------------
   513| # Optional Crosswalk: LineStyle -> LinePattern
   514| # -------------------------
   515| 
   516| optional_crosswalk = []
   517| 
   518| if enable_crosswalk:
   519|     # Separate limits for crosswalk breadth (independent from inventory sampling)
   520|     crosswalk_scan_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 2000
   521|     crosswalk_emit_limit = IN[6] if len(IN) > 6 and IN[6] is not None else 200
   522| 
   523|     try:
   524|         crosswalk_scan_limit = int(crosswalk_scan_limit)
   525|     except:
   526|         crosswalk_scan_limit = 2000
   527| 
   528|     try:
   529|         crosswalk_emit_limit = int(crosswalk_emit_limit)
   530|     except:
   531|         crosswalk_emit_limit = 200
   532| 
   533|     # Relationship-breadth key: (line_pattern_id, graphics_style_type)
   534|     seen_rel = set()
   535| 
   536|     def _resolve_workset(doc, ws_id_obj):
   537|         """Resolve an Element.WorksetId value to (name, resolved_bool) via
   538|         WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   539|         distinct .NET type from ElementId (both happen to expose
   540|         .IntegerValue, which is why reflection reports this member as
   541|         ElementId-storage), and Workset is not derived from Element, so
   542|         doc.GetElement() would never resolve it even with the right type
   543|         assumed."""
   544|         if ws_id_obj is None:
   545|             return (None, False)
   546|         wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   547|         if wt_table is None:
   548|             return (None, False)
   549|         ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   550|         if ws is None:
   551|             return (None, False)
   552|         name = _safe(lambda: ws.Name, None)
   553|         return (name, name is not None)
   554| 
   555|     scanned = 0
   556|     for gs in hits:
   557|         if crosswalk_scan_limit >= 0 and scanned >= crosswalk_scan_limit:
   558|             break
   559|         if crosswalk_emit_limit >= 0 and len(optional_crosswalk) >= crosswalk_emit_limit:
   560|             break
   561| 
   562|         scanned += 1
   563| 
   564|         gst = _safe(lambda: gs.GraphicsStyleType, None)
   565|         c = _safe(lambda: gs.GraphicsStyleCategory, None)
   566|         if c is None:
   567|             continue
   568| 
   569|         # pattern id on category
   570|         pat_id = None
   571|         if GraphicsStyleType is not None:
   572|             pat_id = _safe(lambda: c.GetLinePatternId(GraphicsStyleType.Projection), None)
   573|         if pat_id is None:
   574|             pat_id = _safe(lambda: c.LinePatternId, None)
   575| 
   576|         pat_int = _safe(lambda: pat_id.IntegerValue, None) if pat_id is not None else None
   577|         pat_name = None
   578|         if pat_id is not None and pat_id != ElementId.InvalidElementId:
   579|             pe = _safe(lambda: doc.GetElement(pat_id), None)
   580|             pat_name = _safe(lambda: pe.Name, None) if pe is not None else None
   581| 
   582|         rel_key = "{}|{}".format(str(pat_int), str(gst))
   583|         if rel_key in seen_rel:
   584|             continue
   585| 
   586|         style_name = _safe(lambda: c.Name, None) or _safe(lambda: gs.Name, None)
   587|         gs_ws_id_obj = _safe(lambda: gs.WorksetId, None)
   588|         gs_ws_name, _gs_ws_resolved = _resolve_workset(doc, gs_ws_id_obj)
   589|         gs_ws_id_int = _safe(lambda: gs_ws_id_obj.IntegerValue, None) if gs_ws_id_obj is not None else None
   590| 
   591|         row = {
   592|             "line_style.id": _safe(lambda: gs.Id.IntegerValue, None),
   593|             "line_style.type": str(gst),
   594|             "line_style.name": style_name,
   595|             "line_style.workset_id": gs_ws_id_int,
   596|             "line_style.workset_name": gs_ws_name,
   597|             "line_pattern.resolved": True if (pat_int is not None and pat_name is not None) else False,
   598|             "line_pattern.id": pat_int,
   599|             "line_pattern.name": pat_name
   600|         }
   601| 
   602|         seen_rel.add(rel_key)
   603|         optional_crosswalk.append(row)
   604| 
   605| 
   606| 
   607| # -------------------------
   608| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   609| # -------------------------
   610| # Complements the curated/dynamic capture above with a breadth-only sweep of
   611| # the sampled objects' .NET properties and zero-arg methods. This is
   612| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   613| # key list or a Parameters-only walk could otherwise miss.
   614| 
   615| _REFLECTION_SKIP = set([
   616|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   617|     "Dispose", "GetEnumerator", "Clone",
   618| ])
   619| 
   620| def _reflect_member_names(obj):
   621|     out = []
   622|     if obj is None:
   623|         return out
   624|     try:
   625|         t = obj.GetType()
   626|     except:
   627|         return out
   628|     try:
   629|         for p in t.GetProperties():
   630|             try:
   631|                 n = p.Name
   632|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   633|                     continue
   634|                 if p.GetIndexParameters():
   635|                     continue
   636|                 out.append(("property", n))
   637|             except:
   638|                 pass
   639|     except:
   640|         pass
   641|     try:
   642|         for m in t.GetMethods():
   643|             try:
   644|                 n = m.Name
   645|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   646|                     continue
   647|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   648|                     continue
   649|                 if m.GetParameters().Length != 0:
   650|                     continue
   651|                 if m.IsSpecialName:
   652|                     continue
   653|                 out.append(("method", n))
   654|             except:
   655|                 pass
   656|     except:
   657|         pass
   658|     seen = set()
   659|     uniq = []
   660|     for kind, n in out:
   661|         if n in seen:
   662|             continue
   663|         seen.add(n)
   664|         uniq.append((kind, n))
   665|     return sorted(uniq, key=lambda x: x[1])
   666| 
   667| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   668| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   669| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   670| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   671| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   672| # see the notes below the dict) are ground-truth confirmed, against the live
   673| # RevitAPI 2025 documentation (not name/return-type inference), to be
   674| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   675| # from the branching logic in _reflect_try_get below, so it can be reviewed
   676| # as one block and extended later without touching control flow.
   677| #
   678| # Keyed by method NAME only, not (declaring_class, name): this reflection
   679| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   680| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   681| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   682| # is reached in this codebase via more than a dozen different concrete
   683| # type_labels across the probe domains, never via type_label=="Element"
   684| # itself, since no probe in this file reflects a bare Element instance.
   685| # Scoping the allowlist by declaring-class name would silently fail to match
   686| # nearly every real call site and defeat the point of this allowlist.
   687| # _reflect_member_names() below already restricts candidate methods to
   688| # public, non-special-name, zero-parameter methods before this allowlist is
   689| # ever consulted, so a name-only match here does not weaken the zero-arg/
   690| # no-side-effect intent the allowlist exists to enforce -- it only widens
   691| # which already-zero-arg, already-name-matched members get invoked. The
   692| # dict value (declaring class) is kept for traceability back to the Step 0
   693| # CSV only; it is not used in the match.
   694| _ALLOWLISTED_REFLECTION_METHODS = {
   695|     "GetTypeId": "Element",
   696|     "GetLayers": "CompoundStructure",
   697|     "GetEntitySchemaGuids": "Element",
   698|     "GetSubelements": "Element",
   699|     "GetFamilyPointLocations": "FamilySymbol",
   700|     "GetModelToProjectionTransforms": "View",
   701|     "GetRenderingAsset": "AppearanceAssetElement",
   702|     "GetExternalFileReference": "Element",
   703|     "GetMonitoredLinkElementIds": "Element",
   704|     "GetMonitoredLocalElementIds": "Element",
   705|     "GetSimilarTypes": "ElementType",
   706|     "GetStructuralSection": "FamilySymbol",
   707|     "GetThermalProperties": "FamilySymbol",
   708|     "GetFillPattern": "FillPatternElement",
   709|     "GetCategories": "ParameterFilterElement",
   710|     "GetElementFilter": "ParameterFilterElement",
   711|     "GetReference": "Subelement",
   712|     "GetBackground": "View",
   713|     "GetCalloutParentId": "View",
   714|     "GetCropRegionShapeManager": "View",
   715|     "GetDepthCueing": "View",
   716|     "GetDirectContext3DHandleOverrides": "View",
   717|     "GetFilters": "View",
   718|     "GetOrderedFilters": "View",
   719|     "GetPointCloudOverrides": "View",
   720|     "GetPrimaryViewId": "View",
   721|     "GetReferenceCallouts": "View",
   722|     "GetReferenceElevations": "View",
   723|     "GetReferenceSections": "View",
   724|     "GetSketchyLines": "View",
   725|     "GetTemporaryViewPropertiesId": "View",
   726|     "GetViewDisplayModel": "View",
   727| }
   728| 
   729| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   730| # allowlist above after a live re-run (PR #395 discussion) showed
   731| # Element.GetValidTypes fails 100% of the time -- not with a documented
   732| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   733| # GetModelToProjectionTransforms above, which each match a real
   734| # InvalidOperationException precondition stated on their own RevitAPI doc
   735| # pages), but with a CLR/pythonnet interop binding failure:
   736| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   737| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   738| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   739| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   740| # overload-ambiguity problem either -- the call is rejected by the binder
   741| # before it ever reaches Revit's implementation. This will never succeed
   742| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   743| # allowlisted only adds permanent error noise with zero chance of a real
   744| # value. Subelement.GetValidTypes was never independently tested (no probe
   745| # in this codebase reflects a raw Subelement object as its own type_label),
   746| # but shares the same allowlist name and the same removal; re-evaluate
   747| # independently against a live Subelement instance before re-adding either.
   748| 
   749| # LinePatternElement.GetLinePattern was removed from the allowlist above
   750| # after a live re-run (PR #398's exception-capture work, which surfaced the
   751| # error text for the first time) showed it fails 100% of the time -- not
   752| # with a documented Revit API exception (unlike GetCalloutParentId/
   753| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   754| # match a real InvalidOperationException precondition stated on their own
   755| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   756| # failure family as Element.GetValidTypes above: `TypeError: No method
   757| # matches given arguments for GetLinePattern: (<class
   758| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   759| # binder before it ever reaches Revit's implementation, so it cannot
   760| # succeed through `getattr(obj, name)()` regardless of model, Revit
   761| # version, or which element is sampled -- keeping it allowlisted only adds
   762| # permanent error noise for zero chance of real data.
   763| 
   764| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   765| # see the identity check in _reflect_contract below for why this must never be
   766| # comparable-by-value to a real Revit return.
   767| 
   768| def _reflect_try_get(obj, member_kind, name):
   769|     if member_kind == "method":
   770|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   771|             # SAFETY: never invoke a reflection-discovered method that is not
   772|             # on the allowlist above. Revit API methods can have side effects
   773|             # (printing, export, regenerate, delete, transaction commits,
   774|             # ...) and there is no reliable way to tell a safe zero-arg
   775|             # query method from a side-effecting one by name alone for
   776|             # anything outside the allowlist's ground-truth-verified set.
   777|             # Record that the method exists without calling it.
   778|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   779|         try:
   780|             v = getattr(obj, name)()
   781|         except Exception as ex:
   782|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   783|         return (True, v, None)
   784|     try:
   785|         v = getattr(obj, name)
   786|     except Exception as ex:
   787|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   788|     return (True, v, None)
   789| 
   790| def _reflect_contract(raw_v):
   791|     if raw_v is None:
   792|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   793|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   794|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   795|         # unique object(), never a string, specifically so a genuine reflected
   796|         # property or allowlisted-method return whose real value happens to be
   797|         # the literal text "<method not invoked>" cannot collide with this
   798|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   799|         # an earlier version of this check compared by value against a string
   800|         # constant, which had exactly that collision risk). Checked before
   801|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   802|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   803|     if isinstance(raw_v, bool):
   804|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   805|     if isinstance(raw_v, int):
   806|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   807|     if isinstance(raw_v, float):
   808|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   809|     if isinstance(raw_v, str):
   810|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   811|     try:
   812|         if hasattr(raw_v, "IntegerValue"):
   813|             iv = int(raw_v.IntegerValue)
   814|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   815|     except:
   816|         pass
   817|     try:
   818|         if hasattr(raw_v, "ToString"):
   819|             s = raw_v.ToString()
   820|             if s and "Autodesk.Revit" not in s and "System." not in s:
   821|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   822|     except:
   823|         pass
   824|     try:
   825|         ids = []
   826|         saw_item = False
   827|         for item in raw_v:
   828|             saw_item = True
   829|             if not hasattr(item, "IntegerValue"):
   830|                 raise TypeError("non-ElementId item in collection")
   831|             ids.append(int(item.IntegerValue))
   832|         if not saw_item:
   833|             # An empty collection is vacuously "every item has .IntegerValue"
   834|             # -- there's nothing to fail the check against, so item-by-item
   835|             # duck-typing alone can never tell an empty ElementId collection
   836|             # (GetMonitoredLinkElementIds returning [] because a type has no
   837|             # monitored links) apart from an empty collection of anything
   838|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   839|             # IList<Subelement>, both returning [] because that instance
   840|             # happens to have zero). A CLR generic-type reflection check
   841|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   842|             # found not to reliably discriminate types against a live
   843|             # Revit/pythonnet session (still produced the same false
   844|             # positives), so it was dropped rather than kept as an
   845|             # unreliable safety net. Per this project's fail-soft principle
   846|             # (never silently collapse distinct states), an empty collection
   847|             # of unconfirmed item type gets its own explicit q value instead
   848|             # of defaulting to "ok" (would reintroduce this exact bug) or
   849|             # bare "unsupported" (would make it indistinguishable from a
   850|             # totally opaque complex-object failure). storage stays "None"
   851|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   852|             # _is_elementid_typed() correctly does not treat this as a
   853|             # reference candidate.
   854|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   855|         disp = ",".join(str(i) for i in ids)
   856|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   857|     except:
   858|         pass
   859|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   860| 
   861| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   862|     idx = {}
   863|     for obj in sample_objs:
   864|         if obj is None:
   865|             continue
   866|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   867|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   868|             key = "refl.{}.{}".format(type_label, name)
   869|             if key not in idx:
   870|                 idx[key] = {
   871|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   872|                     "type_label": type_label, "example": None, "example_error": None,
   873|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   874|                 }
   875|             e = idx[key]
   876|             if not ok:
   877|                 e["error_count"] += 1
   878|                 if e["example_error"] is None and err:
   879|                     e["example_error"] = err
   880|                 continue
   881|             contract = _reflect_contract(raw_v)
   882|             e["ok_count"] += 1
   883|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   884|             if sig not in e["_seen"]:
   885|                 e["_seen"].add(sig)
   886|                 e["unique_value_count"] += 1
   887|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   888|                 e["example"] = contract
   889|     records = []
   890|     for key in sorted(idx.keys()):
   891|         e = idx[key]
   892|         records.append({
   893|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   894|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   895|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   896|         })
   897|     return records
   898| 
   899| _reflection_records_0 = _run_reflection_sweep(selected, "GraphicsStyle", "line_styles")
   900| _reflection_records = _reflection_records_0
   901| 
   902| # Assemble labeled output payload
   903| OUT_payload = [
   904|     {
   905|         "kind": "reflection",
   906|         "domain": "line_styles",
   907|         "records": _reflection_records
   908|     },
   909|     {
   910|         "kind": "inventory",
   911|         "domain": "line_styles",
   912|         "records": param_inventory
   913|     },
   914|     {
   915|         "kind": "crosswalk",
   916|         "domain": "line_styles",
   917|         "records": optional_crosswalk
   918|     }
   919| ]
   920| 
   921| # Optional: write to JSON (valid JSON, stable order)
   922| file_written = None
   923| write_error = None
   924| 
   925| # -------------------------
   926| # Unified run metadata (release-separated, not date-filename-separated)
   927| # -------------------------
   928| # extraction_date lives as JSON metadata, not as a filename token; the
   929| # filename groups by Revit release (revit_version) plus an opaque run_id so
   930| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
   931| # which consumes this shape directly.
   932| 
   933| import uuid as _uuid_mod
   934| 
   935| def _probe_revit_version():
   936|     try:
   937|         _uiapp = DocumentManager.Instance.CurrentUIApplication
   938|         _app = _uiapp.Application if _uiapp is not None else None
   939|         v = _safe(lambda: _app.VersionNumber, None)
   940|         return str(v) if v else None
   941|     except:
   942|         return None
   943| 
   944| def _probe_document_identity():
   945|     return {
   946|         "title": _safe(lambda: doc.Title, None),
   947|         "path_name": _safe(lambda: doc.PathName, None),
   948|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
   949|     }
   950| 
   951| def _probe_run_id():
   952|     try:
   953|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
   954|     except:
   955|         return _uuid_mod.uuid4().hex[:12]
   956| 
   957| _PROBE_RUN_ID = _probe_run_id()
   958| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
   959| 
   960| def _probe_wrap(domain, out_payload):
   961|     return {
   962|         "run_metadata": {
   963|             "run_id": _PROBE_RUN_ID,
   964|             "extraction_date": datetime.now().isoformat(),
   965|             "revit_version": _PROBE_REVIT_VERSION,
   966|             "tool_version": None,
   967|             "document": _probe_document_identity(),
   968|             "source": "single_probe",
   969|             "probe": domain,
   970|         },
   971|         "domains": {domain: out_payload},
   972|     }
```
