# Chunk of tools/probes/probe_phase_filters.py

- Source relative path: `tools/probes/probe_phase_filters.py`
- Chunk: 2 of 2
- Original line range: 520-1022
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _resolve_workset, _reflect_member_names, _reflect_try_get, _reflect_contract, _run_reflection_sweep, _probe_revit_version, _probe_document_identity, _probe_run_id, _probe_wrap
- Source SHA-256: a8d1433926c6953d854df7580a5b6491f41b1441d9e0e3f29235eaf75698242c
- Starts inside symbol: no
- Ends inside symbol: no

```
   520| def _resolve_workset(doc, ws_id_obj):
   521|     """Resolve an Element.WorksetId value to (name, resolved_bool) via
   522|     WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   523|     distinct .NET type from ElementId (both happen to expose .IntegerValue,
   524|     which is why reflection reports this member as ElementId-storage), and
   525|     Workset is not derived from Element, so doc.GetElement() would never
   526|     resolve it even with the right type assumed."""
   527|     if ws_id_obj is None:
   528|         return (None, False)
   529|     wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   530|     if wt_table is None:
   531|         return (None, False)
   532|     ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   533|     if ws is None:
   534|         return (None, False)
   535|     name = _safe(lambda: ws.Name, None)
   536|     return (name, name is not None)
   537| 
   538| 
   539| phase_filter_name_by_id = {}
   540| phase_filter_workset_by_id = {}
   541| for pf in phase_filters:
   542|     pid = _safe(lambda: pf.Id.IntegerValue, None)
   543|     if pid is not None and pid not in phase_filter_name_by_id:
   544|         phase_filter_name_by_id[pid] = _safe(lambda: _safe_elem_name(pf), None)
   545|         pf_ws_id_obj = _safe(lambda: pf.WorksetId, None)
   546|         pf_ws_name, _pf_ws_resolved = _resolve_workset(doc, pf_ws_id_obj)
   547|         pf_ws_id_int = _safe(lambda: pf_ws_id_obj.IntegerValue, None) if pf_ws_id_obj is not None else None
   548|         phase_filter_workset_by_id[pid] = (pf_ws_id_int, pf_ws_name)
   549| 
   550| if enable_crosswalk:
   551|     views = _safe(
   552|         lambda: (FilteredElementCollector(doc)
   553|                  .OfClass(View)
   554|                  .ToElements()),
   555|         default=[]
   556|     )
   557|     try:
   558|         views = list(views)
   559|     except:
   560|         views = list(views)
   561| 
   562|     # Limit scan explicitly (avoid whole-model view scan on huge files)
   563|     try:
   564|         vcap = int(max_views_to_scan)
   565|         if vcap >= 0:
   566|             views = views[:vcap]
   567|     except:
   568|         pass
   569| 
   570|     # Keep crosswalk compact: one representative view per distinct phase_filter_id
   571|     seen_pf_ids = set()
   572| 
   573|     for v in views:
   574|         if v is None:
   575|             continue
   576|         # Skip view templates if easily detectable (older builds may differ)
   577|         is_template = _safe(lambda: v.IsTemplate, False)
   578|         if is_template:
   579|             continue
   580| 
   581|         matched_name, p = _get_view_phase_filter_param(v)
   582|         pv = _format_param_contract(p)
   583| 
   584|         # keep only ElementId payloads with a value
   585|         if pv.get("storage") != "ElementId" or pv.get("raw") is None:
   586|             continue
   587| 
   588|         pf_id = int(pv.get("raw"))
   589|         if pf_id in seen_pf_ids:
   590|             continue
   591| 
   592|         row = {
   593|             "view.id": _safe(lambda: v.Id.IntegerValue, None),
   594|             "view.name": _safe(lambda: v.Name, None),
   595|             "phase_filter_param.matched_name": matched_name,
   596|             "phase_filter_param": pv,
   597|             "phase_filter.resolved": False,
   598|             "phase_filter.id": pf_id,
   599|             "phase_filter.name": phase_filter_name_by_id.get(pf_id)
   600|         }
   601| 
   602|         if row["phase_filter.name"] is None:
   603|             ref = _safe(lambda: doc.GetElement(ElementId(pf_id)), None)
   604|             row["phase_filter.name"] = _safe(lambda: _safe_elem_name(ref), None) if ref is not None else None
   605| 
   606|         row["phase_filter.resolved"] = True if row["phase_filter.name"] is not None else False
   607| 
   608|         if not row["phase_filter.resolved"]:
   609|             continue
   610| 
   611|         pf_ws_id_int, pf_ws_name = phase_filter_workset_by_id.get(pf_id, (None, None))
   612|         row["phase_filter.workset_id"] = pf_ws_id_int
   613|         row["phase_filter.workset_name"] = pf_ws_name
   614| 
   615|         seen_pf_ids.add(pf_id)
   616|         optional_crosswalk.append(row)
   617| 
   618| # Assemble labeled output payload
   619| 
   620| # -------------------------
   621| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   622| # -------------------------
   623| # Complements the curated/dynamic capture above with a breadth-only sweep of
   624| # the sampled objects' .NET properties and zero-arg methods. This is
   625| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   626| # key list or a Parameters-only walk could otherwise miss.
   627| 
   628| _REFLECTION_SKIP = set([
   629|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   630|     "Dispose", "GetEnumerator", "Clone",
   631| ])
   632| 
   633| def _reflect_member_names(obj):
   634|     out = []
   635|     if obj is None:
   636|         return out
   637|     try:
   638|         t = obj.GetType()
   639|     except:
   640|         return out
   641|     try:
   642|         for p in t.GetProperties():
   643|             try:
   644|                 n = p.Name
   645|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   646|                     continue
   647|                 if p.GetIndexParameters():
   648|                     continue
   649|                 out.append(("property", n))
   650|             except:
   651|                 pass
   652|     except:
   653|         pass
   654|     try:
   655|         for m in t.GetMethods():
   656|             try:
   657|                 n = m.Name
   658|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   659|                     continue
   660|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   661|                     continue
   662|                 if m.GetParameters().Length != 0:
   663|                     continue
   664|                 if m.IsSpecialName:
   665|                     continue
   666|                 out.append(("method", n))
   667|             except:
   668|                 pass
   669|     except:
   670|         pass
   671|     seen = set()
   672|     uniq = []
   673|     for kind, n in out:
   674|         if n in seen:
   675|             continue
   676|         seen.add(n)
   677|         uniq.append((kind, n))
   678|     return sorted(uniq, key=lambda x: x[1])
   679| 
   680| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   681| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   682| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   683| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   684| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   685| # see the notes below the dict) are ground-truth confirmed, against the live
   686| # RevitAPI 2025 documentation (not name/return-type inference), to be
   687| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   688| # from the branching logic in _reflect_try_get below, so it can be reviewed
   689| # as one block and extended later without touching control flow.
   690| #
   691| # Keyed by method NAME only, not (declaring_class, name): this reflection
   692| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   693| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   694| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   695| # is reached in this codebase via more than a dozen different concrete
   696| # type_labels across the probe domains, never via type_label=="Element"
   697| # itself, since no probe in this file reflects a bare Element instance.
   698| # Scoping the allowlist by declaring-class name would silently fail to match
   699| # nearly every real call site and defeat the point of this allowlist.
   700| # _reflect_member_names() below already restricts candidate methods to
   701| # public, non-special-name, zero-parameter methods before this allowlist is
   702| # ever consulted, so a name-only match here does not weaken the zero-arg/
   703| # no-side-effect intent the allowlist exists to enforce -- it only widens
   704| # which already-zero-arg, already-name-matched members get invoked. The
   705| # dict value (declaring class) is kept for traceability back to the Step 0
   706| # CSV only; it is not used in the match.
   707| _ALLOWLISTED_REFLECTION_METHODS = {
   708|     "GetTypeId": "Element",
   709|     "GetLayers": "CompoundStructure",
   710|     "GetEntitySchemaGuids": "Element",
   711|     "GetSubelements": "Element",
   712|     "GetFamilyPointLocations": "FamilySymbol",
   713|     "GetModelToProjectionTransforms": "View",
   714|     "GetRenderingAsset": "AppearanceAssetElement",
   715|     "GetExternalFileReference": "Element",
   716|     "GetMonitoredLinkElementIds": "Element",
   717|     "GetMonitoredLocalElementIds": "Element",
   718|     "GetSimilarTypes": "ElementType",
   719|     "GetStructuralSection": "FamilySymbol",
   720|     "GetThermalProperties": "FamilySymbol",
   721|     "GetFillPattern": "FillPatternElement",
   722|     "GetCategories": "ParameterFilterElement",
   723|     "GetElementFilter": "ParameterFilterElement",
   724|     "GetReference": "Subelement",
   725|     "GetBackground": "View",
   726|     "GetCalloutParentId": "View",
   727|     "GetCropRegionShapeManager": "View",
   728|     "GetDepthCueing": "View",
   729|     "GetDirectContext3DHandleOverrides": "View",
   730|     "GetFilters": "View",
   731|     "GetOrderedFilters": "View",
   732|     "GetPointCloudOverrides": "View",
   733|     "GetPrimaryViewId": "View",
   734|     "GetReferenceCallouts": "View",
   735|     "GetReferenceElevations": "View",
   736|     "GetReferenceSections": "View",
   737|     "GetSketchyLines": "View",
   738|     "GetTemporaryViewPropertiesId": "View",
   739|     "GetViewDisplayModel": "View",
   740| }
   741| 
   742| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   743| # allowlist above after a live re-run (PR #395 discussion) showed
   744| # Element.GetValidTypes fails 100% of the time -- not with a documented
   745| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   746| # GetModelToProjectionTransforms above, which each match a real
   747| # InvalidOperationException precondition stated on their own RevitAPI doc
   748| # pages), but with a CLR/pythonnet interop binding failure:
   749| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   750| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   751| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   752| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   753| # overload-ambiguity problem either -- the call is rejected by the binder
   754| # before it ever reaches Revit's implementation. This will never succeed
   755| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   756| # allowlisted only adds permanent error noise with zero chance of a real
   757| # value. Subelement.GetValidTypes was never independently tested (no probe
   758| # in this codebase reflects a raw Subelement object as its own type_label),
   759| # but shares the same allowlist name and the same removal; re-evaluate
   760| # independently against a live Subelement instance before re-adding either.
   761| 
   762| # LinePatternElement.GetLinePattern was removed from the allowlist above
   763| # after a live re-run (PR #398's exception-capture work, which surfaced the
   764| # error text for the first time) showed it fails 100% of the time -- not
   765| # with a documented Revit API exception (unlike GetCalloutParentId/
   766| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   767| # match a real InvalidOperationException precondition stated on their own
   768| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   769| # failure family as Element.GetValidTypes above: `TypeError: No method
   770| # matches given arguments for GetLinePattern: (<class
   771| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   772| # binder before it ever reaches Revit's implementation, so it cannot
   773| # succeed through `getattr(obj, name)()` regardless of model, Revit
   774| # version, or which element is sampled -- keeping it allowlisted only adds
   775| # permanent error noise for zero chance of real data.
   776| 
   777| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   778| # see the identity check in _reflect_contract below for why this must never be
   779| # comparable-by-value to a real Revit return.
   780| 
   781| def _reflect_try_get(obj, member_kind, name):
   782|     if member_kind == "method":
   783|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   784|             # SAFETY: never invoke a reflection-discovered method that is not
   785|             # on the allowlist above. Revit API methods can have side effects
   786|             # (printing, export, regenerate, delete, transaction commits,
   787|             # ...) and there is no reliable way to tell a safe zero-arg
   788|             # query method from a side-effecting one by name alone for
   789|             # anything outside the allowlist's ground-truth-verified set.
   790|             # Record that the method exists without calling it.
   791|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   792|         try:
   793|             v = getattr(obj, name)()
   794|         except Exception as ex:
   795|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   796|         return (True, v, None)
   797|     try:
   798|         v = getattr(obj, name)
   799|     except Exception as ex:
   800|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   801|     return (True, v, None)
   802| 
   803| def _reflect_contract(raw_v):
   804|     if raw_v is None:
   805|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   806|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   807|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   808|         # unique object(), never a string, specifically so a genuine reflected
   809|         # property or allowlisted-method return whose real value happens to be
   810|         # the literal text "<method not invoked>" cannot collide with this
   811|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   812|         # an earlier version of this check compared by value against a string
   813|         # constant, which had exactly that collision risk). Checked before
   814|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   815|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   816|     if isinstance(raw_v, bool):
   817|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   818|     if isinstance(raw_v, int):
   819|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   820|     if isinstance(raw_v, float):
   821|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   822|     if isinstance(raw_v, str):
   823|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   824|     try:
   825|         if hasattr(raw_v, "IntegerValue"):
   826|             iv = int(raw_v.IntegerValue)
   827|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   828|     except:
   829|         pass
   830|     try:
   831|         if hasattr(raw_v, "ToString"):
   832|             s = raw_v.ToString()
   833|             if s and "Autodesk.Revit" not in s and "System." not in s:
   834|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   835|     except:
   836|         pass
   837|     try:
   838|         ids = []
   839|         saw_item = False
   840|         for item in raw_v:
   841|             saw_item = True
   842|             if not hasattr(item, "IntegerValue"):
   843|                 raise TypeError("non-ElementId item in collection")
   844|             ids.append(int(item.IntegerValue))
   845|         if not saw_item:
   846|             # An empty collection is vacuously "every item has .IntegerValue"
   847|             # -- there's nothing to fail the check against, so item-by-item
   848|             # duck-typing alone can never tell an empty ElementId collection
   849|             # (GetMonitoredLinkElementIds returning [] because a type has no
   850|             # monitored links) apart from an empty collection of anything
   851|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   852|             # IList<Subelement>, both returning [] because that instance
   853|             # happens to have zero). A CLR generic-type reflection check
   854|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   855|             # found not to reliably discriminate types against a live
   856|             # Revit/pythonnet session (still produced the same false
   857|             # positives), so it was dropped rather than kept as an
   858|             # unreliable safety net. Per this project's fail-soft principle
   859|             # (never silently collapse distinct states), an empty collection
   860|             # of unconfirmed item type gets its own explicit q value instead
   861|             # of defaulting to "ok" (would reintroduce this exact bug) or
   862|             # bare "unsupported" (would make it indistinguishable from a
   863|             # totally opaque complex-object failure). storage stays "None"
   864|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   865|             # _is_elementid_typed() correctly does not treat this as a
   866|             # reference candidate.
   867|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   868|         disp = ",".join(str(i) for i in ids)
   869|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   870|     except:
   871|         pass
   872|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   873| 
   874| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   875|     idx = {}
   876|     for obj in sample_objs:
   877|         if obj is None:
   878|             continue
   879|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   880|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   881|             key = "refl.{}.{}".format(type_label, name)
   882|             if key not in idx:
   883|                 idx[key] = {
   884|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   885|                     "type_label": type_label, "example": None, "example_error": None,
   886|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   887|                 }
   888|             e = idx[key]
   889|             if not ok:
   890|                 e["error_count"] += 1
   891|                 if e["example_error"] is None and err:
   892|                     e["example_error"] = err
   893|                 continue
   894|             contract = _reflect_contract(raw_v)
   895|             e["ok_count"] += 1
   896|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   897|             if sig not in e["_seen"]:
   898|                 e["_seen"].add(sig)
   899|                 e["unique_value_count"] += 1
   900|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   901|                 e["example"] = contract
   902|     records = []
   903|     for key in sorted(idx.keys()):
   904|         e = idx[key]
   905|         records.append({
   906|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   907|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   908|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   909|         })
   910|     return records
   911| 
   912| _reflection_records_0 = _run_reflection_sweep(phase_filters, "PhaseFilter", "phase_filters")
   913| _reflection_records = _reflection_records_0
   914| 
   915| OUT_payload = [
   916|     {
   917|         "kind": "reflection",
   918|         "domain": "phase_filters",
   919|         "records": _reflection_records
   920|     },
   921|     {
   922|         "kind": "inventory",
   923|         "domain": "phase_filters",
   924|         "records": param_inventory
   925|     },
   926|     {
   927|         "kind": "crosswalk",
   928|         "domain": "phase_filters",
   929|         "records": optional_crosswalk
   930|     }
   931| ]
   932| 
   933| # Optional: write to JSON for future reference (valid JSON, stable order)
   934| file_written = None
   935| write_error = None
   936| 
   937| # -------------------------
   938| # Unified run metadata (release-separated, not date-filename-separated)
   939| # -------------------------
   940| # extraction_date lives as JSON metadata, not as a filename token; the
   941| # filename groups by Revit release (revit_version) plus an opaque run_id so
   942| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
   943| # which consumes this shape directly.
   944| 
   945| import uuid as _uuid_mod
   946| 
   947| def _probe_revit_version():
   948|     try:
   949|         _uiapp = DocumentManager.Instance.CurrentUIApplication
   950|         _app = _uiapp.Application if _uiapp is not None else None
   951|         v = _safe(lambda: _app.VersionNumber, None)
   952|         return str(v) if v else None
   953|     except:
   954|         return None
   955| 
   956| def _probe_document_identity():
   957|     return {
   958|         "title": _safe(lambda: doc.Title, None),
   959|         "path_name": _safe(lambda: doc.PathName, None),
   960|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
   961|     }
   962| 
   963| def _probe_run_id():
   964|     try:
   965|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
   966|     except:
   967|         return _uuid_mod.uuid4().hex[:12]
   968| 
   969| _PROBE_RUN_ID = _probe_run_id()
   970| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
   971| 
   972| def _probe_wrap(domain, out_payload):
   973|     return {
   974|         "run_metadata": {
   975|             "run_id": _PROBE_RUN_ID,
   976|             "extraction_date": datetime.now().isoformat(),
   977|             "revit_version": _PROBE_REVIT_VERSION,
   978|             "tool_version": None,
   979|             "document": _probe_document_identity(),
   980|             "source": "single_probe",
   981|             "probe": domain,
   982|         },
   983|         "domains": {domain: out_payload},
   984|     }
   985| 
   986| 
   987| if write_json:
   988|     try:
   989|         # Choose default directory: RVT folder if possible, else temp
   990|         rvt_path = _safe(lambda: doc.PathName, None)
   991|         default_dir = None
   992| 
   993|         if rvt_path and isinstance(rvt_path, str) and len(rvt_path) > 0:
   994|             default_dir = _safe(lambda: os.path.dirname(rvt_path), None)
   995| 
   996|         if not default_dir:
   997|             default_dir = os.environ.get("TEMP") or os.environ.get("TMP") or os.getcwd()
   998| 
   999|         date_stamp = datetime.now().strftime("%Y-%m-%d")
  1000|         fixed_name = "probes_{}_{}.json".format(_PROBE_REVIT_VERSION, _PROBE_RUN_ID)
  1001| 
  1002|         # IN[4] is treated as an output directory (not a filename)
  1003|         target_dir = out_path if out_path else default_dir
  1004|         target_path = os.path.join(target_dir, fixed_name)
  1005| 
  1006|         if target_dir and not os.path.exists(target_dir):
  1007|             os.makedirs(target_dir)
  1008| 
  1009|         with open(target_path, "w") as f:
  1010|             json.dump(_probe_wrap("phase_filters", OUT_payload), f, indent=2, sort_keys=True)
  1011| 
  1012|         file_written = target_path
  1013| 
  1014|     except Exception as ex:
  1015|         write_error = "{}: {}".format(type(ex).__name__, ex)
  1016| 
  1017| # Attach write metadata to inventory header (keeps OUT shape stable)
  1018| OUT_payload[0]["file_written"] = file_written
  1019| if write_error:
  1020|     OUT_payload[0]["file_write_error"] = write_error
  1021| 
  1022| OUT = OUT_payload
```
