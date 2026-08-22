# Chunk of tools/probes/probe_fill_patterns.py

- Source relative path: `tools/probes/probe_fill_patterns.py`
- Chunk: 2 of 3
- Original line range: 511-1025
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _resolve_workset, _reflect_member_names, _reflect_try_get, _reflect_contract, _run_reflection_sweep, _probe_revit_version, _probe_document_identity, _probe_run_id
- Source SHA-256: b7e1557e7ca19327a8137f4deb5ab42ac2779f1fdaf52c22e2c857bbd8e6f712
- Starts inside symbol: no
- Ends inside symbol: no

```
   511| 
   512| 
   513| for fpe in selected:
   514|     bk = _bucket_key_for_fill_pattern(fpe)
   515| 
   516|     # Revit parameters on FillPatternElement
   517|     params = _safe(lambda: list(fpe.GetOrderedParameters()), default=None)
   518|     if params is None:
   519|         params = _safe(lambda: list(fpe.Parameters), default=[])
   520| 
   521|     for p in params:
   522|         dn = _safe(lambda: _safe_param_def_name(p), None)
   523|         if not dn:
   524|             continue
   525|         pk = "p.{}".format(dn)
   526|         pv = _format_param_contract(p)
   527|         _observe(pk, pv, bk)
   528| 
   529|     # Computed surface for the FillPattern itself
   530|     _add_computed_surface(fpe, bk)
   531| 
   532| 
   533| # Emit inventory records (stable order)
   534| param_inventory = []
   535| for pk in sorted(param_index.keys()):
   536|     e = param_index[pk]
   537|     param_inventory.append({
   538|         "domain": "fill_patterns",
   539|         "param_key": pk,
   540|         "selected_element_sample_count": len(selected),
   541|         "example": e["example"],
   542|         "observed": {
   543|             "storage_types": sorted(list(e["storage_types"])),
   544|             "q_counts": e["q_counts"],
   545|             "observed_on_buckets": sorted(list(e["breadth"]["observed_bucket_keys"]))[:25]
   546|         }
   547|     })
   548| 
   549| 
   550| # -------------------------
   551| # Crosswalk: FillPatternElement -> its own id/name/workset (unconditional --
   552| # this domain had no crosswalk kind at all before (see Step 0 findings: zero
   553| # rows in PROBE_CROSSWALK.csv), so this is "built from nothing" the same way
   554| # loaded_family_types' crosswalk was, reusing the existing `selected` sample
   555| # rather than a new collector pass. NOT gated behind enable_crosswalk, unlike
   556| # the FillPattern -> LinePattern grid join below -- that one legitimately
   557| # needs the toggle (GetFillGrids() is heavier and optional), this one is a
   558| # cheap direct property read per already-sampled element).
   559| # -------------------------
   560| optional_crosswalk = []
   561| 
   562| 
   563| def _resolve_workset(doc, ws_id_obj):
   564|     """Resolve an Element.WorksetId value to (name, resolved_bool) via
   565|     WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   566|     distinct .NET type from ElementId (both happen to expose .IntegerValue,
   567|     which is why reflection reports this member as ElementId-storage), and
   568|     Workset is not derived from Element, so doc.GetElement() would never
   569|     resolve it even with the right type assumed."""
   570|     if ws_id_obj is None:
   571|         return (None, False)
   572|     wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   573|     if wt_table is None:
   574|         return (None, False)
   575|     ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   576|     if ws is None:
   577|         return (None, False)
   578|     name = _safe(lambda: ws.Name, None)
   579|     return (name, name is not None)
   580| 
   581| 
   582| for fpe in selected:
   583|     fp_id = _safe(lambda: fpe.Id.IntegerValue, None)
   584|     if fp_id is None:
   585|         continue
   586|     fp_name = _safe(lambda: _safe_type_name(fpe), None)
   587|     fp_ws_id_obj = _safe(lambda: fpe.WorksetId, None)
   588|     fp_ws_name, _fp_ws_resolved = _resolve_workset(doc, fp_ws_id_obj)
   589|     fp_ws_id_int = _safe(lambda: fp_ws_id_obj.IntegerValue, None) if fp_ws_id_obj is not None else None
   590|     optional_crosswalk.append({
   591|         "fill_pattern.id": fp_id,
   592|         "fill_pattern.name": fp_name,
   593|         "fill_pattern.workset_id": fp_ws_id_int,
   594|         "fill_pattern.workset_name": fp_ws_name,
   595|     })
   596| 
   597| 
   598| # -------------------------
   599| # Optional Crosswalk: FillPattern -> LinePattern (via FillGrid.LinePatternId)
   600| # -------------------------
   601| 
   602| if enable_crosswalk:
   603|     # Optional extra input: max crosswalk rows to emit (default 25)
   604|     crosswalk_limit = IN[6] if len(IN) > 6 and IN[6] is not None else 25
   605| 
   606|     seen_line_pattern_ids = set()
   607| 
   608|     for fpe in selected:
   609|         if len(optional_crosswalk) >= int(crosswalk_limit):
   610|             break
   611| 
   612|         fp = _safe(lambda: fpe.GetFillPattern(), None)
   613|         if fp is None:
   614|             continue
   615| 
   616|         fp_id = _safe(lambda: fpe.Id.IntegerValue, None)
   617|         fp_name = _safe(lambda: _safe_type_name(fpe), None)
   618|         fp_target = _safe(lambda: str(fp.Target), None)
   619|         fp_ws_id_obj = _safe(lambda: fpe.WorksetId, None)
   620|         fp_ws_name, _fp_ws_resolved = _resolve_workset(doc, fp_ws_id_obj)
   621|         fp_ws_id_int = _safe(lambda: fp_ws_id_obj.IntegerValue, None) if fp_ws_id_obj is not None else None
   622| 
   623|         grids = _safe(lambda: fp.GetFillGrids(), default=None)
   624|         if grids is None:
   625|             continue
   626|         try:
   627|             grids = list(grids)
   628|         except:
   629|             grids = list(grids)
   630| 
   631|         max_g = None
   632|         try:
   633|             max_g = int(max_grids_per_pattern)
   634|         except:
   635|             max_g = 4
   636| 
   637|         for gi, g in enumerate(grids):
   638|             if len(optional_crosswalk) >= int(crosswalk_limit):
   639|                 break
   640|             if max_g is not None and max_g >= 0 and gi >= max_g:
   641|                 break
   642| 
   643|             lp = _safe(lambda: g.LinePatternId, None)
   644|             if lp is None or lp == ElementId.InvalidElementId:
   645|                 continue
   646| 
   647|             lp_id = _safe(lambda: lp.IntegerValue, None)
   648|             if lp_id is None:
   649|                 continue
   650| 
   651|             if lp_id in seen_line_pattern_ids:
   652|                 continue
   653| 
   654|             lp_elem = _safe(lambda: doc.GetElement(lp), None)
   655|             lp_name = _safe(lambda: lp_elem.Name, None) if lp_elem is not None else None
   656| 
   657|             optional_crosswalk.append({
   658|                 "fill_pattern.id": fp_id,
   659|                 "fill_pattern.name": fp_name,
   660|                 "fill_pattern.target": fp_target,
   661|                 "fill_pattern.workset_id": fp_ws_id_int,
   662|                 "fill_pattern.workset_name": fp_ws_name,
   663|                 "grid.index": gi,
   664|                 "line_pattern.id": lp_id,
   665|                 "line_pattern.name": lp_name,
   666|             })
   667| 
   668|             seen_line_pattern_ids.add(lp_id)
   669| 
   670| 
   671| 
   672| # -------------------------
   673| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   674| # -------------------------
   675| # Complements the curated/dynamic capture above with a breadth-only sweep of
   676| # the sampled objects' .NET properties and zero-arg methods. This is
   677| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   678| # key list or a Parameters-only walk could otherwise miss.
   679| 
   680| _REFLECTION_SKIP = set([
   681|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   682|     "Dispose", "GetEnumerator", "Clone",
   683| ])
   684| 
   685| def _reflect_member_names(obj):
   686|     out = []
   687|     if obj is None:
   688|         return out
   689|     try:
   690|         t = obj.GetType()
   691|     except:
   692|         return out
   693|     try:
   694|         for p in t.GetProperties():
   695|             try:
   696|                 n = p.Name
   697|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   698|                     continue
   699|                 if p.GetIndexParameters():
   700|                     continue
   701|                 out.append(("property", n))
   702|             except:
   703|                 pass
   704|     except:
   705|         pass
   706|     try:
   707|         for m in t.GetMethods():
   708|             try:
   709|                 n = m.Name
   710|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   711|                     continue
   712|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   713|                     continue
   714|                 if m.GetParameters().Length != 0:
   715|                     continue
   716|                 if m.IsSpecialName:
   717|                     continue
   718|                 out.append(("method", n))
   719|             except:
   720|                 pass
   721|     except:
   722|         pass
   723|     seen = set()
   724|     uniq = []
   725|     for kind, n in out:
   726|         if n in seen:
   727|             continue
   728|         seen.add(n)
   729|         uniq.append((kind, n))
   730|     return sorted(uniq, key=lambda x: x[1])
   731| 
   732| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   733| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   734| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   735| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   736| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   737| # see the notes below the dict) are ground-truth confirmed, against the live
   738| # RevitAPI 2025 documentation (not name/return-type inference), to be
   739| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   740| # from the branching logic in _reflect_try_get below, so it can be reviewed
   741| # as one block and extended later without touching control flow.
   742| #
   743| # Keyed by method NAME only, not (declaring_class, name): this reflection
   744| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   745| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   746| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   747| # is reached in this codebase via more than a dozen different concrete
   748| # type_labels across the probe domains, never via type_label=="Element"
   749| # itself, since no probe in this file reflects a bare Element instance.
   750| # Scoping the allowlist by declaring-class name would silently fail to match
   751| # nearly every real call site and defeat the point of this allowlist.
   752| # _reflect_member_names() below already restricts candidate methods to
   753| # public, non-special-name, zero-parameter methods before this allowlist is
   754| # ever consulted, so a name-only match here does not weaken the zero-arg/
   755| # no-side-effect intent the allowlist exists to enforce -- it only widens
   756| # which already-zero-arg, already-name-matched members get invoked. The
   757| # dict value (declaring class) is kept for traceability back to the Step 0
   758| # CSV only; it is not used in the match.
   759| _ALLOWLISTED_REFLECTION_METHODS = {
   760|     "GetTypeId": "Element",
   761|     "GetLayers": "CompoundStructure",
   762|     "GetEntitySchemaGuids": "Element",
   763|     "GetSubelements": "Element",
   764|     "GetFamilyPointLocations": "FamilySymbol",
   765|     "GetModelToProjectionTransforms": "View",
   766|     "GetRenderingAsset": "AppearanceAssetElement",
   767|     "GetExternalFileReference": "Element",
   768|     "GetMonitoredLinkElementIds": "Element",
   769|     "GetMonitoredLocalElementIds": "Element",
   770|     "GetSimilarTypes": "ElementType",
   771|     "GetStructuralSection": "FamilySymbol",
   772|     "GetThermalProperties": "FamilySymbol",
   773|     "GetFillPattern": "FillPatternElement",
   774|     "GetCategories": "ParameterFilterElement",
   775|     "GetElementFilter": "ParameterFilterElement",
   776|     "GetReference": "Subelement",
   777|     "GetBackground": "View",
   778|     "GetCalloutParentId": "View",
   779|     "GetCropRegionShapeManager": "View",
   780|     "GetDepthCueing": "View",
   781|     "GetDirectContext3DHandleOverrides": "View",
   782|     "GetFilters": "View",
   783|     "GetOrderedFilters": "View",
   784|     "GetPointCloudOverrides": "View",
   785|     "GetPrimaryViewId": "View",
   786|     "GetReferenceCallouts": "View",
   787|     "GetReferenceElevations": "View",
   788|     "GetReferenceSections": "View",
   789|     "GetSketchyLines": "View",
   790|     "GetTemporaryViewPropertiesId": "View",
   791|     "GetViewDisplayModel": "View",
   792| }
   793| 
   794| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   795| # allowlist above after a live re-run (PR #395 discussion) showed
   796| # Element.GetValidTypes fails 100% of the time -- not with a documented
   797| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   798| # GetModelToProjectionTransforms above, which each match a real
   799| # InvalidOperationException precondition stated on their own RevitAPI doc
   800| # pages), but with a CLR/pythonnet interop binding failure:
   801| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   802| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   803| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   804| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   805| # overload-ambiguity problem either -- the call is rejected by the binder
   806| # before it ever reaches Revit's implementation. This will never succeed
   807| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   808| # allowlisted only adds permanent error noise with zero chance of a real
   809| # value. Subelement.GetValidTypes was never independently tested (no probe
   810| # in this codebase reflects a raw Subelement object as its own type_label),
   811| # but shares the same allowlist name and the same removal; re-evaluate
   812| # independently against a live Subelement instance before re-adding either.
   813| 
   814| # LinePatternElement.GetLinePattern was removed from the allowlist above
   815| # after a live re-run (PR #398's exception-capture work, which surfaced the
   816| # error text for the first time) showed it fails 100% of the time -- not
   817| # with a documented Revit API exception (unlike GetCalloutParentId/
   818| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   819| # match a real InvalidOperationException precondition stated on their own
   820| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   821| # failure family as Element.GetValidTypes above: `TypeError: No method
   822| # matches given arguments for GetLinePattern: (<class
   823| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   824| # binder before it ever reaches Revit's implementation, so it cannot
   825| # succeed through `getattr(obj, name)()` regardless of model, Revit
   826| # version, or which element is sampled -- keeping it allowlisted only adds
   827| # permanent error noise for zero chance of real data.
   828| 
   829| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   830| # see the identity check in _reflect_contract below for why this must never be
   831| # comparable-by-value to a real Revit return.
   832| 
   833| def _reflect_try_get(obj, member_kind, name):
   834|     if member_kind == "method":
   835|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   836|             # SAFETY: never invoke a reflection-discovered method that is not
   837|             # on the allowlist above. Revit API methods can have side effects
   838|             # (printing, export, regenerate, delete, transaction commits,
   839|             # ...) and there is no reliable way to tell a safe zero-arg
   840|             # query method from a side-effecting one by name alone for
   841|             # anything outside the allowlist's ground-truth-verified set.
   842|             # Record that the method exists without calling it.
   843|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   844|         try:
   845|             v = getattr(obj, name)()
   846|         except Exception as ex:
   847|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   848|         return (True, v, None)
   849|     try:
   850|         v = getattr(obj, name)
   851|     except Exception as ex:
   852|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   853|     return (True, v, None)
   854| 
   855| def _reflect_contract(raw_v):
   856|     if raw_v is None:
   857|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   858|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   859|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   860|         # unique object(), never a string, specifically so a genuine reflected
   861|         # property or allowlisted-method return whose real value happens to be
   862|         # the literal text "<method not invoked>" cannot collide with this
   863|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   864|         # an earlier version of this check compared by value against a string
   865|         # constant, which had exactly that collision risk). Checked before
   866|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   867|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   868|     if isinstance(raw_v, bool):
   869|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   870|     if isinstance(raw_v, int):
   871|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   872|     if isinstance(raw_v, float):
   873|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   874|     if isinstance(raw_v, str):
   875|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   876|     try:
   877|         if hasattr(raw_v, "IntegerValue"):
   878|             iv = int(raw_v.IntegerValue)
   879|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   880|     except:
   881|         pass
   882|     try:
   883|         if hasattr(raw_v, "ToString"):
   884|             s = raw_v.ToString()
   885|             if s and "Autodesk.Revit" not in s and "System." not in s:
   886|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   887|     except:
   888|         pass
   889|     try:
   890|         ids = []
   891|         saw_item = False
   892|         for item in raw_v:
   893|             saw_item = True
   894|             if not hasattr(item, "IntegerValue"):
   895|                 raise TypeError("non-ElementId item in collection")
   896|             ids.append(int(item.IntegerValue))
   897|         if not saw_item:
   898|             # An empty collection is vacuously "every item has .IntegerValue"
   899|             # -- there's nothing to fail the check against, so item-by-item
   900|             # duck-typing alone can never tell an empty ElementId collection
   901|             # (GetMonitoredLinkElementIds returning [] because a type has no
   902|             # monitored links) apart from an empty collection of anything
   903|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   904|             # IList<Subelement>, both returning [] because that instance
   905|             # happens to have zero). A CLR generic-type reflection check
   906|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   907|             # found not to reliably discriminate types against a live
   908|             # Revit/pythonnet session (still produced the same false
   909|             # positives), so it was dropped rather than kept as an
   910|             # unreliable safety net. Per this project's fail-soft principle
   911|             # (never silently collapse distinct states), an empty collection
   912|             # of unconfirmed item type gets its own explicit q value instead
   913|             # of defaulting to "ok" (would reintroduce this exact bug) or
   914|             # bare "unsupported" (would make it indistinguishable from a
   915|             # totally opaque complex-object failure). storage stays "None"
   916|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   917|             # _is_elementid_typed() correctly does not treat this as a
   918|             # reference candidate.
   919|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   920|         disp = ",".join(str(i) for i in ids)
   921|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   922|     except:
   923|         pass
   924|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   925| 
   926| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   927|     idx = {}
   928|     for obj in sample_objs:
   929|         if obj is None:
   930|             continue
   931|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   932|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   933|             key = "refl.{}.{}".format(type_label, name)
   934|             if key not in idx:
   935|                 idx[key] = {
   936|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   937|                     "type_label": type_label, "example": None, "example_error": None,
   938|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   939|                 }
   940|             e = idx[key]
   941|             if not ok:
   942|                 e["error_count"] += 1
   943|                 if e["example_error"] is None and err:
   944|                     e["example_error"] = err
   945|                 continue
   946|             contract = _reflect_contract(raw_v)
   947|             e["ok_count"] += 1
   948|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   949|             if sig not in e["_seen"]:
   950|                 e["_seen"].add(sig)
   951|                 e["unique_value_count"] += 1
   952|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   953|                 e["example"] = contract
   954|     records = []
   955|     for key in sorted(idx.keys()):
   956|         e = idx[key]
   957|         records.append({
   958|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   959|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   960|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   961|         })
   962|     return records
   963| 
   964| _reflection_records_0 = _run_reflection_sweep(selected, "FillPatternElement", "fill_patterns")
   965| _reflection_records = _reflection_records_0
   966| 
   967| # Assemble labeled output payload
   968| OUT_payload = [
   969|     {
   970|         "kind": "reflection",
   971|         "domain": "fill_patterns",
   972|         "records": _reflection_records
   973|     },
   974|     {
   975|         "kind": "inventory",
   976|         "domain": "fill_patterns",
   977|         "records": param_inventory
   978|     },
   979|     {
   980|         "kind": "crosswalk",
   981|         "domain": "fill_patterns",
   982|         "records": optional_crosswalk
   983|     }
   984| ]
   985| 
   986| 
   987| # Optional: write to JSON for future reference (valid JSON, stable order)
   988| file_written = None
   989| write_error = None
   990| 
   991| # -------------------------
   992| # Unified run metadata (release-separated, not date-filename-separated)
   993| # -------------------------
   994| # extraction_date lives as JSON metadata, not as a filename token; the
   995| # filename groups by Revit release (revit_version) plus an opaque run_id so
   996| # repeated runs don't collide. See tools/probes/build_probe_inventory.py,
   997| # which consumes this shape directly.
   998| 
   999| import uuid as _uuid_mod
  1000| 
  1001| def _probe_revit_version():
  1002|     try:
  1003|         _uiapp = DocumentManager.Instance.CurrentUIApplication
  1004|         _app = _uiapp.Application if _uiapp is not None else None
  1005|         v = _safe(lambda: _app.VersionNumber, None)
  1006|         return str(v) if v else None
  1007|     except:
  1008|         return None
  1009| 
  1010| def _probe_document_identity():
  1011|     return {
  1012|         "title": _safe(lambda: doc.Title, None),
  1013|         "path_name": _safe(lambda: doc.PathName, None),
  1014|         "is_workshared": _safe(lambda: bool(doc.IsWorkshared), None),
  1015|     }
  1016| 
  1017| def _probe_run_id():
  1018|     try:
  1019|         return datetime.now().strftime("%Y%m%dT%H%M%S") + "-" + _uuid_mod.uuid4().hex[:6]
  1020|     except:
  1021|         return _uuid_mod.uuid4().hex[:12]
  1022| 
  1023| _PROBE_RUN_ID = _probe_run_id()
  1024| _PROBE_REVIT_VERSION = _probe_revit_version() or "unknown"
  1025| 
```
