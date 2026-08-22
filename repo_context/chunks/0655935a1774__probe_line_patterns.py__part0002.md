# Chunk of tools/probes/probe_line_patterns.py

- Source relative path: `tools/probes/probe_line_patterns.py`
- Chunk: 2 of 3
- Original line range: 489-989
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _iter_line_style_categories, _category_line_pattern_id, _resolve_workset, _reflect_member_names, _reflect_try_get, _reflect_contract, _run_reflection_sweep
- Source SHA-256: 09adef2ca571518818011587b6cc8376cc961624aa6e3b91af87f79082b3d74b
- Starts inside symbol: no
- Ends inside symbol: no

```
   489| 
   490| for e in selected:
   491|     # GetLinePattern: match production extractor fallback behavior
   492|     lp = None
   493|     try:
   494|         lp = e.GetLinePattern()
   495|     except Exception:
   496|         lp = None
   497| 
   498|     if lp is None:
   499|         try:
   500|             # Static fallback is required in some environments
   501|             lp = LinePatternElement.GetLinePattern(doc, e.Id)
   502|         except Exception:
   503|             lp = None
   504| 
   505|     seg_count, is_solid, h, meta = _linepattern_signature(lp)
   506|     bucket_key = meta.get("bucket") if meta else ("lp=None" if lp is None else "unknown")
   507| 
   508|     # Real parameters (if any exist for LinePatternElement in this environment)
   509|     params = _safe(lambda: list(e.GetOrderedParameters()), default=None)
   510|     if params is None:
   511|         params = _safe(lambda: list(e.Parameters), default=[])
   512| 
   513|     for p in params:
   514|         dn = _safe(lambda: _safe_param_def_name(p), None)
   515|         if not dn:
   516|             continue
   517|         pk = "p.{}".format(dn)
   518|         pv = _format_param_contract(p)
   519|         _touch_param(pk, pv, bucket_key)
   520| 
   521|     # Synthetic properties (these are typically the meaningful surface for line patterns)
   522|     name = _safe_elem_name(e)
   523|     _touch_param("prop.name", _contract_from_raw("ok", "String", name, name, name), bucket_key)
   524| 
   525|     if seg_count is None:
   526|         _touch_param(
   527|             "prop.segment_count",
   528|             _contract_from_raw("unreadable", "Integer", None, None, None),
   529|             bucket_key
   530|         )
   531|     else:
   532|         _touch_param(
   533|             "prop.segment_count",
   534|             _contract_from_raw("ok", "Integer", seg_count, str(seg_count), seg_count),
   535|             bucket_key
   536|         )
   537| 
   538|     solid_raw = 1 if is_solid else 0
   539|     _touch_param("prop.is_solid", _contract_from_raw("ok", "Integer", solid_raw, str(bool(is_solid)), solid_raw), bucket_key)
   540| 
   541|     if h is None:
   542|         _touch_param("prop.sequence_hash", _contract_from_raw("unreadable", "String", None, None, None), bucket_key)
   543|     else:
   544|         _touch_param("prop.sequence_hash", _contract_from_raw("ok", "String", h, h, h), bucket_key)
   545| 
   546|     seq_str = None
   547|     if meta is not None:
   548|         try:
   549|             seq_str = "|".join(meta.get("seq") or [])
   550|         except Exception:
   551|             seq_str = None
   552| 
   553|     if seq_str is None:
   554|         _touch_param("prop.sequence", _contract_from_raw("unreadable", "String", None, None, None), bucket_key)
   555|     else:
   556|         _touch_param("prop.sequence", _contract_from_raw("ok", "String", seq_str, seq_str, seq_str), bucket_key)
   557| 
   558| # Emit inventory records (stable order)
   559| param_inventory = []
   560| for pk in sorted(param_index.keys()):
   561|     e = param_index[pk]
   562|     param_inventory.append({
   563|         "domain": "line_patterns",
   564|         "param_key": pk,
   565|         "selected_sample_count": len(selected),
   566|         "example": e["example"],
   567|         "observed": {
   568|             "storage_types": sorted(list(e["storage_types"])),
   569|             "q_counts": e["q_counts"],
   570|             "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25],
   571|             "bucket_counts": e.get("bucket_counts") or {}
   572|         }
   573|     })
   574| 
   575| # -------------------------
   576| # Optional Crosswalk: LineStyle -> LinePattern
   577| # -------------------------
   578| 
   579| optional_crosswalk = []
   580| 
   581| def _iter_line_style_categories():
   582|     """
   583|     Prefer category-driven discovery for line styles only (crosswalk),
   584|     because LineStyle is not a distinct element class we can collect directly.
   585| 
   586|     Returns Categories (subcategories) under OST_Lines when available.
   587|     """
   588|     cats = _safe(lambda: doc.Settings.Categories, None)
   589|     if cats is None:
   590|         return []
   591|     lines_cat = _safe(lambda: cats.get_Item(BuiltInCategory.OST_Lines), None)
   592|     if lines_cat is None:
   593|         return []
   594| 
   595|     subs = _safe(lambda: list(lines_cat.SubCategories), default=[])
   596|     try:
   597|         subs = list(subs)
   598|     except:
   599|         subs = list(subs)
   600| 
   601|     return subs
   602| 
   603| def _category_line_pattern_id(cat, gst):
   604|     # Some categories may throw or return InvalidElementId
   605|     try:
   606|         return cat.GetLinePatternId(gst)
   607|     except:
   608|         return ElementId.InvalidElementId
   609| 
   610| def _resolve_workset(doc, ws_id_obj):
   611|     """Resolve an Element.WorksetId value to (name, resolved_bool) via
   612|     WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   613|     distinct .NET type from ElementId (both happen to expose .IntegerValue,
   614|     which is why reflection reports this member as ElementId-storage), and
   615|     Workset is not derived from Element, so doc.GetElement() would never
   616|     resolve it even with the right type assumed."""
   617|     if ws_id_obj is None:
   618|         return (None, False)
   619|     wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   620|     if wt_table is None:
   621|         return (None, False)
   622|     ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   623|     if ws is None:
   624|         return (None, False)
   625|     name = _safe(lambda: ws.Name, None)
   626|     return (name, name is not None)
   627| 
   628| 
   629| # Build quick lookup: pattern_id -> name / workset. The crosswalk row's
   630| # subject is a line-style Category (linestyle.category_id/.name), and
   631| # Category is not an Element -- it has no WorksetId at all. The pattern it
   632| # resolves to (pattern.id/.name) IS an Element (LinePatternElement), so
   633| # that's the side WorksetId belongs on.
   634| pattern_name_by_id = {}
   635| pattern_workset_by_id = {}
   636| for pe in all_patterns:
   637|     pid = _safe(lambda: pe.Id.IntegerValue, None)
   638|     if pid is not None and pid not in pattern_name_by_id:
   639|         pattern_name_by_id[pid] = _safe_elem_name(pe)
   640|         pe_ws_id_obj = _safe(lambda: pe.WorksetId, None)
   641|         pe_ws_name, _pe_ws_resolved = _resolve_workset(doc, pe_ws_id_obj)
   642|         pe_ws_id_int = _safe(lambda: pe_ws_id_obj.IntegerValue, None) if pe_ws_id_obj is not None else None
   643|         pattern_workset_by_id[pid] = (pe_ws_id_int, pe_ws_name)
   644| 
   645| if enable_crosswalk:
   646|     crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 50
   647| 
   648|     seen = set()  # (gst_label, pattern_id)
   649|     gst_plan = [
   650|         (GraphicsStyleType.Projection, "Projection"),
   651|         (GraphicsStyleType.Cut, "Cut")
   652|     ]
   653| 
   654|     for gst, gst_label in gst_plan:
   655|         for cat in _iter_line_style_categories():
   656|             if len(optional_crosswalk) >= int(crosswalk_limit):
   657|                 break
   658| 
   659|             row = {
   660|                 "linestyle.category_id": _safe(lambda: cat.Id.IntegerValue, None),
   661|                 "linestyle.name": _safe(lambda: cat.Name, None),
   662|                 "linestyle.graphics_style_type": gst_label,
   663|                 "pattern.resolved": False,
   664|                 "pattern.id": None,
   665|                 "pattern.name": None
   666|             }
   667| 
   668|             pid = _category_line_pattern_id(cat, gst)
   669|             if pid is None or pid == ElementId.InvalidElementId:
   670|                 continue
   671| 
   672|             raw = _safe(lambda: pid.IntegerValue, None)
   673|             if raw is None:
   674|                 continue
   675| 
   676|             k = (gst_label, raw)
   677|             if k in seen:
   678|                 continue
   679| 
   680|             row["pattern.id"] = raw
   681|             row["pattern.name"] = pattern_name_by_id.get(raw)
   682| 
   683|             if row["pattern.name"] is None:
   684|                 ref = _safe(lambda: doc.GetElement(pid), None)
   685|                 row["pattern.name"] = _safe_elem_name(ref) if ref is not None else None
   686| 
   687|             row["pattern.resolved"] = True if row["pattern.name"] is not None else False
   688|             if not row["pattern.resolved"]:
   689|                 continue
   690| 
   691|             p_ws_id_int, p_ws_name = pattern_workset_by_id.get(raw, (None, None))
   692|             row["pattern.workset_id"] = p_ws_id_int
   693|             row["pattern.workset_name"] = p_ws_name
   694| 
   695|             seen.add(k)
   696|             optional_crosswalk.append(row)
   697| 
   698| 
   699| # -------------------------
   700| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   701| # -------------------------
   702| # Complements the curated/dynamic capture above with a breadth-only sweep of
   703| # the sampled objects' .NET properties and zero-arg methods. This is
   704| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   705| # key list or a Parameters-only walk could otherwise miss.
   706| 
   707| _REFLECTION_SKIP = set([
   708|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   709|     "Dispose", "GetEnumerator", "Clone",
   710| ])
   711| 
   712| def _reflect_member_names(obj):
   713|     out = []
   714|     if obj is None:
   715|         return out
   716|     try:
   717|         t = obj.GetType()
   718|     except:
   719|         return out
   720|     try:
   721|         for p in t.GetProperties():
   722|             try:
   723|                 n = p.Name
   724|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   725|                     continue
   726|                 if p.GetIndexParameters():
   727|                     continue
   728|                 out.append(("property", n))
   729|             except:
   730|                 pass
   731|     except:
   732|         pass
   733|     try:
   734|         for m in t.GetMethods():
   735|             try:
   736|                 n = m.Name
   737|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   738|                     continue
   739|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   740|                     continue
   741|                 if m.GetParameters().Length != 0:
   742|                     continue
   743|                 if m.IsSpecialName:
   744|                     continue
   745|                 out.append(("method", n))
   746|             except:
   747|                 pass
   748|     except:
   749|         pass
   750|     seen = set()
   751|     uniq = []
   752|     for kind, n in out:
   753|         if n in seen:
   754|             continue
   755|         seen.add(n)
   756|         uniq.append((kind, n))
   757|     return sorted(uniq, key=lambda x: x[1])
   758| 
   759| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   760| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   761| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   762| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   763| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   764| # see the notes below the dict) are ground-truth confirmed, against the live
   765| # RevitAPI 2025 documentation (not name/return-type inference), to be
   766| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   767| # from the branching logic in _reflect_try_get below, so it can be reviewed
   768| # as one block and extended later without touching control flow.
   769| #
   770| # Keyed by method NAME only, not (declaring_class, name): this reflection
   771| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   772| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   773| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   774| # is reached in this codebase via more than a dozen different concrete
   775| # type_labels across the probe domains, never via type_label=="Element"
   776| # itself, since no probe in this file reflects a bare Element instance.
   777| # Scoping the allowlist by declaring-class name would silently fail to match
   778| # nearly every real call site and defeat the point of this allowlist.
   779| # _reflect_member_names() below already restricts candidate methods to
   780| # public, non-special-name, zero-parameter methods before this allowlist is
   781| # ever consulted, so a name-only match here does not weaken the zero-arg/
   782| # no-side-effect intent the allowlist exists to enforce -- it only widens
   783| # which already-zero-arg, already-name-matched members get invoked. The
   784| # dict value (declaring class) is kept for traceability back to the Step 0
   785| # CSV only; it is not used in the match.
   786| _ALLOWLISTED_REFLECTION_METHODS = {
   787|     "GetTypeId": "Element",
   788|     "GetLayers": "CompoundStructure",
   789|     "GetEntitySchemaGuids": "Element",
   790|     "GetSubelements": "Element",
   791|     "GetFamilyPointLocations": "FamilySymbol",
   792|     "GetModelToProjectionTransforms": "View",
   793|     "GetRenderingAsset": "AppearanceAssetElement",
   794|     "GetExternalFileReference": "Element",
   795|     "GetMonitoredLinkElementIds": "Element",
   796|     "GetMonitoredLocalElementIds": "Element",
   797|     "GetSimilarTypes": "ElementType",
   798|     "GetStructuralSection": "FamilySymbol",
   799|     "GetThermalProperties": "FamilySymbol",
   800|     "GetFillPattern": "FillPatternElement",
   801|     "GetCategories": "ParameterFilterElement",
   802|     "GetElementFilter": "ParameterFilterElement",
   803|     "GetReference": "Subelement",
   804|     "GetBackground": "View",
   805|     "GetCalloutParentId": "View",
   806|     "GetCropRegionShapeManager": "View",
   807|     "GetDepthCueing": "View",
   808|     "GetDirectContext3DHandleOverrides": "View",
   809|     "GetFilters": "View",
   810|     "GetOrderedFilters": "View",
   811|     "GetPointCloudOverrides": "View",
   812|     "GetPrimaryViewId": "View",
   813|     "GetReferenceCallouts": "View",
   814|     "GetReferenceElevations": "View",
   815|     "GetReferenceSections": "View",
   816|     "GetSketchyLines": "View",
   817|     "GetTemporaryViewPropertiesId": "View",
   818|     "GetViewDisplayModel": "View",
   819| }
   820| 
   821| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   822| # allowlist above after a live re-run (PR #395 discussion) showed
   823| # Element.GetValidTypes fails 100% of the time -- not with a documented
   824| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   825| # GetModelToProjectionTransforms above, which each match a real
   826| # InvalidOperationException precondition stated on their own RevitAPI doc
   827| # pages), but with a CLR/pythonnet interop binding failure:
   828| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   829| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   830| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   831| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   832| # overload-ambiguity problem either -- the call is rejected by the binder
   833| # before it ever reaches Revit's implementation. This will never succeed
   834| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   835| # allowlisted only adds permanent error noise with zero chance of a real
   836| # value. Subelement.GetValidTypes was never independently tested (no probe
   837| # in this codebase reflects a raw Subelement object as its own type_label),
   838| # but shares the same allowlist name and the same removal; re-evaluate
   839| # independently against a live Subelement instance before re-adding either.
   840| 
   841| # LinePatternElement.GetLinePattern was removed from the allowlist above
   842| # after a live re-run (PR #398's exception-capture work, which surfaced the
   843| # error text for the first time) showed it fails 100% of the time -- not
   844| # with a documented Revit API exception (unlike GetCalloutParentId/
   845| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   846| # match a real InvalidOperationException precondition stated on their own
   847| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   848| # failure family as Element.GetValidTypes above: `TypeError: No method
   849| # matches given arguments for GetLinePattern: (<class
   850| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   851| # binder before it ever reaches Revit's implementation, so it cannot
   852| # succeed through `getattr(obj, name)()` regardless of model, Revit
   853| # version, or which element is sampled -- keeping it allowlisted only adds
   854| # permanent error noise for zero chance of real data.
   855| 
   856| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   857| # see the identity check in _reflect_contract below for why this must never be
   858| # comparable-by-value to a real Revit return.
   859| 
   860| def _reflect_try_get(obj, member_kind, name):
   861|     if member_kind == "method":
   862|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   863|             # SAFETY: never invoke a reflection-discovered method that is not
   864|             # on the allowlist above. Revit API methods can have side effects
   865|             # (printing, export, regenerate, delete, transaction commits,
   866|             # ...) and there is no reliable way to tell a safe zero-arg
   867|             # query method from a side-effecting one by name alone for
   868|             # anything outside the allowlist's ground-truth-verified set.
   869|             # Record that the method exists without calling it.
   870|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   871|         try:
   872|             v = getattr(obj, name)()
   873|         except Exception as ex:
   874|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   875|         return (True, v, None)
   876|     try:
   877|         v = getattr(obj, name)
   878|     except Exception as ex:
   879|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   880|     return (True, v, None)
   881| 
   882| def _reflect_contract(raw_v):
   883|     if raw_v is None:
   884|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   885|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   886|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   887|         # unique object(), never a string, specifically so a genuine reflected
   888|         # property or allowlisted-method return whose real value happens to be
   889|         # the literal text "<method not invoked>" cannot collide with this
   890|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   891|         # an earlier version of this check compared by value against a string
   892|         # constant, which had exactly that collision risk). Checked before
   893|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   894|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   895|     if isinstance(raw_v, bool):
   896|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   897|     if isinstance(raw_v, int):
   898|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   899|     if isinstance(raw_v, float):
   900|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   901|     if isinstance(raw_v, str):
   902|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   903|     try:
   904|         if hasattr(raw_v, "IntegerValue"):
   905|             iv = int(raw_v.IntegerValue)
   906|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   907|     except:
   908|         pass
   909|     try:
   910|         if hasattr(raw_v, "ToString"):
   911|             s = raw_v.ToString()
   912|             if s and "Autodesk.Revit" not in s and "System." not in s:
   913|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   914|     except:
   915|         pass
   916|     try:
   917|         ids = []
   918|         saw_item = False
   919|         for item in raw_v:
   920|             saw_item = True
   921|             if not hasattr(item, "IntegerValue"):
   922|                 raise TypeError("non-ElementId item in collection")
   923|             ids.append(int(item.IntegerValue))
   924|         if not saw_item:
   925|             # An empty collection is vacuously "every item has .IntegerValue"
   926|             # -- there's nothing to fail the check against, so item-by-item
   927|             # duck-typing alone can never tell an empty ElementId collection
   928|             # (GetMonitoredLinkElementIds returning [] because a type has no
   929|             # monitored links) apart from an empty collection of anything
   930|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   931|             # IList<Subelement>, both returning [] because that instance
   932|             # happens to have zero). A CLR generic-type reflection check
   933|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   934|             # found not to reliably discriminate types against a live
   935|             # Revit/pythonnet session (still produced the same false
   936|             # positives), so it was dropped rather than kept as an
   937|             # unreliable safety net. Per this project's fail-soft principle
   938|             # (never silently collapse distinct states), an empty collection
   939|             # of unconfirmed item type gets its own explicit q value instead
   940|             # of defaulting to "ok" (would reintroduce this exact bug) or
   941|             # bare "unsupported" (would make it indistinguishable from a
   942|             # totally opaque complex-object failure). storage stays "None"
   943|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   944|             # _is_elementid_typed() correctly does not treat this as a
   945|             # reference candidate.
   946|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   947|         disp = ",".join(str(i) for i in ids)
   948|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   949|     except:
   950|         pass
   951|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   952| 
   953| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   954|     idx = {}
   955|     for obj in sample_objs:
   956|         if obj is None:
   957|             continue
   958|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   959|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   960|             key = "refl.{}.{}".format(type_label, name)
   961|             if key not in idx:
   962|                 idx[key] = {
   963|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   964|                     "type_label": type_label, "example": None, "example_error": None,
   965|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   966|                 }
   967|             e = idx[key]
   968|             if not ok:
   969|                 e["error_count"] += 1
   970|                 if e["example_error"] is None and err:
   971|                     e["example_error"] = err
   972|                 continue
   973|             contract = _reflect_contract(raw_v)
   974|             e["ok_count"] += 1
   975|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   976|             if sig not in e["_seen"]:
   977|                 e["_seen"].add(sig)
   978|                 e["unique_value_count"] += 1
   979|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   980|                 e["example"] = contract
   981|     records = []
   982|     for key in sorted(idx.keys()):
   983|         e = idx[key]
   984|         records.append({
   985|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   986|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   987|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   988|         })
   989|     return records
```
