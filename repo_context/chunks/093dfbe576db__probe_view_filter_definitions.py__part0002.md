# Chunk of tools/probes/probe_view_filter_definitions.py

- Source relative path: `tools/probes/probe_view_filter_definitions.py`
- Chunk: 2 of 3
- Original line range: 334-851
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _reflect_member_names, _reflect_try_get, _reflect_contract, _run_reflection_sweep
- Source SHA-256: 55514de8160889f7abe33d957fabe54c77f668e09919199e92f16fe13ccf2c44
- Starts inside symbol: no
- Ends inside symbol: no

```
   334| 
   335| 
   336| # -------------------------
   337| # Discovery + Sampling
   338| # -------------------------
   339| 
   340| filters = _safe(
   341|     lambda: list(FilteredElementCollector(doc).OfClass(ParameterFilterElement).ToElements()),
   342|     default=[]
   343| )
   344| 
   345| # Cap collector list early (then apply bucketing) to avoid pathological docs
   346| try:
   347|     mf = int(max_filters_to_inspect)
   348|     if mf >= 0:
   349|         filters = filters[:mf]
   350| except:
   351|     pass
   352| 
   353| selected = []
   354| bucket_counts = {}  # category_sig -> count
   355| 
   356| for f in filters:
   357|     cat_ids = _safe(lambda: list(f.GetCategories()), default=[])
   358|     cat_ints = []
   359|     for cid in cat_ids:
   360|         try:
   361|             if isinstance(cid, ElementId):
   362|                 cat_ints.append(int(cid.IntegerValue))
   363|             else:
   364|                 cat_ints.append(int(cid))
   365|         except:
   366|             continue
   367|     cat_ints_sorted = sorted(list(set(cat_ints)))
   368|     cat_sig = "|".join([str(i) for i in cat_ints_sorted])
   369| 
   370|     c = bucket_counts.get(cat_sig, 0)
   371| 
   372|     if per_category_sig_limit is None:
   373|         ok = True
   374|     else:
   375|         try:
   376|             ok = c < int(per_category_sig_limit)
   377|         except:
   378|             ok = c < 5
   379| 
   380|     if ok:
   381|         selected.append(f)
   382|         bucket_counts[cat_sig] = c + 1
   383| 
   384| # Fallback: if bucketing excluded everything, take first few
   385| if len(selected) == 0 and len(filters) > 0:
   386|     selected = filters[:min(25, len(filters))]
   387| 
   388| 
   389| # -------------------------
   390| # Build inventory (synthetic param surface over selected filters)
   391| # -------------------------
   392| 
   393| param_index = {}
   394| 
   395| for f in selected:
   396|     # Bucket label for breadth
   397|     cat_ids = _safe(lambda: list(f.GetCategories()), default=[])
   398|     cat_ints = []
   399|     for cid in cat_ids:
   400|         try:
   401|             cat_ints.append(int(cid.IntegerValue) if isinstance(cid, ElementId) else int(cid))
   402|         except:
   403|             continue
   404|     cat_ints_sorted = sorted(list(set(cat_ints)))
   405|     bucket_label = _bucket_label_from_categories(cat_ints_sorted)
   406| 
   407|     # vfd.id
   408|     fid = _safe(lambda: f.Id.IntegerValue, None)
   409|     _observe(param_index, "v.filter.id", _as_param_payload("ok", "Integer", fid, str(fid) if fid is not None else None, fid), bucket_label)
   410| 
   411|     # vfd.name (ParameterFilterElement.Name)
   412|     nm = _safe(lambda: f.Name, None)
   413|     if nm is None:
   414|         _observe(param_index, "v.filter.name", _as_param_payload("missing", "String", None, None, None), bucket_label)
   415|     else:
   416|         _observe(param_index, "v.filter.name", _as_param_payload("ok", "String", nm, nm, nm), bucket_label)
   417| 
   418|     # vfd.categories.ids (stable string norm)
   419|     if cat_ints_sorted is None:
   420|         _observe(param_index, "v.filter.category_ids", _as_param_payload("unreadable", "String", None, None, None), bucket_label)
   421|     else:
   422|         raw_ids = cat_ints_sorted
   423|         norm_ids = "|".join([str(i) for i in raw_ids])
   424|         _observe(
   425|             param_index,
   426|             "v.filter.category_ids",
   427|             _as_param_payload("ok", "String", raw_ids, norm_ids, norm_ids),
   428|             bucket_label
   429|         )
   430| 
   431|     # vfd.categories.names (best-effort)
   432|     cat_names = []
   433|     for ci in cat_ints_sorted:
   434|         n = _resolve_category_name(ci)
   435|         if n:
   436|             cat_names.append(n)
   437|     cat_names_sorted = sorted(list(set(cat_names))) if cat_names else []
   438|     disp_names = "|".join(cat_names_sorted) if cat_names_sorted else None
   439|     if disp_names is None:
   440|         _observe(param_index, "v.filter.category_names", _as_param_payload("missing", "String", None, None, None), bucket_label)
   441|     else:
   442|         _observe(param_index, "v.filter.category_names", _as_param_payload("ok", "String", cat_names_sorted, disp_names, disp_names), bucket_label)
   443| 
   444|     # vfd.category_count
   445|     cc = len(cat_ints_sorted) if cat_ints_sorted is not None else None
   446|     if cc is None:
   447|         _observe(param_index, "v.filter.category_count", _as_param_payload("unreadable", "Integer", None, None, None), bucket_label)
   448|     else:
   449|         _observe(param_index, "v.filter.category_count", _as_param_payload("ok", "Integer", cc, str(cc), cc), bucket_label)
   450| 
   451|     # vfd.logic + rules (flatten element filter)
   452|     ef = _safe(lambda: f.GetElementFilter(), default=None)
   453|     logic, rules = _flatten_element_filter(ef, int(max_rules_to_read_per_filter) if max_rules_to_read_per_filter is not None else 200)
   454| 
   455|     _observe(param_index, "v.filter.logic", _as_param_payload("ok", "String", logic, logic, logic), bucket_label)
   456| 
   457|     # vfd.rule_count
   458|     rc = len(rules) if rules is not None else 0
   459|     _observe(param_index, "v.filter.rule_count", _as_param_payload("ok", "Integer", rc, str(rc), rc), bucket_label)
   460| 
   461|     # vfd.rule_types (set -> stable string)
   462|     rtypes = []
   463|     if rules:
   464|         for r in rules:
   465|             rt = r.get("rule.type")
   466|             if rt:
   467|                 rtypes.append(rt)
   468|     rtypes_sorted = sorted(list(set(rtypes)))
   469| 
   470|     # zero-rule filters are a valid state, not "missing"
   471|     if rc == 0:
   472|         _observe(
   473|             param_index,
   474|             "v.filter.rule_types",
   475|             _as_param_payload("ok", "String", "", "", ""),
   476|             bucket_label
   477|         )
   478|     else:
   479|         rtypes_disp = "|".join(rtypes_sorted) if rtypes_sorted else ""
   480|         _observe(
   481|             param_index,
   482|             "v.filter.rule_types",
   483|             _as_param_payload("ok", "String", rtypes_disp, rtypes_disp, rtypes_disp),
   484|             bucket_label
   485|         )
   486| 
   487|     # vfd.rule_param_ids (unique)
   488|     rpids = []
   489|     if rules:
   490|         for r in rules:
   491|             pid = r.get("rule.param_id")
   492|             if pid is not None:
   493|                 try:
   494|                     rpids.append(int(pid))
   495|                 except:
   496|                     continue
   497|     rpids_sorted = sorted(list(set(rpids)))
   498| 
   499|     # zero-rule filters are a valid state, not "missing"
   500|     if rc == 0:
   501|         _observe(
   502|             param_index,
   503|             "v.filter.rule_param_ids",
   504|             _as_param_payload("ok", "String", "", "", ""),
   505|             bucket_label
   506|         )
   507|     else:
   508|         rpids_disp = "|".join([str(i) for i in rpids_sorted]) if rpids_sorted else ""
   509|         _observe(
   510|             param_index,
   511|             "v.filter.rule_param_ids",
   512|             _as_param_payload("ok", "String", rpids_disp, rpids_disp, rpids_disp),
   513|             bucket_label
   514|         )
   515| 
   516|     # vfd.rule_sig_hash (join-key candidate; stable signature over rules)
   517|     # Signature uses: rule.type, rule.param_id, rule.evaluator, stringified rule.value
   518|     sig_parts = []
   519|     if rules:
   520|         for r in rules:
   521|             rt = r.get("rule.type")
   522|             pid = r.get("rule.param_id")
   523|             ev = r.get("rule.evaluator")
   524|             vv = r.get("rule.value")
   525|             sig_parts.append("{}|{}|{}|{}".format(
   526|                 str(rt) if rt is not None else "",
   527|                 str(pid) if pid is not None else "",
   528|                 str(ev) if ev is not None else "",
   529|                 str(vv) if vv is not None else ""
   530|             ))
   531|     sig_text = "||".join(sig_parts)
   532|     sig_hash = _sha1(sig_text) if sig_text is not None else None
   533|     if sig_hash is None:
   534|         _observe(param_index, "v.filter.rule_sig_hash", _as_param_payload("unreadable", "String", None, None, None), bucket_label)
   535|     else:
   536|         _observe(param_index, "v.filter.rule_sig_hash", _as_param_payload("ok", "String", sig_hash, sig_hash, sig_hash), bucket_label)
   537| 
   538| 
   539| # Emit inventory records (stable order)
   540| param_inventory = []
   541| for pk in sorted(param_index.keys()):
   542|     e = param_index[pk]
   543|     param_inventory.append({
   544|         "domain": "view_filter_definitions",
   545|         "param_key": pk,
   546|         "selected_filter_sample_count": len(selected),
   547|         "example": e["example"],
   548|         "observed": {
   549|             "storage_types": sorted(list(e["storage_types"])),
   550|             "q_counts": e["q_counts"],
   551|             "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25]
   552|         }
   553|     })
   554| 
   555| 
   556| # -------------------------
   557| # Assemble labeled output payload
   558| # -------------------------
   559| 
   560| 
   561| # -------------------------
   562| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   563| # -------------------------
   564| # Complements the curated/dynamic capture above with a breadth-only sweep of
   565| # the sampled objects' .NET properties and zero-arg methods. This is
   566| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   567| # key list or a Parameters-only walk could otherwise miss.
   568| 
   569| _REFLECTION_SKIP = set([
   570|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   571|     "Dispose", "GetEnumerator", "Clone",
   572| ])
   573| 
   574| def _reflect_member_names(obj):
   575|     out = []
   576|     if obj is None:
   577|         return out
   578|     try:
   579|         t = obj.GetType()
   580|     except:
   581|         return out
   582|     try:
   583|         for p in t.GetProperties():
   584|             try:
   585|                 n = p.Name
   586|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   587|                     continue
   588|                 if p.GetIndexParameters():
   589|                     continue
   590|                 out.append(("property", n))
   591|             except:
   592|                 pass
   593|     except:
   594|         pass
   595|     try:
   596|         for m in t.GetMethods():
   597|             try:
   598|                 n = m.Name
   599|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   600|                     continue
   601|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   602|                     continue
   603|                 if m.GetParameters().Length != 0:
   604|                     continue
   605|                 if m.IsSpecialName:
   606|                     continue
   607|                 out.append(("method", n))
   608|             except:
   609|                 pass
   610|     except:
   611|         pass
   612|     seen = set()
   613|     uniq = []
   614|     for kind, n in out:
   615|         if n in seen:
   616|             continue
   617|         seen.add(n)
   618|         uniq.append((kind, n))
   619|     return sorted(uniq, key=lambda x: x[1])
   620| 
   621| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   622| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   623| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   624| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   625| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   626| # see the notes below the dict) are ground-truth confirmed, against the live
   627| # RevitAPI 2025 documentation (not name/return-type inference), to be
   628| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   629| # from the branching logic in _reflect_try_get below, so it can be reviewed
   630| # as one block and extended later without touching control flow.
   631| #
   632| # Keyed by method NAME only, not (declaring_class, name): this reflection
   633| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   634| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   635| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   636| # is reached in this codebase via more than a dozen different concrete
   637| # type_labels across the probe domains, never via type_label=="Element"
   638| # itself, since no probe in this file reflects a bare Element instance.
   639| # Scoping the allowlist by declaring-class name would silently fail to match
   640| # nearly every real call site and defeat the point of this allowlist.
   641| # _reflect_member_names() below already restricts candidate methods to
   642| # public, non-special-name, zero-parameter methods before this allowlist is
   643| # ever consulted, so a name-only match here does not weaken the zero-arg/
   644| # no-side-effect intent the allowlist exists to enforce -- it only widens
   645| # which already-zero-arg, already-name-matched members get invoked. The
   646| # dict value (declaring class) is kept for traceability back to the Step 0
   647| # CSV only; it is not used in the match.
   648| _ALLOWLISTED_REFLECTION_METHODS = {
   649|     "GetTypeId": "Element",
   650|     "GetLayers": "CompoundStructure",
   651|     "GetEntitySchemaGuids": "Element",
   652|     "GetSubelements": "Element",
   653|     "GetFamilyPointLocations": "FamilySymbol",
   654|     "GetModelToProjectionTransforms": "View",
   655|     "GetRenderingAsset": "AppearanceAssetElement",
   656|     "GetExternalFileReference": "Element",
   657|     "GetMonitoredLinkElementIds": "Element",
   658|     "GetMonitoredLocalElementIds": "Element",
   659|     "GetSimilarTypes": "ElementType",
   660|     "GetStructuralSection": "FamilySymbol",
   661|     "GetThermalProperties": "FamilySymbol",
   662|     "GetFillPattern": "FillPatternElement",
   663|     "GetCategories": "ParameterFilterElement",
   664|     "GetElementFilter": "ParameterFilterElement",
   665|     "GetReference": "Subelement",
   666|     "GetBackground": "View",
   667|     "GetCalloutParentId": "View",
   668|     "GetCropRegionShapeManager": "View",
   669|     "GetDepthCueing": "View",
   670|     "GetDirectContext3DHandleOverrides": "View",
   671|     "GetFilters": "View",
   672|     "GetOrderedFilters": "View",
   673|     "GetPointCloudOverrides": "View",
   674|     "GetPrimaryViewId": "View",
   675|     "GetReferenceCallouts": "View",
   676|     "GetReferenceElevations": "View",
   677|     "GetReferenceSections": "View",
   678|     "GetSketchyLines": "View",
   679|     "GetTemporaryViewPropertiesId": "View",
   680|     "GetViewDisplayModel": "View",
   681| }
   682| 
   683| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   684| # allowlist above after a live re-run (PR #395 discussion) showed
   685| # Element.GetValidTypes fails 100% of the time -- not with a documented
   686| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   687| # GetModelToProjectionTransforms above, which each match a real
   688| # InvalidOperationException precondition stated on their own RevitAPI doc
   689| # pages), but with a CLR/pythonnet interop binding failure:
   690| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   691| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   692| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   693| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   694| # overload-ambiguity problem either -- the call is rejected by the binder
   695| # before it ever reaches Revit's implementation. This will never succeed
   696| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   697| # allowlisted only adds permanent error noise with zero chance of a real
   698| # value. Subelement.GetValidTypes was never independently tested (no probe
   699| # in this codebase reflects a raw Subelement object as its own type_label),
   700| # but shares the same allowlist name and the same removal; re-evaluate
   701| # independently against a live Subelement instance before re-adding either.
   702| 
   703| # LinePatternElement.GetLinePattern was removed from the allowlist above
   704| # after a live re-run (PR #398's exception-capture work, which surfaced the
   705| # error text for the first time) showed it fails 100% of the time -- not
   706| # with a documented Revit API exception (unlike GetCalloutParentId/
   707| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   708| # match a real InvalidOperationException precondition stated on their own
   709| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   710| # failure family as Element.GetValidTypes above: `TypeError: No method
   711| # matches given arguments for GetLinePattern: (<class
   712| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   713| # binder before it ever reaches Revit's implementation, so it cannot
   714| # succeed through `getattr(obj, name)()` regardless of model, Revit
   715| # version, or which element is sampled -- keeping it allowlisted only adds
   716| # permanent error noise for zero chance of real data.
   717| 
   718| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   719| # see the identity check in _reflect_contract below for why this must never be
   720| # comparable-by-value to a real Revit return.
   721| 
   722| def _reflect_try_get(obj, member_kind, name):
   723|     if member_kind == "method":
   724|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   725|             # SAFETY: never invoke a reflection-discovered method that is not
   726|             # on the allowlist above. Revit API methods can have side effects
   727|             # (printing, export, regenerate, delete, transaction commits,
   728|             # ...) and there is no reliable way to tell a safe zero-arg
   729|             # query method from a side-effecting one by name alone for
   730|             # anything outside the allowlist's ground-truth-verified set.
   731|             # Record that the method exists without calling it.
   732|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   733|         try:
   734|             v = getattr(obj, name)()
   735|         except Exception as ex:
   736|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   737|         return (True, v, None)
   738|     try:
   739|         v = getattr(obj, name)
   740|     except Exception as ex:
   741|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   742|     return (True, v, None)
   743| 
   744| def _reflect_contract(raw_v):
   745|     if raw_v is None:
   746|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   747|     if raw_v is _METHOD_NOT_INVOKED_SENTINEL:
   748|         # Identity check ("is"), not equality -- _METHOD_NOT_INVOKED_SENTINEL is a
   749|         # unique object(), never a string, specifically so a genuine reflected
   750|         # property or allowlisted-method return whose real value happens to be
   751|         # the literal text "<method not invoked>" cannot collide with this
   752|         # placeholder and get misclassified/dropped (flagged in PR #398 review:
   753|         # an earlier version of this check compared by value against a string
   754|         # constant, which had exactly that collision risk). Checked before
   755|         # isinstance(raw_v, str) specifically so it never reaches that branch.
   756|         return {"q": "not_invoked", "storage": "None", "raw": None, "display": None, "norm": None}
   757|     if isinstance(raw_v, bool):
   758|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   759|     if isinstance(raw_v, int):
   760|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   761|     if isinstance(raw_v, float):
   762|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   763|     if isinstance(raw_v, str):
   764|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   765|     try:
   766|         if hasattr(raw_v, "IntegerValue"):
   767|             iv = int(raw_v.IntegerValue)
   768|             return {"q": "ok", "storage": "ElementId", "raw": iv, "display": str(iv), "norm": iv}
   769|     except:
   770|         pass
   771|     try:
   772|         if hasattr(raw_v, "ToString"):
   773|             s = raw_v.ToString()
   774|             if s and "Autodesk.Revit" not in s and "System." not in s:
   775|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   776|     except:
   777|         pass
   778|     try:
   779|         ids = []
   780|         saw_item = False
   781|         for item in raw_v:
   782|             saw_item = True
   783|             if not hasattr(item, "IntegerValue"):
   784|                 raise TypeError("non-ElementId item in collection")
   785|             ids.append(int(item.IntegerValue))
   786|         if not saw_item:
   787|             # An empty collection is vacuously "every item has .IntegerValue"
   788|             # -- there's nothing to fail the check against, so item-by-item
   789|             # duck-typing alone can never tell an empty ElementId collection
   790|             # (GetMonitoredLinkElementIds returning [] because a type has no
   791|             # monitored links) apart from an empty collection of anything
   792|             # else (GetEntitySchemaGuids -> IList<Guid>, GetSubelements ->
   793|             # IList<Subelement>, both returning [] because that instance
   794|             # happens to have zero). A CLR generic-type reflection check
   795|             # (raw_v.GetType().GetGenericArguments()) was tried here and
   796|             # found not to reliably discriminate types against a live
   797|             # Revit/pythonnet session (still produced the same false
   798|             # positives), so it was dropped rather than kept as an
   799|             # unreliable safety net. Per this project's fail-soft principle
   800|             # (never silently collapse distinct states), an empty collection
   801|             # of unconfirmed item type gets its own explicit q value instead
   802|             # of defaulting to "ok" (would reintroduce this exact bug) or
   803|             # bare "unsupported" (would make it indistinguishable from a
   804|             # totally opaque complex-object failure). storage stays "None"
   805|             # (not "ElementIdList") so find_crosswalk_candidates.py's
   806|             # _is_elementid_typed() correctly does not treat this as a
   807|             # reference candidate.
   808|             return {"q": "unsupported.empty_type_unconfirmed", "storage": "None", "raw": [], "display": "", "norm": ()}
   809|         disp = ",".join(str(i) for i in ids)
   810|         return {"q": "ok", "storage": "ElementIdList", "raw": ids, "display": disp, "norm": tuple(ids)}
   811|     except:
   812|         pass
   813|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   814| 
   815| def _run_reflection_sweep(sample_objs, type_label, domain_name, max_members=200):
   816|     idx = {}
   817|     for obj in sample_objs:
   818|         if obj is None:
   819|             continue
   820|         for member_kind, name in _reflect_member_names(obj)[:max_members]:
   821|             ok, raw_v, err = _reflect_try_get(obj, member_kind, name)
   822|             key = "refl.{}.{}".format(type_label, name)
   823|             if key not in idx:
   824|                 idx[key] = {
   825|                     "domain": domain_name, "member_key": key, "member_kind": member_kind,
   826|                     "type_label": type_label, "example": None, "example_error": None,
   827|                     "ok_count": 0, "error_count": 0, "unique_value_count": 0, "_seen": set(),
   828|                 }
   829|             e = idx[key]
   830|             if not ok:
   831|                 e["error_count"] += 1
   832|                 if e["example_error"] is None and err:
   833|                     e["example_error"] = err
   834|                 continue
   835|             contract = _reflect_contract(raw_v)
   836|             e["ok_count"] += 1
   837|             sig = (str(contract.get("storage")), str(contract.get("norm")))
   838|             if sig not in e["_seen"]:
   839|                 e["_seen"].add(sig)
   840|                 e["unique_value_count"] += 1
   841|             if e["example"] is None or (contract.get("display") is not None and e["example"].get("display") is None):
   842|                 e["example"] = contract
   843|     records = []
   844|     for key in sorted(idx.keys()):
   845|         e = idx[key]
   846|         records.append({
   847|             "domain": e["domain"], "member_key": e["member_key"], "member_kind": e["member_kind"],
   848|             "type_label": e["type_label"], "example": e["example"], "example_error": e["example_error"],
   849|             "observed": {"ok_count": e["ok_count"], "error_count": e["error_count"], "unique_value_count": e["unique_value_count"]},
   850|         })
   851|     return records
```
