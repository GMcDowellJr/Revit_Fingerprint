# Chunk of tools/probes/probe_text_types.py

- Source relative path: `tools/probes/probe_text_types.py`
- Chunk: 2 of 3
- Original line range: 454-905
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _find_leader_arrow_param, _resolve_workset, _resolve_similar_type, _reflect_member_names, _reflect_try_get
- Source SHA-256: 87bfb05b1c55eb50cd88eaad88e7bb9c9a5d9f1f5e0657eb612ffc30f9ac7ced
- Starts inside symbol: no
- Ends inside symbol: no

```
   454| 
   455| for t in selected:
   456|     font_key, _ = _text_font_key(t)
   457| 
   458|     params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
   459|     if params is None:
   460|         params = _safe(lambda: list(t.Parameters), default=[])
   461| 
   462|     for p in params:
   463|         dn = _safe(lambda: _safe_param_def_name(p), None)
   464|         if not dn:
   465|             continue
   466|         pk = "p.{}".format(dn)
   467| 
   468|         pv = _format_param_contract(p)
   469| 
   470|         # Derived color evidence (rgb/hex) for integer color-like parameters
   471|         # e.g. "Text Color" often surfaces as Integer; we preserve raw int AND add rgb/hex.
   472|         try:
   473|             dn_l = dn.lower()
   474|         except:
   475|             dn_l = ""
   476| 
   477|         if ("color" in dn_l) and (pv.get("storage") == "Integer"):
   478|             raw_int = pv.get("raw")
   479| 
   480|             raw_hex32 = _hex32_from_int(raw_int)
   481| 
   482|             rgb_rrggbb = _rgb_rrggbb_from_int(raw_int)
   483|             hex_rrggbb = _hex_rgb_from_triplet(rgb_rrggbb) if rgb_rrggbb else None
   484| 
   485|             rgb_bbgrr = _rgb_bbgrr_from_int(raw_int)
   486|             hex_bbgrr = _hex_rgb_from_triplet(rgb_bbgrr) if rgb_bbgrr else None
   487| 
   488|             base = "v.color.{}".format(_slug(dn))
   489| 
   490|             derived = [
   491|                 ("{}.raw_hex32".format(base),
   492|                  _contract_value("ok" if raw_hex32 else "missing", "String", raw_hex32, raw_hex32, raw_hex32)),
   493|                 ("{}.rgb_rrggbb".format(base),
   494|                  _contract_value("ok" if rgb_rrggbb else "missing", "String", rgb_rrggbb, rgb_rrggbb, rgb_rrggbb)),
   495|                 ("{}.hex_rrggbb".format(base),
   496|                  _contract_value("ok" if hex_rrggbb else "missing", "String", hex_rrggbb, hex_rrggbb, hex_rrggbb)),
   497|                 ("{}.rgb_bbgrr".format(base),
   498|                  _contract_value("ok" if rgb_bbgrr else "missing", "String", rgb_bbgrr, rgb_bbgrr, rgb_bbgrr)),
   499|                 ("{}.hex_bbgrr".format(base),
   500|                  _contract_value("ok" if hex_bbgrr else "missing", "String", hex_bbgrr, hex_bbgrr, hex_bbgrr)),
   501|             ]
   502| 
   503|             for _pk, _pv in derived:
   504|                 if _pk not in param_index:
   505|                     param_index[_pk] = {
   506|                         "storage_types": set(),
   507|                         "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   508|                         "example": None,
   509|                         "observed_on_font_keys": set(),
   510|                         "seen_sigs": set()
   511|                     }
   512| 
   513|                 _entry = param_index[_pk]
   514|                 _st = _pv.get("storage")
   515|                 _q = _pv.get("q") or "unreadable"
   516|                 _norm = _pv.get("norm")
   517|                 _sig = (str(_st), str(_norm), str(_q))
   518| 
   519|                 if _sig not in _entry["seen_sigs"]:
   520|                     _entry["seen_sigs"].add(_sig)
   521|                     if _st:
   522|                         _entry["storage_types"].add(_st)
   523|                     if _q not in _entry["q_counts"]:
   524|                         _entry["q_counts"][_q] = 0
   525|                     _entry["q_counts"][_q] += 1
   526|                     _entry["observed_on_font_keys"].add(font_key)
   527|                     _maybe_set_example(_entry, _pv)
   528|                 else:
   529|                     _entry["observed_on_font_keys"].add(font_key)
   530| 
   531|         if pk not in param_index:
   532|             param_index[pk] = {
   533|                 "storage_types": set(),
   534|                 "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   535|                 "example": None,
   536|                 "observed_on_font_keys": set(),
   537|                 "seen_sigs": set()
   538|             }
   539| 
   540|         entry = param_index[pk]
   541| 
   542|         st = pv.get("storage")
   543|         q = pv.get("q") or "unreadable"
   544|         norm = pv.get("norm")
   545| 
   546|         # Dedup signature (probe-local)
   547|         sig = (str(st), str(norm), str(q))
   548|         if sig in entry["seen_sigs"]:
   549|             # still mark breadth (where seen)
   550|             entry["observed_on_font_keys"].add(font_key)
   551|             continue
   552| 
   553|         entry["seen_sigs"].add(sig)
   554| 
   555|         if st:
   556|             entry["storage_types"].add(st)
   557|         if q not in entry["q_counts"]:
   558|             entry["q_counts"][q] = 0
   559|         entry["q_counts"][q] += 1
   560| 
   561|         entry["observed_on_font_keys"].add(font_key)
   562|         _maybe_set_example(entry, pv)
   563| 
   564| # Emit inventory records (stable order)
   565| param_inventory = []
   566| for pk in sorted(param_index.keys()):
   567|     e = param_index[pk]
   568|     param_inventory.append({
   569|         "domain": "text_types",
   570|         "param_key": pk,
   571|         "selected_type_sample_count": len(selected),
   572|         "example": e["example"],
   573|         "observed": {
   574|             "storage_types": sorted(list(e["storage_types"])),
   575|             "q_counts": e["q_counts"],
   576|             "observed_on_fonts": sorted(list(e["observed_on_font_keys"]))[:25]
   577|         }
   578|     })
   579| 
   580| 
   581| # -------------------------
   582| # Optional Crosswalk: TextType -> Leader Arrowhead
   583| # -------------------------
   584| 
   585| optional_crosswalk = []
   586| 
   587| LEADER_ARROW_PARAM_CANDIDATES = [
   588|     "Leader Arrowhead",
   589|     "Leader Arrowhead Type",
   590|     "Leader Arrowhead Symbol",
   591|     "Leader Arrow Head",   # odd variants
   592| ]
   593| 
   594| def _find_leader_arrow_param(t):
   595|     for cand in LEADER_ARROW_PARAM_CANDIDATES:
   596|         p = _safe(lambda: t.LookupParameter(cand), None)
   597|         if p is not None:
   598|             return cand, p
   599|     return None, None
   600| 
   601| 
   602| def _resolve_workset(doc, ws_id_obj):
   603|     """Resolve an Element.WorksetId value to (name, resolved_bool) via
   604|     WorksetTable.GetWorkset() -- NOT doc.GetElement(). WorksetId is a
   605|     distinct .NET type from ElementId (both happen to expose .IntegerValue,
   606|     which is why reflection reports this member as ElementId-storage), and
   607|     Workset is not derived from Element, so doc.GetElement() would never
   608|     resolve it even with the right type assumed."""
   609|     if ws_id_obj is None:
   610|         return (None, False)
   611|     wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   612|     if wt_table is None:
   613|         return (None, False)
   614|     ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   615|     if ws is None:
   616|         return (None, False)
   617|     name = _safe(lambda: ws.Name, None)
   618|     return (name, name is not None)
   619| 
   620| 
   621| def _resolve_similar_type(sid_int):
   622|     """Resolve a GetSimilarTypes() id via a document-wide doc.GetElement()
   623|     lookup -- GetSimilarTypes() is not formally documented as
   624|     same-category-only (see Step 0 findings), so this must not assume the
   625|     returned id is necessarily another TextNoteType."""
   626|     if sid_int is None:
   627|         return (None, False)
   628|     ref = _safe(lambda: doc.GetElement(ElementId(sid_int)), None)
   629|     if ref is None:
   630|         return (None, False)
   631|     name = _safe(lambda: ref.Name, None)
   632|     return (name, name is not None)
   633| 
   634| 
   635| # TextNoteType -> GetSimilarTypes() crosswalk. Not gated behind
   636| # enable_crosswalk (unlike the TextType -> Leader Arrowhead join below) --
   637| # a standalone resolution, run over all_types (not the sampled `selected`
   638| # subset) for the same reason arrowheads' arrowhead_name_by_id is built
   639| # from all hits: broader coverage of the id->name lookup.
   640| for tt in all_types:
   641|     tt_id = _safe(lambda: tt.Id.IntegerValue, None)
   642|     if tt_id is None:
   643|         continue
   644|     tt_name = _safe(lambda: _safe_type_name(tt), None)
   645|     tt_ws_id_obj = _safe(lambda: tt.WorksetId, None)
   646|     tt_ws_name, _tt_ws_resolved = _resolve_workset(doc, tt_ws_id_obj)
   647|     tt_ws_id_int = _safe(lambda: tt_ws_id_obj.IntegerValue, None) if tt_ws_id_obj is not None else None
   648| 
   649|     similar_ids = _safe(lambda: list(tt.GetSimilarTypes() or []), default=[])
   650|     for si, sid in enumerate(similar_ids):
   651|         sid_int = _safe(lambda: sid.IntegerValue, None) if sid is not None else None
   652|         s_name, s_resolved = _resolve_similar_type(sid_int)
   653|         optional_crosswalk.append({
   654|             "text_type.id": tt_id,
   655|             "text_type.name": tt_name,
   656|             "text_type.workset_id": tt_ws_id_int,
   657|             "text_type.workset_name": tt_ws_name,
   658|             "get_similar_types.index": si,
   659|             "get_similar_types.id": sid_int,
   660|             "get_similar_types.name": s_name,
   661|             "get_similar_types.resolved": s_resolved,
   662|         })
   663| 
   664| if enable_crosswalk:
   665|     # Optional extra input: max crosswalk rows to emit (default 25)
   666|     crosswalk_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 25
   667| 
   668|     seen_arrowhead_ids = set()
   669| 
   670|     for tt in selected:
   671|         if len(optional_crosswalk) >= int(crosswalk_limit):
   672|             break
   673| 
   674|         matched, p = _find_leader_arrow_param(tt)
   675|         if p is None:
   676|             continue
   677| 
   678|         pv = _format_param_contract(p)
   679| 
   680|         # Must be ElementId pointing to an Arrowhead type
   681|         if pv.get("storage") != "ElementId" or pv.get("raw") is None:
   682|             continue
   683| 
   684|         ah_id = int(pv.get("raw"))
   685|         if ah_id in seen_arrowhead_ids:
   686|             continue
   687| 
   688|         ah_name = None
   689|         ref = _safe(lambda: doc.GetElement(ElementId(ah_id)), None)
   690|         if ref is not None:
   691|             ah_name = _safe(lambda: ref.Name, None)
   692|             if ah_name is None:
   693|                 ah_name = _safe(lambda: _safe_type_name(ref), None)
   694| 
   695|         tt_ws_id_obj = _safe(lambda: tt.WorksetId, None)
   696|         tt_ws_name, _tt_ws_resolved = _resolve_workset(doc, tt_ws_id_obj)
   697|         tt_ws_id_int = _safe(lambda: tt_ws_id_obj.IntegerValue, None) if tt_ws_id_obj is not None else None
   698| 
   699|         row = {
   700|             "text_type.id": _safe(lambda: tt.Id.IntegerValue, None),
   701|             "text_type.name": _safe(lambda: _safe_type_name(tt), None),
   702|             "text_type.workset_id": tt_ws_id_int,
   703|             "text_type.workset_name": tt_ws_name,
   704|             "leader_arrow_param.matched_name": matched,
   705|             "leader_arrow_param": pv,
   706|             "arrowhead.resolved": True if ah_name is not None else False,
   707|             "arrowhead.type_id": ah_id,
   708|             "arrowhead.name": ah_name
   709|         }
   710| 
   711|         if not row["arrowhead.resolved"]:
   712|             continue
   713| 
   714|         seen_arrowhead_ids.add(ah_id)
   715|         optional_crosswalk.append(row)
   716| 
   717| 
   718| # -------------------------
   719| # Assemble labeled output payload
   720| # -------------------------
   721| 
   722| 
   723| # -------------------------
   724| # Reflection sweep (breadth): non-Parameter .NET members via reflection
   725| # -------------------------
   726| # Complements the curated/dynamic capture above with a breadth-only sweep of
   727| # the sampled objects' .NET properties and zero-arg methods. This is
   728| # diagnostics/breadth, not identity -- it surfaces members a fixed/curated
   729| # key list or a Parameters-only walk could otherwise miss.
   730| 
   731| _REFLECTION_SKIP = set([
   732|     "Equals", "GetHashCode", "GetType", "ToString", "MemberwiseClone",
   733|     "Dispose", "GetEnumerator", "Clone",
   734| ])
   735| 
   736| def _reflect_member_names(obj):
   737|     out = []
   738|     if obj is None:
   739|         return out
   740|     try:
   741|         t = obj.GetType()
   742|     except:
   743|         return out
   744|     try:
   745|         for p in t.GetProperties():
   746|             try:
   747|                 n = p.Name
   748|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   749|                     continue
   750|                 if p.GetIndexParameters():
   751|                     continue
   752|                 out.append(("property", n))
   753|             except:
   754|                 pass
   755|     except:
   756|         pass
   757|     try:
   758|         for m in t.GetMethods():
   759|             try:
   760|                 n = m.Name
   761|                 if n in _REFLECTION_SKIP or n.startswith("_"):
   762|                     continue
   763|                 if n.startswith("get_") or n.startswith("set_") or n.startswith("add_") or n.startswith("remove_"):
   764|                     continue
   765|                 if m.GetParameters().Length != 0:
   766|                     continue
   767|                 if m.IsSpecialName:
   768|                     continue
   769|                 out.append(("method", n))
   770|             except:
   771|                 pass
   772|     except:
   773|         pass
   774|     seen = set()
   775|     uniq = []
   776|     for kind, n in out:
   777|         if n in seen:
   778|             continue
   779|         seen.add(n)
   780|         uniq.append((kind, n))
   781|     return sorted(uniq, key=lambda x: x[1])
   782| 
   783| # Step 0 verification (docs/probe_method_invocation_candidates_verification.md,
   784| # docs/method_invocation_candidates_annotated.csv): these 32 method names (33
   785| # (declaring_class, method) pairs from the Step 0 CSV -- confirmed ground-truth
   786| # zero-arg/instance/non-mutating, minus Element.GetValidTypes/Subelement.
   787| # GetValidTypes and LinePatternElement.GetLinePattern, removed post-merge --
   788| # see the notes below the dict) are ground-truth confirmed, against the live
   789| # RevitAPI 2025 documentation (not name/return-type inference), to be
   790| # zero-arg, instance, non-mutating getters. Declared here as data, separate
   791| # from the branching logic in _reflect_try_get below, so it can be reviewed
   792| # as one block and extended later without touching control flow.
   793| #
   794| # Keyed by method NAME only, not (declaring_class, name): this reflection
   795| # sweep is invoked per concrete probed type_label (e.g. "WallType",
   796| # "ProjectInformation", "FamilySymbol"), which is almost never the literal
   797| # Revit API class that actually declares the member -- e.g. Element.GetTypeId
   798| # is reached in this codebase via more than a dozen different concrete
   799| # type_labels across the probe domains, never via type_label=="Element"
   800| # itself, since no probe in this file reflects a bare Element instance.
   801| # Scoping the allowlist by declaring-class name would silently fail to match
   802| # nearly every real call site and defeat the point of this allowlist.
   803| # _reflect_member_names() below already restricts candidate methods to
   804| # public, non-special-name, zero-parameter methods before this allowlist is
   805| # ever consulted, so a name-only match here does not weaken the zero-arg/
   806| # no-side-effect intent the allowlist exists to enforce -- it only widens
   807| # which already-zero-arg, already-name-matched members get invoked. The
   808| # dict value (declaring class) is kept for traceability back to the Step 0
   809| # CSV only; it is not used in the match.
   810| _ALLOWLISTED_REFLECTION_METHODS = {
   811|     "GetTypeId": "Element",
   812|     "GetLayers": "CompoundStructure",
   813|     "GetEntitySchemaGuids": "Element",
   814|     "GetSubelements": "Element",
   815|     "GetFamilyPointLocations": "FamilySymbol",
   816|     "GetModelToProjectionTransforms": "View",
   817|     "GetRenderingAsset": "AppearanceAssetElement",
   818|     "GetExternalFileReference": "Element",
   819|     "GetMonitoredLinkElementIds": "Element",
   820|     "GetMonitoredLocalElementIds": "Element",
   821|     "GetSimilarTypes": "ElementType",
   822|     "GetStructuralSection": "FamilySymbol",
   823|     "GetThermalProperties": "FamilySymbol",
   824|     "GetFillPattern": "FillPatternElement",
   825|     "GetCategories": "ParameterFilterElement",
   826|     "GetElementFilter": "ParameterFilterElement",
   827|     "GetReference": "Subelement",
   828|     "GetBackground": "View",
   829|     "GetCalloutParentId": "View",
   830|     "GetCropRegionShapeManager": "View",
   831|     "GetDepthCueing": "View",
   832|     "GetDirectContext3DHandleOverrides": "View",
   833|     "GetFilters": "View",
   834|     "GetOrderedFilters": "View",
   835|     "GetPointCloudOverrides": "View",
   836|     "GetPrimaryViewId": "View",
   837|     "GetReferenceCallouts": "View",
   838|     "GetReferenceElevations": "View",
   839|     "GetReferenceSections": "View",
   840|     "GetSketchyLines": "View",
   841|     "GetTemporaryViewPropertiesId": "View",
   842|     "GetViewDisplayModel": "View",
   843| }
   844| 
   845| # Element.GetValidTypes / Subelement.GetValidTypes were removed from the
   846| # allowlist above after a live re-run (PR #395 discussion) showed
   847| # Element.GetValidTypes fails 100% of the time -- not with a documented
   848| # Revit API exception (unlike GetCalloutParentId/GetExternalFileReference/
   849| # GetModelToProjectionTransforms above, which each match a real
   850| # InvalidOperationException precondition stated on their own RevitAPI doc
   851| # pages), but with a CLR/pythonnet interop binding failure:
   852| # `TypeError: No method matches given arguments for GetValidTypes: (<class
   853| # '...'>)`, confirmed via a standalone diagnostic against live ElementType/
   854| # WallType/View objects. .NET reflection sees exactly one GetValidTypes
   855| # overload (declaring type Autodesk.Revit.DB.Element) -- so this isn't an
   856| # overload-ambiguity problem either -- the call is rejected by the binder
   857| # before it ever reaches Revit's implementation. This will never succeed
   858| # through `getattr(obj, name)()` regardless of model/version, so keeping it
   859| # allowlisted only adds permanent error noise with zero chance of a real
   860| # value. Subelement.GetValidTypes was never independently tested (no probe
   861| # in this codebase reflects a raw Subelement object as its own type_label),
   862| # but shares the same allowlist name and the same removal; re-evaluate
   863| # independently against a live Subelement instance before re-adding either.
   864| 
   865| # LinePatternElement.GetLinePattern was removed from the allowlist above
   866| # after a live re-run (PR #398's exception-capture work, which surfaced the
   867| # error text for the first time) showed it fails 100% of the time -- not
   868| # with a documented Revit API exception (unlike GetCalloutParentId/
   869| # GetExternalFileReference/GetModelToProjectionTransforms above, which each
   870| # match a real InvalidOperationException precondition stated on their own
   871| # RevitAPI doc pages), but with the same CLR/pythonnet interop binding
   872| # failure family as Element.GetValidTypes above: `TypeError: No method
   873| # matches given arguments for GetLinePattern: (<class
   874| # 'Autodesk.Revit.DB.LinePatternElement'>)`. The call is rejected by the
   875| # binder before it ever reaches Revit's implementation, so it cannot
   876| # succeed through `getattr(obj, name)()` regardless of model, Revit
   877| # version, or which element is sampled -- keeping it allowlisted only adds
   878| # permanent error noise for zero chance of real data.
   879| 
   880| _METHOD_NOT_INVOKED_SENTINEL = object()  # unique marker object, not a string --
   881| # see the identity check in _reflect_contract below for why this must never be
   882| # comparable-by-value to a real Revit return.
   883| 
   884| def _reflect_try_get(obj, member_kind, name):
   885|     if member_kind == "method":
   886|         if name not in _ALLOWLISTED_REFLECTION_METHODS:
   887|             # SAFETY: never invoke a reflection-discovered method that is not
   888|             # on the allowlist above. Revit API methods can have side effects
   889|             # (printing, export, regenerate, delete, transaction commits,
   890|             # ...) and there is no reliable way to tell a safe zero-arg
   891|             # query method from a side-effecting one by name alone for
   892|             # anything outside the allowlist's ground-truth-verified set.
   893|             # Record that the method exists without calling it.
   894|             return (True, _METHOD_NOT_INVOKED_SENTINEL, None)
   895|         try:
   896|             v = getattr(obj, name)()
   897|         except Exception as ex:
   898|             return (False, None, "{}: {}".format(type(ex).__name__, ex))
   899|         return (True, v, None)
   900|     try:
   901|         v = getattr(obj, name)
   902|     except Exception as ex:
   903|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   904|     return (True, v, None)
   905| 
```
