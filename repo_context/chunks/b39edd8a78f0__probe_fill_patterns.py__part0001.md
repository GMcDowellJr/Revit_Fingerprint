# Chunk of tools/probes/probe_fill_patterns.py

- Source relative path: `tools/probes/probe_fill_patterns.py`
- Chunk: 1 of 3
- Original line range: 1-510
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_type_name, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display, _format_param_contract, _contract_from_value, _to_inches, _to_degrees, _bucket_key_for_fill_pattern, _ensure_entry, _maybe_set_example, _observe, _add_computed_surface
- Source SHA-256: b7e1557e7ca19327a8137f4deb5ab42ac2779f1fdaf52c22e2c857bbd8e6f712
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: fill_patterns (INVENTORY OUTPUT)
     2| #
     3| # OUT = [
     4| #   {
     5| #     "kind": "inventory",
     6| #     "domain": "fill_patterns",
     7| #     "records": [...],
     8| #     "file_written": "<path>|None",        # present only if write_json=True
     9| #     "file_write_error": "<error>|None"    # present only on failure
    10| #   },
    11| #   {
    12| #     "kind": "crosswalk",
    13| #     "domain": "fill_patterns",
    14| #     "records": [...]
    15| #   }
    16| # ]
    17| #
    18| # Inputs:
    19| #   IN[0] max_fill_patterns_to_inspect (int)
    20| #        Maximum number of FillPatternElements to inspect AFTER filtering.
    21| #        Default: 500
    22| #
    23| #   IN[1] per_bucket_limit (int)
    24| #        Sample at most N fill patterns per bucket.
    25| #        Buckets are keyed by: target + is_solid + grid_count
    26| #        Default: 3
    27| #
    28| #   IN[2] max_grids_per_pattern (int)
    29| #        At most N fill grids to inspect per fill pattern when deriving
    30| #        computed evidence (angles/offsets/line patterns).
    31| #        Default: 4
    32| #
    33| #   IN[3] enable_crosswalk (bool)
    34| #        Whether to emit FillPattern -> LinePattern crosswalk (via FillGrid.LinePatternId).
    35| #        Default: False
    36| #
    37| #   IN[4] write_json (bool)
    38| #        When True, serialize OUT to a valid JSON file on disk.
    39| #        Default: False
    40| #
    41| #   IN[5] output_directory (str)
    42| #        Directory path where JSON will be written.
    43| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    44| #        If None, falls back to RVT directory, then TEMP.
    45| 
    46| 
    47| import clr
    48| import os
    49| import json
    50| from datetime import datetime
    51| 
    52| clr.AddReference("RevitServices")
    53| from RevitServices.Persistence import DocumentManager
    54| 
    55| clr.AddReference("RevitAPI")
    56| from Autodesk.Revit.DB import (
    57|     FilteredElementCollector, ElementId,
    58|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    59|     BuiltInParameter,
    60|     FillPatternElement
    61| )
    62| 
    63| try:
    64|     from Autodesk.Revit.DB import SpecTypeId
    65| except:
    66|     SpecTypeId = None
    67| 
    68| doc = DocumentManager.Instance.CurrentDBDocument
    69| 
    70| max_fill_patterns_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
    71| per_bucket_limit = IN[1] if len(IN) > 1 and IN[1] is not None else 3
    72| max_grids_per_pattern = IN[2] if len(IN) > 2 and IN[2] is not None else 4
    73| enable_crosswalk = IN[3] if len(IN) > 3 and IN[3] is not None else False
    74| write_json = IN[4] if len(IN) > 4 and IN[4] is not None else False
    75| out_path = IN[5] if len(IN) > 5 and IN[5] is not None else None
    76| 
    77| 
    78| # -------------------------
    79| # Helpers (defensive)
    80| # -------------------------
    81| 
    82| def _safe(fn, default=None):
    83|     try:
    84|         return fn()
    85|     except:
    86|         return default
    87| 
    88| def _safe_type_name(elem):
    89|     # FillPatternElement.Name is usually valid, but keep parity with reference probe.
    90|     for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
    91|         try:
    92|             p = elem.get_Parameter(bip)
    93|             if p is not None:
    94|                 s = p.AsString()
    95|                 if s:
    96|                     return s
    97|         except:
    98|             pass
    99|     try:
   100|         return elem.Name
   101|     except:
   102|         return None
   103| 
   104| def _safe_param_def_name(p):
   105|     try:
   106|         d = p.Definition
   107|         return d.Name if d is not None else None
   108|     except:
   109|         return None
   110| 
   111| def _safe_get_datatype(p):
   112|     try:
   113|         d = p.Definition
   114|         if d is None:
   115|             return None
   116|         return d.GetDataType()
   117|     except:
   118|         return None
   119| 
   120| def _is_length_datatype(dt):
   121|     if dt is None or SpecTypeId is None:
   122|         return False
   123|     try:
   124|         return dt == SpecTypeId.Length
   125|     except:
   126|         return False
   127| 
   128| def _is_angle_datatype(dt):
   129|     if dt is None or SpecTypeId is None:
   130|         return False
   131|     try:
   132|         return dt == SpecTypeId.Angle
   133|     except:
   134|         return False
   135| 
   136| def _fmt_display(p, raw_double=None):
   137|     try:
   138|         if raw_double is not None:
   139|             dt = _safe_get_datatype(p)
   140|             if dt is not None:
   141|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   142|             return str(raw_double)
   143|         return p.AsValueString()
   144|     except:
   145|         return _safe(lambda: p.AsValueString(), None)
   146| 
   147| def _format_param_contract(p):
   148|     """
   149|     Contract:
   150|       {
   151|         "q": "ok|missing|unreadable|unsupported",
   152|         "storage": "String|Integer|Double|ElementId|None",
   153|         "raw": ...,
   154|         "display": ...,
   155|         "norm": ...
   156|       }
   157| 
   158|     Probe choices:
   159|       - Integer.norm stays as raw int (do NOT coerce 0/1 to bool).
   160|       - Length -> inches (float) when datatype is Length
   161|       - Angle  -> degrees (float) when datatype is Angle
   162|       - ElementId -> IntegerValue (norm=int), display tries to resolve name cheaply
   163|     """
   164|     if p is None:
   165|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   166| 
   167|     st = _safe(lambda: p.StorageType, None)
   168|     if st is None:
   169|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   170| 
   171|     if st == StorageType.String:
   172|         raw = _safe(lambda: p.AsString(), None)
   173|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   174| 
   175|     if st == StorageType.Integer:
   176|         raw = _safe(lambda: p.AsInteger(), None)
   177|         disp = _fmt_display(p, None)
   178|         return {
   179|             "q": "ok",
   180|             "storage": "Integer",
   181|             "raw": raw,
   182|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   183|             "norm": raw
   184|         }
   185| 
   186|     if st == StorageType.Double:
   187|         raw = _safe(lambda: p.AsDouble(), None)
   188|         disp = _fmt_display(p, raw)
   189|         dt = _safe_get_datatype(p)
   190|         if raw is None:
   191|             norm = None
   192|         elif _is_length_datatype(dt):
   193|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   194|         elif _is_angle_datatype(dt):
   195|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   196|         else:
   197|             norm = raw
   198|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   199| 
   200|     if st == StorageType.ElementId:
   201|         eid = _safe(lambda: p.AsElementId(), None)
   202|         if eid is None or eid == ElementId.InvalidElementId:
   203|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   204| 
   205|         raw = _safe(lambda: eid.IntegerValue, None)
   206|         ref_name = None
   207|         ref = _safe(lambda: doc.GetElement(eid), None)
   208|         if ref is not None:
   209|             ref_name = _safe(lambda: ref.Name, None)
   210| 
   211|         return {
   212|             "q": "ok",
   213|             "storage": "ElementId",
   214|             "raw": raw,
   215|             "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
   216|             "norm": raw
   217|         }
   218| 
   219|     return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}
   220| 
   221| def _contract_from_value(q, storage, raw, display, norm):
   222|     # Used for computed / derived evidence that is not a Revit Parameter.
   223|     return {
   224|         "q": q,
   225|         "storage": storage,
   226|         "raw": raw,
   227|         "display": display,
   228|         "norm": norm
   229|     }
   230| 
   231| def _to_inches(x_internal):
   232|     if x_internal is None:
   233|         return None
   234|     return _safe(lambda: UnitUtils.ConvertFromInternalUnits(x_internal, UnitTypeId.Inches), x_internal)
   235| 
   236| def _to_degrees(x_internal):
   237|     if x_internal is None:
   238|         return None
   239|     return _safe(lambda: UnitUtils.ConvertFromInternalUnits(x_internal, UnitTypeId.Degrees), x_internal)
   240| 
   241| def _bucket_key_for_fill_pattern(fpe):
   242|     """
   243|     Bucket = target + is_solid + grid_count (breadth-biased sampling).
   244|     """
   245|     try:
   246|         fp = _safe(lambda: fpe.GetFillPattern(), None)
   247|         if fp is None:
   248|             return "missing_fillpattern"
   249|         tgt = _safe(lambda: str(fp.Target), None)
   250|         solid = _safe(lambda: fp.IsSolidFill, None)
   251|         gc = _safe(lambda: fp.GridCount, None)
   252|         return "{}|solid={}|grids={}".format(tgt, solid, gc)
   253|     except:
   254|         return "unreadable_fillpattern"
   255| 
   256| 
   257| # -------------------------
   258| # Discovery + Sampling
   259| # -------------------------
   260| # Progressive strategy:
   261| #   1) Category-free ElementType signature discovery (not viable here; FillPatterns are elements, not ElementTypes)
   262| #   2) Class-based collector: FillPatternElement
   263| #   3) Instance sampling (already inherent; FillPatternElement is an element)
   264| 
   265| all_fill_patterns = _safe(
   266|     lambda: (FilteredElementCollector(doc)
   267|              .OfClass(FillPatternElement)
   268|              .ToElements()),
   269|     default=[]
   270| )
   271| 
   272| try:
   273|     all_fill_patterns = list(all_fill_patterns)
   274| except:
   275|     all_fill_patterns = list(all_fill_patterns)
   276| 
   277| # Cap AFTER discovery (collector ordering shouldn't hide rare buckets)
   278| try:
   279|     max_n = int(max_fill_patterns_to_inspect)
   280|     if max_n >= 0:
   281|         all_fill_patterns = all_fill_patterns[:max_n]
   282| except:
   283|     pass
   284| 
   285| # Breadth-biased sampling by bucket
   286| selected = []
   287| by_bucket = {}  # bucket_key -> count
   288| for fpe in all_fill_patterns:
   289|     bk = _bucket_key_for_fill_pattern(fpe)
   290|     c = by_bucket.get(bk, 0)
   291| 
   292|     if per_bucket_limit is None:
   293|         ok = True
   294|     else:
   295|         try:
   296|             ok = c < int(per_bucket_limit)
   297|         except:
   298|             ok = c < 3
   299| 
   300|     if ok:
   301|         selected.append(fpe)
   302|         by_bucket[bk] = c + 1
   303| 
   304| # If per_bucket_limit is 0/negative, fallback to at least 1 per bucket
   305| if len(selected) == 0 and len(all_fill_patterns) > 0:
   306|     seen = set()
   307|     for fpe in all_fill_patterns:
   308|         bk = _bucket_key_for_fill_pattern(fpe)
   309|         if bk not in seen:
   310|             selected.append(fpe)
   311|             seen.add(bk)
   312| 
   313| 
   314| # -------------------------
   315| # Build inventory (union over selected)
   316| # -------------------------
   317| # Inventory records are per parameter key:
   318| #   - Revit parameters: "p.<DefinitionName>"
   319| #   - Computed evidence: "fp.<key>"
   320| #
   321| # param_key -> {
   322| #   storage_types: set(str),
   323| #   q_counts: dict,
   324| #   example: dict or None,
   325| #   breadth: dict (lightweight)
   326| # }
   327| param_index = {}
   328| 
   329| def _ensure_entry(pk):
   330|     if pk not in param_index:
   331|         param_index[pk] = {
   332|             "storage_types": set(),
   333|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   334|             "example": None,
   335|             "breadth": {
   336|                 "observed_bucket_keys": set()
   337|             }
   338|         }
   339|     return param_index[pk]
   340| 
   341| def _maybe_set_example(entry, pv):
   342|     # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
   343|     if pv is None:
   344|         return
   345|     ex = entry.get("example")
   346|     if ex is None:
   347|         entry["example"] = {
   348|             "q": pv.get("q"),
   349|             "storage": pv.get("storage"),
   350|             "raw": pv.get("raw"),
   351|             "display": pv.get("display"),
   352|             "norm": pv.get("norm")
   353|         }
   354|         return
   355|     # upgrade existing non-ok example to ok if we see one
   356|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   357|         entry["example"] = {
   358|             "q": pv.get("q"),
   359|             "storage": pv.get("storage"),
   360|             "raw": pv.get("raw"),
   361|             "display": pv.get("display"),
   362|             "norm": pv.get("norm")
   363|         }
   364| 
   365| def _observe(pk, pv, bucket_key=None):
   366|     entry = _ensure_entry(pk)
   367| 
   368|     st = pv.get("storage")
   369|     q = pv.get("q") or "unreadable"
   370| 
   371|     if st:
   372|         entry["storage_types"].add(st)
   373|     if q not in entry["q_counts"]:
   374|         entry["q_counts"][q] = 0
   375|     entry["q_counts"][q] += 1
   376| 
   377|     if bucket_key is not None:
   378|         entry["breadth"]["observed_bucket_keys"].add(bucket_key)
   379| 
   380|     _maybe_set_example(entry, pv)
   381| 
   382| 
   383| def _add_computed_surface(fpe, bucket_key):
   384|     """
   385|     Captures a conservative, join-key-relevant surface from FillPattern itself.
   386|     This is not production export logic: it's evidence capture for later policy design.
   387|     """
   388|     fp = _safe(lambda: fpe.GetFillPattern(), None)
   389|     if fp is None:
   390|         _observe("fp.q", _contract_from_value("unreadable", "String", None, None, None), bucket_key)
   391|         return
   392| 
   393|     # name
   394|     fp_name = _safe(lambda: _safe_type_name(fpe), None)
   395|     _observe("fp.name", _contract_from_value("ok", "String", fp_name, fp_name, fp_name), bucket_key)
   396| 
   397|     # target (Drafting/Model)
   398|     tgt = _safe(lambda: fp.Target, None)
   399| 
   400|     # Prefer enum name. In pythonnet, enum.ToString() yields "Drafting"/"Model".
   401|     # Some environments stringify enums as underlying integers ("0"/"1"), so map those too.
   402|     tgt_s = None
   403|     if tgt is not None:
   404|         tgt_s = _safe(lambda: tgt.ToString(), None)
   405|         if tgt_s is None:
   406|             tgt_s = str(tgt)
   407|         if tgt_s in ("0", "1"):
   408|             tgt_s = "Drafting" if tgt_s == "0" else "Model"
   409| 
   410|     _observe("fp.target", _contract_from_value("ok", "String", tgt_s, tgt_s, tgt_s), bucket_key)
   411| 
   412|     # is_solid (store as Integer 1/0/None)
   413|     is_solid = _safe(lambda: fp.IsSolidFill, None)
   414|     is_solid_i = None
   415|     if is_solid is True:
   416|         is_solid_i = 1
   417|     elif is_solid is False:
   418|         is_solid_i = 0
   419|     _observe("fp.is_solid", _contract_from_value("ok", "Integer", is_solid_i, str(is_solid), is_solid_i), bucket_key)
   420| 
   421|     # grid_count
   422|     gc = _safe(lambda: fp.GridCount, None)
   423|     _observe("fp.grid_count", _contract_from_value("ok", "Integer", gc, str(gc) if gc is not None else None, gc), bucket_key)
   424| 
   425|     # Derive a compact signature for the first N grids (angles/offsets/shifts/line patterns).
   426|     # Note: We serialize list-like structures as String payloads to preserve the contract storage types.
   427|     max_g = None
   428|     try:
   429|         max_g = int(max_grids_per_pattern)
   430|     except:
   431|         max_g = 4
   432| 
   433|     angles_deg = []
   434|     offsets_in = []
   435|     shifts_in = []
   436|     origins_in = []
   437|     line_pattern_ids = []
   438| 
   439|     # FillPattern.GetFillGrids() returns a collection of FillGrid
   440|     grids = _safe(lambda: fp.GetFillGrids(), default=None)
   441|     if grids is None:
   442|         _observe("fp.grids.q", _contract_from_value("unreadable", "String", None, None, None), bucket_key)
   443|         return
   444| 
   445|     try:
   446|         grids = list(grids)
   447|     except:
   448|         grids = list(grids)
   449| 
   450|     for i, g in enumerate(grids):
   451|         if max_g is not None and max_g >= 0 and i >= max_g:
   452|             break
   453| 
   454|         ang = _safe(lambda: g.Angle, None)
   455|         off = _safe(lambda: g.Offset, None)
   456|         shf = _safe(lambda: g.Shift, None)
   457|         org = _safe(lambda: g.Origin, None)
   458|         lpid = _safe(lambda: g.LinePatternId, None)
   459| 
   460|         angles_deg.append(_to_degrees(ang))
   461|         offsets_in.append(_to_inches(off))
   462|         shifts_in.append(_to_inches(shf))
   463| 
   464|         # Origin is UV in feet (internal units) in many Revit contexts; convert components conservatively.
   465|         try:
   466|             if org is not None:
   467|                 ou = _safe(lambda: org.U, None)
   468|                 ov = _safe(lambda: org.V, None)
   469|                 origins_in.append([_to_inches(ou), _to_inches(ov)])
   470|             else:
   471|                 origins_in.append(None)
   472|         except:
   473|             origins_in.append(None)
   474| 
   475|         if lpid is not None and lpid != ElementId.InvalidElementId:
   476|             line_pattern_ids.append(_safe(lambda: lpid.IntegerValue, None))
   477|         else:
   478|             line_pattern_ids.append(None)
   479| 
   480|     # Serialize as JSON-ish strings for auditability.
   481|     try:
   482|         angles_s = json.dumps(angles_deg)
   483|     except:
   484|         angles_s = str(angles_deg)
   485| 
   486|     try:
   487|         offsets_s = json.dumps(offsets_in)
   488|     except:
   489|         offsets_s = str(offsets_in)
   490| 
   491|     try:
   492|         shifts_s = json.dumps(shifts_in)
   493|     except:
   494|         shifts_s = str(shifts_in)
   495| 
   496|     try:
   497|         origins_s = json.dumps(origins_in)
   498|     except:
   499|         origins_s = str(origins_in)
   500| 
   501|     try:
   502|         lpids_s = json.dumps(line_pattern_ids)
   503|     except:
   504|         lpids_s = str(line_pattern_ids)
   505| 
   506|     _observe("fp.grids.angles_deg", _contract_from_value("ok", "String", angles_s, angles_s, angles_s), bucket_key)
   507|     _observe("fp.grids.offsets_in", _contract_from_value("ok", "String", offsets_s, offsets_s, offsets_s), bucket_key)
   508|     _observe("fp.grids.shifts_in", _contract_from_value("ok", "String", shifts_s, shifts_s, shifts_s), bucket_key)
   509|     _observe("fp.grids.origins_in", _contract_from_value("ok", "String", origins_s, origins_s, origins_s), bucket_key)
   510|     _observe("fp.grids.line_pattern_ids", _contract_from_value("ok", "String", lpids_s, lpids_s, lpids_s), bucket_key)
```
