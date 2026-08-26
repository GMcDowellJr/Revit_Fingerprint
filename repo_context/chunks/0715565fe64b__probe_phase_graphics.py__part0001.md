# Chunk of tools/probes/probe_phase_graphics.py

- Source relative path: `tools/probes/probe_phase_graphics.py`
- Chunk: 1 of 3
- Original line range: 1-467
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_elem_name, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display, _format_param_contract, _cap, _maybe_set_example, _index_params_from_elem, _resolve_workset_for_view_crosswalk, _resolve_filter_name
- Source SHA-256: 7c3277cf7d241a99aedfa2014cd4c5b4c1c314e89283d9945dd9c402f059fbe9
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: phase_graphics (INVENTORY OUTPUT)
     2| #
     3| # Goal:
     4| #   Exploratory evidence capture for "phase_graphics" parameter surface.
     5| #   This is NOT production export logic.
     6| #
     7| # OUT = [
     8| #   {
     9| #     "kind": "inventory",
    10| #     "domain": "phase_graphics",
    11| #     "records": [...],
    12| #     "file_written": "<path>|None",        # present only if write_json=True
    13| #     "file_write_error": "<error>|None"    # present only on failure
    14| #   },
    15| #   {
    16| #     "kind": "crosswalk",
    17| #     "domain": "phase_graphics",
    18| #     "records": [...]
    19| #   }
    20| # ]
    21| #
    22| # Inputs:
    23| #   IN[0] max_views_to_inspect (int)
    24| #        Maximum number of Views to inspect (templates preferred).
    25| #        Default: 200
    26| #
    27| #   IN[1] max_phasefilters_to_inspect (int)
    28| #        Maximum number of PhaseFilter elements to inspect (if accessible).
    29| #        Default: 200
    30| #
    31| #   IN[2] enable_crosswalk (bool)
    32| #        Whether to emit ViewTemplate -> PhaseFilter crosswalk (if resolvable).
    33| #        Default: False
    34| #
    35| #   IN[3] per_bucket_limit (int)
    36| #        Sample at most N per bucket (templates vs non-templates).
    37| #        Default: 50
    38| #
    39| #   IN[4] write_json (bool)
    40| #        When True, serialize OUT to a valid JSON file on disk.
    41| #        Default: False
    42| #
    43| #   IN[5] output_directory (str)
    44| #        Directory path where JSON will be written.
    45| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    46| #        If None, falls back to RVT directory, then TEMP.
    47| #
    48| #   IN[6] crosswalk_limit (int)
    49| #        Max crosswalk rows to emit (default 50)
    50| 
    51| 
    52| import clr
    53| import os
    54| import json
    55| from datetime import datetime
    56| 
    57| clr.AddReference("RevitServices")
    58| from RevitServices.Persistence import DocumentManager
    59| 
    60| clr.AddReference("RevitAPI")
    61| from Autodesk.Revit.DB import (
    62|     FilteredElementCollector, ElementId, ElementType,
    63|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    64|     BuiltInParameter, View
    65| )
    66| 
    67| try:
    68|     # Not present in all Revit API surfaces, but usually available
    69|     from Autodesk.Revit.DB import PhaseFilter
    70| except:
    71|     PhaseFilter = None
    72| 
    73| try:
    74|     from Autodesk.Revit.DB import SpecTypeId
    75| except:
    76|     SpecTypeId = None
    77| 
    78| try:
    79|     from Autodesk.Revit.DB import ViewSchedule
    80| except:
    81|     ViewSchedule = None
    82| 
    83| doc = DocumentManager.Instance.CurrentDBDocument
    84| 
    85| max_views_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
    86| max_phasefilters_to_inspect = IN[1] if len(IN) > 1 and IN[1] is not None else 200
    87| enable_crosswalk = IN[2] if len(IN) > 2 and IN[2] is not None else False
    88| per_bucket_limit = IN[3] if len(IN) > 3 and IN[3] is not None else 50
    89| write_json = IN[4] if len(IN) > 4 and IN[4] is not None else False
    90| out_path = IN[5] if len(IN) > 5 and IN[5] is not None else None
    91| crosswalk_limit = IN[6] if len(IN) > 6 and IN[6] is not None else 50
    92| 
    93| 
    94| # -------------------------
    95| # Helpers (defensive)
    96| # -------------------------
    97| 
    98| def _safe(fn, default=None):
    99|     try:
   100|         return fn()
   101|     except:
   102|         return default
   103| 
   104| def _safe_elem_name(e):
   105|     # prefer Name, but guard hard
   106|     try:
   107|         n = e.Name
   108|         return n
   109|     except:
   110|         return None
   111| 
   112| def _safe_param_def_name(p):
   113|     try:
   114|         d = p.Definition
   115|         return d.Name if d is not None else None
   116|     except:
   117|         return None
   118| 
   119| def _safe_get_datatype(p):
   120|     try:
   121|         d = p.Definition
   122|         if d is None:
   123|             return None
   124|         return d.GetDataType()
   125|     except:
   126|         return None
   127| 
   128| def _is_length_datatype(dt):
   129|     if dt is None or SpecTypeId is None:
   130|         return False
   131|     try:
   132|         return dt == SpecTypeId.Length
   133|     except:
   134|         return False
   135| 
   136| def _is_angle_datatype(dt):
   137|     if dt is None or SpecTypeId is None:
   138|         return False
   139|     try:
   140|         return dt == SpecTypeId.Angle
   141|     except:
   142|         return False
   143| 
   144| def _fmt_display(p, raw_double=None):
   145|     try:
   146|         if raw_double is not None:
   147|             dt = _safe_get_datatype(p)
   148|             if dt is not None:
   149|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   150|             return str(raw_double)
   151|         return p.AsValueString()
   152|     except:
   153|         return _safe(lambda: p.AsValueString(), None)
   154| 
   155| def _format_param_contract(p):
   156|     """
   157|     Contract:
   158|       {
   159|         "q": "ok|missing|unreadable|unsupported",
   160|         "storage": "String|Integer|Double|ElementId|None",
   161|         "raw": ...,
   162|         "display": ...,
   163|         "norm": ...
   164|       }
   165| 
   166|     Probe choices:
   167|       - Integer.norm stays as integer (enum-safe).
   168|       - Length -> inches
   169|       - Angle  -> degrees
   170|       - ElementId -> IntegerValue; display tries to resolve name cheaply
   171|       - StorageType.None -> storage "None" (NOT "0"), q="ok"
   172|     """
   173|     if p is None:
   174|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   175| 
   176|     st = _safe(lambda: p.StorageType, None)
   177|     if st is None:
   178|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   179| 
   180|     # Explicit mapping avoids enum->int stringification ("0")
   181|     # NOTE: StorageType has a member named "None" but Python can't parse StorageType.None.
   182|     # Use numeric enum value (0) defensively.
   183|     try:
   184|         st_int = int(st)
   185|     except:
   186|         st_int = None
   187| 
   188|     if st_int == 0:
   189|         # Often represents non-primitive / complex parameter surfaces.
   190|         # Keep it auditably "present but not value-typed".
   191|         disp = _safe(lambda: p.AsValueString(), None)
   192|         return {"q": "ok", "storage": "None", "raw": None, "display": disp, "norm": None}
   193| 
   194|     if st == StorageType.String:
   195|         raw = _safe(lambda: p.AsString(), None)
   196|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   197| 
   198|     if st == StorageType.Integer:
   199|         raw = _safe(lambda: p.AsInteger(), None)
   200|         disp = _fmt_display(p, None)
   201|         return {
   202|             "q": "ok",
   203|             "storage": "Integer",
   204|             "raw": raw,
   205|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   206|             "norm": raw
   207|         }
   208| 
   209|     if st == StorageType.Double:
   210|         raw = _safe(lambda: p.AsDouble(), None)
   211|         disp = _fmt_display(p, raw)
   212|         dt = _safe_get_datatype(p)
   213|         if raw is None:
   214|             norm = None
   215|         elif _is_length_datatype(dt):
   216|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   217|         elif _is_angle_datatype(dt):
   218|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   219|         else:
   220|             norm = raw
   221|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   222| 
   223|     if st == StorageType.ElementId:
   224|         eid = _safe(lambda: p.AsElementId(), None)
   225|         if eid is None or eid == ElementId.InvalidElementId:
   226|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   227| 
   228|         raw = _safe(lambda: eid.IntegerValue, None)
   229| 
   230|         ref_name = None
   231|         ref = _safe(lambda: doc.GetElement(eid), None)
   232|         if ref is not None:
   233|             ref_name = _safe(lambda: _safe_elem_name(ref), None)
   234| 
   235|         return {
   236|             "q": "ok",
   237|             "storage": "ElementId",
   238|             "raw": raw,
   239|             "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
   240|             "norm": raw
   241|         }
   242| 
   243|     # Fallback: label unknown storage types without "0" if possible
   244|     st_label = None
   245|     try:
   246|         st_label = str(st)
   247|         # Some environments stringify enums as ints; keep a clearer label
   248|         if st_label in ("0", "1", "2", "3", "4"):
   249|             st_label = "StorageType({})".format(st_label)
   250|     except:
   251|         st_label = None
   252| 
   253|     return {"q": "unsupported", "storage": st_label, "raw": None, "display": None, "norm": None}
   254| 
   255| # -------------------------
   256| # Progressive Discovery
   257| # -------------------------
   258| 
   259| diagnostics = {
   260|     "phasefilter_class_available": True if PhaseFilter is not None else False,
   261|     "views_collected": 0,
   262|     "view_templates_collected": 0,
   263|     "phasefilters_collected": 0,
   264|     "notes": []
   265| }
   266| 
   267| # Step 1: View-based discovery (templates preferred, but include non-templates as fallback)
   268| all_views = _safe(
   269|     lambda: list(FilteredElementCollector(doc).OfClass(View).ToElements()),
   270|     default=[]
   271| )
   272| 
   273| templates = []
   274| non_templates = []
   275| 
   276| for v in all_views:
   277|     is_t = _safe(lambda: v.IsTemplate, False)
   278|     if is_t:
   279|         templates.append(v)
   280|     else:
   281|         non_templates.append(v)
   282| 
   283| diagnostics["views_collected"] = len(all_views)
   284| diagnostics["view_templates_collected"] = len(templates)
   285| 
   286| def _cap(lst, n):
   287|     try:
   288|         n = int(n)
   289|         if n < 0:
   290|             return lst
   291|         return lst[:n]
   292|     except:
   293|         return lst
   294| 
   295| # Bucket-biased sampling: templates first, then non-templates
   296| selected_views = []
   297| try:
   298|     lim = int(per_bucket_limit)
   299| except:
   300|     lim = 50
   301| 
   302| selected_views.extend(_cap(templates, min(lim, int(max_views_to_inspect) if max_views_to_inspect is not None else lim)))
   303| 
   304| # If we still have room, sample non-templates
   305| remaining = None
   306| try:
   307|     remaining = int(max_views_to_inspect) - len(selected_views)
   308| except:
   309|     remaining = 0
   310| 
   311| if remaining and remaining > 0:
   312|     selected_views.extend(_cap(non_templates, min(lim, remaining)))
   313| 
   314| # Step 2: PhaseFilter elements (if API exposes them)
   315| phase_filters = []
   316| if PhaseFilter is not None:
   317|     phase_filters = _safe(
   318|         lambda: list(FilteredElementCollector(doc).OfClass(PhaseFilter).ToElements()),
   319|         default=[]
   320|     )
   321| else:
   322|     diagnostics["notes"].append("PhaseFilter class not importable in this context; skipping PhaseFilter element discovery.")
   323| 
   324| phase_filters = _cap(phase_filters, max_phasefilters_to_inspect)
   325| diagnostics["phasefilters_collected"] = len(phase_filters)
   326| 
   327| # If both are empty, we still return OUT with empty records and diagnostics carried in observed fields.
   328| if len(selected_views) == 0 and len(phase_filters) == 0:
   329|     diagnostics["notes"].append("No Views (templates or otherwise) were collectible, and no PhaseFilters were collected.")
   330| 
   331| 
   332| # -------------------------
   333| # Build inventory (union over selected elements)
   334| # -------------------------
   335| 
   336| # param_key -> {
   337| #   storage_types: set(str),
   338| #   q_counts: dict,
   339| #   example: dict or None,
   340| #   observed_on_buckets: set(str)
   341| # }
   342| param_index = {}
   343| 
   344| def _maybe_set_example(entry, pv):
   345|     # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
   346|     if pv is None:
   347|         return
   348|     ex = entry.get("example")
   349|     if ex is None:
   350|         entry["example"] = {
   351|             "q": pv.get("q"),
   352|             "storage": pv.get("storage"),
   353|             "raw": pv.get("raw"),
   354|             "display": pv.get("display"),
   355|             "norm": pv.get("norm")
   356|         }
   357|         return
   358|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   359|         entry["example"] = {
   360|             "q": pv.get("q"),
   361|             "storage": pv.get("storage"),
   362|             "raw": pv.get("raw"),
   363|             "display": pv.get("display"),
   364|             "norm": pv.get("norm")
   365|         }
   366| 
   367| def _index_params_from_elem(elem, bucket_key):
   368|     params = _safe(lambda: list(elem.GetOrderedParameters()), default=None)
   369|     if params is None:
   370|         params = _safe(lambda: list(elem.Parameters), default=[])
   371| 
   372|     for p in params:
   373|         dn = _safe(lambda: _safe_param_def_name(p), None)
   374|         if not dn:
   375|             continue
   376|         pk = "p.{}".format(dn)
   377| 
   378|         pv = _format_param_contract(p)
   379| 
   380|         if pk not in param_index:
   381|             param_index[pk] = {
   382|                 "storage_types": set(),
   383|                 "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   384|                 "example": None,
   385|                 "observed_on_buckets": set()
   386|             }
   387| 
   388|         entry = param_index[pk]
   389|         st = pv.get("storage")
   390|         q = pv.get("q") or "unreadable"
   391| 
   392|         if st:
   393|             entry["storage_types"].add(st)
   394|         if q not in entry["q_counts"]:
   395|             entry["q_counts"][q] = 0
   396|         entry["q_counts"][q] += 1
   397| 
   398|         entry["observed_on_buckets"].add(bucket_key)
   399|         _maybe_set_example(entry, pv)
   400| 
   401| # Index selected view parameters
   402| for v in selected_views:
   403|     is_t = _safe(lambda: v.IsTemplate, False)
   404|     vname = _safe(lambda: _safe_elem_name(v), None)
   405|     bucket = "view_template" if is_t else "view"
   406|     # include a tiny bit of identity in bucket key for breadth (capped later)
   407|     bucket_key = "{}|{}".format(bucket, vname if vname else "unnamed")
   408|     _index_params_from_elem(v, bucket_key)
   409| 
   410| # Index phase filter parameters (if any)
   411| for pf in phase_filters:
   412|     pfname = _safe(lambda: _safe_elem_name(pf), None)
   413|     bucket_key = "phasefilter|{}".format(pfname if pfname else "unnamed")
   414|     _index_params_from_elem(pf, bucket_key)
   415| 
   416| # Emit inventory records (stable order)
   417| param_inventory = []
   418| for pk in sorted(param_index.keys()):
   419|     e = param_index[pk]
   420|     observed_buckets = sorted(list(e["observed_on_buckets"]))
   421|     # cap breadth list (signal, not spam)
   422|     observed_buckets = observed_buckets[:25]
   423| 
   424|     param_inventory.append({
   425|         "domain": "phase_graphics",
   426|         "param_key": pk,
   427|         "selected_view_sample_count": len(selected_views),
   428|         "selected_phasefilter_sample_count": len(phase_filters),
   429|         "example": e["example"],
   430|         "observed": {
   431|             "storage_types": sorted(list(e["storage_types"])),
   432|             "q_counts": e["q_counts"],
   433|             "observed_on_buckets": observed_buckets,
   434|             "diagnostics": diagnostics
   435|         }
   436|     })
   437| 
   438| 
   439| # -------------------------
   440| # Optional Crosswalk: ViewTemplate -> PhaseFilter
   441| # -------------------------
   442| 
   443| optional_crosswalk = []
   444| 
   445| 
   446| def _resolve_workset_for_view_crosswalk(doc, ws_id_obj):
   447|     """Same _resolve_workset pattern as every other probe (WorksetTable.GetWorkset(),
   448|     not doc.GetElement() -- see the identical helper further down in this file for
   449|     the full rationale). Named distinctly here because this block runs before the
   450|     enable_crosswalk-gated block below defines its own copy."""
   451|     if ws_id_obj is None:
   452|         return (None, False)
   453|     wt_table = _safe(lambda: doc.GetWorksetTable(), None)
   454|     if wt_table is None:
   455|         return (None, False)
   456|     ws = _safe(lambda: wt_table.GetWorkset(ws_id_obj), None)
   457|     if ws is None:
   458|         return (None, False)
   459|     name = _safe(lambda: ws.Name, None)
   460|     return (name, name is not None)
   461| 
   462| 
   463| def _resolve_filter_name(fid_int):
   464|     if fid_int is None:
   465|         return None
   466|     fe = _safe(lambda: doc.GetElement(ElementId(fid_int)), None)
   467|     return _safe(lambda: fe.Name, None) if fe is not None else None
```
