# Chunk of tools/probes/probe_phase_filters.py

- Source relative path: `tools/probes/probe_phase_filters.py`
- Chunk: 1 of 2
- Original line range: 1-519
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_elem_name, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display, _format_param_contract, _phase_status_bucket, _status_enum, _maybe_set_example, _add_inventory_obs, _get_view_phase_filter_param
- Source SHA-256: a8d1433926c6953d854df7580a5b6491f41b1441d9e0e3f29235eaf75698242c
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # tools/probes/probe_phase_filters.py
     2| #
     3| # Dynamo Python (Revit) — Breadth Probe: phase_filters (INVENTORY OUTPUT)
     4| #
     5| # OUT = [
     6| #   {
     7| #     "kind": "inventory",
     8| #     "domain": "phase_filters",
     9| #     "records": [...],
    10| #     "file_written": "<path>|None",        # present only if write_json=True
    11| #     "file_write_error": "<error>|None"    # present only on failure
    12| #   },
    13| #   {
    14| #     "kind": "crosswalk",
    15| #     "domain": "phase_filters",
    16| #     "records": [...]
    17| #   }
    18| # ]
    19| #
    20| # Inputs:
    21| #   IN[0] max_phase_filters_to_inspect (int)
    22| #        Maximum number of PhaseFilters to inspect.
    23| #        Default: 200
    24| #
    25| #   IN[1] enable_crosswalk (bool)
    26| #        Whether to emit View → PhaseFilter crosswalk.
    27| #        Default: False
    28| #
    29| #   IN[2] max_views_to_scan (int)
    30| #        When crosswalk enabled, scan at most N views for Phase Filter assignments.
    31| #        Default: 2000
    32| #
    33| #   IN[3] write_json (bool)
    34| #        When True, serialize OUT to a valid JSON file on disk.
    35| #        Default: False
    36| #
    37| #   IN[4] output_directory (str)
    38| #        Directory path where JSON will be written.
    39| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    40| #        If None, falls back to RVT directory, then TEMP.
    41| 
    42| 
    43| import clr
    44| import os
    45| import json
    46| from datetime import datetime
    47| 
    48| clr.AddReference("RevitServices")
    49| from RevitServices.Persistence import DocumentManager
    50| 
    51| clr.AddReference("RevitAPI")
    52| from Autodesk.Revit.DB import (
    53|     FilteredElementCollector, ElementId,
    54|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    55|     BuiltInParameter, View
    56| )
    57| 
    58| # PhaseFilter / PhaseStatus are present in common Revit builds,
    59| # but import defensively for Dynamo environments.
    60| try:
    61|     from Autodesk.Revit.DB import PhaseFilter
    62| except:
    63|     PhaseFilter = None
    64| 
    65| try:
    66|     from Autodesk.Revit.DB import PhaseStatus
    67| except:
    68|     PhaseStatus = None
    69| 
    70| try:
    71|     from Autodesk.Revit.DB import SpecTypeId
    72| except:
    73|     SpecTypeId = None
    74| 
    75| doc = DocumentManager.Instance.CurrentDBDocument
    76| 
    77| max_phase_filters_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 200
    78| enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
    79| max_views_to_scan = IN[2] if len(IN) > 2 and IN[2] is not None else 2000
    80| write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
    81| out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
    82| 
    83| 
    84| # -------------------------
    85| # Helpers (defensive)
    86| # -------------------------
    87| 
    88| def _safe(fn, default=None):
    89|     try:
    90|         return fn()
    91|     except:
    92|         return default
    93| 
    94| def _safe_elem_name(elem):
    95|     # Prefer Revit's Name property where present.
    96|     try:
    97|         n = elem.Name
    98|         if n:
    99|             return n
   100|     except:
   101|         pass
   102|     # Fall back to common type-name params if available
   103|     for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
   104|         try:
   105|             p = elem.get_Parameter(bip)
   106|             if p is not None:
   107|                 s = p.AsString()
   108|                 if s:
   109|                     return s
   110|         except:
   111|             pass
   112|     return None
   113| 
   114| def _safe_param_def_name(p):
   115|     try:
   116|         d = p.Definition
   117|         return d.Name if d is not None else None
   118|     except:
   119|         return None
   120| 
   121| def _safe_get_datatype(p):
   122|     try:
   123|         d = p.Definition
   124|         if d is None:
   125|             return None
   126|         return d.GetDataType()
   127|     except:
   128|         return None
   129| 
   130| def _is_length_datatype(dt):
   131|     if dt is None or SpecTypeId is None:
   132|         return False
   133|     try:
   134|         return dt == SpecTypeId.Length
   135|     except:
   136|         return False
   137| 
   138| def _is_angle_datatype(dt):
   139|     if dt is None or SpecTypeId is None:
   140|         return False
   141|     try:
   142|         return dt == SpecTypeId.Angle
   143|     except:
   144|         return False
   145| 
   146| def _fmt_display(p, raw_double=None):
   147|     try:
   148|         if raw_double is not None:
   149|             dt = _safe_get_datatype(p)
   150|             if dt is not None:
   151|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   152|             return str(raw_double)
   153|         return p.AsValueString()
   154|     except:
   155|         return _safe(lambda: p.AsValueString(), None)
   156| 
   157| def _format_param_contract(p):
   158|     """
   159|     Contract:
   160|       {
   161|         "q": "ok|missing|unreadable|unsupported",
   162|         "storage": "String|Integer|Double|ElementId|None",
   163|         "raw": ...,
   164|         "display": ...,
   165|         "norm": ...
   166|       }
   167| 
   168|     Probe choices:
   169|       - Integer.norm stays integer (enum-safe; do NOT coerce to bool)
   170|       - Length -> inches (float) when datatype is Length
   171|       - Angle  -> degrees (float) when datatype is Angle
   172|       - ElementId -> IntegerValue (norm=int), display tries to resolve element name cheaply
   173|     """
   174|     if p is None:
   175|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   176| 
   177|     st = _safe(lambda: p.StorageType, None)
   178|     if st is None:
   179|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   180| 
   181|     if st == StorageType.String:
   182|         raw = _safe(lambda: p.AsString(), None)
   183|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   184| 
   185|     if st == StorageType.Integer:
   186|         raw = _safe(lambda: p.AsInteger(), None)
   187|         disp = _fmt_display(p, None)
   188|         return {
   189|             "q": "ok",
   190|             "storage": "Integer",
   191|             "raw": raw,
   192|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   193|             "norm": raw
   194|         }
   195| 
   196|     if st == StorageType.Double:
   197|         raw = _safe(lambda: p.AsDouble(), None)
   198|         disp = _fmt_display(p, raw)
   199|         dt = _safe_get_datatype(p)
   200|         if raw is None:
   201|             norm = None
   202|         elif _is_length_datatype(dt):
   203|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   204|         elif _is_angle_datatype(dt):
   205|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   206|         else:
   207|             norm = raw
   208|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   209| 
   210|     if st == StorageType.ElementId:
   211|         eid = _safe(lambda: p.AsElementId(), None)
   212|         if eid is None or eid == ElementId.InvalidElementId:
   213|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   214| 
   215|         raw = _safe(lambda: eid.IntegerValue, None)
   216|         ref_name = None
   217|         ref = _safe(lambda: doc.GetElement(eid), None)
   218|         if ref is not None:
   219|             ref_name = _safe(lambda: _safe_elem_name(ref), None)
   220| 
   221|         return {
   222|             "q": "ok",
   223|             "storage": "ElementId",
   224|             "raw": raw,
   225|             "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
   226|             "norm": raw
   227|         }
   228| 
   229|     return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}
   230| 
   231| 
   232| # -------------------------
   233| # Domain-specific breadth bucket: Phase status presentations
   234| # -------------------------
   235| 
   236| def _phase_status_bucket(pf):
   237|     if pf is None:
   238|         return "unsupported|pf"
   239| 
   240|     parts = []
   241|     for status_name in STATUS_ORDER:
   242|         status_enum = _status_enum(status_name)
   243|         if status_enum is None:
   244|             parts.append("{}=?".format(status_name))
   245|             continue
   246| 
   247|         try:
   248|             pres = pf.GetPhaseStatusPresentation(status_enum)
   249|             token = str(pres)
   250|             token_to_label = {
   251|                 # Enum token forms
   252|                 "ByCategory": "By Category",
   253|                 "NotDisplayed": "Not Displayed",
   254|                 "Overridden": "Overridden",
   255| 
   256|                 # Numeric token forms observed in some builds (confirmed by user)
   257|                 "0": "By Category",
   258|                 "1": "Not Displayed",
   259|                 "2": "Overridden",
   260| 
   261|                 # Defensive: if int slips through before str()
   262|                 0: "By Category",
   263|                 1: "Not Displayed",
   264|                 2: "Overridden",
   265|             }
   266|             label = token_to_label.get(token, token)
   267|             parts.append("{}={}".format(status_name, label))
   268|         except:
   269|             parts.append("{}=?".format(status_name))
   270| 
   271|     return "|".join(parts)
   272| 
   273| # -------------------------
   274| # Discovery (progressive)
   275| # -------------------------
   276| 
   277| discovery_notes = []
   278| 
   279| phase_filters = []
   280| 
   281| # Step 1 (preferred): class-based collector (category-free)
   282| if PhaseFilter is not None:
   283|     phase_filters = _safe(
   284|         lambda: (FilteredElementCollector(doc)
   285|                  .OfClass(PhaseFilter)
   286|                  .ToElements()),
   287|         default=[]
   288|     )
   289|     discovery_notes.append("collector: OfClass(PhaseFilter)")
   290| else:
   291|     discovery_notes.append("collector: PhaseFilter class import unavailable")
   292| 
   293| try:
   294|     phase_filters = list(phase_filters)
   295| except:
   296|     phase_filters = list(phase_filters)
   297| 
   298| # Cap scan explicitly
   299| try:
   300|     nmax = int(max_phase_filters_to_inspect)
   301|     if nmax >= 0:
   302|         phase_filters = phase_filters[:nmax]
   303| except:
   304|     pass
   305| 
   306| 
   307| # -------------------------
   308| # Build inventory (union over discovered phase filters)
   309| # -------------------------
   310| 
   311| # Inventory policy for this domain:
   312| # - PhaseFilter often exposes few/no "Parameters"; the meaningful surface is the
   313| #   per-status presentation setting used by the exporter (GetPhaseStatusPresentation).
   314| # - Therefore we synthesize "probe parameters" aligned to exporter identity items:
   315| #     phase_filter.<status>.presentation_id  (Integer)
   316| #   plus a coordination/name item:
   317| #     phase_filter.name  (String)
   318| #
   319| # We still attempt to include any actual Revit Parameters found on PhaseFilter,
   320| # but those are additive-only and not relied upon for non-empty inventory.
   321| 
   322| STATUS_ORDER = ["New", "Existing", "Demolished", "Temporary"]
   323| 
   324| def _status_enum(status_name):
   325|     if PhaseStatus is not None:
   326|         return _safe(lambda: getattr(PhaseStatus, status_name), None)
   327|     # Exporter uses ElementOnPhaseStatus; probe may not have it imported.
   328|     try:
   329|         from Autodesk.Revit.DB import ElementOnPhaseStatus
   330|         return _safe(lambda: getattr(ElementOnPhaseStatus, status_name), None)
   331|     except:
   332|         return None
   333| 
   334| def _maybe_set_example(entry, pv):
   335|     # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
   336|     if pv is None:
   337|         return
   338|     ex = entry.get("example")
   339|     if ex is None:
   340|         entry["example"] = {
   341|             "q": pv.get("q"),
   342|             "storage": pv.get("storage"),
   343|             "raw": pv.get("raw"),
   344|             "display": pv.get("display"),
   345|             "norm": pv.get("norm")
   346|         }
   347|         return
   348|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   349|         entry["example"] = {
   350|             "q": pv.get("q"),
   351|             "storage": pv.get("storage"),
   352|             "raw": pv.get("raw"),
   353|             "display": pv.get("display"),
   354|             "norm": pv.get("norm")
   355|         }
   356| 
   357| def _add_inventory_obs(param_key, pv, pf_name=None, bucket=None):
   358|     if param_key not in param_index:
   359|         param_index[param_key] = {
   360|             "storage_types": set(),
   361|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   362|             "example": None,
   363|             "observed_on_buckets": set(),
   364|             "observed_on_names": set()
   365|         }
   366| 
   367|     entry = param_index[param_key]
   368| 
   369|     st = pv.get("storage")
   370|     q = pv.get("q") or "unreadable"
   371| 
   372|     if st:
   373|         entry["storage_types"].add(st)
   374|     if q not in entry["q_counts"]:
   375|         entry["q_counts"][q] = 0
   376|     entry["q_counts"][q] += 1
   377| 
   378|     if bucket:
   379|         entry["observed_on_buckets"].add(bucket)
   380|     if pf_name:
   381|         entry["observed_on_names"].add(pf_name)
   382| 
   383|     _maybe_set_example(entry, pv)
   384| 
   385| 
   386| # param_key -> accumulator (same shape as before)
   387| param_index = {}
   388| 
   389| for pf in phase_filters:
   390|     pf_name = _safe(lambda: _safe_elem_name(pf), None)
   391|     bucket = _phase_status_bucket(pf)
   392| 
   393|     # --- Synthesized, exporter-modeled surfaces (authoritative) ---
   394|     # phase_filter.name
   395|     name_val = _safe(lambda: getattr(pf, "Name", None), None)
   396|     pv_name = {
   397|         "q": "ok" if (name_val is not None and str(name_val) != "") else "missing",
   398|         "storage": "String",
   399|         "raw": name_val,
   400|         "display": name_val,
   401|         "norm": name_val
   402|     }
   403|     _add_inventory_obs("phase_filter.name", pv_name, pf_name=pf_name, bucket=bucket)
   404| 
   405|     # phase_filter.<status>.presentation_id (Integer)
   406| for status_name in STATUS_ORDER:
   407|     status_enum = _status_enum(status_name)
   408|     k = "phase_filter.{}.presentation_id".format(status_name.lower())
   409| 
   410|     try:
   411|         if status_enum is None:
   412|             raise Exception("status enum unavailable")
   413| 
   414|         pres = pf.GetPhaseStatusPresentation(status_enum)
   415|         token = str(pres)
   416| 
   417|         token_to_label = {
   418|             # Enum token forms
   419|             "ByCategory": "By Category",
   420|             "NotDisplayed": "Not Displayed",
   421|             "Overridden": "Overridden",
   422| 
   423|             # Numeric token forms observed in some builds (confirmed by user)
   424|             "0": "By Category",
   425|             "1": "Not Displayed",
   426|             "2": "Overridden",
   427| 
   428|             # Defensive: if int slips through before str()
   429|             0: "By Category",
   430|             1: "Not Displayed",
   431|             2: "Overridden",
   432|         }
   433| 
   434|         label = token_to_label.get(token, token)
   435| 
   436|         pv = {
   437|             "q": "ok",
   438|             "storage": "String",
   439|             "raw": label,
   440|             "display": label,
   441|             "norm": label
   442|         }
   443|     except:
   444|         pv = {
   445|             "q": "unreadable",
   446|             "storage": "String",
   447|             "raw": None,
   448|             "display": None,
   449|             "norm": None
   450|         }
   451| 
   452|     _add_inventory_obs(k, pv, pf_name=pf_name, bucket=bucket)
   453|     # --- Additive-only: actual Revit Parameters (if any) ---
   454|     params = _safe(lambda: list(pf.GetOrderedParameters()), default=None)
   455|     if params is None:
   456|         params = _safe(lambda: list(pf.Parameters), default=[])
   457| 
   458|     for p in params:
   459|         dn = _safe(lambda: _safe_param_def_name(p), None)
   460|         if not dn:
   461|             continue
   462|         pk = "p.{}".format(dn)
   463|         pv = _format_param_contract(p)
   464|         _add_inventory_obs(pk, pv, pf_name=pf_name, bucket=bucket)
   465| 
   466| 
   467| # Emit inventory records (stable order)
   468| param_inventory = []
   469| for pk in sorted(param_index.keys()):
   470|     e = param_index[pk]
   471|     param_inventory.append({
   472|         "domain": "phase_filters",
   473|         "param_key": pk,
   474|         "selected_phase_filter_sample_count": len(phase_filters),
   475|         "discovery": {
   476|             "notes": discovery_notes[:10],
   477|             "modeled_on_exporter": True if pk.startswith("phase_filter.") else False
   478|         },
   479|         "example": e["example"],
   480|         "observed": {
   481|             "storage_types": sorted(list(e["storage_types"])),
   482|             "q_counts": e["q_counts"],
   483|             # breadth: cap for readability
   484|             "observed_on_buckets": sorted(list(e["observed_on_buckets"]))[:25],
   485|             "observed_on_names": sorted(list(e["observed_on_names"]))[:25]
   486|         }
   487|     })
   488| 
   489| # -------------------------
   490| # Optional Crosswalk: View -> PhaseFilter
   491| # -------------------------
   492| 
   493| optional_crosswalk = []
   494| 
   495| VIEW_PHASE_FILTER_PARAM_CANDIDATES = [
   496|     # UI-facing label (common)
   497|     "Phase Filter",
   498|     "Phase filter",
   499| ]
   500| 
   501| def _get_view_phase_filter_param(v):
   502|     # Prefer BIP if present; fall back to name candidates.
   503|     # Some builds expose the view setting via a built-in parameter.
   504|     for bip in (
   505|         _safe(lambda: BuiltInParameter.VIEW_PHASE_FILTER, None),
   506|     ):
   507|         if bip is None:
   508|             continue
   509|         p = _safe(lambda: v.get_Parameter(bip), None)
   510|         if p is not None:
   511|             return (str(bip), p)
   512| 
   513|     for cand in VIEW_PHASE_FILTER_PARAM_CANDIDATES:
   514|         p = _safe(lambda: v.LookupParameter(cand), None)
   515|         if p is not None:
   516|             return (cand, p)
   517| 
   518|     return (None, None)
   519| 
```
