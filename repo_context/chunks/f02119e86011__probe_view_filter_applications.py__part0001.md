# Chunk of tools/probes/probe_view_filter_applications.py

- Source relative path: `tools/probes/probe_view_filter_applications.py`
- Chunk: 1 of 3
- Original line range: 1-461
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_type_name, _eid_int, _color_rgb_hex, _contract, _as_int_contract, _as_bool_int_contract, _as_string_contract, _as_elementid_contract, _ogs_get, _hash_sig, _ensure_param, _q_rank, _maybe_set_example, _observe, _view_bucket_key, _view_has_filters, _pv_from_ogs_field, _is_defaultish_ogs_value, _collect_applied_filters_in_order
- Source SHA-256: 54662f061e5f0ad2fd398cf9882aff71bf5cf2f3312ded3f562bef5b2eabfb1b
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: view_filter_applications (INVENTORY OUTPUT)
     2| #
     3| # OUT = [
     4| #   {
     5| #     "kind": "inventory",
     6| #     "domain": "view_filter_applications",
     7| #     "records": param_inventory,
     8| #     "file_written": "<path>|None",        # present only if write_json=True
     9| #     "file_write_error": "<error>|None"    # present only on failure
    10| #   },
    11| #   {
    12| #     "kind": "crosswalk",
    13| #     "domain": "view_filter_applications",
    14| #     "records": optional_crosswalk
    15| #   }
    16| # ]
    17| #
    18| # Inputs:
    19| #   IN[0] max_views_to_inspect (int)
    20| #        Maximum number of View elements to inspect AFTER filtering for views that have filters.
    21| #        Default: 300
    22| #
    23| #   IN[1] enable_crosswalk (bool)
    24| #        Whether to emit a compact View/ViewTemplate → ParameterFilterElement crosswalk.
    25| #        Default: False
    26| #
    27| #   IN[2] per_bucket_limit (int)
    28| #        Sample at most N views per bucket where bucket = "<is_template>|<ViewType>".
    29| #        Default: 2
    30| #
    31| #   IN[3] write_json (bool)
    32| #        When True, serialize OUT to a valid JSON file on disk.
    33| #        Default: False
    34| #
    35| #   IN[4] output_directory (str)
    36| #        Directory path where JSON will be written.
    37| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    38| #        If None, falls back to RVT directory, then TEMP.
    39| #
    40| #   IN[5] per_view_filter_limit (int)
    41| #        Sample at most N filters per view/template (preserves order, truncates after N).
    42| #        Default: 25
    43| 
    44| 
    45| import clr
    46| import os
    47| import json
    48| import hashlib
    49| from datetime import datetime
    50| 
    51| clr.AddReference("RevitServices")
    52| from RevitServices.Persistence import DocumentManager
    53| 
    54| clr.AddReference("RevitAPI")
    55| from Autodesk.Revit.DB import (
    56|     FilteredElementCollector, ElementId, StorageType,
    57|     BuiltInParameter, View, OverrideGraphicSettings,
    58|     ParameterFilterElement, Color, Category
    59| )
    60| 
    61| doc = DocumentManager.Instance.CurrentDBDocument
    62| 
    63| max_views_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 300
    64| enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
    65| per_bucket_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 2
    66| write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
    67| out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
    68| per_view_filter_limit = IN[5] if len(IN) > 5 and IN[5] is not None else 25
    69| 
    70| 
    71| # -------------------------
    72| # Helpers (defensive)
    73| # -------------------------
    74| 
    75| def _safe(fn, default=None):
    76|     try:
    77|         return fn()
    78|     except:
    79|         return default
    80| 
    81| def _safe_type_name(elem):
    82|     for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
    83|         try:
    84|             p = elem.get_Parameter(bip)
    85|             if p is not None:
    86|                 s = p.AsString()
    87|                 if s:
    88|                     return s
    89|         except:
    90|             pass
    91|     try:
    92|         return elem.Name
    93|     except:
    94|         return None
    95| 
    96| def _eid_int(eid):
    97|     if eid is None:
    98|         return None
    99|     try:
   100|         if eid == ElementId.InvalidElementId:
   101|             return None
   102|     except:
   103|         pass
   104|     return _safe(lambda: eid.IntegerValue, None)
   105| 
   106| def _color_rgb_hex(c):
   107|     if c is None:
   108|         return (None, None)
   109|     try:
   110|         r = int(c.Red)
   111|         g = int(c.Green)
   112|         b = int(c.Blue)
   113|         rgb = "{}|{}|{}".format(r, g, b)
   114|         hx = "#{:02X}{:02X}{:02X}".format(r, g, b)
   115|         return (rgb, hx)
   116|     except:
   117|         return (None, None)
   118| 
   119| def _contract(q, storage, raw, display, norm):
   120|     # storage must be: String | Integer | Double | ElementId | None
   121|     return {
   122|         "q": q,
   123|         "storage": storage,
   124|         "raw": raw,
   125|         "display": display,
   126|         "norm": norm
   127|     }
   128| 
   129| def _as_int_contract(x):
   130|     if x is None:
   131|         return _contract("missing", "Integer", None, None, None)
   132|     try:
   133|         iv = int(x)
   134|         return _contract("ok", "Integer", iv, str(iv), iv)
   135|     except:
   136|         return _contract("unreadable", "Integer", None, None, None)
   137| 
   138| def _as_bool_int_contract(x):
   139|     if x is None:
   140|         return _contract("missing", "Integer", None, None, None)
   141|     try:
   142|         iv = 1 if bool(x) else 0
   143|         return _contract("ok", "Integer", iv, "True" if iv == 1 else "False", iv)
   144|     except:
   145|         return _contract("unreadable", "Integer", None, None, None)
   146| 
   147| def _as_string_contract(x):
   148|     if x is None:
   149|         return _contract("missing", "String", None, None, None)
   150|     try:
   151|         s = str(x)
   152|         return _contract("ok", "String", s, s, s)
   153|     except:
   154|         return _contract("unreadable", "String", None, None, None)
   155| 
   156| def _as_elementid_contract(eid):
   157|     iv = _eid_int(eid)
   158|     if iv is None:
   159|         return _contract("ok", "ElementId", None, None, None)
   160|     name = None
   161|     ref = _safe(lambda: doc.GetElement(eid), None)
   162|     if ref is not None:
   163|         name = _safe(lambda: ref.Name, None)
   164|         if name is None:
   165|             name = _safe(lambda: _safe_type_name(ref), None)
   166|     disp = name if name is not None else str(iv)
   167|     return _contract("ok", "ElementId", iv, disp, iv)
   168| 
   169| def _ogs_get(ogs, attr_name):
   170|     """
   171|     Read OverrideGraphicSettings member defensively.
   172| 
   173|     IMPORTANT:
   174|       - Do NOT invoke callables here. In pythonnet, some members can present as
   175|         callable proxies; calling the wrong overload will throw and produce false 'unreadable'.
   176|       - Treat AttributeError as 'unsupported' and other exceptions as 'unreadable'.
   177| 
   178|     Returns (q, value) where q is ok|missing|unsupported|unreadable.
   179|     """
   180|     if ogs is None:
   181|         return ("missing", None)
   182| 
   183|     try:
   184|         return ("ok", getattr(ogs, attr_name))
   185|     except AttributeError:
   186|         return ("unsupported", None)
   187|     except Exception:
   188|         return ("unreadable", None)
   189| 
   190| def _hash_sig(pairs):
   191|     """
   192|     pairs: list of (k, norm_str) where norm_str is already stable string.
   193|     """
   194|     try:
   195|         s = "|".join(["{}={}".format(k, v) for (k, v) in pairs])
   196|         return hashlib.sha1(s.encode("utf-8")).hexdigest()
   197|     except:
   198|         return None
   199| 
   200| 
   201| # -------------------------
   202| # Inventory accumulator (dedup by (param_key, storage_type, norm))
   203| # -------------------------
   204| 
   205| # param_key -> entry
   206| param_index = {}
   207| 
   208| def _ensure_param(pk):
   209|     if pk not in param_index:
   210|         param_index[pk] = {
   211|             "storage_types": set(),
   212|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   213|             "example": None,
   214|             "observed_on_buckets": set(),
   215|             # dedup tracking
   216|             "_obs_best_q_by_sig": {}  # (storage, norm_str) -> q
   217|         }
   218|     return param_index[pk]
   219| 
   220| def _q_rank(q):
   221|     # best-signal wins for the same (pk, storage, norm)
   222|     # ok > missing > unreadable > unsupported
   223|     ranks = {"ok": 3, "missing": 2, "unreadable": 1, "unsupported": 0}
   224|     return ranks.get(q, 0)
   225| 
   226| def _maybe_set_example(entry, pv):
   227|     if pv is None:
   228|         return
   229|     ex = entry.get("example")
   230|     if ex is None:
   231|         entry["example"] = {
   232|             "q": pv.get("q"),
   233|             "storage": pv.get("storage"),
   234|             "raw": pv.get("raw"),
   235|             "display": pv.get("display"),
   236|             "norm": pv.get("norm")
   237|         }
   238|         return
   239|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   240|         entry["example"] = {
   241|             "q": pv.get("q"),
   242|             "storage": pv.get("storage"),
   243|             "raw": pv.get("raw"),
   244|             "display": pv.get("display"),
   245|             "norm": pv.get("norm")
   246|         }
   247| 
   248| def _observe(pk, pv, bucket_key):
   249|     entry = _ensure_param(pk)
   250| 
   251|     q = pv.get("q") or "unreadable"
   252|     st = pv.get("storage")
   253| 
   254|     if st:
   255|         entry["storage_types"].add(st)
   256| 
   257|     # probe-local dedup by (param_key, storage_type, normalized_value)
   258|     norm = pv.get("norm")
   259|     norm_str = "None" if norm is None else str(norm)
   260|     sig = (str(st), norm_str)
   261| 
   262|     prev_q = entry["_obs_best_q_by_sig"].get(sig)
   263|     if prev_q is None:
   264|         entry["_obs_best_q_by_sig"][sig] = q
   265|         if q not in entry["q_counts"]:
   266|             entry["q_counts"][q] = 0
   267|         entry["q_counts"][q] += 1
   268|     else:
   269|         # upgrade counts if new observation is "better"
   270|         if _q_rank(q) > _q_rank(prev_q):
   271|             # decrement old
   272|             if prev_q not in entry["q_counts"]:
   273|                 entry["q_counts"][prev_q] = 0
   274|             entry["q_counts"][prev_q] = max(0, entry["q_counts"][prev_q] - 1)
   275|             # increment new
   276|             if q not in entry["q_counts"]:
   277|                 entry["q_counts"][q] = 0
   278|             entry["q_counts"][q] += 1
   279|             entry["_obs_best_q_by_sig"][sig] = q
   280| 
   281|     entry["observed_on_buckets"].add(bucket_key)
   282|     _maybe_set_example(entry, pv)
   283| 
   284| 
   285| # -------------------------
   286| # Discovery + Sampling
   287| # -------------------------
   288| 
   289| all_views = _safe(
   290|     lambda: (FilteredElementCollector(doc)
   291|              .OfClass(View)
   292|              .WhereElementIsNotElementType()
   293|              .ToElements()),
   294|     default=[]
   295| )
   296| 
   297| try:
   298|     all_views = list(all_views)
   299| except:
   300|     all_views = list(all_views)
   301| 
   302| def _view_bucket_key(v):
   303|     is_t = _safe(lambda: v.IsTemplate, False)
   304|     vt = _safe(lambda: v.ViewType, None)
   305|     return "{}|{}".format("T" if is_t else "V", str(vt))
   306| 
   307| def _view_has_filters(v):
   308|     # Guarded: some view types can throw on GetFilters
   309|     fids = _safe(lambda: list(v.GetFilters()), default=None)
   310|     if fids is None:
   311|         return False
   312|     try:
   313|         return len(fids) > 0
   314|     except:
   315|         return False
   316| 
   317| # Filter to views/templates that actually have filters
   318| candidates = []
   319| for v in all_views:
   320|     if _view_has_filters(v):
   321|         candidates.append(v)
   322| 
   323| # Cap AFTER filtering (avoid collector ordering bias)
   324| try:
   325|     max_n = int(max_views_to_inspect)
   326|     if max_n >= 0:
   327|         candidates = candidates[:max_n]
   328| except:
   329|     pass
   330| 
   331| # Sample breadth-first: first N per bucket = "<is_template>|<ViewType>"
   332| selected = []
   333| bucket_counts = {}
   334| for v in candidates:
   335|     bk = _view_bucket_key(v)
   336|     c = bucket_counts.get(bk, 0)
   337|     if per_bucket_limit is None:
   338|         ok = True
   339|     else:
   340|         try:
   341|             lim = int(per_bucket_limit)
   342|             ok = True if lim < 0 else (c < lim)
   343|         except:
   344|             ok = c < 2
   345|     if ok:
   346|         selected.append(v)
   347|         bucket_counts[bk] = c + 1
   348| 
   349| # Fallback: ensure at least one if there are any candidates
   350| if len(selected) == 0 and len(candidates) > 0:
   351|     selected = [candidates[0]]
   352| 
   353| 
   354| # -------------------------
   355| # Extract application surface (synthetic "parameters")
   356| # -------------------------
   357| 
   358| OGS_FIELDS = [
   359|     # Lines
   360|     ("vfa.ogs.proj_line_color.rgb", "ProjectionLineColor", "color_rgb"),
   361|     ("vfa.ogs.proj_line_color.hex", "ProjectionLineColor", "color_hex"),
   362|     ("vfa.ogs.cut_line_color.rgb", "CutLineColor", "color_rgb"),
   363|     ("vfa.ogs.cut_line_color.hex", "CutLineColor", "color_hex"),
   364|     ("vfa.ogs.proj_line_pattern_id", "ProjectionLinePatternId", "elementid"),
   365|     ("vfa.ogs.cut_line_pattern_id", "CutLinePatternId", "elementid"),
   366|     ("vfa.ogs.proj_line_weight", "ProjectionLineWeight", "int"),
   367|     ("vfa.ogs.cut_line_weight", "CutLineWeight", "int"),
   368| 
   369|     # Surface patterns
   370|     ("vfa.ogs.surf_fg_pattern_id", "SurfaceForegroundPatternId", "elementid"),
   371|     ("vfa.ogs.surf_fg_pattern_color.rgb", "SurfaceForegroundPatternColor", "color_rgb"),
   372|     ("vfa.ogs.surf_fg_pattern_color.hex", "SurfaceForegroundPatternColor", "color_hex"),
   373|     ("vfa.ogs.surf_bg_pattern_id", "SurfaceBackgroundPatternId", "elementid"),
   374|     ("vfa.ogs.surf_bg_pattern_color.rgb", "SurfaceBackgroundPatternColor", "color_rgb"),
   375|     ("vfa.ogs.surf_bg_pattern_color.hex", "SurfaceBackgroundPatternColor", "color_hex"),
   376| 
   377|     # Cut patterns
   378|     ("vfa.ogs.cut_fg_pattern_id", "CutForegroundPatternId", "elementid"),
   379|     ("vfa.ogs.cut_fg_pattern_color.rgb", "CutForegroundPatternColor", "color_rgb"),
   380|     ("vfa.ogs.cut_fg_pattern_color.hex", "CutForegroundPatternColor", "color_hex"),
   381|     ("vfa.ogs.cut_bg_pattern_id", "CutBackgroundPatternId", "elementid"),
   382|     ("vfa.ogs.cut_bg_pattern_color.rgb", "CutBackgroundPatternColor", "color_rgb"),
   383|     ("vfa.ogs.cut_bg_pattern_color.hex", "CutBackgroundPatternColor", "color_hex"),
   384| 
   385|     # Misc
   386|     ("vfa.ogs.halftone", "Halftone", "bool_int"),
   387|     ("vfa.ogs.transparency", "Transparency", "int"),
   388| ]
   389| 
   390| def _pv_from_ogs_field(ogs, attr_name, kind):
   391|     q, v = _ogs_get(ogs, attr_name)
   392|     if q != "ok":
   393|         # map to contract q-states
   394|         if q == "unsupported":
   395|             return _contract("unsupported", None, None, None, None)
   396|         if q == "missing":
   397|             return _contract("missing", None, None, None, None)
   398|         return _contract("unreadable", None, None, None, None)
   399| 
   400|     if kind == "int":
   401|         return _as_int_contract(v)
   402| 
   403|     if kind == "bool_int":
   404|         return _as_bool_int_contract(v)
   405| 
   406|     if kind == "elementid":
   407|         return _as_elementid_contract(v)
   408| 
   409|     if kind == "color_rgb":
   410|         rgb, hx = _color_rgb_hex(v)
   411|         if rgb is None:
   412|             return _contract("missing", "String", None, None, None)
   413|         # match line_styles: raw/display/norm are all the rgb triplet
   414|         return _contract("ok", "String", rgb, rgb, rgb)
   415| 
   416|     if kind == "color_hex":
   417|         rgb, hx = _color_rgb_hex(v)
   418|         if hx is None:
   419|             return _contract("missing", "String", None, None, None)
   420|         # match line_styles: raw/display/norm are all the hex string
   421|         return _contract("ok", "String", hx, hx, hx)
   422| 
   423|     # should never happen, but remain defensive
   424|     return _contract("unsupported", None, None, None, None)
   425| 
   426| def _is_defaultish_ogs_value(pk, pv):
   427|     """
   428|     Used only for computing a signature hash (not for inventory).
   429|     Conservatively treat None as default; for ints treat 0 as default; for strings treat None as default.
   430|     """
   431|     if pv is None:
   432|         return True
   433|     if pv.get("q") != "ok":
   434|         return True
   435|     n = pv.get("norm")
   436|     if n is None:
   437|         return True
   438|     # common defaults
   439|     try:
   440|         if pv.get("storage") == "Integer" and int(n) == 0:
   441|             return True
   442|     except:
   443|         pass
   444|     return False
   445| 
   446| def _collect_applied_filters_in_order(v):
   447|     fids = _safe(lambda: list(v.GetFilters()), default=[])
   448|     try:
   449|         fids = list(fids)
   450|     except:
   451|         pass
   452| 
   453|     # truncate per view, preserving order
   454|     try:
   455|         lim = int(per_view_filter_limit)
   456|         if lim >= 0:
   457|             fids = fids[:lim]
   458|     except:
   459|         pass
   460| 
   461|     return fids
```
