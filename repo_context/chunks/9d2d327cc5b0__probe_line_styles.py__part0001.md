# Chunk of tools/probes/probe_line_styles.py

- Source relative path: `tools/probes/probe_line_styles.py`
- Chunk: 1 of 3
- Original line range: 1-472
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display_param, _format_param_contract, _contract_value, _rgb_triplet, _hex_rgb_from_triplet, _get_lines_category_id, _is_line_style_graphicsstyle, _bucket_key, _maybe_set_example, _index_param, _virtual_surface
- Source SHA-256: 37339fee28db23b0f48664b771d0c3bb9d107f1271f8c7ea137315329b786ee7
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # tools/probes/probe_line_styles.py
     2| #
     3| # Dynamo Python (Revit) — Breadth Probe: line_styles (INVENTORY OUTPUT)
     4| #
     5| # OUT = [
     6| #   {
     7| #     "kind": "inventory",
     8| #     "domain": "line_styles",
     9| #     "records": [...],
    10| #     "file_written": "<path>|None",
    11| #     "file_write_error": "<error>|None"
    12| #   },
    13| #   {
    14| #     "kind": "crosswalk",
    15| #     "domain": "line_styles",
    16| #     "records": [...]
    17| #   }
    18| # ]
    19| #
    20| # Inputs:
    21| #   IN[0] max_styles_to_inspect (int)
    22| #        Maximum number of line styles (GraphicsStyle) to inspect AFTER filtering.
    23| #        Default: 500
    24| #
    25| #   IN[1] enable_crosswalk (bool)
    26| #        Whether to emit LineStyle → LinePattern crosswalk.
    27| #        Default: False
    28| #
    29| #   IN[2] per_bucket_limit (int)
    30| #        Sample at most N styles per bucket (GraphicsStyleType + parent category).
    31| #        Default: 50  (set large to effectively scan all)
    32| #
    33| #   IN[3] write_json (bool)
    34| #        When True, serialize OUT to a valid JSON file on disk.
    35| #        Default: False
    36| #
    37| #   IN[4] output_directory (str)
    38| #        Directory path where JSON will be written.
    39| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    40| #        If None, falls back to RVT directory, then TEMP.
    41| #
    42| #   IN[5] crosswalk_scan_limit (int)
    43| #        Maximum number of line styles to *inspect* when building the crosswalk.
    44| #        This controls how far the probe scans to discover distinct
    45| #        LineStyle → LinePattern relationships.
    46| #
    47| #        This limit is:
    48| #          - independent of inventory sampling limits
    49| #          - applied before relationship deduplication
    50| #
    51| #        Default: 2000
    52| #        Set to a large value to approach full-document breadth.
    53| #
    54| #   IN[6] crosswalk_emit_limit (int)
    55| #        Maximum number of *distinct relationship records* emitted
    56| #        in the LineStyle → LinePattern crosswalk.
    57| #
    58| #        Deduplication key:
    59| #          (line_pattern.id, graphics_style_type)
    60| #
    61| #        This bounds output size while preserving relationship breadth.
    62| #
    63| #        Default: 200
    64| #
    65| # Notes:
    66| #   - "line styles" are modeled as GraphicsStyle elements whose GraphicsStyleCategory
    67| #     is a subcategory under the built-in Lines category (OST_Lines).
    68| #   - Many meaningful attributes for a line style are *category properties* (color, pattern, weights),
    69| #     not Revit Parameters. This probe captures both:
    70| #       * real parameters on GraphicsStyle (p.<DefinitionName>)
    71| #       * virtual properties from Category/GraphicsStyle (v.<...>) as parameter-like evidence
    72| #
    73| # Reference pattern: probe_arrowheads.py (authoritative structure & IO).  :contentReference[oaicite:0]{index=0}
    74| 
    75| 
    76| import clr
    77| import os
    78| import json
    79| from datetime import datetime
    80| 
    81| clr.AddReference("RevitServices")
    82| from RevitServices.Persistence import DocumentManager
    83| 
    84| clr.AddReference("RevitAPI")
    85| from Autodesk.Revit.DB import (
    86|     FilteredElementCollector, ElementId,
    87|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    88|     BuiltInCategory
    89| )
    90| 
    91| try:
    92|     from Autodesk.Revit.DB import SpecTypeId
    93| except:
    94|     SpecTypeId = None
    95| 
    96| try:
    97|     from Autodesk.Revit.DB import GraphicsStyle, GraphicsStyleType, LinePatternElement
    98| except:
    99|     GraphicsStyle = None
   100|     GraphicsStyleType = None
   101|     LinePatternElement = None
   102| 
   103| doc = DocumentManager.Instance.CurrentDBDocument
   104| 
   105| max_styles_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
   106| enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
   107| per_bucket_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 50
   108| write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
   109| out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
   110| 
   111| 
   112| # -------------------------
   113| # Helpers (defensive)
   114| # -------------------------
   115| 
   116| def _safe(fn, default=None):
   117|     try:
   118|         return fn()
   119|     except:
   120|         return default
   121| 
   122| def _safe_param_def_name(p):
   123|     try:
   124|         d = p.Definition
   125|         return d.Name if d is not None else None
   126|     except:
   127|         return None
   128| 
   129| def _safe_get_datatype(p):
   130|     try:
   131|         d = p.Definition
   132|         if d is None:
   133|             return None
   134|         return d.GetDataType()
   135|     except:
   136|         return None
   137| 
   138| def _is_length_datatype(dt):
   139|     if dt is None or SpecTypeId is None:
   140|         return False
   141|     try:
   142|         return dt == SpecTypeId.Length
   143|     except:
   144|         return False
   145| 
   146| def _is_angle_datatype(dt):
   147|     if dt is None or SpecTypeId is None:
   148|         return False
   149|     try:
   150|         return dt == SpecTypeId.Angle
   151|     except:
   152|         return False
   153| 
   154| def _fmt_display_param(p, raw_double=None):
   155|     try:
   156|         if raw_double is not None:
   157|             dt = _safe_get_datatype(p)
   158|             if dt is not None:
   159|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   160|             return str(raw_double)
   161|         return p.AsValueString()
   162|     except:
   163|         return _safe(lambda: p.AsValueString(), None)
   164| 
   165| def _format_param_contract(p):
   166|     """
   167|     Contract:
   168|       {
   169|         "q": "ok|missing|unreadable|unsupported",
   170|         "storage": "String|Integer|Double|ElementId|None",
   171|         "raw": ...,
   172|         "display": ...,
   173|         "norm": ...
   174|       }
   175| 
   176|     Probe choices:
   177|       - Integer.norm stays int (enum-safe).
   178|       - Length -> inches, Angle -> degrees (when datatype detected).
   179|       - ElementId -> IntegerValue; attempt to resolve name cheaply.
   180|     """
   181|     if p is None:
   182|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   183| 
   184|     st = _safe(lambda: p.StorageType, None)
   185|     if st is None:
   186|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   187| 
   188|     if st == StorageType.String:
   189|         raw = _safe(lambda: p.AsString(), None)
   190|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   191| 
   192|     if st == StorageType.Integer:
   193|         raw = _safe(lambda: p.AsInteger(), None)
   194|         disp = _fmt_display_param(p, None)
   195|         return {
   196|             "q": "ok",
   197|             "storage": "Integer",
   198|             "raw": raw,
   199|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   200|             "norm": raw
   201|         }
   202| 
   203|     if st == StorageType.Double:
   204|         raw = _safe(lambda: p.AsDouble(), None)
   205|         disp = _fmt_display_param(p, raw)
   206|         dt = _safe_get_datatype(p)
   207|         if raw is None:
   208|             norm = None
   209|         elif _is_length_datatype(dt):
   210|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   211|         elif _is_angle_datatype(dt):
   212|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   213|         else:
   214|             norm = raw
   215|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   216| 
   217|     if st == StorageType.ElementId:
   218|         eid = _safe(lambda: p.AsElementId(), None)
   219|         if eid is None or eid == ElementId.InvalidElementId:
   220|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   221| 
   222|         raw = _safe(lambda: eid.IntegerValue, None)
   223|         ref = _safe(lambda: doc.GetElement(eid), None)
   224|         ref_name = _safe(lambda: ref.Name, None) if ref is not None else None
   225|         return {
   226|             "q": "ok",
   227|             "storage": "ElementId",
   228|             "raw": raw,
   229|             "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
   230|             "norm": raw
   231|         }
   232| 
   233|     return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}
   234| 
   235| def _contract_value(q, storage, raw, display, norm):
   236|     # small helper to treat non-Parameter properties as parameter-like evidence
   237|     return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}
   238| 
   239| def _rgb_triplet(color):
   240|     # Autodesk.Revit.DB.Color -> "R|G|B" string
   241|     if color is None:
   242|         return None
   243|     r = _safe(lambda: int(color.Red), None)
   244|     g = _safe(lambda: int(color.Green), None)
   245|     b = _safe(lambda: int(color.Blue), None)
   246|     if r is None or g is None or b is None:
   247|         return None
   248|     return "{}|{}|{}".format(r, g, b)
   249| 
   250| def _hex_rgb_from_triplet(rgb_triplet):
   251|     if not rgb_triplet:
   252|         return None
   253|     try:
   254|         parts = rgb_triplet.split("|")
   255|         if len(parts) != 3:
   256|             return None
   257|         r = int(parts[0]); g = int(parts[1]); b = int(parts[2])
   258|         return "#{:02X}{:02X}{:02X}".format(r & 0xFF, g & 0xFF, b & 0xFF)
   259|     except:
   260|         return None
   261| 
   262| def _get_lines_category_id():
   263|     cat = _safe(lambda: doc.Settings.Categories.get_Item(BuiltInCategory.OST_Lines), None)
   264|     return _safe(lambda: cat.Id.IntegerValue, None) if cat is not None else None
   265| 
   266| def _is_line_style_graphicsstyle(gs, lines_cat_id_int):
   267|     # True if GraphicsStyleCategory is a subcategory of Lines category
   268|     if gs is None or lines_cat_id_int is None:
   269|         return False
   270|     c = _safe(lambda: gs.GraphicsStyleCategory, None)
   271|     if c is None:
   272|         return False
   273|     parent = _safe(lambda: c.Parent, None)
   274|     if parent is None:
   275|         return False
   276|     pid = _safe(lambda: parent.Id.IntegerValue, None)
   277|     return True if pid == lines_cat_id_int else False
   278| 
   279| def _bucket_key(gs):
   280|     # sampling bucket: style type + parent category (usually "Lines")
   281|     gst = _safe(lambda: gs.GraphicsStyleType, None)
   282|     c = _safe(lambda: gs.GraphicsStyleCategory, None)
   283|     parent = _safe(lambda: c.Parent, None) if c is not None else None
   284|     return "{}|{}".format(str(gst), _safe(lambda: parent.Name, None))
   285| 
   286| 
   287| # -------------------------
   288| # Discovery + Sampling
   289| # -------------------------
   290| 
   291| lines_cat_id_int = _get_lines_category_id()
   292| 
   293| all_gs = []
   294| if GraphicsStyle is not None:
   295|     all_gs = _safe(
   296|         lambda: list(FilteredElementCollector(doc).OfClass(GraphicsStyle).ToElements()),
   297|         default=[]
   298|     )
   299| 
   300| # Filter to line styles (subcategory under Lines)
   301| hits = []
   302| for gs in all_gs:
   303|     if _is_line_style_graphicsstyle(gs, lines_cat_id_int):
   304|         hits.append(gs)
   305| 
   306| # Cap AFTER filtering
   307| try:
   308|     max_n = int(max_styles_to_inspect)
   309|     if max_n >= 0:
   310|         hits = hits[:max_n]
   311| except:
   312|     pass
   313| 
   314| # Sample per bucket (breadth bias)
   315| selected = []
   316| by_bucket = {}  # bucket_key -> count
   317| for gs in hits:
   318|     bk = _bucket_key(gs)
   319|     c = by_bucket.get(bk, 0)
   320| 
   321|     if per_bucket_limit is None:
   322|         ok = True
   323|     else:
   324|         try:
   325|             ok = c < int(per_bucket_limit)
   326|         except:
   327|             ok = c < 50
   328| 
   329|     if ok:
   330|         selected.append(gs)
   331|         by_bucket[bk] = c + 1
   332| 
   333| # Ensure at least one per bucket if limits were too strict
   334| if len(selected) == 0 and len(hits) > 0:
   335|     seen = set()
   336|     for gs in hits:
   337|         bk = _bucket_key(gs)
   338|         if bk not in seen:
   339|             selected.append(gs)
   340|             seen.add(bk)
   341| 
   342| 
   343| # -------------------------
   344| # Build inventory (union over selected)
   345| # -------------------------
   346| 
   347| # param_key -> {
   348| #   storage_types: set(str),
   349| #   q_counts: dict,
   350| #   example: dict or None,
   351| #   observed_on_buckets: set(str)
   352| # }
   353| param_index = {}
   354| 
   355| def _maybe_set_example(entry, pv):
   356|     # exactly one example: prefer first "ok", else first non-ok
   357|     if pv is None:
   358|         return
   359|     ex = entry.get("example")
   360|     if ex is None:
   361|         entry["example"] = {
   362|             "q": pv.get("q"),
   363|             "storage": pv.get("storage"),
   364|             "raw": pv.get("raw"),
   365|             "display": pv.get("display"),
   366|             "norm": pv.get("norm")
   367|         }
   368|         return
   369|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   370|         entry["example"] = {
   371|             "q": pv.get("q"),
   372|             "storage": pv.get("storage"),
   373|             "raw": pv.get("raw"),
   374|             "display": pv.get("display"),
   375|             "norm": pv.get("norm")
   376|         }
   377| 
   378| def _index_param(pk, pv, bucket_key):
   379|     if pk not in param_index:
   380|         param_index[pk] = {
   381|             "storage_types": set(),
   382|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   383|             "example": None,
   384|             "observed_on_buckets": set()
   385|         }
   386| 
   387|     e = param_index[pk]
   388|     st = pv.get("storage")
   389|     q = pv.get("q") or "unreadable"
   390| 
   391|     if st:
   392|         e["storage_types"].add(st)
   393|     if q not in e["q_counts"]:
   394|         e["q_counts"][q] = 0
   395|     e["q_counts"][q] += 1
   396|     e["observed_on_buckets"].add(bucket_key)
   397| 
   398|     _maybe_set_example(e, pv)
   399| 
   400| def _virtual_surface(gs):
   401|     """
   402|     Produce virtual properties treated as parameter-like evidence.
   403|     """
   404|     out = {}
   405| 
   406|     c = _safe(lambda: gs.GraphicsStyleCategory, None)
   407|     parent = _safe(lambda: c.Parent, None) if c is not None else None
   408| 
   409|     # Names / ids
   410|     gs_id = _safe(lambda: gs.Id.IntegerValue, None)
   411|     gs_name = _safe(lambda: gs.Name, None)
   412|     cat_name = _safe(lambda: c.Name, None) if c is not None else None
   413|     cat_id = _safe(lambda: c.Id.IntegerValue, None) if c is not None else None
   414|     parent_name = _safe(lambda: parent.Name, None) if parent is not None else None
   415|     parent_id = _safe(lambda: parent.Id.IntegerValue, None) if parent is not None else None
   416| 
   417|     gst = _safe(lambda: gs.GraphicsStyleType, None)
   418| 
   419|     out["v.gs.id"] = _contract_value("ok", "Integer", gs_id, str(gs_id) if gs_id is not None else None, gs_id)
   420|     out["v.gs.name"] = _contract_value("ok", "String", gs_name, gs_name, gs_name)
   421|     out["v.gs.type"] = _contract_value("ok", "String", str(gst), str(gst), str(gst))
   422| 
   423|     out["v.cat.id"] = _contract_value("ok", "Integer", cat_id, str(cat_id) if cat_id is not None else None, cat_id)
   424|     out["v.cat.name"] = _contract_value("ok", "String", cat_name, cat_name, cat_name)
   425| 
   426|     out["v.parent_cat.id"] = _contract_value("ok", "Integer", parent_id, str(parent_id) if parent_id is not None else None, parent_id)
   427|     out["v.parent_cat.name"] = _contract_value("ok", "String", parent_name, parent_name, parent_name)
   428| 
   429|     # Category properties: line color (R|G|B) + hex
   430|     color = _safe(lambda: c.LineColor, None) if c is not None else None
   431|     rgb = _rgb_triplet(color)
   432|     rgb_hex = _hex_rgb_from_triplet(rgb)
   433| 
   434|     if rgb is None:
   435|         out["v.line_color.rgb"] = _contract_value("missing", "String", None, None, None)
   436|         out["v.line_color.hex"] = _contract_value("missing", "String", None, None, None)
   437|     else:
   438|         out["v.line_color.rgb"] = _contract_value("ok", "String", rgb, rgb, rgb)
   439|         out["v.line_color.hex"] = _contract_value("ok", "String", rgb_hex, rgb_hex, rgb_hex)
   440| 
   441|     # Line pattern is an ElementId on Category
   442|     pat_id = _safe(lambda: c.GetLinePatternId(GraphicsStyleType.Projection), None) if (c is not None and GraphicsStyleType is not None) else None
   443|     if pat_id is None and c is not None:
   444|         pat_id = _safe(lambda: c.LinePatternId, None)
   445| 
   446|     pat_int = _safe(lambda: pat_id.IntegerValue, None) if pat_id is not None else None
   447|     pat_name = None
   448|     if pat_id is not None and pat_id != ElementId.InvalidElementId:
   449|         pe = _safe(lambda: doc.GetElement(pat_id), None)
   450|         pat_name = _safe(lambda: pe.Name, None) if pe is not None else None
   451| 
   452|     if pat_int is None:
   453|         out["v.line_pattern.id"] = _contract_value("missing", "ElementId", None, None, None)
   454|         out["v.line_pattern.name"] = _contract_value("missing", "String", None, None, None)
   455|     else:
   456|         out["v.line_pattern.id"] = _contract_value("ok", "ElementId", pat_int, str(pat_int), pat_int)
   457|         out["v.line_pattern.name"] = _contract_value("ok", "String", pat_name, pat_name if pat_name is not None else str(pat_int), pat_name)
   458| 
   459|     # Line weight: projection only (line styles do not have a "cut" weight surface)
   460|     lw_proj = None
   461|     if c is not None and GraphicsStyleType is not None:
   462|         lw_proj = _safe(lambda: c.GetLineWeight(GraphicsStyleType.Projection), None)
   463| 
   464|     out["v.line_weight.projection"] = _contract_value(
   465|         "ok" if lw_proj is not None else "missing",
   466|         "Integer",
   467|         lw_proj,
   468|         str(lw_proj) if lw_proj is not None else None,
   469|         lw_proj
   470|     )
   471| 
   472|     return out
```
