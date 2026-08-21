# Chunk of tools/probes/probe_text_types.py

- Source relative path: `tools/probes/probe_text_types.py`
- Chunk: 1 of 3
- Original line range: 1-453
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_type_name, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display, _format_param_contract, _contract_value, _rgb_triplet_from_int, _hex32_from_int, _rgb_rrggbb_from_int, _rgb_bbgrr_from_int, _hex_rgb_from_triplet, _slug, _looks_like_text_type, _text_font_key, _maybe_set_example
- Source SHA-256: 87bfb05b1c55eb50cd88eaad88e7bb9c9a5d9f1f5e0657eb612ffc30f9ac7ced
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: text_types (INVENTORY OUTPUT)
     2| #
     3| # OUT = [
     4| #   {
     5| #     "kind": "inventory",
     6| #     "domain": "text_types",
     7| #     "records": param_inventory,
     8| #     "file_written": "<path>|None",        # present only if write_json=True
     9| #     "file_write_error": "<error>|None"    # present only on failure
    10| #   },
    11| #   {
    12| #     "kind": "crosswalk",
    13| #     "domain": "text_types",
    14| #     "records": optional_crosswalk
    15| #   }
    16| # ]
    17| #
    18| # Inputs:
    19| #   IN[0] max_types_to_inspect (int)
    20| #        Maximum number of candidate Text Types (ElementTypes) to inspect AFTER filtering.
    21| #        Default: 500
    22| #
    23| #   IN[1] enable_crosswalk (bool)
    24| #        Whether to emit TextType -> Leader Arrowhead crosswalk (if present).
    25| #        Default: False
    26| #
    27| #   IN[2] per_font_limit (int)
    28| #        Sample at most N text types per Text Font value (breadth bias).
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
    40| # Notes:
    41| #  - This probe is exploratory evidence capture for join-key / semantic policy design.
    42| #  - Discovery is progressive:
    43| #      (1) parameter-signature discovery across ElementType
    44| #      (2) fallback: OfClass(TextNoteType) if signature yields nothing
    45| #  - Inventory is deduped probe-locally for q_counts by (param_key, storage, norm).
    46| 
    47| 
    48| import clr
    49| import os
    50| import json
    51| from datetime import datetime
    52| 
    53| clr.AddReference("RevitServices")
    54| from RevitServices.Persistence import DocumentManager
    55| 
    56| clr.AddReference("RevitAPI")
    57| from Autodesk.Revit.DB import (
    58|     FilteredElementCollector, ElementId, ElementType,
    59|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    60|     BuiltInParameter
    61| )
    62| 
    63| try:
    64|     from Autodesk.Revit.DB import SpecTypeId
    65| except:
    66|     SpecTypeId = None
    67| 
    68| # Optional: class-based fallback
    69| try:
    70|     from Autodesk.Revit.DB import TextNoteType
    71| except:
    72|     TextNoteType = None
    73| 
    74| doc = DocumentManager.Instance.CurrentDBDocument
    75| 
    76| max_types_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
    77| enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
    78| per_font_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 2
    79| write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
    80| out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
    81| 
    82| 
    83| # -------------------------
    84| # Helpers (defensive)
    85| # -------------------------
    86| 
    87| def _safe(fn, default=None):
    88|     try:
    89|         return fn()
    90|     except:
    91|         return default
    92| 
    93| def _safe_type_name(elem):
    94|     for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
    95|         try:
    96|             p = elem.get_Parameter(bip)
    97|             if p is not None:
    98|                 s = p.AsString()
    99|                 if s:
   100|                     return s
   101|         except:
   102|             pass
   103|     try:
   104|         return elem.Name
   105|     except:
   106|         return None
   107| 
   108| def _safe_param_def_name(p):
   109|     try:
   110|         d = p.Definition
   111|         return d.Name if d is not None else None
   112|     except:
   113|         return None
   114| 
   115| def _safe_get_datatype(p):
   116|     try:
   117|         d = p.Definition
   118|         if d is None:
   119|             return None
   120|         return d.GetDataType()
   121|     except:
   122|         return None
   123| 
   124| def _is_length_datatype(dt):
   125|     if dt is None or SpecTypeId is None:
   126|         return False
   127|     try:
   128|         return dt == SpecTypeId.Length
   129|     except:
   130|         return False
   131| 
   132| def _is_angle_datatype(dt):
   133|     if dt is None or SpecTypeId is None:
   134|         return False
   135|     try:
   136|         return dt == SpecTypeId.Angle
   137|     except:
   138|         return False
   139| 
   140| def _fmt_display(p, raw_double=None):
   141|     try:
   142|         if raw_double is not None:
   143|             dt = _safe_get_datatype(p)
   144|             if dt is not None:
   145|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   146|             return str(raw_double)
   147|         return p.AsValueString()
   148|     except:
   149|         return _safe(lambda: p.AsValueString(), None)
   150| 
   151| def _format_param_contract(p):
   152|     """
   153|     Contract:
   154|       {
   155|         "q": "ok|missing|unreadable|unsupported",
   156|         "storage": "String|Integer|Double|ElementId|None",
   157|         "raw": ...,
   158|         "display": ...,
   159|         "norm": ...
   160|       }
   161| 
   162|     Probe choices:
   163|       - Integer.norm stays int (enum-safe)
   164|       - Length -> inches (float) when datatype is Length
   165|       - Angle  -> degrees (float) when datatype is Angle
   166|       - ElementId -> IntegerValue (norm=int), display resolves name cheaply if possible
   167|     """
   168|     if p is None:
   169|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   170| 
   171|     st = _safe(lambda: p.StorageType, None)
   172|     if st is None:
   173|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   174| 
   175|     if st == StorageType.String:
   176|         raw = _safe(lambda: p.AsString(), None)
   177|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   178| 
   179|     if st == StorageType.Integer:
   180|         raw = _safe(lambda: p.AsInteger(), None)
   181|         disp = _fmt_display(p, None)
   182|         return {
   183|             "q": "ok",
   184|             "storage": "Integer",
   185|             "raw": raw,
   186|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   187|             "norm": raw
   188|         }
   189| 
   190|     if st == StorageType.Double:
   191|         raw = _safe(lambda: p.AsDouble(), None)
   192|         disp = _fmt_display(p, raw)
   193|         dt = _safe_get_datatype(p)
   194|         if raw is None:
   195|             norm = None
   196|         elif _is_length_datatype(dt):
   197|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   198|         elif _is_angle_datatype(dt):
   199|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   200|         else:
   201|             norm = raw
   202|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   203| 
   204|     if st == StorageType.ElementId:
   205|         eid = _safe(lambda: p.AsElementId(), None)
   206|         if eid is None or eid == ElementId.InvalidElementId:
   207|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   208| 
   209|         raw = _safe(lambda: eid.IntegerValue, None)
   210|         ref_name = None
   211|         ref = _safe(lambda: doc.GetElement(eid), None)
   212|         if ref is not None:
   213|             ref_name = _safe(lambda: ref.Name, None)
   214|             if ref_name is None:
   215|                 ref_name = _safe(lambda: _safe_type_name(ref), None)
   216| 
   217|         return {
   218|             "q": "ok",
   219|             "storage": "ElementId",
   220|             "raw": raw,
   221|             "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
   222|             "norm": raw
   223|         }
   224| 
   225|     return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}
   226| 
   227| def _contract_value(q, storage, raw, display, norm):
   228|     # helper to treat derived/virtual properties as parameter-like evidence
   229|     return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}
   230| 
   231| def _rgb_triplet_from_int(color_int):
   232|     """
   233|     Best-effort parse for Revit integer color surfaces.
   234|     Assumes a 24-bit packed RGB (0xRRGGBB). If Revit uses a different packing
   235|     in your environment, this will show up immediately in evidence.
   236|     """
   237|     if color_int is None:
   238|         return None
   239|     try:
   240|         n = int(color_int)
   241|         # only trust lower 24 bits
   242|         r = (n >> 16) & 0xFF
   243|         g = (n >> 8) & 0xFF
   244|         b = n & 0xFF
   245|         return "{}|{}|{}".format(r, g, b)
   246|     except:
   247|         return None
   248| 
   249| def _hex32_from_int(n):
   250|     if n is None:
   251|         return None
   252|     try:
   253|         u = int(n) & 0xFFFFFFFF
   254|         return "0x{:08X}".format(u)
   255|     except:
   256|         return None
   257| 
   258| def _rgb_rrggbb_from_int(n):
   259|     if n is None:
   260|         return None
   261|     try:
   262|         u = int(n) & 0xFFFFFFFF
   263|         # assume low 24 bits are RRGGBB
   264|         r = (u >> 16) & 0xFF
   265|         g = (u >> 8) & 0xFF
   266|         b = u & 0xFF
   267|         return "{}|{}|{}".format(r, g, b)
   268|     except:
   269|         return None
   270| 
   271| def _rgb_bbgrr_from_int(n):
   272|     if n is None:
   273|         return None
   274|     try:
   275|         u = int(n) & 0xFFFFFFFF
   276|         # assume low 24 bits are BBGGRR
   277|         b = (u >> 16) & 0xFF
   278|         g = (u >> 8) & 0xFF
   279|         r = u & 0xFF
   280|         return "{}|{}|{}".format(r, g, b)
   281|     except:
   282|         return None
   283| 
   284| def _hex_rgb_from_triplet(rgb_triplet):
   285|     if not rgb_triplet:
   286|         return None
   287|     try:
   288|         parts = rgb_triplet.split("|")
   289|         if len(parts) != 3:
   290|             return None
   291|         r = int(parts[0]); g = int(parts[1]); b = int(parts[2])
   292|         return "#{:02X}{:02X}{:02X}".format(r & 0xFF, g & 0xFF, b & 0xFF)
   293|     except:
   294|         return None
   295| 
   296| def _slug(s):
   297|     try:
   298|         return "".join([c.lower() if c.isalnum() else "_" for c in str(s)]).strip("_")
   299|     except:
   300|         return "unknown"
   301| 
   302| def _looks_like_text_type(t):
   303|     """
   304|     Signature heuristic for TextNoteType / text styles:
   305|       Required-ish params: Text Font, Text Size
   306|       Helpful params (any): Text Width Scale, Background, Show Border, Keep Readable, Bold/Italic/Underline
   307|     """
   308|     required = ["Text Font", "Text Size"]
   309|     optional = [
   310|         "Text Width Scale",
   311|         "Background",
   312|         "Show Border",
   313|         "Keep Readable",
   314|         "Bold",
   315|         "Italic",
   316|         "Underline",
   317|         "Leader Arrowhead",
   318|         "Leader Arrowhead Type",
   319|         "Leader Arrowhead Symbol"
   320|     ]
   321|     try:
   322|         for pn in required:
   323|             if t.LookupParameter(pn) is None:
   324|                 return False
   325|         for pn in optional:
   326|             if t.LookupParameter(pn) is not None:
   327|                 return True
   328|         # If it has the required set but none of the optional set, still accept
   329|         # (some templates expose fewer toggles)
   330|         return True
   331|     except:
   332|         return False
   333| 
   334| def _text_font_key(t):
   335|     p = _safe(lambda: t.LookupParameter("Text Font"), None)
   336|     if p is None:
   337|         return ("missing", None)
   338|     pv = _format_param_contract(p)
   339|     raw = pv.get("raw")
   340|     disp = pv.get("display")
   341|     return ("{}|{}".format(raw, disp), pv)
   342| 
   343| 
   344| # -------------------------
   345| # Discovery + Sampling
   346| # -------------------------
   347| 
   348| hits = []
   349| 
   350| # Step 1 (preferred): class-based collector for Text Types
   351| # This avoids signature bleed-through from unrelated ElementTypes (e.g., stairs).
   352| if TextNoteType is not None:
   353|     hits = _safe(
   354|         lambda: list(
   355|             FilteredElementCollector(doc)
   356|             .WhereElementIsElementType()
   357|             .OfClass(TextNoteType)
   358|             .ToElements()
   359|         ),
   360|         default=[]
   361|     )
   362| 
   363| # Step 2 (fallback): parameter-signature discovery across ElementType
   364| # Only used if TextNoteType is unavailable or yields no results.
   365| if len(hits) == 0:
   366|     all_types = _safe(
   367|         lambda: (FilteredElementCollector(doc)
   368|                  .WhereElementIsElementType()
   369|                  .OfClass(ElementType)
   370|                  .ToElements()),
   371|         default=[]
   372|     )
   373| 
   374|     try:
   375|         all_types = list(all_types)
   376|     except:
   377|         all_types = list(all_types)
   378| 
   379|     for t in all_types:
   380|         if _looks_like_text_type(t):
   381|             hits.append(t)
   382| 
   383| # Cap AFTER filtering / collection
   384| try:
   385|     max_n = int(max_types_to_inspect)
   386|     if max_n >= 0:
   387|         hits = hits[:max_n]
   388| except:
   389|     pass
   390| 
   391| # Sample first N per Text Font (breadth bias)
   392| selected = []
   393| by_font = {}  # font_key -> count
   394| for t in hits:
   395|     fk, _ = _text_font_key(t)
   396|     c = by_font.get(fk, 0)
   397|     if per_font_limit is None:
   398|         per_font_ok = True
   399|     else:
   400|         try:
   401|             per_font_ok = c < int(per_font_limit)
   402|         except:
   403|             per_font_ok = c < 2
   404|     if per_font_ok:
   405|         selected.append(t)
   406|         by_font[fk] = c + 1
   407| 
   408| # Fallback: ensure at least 1 per font if per_font_limit <= 0
   409| if len(selected) == 0 and len(hits) > 0:
   410|     seen = set()
   411|     for t in hits:
   412|         fk, _ = _text_font_key(t)
   413|         if fk not in seen:
   414|             selected.append(t)
   415|             seen.add(fk)
   416| 
   417| 
   418| # -------------------------
   419| # Build inventory (union over selected)
   420| # Dedup for q_counts by (param_key, storage, norm)
   421| # -------------------------
   422| 
   423| # param_key -> {
   424| #   storage_types: set(str),
   425| #   q_counts: dict,
   426| #   example: dict or None,
   427| #   observed_on_font_keys: set(str),
   428| #   seen_sigs: set(tuple(storage, norm, q))
   429| # }
   430| param_index = {}
   431| 
   432| def _maybe_set_example(entry, pv):
   433|     # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
   434|     if pv is None:
   435|         return
   436|     ex = entry.get("example")
   437|     if ex is None:
   438|         entry["example"] = {
   439|             "q": pv.get("q"),
   440|             "storage": pv.get("storage"),
   441|             "raw": pv.get("raw"),
   442|             "display": pv.get("display"),
   443|             "norm": pv.get("norm")
   444|         }
   445|         return
   446|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   447|         entry["example"] = {
   448|             "q": pv.get("q"),
   449|             "storage": pv.get("storage"),
   450|             "raw": pv.get("raw"),
   451|             "display": pv.get("display"),
   452|             "norm": pv.get("norm")
   453|         }
```
