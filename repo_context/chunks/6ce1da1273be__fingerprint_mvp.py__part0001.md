# Chunk of legacy/fingerprint_mvp.py

- Source relative path: `legacy/fingerprint_mvp.py`
- Chunk: 1 of 3
- Original line range: 1-378
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: add_origin, rgb_sig_from_color, canon_str, sig_val, get_element_display_name, _param, _as_string, _as_double, _as_int, _as_bool_from_param, first_param, fnum, format_len_inches, rgb_dict_from_color, try_get_color_rgb_from_elem, get_type_display_name, safe_str, make_hash, get_doc, get_linestyles_fingerprint
- Source SHA-256: 2b3c4e30443f4500e886e1f968d5ea1da344bf6c9fb01c0de3bb148cfc1b7332
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # -*- coding: utf-8 -*-
     2| import clr
     3| import json
     4| 
     5| # Revit/Dynamo plumbing
     6| clr.AddReference("RevitServices")
     7| from RevitServices.Persistence import DocumentManager
     8| 
     9| clr.AddReference("RevitAPI")
    10| from Autodesk.Revit.DB import (
    11|     FilteredElementCollector,
    12|     LinePatternElement,
    13|     TextNoteType,
    14|     DimensionType,
    15|     View,
    16|     GraphicsStyleType,
    17|     WorksharingUtils,
    18|     BuiltInCategory,
    19|     SpecTypeId,
    20|     CategoryType,
    21|     ElementType,
    22|     BuiltInParameter,
    23|     UnitUtils,
    24|     UnitTypeId,
    25|     ElementId,
    26|     FillPatternElement,
    27|     Category
    28| )
    29| 
    30| DEBUG_INCLUDE_LINEPATTERN_SIGNATURES = True
    31| DEBUG_INCLUDE_FILLPATTERN_SIGNATURES = False
    32| 
    33| # ------------- helpers -----------------
    34| 
    35| def add_origin(key="origin"):
    36|     # Try XYZ-style origin
    37|     try:
    38|         o = g.Origin
    39|         parts.append("grid[{}].{}={},{},{}".format(idx, key, f(o.X), f(o.Y), f(o.Z)))
    40|         return
    41|     except:
    42|         pass
    43| 
    44|     # Try UV-style origin (U,V)
    45|     try:
    46|         o = g.Origin
    47|         parts.append("grid[{}].{}={},".format(idx, key) + "{},<None>".format(f(o.U), f(o.V)))
    48|         return
    49|     except:
    50|         pass
    51| 
    52|     # Try separate U/V properties
    53|     for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin"), ("OffsetU", "OffsetV")]:
    54|         try:
    55|             u = getattr(g, u_name)
    56|             v = getattr(g, v_name)
    57|             parts.append("grid[{}].{}={},{},<None>".format(idx, key, f(u), f(v)))
    58|             return
    59|         except:
    60|             pass
    61| 
    62|     parts.append("grid[{}].{}=<None>".format(idx, key))
    63| 
    64| def rgb_sig_from_color(col):
    65|     try:
    66|         return "{},{},{}".format(int(col.Red), int(col.Green), int(col.Blue))
    67|     except:
    68|         return "<None>"
    69| 
    70| def canon_str(s):
    71|     if s is None:
    72|         return None
    73|     try:
    74|         s2 = safe_str(s)
    75|         return s2.strip()
    76|     except:
    77|         return None
    78| 
    79| def sig_val(v):
    80|     if v is None:
    81|         return "<None>"
    82|     s = safe_str(v).strip()
    83|     return s if s else "<None>"
    84| 
    85| def get_element_display_name(elem):
    86|     if elem is None:
    87|         return None
    88| 
    89|     # 1) .Name
    90|     try:
    91|         nm = getattr(elem, "Name", None)
    92|         nm_c = canon_str(nm)
    93|         if nm_c:
    94|             return nm_c
    95|     except:
    96|         pass
    97| 
    98|     # 2) Common name parameters
    99|     for bip_name in ["SYMBOL_NAME_PARAM", "ALL_MODEL_TYPE_NAME", "ALL_MODEL_INSTANCE_COMMENTS"]:
   100|         bip = getattr(BuiltInParameter, bip_name, None)
   101|         if bip is None:
   102|             continue
   103|         try:
   104|             p = elem.get_Parameter(bip)
   105|             if p and p.HasValue:
   106|                 s = p.AsString()
   107|                 s_c = canon_str(s)
   108|                 if s_c:
   109|                     return s_c
   110|         except:
   111|             pass
   112| 
   113|     return None
   114| 
   115| def _param(elem, bip):
   116|     try:
   117|         return elem.get_Parameter(bip)
   118|     except:
   119|         return None
   120| 
   121| def _as_string(p):
   122|     try:
   123|         if p and p.HasValue:
   124|             s = p.AsString()
   125|             if s is not None:
   126|                 return safe_str(s)
   127|     except:
   128|         pass
   129|     return None
   130| 
   131| def _as_double(p):
   132|     try:
   133|         if p and p.HasValue:
   134|             return p.AsDouble()
   135|     except:
   136|         pass
   137|     return None
   138| 
   139| def _as_int(p):
   140|     try:
   141|         if p and p.HasValue:
   142|             return p.AsInteger()
   143|     except:
   144|         pass
   145|     return None
   146| 
   147| def _as_bool_from_param(p):
   148|     v = _as_int(p)
   149|     if v is None:
   150|         return None
   151|     return True if v != 0 else False
   152| 
   153| def first_param(elem, bip_names=None, ui_names=None):
   154|     # BuiltInParameter by NAME safely (no AttributeError)
   155|     for bip_name in (bip_names or []):
   156|         try:
   157|             bip = getattr(BuiltInParameter, bip_name, None)
   158|         except:
   159|             bip = None
   160|         if bip is None:
   161|             continue
   162|         try:
   163|             p = elem.get_Parameter(bip)
   164|             if p and p.HasValue:
   165|                 return p
   166|         except:
   167|             pass
   168| 
   169|     # UI-name fallback (English UI labels)
   170|     for nm in (ui_names or []):
   171|         try:
   172|             p = elem.LookupParameter(nm)
   173|             if p and p.HasValue:
   174|                 return p
   175|         except:
   176|             pass
   177| 
   178|     return None
   179| 
   180| def fnum(v, nd):
   181|     return None if v is None else float(format(float(v), ".{}f".format(nd)))
   182| 
   183| def format_len_inches(feet_val):
   184|     if feet_val is None:
   185|         return None
   186|     try:
   187|         return UnitUtils.ConvertFromInternalUnits(feet_val, UnitTypeId.Inches)
   188|     except:
   189|         try:
   190|             return float(feet_val) * 12.0
   191|         except:
   192|             return None
   193| 
   194| def rgb_dict_from_color(col):
   195|     try:
   196|         return {"r": int(col.Red), "g": int(col.Green), "b": int(col.Blue)}
   197|     except:
   198|         return None
   199| 
   200| def try_get_color_rgb_from_elem(elem):
   201|     """
   202|     Returns (color_int, color_rgb)
   203|     Canonical color representation for all styles.
   204|     """
   205|     p = first_param(elem, bip_names=["TEXT_COLOR", "LINE_COLOR"], ui_names=["Color"])
   206|     color_int = _as_int(p)
   207| 
   208|     if color_int is None:
   209|         return None, None
   210| 
   211|     try:
   212|         r = (color_int      ) & 0xFF
   213|         g = (color_int >>  8) & 0xFF
   214|         b = (color_int >> 16) & 0xFF
   215|         return color_int, {"r": r, "g": g, "b": b}
   216|     except:
   217|         return color_int, None
   218| 
   219| def get_type_display_name(elem):
   220|     """
   221|     Try to get the same name you see in the Type selector:
   222|     SYMBOL_NAME_PARAM first, then .Name as fallback.
   223|     """
   224|     # 1) Type Name parameter
   225|     try:
   226|         p = elem.get_Parameter(BuiltInParameter.SYMBOL_NAME_PARAM)
   227|         if p and p.HasValue:
   228|             nm = p.AsString()
   229|             nm_c = canon_str(nm)
   230|             if nm_c:
   231|                 return nm_c
   232|     except:
   233|         pass
   234| 
   235|     # 2) Fallback to .Name
   236|     try:
   237|         nm = getattr(elem, "Name", None)
   238|         nm_c = canon_str(nm)
   239|         if nm_c:
   240|             return nm_c
   241|     except:
   242|         pass
   243| 
   244|     return None
   245| 
   246| def safe_str(x):
   247|     try:
   248|         return str(x)
   249|     except:
   250|         try:
   251|             return unicode(x)
   252|         except:
   253|             return u"<unrepr>"
   254| 
   255| def make_hash(values):
   256|     """
   257|     Deterministic hash based on a sequence of strings.
   258|     Uses .NET MD5 to avoid IronPython limitations.
   259|     """
   260|     from System.Text import Encoding
   261|     from System.Security.Cryptography import MD5
   262| 
   263|     joined = u"|".join([safe_str(v) for v in values])
   264|     data = Encoding.UTF8.GetBytes(joined)
   265|     md5 = MD5.Create()
   266|     hash_bytes = md5.ComputeHash(data)
   267|     return "".join(["{0:02x}".format(b) for b in hash_bytes])
   268| 
   269| def get_doc():
   270|     return DocumentManager.Instance.CurrentDBDocument
   271| 
   272| # ------------- fill patterns -----------------
   273| 
   274| def get_linestyles_fingerprint(doc):
   275|     info = {
   276|         "count": 0,
   277|         "raw_count": 0,
   278|         "names": [],
   279|         "records": [],
   280|         "signature_hashes": [],
   281|         "hash": None,
   282| 
   283|         # (optional) small, domain-local debug counters
   284|         "debug_fail_get_lines_cat": 0,
   285|         "debug_fail_subcats": 0,
   286|         "debug_skipped_no_name": 0,
   287|         "debug_fail_record_build": 0,
   288|     }
   289| 
   290|     # Only "Lines" category contains actual Line Styles (subcategories)
   291|     try:
   292|         lines_cat = Category.GetCategory(doc, BuiltInCategory.OST_Lines)
   293|     except:
   294|         info["debug_fail_get_lines_cat"] += 1
   295|         lines_cat = None
   296| 
   297|     if not lines_cat:
   298|         return info
   299| 
   300|     try:
   301|         subs = list(lines_cat.SubCategories)
   302|     except:
   303|         info["debug_fail_subcats"] += 1
   304|         subs = []
   305| 
   306|     info["raw_count"] = len(subs)
   307| 
   308|     records = []
   309|     names = []
   310| 
   311|     for sc in subs:
   312|         try:
   313|             sc_name = canon_str(getattr(sc, "Name", None))
   314|             if not sc_name:
   315|                 info["debug_skipped_no_name"] += 1
   316|                 continue
   317|             names.append(sc_name)
   318| 
   319|             # weights
   320|             try: w_proj = sc.GetLineWeight(GraphicsStyleType.Projection)
   321|             except: w_proj = None
   322|             try: w_cut  = sc.GetLineWeight(GraphicsStyleType.Cut)
   323|             except: w_cut = None
   324| 
   325|             # color
   326|             try:
   327|                 c = sc.LineColor
   328|                 rgb_sig = "{}-{}-{}".format(int(c.Red), int(c.Green), int(c.Blue))
   329|             except:
   330|                 rgb_sig = "<None>"
   331| 
   332|             # SINGLE line pattern field (UID) with "<None>" for invalid/solid
   333|             lp_val = "<None>"
   334|             try:
   335|                 lp_id = sc.GetLinePatternId(GraphicsStyleType.Projection)
   336|                 if lp_id and lp_id != ElementId.InvalidElementId:
   337|                     lp_elem = doc.GetElement(lp_id)
   338|                     lp_val = getattr(lp_elem, "UniqueId", None) or "<None>"
   339|             except:
   340|                 lp_val = "<None>"
   341| 
   342|             # record signature (names ARE identity here by your locked semantics)
   343|             records.append("|".join([
   344|                 safe_str(sc_name),
   345|                 safe_str(w_proj),
   346|                 safe_str(w_cut),   # kept for now (pending decision)
   347|                 safe_str(rgb_sig),
   348|                 safe_str(lp_val),
   349|             ]))
   350|         except:
   351|             info["debug_fail_record_build"] += 1
   352|             continue
   353| 
   354|     records_sorted = sorted(records)
   355|     info["records"] = records_sorted
   356|     info["names"] = sorted(set(names))
   357|     info["count"] = len(records_sorted)
   358| 
   359|     # Per-row signature hashes (metadata; NOT used in global hash)
   360|     info["signature_hashes"] = [make_hash([r]) for r in records_sorted] if records_sorted else []
   361|     
   362|     info["record_rows"] = []
   363|     if records_sorted:
   364|         sigs = info.get("signature_hashes") or []
   365|         # Defensive: if something ever goes out of sync, fail-soft by pairing "<None>"
   366|         for i, r in enumerate(records_sorted):
   367|             sh = sigs[i] if i < len(sigs) else "<None>"
   368|             info["record_rows"].append({
   369|                 "record": r,
   370|                 "sig_hash": sh,
   371|             })
   372|             
   373|     # GLOBAL hash stays EXACTLY the same semantic as before
   374|     info["hash"] = make_hash(records_sorted) if records_sorted else None
   375|     return info
   376| 
   377| # ------------- fill patterns -----------------
   378| 
```
