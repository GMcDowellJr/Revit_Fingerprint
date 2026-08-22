# Chunk of tools/probes/probe_dimension_types.py

- Source relative path: `tools/probes/probe_dimension_types.py`
- Chunk: 1 of 3
- Original line range: 1-469
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_type_name, _get_family_name_param, _get_family_name_param._normalize_param_string, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display, _format_param_contract, _shape_family_from_label, _get_dim_shape_info, _get_dim_shape_info._pack, _looks_like_dimension_type
- Source SHA-256: c9e2998c9a3c2f218a004c3c8351e8c52a44975202334b7fe5365c73fa869cc7
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: dimension_types (INVENTORY OUTPUT)
     2| #
     3| # OUT = [
     4| #   {
     5| #     "kind": "inventory",
     6| #     "domain": "dimension_types",
     7| #     "records": param_inventory,
     8| #     "file_written": "<path>|None",        # present only if write_json=True
     9| #     "file_write_error": "<error>|None"    # present only on failure
    10| #   },
    11| #   {
    12| #     "kind": "crosswalk",
    13| #     "domain": "dimension_types",
    14| #     "records": optional_crosswalk
    15| #   }
    16| # ]
    17| #
    18| # Inputs:
    19| #   IN[0] max_dim_types_to_inspect (int)
    20| #        Maximum number of DimensionType ElementTypes to inspect AFTER filtering.
    21| #        Default: 500
    22| #
    23| #   IN[1] enable_crosswalk (bool)
    24| #        Whether to emit DimensionType → Tick Mark (Arrowhead) crosswalk.
    25| #        Default: False
    26| #
    27| #   IN[2] per_shape_limit (int)
    28| #        Sample at most N DimensionTypes per Shape value (StyleType/Shape),
    29| #        to bias breadth over quantity.
    30| #        Default: 8
    31| #
    32| #   IN[3] write_json (bool)
    33| #        When True, serialize OUT to a valid JSON file on disk.
    34| #        Default: False
    35| #
    36| #   IN[4] output_directory (str)
    37| #        Directory path where JSON will be written.
    38| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    39| #        If None, falls back to RVT directory, then TEMP.
    40| 
    41| 
    42| import clr
    43| import os
    44| import json
    45| from datetime import datetime
    46| 
    47| clr.AddReference("RevitServices")
    48| from RevitServices.Persistence import DocumentManager
    49| 
    50| clr.AddReference("RevitAPI")
    51| from Autodesk.Revit.DB import (
    52|     FilteredElementCollector, ElementId, ElementType,
    53|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    54|     BuiltInParameter
    55| )
    56| 
    57| try:
    58|     from Autodesk.Revit.DB import SpecTypeId
    59| except:
    60|     SpecTypeId = None
    61| 
    62| try:
    63|     from Autodesk.Revit.DB import DimensionType
    64| except:
    65|     DimensionType = None
    66| 
    67| doc = DocumentManager.Instance.CurrentDBDocument
    68| 
    69| max_dim_types_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 500
    70| enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
    71| per_shape_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 8
    72| write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
    73| out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
    74| 
    75| # -------------------------
    76| # Helpers (defensive)
    77| # -------------------------
    78| 
    79| def _safe(fn, default=None):
    80|     try:
    81|         return fn()
    82|     except:
    83|         return default
    84| 
    85| def _safe_type_name(elem):
    86|     for bip in (BuiltInParameter.SYMBOL_NAME_PARAM, BuiltInParameter.ALL_MODEL_TYPE_NAME):
    87|         try:
    88|             p = elem.get_Parameter(bip)
    89|             if p is not None:
    90|                 s = p.AsString()
    91|                 if s:
    92|                     return s
    93|         except:
    94|             pass
    95|     try:
    96|         return elem.Name
    97|     except:
    98|         return None
    99| 
   100| def _get_family_name_param(dim_type):
   101|     """
   102|     Read the family-name parameter using the same lookup path as the extractor.
   103|     Returns the raw string value or None when the parameter is absent, unreadable,
   104|     unset, or only whitespace.
   105|     """
   106|     def _normalize_param_string(p):
   107|         if p is None:
   108|             return None
   109|         try:
   110|             if not p.HasValue:
   111|                 return None
   112|         except Exception:
   113|             return None
   114|         try:
   115|             v = p.AsString()
   116|         except Exception:
   117|             return None
   118|         if v is None:
   119|             return None
   120|         s = str(v).strip()
   121|         return s if s else None
   122| 
   123|     if dim_type is None:
   124|         return None
   125|     # Try BIP first
   126|     try:
   127|         v = _normalize_param_string(dim_type.get_Parameter(BuiltInParameter.SYMBOL_FAMILY_NAME_PARAM))
   128|         if v is not None:
   129|             return v
   130|     except Exception:
   131|         pass
   132|     # Try LookupParameter as last resort
   133|     try:
   134|         v = _normalize_param_string(dim_type.LookupParameter("Family Name"))
   135|         if v is not None:
   136|             return v
   137|     except Exception:
   138|         pass
   139|     return None
   140| 
   141| def _safe_param_def_name(p):
   142|     try:
   143|         d = p.Definition
   144|         return d.Name if d is not None else None
   145|     except:
   146|         return None
   147| 
   148| def _safe_get_datatype(p):
   149|     try:
   150|         d = p.Definition
   151|         if d is None:
   152|             return None
   153|         return d.GetDataType()
   154|     except:
   155|         return None
   156| 
   157| def _is_length_datatype(dt):
   158|     if dt is None or SpecTypeId is None:
   159|         return False
   160|     try:
   161|         return dt == SpecTypeId.Length
   162|     except:
   163|         return False
   164| 
   165| def _is_angle_datatype(dt):
   166|     if dt is None or SpecTypeId is None:
   167|         return False
   168|     try:
   169|         return dt == SpecTypeId.Angle
   170|     except:
   171|         return False
   172| 
   173| def _fmt_display(p, raw_double=None):
   174|     try:
   175|         if raw_double is not None:
   176|             dt = _safe_get_datatype(p)
   177|             if dt is not None:
   178|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   179|             return str(raw_double)
   180|         return p.AsValueString()
   181|     except:
   182|         return _safe(lambda: p.AsValueString(), None)
   183| 
   184| def _format_param_contract(p):
   185|     """
   186|     Contract:
   187|       {
   188|         "q": "ok|missing|unreadable|unsupported",
   189|         "storage": "String|Integer|Double|ElementId|None",
   190|         "raw": ...,
   191|         "display": ...,
   192|         "norm": ...
   193|       }
   194|     """
   195|     if p is None:
   196|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   197| 
   198|     st = _safe(lambda: p.StorageType, None)
   199|     if st is None:
   200|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   201| 
   202|     # Wrapper-safe check for StorageType.None (Revit enum value == 0)
   203|     is_none_storage = False
   204|     try:
   205|         is_none_storage = (int(st) == 0)
   206|     except:
   207|         try:
   208|             is_none_storage = (str(st) in ("None", "None_", "0"))
   209|         except:
   210|             is_none_storage = False
   211| 
   212|     if is_none_storage:
   213|         # StorageType.None => no primitive backing store. Often a formatting/spec object.
   214|         disp = _safe(lambda: p.AsValueString(), None)
   215| 
   216|         # Some None-storage params can still provide a display string; capture it if present.
   217|         if disp is not None and str(disp).strip() != "":
   218|             return {"q": "ok", "storage": "None", "raw": None, "display": disp, "norm": disp}
   219| 
   220|         # If no meaningful display string exists, treat as unsupported for join/semantic use.
   221|         return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   222| 
   223|     if st == StorageType.String:
   224|         raw = _safe(lambda: p.AsString(), None)
   225|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   226| 
   227|     if st == StorageType.Integer:
   228|         raw = _safe(lambda: p.AsInteger(), None)
   229|         disp = _fmt_display(p, None)
   230|         return {
   231|             "q": "ok",
   232|             "storage": "Integer",
   233|             "raw": raw,
   234|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   235|             "norm": raw
   236|         }
   237| 
   238|     if st == StorageType.Double:
   239|         raw = _safe(lambda: p.AsDouble(), None)
   240|         disp = _fmt_display(p, raw)
   241|         dt = _safe_get_datatype(p)
   242|         if raw is None:
   243|             norm = None
   244|         elif _is_length_datatype(dt):
   245|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   246|         elif _is_angle_datatype(dt):
   247|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   248|         else:
   249|             norm = raw
   250|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   251| 
   252|     if st == StorageType.ElementId:
   253|         eid = _safe(lambda: p.AsElementId(), None)
   254|         if eid is None or eid == ElementId.InvalidElementId:
   255|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   256| 
   257|         raw = _safe(lambda: eid.IntegerValue, None)
   258| 
   259|         ref_name = None
   260|         ref = _safe(lambda: doc.GetElement(eid), None)
   261|         if ref is not None:
   262|             ref_name = _safe(lambda: ref.Name, None)
   263|             if ref_name is None:
   264|                 ref_name = _safe(lambda: _safe_type_name(ref), None)
   265| 
   266|         display = ref_name if ref_name is not None else (str(raw) if raw is not None else None)
   267| 
   268|         return {
   269|             "q": "ok",
   270|             "storage": "ElementId",
   271|             "raw": raw,
   272|             "display": display,
   273|             "norm": raw
   274|         }
   275| 
   276|     return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}
   277| 
   278| # -------------------------
   279| # Dimension shape key (breadth buckets)
   280| # -------------------------
   281| 
   282| def _shape_family_from_label(label):
   283|     if not label:
   284|         return "unknown"
   285| 
   286|     s = str(label).lower()
   287| 
   288|     # coarse but useful buckets for governance discussion
   289|     if "linear" in s or "aligned" in s:
   290|         return "linear"
   291|     if "angular" in s or "angle" in s:
   292|         return "angular"
   293|     if "radial" in s or "radius" in s:
   294|         return "radial"
   295|     if "diameter" in s:
   296|         return "diameter"
   297|     if "arc" in s:
   298|         return "arc"
   299|     if "spot" in s:
   300|         return "spot"
   301|     if "ordinate" in s:
   302|         return "ordinate"
   303| 
   304|     return "other"
   305| 
   306| 
   307| def _get_dim_shape_info(dt):
   308|     """
   309|     Returns (shape_key, shape_label, shape_family, q)
   310| 
   311|     Goal: label-based bucketing for breadth, not identity.
   312|     Prefer:
   313|       1) Enum name via System.Enum.GetName(type, value)
   314|       2) v.ToString()
   315|       3) A parameter whose display string looks like a dimension style/shape name
   316|     """
   317|     if dt is None:
   318|         return ("missing", None, "unknown", "missing")
   319| 
   320|     # local helper: turn (label, raw_int) into tuple
   321|     def _pack(label, raw_int, q):
   322|         fam = _shape_family_from_label(label)
   323|         sk = "{}|{}".format(label if label else "unknown", raw_int if raw_int is not None else "na")
   324|         return (sk, label, fam, q)
   325| 
   326|     # Try properties first
   327|     for attr in ("StyleType", "Shape", "DimensionShape", "DimensionStyleType"):
   328|         try:
   329|             if not hasattr(dt, attr):
   330|                 continue
   331| 
   332|             v = getattr(dt, attr, None)
   333|             if v is None:
   334|                 continue
   335| 
   336|             raw_int = None
   337|             try:
   338|                 raw_int = int(v)
   339|             except:
   340|                 raw_int = None
   341| 
   342|             label = None
   343| 
   344|             # Best: resolve enum name using the DECLARED property type (reflection),
   345|             # not v.GetType() (which may be int/boxed in some bindings).
   346|             try:
   347|                 import System
   348| 
   349|                 dt_type = dt.GetType()
   350|                 prop = dt_type.GetProperty(attr)
   351|                 if prop is not None:
   352|                     prop_type = prop.PropertyType  # should be an enum type when applicable
   353|                     try:
   354|                         if prop_type is not None and prop_type.IsEnum:
   355|                             nm = System.Enum.GetName(prop_type, v)
   356|                             if nm:
   357|                                 label = nm
   358|                     except:
   359|                         pass
   360|             except:
   361|                 pass
   362| 
   363|             # Next: ToString()
   364|             if not label:
   365|                 try:
   366|                     label = v.ToString()
   367|                 except:
   368|                     label = None
   369| 
   370|             # If we got anything usable, return it.
   371|             if label or raw_int is not None:
   372|                 return _pack(label, raw_int, "ok")
   373| 
   374|         except:
   375|             continue
   376| 
   377|     # Fallback: scan parameters for something that *looks* like style/shape name
   378|     params = _safe(lambda: list(dt.GetOrderedParameters()), default=None)
   379|     if params is None:
   380|         params = _safe(lambda: list(dt.Parameters), default=[])
   381| 
   382|     best_disp = None
   383|     best_raw = None
   384|     best_q = "missing"
   385| 
   386|     for p in params:
   387|         dn = _safe(lambda: _safe_param_def_name(p), None)
   388|         if not dn:
   389|             continue
   390| 
   391|         dn_l = dn.lower()
   392| 
   393|         # Heuristic: candidates likely to carry shape/style names
   394|         if ("style" not in dn_l) and ("shape" not in dn_l) and ("dimension" not in dn_l):
   395|             continue
   396| 
   397|         pv = _format_param_contract(p)
   398|         disp = pv.get("display")
   399|         raw = pv.get("raw")
   400| 
   401|         if pv.get("q") != "ok":
   402|             continue
   403| 
   404|         # Prefer a display string that isn't purely numeric
   405|         if disp and not str(disp).strip().lstrip("-").isdigit():
   406|             best_disp = disp
   407|             best_raw = raw
   408|             best_q = "ok"
   409|             break
   410| 
   411|     if best_disp is not None or best_raw is not None:
   412|         try:
   413|             raw_int = int(best_raw) if best_raw is not None else None
   414|         except:
   415|             raw_int = None
   416|         return _pack(best_disp, raw_int, best_q)
   417| 
   418|     return ("missing", None, "unknown", "missing")
   419| 
   420| # -------------------------
   421| # Progressive Discovery
   422| # -------------------------
   423| 
   424| # Step 1 (preferred): class-based, category-free collector for DimensionType
   425| dim_types = []
   426| if DimensionType is not None:
   427|     dim_types = _safe(
   428|         lambda: (FilteredElementCollector(doc)
   429|                  .WhereElementIsElementType()
   430|                  .OfClass(DimensionType)
   431|                  .ToElements()),
   432|         default=[]
   433|     )
   434| 
   435| try:
   436|     dim_types = list(dim_types)
   437| except:
   438|     dim_types = list(dim_types) if dim_types is not None else []
   439| 
   440| 
   441| # Step 2 (fallback): parameter-signature discovery across ElementType
   442| DIM_TYPE_SIGNATURE_PARAMS = [
   443|     # tick/arrowhead is a common anchor across many dimension shapes
   444|     "Tick Mark",
   445|     "Tick mark",
   446|     "Tick Mark Type",
   447|     "Tick Mark Symbol",
   448|     # text appearance is also common
   449|     "Text Size",
   450|     "Text Font",
   451|     "Text",
   452|     # witness/line properties often exist on linear styles
   453|     "Witness Line Control",
   454|     "Dimension Line",
   455| ]
   456| 
   457| def _looks_like_dimension_type(t):
   458|     # NOTE: this is ONLY used when class-based collection fails.
   459|     if t is None:
   460|         return False
   461|     hits = 0
   462|     try:
   463|         for pn in DIM_TYPE_SIGNATURE_PARAMS:
   464|             if t.LookupParameter(pn) is not None:
   465|                 hits += 1
   466|         # Require multiple hits to avoid false positives
   467|         return hits >= 3
   468|     except:
   469|         return False
```
