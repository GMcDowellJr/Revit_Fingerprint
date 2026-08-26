# Chunk of tools/probes/probe_line_patterns.py

- Source relative path: `tools/probes/probe_line_patterns.py`
- Chunk: 1 of 3
- Original line range: 1-488
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _safe, _safe_elem_name, _safe_param_def_name, _safe_get_datatype, _is_length_datatype, _is_angle_datatype, _fmt_display, _format_param_contract, _contract_from_raw, _to_inches, _lp_seg_type_id_and_name, _linepattern_signature, _maybe_set_example, _touch_param
- Source SHA-256: 09adef2ca571518818011587b6cc8376cc961624aa6e3b91af87f79082b3d74b
- Starts inside symbol: no
- Ends inside symbol: no

```
     1| # Dynamo Python (Revit) — Breadth Probe: line_patterns (INVENTORY OUTPUT)
     2| #
     3| # OUT = [
     4| #   {
     5| #     "kind": "inventory",
     6| #     "domain": "line_patterns",
     7| #     "records": param_inventory,
     8| #     "file_written": "<path>|None",        # present only if write_json=True
     9| #     "file_write_error": "<error>|None"    # present only on failure
    10| #   },
    11| #   {
    12| #     "kind": "crosswalk",
    13| #     "domain": "line_patterns",
    14| #     "records": optional_crosswalk
    15| #   }
    16| # ]
    17| #
    18| # Inputs:
    19| #   IN[0] max_patterns_to_inspect (int)
    20| #        Maximum number of LinePatternElements to inspect.
    21| #        Default: 500
    22| #
    23| #   IN[1] enable_crosswalk (bool)
    24| #        Whether to emit LineStyle → LinePattern crosswalk.
    25| #        Default: False
    26| #
    27| #   IN[2] per_segment_count_limit (int)
    28| #        Sample at most N patterns per segment_count bucket (breadth bias).
    29| #        Default: 5
    30| #
    31| #   IN[3] write_json (bool)
    32| #        When True, serialize OUT to a valid JSON file on disk.
    33| #        Default: False
    34| #
    35| #   IN[4] output_directory (str)
    36| #        Directory path where JSON will be written.
    37| #        Filename is fixed as: probes_<revit_version>_<run_id>.json
    38| #        If None, falls back to RVT directory, then TEMP.
    39| 
    40| import clr
    41| import os
    42| import json
    43| import hashlib
    44| from datetime import datetime
    45| 
    46| clr.AddReference("RevitServices")
    47| from RevitServices.Persistence import DocumentManager
    48| 
    49| clr.AddReference("RevitAPI")
    50| from Autodesk.Revit.DB import (
    51|     FilteredElementCollector, ElementId,
    52|     StorageType, UnitUtils, UnitTypeId, UnitFormatUtils,
    53|     BuiltInCategory, GraphicsStyleType,
    54|     LinePatternElement
    55| )
    56| 
    57| try:
    58|     from Autodesk.Revit.DB import SpecTypeId
    59| except:
    60|     SpecTypeId = None
    61| 
    62| doc = DocumentManager.Instance.CurrentDBDocument
    63| 
    64| max_patterns_to_inspect = IN[0] if len(IN) > 0 and IN[0] is not None else 2000
    65| enable_crosswalk = IN[1] if len(IN) > 1 and IN[1] is not None else False
    66| per_segment_count_limit = IN[2] if len(IN) > 2 and IN[2] is not None else 10
    67| write_json = IN[3] if len(IN) > 3 and IN[3] is not None else False
    68| out_path = IN[4] if len(IN) > 4 and IN[4] is not None else None
    69| 
    70| # -------------------------
    71| # Helpers (defensive)
    72| # -------------------------
    73| 
    74| def _safe(fn, default=None):
    75|     try:
    76|         return fn()
    77|     except:
    78|         return default
    79| 
    80| def _safe_elem_name(elem):
    81|     try:
    82|         n = elem.Name
    83|         return n if n else None
    84|     except:
    85|         return None
    86| 
    87| def _safe_param_def_name(p):
    88|     try:
    89|         d = p.Definition
    90|         return d.Name if d is not None else None
    91|     except:
    92|         return None
    93| 
    94| def _safe_get_datatype(p):
    95|     try:
    96|         d = p.Definition
    97|         if d is None:
    98|             return None
    99|         return d.GetDataType()
   100|     except:
   101|         return None
   102| 
   103| def _is_length_datatype(dt):
   104|     if dt is None or SpecTypeId is None:
   105|         return False
   106|     try:
   107|         return dt == SpecTypeId.Length
   108|     except:
   109|         return False
   110| 
   111| def _is_angle_datatype(dt):
   112|     if dt is None or SpecTypeId is None:
   113|         return False
   114|     try:
   115|         return dt == SpecTypeId.Angle
   116|     except:
   117|         return False
   118| 
   119| def _fmt_display(p, raw_double=None):
   120|     try:
   121|         if raw_double is not None:
   122|             dt = _safe_get_datatype(p)
   123|             if dt is not None:
   124|                 return UnitFormatUtils.Format(doc.GetUnits(), dt, raw_double, False)
   125|             return str(raw_double)
   126|         return p.AsValueString()
   127|     except:
   128|         return _safe(lambda: p.AsValueString(), None)
   129| 
   130| def _format_param_contract(p):
   131|     """
   132|     Contract:
   133|       {
   134|         "q": "ok|missing|unreadable|unsupported",
   135|         "storage": "String|Integer|Double|ElementId|None",
   136|         "raw": ...,
   137|         "display": ...,
   138|         "norm": ...
   139|       }
   140| 
   141|     Probe choice:
   142|       - Integer.norm stays integer (enum-safe).
   143|       - Length -> inches (float) when datatype is Length.
   144|       - Angle  -> degrees (float) when datatype is Angle.
   145|       - ElementId -> IntegerValue (norm=int), display tries to resolve name cheaply.
   146|     """
   147|     if p is None:
   148|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   149| 
   150|     st = _safe(lambda: p.StorageType, None)
   151|     if st is None:
   152|         return {"q": "unreadable", "storage": None, "raw": None, "display": None, "norm": None}
   153| 
   154|     if st == StorageType.String:
   155|         raw = _safe(lambda: p.AsString(), None)
   156|         return {"q": "ok", "storage": "String", "raw": raw, "display": raw, "norm": raw}
   157| 
   158|     if st == StorageType.Integer:
   159|         raw = _safe(lambda: p.AsInteger(), None)
   160|         disp = _fmt_display(p, None)
   161|         return {
   162|             "q": "ok",
   163|             "storage": "Integer",
   164|             "raw": raw,
   165|             "display": disp if disp is not None else (str(raw) if raw is not None else None),
   166|             "norm": raw
   167|         }
   168| 
   169|     if st == StorageType.Double:
   170|         raw = _safe(lambda: p.AsDouble(), None)
   171|         disp = _fmt_display(p, raw)
   172|         dt = _safe_get_datatype(p)
   173|         if raw is None:
   174|             norm = None
   175|         elif _is_length_datatype(dt):
   176|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Inches), raw)
   177|         elif _is_angle_datatype(dt):
   178|             norm = _safe(lambda: UnitUtils.ConvertFromInternalUnits(raw, UnitTypeId.Degrees), raw)
   179|         else:
   180|             norm = raw
   181|         return {"q": "ok", "storage": "Double", "raw": raw, "display": disp, "norm": norm}
   182| 
   183|     if st == StorageType.ElementId:
   184|         eid = _safe(lambda: p.AsElementId(), None)
   185|         if eid is None or eid == ElementId.InvalidElementId:
   186|             return {"q": "ok", "storage": "ElementId", "raw": None, "display": None, "norm": None}
   187| 
   188|         raw = _safe(lambda: eid.IntegerValue, None)
   189|         ref_name = None
   190|         ref = _safe(lambda: doc.GetElement(eid), None)
   191|         if ref is not None:
   192|             ref_name = _safe(lambda: ref.Name, None)
   193| 
   194|         return {
   195|             "q": "ok",
   196|             "storage": "ElementId",
   197|             "raw": raw,
   198|             "display": ref_name if ref_name is not None else (str(raw) if raw is not None else None),
   199|             "norm": raw
   200|         }
   201| 
   202|     return {"q": "unsupported", "storage": str(st), "raw": None, "display": None, "norm": None}
   203| 
   204| def _contract_from_raw(q, storage, raw, display, norm):
   205|     return {"q": q, "storage": storage, "raw": raw, "display": display, "norm": norm}
   206| 
   207| def _to_inches(val_internal):
   208|     if val_internal is None:
   209|         return None
   210|     return _safe(lambda: UnitUtils.ConvertFromInternalUnits(val_internal, UnitTypeId.Inches), val_internal)
   211| 
   212| # Canonical mapping observed in Dynamo output / extractor:
   213| # 0 = Dash, 1 = Space, 2 = Dot
   214| _LP_SEG_TYPE_NAME = {0: "Dash", 1: "Space", 2: "Dot"}
   215| 
   216| def _lp_seg_type_id_and_name(seg):
   217|     """
   218|     Robustly read a line pattern segment type across API surfaces.
   219| 
   220|     Preferred property in many Dynamo/Revit contexts: LinePatternSegment.Type
   221|     Fallback: SegmentType
   222| 
   223|     Returns: (type_id:int|None, type_name:str|None)
   224|     """
   225|     st = None
   226|     try:
   227|         if hasattr(seg, "Type"):
   228|             st = getattr(seg, "Type", None)
   229|     except Exception:
   230|         st = None
   231| 
   232|     if st is None:
   233|         try:
   234|             if hasattr(seg, "SegmentType"):
   235|                 st = getattr(seg, "SegmentType", None)
   236|         except Exception:
   237|             st = None
   238| 
   239|     if st is None:
   240|         return None, None
   241| 
   242|     try:
   243|         st_id = int(st)
   244|     except Exception:
   245|         return None, None
   246| 
   247|     return st_id, _LP_SEG_TYPE_NAME.get(st_id, "Unknown")
   248| 
   249| def _linepattern_signature(lp):
   250|     """
   251|     Build a stable (probe-local) signature for a LinePattern:
   252|       - segment sequence (type_id, length_in) order-sensitive
   253|       - md5 of that sequence string
   254| 
   255|     meta:
   256|       - access: ok | lp_none | segments_none | segments_throw
   257|       - bucket: "<seg_count>|solid=<bool>" OR "lp=None" OR "segments=None" OR "segments:throw"
   258|       - seq: list[str] | None
   259|     """
   260|     if lp is None:
   261|         return (None, None, None, {"access": "lp_none", "bucket": "lp=None", "seq": None})
   262| 
   263|     # Read segments across API surfaces
   264|     segs = None
   265|     try:
   266|         if hasattr(lp, "GetSegments"):
   267|             segs = list(lp.GetSegments() or [])
   268|         else:
   269|             segs = list(getattr(lp, "Segments", None) or [])
   270|     except Exception:
   271|         return (None, None, None, {"access": "segments_throw", "bucket": "segments:throw", "seq": None})
   272| 
   273|     if segs is None:
   274|         return (None, None, None, {"access": "segments_none", "bucket": "segments=None", "seq": None})
   275| 
   276|     seq = []
   277|     for idx, s in enumerate(segs):
   278|         st_id, _st_name = _lp_seg_type_id_and_name(s)
   279| 
   280|         # length
   281|         try:
   282|             slen = getattr(s, "Length", None)
   283|         except Exception:
   284|             slen = None
   285| 
   286|         # Normalize Dot length to 0.0 for stability (matches production extractor)
   287|         if st_id == 2:
   288|             slen = 0.0
   289| 
   290|         slen_in = _to_inches(slen) if slen is not None else None
   291| 
   292|         # token (fixed precision for hashing)
   293|         if st_id is None:
   294|             kind_tok = "None"
   295|         else:
   296|             kind_tok = str(int(st_id))
   297| 
   298|         if slen_in is None:
   299|             tok = "seg[{:03d}].kind={};len=None".format(idx, kind_tok)
   300|         else:
   301|             tok = "seg[{:03d}].kind={};len={:.6f}".format(idx, kind_tok, float(slen_in))
   302| 
   303|         seq.append(tok)
   304| 
   305|     seq_str = "|".join(seq)
   306| 
   307|     try:
   308|         h = hashlib.md5(seq_str.encode("utf-8")).hexdigest()
   309|     except Exception:
   310|         h = None
   311| 
   312|     seg_count = len(seq)
   313|     is_solid = True if seg_count == 0 else False
   314|     bucket = "{}|solid={}".format(seg_count, is_solid)
   315| 
   316|     return (seg_count, is_solid, h, {"access": "ok", "bucket": bucket, "seq": seq})
   317| 
   318| # -------------------------
   319| # Discovery + Sampling
   320| # -------------------------
   321| 
   322| all_patterns = _safe(
   323|     lambda: (FilteredElementCollector(doc)
   324|              .OfClass(LinePatternElement)
   325|              .ToElements()),
   326|     default=[]
   327| )
   328| 
   329| try:
   330|     all_patterns = list(all_patterns)
   331| except:
   332|     all_patterns = list(all_patterns)
   333| 
   334| # Cap AFTER collection
   335| try:
   336|     max_n = int(max_patterns_to_inspect)
   337|     if max_n >= 0:
   338|         all_patterns = all_patterns[:max_n]
   339| except:
   340|     pass
   341| 
   342| # Breadth-biased sampling: cap per segment-count bucket
   343| selected = []
   344| by_bucket = {}  # bucket_key -> count
   345| for e in all_patterns:
   346|     # Robust LP acquisition (sampling stage)
   347|     lp = None
   348|     try:
   349|         lp = e.GetLinePattern()
   350|     except Exception:
   351|         lp = None
   352| 
   353|     if lp is None:
   354|         try:
   355|             lp = LinePatternElement.GetLinePattern(doc, e.Id)
   356|         except Exception:
   357|             lp = None
   358| 
   359|     seg_count, is_solid, h, meta = _linepattern_signature(lp)
   360|     bucket_key = meta.get("bucket") if meta else "unknown"
   361|     c = by_bucket.get(bucket_key, 0)
   362| 
   363|     if per_segment_count_limit is None:
   364|         ok = True
   365|     else:
   366|         try:
   367|             ok = c < int(per_segment_count_limit)
   368|         except:
   369|             ok = c < 5
   370| 
   371|     if ok:
   372|         selected.append(e)
   373|         by_bucket[bucket_key] = c + 1
   374| 
   375| # If limit is 0/negative, fallback to at least 1 per bucket
   376| if len(selected) == 0 and len(all_patterns) > 0:
   377|     seen = set()
   378|     for e in all_patterns:
   379|         lp = _safe(lambda: e.GetLinePattern(), None)
   380|         _, _, _, meta = _linepattern_signature(lp)
   381|         bucket_key = meta.get("bucket") if meta else "unknown"
   382|         if bucket_key not in seen:
   383|             selected.append(e)
   384|             seen.add(bucket_key)
   385| 
   386| # Ensure at least one solid bucket is represented if present
   387| try:
   388|     want_bucket = "0|solid=True"
   389|     have_solid = False
   390|     for e in selected:
   391|         lp = None
   392|         try:
   393|             lp = e.GetLinePattern()
   394|         except Exception:
   395|             lp = None
   396|         if lp is None:
   397|             try:
   398|                 lp = LinePatternElement.GetLinePattern(doc, e.Id)
   399|             except Exception:
   400|                 lp = None
   401|         _, _, _, meta = _linepattern_signature(lp)
   402|         b = meta.get("bucket") if meta else "unknown"
   403|         if b == want_bucket:
   404|             have_solid = True
   405|             break
   406| 
   407|     if not have_solid:
   408|         for e in all_patterns:
   409|             lp = None
   410|             try:
   411|                 lp = e.GetLinePattern()
   412|             except Exception:
   413|                 lp = None
   414|             if lp is None:
   415|                 try:
   416|                     lp = LinePatternElement.GetLinePattern(doc, e.Id)
   417|                 except Exception:
   418|                     lp = None
   419|             _, _, _, meta = _linepattern_signature(lp)
   420|             b = meta.get("bucket") if meta else "unknown"
   421|             if b == want_bucket:
   422|                 selected.append(e)
   423|                 break
   424| except Exception:
   425|     pass
   426| 
   427| # -------------------------
   428| # Build inventory (union over selected)
   429| # -------------------------
   430| 
   431| # param_key -> {
   432| #   storage_types: set(str),
   433| #   q_counts: dict,
   434| #   example: dict or None,
   435| #   observed_on_buckets: set(str)
   436| # }
   437| param_index = {}
   438| 
   439| def _maybe_set_example(entry, pv):
   440|     # Keep exactly one example: prefer first "ok" encountered, otherwise first non-ok.
   441|     if pv is None:
   442|         return
   443|     ex = entry.get("example")
   444|     if ex is None:
   445|         entry["example"] = {
   446|             "q": pv.get("q"),
   447|             "storage": pv.get("storage"),
   448|             "raw": pv.get("raw"),
   449|             "display": pv.get("display"),
   450|             "norm": pv.get("norm")
   451|         }
   452|         return
   453|     if ex.get("q") != "ok" and pv.get("q") == "ok":
   454|         entry["example"] = {
   455|             "q": pv.get("q"),
   456|             "storage": pv.get("storage"),
   457|             "raw": pv.get("raw"),
   458|             "display": pv.get("display"),
   459|             "norm": pv.get("norm")
   460|         }
   461| 
   462| def _touch_param(pk, pv, bucket_key):
   463|     if pk not in param_index:
   464|         param_index[pk] = {
   465|             "storage_types": set(),
   466|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   467|             "example": None,
   468|             "observed_on_buckets": set(),
   469|             "bucket_counts": {}
   470|         }
   471| 
   472|     entry = param_index[pk]
   473|     st = pv.get("storage")
   474|     q = pv.get("q") or "unreadable"
   475| 
   476|     if st:
   477|         entry["storage_types"].add(st)
   478|     if q not in entry["q_counts"]:
   479|         entry["q_counts"][q] = 0
   480|     entry["q_counts"][q] += 1
   481| 
   482|     if bucket_key is not None:
   483|         entry["observed_on_buckets"].add(bucket_key)
   484|         bc = entry.get("bucket_counts") or {}
   485|         bc[bucket_key] = bc.get(bucket_key, 0) + 1
   486|         entry["bucket_counts"] = bc
   487| 
   488|     _maybe_set_example(entry, pv)
```
