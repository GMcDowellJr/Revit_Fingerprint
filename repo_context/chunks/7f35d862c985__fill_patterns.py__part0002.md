# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 2 of 8
- Original line range: 133-532
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_drafting, extract_drafting.f, extract_drafting.grid_sig, extract_drafting.grid_sig.add_float, extract_drafting.grid_sig.add_origin_2d, extract_drafting._bump_v2_reason, extract_drafting._grid_sig_v2, extract_drafting._grid_sig_v2.req_float, extract_drafting._grid_sig_v2.req_origin, extract_drafting._phase2_try_get_grid, extract_drafting._phase2_add_float, extract_drafting._phase2_add_int, extract_drafting._phase2_add_bool, extract_drafting._phase2_add_str, extract_drafting._phase2_build_phase2
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: no
- Ends inside symbol: extract_drafting

```
   133| def extract_drafting(doc, ctx=None):
   134|     _TARGET_INT = _TARGET_DRAFTING_INT
   135|     _TARGET_NAME = "Drafting"
   136|     DOMAIN_NAME = "fill_patterns_drafting"
   137|     """
   138|     Extract Fill Patterns fingerprint from document.
   139| 
   140|     Args:
   141|         doc: Revit Document
   142|         ctx: Context dictionary (unused for this domain)
   143| 
   144|     Returns:
   145|         Dictionary with count, hash, signature_hashes, records,
   146|         record_rows, and debug counters
   147|     """
   148|     info = {
   149|         "count": 0,
   150|         "raw_count": 0,
   151|         "names": [],
   152|                 "records": [],
   153| 
   154|         # debug counters so you can see why things disappear
   155|         "debug_total_elements": 0,
   156|         "debug_kept": 0,
   157|         "debug_skipped_no_name": 0,
   158|         "debug_skipped_wrong_target": 0,
   159|         "debug_fail_getfillpattern": 0,
   160|         "debug_fail_grid_read": 0,
   161| 
   162|         # v2 (contract semantic) surfaces - additive only
   163|         "hash_v2": None,
   164|         "signature_hashes_v2": [],
   165|         "debug_v2_blocked": 0,
   166|         "debug_v2_block_reasons": {},
   167|     }
   168| 
   169|     try:
   170|         col = _collect_fill_patterns(doc, ctx)
   171|     except Exception as e:
   172|         return info
   173|     info["raw_count"] = len(col)
   174| 
   175|     def f(v, nd=9):
   176|         if v is None:
   177|             return S_MISSING
   178|         try:
   179|             return format(float(v), ".{}f".format(nd))
   180|         except Exception as e:
   181|             return canon_str(v)
   182| 
   183|     def grid_sig(fp, i):
   184|         # Return a stable list; never raise
   185|         idx = "{:03d}".format(int(i))
   186|         g = None
   187|         try:
   188|             if hasattr(fp, "GetFillPatternGrid"):
   189|                 g = fp.GetFillPatternGrid(i)
   190|         except Exception as e:
   191|             g = None
   192|         if g is None:
   193|             try:
   194|                 if hasattr(fp, "GetFillGrid"):
   195|                     g = fp.GetFillGrid(i)
   196|             except Exception as e:
   197|                 g = None
   198| 
   199|         if g is None:
   200|             info["debug_fail_grid_read"] += 1
   201|             return ["grid[{}].unreadable={}".format(idx, S_MISSING)]
   202| 
   203|         parts = []
   204| 
   205|         def add_float(prop_name, key):
   206|             try:
   207|                 v = getattr(g, prop_name)
   208|                 parts.append("grid[{}].{}={}".format(idx, key, f(v)))
   209|             except Exception as e:
   210|                 parts.append("grid[{}].{}={}".format(idx, key, S_MISSING))
   211| 
   212|         # origin can vary across versions; try a couple shapes
   213|         def add_origin_2d():
   214|             # Try UV-style origin (U,V)
   215|             try:
   216|                 o = g.Origin
   217|                 u = getattr(o, "U", None)
   218|                 v = getattr(o, "V", None)
   219|                 if u is not None and v is not None:
   220|                     parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
   221|                     return
   222|             except Exception as e:
   223|                 pass
   224| 
   225|             # Try XYZ-style origin but store only X,Y
   226|             try:
   227|                 o = g.Origin
   228|                 x = getattr(o, "X", None)
   229|                 y = getattr(o, "Y", None)
   230|                 if x is not None and y is not None:
   231|                     parts.append("grid[{}].origin_xy={},{}".format(idx, f(x), f(y)))
   232|                     return
   233|             except Exception as e:
   234|                 pass
   235| 
   236|             # Try separate scalars
   237|             for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
   238|                 try:
   239|                     u = getattr(g, u_name)
   240|                     v = getattr(g, v_name)
   241|                     parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
   242|                     return
   243|                 except Exception as e:
   244|                     pass
   245| 
   246|             parts.append("grid[{}].origin={}".format(idx, S_MISSING))
   247| 
   248|         add_float("Angle", "angle")
   249|         add_origin_2d()
   250|         add_float("Offset", "offset")
   251|         add_float("Shift", "shift")
   252| 
   253|         return parts
   254| 
   255|     # v2 helpers (strict: block on unreadables / missing)
   256|     def _bump_v2_reason(reason):
   257|         info["debug_v2_blocked"] += 1
   258|         try:
   259|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
   260|         except Exception as e:
   261|             pass
   262| 
   263|     def _grid_sig_v2(fp, i):
   264|         """
   265|         Return (ok, parts, reason). parts contain only numeric primitives.
   266|         """
   267|         idx = "{:03d}".format(int(i))
   268|         g = None
   269|         try:
   270|             if hasattr(fp, "GetFillPatternGrid"):
   271|                 g = fp.GetFillPatternGrid(i)
   272|         except Exception as e:
   273|             g = None
   274|         if g is None:
   275|             try:
   276|                 if hasattr(fp, "GetFillGrid"):
   277|                     g = fp.GetFillGrid(i)
   278|             except Exception as e:
   279|                 g = None
   280| 
   281|         if g is None:
   282|             return False, [], "grid_unreadable"
   283| 
   284|         parts = []
   285| 
   286|         def req_float(prop_name, key):
   287|             try:
   288|                 v = getattr(g, prop_name)
   289|             except Exception as e:
   290|                 return False, "grid_{}_unreadable".format(key)
   291|             if v is None:
   292|                 return False, "grid_{}_none".format(key)
   293|             try:
   294|                 fv = float(v)
   295|             except Exception as e:
   296|                 return False, "grid_{}_not_float".format(key)
   297|             parts.append("grid[{}].{}={}".format(idx, key, canon_str(f(v, 9))))
   298|             return True, None
   299| 
   300|         # origin: require 2 floats, pick first supported shape
   301|         def req_origin():
   302|             # UV origin
   303|             try:
   304|                 o = g.Origin
   305|                 u = getattr(o, "U", None)
   306|                 v = getattr(o, "V", None)
   307|                 if u is not None and v is not None:
   308|                     fu = float(u)
   309|                     fv = float(v)
   310|                     parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
   311|                     return True, None
   312|             except Exception as e:
   313|                 pass
   314| 
   315|             # XY origin
   316|             try:
   317|                 o = g.Origin
   318|                 x = getattr(o, "X", None)
   319|                 y = getattr(o, "Y", None)
   320|                 if x is not None and y is not None:
   321|                     fx = float(x)
   322|                     fy = float(y)
   323|                     parts.append("grid[{}].origin_xy={},{}".format(idx, canon_str(f(fx, 9)), canon_str(f(fy, 9))))
   324|                     return True, None
   325|             except Exception as e:
   326|                 pass
   327| 
   328|             # scalar origin props
   329|             for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
   330|                 try:
   331|                     u = getattr(g, u_name)
   332|                     v = getattr(g, v_name)
   333|                     if u is None or v is None:
   334|                         continue
   335|                     fu = float(u)
   336|                     fv = float(v)
   337|                     parts.append("grid[{}].origin_uv={},{}".format(idx, canon_str(f(fu, 9)), canon_str(f(fv, 9))))
   338|                     return True, None
   339|                 except Exception as e:
   340|                     continue
   341| 
   342|             return False, "grid_origin_unreadable"
   343| 
   344|         ok, reason = req_float("Angle", "angle")
   345|         if not ok:
   346|             return False, [], reason
   347|         ok, reason = req_origin()
   348|         if not ok:
   349|             return False, [], reason
   350|         ok, reason = req_float("Offset", "offset")
   351|         if not ok:
   352|             return False, [], reason
   353|         ok, reason = req_float("Shift", "shift")
   354|         if not ok:
   355|             return False, [], reason
   356| 
   357|         return True, parts, None
   358| 
   359|     # -------------------------
   360|     # Phase 2 (additive-only) builders
   361|     # -------------------------
   362| 
   363|     def _phase2_try_get_grid(fp, i):
   364|         g = None
   365|         try:
   366|             if hasattr(fp, "GetFillPatternGrid"):
   367|                 g = fp.GetFillPatternGrid(i)
   368|         except Exception:
   369|             g = None
   370|         if g is None:
   371|             try:
   372|                 if hasattr(fp, "GetFillGrid"):
   373|                     g = fp.GetFillGrid(i)
   374|             except Exception:
   375|                 g = None
   376|         return g
   377| 
   378|     def _phase2_add_float(items, k, v, *, unreadable=False):
   379|         if unreadable:
   380|             items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
   381|             return
   382|         v2, q2 = canonicalize_float(v)
   383|         items.append({"k": k, "v": v2, "q": q2})
   384| 
   385|     def _phase2_add_int(items, k, v, *, unreadable=False):
   386|         if unreadable:
   387|             items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
   388|             return
   389|         v2, q2 = canonicalize_int(v)
   390|         items.append({"k": k, "v": v2, "q": q2})
   391| 
   392|     def _phase2_add_bool(items, k, v, *, unreadable=False):
   393|         if unreadable:
   394|             items.append({"k": k, "v": None, "q": ITEM_Q_UNREADABLE})
   395|             return
   396|         v2, q2 = canonicalize_bool(v)
   397|         items.append({"k": k, "v": v2, "q": q2})
   398| 
   399|     def _phase2_add_str(items, k, v, *, allow_empty=False):
   400|         if allow_empty:
   401|             v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=True)
   402|         else:
   403|             v2, q2 = phase2_qv_from_legacy_sentinel_str(v, allow_empty=False)
   404|         items.append({"k": k, "v": v2, "q": q2})
   405| 
   406|     def _phase2_build_phase2(name, uid, elem_id_str, fp, elem):
   407|         semantic = []
   408|         cosmetic = []
   409|         coordination = []
   410|         unknown = []
   411| 
   412|         # cosmetic
   413|         v_name, q_name = phase2_qv_from_legacy_sentinel_str(name, allow_empty=False)
   414|         cosmetic.append({"k": "fill_pattern.name", "v": v_name, "q": q_name})
   415| 
   416|         # unknown identifiers (do not affect semantic hypotheses)
   417|         v_uid, q_uid = canonicalize_str(uid)
   418|         unknown.append({"k": "fill_pattern.uid", "v": v_uid, "q": q_uid})
   419|         v_eid, q_eid = canonicalize_str(elem_id_str)
   420|         unknown.append({"k": "fill_pattern.elem_id", "v": v_eid, "q": q_eid})
   421| 
   422|         # Traceability fields (metadata only — never in hash/sig/join)
   423|         try:
   424|             _eid_raw = getattr(getattr(elem, "Id", None), "IntegerValue", None)
   425|             _eid_v, _eid_q = canonicalize_int(_eid_raw)
   426|         except Exception:
   427|             _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
   428|         try:
   429|             _uid_raw = getattr(elem, "UniqueId", None)
   430|             _uid_v, _uid_q = canonicalize_str(_uid_raw)
   431|         except Exception:
   432|             _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
   433|         unknown.append({"k": "fill_pattern.source_element_id", "v": _eid_v, "q": _eid_q})
   434|         unknown.append({"k": "fill_pattern.source_unique_id", "v": _uid_v, "q": _uid_q})
   435| 
   436|         # target is always _TARGET_NAME for this domain
   437|         semantic.append({"k": "fill_pattern.target", "v": _TARGET_NAME, "q": ITEM_Q_OK})
   438| 
   439|         if fp is None:
   440|             # Explicit unreadable (GetFillPattern failed)
   441|             _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
   442|             # is_solid in coordination only (filter criterion, not identity)
   443|             _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
   444|             coordination.append(make_identity_item("fill_pattern.is_import", None, ITEM_Q_UNREADABLE))
   445|         else:
   446|             # is_solid goes to coordination_items only — it is a filter criterion, not identity
   447|             try:
   448|                 is_solid = fp.IsSolidFill
   449|             except Exception:
   450|                 _phase2_add_bool(coordination, "fill_pattern.is_solid", None, unreadable=True)
   451|             else:
   452|                 _phase2_add_bool(coordination, "fill_pattern.is_solid", bool(is_solid))
   453| 
   454|             is_import_v, is_import_q = _phase2_fill_pattern_is_import(elem, name)
   455|             coordination.append(make_identity_item("fill_pattern.is_import", is_import_v, is_import_q))
   456| 
   457|             # grid_count
   458|             try:
   459|                 gc = fp.GridCount
   460|             except Exception:
   461|                 _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
   462|                 gc_i = None
   463|             else:
   464|                 if gc is None:
   465|                     _phase2_add_int(semantic, "fill_pattern.grid_count", None)
   466|                     gc_i = None
   467|                 else:
   468|                     try:
   469|                         gc_i = int(gc)
   470|                     except Exception:
   471|                         _phase2_add_int(semantic, "fill_pattern.grid_count", None, unreadable=True)
   472|                         gc_i = None
   473|                     else:
   474|                         _phase2_add_int(semantic, "fill_pattern.grid_count", gc_i)
   475| 
   476|             # grids (no inference; explicit kind for origin)
   477|             if gc_i:
   478|                 for i in range(int(gc_i)):
   479|                     idx = "{:03d}".format(int(i))
   480|                     g = _phase2_try_get_grid(fp, i)
   481|                     if g is None:
   482|                         semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   483|                         semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   484|                         semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   485|                         semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   486|                         continue
   487| 
   488|                     # Angle / Offset / Shift
   489|                     try:
   490|                         _phase2_add_float(semantic, "fill_pattern.grid[{}].angle".format(idx), float(getattr(g, "Angle")))
   491|                     except Exception:
   492|                         semantic.append({"k": "fill_pattern.grid[{}].angle".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   493| 
   494|                     # Origin (explicit kind)
   495|                     origin_kind = None
   496|                     ox = oy = None
   497| 
   498|                     # UV origin
   499|                     try:
   500|                         o = g.Origin
   501|                         u = getattr(o, "U", None)
   502|                         v = getattr(o, "V", None)
   503|                         if u is not None and v is not None:
   504|                             origin_kind = "uv"
   505|                             ox = float(u)
   506|                             oy = float(v)
   507|                     except Exception:
   508|                         pass
   509| 
   510|                     # XY origin
   511|                     if origin_kind is None:
   512|                         try:
   513|                             o = g.Origin
   514|                             x = getattr(o, "X", None)
   515|                             y = getattr(o, "Y", None)
   516|                             if x is not None and y is not None:
   517|                                 origin_kind = "xy"
   518|                                 ox = float(x)
   519|                                 oy = float(y)
   520|                         except Exception:
   521|                             pass
   522| 
   523|                     # Scalar origin props
   524|                     if origin_kind is None:
   525|                         for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
   526|                             try:
   527|                                 u2 = getattr(g, u_name)
   528|                                 v2 = getattr(g, v_name)
   529|                                 if u2 is None or v2 is None:
   530|                                     continue
   531|                                 origin_kind = "uv"
   532|                                 ox = float(u2)
```
