# Chunk of legacy/fingerprint_mvp.py

- Source relative path: `legacy/fingerprint_mvp.py`
- Chunk: 2 of 3
- Original line range: 379-883
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: get_fillpattern_fingerprint, get_fillpattern_fingerprint.f, get_fillpattern_fingerprint.read_is_model, get_fillpattern_fingerprint.grid_sig, get_fillpattern_fingerprint.grid_sig.add_float, get_fillpattern_fingerprint.grid_sig.add_origin_2d, get_identity_fingerprint, get_units_fingerprint, get_objectstyles_fingerprint, get_objectstyles_fingerprint.row_sig
- Source SHA-256: 2b3c4e30443f4500e886e1f968d5ea1da344bf6c9fb01c0de3bb148cfc1b7332
- Starts inside symbol: no
- Ends inside symbol: no

```
   379| def get_fillpattern_fingerprint(doc):
   380|     info = {
   381|         "count": 0,
   382|         "raw_count": 0,
   383|         "names": [],
   384|         "signature_hashes": [],
   385|         "hash": None,
   386|         "records": [],
   387|         # debug counters so you can see why things disappear
   388|         "debug_total_elements": 0,
   389|         "debug_kept": 0,
   390|         "debug_skipped_no_name": 0,
   391|         "debug_fail_getfillpattern": 0,
   392|         "debug_fail_grid_read": 0,
   393|     }
   394|     
   395| 
   396|     try:
   397|         col = list(FilteredElementCollector(doc).OfClass(FillPatternElement))
   398|     except:
   399|         return info
   400|     info["raw_count"] = len(col)
   401| 
   402|     def f(v, nd=9):
   403|         if v is None:
   404|             return "<None>"
   405|         try:
   406|             return format(float(v), ".{}f".format(nd))
   407|         except:
   408|             return sig_val(v)
   409| 
   410|     def read_is_model(fp, target):
   411|         # Prefer explicit property, else infer from target when possible
   412|         is_model = None
   413|         for attr in ["IsModelFillPattern", "IsModel", "IsModelFill"]:
   414|             try:
   415|                 if hasattr(fp, attr):
   416|                     is_model = getattr(fp, attr)
   417|                     break
   418|             except:
   419|                 pass
   420|         if is_model is None:
   421|             try:
   422|                 if target is not None:
   423|                     is_model = (int(target) == 1)  # Drafting=0, Model=1 in many builds
   424|             except:
   425|                 pass
   426|         return is_model
   427| 
   428|     def grid_sig(fp, i):
   429|         # Return a stable list; never raise
   430|         idx = "{:03d}".format(int(i))
   431|         g = None
   432|         try:
   433|             if hasattr(fp, "GetFillPatternGrid"):
   434|                 g = fp.GetFillPatternGrid(i)
   435|         except:
   436|             g = None
   437|         if g is None:
   438|             try:
   439|                 if hasattr(fp, "GetFillGrid"):
   440|                     g = fp.GetFillGrid(i)
   441|             except:
   442|                 g = None
   443| 
   444|         if g is None:
   445|             info["debug_fail_grid_read"] += 1
   446|             return ["grid[{}].unreadable=<None>".format(idx)]
   447| 
   448|         parts = []
   449| 
   450|         def add_float(prop_name, key):
   451|             try:
   452|                 v = getattr(g, prop_name)
   453|                 parts.append("grid[{}].{}={}".format(idx, key, f(v)))
   454|             except:
   455|                 parts.append("grid[{}].{}=<None>".format(idx, key))
   456| 
   457|         # origin can vary across versions; try a couple shapes
   458|         def add_origin_2d():
   459|             # Try UV-style origin (U,V)
   460|             try:
   461|                 o = g.Origin
   462|                 u = getattr(o, "U", None)
   463|                 v = getattr(o, "V", None)
   464|                 if u is not None and v is not None:
   465|                     parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
   466|                     return
   467|             except:
   468|                 pass
   469| 
   470|             # Try XYZ-style origin but store only X,Y
   471|             try:
   472|                 o = g.Origin
   473|                 x = getattr(o, "X", None)
   474|                 y = getattr(o, "Y", None)
   475|                 if x is not None and y is not None:
   476|                     parts.append("grid[{}].origin_xy={},{}".format(idx, f(x), f(y)))
   477|                     return
   478|             except:
   479|                 pass
   480| 
   481|             # Try separate scalars
   482|             for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
   483|                 try:
   484|                     u = getattr(g, u_name)
   485|                     v = getattr(g, v_name)
   486|                     parts.append("grid[{}].origin_uv={},{}".format(idx, f(u), f(v)))
   487|                     return
   488|                 except:
   489|                     pass
   490| 
   491|             parts.append("grid[{}].origin=<None>".format(idx))
   492| 
   493|         add_float("Angle", "angle")
   494|         add_origin_2d()
   495|         add_float("Offset", "offset")
   496|         add_float("Shift", "shift")
   497| 
   498|         return parts
   499| 
   500|     records = []
   501|     per_hashes = []
   502|     names= []
   503| 
   504|     for e in col:
   505|         info["debug_total_elements"] += 1
   506| 
   507|         name = canon_str(getattr(e, "Name", None))
   508|         if not name:
   509|             info["debug_skipped_no_name"] += 1
   510|             continue
   511|         names.append(name)
   512| 
   513|         # Always keep the element, even if we can't read its FillPattern
   514|         fp = None
   515|         try:
   516|             fp = e.GetFillPattern()
   517|         except:
   518|             fp = None
   519| 
   520|         if fp is None:
   521|             info["debug_fail_getfillpattern"] += 1
   522|             sig = [
   523|                 "is_solid=<None>",
   524|                 "is_model=<None>",
   525|                 "target=<None>",
   526|                 "grid_count=<None>",
   527|                 "grid[000].unreadable=<None>",
   528|                 "error=GetFillPatternFailed",
   529|             ]
   530|         else:
   531|             # Core fields
   532|             is_solid = None
   533|             try: is_solid = fp.IsSolidFill
   534|             except: pass
   535| 
   536|             target = None
   537|             try: target = fp.Target
   538|             except: pass
   539| 
   540|             is_model = read_is_model(fp, target)
   541| 
   542|             gc = None
   543|             try: gc = fp.GridCount
   544|             except: pass
   545| 
   546|             sig = [
   547|                 "is_solid={}".format(sig_val(is_solid)),
   548|                 "is_model={}".format(sig_val(is_model)),
   549|                 "target={}".format(sig_val(target)),
   550|                 "grid_count={}".format(sig_val(gc)),
   551|             ]
   552| 
   553|             # Grids (fail-soft: if grid read fails, you still keep pattern)
   554|             if gc:
   555|                 try:
   556|                     for i in range(int(gc)):
   557|                         sig.extend(grid_sig(fp, i))
   558|                 except:
   559|                     info["debug_fail_grid_read"] += 1
   560|                     sig.append("error=GridLoopFailed")
   561| 
   562|         # Keep signature deterministic
   563|         sig_sorted = sorted(sig)
   564|         def_hash = make_hash(sig_sorted)
   565| 
   566|         rec = {
   567|             "id": safe_str(e.Id.IntegerValue),
   568|             "uid":getattr(e, "UniqueId", "") or "",
   569|             "name": name,          # metadata only
   570|             "def_hash": def_hash,  # hashed definition
   571|         }
   572|         if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
   573|             rec["def_signature"] = sig_sorted
   574| 
   575|         records.append(rec)
   576|         per_hashes.append(def_hash)
   577|         info["debug_kept"] += 1
   578| 
   579|     per_hashes = sorted(per_hashes)
   580|     info["signature_hashes"] = sorted(per_hashes)
   581|     info["names"] = sorted(set(names))
   582|     info["count"] = len(info["names"])
   583|     info["hash"] = make_hash(info["signature_hashes"]) if info["signature_hashes"] else None
   584|     info["records"] = sorted(records, key=lambda r: (r.get("name",""), r.get("id","")))
   585|     
   586|     info["record_rows"] = []
   587|     try:
   588|         recs = info.get("records") or []
   589|         info["record_rows"] = [{
   590|             "record_key": safe_str(r.get("uid", "")),        # <-- UniqueId
   591|             "sig_hash":   safe_str(r.get("def_hash", "")),
   592|             "name":       safe_str(r.get("name", "")),       # optional metadata
   593|         } for r in recs]
   594|     except:
   595|         info["record_rows"] = []
   596|     
   597|     return info
   598| 
   599| # ------------- identity & context -----------------
   600| 
   601| def get_identity_fingerprint(doc):
   602|     app = doc.Application
   603|     info = {}
   604| 
   605|     info["project_title"] = safe_str(doc.Title)
   606| 
   607|     try:
   608|         if doc.IsWorkshared:
   609|             # Central path or model path
   610|             try:
   611|                 mp = WorksharingUtils.GetModelPath(doc)
   612|                 info["central_path"] = safe_str(mp.CentralServerPath)
   613|             except:
   614|                 info["central_path"] = safe_str(doc.PathName)
   615|         else:
   616|             info["central_path"] = safe_str(doc.PathName)
   617|     except:
   618|         info["central_path"] = safe_str(doc.PathName)
   619| 
   620|     info["is_workshared"] = bool(getattr(doc, "IsWorkshared", False))
   621| 
   622|     # Revit version/build
   623|     info["revit_version_number"] = safe_str(app.VersionNumber)
   624|     info["revit_version_name"]   = safe_str(app.VersionName)
   625|     info["revit_build"]          = safe_str(app.VersionBuild)
   626| 
   627|     return info
   628| 
   629| # ------------- units fingerprint (minimal, no UnitType) -----------------
   630| 
   631| def get_units_fingerprint(doc):
   632|     """
   633|     Version-safe units snapshot (Revit 2022+).
   634|     - 'repr' is the raw Units.ToString() for quick sanity.
   635|     - 'specs' holds explicit Length/Area/Volume format options.
   636|     """
   637|     result = {
   638|         "repr": None,
   639|         "specs": {},
   640|         "hash": None
   641|     }
   642| 
   643|     try:
   644|         u = doc.GetUnits()
   645|     except:
   646|         return result
   647| 
   648|     result["repr"] = safe_str(u)
   649| 
   650|     records = []
   651| 
   652|     specs = [
   653|         ("length", SpecTypeId.Length),
   654|         ("area",   SpecTypeId.Area),
   655|         ("volume", SpecTypeId.Volume)
   656|     ]
   657| 
   658|     for label, spec_id in specs:
   659|         try:
   660|             fmt = u.GetFormatOptions(spec_id)
   661|         except:
   662|             continue
   663| 
   664|         try:
   665|             unit_id   = safe_str(fmt.GetUnitTypeId())
   666|         except:
   667|             unit_id   = "<no-unit>"
   668| 
   669|         try:
   670|             symbol_id = safe_str(fmt.GetSymbolTypeId())
   671|         except:
   672|             symbol_id = "<no-symbol>"
   673| 
   674|         try:
   675|             acc = fmt.Accuracy
   676|         except:
   677|             acc = None
   678| 
   679|         rec = {
   680|             "spec": label,
   681|             "unit_id": unit_id,
   682|             "symbol_id": symbol_id,
   683|             "accuracy": acc
   684|         }
   685|         result["specs"][label] = rec
   686|         records.append("{}|{}|{}|{}".format(label, unit_id, symbol_id, acc))
   687| 
   688|     if records:
   689|         result["hash"] = make_hash(sorted(records))
   690| 
   691|     return result
   692| 
   693| # ------------- lineweights fingerprint -----------------
   694| 
   695| def get_objectstyles_fingerprint(doc):
   696|     """
   697|     Object Styles / Category graphics fingerprint (non-import categories).
   698| 
   699|     Per ROW (category + each subcategory row):
   700|       - parent category name
   701|       - row name (subcategory name or "<self>")
   702|       - CategoryType (Model, Annotation, Tag, etc.)
   703|       - Projection lineweight index
   704|       - Cut lineweight index
   705|       - Line color (RGB sig)
   706|       - Projection line pattern Id
   707|       - Cut line pattern Id
   708|       - Category material Id (if any)
   709| 
   710|     Output:
   711|       - count: number of rows
   712|       - hash: global hash of row hashes
   713|       - signature_hashes: per-row hashes (sorted)
   714|       - category_hashes: per parent category hash (row hashes under that parent)
   715|       - records: row signature strings (sorted)
   716|     """
   717|     info = {
   718|         "count": 0,
   719|         "raw_count": 0,
   720|         "names": [],
   721|         "hash": None,
   722|         "signature_hashes": [],
   723|         "category_hashes": {},
   724|         "records": [],
   725|         # debug counters
   726|         "debug_total_categories": 0,
   727|         "debug_rows_emitted": 0,
   728|         "debug_skipped_import": 0,
   729|         "debug_fail_row": 0
   730|     }
   731|     
   732|     row_pairs = []
   733|     
   734|     try:
   735|         cats = doc.Settings.Categories
   736|     except:
   737|         return info
   738| 
   739|     def row_sig(cat_obj, parent_name, row_name, cat_type):
   740|         # Projection / cut lineweights
   741|         try:
   742|             w_proj = cat_obj.GetLineWeight(GraphicsStyleType.Projection)
   743|         except:
   744|             w_proj = None
   745| 
   746|         try:
   747|             w_cut = cat_obj.GetLineWeight(GraphicsStyleType.Cut)
   748|         except:
   749|             w_cut = None
   750| 
   751|         # Line color
   752|         try:
   753|             col = cat_obj.LineColor
   754|             rgb_sig = rgb_sig_from_color(col)
   755|         except:
   756|             rgb_sig = "<None>"
   757| 
   758|         # Line pattern (Object Styles has ONE pattern, not proj/cut)
   759|         try:
   760|             lp_id = cat_obj.GetLinePatternId(GraphicsStyleType.Projection)
   761|             lp_val = "<None>"
   762|             if lp_id and lp_id.IntegerValue > 0:
   763|                 lp_e = doc.GetElement(lp_id)
   764|                 lp_val = canon_str(getattr(lp_e, "UniqueId", None)) or "<None>"
   765|         except:
   766|             lp_val = "<None>"
   767| 
   768|         # Category material Id
   769|         # Material (UID for stability)
   770|         try:
   771|             mat_id = cat_obj.Material
   772|             mat_val = "<None>"
   773|             if mat_id and mat_id.IntegerValue > 0:
   774|                 m = doc.GetElement(mat_id)
   775|                 mat_val = canon_str(getattr(m, "UniqueId", None)) or "<None>"
   776|         except:
   777|             mat_val = "<None>"
   778| 
   779|         # Deterministic row signature
   780|         return "|".join([
   781|             parent_name,
   782|             row_name,
   783|             cat_type,
   784|             safe_str(w_proj),
   785|             safe_str(w_cut),
   786|             rgb_sig,
   787|             lp_val,
   788|             mat_val
   789|         ])
   790| 
   791|     records = []
   792|     row_hashes = []
   793|     names = []
   794|     per_parent_hashes = {}  # parent_name -> [row_hash,...]
   795| 
   796|     for cat in cats:
   797|         info["debug_total_categories"] += 1
   798|         if cat is None:
   799|             continue
   800| 
   801|         # Skip import categories
   802|         try:
   803|             from Autodesk.Revit.DB import CategoryType
   804|             if cat.CategoryType == CategoryType.Import:
   805|                 info["debug_skipped_import"] += 1
   806|                 continue
   807|         except:
   808|             pass
   809| 
   810|         # Parent name
   811|         try:
   812|             parent_name = canon_str(cat.Name)
   813|         except:
   814|             continue
   815| 
   816|         # Category type
   817|         try:
   818|             cat_type = safe_str(cat.CategoryType)
   819|         except:
   820|             cat_type = "<unknown>"
   821| 
   822|         # Emit the parent row ("<self>")
   823|         try:
   824|             sig = row_sig(cat, parent_name, "<self>", cat_type)
   825|             row_key = "{}|{}".format(parent_name, "<self>")
   826|             names.append(row_key)
   827|             h = make_hash([sig])  # stable, deterministic
   828|             records.append(sig)
   829|             row_hashes.append(h)
   830|             row_pairs.append((sig, h))
   831|             per_parent_hashes.setdefault(parent_name, []).append(h)
   832|             info["debug_rows_emitted"] += 1
   833|         except:
   834|             info["debug_fail_row"] += 1
   835| 
   836|         # Emit each subcategory row
   837|         try:
   838|             subs = cat.SubCategories
   839|         except:
   840|             subs = None
   841| 
   842|         if subs:
   843|             for sub in subs:
   844|                 try:
   845|                     sub_name = canon_str(sub.Name)
   846|                     row_key = "{}|{}".format(parent_name, sub_name)
   847|                     names.append(row_key)
   848|                     sig = row_sig(sub, parent_name, sub_name, cat_type)
   849|                     h = make_hash([sig])
   850|                     records.append(sig)
   851|                     row_hashes.append(h)
   852|                     per_parent_hashes.setdefault(parent_name, []).append(h)
   853|                     info["debug_rows_emitted"] += 1
   854|                 except:
   855|                     info["debug_fail_row"] += 1
   856|                     continue
   857| 
   858|     records_sorted = sorted(records)
   859|     row_hashes_sorted = sorted(row_hashes)
   860|     
   861|     info["raw_count"] = len(names)
   862|     info["names"] = sorted(set(names))
   863|     info["count"] = len(info["names"])
   864|     info["records"] = records_sorted
   865|     info["signature_hashes"] = row_hashes_sorted
   866|     info["count"] = len(records_sorted)
   867|     info["hash"] = make_hash(row_hashes_sorted) if row_hashes_sorted else None
   868|     info["record_rows"] = []
   869|     if row_pairs:
   870|         row_pairs_sorted = sorted(row_pairs, key=lambda t: t[0])
   871|         info["record_rows"] = [{"record": s, "sig_hash": h} for (s, h) in row_pairs_sorted]
   872|         
   873|     # Per-parent rollups
   874|     cat_hashes = {}
   875|     for pname, hs in per_parent_hashes.items():
   876|         hs_sorted = sorted(hs)
   877|         cat_hashes[pname] = make_hash(hs_sorted) if hs_sorted else None
   878|     info["category_hashes"] = cat_hashes
   879| 
   880|     return info
   881| 
   882| # ------------- line patterns fingerprint -----------------
   883| 
```
