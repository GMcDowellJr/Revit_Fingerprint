# Chunk of tools/probes/probe_dimension_types.py

- Source relative path: `tools/probes/probe_dimension_types.py`
- Chunk: 2 of 3
- Original line range: 470-985
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _example_score, _maybe_set_example, _md5, _try_call, _kv_norm, _format_synth_contract, _reflect_members, _try_extract_format_surface, _try_extract_format_surface._sig, _upsert_synth_inventory
- Source SHA-256: c9e2998c9a3c2f218a004c3c8351e8c52a44975202334b7fe5365c73fa869cc7
- Starts inside symbol: no
- Ends inside symbol: no

```
   470| 
   471| discovery_notes = []
   472| 
   473| if len(dim_types) == 0:
   474|     discovery_notes.append("fallback:param_signature:DimensionType collector returned 0")
   475|     all_types = _safe(
   476|         lambda: (FilteredElementCollector(doc)
   477|                  .WhereElementIsElementType()
   478|                  .OfClass(ElementType)
   479|                  .ToElements()),
   480|         default=[]
   481|     )
   482|     try:
   483|         all_types = list(all_types)
   484|     except:
   485|         all_types = list(all_types) if all_types is not None else []
   486| 
   487|     dim_types = []
   488|     for t in all_types:
   489|         if _looks_like_dimension_type(t):
   490|             dim_types.append(t)
   491| 
   492| 
   493| # Cap AFTER filtering
   494| try:
   495|     max_n = int(max_dim_types_to_inspect)
   496|     if max_n >= 0:
   497|         dim_types = dim_types[:max_n]
   498| except:
   499|     pass
   500| 
   501| 
   502| # -------------------------
   503| # Sampling (breadth bias): first N per Shape
   504| # -------------------------
   505| 
   506| selected = []
   507| by_shape = {}  # shape_key -> count
   508| 
   509| for t in dim_types:
   510|     shape_key, shape_label, shape_family, _ = _get_dim_shape_info(t)
   511|     c = by_shape.get(shape_key, 0)
   512| 
   513|     if per_shape_limit is None:
   514|         shape_ok = True
   515|     else:
   516|         try:
   517|             shape_ok = c < int(per_shape_limit)
   518|         except:
   519|             shape_ok = c < 8
   520| 
   521|     if shape_ok:
   522|         selected.append(t)
   523|         by_shape[shape_key] = c + 1
   524| 
   525| # If per_shape_limit is 0 or negative, fallback to at least 1 per shape
   526| if len(selected) == 0 and len(dim_types) > 0:
   527|     seen = set()
   528|     for t in dim_types:
   529|         shape_key, shape_label, shape_family, _ = _get_dim_shape_info(t)
   530|         if shape_key not in seen:
   531|             selected.append(t)
   532|             seen.add(shape_key)
   533| 
   534| 
   535| # -------------------------
   536| # Build inventory (union over selected)
   537| # Dedup observations by (param_key, storage, norm)
   538| # -------------------------
   539| 
   540| # -------------------------
   541| # Synthetic inventory injection (format surface)
   542| # -------------------------
   543| 
   544| param_index = {}
   545| 
   546| def _example_score(pv):
   547|     if pv is None:
   548|         return -1
   549|     q = pv.get("q")
   550|     if q == "ok":
   551|         base = 100
   552|     elif q == "missing":
   553|         base = 10
   554|     elif q == "unreadable":
   555|         base = 5
   556|     else:
   557|         base = 0
   558| 
   559|     disp = pv.get("display")
   560|     raw = pv.get("raw")
   561|     norm = pv.get("norm")
   562| 
   563|     if disp is not None and str(disp).strip() != "":
   564|         base += 20
   565|     if norm is not None:
   566|         base += 10
   567|     if raw is not None:
   568|         base += 5
   569| 
   570|     return base
   571| 
   572| def _maybe_set_example(entry, pv):
   573|     if pv is None:
   574|         return
   575| 
   576|     ex = entry.get("example")
   577|     if ex is None:
   578|         entry["example"] = {
   579|             "q": pv.get("q"),
   580|             "storage": pv.get("storage"),
   581|             "raw": pv.get("raw"),
   582|             "display": pv.get("display"),
   583|             "norm": pv.get("norm")
   584|         }
   585|         return
   586| 
   587|     cur_score = _example_score(ex)
   588|     new_score = _example_score(pv)
   589| 
   590|     if new_score > cur_score:
   591|         entry["example"] = {
   592|             "q": pv.get("q"),
   593|             "storage": pv.get("storage"),
   594|             "raw": pv.get("raw"),
   595|             "display": pv.get("display"),
   596|             "norm": pv.get("norm")
   597|         }
   598|         
   599| # -------------------------
   600| # Synthetic extraction: Primary/Alternate units format options (probe-only)
   601| # -------------------------
   602| 
   603| import hashlib
   604| 
   605| def _md5(s):
   606|     try:
   607|         h = hashlib.md5()
   608|         h.update(s.encode("utf-8"))
   609|         return h.hexdigest()
   610|     except:
   611|         return None
   612| 
   613| def _try_call(obj, member_name, allow_call=True):
   614|     if obj is None or not member_name:
   615|         return (False, None, "missing_target_or_member")
   616|     try:
   617|         if hasattr(obj, member_name):
   618|             v = getattr(obj, member_name)
   619|             if callable(v):
   620|                 if not allow_call:
   621|                     # SAFETY: member_name came from open-ended reflection
   622|                     # (not the small hardcoded root_candidates getter list
   623|                     # below) -- never invoke it. Revit API methods can have
   624|                     # side effects (printing, export, regenerate, delete,
   625|                     # ...) and there is no reliable way to tell a safe
   626|                     # zero-arg query method from a side-effecting one by
   627|                     # name alone. Record that it exists without calling it.
   628|                     return (True, "<method not invoked>", None)
   629|                 try:
   630|                     return (True, v(), None)
   631|                 except Exception as ex:
   632|                     return (False, None, "{}: {}".format(type(ex).__name__, ex))
   633|             return (True, v, None)
   634|     except Exception as ex:
   635|         return (False, None, "{}: {}".format(type(ex).__name__, ex))
   636|     return (False, None, "no_such_member")
   637| 
   638| def _kv_norm(k, v):
   639|     if v is None:
   640|         return (k, None)
   641|     try:
   642|         if hasattr(v, "IntegerValue"):
   643|             return (k, int(v.IntegerValue))
   644|         if isinstance(v, (bool, int, float, str)):
   645|             return (k, v)
   646|         if hasattr(v, "ToString"):
   647|             s = v.ToString()
   648|             if s and "Autodesk.Revit" not in s and "System." not in s:
   649|                 return (k, s)
   650|         s2 = str(v)
   651|         if s2 and "Autodesk.Revit" not in s2 and "System." not in s2:
   652|             return (k, s2)
   653|         return (k, None)
   654|     except:
   655|         return (k, None)
   656| 
   657| def _format_synth_contract(raw_v):
   658|     if raw_v is None:
   659|         return {"q": "missing", "storage": "None", "raw": None, "display": None, "norm": None}
   660|     if isinstance(raw_v, str):
   661|         return {"q": "ok", "storage": "String", "raw": raw_v, "display": raw_v, "norm": raw_v}
   662|     if isinstance(raw_v, bool):
   663|         return {"q": "ok", "storage": "Integer", "raw": int(raw_v), "display": str(raw_v), "norm": int(raw_v)}
   664|     if isinstance(raw_v, int):
   665|         return {"q": "ok", "storage": "Integer", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   666|     if isinstance(raw_v, float):
   667|         return {"q": "ok", "storage": "Double", "raw": raw_v, "display": str(raw_v), "norm": raw_v}
   668|     try:
   669|         if hasattr(raw_v, "ToString"):
   670|             s = raw_v.ToString()
   671|             if s and "Autodesk.Revit" not in s and "System." not in s:
   672|                 return {"q": "ok", "storage": "None", "raw": None, "display": s, "norm": s}
   673|     except:
   674|         pass
   675|     return {"q": "unsupported", "storage": "None", "raw": None, "display": None, "norm": None}
   676| 
   677| def _reflect_members(obj, keywords):
   678|     names = []
   679|     if obj is None:
   680|         return names
   681|     try:
   682|         t = obj.GetType()
   683|         try:
   684|             props = t.GetProperties()
   685|             for p in props:
   686|                 try:
   687|                     n = p.Name
   688|                     nl = n.lower()
   689|                     for kw in keywords:
   690|                         if kw in nl:
   691|                             names.append(n)
   692|                             break
   693|                 except:
   694|                     pass
   695|         except:
   696|             pass
   697|         try:
   698|             meths = t.GetMethods()
   699|             for m in meths:
   700|                 try:
   701|                     n = m.Name
   702|                     nl = n.lower()
   703|                     if m.GetParameters().Length != 0:
   704|                         continue
   705|                     for kw in keywords:
   706|                         if kw in nl:
   707|                             names.append(n)
   708|                             break
   709|                 except:
   710|                     pass
   711|         except:
   712|             pass
   713|     except:
   714|         pass
   715|     return sorted(list(set(names)))
   716| 
   717| def _try_extract_format_surface(dim_type):
   718|     out = {"found_members": [], "values": {}, "signatures": {"primary": None, "alternate": None}}
   719|     if dim_type is None:
   720|         return out
   721| 
   722|     primary_keywords = ["primary", "unit", "format", "round", "symbol", "suppress", "digits", "accuracy"]
   723|     alt_keywords = ["alternate", "alt", "unit", "format", "round", "symbol", "suppress", "digits", "accuracy"]
   724| 
   725|     root_candidates = [
   726|         "PrimaryUnits", "PrimaryUnit", "PrimaryFormatOptions", "PrimaryFormat",
   727|         "AlternateUnits", "AlternateUnit", "AlternateFormatOptions", "AlternateFormat",
   728|         "GetPrimaryUnits", "GetAlternateUnits", "GetPrimaryFormatOptions", "GetAlternateFormatOptions"
   729|     ]
   730| 
   731|     roots = []
   732|     for rc in root_candidates:
   733|         ok, v, err = _try_call(dim_type, rc)
   734|         if ok and v is not None:
   735|             roots.append((rc, v))
   736| 
   737|     if len(roots) == 0:
   738|         out["found_members"] = _reflect_members(dim_type, ["alternate", "alt", "primary", "format", "unit"])
   739|         for n in out["found_members"][:60]:
   740|             ok, v, err = _try_call(dim_type, n, allow_call=False)
   741|             if ok:
   742|                 key = "x.dim_type.{}".format(n)
   743|                 out["values"][key] = _format_synth_contract(_kv_norm(n, v)[1])
   744|         return out
   745| 
   746|     primary_kvs = []
   747|     alt_kvs = []
   748| 
   749|     for (root_name, root_obj) in roots:
   750|         is_alt = ("alt" in root_name.lower()) or ("alternate" in root_name.lower())
   751|         leaf_names = _reflect_members(root_obj, alt_keywords if is_alt else primary_keywords)
   752| 
   753|         for ln in leaf_names:
   754|             out["found_members"].append("{}::{}".format(root_name, ln))
   755| 
   756|         for ln in leaf_names[:60]:
   757|             ok, v, err = _try_call(root_obj, ln, allow_call=False)
   758|             if not ok:
   759|                 continue
   760|             k = ("x.alt_units.{}::{}".format(root_name, ln) if is_alt else "x.primary_units.{}::{}".format(root_name, ln))
   761|             _, normv = _kv_norm(ln, v)
   762|             out["values"][k] = _format_synth_contract(normv)
   763|             if is_alt:
   764|                 alt_kvs.append((ln, normv))
   765|             else:
   766|                 primary_kvs.append((ln, normv))
   767| 
   768|     def _sig(kvs):
   769|         parts = []
   770|         for (k, v) in sorted(kvs, key=lambda x: x[0]):
   771|             parts.append("{}={}".format(k, "" if v is None else str(v)))
   772|         return _md5("|".join(parts))
   773| 
   774|     out["signatures"]["primary"] = _sig(primary_kvs) if len(primary_kvs) > 0 else None
   775|     out["signatures"]["alternate"] = _sig(alt_kvs) if len(alt_kvs) > 0 else None
   776| 
   777|     out["found_members"] = sorted(list(set(out["found_members"])))
   778|     return out
   779| 
   780| synth_member_samples = []
   781| synth_crosswalk_rows = []
   782| 
   783| def _upsert_synth_inventory(param_key, contract, shape_key, shape_family):
   784|     if not param_key or contract is None:
   785|         return
   786|     if param_key not in param_index:
   787|         param_index[param_key] = {
   788|             "storage_types": set(),
   789|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   790|             "example": None,
   791|             "observed_on_shapes": set(),
   792|             "observed_on_families": set(),
   793|             "_seen_obs": set(),
   794|             "unique_value_count": 0,
   795|             "ok_but_unset_count": 0
   796|         }
   797| 
   798|     entry = param_index[param_key]
   799|     q = contract.get("q") or "unreadable"
   800|     st = contract.get("storage")
   801|     norm = contract.get("norm")
   802| 
   803|     obs_sig = (param_key, str(st), str(norm))
   804|     if obs_sig in entry["_seen_obs"]:
   805|         entry["observed_on_shapes"].add(shape_key)
   806|         entry["observed_on_families"].add(shape_family)
   807|         _maybe_set_example(entry, contract)
   808|         return
   809| 
   810|     entry["_seen_obs"].add(obs_sig)
   811|     entry["unique_value_count"] += 1
   812| 
   813|     if st:
   814|         entry["storage_types"].add(st)
   815|     if q not in entry["q_counts"]:
   816|         entry["q_counts"][q] = 0
   817|     entry["q_counts"][q] += 1
   818| 
   819|     if q == "ok" and (contract.get("raw") is None) and (contract.get("norm") is None):
   820|         entry["ok_but_unset_count"] += 1
   821| 
   822|     entry["observed_on_shapes"].add(shape_key)
   823|     entry["observed_on_families"].add(shape_family)
   824|     _maybe_set_example(entry, contract)
   825| 
   826| # Run synthetic extraction on the same sampled DimensionTypes
   827| for dt in selected:
   828|     shape_key, shape_label, shape_family, _ = _get_dim_shape_info(dt)
   829| 
   830|     fx = _try_extract_format_surface(dt)
   831| 
   832|     # keep small diagnostics sample only
   833|     if fx.get("found_members"):
   834|         synth_member_samples.append({
   835|             "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
   836|             "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
   837|             "found_members": fx.get("found_members")[:30]
   838|         })
   839| 
   840|     vals = fx.get("values") or {}
   841|     for k, contract in vals.items():
   842|         # synth keys should not collide with "p.*"
   843|         _upsert_synth_inventory(k, contract, shape_key, shape_family)
   844| 
   845|     sigs = fx.get("signatures") or {}
   846|     if sigs.get("primary") or sigs.get("alternate"):
   847|         synth_crosswalk_rows.append({
   848|             "dim_type.id": _safe(lambda: dt.Id.IntegerValue, None),
   849|             "dim_type.name": _safe(lambda: _safe_type_name(dt), None),
   850|             "dim_type.family_name_param": _get_family_name_param(dt),
   851|             "dim_type.shape": shape_key,
   852|             "dim_type.shape_family": shape_family,
   853|             "format_sig.primary": sigs.get("primary"),
   854|             "format_sig.alternate": sigs.get("alternate")
   855|         })
   856| 
   857| for t in selected:
   858|     shape_key, shape_label, shape_family, _ = _get_dim_shape_info(t)
   859| 
   860|     params = _safe(lambda: list(t.GetOrderedParameters()), default=None)
   861|     if params is None:
   862|         params = _safe(lambda: list(t.Parameters), default=[])
   863| 
   864|     for p in params:
   865|         dn = _safe(lambda: _safe_param_def_name(p), None)
   866|         if not dn:
   867|             continue
   868| 
   869|         pk = "p.{}".format(dn)
   870|         pv = _format_param_contract(p)
   871| 
   872|         if pk not in param_index:
   873|             param_index[pk] = {
   874|                 "storage_types": set(),
   875|                 "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   876|                 "ok_but_unset_count": 0,
   877|                 "example": None,
   878|                 "observed_on_shapes": set(),
   879|                 "observed_on_families": set(),
   880|                 "_seen_obs": set(),
   881|                 "unique_value_count": 0
   882|             }
   883| 
   884|         entry = param_index[pk]
   885|         q = pv.get("q") or "unreadable"
   886|         st = pv.get("storage")
   887|         norm = pv.get("norm")
   888| 
   889|         obs_sig = (pk, str(st), str(norm))
   890|         if obs_sig in entry["_seen_obs"]:
   891|             # Probe-local dedupe: do not double-count identical observations
   892|             entry["observed_on_shapes"].add(shape_key)
   893|             entry["observed_on_families"].add(shape_family)
   894|             _maybe_set_example(entry, pv)
   895|             continue
   896| 
   897|         entry["_seen_obs"].add(obs_sig)
   898|         entry["unique_value_count"] += 1
   899| 
   900|         if st:
   901|             entry["storage_types"].add(st)
   902|         if q not in entry["q_counts"]:
   903|             entry["q_counts"][q] = 0
   904|         entry["q_counts"][q] += 1
   905|         if q == "ok" and (pv.get("raw") is None) and (pv.get("norm") is None):
   906|             entry["ok_but_unset_count"] += 1
   907| 
   908|         entry["observed_on_shapes"].add(shape_key)
   909|         entry["observed_on_families"].add(shape_family)
   910|         _maybe_set_example(entry, pv)
   911| 
   912|     # Explicit family name capture — not always in GetOrderedParameters()
   913|     fn_val = _get_family_name_param(t)
   914|     fn_contract = {
   915|         "q": "ok" if fn_val is not None else "unreadable",
   916|         "storage": "String",
   917|         "raw": fn_val,
   918|         "display": fn_val,
   919|         "norm": fn_val
   920|     }
   921|     pk_fn = "x.dim_type.family_name_param"
   922|     if pk_fn not in param_index:
   923|         param_index[pk_fn] = {
   924|             "storage_types": set(),
   925|             "q_counts": {"ok": 0, "missing": 0, "unreadable": 0, "unsupported": 0},
   926|             "ok_but_unset_count": 0,
   927|             "example": None,
   928|             "observed_on_shapes": set(),
   929|             "observed_on_families": set(),
   930|             "_seen_obs": set(),
   931|             "unique_value_count": 0
   932|         }
   933|     entry = param_index[pk_fn]
   934|     q = fn_contract.get("q") or "unreadable"
   935|     st = fn_contract.get("storage")
   936|     norm = fn_contract.get("norm")
   937|     obs_sig = (pk_fn, str(st), str(norm))
   938|     if obs_sig not in entry["_seen_obs"]:
   939|         entry["_seen_obs"].add(obs_sig)
   940|         entry["unique_value_count"] += 1
   941|         if st:
   942|             entry["storage_types"].add(st)
   943|         entry["q_counts"][q] = entry["q_counts"].get(q, 0) + 1
   944|         _maybe_set_example(entry, fn_contract)
   945|     entry["observed_on_shapes"].add(shape_key)
   946|     entry["observed_on_families"].add(shape_family)
   947| 
   948| 
   949| # Emit inventory records (stable order)
   950| param_inventory = []
   951| for pk in sorted(param_index.keys()):
   952|     e = param_index[pk]
   953|     param_inventory.append({
   954|         "domain": "dimension_types",
   955|         "param_key": pk,
   956|         "selected_type_sample_count": len(selected),
   957|         "example": e.get("example"),
   958|         "observed": {
   959|             "storage_types": sorted(list(e["storage_types"])),
   960|             "q_counts": e["q_counts"],
   961|             "ok_but_unset_count": e.get("ok_but_unset_count", 0),
   962|             "unique_value_count": e.get("unique_value_count", 0),
   963|             "observed_on_shapes": sorted(list(e["observed_on_shapes"]))[:25],
   964|             "observed_on_families": sorted(list(e.get("observed_on_families", set())))
   965|         }
   966|     })
   967| 
   968| # -------------------------
   969| # Optional Crosswalk: DimensionType -> Tick Mark (Arrowhead)
   970| # -------------------------
   971| 
   972| optional_crosswalk = []
   973| 
   974| # Always include format-signature crosswalk rows when discovered (probe-only).
   975| # (This is analogous to arrowhead crosswalk but does not require enable_crosswalk.)
   976| optional_crosswalk.extend(synth_crosswalk_rows)
   977| 
   978| 
   979| DIM_TICK_PARAM_CANDIDATES = [
   980|     "Tick Mark",
   981|     "Tick mark",
   982|     "Tick Mark Type",
   983|     "Tick Mark Symbol",
   984| ]
   985| 
```
