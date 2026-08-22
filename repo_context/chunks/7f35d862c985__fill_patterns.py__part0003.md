# Chunk of domains/fill_patterns.py

- Source relative path: `domains/fill_patterns.py`
- Chunk: 3 of 8
- Original line range: 533-932
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_drafting, extract_drafting._phase2_build_phase2
- Source SHA-256: 30da073fc127a2ee2c9133e6348b0a2099f02ec5ae001d02fcf0ce69a1287358
- Starts inside symbol: extract_drafting
- Ends inside symbol: extract_drafting

```
   533|                                 oy = float(v2)
   534|                                 break
   535|                             except Exception:
   536|                                 continue
   537| 
   538|                     if origin_kind is None:
   539|                         semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   540|                     else:
   541|                         v_kind, q_kind = canonicalize_str(origin_kind)
   542|                         semantic.append({"k": "fill_pattern.grid[{}].origin.kind".format(idx), "v": v_kind, "q": q_kind})
   543| 
   544|                         if origin_kind == "uv":
   545|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.u".format(idx), ox)
   546|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.v".format(idx), oy)
   547|                         else:
   548|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.x".format(idx), ox)
   549|                             _phase2_add_float(semantic, "fill_pattern.grid[{}].origin.y".format(idx), oy)
   550| 
   551|                     try:
   552|                         _phase2_add_float(semantic, "fill_pattern.grid[{}].offset".format(idx), float(getattr(g, "Offset")))
   553|                     except Exception:
   554|                         semantic.append({"k": "fill_pattern.grid[{}].offset".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   555| 
   556|                     try:
   557|                         _phase2_add_float(semantic, "fill_pattern.grid[{}].shift".format(idx), float(getattr(g, "Shift")))
   558|                     except Exception:
   559|                         semantic.append({"k": "fill_pattern.grid[{}].shift".format(idx), "v": None, "q": ITEM_Q_UNREADABLE})
   560| 
   561|         # Derived structural identity helper for Phase-2:
   562|         # Collapse all per-grid semantic items into a single hash so join-key discovery
   563|         # can treat the grid bundle as one "field" without losing the detailed items.
   564|         #
   565|         # IMPORTANT: grid order is identity-significant; do NOT sort the preimage.
   566|         try:
   567|             grid_like = []
   568|             for it in (semantic or []):
   569|                 k = safe_str(it.get("k", ""))
   570|                 if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
   571|                     # Stable preimage: include k/q/v so unreadables affect the hash deterministically
   572|                     grid_like.append("k={}|q={}|v={}".format(
   573|                         safe_str(it.get("k", "")),
   574|                         safe_str(it.get("q", "")),
   575|                         safe_str(it.get("v", "")),
   576|                     ))
   577|             grids_def_hash = make_hash(grid_like) if grid_like else None
   578|         except Exception:
   579|             grids_def_hash = None
   580| 
   581|         if grids_def_hash:
   582|             semantic.append({"k": "fill_pattern.grids_def_hash", "v": grids_def_hash, "q": ITEM_Q_OK})
   583|         else:
   584|             # If we can't compute it, make the failure explicit (but keep it out of identity)
   585|             semantic.append({"k": "fill_pattern.grids_def_hash", "v": None, "q": ITEM_Q_UNREADABLE})
   586| 
   587|         # Phase-2 bloat control:
   588|         # The full grid definition is already present in identity_basis.items (for sig_hash reproducibility).
   589|         # Avoid duplicating per-grid items in phase2.semantic_items; keep only pointer + small scalars.
   590|         semantic_reduced = []
   591|         for it in (semantic or []):
   592|             k = safe_str(it.get("k", ""))
   593|             if k.startswith("fill_pattern.grid["):
   594|                 continue
   595|             semantic_reduced.append(it)
   596| 
   597|         return {
   598|             "schema": "phase2.{}.v1".format(DOMAIN_NAME),
   599|             "grouping_basis": "phase2.hypothesis",
   600|             "semantic_items": phase2_sorted_items(semantic_reduced),
   601|             "cosmetic_items": phase2_sorted_items(cosmetic),
   602|             "coordination_items": phase2_sorted_items(coordination),
   603|             "unknown_items": phase2_sorted_items(unknown),
   604|         }
   605| 
   606|     records = []
   607|     per_hashes = []
   608|     per_hashes_v2 = []
   609|     v2_records = []
   610|     v2_sig_hashes = []
   611|     names = []
   612|     uid_to_hash_v2 = {}
   613|     id_to_value = {}
   614|     uid_to_hash = {}
   615| 
   616|     for e in col:
   617|         info["debug_total_elements"] += 1
   618| 
   619|         name = canon_str(getattr(e, "Name", None))
   620|         if not name:
   621|             info["debug_skipped_no_name"] += 1
   622|             continue
   623| 
   624|         uid = getattr(e, "UniqueId", "") or ""
   625| 
   626|         # Always keep the element, even if we can't read its FillPattern
   627|         fp = None
   628|         try:
   629|             fp = e.GetFillPattern()
   630|         except Exception as e:
   631|             fp = None
   632| 
   633|         # Filter: only process patterns matching this domain's target
   634|         if fp is not None:
   635|             try:
   636|                 _fp_target_int = int(fp.Target)
   637|             except Exception:
   638|                 _fp_target_int = -1
   639|             if _fp_target_int != _TARGET_INT:
   640|                 info["debug_skipped_wrong_target"] += 1
   641|                 continue
   642| 
   643|         # Filter: skip solid fills — system defaults, ungoverned
   644|         if fp is not None:
   645|             try:
   646|                 if fp.IsSolidFill:
   647|                     id_to_value[safe_str(e.Id.IntegerValue)] = FILL_PATTERN_SYMBOLIC_SOLID
   648|                     continue
   649|             except Exception:
   650|                 pass  # if unreadable, proceed and let field-level q handle it
   651| 
   652|         names.append(name)
   653| 
   654|         # -------------------------
   655|         # Legacy signature (UNCHANGED meaning)
   656|         # -------------------------
   657|         if fp is None:
   658|             info["debug_fail_getfillpattern"] += 1
   659|             sig = [
   660|                 f"is_solid={S_MISSING}",
   661|                 f"target={_TARGET_NAME}",
   662|                 f"grid_count={S_MISSING}",
   663|                 f"grid[000].unreadable={S_MISSING}",
   664|                 "error=GetFillPatternFailed",
   665|             ]
   666|         else:
   667|             is_solid = None
   668|             try: is_solid = fp.IsSolidFill
   669|             except Exception as e: pass
   670| 
   671|             gc = None
   672|             try: gc = fp.GridCount
   673|             except Exception as e: pass
   674| 
   675|             sig = [
   676|                 "is_solid={}".format(canon_str(is_solid)),
   677|                 "target={}".format(_TARGET_NAME),
   678|                 "grid_count={}".format(canon_str(gc)),
   679|             ]
   680| 
   681|             if gc:
   682|                 try:
   683|                     for i in range(int(gc)):
   684|                         sig.extend(grid_sig(fp, i))
   685|                 except Exception as e:
   686|                     info["debug_fail_grid_read"] += 1
   687|                     sig.append("error=GridLoopFailed")
   688| 
   689|         sig_sorted = sorted(sig)
   690|         def_hash = make_hash(sig_sorted)
   691|         if uid:
   692|             uid_to_hash[uid] = def_hash
   693| 
   694|         # -------------------------
   695|         # v2 (contract semantic): NO names; block on unreadable/missing
   696|         # -------------------------
   697|         v2_ok = True
   698|         v2_reason = None
   699|         sig_v2 = []
   700| 
   701|         if fp is None:
   702|             v2_ok = False
   703|             v2_reason = "get_fillpattern_failed"
   704|         else:
   705|             # is_solid: require bool-coercible
   706|             try:
   707|                 is_solid_v2 = fp.IsSolidFill
   708|             except Exception as e:
   709|                 v2_ok = False
   710|                 v2_reason = "is_solid_unreadable"
   711| 
   712|             if v2_ok:
   713|                 # grid_count: require int (0 allowed)
   714|                 try:
   715|                     gc_v2 = fp.GridCount
   716|                     gc_i = int(gc_v2)
   717|                 except Exception as e:
   718|                     v2_ok = False
   719|                     v2_reason = "grid_count_unreadable"
   720| 
   721|             if v2_ok:
   722|                 sig_v2.append("target={}".format(_TARGET_NAME))
   723|                 sig_v2.append("is_solid={}".format(canon_str(bool(is_solid_v2))))
   724|                 sig_v2.append("grid_count={}".format(canon_str(gc_i)))
   725| 
   726|                 # grids: every grid must be readable
   727|                 if gc_i:
   728|                     for i in range(gc_i):
   729|                         ok, parts, reason = _grid_sig_v2(fp, i)
   730|                         if not ok:
   731|                             v2_ok = False
   732|                             v2_reason = reason
   733|                             break
   734|                         sig_v2.extend(parts)
   735| 
   736|         if v2_ok:
   737|             # keep deterministic: sort like legacy (order-insensitive at record level)
   738|             sig_v2_sorted = sorted(sig_v2)
   739|             def_hash_v2 = make_hash(sig_v2_sorted)
   740|             per_hashes_v2.append(def_hash_v2)
   741|             if uid:
   742|                 uid_to_hash_v2[uid] = def_hash_v2
   743|         else:
   744|             _bump_v2_reason(v2_reason or "unknown")
   745| 
   746|         phase2_payload = _phase2_build_phase2(
   747|             name=name,
   748|             uid=uid,
   749|             elem_id_str=safe_str(e.Id.IntegerValue),
   750|             fp=fp,
   751|             elem=e,
   752|         )
   753| 
   754|         rec = {
   755|             "id": safe_str(e.Id.IntegerValue),
   756|             "uid": uid,
   757|             "name": name,          # metadata only
   758|             "def_hash": def_hash,  # hashed legacy definition
   759|         }
   760| 
   761|         if DEBUG_INCLUDE_FILLPATTERN_SIGNATURES:
   762|             rec["def_signature"] = sig_sorted
   763| 
   764|         status_v2 = STATUS_OK
   765|         status_reasons_v2 = []
   766|         
   767|         identity_items_v2 = []
   768| 
   769|         # NOTE: name/uid/elem_id are labels/metadata and MUST NOT participate in identity.
   770|         # Name is carried in label{} and in the phase2 cosmetic surface.
   771| 
   772|         if fp is None:
   773|             gc_v, gc_q = (None, ITEM_Q_UNREADABLE)
   774|             gc_i = None
   775|         else:
   776|             try:
   777|                 gc_i = int(fp.GridCount)
   778|                 gc_v, gc_q = canonicalize_int(gc_i)
   779|             except Exception:
   780|                 gc_i = None
   781|                 gc_v, gc_q = (None, ITEM_Q_UNREADABLE)
   782| 
   783|         # target is always _TARGET_NAME / ITEM_Q_OK - not part of required_qs check
   784|         identity_items_v2.append(make_identity_item("fill_pattern.target", _TARGET_NAME, ITEM_Q_OK))
   785|         # is_solid is a filter criterion, not an identity field — omitted from identity_items
   786|         identity_items_v2.append(make_identity_item("fill_pattern.grid_count", gc_v, gc_q))
   787|         required_qs = [gc_q]
   788| 
   789|         if gc_i and gc_i > 0:
   790|             for i in range(gc_i):
   791|                 idx = "{:03d}".format(int(i))
   792|                 g = _phase2_try_get_grid(fp, i)
   793|                 if g is None:
   794|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", None, ITEM_Q_UNREADABLE))
   795|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", None, ITEM_Q_UNREADABLE))
   796|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", None, ITEM_Q_UNREADABLE))
   797|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", None, ITEM_Q_UNREADABLE))
   798|                     required_qs.extend([ITEM_Q_UNREADABLE] * 4)
   799|                     continue
   800| 
   801|                 # angle / offset / shift
   802|                 try:
   803|                     ang_v, ang_q = canonicalize_float(getattr(g, "Angle", None))
   804|                 except Exception:
   805|                     ang_v, ang_q = (None, ITEM_Q_UNREADABLE)
   806| 
   807|                 try:
   808|                     off_v, off_q = canonicalize_float(getattr(g, "Offset", None))
   809|                 except Exception:
   810|                     off_v, off_q = (None, ITEM_Q_UNREADABLE)
   811| 
   812|                 try:
   813|                     sh_v, sh_q = canonicalize_float(getattr(g, "Shift", None))
   814|                 except Exception:
   815|                     sh_v, sh_q = (None, ITEM_Q_UNREADABLE)
   816| 
   817|                 # origin: explicit kind + conditional leaf members (uv vs xy)
   818|                 origin_kind = None
   819|                 a = b = None
   820| 
   821|                 # UV origin
   822|                 try:
   823|                     o = getattr(g, "Origin", None)
   824|                     u = getattr(o, "U", None)
   825|                     v = getattr(o, "V", None)
   826|                     if u is not None and v is not None:
   827|                         origin_kind = "uv"
   828|                         a = u
   829|                         b = v
   830|                 except Exception:
   831|                     pass
   832| 
   833|                 # XY origin
   834|                 if origin_kind is None:
   835|                     try:
   836|                         o = getattr(g, "Origin", None)
   837|                         x = getattr(o, "X", None)
   838|                         y = getattr(o, "Y", None)
   839|                         if x is not None and y is not None:
   840|                             origin_kind = "xy"
   841|                             a = x
   842|                             b = y
   843|                     except Exception:
   844|                         pass
   845| 
   846|                 # Scalar origin props (treated as uv)
   847|                 if origin_kind is None:
   848|                     for u_name, v_name in [("OriginU", "OriginV"), ("UOrigin", "VOrigin")]:
   849|                         try:
   850|                             u2 = getattr(g, u_name)
   851|                             v2 = getattr(g, v_name)
   852|                             if u2 is None or v2 is None:
   853|                                 continue
   854|                             origin_kind = "uv"
   855|                             a = u2
   856|                             b = v2
   857|                             break
   858|                         except Exception:
   859|                             continue
   860| 
   861|                 if origin_kind is None:
   862|                     ok_kind = (None, ITEM_Q_UNREADABLE)
   863|                 else:
   864|                     ok_kind = canonicalize_str(origin_kind)
   865| 
   866|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].angle", ang_v, ang_q))
   867|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.kind", ok_kind[0], ok_kind[1]))
   868| 
   869|                 if origin_kind == "uv":
   870|                     try:
   871|                         ou_v, ou_q = canonicalize_float(a)
   872|                     except Exception:
   873|                         ou_v, ou_q = (None, ITEM_Q_UNREADABLE)
   874|                     try:
   875|                         ov_v, ov_q = canonicalize_float(b)
   876|                     except Exception:
   877|                         ov_v, ov_q = (None, ITEM_Q_UNREADABLE)
   878| 
   879|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.u", ou_v, ou_q))
   880|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.v", ov_v, ov_q))
   881|                     required_qs.extend([ang_q, ok_kind[1], ou_q, ov_q, off_q, sh_q])
   882| 
   883|                 elif origin_kind == "xy":
   884|                     try:
   885|                         ox_v, ox_q = canonicalize_float(a)
   886|                     except Exception:
   887|                         ox_v, ox_q = (None, ITEM_Q_UNREADABLE)
   888|                     try:
   889|                         oy_v, oy_q = canonicalize_float(b)
   890|                     except Exception:
   891|                         oy_v, oy_q = (None, ITEM_Q_UNREADABLE)
   892| 
   893|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.x", ox_v, ox_q))
   894|                     identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].origin.y", oy_v, oy_q))
   895|                     required_qs.extend([ang_q, ok_kind[1], ox_q, oy_q, off_q, sh_q])
   896| 
   897|                 else:
   898|                     # kind unreadable => identity blocked; no leaf members
   899|                     required_qs.extend([ang_q, ok_kind[1], off_q, sh_q])
   900| 
   901|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].offset", off_v, off_q))
   902|                 identity_items_v2.append(make_identity_item(f"fill_pattern.grid[{idx}].shift", sh_v, sh_q))
   903| 
   904|         # Derived join helper (policy-required key): capture the entire grid definition bundle.
   905|         # Canonical evidence source is identity_basis.items; selectors reference subsets.
   906|         # Keep preimage order-sensitive so grid index order remains identity-significant.
   907|         try:
   908|             grid_like = []
   909|             for it in (identity_items_v2 or []):
   910|                 k = safe_str(it.get("k", ""))
   911|                 if k == "fill_pattern.grid_count" or k.startswith("fill_pattern.grid["):
   912|                     grid_like.append("k={}|q={}|v={}".format(
   913|                         safe_str(it.get("k", "")),
   914|                         safe_str(it.get("q", "")),
   915|                         safe_str(it.get("v", "")),
   916|                     ))
   917|             grids_def_hash_v, grids_def_hash_q = (
   918|                 (make_hash(grid_like), ITEM_Q_OK) if grid_like else (None, ITEM_Q_UNREADABLE)
   919|             )
   920|         except Exception:
   921|             grids_def_hash_v, grids_def_hash_q = (None, ITEM_Q_UNREADABLE)
   922| 
   923|         identity_items_v2.append(
   924|             make_identity_item("fill_pattern.grids_def_hash", grids_def_hash_v, grids_def_hash_q)
   925|         )
   926| 
   927|         if any(q != ITEM_Q_OK for q in required_qs):
   928|             status_v2 = STATUS_BLOCKED
   929|             status_reasons_v2.append("required_identity_not_ok")
   930| 
   931|         identity_items_v2_sorted = sorted(identity_items_v2, key=lambda d: str(d.get("k","")))
   932|         sig_preimage_v2 = serialize_identity_items(identity_items_v2_sorted)
```
