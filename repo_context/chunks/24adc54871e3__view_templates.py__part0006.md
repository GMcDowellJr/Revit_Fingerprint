# Chunk of domains/view_templates.py

- Source relative path: `domains/view_templates.py`
- Chunk: 6 of 6
- Original line range: 2145-2557
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_schedules, extract_schedules._v2_block
- Source SHA-256: ca478c676990e318341a80d987cc318a4531ef7d17b52cb5fd1b41c67678296d
- Starts inside symbol: no
- Ends inside symbol: no

```
  2145| def extract_schedules(doc, ctx=None):
  2146|     DOMAIN_NAME = "view_templates_schedules"
  2147|     """
  2148|     Extract view templates fingerprint - Schedules only.
  2149| 
  2150|     Uses the minimal stable schedule surface (no VG/filter stack).
  2151| 
  2152|     Args:
  2153|         doc: Revit document
  2154|         ctx: context dict with mappings from other domains
  2155| 
  2156|     Returns:
  2157|         Dictionary with count, hash, signature_hashes, records,
  2158|         record_rows, and debug counters
  2159|     """
  2160|     info = {
  2161|         "count": 0,
  2162|         "raw_count": 0,
  2163|         "names": [],
  2164|         "records": [],
  2165| 
  2166|         # debug counters
  2167|         "debug_not_template": 0,
  2168|         "debug_missing_name": 0,
  2169|         "debug_missing_uid": 0,
  2170|         "debug_fail_read": 0,
  2171|         "debug_kept": 0,
  2172|         "debug_view_type_filtered": 0,
  2173| 
  2174|         # v2 (contract semantic) surfaces - additive only
  2175|         "hash_v2": None,
  2176|         "signature_hashes_v2": [],
  2177|         "debug_v2_blocked": False,
  2178|         "debug_v2_block_reasons": {},
  2179|         # PR6: deterministic degraded signaling
  2180|         "debug_view_context_problem": 0,
  2181|         "debug_view_context_reasons": {},
  2182|         "debug_collect_types_failed": 0,
  2183|     }
  2184| 
  2185|     ctx_map = ctx or {}
  2186| 
  2187|     # CRITICAL DEPENDENCIES - schedules need phase_filters at minimum
  2188|     try:
  2189|         require_domain(ctx_map.get("_domains", {}), "phase_filters")
  2190|     except Blocked as b:
  2191|         info["debug_v2_blocked"] = True
  2192|         info["debug_v2_block_reasons"] = {"dependency_blocked": str(b.reasons)}
  2193|         info["count"] = 0
  2194|         info["records"] = []
  2195|         info["hash_v2"] = None
  2196|         return info
  2197| 
  2198|     # Get context mappings
  2199|     phase_filter_map_v2 = ctx_map.get("phase_filter_uid_to_hash", {})
  2200|     phase_filter_map = ctx_map.get("phase_filter_uid_to_hash", {})
  2201| 
  2202|     try:
  2203|         col = list(
  2204|             collect_instances(
  2205|                 doc,
  2206|                 of_class=View,
  2207|                 require_unique_id=True,
  2208|                 cctx=(ctx or {}).get("_collect") if ctx is not None else None,
  2209|                 cache_key=_VIEW_INSTANCES_CACHE_KEY,
  2210|             )
  2211|         )
  2212|     except Exception as e:
  2213|         info["debug_collect_types_failed"] += 1
  2214|         info["_domain_status"] = "degraded"
  2215|         info["_domain_diag"] = {
  2216|             "degraded_reasons": ["collect_types_failed"],
  2217|             "degraded_reason_counts": {"collect_types_failed": 1},
  2218|             "error": str(e),
  2219|         }
  2220|         return info
  2221| 
  2222|     info["raw_count"] = len(col)
  2223| 
  2224|     names = []
  2225|     records = []
  2226|     per_hashes = []
  2227|     per_hashes_v2 = []
  2228|     v2_any_blocked = False
  2229| 
  2230|     def _v2_block(reason):
  2231|         nonlocal v2_any_blocked
  2232|         v2_any_blocked = True
  2233|         info["debug_v2_blocked"] += 1
  2234|         try:
  2235|             info["debug_v2_block_reasons"][reason] = info["debug_v2_block_reasons"].get(reason, 0) + 1
  2236|         except Exception:
  2237|             pass
  2238| 
  2239|     for v in col:
  2240|         # Only process view templates
  2241|         try:
  2242|             is_template = v.IsTemplate
  2243|         except Exception:
  2244|             is_template = False
  2245| 
  2246|         if not is_template:
  2247|             info["debug_not_template"] += 1
  2248|             continue
  2249| 
  2250|         # Check that this is a schedule template
  2251|         if not _is_schedule_view(v):
  2252|             info["debug_view_type_filtered"] += 1
  2253|             continue
  2254| 
  2255|         # name/uid metadata
  2256|         name = canon_str(getattr(v, "Name", None))
  2257|         if not name:
  2258|             info["debug_missing_name"] += 1
  2259|             name = S_MISSING
  2260|         names.append(name)
  2261| 
  2262|         uid = None
  2263|         try:
  2264|             uid = canon_str(getattr(v, "UniqueId", None))
  2265|         except Exception:
  2266|             uid = None
  2267| 
  2268|         if not uid:
  2269|             info["debug_missing_uid"] += 1
  2270| 
  2271|         # PR6: view-scoped context snapshot (explicit missing vs unreadable)
  2272|         try:
  2273|             dv = (ctx or {}).get("_doc_view") if ctx is not None else None
  2274|             if dv is not None:
  2275|                 vi = dv.view_info(v, source="HOST")
  2276|                 if vi.reasons:
  2277|                     info["debug_view_context_problem"] += 1
  2278|                     for r in vi.reasons:
  2279|                         info["debug_view_context_reasons"][r] = info["debug_view_context_reasons"].get(r, 0) + 1
  2280|         except Exception:
  2281|             info["debug_view_context_problem"] += 1
  2282|             info["debug_view_context_reasons"]["view_context_unreadable"] = (
  2283|                 info["debug_view_context_reasons"].get("view_context_unreadable", 0) + 1
  2284|             )
  2285| 
  2286|         # v2 per-template signature (contract semantic)
  2287|         v2_ok = True
  2288|         sig_v2 = []
  2289| 
  2290|         # -----------------------------------------
  2291|         # SCHEDULE templates: minimal stable surface
  2292|         # -----------------------------------------
  2293|         sig = []
  2294| 
  2295|         # Template-controlled parameters ("Include" surface)
  2296|         try:
  2297|             tpl_ids = v.GetTemplateParameterIds() or []
  2298|             tpl_bips = set(
  2299|                 pid.IntegerValue for pid in tpl_ids
  2300|                 if hasattr(pid, "IntegerValue") and pid.IntegerValue < 0
  2301|             )
  2302|         except Exception:
  2303|             tpl_ids = []
  2304|             tpl_bips = set()
  2305| 
  2306|         non_ctrl_bips = _non_ctrl_bips_from_view(v)
  2307|         info["debug_non_ctrl_bips_count"] = len(non_ctrl_bips)
  2308| 
  2309|         # Include flags (stable)
  2310|         try:
  2311|             sig.append(
  2312|                 "include_phase_filter={}".format(
  2313|                     _is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")
  2314|                 )
  2315|             )
  2316|         except Exception:
  2317|             sig.append("include_phase_filter=False")
  2318| 
  2319|         try:
  2320|             sig.append(
  2321|                 "include_filters={}".format(
  2322|                     _is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_FILTERS")
  2323|                 )
  2324|             )
  2325|         except Exception:
  2326|             sig.append("include_filters=False")
  2327| 
  2328|         try:
  2329|             sig.append(
  2330|                 "include_appearance={}".format(
  2331|                     _is_template_param_included(non_ctrl_bips, "VIS_GRAPHICS_APPEARANCE")
  2332|                 )
  2333|             )
  2334|         except Exception:
  2335|             sig.append("include_appearance=False")
  2336| 
  2337|         # Phase Filter (reference global phase_filters domain) - legacy
  2338|         try:
  2339|             include_pf = _is_template_param_included(non_ctrl_bips, "VIEW_PHASE_FILTER")
  2340|         except Exception:
  2341|             include_pf = False
  2342| 
  2343|         v2_ok = _append_phase_filter_value(
  2344|             v=v,
  2345|             doc=doc,
  2346|             include_pf=include_pf,
  2347|             phase_filter_map=phase_filter_map,
  2348|             phase_filter_map_v2=phase_filter_map_v2,
  2349|             sig=sig,
  2350|             sig_v2=sig_v2,
  2351|             v2_ok=v2_ok,
  2352|             v2_block_fn=_v2_block,
  2353|             debug_counters=info,
  2354|         )
  2355|         v2_ok = _append_workset_visibility(v, doc, sig, sig_v2, v2_ok, _v2_block)
  2356|         # Built-in visual/behavioural parameters
  2357|         emit_builtin_params(v, DOMAIN_NAME, tpl_bips, non_ctrl_bips, sig, sig_v2,
  2358|                             debug_counters=info)
  2359| 
  2360|         # Shared/project parameters (stub — no-op until GUIDs confirmed)
  2361|         emit_shared_params_stub(v, DOMAIN_NAME, tpl_ids, sig, sig_v2,
  2362|                                 debug_counters=info)
  2363| 
  2364|         # NOTE: Schedule filter stack + VG signatures are not consistently supported across versions.
  2365|         # We keep schedule signature minimal and stable.
  2366| 
  2367|         # Finalize schedule signature
  2368|         sig_final = sorted(sig)
  2369|         def_hash = make_hash(sig_final)
  2370| 
  2371|         # v2 finalize (schedule)
  2372|         if v2_ok:
  2373|             try:
  2374|                 sig_v2.extend([s for s in sig_final if not s.startswith("name=")])
  2375|                 sig_v2_final = sorted(set(sig_v2))
  2376|                 def_hash_v2 = make_hash(sig_v2_final)
  2377|                 per_hashes_v2.append(def_hash_v2)
  2378|             except Exception:
  2379|                 _v2_block("schedule_finalize_failed")
  2380|                 v2_ok = False
  2381| 
  2382|         # -------------------------
  2383|         # record.v2 + Phase-2 (contract-aligned)
  2384|         # -------------------------
  2385|         identity_items = _canonical_identity_items_from_signature(def_hash, sig_final)
  2386|         semantic_keys = _semantic_keys_from_identity_items(identity_items)
  2387|         semantic_items = [it for it in identity_items if it.get("k") in set(semantic_keys)]
  2388|         sig_hash = make_hash(serialize_identity_items(semantic_items))
  2389| 
  2390|         rid_info = make_record_id_from_element(v)
  2391|         if rid_info:
  2392|             record_id, record_id_alg = rid_info
  2393|         else:
  2394|             record_id = "eid:{}".format(safe_str(getattr(getattr(v, "Id", None), "IntegerValue", "")))
  2395|             record_id_alg = "revit_elementid_v1"
  2396| 
  2397|         status = STATUS_OK
  2398|         status_reasons = []
  2399|         for it in identity_items:
  2400|             if it.get("q") != ITEM_Q_OK:
  2401|                 status = STATUS_DEGRADED
  2402|                 status_reasons.append("identity.incomplete:{}:{}".format(it.get("q"), it.get("k")))
  2403|         if not v2_ok:
  2404|             status = STATUS_BLOCKED
  2405|             status_reasons.append("semantic_v2_unresolved_dependency")
  2406|             sig_hash = None
  2407| 
  2408|         rec = build_record_v2(
  2409|             domain=DOMAIN_NAME,
  2410|             record_id=record_id,
  2411|             record_id_alg=record_id_alg,
  2412|             status=status,
  2413|             status_reasons=sorted(set(status_reasons)),
  2414|             sig_hash=sig_hash,
  2415|             identity_items=identity_items,
  2416|             required_qs=tuple(it.get("q") for it in identity_items),
  2417|             label={
  2418|                 "display": safe_str(name),
  2419|                 "quality": "human" if safe_str(name) and safe_str(name) != S_MISSING else "placeholder_missing",
  2420|                 "provenance": "revit.ViewName",
  2421|                 "components": {
  2422|                     "view_type": safe_str(v.ViewType),
  2423|                 },
  2424|             },
  2425|         )
  2426|         _ip, _ip_q = purge_lookup(getattr(getattr(v, "Id", None), "IntegerValue", None), ctx)
  2427|         rec["is_purgeable"] = _ip
  2428|         rec["is_purgeable_q"] = _ip_q
  2429| 
  2430|         rec["phase2"] = {
  2431|             "schema": "phase2.{}.v2".format(DOMAIN_NAME),
  2432|             "grouping_basis": "join_key.join_hash",
  2433|             "cosmetic_items": [],
  2434|             "coordination_items": [],
  2435|             "unknown_items": _traceability_unknown_items(v),
  2436|         }
  2437|         _append_assigned_view_count_cosmetic_item(rec, doc, v, ctx)
  2438| 
  2439|         rec["sig_basis"] = {
  2440|             "hash_alg": "md5_utf8_join_pipe",
  2441|             "keys_used": semantic_keys,
  2442|         }
  2443| 
  2444|         pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  2445|         vt_join_key, _vt_missing = build_join_key_from_policy(
  2446|             domain_policy=pol,
  2447|             identity_items=identity_items,
  2448|             include_optional_items=False,
  2449|             emit_keys_used=True,
  2450|             hash_optional_items=False,
  2451|             emit_items=False,
  2452|             emit_selectors=True,
  2453|         )
  2454|         rec["join_key"] = vt_join_key
  2455| 
  2456|         # Canonical Name Identity Projection (PR1): second, independent join_hash variant
  2457|         # keyed off this record's own label.display-backing item (view_template.name).
  2458|         # view_template.name does not exist in identity_items for any partition --
  2459|         # identity_items are built from _canonical_identity_items_from_signature(def_hash,
  2460|         # sig_final), a structured signature that explicitly strips "name="-prefixed
  2461|         # entries before hashing. Widened items list used only for this call;
  2462|         # identity_basis.items/sig_hash/join_key above are unaffected.
  2463|         vt_name_v, vt_name_q = canonicalize_str(name)
  2464|         name_key_items = identity_items + [
  2465|             make_identity_item("view_template.name", vt_name_v, vt_name_q)
  2466|         ]
  2467|         name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  2468|         rec["join_key_name_identity"], _vt_name_key_missing = build_join_key_from_policy(
  2469|             domain_policy=name_key_pol,
  2470|             identity_items=name_key_items,
  2471|             include_optional_items=False,
  2472|             emit_keys_used=True,
  2473|             hash_optional_items=False,
  2474|             emit_items=False,
  2475|             emit_selectors=True,
  2476|         )
  2477|         rec["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _vt_name_key_missing)
  2478| 
  2479|         rec["def_hash"] = def_hash
  2480|         rec["def_signature"] = sig_final
  2481| 
  2482|         records.append(rec)
  2483|         per_hashes.append(def_hash)
  2484|         info["debug_kept"] += 1
  2485| 
  2486|     # Finalize
  2487|     info["names"] = sorted(set(names))
  2488|     info["count"] = len(records)
  2489| 
  2490|     info["records"] = sorted(
  2491|         records,
  2492|         key=lambda r: (
  2493|             safe_str(((r.get("label", {}) or {}).get("display", ""))),
  2494|             safe_str(r.get("record_id", "")),
  2495|         ),
  2496|     )
  2497| 
  2498|     info["signature_hashes_v2"] = sorted(per_hashes_v2)
  2499|     if v2_any_blocked:
  2500|         info["hash_v2"] = None
  2501|     else:
  2502|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  2503| 
  2504|     info["record_rows"] = []
  2505|     try:
  2506|         recs = info.get("records") or []
  2507|         info["record_rows"] = [{
  2508|             "record_key": safe_str(r.get("record_id", "")),
  2509|             "sig_hash":   safe_str(r.get("sig_hash", "")),
  2510|             "name":       safe_str((r.get("label", {}) or {}).get("display", "")),
  2511|             "view_type":  safe_str(((r.get("label", {}) or {}).get("components", {}) or {}).get("view_type", "")),
  2512|         } for r in recs]
  2513|     except Exception:
  2514|         info["record_rows"] = []
  2515| 
  2516|     # PR6: deterministic degraded signaling into contract
  2517|     degraded_reason_counts = {}
  2518| 
  2519|     try:
  2520|         if int(info.get("debug_missing_uid", 0)) > 0:
  2521|             degraded_reason_counts["template_missing_uid"] = int(info.get("debug_missing_uid", 0))
  2522|     except Exception:
  2523|         pass
  2524| 
  2525|     try:
  2526|         if int(info.get("debug_fail_read", 0)) > 0:
  2527|             degraded_reason_counts["api_read_failure"] = int(info.get("debug_fail_read", 0))
  2528|     except Exception:
  2529|         pass
  2530| 
  2531|     try:
  2532|         if int(info.get("debug_view_context_problem", 0)) > 0:
  2533|             for k, vv in dict(info.get("debug_view_context_reasons", {})).items():
  2534|                 key = str(k)
  2535|                 if key.endswith("_not_applicable"):
  2536|                     continue
  2537|                 degraded_reason_counts[key] = int(vv)
  2538|     except Exception:
  2539|         pass
  2540| 
  2541|     try:
  2542|         if int(info.get("debug_v2_blocked", 0)) > 0:
  2543|             degraded_reason_counts["semantic_v2_blocked"] = int(info.get("debug_v2_blocked", 0))
  2544|     except Exception:
  2545|         pass
  2546| 
  2547|     if degraded_reason_counts:
  2548|         info["_domain_status"] = "degraded"
  2549|         info["_domain_diag"] = {
  2550|             "degraded_reasons": sorted(degraded_reason_counts.keys()),
  2551|             "degraded_reason_counts": degraded_reason_counts,
  2552|         }
  2553|     else:
  2554|         info["_domain_status"] = "ok"
  2555|         info["_domain_diag"] = {}
  2556| 
  2557|     return info
```
