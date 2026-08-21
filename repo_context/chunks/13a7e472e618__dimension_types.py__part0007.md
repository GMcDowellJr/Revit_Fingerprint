# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 7 of 8
- Original line range: 2330-2760
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_spot_coordinate, _apply_family_name_override
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
  2330| def extract_spot_coordinate(doc, ctx=None):
  2331|     _HANDLED_SHAPES = _SPOT_COORD_HANDLED
  2332|     EXPECTED_FAMILY = _SPOT_COORD_EXPECTED_FAMILY
  2333|     DOMAIN_NAME = "dimension_types_spot_coordinate"
  2334|     ACCEPTED_FAMILIES = frozenset({
  2335|         "Spot Coordinates",
  2336|         "Alignment Station Labels",
  2337|     })
  2338|     """
  2339|     Extract SpotCoordinate dimension types fingerprint.
  2340| 
  2341|     Args:
  2342|         doc: Revit Document
  2343|         ctx: Context dictionary
  2344| 
  2345|     Returns:
  2346|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
  2347|     """
  2348|     info = {
  2349|         "count": 0,
  2350|         "raw_count": 0,
  2351|         "records": [],
  2352|         "signature_hashes_v2": [],
  2353|         "hash_v2": None,
  2354|         "debug_v2_blocked": False,
  2355|         "debug_v2_block_reasons": {},
  2356|     }
  2357| 
  2358|     if ctx is None:
  2359|         ctx = {}
  2360| 
  2361|     if DimensionType is None:
  2362|         info["debug_v2_blocked"] = True
  2363|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
  2364|         return info
  2365| 
  2366|     try:
  2367|         all_types = _collect_dim_types(doc, ctx)
  2368|     except Exception:
  2369|         all_types = []
  2370| 
  2371|     info["raw_count"] = len(all_types)
  2372|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
  2373| 
  2374|     v2_records = []
  2375|     v2_sig_hashes = []
  2376|     _eligible_type_count = 0
  2377| 
  2378|     for d in all_types:
  2379|         try:
  2380|             type_name = get_type_display_name(d)
  2381| 
  2382|             # Exclude system built-in types with id-based labels (not user-accessible)
  2383|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
  2384|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
  2385|                 continue
  2386| 
  2387|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
  2388| 
  2389|             # Apply family-name heuristic override
  2390|             shape_v, shape_family, shape_q = _apply_family_name_override(
  2391|                 d, shape_v, shape_family, shape_q, type_name
  2392|             )
  2393| 
  2394|             # Filter: skip shapes not handled by this domain
  2395|             if shape_v not in _HANDLED_SHAPES:
  2396|                 continue
  2397| 
  2398|             # Exclude confirmed wrong-family types (system/infrastructure types)
  2399|             family_name = None
  2400|             try:
  2401|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
  2402|                 if p_fam:
  2403|                     family_name = _as_string(p_fam)
  2404|                     if family_name:
  2405|                         family_name = canon_str(family_name)
  2406|             except Exception:
  2407|                 pass
  2408|             if family_name and family_name not in ACCEPTED_FAMILIES:
  2409|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
  2410|                 continue
  2411| 
  2412|             _eligible_type_count += 1
  2413| 
  2414|             # --- Read core identity fields ---
  2415| 
  2416|             # Unit format info
  2417|             (unit_format_id_v, unit_format_id_q,
  2418|              _rounding_v, _rounding_q,
  2419|              _accuracy_v, _accuracy_q,
  2420|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
  2421| 
  2422|             # Top Coordinate (storage=Integer/enum, display='North / South' — use AsValueString)
  2423|             top_coordinate_v, top_coordinate_q = (None, ITEM_Q_MISSING)
  2424|             try:
  2425|                 p_tc = first_param(d, ui_names=["Top Coordinate", "Top Value"])
  2426|                 tc_raw = _as_value_string(p_tc) if p_tc is not None else None
  2427|                 top_coordinate_v, top_coordinate_q = canonicalize_str(tc_raw)
  2428|             except Exception:
  2429|                 top_coordinate_v, top_coordinate_q = (None, ITEM_Q_UNREADABLE)
  2430| 
  2431|             # Bottom Coordinate (storage=Integer/enum, display='East / West' — use AsValueString)
  2432|             bottom_coordinate_v, bottom_coordinate_q = (None, ITEM_Q_MISSING)
  2433|             try:
  2434|                 p_bc = first_param(d, ui_names=["Bottom Coordinate", "Bottom Value"])
  2435|                 bc_raw = _as_value_string(p_bc) if p_bc is not None else None
  2436|                 bottom_coordinate_v, bottom_coordinate_q = canonicalize_str(bc_raw)
  2437|             except Exception:
  2438|                 bottom_coordinate_v, bottom_coordinate_q = (None, ITEM_Q_UNREADABLE)
  2439| 
  2440|             # N/S Indicator
  2441|             north_south_indicator_v, north_south_indicator_q = (None, ITEM_Q_MISSING)
  2442|             try:
  2443|                 p_ns = first_param(d, ui_names=["North / South Indicator", "N/S Indicator"])
  2444|                 ns_raw = _as_string(p_ns) if p_ns is not None else None
  2445|                 north_south_indicator_v, north_south_indicator_q = canonicalize_str_allow_empty(ns_raw)
  2446|             except Exception:
  2447|                 north_south_indicator_v, north_south_indicator_q = (None, ITEM_Q_UNREADABLE)
  2448| 
  2449|             # E/W Indicator
  2450|             east_west_indicator_v, east_west_indicator_q = (None, ITEM_Q_MISSING)
  2451|             try:
  2452|                 p_ew = first_param(d, ui_names=["East / West Indicator", "E/W Indicator"])
  2453|                 ew_raw = _as_string(p_ew) if p_ew is not None else None
  2454|                 east_west_indicator_v, east_west_indicator_q = canonicalize_str_allow_empty(ew_raw)
  2455|             except Exception:
  2456|                 east_west_indicator_v, east_west_indicator_q = (None, ITEM_Q_UNREADABLE)
  2457| 
  2458|             # Include Elevation
  2459|             include_elevation_v, include_elevation_q = (None, ITEM_Q_MISSING)
  2460|             try:
  2461|                 p_ie = first_param(d, ui_names=["Include Elevation"])
  2462|                 ie_int = _as_int(p_ie) if p_ie is not None else None
  2463|                 include_elevation_v, include_elevation_q = canonicalize_bool(ie_int)
  2464|             except Exception:
  2465|                 include_elevation_v, include_elevation_q = (None, ITEM_Q_UNREADABLE)
  2466| 
  2467|             # Elevation Indicator
  2468|             elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_MISSING)
  2469|             try:
  2470|                 p_ei = first_param(d, ui_names=["Elevation Indicator"])
  2471|                 ei_raw = _as_string(p_ei) if p_ei is not None else None
  2472|                 elevation_indicator_v, elevation_indicator_q = canonicalize_str_allow_empty(ei_raw)
  2473|             except Exception:
  2474|                 elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_UNREADABLE)
  2475| 
  2476|             # Indicator as Prefix/Suffix
  2477|             indicator_prefix_v, indicator_prefix_q = (None, ITEM_Q_MISSING)
  2478|             try:
  2479|                 p_ip = first_param(d, ui_names=["Indicator as Prefix / Suffix", "Indicator as Prefix/Suffix"])
  2480|                 ip_int = _as_int(p_ip) if p_ip is not None else None
  2481|                 indicator_prefix_v, indicator_prefix_q = canonicalize_bool(ip_int)
  2482|             except Exception:
  2483|                 indicator_prefix_v, indicator_prefix_q = (None, ITEM_Q_UNREADABLE)
  2484| 
  2485|             # Text Orientation (storage=Integer/enum — use AsValueString)
  2486|             text_orientation_v, text_orientation_q = (None, ITEM_Q_MISSING)
  2487|             try:
  2488|                 p_to = first_param(d, ui_names=["Text Orientation"])
  2489|                 to_raw = _as_value_string(p_to) if p_to is not None else None
  2490|                 text_orientation_v, text_orientation_q = canonicalize_str(to_raw)
  2491|             except Exception:
  2492|                 text_orientation_v, text_orientation_q = (None, ITEM_Q_UNREADABLE)
  2493| 
  2494|             # Text Location (storage=Integer/enum — use AsValueString; probe name is "Text Location")
  2495|             text_location_v, text_location_q = (None, ITEM_Q_MISSING)
  2496|             try:
  2497|                 p_tl = first_param(d, ui_names=["Text Location", "Note Location"])
  2498|                 tl_raw = _as_value_string(p_tl) if p_tl is not None else None
  2499|                 text_location_v, text_location_q = canonicalize_str(tl_raw)
  2500|             except Exception:
  2501|                 text_location_v, text_location_q = (None, ITEM_Q_UNREADABLE)
  2502| 
  2503|             # Symbol name (ElementId resolved to name; no ctx map available for sig_hash)
  2504|             symbol_name_v, symbol_name_q = _read_symbol_name(d, doc)
  2505| 
  2506|             # --- Area 7 §1: Leader Arrowhead cluster (shared helper) ---
  2507|             (leader_arrowhead_uid_v, leader_arrowhead_uid_q,
  2508|              leader_arrowhead_name_v, leader_arrowhead_name_q,
  2509|              leader_arrowhead_sig_hash_v, leader_arrowhead_sig_hash_q) = _read_leader_arrowhead(d, ctx, doc)
  2510|             leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = (None, ITEM_Q_MISSING)
  2511|             try:
  2512|                 p_alw = first_param(d, ui_names=["Leader Arrowhead Line Weight"])
  2513|                 alw_int = _as_int(p_alw) if p_alw is not None else None
  2514|                 leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = canonicalize_int(alw_int)
  2515|             except Exception:
  2516|                 leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = (None, ITEM_Q_UNREADABLE)
  2517|             leader_line_weight_v, leader_line_weight_q = (None, ITEM_Q_MISSING)
  2518|             try:
  2519|                 p_llw = first_param(d, ui_names=["Leader Line Weight"])
  2520|                 llw_int = _as_int(p_llw) if p_llw is not None else None
  2521|                 leader_line_weight_v, leader_line_weight_q = canonicalize_int(llw_int)
  2522|             except Exception:
  2523|                 leader_line_weight_v, leader_line_weight_q = (None, ITEM_Q_UNREADABLE)
  2524| 
  2525|             # --- Area 7 §7: Rotate with Component / Coordinate Base / Text Offsets (spot family) ---
  2526|             rotate_with_component_v, rotate_with_component_q = (None, ITEM_Q_MISSING)
  2527|             try:
  2528|                 p_rwc = first_param(d, ui_names=["Rotate with Component"])
  2529|                 rwc_int = _as_int(p_rwc) if p_rwc is not None else None
  2530|                 rotate_with_component_v, rotate_with_component_q = canonicalize_bool(rwc_int)
  2531|             except Exception:
  2532|                 rotate_with_component_v, rotate_with_component_q = (None, ITEM_Q_UNREADABLE)
  2533|             coordinate_base_v, coordinate_base_q = (None, ITEM_Q_MISSING)
  2534|             try:
  2535|                 p_cb = first_param(d, ui_names=["Coordinate Base"])
  2536|                 cb_raw = _as_value_string(p_cb) if p_cb is not None else None
  2537|                 coordinate_base_v, coordinate_base_q = canonicalize_str(cb_raw)
  2538|             except Exception:
  2539|                 coordinate_base_v, coordinate_base_q = (None, ITEM_Q_UNREADABLE)
  2540|             text_offset_from_leader_v, text_offset_from_leader_q = (None, ITEM_Q_MISSING)
  2541|             try:
  2542|                 p_tofl = first_param(d, ui_names=["Text Offset from Leader"])
  2543|                 tofl_ft = _as_double(p_tofl) if p_tofl is not None else None
  2544|                 text_offset_from_leader_v, text_offset_from_leader_q = canonicalize_float(_fmt_in_from_ft(tofl_ft))
  2545|             except Exception:
  2546|                 text_offset_from_leader_v, text_offset_from_leader_q = (None, ITEM_Q_UNREADABLE)
  2547|             text_offset_from_symbol_v, text_offset_from_symbol_q = (None, ITEM_Q_MISSING)
  2548|             try:
  2549|                 p_tofs = first_param(d, ui_names=["Text Offset from Symbol"])
  2550|                 tofs_ft = _as_double(p_tofs) if p_tofs is not None else None
  2551|                 text_offset_from_symbol_v, text_offset_from_symbol_q = canonicalize_float(_fmt_in_from_ft(tofs_ft))
  2552|             except Exception:
  2553|                 text_offset_from_symbol_v, text_offset_from_symbol_q = (None, ITEM_Q_UNREADABLE)
  2554| 
  2555|             # --- Build identity items ---
  2556|             core_items = [
  2557|                 make_identity_item("dim_type.shape", shape_v, shape_q),
  2558|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
  2559|                 make_identity_item("dim_type.top_coordinate", top_coordinate_v, top_coordinate_q),
  2560|                 make_identity_item("dim_type.bottom_coordinate", bottom_coordinate_v, bottom_coordinate_q),
  2561|                 make_identity_item("dim_type.north_south_indicator", north_south_indicator_v, north_south_indicator_q),
  2562|                 make_identity_item("dim_type.east_west_indicator", east_west_indicator_v, east_west_indicator_q),
  2563|                 make_identity_item("dim_type.include_elevation", include_elevation_v, include_elevation_q),
  2564|                 make_identity_item("dim_type.elevation_indicator", elevation_indicator_v, elevation_indicator_q),
  2565|                 make_identity_item("dim_type.indicator_as_prefix_suffix", indicator_prefix_v, indicator_prefix_q),
  2566|                 make_identity_item("dim_type.text_orientation", text_orientation_v, text_orientation_q),
  2567|                 make_identity_item("dim_type.text_location", text_location_v, text_location_q),
  2568|                 make_identity_item("dim_type.symbol_name", symbol_name_v, symbol_name_q),
  2569|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
  2570|                 make_identity_item("dim_type.leader_arrowhead_uid", leader_arrowhead_uid_v, leader_arrowhead_uid_q),
  2571|                 make_identity_item("dim_type.leader_arrowhead_name", leader_arrowhead_name_v, leader_arrowhead_name_q),
  2572|                 make_identity_item("dim_type.leader_arrowhead_sig_hash", leader_arrowhead_sig_hash_v, leader_arrowhead_sig_hash_q),
  2573|                 make_identity_item("dim_type.leader_arrowhead_line_weight", leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q),
  2574|                 make_identity_item("dim_type.leader_line_weight", leader_line_weight_v, leader_line_weight_q),
  2575|                 make_identity_item("dim_type.rotate_with_component", rotate_with_component_v, rotate_with_component_q),
  2576|                 make_identity_item("dim_type.coordinate_base", coordinate_base_v, coordinate_base_q),
  2577|                 make_identity_item("dim_type.text_offset_from_leader_in", text_offset_from_leader_v, text_offset_from_leader_q),
  2578|                 make_identity_item("dim_type.text_offset_from_symbol_in", text_offset_from_symbol_v, text_offset_from_symbol_q),
  2579|             ]
  2580| 
  2581|             text_items = _build_text_appearance_items(d)
  2582|             alt_units_items = _build_alternate_units_items(d)
  2583|             all_items = core_items + text_items + alt_units_items
  2584| 
  2585|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
  2586| 
  2587|             # Required qualities for blocking
  2588|             # include_elevation, elevation_indicator, indicator_prefix, symbol_name are optional — not blocking
  2589|             # All Area 7 additions (leader arrowhead cluster, alternate units,
  2590|             # suppress_spaces, rotate_with_component, coordinate_base, text offsets)
  2591|             # are likewise non-blocking enrichment.
  2592|             required_qs = [
  2593|                 shape_q,
  2594|                 unit_format_id_q,
  2595|                 top_coordinate_q,
  2596|                 bottom_coordinate_q,
  2597|                 north_south_indicator_q,
  2598|                 east_west_indicator_q,
  2599|                 text_orientation_q,
  2600|                 text_location_q,
  2601|             ]
  2602|             # text/appearance fields are cross-family alignment, not primary identity — not blocking
  2603| 
  2604|             blocked = any(q != ITEM_Q_OK for q in required_qs)
  2605| 
  2606|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
  2607|                 "dim_type.leader_arrowhead_uid",
  2608|                 "dim_type.leader_arrowhead_name",
  2609|                 "dim_type.leader_arrowhead_sig_hash",
  2610|             })
  2611| 
  2612|             status_reasons = []
  2613|             for it in identity_items:
  2614|                 q = it.get("q")
  2615|                 k = it.get("k", "")
  2616|                 if q == ITEM_Q_OK:
  2617|                     continue
  2618|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
  2619|                     continue
  2620|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
  2621| 
  2622|             if blocked:
  2623|                 status = STATUS_BLOCKED
  2624|             elif status_reasons:
  2625|                 status = STATUS_DEGRADED
  2626|             else:
  2627|                 status = STATUS_OK
  2628| 
  2629|             # dim_type.leader_arrowhead_uid/_name are file-local/cosmetic metadata
  2630|             # (D-004 restricts UniqueId use to element-backed identities; names are
  2631|             # metadata only per the Hash Semantics rule) -- kept in identity_items for
  2632|             # governance/join-key visibility but excluded from the sig_hash preimage
  2633|             # itself, matching the contract's sig_hash_keys pin for these 3 domains.
  2634|             # Without this, two files with a semantically-identical spot dimension
  2635|             # type (same arrowhead style/name) would hash differently purely because
  2636|             # Revit UniqueIds are per-file-random (PR #412 review).
  2637|             _SIG_HASH_EXCLUDED_KEYS = frozenset({
  2638|                 "dim_type.leader_arrowhead_uid",
  2639|                 "dim_type.leader_arrowhead_name",
  2640|             })
  2641|             sig_hash_items = [it for it in identity_items if it.get("k") not in _SIG_HASH_EXCLUDED_KEYS]
  2642|             preimage = serialize_identity_items(sig_hash_items)
  2643|             sig_hash = None if blocked else make_hash(preimage)
  2644| 
  2645|             try:
  2646|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
  2647|             except Exception:
  2648|                 type_id_int = None
  2649| 
  2650|             try:
  2651|                 uid_raw = getattr(d, "UniqueId", None)
  2652|             except Exception:
  2653|                 uid_raw = None
  2654| 
  2655|             label_str = type_name
  2656|             rec_v2 = build_record_v2(
  2657|                 domain=DOMAIN_NAME,
  2658|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
  2659|                 status=status,
  2660|                 status_reasons=sorted(set(status_reasons)),
  2661|                 sig_hash=sig_hash,
  2662|                 identity_items=identity_items,
  2663|                 required_qs=tuple(required_qs),
  2664|                 label={
  2665|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
  2666|                     "quality": "human" if label_str else "placeholder_missing",
  2667|                     "provenance": "revit.DimensionType.params",
  2668|                 },
  2669|             )
  2670|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
  2671|             rec_v2["is_purgeable"] = _ip
  2672|             rec_v2["is_purgeable_q"] = _ip_q
  2673|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
  2674| 
  2675|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  2676|             rec_v2["join_key"], _missing = build_join_key_from_policy(
  2677|                 domain_policy=pol,
  2678|                 identity_items=identity_items,
  2679|                 include_optional_items=False,
  2680|                 emit_keys_used=True,
  2681|                 hash_optional_items=False,
  2682|                 emit_items=False,
  2683|                 emit_selectors=True,
  2684|             )
  2685| 
  2686|             coordination_items = [
  2687|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
  2688|             ]
  2689| 
  2690|             unknown_items = []
  2691|             try:
  2692|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
  2693|             except Exception:
  2694|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  2695|             try:
  2696|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
  2697|             except Exception:
  2698|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  2699|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
  2700|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
  2701| 
  2702|             rec_v2["phase2"] = {
  2703|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  2704|                 "grouping_basis": "phase2.hypothesis",
  2705|                 "cosmetic_items": phase2_sorted_items([]),
  2706|                 "coordination_items": phase2_sorted_items(coordination_items),
  2707|                 "unknown_items": phase2_sorted_items(unknown_items),
  2708|             }
  2709| 
  2710|             if sig_hash:
  2711|                 v2_sig_hashes.append(sig_hash)
  2712|             v2_records.append(rec_v2)
  2713| 
  2714|         except Exception:
  2715|             continue  # fail-soft per record
  2716| 
  2717|     _total_type_count = _eligible_type_count
  2718|     for rec in v2_records:
  2719|         try:
  2720|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
  2721|             rec["is_sole_type_in_category_q"] = "ok"
  2722|         except Exception:
  2723|             rec["is_sole_type_in_category"] = None
  2724|             rec["is_sole_type_in_category_q"] = "unreadable"
  2725| 
  2726|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
  2727|     info["count"] = len(v2_records)
  2728|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  2729| 
  2730|     if v2_sig_hashes:
  2731|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  2732|         info["debug_v2_blocked"] = False
  2733|     else:
  2734|         info["hash_v2"] = None
  2735|         info["debug_v2_blocked"] = True
  2736|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
  2737| 
  2738|     return info
  2739| 
  2740| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
  2741|     """
  2742|     Heuristic override: use FamilyName prefix to more precisely classify Spot types.
  2743|     Returns updated (shape_v, shape_family, shape_q).
  2744|     """
  2745|     try:
  2746|         family_name = getattr(d, "FamilyName", None)
  2747|         basis = family_name if family_name else type_name
  2748|         bn_l = safe_str(basis).strip().lower()
  2749| 
  2750|         if bn_l.startswith("spot slopes"):
  2751|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
  2752|         elif bn_l.startswith("spot elevations"):
  2753|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
  2754|         elif bn_l.startswith("spot coordinates"):
  2755|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
  2756|     except Exception:
  2757|         pass
  2758|     return (shape_v, shape_family, shape_q)
  2759| 
  2760| 
```
