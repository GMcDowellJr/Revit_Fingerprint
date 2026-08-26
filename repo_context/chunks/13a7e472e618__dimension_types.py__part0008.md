# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 8 of 8
- Original line range: 2761-3103
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_spot_slope
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
  2761| def extract_spot_slope(doc, ctx=None):
  2762|     _HANDLED_SHAPES = _SPOT_SLOPE_HANDLED
  2763|     EXPECTED_FAMILY = _SPOT_SLOPE_EXPECTED_FAMILY
  2764|     DOMAIN_NAME = "dimension_types_spot_slope"
  2765|     """
  2766|     Extract SpotSlope dimension types fingerprint.
  2767| 
  2768|     Args:
  2769|         doc: Revit Document
  2770|         ctx: Context dictionary
  2771| 
  2772|     Returns:
  2773|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
  2774|     """
  2775|     info = {
  2776|         "count": 0,
  2777|         "raw_count": 0,
  2778|         "records": [],
  2779|         "signature_hashes_v2": [],
  2780|         "hash_v2": None,
  2781|         "debug_v2_blocked": False,
  2782|         "debug_v2_block_reasons": {},
  2783|     }
  2784| 
  2785|     if ctx is None:
  2786|         ctx = {}
  2787| 
  2788|     if DimensionType is None:
  2789|         info["debug_v2_blocked"] = True
  2790|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
  2791|         return info
  2792| 
  2793|     try:
  2794|         all_types = _collect_dim_types(doc, ctx)
  2795|     except Exception:
  2796|         all_types = []
  2797| 
  2798|     info["raw_count"] = len(all_types)
  2799|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
  2800| 
  2801|     v2_records = []
  2802|     v2_sig_hashes = []
  2803|     _eligible_type_count = 0
  2804| 
  2805|     for d in all_types:
  2806|         try:
  2807|             type_name = get_type_display_name(d)
  2808| 
  2809|             # Exclude system built-in types with id-based labels (not user-accessible)
  2810|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
  2811|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
  2812|                 continue
  2813| 
  2814|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
  2815| 
  2816|             # Apply family-name heuristic override
  2817|             shape_v, shape_family, shape_q = _apply_family_name_override(
  2818|                 d, shape_v, shape_family, shape_q, type_name
  2819|             )
  2820| 
  2821|             # Filter: skip shapes not handled by this domain
  2822|             if shape_v not in _HANDLED_SHAPES:
  2823|                 continue
  2824| 
  2825|             # Exclude confirmed wrong-family types (system/infrastructure types)
  2826|             family_name = None
  2827|             try:
  2828|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
  2829|                 if p_fam:
  2830|                     family_name = _as_string(p_fam)
  2831|                     if family_name:
  2832|                         family_name = canon_str(family_name)
  2833|             except Exception:
  2834|                 pass
  2835|             if family_name and family_name != EXPECTED_FAMILY:
  2836|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
  2837|                 continue
  2838| 
  2839|             _eligible_type_count += 1
  2840| 
  2841|             # --- Read core identity fields ---
  2842| 
  2843|             # Unit format info
  2844|             (unit_format_id_v, unit_format_id_q,
  2845|              _rounding_v, _rounding_q,
  2846|              _accuracy_v, _accuracy_q,
  2847|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
  2848| 
  2849|             # Slope Direction / Read Convention (storage=Integer/enum — use AsValueString)
  2850|             slope_direction_v, slope_direction_q = (None, ITEM_Q_MISSING)
  2851|             try:
  2852|                 p_sd = first_param(d, ui_names=["Slope Direction", "Read Convention"])
  2853|                 # Probe confirms storage=Integer (display='Down') — must use AsValueString
  2854|                 sd_raw = _as_value_string(p_sd) if p_sd is not None else None
  2855|                 slope_direction_v, slope_direction_q = canonicalize_str(sd_raw)
  2856|             except Exception:
  2857|                 slope_direction_v, slope_direction_q = (None, ITEM_Q_UNREADABLE)
  2858| 
  2859|             # Leader Line Length (stored in feet, convert to inches)
  2860|             leader_line_length_v, leader_line_length_q = (None, ITEM_Q_MISSING)
  2861|             try:
  2862|                 p_lll = first_param(d, ui_names=["Leader Line Length"])
  2863|                 lll_ft = _as_double(p_lll) if p_lll is not None else None
  2864|                 if lll_ft is not None:
  2865|                     leader_line_length_v, leader_line_length_q = canonicalize_float(_fmt_in_from_ft(lll_ft))
  2866|                 else:
  2867|                     leader_line_length_v, leader_line_length_q = (None, ITEM_Q_MISSING)
  2868|             except Exception:
  2869|                 leader_line_length_v, leader_line_length_q = (None, ITEM_Q_UNREADABLE)
  2870| 
  2871|             # --- Area 7 §1: Leader Arrowhead cluster (shared helper) ---
  2872|             (leader_arrowhead_uid_v, leader_arrowhead_uid_q,
  2873|              leader_arrowhead_name_v, leader_arrowhead_name_q,
  2874|              leader_arrowhead_sig_hash_v, leader_arrowhead_sig_hash_q) = _read_leader_arrowhead(d, ctx, doc)
  2875|             leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = (None, ITEM_Q_MISSING)
  2876|             try:
  2877|                 p_alw = first_param(d, ui_names=["Leader Arrowhead Line Weight"])
  2878|                 alw_int = _as_int(p_alw) if p_alw is not None else None
  2879|                 leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = canonicalize_int(alw_int)
  2880|             except Exception:
  2881|                 leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = (None, ITEM_Q_UNREADABLE)
  2882|             leader_line_weight_v, leader_line_weight_q = (None, ITEM_Q_MISSING)
  2883|             try:
  2884|                 p_llw = first_param(d, ui_names=["Leader Line Weight"])
  2885|                 llw_int = _as_int(p_llw) if p_llw is not None else None
  2886|                 leader_line_weight_v, leader_line_weight_q = canonicalize_int(llw_int)
  2887|             except Exception:
  2888|                 leader_line_weight_v, leader_line_weight_q = (None, ITEM_Q_UNREADABLE)
  2889| 
  2890|             # --- Area 7 §7: Rotate with Component / Text Offset from Leader (spot family) ---
  2891|             # Coordinate Base/Elevation Base don't apply to Spot Slope (spot_coordinate/
  2892|             # spot_elevation only). Text Offset from Symbol also not observed on Spot Slope
  2893|             # in probe data (consistent with Spot Slope having no "Symbol" field either --
  2894|             # see the absence of a symbol_name identity item in this domain).
  2895|             rotate_with_component_v, rotate_with_component_q = (None, ITEM_Q_MISSING)
  2896|             try:
  2897|                 p_rwc = first_param(d, ui_names=["Rotate with Component"])
  2898|                 rwc_int = _as_int(p_rwc) if p_rwc is not None else None
  2899|                 rotate_with_component_v, rotate_with_component_q = canonicalize_bool(rwc_int)
  2900|             except Exception:
  2901|                 rotate_with_component_v, rotate_with_component_q = (None, ITEM_Q_UNREADABLE)
  2902|             text_offset_from_leader_v, text_offset_from_leader_q = (None, ITEM_Q_MISSING)
  2903|             try:
  2904|                 p_tofl = first_param(d, ui_names=["Text Offset from Leader"])
  2905|                 tofl_ft = _as_double(p_tofl) if p_tofl is not None else None
  2906|                 text_offset_from_leader_v, text_offset_from_leader_q = canonicalize_float(_fmt_in_from_ft(tofl_ft))
  2907|             except Exception:
  2908|                 text_offset_from_leader_v, text_offset_from_leader_q = (None, ITEM_Q_UNREADABLE)
  2909| 
  2910|             # --- Build identity items ---
  2911|             core_items = [
  2912|                 make_identity_item("dim_type.shape", shape_v, shape_q),
  2913|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
  2914|                 make_identity_item("dim_type.slope_direction", slope_direction_v, slope_direction_q),
  2915|                 make_identity_item("dim_type.leader_line_length", leader_line_length_v, leader_line_length_q),
  2916|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
  2917|                 make_identity_item("dim_type.leader_arrowhead_uid", leader_arrowhead_uid_v, leader_arrowhead_uid_q),
  2918|                 make_identity_item("dim_type.leader_arrowhead_name", leader_arrowhead_name_v, leader_arrowhead_name_q),
  2919|                 make_identity_item("dim_type.leader_arrowhead_sig_hash", leader_arrowhead_sig_hash_v, leader_arrowhead_sig_hash_q),
  2920|                 make_identity_item("dim_type.leader_arrowhead_line_weight", leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q),
  2921|                 make_identity_item("dim_type.leader_line_weight", leader_line_weight_v, leader_line_weight_q),
  2922|                 make_identity_item("dim_type.rotate_with_component", rotate_with_component_v, rotate_with_component_q),
  2923|                 make_identity_item("dim_type.text_offset_from_leader_in", text_offset_from_leader_v, text_offset_from_leader_q),
  2924|             ]
  2925| 
  2926|             text_items = _build_text_appearance_items(d)
  2927|             alt_units_items = _build_alternate_units_items(d)
  2928|             all_items = core_items + text_items + alt_units_items
  2929| 
  2930|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
  2931| 
  2932|             # Required qualities for blocking
  2933|             # leader_line_length is optional enrichment — not blocking
  2934|             # All Area 7 additions (leader arrowhead cluster, alternate units,
  2935|             # suppress_spaces, rotate_with_component, text_offset_from_leader) are
  2936|             # likewise non-blocking enrichment.
  2937|             required_qs = [
  2938|                 shape_q,
  2939|                 unit_format_id_q,
  2940|                 slope_direction_q,
  2941|             ]
  2942|             # text/appearance fields are cross-family alignment, not primary identity — not blocking
  2943| 
  2944|             blocked = any(q != ITEM_Q_OK for q in required_qs)
  2945| 
  2946|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
  2947|                 "dim_type.leader_arrowhead_uid",
  2948|                 "dim_type.leader_arrowhead_name",
  2949|                 "dim_type.leader_arrowhead_sig_hash",
  2950|             })
  2951| 
  2952|             status_reasons = []
  2953|             for it in identity_items:
  2954|                 q = it.get("q")
  2955|                 k = it.get("k", "")
  2956|                 if q == ITEM_Q_OK:
  2957|                     continue
  2958|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
  2959|                     continue
  2960|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
  2961| 
  2962|             if blocked:
  2963|                 status = STATUS_BLOCKED
  2964|             elif status_reasons:
  2965|                 status = STATUS_DEGRADED
  2966|             else:
  2967|                 status = STATUS_OK
  2968| 
  2969|             # dim_type.leader_arrowhead_uid/_name are file-local/cosmetic metadata
  2970|             # (D-004 restricts UniqueId use to element-backed identities; names are
  2971|             # metadata only per the Hash Semantics rule) -- kept in identity_items for
  2972|             # governance/join-key visibility but excluded from the sig_hash preimage
  2973|             # itself, matching the contract's sig_hash_keys pin for these 3 domains.
  2974|             # Without this, two files with a semantically-identical spot dimension
  2975|             # type (same arrowhead style/name) would hash differently purely because
  2976|             # Revit UniqueIds are per-file-random (PR #412 review).
  2977|             _SIG_HASH_EXCLUDED_KEYS = frozenset({
  2978|                 "dim_type.leader_arrowhead_uid",
  2979|                 "dim_type.leader_arrowhead_name",
  2980|             })
  2981|             sig_hash_items = [it for it in identity_items if it.get("k") not in _SIG_HASH_EXCLUDED_KEYS]
  2982|             preimage = serialize_identity_items(sig_hash_items)
  2983|             sig_hash = None if blocked else make_hash(preimage)
  2984| 
  2985|             try:
  2986|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
  2987|             except Exception:
  2988|                 type_id_int = None
  2989| 
  2990|             try:
  2991|                 uid_raw = getattr(d, "UniqueId", None)
  2992|             except Exception:
  2993|                 uid_raw = None
  2994| 
  2995|             label_str = type_name
  2996|             rec_v2 = build_record_v2(
  2997|                 domain=DOMAIN_NAME,
  2998|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
  2999|                 status=status,
  3000|                 status_reasons=sorted(set(status_reasons)),
  3001|                 sig_hash=sig_hash,
  3002|                 identity_items=identity_items,
  3003|                 required_qs=tuple(required_qs),
  3004|                 label={
  3005|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
  3006|                     "quality": "human" if label_str else "placeholder_missing",
  3007|                     "provenance": "revit.DimensionType.params",
  3008|                 },
  3009|             )
  3010|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
  3011|             rec_v2["is_purgeable"] = _ip
  3012|             rec_v2["is_purgeable_q"] = _ip_q
  3013|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
  3014| 
  3015|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  3016|             rec_v2["join_key"], _missing = build_join_key_from_policy(
  3017|                 domain_policy=pol,
  3018|                 identity_items=identity_items,
  3019|                 include_optional_items=False,
  3020|                 emit_keys_used=True,
  3021|                 hash_optional_items=False,
  3022|                 emit_items=False,
  3023|                 emit_selectors=True,
  3024|             )
  3025| 
  3026|             # Canonical Name Identity Projection (PR1): second, independent join_hash
  3027|             # variant keyed off this record's own label.display-backing item
  3028|             # (dim_type.name). dim_type.name does not exist anywhere in this file --
  3029|             # type_name/label_str feeds label.display only. Widened items list used
  3030|             # only for this call; identity_basis.items/sig_hash/join_key above are
  3031|             # unaffected. (dimension_types_spot_coordinate/spot_elevation are excluded
  3032|             # from the name-key policy entirely -- their only other name-shaped item,
  3033|             # dim_type.symbol_name, names a different, referenced tick-mark/leader
  3034|             # symbol element, not this record's own label.)
  3035|             dt_name_v, dt_name_q = canonicalize_str(type_name)
  3036|             name_key_items = identity_items + [
  3037|                 make_identity_item("dim_type.name", dt_name_v, dt_name_q)
  3038|             ]
  3039|             name_key_pol = get_domain_join_key_policy((ctx or {}).get("name_key_policies"), DOMAIN_NAME)
  3040|             rec_v2["join_key_name_identity"], _name_key_missing = build_join_key_from_policy(
  3041|                 domain_policy=name_key_pol,
  3042|                 identity_items=name_key_items,
  3043|                 include_optional_items=False,
  3044|                 emit_keys_used=True,
  3045|                 hash_optional_items=False,
  3046|                 emit_items=False,
  3047|                 emit_selectors=True,
  3048|             )
  3049|             rec_v2["join_key_name_identity"]["status"] = compute_projection_status(name_key_pol, _name_key_missing)
  3050| 
  3051|             coordination_items = [
  3052|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
  3053|             ]
  3054| 
  3055|             unknown_items = []
  3056|             try:
  3057|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
  3058|             except Exception:
  3059|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  3060|             try:
  3061|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
  3062|             except Exception:
  3063|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  3064|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
  3065|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
  3066| 
  3067|             rec_v2["phase2"] = {
  3068|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  3069|                 "grouping_basis": "phase2.hypothesis",
  3070|                 "cosmetic_items": phase2_sorted_items([]),
  3071|                 "coordination_items": phase2_sorted_items(coordination_items),
  3072|                 "unknown_items": phase2_sorted_items(unknown_items),
  3073|             }
  3074| 
  3075|             if sig_hash:
  3076|                 v2_sig_hashes.append(sig_hash)
  3077|             v2_records.append(rec_v2)
  3078| 
  3079|         except Exception:
  3080|             continue  # fail-soft per record
  3081| 
  3082|     _total_type_count = _eligible_type_count
  3083|     for rec in v2_records:
  3084|         try:
  3085|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
  3086|             rec["is_sole_type_in_category_q"] = "ok"
  3087|         except Exception:
  3088|             rec["is_sole_type_in_category"] = None
  3089|             rec["is_sole_type_in_category_q"] = "unreadable"
  3090| 
  3091|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
  3092|     info["count"] = len(v2_records)
  3093|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  3094| 
  3095|     if v2_sig_hashes:
  3096|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  3097|         info["debug_v2_blocked"] = False
  3098|     else:
  3099|         info["hash_v2"] = None
  3100|         info["debug_v2_blocked"] = True
  3101|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
  3102| 
  3103|     return info
```
