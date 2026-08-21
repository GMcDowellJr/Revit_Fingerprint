# Chunk of domains/dimension_types.py

- Source relative path: `domains/dimension_types.py`
- Chunk: 6 of 8
- Original line range: 1871-2329
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: extract_spot_elevation, _apply_family_name_override, _read_symbol_name
- Source SHA-256: 29cea2f388ccdc1ff2966274109704ce2ee7520daee1439183b6ad89017586ab
- Starts inside symbol: no
- Ends inside symbol: no

```
  1871| def extract_spot_elevation(doc, ctx=None):
  1872|     _HANDLED_SHAPES = _SPOT_ELEV_HANDLED
  1873|     EXPECTED_FAMILY = _SPOT_ELEV_EXPECTED_FAMILY
  1874|     DOMAIN_NAME = "dimension_types_spot_elevation"
  1875|     """
  1876|     Extract SpotElevation and SpotElevationFixed dimension types fingerprint.
  1877| 
  1878|     Args:
  1879|         doc: Revit Document
  1880|         ctx: Context dictionary
  1881| 
  1882|     Returns:
  1883|         Dictionary with count, hash_v2, records, signature_hashes_v2, debug counters
  1884|     """
  1885|     info = {
  1886|         "count": 0,
  1887|         "raw_count": 0,
  1888|         "records": [],
  1889|         "signature_hashes_v2": [],
  1890|         "hash_v2": None,
  1891|         "debug_v2_blocked": False,
  1892|         "debug_v2_block_reasons": {},
  1893|     }
  1894| 
  1895|     if ctx is None:
  1896|         ctx = {}
  1897| 
  1898|     if DimensionType is None:
  1899|         info["debug_v2_blocked"] = True
  1900|         info["debug_v2_block_reasons"] = {"api_unreachable": True}
  1901|         return info
  1902| 
  1903|     try:
  1904|         all_types = _collect_dim_types(doc, ctx)
  1905|     except Exception:
  1906|         all_types = []
  1907| 
  1908|     info["raw_count"] = len(all_types)
  1909|     _instance_count_map, _instance_count_map_q = _build_dimension_instance_count_map(doc, ctx)
  1910| 
  1911|     v2_records = []
  1912|     v2_sig_hashes = []
  1913|     _eligible_type_count = 0
  1914| 
  1915|     for d in all_types:
  1916|         try:
  1917|             type_name = get_type_display_name(d)
  1918| 
  1919|             # Exclude system built-in types with id-based labels (not user-accessible)
  1920|             if type_name is None or (isinstance(type_name, str) and ":id:" in type_name):
  1921|                 info["debug_system_types_excluded"] = info.get("debug_system_types_excluded", 0) + 1
  1922|                 continue
  1923| 
  1924|             shape_v, shape_family, shape_q = _get_dimension_shape(d)
  1925| 
  1926|             # Apply family-name heuristic override
  1927|             shape_v, shape_family, shape_q = _apply_family_name_override(
  1928|                 d, shape_v, shape_family, shape_q, type_name
  1929|             )
  1930| 
  1931|             # Filter: skip shapes not handled by this domain
  1932|             if shape_v not in _HANDLED_SHAPES:
  1933|                 continue
  1934| 
  1935|             # Exclude confirmed wrong-family types (e.g. Diameter types misrouted via SpotElevationFixed)
  1936|             family_name = None
  1937|             try:
  1938|                 p_fam = first_param(d, bip_names=["SYMBOL_FAMILY_NAME_PARAM"], ui_names=["Family Name"])
  1939|                 if p_fam:
  1940|                     family_name = _as_string(p_fam)
  1941|                     if family_name:
  1942|                         family_name = canon_str(family_name)
  1943|             except Exception:
  1944|                 pass
  1945|             if family_name and family_name != EXPECTED_FAMILY:
  1946|                 info["debug_wrong_family_excluded"] = info.get("debug_wrong_family_excluded", 0) + 1
  1947|                 continue
  1948| 
  1949|             _eligible_type_count += 1
  1950| 
  1951|             # --- Read core identity fields ---
  1952| 
  1953|             # Unit format info
  1954|             (unit_format_id_v, unit_format_id_q,
  1955|              _rounding_v, _rounding_q,
  1956|              _accuracy_v, _accuracy_q,
  1957|              suppress_spaces_v, suppress_spaces_q) = _read_unit_format_info(d)
  1958| 
  1959|             # Elevation Indicator
  1960|             elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_MISSING)
  1961|             try:
  1962|                 p_ei = first_param(d, ui_names=["Elevation Indicator"])
  1963|                 ei_raw = _as_string(p_ei) if p_ei is not None else None
  1964|                 elevation_indicator_v, elevation_indicator_q = canonicalize_str_allow_empty(ei_raw)
  1965|             except Exception:
  1966|                 elevation_indicator_v, elevation_indicator_q = (None, ITEM_Q_UNREADABLE)
  1967| 
  1968|             # Elevation Indicator as Prefix/Suffix
  1969|             elev_ind_prefix_v, elev_ind_prefix_q = (None, ITEM_Q_MISSING)
  1970|             try:
  1971|                 p_eip = first_param(d, ui_names=["Elevation Indicator as Prefix/Suffix", "Elevation Indicator as Prefix/S"])
  1972|                 eip_int = _as_int(p_eip) if p_eip is not None else None
  1973|                 elev_ind_prefix_v, elev_ind_prefix_q = canonicalize_bool(eip_int)
  1974|             except Exception:
  1975|                 elev_ind_prefix_v, elev_ind_prefix_q = (None, ITEM_Q_UNREADABLE)
  1976| 
  1977|             # Top Indicator
  1978|             top_indicator_v, top_indicator_q = (None, ITEM_Q_MISSING)
  1979|             try:
  1980|                 p_top = first_param(d, ui_names=["Top Indicator"])
  1981|                 top_raw = _as_string(p_top) if p_top is not None else None
  1982|                 top_indicator_v, top_indicator_q = canonicalize_str_allow_empty(top_raw)
  1983|             except Exception:
  1984|                 top_indicator_v, top_indicator_q = (None, ITEM_Q_UNREADABLE)
  1985| 
  1986|             # Bottom Indicator
  1987|             bottom_indicator_v, bottom_indicator_q = (None, ITEM_Q_MISSING)
  1988|             try:
  1989|                 p_bot = first_param(d, ui_names=["Bottom Indicator"])
  1990|                 bot_raw = _as_string(p_bot) if p_bot is not None else None
  1991|                 bottom_indicator_v, bottom_indicator_q = canonicalize_str_allow_empty(bot_raw)
  1992|             except Exception:
  1993|                 bottom_indicator_v, bottom_indicator_q = (None, ITEM_Q_UNREADABLE)
  1994| 
  1995|             # Top Indicator as Prefix/Suffix
  1996|             top_ind_prefix_v, top_ind_prefix_q = (None, ITEM_Q_MISSING)
  1997|             try:
  1998|                 p_tip = first_param(d, ui_names=["Top Indicator as Prefix/Suffix"])
  1999|                 tip_int = _as_int(p_tip) if p_tip is not None else None
  2000|                 top_ind_prefix_v, top_ind_prefix_q = canonicalize_bool(tip_int)
  2001|             except Exception:
  2002|                 top_ind_prefix_v, top_ind_prefix_q = (None, ITEM_Q_UNREADABLE)
  2003| 
  2004|             # Bottom Indicator as Prefix/Suffix
  2005|             bot_ind_prefix_v, bot_ind_prefix_q = (None, ITEM_Q_MISSING)
  2006|             try:
  2007|                 p_bip = first_param(d, ui_names=["Bottom Indicator as Prefix/Suffix", "Bottom Indicator as Prefix/Suf"])
  2008|                 bip_int = _as_int(p_bip) if p_bip is not None else None
  2009|                 bot_ind_prefix_v, bot_ind_prefix_q = canonicalize_bool(bip_int)
  2010|             except Exception:
  2011|                 bot_ind_prefix_v, bot_ind_prefix_q = (None, ITEM_Q_UNREADABLE)
  2012| 
  2013|             # Text Orientation (storage=Integer/enum — use AsValueString)
  2014|             text_orientation_v, text_orientation_q = (None, ITEM_Q_MISSING)
  2015|             try:
  2016|                 p_to = first_param(d, ui_names=["Text Orientation"])
  2017|                 to_raw = _as_value_string(p_to) if p_to is not None else None
  2018|                 text_orientation_v, text_orientation_q = canonicalize_str(to_raw)
  2019|             except Exception:
  2020|                 text_orientation_v, text_orientation_q = (None, ITEM_Q_UNREADABLE)
  2021| 
  2022|             # Text Location (storage=Integer/enum — use AsValueString; probe name is "Text Location")
  2023|             text_location_v, text_location_q = (None, ITEM_Q_MISSING)
  2024|             try:
  2025|                 p_tl = first_param(d, ui_names=["Text Location", "Note Location"])
  2026|                 tl_raw = _as_value_string(p_tl) if p_tl is not None else None
  2027|                 text_location_v, text_location_q = canonicalize_str(tl_raw)
  2028|             except Exception:
  2029|                 text_location_v, text_location_q = (None, ITEM_Q_UNREADABLE)
  2030| 
  2031|             # Symbol name (ElementId resolved to name; no ctx map available for sig_hash)
  2032|             symbol_name_v, symbol_name_q = _read_symbol_name(d, doc)
  2033| 
  2034|             # --- Area 7 §1: Leader Arrowhead cluster (shared helper) ---
  2035|             (leader_arrowhead_uid_v, leader_arrowhead_uid_q,
  2036|              leader_arrowhead_name_v, leader_arrowhead_name_q,
  2037|              leader_arrowhead_sig_hash_v, leader_arrowhead_sig_hash_q) = _read_leader_arrowhead(d, ctx, doc)
  2038|             leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = (None, ITEM_Q_MISSING)
  2039|             try:
  2040|                 p_alw = first_param(d, ui_names=["Leader Arrowhead Line Weight"])
  2041|                 alw_int = _as_int(p_alw) if p_alw is not None else None
  2042|                 leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = canonicalize_int(alw_int)
  2043|             except Exception:
  2044|                 leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q = (None, ITEM_Q_UNREADABLE)
  2045|             leader_line_weight_v, leader_line_weight_q = (None, ITEM_Q_MISSING)
  2046|             try:
  2047|                 p_llw = first_param(d, ui_names=["Leader Line Weight"])
  2048|                 llw_int = _as_int(p_llw) if p_llw is not None else None
  2049|                 leader_line_weight_v, leader_line_weight_q = canonicalize_int(llw_int)
  2050|             except Exception:
  2051|                 leader_line_weight_v, leader_line_weight_q = (None, ITEM_Q_UNREADABLE)
  2052| 
  2053|             # --- Area 7 §7: Rotate with Component / Elevation Base / Text Offsets (spot family) ---
  2054|             rotate_with_component_v, rotate_with_component_q = (None, ITEM_Q_MISSING)
  2055|             try:
  2056|                 p_rwc = first_param(d, ui_names=["Rotate with Component"])
  2057|                 rwc_int = _as_int(p_rwc) if p_rwc is not None else None
  2058|                 rotate_with_component_v, rotate_with_component_q = canonicalize_bool(rwc_int)
  2059|             except Exception:
  2060|                 rotate_with_component_v, rotate_with_component_q = (None, ITEM_Q_UNREADABLE)
  2061|             elevation_base_v, elevation_base_q = (None, ITEM_Q_MISSING)
  2062|             try:
  2063|                 p_eb = first_param(d, ui_names=["Elevation Base"])
  2064|                 eb_raw = _as_value_string(p_eb) if p_eb is not None else None
  2065|                 elevation_base_v, elevation_base_q = canonicalize_str(eb_raw)
  2066|             except Exception:
  2067|                 elevation_base_v, elevation_base_q = (None, ITEM_Q_UNREADABLE)
  2068|             text_offset_from_leader_v, text_offset_from_leader_q = (None, ITEM_Q_MISSING)
  2069|             try:
  2070|                 p_tofl = first_param(d, ui_names=["Text Offset from Leader"])
  2071|                 tofl_ft = _as_double(p_tofl) if p_tofl is not None else None
  2072|                 text_offset_from_leader_v, text_offset_from_leader_q = canonicalize_float(_fmt_in_from_ft(tofl_ft))
  2073|             except Exception:
  2074|                 text_offset_from_leader_v, text_offset_from_leader_q = (None, ITEM_Q_UNREADABLE)
  2075|             text_offset_from_symbol_v, text_offset_from_symbol_q = (None, ITEM_Q_MISSING)
  2076|             try:
  2077|                 p_tofs = first_param(d, ui_names=["Text Offset from Symbol"])
  2078|                 tofs_ft = _as_double(p_tofs) if p_tofs is not None else None
  2079|                 text_offset_from_symbol_v, text_offset_from_symbol_q = canonicalize_float(_fmt_in_from_ft(tofs_ft))
  2080|             except Exception:
  2081|                 text_offset_from_symbol_v, text_offset_from_symbol_q = (None, ITEM_Q_UNREADABLE)
  2082| 
  2083|             # --- Build identity items ---
  2084|             core_items = [
  2085|                 make_identity_item("dim_type.shape", shape_v, shape_q),
  2086|                 make_identity_item("dim_type.unit_format_id", unit_format_id_v, unit_format_id_q),
  2087|                 make_identity_item("dim_type.elevation_indicator", elevation_indicator_v, elevation_indicator_q),
  2088|                 make_identity_item("dim_type.elevation_indicator_as_prefix_suffix", elev_ind_prefix_v, elev_ind_prefix_q),
  2089|                 make_identity_item("dim_type.top_indicator", top_indicator_v, top_indicator_q),
  2090|                 make_identity_item("dim_type.bottom_indicator", bottom_indicator_v, bottom_indicator_q),
  2091|                 make_identity_item("dim_type.top_indicator_as_prefix_suffix", top_ind_prefix_v, top_ind_prefix_q),
  2092|                 make_identity_item("dim_type.bottom_indicator_as_prefix_suffix", bot_ind_prefix_v, bot_ind_prefix_q),
  2093|                 make_identity_item("dim_type.text_orientation", text_orientation_v, text_orientation_q),
  2094|                 make_identity_item("dim_type.text_location", text_location_v, text_location_q),
  2095|                 make_identity_item("dim_type.symbol_name", symbol_name_v, symbol_name_q),
  2096|                 make_identity_item("dim_type.suppress_spaces", suppress_spaces_v, suppress_spaces_q),
  2097|                 make_identity_item("dim_type.leader_arrowhead_uid", leader_arrowhead_uid_v, leader_arrowhead_uid_q),
  2098|                 make_identity_item("dim_type.leader_arrowhead_name", leader_arrowhead_name_v, leader_arrowhead_name_q),
  2099|                 make_identity_item("dim_type.leader_arrowhead_sig_hash", leader_arrowhead_sig_hash_v, leader_arrowhead_sig_hash_q),
  2100|                 make_identity_item("dim_type.leader_arrowhead_line_weight", leader_arrowhead_line_weight_v, leader_arrowhead_line_weight_q),
  2101|                 make_identity_item("dim_type.leader_line_weight", leader_line_weight_v, leader_line_weight_q),
  2102|                 make_identity_item("dim_type.rotate_with_component", rotate_with_component_v, rotate_with_component_q),
  2103|                 make_identity_item("dim_type.elevation_base", elevation_base_v, elevation_base_q),
  2104|                 make_identity_item("dim_type.text_offset_from_leader_in", text_offset_from_leader_v, text_offset_from_leader_q),
  2105|                 make_identity_item("dim_type.text_offset_from_symbol_in", text_offset_from_symbol_v, text_offset_from_symbol_q),
  2106|             ]
  2107| 
  2108|             text_items = _build_text_appearance_items(d)
  2109|             alt_units_items = _build_alternate_units_items(d)
  2110|             all_items = core_items + text_items + alt_units_items
  2111| 
  2112|             identity_items = sorted(all_items, key=lambda it: it.get("k", ""))
  2113| 
  2114|             # Required qualities for blocking
  2115|             # Indicator fields, text placement, and symbol_name are non-blocking:
  2116|             # SpotElevationFixed records may not expose all indicator params,
  2117|             # and missing optional fields should degrade (not block) a record.
  2118|             # All Area 7 additions (leader arrowhead cluster, alternate units,
  2119|             # suppress_spaces, rotate_with_component, elevation_base, text offsets)
  2120|             # are likewise non-blocking enrichment.
  2121|             required_qs = [
  2122|                 shape_q,
  2123|                 unit_format_id_q,
  2124|             ]
  2125|             # text/appearance fields are cross-family alignment, not primary identity — not blocking
  2126| 
  2127|             blocked = any(q != ITEM_Q_OK for q in required_qs)
  2128| 
  2129|             _OPTIONAL_REF_SIG_HASH_KEYS = frozenset({
  2130|                 "dim_type.leader_arrowhead_uid",
  2131|                 "dim_type.leader_arrowhead_name",
  2132|                 "dim_type.leader_arrowhead_sig_hash",
  2133|             })
  2134| 
  2135|             status_reasons = []
  2136|             for it in identity_items:
  2137|                 q = it.get("q")
  2138|                 k = it.get("k", "")
  2139|                 if q == ITEM_Q_OK:
  2140|                     continue
  2141|                 if q == ITEM_Q_MISSING and k in _OPTIONAL_REF_SIG_HASH_KEYS:
  2142|                     continue
  2143|                 status_reasons.append("identity.incomplete:{}:{}".format(q, k))
  2144| 
  2145|             if blocked:
  2146|                 status = STATUS_BLOCKED
  2147|             elif status_reasons:
  2148|                 status = STATUS_DEGRADED
  2149|             else:
  2150|                 status = STATUS_OK
  2151| 
  2152|             # dim_type.leader_arrowhead_uid/_name are file-local/cosmetic metadata
  2153|             # (D-004 restricts UniqueId use to element-backed identities; names are
  2154|             # metadata only per the Hash Semantics rule) -- kept in identity_items for
  2155|             # governance/join-key visibility but excluded from the sig_hash preimage
  2156|             # itself, matching the contract's sig_hash_keys pin for these 3 domains.
  2157|             # Without this, two files with a semantically-identical spot dimension
  2158|             # type (same arrowhead style/name) would hash differently purely because
  2159|             # Revit UniqueIds are per-file-random (PR #412 review).
  2160|             _SIG_HASH_EXCLUDED_KEYS = frozenset({
  2161|                 "dim_type.leader_arrowhead_uid",
  2162|                 "dim_type.leader_arrowhead_name",
  2163|             })
  2164|             sig_hash_items = [it for it in identity_items if it.get("k") not in _SIG_HASH_EXCLUDED_KEYS]
  2165|             preimage = serialize_identity_items(sig_hash_items)
  2166|             sig_hash = None if blocked else make_hash(preimage)
  2167| 
  2168|             try:
  2169|                 type_id_int = getattr(getattr(d, "Id", None), "IntegerValue", None)
  2170|             except Exception:
  2171|                 type_id_int = None
  2172| 
  2173|             try:
  2174|                 uid_raw = getattr(d, "UniqueId", None)
  2175|             except Exception:
  2176|                 uid_raw = None
  2177| 
  2178|             label_str = type_name
  2179|             rec_v2 = build_record_v2(
  2180|                 domain=DOMAIN_NAME,
  2181|                 record_id=safe_str(type_id_int) if type_id_int is not None else DOMAIN_NAME,
  2182|                 status=status,
  2183|                 status_reasons=sorted(set(status_reasons)),
  2184|                 sig_hash=sig_hash,
  2185|                 identity_items=identity_items,
  2186|                 required_qs=tuple(required_qs),
  2187|                 label={
  2188|                     "display": safe_str(label_str) if label_str else DOMAIN_NAME,
  2189|                     "quality": "human" if label_str else "placeholder_missing",
  2190|                     "provenance": "revit.DimensionType.params",
  2191|                 },
  2192|             )
  2193|             _ip, _ip_q = purge_lookup(type_id_int, ctx)
  2194|             rec_v2["is_purgeable"] = _ip
  2195|             rec_v2["is_purgeable_q"] = _ip_q
  2196|             _attach_placeholder_metadata(rec_v2, type_id_int, _instance_count_map, _instance_count_map_q)
  2197| 
  2198|             pol = get_domain_join_key_policy((ctx or {}).get("join_key_policies"), DOMAIN_NAME)
  2199|             rec_v2["join_key"], _missing = build_join_key_from_policy(
  2200|                 domain_policy=pol,
  2201|                 identity_items=identity_items,
  2202|                 include_optional_items=False,
  2203|                 emit_keys_used=True,
  2204|                 hash_optional_items=False,
  2205|                 emit_items=False,
  2206|                 emit_selectors=True,
  2207|             )
  2208| 
  2209|             coordination_items = [
  2210|                 make_identity_item("dim_type.domain_family", "dimension_types", ITEM_Q_OK),
  2211|             ]
  2212| 
  2213|             unknown_items = []
  2214|             try:
  2215|                 _eid_v, _eid_q = canonicalize_int(type_id_int)
  2216|             except Exception:
  2217|                 _eid_v, _eid_q = (None, ITEM_Q_UNREADABLE)
  2218|             try:
  2219|                 _uid_v, _uid_q = canonicalize_str(uid_raw)
  2220|             except Exception:
  2221|                 _uid_v, _uid_q = (None, ITEM_Q_UNREADABLE)
  2222|             unknown_items.append(make_identity_item("dim_type.source_element_id", _eid_v, _eid_q))
  2223|             unknown_items.append(make_identity_item("dim_type.source_unique_id", _uid_v, _uid_q))
  2224| 
  2225|             rec_v2["phase2"] = {
  2226|                 "schema": "phase2.{}.v1".format(DOMAIN_NAME),
  2227|                 "grouping_basis": "phase2.hypothesis",
  2228|                 "cosmetic_items": phase2_sorted_items([]),
  2229|                 "coordination_items": phase2_sorted_items(coordination_items),
  2230|                 "unknown_items": phase2_sorted_items(unknown_items),
  2231|             }
  2232| 
  2233|             if sig_hash:
  2234|                 v2_sig_hashes.append(sig_hash)
  2235|             v2_records.append(rec_v2)
  2236| 
  2237|         except Exception:
  2238|             continue  # fail-soft per record
  2239| 
  2240|     _total_type_count = _eligible_type_count
  2241|     for rec in v2_records:
  2242|         try:
  2243|             rec["is_sole_type_in_category"] = (_total_type_count == 1)
  2244|             rec["is_sole_type_in_category_q"] = "ok"
  2245|         except Exception:
  2246|             rec["is_sole_type_in_category"] = None
  2247|             rec["is_sole_type_in_category_q"] = "unreadable"
  2248| 
  2249|     info["records"] = sorted(v2_records, key=lambda r: str(r.get("record_id", "")))
  2250|     info["count"] = len(v2_records)
  2251|     info["signature_hashes_v2"] = sorted(v2_sig_hashes)
  2252| 
  2253|     if v2_sig_hashes:
  2254|         info["hash_v2"] = make_hash(info["signature_hashes_v2"])
  2255|         info["debug_v2_blocked"] = False
  2256|     else:
  2257|         info["hash_v2"] = None
  2258|         info["debug_v2_blocked"] = True
  2259|         info["debug_v2_block_reasons"] = {"no_records_or_all_blocked": True}
  2260| 
  2261|     return info
  2262| 
  2263| def _apply_family_name_override(d, shape_v, shape_family, shape_q, type_name):
  2264|     """
  2265|     Heuristic override: use FamilyName prefix to more precisely classify Spot types.
  2266|     Returns updated (shape_v, shape_family, shape_q).
  2267|     """
  2268|     try:
  2269|         family_name = getattr(d, "FamilyName", None)
  2270|         basis = family_name if family_name else type_name
  2271|         bn_l = safe_str(basis).strip().lower()
  2272| 
  2273|         if bn_l.startswith("spot slopes"):
  2274|             return (SHAPE_SPOT_SLOPE, FAMILY_SPOT, ITEM_Q_OK)
  2275|         elif bn_l.startswith("spot elevations"):
  2276|             return (SHAPE_SPOT_ELEVATION, FAMILY_SPOT, ITEM_Q_OK)
  2277|         elif bn_l.startswith("spot coordinates"):
  2278|             return (SHAPE_SPOT_COORDINATE, FAMILY_SPOT, ITEM_Q_OK)
  2279|     except Exception:
  2280|         pass
  2281|     return (shape_v, shape_family, shape_q)
  2282| 
  2283| 
  2284| def _read_symbol_name(d, doc):
  2285|     """
  2286|     Try to read the "Symbol" parameter that references a loaded family.
  2287|     If ElementId > 0, resolve element and return its name directly.
  2288|     Returns (symbol_name_v, symbol_name_q).
  2289|     """
  2290|     try:
  2291|         p_sym = first_param(d, ui_names=["Symbol"])
  2292|         if p_sym is None:
  2293|             return (None, ITEM_Q_MISSING)
  2294| 
  2295|         if not getattr(p_sym, "HasValue", False):
  2296|             return (None, ITEM_Q_MISSING)
  2297| 
  2298|         eid = None
  2299|         try:
  2300|             eid = p_sym.AsElementId()
  2301|         except Exception:
  2302|             return (None, ITEM_Q_UNREADABLE)
  2303| 
  2304|         if eid is None or getattr(eid, "IntegerValue", -1) <= 0:
  2305|             return (None, ITEM_Q_MISSING)
  2306| 
  2307|         sym_elem = None
  2308|         try:
  2309|             sym_elem = doc.GetElement(eid)
  2310|         except Exception:
  2311|             return (None, ITEM_Q_UNREADABLE)
  2312| 
  2313|         if sym_elem is None:
  2314|             return (None, ITEM_Q_MISSING)
  2315| 
  2316|         sym_name = None
  2317|         try:
  2318|             sym_name = getattr(sym_elem, "Name", None)
  2319|         except Exception:
  2320|             pass
  2321| 
  2322|         if sym_name:
  2323|             return canonicalize_str(str(sym_name))
  2324|         return (None, ITEM_Q_MISSING)
  2325| 
  2326|     except Exception:
  2327|         return (None, ITEM_Q_UNREADABLE)
  2328| 
  2329| 
```
