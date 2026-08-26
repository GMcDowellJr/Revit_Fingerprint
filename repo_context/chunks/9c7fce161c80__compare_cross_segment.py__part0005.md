# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 5 of 13
- Original line range: 2005-2517
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _is_client_wide_rollup, _is_standard_role, detect_stale_ancestor_encoding, _build_ancestor_map, _build_ancestor_map._immediate_parents, _build_ancestor_map._walk, _is_lineage_related, _compute_containment_thresholds, write_population_containment_thresholds, _population_containment_map, _is_population_contained, _same_unit, discover_within_segment, _redundant_child_segment_id, _resolve_runnable_segment, _scope_override_key
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  2005| def _is_client_wide_rollup(row: Dict[str, str], policy: EnterprisePolicy) -> bool:
  2006|     """A real, non-InternalEnterprise client's row with business_center_label not cut
  2007|     (pools that client's work across whichever real BCs it touches). This
  2008|     is the "client-wide roll-up" population -- distinct from
  2009|     _scope_level()'s "client_business_center" bucket, which requires bc to
  2010|     be cut to one specific real value.
  2011|     """
  2012|     client = _client_of(row)
  2013|     if not client or _is_enterprise_client(client, policy):
  2014|         return False
  2015|     return not _bc_of(row)
  2016| 
  2017| 
  2018| def _is_standard_role(role_key: str) -> bool:
  2019|     # Template/Container are the two governance roles that can carry an
  2020|     # independent enterprise/bc/client scope identity for this fan-out.
  2021|     # Generic/Generic-Host already pairs unconditionally against every
  2022|     # Template/Container/Project in discover_governance_chain()'s
  2023|     # generic_ids loop below (no client/bc scoping at all today), so it has
  2024|     # no separate scope-scoped edge to add here.
  2025|     return role_key in ("template", "container")
  2026| 
  2027| 
  2028| # Non-root DIMENSION_CONFIG fields (build_segment_manifest.py), duplicated
  2029| # here rather than imported since this module has no dependency on that
  2030| # one -- used only by detect_stale_ancestor_encoding()'s heuristic below.
  2031| _NON_ROOT_DIMENSION_FIELDS = ("governance_role", "client_label", "discipline_label", "business_center_label")
  2032| 
  2033| 
  2034| def detect_stale_ancestor_encoding(manifest: Dict[str, Dict[str, str]]) -> List[str]:
  2035|     """Return one warning string per segment whose ancestor_segment_ids value
  2036|     looks like it was written before D-028 (pipe-joined instead of
  2037|     semicolon-joined) rather than genuinely having only one immediate
  2038|     structural ancestor.
  2039| 
  2040|     Heuristic, not a proof: a segment with N non-root dimension fields
  2041|     present has up to N one-field-drop immediate ancestors (see
  2042|     build_segment_manifest.py's _build_segments()), but can legitimately
  2043|     have fewer if not every dropped-field variant exists as its own row in a
  2044|     sparse corpus -- so "fewer ancestors than fields present" is not itself
  2045|     abnormal. What IS a strong, low-false-positive signal: N >= 2 non-root
  2046|     fields present, a non-empty ancestor_segment_ids value, ";" not present
  2047|     in it at all (splitting on ";" yields exactly one token), AND that one
  2048|     token itself contains more than one "|" -- pointing at a single
  2049|     multi-part string that looks like more than one concatenated segment_id
  2050|     with no way to tell them apart. A well-formed post-D-028 field would
  2051|     either have used ";" to separate multiple real ancestors, or have
  2052|     exactly one real ancestor whose own segment_id naturally contains at
  2053|     most a few "|" characters for a low non-root-field-count segment -- the
  2054|     combination of "many fields present, one blob, multiple internal
  2055|     pipes" is what the pre-D-028 "|".join(ancestor_ids) bug produces on any
  2056|     segment with more than one real ancestor.
  2057| 
  2058|     Warning-only (not blocking): a false positive here would incorrectly
  2059|     accuse a legitimately sparse, single-ancestor segment of being stale,
  2060|     and _build_ancestor_map()'s parent_segment_id fallback already keeps
  2061|     lineage exclusion from silently disappearing entirely even in the worst
  2062|     case -- this is a diagnostic aid pointed at DECISIONS.md D-028's
  2063|     documented "requires a full manifest regeneration" guidance, not a new
  2064|     trust gate like the cyclic-ancestry guard below.
  2065|     """
  2066|     warnings: List[str] = []
  2067|     for sid, row in manifest.items():
  2068|         raw = (row.get("ancestor_segment_ids") or "").strip()
  2069|         if not raw or ";" in raw:
  2070|             continue
  2071|         fields_present = sum(1 for f in _NON_ROOT_DIMENSION_FIELDS if (row.get(f) or "").strip())
  2072|         if fields_present >= 2 and raw.count("|") >= 2:
  2073|             warnings.append(
  2074|                 f"segment={sid}: ancestor_segment_ids={raw!r} has no ';' but {fields_present} "
  2075|                 f"non-root dimension fields are present and the value contains multiple '|' -- "
  2076|                 f"this looks like a pre-D-028 pipe-joined manifest (stale/unparseable ancestor "
  2077|                 f"data). Re-run build_segment_manifest.py to regenerate segment_manifest.csv."
  2078|             )
  2079|     return warnings
  2080| 
  2081| 
  2082| def _build_ancestor_map(manifest: Dict[str, Dict[str, str]]) -> Dict[str, Set[str]]:
  2083|     """Map each segment_id to the full transitive closure of its structural
  2084|     ancestors (the `structural_ancestor` relation, D-027) — every segment
  2085|     whose dimension key is a proper subset of this one's, at any lattice
  2086|     depth, not just the single primary parent_segment_id chain.
  2087| 
  2088|     Segments are hierarchical cuts of the same underlying file population —
  2089|     build_segment_manifest.py derives each child as its parent's population
  2090|     narrowed by one additional cut dimension, so a child's files are always
  2091|     a subset of its parent's (this is also why a collection-blank BC
  2092|     roll-up's own population can be a strict superset of its
  2093|     collection-specific children's — see discover_governance_chain()'s
  2094|     _is_collection_rollup). Treating an ancestor and its own descendant as
  2095|     independent peers — whether pooled together or paired directly — compares
  2096|     a segment against data that already contains (some or all of) its own.
  2097| 
  2098|     Source: `ancestor_segment_ids` (";"-delimited — see
  2099|     build_segment_manifest.py's _build_segments() comment on the encoding),
  2100|     which for each segment lists its immediate structural parents — one per
  2101|     dropped non-root dimension field, so a segment with N non-root fields
  2102|     present can have up to N distinct immediate parents. This is a
  2103|     multi-parent adjacency list, not itself the full closure; the walk below
  2104|     recursively unions each immediate parent's own ancestor set to complete
  2105|     it.
  2106| 
  2107|     `parent_segment_id` (the single primary parent) is folded in as an
  2108|     additional immediate parent alongside whatever `ancestor_segment_ids`
  2109|     lists, rather than replaced outright — this is what actually guarantees
  2110|     the "never removes a previously-detected ancestor" superset property:
  2111|     `ancestor_segment_ids` may be blank/absent on a manifest a caller built
  2112|     by hand (every pre-D-027 test fixture in this repo populates only
  2113|     `parent_segment_id`), and treating that as "no ancestors" would silently
  2114|     regress exactly the lineage exclusion this function exists to provide.
  2115|     On a real, freshly-built segment_manifest.csv the two sources agree
  2116|     (`parent_segment_id` is always itself one of `ancestor_segment_ids`'
  2117|     entries — see build_segment_manifest.py's _build_segments()), so this
  2118|     union changes nothing there; it only matters as a fallback.
  2119|     """
  2120|     ancestors: Dict[str, Set[str]] = {}
  2121| 
  2122|     def _immediate_parents(sid: str) -> Set[str]:
  2123|         row = manifest.get(sid, {})
  2124|         raw = row.get("ancestor_segment_ids", "")
  2125|         parents = {p for p in raw.split(";") if p}
  2126|         primary_parent = row.get("parent_segment_id", "").strip()
  2127|         if primary_parent:
  2128|             parents.add(primary_parent)
  2129|         return parents
  2130| 
  2131|     def _walk(sid: str, seen: Set[str]) -> Set[str]:
  2132|         if sid in ancestors:
  2133|             return ancestors[sid]
  2134|         result: Set[str] = set()
  2135|         for parent in _immediate_parents(sid):
  2136|             if parent == sid or parent in seen:
  2137|                 sys.exit(
  2138|                     "[error] Blocked: cyclic segment ancestry detected — "
  2139|                     f"{sid!r} revisits already-seen segment {parent!r} while "
  2140|                     "walking ancestor_segment_ids; segment_manifest.csv cannot "
  2141|                     "be trusted for lineage exclusion until this is fixed"
  2142|                 )
  2143|             result.add(parent)
  2144|             result |= _walk(parent, seen | {sid})
  2145|         ancestors[sid] = result
  2146|         return result
  2147| 
  2148|     for sid in manifest:
  2149|         _walk(sid, set())
  2150|     return ancestors
  2151| 
  2152| 
  2153| def _is_lineage_related(ancestor_map: Dict[str, Set[str]], sid_a: str, sid_b: str) -> bool:
  2154|     return sid_b in ancestor_map.get(sid_a, set()) or sid_a in ancestor_map.get(sid_b, set())
  2155| 
  2156| 
  2157| # ---------------------------------------------------------------------------
  2158| # population_containment — empirical containment relation (D-027)
  2159| #
  2160| # structural_ancestor (_build_ancestor_map/_is_lineage_related above) is
  2161| # derived from the dimension lattice and is reliable but incomplete — many
  2162| # real population-subset relationships in the corpus have no dimensional
  2163| # explanation at all (e.g. a segment whose files all happen to also belong
  2164| # to another segment with no shared cut dimension). population_containment
  2165| # is computed directly from real export_run_id membership instead, so it
  2166| # does not require or assume a dimensional relationship exists, and can
  2167| # catch a materially-significant containment coincidence whether or not
  2168| # structural_ancestor also explains it.
  2169| #
  2170| # Materiality-gated: an exact population-subset relationship between two
  2171| # very small or very lopsided segments is common by pure chance (a 1-file
  2172| # segment is trivially "contained" in nearly anything) and is not evidence
  2173| # of real inheritance. Two Jenks-natural-breaks (tools/jenks_utils.py —
  2174| # the general-purpose implementation reused here; tools/compute_governance_
  2175| # thresholds.py carries its own near-duplicate jenks_natural_breaks() for a
  2176| # narrower use, not consolidated here to avoid an unrelated behavior change)
  2177| # passes gate this:
  2178| #   1. size-noise filter: drop pairs whose smaller side is strictly below the
  2179| #      break in min(|pop_a|, |pop_b|) across all non-structural subset pairs
  2180| #      (the break value itself belongs to the upper/signal class, per
  2181| #      jenks_breaks()'s own documented "values below break_0 are class 1
  2182| #      (lowest)" contract — a pair sitting exactly at the break clears the
  2183| #      floor, it isn't treated as noise).
  2184| #   2. containment-ratio filter, among size survivors only: drop pairs whose
  2185| #      min/max size ratio is below the break (same convention: at-the-break
  2186| #      clears the floor).
  2187| # Both thresholds are fit ONLY on non-structural subset pairs (pairs
  2188| # structural_ancestor does not already explain), so the fit isn't diluted by
  2189| # structural's own well-behaved signal — but the resulting containment MAP is
  2190| # evaluated over every population pair with membership data, structural or
  2191| # not, so a guard using it catches both kinds uniformly (this is what lets
  2192| # discover_sibling_segments() rely on it alone rather than needing a second,
  2193| # separate structural check — see its docstring). Byte-identical populations
  2194| # (pa == pb) bypass both thresholds entirely and are always treated as
  2195| # contained — equality is the strongest possible form of the subset
  2196| # relationship this guard exists to catch, so there is no "how much overlap"
  2197| # materiality question left to ask.
  2198| # ---------------------------------------------------------------------------
  2199| 
  2200| POPULATION_CONTAINMENT_THRESHOLDS_FIELDS: List[str] = [
  2201|     "stage", "algorithm", "n_classes", "break_value",
  2202|     "source_value_min", "source_value_max",
  2203|     "pairs_before", "pairs_after",
  2204| ]
  2205| 
  2206| 
  2207| def _compute_containment_thresholds(
  2208|     manifest: Dict[str, Dict[str, str]],
  2209|     membership: Dict[str, Set[str]],
  2210|     ancestor_map: Dict[str, Set[str]],
  2211| ) -> Dict[str, object]:
  2212|     """Derive population_containment's two materiality thresholds
  2213|     (min_population_for_containment, min_containment_ratio) via Jenks
  2214|     natural breaks (n_classes=2) over real, non-structural population-subset
  2215|     pairs. See the module-level population_containment comment block above
  2216|     for the two-stage method and why the fit is restricted to non-structural
  2217|     pairs. Returns a dict with both thresholds plus the audit trail consumed
  2218|     by write_population_containment_thresholds().
  2219|     """
  2220|     sids = sorted(sid for sid in manifest if membership.get(sid))
  2221|     non_structural_pairs: List[Tuple[str, str]] = []
  2222|     for i in range(len(sids)):
  2223|         pa = membership[sids[i]]
  2224|         for j in range(i + 1, len(sids)):
  2225|             b = sids[j]
  2226|             pb = membership[b]
  2227|             if pa == pb:
  2228|                 continue
  2229|             if not (pa <= pb or pb <= pa):
  2230|                 continue
  2231|             if _is_lineage_related(ancestor_map, sids[i], b):
  2232|                 continue
  2233|             non_structural_pairs.append((sids[i], b))
  2234| 
  2235|     sizes = [min(len(membership[a]), len(membership[b])) for a, b in non_structural_pairs]
  2236|     size_breaks = jenks_breaks(sizes, n_classes=2)
  2237|     min_population_for_containment = float(size_breaks[0]) if size_breaks else 0.0
  2238| 
  2239|     size_survivors = [
  2240|         (a, b) for a, b in non_structural_pairs
  2241|         if min(len(membership[a]), len(membership[b])) >= min_population_for_containment
  2242|     ]
  2243|     ratios = [
  2244|         min(len(membership[a]), len(membership[b])) / max(len(membership[a]), len(membership[b]))
  2245|         for a, b in size_survivors
  2246|     ]
  2247|     ratio_breaks = jenks_breaks(ratios, n_classes=2)
  2248|     min_containment_ratio = float(ratio_breaks[0]) if ratio_breaks else 0.0
  2249| 
  2250|     ratio_survivors = [
  2251|         (a, b) for a, b in size_survivors
  2252|         if (min(len(membership[a]), len(membership[b])) / max(len(membership[a]), len(membership[b])))
  2253|         >= min_containment_ratio
  2254|     ]
  2255| 
  2256|     return {
  2257|         "min_population_for_containment": min_population_for_containment,
  2258|         "min_containment_ratio": min_containment_ratio,
  2259|         "size_stage": {
  2260|             "source_value_min": min(sizes) if sizes else None,
  2261|             "source_value_max": max(sizes) if sizes else None,
  2262|             "pairs_before": len(non_structural_pairs),
  2263|             "pairs_after": len(size_survivors),
  2264|         },
  2265|         "ratio_stage": {
  2266|             "source_value_min": round(min(ratios), 4) if ratios else None,
  2267|             "source_value_max": round(max(ratios), 4) if ratios else None,
  2268|             "pairs_before": len(size_survivors),
  2269|             "pairs_after": len(ratio_survivors),
  2270|         },
  2271|     }
  2272| 
  2273| 
  2274| def write_population_containment_thresholds(out_dir: Path, thresholds: Dict[str, object]) -> Path:
  2275|     """Write the Jenks-derived population_containment thresholds to
  2276|     population_containment_thresholds.csv, following the same
  2277|     break-value/n_classes/algorithm/source-range/pair-count audit pattern as
  2278|     tools/compute_governance_thresholds.py's thresholds.csv — a first-pass,
  2279|     inspectable output for human sanity-check, not silently baked into code.
  2280|     """
  2281|     size_stage = thresholds["size_stage"]
  2282|     ratio_stage = thresholds["ratio_stage"]
  2283|     rows = [
  2284|         {
  2285|             "stage": "size_noise_filter",
  2286|             "algorithm": "jenks_breaks",
  2287|             "n_classes": "2",
  2288|             "break_value": str(thresholds["min_population_for_containment"]),
  2289|             "source_value_min": str(size_stage["source_value_min"]),
  2290|             "source_value_max": str(size_stage["source_value_max"]),
  2291|             "pairs_before": str(size_stage["pairs_before"]),
  2292|             "pairs_after": str(size_stage["pairs_after"]),
  2293|         },
  2294|         {
  2295|             "stage": "containment_ratio_filter",
  2296|             "algorithm": "jenks_breaks",
  2297|             "n_classes": "2",
  2298|             "break_value": str(thresholds["min_containment_ratio"]),
  2299|             "source_value_min": str(ratio_stage["source_value_min"]),
  2300|             "source_value_max": str(ratio_stage["source_value_max"]),
  2301|             "pairs_before": str(ratio_stage["pairs_before"]),
  2302|             "pairs_after": str(ratio_stage["pairs_after"]),
  2303|         },
  2304|     ]
  2305|     path = out_dir / "population_containment_thresholds.csv"
  2306|     atomic_write_csv(path, POPULATION_CONTAINMENT_THRESHOLDS_FIELDS, rows)
  2307|     return path
  2308| 
  2309| 
  2310| def _population_containment_map(
  2311|     manifest: Dict[str, Dict[str, str]],
  2312|     membership: Dict[str, Set[str]],
  2313|     thresholds: Dict[str, object],
  2314| ) -> Dict[str, Set[str]]:
  2315|     """Map each segment_id to the set of other segment_ids it has a
  2316|     materially-significant, empirically-real population containment
  2317|     relationship with (either direction — the map is symmetric, same
  2318|     convention as _build_ancestor_map's per-segment sets).
  2319| 
  2320|     Evaluated over every segment pair with real membership data (not
  2321|     restricted to non-structural pairs, unlike the threshold fit in
  2322|     _compute_containment_thresholds() — see the module-level comment above).
  2323|     """
  2324|     min_pop = thresholds["min_population_for_containment"]
  2325|     min_ratio = thresholds["min_containment_ratio"]
  2326|     contains: Dict[str, Set[str]] = defaultdict(set)
  2327|     sids = sorted(sid for sid in manifest if membership.get(sid))
  2328|     for i in range(len(sids)):
  2329|         pa = membership[sids[i]]
  2330|         for j in range(i + 1, len(sids)):
  2331|             b = sids[j]
  2332|             pb = membership[b]
  2333|             if pa == pb:
  2334|                 # Byte-identical populations are the strongest possible form
  2335|                 # of the subset relationship this guard exists to catch --
  2336|                 # unconditionally contained, no materiality threshold needed
  2337|                 # (there's no "how much overlap" question when it's total).
  2338|                 # A real, if currently unobserved, case: build_segment_
  2339|                 # manifest.py only WARNS on duplicate bundle population_hash
  2340|                 # values, it doesn't block the build, so two distinct
  2341|                 # segment_ids with identical populations are a possible live
  2342|                 # state, not just a hypothetical one.
  2343|                 contains[sids[i]].add(b)
  2344|                 contains[b].add(sids[i])
  2345|                 continue
  2346|             if not (pa <= pb or pb <= pa):
  2347|                 continue
  2348|             smin, smax = sorted((len(pa), len(pb)))
  2349|             if smin < min_pop:
  2350|                 continue
  2351|             if smax and (smin / smax) < min_ratio:
  2352|                 continue
  2353|             contains[sids[i]].add(b)
  2354|             contains[b].add(sids[i])
  2355|     return dict(contains)
  2356| 
  2357| 
  2358| def _is_population_contained(
  2359|     containment_map: Dict[str, Set[str]], sid_a: str, sid_b: str,
  2360| ) -> bool:
  2361|     return sid_b in containment_map.get(sid_a, set())
  2362| 
  2363| 
  2364| ComparisonPair = Tuple[str, str, str]  # (seg_a, seg_b, comparison_type)
  2365| 
  2366| 
  2367| # ---------------------------------------------------------------------------
  2368| # Pair discovery
  2369| #
  2370| # Lineage/containment guard audit (D-027): of the pair-emitting functions
  2371| # below, discover_sibling_segments() carries both the structural_ancestor
  2372| # and population_containment guards (the corpus-verified 101-violation
  2373| # defect lived there). discover_governance_chain() carries structural_ancestor
  2374| # only, via its own internal _build_ancestor_map() call (see that function's
  2375| # decision note). discover_within_segment(), discover_cross_client(),
  2376| # discover_client_cross_bc(), and discover_parent_siblings() carry NEITHER
  2377| # guard as of this audit — re-running the corpus-level violation check
  2378| # (pa <= pb or pb <= pa on real export_run_id sets) against each of their
  2379| # emitted pairs found zero real violations on the current corpus, so they
  2380| # are flagged here as a known, currently-latent gap rather than fixed
  2381| # outright (same posture discover_governance_chain itself was in before this
  2382| # session — see finding 5 in the D-027 write-up). discover_within_project()
  2383| # is exempt by construction: both sides of every pair it emits are the same
  2384| # segment_id, so there is no cross-segment lineage question to guard.
  2385| # ---------------------------------------------------------------------------
  2386| 
  2387| def _same_unit(
  2388|     manifest: Dict[str, Dict[str, str]],
  2389|     sid_a: str,
  2390|     sid_b: str,
  2391| ) -> bool:
  2392|     return (
  2393|         manifest.get(sid_a, {}).get("unit_system", "")
  2394|         == manifest.get(sid_b, {}).get("unit_system", "")
  2395|         and manifest.get(sid_a, {}).get("unit_system", "") != ""
  2396|     )
  2397| 
  2398| 
  2399| def discover_within_segment(
  2400|     manifest: Dict[str, Dict[str, str]],
  2401| ) -> List[ComparisonPair]:
  2402|     by_parent: Dict[str, List[str]] = defaultdict(list)
  2403|     for sid, row in manifest.items():
  2404|         parent = row.get("parent_segment_id", "").strip()
  2405|         rt = row.get("run_type", "").strip().lower()
  2406|         if parent and rt in ("bundle", "reference"):
  2407|             by_parent[parent].append(sid)
  2408| 
  2409|     pairs: List[ComparisonPair] = []
  2410|     for _parent, children in by_parent.items():
  2411|         role_map: Dict[str, List[str]] = defaultdict(list)
  2412|         for c in children:
  2413|             role = manifest[c].get("governance_role", "").strip().lower()
  2414|             role_map["generic" if _is_generic_role(role) else role].append(c)
  2415| 
  2416|         generics = role_map.get("generic", [])
  2417|         templates = role_map.get("template", [])
  2418|         projects = role_map.get("project", [])
  2419|         containers = role_map.get("container", [])
  2420| 
  2421|         for g in generics:
  2422|             for t in templates:
  2423|                 if _same_unit(manifest, g, t):
  2424|                     pairs.append((g, t, "generic_to_template"))
  2425|             for c in containers:
  2426|                 if _same_unit(manifest, g, c):
  2427|                     pairs.append((g, c, "generic_to_container"))
  2428|             for p in projects:
  2429|                 if _same_unit(manifest, g, p):
  2430|                     pairs.append((g, p, "generic_to_project"))
  2431| 
  2432|         for t in templates:
  2433|             for p in projects:
  2434|                 if _same_unit(manifest, t, p):
  2435|                     pairs.append((t, p, "template_to_project"))
  2436|             for c in containers:
  2437|                 if _same_unit(manifest, t, c):
  2438|                     pairs.append((t, c, "template_to_container"))
  2439| 
  2440|         for c in containers:
  2441|             for p in projects:
  2442|                 if _same_unit(manifest, c, p):
  2443|                     pairs.append((c, p, "container_to_project"))
  2444| 
  2445|     return pairs
  2446| 
  2447| 
  2448| def _redundant_child_segment_id(row: Dict[str, str]) -> Optional[str]:
  2449|     """Extract the target segment_id from a "redundant_single_child:<segment_id>"
  2450|     note, if present (see build_segment_manifest.py's _build_segments() pass5).
  2451| 
  2452|     build_segment_manifest.py demotes a segment to run_type="registration"
  2453|     whenever a direct child's population is byte-identical to its own
  2454|     (e.g. a client whose every Project file happens to sit in a single
  2455|     business_center_label, now that business_center_label is a real cut
  2456|     dimension rather than always-blank) -- correctly avoiding running the
  2457|     same population twice under two different segment_ids. That child is not
  2458|     a narrower/rescoped population; it IS the same population_hash, just
  2459|     recorded under a more specific segment_id. Substituting it back in where
  2460|     the demoted row would otherwise have been used is therefore not blending
  2461|     distinct comparison grains -- see _is_client_only_project_segment()'s and
  2462|     discover_cross_client()'s docstrings on why that anti-pattern must be
  2463|     avoided elsewhere in this module.
  2464| 
  2465|     segment_id itself uses "|" as its own internal field separator, and other
  2466|     notes may already share the pipe-joined `notes` string, so a naive
  2467|     `notes.split("|")` would mangle a multi-part child segment_id. pass5 always
  2468|     runs last (see build_segment_manifest.py), so "redundant_single_child:" is
  2469|     guaranteed to be the final note appended -- take everything after the
  2470|     marker to the end of the string instead of splitting.
  2471|     """
  2472|     notes = row.get("notes", "") or ""
  2473|     marker = "redundant_single_child:"
  2474|     idx = notes.find(marker)
  2475|     if idx == -1:
  2476|         return None
  2477|     return notes[idx + len(marker):]
  2478| 
  2479| 
  2480| def _resolve_runnable_segment(
  2481|     manifest: Dict[str, Dict[str, str]], sid: str
  2482| ) -> Optional[str]:
  2483|     """Resolve sid to a run_type in (bundle, reference) segment_id: sid
  2484|     itself if already eligible, or -- transitively -- whatever
  2485|     population-identical segment build_segment_manifest.py's
  2486|     redundant_single_child pass ultimately points at, if any. Transitive
  2487|     because a redundant_single_child pointer can itself be redundant to a
  2488|     further child (e.g. a Template rollup redundant to its
  2489|     single-real-client child, which -- since business_center_label's
  2490|     promotion -- is itself redundant to a single-business-center child one
  2491|     level deeper); a single-hop lookup would wrongly treat that intermediate,
  2492|     still-ineligible row as a dead end. Returns None if sid isn't eligible and
  2493|     carries no pointer that eventually resolves to an eligible segment (e.g.
  2494|     a genuinely below-min-files/skip segment, or a cycle -- guarded against
  2495|     via `visited`, though build_segment_manifest.py's population-subset
  2496|     strictly shrinks along any real chain and cannot actually cycle).
  2497|     """
  2498|     visited: Set[str] = set()
  2499|     cur = sid
  2500|     while cur not in visited:
  2501|         visited.add(cur)
  2502|         row = manifest.get(cur)
  2503|         if row is None:
  2504|             return None
  2505|         if row.get("run_type", "").strip().lower() in ("bundle", "reference"):
  2506|             return cur
  2507|         nxt = _redundant_child_segment_id(row)
  2508|         if not nxt:
  2509|             return None
  2510|         cur = nxt
  2511|     return None
  2512| 
  2513| 
  2514| def _scope_override_key(comparison_type: str) -> str:
  2515|     return f"_scope_override__{comparison_type}"
  2516| 
  2517| 
```
