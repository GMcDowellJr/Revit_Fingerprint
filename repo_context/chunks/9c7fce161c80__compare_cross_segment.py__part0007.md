# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 7 of 13
- Original line range: 2840-3310
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: discover_governance_chain, discover_governance_chain._key, discover_governance_chain._disc, discover_governance_chain._disc_match, discover_governance_chain._collection, discover_governance_chain._is_collection_rollup, discover_governance_chain._collection_match, discover_within_project, deduplicate_pairs, drop_legacy_siblings_covered_by_peer_comparisons, make_comparison_run_id
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  2840| def discover_governance_chain(
  2841|     policy: EnterprisePolicy,
  2842|     manifest: Dict[str, Dict[str, str]],
  2843| ) -> List[ComparisonPair]:
  2844|     # Directed pairs along the provision chain:
  2845|     # Generic/Generic-Host→Template/Container/Project, Template→Project/Container,
  2846|     # and Container→Project. Project target used-view is usage; other target roles
  2847|     # remain provided-vocabulary inventories.
  2848|     # Reference segments are included — they participate using their file inventories.
  2849|     def _key(row: Dict[str, str]) -> Tuple[str, ...]:
  2850|         # client_label is blank, or an explicit "not applicable" spelling
  2851|         # (na, N/A, __NOT_APPLICABLE__, ...), for roll-up rows that don't cut
  2852|         # on client at all (e.g. a BC-wide aggregate). Pooling all of those
  2853|         # under a single "" key would group unrelated collections together;
  2854|         # fall back to business_center_label first (the real, populated cut
  2855|         # dimension for BC-scoped rows per build_segment_manifest.py), then
  2856|         # to collection_label as a last-resort fallback for whenever that
  2857|         # field does get wired in. is_blank_or_na() (shared with
  2858|         # build_segment_manifest.py) recognizes any NA spelling, not just the
  2859|         # one literal "__NOT_APPLICABLE__" token this used to hardcode.
  2860|         #
  2861|         # client_label, business_center_label, and collection_label are
  2862|         # distinct cut dimensions with independent text namespaces — a real
  2863|         # client named e.g. "BC_2270" must not collide with a business-center
  2864|         # row whose business_center_label happens to be the same text. The
  2865|         # key therefore tags which dimension supplied the value instead of
  2866|         # collapsing them all into one bare string slot.
  2867|         #
  2868|         # When client_label is populated, business_center_label is folded
  2869|         # into the same key alongside it (rather than being ignored) --
  2870|         # under the explicit-metadata contract client_label is always
  2871|         # populated for InternalEnterprise-internal rows too ("InternalEnterprise"), so without
  2872|         # this, an Enterprise-scoped Template (InternalEnterprise/0000) and a specific
  2873|         # business center's Template (InternalEnterprise/2270) would collapse into one
  2874|         # "client=InternalEnterprise" bucket and incorrectly pair with that business
  2875|         # center's Projects as if they were the same governance population.
  2876|         # A populated-client, blank-bc row (a client-wide roll-up) still
  2877|         # gets its own distinct bucket via the empty bc slot.
  2878|         #
  2879|         # collection_label is NOT folded into this key, even though it is a
  2880|         # real cut dimension in build_segment_manifest.py's DIMENSION_CONFIG
  2881|         # that can distinguish multiple named collections under the same
  2882|         # client or business_center. It is intentionally handled the same
  2883|         # way discipline_label is — via _collection_match() below, applied
  2884|         # when pairs are generated — rather than as a hard partition here.
  2885|         # Hard-partitioning by collection would sever the client_label case:
  2886|         # a real client's Container/Template rows are typically tagged with
  2887|         # that client's own collection_label (e.g. "ClientAlpha Standards"), but
  2888|         # its Project rows are typically not tagged with any collection at
  2889|         # all. Splitting on collection here would put those two populations
  2890|         # in different buckets and silently stop producing
  2891|         # template_to_project/container_to_project pairs for that client —
  2892|         # the tool's primary comparison. A soft match (required only when
  2893|         # both sides have a populated value) blocks two different, both-
  2894|         # populated collections from pairing while still letting a
  2895|         # collection-tagged standards segment pair against its
  2896|         # collection-blank usage.
  2897|         unit = row.get("unit_system", "").strip()
  2898|         client = row.get("client_label", "").strip()
  2899|         if not is_blank_or_na(client):
  2900|             return ("client", client, _bc_of(row), unit)
  2901|         bc = _bc_of(row)
  2902|         if bc:
  2903|             return ("business_center", bc, unit)
  2904|         collection = row.get("collection_label", "").strip()
  2905|         if not is_blank_or_na(collection):
  2906|             return ("collection", collection, unit)
  2907|         # client_label, business_center_label, and collection_label are all
  2908|         # blank/NA — every spelling of "not applicable" must land on the
  2909|         # same key here, or e.g. a Template row spelled "__NOT_APPLICABLE__"
  2910|         # and a Container row spelled "n/a" (both otherwise-blank, no bc, no
  2911|         # collection) would fragment into different by_key buckets and never
  2912|         # get compared. Returning the raw `client` token instead of a
  2913|         # canonical "" would reintroduce exactly the fragmentation this
  2914|         # fallback chain exists to prevent.
  2915|         return ("client", "", unit)
  2916| 
  2917|     def _disc(row: Dict[str, str]) -> str:
  2918|         return row.get("discipline_label", "").strip()
  2919| 
  2920|     def _disc_match(ra: Dict[str, str], rb: Dict[str, str]) -> bool:
  2921|         # Discipline comparisons require the same unit_system and the same
  2922|         # discipline_label, full stop -- no cross-discipline wildcard mode.
  2923|         # Under the explicit-metadata contract discipline_label is a
  2924|         # required, always-populated field, so this is an exact match in
  2925|         # practice; it is intentionally not blank-tolerant for any
  2926|         # malformed/legacy row that reaches this function directly.
  2927|         return _disc(ra) == _disc(rb)
  2928| 
  2929|     def _collection(row: Dict[str, str]) -> str:
  2930|         value = row.get("collection_label", "").strip()
  2931|         return "" if is_blank_or_na(value) else value
  2932| 
  2933|     # A collection-blank row is a wildcard ONLY when its blankness means
  2934|     # "collection is simply not tracked here" (the ClientAlpha-shaped case: a
  2935|     # Project row that never got a collection_label). It must NOT wildcard
  2936|     # when the blankness instead means "this segment is a roll-up pooling
  2937|     # every collection under it together" — e.g. build_segment_manifest.py
  2938|     # now keeps a runnable business-center-scoped Template/Container
  2939|     # aggregate (blank collection_label) alongside its collection-specific
  2940|     # children whenever the aggregate's population isn't identical to any
  2941|     # single child's (i.e. the business center hosts more than one named
  2942|     # collection). Wildcard-matching that aggregate against one specific
  2943|     # collection's segment on the other side would mix the pooled
  2944|     # population with a single library's population in the same
  2945|     # comparison — precisely what collection_label was added to keep
  2946|     # apart. A row counts as a roll-up when some OTHER manifest row's
  2947|     # parent_segment_id points at it and that other row has a populated
  2948|     # collection_label.
  2949|     _collection_rollup_ids = {
  2950|         row.get("parent_segment_id", "").strip()
  2951|         for row in manifest.values()
  2952|         if row.get("parent_segment_id", "").strip()
  2953|         and not is_blank_or_na(row.get("collection_label", ""))
  2954|     }
  2955| 
  2956|     def _is_collection_rollup(row: Dict[str, str]) -> bool:
  2957|         return row.get("segment_id", "") in _collection_rollup_ids
  2958| 
  2959|     def _collection_match(ra: Dict[str, str], rb: Dict[str, str]) -> bool:
  2960|         ca, cb = _collection(ra), _collection(rb)
  2961|         if ca and cb:
  2962|             return ca == cb
  2963|         if ca and not cb:
  2964|             return not _is_collection_rollup(rb)
  2965|         if cb and not ca:
  2966|             return not _is_collection_rollup(ra)
  2967|         return True
  2968| 
  2969|     by_key: Dict[Tuple[str, ...], Dict[str, List[str]]] = defaultdict(
  2970|         lambda: defaultdict(list)
  2971|     )
  2972|     for sid, row in manifest.items():
  2973|         role = row.get("governance_role", "").strip().lower()
  2974|         rt = row.get("run_type", "").strip().lower()
  2975|         if (role in ("template", "project", "container") or _is_generic_role(role)) and rt in ("bundle", "reference"):
  2976|             by_key[_key(row)]["generic" if _is_generic_role(role) else role].append(sid)
  2977| 
  2978|     pairs: List[ComparisonPair] = []
  2979| 
  2980|     # Generic / Generic-Host is an upstream stock vocabulary. Compare it across
  2981|     # matching unit_system even when its client_label differs from the downstream
  2982|     # Template/Container/Project client scope. Discipline and collection, when
  2983|     # populated on both sides, still scope the comparison.
  2984|     generic_ids = [
  2985|         sid for sid, row in manifest.items()
  2986|         if _is_generic_role(row.get("governance_role", ""))
  2987|         and row.get("run_type", "").strip().lower() in ("bundle", "reference")
  2988|     ]
  2989|     for g in generic_ids:
  2990|         for sid, row in manifest.items():
  2991|             role = row.get("governance_role", "").strip().lower()
  2992|             if role not in ("template", "container", "project"):
  2993|                 continue
  2994|             if row.get("run_type", "").strip().lower() not in ("bundle", "reference"):
  2995|                 continue
  2996|             if not _same_unit(manifest, g, sid) or not _disc_match(manifest[g], row) or not _collection_match(manifest[g], row):
  2997|                 continue
  2998|             pairs.append((g, sid, f"generic_to_{role}"))
  2999| 
  3000|     for _key_tuple, role_map in by_key.items():
  3001|         generics = role_map.get("generic", [])
  3002|         templates = role_map.get("template", [])
  3003|         projects = role_map.get("project", [])
  3004|         containers = role_map.get("container", [])
  3005| 
  3006|         for g in generics:
  3007|             for t in templates:
  3008|                 if _disc_match(manifest[g], manifest[t]) and _collection_match(manifest[g], manifest[t]):
  3009|                     pairs.append((g, t, "generic_to_template"))
  3010|             for c in containers:
  3011|                 if _disc_match(manifest[g], manifest[c]) and _collection_match(manifest[g], manifest[c]):
  3012|                     pairs.append((g, c, "generic_to_container"))
  3013|             for p in projects:
  3014|                 if _disc_match(manifest[g], manifest[p]) and _collection_match(manifest[g], manifest[p]):
  3015|                     pairs.append((g, p, "generic_to_project"))
  3016| 
  3017|         for t in templates:
  3018|             for p in projects:
  3019|                 if _disc_match(manifest[t], manifest[p]) and _collection_match(manifest[t], manifest[p]):
  3020|                     pairs.append((t, p, "template_to_project"))
  3021|             for c in containers:
  3022|                 if _disc_match(manifest[t], manifest[c]) and _collection_match(manifest[t], manifest[c]):
  3023|                     pairs.append((t, c, "template_to_container"))
  3024|         for c in containers:
  3025|             for p in projects:
  3026|                 if _disc_match(manifest[c], manifest[p]) and _collection_match(manifest[c], manifest[p]):
  3027|                     pairs.append((c, p, "container_to_project"))
  3028| 
  3029|     # --- Scope-level fan-out (enterprise / business_center / client) ---
  3030|     # Independent parallel edges alongside the by_key() pairs above — no
  3031|     # fixed override precedence is assumed between enterprise/business_center/
  3032|     # client standards, since any of them may or may not have adapted from
  3033|     # any other. A business_center-scoped Template/Container is meant to
  3034|     # apply across whichever clients happen to have work in that bc. An
  3035|     # enterprise-scoped Template/Container (InternalEnterprise/"0000") has no client/bc
  3036|     # narrowing of its own, so it is compared against every runnable Project
  3037|     # regardless of scope.
  3038|     eligible_rows = [
  3039|         (sid, row) for sid, row in manifest.items()
  3040|         if row.get("run_type", "").strip().lower() in ("bundle", "reference")
  3041|     ]
  3042|     standard_rows = [
  3043|         (sid, row) for sid, row in eligible_rows
  3044|         if _is_standard_role(_role_key(row.get("governance_role", "")))
  3045|     ]
  3046|     project_rows = [
  3047|         (sid, row) for sid, row in eligible_rows
  3048|         if _role_key(row.get("governance_role", "")) == "project"
  3049|     ]
  3050|     enterprise_standards = [(sid, row) for sid, row in standard_rows if _scope_level(row, policy) == "enterprise"]
  3051|     bc_standards = [(sid, row) for sid, row in standard_rows if _scope_level(row, policy) == "business_center"]
  3052|     # enterprise_to_client targets: a client's standards, whether narrowed to
  3053|     # one specific business center (client_business_center scope) or pooled
  3054|     # across every business center that client touches (a client-wide
  3055|     # roll-up). Both are legitimate, distinct targets -- if a client has
  3056|     # both, they produce separate comparison rows, not a merged population.
  3057|     client_standards = [
  3058|         (sid, row) for sid, row in standard_rows
  3059|         if _scope_level(row, policy) == "client_business_center" or _is_client_wide_rollup(row, policy)
  3060|     ]
  3061| 
  3062|     # These loops group purely by scope level, ignoring parent_segment_id —
  3063|     # so an ancestor and its own descendant (e.g. an enterprise-scoped
  3064|     # Template and a bc/client-scoped Template nested under it) can
  3065|     # otherwise land on opposite sides of one of these edges even though
  3066|     # segments are hierarchical cuts of the same underlying file population
  3067|     # (a descendant's data is always a subset of its ancestor's). Pairing
  3068|     # them as independent standards would compare a segment against data
  3069|     # that already contains its own.
  3070|     #
  3071|     # structural_ancestor only (D-027 decision): this function's guard stays
  3072|     # on _build_ancestor_map()/_is_lineage_related() alone, now upgraded for
  3073|     # free to the complete transitive-closure lattice (previously a single
  3074|     # parent_segment_id chain, which could under-report ancestors whenever a
  3075|     # segment had more than one non-root dimension present). Re-running the
  3076|     # corpus-level violation check against this function found zero real
  3077|     # population-subset violations both before and after that completeness
  3078|     # fix, so layering population_containment here too — as
  3079|     # discover_sibling_segments() does, where a corpus-verified defect
  3080|     # justified it — is deferred rather than spent speculatively. Revisit if
  3081|     # a future corpus run surfaces a governance_chain violation
  3082|     # structural_ancestor alone doesn't catch.
  3083|     ancestor_map = _build_ancestor_map(manifest)
  3084| 
  3085|     for e_sid, e_row in enterprise_standards:
  3086|         for p_sid, p_row in project_rows:
  3087|             if _is_lineage_related(ancestor_map, e_sid, p_sid):
  3088|                 continue
  3089|             if (
  3090|                 _same_unit(manifest, e_sid, p_sid)
  3091|                 and _disc_match(e_row, p_row)
  3092|                 and _collection_match(e_row, p_row)
  3093|             ):
  3094|                 pairs.append((e_sid, p_sid, "enterprise_to_project"))
  3095| 
  3096|     for bc_sid, bc_row in bc_standards:
  3097|         bc_value = _bc_of(bc_row)
  3098|         for p_sid, p_row in project_rows:
  3099|             if _bc_of(p_row) != bc_value:
  3100|                 continue
  3101|             if _is_lineage_related(ancestor_map, bc_sid, p_sid):
  3102|                 continue
  3103|             if (
  3104|                 _same_unit(manifest, bc_sid, p_sid)
  3105|                 and _disc_match(bc_row, p_row)
  3106|                 and _collection_match(bc_row, p_row)
  3107|             ):
  3108|                 pairs.append((bc_sid, p_sid, "bc_to_project"))
  3109| 
  3110|     for e_sid, e_row in enterprise_standards:
  3111|         e_role = _role_key(e_row.get("governance_role", ""))
  3112|         for bc_sid, bc_row in bc_standards:
  3113|             if _role_key(bc_row.get("governance_role", "")) != e_role:
  3114|                 continue
  3115|             if _is_lineage_related(ancestor_map, e_sid, bc_sid):
  3116|                 continue
  3117|             if (
  3118|                 _same_unit(manifest, e_sid, bc_sid)
  3119|                 and _disc_match(e_row, bc_row)
  3120|                 and _collection_match(e_row, bc_row)
  3121|             ):
  3122|                 pairs.append((e_sid, bc_sid, "enterprise_to_bc"))
  3123| 
  3124|     for e_sid, e_row in enterprise_standards:
  3125|         e_role = _role_key(e_row.get("governance_role", ""))
  3126|         for c_sid, c_row in client_standards:
  3127|             if _role_key(c_row.get("governance_role", "")) != e_role:
  3128|                 continue
  3129|             if _is_lineage_related(ancestor_map, e_sid, c_sid):
  3130|                 continue
  3131|             if (
  3132|                 _same_unit(manifest, e_sid, c_sid)
  3133|                 and _disc_match(e_row, c_row)
  3134|                 and _collection_match(e_row, c_row)
  3135|             ):
  3136|                 pairs.append((e_sid, c_sid, "enterprise_to_client"))
  3137| 
  3138|     # --- BC-to-BC peers ---
  3139|     # Purpose-built discovery, not an accident of shared parent_segment_id:
  3140|     # every pair of real business centers' same-role, same-discipline
  3141|     # populations. Spans whichever role (Template/Container/Project) has
  3142|     # business_center-scoped rows -- scope level is orthogonal to role.
  3143|     by_role_bc: Dict[str, List[str]] = defaultdict(list)
  3144|     for sid, row in eligible_rows:
  3145|         if _scope_level(row, policy) == "business_center":
  3146|             by_role_bc[_role_key(row.get("governance_role", ""))].append(sid)
  3147|     for _role, sids in by_role_bc.items():
  3148|         for a_sid, b_sid in combinations(sorted(sids), 2):
  3149|             a_row, b_row = manifest[a_sid], manifest[b_sid]
  3150|             if _bc_of(a_row) == _bc_of(b_row):
  3151|                 continue
  3152|             if _is_lineage_related(ancestor_map, a_sid, b_sid):
  3153|                 continue
  3154|             if _same_unit(manifest, a_sid, b_sid) and _disc_match(a_row, b_row):
  3155|                 pairs.append((a_sid, b_sid, "bc_to_bc"))
  3156| 
  3157|     return pairs
  3158| 
  3159| 
  3160| def discover_within_project(
  3161|     manifest: Dict[str, Dict[str, str]],
  3162|     registry: Dict[str, Dict[str, str]],
  3163|     file_metadata: Dict[str, Dict[str, str]],
  3164|     segments_root: Path,
  3165| ) -> List[ComparisonPair]:
  3166|     # Within a single segment, group files by project_label, pair files within group
  3167|     # Represented as (segment_id, segment_id, "within_project") with same seg on both sides
  3168|     pairs: List[ComparisonPair] = []
  3169|     for sid in manifest:
  3170|         reg = registry.get(sid, {})
  3171|         rt = reg.get("run_type", "").strip().lower()
  3172|         if rt in ("skip", "registration"):
  3173|             continue
  3174|         seg_out = segment_output_dir(segments_root, registry, sid)
  3175|         if seg_out is None:
  3176|             continue
  3177|         # Always discover from the all view
  3178|         ba_root = seg_out / "results" / "bundle_analysis" / "all"
  3179|         if not ba_root.exists():
  3180|             continue
  3181|         # Collect eids from ALL domains so eligibility doesn't depend on which
  3182|         # membership_matrix.csv glob happens to return first.
  3183|         eids: Set[str] = set()
  3184|         for mm_path in ba_root.glob("*/membership_matrix.csv"):
  3185|             for row in read_csv_rows(mm_path):
  3186|                 eid = row.get("export_run_id", "").strip()
  3187|                 if eid:
  3188|                     eids.add(eid)
  3189|         if not eids:
  3190|             continue
  3191|         by_proj: Dict[str, List[str]] = defaultdict(list)
  3192|         for eid in eids:
  3193|             meta = file_metadata.get(eid, {})
  3194|             label = meta.get("project_label", "").strip()
  3195|             proj = eid if is_blank_or_na(label) else label
  3196|             by_proj[proj].append(eid)
  3197|         if any(len(v) >= 2 for v in by_proj.values()):
  3198|             pairs.append((sid, sid, "within_project"))
  3199|     return pairs
  3200| 
  3201| 
  3202| # ---------------------------------------------------------------------------
  3203| # Pair deduplication
  3204| # ---------------------------------------------------------------------------
  3205| 
  3206| def deduplicate_pairs(pairs: List[ComparisonPair]) -> List[ComparisonPair]:
  3207|     # Dedup on the full (seg_a, seg_b, comparison_type) triple. Different comparison
  3208|     # types for the same segment pair represent distinct analytical questions and must
  3209|     # all be preserved — only exact triple duplicates are dropped.
  3210|     seen: Set[ComparisonPair] = set()
  3211|     result: List[ComparisonPair] = []
  3212|     for triple in pairs:
  3213|         if triple not in seen:
  3214|             seen.add(triple)
  3215|             result.append(triple)
  3216|     return result
  3217| 
  3218| 
  3219| # sibling_* comparison_type values discover_sibling_segments() can emit,
  3220| # grouped purely by shared parent_segment_id -- every one of these can
  3221| # collide with a purpose-built peer comparison for the exact same
  3222| # (seg_a, seg_b) pair (see drop_legacy_siblings_covered_by_peer_comparisons()).
  3223| _SIBLING_PEER_TYPES = {
  3224|     "sibling_projects", "sibling_templates", "sibling_containers",
  3225|     "sibling_generic", "sibling_segments",
  3226| }
  3227| 
  3228| # Purpose-built peer-comparison types, each discovered independently of
  3229| # parent_segment_id (cross_client: client_label; bc_to_bc/client_cross_bc:
  3230| # business_center_label/scope_level) -- any of these can duplicate a
  3231| # sibling_* row for the same pair whenever the peers they connect also
  3232| # happen to share an immediate parent.
  3233| _PURPOSE_BUILT_PEER_TYPES = {"cross_client", "bc_to_bc", "client_cross_bc"}
  3234| 
  3235| 
  3236| def drop_legacy_siblings_covered_by_peer_comparisons(
  3237|     pairs: List[ComparisonPair],
  3238| ) -> List[ComparisonPair]:
  3239|     """A sibling_* row and a purpose-built peer comparison (cross_client,
  3240|     bc_to_bc, client_cross_bc) can both fire for the exact same (seg_a, seg_b)
  3241|     pair: discover_sibling_segments() groups segments purely by
  3242|     (parent_segment_id, governance_role, unit_system), so two segments a
  3243|     purpose-built peer function already pairs (by client_label, or by
  3244|     scope_level/business_center_label) can ALSO share an immediate parent
  3245|     (e.g. an enterprise-wide "unit|Project" rollup, or a client/bc segment's
  3246|     natural lattice parent) and get re-paired as a sibling_* type too. Unlike
  3247|     deduplicate_pairs()'s general case (different comparison_types for the
  3248|     same pair are usually distinct analytical questions and must all be
  3249|     preserved), a sibling_* row and its purpose-built counterpart measure the
  3250|     identical underlying file-level comparison for the identical two
  3251|     segments (both symmetric Jaccard/containment over the same file
  3252|     inventories) -- keeping both would just double the row count for zero
  3253|     additional signal, since cross_segment_file_pairs.csv carries no
  3254|     comparison_type column to distinguish them by. make_comparison_run_id()
  3255|     now includes comparison_type in its hash, so the two rows would no
  3256|     longer collide on ID -- but they would still be exact duplicates. The
  3257|     purpose-built type is the unambiguous producer for its signal; drop the
  3258|     sibling_* entry (order-independent) for any pair a purpose-built peer
  3259|     type already covers, and leave every other pair/type untouched.
  3260| 
  3261|     enterprise_to_bc/enterprise_to_client (discover_governance_chain()) are
  3262|     NOT in _PURPOSE_BUILT_PEER_TYPES despite having the identical
  3263|     shared-parent collision risk with sibling_templates/sibling_containers,
  3264|     because they are directed reference-union containment, not a duplicate
  3265|     of the sibling_* symmetric measurement -- dropping the sibling_* row
  3266|     there would silently discard real, distinct signal. That case is
  3267|     resolved by make_comparison_run_id() disambiguating on comparison_type
  3268|     instead: both rows survive, each with its own correct ID.
  3269|     """
  3270|     peer_covered_pairs = {
  3271|         frozenset((a, b)) for a, b, ctype in pairs if ctype in _PURPOSE_BUILT_PEER_TYPES
  3272|     }
  3273|     return [
  3274|         (a, b, ctype) for a, b, ctype in pairs
  3275|         if not (ctype in _SIBLING_PEER_TYPES and frozenset((a, b)) in peer_covered_pairs)
  3276|     ]
  3277| 
  3278| 
  3279| # ---------------------------------------------------------------------------
  3280| # comparison_run_id
  3281| # ---------------------------------------------------------------------------
  3282| 
  3283| def make_comparison_run_id(
  3284|     seg_a: str, seg_b: str, executed_utc: str, comparison_type: str = "",
  3285| ) -> str:
  3286|     """comparison_type is included so that two distinct comparison types for
  3287|     the exact same (seg_a, seg_b) pair and timestamp never collide on the
  3288|     same ID. This does happen in practice: e.g. an enterprise (InternalEnterprise/0000)
  3289|     standard and a real-BC standard of the same role can share a
  3290|     parent_segment_id, so discover_sibling_segments() pairs them as
  3291|     sibling_templates/sibling_containers *in addition to*
  3292|     discover_governance_chain() pairing them as enterprise_to_bc -- both use
  3293|     the same (seg_a, seg_b) orientation (sibling's sorted-ID order happens to
  3294|     match enterprise-then-bc order whenever the enterprise segment's
  3295|     generated ID sorts first). Unlike the sibling_*-vs-purpose-built-peer
  3296|     overlap drop_legacy_siblings_covered_by_peer_comparisons() handles
  3297|     (genuinely duplicate symmetric measurements of the same pair), sibling_*
  3298|     and enterprise_to_bc/enterprise_to_client are not duplicates -- sibling_*
  3299|     is symmetric Jaccard, the enterprise_to_* pairing is directed reference-
  3300|     union containment -- so the fix here is to keep both rows and give them
  3301|     distinct IDs, not to drop one."""
  3302|     token = f"{seg_a}|{seg_b}|{comparison_type}|{executed_utc}"
  3303|     digest = hashlib.sha1(token.encode("utf-8")).hexdigest()
  3304|     return f"cmp_{digest[:12]}"
  3305| 
  3306| 
  3307| # ---------------------------------------------------------------------------
  3308| # Core comparison dispatcher
  3309| # ---------------------------------------------------------------------------
  3310| 
```
