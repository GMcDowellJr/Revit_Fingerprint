# Chunk of tools/compare_cross_segment.py

- Source relative path: `tools/compare_cross_segment.py`
- Chunk: 6 of 13
- Original line range: 2518-2839
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _stash_scope_override, discover_sibling_segments, _is_client_only_project_segment, discover_cross_client, discover_client_cross_bc, discover_parent_siblings
- Source SHA-256: 972c63d7ad4cfd0b45f82d3a62dbb7c62fb4c47bea5596bb5f9b5c34f7f825c4
- Starts inside symbol: no
- Ends inside symbol: no

```
  2518| def _stash_scope_override(
  2519|     policy: EnterprisePolicy,
  2520|     manifest: Dict[str, Dict[str, str]],
  2521|     resolved_sid: str,
  2522|     comparison_type: str,
  2523|     original_row: Dict[str, str],
  2524| ) -> None:
  2525|     """Record `original_row`'s scope metadata onto the RESOLVED descendant's
  2526|     manifest entry, namespaced by comparison_type.
  2527| 
  2528|     segment_id_a/segment_id_b in cross_segment_summary.csv must stay the
  2529|     resolved descendant -- it's the only segment with real on-disk analysis
  2530|     data (segment_output_dir() looks it up via the registry, and the demoted
  2531|     original never gets its own analysis run). But _build_summary_row() also
  2532|     derives business_center_label_a/_b, discipline_label_a/_b, and
  2533|     scope_level_a/_b straight from that same segment's manifest row, which for
  2534|     a resolved descendant is its own narrower identity (e.g.
  2535|     business_center_label="BC_C") rather than the broader (typically blank-bc)
  2536|     population this comparison was actually matched under (Codex review
  2537|     finding on PR #380). Stashing the override here lets _build_summary_row()
  2538|     show the scope the comparison was grouped on, without needing to change
  2539|     which segment_id is used to load data.
  2540| 
  2541|     Namespaced by comparison_type (not just resolved_sid) because the SAME
  2542|     resolved segment can legitimately appear under its own true identity in a
  2543|     different comparison_type -- e.g. "imperial|Project|ClientAlpha|BC_C" is
  2544|     correctly bc-scoped when discover_client_cross_bc() uses it directly, even
  2545|     while cross_client's override for the SAME sid says otherwise.
  2546|     """
  2547|     manifest[resolved_sid][_scope_override_key(comparison_type)] = {
  2548|         "business_center_label": _bc_of(original_row),
  2549|         "discipline_label": original_row.get("discipline_label", ""),
  2550|         "scope_level": _scope_level(original_row, policy) or "",
  2551|     }
  2552| 
  2553| 
  2554| # role_key -> sibling comparison_type, shared between discover_sibling_segments()'s
  2555| # candidate-collection pass (which needs the eventual ctype to key a scope
  2556| # override -- see _stash_scope_override()) and its pair-emission pass.
  2557| _SIBLING_CTYPE_BY_ROLE = {
  2558|     "template": "sibling_templates",
  2559|     "project": "sibling_projects",
  2560|     "container": "sibling_containers",
  2561|     "generic": "sibling_generic",
  2562|     "generic-host": "sibling_generic",
  2563|     "generic_host": "sibling_generic",
  2564| }
  2565| 
  2566| 
  2567| def discover_sibling_segments(
  2568|     policy: EnterprisePolicy,
  2569|     manifest: Dict[str, Dict[str, str]],
  2570|     ancestor_map: Optional[Dict[str, Set[str]]] = None,
  2571|     containment_map: Optional[Dict[str, Set[str]]] = None,
  2572| ) -> List[ComparisonPair]:
  2573|     # Group by (parent_segment_id, governance_role, unit_system). A segment
  2574|     # demoted to run_type="registration" by build_segment_manifest.py's
  2575|     # redundant_single_child pass is resolved to its population-identical
  2576|     # runnable descendant (see _resolve_runnable_segment()) and bucketed under
  2577|     # THIS row's own (parent, role, unit) key, not the descendant's own --
  2578|     # the descendant's parent_segment_id is one or more levels deeper (e.g.
  2579|     # this client's own Project node) and would not be shared with sibling
  2580|     # clients' equivalent substitutes.
  2581|     #
  2582|     # That resolution is also the mechanism behind a real, corpus-verified
  2583|     # defect (D-027): resolving a demoted segment to its population-identical
  2584|     # runnable descendant and then bucketing that descendant under the
  2585|     # DEMOTED row's own (parent, role, unit) key can land two segments in the
  2586|     # same sibling group even though one is a genuine structural or empirical
  2587|     # ancestor/descendant of the other (e.g. a client-wide Container rollup
  2588|     # and that same client's discipline-scoped Container child, both folded
  2589|     # into "sibling_containers" once the discipline child's own parent chain
  2590|     # resolves through a redundant intermediate). Both guards below are
  2591|     # checked -- not just population_containment -- because on the real
  2592|     # corpus every verified violation of this kind turned out to be a
  2593|     # structural_ancestor relation once _build_ancestor_map() was made
  2594|     # complete (D-027); population_containment's Jenks materiality
  2595|     # thresholds, fit deliberately only on non-structural pairs, did not
  2596|     # independently flag any of them (several are small-population pairs
  2597|     # that the materiality filter is designed to treat as noise). It is kept
  2598|     # as a second, independent guard for the non-structural coincidental-
  2599|     # containment case findings showed exists elsewhere in the corpus (see
  2600|     # docs/... population_containment write-up) even though it was not what
  2601|     # fixed today's known violations.
  2602|     #
  2603|     # Both maps are optional and default to "no exclusion" when omitted, so
  2604|     # a caller (or test fixture) with no ancestor_segment_ids data and no
  2605|     # membership data gets the pre-D-027 behavior unchanged; ancestor_map
  2606|     # itself is cheap to derive from the manifest alone when not supplied.
  2607|     if ancestor_map is None:
  2608|         ancestor_map = _build_ancestor_map(manifest)
  2609| 
  2610|     groups: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)
  2611|     for sid, row in manifest.items():
  2612|         parent = row.get("parent_segment_id", "").strip()
  2613|         role = row.get("governance_role", "").strip().lower()
  2614|         role_key = "generic" if _is_generic_role(role) else role
  2615|         us = row.get("unit_system", "").strip()
  2616|         if not (parent and role_key and us):
  2617|             continue
  2618|         resolved = _resolve_runnable_segment(manifest, sid)
  2619|         if resolved is None:
  2620|             continue
  2621|         if resolved != sid:
  2622|             _stash_scope_override(
  2623|                 policy, manifest, resolved,
  2624|                 _SIBLING_CTYPE_BY_ROLE.get(role_key, "sibling_segments"),
  2625|                 row,
  2626|             )
  2627|         groups[(parent, role_key, us)].append(resolved)
  2628| 
  2629|     pairs: List[ComparisonPair] = []
  2630|     for (_, role, _), members in groups.items():
  2631|         members = sorted(set(members))
  2632|         if len(members) < 2:
  2633|             continue
  2634|         ctype = _SIBLING_CTYPE_BY_ROLE.get(role, "sibling_segments")
  2635|         for a, b in combinations(members, 2):
  2636|             if _is_lineage_related(ancestor_map, a, b):
  2637|                 continue
  2638|             if containment_map is not None and _is_population_contained(containment_map, a, b):
  2639|                 continue
  2640|             pairs.append((a, b, ctype))
  2641|     return pairs
  2642| 
  2643| 
  2644| def _is_client_only_project_segment(row: Dict[str, str]) -> bool:
  2645|     """True for a Project-role segment scoped by client_label (and,
  2646|     optionally, discipline_label) alone -- no business_center/collection
  2647|     narrowing -- the "client-level pooled vocabulary" population
  2648|     discover_cross_client() compares peer-to-peer. discipline_label is a
  2649|     grouping dimension for that comparison, not a disqualifier: a client's
  2650|     per-discipline roll-up (e.g. "ClientBeta, Architectural") is just as valid a
  2651|     client-only population as the client's fully blank-discipline portfolio,
  2652|     as long as it isn't further narrowed by business_center or collection.
  2653| 
  2654|     Deliberately stricter than _scope_level(row) == "client_business_center":
  2655|     a client's own business_center- or collection-scoped Project child would
  2656|     also fail that check but is a narrower population than the client's
  2657|     per-discipline portfolio, and comparing a narrower slice for one client
  2658|     against a broader one for another would silently mix comparison grains --
  2659|     exactly the anti-pattern documented on CASCADE_GROUP2_TYPES in
  2660|     generate_governance_narrative.py.
  2661|     """
  2662|     role = row.get("governance_role", "").strip().lower()
  2663|     if role != "project":
  2664|         return False
  2665|     client = row.get("client_label", "").strip()
  2666|     if is_blank_or_na(client):
  2667|         return False
  2668|     return (
  2669|         is_blank_or_na(row.get("business_center_label", ""))
  2670|         and is_blank_or_na(row.get("collection_label", ""))
  2671|     )
  2672| 
  2673| 
  2674| def discover_cross_client(
  2675|     policy: EnterprisePolicy,
  2676|     manifest: Dict[str, Dict[str, str]],
  2677| ) -> List[ComparisonPair]:
  2678|     """Purpose-built client-vs-client comparison: each client's own broadest
  2679|     (client-only-scoped) Project population for a given discipline, paired
  2680|     against every OTHER client's population for that SAME discipline, within
  2681|     the same unit_system. A client's fully blank-discipline portfolio and its
  2682|     per-discipline roll-ups are each distinct populations, compared only
  2683|     against the matching population (same discipline value, blank included)
  2684|     on the other client's side -- never mixed across disciplines.
  2685| 
  2686|     Unlike discover_sibling_segments()'s "sibling_projects" -- which only pairs
  2687|     Project segments sharing an immediate parent_segment_id, an accident of the
  2688|     segment lattice's hierarchy that a corpus with client-scoped Project
  2689|     segments nested straight under one enterprise-wide "Project" parent may
  2690|     still satisfy, but is not guaranteed to -- this function groups purely by
  2691|     (client_label, unit_system, discipline_label) and pairs every distinct
  2692|     client combination sharing a discipline. No shared-parent requirement, no
  2693|     hardcoded sector restriction (sector filtering, where wanted, is a
  2694|     downstream concern of the comparison_type's consumers -- see
  2695|     policies/client_sector.csv).
  2696| 
  2697|     bc-scoped fallback: a client-only Project segment whose Project files all
  2698|     sit in a single business_center_label is population-identical to that
  2699|     business-center-scoped child, so build_segment_manifest.py's
  2700|     redundant_single_child pass demotes it to run_type="registration" instead
  2701|     of leaving a duplicate-population segment runnable -- see
  2702|     _resolve_runnable_segment(). That demotion is now common for single-BC
  2703|     clients (business_center_label having been promoted to a real cut
  2704|     dimension), so the row is resolved to its population-identical runnable
  2705|     descendant instead, so those clients aren't silently dropped from
  2706|     cross_client entirely. This is not the "loosen the blank-bc requirement"
  2707|     anti-pattern _is_client_only_project_segment()'s docstring warns against
  2708|     -- the substitute carries the exact same population_hash the demoted
  2709|     client-only segment would have, not a narrower slice of it.
  2710|     """
  2711|     by_client_unit_disc: Dict[Tuple[str, str, str], str] = {}
  2712|     for sid, row in manifest.items():
  2713|         if not _is_client_only_project_segment(row):
  2714|             continue
  2715|         client = row.get("client_label", "").strip()
  2716|         unit = row.get("unit_system", "").strip()
  2717|         disc = row.get("discipline_label", "").strip()
  2718|         if not unit:
  2719|             continue
  2720|         resolved = _resolve_runnable_segment(manifest, sid)
  2721|         if resolved is None:
  2722|             continue
  2723|         if resolved != sid:
  2724|             _stash_scope_override(policy, manifest, resolved, "cross_client", row)
  2725|         # First-seen wins if the manifest somehow carries more than one
  2726|         # client-only Project segment for the same (client, unit, discipline)
  2727|         # -- shouldn't happen given build_segment_manifest.py's
  2728|         # one-row-per-subset-key contract, but a silent duplicate overwrite
  2729|         # would be worse than a deterministic pick.
  2730|         by_client_unit_disc.setdefault((client, unit, disc), resolved)
  2731| 
  2732|     pairs: List[ComparisonPair] = []
  2733|     items = sorted(by_client_unit_disc.items())
  2734|     for i, ((client_a, unit_a, disc_a), sid_a) in enumerate(items):
  2735|         for (client_b, unit_b, disc_b), sid_b in items[i + 1:]:
  2736|             if client_a == client_b or unit_a != unit_b or disc_a != disc_b:
  2737|                 continue
  2738|             pairs.append((sid_a, sid_b, "cross_client"))
  2739|     return pairs
  2740| 
  2741| 
  2742| def discover_client_cross_bc(
  2743|     policy: EnterprisePolicy,
  2744|     manifest: Dict[str, Dict[str, str]],
  2745| ) -> List[ComparisonPair]:
  2746|     """Same-client, cross-business-center comparison: for a real (non-InternalEnterprise)
  2747|     client whose work spans more than one real business center, compare that
  2748|     client's per-business-center populations against each other, for every
  2749|     pair of business centers the client actually appears in -- not a fixed
  2750|     two-BC comparison.
  2751| 
  2752|     Matched by client_label, governance_role, discipline_label, unit_system.
  2753|     Client-wide roll-ups (business_center_label not cut -- see
  2754|     _is_client_wide_rollup()) are out of scope here; this compares only the
  2755|     client's client_business_center-scoped populations against each other.
  2756|     """
  2757|     by_group: Dict[Tuple[str, str, str, str], List[str]] = defaultdict(list)
  2758|     for sid, row in manifest.items():
  2759|         if row.get("run_type", "").strip().lower() not in ("bundle", "reference"):
  2760|             continue
  2761|         if _scope_level(row, policy) != "client_business_center":
  2762|             continue
  2763|         client = row.get("client_label", "").strip()
  2764|         unit = row.get("unit_system", "").strip()
  2765|         disc = row.get("discipline_label", "").strip()
  2766|         role = row.get("governance_role", "").strip().lower()
  2767|         by_group[(client, unit, disc, role)].append(sid)
  2768| 
  2769|     pairs: List[ComparisonPair] = []
  2770|     for _group_key, sids in by_group.items():
  2771|         for a_sid, b_sid in combinations(sorted(sids), 2):
  2772|             if _bc_of(manifest[a_sid]) == _bc_of(manifest[b_sid]):
  2773|                 continue
  2774|             pairs.append((a_sid, b_sid, "client_cross_bc"))
  2775|     return pairs
  2776| 
  2777| 
  2778| def discover_parent_siblings(
  2779|     manifest: Dict[str, Dict[str, str]],
  2780| ) -> List[ComparisonPair]:
  2781|     # Level-2 segments sharing same level-1 parent, different governance_role
  2782|     # Specifically: Template-role vs Project-role. A segment demoted to
  2783|     # run_type="registration" by build_segment_manifest.py's
  2784|     # redundant_single_child pass is resolved to its population-identical
  2785|     # runnable descendant (see _resolve_runnable_segment()), grouped under
  2786|     # THIS row's own parent since the descendant's own parent_segment_id is
  2787|     # one or more levels deeper. Role is classified from the ORIGINAL
  2788|     # (level-2) row, not the resolved descendant -- a blank-role, client-only
  2789|     # rollup (e.g. "imperial|ClientBeta", pooling every role for that client) can
  2790|     # itself be redundant_single_child to a role-scoped descendant (e.g.
  2791|     # "imperial|Project|ClientBeta", if that client happens to have no non-Project
  2792|     # files) whose OWN governance_role is "Project"; classifying by the
  2793|     # descendant's role would misfile that blank-role rollup as a genuine
  2794|     # Project sibling, which it was never scoped to be.
  2795|     #
  2796|     # Unlike discover_cross_client()/discover_sibling_segments(), no
  2797|     # _stash_scope_override() call here: parent_sibling_roles feeds
  2798|     # generate_governance_narrative.py's _group1_scope_pair() (via
  2799|     # _target_scope_label()/_is_unscoped_segment()), which classifies
  2800|     # "enterprise" scope by re-deriving structure from segment_id_a/_b itself
  2801|     # (splitting on "|" and requiring every part past index 2 to be blank) --
  2802|     # not by trusting business_center_label_a/_b/discipline_label_a/_b at face
  2803|     # value. Since segment_id must stay the resolved descendant (the only
  2804|     # segment with real on-disk data), no column override can make
  2805|     # _is_unscoped_segment() see it as unscoped; overriding the label columns
  2806|     # here would only make the row internally inconsistent (columns
  2807|     # disagreeing with segment_id) without changing that already-shipped,
  2808|     # untouchable classification. The row still lands in whichever
  2809|     # non-enterprise scope_pair bucket its resolved descendant's TRUE shape
  2810|     # implies (e.g. tp_by_scope["bc::enterprise"]) -- a real, if not headline,
  2811|     # Group 1 evidence source, same as any other already-supported
  2812|     # non-enterprise scope_pair.
  2813|     by_parent: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
  2814|     for sid, row in manifest.items():
  2815|         if row.get("segment_level", "").strip() != "2":
  2816|             continue
  2817|         parent = row.get("parent_segment_id", "").strip()
  2818|         if not parent:
  2819|             continue
  2820|         role = row.get("governance_role", "").strip().lower()
  2821|         if role not in ("template", "project"):
  2822|             continue
  2823|         resolved = _resolve_runnable_segment(manifest, sid)
  2824|         if resolved is None:
  2825|             continue
  2826|         by_parent[parent].append((resolved, role))
  2827| 
  2828|     pairs: List[ComparisonPair] = []
  2829|     for _parent, siblings in by_parent.items():
  2830|         siblings = sorted(set(siblings))
  2831|         templates = [s for s, role in siblings if role == "template"]
  2832|         projects = [s for s, role in siblings if role == "project"]
  2833|         for t in templates:
  2834|             for p in projects:
  2835|                 if _same_unit(manifest, t, p):
  2836|                     pairs.append((t, p, "parent_sibling_roles"))
  2837|     return pairs
  2838| 
  2839| 
```
