# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 7 of 17
- Original line range: 2433-2942
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: build_client_summary, build_client_summary._confirmed_non_healthcare, build_bc_summary, build_bc_summary._note_bc_file, _pick, _truthy, _add_float
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  2433| def build_client_summary(
  2434|     summary_rows: list[dict],
  2435|     pooled_rows: list[dict],
  2436|     sector_map: Optional[dict] = None,
  2437| ) -> list[dict]:
  2438|     """Per-client alignment summary."""
  2439|     sector_map = sector_map or {}
  2440|     # Client existence must not depend on which pool grain happens to have >=2
  2441|     # siblings for that client. _emit_for_groups() in compare_cross_segment.py
  2442|     # requires len(members) >= 2 INDEPENDENTLY per grain (parent_sibling/bc/client),
  2443|     # so a client whose Project segments never share a common immediate parent with
  2444|     # another sibling gets ZERO parent_sibling rows, even though it may have real
  2445|     # bc- or client-grain pooled rows, or real summary_rows (within_project/
  2446|     # sibling_projects) data. An earlier pool_scope filter here (meant to stop
  2447|     # pool-relative metrics from blending three distinct pools together -- see
  2448|     # docs/governance_narrative_scope_gap_audit.md A2) accidentally dropped such
  2449|     # clients from the client section entirely. client_label and n_files_focal both
  2450|     # describe the FOCAL segment itself, not the pool, so they're identical across
  2451|     # a segment's parent_sibling/bc/client pooled rows -- there is no blending risk
  2452|     # in reading them from every pool_scope grain. (If a genuinely pool-relative
  2453|     # metric -- e.g. all_containment_focal_in_pool, used_containment_pool_in_focal,
  2454|     # n_shared_join_hash -- is ever read from pooled_rows in this function, THAT
  2455|     # read must filter by pool_scope at its own point of use; client discovery and
  2456|     # n_files below must not.)
  2457|     all_clients = set()
  2458|     for r in pooled_rows:
  2459|         c = _pick(r, "client_label")
  2460|         if c and r["governance_role"] == "Project":
  2461|             all_clients.add(c)
  2462|     for r in summary_rows:
  2463|         # discover_within_project() can emit within_project rows for ANY
  2464|         # non-skip/non-registration segment, not just Project-role ones (e.g. a
  2465|         # client-scoped Template/Container/Generic segment). This section is
  2466|         # specifically about the client's PROJECT portfolio (project file counts,
  2467|         # project-vs-project coherence), so gate on governance_role_a == "Project"
  2468|         # before treating a row as evidence this client has project data.
  2469|         # sibling_projects rows are already structurally Project-only by
  2470|         # construction (discover_sibling_segments() only labels a pair
  2471|         # "sibling_projects" when both sides share governance_role == "project"),
  2472|         # but the check is kept here too for defense-in-depth.
  2473|         if r["comparison_type"] == "within_project" and r["governance_role_a"] == "Project":
  2474|             c = _pick(r, "client_label_a")
  2475|             if c:
  2476|                 all_clients.add(c)
  2477|         elif r["comparison_type"] in ("sibling_projects", "cross_client") and r["governance_role_a"] == "Project" and r["governance_role_b"] == "Project":
  2478|             for suffix in ("a", "b"):
  2479|                 c = _pick(r, f"client_label_{suffix}")
  2480|                 if c:
  2481|                     all_clients.add(c)
  2482| 
  2483|     # Cross-client Jaccard. cross_client is the purpose-built comparison
  2484|     # (discover_cross_client() in compare_cross_segment.py); sibling_projects is
  2485|     # kept alongside it for corpora that predate that producer change. Reads
  2486|     # client_label_a/b directly rather than positionally parsing segment_id
  2487|     # (the old "len(pa) == 3" assumption only holds for the
  2488|     # unit|role|client-shaped IDs build_segment_manifest.py happens to emit for
  2489|     # a client-only-scoped Project segment -- discover_cross_client() places no
  2490|     # such constraint on segment_id shape, and the row already carries the
  2491|     # client labels directly). ca != cb excludes within-client sibling_projects
  2492|     # pairs (see discover_sibling_segments()'s own docstring on this) -- the
  2493|     # old segment_id-length==3 check happened to reject these too (a
  2494|     # discipline-scoped within-client sibling's segment_id has 4 parts), so
  2495|     # this guard is required to preserve that exclusion now that segment_id
  2496|     # shape is no longer being read at all.
  2497|     #
  2498|     # A pair is excluded here if EITHER side has a CONFIRMED non-comparable
  2499|     # sector -- sector_map.get(c, "unknown") not in ("unknown", "healthcare") --
  2500|     # matching this exact function's own tier definition of "comparable"
  2501|     # below ("Non-comparable (different sector)" fires only for a KNOWN
  2502|     # non-healthcare sector, never for "unknown"). Without this, cross_client
  2503|     # being default-on and pairing every client regardless of sector means a
  2504|     # healthcare client's xc_mean/tier can be driven by a comparison against a
  2505|     # client whose OWN row is separately (and correctly) marked
  2506|     # non-comparable -- a real contamination risk this loop had no defense
  2507|     # against for either source type before.
  2508|     def _confirmed_non_healthcare(client: str) -> bool:
  2509|         return sector_map.get(client, "unknown") not in ("unknown", "healthcare")
  2510| 
  2511|     # xc_by_client/wp_by_client are PRIMARY = used-view union (active practice, per
  2512|     # _recommended_primary_view() in compare_cross_segment.py for sibling_projects/
  2513|     # cross_client/within_project-Project rows); the _all suffix dicts carry the
  2514|     # all-view union as secondary/context (configured/inherited), opposite of the
  2515|     # tc/cp/tp convention where the bare name is all-view.
  2516|     xc_by_client = defaultdict(list)
  2517|     xc_by_client_all = defaultdict(list)
  2518|     for r in summary_rows:
  2519|         if r["comparison_type"] not in ("sibling_projects", "cross_client"):
  2520|             continue
  2521|         if r["domain"] in EXCLUDED_FROM_SCORING:
  2522|             continue
  2523|         ca = _pick(r, "client_label_a")
  2524|         cb = _pick(r, "client_label_b")
  2525|         if ca == cb or ca not in all_clients or cb not in all_clients:
  2526|             continue
  2527|         if _confirmed_non_healthcare(ca) or _confirmed_non_healthcare(cb):
  2528|             continue
  2529|         v = pf(_col(r, "used_union_jaccard"))
  2530|         if v is not None:
  2531|             xc_by_client[ca].append(v)
  2532|             xc_by_client[cb].append(v)
  2533|         v_all = pf(_col(r, "all_union_jaccard"))
  2534|         if v_all is not None:
  2535|             xc_by_client_all[ca].append(v_all)
  2536|             xc_by_client_all[cb].append(v_all)
  2537| 
  2538|     # Within-project coherence. Gated on governance_role_a == "Project" for the
  2539|     # same reason as the all_clients fallback above -- within_project rows exist
  2540|     # for any role, and this section reports PROJECT coherence specifically.
  2541|     wp_by_client = defaultdict(list)
  2542|     wp_by_client_all = defaultdict(list)
  2543|     for r in summary_rows:
  2544|         if r["comparison_type"] != "within_project" or r["governance_role_a"] != "Project":
  2545|             continue
  2546|         c = _pick(r, "client_label_a")
  2547|         # within_project rows never carry all_union_*/used_union_* from the
  2548|         # producer -- see _col_union_or_pairwise()'s docstring.
  2549|         v = pf(_col_union_or_pairwise(r, "used_union_jaccard", "used_jaccard_mean"))
  2550|         if v is not None and c:
  2551|             wp_by_client[c].append(v)
  2552|         v_all = pf(_col_union_or_pairwise(r, "all_union_jaccard", "jaccard_mean"))
  2553|         if v_all is not None and c:
  2554|             wp_by_client_all[c].append(v_all)
  2555| 
  2556|     # n_files from pooled — every pool_scope grain, same rationale as all_clients
  2557|     # above: n_files_focal describes the focal segment, not the pool, so it's
  2558|     # identical across a segment's parent_sibling/bc/client rows. Falls back to
  2559|     # summary_rows' own n_files_a/n_files_b for clients discovered only from
  2560|     # summary_rows above (e.g. a single-project client with no >=2-member pool
  2561|     # grain at all) -- without this, such a client reports n_files=0 / "Low
  2562|     # corpus confidence" despite the summary row itself carrying a real count.
  2563|     client_files = {}
  2564|     for r in pooled_rows:
  2565|         c = _pick(r, "client_label")
  2566|         if c and r["governance_role"] == "Project":
  2567|             nf = int(r["n_files_focal"]) if r.get("n_files_focal") else 0
  2568|             if c not in client_files or nf > client_files[c]:
  2569|                 client_files[c] = nf
  2570|     for r in summary_rows:
  2571|         # Same governance_role_a == "Project" gating as all_clients above --
  2572|         # within_project rows exist for any role, and n_files here specifically
  2573|         # means project file counts for the Client Analysis section.
  2574|         if r["comparison_type"] == "within_project" and r["governance_role_a"] == "Project":
  2575|             c = _pick(r, "client_label_a")
  2576|             nf = int(r["n_files_a"]) if r.get("n_files_a") else 0
  2577|             if c and (c not in client_files or nf > client_files[c]):
  2578|                 client_files[c] = nf
  2579|         elif r["comparison_type"] in ("sibling_projects", "cross_client") and r["governance_role_a"] == "Project" and r["governance_role_b"] == "Project":
  2580|             for suffix in ("a", "b"):
  2581|                 c = _pick(r, f"client_label_{suffix}")
  2582|                 nf = int(r[f"n_files_{suffix}"]) if r.get(f"n_files_{suffix}") else 0
  2583|                 if c and (c not in client_files or nf > client_files[c]):
  2584|                     client_files[c] = nf
  2585| 
  2586|     # Domain-level xc means. Same client_label_a/b-direct-read fix, same
  2587|     # ca != cb within-client exclusion, and same confirmed-non-healthcare
  2588|     # exclusion as xc_by_client above.
  2589|     xc_dom_by_client = defaultdict(lambda: defaultdict(list))
  2590|     xc_dom_by_client_all = defaultdict(lambda: defaultdict(list))
  2591|     for r in summary_rows:
  2592|         if r["comparison_type"] not in ("sibling_projects", "cross_client"):
  2593|             continue
  2594|         ca = _pick(r, "client_label_a")
  2595|         cb = _pick(r, "client_label_b")
  2596|         if ca == cb or ca not in all_clients or cb not in all_clients:
  2597|             continue
  2598|         if _confirmed_non_healthcare(ca) or _confirmed_non_healthcare(cb):
  2599|             continue
  2600|         v = pf(_col(r, "used_union_jaccard"))
  2601|         if v is not None and r["domain"] not in EXCLUDED_FROM_SCORING:
  2602|             xc_dom_by_client[ca][r["domain"]].append(v)
  2603|             xc_dom_by_client[cb][r["domain"]].append(v)
  2604|         v_all = pf(_col(r, "all_union_jaccard"))
  2605|         if v_all is not None and r["domain"] not in EXCLUDED_FROM_SCORING:
  2606|             xc_dom_by_client_all[ca][r["domain"]].append(v_all)
  2607|             xc_dom_by_client_all[cb][r["domain"]].append(v_all)
  2608| 
  2609|     rows_out = []
  2610|     for client in sorted(all_clients):
  2611|         xc_vals = xc_by_client.get(client, [])
  2612|         xc_mean = statistics.mean(xc_vals) if xc_vals else None
  2613|         xc_vals_all = xc_by_client_all.get(client, [])
  2614|         xc_mean_all = statistics.mean(xc_vals_all) if xc_vals_all else None
  2615|         wp_vals = wp_by_client.get(client, [])
  2616|         wp_mean = statistics.mean(wp_vals) if wp_vals else None
  2617|         wp_vals_all = wp_by_client_all.get(client, [])
  2618|         wp_mean_all = statistics.mean(wp_vals_all) if wp_vals_all else None
  2619|         n_files = client_files.get(client, 0)
  2620| 
  2621|         dom_means = {
  2622|             d: statistics.mean(v)
  2623|             for d, v in xc_dom_by_client[client].items()
  2624|             if v
  2625|         }
  2626|         dom_means_all = {
  2627|             d: statistics.mean(v)
  2628|             for d, v in xc_dom_by_client_all[client].items()
  2629|             if v
  2630|         }
  2631|         strongest = sorted(dom_means.items(), key=lambda x: -x[1])[:3]
  2632|         weakest = sorted(dom_means.items(), key=lambda x: x[1])[:3]
  2633| 
  2634|         # Tier from cross-client Jaccard. sector_map is external, editable data
  2635|         # (see load_client_sectors()/--client-sector) -- a client tagged with a
  2636|         # known, non-healthcare sector is treated as non-comparable; an
  2637|         # unclassified client (absent from the file, or no file supplied at all)
  2638|         # falls through to the normal alignment tiers below rather than being
  2639|         # guessed at. See docs/governance_narrative_scope_gap_audit.md C7.
  2640|         sector = sector_map.get(client, "unknown")
  2641|         if sector not in ("unknown", "healthcare"):
  2642|             tier = "Non-comparable (different sector)"
  2643|         elif xc_mean is None:
  2644|             tier = "Insufficient Data"
  2645|         elif xc_mean >= CLIENT_ALIGNMENT_HIGH:
  2646|             tier = "High Cross-Client Alignment"
  2647|         elif xc_mean >= CLIENT_ALIGNMENT_MODERATE:
  2648|             tier = "Moderate Cross-Client Alignment"
  2649|         else:
  2650|             tier = "Low Cross-Client Alignment"
  2651| 
  2652|         # Confidence note based on file count
  2653|         if n_files < CLIENT_CONFIDENCE_LOW_MAX_FILES:
  2654|             conf = f"Low corpus confidence — only {n_files} project files"
  2655|         elif n_files < CLIENT_CONFIDENCE_MODERATE_MAX_FILES:
  2656|             conf = f"Moderate corpus ({n_files} files)"
  2657|         else:
  2658|             conf = f"Good corpus ({n_files} files)"
  2659| 
  2660|         rows_out.append({
  2661|             "client": client,
  2662|             "n_files": n_files,
  2663|             "tier": tier,
  2664|             "xc_mean": xc_mean,
  2665|             "xc_mean_all": xc_mean_all,
  2666|             "wp_mean": wp_mean,
  2667|             "wp_mean_all": wp_mean_all,
  2668|             "confidence_note": conf,
  2669|             "strongest": strongest,
  2670|             "weakest": weakest,
  2671|             "dom_means_all": dom_means_all,
  2672|             "sector": sector,
  2673|             "is_healthcare": sector == "healthcare",
  2674|         })
  2675| 
  2676|     rows_out.sort(key=lambda r: -(r["xc_mean"] or 0))
  2677|     return rows_out
  2678| 
  2679| 
  2680| def build_bc_summary(summary_rows: list[dict], cascade: dict) -> list[dict]:
  2681|     """Per-business-center peer-alignment summary. Structural mirror of
  2682|     build_client_summary() -- same discover/accumulate/tier/sort shape, see
  2683|     that function's own comments for the general rationale.
  2684| 
  2685|     Enterprise is deliberately NOT a row here -- see render_enterprise_section().
  2686|     A peer table exists to compare business centers against each other;
  2687|     Enterprise sits above all of them in the provision chain (Generic/
  2688|     Enterprise -> Template/Container -> Project) and has no peer at its own
  2689|     scope level, so a row here would misrepresent it as one more BC among
  2690|     equals.
  2691| 
  2692|     No sector gating: client_sector.csv classifies CLIENTS (a client-level
  2693|     concept -- see load_client_sectors()), not business centers, so it does
  2694|     not apply to this summary at all, unlike build_client_summary()'s
  2695|     sector-aware tiering.
  2696|     """
  2697|     # BC discovery mirrors build_client_summary()'s own multi-source posture
  2698|     # ("Client existence must not depend on which pool grain happens to have
  2699|     # >=2 siblings for that client"): bc_to_bc requires >=2 peer BCs sharing a
  2700|     # role to exist at all (discover_governance_chain()'s by_role_bc grouping
  2701|     # only pairs BCs that share a role with at least one other BC), so a BC
  2702|     # whose only Template/Container never got a same-role peer elsewhere would
  2703|     # be invisible if bc_to_bc were the only source. enterprise_to_bc has no
  2704|     # such >=2-peer requirement (every real BC-scoped Template/Container is
  2705|     # paired against a matching-role enterprise standard, if one exists), and
  2706|     # bc_to_project pairs every real BC-scoped standard against every Project
  2707|     # unconditionally -- both are included as independent discovery sources
  2708|     # for the same robustness reason. cascade's own tc_bc_by_bc/eb_by_bc keys
  2709|     # are folded in too, as a final defensive backstop.
  2710|     all_bcs: set = set()
  2711|     for r in summary_rows:
  2712|         ct = r["comparison_type"]
  2713|         if ct == "bc_to_bc":
  2714|             # Role-gated to Template/Container (Codex review finding on this
  2715|             # PR, following the earlier bb/bb_used role filter): unlike
  2716|             # enterprise_to_bc/bc_to_project, whose standards side is
  2717|             # structurally restricted to Template/Container by
  2718|             # _is_standard_role() in compare_cross_segment.py, bc_to_bc's
  2719|             # by_role_bc groups ANY role sharing business_center scope --
  2720|             # including Project. A BC visible ONLY through a Project-role
  2721|             # bc_to_bc pair has no Template/Container evidence anywhere this
  2722|             # summary reads, so every field would be permanently blank/
  2723|             # Insufficient Data for it; discovering it here would just add a
  2724|             # row this section can never fill in, not a real coverage gap.
  2725|             for suffix in ("a", "b"):
  2726|                 role = _pick(r, f"governance_role_{suffix}")
  2727|                 if role not in ("Template", "Container"):
  2728|                     continue
  2729|                 bc = _pick(r, f"business_center_label_{suffix}")
  2730|                 if bc:
  2731|                     all_bcs.add(bc)
  2732|         elif ct == "enterprise_to_bc":
  2733|             bc = _pick(r, "business_center_label_b")
  2734|             if bc:
  2735|                 all_bcs.add(bc)
  2736|         elif ct == "bc_to_project":
  2737|             bc = _pick(r, "business_center_label_a")
  2738|             if bc:
  2739|                 all_bcs.add(bc)
  2740|     for d in cascade.values():
  2741|         all_bcs.update(d.get("tc_bc_by_bc", {}).keys())
  2742|         all_bcs.update(d.get("eb_by_bc", {}).keys())
  2743| 
  2744|     # Template+Container file count per BC -- mirrors build_client_summary()'s
  2745|     # client_files (max n_files seen per role, across whichever rows mention
  2746|     # that BC, so a domain-repeated row never inflates the count) -- summed
  2747|     # across the two roles, since a BC's standards footprint spans both
  2748|     # Template and Container files (client_files has only the single Project
  2749|     # role to sum). No EXCLUDED_FROM_SCORING gating, matching client_files'
  2750|     # own precedent: file counts describe corpus size, not a domain-scored
  2751|     # signal.
  2752|     bc_role_files: dict = defaultdict(lambda: defaultdict(int))
  2753| 
  2754|     def _note_bc_file(bc_label: str, role: str, n_files: int) -> None:
  2755|         role = (role or "").strip()
  2756|         if bc_label and role in ("Template", "Container") and n_files > bc_role_files[bc_label][role]:
  2757|             bc_role_files[bc_label][role] = n_files
  2758| 
  2759|     for r in summary_rows:
  2760|         ct = r["comparison_type"]
  2761|         if ct == "bc_to_bc":
  2762|             for suffix in ("a", "b"):
  2763|                 bc = _pick(r, f"business_center_label_{suffix}")
  2764|                 role = _pick(r, f"governance_role_{suffix}")
  2765|                 nf = int(r[f"n_files_{suffix}"]) if r.get(f"n_files_{suffix}") else 0
  2766|                 _note_bc_file(bc, role, nf)
  2767|         elif ct == "enterprise_to_bc":
  2768|             bc = _pick(r, "business_center_label_b")
  2769|             role = _pick(r, "governance_role_b")
  2770|             nf = int(r["n_files_b"]) if r.get("n_files_b") else 0
  2771|             _note_bc_file(bc, role, nf)
  2772|         elif ct == "bc_to_project":
  2773|             # a-side is always the real BC-scoped Template/Container standard
  2774|             # (per discover_governance_chain()'s bc_to_project pairing) -- a BC
  2775|             # discovered ONLY via this comparison_type (no bc_to_bc peer, no
  2776|             # matching-role enterprise standard to pair against) still has a
  2777|             # real file count on this row; without this branch it would report
  2778|             # n_files=0 and a spurious low-confidence note despite the source
  2779|             # row carrying real data.
  2780|             bc = _pick(r, "business_center_label_a")
  2781|             role = _pick(r, "governance_role_a")
  2782|             nf = int(r["n_files_a"]) if r.get("n_files_a") else 0
  2783|             _note_bc_file(bc, role, nf)
  2784|         elif ct == "template_to_container":
  2785|             # Mirrors the bc_to_project fix above for the third discovery
  2786|             # source (all_bcs also folds in tc_bc_by_bc's own keys): a BC
  2787|             # whose only evidence is a genuine same-value "bc::bc" Template->
  2788|             # Container reading (see tc_bc_by_bc's own accumulation in
  2789|             # build_cascade()) still has real n_files_a (Template)/n_files_b
  2790|             # (Container) on that row -- without this branch it would report
  2791|             # n_files=0 despite the row carrying both counts. Only the
  2792|             # value-verified "bc::bc" shape counts, same guard tc_bc_by_bc
  2793|             # itself uses -- a shape-only "client_bc::client_bc" or mismatched-
  2794|             # value "bc!cross::bc!cross" pair is not this BC's own reading.
  2795|             scope_a, scope_b, scope_pair = _group1_scope_pair(r)
  2796|             if scope_pair == "bc::bc":
  2797|                 bc = r.get("business_center_label_a", "")
  2798|                 _note_bc_file(bc, r.get("governance_role_a", ""),
  2799|                                int(r["n_files_a"]) if r.get("n_files_a") else 0)
  2800|                 _note_bc_file(bc, r.get("governance_role_b", ""),
  2801|                                int(r["n_files_b"]) if r.get("n_files_b") else 0)
  2802| 
  2803|     # Cross-BC peer alignment: reuses PR1's cascade[dom]["bb"]/["bb_used"]
  2804|     # (per-domain means already keyed by real (bc_a, bc_b) pair), fanned out
  2805|     # to both sides' flat per-BC pools -- same flat-pool shape as
  2806|     # xc_by_client, but PRIMARY = ALL-view, the OPPOSITE convention from
  2807|     # xc_by_client's used-view-primary. bc_to_bc pairs are (per
  2808|     # _recommended_primary_view() in compare_cross_segment.py) Template/
  2809|     # Container peer comparisons -- role_b is never "project" and
  2810|     # comparison_type is never sibling_projects/cross_client for these rows,
  2811|     # so all-view is the recommended primary, same as sibling_templates/
  2812|     # sibling_containers and the tc/cp/tp family's own bare-name-is-all-view
  2813|     # convention. Reusing xc_by_client's used-primary convention here would be
  2814|     # exactly the bug class flagged before PR1 (build_cascade()'s bc_to_bc
  2815|     # capture) was written.
  2816|     bb_by_bc = defaultdict(list)
  2817|     bb_used_by_bc = defaultdict(list)
  2818|     bb_dom_by_bc = defaultdict(lambda: defaultdict(list))
  2819|     tc_bc_vals = defaultdict(list)
  2820|     tc_bc_used_vals = defaultdict(list)
  2821|     eb_bc_vals = defaultdict(list)
  2822|     eb_bc_used_vals = defaultdict(list)
  2823|     for dom, d in cascade.items():
  2824|         # Key shape is "{role}::{bc_a}::{bc_b}" (role-scoped -- see the
  2825|         # bc_to_bc branch's own comment in the accumulation loop above).
  2826|         # discover_governance_chain()'s by_role_bc groups ANY role sharing
  2827|         # business_center scope (Template, Container, OR Project -- see
  2828|         # compare_cross_segment.py:2509-2513's own "whichever role has
  2829|         # business_center-scoped rows" comment), so a bc_to_bc pair can
  2830|         # legitimately be a Project-vs-Project peer comparison between two
  2831|         # BCs' own Project populations. This CSV's file count and framing
  2832|         # ("Template+Container files", "Internal T->C Coherence") are
  2833|         # explicitly Template/Container-scoped -- pooling in a Project-role
  2834|         # reading would let a Project-only BC pair produce a cross-BC
  2835|         # similarity/tier backed by 0 Template/Container files. Filter to
  2836|         # Template/Container roles only (Codex review finding on this PR);
  2837|         # Project-role bc_to_bc peer evidence is a different signal (BC-scoped
  2838|         # project-portfolio convergence, closer to sibling_projects/
  2839|         # cross_client) not represented anywhere in this summary today.
  2840|         for pair, mean_v in d.get("bb", {}).items():
  2841|             role, bc_a, bc_b = pair.split("::", 2)
  2842|             if role not in ("Template", "Container"):
  2843|                 continue
  2844|             if bc_a in all_bcs:
  2845|                 bb_by_bc[bc_a].append(mean_v)
  2846|                 bb_dom_by_bc[bc_a][dom].append(mean_v)
  2847|             if bc_b in all_bcs:
  2848|                 bb_by_bc[bc_b].append(mean_v)
  2849|                 bb_dom_by_bc[bc_b][dom].append(mean_v)
  2850|         for pair, mean_v in d.get("bb_used", {}).items():
  2851|             role, bc_a, bc_b = pair.split("::", 2)
  2852|             if role not in ("Template", "Container"):
  2853|                 continue
  2854|             if bc_a in all_bcs:
  2855|                 bb_used_by_bc[bc_a].append(mean_v)
  2856|             if bc_b in all_bcs:
  2857|                 bb_used_by_bc[bc_b].append(mean_v)
  2858|         for bc, v in d.get("tc_bc_by_bc", {}).items():
  2859|             tc_bc_vals[bc].append(v)
  2860|         for bc, v in d.get("tc_used_bc_by_bc", {}).items():
  2861|             tc_bc_used_vals[bc].append(v)
  2862|         for bc, v in d.get("eb_by_bc", {}).items():
  2863|             eb_bc_vals[bc].append(v)
  2864|         for bc, v in d.get("eb_used_by_bc", {}).items():
  2865|             eb_bc_used_vals[bc].append(v)
  2866| 
  2867|     rows_out = []
  2868|     for bc in sorted(all_bcs):
  2869|         bb_vals = bb_by_bc.get(bc, [])
  2870|         bb_mean = statistics.mean(bb_vals) if bb_vals else None
  2871|         bb_used_vals = bb_used_by_bc.get(bc, [])
  2872|         bb_used_mean = statistics.mean(bb_used_vals) if bb_used_vals else None
  2873| 
  2874|         tc_bc_mean = statistics.mean(tc_bc_vals[bc]) if tc_bc_vals.get(bc) else None
  2875|         tc_bc_used_mean = statistics.mean(tc_bc_used_vals[bc]) if tc_bc_used_vals.get(bc) else None
  2876|         eb_bc_mean = statistics.mean(eb_bc_vals[bc]) if eb_bc_vals.get(bc) else None
  2877|         eb_bc_used_mean = statistics.mean(eb_bc_used_vals[bc]) if eb_bc_used_vals.get(bc) else None
  2878| 
  2879|         n_files = bc_role_files[bc].get("Template", 0) + bc_role_files[bc].get("Container", 0)
  2880| 
  2881|         dom_means = {d: statistics.mean(v) for d, v in bb_dom_by_bc[bc].items() if v}
  2882|         strongest = sorted(dom_means.items(), key=lambda x: -x[1])[:3]
  2883|         weakest = sorted(dom_means.items(), key=lambda x: x[1])[:3]
  2884| 
  2885|         # Tier from cross-BC peer alignment (all-view), same three-tier shape
  2886|         # as build_client_summary()'s cross-client tier -- see BC_ALIGNMENT_HIGH/
  2887|         # _MODERATE's own definition comment for why these are a separate
  2888|         # (currently value-coincident) profile from CLIENT_ALIGNMENT_HIGH/_MODERATE.
  2889|         if bb_mean is None:
  2890|             tier = "Insufficient Data"
  2891|         elif bb_mean >= BC_ALIGNMENT_HIGH:
  2892|             tier = "High Cross-BC Alignment"
  2893|         elif bb_mean >= BC_ALIGNMENT_MODERATE:
  2894|             tier = "Moderate Cross-BC Alignment"
  2895|         else:
  2896|             tier = "Low Cross-BC Alignment"
  2897| 
  2898|         if n_files < BC_CONFIDENCE_LOW_MAX_FILES:
  2899|             conf = f"Low corpus confidence — only {n_files} Template/Container files"
  2900|         elif n_files < BC_CONFIDENCE_MODERATE_MAX_FILES:
  2901|             conf = f"Moderate corpus ({n_files} files)"
  2902|         else:
  2903|             conf = f"Good corpus ({n_files} files)"
  2904| 
  2905|         rows_out.append({
  2906|             "bc": bc,
  2907|             "n_files": n_files,
  2908|             "tier": tier,
  2909|             "bb_mean": bb_mean,
  2910|             "bb_mean_used": bb_used_mean,
  2911|             "tc_bc_mean": tc_bc_mean,
  2912|             "tc_bc_mean_used": tc_bc_used_mean,
  2913|             "eb_bc_mean": eb_bc_mean,
  2914|             "eb_bc_mean_used": eb_bc_used_mean,
  2915|             "confidence_note": conf,
  2916|             "strongest": strongest,
  2917|             "weakest": weakest,
  2918|         })
  2919| 
  2920|     rows_out.sort(key=lambda r: -(r["bb_mean"] or 0))
  2921|     return rows_out
  2922| 
  2923| 
  2924| def _pick(row: dict, *names: str) -> str:
  2925|     """Return the first non-empty value from row for the provided column names."""
  2926|     for name in names:
  2927|         value = row.get(name, "")
  2928|         if value not in (None, ""):
  2929|             return str(value)
  2930|     return ""
  2931| 
  2932| 
  2933| def _truthy(value) -> bool:
  2934|     return str(value or "").strip().lower() in {"true", "1", "yes", "y"}
  2935| 
  2936| 
  2937| def _add_float(values: list[float], row: dict, *names: str) -> None:
  2938|     v = pf(_pick(row, *names))
  2939|     if v is not None:
  2940|         values.append(v)
  2941| 
  2942| 
```
