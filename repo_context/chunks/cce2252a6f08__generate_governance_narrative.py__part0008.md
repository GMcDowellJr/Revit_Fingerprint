# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 8 of 17
- Original line range: 2943-3443
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: _state_bucket, _mean, _merge_state_buckets, _finalize_state_bucket, build_governance_state_summary, load_delta_summary, render_header, render_evidence_authority_header, render_evidence_authority_header._static_doc_pointer, render_governance_state_model
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  2943| def _state_bucket() -> dict:
  2944|     return {
  2945|         "provided_and_used_count": 0,
  2946|         "provided_but_passive_count": 0,
  2947|         "provided_but_missing_count": 0,
  2948|         "local_active_count": 0,
  2949|         "local_passive_count": 0,
  2950|         "local_unbundled_count": 0,
  2951|         "reference_all_count": 0,
  2952|         "target_all_count": 0,
  2953|         "target_used_count": 0,
  2954|         "generic_to_template_vals": [],
  2955|         "generic_to_container_vals": [],
  2956|         "generic_to_project_vals": [],
  2957|         "provided_to_configured_vals": [],
  2958|         "provided_to_used_vals": [],
  2959|         "provided_passive_vals": [],
  2960|         "provided_missing_vals": [],
  2961|         "local_active_vals": [],
  2962|     }
  2963| 
  2964| 
  2965| _STATE_COUNT_FIELDS = {
  2966|     "provided_and_used": "provided_and_used_count",
  2967|     "provided_but_passive": "provided_but_passive_count",
  2968|     "provided_but_missing": "provided_but_missing_count",
  2969|     "local_active": "local_active_count",
  2970|     "local_passive": "local_passive_count",
  2971|     "local_unbundled": "local_unbundled_count",
  2972| }
  2973| 
  2974| 
  2975| 
  2976| # Synced from compare_cross_segment.py's GOVERNANCE_STATE_DIRECTED_TYPES via direct
  2977| # import (this local copy had drifted -- missing all 4 new scope types, and carrying
  2978| # two entries the producer's write-gate (compare_cross_segment.py:3697,
  2979| # `if ctype in GOVERNANCE_STATE_DIRECTED_TYPES:`) confirms never reach a governance-
  2980| # state output file today:
  2981| #   - "parent_sibling_roles" -- governance-state rows are only ever written for the
  2982| #     imported 10-type set; parent_sibling_roles pairs are cascade-only (Prompt 2).
  2983| #   - "generic_to_downstream" -- appears nowhere in compare_cross_segment.py,
  2984| #     CHANGELOG.md, DECISIONS.md, or the canonical directed-type list in
  2985| #     docs/cross_segment_comparison.md:280. This repo's git history is a shallow
  2986| #     clone whose earliest visible commit already contains it in this file, so its
  2987| #     origin/intent can't be traced further from available history.
  2988| # Per this prompt's instruction not to guess and silently drop, both are kept as a
  2989| # defensive superset rather than removed -- flagged for Greg to confirm neither is
  2990| # needed before deleting.
  2991| _DIRECTED_GOVERNANCE_TYPES = GOVERNANCE_STATE_DIRECTED_TYPES | {
  2992|     "generic_to_downstream",
  2993|     "parent_sibling_roles",
  2994| }
  2995| 
  2996| # The subset of _DIRECTED_GOVERNANCE_TYPES that render_governance_state_section()
  2997| # actually renders today -- everything EXCEPT the four new scope-level types
  2998| # (CASCADE_GROUP3_TYPES), consistent with Prompt 2's deferred-rendering treatment of
  2999| # the same four types in build_cascade. Used to build the domain-level merged view
  3000| # without blending in enterprise_to_project/bc_to_project/enterprise_to_bc/
  3001| # enterprise_to_client -- see build_governance_state_summary().
  3002| _GOVERNANCE_STATE_RENDERED_TYPES = _DIRECTED_GOVERNANCE_TYPES - CASCADE_GROUP3_TYPES
  3003| 
  3004| 
  3005| def _mean(values: list[float]) -> Optional[float]:
  3006|     return statistics.mean(values) if values else None
  3007| 
  3008| 
  3009| def _merge_state_buckets(buckets: list) -> dict:
  3010|     """Sum a list of _state_bucket()-shaped dicts into one (counts add, vals concat)."""
  3011|     merged = _state_bucket()
  3012|     for bucket in buckets:
  3013|         for k, v in bucket.items():
  3014|             if k.endswith("_vals"):
  3015|                 merged[k].extend(v)
  3016|             else:
  3017|                 merged[k] += v
  3018|     return merged
  3019| 
  3020| 
  3021| def _finalize_state_bucket(bucket: dict) -> dict:
  3022|     """Compute shares/labels from one _state_bucket()-shaped dict. Same math regardless
  3023|     of whether `bucket` represents one comparison_type or a merge of several."""
  3024|     ref_n = bucket["reference_all_count"]
  3025|     tgt_used_n = bucket["target_used_count"]
  3026|     provided_used = bucket["provided_and_used_count"]
  3027|     provided_passive = bucket["provided_but_passive_count"]
  3028|     provided_missing = bucket["provided_but_missing_count"]
  3029|     local_active = bucket["local_active_count"]
  3030| 
  3031|     # Prefer explicit summary metrics; otherwise derive from state counts.
  3032|     provided_to_configured = _mean(bucket["provided_to_configured_vals"])
  3033|     if provided_to_configured is None and ref_n:
  3034|         provided_to_configured = (provided_used + provided_passive) / ref_n
  3035| 
  3036|     provided_to_used = _mean(bucket["provided_to_used_vals"])
  3037|     if provided_to_used is None and ref_n:
  3038|         provided_to_used = provided_used / ref_n
  3039| 
  3040|     provided_passive_share = _mean(bucket["provided_passive_vals"])
  3041|     if provided_passive_share is None and ref_n:
  3042|         provided_passive_share = provided_passive / ref_n
  3043| 
  3044|     provided_missing_share = _mean(bucket["provided_missing_vals"])
  3045|     if provided_missing_share is None and ref_n:
  3046|         provided_missing_share = provided_missing / ref_n
  3047| 
  3048|     local_active_share = _mean(bucket["local_active_vals"])
  3049|     if local_active_share is None and tgt_used_n:
  3050|         local_active_share = local_active / tgt_used_n
  3051| 
  3052|     if provided_to_used is not None and provided_to_used >= PRIMARY_READ_ACTIVE_USE_MIN:
  3053|         primary_read = "Provided standard is actively used"
  3054|     elif provided_passive_share is not None and provided_passive_share >= PASSIVE_MATERIAL_THRESHOLD:
  3055|         primary_read = "Provided standard is carried but partly passive"
  3056|     elif local_active_share is not None and local_active_share >= LOCAL_ACTIVE_MATERIAL_THRESHOLD:
  3057|         primary_read = "Active local practice may need roll-up review"
  3058|     elif provided_missing_share is not None and provided_missing_share >= MISSING_MATERIAL_THRESHOLD:
  3059|         primary_read = "Provided content is missing downstream"
  3060|     else:
  3061|         primary_read = "State signal available; no dominant exception pattern"
  3062| 
  3063|     return {
  3064|         **{k: v for k, v in bucket.items() if not k.endswith("_vals")},
  3065|         "generic_to_template": _mean(bucket["generic_to_template_vals"]),
  3066|         "generic_to_container": _mean(bucket["generic_to_container_vals"]),
  3067|         "generic_to_project": _mean(bucket["generic_to_project_vals"]),
  3068|         "provided_to_configured_containment": provided_to_configured,
  3069|         "provided_to_used_containment": provided_to_used,
  3070|         "provided_passive_share": provided_passive_share,
  3071|         "provided_missing_share": provided_missing_share,
  3072|         "local_active_share": local_active_share,
  3073|         "primary_governance_read": primary_read,
  3074|     }
  3075| 
  3076| 
  3077| def build_governance_state_summary(
  3078|     state_rows: list[dict],
  3079|     summary_rows: list[dict],
  3080| ) -> dict:
  3081|     """Aggregate optional governance-state outputs by domain.
  3082| 
  3083|     The renderer accepts either a compact pre-aggregated state summary or the
  3084|     detailed per-pattern governance-state file. Column names are intentionally
  3085|     read leniently so the narrative can remain compatible with early pipeline
  3086|     revisions while the comparison output stabilises.
  3087| 
  3088|     Aggregation is keyed by (domain, comparison_type) throughout, never by domain
  3089|     alone -- rows for enterprise_to_project/bc_to_project/enterprise_to_bc/
  3090|     enterprise_to_client must never be blended into the same number as
  3091|     template_to_project/container_to_project/generic_to_*, since they measure a
  3092|     different axis (scope level, not cascade stage). See
  3093|     docs/governance_narrative_scope_gap_audit.md A3.
  3094|     """
  3095|     by_type = defaultdict(_state_bucket)  # keyed by (domain, comparison_type)
  3096|     seen_comparison_types: set = set()
  3097| 
  3098|     # Compact summary rows, when provided, are authoritative for counts/shares.
  3099|     for row in summary_rows:
  3100|         dom = row.get("domain", "").strip()
  3101|         if not dom or dom in EXCLUDED_FROM_SCORING:
  3102|             continue
  3103|         ctype = row.get("comparison_type", "").strip()
  3104|         if ctype:
  3105|             seen_comparison_types.add(ctype)
  3106|         bucket = by_type[(dom, ctype)]
  3107| 
  3108|         for state, field in _STATE_COUNT_FIELDS.items():
  3109|             raw = _pick(row, field, f"{state}_n", f"n_{state}")
  3110|             if raw:
  3111|                 try:
  3112|                     bucket[field] += int(float(raw))
  3113|                 except ValueError:
  3114|                     pass
  3115| 
  3116|         for field in ("reference_all_count", "target_all_count", "target_used_count"):
  3117|             raw = _pick(row, field, f"n_{field}")
  3118|             if raw:
  3119|                 try:
  3120|                     bucket[field] += int(float(raw))
  3121|                 except ValueError:
  3122|                     pass
  3123| 
  3124|         _add_float(bucket["provided_to_configured_vals"], row, "provided_to_configured_containment")
  3125|         _add_float(bucket["provided_to_used_vals"], row, "provided_to_used_containment")
  3126|         _add_float(bucket["provided_passive_vals"], row, "provided_passive_share")
  3127|         _add_float(bucket["provided_missing_vals"], row, "provided_missing_share")
  3128|         _add_float(bucket["local_active_vals"], row, "local_active_share")
  3129| 
  3130|         if ctype == "generic_to_template":
  3131|             _add_float(bucket["generic_to_template_vals"], row, "provided_to_configured_containment", _resolved_col_name("containment_a_in_b_mean"))
  3132|         elif ctype == "generic_to_container":
  3133|             _add_float(bucket["generic_to_container_vals"], row, "provided_to_configured_containment", _resolved_col_name("containment_a_in_b_mean"))
  3134|         elif ctype == "generic_to_project":
  3135|             _add_float(bucket["generic_to_project_vals"], row, "provided_to_configured_containment", _resolved_col_name("containment_a_in_b_mean"))
  3136| 
  3137|     # Detailed per-pattern rows fill gaps and support early-state files. Kept under
  3138|     # the same (dom, ctype) key as the compact loop so the two data sources merge
  3139|     # coherently below rather than one being type-separated and the other not.
  3140|     for row in state_rows:
  3141|         dom = row.get("domain", "").strip()
  3142|         if not dom or dom in EXCLUDED_FROM_SCORING:
  3143|             continue
  3144|         ctype = row.get("comparison_type", "").strip()
  3145|         if ctype:
  3146|             seen_comparison_types.add(ctype)
  3147|         if ctype and ctype not in _DIRECTED_GOVERNANCE_TYPES:
  3148|             continue
  3149|         bucket = by_type[(dom, ctype)]
  3150|         state = row.get("state", "").strip()
  3151|         field = _STATE_COUNT_FIELDS.get(state)
  3152|         if field:
  3153|             bucket[field] += 1
  3154|         if _truthy(row.get("in_reference_all")):
  3155|             bucket["reference_all_count"] += 1
  3156|         if _truthy(row.get("in_target_all")):
  3157|             bucket["target_all_count"] += 1
  3158|         if _truthy(row.get("in_target_used")):
  3159|             bucket["target_used_count"] += 1
  3160| 
  3161|     _warn_unrecognized_comparison_types(
  3162|         seen_comparison_types, _DIRECTED_GOVERNANCE_TYPES, "build_governance_state_summary"
  3163|     )
  3164| 
  3165|     # Finalise shares and labels: one fully-separated view per (domain, comparison_type)
  3166|     # for inspection/future use, and one merged per-domain view -- ONLY over the types
  3167|     # render_governance_state_section() actually renders today -- for the renderer.
  3168|     domains = {dom for dom, _ctype in by_type}
  3169|     result = {}
  3170|     for dom in domains:
  3171|         by_ctype = {}
  3172|         rendered_buckets = []
  3173|         for (d2, ctype), bucket in by_type.items():
  3174|             if d2 != dom:
  3175|                 continue
  3176|             by_ctype[ctype or "(unspecified)"] = _finalize_state_bucket(bucket)
  3177|             if not ctype or ctype in _GOVERNANCE_STATE_RENDERED_TYPES:
  3178|                 rendered_buckets.append(bucket)
  3179| 
  3180|         if not rendered_buckets:
  3181|             # This domain's ENTIRE governance-state signal is Group 3 (scope-level
  3182|             # fan-out) rows -- deferred, not rendered (see CASCADE_GROUP3_TYPES).
  3183|             # Omit it from the returned map entirely rather than storing an
  3184|             # all-None-but-truthy dict: render_domain_tiers()'s has_state check
  3185|             # (`any(state for _, _, state in group)`) treats ANY non-None dict as
  3186|             # "this tier group has state data" regardless of its values, which
  3187|             # would switch the WHOLE tier group's table to state columns -- hiding
  3188|             # bundle/passive columns for every domain in that group while showing
  3189|             # blank state values for this one. state_summary.get(dom) returning
  3190|             # None here is what every downstream consumer (assign_tier,
  3191|             # detect_anomalies, render_domain_tiers, the CSV writer) already
  3192|             # expects for "no governance-state input."
  3193|             continue
  3194|         merged = _finalize_state_bucket(_merge_state_buckets(rendered_buckets))
  3195|         merged["by_comparison_type"] = by_ctype
  3196|         result[dom] = merged
  3197|     return result
  3198| 
  3199| 
  3200| def load_delta_summary(delta_rows: list[dict]) -> dict:
  3201|     """
  3202|     Summarise legacy delta patterns by attribution category per comparison type.
  3203| 
  3204|     Supports both older delta schema (segment_id_a / segment_id_b) and newer
  3205|     directed schema (segment_id_reference / segment_id_target). This remains a
  3206|     fallback section; governance-state files are preferred when available because
  3207|     target-only deltas cannot surface inherited-but-unused patterns.
  3208|     """
  3209|     summary = defaultdict(lambda: defaultdict(lambda: {
  3210|         "ungoverned": 0, "container_governed": 0, "alt_template": 0
  3211|     }))
  3212|     for r in delta_rows:
  3213|         ref = _pick(r, "segment_id_reference", "segment_id_a")
  3214|         tgt = _pick(r, "segment_id_target", "segment_id_b")
  3215|         key = (ref, tgt)
  3216|         dom = r.get("domain", "")
  3217|         in_c = _truthy(r.get("in_any_container"))
  3218|         in_t = _truthy(r.get("in_any_template"))
  3219|         if in_t:
  3220|             summary[key][dom]["alt_template"] += 1
  3221|         elif in_c:
  3222|             summary[key][dom]["container_governed"] += 1
  3223|         else:
  3224|             summary[key][dom]["ungoverned"] += 1
  3225|     return summary
  3226| 
  3227| 
  3228| # ── section renderers ──────────────────────────────────────────────────────────
  3229| # ── section renderers ──────────────────────────────────────────────────────────
  3230| 
  3231| 
  3232| def render_header(analysis_date: str, corpus: dict, has_state_outputs: bool, legacy_used_fallback: bool,
  3233|                    interpretation_guide_will_be_copied: bool = False) -> str:
  3234|     n_disc = len(corpus.get("disciplines", set()))
  3235|     disc_list = ", ".join(
  3236|         _disc_label(d)
  3237|         for d in sorted(corpus.get("disciplines", set()))
  3238|     ) or "Unknown"
  3239|     client_list = ", ".join(sorted(corpus.get("clients", set()))) or "Unknown"
  3240|     state_note = (
  3241|         "Explicit governance-state outputs are present, so provided/used/passive/missing/local signals are used in the interpretation."
  3242|         if has_state_outputs else
  3243|         "Explicit governance-state outputs are not present; inherited-but-unused and local-active findings are inferred only indirectly."
  3244|     )
  3245|     used_note = (
  3246|         "Used-view columns were not found in the summary schema, so used-view measures fall back to legacy all-view columns where necessary. Claims depending on active use are therefore limited."
  3247|         if legacy_used_fallback else
  3248|         "Used-view columns are present in the summary schema and are kept separate from all-view configured vocabulary."
  3249|     )
  3250|     # PR review finding: this pointer previously named the guide by bare
  3251|     # basename unconditionally, but the guide is only actually copied
  3252|     # alongside this run's output inside main()'s `if args.emit_evidence_
  3253|     # package:` block (D-034) -- with --no-emit-evidence-package, or a
  3254|     # deployment missing the source doc, the pointer named a file that
  3255|     # would not exist beside the narrative. When the guide won't be copied
  3256|     # this run, point at the checked-in repo path instead and say so.
  3257|     interpretation_guide_pointer = (
  3258|         f"`{INTERPRETATION_GUIDE_PATH.name}` in this run's output directory"
  3259|         if interpretation_guide_will_be_copied else
  3260|         f"`docs/governance/{INTERPRETATION_GUIDE_PATH.name}` in the repository "
  3261|         "(not included alongside this run's output -- this run used "
  3262|         "--no-emit-evidence-package or the source doc was not found on disk)"
  3263|     )
  3264|     return f"""# Revit Configuration Governance Analysis
  3265| ## Enterprise BIM Fingerprint System
  3266| ### Analysis Date: {analysis_date}
  3267| 
  3268| ---
  3269| 
  3270| ## Executive Summary
  3271| 
  3272| This document is a deterministic discovery and interpretation report. It identifies where
  3273| Revit configuration vocabulary appears to provide a stable common base, where upstream
  3274| content is carried into downstream files, where project files actively exercise that
  3275| content, and where local or project-created vocabulary may require governance review.
  3276| 
  3277| The report does **not** approve standards, assign ownership, measure compliance, or label
  3278| teams as compliant/non-compliant. Baseline language should be read as a candidate for
  3279| leadership review, not as a decision.
  3280| 
  3281| {state_note} {used_note}
  3282| 
  3283| ---
  3284| 
  3285| ## What This Analysis Is
  3286| 
  3287| This document summarises findings from the Revit Fingerprint System — a pipeline that
  3288| extracts configuration fingerprints from Revit project files and measures how consistently
  3289| those configurations flow from enterprise baseline content through templates, coordination
  3290| files, and live project models.
  3291| 
  3292| The goal is to identify which parts of the enterprise configuration landscape have
  3293| evidence of convergence, propagation, active use, passive inheritance, local creation, or
  3294| missing downstream content. These findings frame governance questions; they do not decide
  3295| standards.
  3296| 
  3297| ---
  3298| 
  3299| ## The Corpus
  3300| 
  3301| This analysis covers **{corpus['total']} Revit files**:
  3302| 
  3303| | Role | Count | Description |
  3304| |---|---:|---|
  3305| | Templates (.rte) | {corpus['Template']} | Standard Revit template files — provided-vocabulary carriers |
  3306| | Coordination Files | {corpus['Container']} | Project coordination, container, and linked model files |
  3307| | Project Models | {corpus['Project']} | Live project Revit files where used-view interpretation is most meaningful |
  3308| 
  3309| Disciplines represented ({n_disc}): {disc_list}.
  3310| Clients represented: {client_list}.
  3311| 
  3312| Unavailable or future segment dimensions, such as project type, business center, and
  3313| region, should not be inferred from this output unless supplied by upstream CSVs.
  3314| 
  3315| ---
  3316| 
  3317| ## How to Read the Analysis
  3318| 
  3319| See {interpretation_guide_pointer}'s "Metric semantics" section for what
  3320| provided/configured/active-use vocabulary, all-view/used-view, containment,
  3321| cross-client similarity, and score-range mean in this report, and how they
  3322| should (and should not) be read.
  3323| 
  3324| ---
  3325| 
  3326| ## Governance Cascade
  3327| 
  3328| ```
  3329| Generic / Enterprise Baseline
  3330|     ↓  [generic → template/container/project containment]
  3331| Templates (.rte)
  3332|     ↓  [template → coordination file / project containment]
  3333| Coordination Files
  3334|     ↓  [coordination file → project containment]
  3335| Project Models — all configured vocabulary
  3336|     ↓  [project all → project used]
  3337| Active Project Use
  3338| ```
  3339| 
  3340| """
  3341| 
  3342| 
  3343| def render_evidence_authority_header(
  3344|     package_schema_version: str,
  3345|     generator_identity: str,
  3346|     emit_evidence_package: bool = True,
  3347|     emit_interpretation_layer: bool = True,
  3348| ) -> str:
  3349|     """States this document's own epistemic role and authority ordering within
  3350|     the governance evidence package. Added alongside governance_package_manifest.json/
  3351|     _health.json/_evidence_map.json/_findings.json (see docs/governance_evidence_package.md)
  3352|     -- this document remains a controlled_interpretation artifact, not authoritative
  3353|     evidence, and no LLM is involved in producing it or any other artifact in this package.
  3354| 
  3355|     The health/findings/evidence-map/reasoning-prerequisites pointer lines are gated
  3356|     on emit_evidence_package -- when a caller passes --no-emit-evidence-package, those
  3357|     files are never written, so this document must not point readers at files that
  3358|     don't exist. The governance_brief.md pointer is separately gated on
  3359|     emit_interpretation_layer (only meaningful when emit_evidence_package is also on --
  3360|     see main()). PR review finding: the interpretation guide/question routes/reading
  3361|     order pointers used to be treated as unconditional on the theory that they're
  3362|     static repo docs that always exist -- true of the repo copy, but not of the
  3363|     per-run copy this document's own reader may only have (D-034's copy-into-`--out`
  3364|     only runs inside main()'s emit_evidence_package branch, and only for a source
  3365|     doc that's actually present on disk). Each pointer below is gated on that same
  3366|     "will this run's --out actually contain a usable copy" condition, falling back
  3367|     to a repository-qualified path with an explicit note when it won't.
  3368|     """
  3369|     def _static_doc_pointer(path: Path, label: str) -> str:
  3370|         will_be_copied = emit_evidence_package and path.exists()
  3371|         return (
  3372|             f"`{path.name}`" if will_be_copied else
  3373|             f"`docs/governance/{path.name}` in the repository (not included "
  3374|             f"alongside this run's output -- {label})"
  3375|         )
  3376|     _not_copied_reason = (
  3377|         "this run used --no-emit-evidence-package" if not emit_evidence_package else
  3378|         "the source doc was not found on disk"
  3379|     )
  3380|     interpretation_guide_pointer = _static_doc_pointer(INTERPRETATION_GUIDE_PATH, _not_copied_reason)
  3381|     question_routes_pointer = _static_doc_pointer(QUESTION_ROUTES_PATH, _not_copied_reason)
  3382|     reading_order_pointer = _static_doc_pointer(READING_ORDER_PATH, _not_copied_reason)
  3383|     package_pointers = (
  3384|         f"""
  3385| > **Package health:** `governance_package_health.json` (schema {package_schema_version})
  3386| > **Structured findings:** `governance_findings.json`
  3387| > **Evidence navigation:** `governance_evidence_map.json`
  3388| > **Reasoning prerequisites:** `governance_evidence_map.json`'s `reasoning_prerequisites`
  3389| > field names the artifacts to check before stating a conclusion -- see
  3390| > {reading_order_pointer} for the reading sequence.
  3391| """
  3392|         if emit_evidence_package else
  3393|         "\n> This run was generated with `--no-emit-evidence-package`, so no "
  3394|         "package health, structured findings, or evidence-map file exists "
  3395|         "alongside this document.\n"
  3396|     )
  3397|     brief_pointer = (
  3398|         "> **Quick top-line read:** `governance_brief.md`\n"
  3399|         if emit_evidence_package and emit_interpretation_layer else ""
  3400|     )
  3401|     return f"""> **Artifact role:** Convenience summary and controlled interpretation
  3402| > (`authority_level: {AUTHORITY_CONTROLLED_INTERPRETATION}`). This document is
  3403| > template-rendered from the deterministic CSVs below by `{generator_identity}` --
  3404| > no LLM is involved in producing this narrative or any other artifact in this package.
  3405| >
  3406| > **Authority ordering:** package health and the source comparison CSVs
  3407| > (`cross_segment_summary.csv`, `cross_segment_pooled.csv`) outrank the
  3408| > deterministic rollups below them (`governance_domain_summary.csv`,
  3409| > `governance_client_summary.csv`, `governance_bc_summary.csv`), which in
  3410| > turn outrank this narrative's prose.
  3411| > If this document disagrees with a rollup CSV or a source CSV, the CSV wins.
  3412| {package_pointers}{brief_pointer}> **Metric semantics and known bad inferences:** {interpretation_guide_pointer}
  3413| > **Where to look for a specific recurring question:** {question_routes_pointer}
  3414| """
  3415| 
  3416| 
  3417| def render_governance_state_model(has_state_outputs: bool) -> str:
  3418|     limitation = "" if has_state_outputs else (
  3419|         "\n> Governance-state CSVs were not provided. Inherited-but-unused and "
  3420|         "local-active roll-up findings are therefore inferred only indirectly from "
  3421|         "summary, bundle, and legacy delta outputs.\n"
  3422|     )
  3423|     return """## Governance State Model
  3424| 
  3425| For directed governance comparisons, the analysis separates three questions:
  3426| 
  3427| 1. Was the pattern provided by an upstream governance source?
  3428| 2. Was it carried into the downstream file vocabulary?
  3429| 3. Was it actively used in project delivery?
  3430| 
  3431| | State | Meaning |
  3432| |---|---|
  3433| | `provided_and_used` | Upstream content reached projects and is active in delivery. |
  3434| | `provided_but_passive` | Upstream content is carried downstream but not actively used by project files. |
  3435| | `provided_but_missing` | Upstream content did not reach the downstream target. |
  3436| | `local_active` | Downstream/project-created or modified content is actively used; possible roll-up candidate. |
  3437| | `local_passive` | Local/downstream content exists but is not actively used. |
  3438| | `local_unbundled` | Local content exists but has weak or no bundle evidence. |
  3439| 
  3440| """ + limitation
  3441| 
  3442| 
  3443| 
```
