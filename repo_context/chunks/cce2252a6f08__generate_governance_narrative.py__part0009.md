# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 9 of 17
- Original line range: 3444-3942
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: render_domain_tiers, render_generic_baseline_scope_section, render_group1_scope_section, render_discipline_section, _format_domain_items, _client_onboarding_profile, render_onboarding_section
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  3444| def render_domain_tiers(cascade: dict, state_summary: Optional[dict] = None,
  3445|                          union_breadth_by_domain: Optional[dict] = None) -> str:
  3446|     # Sort domains by DoD-safe governance classification then score.
  3447|     state_summary = state_summary or {}
  3448|     scored = []
  3449|     for dom, d in cascade.items():
  3450|         if not _has_renderable_cascade_signal(d):
  3451|             # Scope-only domain (Group 3 fan-out data only) -- captured in
  3452|             # `cascade` but not yet tiered/rendered. See CASCADE_GROUP3_TYPES.
  3453|             continue
  3454|         state = state_summary.get(dom)
  3455|         tier = assign_tier(d, state)
  3456|         primary = d["tp"] if d["tp"] is not None else d["cp"]
  3457|         scored.append((dom, tier, d, state, primary or 0.0))
  3458| 
  3459|     scored.sort(key=lambda x: (TIER_ORDER.get(x[1], 99), -x[4], DOMAIN_LABELS.get(x[0], x[0])))
  3460| 
  3461|     sections = ["## Domain Governance Classification\n"]
  3462|     tier_groups = defaultdict(list)
  3463|     for dom, tier, d, state, _ in scored:
  3464|         tier_groups[tier].append((dom, d, state))
  3465| 
  3466|     tier_intros = {
  3467|         TIER_STRONG_BASELINE: (
  3468|             "These domains have strong propagation evidence and no material state exception in the available data. "
  3469|             "They are candidates for baseline ratification review, not already-approved standards."
  3470|         ),
  3471|         TIER_BASELINE_LOCAL_REVIEW: (
  3472|             "These domains have meaningful baseline evidence, but local-active, passive, missing, or active-use signals "
  3473|             "must be resolved before leadership treats them as baseline candidates for approval review."
  3474|         ),
  3475|         TIER_BASELINE_CONTAINER_GAP: (
  3476|             "These domains have strong end-to-end propagation while coordination files are not the main governance vehicle. "
  3477|             "Review whether direct template inheritance is the intended path."
  3478|         ),
  3479|         TIER_INVESTIGATE: (
  3480|             "These domains have useful common-base evidence but need review before baseline language is safe."
  3481|         ),
  3482|         TIER_ACTIVE_LOCAL: (
  3483|             "These domains show material active local/project-created vocabulary. Review for roll-up, playbook, approved-list, "
  3484|             "permitted-variant, or exception-governance treatment."
  3485|         ),
  3486|         TIER_MODERATE_VARIATION: (
  3487|             "These domains show meaningful variation. Governance may require discipline-specific treatment or explicit acceptance "
  3488|             "of client/project variation."
  3489|         ),
  3490|         TIER_SPARSE_LIMITED: (
  3491|             "These domains are sparse or presence-limited. The first governance question is whether the domain should be expected "
  3492|             "across the population before convergence is assessed."
  3493|         ),
  3494|         TIER_HIGH_FRAGMENTATION: (
  3495|             "These domains show high variation. A single baseline is not supported by the current data."
  3496|         ),
  3497|         TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE: (
  3498|             "These domains have no enterprise-wide (fully unscoped) evidence, but DO have pooled "
  3499|             "same-business-center-pair evidence (see the Group 1 Propagation by Scope section below). "
  3500|             "This is business-center-level evidence only — not an enterprise reading — and should not "
  3501|             "be treated as equivalent to the tiers above it."
  3502|         ),
  3503|         TIER_INSUFFICIENT: (
  3504|             "These domains have too little usable evidence in the current corpus for a reliable governance read."
  3505|         ),
  3506|     }
  3507| 
  3508|     sections.append(
  3509|         "> **Classification key:** These labels describe evidence posture only. "
  3510|         "They do not approve standards, assign ownership, or measure compliance. "
  3511|         "Materiality thresholds used by this renderer: local-active ≥15%, passive ≥20%, "
  3512|         "missing ≥20%, and strong-baseline active-use containment ≥75%. These thresholds "
  3513|         "are deterministic narrative guardrails, not governance policy. "
  3514|         "**Tight** reliability means file-pair agreement is consistently high. "
  3515|         "**Presence-based** means the score may reflect whether files carry a domain at all, not agreement quality. "
  3516|         "**Sparse** means the domain appears in too few files for a simple mean to support broad governance claims.\n"
  3517|     )
  3518| 
  3519|     ordered_tiers = [
  3520|         TIER_STRONG_BASELINE,
  3521|         TIER_BASELINE_LOCAL_REVIEW,
  3522|         TIER_BASELINE_CONTAINER_GAP,
  3523|         TIER_INVESTIGATE,
  3524|         TIER_ACTIVE_LOCAL,
  3525|         TIER_MODERATE_VARIATION,
  3526|         TIER_SPARSE_LIMITED,
  3527|         TIER_HIGH_FRAGMENTATION,
  3528|         TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE,
  3529|         TIER_INSUFFICIENT,
  3530|     ]
  3531| 
  3532|     for tier in ordered_tiers:
  3533|         group = tier_groups.get(tier, [])
  3534|         if not group:
  3535|             continue
  3536|         n = len(group)
  3537|         sections.append(f"### {tier} ({n} domain{'s' if n != 1 else ''})\n")
  3538|         sections.append(tier_intros.get(tier, "") + "\n")
  3539| 
  3540|         has_state = any(state for _, _, state in group)
  3541|         if has_state:
  3542|             sections.append(
  3543|                 "| Domain | G→Template | G→Container | G→Project | T→Container | T→Project | C→Project | Cross-Client | Reliability | Provided→Used | Local Active | Passive | Missing |"
  3544|             )
  3545|             sections.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|")
  3546|         else:
  3547|             sections.append(
  3548|                 "| Domain | G→Template | G→Container | G→Project | T→Container | T→Project | C→Project | Cross-Client | Reliability | Bundle Density | Passive Inherit. |"
  3549|             )
  3550|             sections.append("|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|")
  3551| 
  3552|         for dom, d, state in group:
  3553|             label = DOMAIN_LABELS.get(dom, dom)
  3554|             reliability = score_reliability(d)
  3555|             pi_flag = " ⚠️" if dom in PASSIVE_INHERITANCE_RISK_DOMAINS else ""
  3556|             if has_state:
  3557|                 state = state or {}
  3558|                 row = (
  3559|                     f"| {label}{pi_flag} "
  3560|                     f"| {fmt(d.get('gt'))} "
  3561|                     f"| {fmt(d.get('gc'))} "
  3562|                     f"| {fmt(d.get('gp'))} "
  3563|                     f"| {fmt(d['tc'])} "
  3564|                     f"| {fmt(d['tp'])} "
  3565|                     f"| {fmt(d['cp'])} "
  3566|                     f"| {fmt(d['xc'])} "
  3567|                     f"| {reliability} "
  3568|                     f"| {pct(state.get('provided_to_used_containment'))} "
  3569|                     f"| {pct(state.get('local_active_share'))} "
  3570|                     f"| {pct(state.get('provided_passive_share'))} "
  3571|                     f"| {pct(state.get('provided_missing_share'))} |"
  3572|                 )
  3573|             else:
  3574|                 row = (
  3575|                     f"| {label}{pi_flag} "
  3576|                     f"| {fmt(d.get('gt'))} "
  3577|                     f"| {fmt(d.get('gc'))} "
  3578|                     f"| {fmt(d.get('gp'))} "
  3579|                     f"| {fmt(d['tc'])} "
  3580|                     f"| {fmt(d['tp'])} "
  3581|                     f"| {fmt(d['cp'])} "
  3582|                     f"| {fmt(d['xc'])} "
  3583|                     f"| {reliability} "
  3584|                     f"| {pct(d.get('bundle_share_all'), 0)} "
  3585|                     f"| {pct(d.get('passive_indicator'), 0)} |"
  3586|                 )
  3587|             sections.append(row)
  3588| 
  3589|         sections.append("")
  3590| 
  3591|         for dom, d, state in group:
  3592|             notes = detect_anomalies(dom, d, state, (union_breadth_by_domain or {}).get(dom))
  3593|             if notes:
  3594|                 label = DOMAIN_LABELS.get(dom, dom)
  3595|                 sections.append(f"**{label}:** " + " ".join(notes) + "\n")
  3596| 
  3597|     return "\n".join(sections)
  3598| 
  3599| 
  3600| def render_generic_baseline_scope_section(cascade: dict) -> str:
  3601|     """Render the Option C per-target-scope-level breakdown for gt/gc/gp.
  3602| 
  3603|     The scope buckets (enterprise/client/bc/discipline and combinations) are
  3604|     dynamic, not a small fixed set like disciplines -- a per-domain fixed-column
  3605|     table would either explode in width or silently drop combined-dimension
  3606|     buckets (e.g. "client_discipline"). One row per (domain, scope) instead, so
  3607|     every bucket that actually occurred is shown without inventing new columns.
  3608|     """
  3609|     rows = []
  3610|     for dom, d in cascade.items():
  3611|         scopes = set(d.get("gt_by_scope") or {}) | set(d.get("gc_by_scope") or {}) | set(d.get("gp_by_scope") or {})
  3612|         for scope in scopes:
  3613|             rows.append((
  3614|                 dom, scope,
  3615|                 (d.get("gt_by_scope") or {}).get(scope),
  3616|                 (d.get("gc_by_scope") or {}).get(scope),
  3617|                 (d.get("gp_by_scope") or {}).get(scope),
  3618|             ))
  3619|     if not rows:
  3620|         return ""
  3621| 
  3622|     lines = [
  3623|         "## Generic Baseline Propagation by Scope\n",
  3624|         "Breaks the Generic/Enterprise Baseline → Template/Container/Project cascade "
  3625|         "(the top rung of the Governance Cascade diagram above) down by the TARGET's "
  3626|         "own scope level, instead of only the single broadest (enterprise-wide) "
  3627|         "reading. **enterprise** is the same value already shown as G→Template/"
  3628|         "G→Container/G→Project in the Domain Governance Classification table above; "
  3629|         "the other rows are client-/business-center-/discipline-specific evidence "
  3630|         "that a prior pass deliberately excluded from that headline number to avoid "
  3631|         "blending distinct scope grains together, but which is real "
  3632|         "baseline-propagation evidence in its own right.\n",
  3633|         "| Domain | Scope | G→Template | G→Container | G→Project |",
  3634|         "|---|---|---:|---:|---:|",
  3635|     ]
  3636|     for dom, scope, gt_v, gc_v, gp_v in sorted(
  3637|         rows, key=lambda r: (DOMAIN_LABELS.get(r[0], r[0]), r[1] != "enterprise", r[1])
  3638|     ):
  3639|         lines.append(
  3640|             f"| {DOMAIN_LABELS.get(dom, dom)} | {scope} "
  3641|             f"| {fmt(gt_v)} | {fmt(gc_v)} | {fmt(gp_v)} |"
  3642|         )
  3643|     lines.append("")
  3644|     return "\n".join(lines)
  3645| 
  3646| 
  3647| def render_group1_scope_section(cascade: dict) -> str:
  3648|     """Render the bc-pooled fallback per-scope-pair breakdown for tc/cp/tp.
  3649| 
  3650|     Mirrors render_generic_baseline_scope_section() (Group 2's Option C
  3651|     section) exactly, adapted for Group 1's two-sided scope key: each row is
  3652|     keyed by a (scope_a, scope_b) PAIR (e.g. "enterprise::enterprise", "bc::bc"),
  3653|     not a single target scope label, since neither side of a Group 1
  3654|     comparison is gated to a fixed role population the way Group 2's Generic
  3655|     reference side is. "enterprise::enterprise" is the same value already shown
  3656|     as T→Container/T→Project/C→Project in the Domain Governance Classification
  3657|     table above; every other row (typically "bc::bc") is the pooled evidence
  3658|     that used to be silently discarded -- see
  3659|     docs/governance_narrative_group1_scope_gap_investigation.md. Rendered only
  3660|     for domains that actually have by-scope data; omitted entirely otherwise.
  3661|     """
  3662|     rows = []
  3663|     for dom, d in cascade.items():
  3664|         scope_pairs = (
  3665|             set(d.get("tc_by_scope") or {})
  3666|             | set(d.get("cp_by_scope") or {})
  3667|             | set(d.get("tp_by_scope") or {})
  3668|         )
  3669|         for scope_pair in scope_pairs:
  3670|             rows.append((
  3671|                 dom, scope_pair,
  3672|                 (d.get("tc_by_scope") or {}).get(scope_pair),
  3673|                 (d.get("tp_by_scope") or {}).get(scope_pair),
  3674|                 (d.get("cp_by_scope") or {}).get(scope_pair),
  3675|             ))
  3676|     if not rows:
  3677|         return ""
  3678| 
  3679|     lines = [
  3680|         "## Group 1 Propagation by Scope\n",
  3681|         "Breaks the Template → Coordination File → Project cascade (T→Container, "
  3682|         "T→Project, C→Project in the Domain Governance Classification table above) "
  3683|         "down by the (a-side scope, b-side scope) pair of each comparison, instead "
  3684|         "of only the single broadest (enterprise-wide) pair. "
  3685|         "**enterprise::enterprise** is the same value already shown as T→Container/"
  3686|         "T→Project/C→Project above; every other row is scoped evidence that a prior "
  3687|         "pass discarded whenever no fully enterprise-wide pair existed, which is why "
  3688|         "most domains previously showed as Insufficient Evidence despite this "
  3689|         "evidence being present. The scope on each side is not always "
  3690|         "business-center: it can be client-, discipline-, business-center-scoped, "
  3691|         "or a combination — read the scope label itself (e.g. `client::bc`, "
  3692|         "`bc_discipline::bc`) to see which. Only a domain with a genuine "
  3693|         "**bc::bc** row (both sides scoped to the SAME business center) reaches "
  3694|         "the **Insufficient Evidence — Enterprise; BC-Level Evidence Available** "
  3695|         "tier above; other scope pairs are real evidence in their own right but do "
  3696|         "not by themselves place a domain into that tier.\n",
  3697|         "| Domain | Scope | T→Container | T→Project | C→Project |",
  3698|         "|---|---|---:|---:|---:|",
  3699|     ]
  3700|     for dom, scope_pair, tc_v, tp_v, cp_v in sorted(
  3701|         rows, key=lambda r: (DOMAIN_LABELS.get(r[0], r[0]), r[1] != "enterprise::enterprise", r[1])
  3702|     ):
  3703|         lines.append(
  3704|             f"| {DOMAIN_LABELS.get(dom, dom)} | {scope_pair} "
  3705|             f"| {fmt(tc_v)} | {fmt(tp_v)} | {fmt(cp_v)} |"
  3706|         )
  3707|     lines.append("")
  3708|     return "\n".join(lines)
  3709| 
  3710| 
  3711| def render_discipline_section(cascade: dict, summary_rows: list[dict]) -> str:
  3712|     """Render per-discipline within-project coherence and cascade summary."""
  3713|     lines = ["## Discipline Analysis\n"]
  3714| 
  3715|     # Gather within-project by discipline. discover_within_project() emits
  3716|     # within_project rows for ANY non-skip/non-registration segment, not just
  3717|     # Project ones -- a discipline-scoped Template/Container/Generic standards
  3718|     # segment self-compared for internal consistency is a real, common case
  3719|     # here (unlike wp_by_client in build_client_summary(), which gates to
  3720|     # governance_role_a == "Project" and simply excludes non-Project rows,
  3721|     # this section is meant to show discipline coherence from ALL of them).
  3722|     # PRIMARY is therefore picked PER ROW by role: used-view union for Project
  3723|     # rows (active practice, per _recommended_primary_view()), all-view union
  3724|     # for every other role (used-view is not meaningful/primary outside
  3725|     # Project targets -- matches this section's pre-union-adoption behavior,
  3726|     # which read all-view unconditionally for every row regardless of role).
  3727|     # _all only carries a genuine all-view secondary for Project rows -- for
  3728|     # non-Project rows all-view IS primary, so there is no separate "context"
  3729|     # value to expose without mislabeling a not-meaningful used-view number
  3730|     # as if it were all-view.
  3731|     disc_domain_wp = defaultdict(lambda: defaultdict(list))
  3732|     disc_domain_wp_all = defaultdict(lambda: defaultdict(list))
  3733|     disc_file_counts = {}
  3734|     # Tracks whether a discipline's domain_means values came from Project rows
  3735|     # (used-view, active practice), non-Project rows (all-view, configured
  3736|     # standards), or both -- so the rendered "coherence" sentence never
  3737|     # mislabels a standards-only or mixed discipline's all-view number as if
  3738|     # it were an active-usage read (see PR #376 review, second P2 finding).
  3739|     disc_role_mix: dict = defaultdict(lambda: {"project": False, "non_project": False})
  3740|     for r in summary_rows:
  3741|         if r["comparison_type"] != "within_project":
  3742|             continue
  3743|         disc = _pick(r, "discipline_label_a")
  3744|         is_project = r["governance_role_a"] == "Project"
  3745|         # within_project rows never carry all_union_*/used_union_* from the
  3746|         # producer -- see _col_union_or_pairwise()'s docstring.
  3747|         if is_project:
  3748|             v = pf(_col_union_or_pairwise(r, "used_union_jaccard", "used_jaccard_mean"))
  3749|         else:
  3750|             v = pf(_col_union_or_pairwise(r, "all_union_jaccard", "jaccard_mean"))
  3751|         if disc and v is not None:
  3752|             disc_domain_wp[disc][r["domain"]].append(v)
  3753|             disc_role_mix[disc]["project" if is_project else "non_project"] = True
  3754|             if disc not in disc_file_counts:
  3755|                 disc_file_counts[disc] = int(r["n_files_a"]) if r["n_files_a"] else 0
  3756|         if is_project:
  3757|             v_all = pf(_col_union_or_pairwise(r, "all_union_jaccard", "jaccard_mean"))
  3758|             if disc and v_all is not None:
  3759|                 disc_domain_wp_all[disc][r["domain"]].append(v_all)
  3760| 
  3761|     # Has-template flag
  3762|     template_discs = set()
  3763|     for r in summary_rows:
  3764|         disc = _pick(r, "discipline_label_a")
  3765|         if r["governance_role_a"] == "Template" and disc:
  3766|             template_discs.add(disc)
  3767| 
  3768|     for disc in sorted(disc_domain_wp.keys()):
  3769|         label = _disc_label(disc)
  3770|         n_files = disc_file_counts.get(disc, "?")
  3771|         has_template = disc in template_discs
  3772| 
  3773|         domain_means = {
  3774|             d: statistics.mean(v)
  3775|             for d, v in disc_domain_wp[disc].items()
  3776|             if v and d not in EXCLUDED_FROM_SCORING
  3777|         }
  3778|         if not domain_means:
  3779|             continue
  3780| 
  3781|         domain_means_all = {
  3782|             d: statistics.mean(v)
  3783|             for d, v in disc_domain_wp_all[disc].items()
  3784|             if v and d not in EXCLUDED_FROM_SCORING
  3785|         }
  3786| 
  3787|         overall = statistics.mean(domain_means.values())
  3788|         overall_all = statistics.mean(domain_means_all.values()) if domain_means_all else None
  3789|         strongest = sorted(domain_means.items(), key=lambda x: -x[1])[:3]
  3790|         weakest = sorted(domain_means.items(), key=lambda x: x[1])[:3]
  3791| 
  3792|         # Label reflects what domain_means actually contains for THIS discipline
  3793|         # -- Project rows contribute used-view (active practice), non-Project
  3794|         # rows contribute all-view (configured standards); a discipline fed by
  3795|         # both is neither purely one nor the other, so it gets a neutral,
  3796|         # explicit mixed label rather than defaulting to either single claim.
  3797|         role_mix = disc_role_mix.get(disc, {"project": False, "non_project": False})
  3798|         if role_mix["project"] and role_mix["non_project"]:
  3799|             coherence_label = "mixed used-view (Project rows) / all-view (standards rows)"
  3800|         elif role_mix["project"]:
  3801|             coherence_label = "used-view, active practice"
  3802|         else:
  3803|             coherence_label = "all-view, configured standards"
  3804| 
  3805|         lines.append(f"### {label}\n")
  3806|         lines.append(
  3807|             f"Files in corpus: **{n_files}**. "
  3808|             f"{'Discipline-specific templates exist. ' if has_template else 'No discipline-specific templates — coordination files are the primary governance source. '}"
  3809|             f"Mean within-population coherence ({coherence_label}): **{pct(overall)}**"
  3810|             f"{f' (all-view/configured: {pct(overall_all)})' if overall_all is not None else ''}.\n"
  3811|         )
  3812| 
  3813|         lines.append("**Most consistent domains:**")
  3814|         for d, v in strongest:
  3815|             lines.append(f"- {DOMAIN_LABELS.get(d, d)}: {pct(v)}")
  3816| 
  3817|         lines.append("")
  3818|         lines.append("**Least consistent domains:**")
  3819|         for d, v in weakest:
  3820|             lines.append(f"- {DOMAIN_LABELS.get(d, d)}: {pct(v)}")
  3821| 
  3822|         lines.append("")
  3823| 
  3824|     return "\n".join(lines)
  3825| 
  3826| 
  3827| 
  3828| def _format_domain_items(items: list[tuple[str, float]], limit: int = 3) -> str:
  3829|     if not items:
  3830|         return "—"
  3831|     return ", ".join(
  3832|         f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})"
  3833|         for d, v in items[:limit]
  3834|     )
  3835| 
  3836| 
  3837| def _client_onboarding_profile(r: dict) -> dict:
  3838|     """Return deterministic onboarding implications from client-level metrics."""
  3839|     n = r.get("n_files", 0) or 0
  3840|     xc = r.get("xc_mean")
  3841|     wp = r.get("wp_mean")
  3842| 
  3843|     if wp is None:
  3844|         internal_read = "Internal coherence is unavailable; onboarding should not rely on this run alone."
  3845|     elif wp >= ONBOARD_WP_STABLE_MIN:
  3846|         internal_read = "Stable internal portfolio; a new team member can likely rely on a repeatable client/project vocabulary."
  3847|     elif wp >= ONBOARD_WP_MIXED_MIN:
  3848|         internal_read = "Mixed internal portfolio; a new team member needs a client orientation plus project-specific checks."
  3849|     else:
  3850|         internal_read = "High internal variation; learning this client likely means learning several local variants."
  3851| 
  3852|     if xc is None:
  3853|         portability_read = "Cross-client portability is unavailable from this run."
  3854|     elif xc >= ONBOARD_XC_HIGH_PORTABILITY_MIN:
  3855|         portability_read = "High portability from the wider corpus is plausible, subject to domain-level review."
  3856|     elif xc >= ONBOARD_XC_MODERATE_PORTABILITY_MIN:
  3857|         portability_read = "Some common base is portable, but client-specific departures should be documented."
  3858|     else:
  3859|         portability_read = "Client-specific orientation is required; wider-corpus assumptions may not transfer cleanly."
  3860| 
  3861|     if n < ONBOARD_N_FILES_LOW_MAX:
  3862|         confidence_read = "Low sample size; treat as a prompt for review, not a settled client profile."
  3863|     elif n < ONBOARD_N_FILES_MODERATE_MAX:
  3864|         confidence_read = "Moderate sample size; useful for orientation but still sensitive to project mix."
  3865|     else:
  3866|         confidence_read = "Good sample size for an initial onboarding read."
  3867| 
  3868|     common_base = _format_domain_items(r.get("strongest", []))
  3869|     variant_burden = _format_domain_items(r.get("weakest", []))
  3870| 
  3871|     # Only a client with a KNOWN non-healthcare sector gets the different-sector
  3872|     # implication -- an unclassified client (sector == "unknown") must not be
  3873|     # treated as confirmed non-healthcare, since is_healthcare=False alone can't
  3874|     # distinguish "known different sector" from "we don't know."
  3875|     sector = r.get("sector", "unknown")
  3876|     if sector not in ("unknown", "healthcare"):
  3877|         operating_implication = (
  3878|             "Do not use healthcare baseline assumptions as the default. Treat this as a separate sector profile."
  3879|         )
  3880|     elif wp is not None and wp < ONBOARD_WP_MIXED_MIN:
  3881|         operating_implication = (
  3882|             "Create project-start reference material and review local variants before assigning staff across projects."
  3883|         )
  3884|     elif xc is not None and xc < ONBOARD_XC_MODERATE_PORTABILITY_MIN:
  3885|         operating_implication = (
  3886|             "Document client-specific departures from the wider corpus before using firmwide playbooks unchanged."
  3887|         )
  3888|     elif wp is not None and wp >= ONBOARD_WP_STABLE_MIN:
  3889|         operating_implication = (
  3890|             "A compact client playbook is likely useful: capture the common base and the recurring exceptions."
  3891|         )
  3892|     else:
  3893|         operating_implication = (
  3894|             "Use a short client orientation plus domain-specific checks for the weakest-alignment areas."
  3895|         )
  3896| 
  3897|     return {
  3898|         "internal_read": internal_read,
  3899|         "portability_read": portability_read,
  3900|         "confidence_read": confidence_read,
  3901|         "common_base": common_base,
  3902|         "variant_burden": variant_burden,
  3903|         "operating_implication": operating_implication,
  3904|     }
  3905| 
  3906| 
  3907| def render_onboarding_section(client_rows: list[dict]) -> str:
  3908|     """Render client-specific onboarding and operating implications."""
  3909|     if not client_rows:
  3910|         return ""
  3911| 
  3912|     lines = [
  3913|         "## Onboarding / Operating Implications\n",
  3914|         "This section translates client-level consistency into practical onboarding reads. "
  3915|         "It does not judge whether divergence is good or bad. It identifies where a new team "
  3916|         "member can probably rely on a common base, where client-specific orientation is needed, "
  3917|         "and where project-to-project variants should be made explicit.\n",
  3918|         "| Client | New-team-member read | Common base to teach first | Variant / coaching burden | Operating implication |",
  3919|         "|---|---|---|---|---|",
  3920|     ]
  3921| 
  3922|     for r in client_rows:
  3923|         profile = _client_onboarding_profile(r)
  3924|         read = f"{profile['internal_read']} {profile['portability_read']} {profile['confidence_read']}"
  3925|         lines.append(
  3926|             f"| {r['client']} "
  3927|             f"| {read} "
  3928|             f"| {profile['common_base']} "
  3929|             f"| {profile['variant_burden']} "
  3930|             f"| {profile['operating_implication']} |"
  3931|         )
  3932| 
  3933|     lines += [
  3934|         "",
  3935|         "### How leadership can use this\n",
  3936|         "- Use **common base** domains as starting points for onboarding guides or client playbooks.",
  3937|         "- Use **variant / coaching burden** domains as prompts for reference examples, project-start checks, or discipline/client-specific coaching.",
  3938|         "- Treat low sample counts as review triggers, not conclusions.",
  3939|         "- Do not treat lower cross-client similarity as failure unless it affects staff portability, governance clarity, or standards maintenance.\n",
  3940|     ]
  3941|     return "\n".join(lines)
  3942| 
```
