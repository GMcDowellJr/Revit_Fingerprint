# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 10 of 17
- Original line range: 3943-4391
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: render_client_section, render_enterprise_section, render_bc_section, render_governance_state_section, render_governance_state_section.top_by, render_delta_section, build_union_breadth_by_domain
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  3943| def render_client_section(client_rows: list[dict]) -> str:
  3944|     lines = ["## Client Analysis\n"]
  3945|     lines.append(
  3946|         "Cross-client similarity measures how consistent project configurations are "
  3947|         "across different client engagements, independent of the formal standard. "
  3948|         "High scores indicate practice convergence; low scores indicate client-specific divergence.\n"
  3949|     )
  3950| 
  3951|     lines.append("| Client | Files | Alignment | Cross-Client Similarity | Internal Coherence | Confidence |")
  3952|     lines.append("|---|---|---|---|---|---|")
  3953|     for r in client_rows:
  3954|         lines.append(
  3955|             f"| {r['client']} "
  3956|             f"| {r['n_files']} "
  3957|             f"| {r['tier']} "
  3958|             f"| {fmt(r['xc_mean'])} "
  3959|             f"| {fmt(r['wp_mean'])} "
  3960|             f"| {r['confidence_note']} |"
  3961|         )
  3962| 
  3963|     lines.append("")
  3964|     lines.append(
  3965|         "> **Note on scores:** Cross-client similarity in the 0.30–0.36 range is not a "
  3966|         "failure. It reflects that project configuration is partly client-specific. "
  3967|         "The scores show where common ground exists, not that divergence is wrong.\n"
  3968|     )
  3969| 
  3970|     # Per-client narrative
  3971|     for r in client_rows:
  3972|         lines.append(f"### {r['client']}\n")
  3973|         lines.append(
  3974|             f"**{r['n_files']} project files.** "
  3975|             f"Alignment tier: {r['tier']}. "
  3976|             f"Cross-client similarity: {fmt(r['xc_mean'])}. "
  3977|             f"Internal coherence: {fmt(r['wp_mean'])}.\n"
  3978|         )
  3979|         if r["strongest"]:
  3980|             strong_str = ", ".join(
  3981|                 f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["strongest"]
  3982|             )
  3983|             lines.append(f"Strongest alignment domains: {strong_str}.\n")
  3984|         if r["weakest"]:
  3985|             weak_str = ", ".join(
  3986|                 f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["weakest"]
  3987|             )
  3988|             lines.append(f"Weakest alignment domains: {weak_str}.\n")
  3989|         # Only note a different sector when it's actually KNOWN (not "unknown") --
  3990|         # an unclassified client must not be presented as confirmed non-healthcare.
  3991|         if r.get("sector", "unknown") not in ("unknown", "healthcare"):
  3992|             lines.append(
  3993|                 "_Non-healthcare sector — configuration baseline differs from healthcare "
  3994|                 "client comparisons. Excluded from healthcare cross-client convergence reads._\n"
  3995|             )
  3996|         lines.append("")
  3997| 
  3998|     return "\n".join(lines)
  3999| 
  4000| 
  4001| def render_enterprise_section(cascade: dict) -> str:
  4002|     """Short Enterprise-level summary. Deliberately NOT a governance_bc_summary.csv
  4003|     row or a Business Center Analysis table entry -- Enterprise sits above every
  4004|     business center in the provision chain (Generic/Enterprise -> Template/
  4005|     Container -> Project) and has no peer at its own scope level, so a row in a
  4006|     peer table would misrepresent it as one more BC among equals. Kept as its
  4007|     own top-level section (rendered before Business Center Analysis, not
  4008|     embedded inside it) so it stays visually and structurally distinct.
  4009| 
  4010|     Reads cascade[dom]["tc"] (the existing enterprise::enterprise Template->
  4011|     Container reading -- unchanged, same value already in
  4012|     governance_domain_summary.csv's template_to_container column) and the
  4013|     pooled Group 3 eb/ec means (enterprise's average reach into business
  4014|     centers / client-wide standards, respectively). eb/ec were previously
  4015|     captured-only per CASCADE_GROUP3_TYPES's "not rendered" contract; this is
  4016|     their first render, but only as this short summary -- they are still not
  4017|     tiered, anomaly-detected, or added to governance_domain_summary.csv.
  4018|     """
  4019|     tc_vals = [d["tc"] for d in cascade.values() if d.get("tc") is not None]
  4020|     eb_vals = [d["eb"] for d in cascade.values() if d.get("eb") is not None]
  4021|     ec_vals = [d["ec"] for d in cascade.values() if d.get("ec") is not None]
  4022| 
  4023|     if not (tc_vals or eb_vals or ec_vals):
  4024|         return ""
  4025| 
  4026|     tc_mean = statistics.mean(tc_vals) if tc_vals else None
  4027|     eb_mean = statistics.mean(eb_vals) if eb_vals else None
  4028|     ec_mean = statistics.mean(ec_vals) if ec_vals else None
  4029| 
  4030|     lines = ["## Enterprise Overview\n"]
  4031|     lines.append(
  4032|         "Enterprise is the top of the provision chain (Generic/Enterprise → "
  4033|         "Template/Container → Project), not a peer of any business center or "
  4034|         "client — it has no row in the Business Center Analysis table below or "
  4035|         "the Client Analysis table above.\n"
  4036|     )
  4037|     lines.append(
  4038|         f"Enterprise's own Template→Container coherence (all-view, "
  4039|         f"enterprise::enterprise): {fmt(tc_mean)}. "
  4040|         f"Enterprise standard reach into business centers (all-view mean across "
  4041|         f"{len(eb_vals)} domain reading{'s' if len(eb_vals) != 1 else ''}): {fmt(eb_mean)}. "
  4042|         f"Enterprise standard reach into client-wide standards (all-view mean "
  4043|         f"across {len(ec_vals)} domain reading{'s' if len(ec_vals) != 1 else ''}): {fmt(ec_mean)}.\n"
  4044|     )
  4045|     return "\n".join(lines)
  4046| 
  4047| 
  4048| def render_bc_section(bc_rows: list[dict]) -> str:
  4049|     lines = ["## Business Center Analysis\n"]
  4050|     lines.append(
  4051|         "Cross-BC alignment measures how consistent Template/Container standards "
  4052|         "are across different business centers' own populations, independent of "
  4053|         "the Enterprise-level baseline (see Enterprise Overview above). High "
  4054|         "scores indicate practice convergence between business centers; low "
  4055|         "scores indicate business-center-specific divergence.\n"
  4056|     )
  4057|     lines.append(
  4058|         "> **Primary view note:** unlike the Client Analysis section above (used-"
  4059|         "view primary), the primary reading here is the **all-view** score. "
  4060|         "Cross-BC pairs compare Template/Container populations, not Project "
  4061|         "usage -- per compare_cross_segment.py's own _recommended_primary_view() "
  4062|         "rule, used-view is only primary for Project-target/cross_client/"
  4063|         "sibling_projects comparisons. Reading the used-view column as primary "
  4064|         "here would misread configured standards vocabulary as unused bloat.\n"
  4065|     )
  4066| 
  4067|     lines.append("| Business Center | Files | Alignment | Cross-BC Similarity (all-view) | Internal T→C Coherence (all-view) | Enterprise Reach (all-view) | Confidence |")
  4068|     lines.append("|---|---|---|---|---|---|---|")
  4069|     for r in bc_rows:
  4070|         lines.append(
  4071|             f"| {r['bc']} "
  4072|             f"| {r['n_files']} "
  4073|             f"| {r['tier']} "
  4074|             f"| {fmt(r['bb_mean'])} "
  4075|             f"| {fmt(r['tc_bc_mean'])} "
  4076|             f"| {fmt(r['eb_bc_mean'])} "
  4077|             f"| {r['confidence_note']} |"
  4078|         )
  4079| 
  4080|     lines.append("")
  4081| 
  4082|     # Per-BC narrative
  4083|     for r in bc_rows:
  4084|         lines.append(f"### {r['bc']}\n")
  4085|         lines.append(
  4086|             f"**{r['n_files']} Template/Container files.** "
  4087|             f"Alignment tier: {r['tier']}. "
  4088|             f"Cross-BC similarity (all-view): {fmt(r['bb_mean'])} "
  4089|             f"(used-view: {fmt(r['bb_mean_used'])}). "
  4090|             f"Internal Template→Container coherence (all-view): {fmt(r['tc_bc_mean'])}. "
  4091|             f"Enterprise standard reach into this BC (all-view): {fmt(r['eb_bc_mean'])}.\n"
  4092|         )
  4093|         if r["strongest"]:
  4094|             strong_str = ", ".join(
  4095|                 f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["strongest"]
  4096|             )
  4097|             lines.append(f"Strongest alignment domains: {strong_str}.\n")
  4098|         if r["weakest"]:
  4099|             weak_str = ", ".join(
  4100|                 f"{DOMAIN_LABELS.get(d, d)} ({pct(v)})" for d, v in r["weakest"]
  4101|             )
  4102|             lines.append(f"Weakest alignment domains: {weak_str}.\n")
  4103|         lines.append("")
  4104| 
  4105|     return "\n".join(lines)
  4106| 
  4107| 
  4108| def render_governance_state_section(state_summary: dict) -> str:
  4109|     """Render explicit all/used governance-state findings when available."""
  4110|     if not state_summary:
  4111|         return ""
  4112| 
  4113|     lines = [
  4114|         "## Governance State / Roll-Up Analysis\n",
  4115|         "This section uses explicit governance-state outputs when available. It separates "
  4116|         "standards that were provided and used, standards that were provided but passive, "
  4117|         "provided content that is missing downstream, and active local/project-created "
  4118|         "patterns that may deserve roll-up review.\n",
  4119|         "> Count fields in this section are summed governance-state rows unless the upstream "
  4120|         "state summary has already deduplicated them. Use shares and rankings for leadership "
  4121|         "claims unless a unique-pattern-count guarantee is supplied by the pipeline.\n",
  4122|     ]
  4123| 
  4124|     def top_by(field: str, limit: int = 10):
  4125|         rows = [
  4126|             (dom, d.get(field))
  4127|             for dom, d in state_summary.items()
  4128|             if d.get(field) is not None
  4129|         ]
  4130|         rows.sort(key=lambda x: -x[1])
  4131|         return rows[:limit]
  4132| 
  4133|     generic_rows = []
  4134|     for dom, d in state_summary.items():
  4135|         if any(d.get(k) is not None for k in ("generic_to_template", "generic_to_container", "generic_to_project")):
  4136|             generic_rows.append((dom, d))
  4137|     if generic_rows:
  4138|         generic_rows.sort(key=lambda x: DOMAIN_LABELS.get(x[0], x[0]))
  4139|         lines.append("### Generic / Enterprise Baseline Propagation\n")
  4140|         lines.append("| Domain | Generic→Template | Generic→Container | Generic→Project |")
  4141|         lines.append("|---|---:|---:|---:|")
  4142|         for dom, d in generic_rows[:20]:
  4143|             lines.append(
  4144|                 f"| {DOMAIN_LABELS.get(dom, dom)} "
  4145|                 f"| {fmt(d.get('generic_to_template'))} "
  4146|                 f"| {fmt(d.get('generic_to_container'))} "
  4147|                 f"| {fmt(d.get('generic_to_project'))} |"
  4148|             )
  4149|         lines.append("")
  4150| 
  4151|     passive = top_by("provided_passive_share", 10)
  4152|     local_active = top_by("local_active_share", 10)
  4153|     missing = top_by("provided_missing_share", 10)
  4154| 
  4155|     if passive:
  4156|         lines.append("### Highest Inherited-but-Passive Signal\n")
  4157|         lines.append("These are candidates for starter-content, pruning, approved-list, or exception-governance review; passive inheritance is not automatically bloat.\n")
  4158|         lines.append("| Domain | Passive Share | Provided→Used | Relative Signal |")
  4159|         lines.append("|---|---:|---:|---|")
  4160|         for dom, val in passive:
  4161|             d = state_summary[dom]
  4162|             lines.append(
  4163|                 f"| {DOMAIN_LABELS.get(dom, dom)} "
  4164|                 f"| {pct(val)} "
  4165|                 f"| {pct(d.get('provided_to_used_containment'))} "
  4166|                 f"| {d.get('primary_governance_read', '')} |"
  4167|             )
  4168|         lines.append("")
  4169| 
  4170|     if local_active:
  4171|         lines.append("### Highest Active Local / Roll-Up Candidate Signal\n")
  4172|         lines.append("These domains should be reviewed to decide whether active local practice represents roll-up content, client/discipline playbook material, permitted variants, or legitimate project-specific exceptions.\n")
  4173|         lines.append("| Domain | Local Active Share | Primary Read |")
  4174|         lines.append("|---|---:|---|")
  4175|         for dom, val in local_active:
  4176|             d = state_summary[dom]
  4177|             lines.append(
  4178|                 f"| {DOMAIN_LABELS.get(dom, dom)} "
  4179|                 f"| {pct(val)} "
  4180|                 f"| {d.get('primary_governance_read', '')} |"
  4181|             )
  4182|         lines.append("")
  4183| 
  4184|     if missing:
  4185|         lines.append("### Highest Provided-but-Missing Signal\n")
  4186|         lines.append("These domains need propagation review before the provided vocabulary can be treated as a dependable downstream baseline.\n")
  4187|         lines.append("| Domain | Missing Share | Provided→Configured |")
  4188|         lines.append("|---|---:|---:|")
  4189|         for dom, val in missing:
  4190|             d = state_summary[dom]
  4191|             lines.append(
  4192|                 f"| {DOMAIN_LABELS.get(dom, dom)} "
  4193|                 f"| {pct(val)} "
  4194|                 f"| {pct(d.get('provided_to_configured_containment'))} |"
  4195|             )
  4196|         lines.append("")
  4197| 
  4198|     return "\n".join(lines)
  4199| 
  4200| def render_delta_section(delta_summary: dict) -> str:
  4201|     """Render ungoverned drift section from delta data."""
  4202|     if not delta_summary:
  4203|         return ""
  4204| 
  4205|     lines = [
  4206|         "## Configuration Drift Analysis\n",
  4207|         "Delta patterns are configurations present in project or coordination files "
  4208|         "but absent from the reference template set. They are classified by source:\n",
  4209|         "- **Ungoverned drift:** Not in any template or coordination file — "
  4210|         "project-originated configuration accumulating outside any reference file.\n",
  4211|         "- **Container-governed:** Present in coordination files but not templates — "
  4212|         "governed at the coordination file layer, not the template layer.\n",
  4213|         "- **Alternate template:** Present in a different template — may indicate "
  4214|         "wrong template in use or cross-client convergence patterns.\n",
  4215|         "",
  4216|     ]
  4217| 
  4218|     # Aggregate across all comparison pairs to domain-level totals
  4219|     dom_totals = defaultdict(lambda: {"ungoverned": 0, "container_governed": 0, "alt_template": 0})
  4220|     for pair_data in delta_summary.values():
  4221|         for dom, counts in pair_data.items():
  4222|             for k, v in counts.items():
  4223|                 dom_totals[dom][k] += v
  4224| 
  4225|     # Sort by ungoverned count descending
  4226|     sorted_domains = sorted(
  4227|         dom_totals.items(), key=lambda x: -x[1]["ungoverned"]
  4228|     )[:15]
  4229| 
  4230|     if sorted_domains:
  4231|         lines.append("### Ungoverned Drift by Domain (top 15)\n")
  4232|         lines.append("| Domain | Ungoverned | Container-Governed | Alt-Template |")
  4233|         lines.append("|---|---|---|---|")
  4234|         for dom, counts in sorted_domains:
  4235|             label = DOMAIN_LABELS.get(dom, dom)
  4236|             lines.append(
  4237|                 f"| {label} "
  4238|                 f"| {counts['ungoverned']} "
  4239|                 f"| {counts['container_governed']} "
  4240|                 f"| {counts['alt_template']} |"
  4241|             )
  4242|         lines.append("")
  4243| 
  4244|     return "\n".join(lines)
  4245| 
  4246| 
  4247| _UNION_BREADTH_TIERS = ("corpus_wide", "client_wide", "project_wide", "file_level", "unclassified")
  4248| 
  4249| 
  4250| def build_union_breadth_by_domain(union_inventory_rows: list) -> dict:
  4251|     """Per-domain reuse-breadth pattern counts derived from
  4252|     cross_segment_union_inventory.csv's own presence-percentage columns
  4253|     (D-033) -- corpus-wide/client-wide/project-wide/file-level pattern
  4254|     counts, following the same corpus_wide/client_wide/... vocabulary
  4255|     already rendered from pattern_reuse_distribution.csv in
  4256|     render_union_reuse_summary()'s "Reuse breadth summary" table above, but
  4257|     computed independently from union inventory's own pct_clients_present/
  4258|     n_projects_present/n_files_present fields -- that file carries no
  4259|     reuse_bucket column of its own (UNION_INVENTORY_FIELDS in
  4260|     compare_cross_segment.py), unlike pattern_reuse_distribution.csv
  4261|     (REUSE_DISTRIBUTION_FIELDS).
  4262| 
  4263|     Restricted to governance_role == "Project", view_scope == "all" rows,
  4264|     to avoid double-counting a pattern once per view scope and to keep this
  4265|     a presence/reuse-breadth question (all-view), not an active-use one.
  4266| 
  4267|     build_union_inventory_rows() in compare_cross_segment.py emits one row
  4268|     per (client_label, ..., join_hash) grain -- the same (domain, join_hash)
  4269|     pattern recurs once per client that carries it. pct_clients_present/
  4270|     n_clients_present are identical across every row sharing the same
  4271|     (view_scope, governance_role, discipline_label, unit_system, domain)
  4272|     group -- NOT corpus-wide across the whole domain; a pattern's presence
  4273|     percentage is computed independently per discipline/unit_system grain
  4274|     (compare_cross_segment.py's build_union_inventory_rows(), the
  4275|     clients_by_group/clients_by_pattern grouping keyed on discipline_label/
  4276|     unit_system). n_files_present/n_projects_present are computed per-client
  4277|     (that client's own files/projects only) on top of that. A naive
  4278|     last-row-wins classification would make a pattern's tier depend on
  4279|     row/client iteration order, and merging rows across different
  4280|     discipline/unit_system grains under one key would combine
  4281|     percentages computed against different denominators (PR review
  4282|     findings). Each pattern -- (join_hash, discipline_label, unit_system,
  4283|     within a domain) -- is instead classified into exactly one,
  4284|     highest-qualifying tier across ALL of its same-scope rows -- corpus_wide
  4285|     > client_wide > project_wide > file_level > unclassified -- mirroring
  4286|     the bucket_priority pattern already used above for pattern_reuse_
  4287|     distribution.csv rows (D-033). This means the same conceptual pattern
  4288|     reused identically across multiple disciplines is counted once per
  4289|     discipline, not once per domain -- a direct consequence of the
  4290|     denominators themselves being discipline/unit_system-scoped upstream,
  4291|     not an independent design choice here.
  4292| 
  4293|     corpus_wide/client_wide additionally require n_clients_denominator > 1,
  4294|     mirroring the identical guard compare_cross_segment.py's own
  4295|     _reuse_bucket_for() applies to its corpus_wide classification (PR
  4296|     review finding) -- with only one client in the grain, pct_clients_present
  4297|     is trivially 1.0 for every pattern that client carries at all, which
  4298|     would otherwise label every single-client domain's patterns
  4299|     "corpus-wide reuse" with no actual cross-client evidence.
  4300|     """
  4301|     tier_rank = {t: i for i, t in enumerate(_UNION_BREADTH_TIERS)}
  4302|     pattern_tier: dict = {}
  4303|     # PR review finding: a degraded row's own pct_clients_present, computed
  4304|     # upstream over ALL client rows for that scope (degraded ones included),
  4305|     # doesn't stop a co-occurring healthy row for the SAME pattern_key from
  4306|     # still classifying as e.g. corpus_wide -- and the highest-priority-wins
  4307|     # rule would then let that healthy classification override the degraded
  4308|     # row's "unclassified", using data that shares a contaminated
  4309|     # denominator. Degraded status is a different axis from breadth
  4310|     # (data-quality, not tier), so it must veto the whole pattern_key rather
  4311|     # than just lose an OR-across-tiers contest with a higher-priority tier.
  4312|     pattern_degraded: set = set()
  4313|     for row in union_inventory_rows:
  4314|         if row.get("governance_role") != "Project":
  4315|             continue
  4316|         if row.get("view_scope") != "all":
  4317|             continue
  4318|         domain = row.get("domain", "")
  4319|         join_hash = row.get("join_hash", "")
  4320|         if not domain or not join_hash:
  4321|             continue
  4322|         # PR review finding: build_pattern_reuse_distribution_rows() sends a
  4323|         # row with source_status/inventory_status != "ok" (e.g. missing
  4324|         # source-cluster IDs) straight to unclassified before any breadth
  4325|         # check -- this independent classifier must honor the same gate
  4326|         # instead of presenting degraded inventory as confident breadth
  4327|         # evidence. Same "ok" default as the upstream check.
  4328|         row_degraded = (row.get("source_status") or "ok") != "ok" or (row.get("inventory_status") or "ok") != "ok"
  4329|         if row_degraded:
  4330|             tier = "unclassified"
  4331|         else:
  4332|             pct_clients = pf(row.get("pct_clients_present"))
  4333|             n_clients_den = pf(row.get("n_clients_denominator"))
  4334|             n_projects = pf(row.get("n_projects_present"))
  4335|             n_files = pf(row.get("n_files_present"))
  4336|             multi_client = n_clients_den is not None and n_clients_den > 1
  4337|             if multi_client and pct_clients is not None and pct_clients >= UNION_BREADTH_CORPUS_WIDE_CLIENTS_PCT_MIN:
  4338|                 tier = "corpus_wide"
  4339|             elif multi_client and pct_clients is not None and pct_clients >= UNION_BREADTH_CLIENT_WIDE_CLIENTS_PCT_MIN:
  4340|                 tier = "client_wide"
  4341|             elif n_projects is not None and n_projects >= UNION_BREADTH_PROJECT_WIDE_MIN_PROJECTS:
  4342|                 tier = "project_wide"
  4343|             elif n_files is not None and n_files <= UNION_BREADTH_FILE_LEVEL_MAX_FILES:
  4344|                 tier = "file_level"
  4345|             else:
  4346|                 tier = "unclassified"
  4347|         # PR review finding: compare_cross_segment.py's own pct_clients_present/
  4348|         # n_clients_denominator computation (build_union_inventory_rows(),
  4349|         # ~line 1413-1437) groups by (view_scope, governance_role,
  4350|         # discipline_label, unit_system, domain), NOT corpus-wide across the
  4351|         # whole domain -- the earlier docstring claim that these fields are
  4352|         # "corpus-wide there (identical across every such row)" only holds
  4353|         # WITHIN one discipline/unit_system grain, not across them. Keying
  4354|         # this classifier's pattern_tier by (domain, join_hash) alone merged
  4355|         # rows from incompatible denominator scopes, so a pattern present in
  4356|         # every client of one small discipline could be misreported as
  4357|         # corpus-wide for the entire domain. Include discipline_label/
  4358|         # unit_system in the key so only same-scope rows are ever compared.
  4359|         pattern_key = (domain, row.get("discipline_label", ""), row.get("unit_system", ""), join_hash)
  4360|         if row_degraded:
  4361|             pattern_degraded.add(pattern_key)
  4362|         existing = pattern_tier.get(pattern_key)
  4363|         if existing is None or tier_rank[tier] < tier_rank[existing]:
  4364|             pattern_tier[pattern_key] = tier
  4365| 
  4366|     for pattern_key in pattern_degraded:
  4367|         pattern_tier[pattern_key] = "unclassified"
  4368| 
  4369|     # PR review finding: by_domain sums tier counts across every discipline/
  4370|     # unit_system scope for a domain, so a single narrow scope (e.g. one
  4371|     # small discipline where 2/2 clients happen to carry a pattern) can make
  4372|     # broad == 1 at the domain level even though no domain-wide breadth
  4373|     # evidence exists -- a "Broad natural reuse" note built only from these
  4374|     # counts would overclaim domain-wide reuse from one scoped grain.
  4375|     # broad_scopes records which (discipline_label, unit_system) scopes
  4376|     # actually contributed a corpus_wide/client_wide classification, so
  4377|     # detect_anomalies() can name the qualifying scope(s) instead of
  4378|     # implying the whole domain.
  4379|     by_domain: dict = defaultdict(
  4380|         lambda: {t: 0 for t in _UNION_BREADTH_TIERS} | {"total": 0, "broad_scopes": set()}
  4381|     )
  4382|     for (domain, discipline, unit_system, _join_hash), tier in pattern_tier.items():
  4383|         by_domain[domain][tier] += 1
  4384|         by_domain[domain]["total"] += 1
  4385|         if tier in ("corpus_wide", "client_wide"):
  4386|             by_domain[domain]["broad_scopes"].add((discipline, unit_system))
  4387|     for domain, counts in by_domain.items():
  4388|         counts["broad_scopes"] = sorted(counts["broad_scopes"])
  4389|     return dict(by_domain)
  4390| 
  4391| 
```
