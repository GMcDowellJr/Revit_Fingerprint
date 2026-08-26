# Chunk of tools/generate_governance_narrative.py

- Source relative path: `tools/generate_governance_narrative.py`
- Chunk: 12 of 17
- Original line range: 4901-5371
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: render_bc_composition_section, render_client_bc_distribution_section, _classify_domains_for_findings, _passive_inheritance_risk_domains, _low_coherence_clients, build_structured_findings, build_structured_findings.next_id, build_structured_findings.domain_support, build_structured_findings.add_domain_finding
- Source SHA-256: 7a8e1def8713100e21b852dc24c2c714a8e5330815affb3e703ed572a2829d9c
- Starts inside symbol: no
- Ends inside symbol: no

```
  4901| def render_bc_composition_section(bc_client_rows: list) -> Optional[str]:
  4902|     """Deliverable 4 -- client composition of each business center's project
  4903|     population. Population-composition facts only (project/file counts) --
  4904|     no compliance, ownership, or quality judgment."""
  4905|     if not bc_client_rows:
  4906|         return None
  4907|     lines = [
  4908|         "## Business Center Composition\n",
  4909|         "Client composition of each business center's project population, from "
  4910|         "`governance_bc_client_matrix.csv`. Project/file counts only -- this is not a "
  4911|         "governance, compliance, ownership, or quality read. See "
  4912|         "`governance_relationships.csv` for the underlying per-project rows.\n",
  4913|         "> **Not the same thing as the Project Portfolio section's peer-pool containment "
  4914|         "paragraph above.** That paragraph measures behavioral similarity between "
  4915|         "(client, discipline, unit_system) governance populations, which can each pool "
  4916|         "several physical projects; this section counts physical projects and files by "
  4917|         "client within a business center. The two do not share a grain and are not "
  4918|         "directly comparable row-for-row -- see the module-level comment above "
  4919|         "render_bc_composition_section() in this file.\n",
  4920|     ]
  4921|     by_bc: dict = defaultdict(list)
  4922|     for r in bc_client_rows:
  4923|         by_bc[r["business_center_label"]].append(r)
  4924|     for bc in sorted(by_bc.keys()):
  4925|         rows = sorted(
  4926|             by_bc[bc],
  4927|             key=lambda r: (-(pf(r["percentage_of_bc"]) or 0.0), r["client_label"]),
  4928|         )
  4929|         n_projects = sum(int(r["project_count"]) for r in rows)
  4930|         n_files = sum(int(r["project_file_count"]) for r in rows)
  4931|         n_clients = len(rows)
  4932|         dominant = rows[0]
  4933|         lines.append(f"### {bc}\n")
  4934|         lines.append(
  4935|             f"**{n_projects} project{'s' if n_projects != 1 else ''} across {n_files} "
  4936|             f"file{'s' if n_files != 1 else ''}, {n_clients} client{'s' if n_clients != 1 else ''}.** "
  4937|             f"Largest client by file count: {dominant['client_label']} "
  4938|             f"({pct(pf(dominant['percentage_of_bc']))} of this BC's files).\n"
  4939|         )
  4940|         for r in rows:
  4941|             lines.append(
  4942|                 f"- {r['client_label']}: {r['project_count']} project(s), "
  4943|                 f"{r['project_file_count']} file(s) ({pct(pf(r['percentage_of_bc']))} of BC)"
  4944|             )
  4945|         lines.append("")
  4946|     return "\n".join(lines)
  4947| 
  4948| 
  4949| def render_client_bc_distribution_section(client_bc_rows: list, bc_client_rows: list) -> Optional[str]:
  4950|     """Deliverable 5 -- business-center distribution of each client's project
  4951|     population (mirror of render_bc_composition_section() from the client
  4952|     vantage point). Reads governance_client_bc_matrix.csv (rollup) and
  4953|     governance_bc_client_matrix.csv (per-BC percentage_of_client breakdown)
  4954|     verbatim; computes nothing new."""
  4955|     if not client_bc_rows:
  4956|         return None
  4957|     lines = [
  4958|         "## Business Center Distribution\n",
  4959|         "Business-center distribution of each client's project population, from "
  4960|         "`governance_client_bc_matrix.csv`. Project/file counts only -- this is not a "
  4961|         "governance, compliance, ownership, or quality read.\n",
  4962|         "> **Not the same thing as the Project Portfolio section's peer-pool containment "
  4963|         "paragraph above** -- see the caveat under Business Center Composition; the same "
  4964|         "grain mismatch applies here (physical-project composition vs. governance-"
  4965|         "population behavioral similarity).\n",
  4966|     ]
  4967|     by_client: dict = defaultdict(list)
  4968|     for r in bc_client_rows:
  4969|         by_client[r["client_label"]].append(r)
  4970|     for client_row in client_bc_rows:
  4971|         client = client_row["client_label"]
  4972|         rows = sorted(
  4973|             by_client.get(client, []),
  4974|             key=lambda r: (-(pf(r["percentage_of_client"]) or 0.0), r["business_center_label"]),
  4975|         )
  4976|         lines.append(f"### {client}\n")
  4977|         lines.append(
  4978|             f"**{client_row['project_count']} project(s) across {client_row['project_file_count']} "
  4979|             f"file(s), {client_row['business_center_count']} business center"
  4980|             f"{'s' if client_row['business_center_count'] != '1' else ''}.**\n"
  4981|         )
  4982|         if rows:
  4983|             for r in rows:
  4984|                 lines.append(
  4985|                     f"- {r['business_center_label']}: {r['project_count']} project(s), "
  4986|                     f"{r['project_file_count']} file(s) ({pct(pf(r['percentage_of_client']))} of this client)"
  4987|                 )
  4988|         else:
  4989|             # --governance-bc-client-matrix not supplied (or has no rows for
  4990|             # this client) -- both matrix flags are independently optional,
  4991|             # so this is reachable even when governance_client_bc_matrix.csv
  4992|             # itself is present. Fall back to that file's own ordered
  4993|             # business_centers list rather than silently omitting the BC
  4994|             # breakdown entirely; it carries no per-BC project_count/
  4995|             # project_file_count/percentage_of_client of its own (only the
  4996|             # labels, already ordered by percentage_of_client descending),
  4997|             # so say so rather than presenting a truncated table as complete.
  4998|             bcs = [b for b in (client_row.get("business_centers") or "").split("|") if b]
  4999|             if bcs:
  5000|                 lines.append(
  5001|                     "_Per-BC project/file counts unavailable this run (--governance-bc-client-matrix "
  5002|                     "not supplied) -- business centers below, ordered by percentage_of_client descending:_"
  5003|                 )
  5004|                 for bc in bcs:
  5005|                     lines.append(f"- {bc}")
  5006|         lines.append("")
  5007|     return "\n".join(lines)
  5008| 
  5009| 
  5010| # Cross-client-convergence "strong" and client-coherence "low" thresholds are
  5011| # intentionally the same literal values (0.70 / 0.45) already used in
  5012| # detect_anomalies() (lines ~1438-1442) and the client-tier assignment inside
  5013| # build_client_summary() -- duplicated here as in those places rather than
  5014| # centralized, matching this generator's current state (policy/threshold
  5015| # externalization is deferred to a later PR; see docs/governance_evidence_package.md
  5016| # and Sig-Hash/Shape-Gating precedent for the externalization pattern
  5017| # this will eventually follow).
  5018| 
  5019| _RULE_STRONG_BASELINE = "GOV-TIER-STRONG-BASELINE"
  5020| _RULE_BASELINE_CANDIDATE = "GOV-TIER-BASELINE-CANDIDATE"
  5021| _RULE_LOCAL_REVIEW_REQUIRED = "GOV-TIER-LOCAL-REVIEW-REQUIRED"
  5022| _RULE_ACTIVE_LOCAL_PRACTICE = "GOV-TIER-ACTIVE-LOCAL-PRACTICE"
  5023| _RULE_HIGH_FRAGMENTATION = "GOV-TIER-HIGH-FRAGMENTATION"
  5024| _RULE_INSUFFICIENT_EVIDENCE = "GOV-TIER-INSUFFICIENT-EVIDENCE"
  5025| _RULE_XC_STRONG_CONVERGENCE = "GOV-XC-STRONG-CONVERGENCE"
  5026| _RULE_PASSIVE_INHERITANCE_RISK = "GOV-PASSIVE-INHERITANCE-RISK"
  5027| _RULE_CLIENT_LOW_COHERENCE = "GOV-CLIENT-LOW-COHERENCE"
  5028| _RULE_LEADERSHIP_QUESTION = "GOV-LEADERSHIP-QUESTION"
  5029| 
  5030| _FINDING_LIMITS_STANDARD = [
  5031|     "Evidence posture only -- does not approve standards, assign ownership, "
  5032|     "measure compliance, or label teams as compliant/non-compliant "
  5033|     "(governance_narrative_context.md's own stated scope boundary).",
  5034|     "Does not establish organizational intent.",
  5035| ]
  5036| 
  5037| # Every governance_domain_summary.csv column assign_tier() can read to decide
  5038| # among strong_baseline_candidate/baseline_candidate/local_review_required/
  5039| # missing_or_degraded_evidence/high_fragmentation. Consolidated into one list
  5040| # (rather than a hand-curated subset per finding type) after five separate PR
  5041| # review findings each flagged a different finding type missing one of these
  5042| # fields -- every tier-based finding needs the full set to let drill-through
  5043| # verify not just why its own tier's primary threshold matched, but why every
  5044| # *other* tier's exception/threshold did NOT fire. Some fields are irrelevant
  5045| # to a given instance (e.g. template_to_container never drives
  5046| # high_fragmentation), but including them is harmless and this list only ever
  5047| # needs to grow when assign_tier() itself grows, not per finding type.
  5048| _TIER_DRIVER_SUPPORT_FIELDS = [
  5049|     "governance_tier", "template_to_project", "container_to_project",
  5050|     # container_to_project_scoped/_pair: the only scalar evidence for the
  5051|     # data_sufficient scoped Container->Project fallback (see cp_scoped in
  5052|     # build_cascade()) when container_to_project itself is blank -- a finding
  5053|     # drill-through that only lists container_to_project would miss this
  5054|     # populated fallback entirely for exactly the domains it matters most for
  5055|     # (missing_or_degraded_evidence, where container_to_project is empty).
  5056|     "container_to_project_scoped", "container_to_project_scoped_pair",
  5057|     "template_to_container", "score_reliability", "local_active_share",
  5058|     "provided_passive_share", "provided_missing_share", "provided_to_used_containment",
  5059| ]
  5060| 
  5061| 
  5062| def _classify_domains_for_findings(cascade: dict, state_summary: Optional[dict] = None) -> dict:
  5063|     """Single source of truth for domain-tier-derived classification buckets,
  5064|     keyed by raw domain id (not DOMAIN_LABELS display text). Shared by
  5065|     build_structured_findings() and render_findings_and_recommendations() so
  5066|     the two never drift into independent implementations of the same rule --
  5067|     see docs/governance_evidence_package.md.
  5068| 
  5069|     Restricted to domains passing _has_renderable_cascade_signal() -- the
  5070|     same gate main() applies before writing a governance_domain_summary.csv
  5071|     row. A domain whose only signal is Group-3 scope-level data (
  5072|     enterprise_to_project/bc_to_project/enterprise_to_bc/enterprise_to_client)
  5073|     is captured in `cascade` but never gets a CSV row; a finding whose
  5074|     support[].selector points at that (nonexistent) row would be unresolvable.
  5075|     Findings and the CSV must agree on which domains exist.
  5076|     """
  5077|     state_summary = state_summary or {}
  5078|     renderable = {dom: d for dom, d in cascade.items() if _has_renderable_cascade_signal(d)}
  5079|     tiers = {dom: assign_tier(d, state_summary.get(dom)) for dom, d in renderable.items()}
  5080|     return {
  5081|         "strong_baseline_candidate": sorted(
  5082|             dom for dom, t in tiers.items() if t == TIER_STRONG_BASELINE
  5083|         ),
  5084|         "baseline_candidate": sorted(
  5085|             dom for dom, t in tiers.items()
  5086|             if t in (TIER_STRONG_BASELINE, TIER_BASELINE_LOCAL_REVIEW, TIER_BASELINE_CONTAINER_GAP)
  5087|         ),
  5088|         "local_review_required": sorted(
  5089|             dom for dom, t in tiers.items()
  5090|             if t in (TIER_BASELINE_LOCAL_REVIEW, TIER_INVESTIGATE, TIER_ACTIVE_LOCAL)
  5091|         ),
  5092|         "active_local_practice": sorted(
  5093|             dom for dom in renderable
  5094|             if tiers[dom] == TIER_ACTIVE_LOCAL
  5095|             or (_state_value(state_summary.get(dom), "local_active_share") or 0)
  5096|             >= LOCAL_ACTIVE_MATERIAL_THRESHOLD
  5097|         ),
  5098|         "high_fragmentation": sorted(dom for dom, t in tiers.items() if t == TIER_HIGH_FRAGMENTATION),
  5099|         "missing_or_degraded_evidence": sorted(
  5100|             dom for dom, t in tiers.items()
  5101|             if t in (TIER_INSUFFICIENT, TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE, TIER_SPARSE_LIMITED)
  5102|         ),
  5103|         "cross_client_convergence": sorted(
  5104|             dom for dom, d in renderable.items() if d["xc"] is not None and d["xc"] >= XC_STRONG_CONVERGENCE
  5105|         ),
  5106|     }
  5107| 
  5108| 
  5109| def _passive_inheritance_risk_domains(cascade: dict, state_summary: Optional[dict] = None) -> list:
  5110|     """Domains in PASSIVE_INHERITANCE_RISK_DOMAINS showing a material passive
  5111|     signal, using the same thresholds and dual/single-schema branching as
  5112|     detect_anomalies()'s bundle/passive-inheritance fallback block -- mirrored
  5113|     rather than shared because detect_anomalies() returns rendered prose
  5114|     strings, not a reusable boolean/value pair.
  5115| 
  5116|     When explicit governance-state data is available for a domain, this
  5117|     mirrors detect_anomalies()'s own `if state: ... if not state: <bundle
  5118|     fallback>` gating: the state's own provided_passive_share is authoritative
  5119|     and used instead of the bundle/passive_indicator heuristic, so a domain
  5120|     whose explicit state says passive share is clean can't still get flagged
  5121|     passive_inheritance_risk here purely from a bundle-density signal the
  5122|     narrative itself would not have used for that same domain.
  5123| 
  5124|     Also checks state_summary's provided_passive_share directly (mirroring
  5125|     detect_anomalies()'s state-based material-passive note, lines
  5126|     ~1302-1306), since that explicit state signal is available whenever
  5127|     --governance-state-summary is supplied, independent of whether the
  5128|     domain also has matching bundle/passive-indicator data in cascade.
  5129| 
  5130|     When a state row is present for the domain, the bundle fallback is never
  5131|     consulted, even if that state row's provided_passive_share isn't
  5132|     material -- this mirrors detect_anomalies()'s own `if not state:` gate
  5133|     (lines ~1323) around its bundle fallback block. A present-but-not-material
  5134|     state row is the domain's authoritative signal for this domain, exactly
  5135|     like the CSV/anomaly-note text; falling through to older bundle data
  5136|     would make governance_findings.json disagree with the rendered evidence.
  5137| 
  5138|     Restricted to domains passing _has_renderable_cascade_signal(), matching
  5139|     _classify_domains_for_findings() -- see that function's docstring.
  5140|     """
  5141|     state_summary = state_summary or {}
  5142|     flagged = []
  5143|     for dom, d in cascade.items():
  5144|         if dom not in PASSIVE_INHERITANCE_RISK_DOMAINS or not _has_renderable_cascade_signal(d):
  5145|             continue
  5146|         state = state_summary.get(dom)
  5147|         if state:
  5148|             passive_state = _state_value(state, "provided_passive_share")
  5149|             if passive_state is not None and passive_state >= PASSIVE_MATERIAL_THRESHOLD:
  5150|                 flagged.append(dom)
  5151|             continue
  5152|         bundle_schema = d.get("bundle_schema", "none")
  5153|         if bundle_schema == "dual":
  5154|             passive_ind = d.get("passive_indicator")
  5155|             if passive_ind is not None and passive_ind >= PASSIVE_MATERIAL_THRESHOLD:
  5156|                 flagged.append(dom)
  5157|         elif bundle_schema == "single":
  5158|             bundle_share = d.get("bundle_share_all")
  5159|             if bundle_share is not None and bundle_share < PASSIVE_INHERITANCE_RISK_BUNDLE_SHARE_MAX:
  5160|                 flagged.append(dom)
  5161|     return sorted(flagged)
  5162| 
  5163| 
  5164| def _low_coherence_clients(client_rows: list[dict]) -> list:
  5165|     return sorted(
  5166|         r["client"] for r in client_rows
  5167|         if r["wp_mean"] is not None and r["wp_mean"] < CLIENT_COHERENCE_LOW
  5168|     )
  5169| 
  5170| 
  5171| _LEADERSHIP_QUESTIONS = [
  5172|     ("Which baseline candidates should enter ratification review?",
  5173|      "Confirm intent, portability, active-use evidence, and whether local-active "
  5174|      "variants need separate handling before approval."),
  5175|     ("Where should governance use an approved-list or starter-content model "
  5176|      "instead of full convergence?",
  5177|      "This is especially relevant for families, materials, and domains with "
  5178|      "project-specific vocabulary."),
  5179|     ("Which active local practices deserve roll-up or documentation?",
  5180|      "Decide whether they are firmwide candidates, client/discipline playbook "
  5181|      "content, permitted variants, or project exceptions."),
  5182|     ("Which missing or passive inherited content is intentional?",
  5183|      "Distinguish deliberate pruning, unused starter stock, role-specific "
  5184|      "specialization, and propagation failure."),
  5185|     ("What additional segmentation is needed before stronger claims are made?",
  5186|      "Project type, business center, region, and larger segment samples remain "
  5187|      "future enhancements unless supplied upstream."),
  5188| ]
  5189| 
  5190| 
  5191| def build_structured_findings(
  5192|     cascade: dict,
  5193|     client_rows: list[dict],
  5194|     state_summary: Optional[dict] = None,
  5195| ) -> list[dict]:
  5196|     """Build the structured findings list backing governance_findings.json,
  5197|     reusing the exact same classification buckets render_findings_and_recommendations()
  5198|     renders as prose (see _classify_domains_for_findings()) so the two never
  5199|     diverge. finding_id assignment order is fixed (category, then sorted
  5200|     domain/client id) for run-to-run determinism.
  5201| 
  5202|     Only emits a finding when the underlying tier/metric already gates on
  5203|     sufficient evidence -- e.g. baseline_candidate/strong_baseline_candidate
  5204|     can never fire for a domain whose primary metric is None, because
  5205|     assign_tier() itself routes that domain to TIER_INSUFFICIENT instead.
  5206|     """
  5207|     domain_buckets = _classify_domains_for_findings(cascade, state_summary)
  5208|     passive_risk_domains = _passive_inheritance_risk_domains(cascade, state_summary)
  5209|     low_coherence_clients = _low_coherence_clients(client_rows)
  5210| 
  5211|     findings: list[dict] = []
  5212|     counter = [0]
  5213| 
  5214|     def next_id() -> str:
  5215|         counter[0] += 1
  5216|         return f"GF-{counter[0]:03d}"
  5217| 
  5218|     def domain_support(dom: str, fields: list) -> list:
  5219|         return [{
  5220|             "artifact_id": "governance_domain_summary",
  5221|             "selector": {"domain": dom},
  5222|             "fields": fields,
  5223|         }]
  5224| 
  5225|     def add_domain_finding(dom: str, finding_type: str, summary: str, rule_id: str, fields: list) -> None:
  5226|         findings.append({
  5227|             "finding_id": next_id(),
  5228|             "subject": {"type": "domain", "id": dom},
  5229|             "finding_type": finding_type,
  5230|             "status": FINDING_STATUS_SUPPORTED,
  5231|             "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
  5232|             "fidelity": FINDING_FIDELITY_EXACT,
  5233|             "authority_level": AUTHORITY_CONTROLLED_INTERPRETATION,
  5234|             "summary": summary,
  5235|             "support": domain_support(dom, fields),
  5236|             "rule_ids": [rule_id],
  5237|             "limits": list(_FINDING_LIMITS_STANDARD),
  5238|         })
  5239| 
  5240|     label = lambda dom: DOMAIN_LABELS.get(dom, dom)
  5241| 
  5242|     for dom in domain_buckets["strong_baseline_candidate"]:
  5243|         add_domain_finding(
  5244|             dom, "strong_baseline_candidate",
  5245|             f"{label(dom)} meets the strong-baseline-candidate rule (governance_tier: "
  5246|             f"{TIER_STRONG_BASELINE}).",
  5247|             _RULE_STRONG_BASELINE,
  5248|             list(_TIER_DRIVER_SUPPORT_FIELDS),
  5249|         )
  5250|     for dom in domain_buckets["baseline_candidate"]:
  5251|         tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
  5252|         add_domain_finding(
  5253|             dom, "baseline_candidate",
  5254|             f"{label(dom)} meets the baseline-candidate rule (governance_tier: {tier}).",
  5255|             _RULE_BASELINE_CANDIDATE,
  5256|             list(_TIER_DRIVER_SUPPORT_FIELDS),
  5257|         )
  5258|     for dom in domain_buckets["local_review_required"]:
  5259|         tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
  5260|         add_domain_finding(
  5261|             dom, "local_review_required",
  5262|             f"{label(dom)} requires local/use review before baseline language is "
  5263|             f"safe (governance_tier: {tier}).",
  5264|             _RULE_LOCAL_REVIEW_REQUIRED,
  5265|             list(_TIER_DRIVER_SUPPORT_FIELDS),
  5266|         )
  5267|     for dom in domain_buckets["active_local_practice"]:
  5268|         tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
  5269|         add_domain_finding(
  5270|             dom, "active_local_practice",
  5271|             f"{label(dom)} shows material active local practice (governance_tier: "
  5272|             f"{tier}).",
  5273|             _RULE_ACTIVE_LOCAL_PRACTICE,
  5274|             ["governance_tier", "local_active_share"],
  5275|         )
  5276|     for dom in domain_buckets["high_fragmentation"]:
  5277|         add_domain_finding(
  5278|             dom, "high_fragmentation",
  5279|             f"{label(dom)} is classified {TIER_HIGH_FRAGMENTATION} and is not a "
  5280|             "single-standard candidate in this run.",
  5281|             _RULE_HIGH_FRAGMENTATION,
  5282|             list(_TIER_DRIVER_SUPPORT_FIELDS),
  5283|         )
  5284|     for dom in domain_buckets["missing_or_degraded_evidence"]:
  5285|         tier = assign_tier(cascade[dom], (state_summary or {}).get(dom))
  5286|         add_domain_finding(
  5287|             dom, "missing_or_degraded_evidence",
  5288|             f"{label(dom)} has insufficient or degraded evidence for governance "
  5289|             f"classification (governance_tier: {tier}).",
  5290|             _RULE_INSUFFICIENT_EVIDENCE,
  5291|             # Note: TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE (one of the three
  5292|             # tiers in this bucket) is driven by _has_group1_bc_pooled_evidence()
  5293|             # finding bc-pooled tp_by_scope/cp_by_scope data -- not itself a
  5294|             # scalar governance_domain_summary.csv column, so it can't be listed
  5295|             # in _TIER_DRIVER_SUPPORT_FIELDS.
  5296|             list(_TIER_DRIVER_SUPPORT_FIELDS),
  5297|         )
  5298|     for dom in domain_buckets["cross_client_convergence"]:
  5299|         add_domain_finding(
  5300|             dom, "cross_client_convergence",
  5301|             f"{label(dom)} shows strong cross-client convergence "
  5302|             f"({pct(cascade[dom]['xc'])}) -- a natural common-base candidate.",
  5303|             _RULE_XC_STRONG_CONVERGENCE,
  5304|             ["cross_client_convergence"],
  5305|         )
  5306|     for dom in passive_risk_domains:
  5307|         d = cascade[dom]
  5308|         passive_state = _state_value((state_summary or {}).get(dom), "provided_passive_share")
  5309|         if passive_state is not None and passive_state >= PASSIVE_MATERIAL_THRESHOLD:
  5310|             detail = f"provided_passive_share={fmt(passive_state)}"
  5311|         elif d.get("bundle_schema") == "dual":
  5312|             detail = f"passive_inheritance_indicator={fmt(d.get('passive_indicator'))}"
  5313|         else:
  5314|             detail = f"bundle_share_all={fmt(d.get('bundle_share_all'))}"
  5315|         add_domain_finding(
  5316|             dom, "passive_inheritance_risk",
  5317|             f"{label(dom)} is in the passive-inheritance risk group and shows a "
  5318|             f"material passive signal ({detail}).",
  5319|             _RULE_PASSIVE_INHERITANCE_RISK,
  5320|             ["passive_inheritance_indicator", "bundle_share_all", "provided_passive_share",
  5321|              "passive_inheritance_risk"],
  5322|         )
  5323| 
  5324|     for client in low_coherence_clients:
  5325|         row = next(r for r in client_rows if r["client"] == client)
  5326|         findings.append({
  5327|             "finding_id": next_id(),
  5328|             "subject": {"type": "client", "id": client},
  5329|             "finding_type": "low_client_coherence",
  5330|             "status": FINDING_STATUS_SUPPORTED,
  5331|             "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
  5332|             "fidelity": FINDING_FIDELITY_EXACT,
  5333|             "authority_level": AUTHORITY_CONTROLLED_INTERPRETATION,
  5334|             "summary": (
  5335|                 f"{client} shows high within-client variation "
  5336|                 f"(within_project_coherence: {fmt(row['wp_mean'])})."
  5337|             ),
  5338|             "support": [{
  5339|                 "artifact_id": "governance_client_summary",
  5340|                 "selector": {"client": client},
  5341|                 "fields": ["within_project_coherence", "n_project_files"],
  5342|             }],
  5343|             "rule_ids": [_RULE_CLIENT_LOW_COHERENCE],
  5344|             "limits": list(_FINDING_LIMITS_STANDARD) + [
  5345|                 "Where file counts are small, treat as a signal for further "
  5346|                 "sampling rather than a definitive client judgement.",
  5347|             ],
  5348|         })
  5349| 
  5350|     for question, framing in _LEADERSHIP_QUESTIONS:
  5351|         findings.append({
  5352|             "finding_id": next_id(),
  5353|             "subject": {"type": "package", "id": "governance_evidence_package"},
  5354|             "finding_type": "leadership_question",
  5355|             "status": FINDING_STATUS_QUESTION_NOT_CLAIM,
  5356|             "origin": FINDING_ORIGIN_DETERMINISTIC_COMPUTATION,
  5357|             "fidelity": FINDING_FIDELITY_EXACT,
  5358|             "authority_level": AUTHORITY_CONVENIENCE_SUMMARY,
  5359|             "summary": question,
  5360|             "support": [],
  5361|             "rule_ids": [_RULE_LEADERSHIP_QUESTION],
  5362|             "limits": [
  5363|                 "This is a suggested question for human leadership review, not "
  5364|                 "an observed result or a claim.",
  5365|                 framing,
  5366|             ],
  5367|         })
  5368| 
  5369|     return findings
  5370| 
  5371| 
```
