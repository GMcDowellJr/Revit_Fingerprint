# Governance Question Routes

`question_routes_version: 0.1`

This is a **candidate question-route catalog** for a
`revit_fingerprint_governance` evidence package — it answers *where to
look*, not *how to extract* (that would be a script recipe) or *what the
answer is* (that's the artifact itself). It follows the discovery scaffold
in the design-reference `GMcDowellJr/llm_evidence_framework` repository
(`discovery/question_route_discovery.md`); this file is this package's own
content, not an import from that repository, and none of these routes are
promoted past **candidate** status yet — none has a proven history of
repeated use for this package type. Per that repository's own guidance
("a route should not be codified just because it was imagined"), each route
below was seeded from a question this package's own generator already
treats as recurring (the leadership questions in
`governance_narrative_context.md` and the ten `governance_findings.json`
finding types), not invented from nothing.

See `docs/governance/governance_interpretation_guide.md` for what the fields below
mean and how to read them, and `docs/governance_evidence_package.md` for
the full artifact inventory.

---

## Question route: Which domains are ready for standards/baseline review?

Status:
- candidate

Question forms:
- Which domains should enter ratification review?
- What are our strongest baseline candidates?
- Is domain X ready to become a standard?

Intent:
- Identify domains with strong enough propagation evidence to bring to a governance review.

Primary artifacts:
1. `governance_findings.json` — filter `finding_type` in (`strong_baseline_candidate`, `baseline_candidate`); `rule_ids` cite the exact rule.
2. `governance_domain_summary.csv` — the `governance_tier`, `template_to_project`, `container_to_project` columns behind the finding.

Secondary artifacts:
1. `governance_narrative_context.md`'s "Key Findings" section — same conclusions, in prose.

Relevant fields:
- `governance_domain_summary.csv`: `domain`, `governance_tier`, `score_reliability`, `template_to_project`, `container_to_project`, `local_active_share`, `provided_passive_share`, `provided_missing_share`, `provided_to_used_containment`

Suggested first check:
- `governance_findings.json`, `finding_type == "strong_baseline_candidate"` or `"baseline_candidate"`.

Evidence type:
- direct

Supported conclusion types:
- domain meets the current baseline-candidate rule
- domain's tier and the specific metrics that produced it

Unsupported conclusion types:
- standards approval
- organizational intent
- design/configuration quality

Comparability requirements:
- None beyond the domain existing in `governance_domain_summary.csv` (a domain with only Group-3 scope-level signal is excluded — see `governance_package_health.json`'s `domain_rows_excluded_no_signal`).

Common traps:
- Do not treat "Strong Baseline Candidate" as an approved standard — see `docs/governance/governance_interpretation_guide.md`'s "Known bad inferences."
- Do not ignore `score_reliability` — a `Presence-based`/`Sparse` domain needs more caution than the tier label alone implies.

Escalation:
- If the question is "why isn't this domain a strong candidate," inspect the `baseline_candidate`-but-not-`strong_baseline_candidate` finding's `support[].fields` for the specific gating metric (`local_active_share`, `provided_passive_share`, `provided_missing_share`, or `provided_to_used_containment`).

---

## Question route: Which domains need local/use review before baseline language is safe?

Status:
- candidate

Question forms:
- What needs attention before we call anything a standard?
- Which domains have a material local-active or passive-inheritance exception?

Intent:
- Surface domains whose high containment score alone would overstate readiness.

Primary artifacts:
1. `governance_findings.json` — `finding_type == "local_review_required"`.
2. `governance_domain_summary.csv` — `local_active_share`, `provided_passive_share`, `provided_missing_share`, `provided_to_used_containment` columns.

Relevant fields:
- `governance_domain_summary.csv`: `domain`, `governance_tier`, `local_active_share`, `provided_passive_share`, `provided_missing_share`, `provided_to_used_containment`

Suggested first check:
- `governance_findings.json`, `finding_type == "local_review_required"`.

Evidence type:
- direct

Supported conclusion types:
- domain has a material state exception limiting how safely a baseline claim can be made

Unsupported conclusion types:
- whether the exception is a problem (intentional pruning, specialization, and propagation failure all look the same in this field alone)

Comparability requirements:
- Requires `--governance-state-summary` (or `--governance-states`) to have been supplied for the domain; otherwise this reads as `missing_or_degraded_evidence` instead.

Common traps:
- Do not assume `local_active_share` material = bad. It may be legitimate client/discipline-specific practice worth documenting, not converging away.

Escalation:
- If governance-state data was not supplied, check `governance_package_health.json`'s `optional_inputs.cross_segment_governance_state_summary` before concluding the domain has no review need.

---

## Question route: Which domains show high fragmentation?

Status:
- candidate

Question forms:
- Which domains are not single-standard candidates?
- Where is there no common ground at all?

Intent:
- Identify domains where governance should start with a design question, not a convergence push.

Primary artifacts:
1. `governance_findings.json` — `finding_type == "high_fragmentation"`.
2. `governance_domain_summary.csv` — `governance_tier == "High Fragmentation"` rows.

Relevant fields:
- `governance_domain_summary.csv`: `domain`, `template_to_project`, `container_to_project`, `within_project_all`

Evidence type:
- direct

Supported conclusion types:
- domain shows low propagation/coherence in this run

Unsupported conclusion types:
- root cause of the fragmentation
- whether fragmentation is acceptable for that domain's nature (e.g. `loaded_family_types`, `materials` are expected to be more project-specific — see the interpretation guide)

Common traps:
- Do not apply the same convergence expectation to every domain — `docs/governance/governance_interpretation_guide.md` and `governance_narrative_context.md`'s findings section both note some domains (families, materials) are inherently project-specific.

Escalation:
- For per-discipline breakdown of a fragmented view-template domain, see `governance_narrative_context.md`'s discipline section (not yet a separate artifact).

---

## Question route: Which domains show passive-inheritance risk?

Status:
- candidate

Question forms:
- Is this domain actually being used, or just inherited and ignored?
- Where might we have starter content nobody configures?

Intent:
- Distinguish "present because inherited" from "actively configured."

Primary artifacts:
1. `governance_findings.json` — `finding_type == "passive_inheritance_risk"`; check `support[].fields` for whether the signal came from `provided_passive_share` (state-authoritative) or `passive_inheritance_indicator`/`bundle_share_all` (bundle fallback, lower confidence).
2. `governance_domain_summary.csv` — `passive_inheritance_indicator`, `bundle_share_all`, `passive_inheritance_risk` columns.

Relevant fields:
- `governance_domain_summary.csv`: `domain`, `passive_inheritance_indicator`, `passive_indicator_method`, `bundle_share_all`, `bundle_share_used`, `provided_passive_share`

Evidence type:
- mixed (direct when governance-state-backed; indirect when bundle-fallback-backed — check `passive_indicator_method`)

Supported conclusion types:
- material passive-inheritance signal observed for this domain

Unsupported conclusion types:
- whether the passive content is intentional starter stock, unmanaged propagation, or something else

Common traps:
- Do not treat the bundle-density fallback (used only when governance-state data is absent) as equally strong evidence to a state-backed finding.

Escalation:
- If `passive_indicator_method == "none"`, there is no passive-inheritance evidence at all for that domain in this run — do not infer risk from absence.

---

## Question route: Which clients need onboarding attention or show low internal coherence?

Status:
- candidate

Question forms:
- Which clients are hardest to onboard a new team member into?
- Where does a firmwide playbook not apply?

Intent:
- Translate client-level alignment/coherence numbers into a practical onboarding read.

Primary artifacts:
1. `governance_client_summary.csv` — `alignment_tier`, `within_project_coherence`, `onboarding_*` columns.
2. `governance_findings.json` — `finding_type == "low_client_coherence"`.

Relevant fields:
- `governance_client_summary.csv`: `client`, `n_project_files`, `alignment_tier`, `cross_client_similarity_mean`, `within_project_coherence`, `confidence_note`, `onboarding_internal_read`, `onboarding_portability_read`, `onboarding_operating_implication`

Suggested first check:
- `governance_client_summary.csv` sorted by `within_project_coherence` ascending.

Evidence type:
- mixed (`within_project_coherence`/`cross_client_similarity_mean` direct; `onboarding_*` controlled interpretation derived from them)

Supported conclusion types:
- client shows high/low internal variation or cross-client alignment in this run

Unsupported conclusion types:
- staff performance
- whether variation is a problem for that specific client's business needs

Comparability requirements:
- `confidence_note`/`n_project_files` — a client below `client_confidence_low_max_files` (see `policies/governance/governance_thresholds.json`) should be treated as a review prompt, not a settled profile.

Common traps:
- Do not treat low cross-client similarity as failure — see the interpretation guide's known-bad-inferences section.
- A client absent from `policies/client_sector.csv` is "unclassified," not "confirmed non-healthcare" — do not conflate the two when reading `alignment_tier`.

Escalation:
- None beyond the CSV/findings above for this package type today.

---

## Question route: Which domains show strong cross-client convergence?

Status:
- candidate

Question forms:
- What's naturally common across clients, even without a formal standard?
- Where is there a common-base candidate we haven't formalized?

Intent:
- Identify domains where independent clients have converged on similar configuration without being told to.

Primary artifacts:
1. `governance_findings.json` — `finding_type == "cross_client_convergence"`.
2. `governance_domain_summary.csv` — `cross_client_convergence` column.

Relevant fields:
- `governance_domain_summary.csv`: `domain`, `cross_client_convergence`

Evidence type:
- direct

Supported conclusion types:
- domain shows convergence among healthcare-sector clients in this run

Unsupported conclusion types:
- convergence was intentional or should be formalized as policy
- convergence extends to non-healthcare clients (not measured by this signal)

Comparability requirements:
- Only computed between clients classified `healthcare` in `policies/client_sector.csv` — see the interpretation guide's "Comparability" section.

Common traps:
- Do not read this as firmwide convergence if only healthcare clients were compared.

Escalation:
- None — this is a corpus-scope limitation of the current pipeline, not something a drill-down file resolves.

---

## Question route: Is this package usable at face value / can this run's numbers be trusted?

Status:
- candidate

Question forms:
- Can I trust this governance run?
- Is anything degraded or missing in this package?
- Did every input actually load?

Intent:
- Confirm package health before reasoning from any downstream number.

Primary artifacts:
1. `governance_package_health.json` — `overall_status`, `blocking_conditions`, `warnings`, `fallbacks_used`.
2. `governance_package_manifest.json` — `inputs[].present`, `policy_profiles.profiles`.

Relevant fields:
- `governance_package_health.json`: `overall_status`, `required_inputs`, `optional_inputs`, `used_view_fallback`, `comparison_type_coverage`, `client_sector_status`, `policy_load_status`, `blocking_conditions`, `warnings`

Suggested first check:
- `governance_package_health.json`'s `overall_status` — `"invalid"` means a required input is missing and the rest of the package should not be trusted; `"degraded"` means a specific, named condition limits interpretation (see `warnings`); `"complete"` means neither.

Evidence type:
- direct

Supported conclusion types:
- whether the package meets minimum data requirements for this run
- which specific optional signals are degraded or defaulted, and why

Unsupported conclusion types:
- whether the underlying corpus itself is "healthy" (this is a package-quality check, not a corpus-quality judgment)

Common traps:
- Do not skip this check and go straight to the narrative or CSVs — a `degraded` or `invalid` package can still produce plausible-looking numbers.

Escalation:
- If `overall_status` is `"degraded"` due to `unrecognized_comparison_type`, that is a real signal that `compare_cross_segment.py`'s vocabulary has drifted from this generator's — treat as a data-pipeline issue, not a corpus finding.

---

## Question route: Where is the underlying evidence for a specific narrative claim or finding?

Status:
- candidate

Question forms:
- Where does this number come from?
- I read X in the narrative — what CSV row backs it?
- Does the narrative agree with the CSV?

Intent:
- Trace a prose claim or a JSON finding back to its authoritative source, per this package's authority ordering.

Primary artifacts:
1. `governance_findings.json` — every finding's `support[]` names an `artifact_id` + `selector` + `fields`.
2. `governance_evidence_map.json` — resolves an `artifact_id` to a path, grain, and `can_answer`/`cannot_answer`.

Relevant fields:
- `governance_findings.json`: `support[].artifact_id`, `support[].selector`, `support[].fields`

Suggested first check:
- If starting from `governance_narrative_context.md` prose, first check whether the same conclusion appears in `governance_findings.json` (it likely does — the narrative renders from the same findings list) before treating the prose as its own source.

Evidence type:
- direct

Supported conclusion types:
- exact CSV row/field backing a specific finding or narrative sentence

Unsupported conclusion types:
- none (this route is purely navigational)

Common traps:
- Do not treat `governance_brief.md` or `governance_narrative_context.md` prose as authoritative if it appears to disagree with a CSV or `governance_findings.json` — the CSV/findings win (see the interpretation guide's authority ordering).

Escalation:
- If a `support[].artifact_id` does not exist in `governance_evidence_map.json`, that is a package-generation bug, not a governance signal — report it.

---

## Question route: Which project pairs share the most or least configuration footprint?
 
Status:
- candidate

Question forms:
- Which projects are most/least similar to each other?
- Does this project's configuration resemble its peers, or is it an outlier?
- Are two projects "the same shape" even if their content differs?

Intent:
- Identify project-to-project footprint overlap and density-population
  similarity at portfolio grain, separate from domain-level baseline
  questions.
  
Primary artifacts:
1. `governance_narrative_context.md`'s "Project Portfolio" section --
   footprint identity and density similarity paragraphs, with top/bottom-N
   pairs already computed.
2. `project_union_jaccard_matrix.csv` / `project_density_similarity_matrix.csv`
   directly, for pairs not in the top/bottom-N shown.
   
Secondary artifacts:
1. `governance_package_health.json`'s `matrix_manifest` -- confirms which
   matrices were actually supplied for this run before trusting an absence
   as a real zero result.
   
Relevant fields:
- `union_jaccard` (footprint overlap), `density_similarity` (population-degree
  similarity), both ALL_DOMAINS/all-view.
  
Suggested first check:
- The Project Portfolio section's "Most/least similar footprint" and
  "Most/least similar density" lists.
  
Evidence type:
- direct

Supported conclusion types:
- degree of footprint/population overlap between two named projects
- whether high density similarity coexists with low footprint overlap
  ("same shape, different content")
  
Unsupported conclusion types:
- governance compliance or noncompliance of either project
- domain-level baseline/standards readiness (this section is kept outside
  `assign_tier()`/`governance_domain_summary.csv` by design)
  
Comparability requirements:
- None beyond both projects appearing in the relevant matrix; this grain is
  project x project, not gated by the domain-level comparability rules in
  `docs/governance/governance_interpretation_guide.md`.
  
Common traps:
- Do not read a low `union_jaccard` pair as evidence of poor practice --
  see `docs/governance/governance_interpretation_guide.md`'s "Known bad inferences."
- Do not treat "no ALL_DOMAINS rows available" for a matrix as a measured
  zero -- it means the underlying matrix had no row at that grain for this
  corpus.
  
Escalation:
- For per-domain (not ALL_DOMAINS) footprint/density detail on a specific
  pair, drill into the raw matrix CSVs directly -- the narrative section
  only surfaces the ALL_DOMAINS, all-view top/bottom-N.
  
---
 
## Question route: Is a domain's reuse genuinely broad across clients, or does it just clear a low bar?
 
Status:
- candidate

Question forms:
- How many clients actually use this domain's common patterns?
- Is this domain's reuse deep, or just technically present everywhere?
- Should we trust a domain's adoption-breadth number as evidence of
  convergence?
  
Intent:
- Distinguish genuine broad client adoption of a domain's shared vocabulary
  from a low bar (one corpus-wide pattern) being cleared by every client.
  
Primary artifacts:
1. `governance_narrative_context.md`'s "Adoption breadth by domain (client
   reach)" table (Union Inventory Reuse Summary section).
2. `pattern_reuse_summary_by_client.csv` directly, for domains beyond the
   top-20 shown.
   
Secondary artifacts:
1. `governance_narrative_context.md`'s "Reuse breadth summary" table
   (distinct-pattern reuse, `corpus_wide`/`client_wide`/etc. buckets) --
   cross-check against this before treating adoption breadth alone as
   evidence of depth.
   
Relevant fields:
- `clients with corpus-wide patterns`, `clients seen`, `corpus-wide
  pattern-instances` (adoption-breadth table); `n_patterns` by
  `reuse_bucket`/`bucket_basis` (distinct-pattern table).
  
Suggested first check:
- Compare `clients with corpus-wide patterns` / `clients seen` against
  `corpus-wide pattern-instances` for the domain -- a high ratio with a low
  instance count is a saturated-bar signal, not deep adoption.

Evidence type:
- direct, but easily overread without the cross-check above
Supported conclusion types:
- count of clients reaching at least one corpus-wide-reused pattern for a
  domain
- whether that count is universal (N/N) across the corpus's clients

Unsupported conclusion types:
- depth or intentionality of adoption
- governance convergence or standards readiness on its own (this is an
  additive breadth cut, not a replacement for the distinct-pattern table or
  the domain's `governance_tier`)
  
Comparability requirements:
- None beyond the domain appearing in `pattern_reuse_summary_by_client.csv`.

Common traps:
- Do not report "N/N clients" as strong convergence evidence without
  checking `corpus-wide pattern-instances` -- see
  `docs/governance/governance_interpretation_guide.md`'s "Known bad inferences": in at
  least one production run every domain shown reached 7/7 clients, which
  does not by itself distinguish deep adoption from a saturated low bar.
  
Escalation:
- If breadth looks uniformly high across many domains, check whether the
  underlying `corpus_wide` bucket threshold itself needs revisiting before
  drawing any cross-domain comparison from this table.

---

## Question route: Which clients exist inside a business center?

Status:
- candidate

Question forms:
- Which clients does business center X do work for?
- How is business center X's project population split by client?
- Who is business center X's largest client by file count?

Intent:
- Surface the client composition of one business center's project population as a factual rollup, separate from that business center's Template/Container peer-alignment scores.

Primary artifacts:
1. `governance_narrative_context.md`'s "Business Center Composition" section -- per-BC client breakdown, already computed.
2. `governance_bc_client_matrix.csv` directly, for the full precision (`percentage_of_bc` is rounded for display in the narrative).

Secondary artifacts:
1. `governance_relationships.csv` -- the underlying one-row-per-project rows this matrix is aggregated from.

Relevant fields:
- `governance_bc_client_matrix.csv`: `business_center_label`, `client_label`, `project_count`, `project_file_count`, `percentage_of_bc`

Suggested first check:
- `governance_bc_client_matrix.csv` filtered to the business center of interest, sorted by `percentage_of_bc` descending.

Evidence type:
- direct

Supported conclusion types:
- how many projects/files a given client contributes to a business center's population, in this run
- which client is the largest contributor to a business center by file count

Unsupported conclusion types:
- governance compliance, ownership, or quality of any client's work in that business center
- behavioral similarity between that business center's projects (see the Project Portfolio section's peer-pool containment paragraph for that, and its own comparability caveat against this matrix)

Comparability requirements:
- None beyond the business center appearing in `governance_bc_client_matrix.csv`.

Common traps:
- Do not read `percentage_of_bc` against the Project Portfolio section's peer-pool containment scores as if they measured the same thing for the same entities -- `governance_bc_client_matrix.csv`'s grain is one physical project (`file_metadata.csv`'s `project_label`); the Portfolio section's "project" grain is a (client, discipline, unit_system) governance population that can itself pool several physical projects. See the caveat rendered directly under the narrative's "Business Center Composition" heading.

Escalation:
- None beyond the CSV/narrative section above for this package type today.

---

## Question route: Which business centers deliver work for a client?

Status:
- candidate

Question forms:
- Which business centers does client X's work span?
- Is client X's work concentrated in one business center or spread across several?

Intent:
- Surface the business-center distribution of one client's project population as a factual rollup, from the client vantage point (mirror of the business-center-vantage route above).

Primary artifacts:
1. `governance_narrative_context.md`'s "Business Center Distribution" section -- per-client business-center breakdown, already computed.
2. `governance_client_bc_matrix.csv` directly, for the rollup (`business_center_count`, ordered `business_centers` list).

Secondary artifacts:
1. `governance_bc_client_matrix.csv` -- the per-BC `percentage_of_client` rows this rollup is aggregated from (no independent computation happens at the client-matrix level).

Relevant fields:
- `governance_client_bc_matrix.csv`: `client_label`, `business_center_count`, `business_centers`, `project_count`, `project_file_count`
- `governance_bc_client_matrix.csv`: `percentage_of_client` (per business center, for this client)

Suggested first check:
- `governance_client_bc_matrix.csv` filtered to the client of interest; `business_centers` is already ordered by `percentage_of_client` descending.

Evidence type:
- direct

Supported conclusion types:
- how many business centers a client's projects span, in this run
- how a client's project/file count is divided across those business centers

Unsupported conclusion types:
- governance compliance, ownership, or quality implications of a client spanning multiple business centers
- behavioral similarity between that client's projects across business centers (see the Project Portfolio section's peer-pool containment paragraph for that, and its own comparability caveat against this matrix)

Comparability requirements:
- None beyond the client appearing in `governance_client_bc_matrix.csv`. In the corpus this route catalog was seeded from, no client's projects actually spanned more than one business center (`client_cross_bc` = 0 rows in `cross_segment_summary.csv`) -- a single-BC client here is a real, verified-common case, not a data gap.

Common traps:
- Same grain-mismatch trap as the business-center-vantage route above -- do not cross-read this matrix's percentages against Project Portfolio behavioral-similarity numbers as if they described the same entities.

Escalation:
- None beyond the CSV/narrative section above for this package type today.

---

## Question route: Which projects make up a business center?

Status:
- candidate

Question forms:
- What is the full project roster for business center X?
- How many files does project Y have?
- Is project label Z the same project everywhere it appears, or does it collide across clients?

Intent:
- Resolve one physical project's identity (client, business center, disciplines, file count) rather than a governance-population rollup.

Primary artifacts:
1. `governance_relationships.csv` -- one row per physical project, keyed by (`client_label`, `business_center_label`, `project_name`).

Secondary artifacts:
1. `governance_narrative_context.md`'s "Business Center Composition" / "Business Center Distribution" sections, for the rolled-up counts this file feeds.

Relevant fields:
- `governance_relationships.csv`: `project_id`, `project_name`, `project_name_is_fallback`, `client_label`, `business_center_label`, `discipline_labels`, `unit_system`, `project_file_count`, `export_run_ids`

Suggested first check:
- `governance_relationships.csv` filtered to the business center or client of interest.

Evidence type:
- direct

Supported conclusion types:
- which physical projects exist for a given client/business center, and how many files each carries
- whether a `project_name` string is genuinely one project or a same-named collision across different clients (identity is `(client_label, business_center_label, project_name)`, not `project_name` alone -- two different clients can legitimately share a project-name string)

Unsupported conclusion types:
- governance compliance, ownership, or quality of the project
- behavioral similarity between projects (not this file's grain; see the Project Portfolio section)

Comparability requirements:
- `project_name_is_fallback == "true"` means `file_metadata.csv` had no real `project_label` for that file, so `project_name` is a synthetic per-file identifier (that file's own `export_run_id`), not a human-assigned project name -- treat it as "one unlabeled file," not as evidence the project itself is single-file.

Common traps:
- Do not assume every row with the same `project_name` string is the same project -- check `client_label`/`business_center_label` too; see Intent above.

Escalation:
- None beyond this CSV for this package type today.

---

## Route categories represented above

```text
governance alignment       — baseline/standards readiness, local review, fragmentation
data quality / missing evidence — package health, comparability
activity/churn interpretation   — passive inheritance
user/project attribution        — client onboarding/coherence
```

## Not yet covered (candidates for future capture, not yet routes)

- Before/after comparison across two runs (no accepted Phase-2 baseline yet
  per "Current operating mode" — out of scope until authority
  is established).
- Business-center-level **project-composition** rollups are now covered —
  see the three routes above (`governance_bc_client_matrix.csv`,
  `governance_client_bc_matrix.csv`, `governance_relationships.csv`).
  Business-center-level **behavioral** rollups (Group 3 cascade fan-out —
  `enterprise_to_bc`/`bc_to_project`/`enterprise_to_client`/`bc_to_bc`) are a
  separate, still-open gap: captured in `cascade` but not yet rendered/
  tiered as their own section — see `CASCADE_GROUP3_TYPES`/
  `CASCADE_GROUP3B_TYPES` in `tools/generate_governance_narrative.py`. Do
  not conflate the two — composition (project/file counts) and behavioral
  similarity (containment/Jaccard) answer different questions, per the
  grain-mismatch caveat rendered in the Business Center Composition/
  Distribution sections. If the pair-mean cascade score for a `bc_to_bc`/
  `enterprise_to_bc` comparison needs file-level audit (e.g. "is this
  cross-BC/enterprise pattern consistency real or an artifact of a small
  file sample?"), that is exactly `docs/governance/governance_interpretation_guide.md`'s
  escalation path into `cross_segment_file_pairs.csv` filtered by
  `comparison_type=bc_to_bc`/`enterprise_to_bc` and `domain`/`segment_id_a`/
  `segment_id_b` — see that file's `governance_evidence_map.json` entry
  (D-024) for its real column header and row count before writing the
  filtered extraction script.
- A dedicated per-discipline evidence artifact (today only available inside
  `governance_narrative_context.md`'s discipline section, not as its own
  file).
