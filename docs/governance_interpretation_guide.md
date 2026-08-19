# Governance Interpretation Guide

`interpretation_guide_version: 0.1`

This document is the **package-specific interpretation layer** for a
`revit_fingerprint_governance` evidence package (the outputs of
`tools/generate_governance_narrative.py`). It explains what the package's
metrics and classifications mean, what they can and cannot support, and how
to read missing values and authority — the questions a reader (human or LLM)
needs answered *before* reasoning from the raw artifacts, not after.

It is modeled on the "interpretation layer" concept in the design-reference
`GMcDowellJr/llm_evidence_framework` repository (`notes/current_thesis.md`,
`patterns/deterministic_to_llm_boundary.md`) — that repository is explicitly
provisional, and this guide is this package's own content, not an import
from it. See `docs/governance_evidence_package.md` for the full artifact
inventory this guide accompanies, and `docs/governance_question_routes.md`
for where to look for specific recurring questions.

## What this package is for

This package answers questions about **what configuration content actually
propagates and converges** across a Revit fingerprint corpus — from
enterprise/generic baselines, through templates and coordination
("container") files, into projects — and where client-, discipline-, or
project-level practice diverges from that baseline. It supports **evidence
discovery and classification for governance review**.

This package is written for a reader who does not need Revit domain
knowledge. It is written for someone who is expected to ask governance
convergence/fragmentation questions, not resolve them unassisted — "what to
do about it" is explicitly out of this package's scope.

## What this package is *not* for

- It does not approve standards, assign domain ownership, or certify a team
  as compliant/non-compliant.
- It does not establish organizational intent. A high containment score
  shows configuration is shared; it does not show the sharing was
  deliberate policy.
- It does not measure design quality, correctness of the underlying
  configuration, or user skill.
- It is not a live/real-time system — every number reflects the corpus as
  of the `comparison_run_id`(s) recorded in `governance_package_manifest.json`.

## How the evidence was produced

`compare_cross_segment.py` computes pairwise Jaccard/containment metrics
between corpus segments (populations defined by `unit_system` /
`governance_role` / `client_label` / `discipline_label` / …, see
`tools/build_segment_manifest.py`) using `join_hash` as the identity unit —
a normalized configuration fingerprint, independent of Revit element IDs.
`tools/generate_governance_narrative.py` reads those comparison outputs and
applies deterministic classification rules (tier assignment, reliability
banding, findings) on top. **No LLM is involved in producing any artifact in
this package** — every classification is a fixed rule over CSV fields, and
the rule (and its threshold values) are documented and versioned (see
"Policy profiles," below).

## Metric semantics

### Cascade fields (`governance_domain_summary.csv`)

| Field | Meaning |
|---|---|
| `template_to_container` (`tc`) | Mean containment of a Template's configuration inside paired Container (coordination-file) segments. |
| `container_to_project` (`cp`) | Mean containment of a Container's configuration inside paired Project segments. |
| `template_to_project` (`tp`) | Mean containment of a Template's configuration directly inside paired Project segments (bypasses coordination files). |
| `cascade_generic_to_template/_container/_project` (`gt`/`gc`/`gp`) | Mean containment of the enterprise/out-of-box Generic baseline inside Template/Container/Project — one level *above* `tc`/`cp`/`tp` in the cascade. |
| `cross_client_convergence` (`xc`) | Mean Jaccard similarity between different clients' Project segments (healthcare-sector only, per `policies/client_sector.csv` — see "Comparability," below). High values indicate practice convergence *independent of* any formal template. |
| `within_project_all`, `_p10`, `_p90` | Mean/10th-percentile/90th-percentile Jaccard between pairs of Project files within the same population — the spread signal `score_reliability` is derived from. |

`tp`, `cp`, `gt`/`gc`/`gp` are containment measures (asymmetric: "how much
of A is inside B"), not Jaccard (symmetric similarity) — do not treat them
interchangeably. `xc` and `within_project_*` are Jaccard.

**Containment is evidence of reuse or propagation, not proof of governance
approval or active use.** A high containment score shows that one
vocabulary is present inside another; it does not show the sharing was
approved, intended, or actually exercised in delivery — see `governance_tier`
below and "Known bad inferences." All cascade scores are on a 0–1 scale;
in this package, a higher score indicates stronger propagation/convergence
evidence, not automatic ratification of a standard.

**Used-view interpretation is meaningful primarily for Project targets.**
Template, Generic, and most Container roles are provided-vocabulary
references, not production-use environments — a Template's own "used view"
does not mean anything was used in project delivery. See "Comparability,"
below, for the all-view/used-view distinction itself.

### `governance_tier`

A **classification of evidence readiness**, computed by `assign_tier()`
from the cascade fields above plus explicit governance-state signals when
available (`local_active_share`, `provided_passive_share`,
`provided_missing_share`, `provided_to_used_containment`). Ordered from
strongest to weakest evidence: Strong Baseline Candidate → Baseline
Candidate — Local/Use Review → Baseline Candidate — Container Gap →
Investigate Before Baseline → Active Local Practice Review → Moderate
Variation → Sparse / Presence-Limited → High Fragmentation → Insufficient
Evidence — Enterprise; BC-Level Evidence Available → Insufficient Evidence.

**A tier is a readiness signal, not an approval.** "Strong Baseline
Candidate" means the evidence is strong enough to bring to a governance
review, not that the domain has been ratified as a standard.

### `score_reliability`

A **separate axis from `governance_tier`**: how trustworthy the *mean*
cascade value is, derived from the within-project p10/p90 spread. `Tight`
and `Convergent` mean the mean reflects genuine agreement across file
pairs. `Presence-based` means the domain follows a binary
present-or-absent pattern — the mean reflects *how many* files carry the
domain, not how well the files that have it agree. `Sparse` means the
domain is rarely present at all, and the mean understates fragmentation.
**A high `governance_tier` with `Presence-based` or `Sparse` reliability
should be read more cautiously than the tier label alone suggests** —
always check `score_reliability` alongside the tier.

### Governance-state fields (`--governance-state-summary`, when supplied)

| Field | Meaning |
|---|---|
| `provided_to_used_containment` | Of the reference (Template/enterprise) vocabulary, how much is actually *used* in projects — not just present. |
| `provided_passive_share` | Share of reference vocabulary that is inherited but sits unused ("passive inheritance"). |
| `provided_missing_share` | Share of reference vocabulary missing downstream — could be intentional pruning, specialization, or a propagation failure; the field alone cannot distinguish these. |
| `local_active_share` | Share of a project's content that is locally created, not inherited, and actively used — a signal of active local practice that may deserve roll-up review, or may be legitimate project-specific work. |

These fields are **authoritative when present** — `assign_tier()` and
`detect_anomalies()` prefer them over the bundle/passive-indicator fallback
signals below whenever a domain has explicit governance-state data (see
`policies/governance/governance_thresholds.json`'s
`passive_material_threshold`/`missing_material_threshold`/
`local_active_material_threshold`).

### Bundle / passive-inheritance fallback (used only when governance-state data is absent)

`bundle_share_all`/`bundle_share_used`/`passive_inheritance_indicator` are
a *weaker, indirect* proxy for the same passive-inheritance question, used
only when `--governance-state-summary`/`--governance-states` were not
supplied for a domain. Treat a `passive_inheritance_risk` finding backed by
this fallback as lower-confidence than one backed by
`provided_passive_share` — check `governance_findings.json`'s
`support[].fields` on the specific finding to see which signal was used.

### Client fields (`governance_client_summary.csv`)

`alignment_tier` is derived from `cross_client_similarity_mean`
(`xc`, healthcare-sector clients only). `onboarding_*` fields are
**controlled interpretation**, not raw observations — they translate the
numeric alignment/coherence signals into practical onboarding language, but
carry no more evidence than the `xc`/`within_project_coherence` fields they
are derived from.

### Project Portfolio fields (Project Portfolio section, `governance_narrative_context.md`)
 
Kept **outside** `assign_tier()`/`governance_domain_summary.csv` by design —
these are project x project portfolio-shape diagnostics, not domain-standard
approval signals, and never override a domain's `governance_tier`. Verified:
adding these matrices to a run left `governance_findings.json`'s 71 findings
and their category counts unchanged.
 
| Field | Source | Meaning |
|---|---|---|
| `union_jaccard` | `project_union_jaccard_matrix.csv` | System-level footprint overlap between two project scopes -- do these projects contain/use the same canonical patterns. ALL_DOMAINS, all-view. |
| `density_similarity` | `project_density_similarity_matrix.csv` | Whether two projects populate domains to a similar *degree*, independent of whether the populated patterns are the same ones. |
| `pool_containment_similarity` | `project_pool_containment_similarity_matrix.csv` | How much a project's system aligns with its own peer pool (client- or BC-scoped), averaged across available domains -- this matrix carries no ALL_DOMAINS aggregate row, so the mean is taken across domains, not read from one. |
| `fragmentation_diagnostic` / `exact_identity_overlap` | `project_fragmentation_diagnostic.csv` | Divergence between footprint overlap (`union_jaccard`) and exact per-file identity overlap. Folds in `project_mean_file_pair_jaccard_matrix.csv`'s signal via this column rather than rendering that matrix standalone -- see `docs/governance_generator_cross_compare_coverage.md`. |
 
**Permissible interpretation:** high `union_jaccard` + high `density_similarity`
is consistent with two projects sharing real configuration content, not just
similar population habits. High `density_similarity` alone, with low
`union_jaccard`, means the two projects populate the same domains to a similar
*extent* without holding the same canonical patterns -- state this as "same
shape, different content," not as a softer approximation of similarity.
 
**Prohibited interpretation:** do not read a low `union_jaccard` pair as
evidence of noncompliance, poor practice, or governance failure -- a
portfolio-shape diagnostic does not establish that convergence was expected
or desirable for that pair (see "Known bad inferences," cross-client
convergence entry, for the same reasoning applied at project grain).
 
**Resolved defect -- absent rows here were a real bug, now fixed; check
`comparison_type_coverage` before trusting either signal.** An empty
`fragmentation_diagnostic` ALL_DOMAINS paragraph, and a blank
`cross_client_similarity_mean` across every client, were both symptoms of
one upstream defect in `compare_cross_segment.py`'s pair discovery, fixed
as of PR 381 07/21/26 -- not a data gap, and not the confirmed
segmentation-design characteristic an earlier version of this guide
described it as.

- **Root cause.** `build_segment_manifest.py`'s `redundant_single_child`
  pass demotes a segment to `run_type="registration"` whenever a direct
  child's population is byte-identical to its own -- correctly avoiding
  running the same population twice. Once `business_center_label` became
  a real cut dimension, this made "this client's Project files all sit
  in one business center" the common case, so a client-only Project
  rollup routinely got demoted to a business-center-scoped child.
  `discover_cross_client()`, `discover_sibling_segments()`, and
  `discover_parent_siblings()` all gated on `run_type in ("bundle",
  "reference")` before ever reaching their own eligibility checks (e.g.
  `_is_client_only_project_segment()`'s blank-`business_center_label`
  test) -- so the demoted row vanished from all three before its shape
  was even evaluated, not because the shape check itself failed.
- **Fix.** `_resolve_runnable_segment()` reads the demoted row's
  `redundant_single_child:<segment_id>` note (added by
  `_redundant_child_segment_id()`) and resolves -- transitively, since a
  redundant pointer can itself point to a further-redundant child -- to
  the population-identical runnable descendant. This is not the
  "loosen the blank-`business_center_label` requirement" anti-pattern
  flagged elsewhere in this guide: the substitute carries the exact same
  `population_hash` the demoted segment would have, not a narrower
  slice of it. Role/grain classification still reads from the
  *original*, pre-resolution row (a blank-role client rollup redundant
  to a Project-scoped descendant must not be misfiled as a genuine
  Project sibling).
- **Current state.** `cross_client`, `sibling_projects`, and
  `parent_sibling_roles` are now populated wherever an eligible client/
  sibling relationship exists, and `fragmentation_diagnostic` folds in
  real (non-self) `project_mean_file_pair_jaccard_matrix.csv` cells
  again. `governance_client_summary.csv`'s `cross_client_similarity_mean`
  is populated for every client with project files in a comparable
  sector.
- **What can still legitimately be empty.** A domain or client can still
  show no `cross_client`/`sibling_projects` evidence for real reasons
  unrelated to this defect -- e.g. a client with zero project files, or
  one classified `Non-comparable (different sector)`. Absence is only
  suspicious now if `comparison_type_coverage` in
  `governance_package_health.json` shows the comparison type missing
  from `seen` entirely across the whole corpus, not merely absent for
  one client/domain.

**`client_cross_bc` is a separate comparison type with the opposite
trigger condition**, and was not affected by the above defect:
`discover_client_cross_bc()` fires only when a single client's Project
rows span two or more *distinct, non-blank* `business_center_label`
values. In the current corpus every client sits in exactly one business
center -- confirmed directly, not inferred -- so `client_cross_bc` is
correctly and expectedly empty here. A client operating across multiple
business centers would populate `client_cross_bc`, not `cross_client`;
these answer different questions and do not fail or succeed together.
Revisit if a genuine multi-business-center client appears in a future
corpus.
 
### Adoption breadth (reuse-by-client cut, `pattern_reuse_summary_by_client.csv`)
 
An **additive** breadth signal alongside the existing distinct-pattern reuse
table -- not a replacement, and not deduplicated against it. Reports, per
domain, how many of that domain's clients have at least one pattern in the
`corpus_wide` reuse bucket (`bucket_basis: clients_in_corpus_domain`).
 
**Permissible interpretation:** a domain where few clients clear this bar is
a real signal of narrow adoption.
 
**Prohibited interpretation / known limitation -- the bar can saturate.**
"At least one corpus-wide-reused pattern" is a low bar: in at least one
production run, every domain shown reached 7/7 clients, which does not by
itself distinguish a domain with deep, broad genuine adoption from one that
clears the bar once per client and no further. Do not report "N/N clients"
as evidence of strong governance convergence without also checking the
`corpus-wide pattern-instances` count (or the distinct-pattern reuse table
above it) for whether the adoption is substantial or marginal.
  
## Comparability

- **Sector.** Cross-client convergence (`xc`) is computed only between
  clients classified `healthcare` in `policies/client_sector.csv`. A client
  with a known non-healthcare sector gets tier `Non-comparable (different
  sector)`; an *unclassified* client (absent from that file) is **not**
  the same thing as "confirmed non-healthcare" — it falls through to
  normal tiering. Do not assume an unclassified client's absence from a
  cross-client reading means anything about its sector.
- **Unit system.** Metric and imperial segments are never blended into one
  comparison by this pipeline; `governance_package_health.json`'s
  `scope_coverage.unit_systems_seen` is a factual inventory, not a
  comparability gate. As of this guide's version, metric project data is
  limited to template-to-container comparisons only (see
  `governance_narrative_context.md`'s own limitations section for the
  current corpus's specific coverage).
- **All-view vs. used-view.** Every cascade field has an "all-view"
  (presence) and, where the schema supports it, a "used-view" (active-use)
  reading. `governance_package_health.json`'s `used_view_fallback` /
  `schema_detection` report whether used-view columns actually exist in
  this run's `cross_segment_summary.csv`, or whether used-view numbers are
  silently the same as all-view (single-schema fallback). **Do not treat an
  all-view containment score as evidence of active use** unless
  `used_view_fallback` is `false`.
- **Excluded domains.** `governance_domain_summary.csv` still lists domains
  in `excluded_from_scoring` (`policies/governance/domain_governance_policy.json`),
  but their scores are excluded from aggregate/cross-domain framing because
  they are structurally anomalous in the current corpus — do not silently
  fold them into an "average governance health" claim.
- **Small samples.** `governance_client_summary.csv`'s `confidence_note`
  and `policies/governance/governance_thresholds.json`'s
  `client_confidence_low_max_files`/`client_confidence_moderate_max_files`
  gate how much weight a client-level reading can bear. A tier or
  onboarding read backed by a "Low corpus confidence" client is a prompt
  for more sampling, not a settled conclusion.

## Missing and null values

- **CSV blanks are not uniform.** `governance_domain_summary.csv` renders a
  present-but-unavailable *numeric* field as the em-dash `"—"` (via
  `fmt()`/`pct()`), but a governance-state-sourced column for a domain with
  *no governance-state entry at all* renders as `""` (empty string) — two
  different "missing" conditions, two different characters. Do not treat
  them as the same signal.
- **A missing optional input is not evidence of no activity.** If
  `--governance-state-summary`, `--file-meta`, `--union-inventory`, etc.
  were not supplied for a run, the corresponding sections/columns are
  blank because the *input wasn't provided* — never infer "nothing
  happened" from an absent optional artifact. Check
  `governance_package_health.json`'s `optional_inputs` to see what was
  actually supplied for this run.
- **`governance_findings.json` distinguishes evidence from questions.**
  Every finding except `leadership_question` carries `status: supported`
  and `authority_level: controlled_interpretation`. `leadership_question`
  findings carry `status: question_not_claim` and
  `authority_level: convenience_summary` — they are suggested review
  prompts, never observed results. Do not present a leadership question as
  a finding.

## Authority ordering

Highest to lowest:

1. `governance_package_health.json` and the source comparison CSVs
   (`cross_segment_summary.csv`, `cross_segment_pooled.csv`, and any
   optional governance-state/delta/file-metadata CSVs actually supplied).
2. The deterministic rollups (`governance_domain_summary.csv`,
   `governance_client_summary.csv`).
3. `governance_findings.json` (rule-derived classification, traceable to
   rule IDs in `policies/governance/finding_rules.json` and specific rollup
   fields).
4. `governance_brief.md` and `governance_narrative_context.md`
   (convenience-summary/controlled-interpretation prose — a distillation of
   the artifacts above, never a new source of fact).
5. Any LLM-generated conversational interpretation of the above (not
   produced by this pipeline; if a downstream tool adds one, it must be the
   lowest-authority layer).

If a lower-authority artifact appears to disagree with a higher one, the
higher-authority artifact wins — that disagreement itself is worth
surfacing to a human, not silently resolved in the LLM's favor.

## Known bad inferences

- Do not treat `governance_tier` as a standards-approval decision or a
  statement of organizational intent — it is an evidence-readiness
  classification only (see `render_findings_and_recommendations()`'s own
  stated scope boundary).
- Do not treat low `cross_client_convergence` as a governance failure — the
  scores show where common ground exists, not that divergence is wrong; a
  client is not obligated to converge with others.
- Do not treat a domain's absence from `governance_domain_summary.csv` as
  "no data" without checking `governance_package_health.json`'s
  `domain_rows_excluded_no_signal` — a domain can be legitimately excluded
  for having only scope-level (Group 3) signal, which this generator
  captures but does not yet render.
- Do not compare an all-view score against a used-view score, or a score
  from one `pool_scope` grain against another, as if they were the same
  measurement — check `governance_package_health.json` and the relevant
  evidence-map entry's `known_limitations` first.
- Do not treat `passive_inheritance_risk` or `missing_or_degraded_evidence`
  findings as a verdict that something is wrong — they are prompts for
  human review of *why* (intentional pruning vs. propagation failure vs.
  role-specific specialization are all consistent with the same signal).
- Do not silently blend a client-scoped, business-center-scoped, and
  enterprise-scoped reading into one number — this pipeline keeps them
  distinct (`*_by_scope` fields, Group 1/2/3 cascade types) specifically to
  prevent that blend; a "pooled mean" hiding sharp per-scope disagreement
  is itself flagged as an anomaly note when it occurs.
- Do not treat a Project Portfolio pair's low `union_jaccard` as evidence of
  noncompliance -- it is a footprint-overlap diagnostic, not a governance
  verdict, and this section is deliberately excluded from `assign_tier()`.
- Do not treat 100% (or near-100%) adoption breadth in the reuse-by-client
  cut as proof of deep convergence -- the underlying bar (one corpus-wide
  pattern) is easy to clear and can saturate across all clients while actual
  adoption depth varies widely; cross-check the pattern-instance count or
  the distinct-pattern table before making a convergence claim.
- Do not treat `Insufficient Evidence` at the enterprise scope as evidence a
  domain has no usable data anywhere in the package — it is scope-specific:
  a domain's enterprise-scoped tier reading `Insufficient Evidence` does not
  mean the domain has no usable evidence anywhere in the package; check
  `governance_client_summary.csv`, `governance_bc_summary.csv`, and the
  domain's `cross_client_convergence` field before concluding nothing is
  known about it.
- Do not treat "Region" and "Enterprise" reading identically as completed
  cross-region standardization — all corpus files currently come from one
  region, so a future `region` segmentation dimension will produce results
  identical to the existing enterprise-level rollup until a second region's
  data actually exists; this reflects current data coverage, not
  standardization.

## Policy profiles (where the thresholds live)

Every threshold referenced above is externalized to
`policies/governance/*.json` (`governance_thresholds.json`,
`domain_governance_policy.json`, `client_onboarding_policy.json`,
`finding_rules.json`) and loaded at run time — see
`docs/governance_evidence_package.md`'s "Policy profiles" section for the
full list. `governance_package_manifest.json`'s `policy_profiles.profiles`
records exactly which profile version was applied to a given run.

## Where to go next

- **Quick top-line read:** `governance_brief.md`.
- **A specific recurring question:** `docs/governance_question_routes.md`.
- **Full detail:** `governance_narrative_context.md`,
  `governance_domain_summary.csv`, `governance_client_summary.csv`,
  `governance_findings.json`.
- **Is this package usable at face value:** `governance_package_health.json`.
- **What exists and where:** `governance_evidence_map.json`.

## What to do when a pre-built route isn't enough
 
`docs/governance_question_routes.md` routes are versioned by maturity, per
the design reference this package's routing layer follows
(`GMcDowellJr/llm_evidence_framework/discovery/question_route_discovery.md`):
 
```
candidate -> active -> recipe-backed -> extractor-backed
```
 
**Every route in this package is currently `candidate`** -- seeded from
recurring questions, but none has a proven history of repeated use, and none
has an attached extraction script. A `candidate` route's "Primary artifacts"
and "Suggested first check" fields tell you *where* the answer lives; they
do not, by themselves, make a multi-GB source file (`cross_segment_file_pairs.csv`
is ~3.8GB) tractable to search directly.
 
**If a question resolves cleanly from the rollup CSVs, `governance_findings.json`,
or `governance_evidence_map.json` -- stop there.** Most questions this
package was built for do. This section only applies when a route's own
"Escalation" field points past those, into the full evidence archive.

**The exhaustive list of files this generator never parses** (per its own
module docstring in `tools/generate_governance_narrative.py`), each now its
own `governance_evidence_map.json` artifact (D-024): `cross_segment_file_pairs.csv`,
`comparison_registry.csv`, `pattern_reuse_summary_by_domain.csv`, and
`project_mean_file_pair_jaccard_matrix.csv`. Do not assume there are more --
this is the confirmed set, not a partial example list.

**Each of those four entries already carries its column header (name +
inferred dtype) and row count**, populated by a live scan of the file when
present (the same scan `governance_file_inventory.json` uses for files with
no artifact_id at all). Read the entry's `columns`/`row_count` fields before
writing an extraction script in step 2 below -- this replaces guessing at a
multi-GB file's schema from its filename alone with the real header, sourced
from the file itself.

**When escalation is needed:**
 
1. **Recognize the gap explicitly, don't silently improvise.** State that the
   question requires drill-down beyond what the package's compact layer
   supports, and name which large source file is needed -- with its real
   schema from `governance_evidence_map.json`, not an assumed one.
2. **Write a small, parameterized, streaming-safe extraction script** rather
   than attempting to read or reason over the raw file directly --
   filtered by the specific fields a finding's `support[]` or a route's
   "Relevant fields" already name (e.g. `domain`, `comparison_type`,
   `segment_id`), not a free-form search. Chunked/streaming reads only; do
   not load a multi-GB CSV into memory or into context.
3. **Report the filter and the row count against the total**, not just the
   matching rows -- e.g. "47 rows matched `domain=floor_types`,
   `comparison_type=container_to_project` out of 3,804,274 total in
   `cross_segment_file_pairs.csv`." The filtered result is still
   `authoritative_deterministic_evidence` (a filter over an already-authoritative
   file, not a new computation) -- but only if the filter itself is stated,
   so the result is auditable rather than a black box.
4. **A recipe that gets reused is worth promoting.** If the same extraction
   pattern would answer a route's question repeatedly (not just this one
   instance), that's the signal to attach it to the route in
   `docs/governance_question_routes.md` and move that route from `candidate`
   toward `recipe-backed` -- per the design reference's own guidance, a
   route shouldn't be promoted just because it was imagined once; it should
   be promoted because it proved useful more than once.
   
**This loop is itself part of what makes this package usable, not an
afterthought** -- the compact layer (338KB across the package's core JSON/MD
artifacts) is a ~16,000:1 compression of the ~5.16GB `cross_segment_*`
comparison layer, which is itself already a major compression of the raw
per-file corpus data. That ratio is only defensible because the package is
built to escalate cleanly into the full archive when a question genuinely
needs file-level identity, not because nothing was lost in the compression.
Losing track of *that this escalation path exists* would quietly turn a
designed boundary into an unexplained dead end.