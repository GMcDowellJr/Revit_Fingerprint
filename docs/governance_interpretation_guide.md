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
