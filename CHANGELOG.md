# CHANGELOG

This file tracks **semantic changes only**:
- anything that changes hashes
- anything that changes what a hash *means*
- anything that changes interpretation, scope, or dependency structure

Pure refactors, moves, renames, formatting, and perf tweaks do **not** belong here.

---

## [Unreleased]

### Added
- New `cross_client` comparison type in `tools/compare_cross_segment.py`
  (`discover_cross_client()`, `--cross-client` CLI flag, default-on): pairs
  each client's own broadest (client-only-scoped) Project population against
  every other client's, within the same unit_system, independent of segment
  lineage. Fixes `cross_client_convergence` (governance_domain_summary.csv)
  and `cross_client_similarity_mean` (governance_client_summary.csv) being
  blank for every row -- the only prior source for those columns was
  `sibling_projects`, which only pairs Project segments sharing an immediate
  `parent_segment_id` and is additionally sector-gated (both clients must be
  tagged `healthcare` in `policies/client_sector.csv`) in
  `build_cascade()`'s `xc` accumulation. `cross_client` has no shared-parent
  requirement and no hardcoded sector gate (sector filtering, where wanted,
  is left to downstream consumers). `tools/generate_governance_narrative.py`'s
  `build_cascade()` and `build_client_summary()` now also accumulate `xc`/
  `xc_mean` from `cross_client` rows alongside the existing `sibling_projects`
  source. Jaccard-based, undirected (mirrors `sibling_projects`'s scoring
  path); no governance-state rows are written for it (not in
  `GOVERNANCE_STATE_DIRECTED_TYPES`), matching `sibling_projects`.
  `build_client_summary()`'s `xc_by_client`/`xc_dom_by_client` read
  `client_label_a`/`client_label_b` directly rather than positionally parsing
  `segment_id` (the old `len(pa) == 3` assumption only held for the
  `unit|role|client`-shaped IDs `build_segment_manifest.py` happens to emit
  for a client-only Project segment; `discover_cross_client()` places no such
  constraint on `segment_id` shape), with an explicit `ca != cb` guard to
  preserve the existing within-client-sibling exclusion the old check
  enforced incidentally. `client_files`'s `n_project_files` backfill now also
  recognizes `cross_client` rows (previously `sibling_projects`-only), so a
  client discoverable only via a `cross_client` row no longer falsely reports
  `n_project_files=0`. New `drop_legacy_sibling_projects_covered_by_cross_client()`
  in `compare_cross_segment.py` drops a `sibling_projects` pair when
  `cross_client` already covers the identical two segments (they can share an
  immediate `parent_segment_id`, since `discover_sibling_segments()` groups
  purely by parent/role/unit) -- otherwise both would double-count that one
  pair in `xc`/`xc_by_client` and collide on `comparison_run_id`
  (`make_comparison_run_id()` hashes only segment IDs + timestamp, not
  comparison_type -- a broader, pre-existing characteristic of that
  identifier, not changed here). `cross_client`'s contribution to `xc`
  (`build_cascade()`) is gated to both-healthcare pairs, matching
  `sibling_projects`'s existing gate -- `xc` is documented and consumed
  elsewhere (client-tier "Non-comparable (different sector)" logic) as a
  healthcare-cohort metric; `discover_cross_client()` itself is unaffected and
  still emits every client pair into `cross_segment_summary.csv` regardless
  of sector. `xc_by_client`/`xc_dom_by_client` (`build_client_summary()`,
  feeding `cross_client_similarity_mean`) gain a softer, consumer-appropriate
  exclusion -- a pair is dropped only when a side has a CONFIRMED
  non-healthcare sector (`sector not in ("unknown", "healthcare")`), matching
  this function's own definition of "comparable"; an unclassified client
  still counts. This closes a pre-existing gap (this rollup never filtered by
  sector for either source type) that `cross_client` being default-on and
  pairing every client made routinely consequential. `main()` in
  `compare_cross_segment.py` now applies `--segment-a`/`--segment-b`
  filtering *before*
  `drop_legacy_sibling_projects_covered_by_cross_client()` rather than after:
  `discover_sibling_segments()` orders its pair by sorted segment ID while
  `discover_cross_client()` orders by sorted client label, so the surviving
  `cross_client` row replacing a dropped `sibling_projects` row can be in the
  reverse orientation -- which the position-sensitive segment filters would
  then also reject, making a scoped run silently report zero pairs for
  segments that do have a comparison. No effect on the default (unscoped)
  path.
- `governance_domain_summary.csv` gains `container_to_project_scoped` /
  `container_to_project_scoped_pair` columns in
  `tools/generate_governance_narrative.py`. Root cause: `container_to_project`
  (`cp`) is populated only from rows where BOTH sides are the fully unscoped
  ("enterprise::enterprise") segment -- real Project segments are almost never
  fully unscoped, so `cp` stayed empty for effectively every domain even
  though real, `data_sufficient == "true"` container_to_project evidence
  existed at other scope levels (`cp_by_scope`, already computed but never
  surfaced in this CSV). The new columns report the mean of the largest
  (most rows) non-enterprise, `data_sufficient` scope_pair bucket, plus which
  scope_pair it came from, and are populated only when `container_to_project`
  itself is empty -- `container_to_project`'s own enterprise-only meaning is
  unchanged, so this never competes with or is mistaken for enterprise-level
  evidence (same posture as `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE`).
  Sourced from a new, separate accumulator (`cp_by_scope_suff`) rather than a
  filtered view of `cp_by_scope`, so `_has_group1_bc_pooled_evidence()`/
  `render_group1_scope_section()` (existing `cp_by_scope` consumers) are
  unaffected. No other comparison type's `data_sufficient` handling changed.
  `_TIER_DRIVER_SUPPORT_FIELDS` (the shared list of `governance_domain_summary.csv`
  columns every tier-based `governance_findings.json` finding's `support[].fields`
  references) now includes both new columns, so a `missing_or_degraded_evidence`
  finding for a domain whose only evidence is the scoped fallback (i.e.
  `container_to_project` itself is blank) still points a consumer at the
  actual populated value instead of only the blank primary column.
- `tools/generate_governance_narrative.py` now emits an interpretation/
  routing layer: `docs/governance_interpretation_guide.md` (stable,
  package-type-level -- what each metric/tier means, comparability rules,
  missing-value semantics, authority ordering, known bad inferences),
  `docs/governance_question_routes.md` (a candidate question-route catalog,
  all routes at "candidate" maturity, following the discovery scaffold in
  the design-reference `llm_evidence_framework` repo's
  `discovery/question_route_discovery.md`), and `governance_brief.md` (the
  one new generated, per-run artifact -- a narrower digest built by a new
  `render_governance_brief()`, which consumes the already-computed findings
  list and package health directly, computing nothing new). New CLI flags
  `--emit-interpretation-layer`/`--no-emit-interpretation-layer` (default:
  on) control `governance_brief.md` only, independently of
  `--emit-evidence-package`. `governance_evidence_map.json` grows from 19 to
  22 artifacts; `governance_narrative_context.md`'s authority header gains
  pointers to all three new artifacts. No existing classification, scoring,
  or CSV column changed. See D-022 and `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py`'s governance thresholds, excluded/
  passive-inheritance-risk domain lists, per-domain guidance text, and
  client-onboarding interpretation thresholds are now loaded from JSON policy
  profiles under `policies/governance/` (`governance_thresholds.json`,
  `domain_governance_policy.json`, `client_onboarding_policy.json`,
  `finding_rules.json`), via a new sibling module `tools/governance_policy.py`
  (generic load/fallback loader; no governance business content of its own).
  `--policy-dir` (accepted but inert since the Phase 1 evidence-package work)
  now defaults to `policies/governance/` and is actually read: a new
  `apply_governance_policy()` reassigns every module-level threshold/domain-
  policy constant this file's existing functions already read as plain
  globals, so no existing function body changed -- only the source of each
  constant's value did. The shipped JSON files reproduce this generator's
  pre-externalization Python literals value-for-value, so no existing
  invocation's classification output changes by default (locked in by a
  regression test running the CLI twice -- default vs. explicit
  `--policy-dir policies/governance/` -- and asserting byte-identical
  `governance_domain_summary.csv`). A profile file missing from `--policy-dir`
  falls back, per file, to this generator's own built-in default for that
  profile only, reported in `governance_package_health.json`'s new
  `policy_load_status`/a `governance_policy_profile_defaulted` warning
  (degrades `overall_status` to `degraded`) and in
  `governance_package_manifest.json`'s `policy_profiles.profiles` (resolved
  `profile_id`/`schema_version`/`source` per profile). See D-021 and
  `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py` now emits `governance_findings.json`:
  structured, rule-derived governance findings (`baseline_candidate`,
  `strong_baseline_candidate`, `local_review_required`, `high_fragmentation`,
  `active_local_practice`, `cross_client_convergence`, `low_client_coherence`,
  `passive_inheritance_risk`, `missing_or_degraded_evidence`,
  `leadership_question`) with epistemic provenance (`origin`/`fidelity`/
  `authority_level`/`limits`) and `support[]` references back to specific
  `governance_domain_summary.csv`/`governance_client_summary.csv` rows and
  fields, via a new `build_structured_findings()`. `render_findings_and_recommendations()`
  now consumes the same structured findings instead of independently
  recomputing the classification, via a new shared
  `_classify_domains_for_findings()`, so the narrative's prose and the JSON
  findings can no longer disagree. Leadership questions are marked
  `status: question_not_claim` / `authority_level: convenience_summary`,
  distinct from evidence findings (`status: supported`). No existing CSV
  column, classification/scoring logic, or threshold changed. See D-020 and
  `docs/governance_evidence_package.md`.
- `tools/generate_governance_narrative.py` now emits a governance evidence-package
  layer alongside its existing outputs: `governance_package_manifest.json`
  (provenance -- which inputs were provided/found, which outputs were written and
  their sizes, comparison_run_id(s)/executed_utc observed in the loaded rows),
  `governance_package_health.json` (schema detection, used-view fallback,
  comparison_type coverage, blocking conditions, warnings), and
  `governance_evidence_map.json` (one entry per artifact -- the CSVs the
  generator reads, two sibling CSVs it produces but never reads
  (`cross_segment_file_pairs.csv`, `comparison_registry.csv`), and its own six
  generated artifacts -- with authority_level/grain/can_answer/cannot_answer/
  known_limitations per the new `tools/governance_evidence_package.py` module).
  New CLI flags `--emit-evidence-package`/`--no-emit-evidence-package` (default:
  on), `--policy-dir` (recorded, not yet read), and `--package-schema-version`.
  The narrative gains a new authority-header section stating its own
  `controlled_interpretation` role, and the previously-stale producer-identity
  footer (`generate_governance_narrative_dod_aligned_v2.py`, which never matched
  the actual script) now references the real generator name. No existing CSV
  column, classification/scoring logic, or threshold changed -- see D-019 and
  `docs/governance_evidence_package.md`. Structured findings
  (`governance_findings.json`) and policy externalization are deferred to later
  work.
- `tools/generate_governance_narrative.py`'s `build_cascade()` now breaks
  `gt`/`gc`/`gp` (generic->template/container/project containment) down by the
  TARGET's own scope level, instead of discarding every row where the target
  isn't the single broadest ("enterprise") population. `compare_cross_segment.py`
  intentionally emits `generic_to_template`/`_container`/`_project` rows for
  client-/bc-/discipline-scoped targets too — real baseline-propagation evidence
  that a prior pass (PR #350) deliberately gated away to keep `gt`/`gc`/`gp` as a
  single clean enterprise-wide number (Option A, avoiding the blend-distinct-
  scope-grains anti-pattern this file's other fixes already correct for). `gt`/
  `gc`/`gp` themselves are unchanged — still the enterprise-only slice — but a
  new `gt_by_scope`/`gc_by_scope`/`gp_by_scope` (`{scope_label: mean_containment}`,
  mirroring the existing `wp_disc` per-discipline breakdown pattern) now captures
  every other scope level (`client`, `bc`, `discipline`, and combinations, via a
  new `_target_scope_label()` using the `business_center_label_a/b` columns added
  in the intervening B6 schema fix) rather than silently dropping it. The
  GENERIC (reference) side of the comparison is still required to be the one
  canonical enterprise-wide Generic population.

  Rendering/anomaly-detection followed as a second pass: `detect_anomalies()`
  now flags a material (≥0.25 absolute) divergence between the enterprise
  reading and the mean of a domain's scoped buckets, in either direction, per
  cascade stage (Generic→Template/Container/Project); a new
  `render_generic_baseline_scope_section()` renders one row per
  `(domain, scope)` pair actually observed (`Domain | Scope | G→Template |
  G→Container | G→Project`) — a fixed-column table doesn't fit here since scope
  buckets are combinatorial (`client`, `bc`, `discipline`, `client_discipline`,
  etc.), not a small fixed set like disciplines. The section is omitted
  entirely when no domain has any scope-breakdown data.

- `tools/generate_governance_narrative.py`'s Group 1 dispatch (`tc`/`cp`/`tp`
  from `template_to_container`/`container_to_project`/`template_to_project`)
  gets the same Option C treatment Group 2 (`gt`/`gc`/`gp`) got above, closing
  the gap documented in
  `docs/governance_narrative_group1_scope_gap_investigation.md`: since
  `business_center_label` became a real segmentation cut, almost no segment is
  fully unscoped anymore, so `tp`/`cp` were `None` for effectively every
  domain and `assign_tier()` always fell to `TIER_INSUFFICIENT` regardless of
  real bc-pooled evidence sitting unused in `cross_segment_summary.csv`. `tc`/
  `cp`/`tp` themselves are unchanged — still populated only from the
  `"enterprise::enterprise"` (both sides pass `_is_unscoped_segment()`) pair —
  but new `tc_by_scope`/`cp_by_scope`/`tp_by_scope` (`{scope_pair:
  mean_containment}`, keyed `f"{scope_a}::{scope_b}"` since, unlike Group 2,
  neither side of a Group 1 pair is gated to a fixed role population) now
  capture every other `(scope_a, scope_b)` pair instead of discarding it. The
  separator is `"::"`, not a bare `"_"`, because `_target_scope_label()`'s own
  multi-dimension labels (e.g. `"bc_discipline"`, `"client_bc"`) already
  contain underscores — joining two such labels with `"_"` is ambiguous
  (`("client", "bc_discipline")` and `("client_bc", "discipline")` both
  produce the literal string `"client_bc_discipline"`) and this was confirmed
  to actually occur against a real `cross_segment_summary.csv` export during
  review, not just a theoretical edge case.

  A same-bc-both-sides (`"bc::bc"`) pooled value gives `assign_tier()` a new,
  distinctly-named fallback tier, `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE`
  (ordered directly before `TIER_INSUFFICIENT`, i.e. the weakest tier that
  still has *some* evidence), when `tp`/`cp` are both `None` — deliberately
  NOT blended into the existing enterprise-only `primary`/score-banded tiers,
  since bc-pooled evidence is not enterprise-level evidence. The `T→Container`/
  `T→Project`/`C→Project` columns in `render_domain_tiers()` stay `—` for
  domains in the new tier (never silently repointed at a pooled number); a new
  `render_group1_scope_section()` (mirroring `render_generic_baseline_scope_section()`)
  renders the per-`(domain, scope_pair)` detail instead. `detect_anomalies()`
  gained a Group 1 analog of the existing scope-divergence note: since Group 1
  usually has no enterprise reading to diverge from (that's the gap this fix
  closes), the check instead flags when a pooled bucket's own intra-bucket
  spread (min/max across the individual rows pooled into it) is ≥0.25
  absolute — the same materiality threshold as Group 2's check — meaning the
  pooled mean is hiding sharp disagreement rather than reflecting genuine
  convergence. The note's wording is deliberately scope-neutral rather than
  always saying "business-center": validating against a real
  `cross_segment_summary.csv` showed most divergence notes actually fire for
  scope pairs like `client_bc::client_discipline`, where the client and
  business center are held constant and only the discipline varies across the
  pooled rows — an earlier wording draft said "across individual
  business-center pairs" unconditionally, which was accurate only for the
  `"bc::bc"` case and misleading for every other scope_pair.

- Four PR-review findings on the Group 1 bc-pooled fallback above, all
  confirmed against the real `cross_segment_summary.csv`/`segment_manifest.csv`
  export supplied during review:
  1. **Value-mismatch guard (new `_group1_scope_pair()`)**: `_target_scope_label()`
     only records SHAPE (which dimensions are populated), not VALUE.
     `discover_within_segment()` in `compare_cross_segment.py` pairs same-parent,
     same-unit Template/Container/Project segments without checking that scope
     label VALUES match, so a `BC_1`-scoped segment paired against a
     `BC_2`-scoped segment was silently bucketed as `"bc::bc"` — the same key as
     genuine same-business-center evidence. Confirmed reachable in the real
     export: one real row (`client_bc_discipline` shape on both sides, one field
     mismatched) was landing in a merged bucket, corrupting 20 domains'
     `tc_by_scope` entries. New `_group1_scope_pair()` verifies every field
     making up a shared shape actually matches before using the plain
     `f"{scope_a}::{scope_b}"` key; a same-shape-different-value pair now gets a
     distinct `f"{scope_a}!cross::{scope_b}!cross"` key instead — captured, not
     discarded, but never conflated with same-value pooled evidence. `tc`/`cp`/`tp`
     remain byte-for-byte unchanged (re-verified: 0 mismatches across all 32 real
     domains).
  2. **`_has_renderable_cascade_signal()` scope-only gap**: a domain whose ONLY
     Group 1 signal is scoped evidence (e.g. `tp_by_scope["bc::bc"]` populated
     but no enterprise `tc`/`cp`/`tp` and no `wp_all`/Group 2 signal) would get
     `TIER_INSUFFICIENT_ENTERPRISE_BC_EVIDENCE` from `assign_tier()` but never
     appear in `render_domain_tiers()`/the domain CSV, since
     `_has_renderable_cascade_signal()`'s key list didn't include
     `tc_by_scope`/`cp_by_scope`/`tp_by_scope` (which are always non-`None`
     dicts, so they can't reuse the existing `is not None` check). Now also
     checks for a non-empty by-scope dict.
  3. **`render_group1_scope_section()` prose overclaimed "business-center-level"**:
     the section's intro described every non-enterprise row as "pooled
     business-center-level evidence," but it renders every scope_pair, most of
     which (`client::bc`, `client_bc::discipline`, etc.) are not business-center
     evidence at all. Reworded to name only `"bc::bc"` as business-center-level
     and tier-relevant; other scope pairs are described as real evidence in
     their own right that does not by itself grant the new tier. (The
     equivalent `detect_anomalies()` wording was already fixed in the prior
     commit.)

### Fixed
- `tools/archetype/generate_archetype_candidates.py`'s `_governance_question_hint()`
  only ever inspected `target_domain`, so it couldn't distinguish a dynamic
  View Filter Definition (VFD) edge from a structural one. Dynamic VFD edges
  carry `source_domain == "view_filter_definitions"` but `target_domain` ==
  whatever element-type domain the filter scopes to (`wall_types`,
  `ceiling_types`, `floor_types`, `roof_types`); the static
  `view_filter_applications_view_templates.stack_filter__view_filter_definitions`
  chain edge instead carries `target_domain == "view_filter_definitions"`.
  Two consequences, both independently documented in
  `tools/archetype/review/archetype_dp1_prompt.md`'s known-misfire list as a
  manual correction required every Decision Point 1 cycle: (1) a VFD-to-VFD
  pair targeting `wall_types` collided with the `wall_graphics` predicate
  (`"wall_types" in target_domain`) before any VFD-aware check existed; (2) a
  VFD-to-VFD pair targeting `ceiling_types`/`floor_types`/`roof_types` matched
  none of the target-domain predicates and fell through to `"unknown"`.
  Fixed by adding `_is_vfd_related(source_domain, target_domain)` (true when
  `source_domain == "view_filter_definitions"` OR `target_domain ==
  "view_filter_definitions"`), returning `"view_filter_strategy"` when both
  sides of the pair are VFD-related, checked before the existing
  target-domain-only priority list. The first version of this fix only
  checked `source_domain_a == source_domain_b == "view_filter_definitions"`
  (VFD-to-VFD only) and still misclassified a VFD edge paired with the static
  stack_filter chain edge as `wall_graphics` — caught in PR #357 review and
  corrected to the broader `_is_vfd_related()` form above. This only affects
  auto-generated candidates in `archetype_definitions_candidates.json`; it
  does not retroactively change `governance_question` on already-promoted
  archetypes in `config/archetype/archetype_definitions.json`, which are set
  by human curation at DP1 independent of this hint.

- `tools/generate_governance_narrative.py` read `client_label`/`discipline_label`/
  the "is this the broadest population for its role" condition by parsing
  `segment_id` positionally (`get_client()`, `get_disc()`, `is_generic()`, a
  `"Template" in segment_id` substring check) instead of the real
  `client_label_a/b`/`discipline_label_a/b`/`governance_role_a/b` columns that
  already exist on `SUMMARY_FIELDS`. This silently misparsed segments whose
  third pipe-separated part is a `business_center_label`/`collection_label`
  rather than a client (e.g. `imperial|Template|Shared` read as
  `client="Shared"`), and `is_generic()`'s length-2 heuristic couldn't
  distinguish a genuine broadest-role segment from a blank-`governance_role`
  scope rollup that also happens to produce 2 parts (e.g. `imperial|BC_2014`).
  Replaced with direct column reads and a `_is_unscoped_segment()` helper
  (role non-blank, `client_label`/`discipline_label` both blank). Two follow-on
  refinements to that helper, both confirmed against real segment-manifest
  construction: (1) `business_center_label`/`collection_label` are not yet
  columns on `SUMMARY_FIELDS`, so a segment scoped only by one of those two
  dimensions (e.g. `imperial|Template|BC_1234`) can slip past the column checks
  — rejected via a structural check that any segment_id part beyond
  `unit_system+role` must be blank once client/discipline are confirmed blank
  via their own columns; (2) that same check initially rejected a *genuinely*
  unscoped segment whose `client_label`/`discipline_label` dimension is
  explicitly selected-but-blank in its key (`build_segment_manifest.py`'s
  `_subset_to_id()` emits a literal empty token for this, e.g.
  `imperial|Template||Shared` for a blank client alongside a real
  `business_center_label` — see that function's own code comment), which is
  not hidden scope data and must not cause rejection; fixed by requiring only
  that any extra part be *empty*, not merely that there are exactly 2 parts.

- `tools/generate_governance_narrative.py`'s `build_cascade()` was a bare
  `if/elif` chain recognizing 5 of the ~16 `comparison_type` values
  `compare_cross_segment.py` can emit, silently dropping every other row with
  no signal that anything was excluded — including all four new scope-level
  types (`enterprise_to_project`, `bc_to_project`, `enterprise_to_bc`,
  `enterprise_to_client`) and the `generic_to_template`/`_container`/`_project`
  triple that is the literal top rung of the "Governance Cascade" diagram the
  narrative's own header already describes but never computed. Replaced with
  an explicit dispatch naming every known type across four groups (already-
  handled cascade stages; the newly-wired generic-to-* stage, threaded through
  as new `gt`/`gc`/`gp` fields and rendered as new table columns; the four
  scope-level types, captured under new `ep`/`bp`/`eb`/`ec` keys but
  deliberately not rendered/tiered yet — a scope-level axis, not one more
  cascade stage; and an explicit "known, deliberately excluded" registry for
  `sibling_templates`/`sibling_containers`/`sibling_generic`/`sibling_segments`/
  `governance_chain`, each with a verified reason) plus a coverage-check
  warning for any comparison_type not accounted for by name in any group.

- `build_governance_state_summary()`'s compact-summary loop had no
  `comparison_type` filter on any of its count/share fields, so rows for the
  four new scope-level types were silently averaged into the same per-domain
  number as `template_to_project`/`container_to_project` — a scope-level axis
  blended into a cascade-stage number with no indication it happened (traced:
  a synthetic `bc_to_project` + `template_to_project` pair for one domain
  produced a blended `provided_passive_share` of 0.375 pre-fix; 0.05 —
  `template_to_project` alone — post-fix). Its detailed per-pattern loop's own
  `_DIRECTED_GOVERNANCE_TYPES` gate was a stale hand-maintained copy of
  `compare_cross_segment.py`'s `GOVERNANCE_STATE_DIRECTED_TYPES`, missing all
  four new types and carrying two entries (`generic_to_downstream`,
  `parent_sibling_roles`) confirmed to never reach a governance-state output
  file today. Fixed by keying aggregation by `(domain, comparison_type)`
  throughout and importing `GOVERNANCE_STATE_DIRECTED_TYPES` directly instead
  of hand-copying it; the two unexplained legacy entries are kept rather than
  silently dropped pending confirmation of their disposition. A domain whose
  *entire* governance-state signal is scope-level-only is now correctly
  omitted from the returned map rather than stored as an all-`None`-valued but
  still-truthy dict, which had been switching its whole tier group's rendered
  table to state-columns mode with every visible state value blank.

- `DISC_KEYWORDS`/`DISC_LABELS` hardcoded a 7-discipline set that `get_disc()`
  used as the sole vocabulary for discipline detection, and
  `render_discipline_section()` iterated `DISC_LABELS.keys()` to decide which
  disciplines to render a section for — so any discipline outside that set
  (confirmed real: `lighting`, `medical_equipment`, `security`, alongside the
  existing 7) was invisible in that section even though the underlying
  `discipline_label_a/b` data already had it. Discipline vocabulary is now
  computed from the data actually present (`disc_domain_wp.keys()`);
  `DISC_LABELS` is kept only as an optional display-name override, falling
  back to a humanized title-case render (e.g. `medical_equipment` ->
  `"Medical Equipment"`) for anything not in the override map.

- `HEALTHCARE_CLIENTS = {"Kaiser", "Sutter", "Renown", "DCMH"}` plus a
  standalone `if client == "Intel": tier = "Non-comparable (different
  sector)"` special case hardcoded a business fact (client sector membership)
  that cannot be derived from the pipeline's own data into Python literals,
  requiring a code change and redeploy for every new client. Replaced with a
  `sector_map` lookup loaded from a new optional `client_sector.csv`
  (`client_label,sector` columns, `--client-sector`, defaulting to
  `policies/client_sector.csv` so existing invocations that don't pass the
  flag still get today's classification rather than silently losing the
  cross-client-convergence signal for every domain). An unclassified client
  (absent from the file, or the file itself absent) is `sector = "unknown"`,
  which now falls through to normal alignment tiering rather than being
  treated as either "Non-comparable" (that requires an explicit, *known*
  non-healthcare sector) or a confirmed different-sector profile in the
  onboarding-implications text — both of those previously fired for any
  `is_healthcare == False`, which conflated "known different sector" with "we
  don't know."
- `tools/compare_cross_segment.py` Mode D (`within_project`) grouped files by
  `project_label` using `.strip() or eid` — a fallback that only catches a
  truly-blank string, not a populated NA placeholder like
  `"__NOT_APPLICABLE__"`, `"n/a"`, or `"NA"`. Every file in a segment whose
  project is unassigned carries the exact same placeholder string, so all of
  them collapsed into one giant fake "project" and got pairwise-compared
  against each other (`C(n,2)` spurious pairs for `n` unassigned files —
  484 files in the `imperial` segment pre-fix). Fixed at all four sites that
  used this pattern: the `discover_within_project()` pair-discovery gate,
  both grouping loops (`by_proj`/`by_proj_used`, all-view and used-view) in
  `run_pair()`'s `is_within_project` branch, and `_project_label_for_file()`
  (used by `build_union_inventory_rows()` for the `n_projects_present`/
  `n_projects_denominator` union-inventory counts). All four now use
  `na_token.is_blank_or_na()` — the same NA-recognition helper Mode E's
  `discover_governance_chain()` already uses for `client_label`/
  `collection_label` — to decide when to fall back to the per-file `eid`
  singleton key, so unassigned-project files no longer group with each
  other (each remains its own singleton, same as a truly-blank label
  already did) while real shared `project_label` values (e.g. `"Renown"`,
  41 files) are unaffected.

- `tools/build_segment_manifest.py` `_sanitize_folder()` collapsed consecutive
  separator characters into one `_` and trimmed leading/trailing `_`, which
  erased a real distinction in `segment_id`: a cut dimension explicitly
  selected in a subset with a blank value (today only `client_label` — see
  `_build_segments()`'s blank-client handling) renders as an empty part
  between/after separator pipes (e.g. `imperial|Template|` or
  `imperial|Container||architectural`), which is a *different, smaller*
  population than that same dimension not being selected at all (e.g.
  `imperial|Template`, which pools every value of the field, blank
  included — always a superset of the selected-blank population). Both
  forms sanitized to the identical folder name, so once enough blank-client
  rows exist for the two populations to diverge (no longer collapsible via
  the existing `redundant_single_child` dedup), both become real,
  independently `bundle`/`reference`-eligible segments competing for the
  same `output_folder` — surfaced only as an opaque `_2` collision-avoidance
  suffix rather than a clear identity. `_UNSAFE_FOLDER_CHARS` no longer uses
  a `+` quantifier (each unsafe character is replaced one-for-one, so
  consecutive separators no longer collapse to a single `_`) and the final
  `.strip("_")` was removed, so a trailing/embedded blank-selected segment
  now sanitizes to a distinguishable folder name. Each blank part is also
  rendered as the literal token `enterprise` (the same scope-level term
  `compare_cross_segment.py` already uses for "no client, no bc" rows)
  rather than a bare `_`/`__`, so e.g. `imperial|Template|` sanitizes to
  `imperial_template_enterprise` instead of `imperial_template_` — a
  self-explanatory name instead of something that reads as a naming
  mistake. `segment_id` text itself (used elsewhere — parsed positionally
  in `tools/generate_governance_narrative.py` and hardcoded across dozens
  of existing tests) is completely unchanged; only the derived folder name
  changes, and only for segments that select a blank cut-dimension value.
  Verified against a real corpus manifest: 5 `bundle`/`reference`-eligible
  folder-name collisions, all resolved.

### Changed (breaking pipeline-contract)
- `segment_manifest.csv` and `run_registry.csv` no longer carry per-segment file
  membership as inline pipe-delimited columns (`export_run_ids`,
  `seed_export_run_ids`). For large populations these columns exceeded
  spreadsheet cell limits (Excel ~32,767 chars/cell, Google Sheets ~50,000
  chars/cell — confirmed offenders in the current corpus reached 59,271
  chars), and a viewer truncating a field mid-quote desyncs the CSV parser
  for every row after it. Membership now lives in a new normalized join
  table, `segment_membership.csv` (`segment_id,export_run_id,is_seed`), one
  row per (segment, file) pair, written alongside the other two files by
  `build_segment_manifest.py`. `segment_manifest.csv` keeps only scalar
  summary fields (`file_count`, `has_seed_file`, `population_hash`);
  `run_registry.csv` keeps `population_hash` only — neither file will ever
  again carry a variable-length filename list. `population_hash` is computed
  identically to before (still from the in-memory `eids` list, not a
  round-trip through any CSV), so it is unchanged for any segment given the
  same file population — skip-logic/staleness comparisons are unaffected.
  `tools/run_segment_orchestrator.py` gains a `--membership-file` flag
  (default: sibling of `--manifest-file`) and now sources every
  `export_run_ids`/`allowed_ids` lookup from `segment_membership.csv` instead
  of the retired manifest column. `_build_registry()`'s `new_files`/
  `removed_files` staleness-reason diffing now reads the prior run's
  population from `existing_membership` (loaded from a prior
  `segment_membership.csv`) instead of an `export_run_ids` field embedded in
  the prior `run_registry.csv` row; a registry rebuilt for the first time
  after this migration (no prior `segment_membership.csv` on disk) will show
  every current file as `new_files` with no `removed_files` on that one
  transitional run, since there is no prior per-segment file list to diff
  against — a one-time artifact of the migration, not an ongoing dual-path.
  No fallback to the old inline column was kept; this is a schema change, not
  an additive one.

### Added
- `tools/compare_cross_segment.py` governance comparisons now fan out across
  enterprise/bc/client scope levels instead of routing everything through a
  single client-scoped grouping key. Scope level is derived per-row from
  which of `client_label`/`business_center_label` are populated (enterprise =
  neither; bc = business_center_label only; client = client_label only;
  project = both) — orthogonal to `governance_role`, and computed the same
  way for every comparison in this file.
  `discover_governance_chain()` gains four new directed pairwise comparison
  types, each an independent parallel edge (no fixed override precedence
  between enterprise/bc/client standards, since any one may or may not have
  adapted from another): `enterprise_to_project` (an enterprise-scoped
  Template/Container reaches every Project regardless of its own client/bc),
  `bc_to_project` (a bc-scoped Template/Container reaches only Projects in
  the same normalized business center), `enterprise_to_bc`, and
  `enterprise_to_client` (same-role, standard-to-standard — Template vs.
  Template or Container vs. Container, never mixed roles). All four are
  registered in `DIRECTED_TYPES` and `GOVERNANCE_STATE_DIRECTED_TYPES`;
  `enterprise_to_project`/`bc_to_project` are additionally registered in
  `DELTA_DIRECTED_TYPES` alongside `template_to_project`/`container_to_project`,
  since they are the same shape of comparison (standard reference vs. Project
  target) just at a different scope level. Generic/Generic-Host is
  deliberately excluded from this fan-out — it already pairs unconditionally
  against every Template/Container/Project via the pre-existing `generic_ids`
  loop, so a separate scope-scoped edge would be redundant.
  `run_pooled_comparison()` gains two new pool grains alongside the existing
  `(parent_segment_id, role, unit_system)` pool (now labeled `pool_scope=
  parent_sibling` in `cross_segment_pooled.csv`, a new column): `pool_scope=
  bc` pools `(business_center_label, role, unit_system)` ignoring
  client_label (whichever clients happen to have work in that bc), and
  `pool_scope=client` pools `(client_label, role, unit_system)` ignoring
  business_center_label (whichever bcs happen to have work for that client).
  These are genuinely different pools with different membership, not two
  views of the same pool — a segment can now appear in `cross_segment_pooled.csv`
  once per applicable pool grain. The per-pool containment/bundle computation
  itself is unchanged; it was extracted into a shared `_build_pooled_row()`
  helper so all three grains share one implementation.
- Segment staleness model extended (build_segment_manifest.py `_build_registry()`):
  `run_registry.csv` gains `export_run_ids` (persisted per-run member list, enabling
  next-run diffing) and `conformance_reference_mode` (currently always `"latest"` —
  compare_cross_segment.py always resolves reference segments dynamically against
  current output; a pinned/snapshot mode is deferred until Phase-2 baseline authority
  is established). When `population_hash` changes, the registry now records
  `new_files:<n>` and/or `removed_files:<n>` reason counts alongside the existing
  `population_changed` marker, diffed against the prior run's `export_run_ids`. A
  metadata edit that moves a file between segments (e.g. a corrected `client_label`)
  surfaces as `removed_files` on the old segment and `new_files` on the new one —
  no separate "metadata change" detection path was needed or added.
- `tools/run_segment_orchestrator.py --dry-run` now prints each pending segment's
  registry `notes` (the staleness reason) alongside its status.
- `tools/compare_cross_segment.py` now writes `comparison_registry.csv` after every
  run: one row per actually-recomputed (segment_a, segment_b, comparison_type,
  domain) work item, stamped with each side's `population_hash`/`last_run_utc`
  (read from `run_registry.csv`) and `computed_utc`. Keyed at the domain granularity
  matching `work_items`, not the coarser pair — a `--domain`-scoped invocation only
  recomputes one domain per pair, so stamping at pair granularity would silently
  mark every other domain on that pair "current" without having recomputed it,
  hiding real staleness from a later `--dry-run`. This is new tracking state
  only — comparisons are still always fully recomputed on every invocation; nothing
  is skipped based on this registry. `--dry-run` now looks up each discovered
  (pair, domain) work item against this registry and labels it `stale` (never
  computed, or either side's stamp has moved since — this is how a Template/
  Container reference re-running with new bundle output is surfaced as
  invalidating its downstream Project/Container comparisons, even though the
  target's own file population is unchanged) or `current`.

### Fixed
- `tools/compare_cross_segment.py` `discover_governance_chain()`'s four
  scope-level fan-out edges (`enterprise_to_project`, `bc_to_project`,
  `enterprise_to_bc`, `enterprise_to_client`) group purely by scope level,
  ignoring `parent_segment_id` — the same class of bug just fixed in
  `run_pooled_comparison()`'s bc/client pool grains, but in the pairwise
  path. Verified against a real corpus manifest: 14 of 139 new-type pairs
  were a segment paired against its own `parent_segment_id` ancestor or
  descendant (e.g. an enterprise-scoped Template paired against a
  bc-scoped Template nested directly under it), which would inflate
  containment toward a false 1.0 the same way. `_build_ancestor_map()` and
  a shared `_is_lineage_related()` helper (used by both this fix and the
  pooled-comparison one) now exclude any such pair from all four edges.
- `tools/compare_cross_segment.py` `run_pooled_comparison()`'s new `bc`/`client`
  pool grains ignore `parent_segment_id` for grouping, so a collection-blank
  BC roll-up and its own collection-specific child (or any ancestor/descendant
  pair sharing the same normalized bc/client value) could land in the same
  pool. Since segments are hierarchical cuts of the same underlying file
  population, an ancestor's data is always a superset of its descendants' —
  pooling them as peers compared a segment against a pool already containing
  its own data, inflating `all_containment_focal_in_pool`/
  `used_containment_focal_in_pool` toward a false 1.0. A new `_build_ancestor_map()`
  walks each segment's `parent_segment_id` chain once per invocation;
  `run_pooled_comparison()` now excludes any segment in the focal segment's
  own ancestor/descendant lineage from its pool, for all three pool_scope
  grains.
- `tools/compare_cross_segment.py` `build_explicit_matrix_outputs()`'s pool
  matrix (`project_pool_containment_similarity_matrix.csv`) keyed cells only
  as `row_id -> peer_pool:<row_id>` by view/domain, ignoring `pool_scope`.
  Since a project can now emit one pooled row per applicable `pool_scope`
  grain (`parent_sibling`/`bc`/`client`), different grains for the same
  project collided on identical matrix coordinates with different values.
  `column_id` now folds in `pool_scope` (`peer_pool:<pool_scope>:<row_id>`)
  so each grain gets its own cell.
- `tools/compare_cross_segment.py` `_normalize_bc_label()` only blanked empty
  strings and the `"0000"`/`"BC_0000"` bookkeeping tokens, dropping the
  `is_blank_or_na()` NA-token handling (`n/a`, `NA`, `__NOT_APPLICABLE__`, ...)
  that `discover_governance_chain()`'s `_key()` previously relied on for its
  `business_center_label` fallback. An NA-spelled bc was being treated as a
  real peer business center by `_bc_of()`, `_scope_level()` (misclassified as
  `"bc"` scope instead of `"enterprise"`), and the new bc-scoped
  pooling/pairwise comparisons. Restored the `is_blank_or_na()` check
  alongside the bookkeeping-token check.
- `tools/compare_cross_segment.py` `discover_governance_chain()`'s `_key()`
  business_center_label fallback (used when `client_label` is blank/NA) no
  longer treats bookkeeping tags `"0000"`/`"BC_0000"` (any case) as a real
  peer business center. Those values mean "enterprise work, tagged for
  bookkeeping," not a specific business center; grouping by the raw string
  would have silently pooled unrelated enterprise-wide rows together as if
  they shared one bc. The fallback now normalizes through the same
  `_bc_of()`/`_normalize_bc_label()` helpers the new enterprise/bc/client
  scope-level fan-out uses, falling through to the collection/blank
  fallbacks below when the tag normalizes to blank. Existing fixtures using
  `business_center_label="BC_0000"` alongside a populated `collection_label`
  are unaffected (they already grouped via collection_label, not
  business_center_label); a row with an unadorned `"0000"`/`"BC_0000"` and no
  collection_label now falls through to the blank-key fallback instead of
  pairing with unrelated same-tagged rows.
- `tools/compare_cross_segment.py` `comparison_registry.csv`: a (pair, domain) is now
  also omitted from the stamp if either side's `run_registry.csv` `status` is not
  `"complete"`. `build_segment_manifest.py` updates `population_hash` to a segment's
  new file population immediately on manifest rebuild, resetting `status` to
  `"pending"` (and clearing `last_run_utc`) until the orchestrator actually re-runs
  that segment — but its output folder on disk still holds the *old* population's
  results until then. A compare run in that window read the stale on-disk data yet
  got stamped with the segment's already-updated (new) `population_hash`; once the
  segment reached `"complete"` with that same hash, a later `--dry-run` would have
  wrongly reported the pair as already current.
- `tools/compare_cross_segment.py` `comparison_registry.csv`: removed the carryover of
  prior (pair, domain) entries not recomputed this run, and stopped stamping work
  items that produced no output. Every other output this tool writes
  (`cross_segment_summary.csv` etc.) is a full `atomic_write_csv` replace from only
  the current invocation's rows, never a merge — so a `--domain`/`--segment`-scoped
  run sharing the same `--out-dir` as an earlier full run already destroys those
  other domains'/pairs' output rows. Carrying their old `comparison_registry.csv`
  stamp forward falsely claimed that data was still current. The registry is now a
  full snapshot of only what this invocation actually produced: a scoped run leaves
  every non-recomputed (pair, domain) with no row at all (correctly staleness-flagged
  on the next `--dry-run`), and a work item where `run_pair()`/`_run_pair_domain()`
  returned `None` with no governance-state rows either (e.g. below `--min-patterns`,
  or a within-project pair with no eligible file pairs) is omitted rather than
  stamped current for output that was never written.
- `build_segment_manifest.py` `_build_registry()`: the new `new_files`/`removed_files`
  reason diff reused the name `new_ids` for the per-segment export_run_id diff,
  shadowing the outer `new_ids` (the full eligible segment_id set) used later to
  compute `dropped_ids`. Any retained segment whose population changed left
  `new_ids` holding export_run_ids instead of segment_ids, so every other
  still-present segment was reported as removed from the registry with a false
  "review corresponding folders for manual cleanup" warning. Renamed the
  per-segment locals to `old_export_ids`/`new_export_ids` so they no longer
  collide with the outer set.
- line_patterns sig_hash policy corrected to segments_def_hash (sig_hash.v2):
  segments_norm_hash was incorrectly used as sig_hash basis — it belongs in join_hash only.
  sig_hash answers exact identity (scale variants are distinct records);
  join_hash answers governance equivalence (scale variants collapse to one pattern).
  Cross-domain reference joins (obj_style.pattern_ref.sig_hash, line_style.pattern_ref.sig_hash)
  were broken while norm_hash was in sig_hash; this change restores them.
  Downstream: re-run sig_hash stage only. Bundle/pattern/segment pipelines unaffected
  (they operate on join_hash exclusively).

### Added
- `instance_count` and `is_sole_type_in_category` metadata fields added to
  `text_types`, `dimension_types` (all 7 splits), `arrowheads`, and `compound_types`
  (all 4 partitions — wall, floor, roof, ceiling). Both fields are additive metadata
  only — never in sig_hash, join_key, or identity_basis.items. Arrowheads emit
  `instance_count = None / not_applicable` (tick-mark reverse-lookup deferred).
  Enables compound placeholder condition `is_purgeable OR (is_sole_type_in_category
  AND instance_count == 0)` at pipeline/BI layer.

### Fixed
- `view_filter_applications_view_templates` and `view_templates`: `GetIsFilterEnabled`
  now captured alongside `GetFilterVisibility` — a toggled-off filter is now
  distinguishable from a visible one.
- `view_category_overrides_model` / `view_category_overrides_annotation`:
  `GetCategoryHidden()` captured per category via `_category_hidden_item()` — template-
  hidden categories are now reflected in the override record.
- `object_styles_model`: material resolved to name + class hash (`obj_style.material_sig_hash`)
  via `_material_ref_item()`, not raw ElementId — cross-project stable.
- `identity`: `doc.IsWorkshared`, `app.VersionNumber`, `app.VersionName`,
  `app.VersionBuild` all captured and emitted.
- `view_templates`: `GetWorksetVisibility()` captured per user workset via
  `_append_workset_visibility()`.

### Changed
- `units` domain expanded from 3 specs (length/area/volume) to 38 specs covering
  all Revit disciplines. Common additions: angle, slope, speed, time, mass_density,
  currency, rotation_angle, distance. Electrical: 7 specs. HVAC: 8 specs. Piping:
  5 specs. Structural: 7 specs. All specs are extracted for every document regardless
  of discipline — GetFormatOptions returns a valid FormatOptions object for all
  SpecTypeId specs on any live document. ITEM_Q_UNREADABLE paths are defensive
  fallbacks only and are not expected to fire in normal execution. SpecTypeId nested
  attribute paths are resolved once at function entry and filtered before the loop —
  unresolvable paths are skipped cleanly without blocking the domain. Attribute names
  verified against probe_units_2026-02-04.json.
- units domain SpecTypeId access corrected to Python flat top-level members for Electrical/HVAC/Piping/Structural entries (instead of nested C#-style paths), enabling those discipline records to resolve and emit.
- file_metadata.csv: `project_label` now extracted from Autodesk Docs:// central path
  (ACC projects only); blank for non-ACC paths
- line_patterns join key policy upgraded from `line_patterns.join_key.v2`
  (`line_pattern.segments_def_hash`) to `line_patterns.join_key.v3`
  (`line_pattern.segments_norm_hash`) to enforce scale-invariant structural identity;
  same kind sequence + ratio now collapses length-scaled variants into one pattern
- Bundle analysis `bundle_id` stability explicitly scopes hash identity to
  `(domain, scope_key, sorted_pattern_ids)`; identical pattern sets in different
  scope keys (for example `dimension_types` linear vs angular) intentionally
  receive different bundle IDs and are not cross-scope comparable
- `line_pattern.segments_norm_hash` is now computed automatically during flatten
  in `tools/run_extract_all.py` (no `--synthetic-domains line_patterns` flag required)
- line_patterns normalized token precision set to `.6f` (from `.9f`) after
  sensitivity sweep; decision now includes a documented ±2 decimal neighbor
  validation practice to confirm elbow stability over time
- view_category_overrides split into `view_category_overrides_model` and
  `view_category_overrides_annotation` partitions; `vco.include_controlled`
  coordination item removed; include state now sourced from
  `view_templates.include_vg_model` / `view_templates.include_vg_annotation`
- view_templates V/G include surface changed from a single `include_vg` flag
  to per-tab flags: `include_vg_model`, `include_vg_annotation`,
  `include_vg_analytical`

### Fixed
- file_metadata.csv: re-running the pipeline now preserves existing non-empty
  `client_label` and `governance_role` values by `export_run_id`
- VCO `dflt_map` computation hoisted out of O(templates × categories) inner loop;
  `other_seconds` reduced from ~920s to ~9s on large files, total VCO time reduced ~73%
- FEC cache deduplication: all `(doc, View, instances)` collection sites normalized to
  `_VIEW_INSTANCES_CACHE_KEY`; redundant FEC calls reduced from 12 to 7 per run
- View instances cache pre-warm repositioned before `view_filter_applications_view_templates`,
  ensuring the cache is populated before any view-related domain runs
- `_timing` scope resolved via injection pattern (`run_fingerprint(doc, timing=None)`);
  timing report merge restored to correct location inside `run_fingerprint()`

### Added
- file_metadata.csv: added `client_label` and `governance_role` columns
  (empty strings, manually curated)
- `TimingCollector.record_elapsed()` for hot-loop accumulation without per-iteration
  lock overhead
- VCO inner loop sub-timers: `vco.enumerate_categories`, `vco.get_param_ids`,
  `vco.get_category_overrides`, `vco.extract_graphics` — `other_seconds` is now
  attributable residual Python overhead rather than a black hole
- `total_serialization` and `total_run` timer scaffolding in runner; both correctly
  report 0.0 in written fingerprint (ordering constraint — captured in Dynamo summary
  surface instead)

---

### Changed (hash-breaking — full re-extraction required)
**Domain family splits (D-015):**
- `dimension_types` split into 7 domains: `dimension_types_linear`
  (Linear/LinearFixed/Angular/ArcLength), `dimension_types_angular`,
  `dimension_types_radial`, `dimension_types_diameter`,
  `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  `dimension_types_spot_slope`
- `object_styles` split into 4 domains by CategoryType tab:
  `object_styles_model`, `object_styles_annotation`,
  `object_styles_analytical`, `object_styles_imported`
- `fill_patterns` split into 2 domains by target:
  `fill_patterns_drafting`, `fill_patterns_model`. Solid fills
  (system defaults) excluded from both domains.
- `view_templates` split into 5 domains by ViewType group:
  `view_templates_floor_structural_area_plans`,
  `view_templates_ceiling_plans`,
  `view_templates_elevations_sections_detail`,
  `view_templates_renderings_drafting`,
  `view_templates_schedules`

**Arrowhead record class corrections:**
- Dot, Diagonal, Box, Loop, Elevation Target, Datum triangle record
  classes corrected to size-only (tick_size_in only). Previous hashes
  for these styles incorrectly included tick_mark_centered and
  heavy_end_pen_weight.

**object_styles join-key correction:**
- pattern_ref.kind record class gate removed. Was incorrect —
  pattern_ref.sig_hash moves to optional_items.

**Dimension type policy corrections:**
- Angular: witness_line_control added to required identity
  (confirmed active in UI for Angular, not previously included)
- Radial: radius_symbol_location and radius_symbol_text added
- Diameter: diameter_symbol_location and diameter_symbol_text added
- Spot families: shape-specific indicator and placement fields added

**System type exclusion:**
- Dimension type extractors now exclude system built-in types not
  accessible in the Revit UI (detected via id-based label fallback
  and family name gate). These types cannot be governed.
- Arrowhead extractor now excludes placeholder_missing records
  (unidentifiable system types).
- Domain routing bugs fixed: DiameterLinked/Alignment Station Labels
  excluded from dimension_types_diameter; Diameter types with
  SpotElevationFixed shape enum correctly routed to diameter domain
  via family name gate.

### Added
- `policies/cross_domain_alignment_keys.json` — domain family registry
  and alignment key definitions
- `arrowhead.record_class` in coordination_items for all arrowhead records
- `lp.is_import` in coordination_items for line_patterns records
- `dim_type.domain_family` in coordination_items for all dimension type records
- `obj_style.category_type`, `obj_style.domain_family`, `obj_style.is_subcategory`
  in coordination_items for all object style records
- `vt.view_type_family`, `vt.view_type_raw` in coordination_items for all
  view template records
- `object_styles_annotation` now populates
  `ctx["object_style_annotation_row_key_to_sig_hash"]` for VCO baseline lookup
- View category overrides: `vco.include_controlled`, `vco.vg_category_type`,
  `vco.context_type` added to coordination_items (D-016)
- View category overrides: category 2 (latent overrides, V/G checkbox unchecked)
  now captured alongside category 1

### Decisions captured
- D-015: Domain family architecture — split criteria, vocabulary, alignment key
  registry
- D-016: VCO scope — category 1 (template-controlled) and category 2 (latent)
  implemented; category 3 (view-local) deferred with hooks

---

### Changed (D-015 — Domain Family Split Architecture)

Domain scope redefined: four monolithic extractors split into 18 per-partition domains.
No hash values changed within any record class — this is a structural change only.

- **`object_styles`** split into `object_styles_model`, `object_styles_annotation`,
  `object_styles_analytical`, `object_styles_imported` — each covers one CategoryType.
  `require_domain` references updated to split names throughout.

- **`fill_patterns`** split into `fill_patterns_drafting`, `fill_patterns_model` —
  each covers one FillPatternTarget. Join-key policy updated to use `fill_pattern.target`
  (was `fill_pattern.target_id`) and `fill_pattern.grid_count` as co-required keys.

- **`dimension_types`** split into 7 per-shape domains (`dimension_types_linear`,
  `dimension_types_angular`, `dimension_types_radial`, `dimension_types_diameter`,
  `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  `dimension_types_spot_slope`). Shape discrimination now happens at domain-level
  (handled shapes frozenset). Shared helpers moved to `core/dimension_type_helpers.py`.

- **`view_templates`** split into 5 per-ViewType-family domains
  (`view_templates_floor_structural_area_plans`, `view_templates_ceiling_plans`,
  `view_templates_elevations_sections_detail`, `view_templates_renderings_drafting`,
  `view_templates_schedules`). Shared VG helpers in `core/vg_sig.py`.

- Dependency chain (`require_domain` calls) updated in `view_category_overrides`
  and runner to reference split domain names.

- Join-key policies updated: all split domains have flat per-domain policies.
  Arrowheads policy corrected: shape-gated keys moved from `explicitly_excluded_items`
  to `optional_items` to satisfy A3 validation rule.

---

### Removed
- Legacy hash infrastructure (pipe-delimited signatures) removed across all domains
- `REVIT_FINGERPRINT_HASH_MODE` environment variable (semantic mode now default and only mode)
- `domains/view_filters_deprecated.py` (unused, 741 lines)
- `core/canon.py`: deprecated `sig_val()` helper
- Phase-2 `semantic_keys` duplication in domain payloads
- Legacy context maps: `*_uid_to_hash_v2` (replaced by canonical `*_uid_to_hash`)

### Changed
- All domains now emit only `hash_v2` as the canonical domain hash in runner contract output
- Context maps simplified: removed `_v2` suffix from semantic hash maps
- Contract building simplified: single semantic hash source instead of mode-dependent logic

### Added
- Root governance docs: `INVARIANTS.md`, `ARCHITECTURE.md`, `DECISIONS.md`.
- **NEW DOMAINS (M4):**
  - `view_filter_definitions` - Global domain capturing filter definitions (rules, categories)
  - `phases` - Global domain capturing phase inventory and sequence (names included in hash per D-010 revised)
  - `phase_filters` - Global domain capturing phase filter settings (New/Existing/Demolished/Temporary visibility)
  - `phase_graphics` - Global domain capturing phase graphic override settings (disabled per D-013)
- Context dictionary (`ctx`) now populated by global domains:
  - `filter_uid_to_hash` - Mapping of view filter UIDs to definition hashes
  - `phase_uid_to_hash` - Mapping of phase UIDs to definition hashes
  - `phase_filter_uid_to_hash` - Mapping of phase filter UIDs to definition hashes
- **Canonical evidence selectors (PRs #106–#119):** All 15 domain extractors migrated to policy-driven join-key and sig-hash composition via `build_join_key_from_policy()`. Each domain now emits `join_key`, `join_hash`, and `sig_basis` fields in records, derived from `identity_basis.items` per the join-key policy.
- **Element traceability (PR #126):** `source_element_id` and `source_unique_id` added to `phase2.unknown_items` across all element-backed domains.
- **Timing instrumentation (PR #127):** `core/timing_collector.py` added for extraction profiling. Runner emits `timings.json` sibling artifact.

### Changed
- **BREAKING: View Templates (M5):** Moved from name-only presence hashing to behavior-based hashing
  - Template identity: Now uses UniqueId (was: name)
  - Template hash: Now derived from controlled behavior (was: name presence)
  - Behavioral inputs: view type, detail level, scale, discipline, phase, phase filter, view filters (ordered), display style
  - Names: Now metadata-only (excluded from hash per D-008)
  - Filter stack: Order-sensitive (preserved)
  - References global domains: filters, phases, phase_filters via context
  - record_rows emitted with per-template sig_hash
- Execution order now enforces dependency: global domains run before contextual domains.
- **record_id stabilization (PR #123):** `record_id` generation made deterministic across runs using domain + identity_basis hash.
- **Join-key deduplication (PR #125):** `join_key.items` no longer duplicates `k/q/v` triples already present in `identity_basis.items`; join_key references the canonical source.
- **Object_styles shape-gating (PR #124):** Join-key policy uses `obj_style.pattern_ref.kind` as discriminator; `ref` shape requires `pattern_ref.sig_hash`, `solid` shape does not.

### Semantic Rules Applied
- **View Filters:** Filter rules are order-sensitive (preserved), categories are sorted
- **Phases:** Phase names are included in behavioral hashes for cross-project comparability (D-010 revised), sequence number captured where available
- **Phase Filters:** Settings are order-insensitive (sorted before hashing)
- **Phase Graphics:** Intentionally disabled — API does not expose graphic overrides (D-013)
- **View Templates (M5):**
  - Template names: metadata-only (per D-008)
  - Filter stack: order-sensitive (filter application order matters)
  - Other settings: order-insensitive (sorted)
  - Global references: uses hashes from filters/phases/phase_filters domains
  - Unreadable templates: fail-soft with explicit markers

### Decisions captured
- Nested fenced code blocks are prohibited in documentation (portability rule).
- View filters are global definitions referenced by views and view templates.
- Phase filters and phase graphic overrides are global.
- Phase names ARE included in behavioral hashes (D-010 revised for cross-project comparability).
- Phase sequence number is included in phase signatures to capture ordering.
- Hash mode migration timeline completed (D-014).

---

## 2025-12-17

### Added
- Documented invariants: deterministic hashes, behavior-first, fail-soft, explicit ordering rules.
- Documented architecture layering: core / domains / context / runner.
- Documented decision log to prevent drift and re-litigation.

### Fixed
- Documentation formatting made portable across GitHub Mobile + Obsidian by avoiding nested fenced blocks.
