# Governance Classification Rules

`classification_rules_version: 0.1`

This is a **stable, package-type-level reference** for a
`revit_fingerprint_governance` evidence package, describing the branch order
and exception conditions of `tools/generate_governance_narrative.py`'s core
classification functions. It is not regenerated per run.

`policies/governance/governance_thresholds.json` and
`policies/governance/anomaly_thresholds.json` hold the *values* a threshold
key resolves to for a given run; this document holds the *order and
exception logic* those values are evaluated in. Together, the two let a
reader recreate a tier or anomaly-note output from
`governance_domain_summary.csv`'s inputs instead of rediscovering the logic
from Python source. See `docs/governance/governance_interpretation_guide.md` for what
each field/tier *means*; this document is about evaluation order, not
semantics.

**This is a legibility aid, not a source of truth.** It is hand-maintained
prose describing Python control flow — not mechanically verified against the
functions it describes. A future change to one of these functions' branch
order could make this document stale without anything failing. See
`DECISIONS.md` D-029's Consequences for the known-limitation tracking this.
Threshold keys are named below exactly as they appear in the two JSON
profiles; values are deliberately not restated here.

---

## `score_reliability()`

Classifies how trustworthy a domain's mean cascade score is, from the
within-project `p10`/`p90` spread (`wp_p10`, `wp_p90`, `wp_all` — mean). All
keys below are from `governance_thresholds.json`.

Evaluated in this order; the first matching branch wins:

1. **Unknown** — `p10` or `p90` is missing. No further branches evaluated.
2. **Tight** — `p10 >= reliability_tight_p10`.
3. **Convergent** — `p10 >= reliability_convergent_p10` AND
   `(p90 - p10) < reliability_convergent_spread_max`.
4. **Presence-based** — `p10 < reliability_low_p10_max` AND
   `p90 >= reliability_presence_p90_min`.
5. **Sparse** — `p10 < reliability_low_p10_max` AND
   (`mean` is missing OR `mean < reliability_sparse_mean_max`).
6. **Fallback: Convergent** — none of the above matched (a moderate p10 with
   a moderate-to-large spread). Not a distinct named tier; the same
   `RELIABILITY_CONVERGENT` value as branch 3, reached by exhaustion rather
   than an explicit spread check.

## `assign_tier()`

Assigns one of ten `governance_tier` values from the cascade fields
(`tc`/`cp`/`tp`) plus, when supplied, explicit governance-state fields
(`local_active_share`, `provided_passive_share`, `provided_missing_share`,
`provided_to_used_containment`). `primary` is `tp` if present, else `cp`.

Evaluated in this order; the first matching branch wins:

1. **No primary evidence.** `primary` is `None` (both `tp` and `cp` absent).
   - If `_has_group1_bc_pooled_evidence()` finds a `bc::bc` pooled value in
     `tp_by_scope`/`cp_by_scope` → **Insufficient Evidence — Enterprise;
     BC-Level Evidence Available**.
   - Otherwise → **Insufficient Evidence**. No further branches evaluated
     either way — this is a presence gate, not a score comparison.
2. **Sparse/presence-limited primary.** `score_reliability() == Sparse` AND
   `primary < tier_sparse_primary_max` → **Sparse / Presence-Limited**.
3. **Material local-active override.** `local_active_share >=
   local_active_material_threshold` AND `primary <
   tier_active_local_primary_max` → **Active Local Practice Review**. (A
   *high* `primary` with material local-active share is NOT caught here —
   see branch 4's own local-active check, which applies at a lower score
   band with a different, non-overriding role.)
4. **Strong primary.** `primary >= tier_strong_baseline_min`:
   - `_has_material_state_exception()` true (any of
     `local_active_share`/`provided_passive_share`/`provided_missing_share`
     over its own `governance_thresholds.json` threshold) → **Baseline
     Candidate — Local/Use Review**.
   - Else if `provided_to_used_containment` is present and
     `< active_use_min_for_strong_baseline` → **Baseline Candidate —
     Local/Use Review**.
   - Else if `tc < tier_container_gap_tc_max` → **Baseline Candidate —
     Container Gap**.
   - Else → **Strong Baseline Candidate**.
5. **Investigate-band primary.** `primary >= tier_investigate_min`:
   - `_has_material_state_exception()` true → **Baseline Candidate —
     Local/Use Review**.
   - Else → **Investigate Before Baseline** (reached identically whether or
     not `score_reliability()` is `Presence-based`/`Sparse` — the reliability
     check here is dead in the sense that both its branches return the same
     tier; reliability still shows up separately in
     `governance_domain_summary.csv`'s own column).
6. **Moderate-variation-band primary.** `primary >= tier_moderate_variation_min`:
   - `local_active_share >= local_active_material_threshold` → **Active
     Local Practice Review**.
   - Else if `score_reliability() == Sparse` → **Sparse / Presence-Limited**.
   - Else → **Moderate Variation**.
7. **Below every band.** → **High Fragmentation**.

## `detect_anomalies()`

Returns a list of prose notes appended to `governance_domain_summary.csv`'s
`notable_anomalies` column. Unlike `assign_tier()`, this is **not** a
first-match-wins dispatch — every applicable check runs and every note that
matches gets appended, in this fixed order:

1. **Reliability note** — `score_reliability() == Presence-based` or
   `Sparse` appends the matching `RELIABILITY_DESCRIPTIONS` text (mutually
   exclusive with each other, but independent of everything below).
2. **State-based checks** (only when `state` is supplied — all keys from
   `anomaly_thresholds.json`/`governance_thresholds.json`, evaluated
   independently, not first-match-wins):
   - `provided_to_configured_containment >= provided_carried_downstream_min`
     AND `provided_to_used_containment < provided_active_use_max` → carried-
     but-not-active note.
   - `provided_passive_share >= passive_material_threshold` → material-
     passive note.
   - `provided_missing_share >= missing_material_threshold` → material-
     missing note.
   - `local_active_share >= local_active_material_threshold` → material-
     local-active note.
3. **Bundle/passive-indicator fallback** (only when `state` is absent — the
   dual-schema and single-schema branches are mutually exclusive with each
   other, gated on `bundle_schema`):
   - `bundle_schema == "dual"`: `passive_indicator >=
     passive_indicator_high_min` → high-passive note; else `>=
     passive_indicator_moderate_min` → moderate-passive note.
   - `bundle_schema == "single"`: domain in `PASSIVE_INHERITANCE_RISK_DOMAINS`
     AND `bundle_share_all < passive_inheritance_risk_bundle_share_max` →
     low-bundle-density-risk note; else `bundle_share_all <
     bundle_share_very_low_max` → very-low-bundle-density note.
4. **Group 2 (generic→template) gap** — `gt >= gt_tp_gap_gt_min` AND
   `tp < gt_tp_gap_tp_max` → baseline-reaching-templates-not-projects note.
5. **Group 2 scope-breakdown divergence** — for each of
   Generic→Template/Container/Project: if an enterprise-level value and a
   non-empty `*_by_scope` both exist, `abs(enterprise - scoped_mean) >=
   group2_scope_divergence_gap_min` appends a note (up to 3 notes, one per
   cascade stage).
6. **Group 1 by-scope intra-bucket divergence** — for each of
   Template→Container/Container→Project/Template→Project's
   `*_by_scope_spread`: for every non-enterprise `scope_pair`, `(hi - lo) >=
   group1_scope_spread_gap_min` appends a note (unbounded count — one per
   qualifying scope_pair per cascade stage).
7. **tp bypasses tc** — `tp > tc + tp_tc_bypass_gap_min` → direct-inheritance
   note.
8. **Weak tc** — `tc < weak_tc_max` → weak-template-to-container note.
9. **Weak cp** — `cp < weak_cp_max` → weak-container-to-project note.
10. **Strong cross-client convergence** — `xc >=
    cross_client_convergence_strong` → convergence note.
11. **Low cross-client convergence despite template floor** — `xc <
    cross_client_convergence_low` AND `tp > cross_client_low_tp_min` →
    client-specific-addition note.
12. **View-template zero-discipline** — only when `"view_template" in dom`:
    any per-discipline `wp_disc` value `< view_template_zero_discipline_max`
    (excluding the `"all"` key) → discipline-specific-governance note,
    naming every qualifying discipline.
13. **Phases extension** — only when `dom == "phases"` and domain guidance
    text exists: `tp < phases_tp_extension_max` AND `tw >
    phases_tw_min` → the `DOMAIN_GUIDANCE["phases"]` text.
14. **Static per-domain guidance** — `dom == "loaded_family_types"` and
    guidance text exists → the `DOMAIN_GUIDANCE["loaded_family_types"]` text,
    unconditionally (no threshold gate).
15. **Union-inventory-derived domain confidence** (only when `union_breadth`
    is supplied — a per-domain summary from `build_union_breadth_by_domain()`,
    itself only computed when the separate `--union-inventory` argument
    (`cross_segment_union_inventory.csv`) is supplied; D-033 — this is
    independent of `--comparison-registry`, not a sibling of it). `primary` here
    is `tp` if present, else `cp`. Mutually exclusive (`if`/`elif`):
    - **Broad natural reuse, weak cascade** — `corpus_wide + client_wide
      pattern count >= union_breadth_broad_min_patterns` AND `primary <
      union_breadth_weak_cascade_max` → natural-standard-candidate note (the
      cascade metrics alone would miss this domain).
    - **Narrow natural reuse, strong cascade** — `file_level pattern count /
      total >= union_breadth_narrow_file_level_share_min` AND `primary >=
      union_breadth_strong_cascade_min` → fragile-formal-propagation note.
    - A domain whose `primary` falls between `union_breadth_weak_cascade_max`
      and `union_breadth_strong_cascade_min`, or whose breadth is otherwise
      unremarkable, triggers neither note — this is a deliberate scope limit
      (see `docs/governance_generator_cross_compare_coverage.md`'s own
      guardrail against a per-domain dump of raw breadth numbers), not a gap.

`render_findings_and_recommendations()` independently re-evaluates only
finding 13 above (the phases check), reading the *same*
`phases_tp_extension_max`/`phases_tw_min` keys from
`anomaly_thresholds.json` — see D-029. This is intentional duplication of
the check (two different rendered surfaces — the CSV anomaly note and the
narrative's "What needs attention" section — for the same governance
question), not a second, independently-tunable threshold.

## `build_governance_state_summary()` — `primary_governance_read` selection

Each `(domain, comparison_type)` bucket's `primary_governance_read` label is
computed by `_finalize_state_bucket()`, evaluated first-match-wins over the
bucket's aggregated shares:

1. `provided_to_used_containment >= primary_read_active_use_min`
   (`anomaly_thresholds.json`) → **"Provided standard is actively used."**
2. Else `provided_passive_share >= passive_material_threshold`
   (`governance_thresholds.json`) → **"Provided standard is carried but
   partly passive."**
3. Else `local_active_share >= local_active_material_threshold`
   (`governance_thresholds.json`) → **"Active local practice may need
   roll-up review."**
4. Else `provided_missing_share >= missing_material_threshold`
   (`governance_thresholds.json`) → **"Provided content is missing
   downstream."**
5. Else → **"State signal available; no dominant exception pattern."**

Note the threshold-family split: branch 1's key lives in
`anomaly_thresholds.json` (it gates a narrative-read *label*, the same
family as `detect_anomalies()`'s other materiality checks), while branches
2–4 read the *same* `governance_thresholds.json` keys `assign_tier()`'s
`_has_material_state_exception()` uses — so a `governance_thresholds.json`
edit to `passive_material_threshold`/`local_active_material_threshold`/
`missing_material_threshold` changes both the tier and this narrative read
together, by construction, not by coincidence.

## `_passive_inheritance_risk_domains()`

Returns the list of domains (restricted to `PASSIVE_INHERITANCE_RISK_DOMAINS`
and passing `_has_renderable_cascade_signal()`) driving the
`passive_inheritance_risk` finding in `governance_findings.json`. Mirrors
`detect_anomalies()`'s own state/bundle-fallback gating (see that function's
docstring), evaluated per domain:

1. **Explicit state present** (`state_summary.get(dom)` is truthy):
   `provided_passive_share >= passive_material_threshold`
   (`governance_thresholds.json`) → flagged. The bundle fallback below is
   never consulted for this domain, even if this check does not fire.
2. **No explicit state, `bundle_schema == "dual"`:**
   `passive_indicator >= passive_material_threshold`
   (`governance_thresholds.json` — the *same* key branch 1 uses, closing a
   D-029 drift gap where this branch previously read its own independent
   `0.20` literal) → flagged.
3. **No explicit state, `bundle_schema == "single"`:**
   `bundle_share_all < passive_inheritance_risk_bundle_share_max`
   (`anomaly_thresholds.json` — the same key `detect_anomalies()`'s
   single-schema risk-group check uses) → flagged.
4. Any other `bundle_schema` value, or no explicit state and no bundle
   data → not flagged.

---

## Known limitation

This document is hand-maintained and not mechanically checked against the
functions it describes — see `DECISIONS.md` D-029's Consequences. A future
regression test asserting specific documented example inputs produce the
documented example outputs (without executing this file line-by-line as a
spec) would catch drift; it does not exist yet.
