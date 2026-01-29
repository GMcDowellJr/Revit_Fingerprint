# Phase 2 — Join‑Key Discovery (Summary)

**Purpose**
Phase 2 establishes *global, cross‑project join keys* for Revit standards domains using evidence‑based discovery. The goal is not to invent keys at export time, but to **discover, validate, and pin** join‑key policies that are stable, explainable, and reproducible across projects.

This phase answers a single question:

> *Given exported semantic records, which observable attributes minimally and sufficiently explain semantic identity across the population?*

---

## Guiding Principles

1. **Discovery ≠ Production**
   Join keys are discovered offline (population‑aware), then consumed deterministically by the exporter via a pinned policy. They are not inferred per run.

2. **Global Consistency**
   The join‑key *rule* must be the same across all projects. Piecewise logic is allowed when justified (e.g., shape‑gated), but the policy is global.

3. **Definition‑Based Identity**
   Prefer definition/behavioral parameters over names, labels, or UIDs. UID‑ or name‑based identity breaks cross‑project comparability.

4. **Minimality with Evidence**
   Keys should be as small as possible while meeting acceptance thresholds. Residual ambiguity is acceptable only when quantified.

5. **No Fragmentation**
   Join keys must not split a single semantic identity. Fragmentation must be zero (or explicitly justified).

---

## Methodology (Repeatable Loop)

For each domain:

1. **Export Facts**
   Exporter emits `records` (one per semantic record) and `identity_items` (k/v pairs). UIDs are normalized out upstream.

2. **Atomic Screening**
   Evaluate single‑key discriminative power to shortlist candidates.

3. **Composite Evaluation**
   Build per‑record composite keys from candidate subsets and measure:

   * `max_sigcnt` (worst ambiguity)
   * `collision_records`
   * `collision_groups`
   * `fragmentation` (must be 0)

4. **Pareto Discovery**
   Search the subset space (bounded) to identify non‑dominated solutions. Use sampling for discovery when populations are large; validate finalists on full data.

5. **Policy Freeze & Verification**
   Select a policy at the Pareto knee (or exact identity when required), version it, and verify against the full population.

---

## Domain Outcomes

### 1) `dimension_types`

**Archetype:** Shape‑gated parametric identity

* **Finding:** No single global key suffices. Identity is conditional on `shape`.
* **Base Key:** `dim_type.accuracy`
* **Shape‑Specific Extensions:** Discovered via per‑shape Pareto (e.g., witness controls, unit formatting, prefixes/suffixes).
* **Conclusion:** Shape is a *classifier*, not identity. A global policy with shape‑gated extensions is required.

---

### 2) `line_patterns`

**Archetype:** Structural sequence identity

* **Finding:** Identity is defined by the ordered segment sequence, not name/UID.
* **Minimum Effective Key:**

  * `line_pattern.segment_count`
  * `(seg[i].kind, seg[i].length)` for initial segments
* **Residual Risk:** Prefix collisions if deeper segments differ.
* **Policy Choice:** Either accept quantified ambiguity at a cutoff, or move to a full structural signature.
* **Note:** Strong candidate for exporter evolution to emit a single `segment_signature`.

---

### 3) `line_styles`

**Archetype:** Parametric + reference identity

* **Finding:** Exact identity achievable without names/UIDs.
* **Effective Key:**

  * `line_style.color.rgb`
  * `line_style.weight.projection`
  * `line_style.pattern_ref.sig_hash`
  * `line_style.path` (category lineage)
* **Conclusion:** Definition‑based identity; `path` retained to scope category context.

---

### 4) `object_styles`

**Archetype:** Category‑scoped parametric identity

* **Finding:** Exact identity achievable; no conditional logic needed.
* **Effective Key:**

  * `object_style.path`
  * `object_style.color.rgb`
  * `object_style.weight.projection`
  * `object_style.weight.cut`
  * `object_style.line_pattern_ref.sig_hash`
* **Conclusion:** Object styles are rendering rules per category; names/UIDs excluded.

---

### 5) `units`

**Archetype:** Minimal parametric identity

* **Finding:** Immediate convergence; very low entropy.
* **Effective Key:**

  * `unit.spec`
  * `unit.type`
  * `unit.format.rounding`
  * `unit.format.accuracy`
* **Conclusion:** Phase‑2 complete with minimal policy.

---

## Acceptance Criteria

A join‑key policy is accepted when:

* `fragmentation_groups == 0`
* `max_sigcnt` meets the domain‑specific threshold (often 1)
* `collision_records` is quantified and deemed acceptable
* The key is definition‑based and globally applicable

---

## Architectural Decision

**Exporter Behavior**

* Exporter does **not** discover join keys.
* Exporter **consumes a pinned join‑key policy** (JSON) and emits deterministic join keys.

**Analysis Behavior**

* Pareto discovery and verification run offline, on demand.
* Policy updates are proposed, reviewed, versioned, and pinned (semver; GSR‑aware).

---

## Phase 2 Conclusion

Phase 2 establishes that join‑key identity is:

* **Domain‑specific**
* Sometimes **conditional**
* Often **structural or parametric**, not referential

The correct system design is therefore:

> *Discover identity offline → pin a policy → apply deterministically in production.*

With five core domains completed, Phase 2 is **complete**. Remaining domains (view templates, filters) require higher‑order graph reasoning and are deferred to Phase 3.

---

## Deliverables

* Versioned join‑key policy JSONs per domain
* Verification scripts and acceptance reports
* This summary as the Phase‑2 design record
