# Phase 0–6: Questions Each Phase Can Answer

This document is a **reference map** of the kinds of questions each phase in the
Revit Fingerprint / standards analysis pipeline is designed to answer.

The phases are cumulative: later phases assume earlier ones are trustworthy.

---

## Phase 0 — Inventory & Observability  
**“What exists?”**

These are **descriptive, non-judgmental** questions.

- What objects / records are present in this domain?
- How many instances exist per file?
- What fields are available?
- Are exports complete and structurally valid?
- Are there obvious data gaps or null-heavy fields?
- Can we reliably re-run and get the same raw capture?

**Outputs**
- Raw inventories
- Counts
- Structural diagnostics

---

## Phase 1 — Baseline Gravity & Population Shape  
**“What is common?”**

These questions are about **frequency and gravity**, not correctness.

- Which items appear most frequently across projects?
- Is there a visible baseline (template gravity)?
- Are there multiple competing baselines?
- How skewed or flat is the population distribution?
- Do projects cluster around certain variants?

**Outputs**
- Frequency tables
- Baseline vs population comparisons
- Gravity charts

---

## Phase 2 — Identity & Drift Explanation  
**“What changed, and how?”**

This is where **identity is made explicit** and drift becomes explainable.

- What is the correct join key for this domain?
- Which records are:
  - added
  - removed
  - modified
- Are changes additive, divergent, or mutative?
- Do identical identities have multiple semantic meanings?
- Is non-convergence real or an artifact of identity definition?
- Does slicing (family, lineage, cohort) change conclusions?

**Outputs**
- Join-key design (often via Pareto tradeoffs)
- Drift classification
- Collision examples with field-level diffs
- Core vs optional sets

---

## Phase 3 — Normative Interpretation  
**“Should this drift exist?”**

These questions introduce **standards intent and judgment**, backed by Phase-2 evidence.

- Which differences are violations vs acceptable variance?
- Are some fields implicitly optional?
- Are multiple norms in use (regional, disciplinary, lineage-based)?
- Is the standard underspecified or outdated?
- Are we mistaking flexibility for non-compliance?

**Outputs**
- Allowed / discouraged / prohibited classifications
- Clarified standards intent
- Identification of false drift

---

## Phase 4 — Causal Analysis  
**“Why did this drift happen?”**

These questions look for **systemic causes**, not blame.

- Did drift originate from:
  - template gaps?
  - Revit defaults?
  - copy-paste workflows?
  - training or tooling friction?
- Does drift correlate with:
  - office or region?
  - project size or phase?
  - template version?
  - time since project start?
- Is drift a rational response to constraints?

**Outputs**
- Root-cause hypotheses
- Evidence-backed causal narratives
- Identification of structural pressure points

---

## Phase 5 — Intervention Design  
**“What should we change?”**

These questions are about **leverage and impact**.

- Should we change:
  - templates?
  - defaults?
  - documentation?
  - validation rules?
  - automation?
- Which fields should be:
  - locked?
  - advisory?
  - removed from standards?
- What intervention yields the most benefit with least disruption?
- What should be fixed automatically vs manually?

**Outputs**
- Targeted standards updates
- Tooling or Guardian rules
- Justified change proposals

---

## Phase 6 — Strategic & Predictive Insight  
**“Where does this lead?”**

These questions are **forward-looking**.

- Are we converging, stabilizing, or fragmenting over time?
- Which domains are approaching instability?
- What is the cost of drift vs cost of enforcement?
- Which standards are safe to leave alone?
- Where should governance attention go next?
- What happens if we do nothing?

**Outputs**
- Early warning indicators
- Trend analysis
- Long-term standards strategy

---

## One-Line Summary

> **Phase 0–2 make drift observable and explainable;  
> Phase 3–5 decide what it means and what to do;  
> Phase 6 asks where the system is headed.**

---

## Governance Note

Skipping phases collapses rigor:
- Without Phase 2, Phase 3 is opinion
- Without Phase 4, Phase 5 over-corrects
- Without Phase 6, governance stays reactive

This ladder exists to prevent that.
