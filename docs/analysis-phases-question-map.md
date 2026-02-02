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

**Artifacts**
- records.csv
(file_id, domain, join_hash, sig_hash, status)
- identity_items.csv
(file_id, domain, join_hash, sig_hash?, k, q, v)
- label_components.csv (optional)
- status_reasons.csv (optional)
- Export diagnostics (row counts, null rates, schema version)

**Dashboards / Views**
- Inventory Overview
  - Records per domain
  - Records per file
- Data Quality Panel
  - Null-heavy fields
  - Missing join_hash / sig_hash
  - Status distribution

**Metrics / Signals**
- Record counts
- Field completeness %
- Re-run determinism checks (hash stability)
- Comparable eligibility rates

**Guardrails**
- No clustering
- No joins
- No “baseline” language

This phase is descriptive only.

---

## Phase 1 — Baseline Gravity & Population Shape  
**“What is common?”**

These questions are about **frequency and gravity**, not correctness. Phase 1 operates on frequency and overlap, not authoritative identity.

- Which items appear most frequently across projects?
- Is there a visible baseline (template gravity)?
- Are there multiple competing baselines?
- How skewed or flat is the population distribution?
- Do projects cluster around certain variants?
- Do projects partially overlap in their element sets, even if full domains never match?

**Outputs**
- Frequency tables
- Baseline vs population comparisons
- Gravity charts

**Artifacts**

Derived in Power BI (or cached if large):
- domain_elements
(distinct file_id, domain, join_hash where comparable)
- DomainClusters
(domain, join_hash, cluster_members, cluster_share)
- (optional) DomainPairwiseSimilarity
(domain, file_a, file_b, jaccard, containment)

**Dashboards / Views**
- Domain Portfolio
  - One row per domain
- Population Shape
  - Cluster size distributions
- Similarity / Overlap
  - Heatmaps or matrices (domain-filtered)

**Metrics / Signals**
- DominantClusterShare
- HHI / ConcentrationIndex
- EffectiveClusterCount
- Avg / median Jaccard similarity
- Baseline coverage (set containment, not equality)

**Guardrails**
- No identity diffs
- No “correct vs incorrect”
- Clusters ≠ standards

This phase surfaces gravity, not truth.

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

**Artifacts**
- Join-key definitions / candidates
- Collision groupings:
  - same join_hash, multiple sig_hash
- Fragmentation groupings:
  - same sig_hash, multiple join_hash
- Python-generated:
  - collision_differences.csv
  - fragmentation_examples.csv

**Dashboards / Views**
- Identity Deep Dive
  - Per-domain → per-join_hash drilldown
- Collision Explorer
  - Side-by-side parameter diffs
- Slice Panels
  - Shape, family, lineage, cohort

**Metrics / Signals**
- CollisionRate
- FragmentationRate
- Field-level variance frequency
- Core vs optional field stability

**Guardrails**
- Identity questions live only here
- No normative labels yet
- All explanations must be item-level

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

**Artifacts**
- Field classifications:
  - core / optional / cosmetic
- Allowed variance matrices
- False-drift annotations
- Standards intent notes (structured, not prose-only)

**Dashboards / Views**
- Normative Overlay
  - Phase-2 diffs with allowed/flagged highlighting
- Standards Coverage
  - Which fields lack guidance
-	Multi-Norm Views
  - Regional / lineage norms

**Metrics / Signals**
- % of drift classified as acceptable
- Fields driving violations
- Underspecified-field counts

**Guardrails**
- Norms must cite Phase-2 evidence
-	No retroactive identity changes
-	Multiple norms are allowed

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

**Artifacts**
- Correlation tables:
  -	drift × region
  -	drift × template version
  -	drift × project age
- Lineage clusters
-	Timeline slices

**Dashboards / Views**
- Drift vs Context
  - Region, phase, size, age
- Lineage & Ancestry
  - Template inheritance
-	Timeline Views
  -	Drift accumulation over time

*Metrics / Signals**
- Drift rate over time
- Correlation strength (not causation claims)
- Template inheritance strength

**Guardrails**
- No blame attribution
- Correlation ≠ prescription
- Hypotheses must remain falsifiable

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

**Artifacts**
- Proposed standards changes
-	Template update diffs
-	Guardian / validation rules
- Automation candidates

**Dashboards / Views**
-	Intervention Impact
  -	Fields affected
  -	Projects impacted
-	Effort vs Benefit
  -	Fix cost vs drift reduction

**Metrics / Signals**
- Drift reduction leverage
- Blast radius estimates
-	Manual vs automated fix ratio

*Guardrails**
-	Changes must map to Phase-2 findings
-	Avoid blanket enforcement
-	Prefer reversible interventions

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

**Artifacts**
- Trend summaries
- Early warning indicators
- Governance priority lists
-	Long-term standards roadmaps

**Dashboards / Views**
- Stability Trajectories
  -	Converging vs fragmenting domains
-	Risk Radar
  -	Domains nearing instability
-	Governance Focus
  -	Where attention pays off next

**Metrics / Signals**
- Fragmentation velocity
-	Declining gravity indicators
-	Cost-of-drift vs cost-of-enforcement

**Guardrails**
-	Predictive ≠ deterministic
-	Strategy guides attention, not mandates
-	“Do nothing” is a valid outcome

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
