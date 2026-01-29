# Phase 0 / Phase 1 / Phase 2 Tools Map (tools/)

Status date: 2026-01-29  
Scope: `tools/` entrypoints only  
Audience: Phase-0/1/2 analysis users (no exporter changes implied)

Assumed exporter behavior:
- Legacy bundle: OFF by default (`*.legacy.json` opt-in only)
- Index + details: ON by default (`*.index.json`, `*.details.json`)

This document covers:
- Phase 0: export integrity + transformations
- Phase 1: authority framing (or probing)
- Phase 2: empirical analysis (join keys, stability, collisions, stress)

---

## Shared conventions

### Export surface selection (split-safe)
Preferred input order for tools that consume export JSON:
1) `*.details.json` (record-level, identity_items available)
2) `*.index.json` (summary only; degraded semantics)
3) fallback: `*.json` excluding `*.legacy.json`
4) legacy-only allowed **only** with loud warnings

Any tool that globs `*.json` without filtering is unsafe under split exports.

---

# Phase 0 — Export integrity + transformations

Phase 0 answers:
- “Are exports deterministic and contract-correct?”
- “Can we get clean, analysis-ready tables?”

## 0.1 Flatten split exports to flat tables (CSV)
### tools/export_to_flat_tables.py  (KEEP)

**Purpose**
- Convert record.v2 details into normalized CSV tables.

**Input**
- Requires `*.details.json`

**Output**
- `records.csv`
- `identity_items.csv`
- `status_reasons.csv`
- `label_components.csv`
- `runs.csv`

**Typical command**
```
python tools/export_to_flat_tables.py --root_dir "<ExportsDir>" --out_dir "<OutDir>" --file_id_mode basename
```

**Notes**
- Phase-0 is prerequisite for Power BI and many Phase-2 diagnostics.
- No baselines are involved here.

---

# Phase 1 — Authority framing (or probing)

Phase 1 answers:
- “Does authority exist?”
- “Is any configuration dominant?”
- “Is this domain even suitable for standards?”

⚠️ Phase 1 is **policy-driven**, not discovery-driven.

---

## Phase-1 configuration (critical)

Phase-1 behavior is entirely governed by the **run config JSON**.

### Key Phase-1 config fields

#### `domains_in_scope`  **(REQUIRED to do anything)**
- If empty (`[]`): Phase-1 is **disabled**
- If populated: Phase-1 will analyze only those domains

Phase-1 will **never auto-discover domains**.

#### `seed_baseline_id`  **(OPTIONAL)**
- Biases interpretation and labeling
- Does **not** enable Phase-1
- Safe to omit during exploratory analysis

#### Thresholds
- `observability_min_comparable_rate`
- `convergence_thresholds`
- `ignored_thresholds`

These affect *interpretation*, not execution.

---

## Authority vs Probe modes (Phase-1)

### Authority mode
Used **only after standards exist**.

Characteristics:
- `domains_in_scope` populated
- `seed_baseline_id` provided
- Results framed as “coverage vs baseline”
- Implies authority is real and intentional

### Probe mode  **(your current mode)**
Used **before authority exists**.

Characteristics:
- `domains_in_scope` populated
- `seed_baseline_id` omitted
- No cluster is privileged
- Outputs answer: “what exists?” not “what should be?”

### Disabled mode
- `domains_in_scope: []`
- Phase-1 emits headers only
- This is **intentional**, not an error

---

## 1.1 Domain authority clustering
### tools/phase1_domain_authority.py  (KEEP)

**Purpose**
- Cluster domain configurations across projects
- Compute concentration metrics (HHI, effective clusters)

**Input**
- Prefer `*.details.json`
- Fallback to `*.index.json` with warnings

**Outputs**
- Domain cluster summaries
- Per-project cluster membership
- Concentration metrics

**Important**
- Headers-only output = `domains_in_scope` was empty
- This is a policy result, not a failure

---

## 1.2 Population framing
### tools/phase1_population_framing.py  (KEEP)

**Purpose**
- Translate domain clusters into coverage / adoption framing

**Requires**
- Phase-1 domain clustering outputs

---

## 1.3 Pairwise summaries
### tools/phase1_pairwise_analysis.py  (KEEP)

**Purpose**
- Project-vs-project summaries
- No authority implied
- Safe when no baseline exists

---

# Baseline concepts (critical distinction)

There are **two different “baselines”**, used in different phases.

## Seed-baseline (Phase-1 only)

**What it is**
- A *labeling bias* for authority framing

**Used by**
- `phase1_domain_authority.py`

**What it means**
- “If this configuration appears, treat it as the named reference”

**What it does NOT mean**
- Does NOT define correctness
- Does NOT anchor change detection
- Does NOT affect Phase-2 identity

**Current recommendation**
- ❌ Do not use yet
- Authority has not been established

---

## Baseline (Phase-2 anchor)

**What it is**
- A *comparison anchor* for change analysis

**Used by**
- `run_change_type`
- `run_attribute_stress`
- baseline-anchored `run_dimension_types_by_family`

**What it means**
- “This export represents the reference state”

**What it requires**
- Stable join keys
- Accepted authority (or explicit policy decision)

**Current recommendation**
- ❌ Do not use yet
- Pairwise + population mode is correct

---

# Phase 2 — Empirical analysis

Phase 2 answers:
- “Do these identities work?”
- “Are join keys stable?”
- “Where are collisions or fragmentation?”

Phase-2 is valid **without any baseline**.

---

## 2.0 Phase-2 IO contract
### tools/phase2_analysis/io.py  (INTERNAL)

**Requirements**
- Must prefer `*.details.json`
- Must not treat `*.index.json` as records
- Must never implicitly load `*.legacy.json`

If violated, Phase-2 results are invalid.

---

## 2.1 Join-key discovery (population mode)
- `run_joinhash_label_population`
- `run_joinhash_parameter_population`
- `run_candidate_joinkey_simulation`

These require **no baseline**.

---

## 2.2 Stability
### tools/phase2_analysis/run_population_stability.py

Population-wide stability metrics.  
Baseline-free and safe.

---

## 2.3 Collisions
- `run_identity_collision_diagnostics`
- `run_collision_differencing`

Detects weak identity schemes.  
No baseline required.

---

## 2.4 Change & stress (baseline-anchored)
- `run_change_type`
- `run_attribute_stress`

⚠️ Require a legitimate Phase-2 baseline.  
Do not run yet.

---

## 2.5 Domain-specific decomposition
### tools/phase2_analysis/run_dimension_types_by_family.py

**Two modes**
- Baseline-free (probe):
  - families discovered from population
  - no change_type
- Baseline-anchored:
  - families limited to baseline
  - change_type enabled

Baseline-free mode is correct right now.

---

# Recommended usage (current state)

### Use
- Phase-0 flattening
- Phase-2 population + pairwise analysis
- Phase-1 probe config (domains_in_scope populated, no seed-baseline)

### Avoid
- Seed-baseline
- Phase-2 baselines
- Change-type narratives
- “Drift from standard” language

---

# Key takeaway

- Phase-1 does **not** require a baseline
- Phase-1 requires **explicit scope**
- Baselines imply authority
- You are correctly operating in **pre-authority probe mode**
