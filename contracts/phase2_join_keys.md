# Phase 2 Join Keys — Hypotheses & External Feasibility

## Purpose

This document records **Phase-2 join key hypotheses** and the **empirical feasibility** of generating join keys externally from exported fingerprint JSONs.

It is intentionally:
- **Descriptive, not prescriptive**
- **Reversible**
- **Free of enforcement logic**

No hypothesis in this document is asserted to be correct or final.

---

## Definitions

### Join Key
A deterministic identity used to associate records across exports for Phase-2 comparison.
- Used for joining only (not drift scoring).
- Represented as a set of IdentityItems `{k, q, v}`.
- Has a derived `join_hash`.

### Join Hash
A deterministic hash computed as:

```

join_hash = phase2_join_hash(
phase2_sorted_items(join_key.items)
)

```

Hashing MUST use helpers from `core/phase2.py`.

### IdentityItem Invariants
- `q` explicitly distinguishes:
  - ok
  - missing
  - unreadable
  - unsupported / not_applicable
- `v` MUST NOT contain legacy sentinel literals.
- Missing ≠ unreadable ≠ unsupported is preserved explicitly.

---

## External Join-Key Generation (from Exported JSON)

External Phase-2 generation is considered **feasible** for a domain when the exported JSON contains sufficient explicit data to construct `join_key.items` deterministically, without heuristics.

This is true when **at least one** of the following holds:

1. The export already contains `{k, q, v}` IdentityItems for the join-key hypothesis components, OR
2. The export contains raw values that can be deterministically converted to `(q, v)` using:
   - `phase2_qv_from_legacy_sentinel_str`

External generation:
- MUST use `phase2_sorted_items` and `phase2_join_hash`
- MUST NOT rely on legacy `def_hash`
- MAY be stored as a sidecar artifact keyed by a stable identifier present in the export (e.g., `record_id`)

---

## Domain Hypotheses

### line_patterns

#### Join Key Hypothesis
**Name-only**

```

join_key.items:

* k: line_pattern.name

```

This hypothesis is limited to join identity and does not assert semantic equivalence.

#### Exported Surfaces Relevant to Join

| Surface | Location in Export JSON | Notes |
|------|--------------------------|------|
| Name (string) | `line_patterns.legacy_records[*].name` | Plain string; no explicit `q` |
| Name (display) | `line_patterns.records[*].label.display` | Plain string; no explicit `q` |
| Segment definition | `line_patterns.records[*].identity_basis.items` | Explicit `{k,q,v}` for segment_count and ordered segments |

#### External Feasibility (Empirical)

- **Name-only join**
  - Feasible *iff* missing / unreadable / N/A names are represented as:
    - `null`, or
    - known legacy sentinel strings convertible via `phase2_qv_from_legacy_sentinel_str`
  - If missing names are represented only by omission, tri-state cannot be reconstructed without inference.

- **Name + definition join**
  - Feasible without inference.
  - Segment identity is already expressed as `{k,q,v}` in exports.

#### Notes
- Legacy `def_hash` is not required for Phase-2 join construction.
- Segment identity items already satisfy Phase-2 invariants.

---

## Recording New or Revised Hypotheses

When adding or revising a join key hypothesis:

Record:
- Hypothesis name
- Item keys (`k` values)
- Required export surfaces
- Known gaps or conditional feasibility

Do **not**:
- Assert correctness
- Imply enforcement
- Encode fallback behavior

---

## Non-Goals

This document does **not**:
- Mandate where Phase-2 logic lives
- Specify enforcement or validation rules
- Declare any hypothesis “final”
- Require exporter changes

It exists to preserve intent, assumptions, and empirical constraints for future analysis.
 a **machine-readable mirror** (`contracts/domain_phase2_hypotheses.json`) directly from this doc.
