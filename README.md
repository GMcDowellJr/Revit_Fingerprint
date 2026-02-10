# Revit Fingerprint — MVP → Baseline

This repository extracts **deterministic, behavior-based fingerprints** from Revit models.

The goal is to identify **what a model does**, not how it is named or presented in the UI.

This repo is currently transitioning from:
- **MVP (Minimum Viable Prototype)** → proof of feasibility
to:
- **Baseline** → refactor-safe, extensible foundation

No product guarantees are implied at this stage.

---

## Core Principles

- Hashes reflect **behavior**, not UI noise
- Hashes are **stable, deterministic, cross-session**
- Every hash has an **auditable preimage**
- `record_rows` is the canonical explainability structure
- `UniqueId` is used **only** where element-backed identity is meaningful
- Fail-soft always — unreadable data must not silently collapse distinct states

---

## Scope (Current)

### Metadata Domains
- Identity (project metadata, no hash)
- Units (length/area/volume format options)

### Global Style Domains
- Line Patterns
- Object Styles
- Line Styles
- Fill Patterns
- Arrowheads
- Text Types
- Dimension Types

### Global Filter / Phase Domains
- View Filter Definitions
- Phases
- Phase Filters
- Phase Graphics *(disabled — API limitation, see D-013)*

### Contextual Domains
- View Filter Applications (view templates)
- View Category Overrides
- View Templates

### Planned
- Views *(not yet implemented)*

---

## Execution Environment

- Primary runtime: **Dynamo CPython3**
- Code is structured to allow future execution via:
  - pyRevit
  - RevitPythonShell
  - ExternalCommand / add-in

Revit API access is isolated so non-Dynamo runners can be added later.

---

## Status

This repository is **not yet a product**.
It is a governed extractor with an active baseline (M5 complete).

Semantic changes are deliberate and explicit.
Hashing is semantic-only (`hash_v2`) and legacy hash mode has been removed.
See `CHANGELOG.md` for semantic change history and `DECISIONS.md` for architectural decisions.