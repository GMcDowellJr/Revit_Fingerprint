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

### Global Domains
- Object Styles
- Line Patterns
- Line Styles
- Fill Patterns
- Text Types
- Dimension Types
- View Filters *(in progress)*
- Phases *(in progress)*
- Phase Filters *(in progress)*
- Phase Graphics *(in progress)*

### Contextual Domains
- View Templates *(behavioral refactor in progress)*
- Views *(planned)*

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
It is a governed extractor under active refactor.

Semantic changes are deliberate and explicit.