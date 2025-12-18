# REFACTOR.md

This document defines the refactor approach for transitioning from the current single-file MVP to a modular, domain-driven architecture **without changing hash semantics** unless explicitly declared.

Root intent:
- Stop reprocessing stable domains repeatedly
- Enable selective domain execution
- Prepare for future host runners (pyRevit / add-in) without rewriting core logic

---

## Non-Negotiables

- Hashes must remain stable, deterministic, cross-session
- Hashes must reflect behavior, not UI noise
- Global hashes must have auditable preimages
- `record_rows` is canonical explainability
- Use `UniqueId` only where element-backed identity is meaningful
- Fail-soft always; never silently collapse distinct states
- Do not nest fenced code blocks inside other fenced blocks in documentation

---

## Refactor Strategy

### Principle: Path/structure refactor first, logic later
The first milestone is a **mechanical split**:
- reorganize code into folders
- update imports (or module references)
- introduce a runner
- preserve output behavior

No semantic changes until the baseline is anchored.

---

## Target Structure

    revit-fingerprint/
      core/
        hashing.py
        canon.py
        rows.py
      domains/
        object_styles.py
        line_patterns.py
        text_types.py
        dimension_types.py
        fill_patterns.py
        view_filters.py
        phases.py
        phase_filters.py
        phase_graphics.py
        view_templates.py
      runner/
        run_dynamo.py
      legacy/
        fingerprint_mvp.py
      README.md
      INVARIANTS.md
      ARCHITECTURE.md
      DECISIONS.md
      CHANGELOG.md
      baseline_hashes.json

---

## Execution Order (Dependency Safe)

Global domains must run before contextual domains that reference them.

Baseline order (minimum):
1) Global style domains (already locked)
2) `view_filters` (definition)
3) `phases` (inventory / ordering where available)
4) `phase_filters` (definition)
5) `phase_graphics` (mapping)
6) `view_templates` (references globals)

Views (effective behavior) comes later.

---

## Milestones

### M0 — Anchor Baseline
Goal: preserve current behavior and output as-is.

Steps:
- Commit current MVP script under `legacy/fingerprint_mvp.py` unchanged
- Add governance docs at root
- Add empty folder structure

Exit criteria:
- legacy file is present and runnable in the original environment
- root docs exist and render consistently (mobile-safe)

---

### M1 — Introduce Runner (No Semantic Change)
Goal: create a single entry point that controls which domains run.

Steps:
- Create `runner/run_dynamo.py`
- In the runner, call the existing MVP implementation (temporarily) or directly call the functions once extracted
- Add a domain selection mechanism (allowlist)

Exit criteria:
- Running through `runner/run_dynamo.py` produces identical output to legacy execution
- A subset run can be configured (even if initially only one domain is selectable)

---

### M2 — Extract Core Utilities (No Semantic Change)
Goal: move shared helpers into `core/` without altering behavior.

Candidate moves:
- `make_hash`
- canonical string normalization
- value formatting helpers
- row emit helpers
- fail-soft marker helpers

Exit criteria:
- All extracted helpers are imported by the legacy implementation (or by newly extracted domains)
- Outputs remain identical

---

### M3 — Extract Domains (One at a Time, No Semantic Change)
Goal: migrate domains from the legacy file into `domains/`.

Rules:
- one domain per commit
- keep function names and output schema unchanged unless explicitly approved
- ensure deterministic sorting behavior remains the same

Exit criteria:
- Each extracted domain runs via the runner and matches legacy output
- legacy retains remaining domains until fully migrated

---

### M4 — Add New Global Domains (Semantic Additions Allowed, Explicit)
Goal: implement new domains discussed (filters/phases) with documented semantics.

New domains:
- `domains/view_filters.py`
- `domains/phases.py`
- `domains/phase_filters.py`
- `domains/phase_graphics.py`

Rules:
- semantics must be recorded in `DECISIONS.md` if new rules are introduced
- semantic changes must be logged in `CHANGELOG.md`

Exit criteria:
- new domains exist with `record_rows` and auditable preimages
- views/templates can reference these via context indices

---

### M5 — Upgrade View Templates to Behavioral Fingerprinting (Semantic Change)
Goal: move templates from name-only presence to behavior-based signatures.

Rules:
- treat filter definition as global; templates store application context only
- preserve ordering where behaviorally meaningful (filter stack)
- exclude names from behavior signatures

Exit criteria:
- template global hash derived from per-template `sig_hash`
- `record_rows` emitted per template, keyed by `UniqueId`
- no silent collapse when unreadable

---

## Commit Discipline

Commit types:
- `chore:` docs, scaffolding, non-code setup
- `refactor:` moves/splits with **no semantic change**
- `feat:` new domains or new semantic inputs to signatures
- `fix:` bug fix that changes behavior (must be logged)

Commit messages must state whether semantics changed:
- "no semantic change"
- or describe the semantic change explicitly

---

## CHANGELOG Discipline

Log only semantic changes:
- signature composition changes
- ordering rule changes
- identity rule changes
- domain dependency/scope changes
- fail-soft behavior changes that affect hashes

Do not log pure refactors.

---

## Verification Plan

Baseline verification (minimum):
- Confirm per-domain hash matches legacy for extracted domains
- Confirm record counts match
- Confirm the same keys exist in output JSON

Optional (recommended later):
- `baseline_hashes.json` per sample model
- regression script to compare outputs across commits

---

## Definition of Done (Refactor Phase)

The refactor phase is complete when:
- The legacy monolith is no longer required to run
- The runner can execute selected domains
- All domains produce deterministic hashes with auditable preimages
- Governance docs exist and match actual behavior
- Future host runners can be added without rewriting domain logic