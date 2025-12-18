# Architecture Overview

The system is domain-driven and layered.

---

## Layer 0 — Core (Pure Python)

No Revit API calls.

Includes:
- hashing utilities
- canonicalization helpers
- row emission helpers
- formatting and fail-soft markers

Purpose:
- portability
- testability
- semantic isolation

---

## Layer 1 — Domain Extractors (Revit-aware)

One file per domain.

Rules:
- Each domain exposes a single `extract(doc, ctx)` function
- Domains do not import each other
- Cross-domain references flow only through `ctx`

Examples:
- object_styles
- line_patterns
- view_filters
- phases
- view_templates

---

## Layer 2 — Context Builder

Responsible for:
- building lookup indices (e.g. `filter_uid → def_hash`)
- ordering domain execution (globals before dependents)

`ctx` is a plain dictionary.

---

## Layer 3 — Runner (Host-specific)

Examples:
- `run_dynamo.py`
- `run_pyrevit.py` *(future)*
- `run_external_command.py` *(future)*

Responsibilities:
- acquire Revit `doc`
- select domains to run
- assemble final JSON output

---

## Dependency Direction

    Global Domains
         ↓
       Context
         ↓
    View Templates
         ↓
        Views

Reverse dependencies are forbidden.

---

## Design Intent

- Adding a new domain should not require touching existing ones
- Moving to a new host should not require rewriting domain logic
- Hash semantics must survive refactors intact