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

### Context Dictionary Schema

The `ctx` dictionary flows through domain execution. Keys are populated by upstream domains and consumed by downstream domains.

#### Runner-Populated Keys (guaranteed)

| Key | Type | Description |
|-----|------|-------------|
| `_collect` | CollectCtx | Collector cache instance (from `core/collect.py`) |
| `_doc_view` | DocViewContext | Document + view context (from `core/context.py`) |
| `debug_vg_details` | bool | Enable verbose VG debug output |

#### Domain-Populated Keys

| Key | Populated By | Consumed By | Type |
|-----|--------------|-------------|------|
| `phase_uid_to_hash` | phases | view_templates | dict[str, str] |
| `phase_filter_uid_to_hash` | phase_filters | view_templates | dict[str, str] |
| `view_filter_uid_to_hash` | view_filters | view_templates | dict[str, str] |
| `view_filter_uid_to_sig_hash_v2` | view_filter_definitions | view_templates | dict[str, str] |
| `line_pattern_uid_to_hash` | line_patterns | object_styles, line_styles | dict[str, str] |
| `line_pattern_uid_to_hash_v2` | line_patterns | object_styles, line_styles | dict[str, str] |

#### Dependency Contract

- Downstream domains MUST use `require_domain()` from `core/deps.py` to validate upstream availability
- Missing upstream keys result in `Blocked` status for the dependent domain
- Domains MUST NOT modify keys populated by other domains

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