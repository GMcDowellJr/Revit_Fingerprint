# CLAUDE.md - AI Assistant Guide

This document provides essential context for AI assistants working with the Revit Fingerprint codebase.

## Project Overview

**Revit Fingerprint** extracts deterministic, behavior-based fingerprints from Revit models. It identifies **what a model does**, not how it is named or presented in the UI. The system enables standards governance, drift detection, and cross-project comparison.

**Primary runtime**: Dynamo CPython3 (via `runner/run_dynamo.py`)

## Architecture

The system follows a domain-driven, layered architecture:

```
Layer 0 - Core (Pure Python)     → core/
Layer 1 - Domain Extractors      → domains/
Layer 2 - Context Builder        → core/context.py, core/collect.py
Layer 3 - Host-specific Runners  → runner/
```

**Dependency direction**: `Core → Domains → Context → Runner`

Reverse dependencies are forbidden. Domains do NOT import each other.

## Directory Structure

```
core/           Pure Python utilities (no Revit API calls)
  hashing.py    MD5 hashing (CLR/hashlib dual-runtime)
  canon.py      Canonicalization + sentinels (<MISSING>, <UNREADABLE>, <NOT_APPLICABLE>)
  contracts.py  Contract envelopes, status rollups, bounded errors
  record_v2.py  record.v2 schema utilities
  phase2.py     Phase-2 join-key/join-hash helpers
  context.py    View-scoped context (ViewInfo, DocViewContext)
  deps.py       Dependency enforcement (Blocked exception, require_domain)
  rows.py       Parameter reading, unit conversion
  collect.py    FilteredElementCollector caching

domains/        One extract(doc, ctx) function per domain
  identity.py   Project metadata (NO HASH - metadata only)
  units.py      Length/area/volume format options
  object_styles.py, line_patterns.py, line_styles.py
  fill_patterns.py, text_types.py, dimension_types.py
  view_filters.py, phases.py, phase_filters.py
  view_templates.py (M5 behavioral fingerprinting)
  phase_graphics.py (DISABLED - API limitation per D-013)

runner/         Host-specific entry points
  run_dynamo.py Primary runner (M5 implementation)

validators/     Output validation
  record_v2.py  record.v2 schema validation

tools/          Analysis & comparison utilities
  compare_manifest.py, pairwise_drift.py, similarity_compare.py
  phase2_analysis/  Phase-2 analysis scripts

tests/          pytest test suite
  test_*.py     Unit tests
  revit/        Revit integration harness (requires Revit)
  golden/       Golden file comparisons

contracts/      Machine-readable contracts
  record_contract_v2.md, record_contract_v2.schema.json
  domain_identity_keys_v2.json  Per-domain key registry

legacy/         MVP implementation (preserved reference)
  fingerprint_mvp.py
```

## Critical Rules (Non-Negotiables)

### Hash Semantics
- Hashes MUST be deterministic, stable across sessions, independent of element creation order
- Hash inputs represent **behavior**, not presentation or naming
- Names are **metadata only** - never included in behavior hashes unless explicitly stated

### Sentinel Policy (PR3)
Only THREE angle-bracket sentinels are allowed:
- `<MISSING>` - value None/empty/unset
- `<UNREADABLE>` - value unreadable/exception
- `<NOT_APPLICABLE>` - value not applicable to element type

### Fail-Soft Policy
- NEVER silently collapse distinct states
- Unreadable/inaccessible data MUST emit explicit markers
- Errors propagate into hashes intentionally

### record.v2 Schema
Every record MUST have:
- `schema_version: "record.v2"`
- `domain`, `record_id`, `status`, `status_reasons`
- `sig_hash` (null iff status == "blocked")
- `identity_basis` with `items: [{k, q, v}]` format
- `identity_quality`, `label`

Identity values (`v`) MUST NOT contain sentinel literals - use `v: null` + `q: "missing"` instead.

### UniqueId Usage
Use `UniqueId` ONLY for element-backed entities where identity persistence matters (filters, phases, templates, views). Styles, patterns, and definitions use name-based or composite keys.

### Ordering Rules
- Order-sensitive structures (e.g., view filter stack): preserve order
- Order-insensitive structures: sort before hashing
- Each domain MUST explicitly state its ordering behavior

## Development Workflow

### Commit Message Convention
```
chore:    docs, scaffolding, non-code setup
refactor: moves/splits with NO semantic change
feat:     new domains or semantic inputs
fix:      bug fix that changes behavior
```

Every commit message MUST state "no semantic change" OR describe the semantic change.

### CHANGELOG Discipline
Log ONLY semantic changes (signature composition, ordering rules, identity rules, fail-soft behavior). Do NOT log pure refactors.

### Domain Development Pattern
```python
# domains/example.py
from core.hashing import make_hash, safe_str
from core.canon import canon_str, S_MISSING, S_UNREADABLE
from core.record_v2 import build_record_v2, make_identity_item
from core.phase2 import phase2_sorted_items

try:
    from Autodesk.Revit.DB import ...
except ImportError:
    ... = None  # Allow non-Revit testing

def extract(doc, ctx=None):
    """Extract domain data from Revit document."""
    # ... implementation
    return {
        "hash": "<32-hex MD5 or None>",
        "count": int,
        "record_rows": [...],
        "records": [...],  # record.v2 format
        "status": "ok|degraded|blocked|failed"
    }
```

## Testing

### Run Tests
```bash
pytest tests/                              # All unit tests
pytest tests/test_hashing_incremental.py   # Specific test
```

### Key Test Files
| Test | Purpose |
|------|---------|
| `test_sentinel_policy.py` | Enforce only 3 allowed sentinels |
| `test_hashing_incremental.py` | Hash determinism |
| `test_contracts_run_status.py` | Status rollup (failed > degraded > ok) |
| `test_record_contract_v2.py` | record.v2 schema validation |
| `test_no_direct_filtered_element_collector_in_domains.py` | Architecture enforcement |
| `test_deps_require_domain.py` | Dependency blocking |

### Validate Exported JSON
```bash
FINGERPRINT_JSON_PATH=/path/to/export.json pytest tests/test_record_contract_v2.py
```

## Key Decisions Reference

| Decision | Summary |
|----------|---------|
| D-001 | Behavior-first fingerprinting (not UI presentation) |
| D-002 | Deterministic, auditable hashes with explicit preimages |
| D-003 | `record_rows` is canonical explainability |
| D-004 | `UniqueId` restricted to element-backed identities |
| D-005 | Fail-soft is mandatory |
| D-006 | Ordering rules explicit per domain |
| D-007 | Global vs contextual domain split |
| D-008 | View templates are behavioral, not nominal |
| D-010 | Phase names are non-behavioral (metadata only) |
| D-011 | Domain-driven architecture |
| D-013 | `phase_graphics` disabled (API limitation) |

See `DECISIONS.md` for full rationale.

## Common Tasks

### Adding a New Domain
1. Create `domains/new_domain.py` with `extract(doc, ctx=None)` function
2. Add to import list in `runner/run_dynamo.py`
3. Register allowed keys in `contracts/domain_identity_keys_v2.json`
4. Add tests in `tests/`
5. Document ordering behavior and identity rules
6. Update `DECISIONS.md` if introducing new semantic rules

### Modifying Hash Composition
1. Document the change in `DECISIONS.md`
2. Log the semantic change in `CHANGELOG.md`
3. Update affected tests
4. Verify golden file comparisons still pass (or update them)

### Debugging Hash Mismatches
1. Check `record_rows` for per-record hash preimages
2. Verify ordering (order-sensitive vs. sorted)
3. Check for sentinel handling differences
4. Use `tools/compare_manifest.py` for diff analysis

## Files to Read First

When working on this codebase, start with:
1. `INVARIANTS.md` - Non-negotiable rules
2. `DECISIONS.md` - Architectural decisions
3. `ARCHITECTURE.md` - Layered design
4. `contracts/record_contract_v2.md` - Record schema

## Warnings

- NEVER change hash semantics without updating `DECISIONS.md` and `CHANGELOG.md`
- NEVER add new sentinel literals beyond the 3 approved ones
- NEVER make domains import other domains
- NEVER use `FilteredElementCollector` directly in domains (use `core/collect.py`)
- The `phase_graphics` domain is intentionally disabled - do not attempt to enable without API justification
