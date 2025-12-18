# CHANGELOG

This file tracks **semantic changes only**:
- anything that changes hashes
- anything that changes what a hash *means*
- anything that changes interpretation, scope, or dependency structure

Pure refactors, moves, renames, formatting, and perf tweaks do **not** belong here.

---

## Unreleased

### Added
- Root governance docs: `INVARIANTS.md`, `ARCHITECTURE.md`, `DECISIONS.md`.

### Changed
- View Templates planned to move from name-only presence hashing to behavior-based hashing using record rows and auditable preimages.
- Filters planned to split into a global domain (definition) referenced by views/templates (application).

### Decisions captured
- Nested fenced code blocks are prohibited in documentation (portability rule).
- View filters should be a global section referenced by views and view templates.
- Phase filters and phase graphic overrides should be global; phase names are metadata-only.

---

## 2025-12-17

### Added
- Documented invariants: deterministic hashes, behavior-first, fail-soft, explicit ordering rules.
- Documented architecture layering: core / domains / context / runner.
- Documented decision log to prevent drift and re-litigation.

### Fixed
- Documentation formatting made portable across GitHub Mobile + Obsidian by avoiding nested fenced blocks.