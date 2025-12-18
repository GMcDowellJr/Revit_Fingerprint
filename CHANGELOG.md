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
- **NEW DOMAINS (M4):**
  - `view_filters` - Global domain capturing filter definitions (rules, categories)
  - `phases` - Global domain capturing phase inventory and sequence (names excluded from hash per D-010)
  - `phase_filters` - Global domain capturing phase filter settings (New/Existing/Demolished/Temporary visibility)
  - `phase_graphics` - Global domain capturing phase graphic override settings (limited API access)
- Context dictionary (`ctx`) now populated by global domains:
  - `filter_uid_to_hash` - Mapping of view filter UIDs to definition hashes
  - `phase_uid_to_hash` - Mapping of phase UIDs to definition hashes
  - `phase_filter_uid_to_hash` - Mapping of phase filter UIDs to definition hashes

### Changed
- View Templates planned to move from name-only presence hashing to behavior-based hashing using record rows and auditable preimages (M5).
- Execution order now enforces dependency: global domains run before contextual domains.

### Semantic Rules Applied
- **View Filters:** Filter rules are order-sensitive (preserved), categories are sorted
- **Phases:** Phase names are metadata-only (excluded from hash per D-010), sequence number captured where available
- **Phase Filters:** Settings are order-insensitive (sorted before hashing)
- **Phase Graphics:** Placeholder implementation (API exposure varies by Revit version)

### Decisions captured
- Nested fenced code blocks are prohibited in documentation (portability rule).
- View filters are global definitions referenced by views and view templates.
- Phase filters and phase graphic overrides are global; phase names are metadata-only.
- Phase sequence number is included in phase signatures to capture ordering.

---

## 2025-12-17

### Added
- Documented invariants: deterministic hashes, behavior-first, fail-soft, explicit ordering rules.
- Documented architecture layering: core / domains / context / runner.
- Documented decision log to prevent drift and re-litigation.

### Fixed
- Documentation formatting made portable across GitHub Mobile + Obsidian by avoiding nested fenced blocks.