# CHANGELOG

This file tracks **semantic changes only**:
- anything that changes hashes
- anything that changes what a hash *means*
- anything that changes interpretation, scope, or dependency structure

Pure refactors, moves, renames, formatting, and perf tweaks do **not** belong here.

---

## [Unreleased]

### Changed
- file_metadata.csv: `project_label` now extracted from Autodesk Docs:// central path
  (ACC projects only); blank for non-ACC paths
- line_patterns join key policy upgraded from `line_patterns.join_key.v2`
  (`line_pattern.segments_def_hash`) to `line_patterns.join_key.v3`
  (`line_pattern.segments_norm_hash`) to enforce scale-invariant structural identity;
  same kind sequence + ratio now collapses length-scaled variants into one pattern
- Bundle analysis `bundle_id` stability explicitly scopes hash identity to
  `(domain, scope_key, sorted_pattern_ids)`; identical pattern sets in different
  scope keys (for example `dimension_types` linear vs angular) intentionally
  receive different bundle IDs and are not cross-scope comparable
- `line_pattern.segments_norm_hash` is now computed automatically during flatten
  in `tools/run_extract_all.py` (no `--synthetic-domains line_patterns` flag required)
- line_patterns normalized token precision set to `.6f` (from `.9f`) after
  sensitivity sweep; decision now includes a documented ±2 decimal neighbor
  validation practice to confirm elbow stability over time

### Fixed
- file_metadata.csv: re-running the pipeline now preserves existing non-empty
  `client_label` and `governance_role` values by `export_run_id`
- VCO `dflt_map` computation hoisted out of O(templates × categories) inner loop;
  `other_seconds` reduced from ~920s to ~9s on large files, total VCO time reduced ~73%
- FEC cache deduplication: all `(doc, View, instances)` collection sites normalized to
  `_VIEW_INSTANCES_CACHE_KEY`; redundant FEC calls reduced from 12 to 7 per run
- View instances cache pre-warm repositioned before `view_filter_applications_view_templates`,
  ensuring the cache is populated before any view-related domain runs
- `_timing` scope resolved via injection pattern (`run_fingerprint(doc, timing=None)`);
  timing report merge restored to correct location inside `run_fingerprint()`

### Added
- file_metadata.csv: added `client_label` and `governance_role` columns
  (empty strings, manually curated)
- `TimingCollector.record_elapsed()` for hot-loop accumulation without per-iteration
  lock overhead
- VCO inner loop sub-timers: `vco.enumerate_categories`, `vco.get_param_ids`,
  `vco.get_category_overrides`, `vco.extract_graphics` — `other_seconds` is now
  attributable residual Python overhead rather than a black hole
- `total_serialization` and `total_run` timer scaffolding in runner; both correctly
  report 0.0 in written fingerprint (ordering constraint — captured in Dynamo summary
  surface instead)

---

### Changed (hash-breaking — full re-extraction required)
**Domain family splits (D-015):**
- `dimension_types` split into 7 domains: `dimension_types_linear`
  (Linear/LinearFixed/Angular/ArcLength), `dimension_types_angular`,
  `dimension_types_radial`, `dimension_types_diameter`,
  `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  `dimension_types_spot_slope`
- `object_styles` split into 4 domains by CategoryType tab:
  `object_styles_model`, `object_styles_annotation`,
  `object_styles_analytical`, `object_styles_imported`
- `fill_patterns` split into 2 domains by target:
  `fill_patterns_drafting`, `fill_patterns_model`. Solid fills
  (system defaults) excluded from both domains.
- `view_templates` split into 5 domains by ViewType group:
  `view_templates_floor_structural_area_plans`,
  `view_templates_ceiling_plans`,
  `view_templates_elevations_sections_detail`,
  `view_templates_renderings_drafting`,
  `view_templates_schedules`

**Arrowhead record class corrections:**
- Dot, Diagonal, Box, Loop, Elevation Target, Datum triangle record
  classes corrected to size-only (tick_size_in only). Previous hashes
  for these styles incorrectly included tick_mark_centered and
  heavy_end_pen_weight.

**object_styles join-key correction:**
- pattern_ref.kind record class gate removed. Was incorrect —
  pattern_ref.sig_hash moves to optional_items.

**Dimension type policy corrections:**
- Angular: witness_line_control added to required identity
  (confirmed active in UI for Angular, not previously included)
- Radial: radius_symbol_location and radius_symbol_text added
- Diameter: diameter_symbol_location and diameter_symbol_text added
- Spot families: shape-specific indicator and placement fields added

**System type exclusion:**
- Dimension type extractors now exclude system built-in types not
  accessible in the Revit UI (detected via id-based label fallback
  and family name gate). These types cannot be governed.
- Arrowhead extractor now excludes placeholder_missing records
  (unidentifiable system types).
- Domain routing bugs fixed: DiameterLinked/Alignment Station Labels
  excluded from dimension_types_diameter; Diameter types with
  SpotElevationFixed shape enum correctly routed to diameter domain
  via family name gate.

### Added
- `policies/cross_domain_alignment_keys.json` — domain family registry
  and alignment key definitions
- `arrowhead.record_class` in coordination_items for all arrowhead records
- `lp.is_import` in coordination_items for line_patterns records
- `dim_type.domain_family` in coordination_items for all dimension type records
- `obj_style.category_type`, `obj_style.domain_family`, `obj_style.is_subcategory`
  in coordination_items for all object style records
- `vt.view_type_family`, `vt.view_type_raw` in coordination_items for all
  view template records
- `object_styles_annotation` now populates
  `ctx["object_style_annotation_row_key_to_sig_hash"]` for VCO baseline lookup
- View category overrides: `vco.include_controlled`, `vco.vg_category_type`,
  `vco.context_type` added to coordination_items (D-016)
- View category overrides: category 2 (latent overrides, V/G checkbox unchecked)
  now captured alongside category 1

### Decisions captured
- D-015: Domain family architecture — split criteria, vocabulary, alignment key
  registry
- D-016: VCO scope — category 1 (template-controlled) and category 2 (latent)
  implemented; category 3 (view-local) deferred with hooks

---

### Changed (D-015 — Domain Family Split Architecture)

Domain scope redefined: four monolithic extractors split into 18 per-partition domains.
No hash values changed within any record class — this is a structural change only.

- **`object_styles`** split into `object_styles_model`, `object_styles_annotation`,
  `object_styles_analytical`, `object_styles_imported` — each covers one CategoryType.
  `require_domain` references updated to split names throughout.

- **`fill_patterns`** split into `fill_patterns_drafting`, `fill_patterns_model` —
  each covers one FillPatternTarget. Join-key policy updated to use `fill_pattern.target`
  (was `fill_pattern.target_id`) and `fill_pattern.grid_count` as co-required keys.

- **`dimension_types`** split into 7 per-shape domains (`dimension_types_linear`,
  `dimension_types_angular`, `dimension_types_radial`, `dimension_types_diameter`,
  `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  `dimension_types_spot_slope`). Shape discrimination now happens at domain-level
  (handled shapes frozenset). Shared helpers moved to `core/dimension_type_helpers.py`.

- **`view_templates`** split into 5 per-ViewType-family domains
  (`view_templates_floor_structural_area_plans`, `view_templates_ceiling_plans`,
  `view_templates_elevations_sections_detail`, `view_templates_renderings_drafting`,
  `view_templates_schedules`). Shared VG helpers in `core/vg_sig.py`.

- Dependency chain (`require_domain` calls) updated in `view_category_overrides`
  and runner to reference split domain names.

- Join-key policies updated: all split domains have flat per-domain policies.
  Arrowheads policy corrected: shape-gated keys moved from `explicitly_excluded_items`
  to `optional_items` to satisfy A3 validation rule.

---

### Removed
- Legacy hash infrastructure (pipe-delimited signatures) removed across all domains
- `REVIT_FINGERPRINT_HASH_MODE` environment variable (semantic mode now default and only mode)
- `domains/view_filters_deprecated.py` (unused, 741 lines)
- `core/canon.py`: deprecated `sig_val()` helper
- Phase-2 `semantic_keys` duplication in domain payloads
- Legacy context maps: `*_uid_to_hash_v2` (replaced by canonical `*_uid_to_hash`)

### Changed
- All domains now emit only `hash_v2` as the canonical domain hash in runner contract output
- Context maps simplified: removed `_v2` suffix from semantic hash maps
- Contract building simplified: single semantic hash source instead of mode-dependent logic

### Added
- Root governance docs: `INVARIANTS.md`, `ARCHITECTURE.md`, `DECISIONS.md`.
- **NEW DOMAINS (M4):**
  - `view_filter_definitions` - Global domain capturing filter definitions (rules, categories)
  - `phases` - Global domain capturing phase inventory and sequence (names included in hash per D-010 revised)
  - `phase_filters` - Global domain capturing phase filter settings (New/Existing/Demolished/Temporary visibility)
  - `phase_graphics` - Global domain capturing phase graphic override settings (disabled per D-013)
- Context dictionary (`ctx`) now populated by global domains:
  - `filter_uid_to_hash` - Mapping of view filter UIDs to definition hashes
  - `phase_uid_to_hash` - Mapping of phase UIDs to definition hashes
  - `phase_filter_uid_to_hash` - Mapping of phase filter UIDs to definition hashes
- **Canonical evidence selectors (PRs #106–#119):** All 15 domain extractors migrated to policy-driven join-key and sig-hash composition via `build_join_key_from_policy()`. Each domain now emits `join_key`, `join_hash`, and `sig_basis` fields in records, derived from `identity_basis.items` per the join-key policy.
- **Element traceability (PR #126):** `source_element_id` and `source_unique_id` added to `phase2.unknown_items` across all element-backed domains.
- **Timing instrumentation (PR #127):** `core/timing_collector.py` added for extraction profiling. Runner emits `timings.json` sibling artifact.

### Changed
- **BREAKING: View Templates (M5):** Moved from name-only presence hashing to behavior-based hashing
  - Template identity: Now uses UniqueId (was: name)
  - Template hash: Now derived from controlled behavior (was: name presence)
  - Behavioral inputs: view type, detail level, scale, discipline, phase, phase filter, view filters (ordered), display style
  - Names: Now metadata-only (excluded from hash per D-008)
  - Filter stack: Order-sensitive (preserved)
  - References global domains: filters, phases, phase_filters via context
  - record_rows emitted with per-template sig_hash
- Execution order now enforces dependency: global domains run before contextual domains.
- **record_id stabilization (PR #123):** `record_id` generation made deterministic across runs using domain + identity_basis hash.
- **Join-key deduplication (PR #125):** `join_key.items` no longer duplicates `k/q/v` triples already present in `identity_basis.items`; join_key references the canonical source.
- **Object_styles shape-gating (PR #124):** Join-key policy uses `obj_style.pattern_ref.kind` as discriminator; `ref` shape requires `pattern_ref.sig_hash`, `solid` shape does not.

### Semantic Rules Applied
- **View Filters:** Filter rules are order-sensitive (preserved), categories are sorted
- **Phases:** Phase names are included in behavioral hashes for cross-project comparability (D-010 revised), sequence number captured where available
- **Phase Filters:** Settings are order-insensitive (sorted before hashing)
- **Phase Graphics:** Intentionally disabled — API does not expose graphic overrides (D-013)
- **View Templates (M5):**
  - Template names: metadata-only (per D-008)
  - Filter stack: order-sensitive (filter application order matters)
  - Other settings: order-insensitive (sorted)
  - Global references: uses hashes from filters/phases/phase_filters domains
  - Unreadable templates: fail-soft with explicit markers

### Decisions captured
- Nested fenced code blocks are prohibited in documentation (portability rule).
- View filters are global definitions referenced by views and view templates.
- Phase filters and phase graphic overrides are global.
- Phase names ARE included in behavioral hashes (D-010 revised for cross-project comparability).
- Phase sequence number is included in phase signatures to capture ordering.
- Hash mode migration timeline completed (D-014).

---

## 2025-12-17

### Added
- Documented invariants: deterministic hashes, behavior-first, fail-soft, explicit ordering rules.
- Documented architecture layering: core / domains / context / runner.
- Documented decision log to prevent drift and re-litigation.

### Fixed
- Documentation formatting made portable across GitHub Mobile + Obsidian by avoiding nested fenced blocks.
