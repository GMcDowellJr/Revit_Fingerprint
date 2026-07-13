# CHANGELOG

This file tracks **semantic changes only**:
- anything that changes hashes
- anything that changes what a hash *means*
- anything that changes interpretation, scope, or dependency structure

Pure refactors, moves, renames, formatting, and perf tweaks do **not** belong here.

---

## [Unreleased]

### Fixed
- `tools/build_segment_manifest.py` `_sanitize_folder()` collapsed consecutive
  separator characters into one `_` and trimmed leading/trailing `_`, which
  erased a real distinction in `segment_id`: a cut dimension explicitly
  selected in a subset with a blank value (today only `client_label` — see
  `_build_segments()`'s blank-client handling) renders as an empty part
  between/after separator pipes (e.g. `imperial|Template|` or
  `imperial|Container||architectural`), which is a *different, smaller*
  population than that same dimension not being selected at all (e.g.
  `imperial|Template`, which pools every value of the field, blank
  included — always a superset of the selected-blank population). Both
  forms sanitized to the identical folder name, so once enough blank-client
  rows exist for the two populations to diverge (no longer collapsible via
  the existing `redundant_single_child` dedup), both become real,
  independently `bundle`/`reference`-eligible segments competing for the
  same `output_folder` — surfaced only as an opaque `_2` collision-avoidance
  suffix rather than a clear identity. `_UNSAFE_FOLDER_CHARS` no longer uses
  a `+` quantifier (each unsafe character is replaced one-for-one, so
  consecutive separators no longer collapse to a single `_`) and the final
  `.strip("_")` was removed, so a trailing/embedded blank-selected segment
  now sanitizes to a distinguishable folder name. Each blank part is also
  rendered as the literal token `enterprise` (the same scope-level term
  `compare_cross_segment.py` already uses for "no client, no bc" rows)
  rather than a bare `_`/`__`, so e.g. `imperial|Template|` sanitizes to
  `imperial_template_enterprise` instead of `imperial_template_` — a
  self-explanatory name instead of something that reads as a naming
  mistake. `segment_id` text itself (used elsewhere — parsed positionally
  in `tools/generate_governance_narrative.py` and hardcoded across dozens
  of existing tests) is completely unchanged; only the derived folder name
  changes, and only for segments that select a blank cut-dimension value.
  Verified against a real corpus manifest: 5 `bundle`/`reference`-eligible
  folder-name collisions, all resolved.

### Changed (breaking pipeline-contract)
- `segment_manifest.csv` and `run_registry.csv` no longer carry per-segment file
  membership as inline pipe-delimited columns (`export_run_ids`,
  `seed_export_run_ids`). For large populations these columns exceeded
  spreadsheet cell limits (Excel ~32,767 chars/cell, Google Sheets ~50,000
  chars/cell — confirmed offenders in the current corpus reached 59,271
  chars), and a viewer truncating a field mid-quote desyncs the CSV parser
  for every row after it. Membership now lives in a new normalized join
  table, `segment_membership.csv` (`segment_id,export_run_id,is_seed`), one
  row per (segment, file) pair, written alongside the other two files by
  `build_segment_manifest.py`. `segment_manifest.csv` keeps only scalar
  summary fields (`file_count`, `has_seed_file`, `population_hash`);
  `run_registry.csv` keeps `population_hash` only — neither file will ever
  again carry a variable-length filename list. `population_hash` is computed
  identically to before (still from the in-memory `eids` list, not a
  round-trip through any CSV), so it is unchanged for any segment given the
  same file population — skip-logic/staleness comparisons are unaffected.
  `tools/run_segment_orchestrator.py` gains a `--membership-file` flag
  (default: sibling of `--manifest-file`) and now sources every
  `export_run_ids`/`allowed_ids` lookup from `segment_membership.csv` instead
  of the retired manifest column. `_build_registry()`'s `new_files`/
  `removed_files` staleness-reason diffing now reads the prior run's
  population from `existing_membership` (loaded from a prior
  `segment_membership.csv`) instead of an `export_run_ids` field embedded in
  the prior `run_registry.csv` row; a registry rebuilt for the first time
  after this migration (no prior `segment_membership.csv` on disk) will show
  every current file as `new_files` with no `removed_files` on that one
  transitional run, since there is no prior per-segment file list to diff
  against — a one-time artifact of the migration, not an ongoing dual-path.
  No fallback to the old inline column was kept; this is a schema change, not
  an additive one.

### Added
- `tools/compare_cross_segment.py` governance comparisons now fan out across
  enterprise/bc/client scope levels instead of routing everything through a
  single client-scoped grouping key. Scope level is derived per-row from
  which of `client_label`/`business_center_label` are populated (enterprise =
  neither; bc = business_center_label only; client = client_label only;
  project = both) — orthogonal to `governance_role`, and computed the same
  way for every comparison in this file.
  `discover_governance_chain()` gains four new directed pairwise comparison
  types, each an independent parallel edge (no fixed override precedence
  between enterprise/bc/client standards, since any one may or may not have
  adapted from another): `enterprise_to_project` (an enterprise-scoped
  Template/Container reaches every Project regardless of its own client/bc),
  `bc_to_project` (a bc-scoped Template/Container reaches only Projects in
  the same normalized business center), `enterprise_to_bc`, and
  `enterprise_to_client` (same-role, standard-to-standard — Template vs.
  Template or Container vs. Container, never mixed roles). All four are
  registered in `DIRECTED_TYPES` and `GOVERNANCE_STATE_DIRECTED_TYPES`;
  `enterprise_to_project`/`bc_to_project` are additionally registered in
  `DELTA_DIRECTED_TYPES` alongside `template_to_project`/`container_to_project`,
  since they are the same shape of comparison (standard reference vs. Project
  target) just at a different scope level. Generic/Generic-Host is
  deliberately excluded from this fan-out — it already pairs unconditionally
  against every Template/Container/Project via the pre-existing `generic_ids`
  loop, so a separate scope-scoped edge would be redundant.
  `run_pooled_comparison()` gains two new pool grains alongside the existing
  `(parent_segment_id, role, unit_system)` pool (now labeled `pool_scope=
  parent_sibling` in `cross_segment_pooled.csv`, a new column): `pool_scope=
  bc` pools `(business_center_label, role, unit_system)` ignoring
  client_label (whichever clients happen to have work in that bc), and
  `pool_scope=client` pools `(client_label, role, unit_system)` ignoring
  business_center_label (whichever bcs happen to have work for that client).
  These are genuinely different pools with different membership, not two
  views of the same pool — a segment can now appear in `cross_segment_pooled.csv`
  once per applicable pool grain. The per-pool containment/bundle computation
  itself is unchanged; it was extracted into a shared `_build_pooled_row()`
  helper so all three grains share one implementation.
- Segment staleness model extended (build_segment_manifest.py `_build_registry()`):
  `run_registry.csv` gains `export_run_ids` (persisted per-run member list, enabling
  next-run diffing) and `conformance_reference_mode` (currently always `"latest"` —
  compare_cross_segment.py always resolves reference segments dynamically against
  current output; a pinned/snapshot mode is deferred until Phase-2 baseline authority
  is established). When `population_hash` changes, the registry now records
  `new_files:<n>` and/or `removed_files:<n>` reason counts alongside the existing
  `population_changed` marker, diffed against the prior run's `export_run_ids`. A
  metadata edit that moves a file between segments (e.g. a corrected `client_label`)
  surfaces as `removed_files` on the old segment and `new_files` on the new one —
  no separate "metadata change" detection path was needed or added.
- `tools/run_segment_orchestrator.py --dry-run` now prints each pending segment's
  registry `notes` (the staleness reason) alongside its status.
- `tools/compare_cross_segment.py` now writes `comparison_registry.csv` after every
  run: one row per actually-recomputed (segment_a, segment_b, comparison_type,
  domain) work item, stamped with each side's `population_hash`/`last_run_utc`
  (read from `run_registry.csv`) and `computed_utc`. Keyed at the domain granularity
  matching `work_items`, not the coarser pair — a `--domain`-scoped invocation only
  recomputes one domain per pair, so stamping at pair granularity would silently
  mark every other domain on that pair "current" without having recomputed it,
  hiding real staleness from a later `--dry-run`. This is new tracking state
  only — comparisons are still always fully recomputed on every invocation; nothing
  is skipped based on this registry. `--dry-run` now looks up each discovered
  (pair, domain) work item against this registry and labels it `stale` (never
  computed, or either side's stamp has moved since — this is how a Template/
  Container reference re-running with new bundle output is surfaced as
  invalidating its downstream Project/Container comparisons, even though the
  target's own file population is unchanged) or `current`.

### Fixed
- `tools/compare_cross_segment.py` `discover_governance_chain()`'s four
  scope-level fan-out edges (`enterprise_to_project`, `bc_to_project`,
  `enterprise_to_bc`, `enterprise_to_client`) group purely by scope level,
  ignoring `parent_segment_id` — the same class of bug just fixed in
  `run_pooled_comparison()`'s bc/client pool grains, but in the pairwise
  path. Verified against a real corpus manifest: 14 of 139 new-type pairs
  were a segment paired against its own `parent_segment_id` ancestor or
  descendant (e.g. an enterprise-scoped Template paired against a
  bc-scoped Template nested directly under it), which would inflate
  containment toward a false 1.0 the same way. `_build_ancestor_map()` and
  a shared `_is_lineage_related()` helper (used by both this fix and the
  pooled-comparison one) now exclude any such pair from all four edges.
- `tools/compare_cross_segment.py` `run_pooled_comparison()`'s new `bc`/`client`
  pool grains ignore `parent_segment_id` for grouping, so a collection-blank
  BC roll-up and its own collection-specific child (or any ancestor/descendant
  pair sharing the same normalized bc/client value) could land in the same
  pool. Since segments are hierarchical cuts of the same underlying file
  population, an ancestor's data is always a superset of its descendants' —
  pooling them as peers compared a segment against a pool already containing
  its own data, inflating `all_containment_focal_in_pool`/
  `used_containment_focal_in_pool` toward a false 1.0. A new `_build_ancestor_map()`
  walks each segment's `parent_segment_id` chain once per invocation;
  `run_pooled_comparison()` now excludes any segment in the focal segment's
  own ancestor/descendant lineage from its pool, for all three pool_scope
  grains.
- `tools/compare_cross_segment.py` `build_explicit_matrix_outputs()`'s pool
  matrix (`project_pool_containment_similarity_matrix.csv`) keyed cells only
  as `row_id -> peer_pool:<row_id>` by view/domain, ignoring `pool_scope`.
  Since a project can now emit one pooled row per applicable `pool_scope`
  grain (`parent_sibling`/`bc`/`client`), different grains for the same
  project collided on identical matrix coordinates with different values.
  `column_id` now folds in `pool_scope` (`peer_pool:<pool_scope>:<row_id>`)
  so each grain gets its own cell.
- `tools/compare_cross_segment.py` `_normalize_bc_label()` only blanked empty
  strings and the `"0000"`/`"BC_0000"` bookkeeping tokens, dropping the
  `is_blank_or_na()` NA-token handling (`n/a`, `NA`, `__NOT_APPLICABLE__`, ...)
  that `discover_governance_chain()`'s `_key()` previously relied on for its
  `business_center_label` fallback. An NA-spelled bc was being treated as a
  real peer business center by `_bc_of()`, `_scope_level()` (misclassified as
  `"bc"` scope instead of `"enterprise"`), and the new bc-scoped
  pooling/pairwise comparisons. Restored the `is_blank_or_na()` check
  alongside the bookkeeping-token check.
- `tools/compare_cross_segment.py` `discover_governance_chain()`'s `_key()`
  business_center_label fallback (used when `client_label` is blank/NA) no
  longer treats bookkeeping tags `"0000"`/`"BC_0000"` (any case) as a real
  peer business center. Those values mean "enterprise work, tagged for
  bookkeeping," not a specific business center; grouping by the raw string
  would have silently pooled unrelated enterprise-wide rows together as if
  they shared one bc. The fallback now normalizes through the same
  `_bc_of()`/`_normalize_bc_label()` helpers the new enterprise/bc/client
  scope-level fan-out uses, falling through to the collection/blank
  fallbacks below when the tag normalizes to blank. Existing fixtures using
  `business_center_label="BC_0000"` alongside a populated `collection_label`
  are unaffected (they already grouped via collection_label, not
  business_center_label); a row with an unadorned `"0000"`/`"BC_0000"` and no
  collection_label now falls through to the blank-key fallback instead of
  pairing with unrelated same-tagged rows.
- `tools/compare_cross_segment.py` `comparison_registry.csv`: a (pair, domain) is now
  also omitted from the stamp if either side's `run_registry.csv` `status` is not
  `"complete"`. `build_segment_manifest.py` updates `population_hash` to a segment's
  new file population immediately on manifest rebuild, resetting `status` to
  `"pending"` (and clearing `last_run_utc`) until the orchestrator actually re-runs
  that segment — but its output folder on disk still holds the *old* population's
  results until then. A compare run in that window read the stale on-disk data yet
  got stamped with the segment's already-updated (new) `population_hash`; once the
  segment reached `"complete"` with that same hash, a later `--dry-run` would have
  wrongly reported the pair as already current.
- `tools/compare_cross_segment.py` `comparison_registry.csv`: removed the carryover of
  prior (pair, domain) entries not recomputed this run, and stopped stamping work
  items that produced no output. Every other output this tool writes
  (`cross_segment_summary.csv` etc.) is a full `atomic_write_csv` replace from only
  the current invocation's rows, never a merge — so a `--domain`/`--segment`-scoped
  run sharing the same `--out-dir` as an earlier full run already destroys those
  other domains'/pairs' output rows. Carrying their old `comparison_registry.csv`
  stamp forward falsely claimed that data was still current. The registry is now a
  full snapshot of only what this invocation actually produced: a scoped run leaves
  every non-recomputed (pair, domain) with no row at all (correctly staleness-flagged
  on the next `--dry-run`), and a work item where `run_pair()`/`_run_pair_domain()`
  returned `None` with no governance-state rows either (e.g. below `--min-patterns`,
  or a within-project pair with no eligible file pairs) is omitted rather than
  stamped current for output that was never written.
- `build_segment_manifest.py` `_build_registry()`: the new `new_files`/`removed_files`
  reason diff reused the name `new_ids` for the per-segment export_run_id diff,
  shadowing the outer `new_ids` (the full eligible segment_id set) used later to
  compute `dropped_ids`. Any retained segment whose population changed left
  `new_ids` holding export_run_ids instead of segment_ids, so every other
  still-present segment was reported as removed from the registry with a false
  "review corresponding folders for manual cleanup" warning. Renamed the
  per-segment locals to `old_export_ids`/`new_export_ids` so they no longer
  collide with the outer set.
- line_patterns sig_hash policy corrected to segments_def_hash (sig_hash.v2):
  segments_norm_hash was incorrectly used as sig_hash basis — it belongs in join_hash only.
  sig_hash answers exact identity (scale variants are distinct records);
  join_hash answers governance equivalence (scale variants collapse to one pattern).
  Cross-domain reference joins (obj_style.pattern_ref.sig_hash, line_style.pattern_ref.sig_hash)
  were broken while norm_hash was in sig_hash; this change restores them.
  Downstream: re-run sig_hash stage only. Bundle/pattern/segment pipelines unaffected
  (they operate on join_hash exclusively).

### Added
- `instance_count` and `is_sole_type_in_category` metadata fields added to
  `text_types`, `dimension_types` (all 7 splits), `arrowheads`, and `compound_types`
  (all 4 partitions — wall, floor, roof, ceiling). Both fields are additive metadata
  only — never in sig_hash, join_key, or identity_basis.items. Arrowheads emit
  `instance_count = None / not_applicable` (tick-mark reverse-lookup deferred).
  Enables compound placeholder condition `is_purgeable OR (is_sole_type_in_category
  AND instance_count == 0)` at pipeline/BI layer.

### Fixed
- `view_filter_applications_view_templates` and `view_templates`: `GetIsFilterEnabled`
  now captured alongside `GetFilterVisibility` — a toggled-off filter is now
  distinguishable from a visible one.
- `view_category_overrides_model` / `view_category_overrides_annotation`:
  `GetCategoryHidden()` captured per category via `_category_hidden_item()` — template-
  hidden categories are now reflected in the override record.
- `object_styles_model`: material resolved to name + class hash (`obj_style.material_sig_hash`)
  via `_material_ref_item()`, not raw ElementId — cross-project stable.
- `identity`: `doc.IsWorkshared`, `app.VersionNumber`, `app.VersionName`,
  `app.VersionBuild` all captured and emitted.
- `view_templates`: `GetWorksetVisibility()` captured per user workset via
  `_append_workset_visibility()`.

### Changed
- `units` domain expanded from 3 specs (length/area/volume) to 38 specs covering
  all Revit disciplines. Common additions: angle, slope, speed, time, mass_density,
  currency, rotation_angle, distance. Electrical: 7 specs. HVAC: 8 specs. Piping:
  5 specs. Structural: 7 specs. All specs are extracted for every document regardless
  of discipline — GetFormatOptions returns a valid FormatOptions object for all
  SpecTypeId specs on any live document. ITEM_Q_UNREADABLE paths are defensive
  fallbacks only and are not expected to fire in normal execution. SpecTypeId nested
  attribute paths are resolved once at function entry and filtered before the loop —
  unresolvable paths are skipped cleanly without blocking the domain. Attribute names
  verified against probe_units_2026-02-04.json.
- units domain SpecTypeId access corrected to Python flat top-level members for Electrical/HVAC/Piping/Structural entries (instead of nested C#-style paths), enabling those discipline records to resolve and emit.
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
- view_category_overrides split into `view_category_overrides_model` and
  `view_category_overrides_annotation` partitions; `vco.include_controlled`
  coordination item removed; include state now sourced from
  `view_templates.include_vg_model` / `view_templates.include_vg_annotation`
- view_templates V/G include surface changed from a single `include_vg` flag
  to per-tab flags: `include_vg_model`, `include_vg_annotation`,
  `include_vg_analytical`

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
