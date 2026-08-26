# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 6 of 7
- Original line range: 2009-2409
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
  2009| ### Changed (breaking pipeline-contract)
  2010| - `segment_manifest.csv` and `run_registry.csv` no longer carry per-segment file
  2011|   membership as inline pipe-delimited columns (`export_run_ids`,
  2012|   `seed_export_run_ids`). For large populations these columns exceeded
  2013|   spreadsheet cell limits (Excel ~32,767 chars/cell, Google Sheets ~50,000
  2014|   chars/cell — confirmed offenders in the current corpus reached 59,271
  2015|   chars), and a viewer truncating a field mid-quote desyncs the CSV parser
  2016|   for every row after it. Membership now lives in a new normalized join
  2017|   table, `segment_membership.csv` (`segment_id,export_run_id,is_seed`), one
  2018|   row per (segment, file) pair, written alongside the other two files by
  2019|   `build_segment_manifest.py`. `segment_manifest.csv` keeps only scalar
  2020|   summary fields (`file_count`, `has_seed_file`, `population_hash`);
  2021|   `run_registry.csv` keeps `population_hash` only — neither file will ever
  2022|   again carry a variable-length filename list. `population_hash` is computed
  2023|   identically to before (still from the in-memory `eids` list, not a
  2024|   round-trip through any CSV), so it is unchanged for any segment given the
  2025|   same file population — skip-logic/staleness comparisons are unaffected.
  2026|   `tools/run_segment_orchestrator.py` gains a `--membership-file` flag
  2027|   (default: sibling of `--manifest-file`) and now sources every
  2028|   `export_run_ids`/`allowed_ids` lookup from `segment_membership.csv` instead
  2029|   of the retired manifest column. `_build_registry()`'s `new_files`/
  2030|   `removed_files` staleness-reason diffing now reads the prior run's
  2031|   population from `existing_membership` (loaded from a prior
  2032|   `segment_membership.csv`) instead of an `export_run_ids` field embedded in
  2033|   the prior `run_registry.csv` row; a registry rebuilt for the first time
  2034|   after this migration (no prior `segment_membership.csv` on disk) will show
  2035|   every current file as `new_files` with no `removed_files` on that one
  2036|   transitional run, since there is no prior per-segment file list to diff
  2037|   against — a one-time artifact of the migration, not an ongoing dual-path.
  2038|   No fallback to the old inline column was kept; this is a schema change, not
  2039|   an additive one.
  2040| 
  2041| ### Added
  2042| - `tools/compare_cross_segment.py` governance comparisons now fan out across
  2043|   enterprise/bc/client scope levels instead of routing everything through a
  2044|   single client-scoped grouping key. Scope level is derived per-row from
  2045|   which of `client_label`/`business_center_label` are populated (enterprise =
  2046|   neither; bc = business_center_label only; client = client_label only;
  2047|   project = both) — orthogonal to `governance_role`, and computed the same
  2048|   way for every comparison in this file.
  2049|   `discover_governance_chain()` gains four new directed pairwise comparison
  2050|   types, each an independent parallel edge (no fixed override precedence
  2051|   between enterprise/bc/client standards, since any one may or may not have
  2052|   adapted from another): `enterprise_to_project` (an enterprise-scoped
  2053|   Template/Container reaches every Project regardless of its own client/bc),
  2054|   `bc_to_project` (a bc-scoped Template/Container reaches only Projects in
  2055|   the same normalized business center), `enterprise_to_bc`, and
  2056|   `enterprise_to_client` (same-role, standard-to-standard — Template vs.
  2057|   Template or Container vs. Container, never mixed roles). All four are
  2058|   registered in `DIRECTED_TYPES` and `GOVERNANCE_STATE_DIRECTED_TYPES`;
  2059|   `enterprise_to_project`/`bc_to_project` are additionally registered in
  2060|   `DELTA_DIRECTED_TYPES` alongside `template_to_project`/`container_to_project`,
  2061|   since they are the same shape of comparison (standard reference vs. Project
  2062|   target) just at a different scope level. Generic/Generic-Host is
  2063|   deliberately excluded from this fan-out — it already pairs unconditionally
  2064|   against every Template/Container/Project via the pre-existing `generic_ids`
  2065|   loop, so a separate scope-scoped edge would be redundant.
  2066|   `run_pooled_comparison()` gains two new pool grains alongside the existing
  2067|   `(parent_segment_id, role, unit_system)` pool (now labeled `pool_scope=
  2068|   parent_sibling` in `cross_segment_pooled.csv`, a new column): `pool_scope=
  2069|   bc` pools `(business_center_label, role, unit_system)` ignoring
  2070|   client_label (whichever clients happen to have work in that bc), and
  2071|   `pool_scope=client` pools `(client_label, role, unit_system)` ignoring
  2072|   business_center_label (whichever bcs happen to have work for that client).
  2073|   These are genuinely different pools with different membership, not two
  2074|   views of the same pool — a segment can now appear in `cross_segment_pooled.csv`
  2075|   once per applicable pool grain. The per-pool containment/bundle computation
  2076|   itself is unchanged; it was extracted into a shared `_build_pooled_row()`
  2077|   helper so all three grains share one implementation.
  2078| - Segment staleness model extended (build_segment_manifest.py `_build_registry()`):
  2079|   `run_registry.csv` gains `export_run_ids` (persisted per-run member list, enabling
  2080|   next-run diffing) and `conformance_reference_mode` (currently always `"latest"` —
  2081|   compare_cross_segment.py always resolves reference segments dynamically against
  2082|   current output; a pinned/snapshot mode is deferred until Phase-2 baseline authority
  2083|   is established). When `population_hash` changes, the registry now records
  2084|   `new_files:<n>` and/or `removed_files:<n>` reason counts alongside the existing
  2085|   `population_changed` marker, diffed against the prior run's `export_run_ids`. A
  2086|   metadata edit that moves a file between segments (e.g. a corrected `client_label`)
  2087|   surfaces as `removed_files` on the old segment and `new_files` on the new one —
  2088|   no separate "metadata change" detection path was needed or added.
  2089| - `tools/run_segment_orchestrator.py --dry-run` now prints each pending segment's
  2090|   registry `notes` (the staleness reason) alongside its status.
  2091| - `tools/compare_cross_segment.py` now writes `comparison_registry.csv` after every
  2092|   run: one row per actually-recomputed (segment_a, segment_b, comparison_type,
  2093|   domain) work item, stamped with each side's `population_hash`/`last_run_utc`
  2094|   (read from `run_registry.csv`) and `computed_utc`. Keyed at the domain granularity
  2095|   matching `work_items`, not the coarser pair — a `--domain`-scoped invocation only
  2096|   recomputes one domain per pair, so stamping at pair granularity would silently
  2097|   mark every other domain on that pair "current" without having recomputed it,
  2098|   hiding real staleness from a later `--dry-run`. This is new tracking state
  2099|   only — comparisons are still always fully recomputed on every invocation; nothing
  2100|   is skipped based on this registry. `--dry-run` now looks up each discovered
  2101|   (pair, domain) work item against this registry and labels it `stale` (never
  2102|   computed, or either side's stamp has moved since — this is how a Template/
  2103|   Container reference re-running with new bundle output is surfaced as
  2104|   invalidating its downstream Project/Container comparisons, even though the
  2105|   target's own file population is unchanged) or `current`.
  2106| 
  2107| ### Fixed
  2108| - `tools/compare_cross_segment.py` `discover_governance_chain()`'s four
  2109|   scope-level fan-out edges (`enterprise_to_project`, `bc_to_project`,
  2110|   `enterprise_to_bc`, `enterprise_to_client`) group purely by scope level,
  2111|   ignoring `parent_segment_id` — the same class of bug just fixed in
  2112|   `run_pooled_comparison()`'s bc/client pool grains, but in the pairwise
  2113|   path. Verified against a real corpus manifest: 14 of 139 new-type pairs
  2114|   were a segment paired against its own `parent_segment_id` ancestor or
  2115|   descendant (e.g. an enterprise-scoped Template paired against a
  2116|   bc-scoped Template nested directly under it), which would inflate
  2117|   containment toward a false 1.0 the same way. `_build_ancestor_map()` and
  2118|   a shared `_is_lineage_related()` helper (used by both this fix and the
  2119|   pooled-comparison one) now exclude any such pair from all four edges.
  2120| - `tools/compare_cross_segment.py` `run_pooled_comparison()`'s new `bc`/`client`
  2121|   pool grains ignore `parent_segment_id` for grouping, so a collection-blank
  2122|   BC roll-up and its own collection-specific child (or any ancestor/descendant
  2123|   pair sharing the same normalized bc/client value) could land in the same
  2124|   pool. Since segments are hierarchical cuts of the same underlying file
  2125|   population, an ancestor's data is always a superset of its descendants' —
  2126|   pooling them as peers compared a segment against a pool already containing
  2127|   its own data, inflating `all_containment_focal_in_pool`/
  2128|   `used_containment_focal_in_pool` toward a false 1.0. A new `_build_ancestor_map()`
  2129|   walks each segment's `parent_segment_id` chain once per invocation;
  2130|   `run_pooled_comparison()` now excludes any segment in the focal segment's
  2131|   own ancestor/descendant lineage from its pool, for all three pool_scope
  2132|   grains.
  2133| - `tools/compare_cross_segment.py` `build_explicit_matrix_outputs()`'s pool
  2134|   matrix (`project_pool_containment_similarity_matrix.csv`) keyed cells only
  2135|   as `row_id -> peer_pool:<row_id>` by view/domain, ignoring `pool_scope`.
  2136|   Since a project can now emit one pooled row per applicable `pool_scope`
  2137|   grain (`parent_sibling`/`bc`/`client`), different grains for the same
  2138|   project collided on identical matrix coordinates with different values.
  2139|   `column_id` now folds in `pool_scope` (`peer_pool:<pool_scope>:<row_id>`)
  2140|   so each grain gets its own cell.
  2141| - `tools/compare_cross_segment.py` `_normalize_bc_label()` only blanked empty
  2142|   strings and the `"0000"`/`"BC_0000"` bookkeeping tokens, dropping the
  2143|   `is_blank_or_na()` NA-token handling (`n/a`, `NA`, `__NOT_APPLICABLE__`, ...)
  2144|   that `discover_governance_chain()`'s `_key()` previously relied on for its
  2145|   `business_center_label` fallback. An NA-spelled bc was being treated as a
  2146|   real peer business center by `_bc_of()`, `_scope_level()` (misclassified as
  2147|   `"bc"` scope instead of `"enterprise"`), and the new bc-scoped
  2148|   pooling/pairwise comparisons. Restored the `is_blank_or_na()` check
  2149|   alongside the bookkeeping-token check.
  2150| - `tools/compare_cross_segment.py` `discover_governance_chain()`'s `_key()`
  2151|   business_center_label fallback (used when `client_label` is blank/NA) no
  2152|   longer treats bookkeeping tags `"0000"`/`"BC_0000"` (any case) as a real
  2153|   peer business center. Those values mean "enterprise work, tagged for
  2154|   bookkeeping," not a specific business center; grouping by the raw string
  2155|   would have silently pooled unrelated enterprise-wide rows together as if
  2156|   they shared one bc. The fallback now normalizes through the same
  2157|   `_bc_of()`/`_normalize_bc_label()` helpers the new enterprise/bc/client
  2158|   scope-level fan-out uses, falling through to the collection/blank
  2159|   fallbacks below when the tag normalizes to blank. Existing fixtures using
  2160|   `business_center_label="BC_0000"` alongside a populated `collection_label`
  2161|   are unaffected (they already grouped via collection_label, not
  2162|   business_center_label); a row with an unadorned `"0000"`/`"BC_0000"` and no
  2163|   collection_label now falls through to the blank-key fallback instead of
  2164|   pairing with unrelated same-tagged rows.
  2165| - `tools/compare_cross_segment.py` `comparison_registry.csv`: a (pair, domain) is now
  2166|   also omitted from the stamp if either side's `run_registry.csv` `status` is not
  2167|   `"complete"`. `build_segment_manifest.py` updates `population_hash` to a segment's
  2168|   new file population immediately on manifest rebuild, resetting `status` to
  2169|   `"pending"` (and clearing `last_run_utc`) until the orchestrator actually re-runs
  2170|   that segment — but its output folder on disk still holds the *old* population's
  2171|   results until then. A compare run in that window read the stale on-disk data yet
  2172|   got stamped with the segment's already-updated (new) `population_hash`; once the
  2173|   segment reached `"complete"` with that same hash, a later `--dry-run` would have
  2174|   wrongly reported the pair as already current.
  2175| - `tools/compare_cross_segment.py` `comparison_registry.csv`: removed the carryover of
  2176|   prior (pair, domain) entries not recomputed this run, and stopped stamping work
  2177|   items that produced no output. Every other output this tool writes
  2178|   (`cross_segment_summary.csv` etc.) is a full `atomic_write_csv` replace from only
  2179|   the current invocation's rows, never a merge — so a `--domain`/`--segment`-scoped
  2180|   run sharing the same `--out-dir` as an earlier full run already destroys those
  2181|   other domains'/pairs' output rows. Carrying their old `comparison_registry.csv`
  2182|   stamp forward falsely claimed that data was still current. The registry is now a
  2183|   full snapshot of only what this invocation actually produced: a scoped run leaves
  2184|   every non-recomputed (pair, domain) with no row at all (correctly staleness-flagged
  2185|   on the next `--dry-run`), and a work item where `run_pair()`/`_run_pair_domain()`
  2186|   returned `None` with no governance-state rows either (e.g. below `--min-patterns`,
  2187|   or a within-project pair with no eligible file pairs) is omitted rather than
  2188|   stamped current for output that was never written.
  2189| - `build_segment_manifest.py` `_build_registry()`: the new `new_files`/`removed_files`
  2190|   reason diff reused the name `new_ids` for the per-segment export_run_id diff,
  2191|   shadowing the outer `new_ids` (the full eligible segment_id set) used later to
  2192|   compute `dropped_ids`. Any retained segment whose population changed left
  2193|   `new_ids` holding export_run_ids instead of segment_ids, so every other
  2194|   still-present segment was reported as removed from the registry with a false
  2195|   "review corresponding folders for manual cleanup" warning. Renamed the
  2196|   per-segment locals to `old_export_ids`/`new_export_ids` so they no longer
  2197|   collide with the outer set.
  2198| - line_patterns sig_hash policy corrected to segments_def_hash (sig_hash.v2):
  2199|   segments_norm_hash was incorrectly used as sig_hash basis — it belongs in join_hash only.
  2200|   sig_hash answers exact identity (scale variants are distinct records);
  2201|   join_hash answers governance equivalence (scale variants collapse to one pattern).
  2202|   Cross-domain reference joins (obj_style.pattern_ref.sig_hash, line_style.pattern_ref.sig_hash)
  2203|   were broken while norm_hash was in sig_hash; this change restores them.
  2204|   Downstream: re-run sig_hash stage only. Bundle/pattern/segment pipelines unaffected
  2205|   (they operate on join_hash exclusively).
  2206| 
  2207| ### Added
  2208| - `instance_count` and `is_sole_type_in_category` metadata fields added to
  2209|   `text_types`, `dimension_types` (all 7 splits), `arrowheads`, and `compound_types`
  2210|   (all 4 partitions — wall, floor, roof, ceiling). Both fields are additive metadata
  2211|   only — never in sig_hash, join_key, or identity_basis.items. Arrowheads emit
  2212|   `instance_count = None / not_applicable` (tick-mark reverse-lookup deferred).
  2213|   Enables compound placeholder condition `is_purgeable OR (is_sole_type_in_category
  2214|   AND instance_count == 0)` at pipeline/BI layer.
  2215| 
  2216| ### Fixed
  2217| - `view_filter_applications_view_templates` and `view_templates`: `GetIsFilterEnabled`
  2218|   now captured alongside `GetFilterVisibility` — a toggled-off filter is now
  2219|   distinguishable from a visible one.
  2220| - `view_category_overrides_model` / `view_category_overrides_annotation`:
  2221|   `GetCategoryHidden()` captured per category via `_category_hidden_item()` — template-
  2222|   hidden categories are now reflected in the override record.
  2223| - `object_styles_model`: material resolved to name + class hash (`obj_style.material_sig_hash`)
  2224|   via `_material_ref_item()`, not raw ElementId — cross-project stable.
  2225| - `identity`: `doc.IsWorkshared`, `app.VersionNumber`, `app.VersionName`,
  2226|   `app.VersionBuild` all captured and emitted.
  2227| - `view_templates`: `GetWorksetVisibility()` captured per user workset via
  2228|   `_append_workset_visibility()`.
  2229| 
  2230| ### Changed
  2231| - `units` domain expanded from 3 specs (length/area/volume) to 38 specs covering
  2232|   all Revit disciplines. Common additions: angle, slope, speed, time, mass_density,
  2233|   currency, rotation_angle, distance. Electrical: 7 specs. HVAC: 8 specs. Piping:
  2234|   5 specs. Structural: 7 specs. All specs are extracted for every document regardless
  2235|   of discipline — GetFormatOptions returns a valid FormatOptions object for all
  2236|   SpecTypeId specs on any live document. ITEM_Q_UNREADABLE paths are defensive
  2237|   fallbacks only and are not expected to fire in normal execution. SpecTypeId nested
  2238|   attribute paths are resolved once at function entry and filtered before the loop —
  2239|   unresolvable paths are skipped cleanly without blocking the domain. Attribute names
  2240|   verified against probe_units_2026-02-04.json.
  2241| - units domain SpecTypeId access corrected to Python flat top-level members for Electrical/HVAC/Piping/Structural entries (instead of nested C#-style paths), enabling those discipline records to resolve and emit.
  2242| - file_metadata.csv: `project_label` now extracted from Autodesk Docs:// central path
  2243|   (ACC projects only); blank for non-ACC paths
  2244| - line_patterns join key policy upgraded from `line_patterns.join_key.v2`
  2245|   (`line_pattern.segments_def_hash`) to `line_patterns.join_key.v3`
  2246|   (`line_pattern.segments_norm_hash`) to enforce scale-invariant structural identity;
  2247|   same kind sequence + ratio now collapses length-scaled variants into one pattern
  2248| - Bundle analysis `bundle_id` stability explicitly scopes hash identity to
  2249|   `(domain, scope_key, sorted_pattern_ids)`; identical pattern sets in different
  2250|   scope keys (for example `dimension_types` linear vs angular) intentionally
  2251|   receive different bundle IDs and are not cross-scope comparable
  2252| - `line_pattern.segments_norm_hash` is now computed automatically during flatten
  2253|   in `tools/run_extract_all.py` (no `--synthetic-domains line_patterns` flag required)
  2254| - line_patterns normalized token precision set to `.6f` (from `.9f`) after
  2255|   sensitivity sweep; decision now includes a documented ±2 decimal neighbor
  2256|   validation practice to confirm elbow stability over time
  2257| - view_category_overrides split into `view_category_overrides_model` and
  2258|   `view_category_overrides_annotation` partitions; `vco.include_controlled`
  2259|   coordination item removed; include state now sourced from
  2260|   `view_templates.include_vg_model` / `view_templates.include_vg_annotation`
  2261| - view_templates V/G include surface changed from a single `include_vg` flag
  2262|   to per-tab flags: `include_vg_model`, `include_vg_annotation`,
  2263|   `include_vg_analytical`
  2264| 
  2265| ### Fixed
  2266| - file_metadata.csv: re-running the pipeline now preserves existing non-empty
  2267|   `client_label` and `governance_role` values by `export_run_id`
  2268| - VCO `dflt_map` computation hoisted out of O(templates × categories) inner loop;
  2269|   `other_seconds` reduced from ~920s to ~9s on large files, total VCO time reduced ~73%
  2270| - FEC cache deduplication: all `(doc, View, instances)` collection sites normalized to
  2271|   `_VIEW_INSTANCES_CACHE_KEY`; redundant FEC calls reduced from 12 to 7 per run
  2272| - View instances cache pre-warm repositioned before `view_filter_applications_view_templates`,
  2273|   ensuring the cache is populated before any view-related domain runs
  2274| - `_timing` scope resolved via injection pattern (`run_fingerprint(doc, timing=None)`);
  2275|   timing report merge restored to correct location inside `run_fingerprint()`
  2276| 
  2277| ### Added
  2278| - file_metadata.csv: added `client_label` and `governance_role` columns
  2279|   (empty strings, manually curated)
  2280| - `TimingCollector.record_elapsed()` for hot-loop accumulation without per-iteration
  2281|   lock overhead
  2282| - VCO inner loop sub-timers: `vco.enumerate_categories`, `vco.get_param_ids`,
  2283|   `vco.get_category_overrides`, `vco.extract_graphics` — `other_seconds` is now
  2284|   attributable residual Python overhead rather than a black hole
  2285| - `total_serialization` and `total_run` timer scaffolding in runner; both correctly
  2286|   report 0.0 in written fingerprint (ordering constraint — captured in Dynamo summary
  2287|   surface instead)
  2288| 
  2289| ---
  2290| 
  2291| ### Changed (hash-breaking — full re-extraction required)
  2292| **Domain family splits (D-015):**
  2293| - `dimension_types` split into 7 domains: `dimension_types_linear`
  2294|   (Linear/LinearFixed/Angular/ArcLength), `dimension_types_angular`,
  2295|   `dimension_types_radial`, `dimension_types_diameter`,
  2296|   `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  2297|   `dimension_types_spot_slope`
  2298| - `object_styles` split into 4 domains by CategoryType tab:
  2299|   `object_styles_model`, `object_styles_annotation`,
  2300|   `object_styles_analytical`, `object_styles_imported`
  2301| - `fill_patterns` split into 2 domains by target:
  2302|   `fill_patterns_drafting`, `fill_patterns_model`. Solid fills
  2303|   (system defaults) excluded from both domains.
  2304| - `view_templates` split into 5 domains by ViewType group:
  2305|   `view_templates_floor_structural_area_plans`,
  2306|   `view_templates_ceiling_plans`,
  2307|   `view_templates_elevations_sections_detail`,
  2308|   `view_templates_renderings_drafting`,
  2309|   `view_templates_schedules`
  2310| 
  2311| **Arrowhead record class corrections:**
  2312| - Dot, Diagonal, Box, Loop, Elevation Target, Datum triangle record
  2313|   classes corrected to size-only (tick_size_in only). Previous hashes
  2314|   for these styles incorrectly included tick_mark_centered and
  2315|   heavy_end_pen_weight.
  2316| 
  2317| **object_styles join-key correction:**
  2318| - pattern_ref.kind record class gate removed. Was incorrect —
  2319|   pattern_ref.sig_hash moves to optional_items.
  2320| 
  2321| **Dimension type policy corrections:**
  2322| - Angular: witness_line_control added to required identity
  2323|   (confirmed active in UI for Angular, not previously included)
  2324| - Radial: radius_symbol_location and radius_symbol_text added
  2325| - Diameter: diameter_symbol_location and diameter_symbol_text added
  2326| - Spot families: shape-specific indicator and placement fields added
  2327| 
  2328| **System type exclusion:**
  2329| - Dimension type extractors now exclude system built-in types not
  2330|   accessible in the Revit UI (detected via id-based label fallback
  2331|   and family name gate). These types cannot be governed.
  2332| - Arrowhead extractor now excludes placeholder_missing records
  2333|   (unidentifiable system types).
  2334| - Domain routing bugs fixed: DiameterLinked/Alignment Station Labels
  2335|   excluded from dimension_types_diameter; Diameter types with
  2336|   SpotElevationFixed shape enum correctly routed to diameter domain
  2337|   via family name gate.
  2338| 
  2339| ### Added
  2340| - `policies/cross_domain_alignment_keys.json` — domain family registry
  2341|   and alignment key definitions
  2342| - `arrowhead.record_class` in coordination_items for all arrowhead records
  2343| - `lp.is_import` in coordination_items for line_patterns records
  2344| - `dim_type.domain_family` in coordination_items for all dimension type records
  2345| - `obj_style.category_type`, `obj_style.domain_family`, `obj_style.is_subcategory`
  2346|   in coordination_items for all object style records
  2347| - `vt.view_type_family`, `vt.view_type_raw` in coordination_items for all
  2348|   view template records
  2349| - `object_styles_annotation` now populates
  2350|   `ctx["object_style_annotation_row_key_to_sig_hash"]` for VCO baseline lookup
  2351| - View category overrides: `vco.include_controlled`, `vco.vg_category_type`,
  2352|   `vco.context_type` added to coordination_items (D-016)
  2353| - View category overrides: category 2 (latent overrides, V/G checkbox unchecked)
  2354|   now captured alongside category 1
  2355| 
  2356| ### Decisions captured
  2357| - D-015: Domain family architecture — split criteria, vocabulary, alignment key
  2358|   registry
  2359| - D-016: VCO scope — category 1 (template-controlled) and category 2 (latent)
  2360|   implemented; category 3 (view-local) deferred with hooks
  2361| 
  2362| ---
  2363| 
  2364| ### Changed (D-015 — Domain Family Split Architecture)
  2365| 
  2366| Domain scope redefined: four monolithic extractors split into 18 per-partition domains.
  2367| No hash values changed within any record class — this is a structural change only.
  2368| 
  2369| - **`object_styles`** split into `object_styles_model`, `object_styles_annotation`,
  2370|   `object_styles_analytical`, `object_styles_imported` — each covers one CategoryType.
  2371|   `require_domain` references updated to split names throughout.
  2372| 
  2373| - **`fill_patterns`** split into `fill_patterns_drafting`, `fill_patterns_model` —
  2374|   each covers one FillPatternTarget. Join-key policy updated to use `fill_pattern.target`
  2375|   (was `fill_pattern.target_id`) and `fill_pattern.grid_count` as co-required keys.
  2376| 
  2377| - **`dimension_types`** split into 7 per-shape domains (`dimension_types_linear`,
  2378|   `dimension_types_angular`, `dimension_types_radial`, `dimension_types_diameter`,
  2379|   `dimension_types_spot_elevation`, `dimension_types_spot_coordinate`,
  2380|   `dimension_types_spot_slope`). Shape discrimination now happens at domain-level
  2381|   (handled shapes frozenset). Shared helpers moved to `core/dimension_type_helpers.py`.
  2382| 
  2383| - **`view_templates`** split into 5 per-ViewType-family domains
  2384|   (`view_templates_floor_structural_area_plans`, `view_templates_ceiling_plans`,
  2385|   `view_templates_elevations_sections_detail`, `view_templates_renderings_drafting`,
  2386|   `view_templates_schedules`). Shared VG helpers in `core/vg_sig.py`.
  2387| 
  2388| - Dependency chain (`require_domain` calls) updated in `view_category_overrides`
  2389|   and runner to reference split domain names.
  2390| 
  2391| - Join-key policies updated: all split domains have flat per-domain policies.
  2392|   Arrowheads policy corrected: shape-gated keys moved from `explicitly_excluded_items`
  2393|   to `optional_items` to satisfy A3 validation rule.
  2394| 
  2395| ---
  2396| 
  2397| ### Removed
  2398| - Legacy hash infrastructure (pipe-delimited signatures) removed across all domains
  2399| - `REVIT_FINGERPRINT_HASH_MODE` environment variable (semantic mode now default and only mode)
  2400| - `domains/view_filters_deprecated.py` (unused, 741 lines)
  2401| - `core/canon.py`: deprecated `sig_val()` helper
  2402| - Phase-2 `semantic_keys` duplication in domain payloads
  2403| - Legacy context maps: `*_uid_to_hash_v2` (replaced by canonical `*_uid_to_hash`)
  2404| 
  2405| ### Changed
  2406| - All domains now emit only `hash_v2` as the canonical domain hash in runner contract output
  2407| - Context maps simplified: removed `_v2` suffix from semantic hash maps
  2408| - Contract building simplified: single semantic hash source instead of mode-dependent logic
  2409| 
```
