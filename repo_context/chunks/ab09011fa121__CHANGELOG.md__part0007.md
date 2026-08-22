# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 7 of 7
- Original line range: 2410-2477
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
  2410| ### Added
  2411| - Root governance docs: `INVARIANTS.md`, `ARCHITECTURE.md`, `DECISIONS.md`.
  2412| - **NEW DOMAINS (M4):**
  2413|   - `view_filter_definitions` - Global domain capturing filter definitions (rules, categories)
  2414|   - `phases` - Global domain capturing phase inventory and sequence (names included in hash per D-010 revised)
  2415|   - `phase_filters` - Global domain capturing phase filter settings (New/Existing/Demolished/Temporary visibility)
  2416|   - `phase_graphics` - Global domain capturing phase graphic override settings (disabled per D-013)
  2417| - Context dictionary (`ctx`) now populated by global domains:
  2418|   - `filter_uid_to_hash` - Mapping of view filter UIDs to definition hashes
  2419|   - `phase_uid_to_hash` - Mapping of phase UIDs to definition hashes
  2420|   - `phase_filter_uid_to_hash` - Mapping of phase filter UIDs to definition hashes
  2421| - **Canonical evidence selectors (PRs #106–#119):** All 15 domain extractors migrated to policy-driven join-key and sig-hash composition via `build_join_key_from_policy()`. Each domain now emits `join_key`, `join_hash`, and `sig_basis` fields in records, derived from `identity_basis.items` per the join-key policy.
  2422| - **Element traceability (PR #126):** `source_element_id` and `source_unique_id` added to `phase2.unknown_items` across all element-backed domains.
  2423| - **Timing instrumentation (PR #127):** `core/timing_collector.py` added for extraction profiling. Runner emits `timings.json` sibling artifact.
  2424| 
  2425| ### Changed
  2426| - **BREAKING: View Templates (M5):** Moved from name-only presence hashing to behavior-based hashing
  2427|   - Template identity: Now uses UniqueId (was: name)
  2428|   - Template hash: Now derived from controlled behavior (was: name presence)
  2429|   - Behavioral inputs: view type, detail level, scale, discipline, phase, phase filter, view filters (ordered), display style
  2430|   - Names: Now metadata-only (excluded from hash per D-008)
  2431|   - Filter stack: Order-sensitive (preserved)
  2432|   - References global domains: filters, phases, phase_filters via context
  2433|   - record_rows emitted with per-template sig_hash
  2434| - Execution order now enforces dependency: global domains run before contextual domains.
  2435| - **record_id stabilization (PR #123):** `record_id` generation made deterministic across runs using domain + identity_basis hash.
  2436| - **Join-key deduplication (PR #125):** `join_key.items` no longer duplicates `k/q/v` triples already present in `identity_basis.items`; join_key references the canonical source.
  2437| - **Object_styles shape-gating (PR #124):** Join-key policy uses `obj_style.pattern_ref.kind` as discriminator; `ref` shape requires `pattern_ref.sig_hash`, `solid` shape does not.
  2438| 
  2439| ### Semantic Rules Applied
  2440| - **View Filters:** Filter rules are order-sensitive (preserved), categories are sorted
  2441| - **Phases:** Phase names are included in behavioral hashes for cross-project comparability (D-010 revised), sequence number captured where available
  2442| - **Phase Filters:** Settings are order-insensitive (sorted before hashing)
  2443| - **Phase Graphics:** Intentionally disabled — API does not expose graphic overrides (D-013)
  2444| - **View Templates (M5):**
  2445|   - Template names: metadata-only (per D-008)
  2446|   - Filter stack: order-sensitive (filter application order matters)
  2447|   - Other settings: order-insensitive (sorted)
  2448|   - Global references: uses hashes from filters/phases/phase_filters domains
  2449|   - Unreadable templates: fail-soft with explicit markers
  2450| 
  2451| ### Decisions captured
  2452| - Nested fenced code blocks are prohibited in documentation (portability rule).
  2453| - View filters are global definitions referenced by views and view templates.
  2454| - Phase filters and phase graphic overrides are global.
  2455| - Phase names ARE included in behavioral hashes (D-010 revised for cross-project comparability).
  2456| - Phase sequence number is included in phase signatures to capture ordering.
  2457| - Hash mode migration timeline completed (D-014).
  2458| 
  2459| ---
  2460| 
  2461| ## 2025-12-17
  2462| 
  2463| ### Added
  2464| - Documented invariants: deterministic hashes, behavior-first, fail-soft, explicit ordering rules.
  2465| - Documented architecture layering: core / domains / context / runner.
  2466| - Documented decision log to prevent drift and re-litigation.
  2467| 
  2468| ### Fixed
  2469| - Documentation formatting made portable across GitHub Mobile + Obsidian by avoiding nested fenced blocks.
  2470| 
  2471| ## 2026-08-20
  2472| 
  2473| ### Corrected
  2474| - Identity-aware generated packages now carry deterministic, path-safe
  2475|   enterprise-policy provenance.
  2476| - Removed maintained Stantec-branded narrative output and migrated the public
  2477|   promotion flag to `reuse_client_pool_is_enterprise` without a legacy alias.
```
