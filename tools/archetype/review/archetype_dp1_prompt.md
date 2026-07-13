# Archetype Decision Point 1 — Candidate Promotion
## Purpose
Decide which candidates to promote into `archetype_definitions.json`: set `governance_question`, `approach_label`, flip `promoted: true`.

## Input files (upload all three)
- `Fingerprint_Out/archetype_analysis/archetype_definitions_candidates.json`
- `Fingerprint_Out/archetype_analysis/reference_graph.json`
- `Fingerprint_Out/archetype_analysis/cross_domain_edge_pairs.csv`

## Task
For each candidate in `archetype_definitions_candidates.json`:

1. **Is the signal real?**
   - Check `min_signal_coverage_pct` — candidates with both signals below ~5% are noise; skip them.
   - Check `required` flags — if both signals are `required: false`, there is no reliable classification anchor; skip.
   - Cross-reference `cross_domain_edge_pairs.csv` on `(edge_id_a, edge_id_b)`: use `jaccard`, `n_both`, `containment_a_in_b`, `containment_b_in_a` to assess co-occurrence strength. Jaccard below ~0.10 on a shared-target pair is generally not worth promoting.
   - Check `reference_graph.json` for `available` status on each edge — unavailable edges produce null signals and cannot drive classification.

2. **Is the governance_question correct?**
   - The generator's hint logic can misfire. Specifically: any candidate whose signals are `view_filter_definitions → <element_type>` edges paired with each other or with `view_filter_applications_view_templates.stack_filter` should be `view_filter_strategy`, not `wall_graphics` or `unknown`, regardless of what `governance_question_hint` says.
   - VFD-to-VFD pairs within the same element type (ceiling, floor, roof, wall) are always `view_filter_strategy`.
   - `wall_graphics` is only correct for structural edges (e.g., `wall_types → fill_patterns`).

3. **What is the right `approach_label`?**
   - This is the human-readable slicer value Jon sees in Power BI.
   - Label the governance behavior, not the signal mechanism. Use language like "Wall Fire Rating Filter Bundle", "Matched Arrowhead Standard", "Stantec Line Pattern Standard" — not technical edge IDs.
   - Treat archetypes that will likely cluster together (same target domain, overlapping signals) as a naming family so their labels read coherently side-by-side in a slicer.

4. **Output**
   - Produce a revised `archetype_definitions.json` containing only the promoted archetypes (`promoted: true`).
   - Set `governance_question` and `approach_label` on each.
   - Strip `_coverage_pct` and `_low_coverage_flag` from signal stubs.
   - Keep all other fields from the candidate (signals, join_hash, top_join_hash_pairs, etc.) unchanged.
   - Use the wrapper format: `{ "schema_version": "1.0", "generated_utc": "...", "source": "...", "archetype_count": N, "archetypes": [...] }`

## Known skip conditions (from corpus experience)
- Any signal involving `object_styles_model.material__materials` with `_coverage_pct = 0.0` — deferred pending materials join_hash PR.
- `vfd__wall_types.do_not_edit_acoustic_batts`, `partition_series`, `all_model_manufacturer`, `all_model_instance_comments` — coverage ≤ 1.6%, custom/niche parameters.
- `vfd__roof_types.fire_label__4adcd90c` — 3.5% coverage, low jaccard.
- `vfd__*_types.door_fire_rating` paired with fire_label signals — zero n_both in edge pairs.
- Any candidate where both signals have `required: false` — all-optional archetypes classify as Partial only and are weak governance signals.
- `wall_types.coarse_fill_pattern__fill_patterns_drafting` chain edges — near-zero jaccard in edge pairs.
