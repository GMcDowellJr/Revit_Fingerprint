# Cross-Domain Archetype Discovery Pipeline (`tools/archetype/`)

Status date: 2026-06-10
Scope: `tools/archetype/*.py` + `config/archetype/`
Audience: Phase-2+ analysis users building the Power BI archetype slicer

This pipeline answers: **"What governance archetype does this file behave
like?"** It joins identity items across domains (e.g. dimension types ->
arrowheads, wall types -> fill patterns, view filter applications -> view
filter definitions) to detect recurring cross-domain configurations, turns
those configurations into named **archetypes**, and classifies every file
against the promoted archetypes for use as a Power BI slicer dimension.

All outputs land under `Fingerprint_Out/archetype_analysis/`. All scripts
take `--repo-root` plus explicit `--<input>`/`--out*` overrides, support
`--dry-run`, and write CSV/JSON atomically (temp file + `os.replace`). Every
stage logs row counts and skip/availability counts to stderr with a
`[archetype:<stage>]` prefix.

---

## Two-pass methodology

1. **join_hash grain (stages 1-3)** — classification. Cross-domain links
   are resolved via `join_hash` (the cross-file identity unit; under the
   current bootstrap join policy `join_hash == sig_hash`).
2. **sig_hash grain (stage 4)** — validation. `archetype_validation*.csv`
   re-resolves the same fired signals to `sig_hash` to check whether files
   classified into the same archetype actually share the same underlying
   configuration ("coherence").

## Graceful degradation

- An edge with `available == false` in `reference_graph.json` produces a
  **null signal**, never an error. `build_cross_domain_items.py` skips it
  entirely; `compute_cross_domain_cooccurrence.py` records
  `n_*_unavailable == n_files_total` for it.
- A file with zero fired signals for an archetype gets **no classification
  row** for that archetype.
- An archetype whose **required** signals are all unavailable emits only a
  coverage-summary note (`"all_required_signals_unavailable"`), with
  `n_files_no_evidence == n_files_total` and no per-file rows.
- Missing optional inputs (e.g. `tests/output/vfd_dynamic_edges.csv`) are
  treated as empty, not as errors.

---

## Config

### `config/archetype/static_edges_seed.json`

Hand-maintained seed list of **structural** cross-domain edges (no
`available` flag — that's computed by stage 0). Each entry:

```json
{
  "edge_id": "...",
  "source_domain": "...",
  "source_field": "...",          // identity_items item_key (or [*]-indexed pattern)
  "target_domain": "...",
  "edge_type": "structural",
  "direction": "forward",
  "field_match": "exact" | "indexed",
  "requires_extraction": [...]     // human-visible caveats, e.g. hash-space mismatches
}
```

Edges were derived directly from `make_identity_item()` calls in the
relevant `domains/*.py` extractors (dimension_types, text_types,
object_styles, line_styles, view_filter_applications_view_templates,
compound_types/wall_types, materials, view_category_overrides_*).

### `config/archetype/archetype_definitions.json` (hand-maintained, not generated)

Reviewed/promoted archetype definitions consumed by stages 3 and 4. Only
entries with `"promoted": true` are used. Produced by hand-editing/curating
the output of `generate_archetype_candidates.py`
(`archetype_definitions_candidates.json`) — copy promising candidates in,
set `governance_question` / `approach_label`, flip `promoted` to `true`.

---

## Stage 0 — Build the reference graph

### `generate_reference_graph.py`

**Purpose**
Resolve which cross-domain edges are actually backed by data in this
export, and merge in dynamically-discovered View Filter Definition (VFD)
edges.

**Inputs**
- `config/archetype/static_edges_seed.json`
- `Fingerprint_Out/identity_items_by_domain/*.csv`
- Optional: `tests/output/vfd_dynamic_edges.csv`,
  `tests/output/vfd_param_inventory.csv`, `tests/output/bip_lookup.json`,
  `tests/output/shared_param_names.json`

**Output**
- `Fingerprint_Out/archetype_analysis/reference_graph.json`
  (`{schema_version, generated_utc, support_threshold, edge_count, edges:[...]}`)

**What it does**
- For each static edge, checks whether `source_field` appears as
  `item_key` in the source domain's identity_items shard with at least one
  row carrying a usable value (`item_value` not a sentinel and
  `item_value_type == "ok"`); sets `available` + evidence counts
  accordingly.
- Builds **dynamic** edges from `vfd_dynamic_edges.csv`: drops
  `kind == "project"` rows, applies `--support-threshold` (default 10) on
  `file_count`, groups by `(param_id, target_domain)` into
  `scope_conditions.{param_ids, category_ids}`. Param names resolve via
  `bip_lookup.json` (for `bip:`-prefixed ids) or `shared_param_names.json`,
  normalized into `edge_id = "vfd__{target_domain}.{param_name_normalized}"`.

**Typical command**
```
python tools/archetype/generate_reference_graph.py --repo-root .
```

---

## Stage 1 — Build per-file cross-domain join rows

### `build_cross_domain_items.py`

**Purpose**
Materialize every place a (file, edge) pair actually fires, with the
join hashes needed for downstream co-occurrence and classification.

**Inputs**
- `Fingerprint_Out/archetype_analysis/reference_graph.json`
- `results/records/records.csv`
- `Fingerprint_Out/identity_items_by_domain/*.csv`

**Output**
- `Fingerprint_Out/archetype_analysis/cross_domain_items.csv`
  (`export_run_id, edge_id, source_domain, target_domain, source_record_pk,
  source_join_hash, target_ref_value, target_join_hash`)

**What it does**
- Skips edges with `available == false` entirely.
- **Structural edges**: filters identity_items where `item_key` matches
  `source_field` (exact or `[*]`-indexed) and the value is usable. Joins
  `records.csv` on `(export_run_id, domain=source_domain, record_pk)` for
  `source_join_hash`, and again on `(domain=target_domain, sig_hash=item_value)`
  for `target_join_hash`.
- **Dynamic VFD edges**: per record, requires a `vf.rule[*].param_ref.id`
  item matching `scope_conditions.param_ids` AND a `vf.categories` item
  (comma-separated category ids) intersecting `scope_conditions.category_ids`.
  `target_join_hash` is empty for dynamic edges.

**Typical command**
```
python tools/archetype/build_cross_domain_items.py --repo-root .
```

---

## Stage 2 — Compute co-occurrence and patterns

### `compute_cross_domain_cooccurrence.py`

**Purpose**
Find which pairs of cross-domain edges tend to fire together, and which
specific `join_hash` pairs recur often enough to be candidate archetypes.

**Inputs**
- `Fingerprint_Out/archetype_analysis/cross_domain_items.csv`
- `Fingerprint_Out/archetype_analysis/reference_graph.json`

**Edge aliasing**
Before pairs are enumerated, edges are collapsed onto a canonical edge_id
where they represent the same underlying join in different partitions:
- Edges whose `target_domain` is a `_drafting`/`_model` partition of the
  same prefix and that share `(source_domain, source_field)` are collapsed
  onto the `_drafting` edge (e.g. `fill_patterns_drafting` /
  `fill_patterns_model`).
- The `dimension_types_{linear,angular,radial,diameter}.tick_mark__arrowheads`
  edges (same `source_field`/`target_domain`, different `source_domain`
  partitions) are collapsed onto `dimension_types_linear`.

Collapsed (non-canonical) edge_ids are dropped from the canonical edge set
before pairs are enumerated; their fired/join_hash data is merged into the
canonical edge.

**Edge pair eligibility**
Only canonical edge pairs satisfying one of the following are emitted (all
others produce no rows in either output CSV):
- `shared_target` — `edge_a.target_domain == edge_b.target_domain`
- `chain` — `edge_a.target_domain == edge_b.source_domain` or vice versa
- `whitelist` — the pair appears in `reference_graph.json`'s
  `whitelisted_pairs` (defaults to `[]` if absent)

**Outputs**
- `Fingerprint_Out/archetype_analysis/cross_domain_edge_pairs.csv` —
  edge-pair level: `n_both, n_a_only, n_b_only, n_neither,
  n_a_unavailable, n_b_unavailable, support_pct, jaccard,
  containment_a_in_b, containment_b_in_a, pair_eligibility_reason`
  (`shared_target` | `chain` | `whitelist`), computed for every **eligible**
  canonical edge pair (including unavailable ones, which contribute
  `n_*_unavailable == n_files_total`).
- `Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv` —
  `join_hash`-pair level, only for edge pairs with `n_both >=
  --support-min-files` (default 5). `pattern_id` is an order-independent
  md5 of the sorted `(edge_id, join_hash)` pair. `collapsed_edge_ids_a` /
  `collapsed_edge_ids_b` list (pipe-separated) any non-canonical edge_ids
  collapsed onto `edge_id_a`/`edge_id_b`.

**Typical command**
```
python tools/archetype/compute_cross_domain_cooccurrence.py --repo-root . --support-min-files 5
```

---

## Stage 2.5 — Generate candidate archetype definitions

### `generate_archetype_candidates.py`

**Purpose**
Turn recurring `(edge_id_a, edge_id_b)` patterns into draft archetype
definitions for human review/promotion.

**Inputs**
- `Fingerprint_Out/archetype_analysis/cross_domain_patterns.csv`
- `Fingerprint_Out/archetype_analysis/reference_graph.json`

**Output**
- `Fingerprint_Out/archetype_analysis/archetype_definitions_candidates.json`

**What it does**
- Groups pattern rows by `(edge_id_a, edge_id_b)`.
- Derives a `governance_question_hint` from the pair's target domains
  (checked in priority order):
  - `arrowhead_consistency` — both target domains == `"arrowheads"`
  - `wall_graphics` — either target domain contains `"wall_types"`
  - `fill_pattern_usage` — either target domain starts with `"fill_patterns"`
  - `line_pattern_usage` — either target domain == `"line_patterns"`
  - `view_filter_strategy` — either target domain == `"view_filter_definitions"`
  - otherwise `"unknown"`
- Emits one candidate per cluster: `archetype_id` containing a `CANDIDATE`
  marker, `promoted: false`, `auto_generated: true`, one signal stub per
  edge, and the top `--top-n-join-hash-pairs` (default 5) join_hash pairs
  by `file_count` for human inspection. Each signal stub's `join_hash` is
  seeded from the top-ranked (by `file_count`) `cross_domain_patterns.csv`
  row for that edge pair (`join_hash_a` for edge_id_a's signal, `join_hash_b`
  for edge_id_b's signal); `join_hash_populated` indicates whether that value
  is non-empty. This is a starting point for human review, not a hard filter.

**Typical command**
```
python tools/archetype/generate_archetype_candidates.py --repo-root . --top-n-join-hash-pairs 5
```

**Human step (manual, not scripted)**: review
`archetype_definitions_candidates.json`, copy/curate promising candidates
into `config/archetype/archetype_definitions.json`, set
`governance_question` / `approach_label`, and flip `"promoted": true`.

---

## Stage 3 — Classify files against promoted archetypes

### `assign_archetype_classifications.py`

**Purpose**
Produce the per-file archetype classification table that becomes the
Power BI slicer dimension.

**Inputs**
- `Fingerprint_Out/archetype_analysis/cross_domain_items.csv`
- `config/archetype/archetype_definitions.json` (only `promoted == true`)
- `Fingerprint_Out/archetype_analysis/reference_graph.json`
- `results/records/file_metadata.csv`

**Outputs**
- `Fingerprint_Out/archetype_analysis/archetype_classifications.csv` —
  one row per `(export_run_id, archetype_id)` with `confidence_tier`,
  `is_mixed`, `signals_fired`/`signals_absent`/`signals_null`,
  plus `client_label`, `governance_role`, `discipline_label`,
  `unit_system` from `file_metadata.csv`.
- `Fingerprint_Out/archetype_analysis/archetype_coverage_summary.json` —
  per-archetype `n_files_full` / `n_files_partial` / `n_files_no_evidence`
  and `unavailable_signal_ids`.

**What it does**
- Rebuilds the same edge alias map as Stage 2
  (`_common.build_edge_aliases`) from `reference_graph.json`. Promoted
  archetype signals reference **canonical** edge_ids (as emitted by Stage
  2/2.5); `cross_domain_items.csv` rows for edges collapsed onto a
  canonical edge_id (e.g. `dimension_types_angular.tick_mark__arrowheads` →
  `dimension_types_linear.tick_mark__arrowheads`) are folded into the
  canonical edge_id before evaluating signals, so files whose evidence came
  only through a collapsed edge are still classified. A canonical edge is
  `unavailable` only if it and every edge collapsed into it are
  `available == false`.

For each promoted archetype and each candidate file (any file where a
required signal's canonical edge fired at least once anywhere):
- Evaluates every signal as `unavailable` (edge unavailable),
  `fired` (edge active for this file, and `join_hash` filter — if any —
  matches the row's `source_join_hash`/`target_join_hash`), or `absent`.
- Emits a row only if **at least one required signal fired**.
- `confidence_tier = "Full"` if **all** required signals fired and
  **no** signal is unavailable; otherwise `"Partial"`.
- `is_mixed = true` when a file has more than one archetype row for the
  same `governance_question`.

**Typical command**
```
python tools/archetype/assign_archetype_classifications.py --repo-root .
```

---

## Stage 4 — Validate signal coherence

### `validate_archetype_signals.py`

**Purpose**
Sanity-check the join_hash-grain classifications by re-resolving fired
signals to `sig_hash` and measuring how consistent the underlying
configuration actually is across classified files.

**Inputs**
- `Fingerprint_Out/archetype_analysis/archetype_classifications.csv`
- `Fingerprint_Out/archetype_analysis/cross_domain_items.csv`
- `config/archetype/archetype_definitions.json` (only `promoted == true`,
  used to map `signal_id -> (edge_id, join_hash)`)
- `Fingerprint_Out/archetype_analysis/reference_graph.json` (edge alias map,
  same as Stage 2/3)
- `results/records/records.csv`

**Outputs**
- `Fingerprint_Out/archetype_analysis/archetype_validation.csv` — one row
  per `(archetype_id, signal_id)`:
  - `n_files_classified` — distinct files where the signal fired
  - `n_distinct_sig_hashes` — distinct resolved `sig_hash` values
  - `coherence_score = n_distinct_sig_hashes / n_files_classified`
  - `coherence_tier`: `Convergent` (`< 0.3`) | `Variable` (`0.3-0.8`) |
    `Fragmented` (`>= 0.8`)
  - `n_multi_instance_files` — files where the signal fired with more
    than one distinct `join_hash`
- `Fingerprint_Out/archetype_analysis/archetype_validation_detail.csv` —
  forensic detail, one row per `(export_run_id, archetype_id, signal_id,
  source_join_hash)` with the resolved `sig_hash` and
  `n_join_hashes_in_file`.

**What it does**
- Indexes `cross_domain_items.csv` by `(export_run_id, canonical edge_id)`
  using the same alias map as Stage 2/3, so collapsed-edge rows are folded
  into the canonical edge_id's row set.
- For signals with a non-null `join_hash` filter, restricts that row set to
  rows whose `source_join_hash` or `target_join_hash` matches the filter
  **before** counting `n_distinct_sig_hashes`/`n_multi_instance_files` and
  resolving `sig_hash` — otherwise an unrelated second instance of the same
  edge in a file (e.g. another dimension type or material) would inflate
  those metrics for files that have more than one instance of the edge.
- Joins `records.csv` on `(export_run_id, domain=source_domain,
  join_hash=source_join_hash)` to resolve `sig_hash`.
- Low `coherence_score` = files in this archetype tend to share the same
  underlying signal configuration (good for a "standard" archetype). High
  scores indicate the signal varies a lot even among files classified the
  same way — a candidate for archetype refinement or splitting.

**Typical command**
```
python tools/archetype/validate_archetype_signals.py --repo-root .
```

---

## Running the full pipeline

```bash
python tools/archetype/generate_reference_graph.py --repo-root .
python tools/archetype/build_cross_domain_items.py --repo-root .
python tools/archetype/compute_cross_domain_cooccurrence.py --repo-root .
python tools/archetype/generate_archetype_candidates.py --repo-root .
# --- human review/promotion of config/archetype/archetype_definitions.json ---
python tools/archetype/assign_archetype_classifications.py --repo-root .
python tools/archetype/validate_archetype_signals.py --repo-root .
```

## Shared helpers

### `_common.py`

Internal module (not a CLI entrypoint) shared by all stages:
- `read_csv_rows`, `read_json`, `atomic_write_csv`, `atomic_write_json` —
  IO with the same atomic-write pattern as `tools/compare_cross_segment.py`.
- `is_valid_item(item_value, item_value_type)` — sentinel/quality filter
  for identity_items rows.
- `field_matches(item_key, source_field, field_match)` — exact or
  `[*]`-indexed item_key matching.
- `build_edge_aliases(edges_by_id)` / `strip_partition_suffix(target_domain)`
  / `DIM_TYPE_VARIANTS` — the edge-collapse alias map shared by Stages 2-4
  (fill_patterns drafting/model partitions, dimension_types tick_mark
  variants).
- `slugify(value)` — used to build `CANDIDATE__...` archetype ids.
- `log(stage, msg)` — `[archetype:<stage>]`-prefixed stderr logging.
