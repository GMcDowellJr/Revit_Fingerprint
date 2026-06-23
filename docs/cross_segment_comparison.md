# Cross-Segment Comparison

`tools/compare_cross_segment.py` measures join_hash overlap between segment pairs discovered automatically from the manifest hierarchy. It answers governance questions about template efficacy, container displacement, sibling convergence, and within-project consistency — without re-running any pipeline stage.

---

## 1. Purpose and Analytical Questions

| Question | Mode |
|----------|------|
| Do project files contain the patterns a template mandates? | Template→Project containment (Mode A/E) |
| Does a container's content derive from the template? | Template→Container containment (Mode A/E) |
| Are sibling segments (same role, same parent) converging over time? | Sibling Jaccard (Mode B) |
| Do peer level-2 segments from the same level-1 parent share governance patterns? | Parent-sibling Jaccard (Mode C) |
| How consistent are files within the same project? | Within-project Jaccard (Mode D) |
| Is Generic / Generic-Host stock flowing into downstream standards and projects? | Generic→Template / Generic→Container / Generic→Project (Mode E) |
| Is the template driving pattern adoption across the governance chain? | Full governance chain (Mode E) |

**attribution_gap**: The fraction of project-bundle join_hashes that do *not* appear in the reference template union. High values indicate locally invented (non-governed) patterns.

**phantom_governance**: A template-to-project containment_b_in_a near 1.0 but containment_a_in_b near 0.0 signals the template has patterns the projects never use — governance rules that exist on paper but not in practice.

### All-view vs used-view governance semantics

The workflow separates the provision chain from the usage chain:

* Provision chain: `Generic / Generic-Host → Template → Container → Project all`.
* Usage chain: `Project all → Project used`.

All-view is the full configured vocabulary for a segment. Used-view is the vocabulary after excluding conclusively purgeable records. Used/non-purgeable is meaningful primarily for Project targets where it represents active delivery practice. Generic, Template, and most Container roles are provided-vocabulary references; their used-view values, when present, are annotations and must not be used to call standards stock “unused bloat.”

---

## 2. Comparison Types

| comparison_type | Direction | Side A | Side B | Primary metric | Governance question |
|---|---|---|---|---|---|
| `generic_to_template` | Directed | Generic / Generic-Host segment | Template segment | all-view `provided_to_configured_containment` | Which Generic stock patterns are configured in templates? |
| `generic_to_container` | Directed | Generic / Generic-Host segment | Container segment | all-view `provided_to_configured_containment` | Which Generic stock patterns are configured in containers? |
| `generic_to_project` | Directed | Generic / Generic-Host segment | Project segment | `provided_to_configured_containment`; project `provided_to_used_containment` | Which Generic stock patterns reach project configuration and active project use? |
| `template_to_project` | Directed | Template segment | Project segment | `provided_to_configured_containment`; project `provided_to_used_containment` | What fraction of template patterns appear in project all-view and used-view vocabularies? |
| `template_to_container` | Directed | Template segment | Container segment | all-view `provided_to_configured_containment` | Does the container inherit template patterns? |
| `container_to_project` | Directed | Container segment | Project segment | `provided_to_configured_containment`; project `provided_to_used_containment` | Does the project configure and actively use the container's patterns? |
| `parent_sibling_roles` | Directed | Template-role level-2 | Project-role level-2 | `containment_b_in_a` | Template efficacy at peer level within the hierarchy |
| `sibling_templates` | Symmetric | Template segment | Template segment | `jaccard_mean` | Are template siblings converging? |
| `sibling_projects` | Symmetric | Project segment | Project segment | `jaccard_mean` | Are project siblings consistent? |
| `sibling_containers` | Symmetric | Container segment | Container segment | `jaccard_mean` | Are container siblings aligned? |
| `within_project` | Symmetric | File within segment | File within segment | `jaccard_mean` | Are files from the same project consistent with each other? |
| `governance_chain` | Directed | Template / Container | Project / Container | `containment_b_in_a` | End-to-end governance chain coverage |

Directed pairs use containment metrics; symmetric pairs use Jaccard. Both are always computed at the join_hash level, not pattern_id level.

---

## 3. Measurement Architecture

All comparisons use a single measurement path: file-grain join_hash inventories
loaded from `membership_matrix.csv` per segment. There is no bundle-mode /
file-mode branch. All set operations (Jaccard, containment) operate on the full
join_hash inventories.

**Two views are scored independently:**

- **All-view** (`all_jaccard_*`, `all_containment_*`) — full configured pattern
  vocabulary per segment.
- **Used-view** (`used_jaccard_*`, `used_containment_*`) — patterns present in
  active view/sheet assignments only (excluding conclusively purgeable records).

The delta between all-view and used-view scores quantifies passive inheritance:
patterns configured but never rendered in delivery. `recommended_primary_view`
and `comparison_role_semantics` provide guidance on which view is meaningful for
each comparison type.

**Bundle membership is a post-hoc annotation**, not a scoring input. After scores
are computed, `bundle_membership.csv` is consulted to split `n_shared_join_hash`
into three buckets: `n_shared_bundle_both`, `n_shared_bundle_a_only`,
`n_shared_bundle_b_only`. This surfaces how much of the overlap is formally
institutionalised vs. informally shared. Separate `all_*` and `used_*` buckets
are emitted for both views.

Segments with `run_type = "skip"` or `"registration"` in the registry have no
bundle output by design; they participate using `membership_matrix.csv` when
present and are otherwise skipped with a warning.

---

## 4. join_hash Resolution

`pattern_id` values are segment-local identifiers assigned during population analysis. They are not stable across segments and cannot be compared directly.

`join_hash` is the cross-segment identity. It is extracted from `domain_patterns.csv`:

```
join_hash = source_cluster_id.split("|")[-1]
```

The resolution dict `{pattern_id → join_hash}` is built per (segment, domain) and cached for the lifetime of the run. Patterns with a blank `source_cluster_id` are skipped with a warning to stderr — they cannot participate in cross-segment comparison.

All set operations (union, intersection, Jaccard) are performed on join_hash values. The `n_patterns_a` / `n_patterns_b` columns in the output count distinct join_hashes, not pattern_ids.

---

## 5. Aggregation Approach

### Directed pairs — reference union semantics

The reference side (Template or Container) collapses all its bundles (or files) into a single union of join_hashes. This represents the full behavioral mandate of the reference.

Each target unit (bundle or file) is then scored individually:

- `containment_b_in_a`: `|target_jh ∩ reference_union| / |reference_union|` — what fraction of the mandate appears in this target unit.
- `containment_a_in_b`: `|target_jh ∩ reference_union| / |target_jh|` — what fraction of the target's patterns come from the reference mandate.

Summary columns (`_mean`, `_min`) aggregate across all target units.

Union semantics on the reference side are correct because a template mandates an *or* across its bundles: any governed project should contain patterns from *somewhere* in the template, not necessarily from one bundle.

### Symmetric pairs — pairwise Jaccard

All cross-group pairs are enumerated. For each pair:

```
jaccard = |A ∩ B| / |A ∪ B|
```

Summary columns report mean, P10, and P90 across all pairs. P10/P90 bound the distribution — a high mean with low P10 indicates some outlier pairs pulling the group apart.

When `n_pairs ≤ 50`, every individual pair is also written to `cross_segment_file_pairs.csv` with per-file containment values in both directions.

---

## 6. Output Schema

Three CSV files are written to `--out-dir`:

- **`cross_segment_summary.csv`** — one row per (segment_a, segment_b, domain, comparison_type)
- **`cross_segment_file_pairs.csv`** — individual file pair detail rows when n_pairs ≤ 50
- **`cross_segment_delta.csv`** — one row per delta join_hash for directed pairs (suppressed by `--no-delta`)

### cross_segment_summary.csv

One row per (segment_id_a, segment_id_b, domain, comparison_type).

| Column | Description |
|--------|-------------|
| `comparison_run_id` | `cmp_<sha1[:12]>` of seg_a + seg_b + executed_utc |
| `segment_id_a` | Left segment identifier |
| `segment_id_b` | Right segment identifier |
| `segment_label_a/b` | Human-readable segment labels from manifest |
| `governance_role_a/b` | Role values from manifest (Template / Project / Container) |
| `client_label_a/b` | Client scope from manifest |
| `discipline_label_a/b` | Discipline annotation (may be blank) |
| `unit_system` | Unit system; always matches between a and b |
| `comparison_type` | One of the 9 type values |
| `domain` | Domain name |
| `n_patterns_a` | Distinct join_hashes in segment A (union across all files) |
| `n_patterns_b` | Distinct join_hashes in segment B |
| `n_shared_join_hash` | Intersection size |
| `n_unique_patterns_a/b` | Deduplicated population-level join_hash counts per side |
| `signal_spread` | `(n_shared/min(a,b)) − (n_shared/max(a,b))` — score sensitivity to size asymmetry |
| `score_ambiguity_band` | One of: Unambiguous (≤0.1), Low (≤0.3), Moderate (≤0.6), High (>0.6) |
| `all_containment_a_in_b_mean` | Mean all-view fraction of A's patterns found in each B unit (directed only) |
| `all_containment_a_in_b_min` | Min across B units |
| `all_containment_b_in_a_mean` | Mean all-view fraction of B's mandate covered by each A unit (directed only) |
| `all_containment_b_in_a_min` | Min across A units |
| `all_jaccard_mean` | Mean pairwise Jaccard from all-view inventories (symmetric only) |
| `all_jaccard_p10` | P10 pairwise Jaccard, all-view (symmetric only) |
| `all_jaccard_p90` | P90 pairwise Jaccard, all-view (symmetric only) |
| `used_jaccard_mean` | Mean pairwise Jaccard from used-view inventories (symmetric only) |
| `used_jaccard_p10` | P10 pairwise Jaccard, used-view (symmetric only) |
| `used_jaccard_p90` | P90 pairwise Jaccard, used-view (symmetric only) |
| `used_containment_a_in_b_mean` | Mean used-view containment (directed only) |
| `used_containment_a_in_b_min` | Min across B units, used-view |
| `used_containment_b_in_a_mean` | Mean used-view containment (directed only) |
| `used_containment_b_in_a_min` | Min across A units, used-view |
| `used_n_shared_join_hash` | Count of join_hashes shared in both segments' used-view inventories |
| `all_has_bundles_a/b` | Whether all-view bundle analysis produced output for each side |
| `all_n_shared_bundle_both/a_only/b_only` | All-view bundle overlap annotation buckets |
| `used_has_bundles_a/b` | Whether used-view bundle analysis produced output for each side |
| `used_n_shared_bundle_both/a_only/b_only` | Used-view bundle overlap annotation buckets |
| `n_files_a/b` | File count for each side |
| `n_pairs` | Number of unit pairs that produced Jaccard values, or number of target units for directed |
| `data_sufficient` | `"true"` only when both sides have `n_files >= 5` |
| `reference_usage_interpretable` | Whether used-view is an active-practice signal for the A-side role |
| `target_usage_interpretable` | Whether used-view is an active-practice signal for the B-side role |
| `recommended_primary_view` | `all` or `used` — pipeline guidance on which view to use for this comparison type |
| `comparison_role_semantics` | Plain-language description of what the comparison is measuring |
| `executed_utc` | ISO-8601 UTC timestamp of the comparison run |

Columns that do not apply to a comparison direction are emitted as blank strings. For directed pairs: `jaccard_*` columns are blank. For symmetric pairs: `containment_*` columns are blank. Semantic columns (`reference_usage_interpretable`, `target_usage_interpretable`, `recommended_primary_view`, `comparison_role_semantics`) clarify when used-view scores are active-practice signals versus annotations.

### cross_segment_file_pairs.csv

Written only for (segment_a, segment_b, domain) triples where `n_pairs ≤ 50`.

| Column | Description |
|--------|-------------|
| `comparison_run_id` | Same ID as the corresponding summary row |
| `segment_id_a/b` | Segment identifiers |
| `domain` | Domain name |
| `export_run_id_a/b` | Individual file identifiers |
| `project_label_a/b` | Project label from file_metadata.csv (may be blank) |
| `n_patterns_a/b` | Join_hash count for each file |
| `n_shared` | Intersection count |
| `all_jaccard` | Pairwise Jaccard score, all-view |
| `all_containment_a_in_b` | Fraction of A's patterns in B, all-view |
| `all_containment_b_in_a` | Fraction of B's patterns in A, all-view |
| `used_n_shared` | Intersection count, used-view |
| `used_jaccard` | Pairwise Jaccard score, used-view |
| `used_containment_a_in_b` | Fraction of A's patterns in B, used-view |
| `used_containment_b_in_a` | Fraction of B's patterns in A, used-view |
| `all_n_shared_bundle_both/a_only/b_only` | All-view bundle overlap annotation buckets |
| `used_n_shared_bundle_both/a_only/b_only` | Used-view bundle overlap annotation buckets |

### cross_segment_governance_states.csv

Written for directed governance comparison types (`generic_to_template`, `generic_to_container`, `generic_to_project`, `template_to_project`, `template_to_container`, `container_to_project`). One row is emitted for each join_hash in `reference_all ∪ target_all`, so inherited-but-unused (`provided_but_passive`) and upstream-missing (`provided_but_missing`) states are visible and not limited to legacy target deltas. Governance-state rows are emitted independently of the legacy summary `--min-patterns` filter, so sparse or empty downstream targets can still report provided-but-missing stock. Bundle membership is target-side annotation (`is_bundle_member_target_all`, `is_bundle_member_target_used`) and Generic references do not need bundle output to participate as upstream vocabulary.

State values for Project targets include `provided_and_used`, `provided_but_passive`, `provided_but_missing`, `local_active`, `local_passive`, and `local_unbundled`. For Template, Generic, and most Container targets, `target_usage_interpretable=false`, `recommended_primary_view=all`, and configured inventory uses non-bloat labels such as `provided_configured` / `local_configured`.

### cross_segment_governance_state_summary.csv

One row per directed governance comparison/domain with counts and unambiguous shares for reporting: `provided_and_used_count`, `provided_but_passive_count`, `provided_but_missing_count`, `local_active_count`, `local_passive_count`, `local_unbundled_count`, plus directed metrics such as `provided_to_configured_containment`, `provided_to_used_containment`, `provided_passive_share`, `provided_missing_share`, and `local_active_share`. Provided-state percentages use `reference_all` as denominator; local active share uses `target_used` when available; local passive/unbundled shares use `target_all`. Used-derived summary shares (`provided_to_used_containment`, `provided_passive_share`, `local_active_share`, and matching used/passive percentages) are blank when `target_usage_interpretable=false` so Template/Generic/most Container stock is not summarized as passive bloat.

### cross_segment_delta.csv

Written for directed comparison types (`template_to_project`, `template_to_container`, `container_to_project`) when `--no-delta` is not set. One row per delta join_hash per (segment_pair, domain). Sorted by comparison_type → segment_id_reference → segment_id_target → domain → pct_files_in_target DESC → join_hash.

| Column | Description |
|--------|-------------|
| `comparison_run_id` | Same ID as the corresponding summary row |
| `segment_id_reference` | Reference segment (Template or Container side) |
| `segment_id_target` | Target segment (Project or Container side) |
| `segment_label_reference/target` | Human-readable labels from manifest |
| `comparison_type` | One of the three directed types |
| `domain` | Domain name |
| `join_hash` | The delta pattern's cross-segment identity |
| `pattern_label` | From target's `domain_patterns.csv`: `pattern_label_human` if populated, else `pattern_label`, else blank |
| `n_files_in_target` | Count of files in the target segment that carry this join_hash |
| `pct_files_in_target` | `n_files_in_target / total_files_in_target_segment`, 6 decimal places |
| `in_any_container` | `true` if this join_hash appears in any Container-role segment with matching unit_system |
| `in_any_template` | `true` if this join_hash appears in any Template-role segment with matching unit_system |
| `executed_utc` | ISO-8601 UTC timestamp |

---

## 7. CLI Reference

```bash
python tools/compare_cross_segment.py \
  --segments-root    segments/ \
  --records-dir      results/records/ \
  --out-dir          results/cross_segment/ \
  [--within-segment] \
  [--sibling-segments] \
  [--parent-siblings] \
  [--within-project] \
  [--governance-chain] \
  [--domain DOMAIN] \
  [--segment-a SEGMENT_ID] \
  [--segment-b SEGMENT_ID] \
  [--min-patterns INT] \
  [--dry-run] \
  [--no-delta]
```

### Flags

| Flag | Description |
|------|-------------|
| `--segments-root DIR` | **Required.** Base directory for resolving segment `output_folder` paths from `run_registry.csv`. |
| `--records-dir DIR` | **Required.** Directory containing `segment_manifest.csv`, `run_registry.csv`, and `file_metadata.csv`. |
| `results_registry.csv` | BI-facing registry generated from `segment_manifest.csv` + `run_registry.csv`; one row per segment with output folder, run type, status, and last run timestamp. It is produced by `tools/build_results_registry.py` and kept current by `tools/run_segment_orchestrator.py` when segments complete. |
| `--out-dir DIR` | **Required.** Output directory. Created if absent. |
| `--within-segment` | Mode A: child Template/Project/Container pairs within the same parent. |
| `--sibling-segments` | Mode B: same parent, same governance_role. All pairwise combinations. |
| `--parent-siblings` | Mode C: level-2 Template-vs-Project under the same level-1 parent. |
| `--within-project` | Mode D: per-segment file pairs grouped by `project_label`. |
| `--governance-chain` | Mode E: directed Generic/Generic-Host→Template/Container/Project by `unit_system` (and populated discipline), plus Template→Project/Container and Container→Project scoped by `client_label`. |
| `--domain DOMAIN` | Restrict all comparisons to a single domain name. |
| `--segment-a SEGMENT_ID` | Restrict the left side of all pairs to this segment. |
| `--segment-b SEGMENT_ID` | Restrict the right side of all pairs to this segment. |
| `--min-patterns INT` | Skip any (segment, domain) with fewer than N join_hashes. Default: 3. |
| `--dry-run` | Print discovered pairs and exit. No output files are written. |
| `--no-delta` | Skip delta pattern computation and `cross_segment_delta.csv`. Use for large corpora where delta detail is not needed. |

If no mode flag is specified, all five modes are enabled.

### Examples

```bash
# All modes, all domains
python tools/compare_cross_segment.py \
  --segments-root segments/ \
  --records-dir results/records/ \
  --out-dir results/cross_segment/

# Governance chain only, restricted to line_patterns domain
python tools/compare_cross_segment.py \
  --segments-root segments/ --records-dir results/records/ \
  --out-dir results/cross_segment/ \
  --governance-chain --domain line_patterns

# Dry-run to preview pairs without computing
python tools/compare_cross_segment.py \
  --segments-root segments/ --records-dir results/records/ \
  --out-dir results/cross_segment/ \
  --dry-run

# Single segment pair investigation
python tools/compare_cross_segment.py \
  --segments-root segments/ --records-dir results/records/ \
  --out-dir results/cross_segment/ \
  --segment-a seg_template_001 --segment-b seg_project_014 \
  --governance-chain

# Sibling convergence with raised min-patterns threshold
python tools/compare_cross_segment.py \
  --segments-root segments/ --records-dir results/records/ \
  --out-dir results/cross_segment/ \
  --sibling-segments --min-patterns 10
```

---

## 8. Interpretation Guide

### Template → Project (containment_b_in_a)

| Value | Interpretation |
|-------|----------------|
| ≥ 0.90 | Strong template adoption: the project bundle contains nearly all mandated patterns |
| 0.70–0.89 | Partial adoption: significant gaps — investigate which patterns are missing |
| < 0.70 | Weak adoption: project has drifted substantially from the template mandate |

`containment_b_in_a_min` reveals the worst-performing project bundle. A high mean but low min signals one problematic bundle pulling down overall governance coverage.

### Template → Project (containment_a_in_b)

High values (close to 1.0) mean most of what the project does comes from the template — the project adds little locally. Low values mean the project has invented many patterns outside the template's scope. Neither is inherently bad, but the combination with `containment_b_in_a` tells the story:

- High b_in_a + high a_in_b: tight, well-governed alignment
- High b_in_a + low a_in_b: project uses the template but also extends heavily
- Low b_in_a + high a_in_b: project is tiny and contains almost nothing from the template
- Low b_in_a + low a_in_b: project has drifted and is inventing independently (**governance failure**)

### Sibling Jaccard (sibling_templates, sibling_projects)

| Value | Interpretation |
|-------|----------------|
| ≥ 0.80 | Siblings are well-converged |
| 0.50–0.79 | Moderate divergence — may reflect intentional discipline splits |
| < 0.50 | Siblings have diverged significantly |

A wide P10–P90 spread (e.g., jaccard_p10 = 0.2, jaccard_p90 = 0.9) indicates that some pairs within the sibling group are well-aligned while others are not — possibly reflecting different project phases or disciplines being mixed into one governance role.

### Within-Project Jaccard

Within-project Jaccard measures how consistent files from the same named project are with each other. Values near 1.0 indicate the project has stable, repeatable configuration. Values below 0.5 may indicate version churn, discipline-specific overrides, or configuration drift within a single project.

### attribution_gap concept

For a directed comparison, `attribution_gap = 1.0 - containment_b_in_a_mean`. It represents the fraction of the reference mandate that target bundles do not cover. An attribution gap above 0.3 warrants investigation.

### phantom_governance concept

When `containment_b_in_a_mean` (template coverage in project) is low but `n_patterns_a` is large, the template mandates many patterns that projects never adopt. These patterns exist in the governance structure but have no downstream effect — phantom governance. To distinguish phantom governance from genuine adoption gaps, compare the template segment's `n_patterns_a` to sibling templates: if the pattern count is an outlier, the template may have accumulated stale or over-specified patterns.

---

## 9. Delta Pattern Output

### What delta patterns are

For a directed comparison pair (reference_segment → target_segment, domain):

```
reference_union_jh = union of all join_hashes in the reference segment
target_union_jh    = union of all join_hashes in the target segment
delta_jh           = target_union_jh − reference_union_jh
```

Each join_hash in `delta_jh` is a pattern present in the target that has no counterpart in the reference. Delta patterns are the explicit complement of template-in-project containment: a project with `containment_b_in_a_mean = 0.60` has delta patterns equal to 40% of the reference mandate, and `cross_segment_delta.csv` names every one of them.

Delta rows are only emitted for `template_to_project`, `template_to_container`, and `container_to_project` comparison types. Symmetric types (`sibling_*`, `within_project`) and `parent_sibling_roles` do not produce delta output.

### Interpretation — three categories

Each delta pattern falls into one of three categories based on `in_any_container` and `in_any_template`:

| in_any_container | in_any_template | Category | Meaning |
|---|---|---|---|
| `true` | `false` | Container-sourced enrichment | Governed elsewhere; the pattern exists in a container but was not adopted into this template |
| — | `true` | Governed by another template | Appears in a sibling or peer template; may indicate wrong template in use or cross-client convergence |
| `false` | `false` | Project-originated drift | Ungoverned configuration — no reference file in the corpus owns this pattern |

A pattern can be both `in_any_container=true` and `in_any_template=true`; both flags are independent lookups. Project-originated drift (both false) is the most actionable signal: it represents configuration accumulating outside any governance structure.

### Relationship to containment metrics

`pct_files_in_target` answers the follow-on question after the summary row: not just *that* a delta pattern exists, but *how widely* it is spread across the target's files. A delta pattern at `pct_files_in_target = 1.0` is present in every target file — it is a stable, repeatable non-governed addition. A delta pattern at `pct_files_in_target = 0.1` is rare and may represent a one-off outlier.

The total count of delta rows per (pair, domain) equals `n_patterns_b − n_shared_join_hash` from the summary row.

### Output schema

See section 6 for the full field-by-field description of `cross_segment_delta.csv`.

### --no-delta flag

Pass `--no-delta` when:
- The corpus is large and delta enumeration would produce an unmanageably large CSV
- Only summary metrics (`containment_*`, `jaccard_*`) are needed for the current analysis
- Delta computation is not yet applicable (e.g., early population mode before governance authority is established)

When `--no-delta` is set, `cross_segment_delta.csv` is not written and no role join_hash sets are built, which meaningfully reduces I/O for large segment populations.

---

## 10. Known Limitations

### Small-N caveats

Jaccard is noisy when either segment has fewer than ~5 files. The `--min-patterns` flag helps suppress the noisiest cases, but the threshold applies to join_hash count, not file count. A single file with 50 join_hashes passes the filter; two files with 3 join_hashes each also pass. Interpret results for segments with fewer than 5 files cautiously; `data_sufficient` flags this directly.

### discipline_label sparsity

`discipline_label` in `file_metadata.csv` is a new annotation column and may be blank for most files in early exports. Governance-chain matching ignores the field when either side is blank, which can produce cross-discipline pairs that look like valid template→project comparisons. As discipline labels are populated, re-running with the same flags will automatically tighten the matching.

### project_label as within-project grouping key

Mode D groups files by `project_label` from `file_metadata.csv`. `project_label` is a human-assigned string and may not uniquely identify a project when naming conventions vary across clients. Until a stable `project_id` field is populated in `file_metadata.csv`, treat within-project groupings as approximate. Files that share a `project_label` string are assumed to belong to the same project.

### Bundle analysis prerequisite

Bundle overlap annotation (`all_n_shared_bundle_*`, `used_n_shared_bundle_*`) requires that the segment orchestrator has completed step 6 (`bundle_membership.csv` production) for a segment. Segments where only step 1 ran will have `all_has_bundles_a/b` and `used_has_bundles_a/b` reported as `"false"` and the bundle-overlap buckets reported as zero; Jaccard and containment scores are unaffected since they are computed from `membership_matrix.csv` regardless of bundle-analysis completion.

### No cross-unit-system pairs

All pair discovery rules enforce matching `unit_system`. Imperial and metric segments are never compared. This is intentional — join_hashes for the same logical pattern differ between unit systems because behavioral hashes include unit-bearing values.

### cross_segment_union_inventory.csv

`cross_segment_union_inventory.csv` is an additive cross-segment output that makes the normalized union inventory explicit. It is emitted alongside the existing cross-segment comparison outputs and does not change `cross_segment_summary.csv`, `cross_segment_file_pairs.csv`, `cross_segment_pooled.csv`, governance-state outputs, fingerprints, hashes, or existing schemas.

#### Grain and identity

The file has one row per `(governance_role, client_label, discipline_label, unit_system, domain, view_scope, join_hash)`.

`join_hash` is the identity unit. The loader follows the existing normalized identity path:

`membership_matrix.csv.pattern_id → domain_patterns.csv.source_cluster_id.split("|")[-1] → join_hash`

Raw local `pattern_id` values are not used as cross-segment identities. Multiple files, segments, or local pattern ids that resolve to the same `join_hash` collapse into one union row for the same inventory grain.

#### Views

`view_scope=all` reports the configured/provided normalized inventory. `view_scope=used` reports rows only when a used-view membership source is available. Used-view is an active-practice signal primarily for `Project` role rows. When used-view rows exist for non-Project roles, `usage_interpretable=false` and `inventory_status=not_interpretable`; these rows must not be read as unused-bloat findings.

#### Count fields

- `n_segments_present`: number of runnable segments in the grain that contain the `join_hash`.
- `n_files_present`: number of files in the grain/view that contain the `join_hash`.
- `n_files_denominator`: total files with any pattern in the same role/client/discipline/unit/domain/view pool used as denominator for `pct_files_present`.
- `pct_files_present`: `n_files_present` divided by files with any inventory in that grain/view.
- `n_projects_present`: number of project labels represented by files containing the `join_hash`; when file metadata has no project label, the export run id is used as a stable fallback label for counting.
- `n_projects_denominator`: total projects with any pattern in the same role/client/discipline/unit/domain/view pool used as denominator for `pct_projects_present`.
- `n_clients_present`: number of clients containing the `join_hash` in the same role/discipline/unit/domain/view pool.
- `n_clients_denominator`: total clients with any pattern in the same role/discipline/unit/domain/view pool used as denominator for `pct_clients_present`.
- `pct_clients_present`: `n_clients_present` divided by clients with any inventory in that role/discipline/unit/domain/view pool.
- `pct_projects_present`: `n_projects_present` divided by projects represented by files with any inventory in that grain/view.

#### Status fields

- `inventory_status=ok`: normalized union rows were emitted and the view is interpretable for the role.
- `inventory_status=no_patterns`: domain pattern rows exist but no valid normalized `join_hash` inventory can be produced.
- `inventory_status=missing_domain_patterns`: the segment/domain is missing `domain_patterns.csv` input.
- `inventory_status=missing_membership_matrix`: reserved status for membership-required future producers.
- `inventory_status=used_view_unavailable`: used-view rows cannot be produced from available inputs without inference.
- `inventory_status=not_interpretable`: rows exist, but the role's used-view semantics are not active-practice semantics.
- `source_status=ok`: source rows resolved to canonical `join_hash` identities.
- `source_status=missing_source_cluster_id`: one or more source rows lacked `source_cluster_id`; no synthetic `join_hash` is invented.


### Pattern reuse distribution outputs

`pattern_reuse_distribution.csv` is an additive output derived from `cross_segment_union_inventory.csv`. It uses normalized `join_hash` identity only; raw local `pattern_id` values are not identity and are not used for classification.

The distribution grain is one row per `(view_scope, governance_role, client_label, discipline_label, unit_system, domain, join_hash)`. Percentages always carry explicit denominators:

- `pct_files_present` = `n_files_present / n_files_denominator`, where the denominator is files in the same role/client/discipline/unit/domain/view pool.
- `pct_projects_present` = `n_projects_present / n_projects_denominator`, where the denominator is projects in the same client/domain pool.
- `pct_clients_present` = `n_clients_present / n_clients_denominator`, where the denominator is clients in the same corpus/domain pool for the row's role/discipline/unit/view.

`reuse_bucket` values are neutral reporting classifications, not correctness, approval, or standards-compliance claims:

- `corpus_wide` — the pattern appears across at least the centralized client-share threshold for the corpus/domain pool.
- `client_wide` — the pattern appears across at least the centralized file-share threshold inside a client/domain pool.
- `multi_project` — the pattern appears in at least the centralized multi-project count threshold inside a client/domain pool.
- `single_project` — the pattern is limited to one project denominator context, even when it appears in multiple files in that project.
- `emerging` — the pattern appears in more than one file across a classified pool but does not meet broader or single-project thresholds.
- `single_file` — the pattern appears in exactly one file.
- `unclassified` — denominators are unavailable/zero or source inventory status blocks classification.

Thresholds are centralized in `tools/compare_cross_segment.py` as `REUSE_BUCKET_THRESHOLDS`. The current defaults are `corpus_wide_min_pct_clients=0.80`, `client_wide_min_pct_files=0.80`, `multi_project_min_projects=2`, and `emerging_min_files=2`.

Role interpretation follows the union inventory semantics. Project used-view rows can support active delivery practice reporting. Template, Generic, and most Container all-view rows are configured/published inventory, not active usage claims; their `usage_interpretable` field remains `false` where appropriate. Rows with zero or unavailable denominators are emitted with explicit degraded/unclassified status rather than false zero percentages. Rows with missing source identities (`source_status != ok`) are also degraded/unclassified, even when some valid `join_hash` values remain, because the observed denominators may exclude unresolved source rows.

Two additive summaries are also written from the distribution rows:

- `pattern_reuse_summary_by_domain.csv`
- `pattern_reuse_summary_by_client.csv`

## Explicit matrix/reporting outputs

`tools/compare_cross_segment.py` now emits matrix outputs with filenames that encode metric semantics instead of using a generic “similarity” label. The matrix outputs are behavior-changing reporting semantics: values are not interchangeable across files, and downstream reporting should select the matrix whose question matches the decision being made.

| Output | Metric | Source | Interpretation | What not to infer |
|---|---|---|---|---|
| `project_union_jaccard_matrix.csv` | `union_jaccard` | `cross_segment_union_inventory.csv` | Jaccard between normalized project-level `join_hash` unions. It answers whether project/client scopes contain or use the same canonical patterns. | Do not read this as typical file-to-file similarity. It can differ from mean file-pair Jaccard. Missing union inventory, or union inventory with no usable Project rows, blocks this output with an explicit status row. |
| `project_mean_file_pair_jaccard_matrix.csv` | `mean_file_pair_jaccard` | `cross_segment_summary.csv` | Existing-style mean of pairwise file comparisons. It answers whether individual files are typically similar across compared groups. | Do not treat this as union overlap or as a replacement for `union_jaccard`. |
| `project_density_similarity_matrix.csv` | `density_similarity` | `cross_segment_union_inventory.csv` | Cosine similarity over domain pattern-density vectors. It answers whether domains are populated to similar degrees. | It does not measure exact identity overlap. By definition, absent domains are treated as zero occupancy in the vector. |
| `project_pool_containment_similarity_matrix.csv` | `pool_containment_similarity` | `cross_segment_pooled.csv` | Focal-in-peer-pool containment. It answers how much each project system aligns with its existing manifest-derived peer pool. | Peer pools use only existing manifest sibling grain; no container authority or grouping taxonomy is inferred. |
| `project_fragmentation_diagnostic.csv` | `fragmentation_diagnostic` | union and mean-file-pair matrices | Diagnostic difference between footprint similarity and exact identity overlap when both inputs are available. | This is a diagnostic only, not a mathematically authoritative governance index. It is unavailable unless both required inputs are present. |
| `matrix_output_manifest.csv` | manifest | all matrix builders | Documents each matrix’s governance role, view scope, source grain, identity unit, aggregation method, interpretation, limitations, and execution timestamp. | The manifest is descriptive metadata, not a score table. |

The explicit matrix outputs distinguish `missing`, `unavailable`, `not applicable`, synthetic self-comparisons, and true zero-valued scores through `value_status` and blank values. Diagonal cells are synthetic self-comparisons (`self_comparison=true`) and are deterministic (`1.000000` for applicable similarity matrices), including added file-pair diagonal cells when only observed A→B project summary rows exist. File-pair diagonal cells are emitted only for domains observed for that project in the summary input, plus that project's `ALL_DOMAINS` aggregate; sparse domains are not filled with false available diagonals. Observed project file-pair cells are mirrored as B→A because mean Jaccard is symmetric and sibling discovery emits unordered pairs once. When a project union group can be mapped unambiguously to a human-readable project `segment_label` already present in project summary comparisons, union and density matrix row IDs use that label so matrix outputs can be joined across metrics; otherwise they fall back to the explicit `governance_role|client_label|discipline_label|unit_system` group key. Fragmentation diagnostics compare `ALL_DOMAINS` union Jaccard only to the deterministic `ALL_DOMAINS` aggregate of available domain-level mean file-pair Jaccard rows, rather than selecting an arbitrary domain row.

Container caution: this reporting layer does not introduce container authority rules. Container-specific matrices should only be added where existing metadata provides a defensible grain, and category-specific container libraries must not be interpreted as governance divergence solely from low file-to-file overlap.
