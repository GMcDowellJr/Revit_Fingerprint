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
| Is the template driving pattern adoption across the governance chain? | Full governance chain (Mode E) |

**attribution_gap**: The fraction of project-bundle join_hashes that do *not* appear in the reference template union. High values indicate locally invented (non-governed) patterns.

**phantom_governance**: A template-to-project containment_b_in_a near 1.0 but containment_a_in_b near 0.0 signals the template has patterns the projects never use — governance rules that exist on paper but not in practice.

---

## 2. Comparison Types

| comparison_type | Direction | Side A | Side B | Primary metric | Governance question |
|---|---|---|---|---|---|
| `template_to_project` | Directed | Template segment | Project segment | `containment_b_in_a` | What fraction of template patterns appear in each project bundle? |
| `template_to_container` | Directed | Template segment | Container segment | `containment_b_in_a` | Does the container inherit template patterns? |
| `container_to_project` | Directed | Container segment | Project segment | `containment_b_in_a` | Does the project use the container's patterns? |
| `parent_sibling_roles` | Directed | Template-role level-2 | Project-role level-2 | `containment_b_in_a` | Template efficacy at peer level within the hierarchy |
| `sibling_templates` | Symmetric | Template segment | Template segment | `jaccard_mean` | Are template siblings converging? |
| `sibling_projects` | Symmetric | Project segment | Project segment | `jaccard_mean` | Are project siblings consistent? |
| `sibling_containers` | Symmetric | Container segment | Container segment | `jaccard_mean` | Are container siblings aligned? |
| `within_project` | Symmetric | File within segment | File within segment | `jaccard_mean` | Are files from the same project consistent with each other? |
| `governance_chain` | Directed | Template / Container | Project / Container | `containment_b_in_a` | End-to-end governance chain coverage |

Directed pairs use containment metrics; symmetric pairs use Jaccard. Both are always computed at the join_hash level, not pattern_id level.

---

## 3. Mode Switching: Bundle vs File

For each (segment_a, segment_b, domain) triple:

1. **Bundle mode** — used when `bundle_membership.csv` exists and is non-empty in the bundle analysis directory for **both** segments. Bundle mode groups patterns by their bundle assignment, enabling bundle-to-bundle comparisons rather than file-to-file.

2. **File mode** — fallback when bundle analysis has not completed for one or both segments. Uses `membership_matrix.csv` to build per-file join_hash sets.

The `comparison_mode` column in the output records which was used. Mixing modes across domains within the same pair is expected and normal — domains complete bundle analysis at different times.

Segments with `run_type = "skip"` or `"registration"` in the registry have no bundle output by design. They are treated as file-mode-only when `membership_matrix.csv` happens to exist; otherwise they are skipped with a warning.

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
| `comparison_mode` | `bundle` or `file` |
| `domain` | Domain name |
| `n_patterns_a` | Distinct join_hashes in segment A (union across all bundles/files) |
| `n_patterns_b` | Distinct join_hashes in segment B |
| `n_shared_join_hash` | Intersection size |
| `containment_a_in_b_mean` | Mean fraction of A's patterns found in each B unit (directed only) |
| `containment_a_in_b_min` | Min across B units |
| `containment_b_in_a_mean` | Mean fraction of B's mandate covered by each A unit (directed only) |
| `containment_b_in_a_min` | Min across A units |
| `jaccard_mean` | Mean pairwise Jaccard (symmetric only) |
| `jaccard_p10` | P10 pairwise Jaccard (symmetric only) |
| `jaccard_p90` | P90 pairwise Jaccard (symmetric only) |
| `n_bundles_a/b` | Bundle count for each side (bundle mode only) |
| `n_files_a/b` | File count for each side (file mode only) |
| `n_pairs` | Number of unit pairs that produced Jaccard values, or number of target units for directed |
| `executed_utc` | ISO-8601 UTC timestamp of the comparison run |

Columns that do not apply to a comparison direction are emitted as blank strings. For directed pairs: `jaccard_*` columns are blank. For symmetric pairs: `containment_*` columns are blank.

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
| `jaccard` | Pairwise Jaccard score |
| `containment_a_in_b` | Fraction of A's patterns in B |
| `containment_b_in_a` | Fraction of B's patterns in A |

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
  [--dry-run]
```

### Flags

| Flag | Description |
|------|-------------|
| `--segments-root DIR` | **Required.** Root directory containing `segment_manifest.csv` and `run_registry.csv`. |
| `--records-dir DIR` | **Required.** Directory containing `file_metadata.csv`. |
| `--out-dir DIR` | **Required.** Output directory. Created if absent. |
| `--within-segment` | Mode A: child Template/Project/Container pairs within the same parent. |
| `--sibling-segments` | Mode B: same parent, same governance_role. All pairwise combinations. |
| `--parent-siblings` | Mode C: level-2 Template-vs-Project under the same level-1 parent. |
| `--within-project` | Mode D: per-segment file pairs grouped by `project_label`. |
| `--governance-chain` | Mode E: directed Template→Project, Template→Container, Container→Project, scoped by `client_label`. |
| `--domain DOMAIN` | Restrict all comparisons to a single domain name. |
| `--segment-a SEGMENT_ID` | Restrict the left side of all pairs to this segment. |
| `--segment-b SEGMENT_ID` | Restrict the right side of all pairs to this segment. |
| `--min-patterns INT` | Skip any (segment, domain) with fewer than N join_hashes. Default: 3. |
| `--dry-run` | Print discovered pairs and exit. No output files are written. |

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

## 9. Known Limitations

### Small-N caveats (file mode)

File-mode Jaccard is noisy when either segment has fewer than ~5 files. The `--min-patterns` flag helps suppress the noisiest cases, but the threshold applies to join_hash count, not file count. A single file with 50 join_hashes passes the filter; two files with 3 join_hashes each also pass. Interpret file-mode results for segments with fewer than 5 files cautiously.

### discipline_label sparsity

`discipline_label` in `file_metadata.csv` is a new annotation column and may be blank for most files in early exports. Governance-chain matching ignores the field when either side is blank, which can produce cross-discipline pairs that look like valid template→project comparisons. As discipline labels are populated, re-running with the same flags will automatically tighten the matching.

### project_label as within-project grouping key

Mode D groups files by `project_label` from `file_metadata.csv`. `project_label` is a human-assigned string and may not uniquely identify a project when naming conventions vary across clients. Until a stable `project_id` field is populated in `file_metadata.csv`, treat within-project groupings as approximate. Files that share a `project_label` string are assumed to belong to the same project.

### Bundle analysis prerequisite

Bundle mode requires that the segment orchestrator has completed step 6 (bundle_membership.csv production) for both segments. Segments where only step 1 ran will fall through to file mode silently. Check `comparison_mode` in the output to confirm which mode was used.

### No cross-unit-system pairs

All pair discovery rules enforce matching `unit_system`. Imperial and metric segments are never compared. This is intentional — join_hashes for the same logical pattern differ between unit systems because behavioral hashes include unit-bearing values.
