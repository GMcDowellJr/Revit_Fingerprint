# Archetype Decision Point 2 — Approach Label Ratification
## Purpose
After Stage 5 (`cluster_archetype_signals.py`) runs, ratify `approach_label` values on the archetype definitions based on how signals actually clustered. Then re-run Stage 3 only to propagate labels into `archetype_classifications.csv`.

## Input files (upload all)
- `Fingerprint_Out/archetype_analysis/cluster_coverage_summary.json`
- `Fingerprint_Out/archetype_analysis/signal_clusters.json`
- `Fingerprint_Out/archetype_analysis/archetype_cluster_classifications.csv`
- `Fingerprint_Out/archetype_analysis/archetype_coverage_summary.json`
- `Fingerprint_Out/archetype_analysis/archetype_validation.csv`
- `Fingerprint_Out/archetype_analysis/archetype_review/review_<governance_question>__cluster_NNN.csv` (one per cluster, upload whichever are available)
- `config/archetype/archetype_definitions.json` (current version, to be updated)

## Task

### Step 1 — Read cluster structure from `signal_clusters.json`
For each governance question, list the clusters: `cluster_id`, `n_signals`, `is_singleton`, `min_containment`, `signal_ids`.
Note which signals merged (non-singleton, high `min_containment`) and which stayed isolated (singleton).

### Step 2 — Read coverage from `cluster_coverage_summary.json`
For each cluster: `n_files_all_signals`, `n_files_any_signal`, `n_files_mixed`, `pct_files_all_signals`.
- High `n_files_mixed` relative to `n_files_all_signals` means files appear across multiple archetypes in the same governance question — the question is genuinely multi-valued for those files.
- Very low `pct_files_all_signals` (< 5%) means the cluster lacks meaningful adoption.
- Clusters with identical `n_files_any_signal` and `n_files_all_signals` and `n_files_mixed` are likely drawing from the same file population — flag this.

### Step 3 — Read population profile from `archetype_cluster_classifications.csv`
For each cluster, summarize:
- Governance role breakdown (Template / Container / Project / Generic)
- Discipline mix (`discipline_label`)
- Client mix (`client_label`)
- Unit system
- Confidence tier breakdown (Full / Partial)

Clusters with radically different role/client profiles than others in the same governance question represent genuinely distinct governance approaches and should have distinct `approach_label` values.

### Step 4 — Read coherence from `archetype_validation.csv`
For each signal: `n_distinct_sig_hashes`, `coherence_score`.
- `n_distinct_sig_hashes = 1` with low `coherence_score` = highly standardized (one configuration dominates) — good.
- `n_distinct_sig_hashes = 0` = sig_hash resolution failed (known issue: materials domain pending PR 5) — classification is valid but coherence is unmeasurable.
- High `n_distinct_sig_hashes` with high `coherence_score` = significant variation in what files are using — this archetype may need splitting.

### Step 5 — Read element names from review CSVs
For each available `review_<cluster>__.csv`:
- `element_name` = the Revit element name of the source record (text type name, line style name, VFD name, etc.)
- Templates appear first (sorted by governance_role rank, then n_signals_fired desc).
- For VFD signals: `param_names` and `category_names` resolve the filter's parameter and category scope.
- If `element_name` = "_" for all rows, label resolution failed (materials domain issue — expected until PR 5).
- If the review CSV is empty (0 rows), VFD element name resolution failed — label from signal names only.

### Step 6 — Assign approach_labels
Rules:
- The `approach_label` sits on the **archetype** (in `archetype_definitions.json`), not on the cluster. Multiple archetypes can share a label if they represent the same governance behavior.
- When an archetype's signals split into two different singleton clusters, the approach_label should describe the combined intent (e.g., "Wall Fire Rating + Uniformat Filter Bundle" covers signals in both cluster_007 and cluster_011).
- When multiple archetypes merge into one cluster, give them the same approach_label.
- Labels describe the governance behavior, not the signal mechanism. Use terms practitioners recognize from Revit (arrowhead, line pattern, fill pattern, view filter, view template).
- If a cluster's population is client-specific (e.g., one client dominates at >80%), the label can note that or leave it neutral — Jon decides.

### Step 7 — Output
- Produce an updated `archetype_definitions.json` with `approach_label` fields filled in.
- Do not change any other fields (signals, join_hash, promoted, governance_question).
- Update `generated_utc` and `source` provenance fields.
- Note any archetypes where approach_label ratification is blocked (e.g., materials coherence=0, VFD review CSV empty).

### Step 8 — Re-run instruction
After updating `archetype_definitions.json`, only re-run:
```bash
python tools/archetype/assign_archetype_classifications.py --repo-root .
```
Stages 4, 5, and prepare_archetype_review do **not** need to re-run for approach_label-only changes.
Re-run Stages 3→4→5→review only if: archetypes were added/removed, a signal join_hash changed, or governance_question was changed.

## Known patterns from corpus (healthcare, imperial, Stantec)
- Line pattern signals: expect near-universal coverage (>95%) and coherence_score ≈ 0 (single standard).
- VFD signals: expect `n_distinct_sig_hashes = 1` (standardized filter definitions); low coherence_score is good.
- Clusters 002–005 in view_filter_strategy had identical 112-file populations — the same files use type name filter bundles across all element types simultaneously. Labels for these should read as a parallel family.
- `view_filter_strategy` stratifies by client: Renown/Sutter/Stantec architectural projects use parameter-specific filter bundles; Page containers use function_param and stack_filter approaches.
- Materials signals: `coherence_score = 0.0`, `n_distinct_sig_hashes = 0` — expected until PR 5 closes. Approach_label can still be ratified from signal semantics; flag as "coherence unverifiable pending materials hash policy".
