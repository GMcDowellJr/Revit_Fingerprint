# Investigation: Run A/B/C Reprocessing Scope (`corpus_update_runbook.ps1`)

**Status:** Findings-only investigation. No code, config, or runbook files were
modified. `build_segment_manifest.py` and `compare_cross_segment.py` were treated
strictly read-only throughout.

**Scope:** What each stage of `tools/corpus_update_runbook.ps1`'s Run A / Run B /
Run C actually recomputes, whether each artifact is IDENTITY/NAMESPACE (a
lookup-table build, safe corpus-wide) or PREVALENCE/STATISTICAL (a
denominator-bearing metric that should be population-scoped), and whether any
corpus-wide statistic from Run A/B is consumed downstream without a
segment-scoped override in Run C.

**Method:** Direct reading of `tools/corpus_update_runbook.ps1` plus the Python
entry points it invokes and one level of their imports. Every write-call citation
below was grep-located and read in context; the most load-bearing structural
claims (stage filtering, subprocess argument wiring, `--filter-export-run-ids`
population narrowing, `--analysis-dir` resolution, `domain_patterns.csv` path
resolution in `compare_cross_segment.py`/`export_bundle_pattern_detail.py`, the
absence of a `build_label_population.py` cache) were independently re-verified
in this session, not merely taken from a single pass.

---

## 0. Documentation-vs-code discrepancies (Step 0)

These are listed separately per the investigation's instructions — neither is
folded into, nor resolved in favor of, the main findings below.

1. **`docs/extract_stage_matrix.md`'s stage table omits the `sig_hash` stage
   entirely.** The table (`docs/extract_stage_matrix.md:5-16`) lists `flatten`,
   `discover`, `apply`, `placeholders`, `split`, `authority`, `patterns`,
   `flat_tables` — no `sig_hash` row. `CLAUDE.md`'s own stage table documents
   `sig_hash` as T0.5, "auto-inserted before `discover`/`apply` if selected."
   Run A's actual invocation is `--stages sig_hash,flatten,apply,placeholders`
   (`corpus_update_runbook.ps1:108`), and `tools/run_extract_all.py` does contain
   a `sig_hash` stage (confirmed by the Run A sub-investigation: `records.csv`
   is rewritten with `sig_hash`/`status`/`status_reasons` by
   `_apply_sig_hash_to_phase0`, `run_extract_all.py:378-382`). CLAUDE.md itself
   asserts `docs/extract_stage_matrix.md` is "the authoritative reference
   (current)" for this exact table — that claim does not hold for the `sig_hash`
   stage as of this investigation.
2. **`audit_results/README.md` says "The 15 reports in this directory..."**
   (`audit_results/README.md:3`) but 16 numbered reports already exist on disk
   (`audit_1` through `audit_16`) before this report is added as `audit_17`.
   Minor, unrelated to the substance of this investigation, noted only for
   completeness.

No other doc-vs-code conflicts were found in the files this investigation
touched; `docs/extract_stage_matrix.md`'s other rows (`flatten`/`apply`/
`placeholders`/`authority`/`patterns`) match observed code behavior.

---

## 1. Stage mapping

| Run | Literal invocation (`corpus_update_runbook.ps1`) | Underlying entry point(s) |
|---|---|---|
| A | `python tools/run_extract_all.py $EXPORTS --out-root $RESULTS --out-root-is-results-root --stages sig_hash,flatten,apply,placeholders --sig-hash-policy $SIG_POL --join-policy $JOIN_POL` (:105-110); optional `-NameKey`: `python tools\apply_name_key_policy.py --export-dir $EXPORTS --name-key-policy $NAME_KEY_POL --out $NAME_KEY_CSV` (:116-119) | `tools/run_extract_all.py` (stage machine, `stage_names` at `:728`, filtered to `["flatten","sig_hash","apply","placeholders"]` at `:812-818`); `tools/apply_name_key_policy.py` |
| B | `python tools/run_extract_all.py $EXPORTS --out-root $RESULTS --out-root-is-results-root --stages authority,patterns` (:142-145); then unconditionally `python tools\label_synthesis\patch_all_domain_patterns.py --results-root $RESULTS --segments-root $SEGMENTS` (:149-151); optional `-NameKey`: `python tools\generate_name_key_patterns.py --comparison-target name --name-key-csv $NAME_KEY_CSV --out-root "$RESULTS\name_key\patterns"` (:159-162) | `tools/run_extract_all.py`'s shared `authority`/`patterns` block (`:972-1176`) → `tools/label_synthesis/build_label_population.py` (subprocess), `tools/extractor.py::emit_analysis` (in-process), `tools/emit_element_dominance.py::emit_element_dominance` (in-process); `tools/label_synthesis/patch_all_domain_patterns.py`; `tools/generate_name_key_patterns.py` |
| C | C1 `python tools\build_segment_manifest.py --metadata-file "$RECORDS\file_metadata.csv" --out-dir $RECORDS --enable-parent-bundle-runs` (:186-189); C2 `python tools/run_segment_orchestrator.py --manifest-file ... --registry-file ... --results-registry-file ... --records-dir $RECORDS --exports-dir $EXPORTS --segments-root $SEGMENTS --repo-root $REPO --join-policy $JOIN_POL [--force]` (:245-255); C2.5 `python tools\build_results_registry.py ...` (:266-269); C3 `python tools\label_synthesis\patch_all_domain_patterns.py --results-root $RESULTS --segments-root $SEGMENTS` (:273-275) | `tools/build_segment_manifest.py` (protected, read-only); `tools/run_segment_orchestrator.py` → per-segment subprocesses of `tools/run_extract_all.py --stages patterns` (`run_segment_orchestrator.py:1511-1522`) and `tools/bundle_analysis/run_bundle_analysis.py` (`:1584-1586` et al.) → `tools/bundle_analysis/step1_membership_matrix.py` through `step7_overlap_report.py`; `tools/build_results_registry.py`; `tools/label_synthesis/patch_all_domain_patterns.py` |

Run A does **not** call Revit/Dynamo — confirmed by an explicit grep for
`Autodesk|Revit|clr|RevitAPI|DB` across every module in its traced call path
(`run_extract_all.py`, `extractor.py`, `apply_join_policy.py`,
`placeholder_exclusions.py`, `apply_name_key_policy.py`, and the `core/*`
modules they import): zero hits. The same is true for every module reached by
Run B and Run C in this investigation. All work in A/B/C is Python/CSV
aggregation over already-exported `*.details.json`/`*.index.json` — Revit/Dynamo
extraction is a separate, earlier step this runbook never touches.

---

## 2. Run A — output inventory and classification

Stages actually selected: `flatten`, `sig_hash`, `apply`, `placeholders` (in
that dependency order; `discover`/`authority`/`patterns`/`split`/`flat_tables`
do not run under Run A's `--stages` argument).

| # | Artifact | Producing function (file:line) | Scope today | Class |
|---|---|---|---|---|
| 1 | `records.csv` (flatten init: `sig_hash`,`join_hash`,`join_key_schema`) | `emit_records`, `tools/extractor.py:1061,1136-1158`, promoted `:1268-1269` | per-file, streamed | IDENTITY/NAMESPACE |
| 2 | `label_components.csv` | `extractor.py:1062,1228-1236` | per-file | IDENTITY/NAMESPACE |
| 3 | `status_reasons.csv` | `extractor.py:1063,1163-1170` | per-file | IDENTITY/NAMESPACE |
| 4 | `parameter_rows.csv` | `extractor.py:1064,1199-1220` | per-file | IDENTITY/NAMESPACE |
| 5 | `identity_items_by_domain/<domain>.csv` (native shards) | `extractor.py:1180-1194`, closed `:1238-1241` | per-file | IDENTITY/NAMESPACE |
| 6 | `identity_items_by_domain/.complete` sentinel | `extractor.py:1252` | corpus (control marker) | IDENTITY/NAMESPACE (control artifact) |
| 7 | `file_metadata.csv` | `extractor.py:1259-1266` (helper `:308-314`) | corpus, one row/file | IDENTITY/NAMESPACE |
| 8 | line_patterns synthetic `segments_norm_hash` rows | `_append_line_pattern_synthetic_norm_hash`, `run_extract_all.py:174-179`/`181-185`; called `:885`, `:950` | per-record, corpus-wide file rewrite | IDENTITY/NAMESPACE |
| 9 | `records.csv` overwrite (`sig_hash`,`status`,`status_reasons`,`sig_basis_schema`) | `_apply_sig_hash_to_phase0`, `run_extract_all.py:378-382` | corpus, whole file | IDENTITY/NAMESPACE |
| 10 | `sig_basis_items.csv` | `run_extract_all.py:317,361`, promoted `:384` | per-record rows, corpus file | IDENTITY/NAMESPACE |
| 11 | `diagnostics/sig_hash_policy_diagnostics.json` | `run_extract_all.py:917` | corpus-run summary counts | **PREVALENCE/STATISTICAL** |
| 12 | `identity_items_by_domain/<domain>.csv` legacy re-shard (only when native shards absent) | `_ensure_domain_scoped_identity_items`, `run_extract_all.py:488,496`, gated by mtime check `:465` | corpus | IDENTITY/NAMESPACE |
| 13 | `records.csv` overwrite (`join_hash`,`join_key_status`,`join_key_policy_id/version`,`join_key_schema`) | `tools/apply_join_policy.py:198` | corpus, whole file | IDENTITY/NAMESPACE |
| 14 | `diagnostics/join_policy_failures.csv` | `apply_join_policy.py:200-204` | corpus | IDENTITY/NAMESPACE |
| 15 | `placeholder_exclusions_<domain>.csv` ×19 domains | `tools/bundle_analysis/placeholder_exclusions.py:223` | per-domain, corpus population | **PREVALENCE/STATISTICAL** |
| 16 | `domain_authority_by_file_<domain>.csv` ×19 domains | `placeholder_exclusions.py:224` | per-domain, corpus population | **PREVALENCE/STATISTICAL** |
| 17 | `placeholder_exclusions_all.csv` | `placeholder_exclusions.py:225` | corpus | **PREVALENCE/STATISTICAL** |
| 18 | `domain_authority_by_file.csv` | `placeholder_exclusions.py:226` | corpus | **PREVALENCE/STATISTICAL** |
| 19 | `extract_all.report.json` | `run_extract_all.py:1228-1230` | corpus (run log) | Neither (provenance/log) |
| 20 (opt-in `-NameKey`) | `name_key_results.csv` | `tools/apply_name_key_policy.py:192-196` | per-record rows, corpus file | IDENTITY/NAMESPACE |

**Classification justification (values, not names).** Rows 1-10, 12-14, 20:
every rewritten field is a deterministic function of that one record's own
`identity_basis.items`/label against a policy — `core/sig_hash_builder.py`,
`join_key_discovery/eval.py:95-103` confirmed to look up only
`identity_items_by_record.get(record_pk, {})`, no cross-record aggregation.
Row 11: `records_processed`/`records_hashed`/`records_blocked`/`records_degraded`
(`run_extract_all.py:255-263,329,364-368`) are tallies over whichever population
of records was loaded that run — a corpus-scoped count. Rows 15-18: `suggested_exclude`
and `domain_authority` derive from `thr = lg(list(file_pct.values()))`
(`placeholder_exclusions.py:127-128,163`) and `_choose_threshold`
(`:64-77,199-216`) — a largest-gap/median threshold computed across **every
eligible file's** value in the current population; one file's classification
can flip when another file is added or removed, even though that file was not
touched.

**Completeness cross-check.** Literal `.to_csv(`/`json.dump(`/`.write(` grep
undercounts because this codebase's convention is `csv.DictWriter` +
`path.open("w")` / `atomic_write_csv()`, which those three literals miss for
several real writers (e.g. all 4 sites in `placeholder_exclusions.py` are
`atomic_write_csv(` calls, 0 hits on the literal pattern). Broader-pattern
counts (`DictWriter(`/`open(...,"w"`/`atomic_write_csv(`/`write_text(`)
reconcile with the table: `run_extract_all.py` 17, `extractor.py` real
call-sites 12 (9 shown here are flatten-stage; item 9/10/11/12 above cover
the sig_hash/apply/placeholders side), `apply_join_policy.py` 2,
`bundle_analysis/placeholder_exclusions.py` 4, `apply_name_key_policy.py` 1.
Table row count: 20, matching all reached call sites.

---

## 3. Run B — output inventory and classification

Stages selected: `authority,patterns`, both routed through **one shared code
block** (`run_extract_all.py:972-1176`, gated `"authority" in selected_stages
or "patterns" in selected_stages` — there is no authority-only vs.
patterns-only split). `tools/domain_authority.py`, `tools/population_framing.py`,
`tools/pairwise_analysis.py` are **not** reached anywhere in this call graph
(confirmed by grep across every file Run B touches) despite their names.
`flatten`/`sig_hash`/`discover`/`apply` do not run under Run B (no auto-insert
fires since `apply` is not selected); Run B reads whatever `records.csv`/
`file_metadata.csv` a prior Run A already wrote.

| # | Artifact | Producing function (file:line) | Scope today | Class |
|---|---|---|---|---|
| 1 | `diagnostics/join_policy_gate_diagnostics.csv` | `_emit_join_policy_diagnostics`, `run_extract_all.py:566-570`, invoked `:1017` | corpus | PREVALENCE-adjacent (a completeness signal, not a governance rate) |
| 2 | `label_synthesis/{domain}.joinhash_label_population.csv` ×N domains | `build_label_population()`, `tools/label_synthesis/build_label_population.py:114-120`; subprocess `run_extract_all.py:1035-1046` | corpus (`records_source_dir`, `build_label_population.py:43-45,69-90`) | **Mixed — flagged.** `label_v`/`label_q` are label text (identity-flavored), but `files_count` (`:104-109`) is a corpus-wide occurrence count that picks the *modal* label — a prevalence computation whose output is consumed as identity metadata. |
| 3 | `analysis/corpus_manifest.csv` | `emit_analysis()`, `extractor.py:1291-1314` | corpus | IDENTITY/NAMESPACE (run population size/version, not a rate) |
| 4 | `analysis/export_membership.csv` | `extractor.py:1322-1324` | corpus | IDENTITY/NAMESPACE (roster) |
| 5 | **`analysis/authority_metrics.csv`** | `extractor.py:1463-1467`, rows built `:693-709` | corpus | **PREVALENCE/STATISTICAL — flagged.** `presence_pct=files_present/files_total`, `coverage_pct=cluster_size/total_dom_records` (`:690-691,705-706`); `files_total=len(exports)`, the whole corpus (`run_extract_all.py:979-981`). |
| 6 | `analysis/domain_patterns.csv` (1st write) | `extractor.py:1469-1478`, rows `:731-760` | corpus | **Mixed.** `pattern_id`/`join_hash`/`source_cluster_id` are pure per-record hashes (IDENTITY). `is_candidate_standard=presence_pct>=STANDARD_PRESENCE_MIN` (`:746`) and `pattern_rank` (sorted by corpus-wide `files_present`/`records_count`, `:671-674,745`) and the legacy `pattern_label` (embeds `"Variant {rank} of {n}"`, `:712,738`) are **PREVALENCE/STATISTICAL**. |
| 6b | same file, 2nd write (adds `is_seed="false"`) | `run_extract_all.py:1156-1163` | corpus, same-run overwrite | same as #6 |
| 7 | `analysis/record_pattern_membership.csv` | `extractor.py:1480-1483`, rows `:762-772` | corpus | IDENTITY/NAMESPACE (deterministic per-record assignment, `membership_confidence=1.0`) |
| 8 | **`analysis/authority_patterns.csv`** | `extractor.py:1485-1489`, rows `:774-791` | corpus | **PREVALENCE/STATISTICAL — flagged, the clearest hit.** `presence_pct`,`hhi`,`effective_cluster_count`,`authority_score(=presence_pct)`,`confidence_tier` — `hhi` via `compute_hhi_from_shares([...cnt/total_dom_records...])` (`:774-776`): every pattern's HHI contribution depends on every other pattern's size in the same domain, corpus-wide. |
| 9 | **`analysis/pattern_presence_file.csv`** | `extractor.py:1491-1494`, rows `:845-873` | corpus | **PREVALENCE/STATISTICAL — flagged.** `pattern_share_pct`, `deviation_score`, `corpus_classification` (`CORPUS_STANDARD`/`CORPUS_VARIANT`/`UNKNOWN`, threshold from corpus-wide `presence_pct`, `:793-797,858`). |
| 10 | **`analysis/file_domain_concentration.csv`** | `extractor.py:1496-1499`, rows `:832-844` | corpus | **PREVALENCE/STATISTICAL — flagged.** `hhi_file_records` is per-file, but the pattern universe it's computed over is corpus-derived. |
| 11 | **`analysis/pattern_diagnostics.csv`** | `extractor.py:1501-1509`, rows `:876-940` | corpus | **PREVALENCE/STATISTICAL — flagged.** `dominant_pattern_share_pct`, `entropy_index`, `unknown_rate_pct`, six `hhi_*`/`eff_clusters_*` fields, `governance_state` — domain-level aggregates over the whole run population. |
| 12 | **`analysis/element_dominance.csv`** | `emit_element_dominance()`, `tools/emit_element_dominance.py:276`; invoked unconditionally right after `emit_analysis`, `run_extract_all.py:1166` | corpus | **PREVALENCE/STATISTICAL — flagged.** `dominant_presence_pct`,`lead_gap`,`element_dominance_bucket` derive from `authority_patterns.csv`'s corpus-wide `presence_pct` (`emit_element_dominance.py:94-125`). |
| 13 | **`analysis/element_characterization_thresholds.csv`** | `emit_element_dominance.py:284` | corpus | **PREVALENCE/STATISTICAL — flagged.** `competitive_min`/`standard_min` are Jenks natural breaks over the *entire* corpus's `dominant_presence_pct` distribution (`:131-135,160-172`) — genuinely population-wide, not decomposable per file. |
| 14 | `extract_all.report.json` | `run_extract_all.py:1227-1230` | corpus | Neither (provenance/log) |
| 15 | `{results_root}/analysis/domain_patterns.csv` **patch** + `{segments_root}/*/results/analysis/domain_patterns.csv` **patch** (N segment targets, if any already exist) | `patch_all_domain_patterns.py` `_find_targets` (`:259-290`) → `_patch_one` (`:249-250`) | patch only — mutates `pattern_label_human`/`pattern_label_source` cells on rows #6/#6b already wrote; row set/other columns untouched | same mixed character as target |
| 16 (opt-in) | `name_key/patterns/config/domain_patterns.csv` | `emit_config_patterns()`, `generate_name_key_patterns.py:162` | verbatim byte-copy of #6 | same as #6 |
| 17 (opt-in) | `name_key/patterns/name/domain_patterns.csv` | `emit_name_patterns()`, `generate_name_key_patterns.py:337`, rows `:166-221` | corpus, different identity projection (`join_key_name_identity`) | IDENTITY (`pattern_id`/`join_hash`) + **PREVALENCE flagged** (`pattern_rank`, rank-embedding `pattern_label`) |
| 18 (opt-in) | `name_key/patterns/name/pattern_membership.csv` | `generate_name_key_patterns.py:338`, rows `:224-260` | corpus | IDENTITY/NAMESPACE |
| 19 (opt-in) | `name_key/patterns/name/domain_coverage.csv` | `generate_name_key_patterns.py:339`, rows `:263-310` | corpus | IDENTITY/NAMESPACE (static registry + observed presence) |

**Completeness cross-check.** Literal 3-pattern grep under-detects for the same
convention reason as Run A (0 hits in 4 files that do write CSVs via
`DictWriter`/`open(...,"w"`). Broader pattern (`DictWriter(`/`open(...,"w"`/
`shutil.copy`/`atomic_write_csv(`/`write_sidecar(`) reconciles: `extractor.py`
12 real sites (`_write_csv` helper def `:310-311` invoked 9× for rows 3-11, plus
flatten-stage sites not reached by Run B), `emit_element_dominance.py` 1 (called
2×, rows 12-13), `build_label_population.py` 2 (1 call site), 
`patch_all_domain_patterns.py` 3, `generate_name_key_patterns.py` 5. Table row
count: 19 (17 distinct call sites + #6/#6b same-artifact double-write + 4
opt-in rows), matching every write call reached by Run B's traced call graph.

---

## 4. Run C — output inventory and classification

Confirmed root scoping mechanism, inherited by everything below: the
orchestrator's per-segment `patterns`-stage subprocess passes
`--filter-export-run-ids str(out_root/"export_run_ids.txt")`
(`run_segment_orchestrator.py:1519`), where `export_run_ids.txt` is that
segment's own `segment_membership.csv` row (`:1417,1485`). `run_extract_all.py`
applies this filter to `meta_rows`/`record_rows` **before** calling
`emit_analysis()` (`run_extract_all.py:986-995`) — independently confirmed in
this session. `step0_discover_populations.py` and
`bundle_analysis/placeholder_exclusions.py` are **not reached** by Run C (the
orchestrator always passes `--no-discover-populations`,
`run_segment_orchestrator.py:1521` et al.; `placeholders`/T2b never runs since
the orchestrator's subprocess is `--stages patterns` only). The `--compare`
path (`run_bundle_analysis.py`) is likewise never invoked by the orchestrator.

| # | Artifact | Producing function (file:line) | Scope today | Class |
|---|---|---|---|---|
| 1 | `segment_manifest.csv` | `build_segment_manifest.py:1180` (protected file, read-only) | corpus (whole lattice) | IDENTITY/NAMESPACE |
| 2 | `run_registry.csv` (initial) | `build_segment_manifest.py:1181` | corpus | IDENTITY/NAMESPACE |
| 3 | `segment_membership.csv` | `build_segment_manifest.py:1182` | corpus | IDENTITY/NAMESPACE |
| 4 | segment `records.csv`/`file_metadata.csv` | `_preshard_corpus_records()` (`run_segment_orchestrator.py:653,670`) / `_write_segment_records()` (`:783-786`) | segment | IDENTITY/NAMESPACE |
| 5 | segment `identity_items_by_domain/*.csv` | `:536-551` / `:811-814` | segment | IDENTITY/NAMESPACE |
| 6 | `export_run_ids.txt` | `:1485` | segment | IDENTITY/NAMESPACE |
| 7 (opt-in) | segment `name_key_results.csv` | `_filter_name_key_csv_to_segment()`, `:898-901` | segment | IDENTITY/NAMESPACE |
| 8 | `run_registry.csv` (live update) | `write_registry_atomic()` (`:161-168`), called `:1761` | corpus, one row/segment | IDENTITY/NAMESPACE |
| 9 | `results_registry.csv` (mid-run) | `write_results_registry()`, `:1762-1766` | corpus, aggregation, no stats | IDENTITY/NAMESPACE |
| 10 | `run_summary.txt` | `_write_run_summary()`, `:384-385` | corpus | Neither (operational log) |
| — | segment `analysis/authority_metrics.csv` | same `emit_analysis()` call as Run B row 5 — this code path runs unconditionally inside the shared authority/patterns block, which Run C's `--stages patterns` subprocess also enters | segment (same `--filter-export-run-ids` narrowing as everything else here) | **PREVALENCE/STATISTICAL** (not enumerated separately by this sub-investigation's table; see §6 note) |
| — | segment `analysis/element_dominance.csv`, `element_characterization_thresholds.csv` | same `emit_element_dominance()` call as Run B rows 12-13 | segment | **PREVALENCE/STATISTICAL** (same completeness note) |
| 11 | `membership_matrix.csv` | `step1_membership_matrix.py:210-214` | segment | IDENTITY/NAMESPACE (fact table, `:122-131`) |
| 12 | `scope_registry.csv` | `step1_membership_matrix.py:215-219` | segment | IDENTITY/NAMESPACE — `files_in_scope`/`patterns_in_scope` are raw counts (`:198-207`), the population census other ratios divide by |
| 13 | `bundles.csv` | `step2_find_bundles.py:323-339` | segment | **PREVALENCE/STATISTICAL.** `support_pct=100.0*files_present/files_total` (`:269`) |
| 14 | `bundle_membership.csv` | `step2_find_bundles.py:340-344` | segment | IDENTITY/NAMESPACE |
| 15 | `bundle_file_membership.csv` | `step2_find_bundles.py:345-349` | segment | IDENTITY/NAMESPACE |
| 16 | `bundle_analysis_thresholds.csv` | `step2_find_bundles.py:351-373` | segment | **PREVALENCE/STATISTICAL.** `cooccurrence_p90`/`expected_floor`/`natural_breaks_floor` from the segment's own co-occurrence distribution (`:77-115,193-217`) |
| 17 | `bundle_share_profile.csv` | `step2b_bundle_share_profile.py:46-64,203-221` | segment | **PREVALENCE/STATISTICAL.** `median_share_pct`/`mean_share_pct`/`pct_bundle_files_dominant` (`:147-198`) |
| 18 | `bundle_dag_edges.csv` | `step3_build_dag.py:24-38,194-208` | segment | IDENTITY/NAMESPACE (DAG topology) |
| 19 | `bundle_dag_nodes.csv` | `step3_build_dag.py:39-54,209-224` | segment | IDENTITY/NAMESPACE — `is_root`/`is_leaf`/`depth`/`parent_count`/`child_count` (`:169-182`) |
| 20 | `bundle_dag_differences.csv` | `step4_difference_sets.py:35,40,132-136` | segment | **PREVALENCE/STATISTICAL.** `difference_presence_pct_min/max/mean` (`:103-127`) |
| 21 | `pattern_bundle_classification.csv` | `step5_classify_patterns.py:48,55,219` | segment | **PREVALENCE/STATISTICAL — naming caveat, see §5.** `corpus_presence_pct` (`:183-188`) |
| 22 | `file_bundle_classification.csv` | `step6_classify_files.py:193-212` | per-file, segment population | **PREVALENCE/STATISTICAL.** `noise_pct_primary`/`noise_pct_any` (`:167-172`), denominator is that file's own pattern count |
| 23 | `cross_bundle_overlap.csv` + `bundle_pair_overlap.csv` | `step7_overlap_report.py:19-49` | N/A — unconditional empty stub `[]` regardless of scope | schema names denominator-bearing fields but nothing is currently populated |
| 24 | `bundle_analysis_timing.csv` | `run_bundle_analysis.py:855,1123` | segment | Neither (elapsed seconds, not a rate) |
| 25 | `meta_scatter_thresholds.csv` | `run_bundle_analysis.py:163-167` | segment | **PREVALENCE/STATISTICAL.** `bundle_density=bundle_count/population_files`, `b_alignment_rate` (`:134-142`) |
| 26 | `{stem}_combined.csv` ×10, all-view and used-view separately | `merge_bi_outputs()`, `run_segment_orchestrator.py:1163`, called once per view (`:1673,1696`) | segment | Mixed — concatenates identity fact tables and statistical files verbatim per underlying source (`:53-64`) |
| 27 (opt-in) | name-leg staged `domain_patterns.csv`/`pattern_presence_file.csv`/`domain_coverage.csv` | `name_projection_adapter.py:204-210,258-294` | segment | same mixed character as source |
| 28 (opt-in) | name-leg `*_combined.csv` annotation columns | `name_projection_adapter.py:360` | segment | inherits #26/#27 |
| 29 | `results_registry.csv` (C2.5 final rebuild) | `build_results_registry.py:176` | corpus | IDENTITY/NAMESPACE |
| 30 | `domain_patterns.csv` `pattern_label_human`/`_source`/`_fallback` patch + `.csv.bak` | `patch_all_domain_patterns.py:249-250`, targets `:270-288` | **both** corpus `results-root/analysis/domain_patterns.csv` and every segment's `segments-root/<seg>/results/analysis/domain_patterns.csv` | naming/vocabulary patch, not a fresh statistic — but see §5 on where the value it patches with originates |

Not reached by Run C (present in `bundle_analysis/` but gated off by the
orchestrator's flags): `step0_discover_populations.py`'s
`corpus_populations*.csv` family; `placeholder_exclusions.py`'s outputs
(that's Run A/T2b territory); `run_bundle_analysis.py`'s `--compare`-gated
`compare_run_summary.csv`/`compare_run_status.csv`/reference-bundle diffs.

**Population-correctness verification (Task 3 — the central design question for
Run C).** Traced independently for the highest-risk rows:
- Row 13 (`bundles.csv` `support_pct`): `files_total` comes from
  `files_total_by_scope` (`step2_find_bundles.py:131`), sourced from
  `scope_registry.csv` (row 12), itself built from `pattern_presence_rows`
  already filtered to that segment (`step1_membership_matrix.py:99-107,
  192-207`). `allowed_export_run_ids` (an additional narrowing param) is `None`
  in Run C's normal invocation — only `--roles`, never passed by the
  orchestrator, would change it (`run_bundle_analysis.py:557,572-580,759-770`).
- Rows 16, 17, 20, 21, 25 all trace their `files_total`/`scope_files`/
  `population_files` back to the same `scope_registry.csv`/segment-filtered
  source, or (row 25) to walking the segment's own `results/bundle_analysis/
  {all,used}/` directory (`run_bundle_analysis.py:116-132,668,862,1131`).

**No corpus-wide leakage was found in any Run C bundle-analysis statistic under
the runbook's normal invocation** (`--purge-view both`, `--no-discover-populations`,
no `--compare`, no `--roles`).

**Completeness cross-check.** Literal grep: 17. Broader pattern
(`atomic_write_csv(`/`DictWriter(`/`writerow(`/`writerows(`/`write_text(`): 88,
reconciling with the 30 grouped rows above once repeated near-identical
per-domain/per-view writes inside the same producing function are counted as
one row each (explicitly grouped and stated as such per row, e.g. row 26's ×10
files, all-view and used-view kept as separate invocations not separate rows).

---

## 5. Correctness flags — corpus-level prevalence consumed downstream without a segment-scoped override

*(This section is written to stand alone.)*

**No unguarded read was found.** For every Run A/B corpus-wide PREVALENCE/
STATISTICAL artifact this investigation traced, the governance-relevant
consumers (`compare_cross_segment.py`, `tools/export_bundle_pattern_detail.py`,
the `bundle_analysis` pipeline, `generate_governance_narrative.py`,
`governance_manifest.py`) either:
(a) never read the corpus-level copy at all, or
(b) resolve exclusively to a segment-scoped copy that Run C independently
regenerates via the same code with `--filter-export-run-ids` applied first.

Specifics, each independently verified in this session:

- **`domain_patterns.csv`** (corpus copy, Run B row 6/6b): `compare_cross_segment.py`'s
  `domain_patterns_path(seg_out)` (`compare_cross_segment.py:899-900`) and
  `tools/export_bundle_pattern_detail.py`'s `_resolve_segment_paths()`
  (`export_bundle_pattern_detail.py:204-207`) both build the path from a
  **per-segment** `seg_out`, with **no corpus-level fallback** for this specific
  file (unlike `records_csv`/`identity_items_dir` in the same function, which do
  fall back to corpus scope when a segment hasn't been presharded yet —
  `export_bundle_pattern_detail.py:210-225` — itself worth knowing about, though
  `records.csv` rows are per-record identity data, not a prevalence statistic,
  so it falls outside this section's correctness lens).
- **`authority_patterns.csv`, `pattern_presence_file.csv`,
  `file_domain_concentration.csv`, `pattern_diagnostics.csv`,
  `element_dominance.csv`, `element_characterization_thresholds.csv`**: all
  read exclusively via `--analysis-dir str(out_root/"results"/"analysis")` at
  every `run_bundle_analysis.py` invocation in `run_segment_orchestrator.py`
  (confirmed at all four call sites: `:1586,1640,1958,1971`) — `out_root` is
  always that specific segment's own output directory, never the corpus
  `$RESULTS` root.
- **`authority_metrics.csv`**: a repo-wide grep for the literal string
  `authority_metrics` outside `tools/extractor.py` (its sole writer) returned
  **zero matches**. This artifact — corpus-wide `presence_pct`/`coverage_pct`
  with no scoping issue to worry about, because nothing reads it at all — is
  effectively dead output at both corpus and (per the shared-code-path note in
  §4) segment scope. Not a correctness bug; flagged here as wasted computation
  (see §6).
- `generate_governance_narrative.py` and `tools/governance_manifest.py` take no
  direct CLI input from Run A/B's corpus-level `records.csv`/`domain_patterns.csv`/
  `phase0_records.csv` — their arguments are exclusively Run-C/`compare_cross_segment.py`
  outputs (`cross_segment_*.csv`, `segment_manifest.csv`) plus the hand-edited
  `file_metadata.csv` (confirmed via full `add_argument` listing,
  `generate_governance_narrative.py:5850-5968`, and a targeted grep for
  `records\.csv|domain_patterns\.csv|phase0_records` in that file: zero hits
  outside doc comments). `governance_manifest.py` similarly only reads
  `file_metadata.csv` (`governance_manifest.py:336`).
- `generate_governance_narrative.py`'s live directory-scan inventory
  (`governance_file_inventory.json`, D-023) globs `*.csv` only under
  `Path(args.summary).parent` and three other Run-C-adjacent anchor directories
  (`generate_governance_narrative.py:6744-6749`) — it never scans `$RESULTS`
  (Run A/B's corpus output root), so it wouldn't even catalog the corpus-level
  `domain_patterns.csv`/`authority_metrics.csv` unless they happened to be
  co-located with `cross_segment_summary.csv`.

**One genuine correctness-adjacent flag, found and confirmed:**

**`{domain}.joinhash_label_population.csv`'s modal-label vote is computed from
the CORPUS-wide population even inside Run C's segment-scoped pipeline, by
explicit design.** `run_segment_orchestrator.py`'s own comment states this
directly: *"--records-dir points at corpus records so build_label_population
(run internally by run_extract_all) reads the full population, not just this
segment's subset"* (`run_segment_orchestrator.py:1504-1505`), and the
subprocess call passes `--records-dir str(records_dir)` where `records_dir` is
the corpus records directory, not the segment's own presharded one
(`:1517`, contrast with `:1478` which does use the segment's own
`out_root/"results"/"records"` for everything else in that step). This means
the `pattern_label_human` value later patched into a **segment-scoped**
`domain_patterns.csv` (Run C row 30) — the exact file `compare_cross_segment.py`
and `export_bundle_pattern_detail.py` read as authoritative for that segment —
carries a label chosen by a vote over files entirely outside that segment.
This is a deliberate, documented choice (arguably reasonable: more label
occurrences → a more stable name), not an oversight, and no downstream
consumer was found treating `pattern_label_human` as a governance statistic
(§4/§Run B classify it as identity-flavored, not a rate) — but it is a real
violation of pure segment-scoping for whoever assumes a segment-scoped
`domain_patterns.csv` is *entirely* derived from that segment's own files.

**Presentational (not computational) flag:** `pattern_bundle_classification.csv`'s
column is literally named `corpus_presence_pct` (`step5_classify_patterns.py:
183-188`) despite its denominator being confirmed segment-scoped
(`scope_files_total`, sourced from the segment's own `scope_registry.csv`,
`step5_classify_patterns.py:135-143`). The value is correct for Run C's intent;
the name is a documentation/naming trap that could mislead a reader — especially
after the `*_combined.csv` merge (row 26) lands it in a shared Power BI column —
into treating a segment-local prevalence figure as corpus-wide.

---

## 6. Genuinely corpus-wide vs. incrementality-gap verdicts (per PREVALENCE/STATISTICAL artifact)

**Run A**

| Artifact | Cross-file dependency? | Recomputable from cached per-file facts? | Cost location |
|---|---|---|---|
| `sig_hash_policy_diagnostics.json` | No — plain per-record tallies (`records_processed += 1` etc.), not a ratio | Yes in principle; today forced into a full re-read only because flatten unconditionally rewrites `records.csv` every run (see Run A change-detection, §7) | No dedicated timing instrumentation for the flatten/sig_hash stage bodies; only the generic `_run()` subprocess wall-clock covers `discover`/`apply`/`placeholders`/`split`/`flat_tables`, not the in-process `flatten`/`sig_hash` stages |
| `placeholder_exclusions_*.csv`/`domain_authority_by_file*.csv` | **Yes, genuine.** `thr`/`threshold` computed once per domain over the full current population's `file_pct`/`adjusted_total`; a file's `suggested_exclude`/`domain_authority` can flip from an unrelated file's addition/removal — "changed files only" would be mathematically wrong here, not just slower | Yes for the raw inputs (`is_purgeable`,`instance_count` come from already-flattened `records.csv`), but no per-file fact cache exists today (see §7) — in practice the whole `records.csv` is rebuilt fresh from all exports every flatten run first | Only the coarse subprocess wall-clock (`run_extract_all.py:400-404`); `placeholder_exclusions.py` itself has zero timing instrumentation |

**Run B**

Cluster **assignment** (`pattern_id` per record) is population-independent —
`_stable_pattern_id()` (`extractor.py:289-305`, independently reimplemented at
`tools/pattern_id_utils.py:22-52`) is a pure hash of that record's own
`(domain, join_key_schema, join_hash)`; a single record's own cluster can be
determined in isolation. Every *aggregate statistic* built on top of that
assignment, however, is genuinely corpus-wide:

- `presence_pct=files_present/files_total`, where `files_total=len(exports)` is
  set once per `emit_analysis()` call over the whole run population
  (`extractor.py:690,1326`) — adding or removing any file shifts `files_total`
  for **every** domain/pattern, not just ones that file touches.
- `hhi`/`effective_cluster_count` (`compute_hhi_from_shares`, `extractor.py:
  330-359`) sum squared shares across all clusters in a domain — one new
  cluster shifts every other cluster's HHI contribution.
- `pattern_rank` (sort key `(-files_present,-records_count,pid)`,
  `extractor.py:671-674`) — and the legacy `pattern_label` text embedding
  `"Variant {rank} of {n}"` — can change from another pattern's growth
  elsewhere in the corpus with zero change to this pattern's own membership.
- `element_characterization_thresholds.csv`'s Jenks breaks
  (`emit_element_dominance.py:131-135`) are explicitly a whole-distribution
  operation, undefined for one file.
- `{domain}.joinhash_label_population.csv`'s `files_count` modal vote
  (`build_label_population.py:104-109`) is a full-population count; a label
  can flip `fallback`↔`modal` from a population change elsewhere.

**Verdict: none of these can be correctly computed "changed files only" — the
denominator/share-vector/rank/threshold is defined over the whole population
passed into that `emit_analysis()`/`emit_element_dominance()` call, by
construction (`extractor.py:1274-1284` takes the full row lists, no delta
parameter exists).** But — per §5 — Run B's corpus-wide computation of these
same statistics is not the one any governance consumer actually reads; Run C
recomputes the identical statistic, correctly denominated to each segment, via
the same code. Run B's corpus-wide `authority`/`patterns` output therefore
functions as a **corpus-level preview/dry-run that is fully superseded before
any governance-relevant read** — its own aggregate numbers are not wrong for
"the whole corpus as one population" (that is exactly what they measure), but
nothing downstream currently treats "the whole corpus as one population" as a
meaningful governance answer; Run C's segment lattice is what's consumed.

**Timing evidence exists for Run B**, via `[patterns_timing]` stderr lines:
overall stage timing (`run_extract_all.py:973,1171`), per-`emit_analysis`-call
timing (`:1078,1092,1154`), and a per-domain breakdown inside
`_process_one_domain` (`identity_items`/`label_inputs`/`cluster_loop`/
`file_loop`/`sort`/`csv_write`/`residual`, `extractor.py:630-980`, top-5
slowest domains printed `:1431-1445`). This confirms the Python/pandas-style
aggregation cost is measured and domain-by-domain attributable — but there is
no comparable timing instrumentation for the upstream Revit/Dynamo extraction
step anywhere in this repository (that step lives entirely outside
`run_extract_all.py`, in `runner/run_dynamo.py`, which this investigation's
traced call graph never reaches), and this session has no access to an
"extraction repo PR#127" referenced in the investigation brief — **that
comparison is unverified and out of reach of this session; do not treat any
number in this report as a Revit-vs-Python cost ratio.**

**A genuine, separately-confirmed incrementality gap: `build_label_population.py`
has no caching and is invoked unconditionally on every "authority"/"patterns"
call**, including once per Run C segment. `build_label_population.py` (182
lines total) has no `mtime`/`exists()`/skip check anywhere in it (confirmed by
reading the full file), and `run_extract_all.py:1035-1046` invokes it
unconditionally every time the shared authority/patterns block runs, despite
its own comment reading "Ensure modal label population artifacts exist"
(`:1030`) — the code does not check whether they already do. Because Run C's
per-segment `patterns` subprocess deliberately points `--records-dir` at the
**corpus** records directory for this specific sub-step (`run_segment_orchestrator.py:
1504-1505,1517`), this means the full corpus `records.csv` is re-read and the
modal-label vote is fully recomputed from scratch **once per segment** that Run
C (re)processes in a given invocation — for a result (`{domain}.joinhash_label_population.csv`
in `$RESULTS/label_synthesis/`) that does not vary by segment at all. This is
a real, cited reprocessing-necessity gap distinct from the population-hash
segment-skip mechanism (§7) — even a segment whose own population changed
trivially still pays the cost of a full corpus-wide label re-vote.

**Run C**

Every PREVALENCE/STATISTICAL artifact in Run C (`bundles.csv`,
`bundle_analysis_thresholds.csv`, `bundle_share_profile.csv`,
`bundle_dag_differences.csv`, `pattern_bundle_classification.csv`,
`file_bundle_classification.csv`, `meta_scatter_thresholds.csv`) has the same
structural property as Run B's: its denominator is the full **segment**
population (`scope_registry.csv`'s `files_in_scope`, ultimately sourced from
`segment_membership.csv`), so "changed files only, within a segment" is
mathematically wrong for the same reason Run B's corpus-wide version is wrong
for "changed files only, corpus-wide." Run C's actual incremental unit is
therefore correctly the **segment**, not the file: `build_segment_manifest.py`'s
`population_hash` (`build_segment_manifest.py:807-829` per the Run A
sub-investigation's citation) skips a segment's re-run only when that
segment's own file population is provably unchanged, and reprocesses the
*entire* segment (not incrementally) when it isn't — which matches the math.
This is the one place in the whole runbook where the incremental unit and the
statistical denominator are already the same thing.

---

## 7. Run A change-detection verdict (Task 6)

**Run A has no per-export-file change-detection gate.** It unconditionally
reprocesses every `*.json` in `$EXPORTS` on every invocation:

- Flatten's file-discovery loop, `_iter_export_files(exports_dir)`
  (`tools/extractor.py:67-104`), is a plain `exports_dir.glob("*.json")`
  (`:68`) filtering only `.legacy.json` and pairing `.index.json`/
  `.details.json` — no mtime, hash, or existing-output check anywhere in the
  function. It runs unconditionally whenever `"flatten" in selected_stages`
  (`run_extract_all.py:878-890`), which Run A always selects.
- `apply_name_key_policy.py`'s `_iter_export_paths` (`:52-87`) is the identical
  pattern — a plain glob, no gating.
- `apply` (`tools/apply_join_policy.py`) and `placeholders`
  (`tools/bundle_analysis/placeholder_exclusions.py`) don't discover export
  files at all; they consume the freshly-rewritten `records.csv`/shards from
  the same invocation's flatten step, so they inherit "always full" behavior.
- The one mtime check in this call path (`run_extract_all.py:465`) is a
  legacy fallback inside `_ensure_domain_scoped_identity_items` that only
  fires when native per-domain shards are *absent*, and it compares the mtime
  of a flatten *output* (`identity_items.csv`) — not an export input, and not
  normally exercised since `emit_records` always writes native shards plus a
  `.complete` sentinel (`extractor.py:1037-1041,1252`).

**Run A's expensive step is Python/CSV aggregation, not Revit** — confirmed by
the zero-hit Revit-API-import grep noted in §1. Run A never calls into
Revit/Dynamo; that extraction happens entirely upstream, outside this runbook.

**A manifest-like incremental mechanism does already exist in the codebase —
but it lives one layer downstream of Run A, at the segment level, and is not
wired into Run A at all.** `build_segment_manifest.py`/`run_segment_orchestrator.py`'s
`population_hash` per `segment_id`, tracked in `run_registry.csv`/
`segment_membership.csv`, is "the only genuine incremental-skip mechanism in
the repo" for this pipeline — it governs whether Run C re-runs
`patterns`/`bundle_analysis` for an unchanged segment, and has no connection to
Run A's `flatten`/`sig_hash`/`apply`/`placeholders` stages. A repo-wide search
of `tools/`, `runner/`, and `core/` (excluding `_archive/`/tests) for any
other per-export-file mtime/hash/output-presence cache found only
`tools/acc_scan_dc.py:149` (writes `st_mtime` into an ACC Desktop Connector
scan manifest — a pre-extraction "what to hydrate" inventory, unrelated to
Run A's flatten/apply/placeholders processing state). **Run A currently has no
file-level incremental mechanism of its own to build on or extend without
first checking; this investigation confirms none needs to be "discovered"
because none exists at the file level in Run A's own path — the closest
existing analog (segment-level `population_hash`) is architecturally
one layer downstream and does not gate anything Run A does.**

---

## 8. Acceptance-criteria cross-check

| Run | Table rows | Broader-pattern write-call grep count | Reconciled? |
|---|---|---|---|
| A | 20 | `run_extract_all.py` 17, `extractor.py` 12 real (9 flatten + item-level), `apply_join_policy.py` 2, `placeholder_exclusions.py` 4, `apply_name_key_policy.py` 1 | Yes — every real call site maps to a table row; no silent omissions found |
| B | 19 (17 distinct sites + 1 same-artifact double-write + conditional NameKey group) | `extractor.py` 12, `emit_element_dominance.py` 1 (called 2×), `build_label_population.py` 2, `patch_all_domain_patterns.py` 3, `generate_name_key_patterns.py` 5 | Yes, with the caveat noted inline that the literal 3-pattern grep the brief specifies (`.to_csv(`/`json.dump(`/`.write(`) returns 0 for several real writers in this codebase's `DictWriter`/`atomic_write_csv` convention — the broader pattern was needed for a meaningful cross-check |
| C | 30 grouped rows | 17 literal / 88 broader-pattern | Reconciled once repeated near-identical per-domain/per-view writes inside one producing function are grouped as stated per row; **two known gaps**: `analysis/authority_metrics.csv` and `analysis/element_dominance.csv`/`element_characterization_thresholds.csv` are produced at segment scope via the same shared code path Run B uses (confirmed by code-path inspection, not by the Run C sub-investigation's own table, which did not itemize them) — added as unnumbered rows in §4 rather than silently omitted |

No artifact was found written by code in Run A/B/C's traced call graph that is
missing from all three tables combined, once the two Run C gaps above are
accounted for.
