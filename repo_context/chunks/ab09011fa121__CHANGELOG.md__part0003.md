# Chunk of CHANGELOG.md

- Source relative path: `CHANGELOG.md`
- Chunk: 3 of 7
- Original line range: 797-1205
- Overlap lines with previous chunk: 0
- Symbols fully or partially present: none
- Source SHA-256: 4fec943c22afdfaa820cb9077538d951922289c152ad0d6436e45f8ff6d49213
- Starts inside symbol: no
- Ends inside symbol: no

```
   797|   Canonicalization intentionally matches what `join_hash`/`sig_hash` already do today
   798|   (`core/record_v2.canonicalize_str`/`canonicalize_str_allow_empty`: trim + missing-check
   799|   only) -- no case-folding or Unicode normalization is introduced. `DIMENSION_CONFIG`
   800|   (`tools/build_segment_manifest.py`) is unchanged and does not reference this
   801|   projection, so it cannot affect `population_hash`/segment membership. No existing
   802|   `join_hash`, `sig_hash`, or `identity_basis.items` value changes for any domain.
   803| 
   804| - **Escalation-target file coverage (D-024):** the four files
   805|   `generate_governance_narrative.py`'s own module docstring lists as "not
   806|   yet consumed directly" -- `comparison_registry.csv`,
   807|   `cross_segment_file_pairs.csv`, `pattern_reuse_summary_by_domain.csv`, and
   808|   `project_mean_file_pair_jaccard_matrix.csv` -- are now all registered as
   809|   `sibling_paths` (beside `--summary`'s directory, same inference the first
   810|   two already used) and get a full `governance_evidence_map.json` artifact
   811|   entry (`context_role`/`grain`/`can_answer`/`cannot_answer`/
   812|   `known_limitations`, same voice as every other archive_only sibling). A
   813|   new `_sibling_scan_fields()` helper (`tools/governance_evidence_package.py`)
   814|   reuses D-023's `_scan_csv_file()` -- no second scanning implementation --
   815|   to populate each entry's `columns` (name + inferred dtype) and `row_count`
   816|   when the file is present; both fields are simply absent when the file is
   817|   not present, since scanning a nonexistent path is meaningless. No sample
   818|   row or cell value is ever retained. Registering
   819|   `pattern_reuse_summary_by_domain`/`project_mean_file_pair_jaccard_matrix`
   820|   as known siblings also excludes them from
   821|   `inventory_export_directory_files()`'s generic undiscovered-file scan, so
   822|   each file gets exactly one narrative home (its own `can_answer`/
   823|   `cannot_answer`) instead of two competing descriptions of the same file.
   824|   `pattern_reuse_distribution`/`project_fragmentation_diagnostic` gained a
   825|   reverse `related_artifacts` link to their newly-registered siblings.
   826|   `governance_evidence_map.json` grows from 33 to 35 artifacts. Three D-023
   827|   tests that used these two files as stand-ins for "a generic undiscovered
   828|   file" were updated to fictitious filenames, since those two real filenames
   829|   no longer qualify. `docs/governance_interpretation_guide.md`'s escalation
   830|   section now pins the exhaustive four-file list (previously only gestured
   831|   at "another large sibling artifact") and points a reader at the new
   832|   `columns`/`row_count` fields before writing a filtered extraction script.
   833|   `docs/governance_question_routes.md`'s bc_to_bc/enterprise cascade note
   834|   gained a pointer to this same escalation path for cross-BC/enterprise
   835|   pattern-consistency questions needing file-level audit. No existing
   836|   classification, scoring, CSV column, or narrative content changed.
   837|   `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
   838|   (read-only dependency). PR-review fix folded in: `pattern_reuse_summary_
   839|   by_domain.csv`/`project_mean_file_pair_jaccard_matrix.csv`'s sibling paths
   840|   are anchored to whichever related optional flag was actually supplied
   841|   (`--reuse-by-client`/`--reuse-distribution`;
   842|   `--project-fragmentation-diagnostic`/`--project-union-jaccard-matrix`/
   843|   `--project-density-similarity-matrix`/`--project-pool-containment-matrix`),
   844|   falling back to `--summary`'s directory -- the same anchoring
   845|   `governance_relationships.csv` already used -- so a mixed-directory run
   846|   does not silently report these two escalation targets as absent. The
   847|   D-023 live-scan directories grew to include both new anchor directories
   848|   too. Second follow-up: both anchor chains also fall back to
   849|   `--union-inventory`/`--matrix-manifest` (written by the same
   850|   `compare_cross_segment.py` invocation to the same `--out-dir` as their
   851|   respective escalation target) before falling back to `--summary`'s
   852|   directory, so a run supplying only that broader optional input still
   853|   anchors correctly. See D-024 and `docs/governance_evidence_package.md`.
   854| - **Live file-availability inventory (D-023):** new `governance_file_inventory.json`
   855|   artifact in the governance evidence package, built fresh on every run by
   856|   `inventory_export_directory_files()` (`tools/governance_evidence_package.py`):
   857|   a `Path.glob("*.csv")` scan of the cross_segment export directory
   858|   (`--summary`'s parent) and, when it differs, the relationship-layer output
   859|   directory, excluding every path already tracked as an input/output/sibling
   860|   artifact elsewhere in the package. For each undiscovered file it records
   861|   the column header, an inferred per-column dtype (`integer`/`float`/
   862|   `boolean`/`string`/`empty` via `_column_dtype()`), and the row count --
   863|   never a sample row or cell value. `generate_governance_narrative.py`
   864|   attaches a one/two-sentence narrative per file
   865|   (`_narrative_for_inventory_entry()`): when the filename matches a
   866|   `matrix_name` already documented in `matrix_output_manifest.csv`, it
   867|   reuses that row's own `interpretation`/`known_limitations` text verbatim
   868|   (the same free-text narrative pattern `compare_cross_segment.py`'s
   869|   `add_manifest()` already uses for the registered `project_*` matrix
   870|   artifacts); otherwise it falls back to a structural sentence built only
   871|   from the header/row-count the scan already computed. Neither path
   872|   hand-maintains a per-filename description, so a brand-new future export
   873|   is picked up automatically with no follow-up code change. Confirmed
   874|   against the real gap this closes: `pattern_reuse_summary_by_domain.csv`
   875|   and `project_mean_file_pair_jaccard_matrix.csv` are both written by
   876|   `compare_cross_segment.py` but were never represented as evidence-package
   877|   artifacts before this change -- this generator's own code comments
   878|   already noted them "deliberately not consumed," but that note lived only
   879|   in Python, invisible to a reader of the package. `governance_brief.md`
   880|   gains a new `## Detail-Layer File Inventory` section rendering the same
   881|   already-scanned data (`render_file_inventory_brief_section()`, no second
   882|   scan) -- appended after the leadership questions, entirely omitted (not
   883|   blank-rendered) when the scan finds nothing undiscovered, and deliberately
   884|   not interleaved into the per-domain findings sections above it. Gating:
   885|   `governance_file_inventory.json` follows manifest/health/evidence-map/
   886|   findings under `--emit-evidence-package`; the `governance_brief.md`
   887|   section additionally requires `--emit-interpretation-layer`, matching the
   888|   rest of the brief. `governance_narrative_context.md` itself is unchanged
   889|   -- this phase adds no section there, preserving that document's existing
   890|   `--no-emit-evidence-package` "CSV/MD outputs unaffected" guarantee.
   891|   `governance_file_inventory` is registered as a 33rd `governance_evidence_map.json`
   892|   artifact (`authority_level: authoritative_deterministic_evidence`, empty
   893|   `related_artifacts` since the files it lists vary run to run). No existing
   894|   classification, scoring, CSV column, or narrative section changed.
   895|   `compare_cross_segment.py` and `build_segment_manifest.py` are unchanged
   896|   (read-only dependency). See D-023 and `docs/governance_evidence_package.md`.
   897| 
   898| ### Fixed
   899| - **A persistent failure in the new orchestrator-entry `name_all/` pre-clean itself
   900|   could escape unhandled, leaving the registry row untouched (PR3 follow-up, PR
   901|   review, third round):** `_clear_stale_name_all_before_run()` (added in the previous
   902|   fix below) ran before `_run_one_segment()`'s try/except machinery and its
   903|   registry-update block. If `retry_fs_op` exhausted every retry attempt (a persistent
   904|   lock, not just a transient one) and re-raised, the exception propagated straight out
   905|   of `_run_one_segment()`; the `ThreadPoolExecutor` caller's generic "unhandled
   906|   exception" handler only updates in-memory counters/`segment_results`, never
   907|   `registry_file`, so the segment's registry row (and `bundle_provenance.csv`) were
   908|   left at whatever they were before this run -- often `status=complete` from a prior
   909|   successful run -- and a later non-forced run would skip it forever, silently reading
   910|   stale Power BI output. The call is now wrapped in the same try/except pattern every
   911|   other step in `_run_one_segment()` already uses, setting
   912|   `step_failed = "clear_stale_name_all"` so the failure correctly reaches the
   913|   registry-update block. See
   914|   `audit_results/audit_10_bundle_bi_output_location_correction.md`.
   915| - **Stale `name_all/` survives an orchestrator-level failure, and an annotation failure
   916|   was recorded as segment success (PR3 follow-up, PR review, second round):** two
   917|   further gaps in the previous `name_all/` staleness fix. (1) A failure in
   918|   `run_segment_orchestrator.py`'s step 2b (name-pattern generation) or step 3 (config
   919|   bundle, which gates step 3b even under `comparison_target=both`) skips step 3b
   920|   entirely, so `run_bundle_analysis_for_target()` -- and its own upfront `name_all/`
   921|   clear -- is never invoked at all; a prior successful run's `name_all/` survived
   922|   completely untouched even though the segment is recorded as failed. New
   923|   `_clear_stale_name_all_before_run()` helper now clears it at the very start of
   924|   `_run_one_segment()`, before step 1 even runs, independent of which later step fails
   925|   (or whether the segment was already skipped as complete, which never reaches this
   926|   point at all). (2) The name-leg BI-merge block (`merge_bi_outputs()` +
   927|   `annotate_name_target_combined_files()`) only logged a warning on exception, leaving
   928|   `step_failed` unset -- since `_segment_has_name_leg_output()`'s "already ran" marker
   929|   (`bundle_provenance.csv`) is written earlier by step 3b, independent of this block, a
   930|   merge/annotate failure here still recorded `status=complete`, and a later non-forced
   931|   run would then skip this segment forever, permanently leaving Power BI with combined
   932|   files that are stale or missing the required `comparison_target`/`coverage_class`/
   933|   `provenance_note` columns. This block now sets `step_failed = "bi_merge_name"` on
   934|   exception, unlike the config leg's own (deliberately unchanged, pre-existing)
   935|   non-fatal `bi_merge` handling, which has no equivalent completion marker to protect.
   936|   See `audit_results/audit_10_bundle_bi_output_location_correction.md`.
   937| - **Stale `name_all/` survives a failed name-target bundle run (PR3 follow-up, PR review):**
   938|   `run_bundle_analysis_for_target()`'s name leg relocates its completed output to
   939|   `out_dir/name_all/` as its last step -- if staging, mining, or provenance generation
   940|   raised before reaching that step, a prior successful run's `name_all/` was left
   941|   completely untouched, so Power BI (pointed at `pPurgeView=name_all`) would silently
   942|   keep reading stale combined files from an old run even though the orchestrator marks
   943|   the current segment run failed. `name_all/` is now cleared upfront, before staging
   944|   starts, so a failed rerun leaves an empty/missing `name_all/` instead of misleadingly
   945|   stale data; a successful run still repopulates it normally. See
   946|   `audit_results/audit_10_bundle_bi_output_location_correction.md`.
   947| - `generate_governance_narrative.py`'s within-project `score_reliability` p10/p90
   948|   capture (the sole feeder of `score_reliability()`) was returning `Unknown` for
   949|   all 32 rendered domains in real corpora. Root cause: it only accepted a
   950|   `within_project` row when `a == b and _is_unscoped_segment(r,"a")` -- but
   951|   post `business_center_label`-promotion, the genuinely enterprise-wide root
   952|   segment for the only role that produces `within_project` pairs (`Project`) is
   953|   routinely demoted to `run_type="registration"` by `build_segment_manifest.py`'s
   954|   `redundant_single_child` pass (all Project-role files sitting in one business
   955|   center), and `compare_cross_segment.py`'s `discover_within_project()` -- unlike
   956|   `discover_cross_client()`/`discover_sibling_segments()`/`discover_parent_siblings()`,
   957|   fixed for the same mechanism in PR #380 -- never resolves the demoted root
   958|   through `_resolve_runnable_segment()`, so no `within_project` row for the root
   959|   is ever emitted at all. `build_cascade()` now accepts an optional
   960|   `segment_manifest` dict (loaded from a new optional `--segment-manifest`
   961|   CLI flag) and, when a row's own segment isn't directly unscoped, resolves the
   962|   true root (`f"{unit_system}|{role}"`) via `_resolve_runnable_segment()`
   963|   (imported read-only from `compare_cross_segment.py`); a row is accepted as
   964|   the enterprise-wide evidence source when it IS that resolved segment.
   965|   This is not a scope widening: `redundant_single_child` only fires on
   966|   byte-identical `population_hash`, so the resolved segment is the exact same
   967|   file population as the (never-discovered) root, just under a more specific
   968|   `segment_id` -- `score_reliability()`'s meaning is unchanged. A new
   969|   `wp_p10_source`/`within_project_reliability_source` field (cascade dict /
   970|   `governance_domain_summary.csv`) records which path fired
   971|   (`"enterprise"` vs `"enterprise_resolved:<segment_id>"`) for auditability only.
   972|   Verified against a real corpus: `score_reliability` goes from `Unknown` for
   973|   all 32 rendered domains to a real value for 31 of them (`materials` stays
   974|   `Unknown` -- no `within_project` data at all, an unrelated pre-existing gap);
   975|   `governance_tier` in `governance_domain_summary.csv` is byte-identical
   976|   before/after; the only other CSV column affected is `notable_anomalies`
   977|   (now correctly surfacing the pre-existing Presence-based/Sparse reliability
   978|   note instead of `Unknown` suppressing it). `compare_cross_segment.py` and
   979|   `build_segment_manifest.py` are unchanged (read-only dependency).
   980| 
   981| ### Added
   982| - New `governance_bc_summary.csv` + "Business Center Analysis" narrative section
   983|   in `tools/generate_governance_narrative.py`, structurally mirroring
   984|   `build_client_summary()`/`render_client_section()`/`governance_client_summary.csv`
   985|   (`build_bc_summary()`/`render_bc_section()`, one row per real business center).
   986|   Enterprise is deliberately NOT a row in this file -- it gets its own short
   987|   `## Enterprise Overview` section (`render_enterprise_section()`, reading the
   988|   existing `cascade[dom]["tc"]` enterprise::enterprise reading plus the pooled
   989|   Group 3 `eb`/`ec` means, rendered here for the first time -- still not tiered
   990|   or anomaly-detected). Two new additive parallel accumulators feed this:
   991|   `eb_by_bc[dom][bc_label]` (per-BC breakout of the existing pooled
   992|   `eb[dom]`/`eb_used[dom]`) and `tc_bc_by_bc[dom][bc_label]` (per-BC breakout of
   993|   `tc_by_scope[dom]["bc::bc"]`, which pools every real business center's own
   994|   Template->Container reading into one bucket today) -- both leave their
   995|   existing pooled/scoped counterparts byte-identical. New `bc_alignment_high`/
   996|   `_moderate`/`bc_confidence_low`/`_moderate_max_files` policy thresholds
   997|   (`policies/governance/governance_thresholds.json`) are hand-picked defaults
   998|   value-coincident with, but a separate profile from, `client_alignment_*`/
   999|   `client_confidence_*` -- confirmed via Step 0 that the existing client
  1000|   thresholds are hardcoded literals, not Jenks-derived (`tools/jenks_utils.py`/
  1001|   `compute_governance_thresholds.py` compute an unrelated split-detection
  1002|   threshold and are not wired into this generator at all), so this follows the
  1003|   established convention rather than introducing a new one. BC-to-BC peer
  1004|   alignment (`cross_bc_similarity_mean`) uses ALL-view as primary -- the
  1005|   OPPOSITE convention from `governance_client_summary.csv`'s used-view-primary
  1006|   `cross_client_similarity_mean` -- since `bc_to_bc` pairs compare Template/
  1007|   Container populations, not Project usage (see `_recommended_primary_view()`
  1008|   in `compare_cross_segment.py`); this is the exact bug class PR1's own
  1009|   bc_to_bc capture was written to avoid. Also fixes a real gap PR1's `bb`/
  1010|   `bb_used` accumulator had: its key was `f"{bc_a}::{bc_b}"` with no role
  1011|   component, so a Template-role bc_to_bc row and a Container-role bc_to_bc row
  1012|   for the same BC pair + domain would silently average together under one
  1013|   bucket; the key is now role-scoped (`f"{role}::{bc_a}::{bc_b}"`), caught
  1014|   while hand-verifying this PR's BC rows end-to-end against raw source values.
  1015|   `governance_client_summary.csv`, `governance_domain_summary.csv`, and every
  1016|   pre-existing narrative section are unaffected (verified via a synthetic-
  1017|   corpus trace: `build_client_summary()` output is dict-identical with/without
  1018|   BC-only comparison-type rows present, and BC-only cascade fields don't flip
  1019|   `_has_renderable_cascade_signal()` for any domain). `governance_bc_summary`
  1020|   is registered as a new artifact in the evidence-package layer
  1021|   (`governance_package_manifest.json`/`governance_evidence_map.json`,
  1022|   now 28 artifacts, up from 27) -- `governance_evidence_package.py`'s
  1023|   generic, dict-driven manifest builder required no changes; `build_evidence_map()`'s
  1024|   own hand-maintained artifact list did.
  1025| - `tools/generate_governance_narrative.py`'s `build_cascade()` now captures
  1026|   `bc_to_bc` rows (peer business-center comparisons from
  1027|   `discover_governance_chain()`'s scope-level fan-out) into the `cascade`
  1028|   dict under new keys `bb`/`bb_used`, keyed by domain then by the real
  1029|   `f"{business_center_label_a}::{business_center_label_b}"` pair (not by
  1030|   scope shape — `discover_governance_chain()` already guarantees the two
  1031|   sides are real, distinct business centers by construction, so no
  1032|   `_group1_scope_pair()`-style value-equality guard is needed at this
  1033|   layer). Uses `all_union_jaccard`/`used_union_jaccard` (population-similarity,
  1034|   directionless) rather than Group 3's `containment_a_in_b_mean`, because
  1035|   `bc_to_bc` pairs are symmetric peers, not a directed reference→target
  1036|   relationship — a single `containment_a_in_b` reading would silently
  1037|   privilege whichever business center's `segment_id` happened to sort first
  1038|   in `discover_governance_chain()`'s `combinations(sorted(sids), 2)`. Moved
  1039|   `bc_to_bc` out of `CASCADE_GROUP4_EXCLUDED_TYPES` into new
  1040|   `CASCADE_GROUP3B_TYPES`; same "captured only, not rendered/tiered/
  1041|   anomaly-detected" contract as `CASCADE_GROUP3_TYPES`. Additive only —
  1042|   `governance_domain_summary.csv`, `governance_client_summary.csv`, and the
  1043|   narrative brief are unaffected (verified byte-identical: `bb`/`bb_used`
  1044|   are not in `_CASCADE_RENDERABLE_SIGNAL_KEYS`, so
  1045|   `_has_renderable_cascade_signal()` and the domains it gates are unchanged).
  1046|   `client_cross_bc` remains excluded (separate, unresolved decision).
  1047| - `tools/generate_governance_narrative.py` now consumes two more
  1048|   `compare_cross_segment.py` outputs, narrative-side only (no changes to the
  1049|   producer). `render_union_reuse_summary()` gains an additive adoption-breadth
  1050|   cut from `pattern_reuse_summary_by_client.csv` (new optional
  1051|   `--reuse-by-client` flag) — how many of a domain's clients have at least
  1052|   one corpus-wide-reused pattern — alongside, and independent of, the
  1053|   existing distinct-pattern reuse table (unchanged; verified byte-identical
  1054|   before/after when `--reuse-by-client` is omitted).
  1055|   `pattern_reuse_summary_by_domain.csv` was evaluated and deliberately not
  1056|   wired in: its `n_patterns` duplicates the corpus-wide signal the existing
  1057|   distinct-pattern table already reports.
  1058|   A new top-level **Project Portfolio** section (new `render_project_portfolio_section()`,
  1059|   behind four new optional flags — `--project-union-jaccard-matrix`,
  1060|   `--project-density-similarity-matrix`, `--project-pool-containment-matrix`,
  1061|   `--project-fragmentation-diagnostic`) renders four paragraphs: footprint
  1062|   identity (`project_union_jaccard_matrix.csv`, ALL_DOMAINS-only, top/bottom-N
  1063|   project pairs), density similarity (`project_density_similarity_matrix.csv`,
  1064|   ALL_DOMAINS-only, cross-referenced against footprint identity for an
  1065|   explicit "same shape, different content" caveat when both matrices are
  1066|   supplied), peer-pool containment (`project_pool_containment_similarity_matrix.csv`,
  1067|   rendered as a per-project outlier list — this matrix carries no ALL_DOMAINS
  1068|   aggregate row, so the narrative means `pool_containment_similarity` across
  1069|   a project's available domains per `(project, pool_scope)` itself), and a
  1070|   fragmentation diagnostic (`project_fragmentation_diagnostic.csv`, which also
  1071|   folds in `project_mean_file_pair_jaccard_matrix.csv`'s signal via the
  1072|   diagnostic's own `exact_identity_overlap` column rather than consuming that
  1073|   matrix standalone). Each paragraph degrades to a one-line not-provided note
  1074|   when its source file is absent; the whole section is omitted only when all
  1075|   four are absent. This section is deliberately kept outside `assign_tier()`
  1076|   and `governance_domain_summary.csv` — project x project grain has no
  1077|   natural domain-tier slot, matching the existing guardrail in
  1078|   `docs/governance_generator_cross_compare_coverage.md` ("Do not use matrix
  1079|   values to override domain governance tiers directly"), not an oversight.
  1080|   `governance_domain_summary.csv`/`governance_client_summary.csv` output is
  1081|   unchanged before/after (verified byte-identical).
  1082|   `tools/governance_evidence_package.py`'s `build_evidence_map()` gains five
  1083|   matching artifact entries for the new inputs (evidence-map artifact count:
  1084|   22 → 27), required to keep the existing
  1085|   `test_manifest_input_artifact_ids_match_evidence_map_artifact_ids`
  1086|   invariant (every `governance_package_manifest.json` input must also appear
  1087|   in `governance_evidence_map.json`) satisfied — `build_package_manifest()`/
  1088|   `build_package_health()` already derive their input lists dynamically from
  1089|   `input_paths`, so they needed no code change, only the five new dict
  1090|   entries in `generate_governance_narrative.py`'s `main()`.
  1091| 
  1092| ### Fixed
  1093| - `compare_cross_segment.py`'s `discover_cross_client()`, `discover_sibling_segments()`,
  1094|   and `discover_parent_siblings()` were silently starved once
  1095|   `business_center_label` was promoted to a real `DIMENSION_CONFIG` cut
  1096|   dimension (peer to `client_label`/`discipline_label`): a client (or
  1097|   Template rollup) whose files all sit in a single business center makes its
  1098|   blank-bc rollup segment population-identical to that business-center-scoped
  1099|   child, so `build_segment_manifest.py`'s pre-existing `redundant_single_child`
  1100|   pass correctly demotes the rollup to `run_type="registration"` (avoiding a
  1101|   duplicate-population run) — but all three discovery functions require
  1102|   `run_type in ("bundle", "reference")`, so a demoted rollup vanished from
  1103|   `cross_client`/`sibling_projects`/`parent_sibling_roles` entirely rather
  1104|   than being paired via its population-identical descendant. A single-bc
  1105|   client is now the common case in real corpora post-promotion (previously
  1106|   business_center_label was always blank, so the rollup itself was always the
  1107|   only representative). New `_redundant_child_segment_id()` reads the
  1108|   `redundant_single_child:<segment_id>` note `build_segment_manifest.py`
  1109|   already records (segment_id itself uses `|` as its internal separator, and
  1110|   the pass always runs last, so everything after the marker to end-of-string
  1111|   is taken rather than naively splitting on `|`); new
  1112|   `_resolve_runnable_segment()` follows that pointer *transitively* (a
  1113|   redundant rollup's pointed-to child can itself be redundant one level
  1114|   deeper — e.g. a Template rollup with a real, effectively-constant client
  1115|   value colliding with a further BC-scoped collision) until it reaches an
  1116|   eligible segment or a dead end, with cycle protection. All three discovery
  1117|   functions now resolve each candidate segment through this helper before
  1118|   admitting it, using the *resolved* descendant as the actual pairing unit
  1119|   while classifying role/grain from the *original* row (a blank-role,
  1120|   client-only "all governance roles" rollup can itself be redundant to a
  1121|   role-scoped descendant if that client happens to have only one role
  1122|   present; classifying by the descendant's role would misfile it as a
  1123|   genuine Project/Template sibling it was never scoped to be —
  1124|   `discover_parent_siblings()` guards this explicitly). This is not the
  1125|   "loosen the blank-bc requirement" anti-pattern `_is_client_only_project_segment()`'s
  1126|   docstring warns against: the substitute segment carries the exact same
  1127|   `population_hash` the demoted rollup would have, not a narrower slice of
  1128|   it. `build_segment_manifest.py`, `generate_governance_narrative.py`, and
  1129|   `_is_unscoped_segment()`/the Group 1/2/3 cascade logic are unchanged.
  1130| - (PR #380 Codex review) The fix above kept `segment_id_a`/`_b` as the
  1131|   resolved descendant (required — it's the only segment with real on-disk
  1132|   data), but `_build_summary_row()` also derives
  1133|   `business_center_label_a`/`_b`, `discipline_label_a`/`_b`, and
  1134|   `scope_level_a`/`_b` straight from that same segment's own manifest row —
  1135|   so a rescued row showed the resolved descendant's own (narrower) scope in
  1136|   `cross_segment_summary.csv` (e.g. `business_center_label_b="BC_C"`) instead
  1137|   of the broader, typically blank-bc population the pair was actually matched
  1138|   under. New `_stash_scope_override()`/`_scope_override_key()` record the
  1139|   *original* row's `business_center_label`/`discipline_label`/`scope_level`
  1140|   onto the resolved descendant's manifest entry, namespaced by
  1141|   `comparison_type` (the same physical segment can legitimately appear under
  1142|   its own true bc-scoped identity in a different comparison_type, e.g.
  1143|   `discover_client_cross_bc()`); `_build_summary_row()` now prefers this
  1144|   override when present. Applied to `cross_client`/`sibling_projects`
  1145|   (neither has a consumer that re-derives scope from `segment_id`, so this is
  1146|   a pure accuracy fix) but deliberately **not** to `parent_sibling_roles`:
  1147|   that comparison_type feeds `generate_governance_narrative.py`'s
  1148|   `_group1_scope_pair()`/`_is_unscoped_segment()`, which classifies
  1149|   "enterprise" scope by re-deriving structure from `segment_id_a`/`_b` itself
  1150|   (every `|`-part past index 2 must be blank) rather than trusting the label
  1151|   columns — since `segment_id` can't be overridden without breaking data
  1152|   lookup, no column override changes that already-shipped classification; it
  1153|   would only make the row internally inconsistent (columns disagreeing with
  1154|   segment_id) for no benefit. A rescued `parent_sibling_roles` row therefore
  1155|   still reports its resolved descendant's true (non-blank) scope, landing in
  1156|   whichever non-enterprise `tp_by_scope` bucket that shape implies — a real,
  1157|   if not headline, Group 1 evidence source, not a regression.
  1158| - (PR #376 review) The union-metric adoption above silently dropped all
  1159|   `within_project` evidence: `compare_cross_segment.py`'s dedicated
  1160|   within-project branch (project-internal file-pair aggregation) returns
  1161|   its summary row before reaching the normal path's `_union_similarity()`
  1162|   call, so `all_union_jaccard`/`used_union_jaccard` are never populated for
  1163|   this comparison type — only `all_pairwise_jaccard_mean`/
  1164|   `used_pairwise_jaccard_mean` ever are. `wp_all`/`wp_disc`/`wp_used`/`tw`
  1165|   in `build_cascade()`, `wp_by_client` in `build_client_summary()`, and
  1166|   `disc_domain_wp` in `render_discipline_section()` now read the union
  1167|   field first via a new `_col_union_or_pairwise()` helper, falling back to
  1168|   the pairwise-mean family only when union is blank — a no-op for
  1169|   `cross_client`/`sibling_projects` (which always populate union fields
  1170|   when they have real data, confirmed by reading the producer's non-directed
  1171|   branch) and a real value for `within_project`. Without this, every
  1172|   client's `wp_mean`/within-project coherence and every within_project-fed
  1173|   cascade signal (`wp_all`, `tw`, the `phases` domain guidance trigger,
  1174|   reliability-note text) silently read as unavailable against real exports,
  1175|   despite the underlying pairwise data being present in the CSV the whole
  1176|   time.
  1177| - (PR #376 review, P2) `render_discipline_section()`'s within-project loop
  1178|   has no `governance_role_a` gate — unlike `wp_by_client` in
  1179|   `build_client_summary()`, it also processes discipline-scoped Template/
  1180|   Container/Generic standards segments self-compared for internal
  1181|   consistency, not just Project rows. The union-metric adoption above made
  1182|   the bare/primary value used-view unconditionally, but
  1183|   `_recommended_primary_view()` only makes used-view primary for Project
  1184|   targets — a Template/Container/Generic segment can have no used-view
  1185|   membership at all (used-view is annotation-only for those roles), which
  1186|   silently dropped that discipline's real all-view coherence from the
  1187|   section entirely. Primary is now picked per row by
  1188|   `governance_role_a == "Project"`: used-view for Project rows (unchanged),
  1189|   all-view for every other role, matching this section's pre-union-adoption
  1190|   behavior for non-Project rows. The all-view secondary (`disc_domain_wp_all`)
  1191|   is only populated for Project rows — there is no meaningful secondary to
  1192|   show for a role where used-view isn't primary in the first place.
  1193| - (PR #376 review, second P2 finding) The fix above stores the all-view
  1194|   value into `domain_means` for non-Project rows, but the rendered
  1195|   "Mean within-population coherence" sentence still unconditionally said
  1196|   `used-view, active practice` regardless of what actually fed that
  1197|   discipline's aggregate — misstating configured standards evidence as
  1198|   active usage for a Template/Container/Generic-only discipline. A new
  1199|   per-discipline `disc_role_mix` tracks whether a discipline's rows were
  1200|   Project-only, non-Project-only, or both; the label is now
  1201|   `used-view, active practice` (Project-only), `all-view, configured
  1202|   standards` (non-Project-only), or a neutral
  1203|   `mixed used-view (Project rows) / all-view (standards rows)` for a
  1204|   discipline fed by both.
  1205| 
```
