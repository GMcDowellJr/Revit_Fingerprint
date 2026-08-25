# Analysis Pipeline Topology — Findings

**Status:** Read-only investigation, findings only. No source files were edited. This document is the
skeleton for a later, separately-scoped Phase 1 (in-code trace-block docstrings + generated MD).

**Scope:** Everything under `tools/` except `tools/probes/`. `runner/`/`core/`/`domains/` were touched
only to answer the boundary question (Task 1). `tools/corpus_update_runbook.ps1` and
`tools/label_refresh_runbook.ps1` are treated as the authoritative source of execution order; all
order claims below cite a specific line number in one of those two files.

**Method:** Six parallel read-only research passes (one per directory cluster) gathered raw file-level
facts (line counts, docstrings, argparse flags, import graphs, `.csv`/`.json` I/O). This document is
the synthesis of those passes, cross-checked against `CLAUDE.md`, `docs/tools_DEPRECATED.md`, and
direct reads of both runbooks and `tools/extractor.py`/`tools/run_extract_all.py`.

**Inventory count: 150 of 150 `.py` files under `tools/` (excluding `tools/probes/`) inventoried
exactly once.** See §2.

---

## 1. Boundary determination

**Finding: `tools/extractor.py` and `tools/run_extract_all.py` are both ANALYSIS-side.** They organize,
validate, and cluster JSON that a separate Revit/Dynamo-side process has already written to disk — they
do not drive Dynamo or a live Revit document. The analysis pipeline's true starting point is *"the
moment `*.details.json`/`*.index.json` files exist in an exports directory"*; `tools/extractor.py` and
`tools/run_extract_all.py` are the first stage past that point, not the last stage of extraction.

Evidence:

- Neither file imports `Autodesk.Revit.DB`, Dynamo, or `DocumentManager`. A repo-wide grep across both
  files for `Autodesk|Revit|Dynamo|IN\[0\]|DocumentManager` returns exactly one hit, and it is a string
  literal, not an import: `tools/extractor.py:191/193`, `"""Extract project folder name from Autodesk
  Docs:// path..."""` / `prefix = "Autodesk Docs://"` — parsing an ACC path segment that is already
  present as text inside exported JSON, not a live API call.
- `tools/run_extract_all.py`'s `main()` declares `exports_dir` as its required first positional CLI
  argument: `ap.add_argument("exports_dir", help="Folder containing fingerprint exports
  (*__fingerprint.json, or legacy *.details.json / *.index.json).")` (`run_extract_all.py:746`). The
  entire tool operates on a folder of files already on disk.
- `tools/run_extract_all.py:22` imports the actual engine directly from `extractor.py`:
  `from extractor import emit_analysis, emit_records`. `run_extract_all.py` is the stage-machine
  CLI/subprocess-orchestrator wrapper; `extractor.py` is the library doing the real work
  (`emit_records` for the `flatten` stage, `emit_analysis` for `authority`/`patterns`).
- `extractor.py`'s own file-discovery helpers (`_iter_export_files`, `_pick_sample_file`,
  `_infer_domains`) all take an `exports_dir: Path` and glob for `*.json` / `*.details.json` /
  `*.index.json` already on disk (`extractor.py:67`, `:601`, `:656`).
- `CLAUDE.md` itself (pre-existing project documentation, not derived here) states: *"Analysis
  (`tools/`) — runs on a developer machine against exported JSON/CSV, has no Revit dependency"* and
  describes `extractor.py` as *"Engine behind the `patterns`/`authority` stages —
  `run_extract_all.py` imports `emit_analysis`/`emit_records` from this module directly."*

**Consequence for scope:** `domains/`, `runner/`, `core/` remain out of scope as the true Revit/Dynamo
extraction layer that *produces* the exports directory's contents. `tools/probes/` remains out of scope
as Revit-API probe scripts that run inside Revit — structurally on the extraction side despite living
under `tools/`.

---

## 2. Full file inventory

150 of 150 in-scope `.py` files, one row each. Grouped by directory in roughly pipeline-execution order.
**Invocation** column values: a runbook citation (`Run A l.105`, `L2 l.59`, etc.) means directly
invoked by that runbook line; "subprocess of X" / "imported by X" name an in-repo caller; "standalone /
no caller found" means a repo-wide grep found no importer and no runbook/script reference — treat these
as manual analyst tools unless flagged **ORPHANED** (no caller *and* apparently superseded/duplicated).

### 2.1 `tools/` root — orchestration core, policy tooling, governance (44 files)

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `acc_scan_dc.py` | 315 | Walks a root folder, finds Revit files, writes an include-flagged scan manifest CSV | Standalone — precedes extraction entirely (feeds `acc_sync_dc.py` and the pyRevit `BatchExtract` step that produces the exports) |
| `acc_sync_dc.py` | 405 | Windows Desktop Connector pre-sync: hydrates online-only stub files before extraction | Standalone; consumes `acc_scan_dc.py`'s manifest |
| `analyze_promotion_candidates.py` | 1066 | Scope-consistency classifier: pattern reuse breadth vs. governed scope | Standalone, explicitly not pipeline-wired (confirms CLAUDE.md) |
| `apply_join_policy.py` | 208 | T2 "apply" stage: computes/overwrites `join_hash`/`join_key` in `records.csv` from a join-key policy | subprocess of `run_extract_all.py`'s `apply` stage (Run A, `--stages ...,apply,...`, l.108) |
| `apply_name_key_policy.py` | 142 | PR1: reconstructs `join_key_name_identity` from exported JSON via `core/name_key_builder.py` | **Run A‑NameKey, l.116** |
| `build_results_registry.py` | 136 | Builds BI-friendly `results_registry.csv` from `segment_manifest.csv` + `run_registry.csv` | **Run C2.5, l.266** |
| `build_segment_manifest.py` | 886 | Builds the full segmentation lattice (every subset of unit_system/governance_role/client/discipline/business_center/collection) | **Run C1, l.186** |
| `compare_cross_segment.py` | 5383 | Cross-segment comparison using `join_hash` as identity unit (Jaccard/containment, all/used, pooled, bundle-annotated) | Standalone CLI; not called by either runbook (runbook itself says "run separately", l.280); also a library (`compare_governance_populations.py` imports its comparison functions) |
| `compare_governance_populations.py` | 364 | Same containment/Jaccard mechanics applied to `governance_manifest.py`'s disjoint populations | Standalone; imports from `compare_cross_segment.py` |
| `compute_governance_thresholds.py` | 242 | Jenks natural-breaks on per-domain alignment rates from split-analysis cluster summaries | Standalone / no pipeline caller found (only referenced in a `generate_governance_narrative.py` comment and exercised by a test) |
| `compute_latent_purgeable.py` | 828 | Finds records one hop from purgeable ("latent purgeable") for reference domains | subprocess of `bundle_analysis/run_bundle_analysis.py`, conditional (only if `latent_purgeable.csv` missing) — reached via Run C2 |
| `discover_hash_policy.py` | 363 | Greedy/Pareto discovery of sig-hash policy candidates | Wired into `run_extract_all.py`'s `discover` stage (l.932) but `discover` is **not** in either runbook's confirmed `--stages` — not exercised by confirmed invocations |
| `discover_join_policy.py` | 863 | T1 join-key policy discovery (greedy/Pareto, `--stratify-by`, full-population re-verification) | Not called by either runbook or `run_extract_all.py` directly; imported as a library by `discover_hash_policy.py`, `suggest_discovery_params.py`, `validate_policy_field_coverage.py`; also subprocess-called by `discovery_orchestrator.py` (third, out-of-scope runbook) |
| `discovery_orchestrator.py` | 466 | Cache-aware staged orchestration for join-key/sig-hash discovery sweeps across all domains | Called only by `tools/run_discovery_sweep.py` / the **out-of-scope third runbook** `tools/run_discovery_sweep.ps1` |
| `domain_authority.py` | 966 | Phase-1 authority summary: per-domain baseline coverage, cluster summary, authority outcome | Standalone; no importer found; part of the "pre-authority probe mode" manual Phase-1 chain |
| `emit_element_dominance.py` | 305 | Per-domain element-level dominance/BI export stats (Jenks thresholds) | imported by `run_extract_all.py` (l.21) and called in-process inside the `patterns` stage (l.1166) — reached via Run B1 |
| `enterprise_policy.py` | 98 | Loads immutable enterprise-identity policy; writes provenance metadata | Library only, no `__main__`; imported by `generate_governance_narrative.py`, `analyze_promotion_candidates.py`, `governance_manifest.py`, `compare_cross_segment.py`, `governance_relationships.py` |
| `export_bundle_pattern_detail.py` | 946 | Exports the bundle→pattern→identity_items→name_population chain for one segment as flat BI CSVs | **L4, l.91** (also referenced in CLAUDE.md as a standalone BI export utility) |
| `export_to_flat_tables.py` | 545 | Flattens export JSON directly into flat CSV tables (e.g. layer stacks) | Wired into `run_extract_all.py`'s `flat_tables` stage (l.1215) but that stage is **not** in either runbook's confirmed `--stages` |
| `extract_segment_subtree.py` | 706 | Pulls one segment + ancestor chain's `cross_segment_*.csv` rows into a standalone subset | Standalone; no caller found; ad hoc diagnostic downstream of `compare_cross_segment.py` |
| `extractor.py` | 1515 | Core engine: `emit_records()` (flatten), `emit_analysis()` (authority/patterns clustering, `_stable_pattern_id()`) | Library — imported by `run_extract_all.py` (l.22); see §1 |
| `generate_governance_narrative.py` | 6890 | Deterministic `governance_narrative_context.md` renderer + governance evidence package | Standalone terminal-stage CLI; not called by either runbook |
| `generate_name_key_patterns.py` | 294 | PR2: pattern generation over `--comparison-target {config,name,both}` (config reproduces `join_hash` patterns; name clusters `join_key_name_identity`) | **Run B‑NameKey, l.159**; also invoked per-segment by `run_segment_orchestrator.py`'s Step 2b |
| `generate_sig_hash_policy.py` | 67 | Compiles `contracts/domain_identity_keys_v2.json` into `policies/domain_sig_hash_policies.json` | Standalone; no caller found; default output path matches `$SIG_POL` exactly — presumed manual pre-step run when the contract changes |
| `governance_evidence_package.py` | 1621 | Builds manifest/health/evidence-map/findings/file-inventory artifacts for the governance evidence package | Library only (pure functions, no CLI); imported exclusively by `generate_governance_narrative.py` |
| `governance_manifest.py` | 375 | Builds disjoint governance populations (Enterprise/BC/client/project/Generic) from `file_metadata.csv` | Standalone CLI; also imported by `compare_governance_populations.py` and `governance_relationships.py` |
| `governance_policy.py` | 99 | Generic JSON policy-profile loader for `policies/governance/*.json` | Library only; imported only by `generate_governance_narrative.py` |
| `governance_relationships.py` | 335 | Project-level composition + BC↔client rollups from `file_metadata.csv` | Standalone CLI; imports `governance_manifest.py`; its 2 output matrix CSVs are consumed by `generate_governance_narrative.py` as a **file input via CLI flag**, not an import — manual hand-off |
| `inspect_lft_similarity.py` | 935 | Single-pass `loaded_family_types` similarity analysis across a corpus | Standalone diagnostic; no caller found |
| `jenks_utils.py` | 74 | Pure-Python Fisher-Jenks natural-breaks | Library only; imported by `compute_governance_thresholds.py`, `export_bundle_pattern_detail.py`, `emit_element_dominance.py`, `compare_cross_segment.py`, `bundle_analysis/step2_find_bundles.py`, `archetype/cluster_archetype_signals.py` |
| `join_key_derivation.py` | 8 | Backward-compat shim: `from _archive.join_key_derivation_phase05 import *` | Library; imported by `apply_join_policy.py` (reached via Run A) |
| `na_token.py` | 34 | Shared "N/A"-spelling detection (`is_na_token`) | Library only; imported by `governance_manifest.py`, `compare_cross_segment.py`, `build_segment_manifest.py`, `run_extract_all.py`, `governance_relationships.py` |
| `pairwise_analysis.py` | 162 | Phase-1 domain pairwise coverage matrix/summary | Standalone; no importer found; chained to `domain_authority.py` by filename only |
| `pareto_joinkey_search.py` | 856 | Multi-objective (Pareto-front) join-key candidate search vs. `sig_hash` | Both standalone CLI and library — imported by `discover_join_policy.py`, `join_key_discovery/eval.py`, `patterns_analysis/_archive/pareto_with_splits.py`, `suggest_discovery_params.py` |
| `pattern_id_utils.py` | 86 | Shared `pattern_id`/`pattern_label` formula (independent reimplementation of `extractor.py`'s private `_stable_pattern_id()`) | Library only; imported by `generate_name_key_patterns.py` |
| `population_framing.py` | 189 | Phase-1 population/baseline framing (domain clusters + authority + run config) | Standalone; no importer found; third step of the manual Phase-1 chain |
| `reset_wall_types_for_reapply.py` | 127 | Resets `wall_types` records blocked solely on `wt.function=unsupported.not_applicable` | Standalone manual two-step workflow (docstring instructs re-running `run_extract_all.py --stages sig_hash` afterward) |
| `run_discovery_sweep.py` | 12 | Backward-compat thin wrapper delegating to `discovery_orchestrator.main` | Delegator; driven by the out-of-scope third runbook `tools/run_discovery_sweep.ps1` |
| `run_extract_all.py` | 1234 | **Central stage-machine orchestrator**: `flatten→sig_hash→discover→apply→placeholders→authority/patterns→split→flat_tables` | **Run A, l.105; Run B1, l.142**; also subprocess-invoked per-segment by `run_segment_orchestrator.py` (`--stages patterns`) |
| `run_segment_orchestrator.py` | 1867 | Per-segment execution of patterns then bundle_analysis stages, in segment-lattice level order | **Run C2, l.245** |
| `run_split_detection_all.py` | 618 | Complete split-detection workflow orchestrator over one domain (10-step sequence, see §3) | Not in either target runbook; subprocess of `run_extract_all.py`'s opt-in `split` stage (l.1206); also standalone CLI |
| `similarity_compare.py` | 246 | Whole-file domain-level similarity scoring | Standalone; **deprecated in place**, superseded by `compare_cross_segment.py` (`docs/tools_DEPRECATED.md`) |
| `suggest_discovery_params.py` | 639 | Pre-flight sizing calculator for `discover_join_policy.py`'s sampling flags | Standalone advisory tool; imports `pareto_joinkey_search.py` |
| `validate_policy_field_coverage.py` | 404 | Validates that every policy-declared field appears as an observed `item_key` in the corpus | Standalone, explicitly "single-pass scan" per its own docstring; not called by `discover_*` tools despite documenting the gap they leave |

### 2.2 `tools/bundle_analysis/` — bundle-mining pipeline (18 files)

Confirmed **live pipeline stage**, invoked as `subprocess` of `run_segment_orchestrator.py`'s Step 3 /
Step 3b (reached via **Run C2, l.245**), which in turn calls `run_bundle_analysis.py` for every
`run_type=bundle` segment.

| File | Lines | Purpose | Invocation / step |
|---|---|---|---|
| `__init__.py` | 1 | Package marker | — |
| `common.py` | 85 | Shared CSV I/O, hashing, `retry_fs_op` (documented OneDrive-lock retry) | Library; imported by nearly every file below plus `run_segment_orchestrator.py` |
| `name_projection_adapter.py` | 362 | Stages PR2's name-identity-projection output into the shape step0–step7 expect, for `--comparison-target name/both` | Imported by `run_bundle_analysis.py` and `run_segment_orchestrator.py` |
| `placeholder_exclusions.py` | 229 | Current-generation per-domain placeholder/purgeable-type exclusion generator | subprocess of `run_extract_all.py`'s `placeholders` (T2b) stage (Run A); **also** imported as a library by `run_bundle_analysis.py` (wrapper delegates to the legacy schema below) |
| `placeholder_exclusions_legacy.py` | 124 | Older, simpler file-level exclusion heuristic (wall/ceiling/floor/roof types only) | Imported only by `placeholder_exclusions.py`'s compat wrapper — **this is the schema `run_bundle_analysis.py`'s `discover_populations()` actually consumes**, not `placeholder_exclusions.py`'s own richer `main()` output |
| `reference_bundle.py` | 161 | Read/write/validate `reference_bundle.json` sidecar (human-curated baseline) | Imported by `run_bundle_analysis.py`, gated behind `--compare` |
| `run_bundle_analysis.py` | 1202 | Top-level orchestrator: placeholder exclusions → step0 → (step1→step7)×domain×population×scope×{all,used} → optional `--compare` | Step 3/3b of `run_segment_orchestrator.py`; also standalone CLI |
| `step0_discover_populations.py` | 491 | Discover corpus "populations" per domain/scope (root closed itemsets, viability check) | Step 0 |
| `step1_membership_matrix.py` | 267 | Build per-domain(/population/scope) file×pattern membership matrix | Step 1 |
| `step2_find_bundles.py` | 394 | Mine closed frequent itemsets ("bundles") with auto-derived support threshold | Step 2 |
| `step2b_bundle_share_profile.py` | 256 | Optional per-bundle median/mean pattern-share profile | Step 2b, opt-in (`--compute-share-profile`) |
| `step3_build_dag.py` | 242 | Build parent/child DAG over bundles via subset relationships | Step 3 |
| `step4_difference_sets.py` | 154 | Per DAG edge, compute pattern-id difference set | Step 4 (function named `emit_stub` but fully implemented) |
| `step5_classify_patterns.py` | 237 | Classify each pattern's `bundle_role` via the DAG | Step 5 (fully implemented despite `emit_stub` name) |
| `step6_classify_files.py` | 230 | Select each file's "primary" bundle + noise stats | Step 6 (fully implemented despite `emit_stub` name) |
| `step7_overlap_report.py` | 66 | Cross-bundle/pattern overlap report | Step 7 — **genuine no-op stub**: writes header-only empty CSVs, no computation logic |
| `step_compare.py` | 183 | Score file coverage against `reference_bundle.json`, merge into `file_gap_report.csv` | Opt-in comparison stage, runs after step0–step7 when `--compare` |
| `utils.py` | 122 | Core itemset-mining algorithms (`find_closed_itemsets`, `find_root_bundles`) | Library; imported by `step2_find_bundles.py` and `step0_discover_populations.py` |

### 2.3 `tools/join_key_discovery/` (3 files)

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `eval.py` | 313 | Core join-key candidate evaluation library (identity index, shape-gating, coverage/collision/fragmentation/HHI scoring) | Library; imported by `discover_hash_policy.py`, `discover_join_policy.py`, `apply_join_policy.py`, `suggest_discovery_params.py`, `pareto_joinkey_search.py`, and `greedy.py` |
| `greedy.py` | 113 | Greedy forward-selection search over candidate join-key fields | Library; imported only by `discover_join_policy.py` |
| `materials_joinkey_discover.py` | 491 | Standalone, self-contained two-pass join-key discovery specific to `materials` | **ORPHANED** — own full CLI, zero importers found; docstring even cites a stale path (`tools/materials_joinkey_discover.py`, missing the `join_key_discovery/` segment) |

### 2.4 `tools/lib/` (4 files)

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `__init__.py` | 0 | Package marker | — |
| `diff_engine.py` | 656 | Generic domain-family diff engine (`run_comparison(profile, args)`) | **ORPHANED** — imports `domain_profile.py`, but nothing in the repo imports `diff_engine.py` itself; no CLI wrapper exists anywhere |
| `domain_profile.py` | 175 | `DomainProfile`/`ResolutionSpec` dataclasses configuring the diff engine | Imported by `diff_engine.py` and `vt_profile.py` — not orphaned itself, but both consumers are |
| `vt_profile.py` | 461 | `ViewTemplateDomainProfile` + `make_vt_profile()` for the 5 view_templates partitions | **ORPHANED** — zero importers; distinct from and NOT used by `compare_templates_stand-alone/`, despite topical overlap |

### 2.5 `tools/governance/` (1 file)

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `standards_governance_report.py` | 497 | Standalone HTML report generator: baseline drift, unnecessary template overrides, common patterns | Standalone; reads raw `*.details.json` export files **directly** via CLI positional args (`python tools/governance/standards_governance_report.py export1.json export2.json ...`) — bypasses the entire CSV pipeline; zero internal-repo imports; not connected to `compare_cross_segment.py`/`generate_governance_narrative.py`'s newer pipeline |

### 2.6 `tools/label_synthesis/` (26 files)

Mixed role: one live pipeline-integrated file (`label_resolver.py`), one script shared across **both**
runbooks (`patch_all_domain_patterns.py`), a dedicated runbook of its own (L1–L4 in
`label_refresh_runbook.ps1`), two dynamically-loaded plugin families, and two orphaned/standalone files.

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `__init__.py` | 1 | Package marker | — |
| `build_identity_items_lookup.py` | 345 | Pre-flattens `records.csv`+`identity_items.csv` into `identity_items_by_joinhash.csv` | **L1, l.26** |
| `build_label_population.py` | 182 | Derives per-domain `{domain}.joinhash_label_population.csv` | subprocess of `run_extract_all.py` (unconditional inside `authority`/`patterns` stage, ~l.1037) — reached via Run B1 and per-segment Run C2; also has its own CLI |
| `build_semantic_groups.py` | 1196 | LLM-based "semantic_group" governance-intent labeling for a fixed domain subset | **ORPHANED from either runbook** — only referenced in a `corpus_update_runbook.ps1` *comment* (l.307), never actually invoked |
| `domain_prompts/__init__.py` | 1 | Package marker | — |
| `domain_prompts/arrowheads.py` | 371 | LLM prompt-building plugin for `arrowheads` | Loaded dynamically by `synthesize_fragmented_labels.py._load_domain_prompt_module()` (`importlib.import_module("tools.label_synthesis.domain_prompts.{name}")`, with trailing-segment fallback) |
| `domain_prompts/dimension_types.py` | 660 | Plugin for `dimension_types_*` partitions (shape-aware) | Loaded dynamically |
| `domain_prompts/fill_patterns.py` | 387 | Plugin for `fill_patterns_drafting`/`fill_patterns_model` | Loaded dynamically |
| `domain_prompts/line_patterns.py` | 328 | Plugin for `line_patterns` | Loaded dynamically |
| `domain_prompts/line_styles.py` | 380 | Plugin for `line_styles` | Loaded dynamically |
| `domain_prompts/text_types.py` | 352 | Plugin for `text_types` | Loaded dynamically |
| `domain_prompts/view_filter_definitions.py` | 292 | Plugin for `view_filter_definitions` | Loaded dynamically; no `synopsis_formatters` counterpart exists |
| `label_resolver.py` | 414 | Core 5(+2.5)-layer label resolution chain (curator→synopsis→near-dup merge→modal→LLM cache→rank fallback) | **Imported by `tools/extractor.py` (l.23)** — the one `label_synthesis/` file wired directly into the live `authority`/`patterns` pipeline; also imported by `synthesize_fragmented_labels.py` and `patterns_analysis/_archive/annotate_cluster_labels.py` |
| `patch_all_domain_patterns.py` | 388 | Recursively patches `pattern_label_human`/`pattern_label_source` in every `domain_patterns.csv` from the shared LLM cache | **Shared by both runbooks: Run B2 (l.149), Run C3 (l.273), and L3 (l.67)** — confirmed 3 call sites, 2 runbooks, 1 script |
| `patch_domain_patterns_labels.py` | 349 | Single-file targeted variant of `patch_all_domain_patterns.py` | **ORPHANED / likely superseded** — no caller anywhere; a third copy of the same modal-check logic (after `label_resolver.py` and `patch_all_domain_patterns.py`) |
| `synopsis_formatters/__init__.py` | 1 | Package marker | — |
| `synopsis_formatters/arrowheads.py` | 83 | Deterministic synopsis formatter for `arrowheads` | Loaded dynamically by `label_resolver.py._get_synopsis_formatter()` (`importlib.import_module("label_synthesis.synopsis_formatters.{domain}")`, exact match, no fallback) |
| `synopsis_formatters/dimension_types.py` | 252 | Formatter for `dimension_types_*` | Loaded dynamically |
| `synopsis_formatters/fill_patterns.py` | 63 | Formatter for `fill_patterns_*` | Loaded dynamically |
| `synopsis_formatters/line_patterns.py` | 83 | Formatter for `line_patterns` | Loaded dynamically |
| `synopsis_formatters/line_styles.py` | 85 | Formatter for `line_styles` | Loaded dynamically |
| `synopsis_formatters/object_styles_annotation.py` | 92 | Formatter for `object_styles_annotation` | Loaded dynamically; no `domain_prompts` counterpart |
| `synopsis_formatters/object_styles_model.py` | 114 | Formatter for `object_styles_model` | Loaded dynamically; no `domain_prompts` counterpart |
| `synopsis_formatters/phase_filters.py` | 64 | Formatter for `phase_filters` | Loaded dynamically; no `domain_prompts` counterpart |
| `synopsis_formatters/text_types.py` | 79 | Formatter for `text_types` | Loaded dynamically |
| `synthesize_fragmented_labels.py` | 1023 | Offline batch LLM labeling of fragmented pattern labels into `llm_name_cache.json` | **L2, l.59** (per-domain loop) |

### 2.7 `tools/patterns_analysis/` + `_archive/` — split detection (37 files)

**Key finding:** the 3 top-level `patterns_analysis/*.py` files (`split_detection.py`,
`split_detection_file_level.py`, `split_detection_element_level.py`) are **byte-for-byte identical**
(confirmed via `diff -q`, zero output) to their same-named `_archive/` counterparts, and are never
imported by anything — `run_split_detection_all.py` and the one test that touches this code
(`tests/test_split_named_clusters_and_thresholds.py:11`) both import exclusively from the `_archive.*`
path. So despite the directory name, **the live split-detection code is the `_archive/` copy; the
top-level copies are dead duplicates.**

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `__init__.py` | 11 | Package docstring (Phase-2 post-export analysis, empirical/no-enforcement) | — |
| `split_detection.py` | 246 | Core split-detection dataclasses/helpers | **ORPHANED dead duplicate** of `_archive/split_detection.py` |
| `split_detection_element_level.py` | 487 | (duplicate content) | **ORPHANED dead duplicate** of `_archive/split_detection_element_level.py` |
| `split_detection_file_level.py` | 672 | (duplicate content) | **ORPHANED dead duplicate** of `_archive/split_detection_file_level.py` |
| `_archive/split_detection_file_level.py` | 672 | File-level clustering CLI | subprocess of `run_split_detection_all.py`, Phase 1 (l.353, see §3); also imported directly by `tests/test_split_named_clusters_and_thresholds.py:11` |
| `_archive/build_reference_standards.py` | 167 | Builds one reference-standard JSON per file-level cluster | subprocess of `run_split_detection_all.py`, Phase 2 (l.396) |
| `_archive/intradomain_summary.py` | 250 | Profiles cluster representatives, computes discriminating fields | subprocess of `run_split_detection_all.py`, Phase 2.5 (l.411) |
| `_archive/emit_intradomain_definition.py` | 141 | Materializes the Intradomain Standard (IDS) + file→IDS assignment | subprocess of `run_split_detection_all.py`, Phase 2A/2B (l.424) |
| `_archive/derive_join_keys_by_ids.py` | 570 | Greedy forward-selection of IDS-scoped join keys | subprocess of `run_split_detection_all.py`, Phase 2C (l.438) |
| `_archive/apply_join_keys_by_ids.py` | 194 | Applies an IDS-scoped join-key policy (verification, writes CSV only) | subprocess of `run_split_detection_all.py`, Phase 2D (l.455) |
| `_archive/calibrate_join_key_gates.py` | 79 | Derives suggested gate thresholds from the IDS key-selection report | subprocess of `run_split_detection_all.py`, Phase 2E, opt-in `--run-calibration` (l.471) |
| `_archive/pareto_join_keys_by_ids.py` | 545 | Bounded beam-search Pareto front over IDS-scoped join-key candidates | subprocess of `run_split_detection_all.py`, Phase 2F, opt-in `--run-pareto` (l.484) |
| `_archive/split_detection_element_level.py` | 487 | Element-level classification vs. reference standards, contamination scoring | subprocess of `run_split_detection_all.py`, Phase 3 (l.502) |
| `_archive/io.py` | 333 | Shared export-loading layer (`ExportFile`, `load_exports`, etc.) | Library; imported by 13+ `_archive` modules; also directly by `tests/test_fingerprint_export_discovery.py:13` |
| `_archive/report.py` | 116 | Shared report-writing helpers | Library; imported by 12 `_archive` modules incl. the 9 live ones above |
| `_archive/split_detection.py` | 246 | Core split-detection dataclasses/helpers (`Cluster`, `SplitSignal`, silhouette scoring) | Library; imported by `_archive/split_detection_file_level.py` |
| `_archive/index.py` | 150 | Shared `DomainIndex` dataclass + `build_domain_index()` | Library; imported by 8 paused `_archive` modules below |
| **— remaining 17 files: intentionally-paused Phase-2-baseline tooling, per `CLAUDE.md` and confirmed via `docs/tools_DEPRECATED.md` cross-check —** |||
| `_archive/annotate_cluster_labels.py` | 327 | Annotates cluster summaries with synthesized labels | **PAUSED** — no caller found |
| `_archive/apply_join_keys_by_ids.py` | — | *(listed above, live)* | |
| `_archive/attributes.py` | 69 | Per-attribute stability/stress reporting **stub** (docstring: "implementation will be added later") | **PAUSED** — no caller; no file I/O implemented |
| `_archive/backfill_cluster_label_inputs.py` | 371 | Backfills/repairs intradomain inputs for `annotate_cluster_labels.py` | **PAUSED** — no caller |
| `_archive/calibrate_join_key_gates.py` | — | *(listed above, live)* | |
| `_archive/compare.py` | 154 | Shared pairwise change-classification dataclass/logic | Library; imported by `report.py` and `run_change_type.py` (both paused) |
| `_archive/domain_identity_contract.py` | 41 | Thin loader over `contracts/domain_identity_keys_v2.json` | **PAUSED** — no caller |
| `_archive/pareto_with_splits.py` | 214 | "Enhanced Pareto analysis with automatic split detection" wrapping `pareto_joinkey_search.py` | **PAUSED** — no caller |
| `_archive/run_attribute_stress.py` | 142 | Per-attribute stability/divergence stress ranking | **PAUSED** — explicitly named in `docs/tools_DEPRECATED.md` |
| `_archive/run_attribute_stress_all_joinable.py` | 179 | Same, over all joinable records | **PAUSED** |
| `_archive/run_candidate_joinkey_simulation.py` | 147 | Simulates candidate join-key combos, scores collision/fragmentation | **PAUSED** — only caller is `run_dimension_types_by_family.py`, itself unreferenced |
| `_archive/run_change_type.py` | 119 | Baseline-vs-file change classification | **PAUSED** — named explicitly in `docs/tools_DEPRECATED.md`; only caller is `run_dimension_types_by_family.py` |
| `_archive/run_collision_differencing.py` | 342 | Diagnoses same-join_hash/different-sig_hash collision groups | **PAUSED** — only caller is `run_dimension_types_by_family.py` |
| `_archive/run_dimension_types_by_family.py` | 411 | Orchestrator hub re-running 7 sibling scripts per `dim_attr.shape` family | **PAUSED** — no external caller; named in `docs/tools_DEPRECATED.md`/`docs/tools_PHASE0_1_2_MAP.md` |
| `_archive/run_identity_collision_diagnostics.py` | 178 | Per-file/per-join_hash identity-collision diagnostics | **PAUSED** — only caller is `run_dimension_types_by_family.py` |
| `_archive/run_joinhash_label_population.py` | 231 | Population of cosmetic/label values per join_hash group | **PAUSED** — explicitly named in `docs/tools_DEPRECATED.md`'s "replacements" list; only caller is `run_dimension_types_by_family.py` |
| `_archive/run_joinhash_parameter_population.py` | 440 | Population/variability of parameter values per join_hash group | **PAUSED** — named in `docs/tools_DEPRECATED.md`; only caller is `run_dimension_types_by_family.py` |
| `_archive/run_population_stability.py` | 114 | Presence stability of join_hash keys across files | **PAUSED** — only caller is `run_dimension_types_by_family.py` |
| `_archive/run_text_types_candidate_joinkey_simulation.py` | 175 | `text_types`-specific typography-variant candidate simulation | **PAUSED** — no caller |
| `_archive/run_view_category_overrides_joinkey_analysis.py` | 197 | One-off VCO join-key hypothesis script (single-export CLI, stdout only) | **PAUSED** — no caller |
| `_archive/run_view_templates_joinkey_analysis.py` | 200 | One-off view_templates join-key evaluation (single-export CLI, stdout only) | **PAUSED** — no caller |
| `_archive/stability.py` | 90 | Population-level presence-stability helpers | Library; imported by `run_attribute_stress.py`, `run_attribute_stress_all_joinable.py`, `run_population_stability.py` (all paused) |

*(Table above lists all 37 `patterns_analysis/`+`_archive/` files; the 9 "live" ones are called out with their `run_split_detection_all.py` phase; the remaining 28 — 3 dead top-level duplicates + `io.py`/`report.py`/`split_detection.py`/`index.py`/`compare.py`/`stability.py` shared-by-paused-modules + 17 genuinely-paused entry scripts — are accounted for above.)*

### 2.8 `tools/archetype/` + `review/` — archetype workflow (11 files)

**Not invoked by either target runbook** (confirmed: zero grep hits for "archetype" in both `.ps1`
files). No wrapper script exists inside `tools/archetype/` either — the pipeline is sequenced only by
`tools/archetype/README.md`'s prose, which documents a finer-grained **8-step chain** than CLAUDE.md's
5-name summary:
`generate_reference_graph.py` (Stage 0) → `build_cross_domain_items.py` (1) →
`compute_cross_domain_cooccurrence.py` (2) → `generate_archetype_candidates.py` (2.5) →
*[human curates `config/archetype/archetype_definitions.json`]* →
`assign_archetype_classifications.py` (3) → `validate_archetype_signals.py` (4) →
`cluster_archetype_signals.py` (5) → *[human cluster review]* → `prepare_archetype_review.py`.

| File | Lines | Purpose | Position / invocation |
|---|---|---|---|
| `_common.py` | 190 | Shared IO/logging/edge-alias helpers | Library; imported by all 8 numbered-chain scripts (not by `discover_vfd_edges.py` or `review/select_archetype_review_files.py`) |
| `discover_vfd_edges.py` | 1353 | Discover View Filter Definition dynamic edges from flat identity_items | Optional offline feeder to Stage 0, not itself numbered; degrades gracefully if skipped |
| `generate_reference_graph.py` | 299 | **Stage 0** — resolve which cross-domain edges are data-backed; merge in VFD dynamic edges | Manual step 0 |
| `build_cross_domain_items.py` | 285 | **Stage 1** — materialize per-(file,edge) join rows with join hashes | Manual step 1 |
| `compute_cross_domain_cooccurrence.py` | 305 | **Stage 2** — edge-pair / join_hash-pair co-occurrence detection | Manual step 2 |
| `generate_archetype_candidates.py` | 342 | **Stage 2.5** — turn recurring patterns into draft archetype definitions | Manual step 2.5, followed by unscripted human curation |
| `assign_archetype_classifications.py` | 474 | **Stage 3** — per-file archetype classification (Power BI slicer source) | Manual step 3 |
| `validate_archetype_signals.py` | 458 | **Stage 4** — validate signal coherence at `sig_hash` grain | Manual step 4 (uncredited in CLAUDE.md's summary) |
| `cluster_archetype_signals.py` | 801 | **Stage 5** — cluster co-varying signals into composite groups | Manual step 5 |
| `prepare_archetype_review.py` | 1048 | Build human-reviewable per-cluster drill-down CSVs + file-open schedule | Manual step after Stage 5's human review |
| `review/select_archetype_review_files.py` | 611 | Greedy set-cover selection of minimum files to review all clusters | **ORPHANED / likely superseded** — not mentioned in the README; writes the same output filenames (`archetype_review_schedule.csv`, `archetype_review_gaps.csv`) as `prepare_archetype_review.py`'s own Stage 9, via a different algorithm |

### 2.9 `tools/migration/` — one-off data migrations (4 files)

All 4 confirmed standalone one-offs (argparse + `__main__`, no `tools/` peer imports); not invoked by
either runbook.

| File | Lines | Purpose |
|---|---|---|
| `compress_fingerprint_json.py` | 261 | Converts dev-mode (indented) export JSON to compact production format |
| `extract_first_record.py` | 98 | Extracts first record per domain from a fingerprint JSON payload |
| `migrate_materials_identity_items.py` | 333 | Injects new `material.*` identity items into existing materials records |
| `reformat_to_flat_items.py` | 158 | Reformats nested phase-2 bucket records into canonical flat `items:[{k,v,q}]` shape; imports `core/canonical_items.py` (extraction-side dependency, per CLAUDE.md) |

### 2.10 `tools/compare_templates_stand-alone/` (1 file)

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `compare_view_templates_stand-alone.py` | 776 | Compares view templates + category overrides across two export JSONs without name matching | Standalone, stdlib-only; independent of the segment/governance pipeline; does **not** import `tools/lib/vt_profile.py` or `diff_engine.py` despite topical overlap |

### 2.11 `tools/_archive/` (1 file, tools-root archive — distinct from `patterns_analysis/_archive/`)

| File | Lines | Purpose | Invocation |
|---|---|---|---|
| `join_key_derivation_phase05.py` | 550 | Phase-0.5: derives `join_hash` post-export from `identity_basis.items` + join-key policy | **Live, load-bearing** despite the directory name — star-imported by `tools/join_key_derivation.py`, whose `md5_utf8_join_pipe`/`serialize_identity_items` are in turn imported by `apply_join_policy.py` (reached via Run A's `apply` stage) |

---

## 3. Reconstructed execution order (cited to runbook line numbers)

### 3.1 `corpus_update_runbook.ps1` — the primary corpus pipeline

**Run A** (`-Run A`, dispatched at `corpus_update_runbook.ps1:102`) — *"flatten / apply / placeholders"*:

1. `python tools/run_extract_all.py $EXPORTS --stages sig_hash,flatten,apply,placeholders ...` (l.105–110)
   - **flatten (T0)**: in-process `extractor.emit_records()` → `records.csv`, `identity_items.csv`
     (or sharded `identity_items_by_domain/`), `file_metadata.csv`, `parameter_rows.csv`
   - **sig_hash (T0.5)**: in-process, mutates `records.csv`'s `sig_hash`/`status` in place;
     `diagnostics/sig_hash_policy_diagnostics.json`
   - **apply (T2)**: subprocess `apply_join_policy.py` → mutates `records.csv` join fields in place
   - **placeholders (T2b)**: subprocess `bundle_analysis/placeholder_exclusions.py` → non-fatal on failure
2. If `-NameKey`: `apply_name_key_policy.py` (l.116–119) → `name_key_results.csv`
3. **MANDATORY PAUSE** (l.87–98, restated l.124–134): human edits `file_metadata.csv` — sets
   `governance_role`, `client_label`, `business_center_label`, `collection_label`, `unit_system` per
   new file. Run B hard-fails without this.

**Run B** (`-Run B`, l.138) — *"authority / patterns / patch"*:

1. `python tools/run_extract_all.py $EXPORTS --stages authority,patterns` (l.142–145)
   - Reads `records.csv`/`file_metadata.csv`; `_check_governance_field_completeness()` hard-fails on
     blank/N-A `client_label`/`business_center_label`; enforces the join-policy gate
     (`_enforce_policy_gate`, blocks `sig_hash_as_join_key.v1` unless `--allow-sig-hash-join-key`)
   - subprocess `label_synthesis/build_label_population.py` → `{domain}.joinhash_label_population.csv`
   - in-process `extractor.emit_analysis()` → `domain_patterns.csv`, `pattern_presence_file.csv`, etc.
     (uses `label_resolver.py`'s resolution chain)
   - in-process `emit_element_dominance()` → `element_dominance.csv`,
     `element_characterization_thresholds.csv`
2. `label_synthesis/patch_all_domain_patterns.py` (l.149–151) — B2, patches
   `pattern_label_human`/`_source` from the shared `llm_name_cache.json`
3. If `-NameKey` (optional, not required before Run C): `generate_name_key_patterns.py
   --comparison-target name` (l.159–162)

**Run C** (`-Run C`, l.170) — *"segments + all/used bundle analysis"*:

1. **C1**: `build_segment_manifest.py` (l.186–189) → `segment_manifest.csv`, `run_registry.csv`,
   `segment_membership.csv`
2. **C1.5** (only `-ForceAll`, PowerShell-only, l.198–205): clears stale `latent_purgeable.csv`
3. **C2**: `run_segment_orchestrator.py` (l.245–255) — per segment, in level order:
   - Step 1 (prepare, in-process)
   - Step 2: subprocess `run_extract_all.py --stages patterns` (segment-scoped patterns)
   - Step 2b (opt-in `-NameKey`): subprocess `generate_name_key_patterns.py --comparison-target name`
   - Step 3 (only `run_type=bundle` segments): subprocess `bundle_analysis/run_bundle_analysis.py
     --purge-view both`
   - Step 3b (opt-in `-NameKey`, `run_type=bundle` only): same script, `--comparison-target name`
4. **C2.5**: `build_results_registry.py` (l.266–269) → `results_registry.csv`
5. **C3**: `label_synthesis/patch_all_domain_patterns.py` again (l.273–275) — re-patches every segment's
   `domain_patterns.csv` too

**Explicitly out of either runbook, per the runbook's own on-screen text** (l.280): *"Cross-segment
comparison: run `compare_cross_segment.py` separately."* Everything in §2.1's governance cluster
(`compare_cross_segment.py`, `compare_governance_populations.py`, `governance_manifest.py`,
`governance_relationships.py`, `generate_governance_narrative.py`, `analyze_promotion_candidates.py`,
`governance/standards_governance_report.py`) is manual/standalone, run after Run C at the operator's
discretion.

### 3.2 `label_refresh_runbook.ps1` — separate, on-demand label-quality pass

Per its own header comment (l.106–110): run *"after `corpus_update_runbook.ps1` Run C completes and the
corpus is stable"* — not chained automatically, a separate manual invocation.

1. **L1** (l.25–29): `label_synthesis/build_identity_items_lookup.py` → `identity_items_by_joinhash.csv`
2. **L2** (l.35–64, foreach over 6 domains: arrowheads, fill_patterns_drafting, fill_patterns_model,
   line_patterns, line_styles, view_filter_definitions): `python -m
   tools.label_synthesis.synthesize_fragmented_labels ...` per domain → updates `llm_name_cache.json`
3. **L3** (l.66–70): `label_synthesis/patch_all_domain_patterns.py` — the **same script** as Run
   B2/C3 — re-patches every `domain_patterns.csv` (corpus + all segments) from the refreshed cache
4. **L4** (l.72–99, loop over completed `run_type=bundle` segments × `{all,used}` views):
   `export_bundle_pattern_detail.py` per segment/view → 5 BI CSVs

### 3.3 Out-of-scope third runbook (flagged, not documented further)

`tools/run_discovery_sweep.ps1` (root-level, 3-line wrapper delegating to
`tools/run_discovery_sweep.py` → `discovery_orchestrator.py`) is a **third runbook** not named in this
task's scope. It is the only confirmed caller of `discovery_orchestrator.py`, and transitively of
`discover_join_policy.py`/`discover_hash_policy.py` as direct CLI targets. This matters because
`run_extract_all.py`'s `discover` and `flat_tables` stages exist in the stage machine but are **not**
selected by either of the two in-scope runbooks' confirmed `--stages` lists — so policy discovery, if it
happens in this operator's workflow at all, happens through this out-of-scope third runbook. Flagged as
Open Question 2 (§7).

---

## 4. Data lineage

Chain from raw extractor JSON through to `generate_governance_narrative.py`'s evidence package.
"External/manual" marks a genuine boundary (human action or a separate script run at the operator's
discretion, not a code gap).

```
[EXTERNAL: Revit/Dynamo extraction — domains/, runner/, core/ — OUT OF SCOPE, see §1]
        │
        ▼
$EXPORTS/*.details.json (preferred) or *.index.json  (never *.legacy.json)
        │
        ▼  extractor.emit_records()  [flatten, T0 — in-process within run_extract_all.py]
records.csv, identity_items.csv (or identity_items_by_domain/*.csv + .complete), file_metadata.csv,
parameter_rows.csv
        │
        ▼  _apply_sig_hash_to_phase0()  [sig_hash, T0.5 — in-process]
        │  in: policies/domain_sig_hash_policies.json
        │      [EXTERNAL/MANUAL: generate_sig_hash_policy.py has no runbook caller — presumed run
        │       manually whenever contracts/domain_identity_keys_v2.json changes]
records.csv (sig_hash/status mutated in place), diagnostics/sig_hash_policy_diagnostics.json
        │
        ▼  apply_join_policy.py  [apply, T2 — subprocess]
        │  in: policies/domain_join_key_policies.json ($JOIN_POL)
        │      (uses join_key_derivation.py shim → _archive/join_key_derivation_phase05.py)
records.csv (join_hash/join_key mutated in place)
        │
        ▼  bundle_analysis/placeholder_exclusions.py  [placeholders, T2b — subprocess, non-fatal]
        │  in: policies/placeholder_known_defaults.json, file_metadata.csv
placeholder_exclusions_{domain}.csv, placeholder_exclusions_all.csv, domain_authority_by_file*.csv
        │
        ▼  [MANUAL, MANDATORY: human edits file_metadata.csv — governance_role, client_label,
        │   business_center_label, collection_label, unit_system]
        │
        ▼  build_label_population.py [subprocess]  +  extractor.emit_analysis() [in-process, uses
        │  label_resolver.py's 5-layer chain: curator → synopsis_formatters/*.py → near-dup merge →
        │  modal (joinhash_label_population.csv) → llm_name_cache.json → rank fallback]
        │  +  emit_element_dominance() [in-process]
domain_patterns.csv, pattern_presence_file.csv, element_dominance.csv,
element_characterization_thresholds.csv
        │
        ▼  patch_all_domain_patterns.py  [Run B2 / Run C3 / label_refresh L3 — same script,
        │   3 call sites across both runbooks]
domain_patterns.csv (pattern_label_human/_source patched in place)
        │
        ├──[SEPARATE OFFLINE CYCLE, label_refresh_runbook.ps1, not chained automatically]──┐
        │   L1 build_identity_items_lookup.py → identity_items_by_joinhash.csv             │
        │   L2 synthesize_fragmented_labels.py (×6 domains) → llm_name_cache.json (updated) │
        │   L3 patch_all_domain_patterns.py (same script as above) → re-patches everywhere  │
        │   L4 export_bundle_pattern_detail.py → 5 BI CSVs per segment × view               │
        └──────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼  build_segment_manifest.py  [Run C1]
        │  in: file_metadata.csv (post-human-edit)
segment_manifest.csv, run_registry.csv, segment_membership.csv
        │
        ▼  run_segment_orchestrator.py  [Run C2] — per segment, in level order
        │   Step 2: run_extract_all.py --stages patterns (segment-scoped re-derivation)
        │   Step 2b (opt): generate_name_key_patterns.py --comparison-target name
        │   Step 3 (bundle segments): bundle_analysis/run_bundle_analysis.py --purge-view both
        │     (step0→step7 per domain/population/scope; compute_latent_purgeable.py runs
        │      conditionally if latent_purgeable.csv is missing)
        │   Step 3b (opt): same, --comparison-target name
results/bundle_analysis/{all,used}/<domain>/{bundles.csv, bundle_membership.csv,
bundle_dag_edges.csv, pattern_bundle_classification.csv, file_bundle_classification.csv, ...},
domain_placeholder_exclusions.csv, results/bundle_analysis/name_all/... (opt-in)
        │
        ▼  build_results_registry.py  [Run C2.5]
results_registry.csv
        │
        ▼  [MANUAL/EXTERNAL BOUNDARY — runbook's own text says "run separately", l.280]
        │
        ▼  compare_cross_segment.py  [standalone]
        │  in: segment_manifest.csv, segment_membership.csv, run_registry.csv, file_metadata.csv,
        │      per-segment membership_matrix.csv / domain_patterns.csv / bundle_membership.csv
cross_segment_summary.csv, cross_segment_pooled.csv, cross_segment_delta.csv,
cross_segment_governance_states.csv, cross_segment_governance_state_summary.csv,
cross_segment_file_pairs.csv, cross_segment_union_inventory.csv, comparison_registry.csv,
pattern_reuse_distribution.csv, pattern_reuse_summary_by_{client,domain}.csv, project_*_matrix.csv,
project_fragmentation_diagnostic.csv, population_containment_thresholds.csv,
matrix_output_manifest.csv
        │
        ├──▶ governance_manifest.py [standalone, parallel disjoint partition, in: file_metadata.csv]
        │        → governance_manifest.csv, governance_membership.csv
        │        ├──▶ compare_governance_populations.py [standalone; imports compare_cross_segment.py's
        │        │        comparison functions] → governance comparison CSVs
        │        └──▶ governance_relationships.py [standalone; imports governance_manifest.py]
        │                 → governance_relationships.csv, governance_bc_client_matrix.csv,
        │                   governance_client_bc_matrix.csv
        │                   [MANUAL HAND-OFF: consumed by generate_governance_narrative.py via a
        │                    CLI file-path flag, not a Python import]
        │
        ├──▶ analyze_promotion_candidates.py [standalone, NOT pipeline-wired]
        │        in: cross_segment_governance_states.csv + pattern_reuse_distribution.csv
        │        → promotion_candidates.csv, governed_but_underused.csv, ... (5 more), domain_rollup.csv
        │
        ▼  generate_governance_narrative.py  [standalone, terminal stage]
        │  in: cross_segment_summary.csv + cross_segment_pooled.csv (required), ~20 optional flags
        │      pulling the rest of compare_cross_segment.py's outputs above, plus run_registry.csv,
        │      file_metadata.csv, policies/client_sector.csv, governance_relationships.py's 2 matrix
        │      CSVs, policies/governance/*.json (via governance_policy.py)
governance_narrative_context.md
+ (via governance_evidence_package.py) governance_package_manifest.json, _health.json,
  governance_evidence_map.json, governance_findings.json, governance_brief.md,
  governance_file_inventory.json
```

**Parallel, independent branches off the same `records.csv`/`identity_items` foundation** (not further
upstream of governance narrative):

- **Split detection**: `run_extract_all.py`'s opt-in `split` stage (not in either runbook's confirmed
  `--stages`) → subprocess `run_split_detection_all.py` → 9 live `patterns_analysis/_archive/` modules
  (10-step sequence, §3 mirror in §2.7) → per-domain element-level classification reports. Nothing
  downstream confirmed to consume these reports.
- **Archetype workflow**: fully separate 8-step manual chain (§2.8), rooted in
  `identity_items_by_domain/*.csv` + `config/archetype/static_edges_seed.json`, terminating in
  `archetype_classifications.csv` (Power BI slicer source) and `signal_clusters.json`. No connection to
  the governance narrative pipeline.
- **`governance/standards_governance_report.py`**: reads raw export JSON directly, bypassing the entire
  CSV chain above. Fully disconnected lineage.

**Flagged gaps** (input with no producer traceable inside this pipeline's scope):

- `file_metadata.csv`'s `governance_role`/`client_label`/`business_center_label`/`collection_label`/
  `unit_system` columns — **manual human edit** between Run A and Run B (runbook l.87–98).
- `policies/domain_sig_hash_policies.json`, `policies/domain_join_key_policies.json` — produced by
  `generate_sig_hash_policy.py` / (presumably) `discover_join_policy.py`'s `--out-policy`, neither of
  which either runbook calls. **External/manual**, likely via the out-of-scope third runbook or an
  ad hoc invocation.
- `policies/client_sector.csv` — per `CLAUDE.md`, "synthetic client_label → sector example;
  deployments pass an approved mapping" — **external, deployment-supplied**.
- `config/archetype/archetype_definitions.json` — curated by a human from
  `generate_archetype_candidates.py`'s output; **external/manual**.
- `config/archetype/static_edges_seed.json` — "Human-curated (DP1) archetype definitions" per
  `CLAUDE.md`; **external/manual**.

---

## 5. Subdirectory role summary

| Subdir | Role | Live / standalone / legacy |
|---|---|---|
| `bundle_analysis/` | Bundle-mining pipeline (step0→step7 + placeholder exclusions + optional compare) | **Live** — subprocess of `run_segment_orchestrator.py` (Run C2/C2.5 equivalent). One genuine no-op stub (`step7_overlap_report.py`); `placeholder_exclusions.py` vs `placeholder_exclusions_legacy.py` schema mismatch (§7 Q8) |
| `join_key_discovery/` | Shared join-key candidate scoring/search library | **Live** (`eval.py`, `greedy.py` — imported by 5+ files) **+ one orphan** (`materials_joinkey_discover.py`, self-contained, zero callers) |
| `lib/` | Intended shared diff-engine library for domain-family comparisons | **Mostly orphaned** — `domain_profile.py` is used by its two siblings, but `diff_engine.py` and `vt_profile.py` have zero callers anywhere in the repo; looks like an abandoned generic-diff-CLI effort, distinct from the actual `compare_templates_stand-alone/` tool |
| `governance/` | Standalone HTML governance report generator | **Legacy/parallel, disconnected** — reads raw export JSON directly, zero internal imports, not part of the current CSV-based governance chain; not deprecated formally but functionally superseded in practice |
| `label_synthesis/` | Label resolution (deterministic + LLM) for pattern labels | **Mixed** — `label_resolver.py` is live-pipeline-integrated (via `extractor.py`); `patch_all_domain_patterns.py` is shared by both runbooks; L1/L2/L4 form the separate `label_refresh_runbook.ps1`; `domain_prompts/`+`synopsis_formatters/` are dynamically-loaded plugin families with asymmetric domain coverage; `build_semantic_groups.py` and `patch_domain_patterns_labels.py` are orphaned |
| `patterns_analysis/` | Split-detection analysis | **Live core (9 modules) + large paused tail** — despite the name, the *live* code is inside `_archive/`; the non-`_archive` top-level files are dead duplicates; ~17 of the remaining `_archive/` scripts are intentionally-paused Phase-2-baseline tooling (confirmed via `docs/tools_DEPRECATED.md`) |
| `migration/` | One-off/point-in-time data migration scripts | **Historical/standalone** — confirmed no runbook or peer-script callers; run manually when needed |
| `lib/` *(see above)* | | |
| `compare_templates_stand-alone/` | Standalone view-template comparison tool | **Standalone**, confirmed independent of both the segment/governance pipeline and `tools/lib/` |
| `archetype/` | Cross-domain co-occurrence archetype clustering + DP1 human-curation workflow | **Standalone workflow**, not runbook-invoked, sequenced only by its own README (8 steps, richer than CLAUDE.md's 5); one apparently-orphaned duplicate script (`review/select_archetype_review_files.py`) |

---

## 6. Proposed documentation structure & Phase 1 processing order

### Recommended split: by logical stage, not directory or runbook phase

Directory boundaries don't track live/dead status cleanly (`patterns_analysis/`, `label_synthesis/`,
`lib/` all mix live and orphaned code in the same folder), and some scripts serve multiple runbook
phases (`patch_all_domain_patterns.py` alone has 3 call sites across both runbooks). A per-logical-stage
split keeps each doc's scope coherent regardless of where the files physically live:

1. **Orchestration core** — `run_extract_all.py`, `extractor.py`, `emit_element_dominance.py`
2. **Join-key & sig-hash policy tooling** — `apply_join_policy.py`, `join_key_derivation.py` +
   `_archive/join_key_derivation_phase05.py`, `discover_join_policy.py`, `discover_hash_policy.py`,
   `join_key_discovery/eval.py`+`greedy.py`, `generate_sig_hash_policy.py`,
   `suggest_discovery_params.py`, `validate_policy_field_coverage.py`, `pareto_joinkey_search.py`
3. **Name-key projection** — `apply_name_key_policy.py`, `generate_name_key_patterns.py`,
   `pattern_id_utils.py`
4. **Label synthesis & resolution** — all of `label_synthesis/` (including the two dynamic-plugin
   families and the two orphans, flagged as such)
5. **Segment lattice & orchestration** — `build_segment_manifest.py`, `run_segment_orchestrator.py`,
   `build_results_registry.py`, `extract_segment_subtree.py`
6. **Bundle-mining pipeline** — all of `bundle_analysis/` + `compute_latent_purgeable.py`
7. **Cross-segment & governance comparison** — `compare_cross_segment.py`,
   `compare_governance_populations.py`, `governance_manifest.py`, `governance_relationships.py`,
   `enterprise_policy.py`, `na_token.py`, `compute_governance_thresholds.py`
8. **Governance narrative & evidence package** — `generate_governance_narrative.py`,
   `governance_evidence_package.py`, `governance_policy.py`, `analyze_promotion_candidates.py`
9. **Split detection** — `run_split_detection_all.py` + the 9 live `patterns_analysis/_archive/`
   modules only; the paused tail gets a one-paragraph "historical, not documented in detail" note
10. **Archetype workflow** — all of `archetype/` (own doc, self-contained)
11. **BI export & flat tables** — `export_bundle_pattern_detail.py`, `export_to_flat_tables.py`
12. **Legacy Phase-1 tooling** — `domain_authority.py`, `pairwise_analysis.py`,
    `population_framing.py` (pre-authority probe mode; lower priority)
13. **Standalone/manual diagnostic tools** — `inspect_lft_similarity.py`,
    `reset_wall_types_for_reapply.py`, `compare_templates_stand-alone/`,
    `governance/standards_governance_report.py`, `similarity_compare.py` (deprecated),
    `acc_scan_dc.py`/`acc_sync_dc.py`
14. **Migration one-offs** — `migration/` (lowest priority, historical)
15. **Orphaned/dead-code inventory** — `lib/diff_engine.py`+`vt_profile.py`,
    `patterns_analysis/`'s 3 top-level dead duplicates, `join_key_discovery/materials_joinkey_discover.py`,
    `label_synthesis/patch_domain_patterns_labels.py`+`build_semantic_groups.py`,
    `archetype/review/select_archetype_review_files.py`, `compute_governance_thresholds.py`,
    `extract_segment_subtree.py` — a **flag-only list**, not trace-block docs, pending a human decision
    on delete-vs-document (§7 Q1)

### Proposed Phase 1 processing order (smallest/cleanest first, most-tangled last)

1. Name-key projection (3 files, clean linear chain) — proves the trace-block convention cheaply
2. Segment lattice & orchestration (4 files, clear I/O)
3. BI export & flat tables (2 files)
4. Join-key & sig-hash policy tooling (moderate size, many files but clear roles)
5. Bundle-mining pipeline (18 files, but internally well-structured `stepN` convention)
6. Label synthesis & resolution (26 files, plugin-heavy but mechanical once the dynamic-loading
   convention is documented once)
7. Cross-segment & governance comparison (mid-size files, clear chain)
8. Split detection (live 9 only; needs the live/paused split established first)
9. Archetype workflow (11 files, separate but internally coherent)
10. Orchestration core (`run_extract_all.py` 1234 lines + `extractor.py` 1515 lines — central but
    complex; easier once everything it calls is already documented)
11. Governance narrative & evidence package (`generate_governance_narrative.py` 6890 lines +
    `governance_evidence_package.py` 1621 lines — largest, do last)
12. `compare_cross_segment.py` alone (5383 lines — recommend splitting this single file's trace-block
    pass by function group, e.g. pooled-comparison / delta / governance-state / matrix-emission,
    rather than attempting it as one pass)

---

## 7. Open questions requiring a decision before Phase 1 prompts are written

1. **Orphaned/dead code** (`lib/diff_engine.py`+`vt_profile.py`, `patterns_analysis/`'s 3 dead
   top-level duplicates, `join_key_discovery/materials_joinkey_discover.py`,
   `label_synthesis/patch_domain_patterns_labels.py`, `label_synthesis/build_semantic_groups.py`,
   `archetype/review/select_archetype_review_files.py`) — should these get Phase 1 trace-block docs at
   all, or be explicitly excluded/flagged for deletion first? Documenting dead code as if it were live
   risks misleading future readers.
2. **The third runbook**, `tools/run_discovery_sweep.ps1` → `discovery_orchestrator.py` →
   `discover_join_policy.py`/`discover_hash_policy.py`, is the only confirmed path that exercises the
   `discover` stage — otherwise dead in both in-scope runbooks. Should it be pulled into scope for a
   complete picture, since without it `discover`/`flat_tables` read as orphaned despite being wired into
   `run_extract_all.py`'s stage machine?
3. **`governance/standards_governance_report.py`** reads raw export JSON directly and is structurally
   disconnected from the current `compare_cross_segment.py`/`generate_governance_narrative.py` CSV
   pipeline. Confirm whether it's still in active use, or should be documented as deprecated/candidate
   for removal (parallel to how `similarity_compare.py` is already formally deprecated).
4. **The ~17 paused `patterns_analysis/_archive/` scripts** — document briefly as historical/paused
   context, or skip entirely from Phase 1? (CLAUDE.md already frames them as "not dead code," so
   skipping them silently may be wrong, but full trace-block treatment for genuinely-unused code seems
   like the wrong investment.)
5. The `governance_relationships.py` → `generate_governance_narrative.py` hand-off
   (`governance_bc_client_matrix.csv`/`governance_client_bc_matrix.csv`) is a manual CLI-flag file path,
   not scripted anywhere. Should the doc explicitly call out "run `governance_relationships.py` first"
   as a prerequisite, or is that assumed operator knowledge?
6. Should `bundle_analysis/step7_overlap_report.py` (a genuine no-op stub) and
   `tools/_archive/join_key_derivation_phase05.py` (an "archived" path that is actually load-bearing)
   get a specific trace-block marker convention (e.g. `STUB — always empty` / `ARCHIVED PATH, LOAD-BEARING`)
   so future readers don't misjudge them by directory/filename alone?
7. Three near-identical copies of the same modal-label-check logic and threshold constants now exist
   (`label_resolver.py`, `label_synthesis/patch_all_domain_patterns.py`,
   `label_synthesis/patch_domain_patterns_labels.py`). Worth a doc callout flagging drift risk even
   though fixing it is out of scope for this read-only pass?
8. `bundle_analysis/placeholder_exclusions.py`'s own `main()` writes a richer per-domain output schema
   that nothing downstream reads — `run_bundle_analysis.py`'s `discover_populations()` actually consumes
   `placeholder_exclusions_legacy.py`'s simpler schema via a compatibility wrapper. Worth flagging in the
   eventual trace-block as "two outputs exist; only the legacy-schema one is consumed downstream"?
